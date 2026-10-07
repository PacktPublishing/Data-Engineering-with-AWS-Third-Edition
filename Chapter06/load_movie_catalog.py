# ==================================================================
# load_movie_catalog.py
#
# Data Engineering with AWS, 3rd Edition - Chapter 6
#
# This AWS Glue ETL job copies our movie catalog from the Aurora
# PostgreSQL source database into the Silver zone, writing each table
# to the matching Apache Iceberg table in the "movies" namespace of
# our dataeng-silver-zone S3 Tables bucket.
#
# For each of the five catalog tables, the job:
#   1. Reads the table from Aurora, using a Glue connection
#   2. Selects only the columns our S3 Tables target schema needs
#   3. Replaces the contents of the target table with the fresh data
#
# Because every run fully replaces the target data, the job is safe
# to run more than once.
#
# Job parameters (set on the Glue job's "Job details" tab):
#   --glue_catalog_id  <your-account-id>:s3tablescatalog/dataeng-silver-zone
#   --conf             spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
# ==================================================================

import sys

# Glue-specific libraries. GlueContext wraps Spark with Glue features
# (such as reading through Glue connections), Job tracks the job run,
# and getResolvedOptions reads the parameters passed to the job.
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Standard PySpark libraries. SparkConf holds Spark configuration
# settings, and SparkContext is the entry point to Spark itself.
from pyspark import SparkConf
from pyspark.context import SparkContext


# ------------------------------------------------------------------
# Job parameters
# ------------------------------------------------------------------
# Read the parameters passed in when the job runs. JOB_NAME is
# supplied automatically by Glue. glue_catalog_id is one of our own
# job parameters, and identifies the S3 Tables catalog (our
# dataeng-silver-zone table bucket) that we want to write to.
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "glue_catalog_id"]
)

# The name of the Glue connection we created earlier in this chapter.
# The connection holds everything needed to reach our Aurora cluster:
# the database endpoint, a reference to the credentials stored in
# AWS Secrets Manager, and the VPC networking details. This job always
# reads from the same database, so we hardcode the name here rather
# than passing it in as a job parameter.
CONNECTION_NAME = "movie-catalog-aurora-connection"


# ------------------------------------------------------------------
# Spark and Iceberg catalog configuration
# ------------------------------------------------------------------
# Build a SparkConf BEFORE creating the SparkContext, so these settings
# are applied at session creation time. Only spark.sql.extensions is
# passed as a --conf job parameter, since it is a static configuration
# that must be in place before Glue creates the Spark session. All of
# our other Iceberg and S3 Tables catalog settings are set here.
conf = SparkConf()

# Register a Spark catalog named "s3tables", backed by Apache Iceberg.
# Any table we reference through this catalog is read and written as
# an Iceberg table.
conf.set("spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog")

# Use the AWS Glue Data Catalog to look up table metadata for our
# "s3tables" catalog. S3 Tables buckets are exposed through the Glue
# Data Catalog by the analytics services integration we enabled in
# Chapter 2.
conf.set("spark.sql.catalog.s3tables.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")

# Point the catalog at our specific S3 Tables bucket, using the
# glue_catalog_id job parameter (in the form
# <account-id>:s3tablescatalog/<table-bucket-name>).
conf.set("spark.sql.catalog.s3tables.glue.id", args["glue_catalog_id"])

# Warehouse location setting for the Iceberg catalog.
conf.set("spark.sql.catalog.s3tables.warehouse", "s3://dataeng-silver-zone/warehouse/")

# Use Iceberg's native S3 file I/O implementation to read and write
# the underlying data files.
conf.set("spark.sql.catalog.s3tables.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Make "s3tables" the default catalog, so we can refer to tables as
# namespace.table (for example, movies.directors) without having to
# prefix every table name with the catalog name.
conf.set("spark.sql.defaultCatalog", "s3tables")


# ------------------------------------------------------------------
# Create the Spark and Glue contexts
# ------------------------------------------------------------------
# Create the SparkContext using our configuration, then wrap it in a
# GlueContext so we can use Glue features such as reading through
# Glue connections. The spark variable gives us a Spark session for
# running SQL statements and working with DataFrames.
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job, so Glue can track this run.
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Explicitly set "s3tables" as the current catalog as well. This is a
# defensive measure: some Spark SQL statements, such as DELETE FROM,
# resolve table names strictly against whichever catalog is current
# when they run, so we set it here to be certain.
spark.catalog.setCurrentCatalog("s3tables")


# ------------------------------------------------------------------
# Tables and columns to load
# ------------------------------------------------------------------
# Our Aurora tables include a created_at column that isn't part of our
# S3 Tables target schema. Rather than writing every column the source
# table happens to have, we list the exact columns each target table
# needs. This keeps our source and target schemas independent, and
# avoids a column mismatch error when writing.
#
# Each key is a table name (the same in Aurora and in S3 Tables), and
# each value is the list of columns to copy.
TABLE_COLUMNS = {
    "directors": ["director_id", "full_name", "birth_year"],
    "movies": ["movie_id", "title", "release_year", "genre", "director_id"],
    "actors": ["actor_id", "full_name", "birth_year"],
    "movie_cast": ["movie_id", "actor_id", "role_name", "is_lead"],
    "ratings": ["movie_id", "platform_score", "rating_count", "rated_at"],
}


# ------------------------------------------------------------------
# Load each table: read from Aurora, then replace the S3 Tables data
# ------------------------------------------------------------------
# Each table is loaded as a full refresh: we clear the target table,
# then write the complete, current contents of the source table. This
# makes the job safe to run more than once, and models how a nightly
# full reload might work for a catalog table that changes infrequently
# and doesn't need change data capture (CDC).
for table_name, columns in TABLE_COLUMNS.items():
    print(f"Loading {table_name} from Aurora...")

    # Read the source table from Aurora PostgreSQL. Setting
    # useConnectionProperties to "true" tells Glue to take the
    # endpoint, credentials, and networking details from our Glue
    # connection, so no hostnames or passwords appear in this script.
    # Glue returns the data as a DynamicFrame.
    source_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": CONNECTION_NAME,
            "dbtable": table_name,
        },
    )

    # Convert the DynamicFrame to a standard Spark DataFrame, keeping
    # only the columns listed for this table in TABLE_COLUMNS (this is
    # where created_at gets dropped).
    source_df = source_frame.toDF().select(*columns)

    # Count the rows we read, so the job log shows how much data was
    # loaded for each table.
    row_count = source_df.count()
    print(f"Read {row_count} rows from {table_name}")

    # The target table name, in namespace.table form. Because
    # "s3tables" is our default catalog, movies.directors refers to the
    # directors table in the movies namespace of our S3 Tables bucket.
    target_table = f"movies.{table_name}"

    # Remove all existing rows from the target table, so that rerunning
    # the job replaces the data rather than adding duplicate rows.
    print(f"Clearing existing rows from {target_table}...")
    spark.sql(f"DELETE FROM {target_table}")

    # Write the fresh rows into the now-empty target table. The delete
    # and this append are committed as separate Iceberg operations, so
    # if the job ever fails between the two, simply run it again.
    print(f"Writing {row_count} rows to {target_table}...")
    source_df.writeTo(target_table).append()

    print(f"Finished loading {table_name}")


# ------------------------------------------------------------------
# Finish the job
# ------------------------------------------------------------------
# Tell Glue that the job run completed successfully.
job.commit()
