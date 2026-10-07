-- ============================================================
-- 00_schema.sql
-- Schema for the Kevin Bacon 6-Degrees Demo Database
-- Designed for Aurora PostgreSQL
--
-- Execution order:
--   00_schema.sql       <- this file (run first)
--   01_directors.sql
--   02_movies.sql
--   03_actors.sql
--   04_movie_cast.sql
--   05_ratings.sql
-- ============================================================

-- ============================================================
-- CLEAN UP (safe to re-run)
-- ============================================================
DROP TABLE IF EXISTS ratings      CASCADE;
DROP TABLE IF EXISTS movie_cast   CASCADE;
DROP TABLE IF EXISTS movies       CASCADE;
DROP TABLE IF EXISTS actors       CASCADE;
DROP TABLE IF EXISTS directors    CASCADE;

-- ============================================================
-- DIRECTORS
-- ============================================================
CREATE TABLE directors (
    director_id  SERIAL          PRIMARY KEY,
    full_name    VARCHAR(255)    NOT NULL,
    birth_year   SMALLINT,
    created_at   TIMESTAMP       DEFAULT NOW()
);

COMMENT ON TABLE  directors            IS 'Film directors referenced by the movies table.';
COMMENT ON COLUMN directors.full_name  IS 'Full name of the director.';
COMMENT ON COLUMN directors.birth_year IS 'Year of birth (used for disambiguation).';

-- ============================================================
-- MOVIES
-- ============================================================
CREATE TABLE movies (
    movie_id      SERIAL          PRIMARY KEY,
    title         VARCHAR(255)    NOT NULL,
    release_year  SMALLINT        NOT NULL,
    genre         VARCHAR(100),
    director_id   INT             REFERENCES directors(director_id) ON DELETE SET NULL,
    created_at    TIMESTAMP       DEFAULT NOW()
);

COMMENT ON TABLE  movies              IS 'Core movie catalogue. ~700 films seeded for the Bacon game.';
COMMENT ON COLUMN movies.title        IS 'Official release title of the film.';
COMMENT ON COLUMN movies.release_year IS 'Year the film was theatrically released.';
COMMENT ON COLUMN movies.genre        IS 'Primary genre classification.';
COMMENT ON COLUMN movies.director_id  IS 'FK to directors table. NULL if director unknown.';

-- ============================================================
-- ACTORS
-- ============================================================
CREATE TABLE actors (
    actor_id    SERIAL          PRIMARY KEY,
    full_name   VARCHAR(255)    NOT NULL,
    birth_year  SMALLINT,
    created_at  TIMESTAMP       DEFAULT NOW()
);

COMMENT ON TABLE  actors            IS 'Actors and actresses. ~500 seeded for the Bacon game.';
COMMENT ON COLUMN actors.full_name  IS 'Full name of the actor/actress.';
COMMENT ON COLUMN actors.birth_year IS 'Year of birth (used for disambiguation of same-name actors).';

-- ============================================================
-- MOVIE_CAST  (the graph edge table)
-- ============================================================
CREATE TABLE movie_cast (
    movie_id   INT             NOT NULL REFERENCES movies(movie_id)  ON DELETE CASCADE,
    actor_id   INT             NOT NULL REFERENCES actors(actor_id)  ON DELETE CASCADE,
    role_name  VARCHAR(255),
    is_lead    BOOLEAN         DEFAULT FALSE,
    created_at TIMESTAMP       DEFAULT NOW(),
    PRIMARY KEY (movie_id, actor_id)
);

COMMENT ON TABLE  movie_cast           IS 'Junction table linking actors to movies. This is the GRAPH EDGE for the Bacon game — two actors are connected if they share a movie_id.';
COMMENT ON COLUMN movie_cast.role_name IS 'Character name played by the actor in this film.';
COMMENT ON COLUMN movie_cast.is_lead   IS 'TRUE if this actor received top billing / lead role.';

-- ============================================================
-- RATINGS
-- ============================================================
CREATE TABLE ratings (
    rating_id      SERIAL          PRIMARY KEY,
    movie_id       INT             NOT NULL REFERENCES movies(movie_id) ON DELETE CASCADE,
    platform_score NUMERIC(3,1)    NOT NULL,
    rating_count   INT,
    rated_at       DATE,
    created_at     TIMESTAMP       DEFAULT NOW(),
    CONSTRAINT chk_score_range CHECK (platform_score >= 0 AND platform_score <= 10)
);

COMMENT ON TABLE  ratings                IS 'Single synthetic platform rating per movie, in the style of a streaming platform''s own rating display. Scores are approximate, LLM-reconstructed, and for educational use only — not sourced from or claiming to represent any real third-party platform''s actual rating.';
COMMENT ON COLUMN ratings.platform_score IS 'Synthetic 0-10 platform rating, loosely informed by the film''s real-world critical/audience reception.';
COMMENT ON COLUMN ratings.rating_count   IS 'Simulated popularity count for narrative realism only; does not reflect any real platform''s actual rating volume.';

-- ============================================================
-- INDEXES
-- ============================================================

-- Speed up graph traversal (BFS for Bacon number)
CREATE INDEX idx_movie_cast_actor   ON movie_cast(actor_id);
CREATE INDEX idx_movie_cast_movie   ON movie_cast(movie_id);

-- Common query patterns
CREATE INDEX idx_movies_year        ON movies(release_year);
CREATE INDEX idx_movies_genre       ON movies(genre);
CREATE INDEX idx_movies_director    ON movies(director_id);
CREATE INDEX idx_actors_name        ON actors(full_name);
CREATE INDEX idx_directors_name     ON directors(full_name);
CREATE INDEX idx_ratings_movie      ON ratings(movie_id);

-- ============================================================
-- VIEWS
-- ============================================================

-- Convenient denormalized view for RAG / GenAI queries
CREATE OR REPLACE VIEW v_movie_full AS
SELECT
    m.movie_id,
    m.title,
    m.release_year,
    m.genre,
    d.full_name                          AS director,
    d.birth_year                         AS director_birth_year,
    MAX(r.platform_score)                AS platform_score,
    MAX(r.rating_count)                  AS rating_count,
    COUNT(DISTINCT mc.actor_id)          AS cast_size
FROM movies       m
LEFT JOIN directors  d  ON d.director_id = m.director_id
LEFT JOIN ratings    r  ON r.movie_id    = m.movie_id
LEFT JOIN movie_cast mc ON mc.movie_id   = m.movie_id
GROUP BY m.movie_id, m.title, m.release_year, m.genre,
         d.full_name, d.birth_year;

COMMENT ON VIEW v_movie_full IS 'Denormalized movie view with director name, synthetic platform rating (0-10), rating count, and cast size.';

-- Actor filmography view
CREATE OR REPLACE VIEW v_actor_filmography AS
SELECT
    a.actor_id,
    a.full_name                     AS actor,
    a.birth_year,
    COUNT(mc.movie_id)              AS total_films,
    MIN(m.release_year)             AS first_film_year,
    MAX(m.release_year)             AS last_film_year,
    COUNT(mc.movie_id) FILTER (WHERE mc.is_lead) AS lead_roles
FROM actors      a
JOIN movie_cast  mc ON mc.actor_id = a.actor_id
JOIN movies      m  ON m.movie_id  = mc.movie_id
GROUP BY a.actor_id, a.full_name, a.birth_year;

COMMENT ON VIEW v_actor_filmography IS 'Actor filmography summary: total films, career span, lead role count.';

-- Co-star relationship view (direct connections)
CREATE OR REPLACE VIEW v_costar_pairs AS
SELECT
    a1.full_name   AS actor_1,
    a2.full_name   AS actor_2,
    m.title        AS shared_movie,
    m.release_year
FROM movie_cast  mc1
JOIN movie_cast  mc2 ON mc2.movie_id = mc1.movie_id
                     AND mc2.actor_id > mc1.actor_id   -- avoid duplicates
JOIN actors      a1  ON a1.actor_id  = mc1.actor_id
JOIN actors      a2  ON a2.actor_id  = mc2.actor_id
JOIN movies      m   ON m.movie_id   = mc1.movie_id;

COMMENT ON VIEW v_costar_pairs IS 'All direct co-star pairs with the movie they share. Bacon number = 1 pairs.';

-- ============================================================
-- BACON NUMBER FUNCTION
-- Finds shortest path (Bacon number) from any actor to Kevin Bacon
-- Usage: SELECT * FROM fn_bacon_number('Tom Hanks');
-- ============================================================
CREATE OR REPLACE FUNCTION fn_bacon_number(target_actor_name VARCHAR)
RETURNS TABLE (
    actor_name   VARCHAR,
    bacon_number INT,
    via_movie    VARCHAR,
    via_actor    VARCHAR
) AS $$
WITH RECURSIVE bacon_bfs AS (

    -- Seed: Kevin Bacon (actor_id = 1)
    SELECT
        a.actor_id,
        a.full_name,
        0               AS bacon_number,
        NULL::VARCHAR   AS via_movie,
        NULL::VARCHAR   AS via_actor,
        ARRAY[a.actor_id] AS visited
    FROM actors a
    WHERE a.full_name = 'Kevin Bacon'

    UNION ALL

    -- Hop: find actors sharing a movie with someone already in the frontier
    SELECT
        a2.actor_id,
        a2.full_name,
        bfs.bacon_number + 1,
        m.title,
        bfs.full_name,
        bfs.visited || a2.actor_id
    FROM bacon_bfs        bfs
    JOIN movie_cast       mc1 ON mc1.actor_id = bfs.actor_id
    JOIN movie_cast       mc2 ON mc2.movie_id  = mc1.movie_id
                              AND mc2.actor_id != bfs.actor_id
    JOIN actors           a2  ON a2.actor_id   = mc2.actor_id
    JOIN movies           m   ON m.movie_id     = mc1.movie_id
    WHERE NOT (a2.actor_id = ANY(bfs.visited))
      AND bfs.bacon_number < 6

)
SELECT DISTINCT ON (full_name)
    full_name,
    bacon_number,
    via_movie,
    via_actor
FROM bacon_bfs
WHERE full_name ILIKE '%' || target_actor_name || '%'
ORDER BY full_name, bacon_number ASC;

$$ LANGUAGE sql;

COMMENT ON FUNCTION fn_bacon_number IS 'Breadth-first search to find the Bacon number (shortest path to Kevin Bacon) for any actor name. Uses recursive CTE. Max depth = 6.';

-- ============================================================
-- VERIFY SCHEMA
-- ============================================================
SELECT
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns c
     WHERE c.table_name = t.table_name
       AND c.table_schema = 'public') AS column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
  AND table_type   = 'BASE TABLE'
ORDER BY table_name;
