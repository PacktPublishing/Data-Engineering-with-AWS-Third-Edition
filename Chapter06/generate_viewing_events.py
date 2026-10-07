import boto3
import json
import random
import uuid
from datetime import datetime, timedelta, timezone

STREAM_NAME = "viewing-events"
NUMBER_OF_SESSIONS = 3500

# Our subscriber and movie catalog ranges, matching what is currently
# loaded in customer_records and our movies table. customer_records uses
# customer_id values starting at 1001, not 1, so make sure this matches
# whatever range is actually loaded if that table is ever expanded further.
SUBSCRIBER_ID_RANGE = (1001, 2000)
FILM_ID_RANGE = (1, 627)

DEVICE_TYPES = ["smart_tv", "mobile", "web"]


# Our movies table does not include a runtime, so we generate a synthetic,
# but consistent, duration for each film here. This is not a real runtime,
# it exists only so a simulated viewing session can pace its playback
# position realistically. Using the film_id to seed this means the same
# film always gets the same duration across a single run of this script.
def film_duration_seconds(film_id):
    return random.Random(film_id).randint(80, 180) * 60

z
kinesis = boto3.client("kinesis")


def build_event(event_id, subscriber_id, film_id, event_type,
                 event_time, playback_position_seconds, device_type):
    return {
        "event_id": event_id,
        "subscriber_id": subscriber_id,
        "film_id": film_id,
        "event_type": event_type,
        "event_timestamp": event_time.isoformat(),
        "device_type": device_type,
        "playback_position_seconds": playback_position_seconds,
    }


def generate_session():
    """
    Builds a coherent sequence of events for a single viewing session:
    one subscriber watching one film, starting, optionally pausing one
    or more times, and either completing the film or abandoning it
    partway through.
    """
    subscriber_id = random.randint(*SUBSCRIBER_ID_RANGE)
    film_id = random.randint(*FILM_ID_RANGE)
    device_type = random.choice(DEVICE_TYPES)
    duration = film_duration_seconds(film_id)

    # 50% watch straight through with no pauses, 35% pause one or more
    # times before completing, 15% pause and never finish (abandoned).
    session_type = random.choices(
        ["straight_through", "paused_completed", "abandoned"],
        weights=[50, 35, 15],
        k=1,
    )[0]

    events = []
    # Sessions start at a random point over the last 30 days, so our
    # data has some spread rather than all clustering around "now".
    session_start = datetime.now(timezone.utc) - timedelta(
        days=random.uniform(0, 30)
    )

    current_time = session_start
    events.append(build_event(
        str(uuid.uuid4()), subscriber_id, film_id, "play_start",
        current_time, 0, device_type,
    ))

    if session_type == "straight_through":
        # Jump straight to completion, at the end of the film.
        current_time += timedelta(seconds=duration)
        events.append(build_event(
            str(uuid.uuid4()), subscriber_id, film_id, "play_complete",
            current_time, duration, device_type,
        ))
        return events

    # Both paused_completed and abandoned sessions include one or more
    # pauses. We track playback position, which only moves forward.
    # Every pause is followed by a play_resume event, except for the
    # last pause in an abandoned session, since the subscriber never
    # actually resumed watching in that case.
    pause_count = random.choices([1, 2, 3], weights=[60, 30, 10], k=1)[0]
    position = 0

    for pause_number in range(1, pause_count + 1):
        # Playback advances by a random amount before the next pause,
        # never past the end of the film.
        remaining = duration - position
        if remaining <= 60:
            break
        advance = random.randint(30, max(31, remaining // 2))
        position += advance

        # A pause can happen anywhere from a minute to a few hours after
        # the previous event, since people don't always resume right away.
        current_time += timedelta(minutes=random.uniform(1, 180))

        events.append(build_event(
            str(uuid.uuid4()), subscriber_id, film_id, "play_pause",
            current_time, position, device_type,
        ))

        is_last_pause = pause_number == pause_count
        if is_last_pause and session_type == "abandoned":
            # The subscriber never resumed after this final pause.
            continue

        # Resume playback from the same position, a few minutes to a
        # few hours after pausing.
        current_time += timedelta(minutes=random.uniform(1, 180))
        events.append(build_event(
            str(uuid.uuid4()), subscriber_id, film_id, "play_resume",
            current_time, position, device_type,
        ))

    if session_type == "paused_completed":
        current_time += timedelta(minutes=random.uniform(1, 180))
        events.append(build_event(
            str(uuid.uuid4()), subscriber_id, film_id, "play_complete",
            current_time, duration, device_type,
        ))

    return events


def send_events(records):
    """Sends a list of already-built event dicts to Kinesis, in batches
    of up to 500, since that is the maximum PutRecords can accept in a
    single call."""
    total_sent = 0

    for batch_start in range(0, len(records), 500):
        batch = records[batch_start:batch_start + 500]
        kinesis_records = [
            {
                "Data": json.dumps(event).encode("utf-8"),
                "PartitionKey": str(event["subscriber_id"]),
            }
            for event in batch
        ]

        response = kinesis.put_records(StreamName=STREAM_NAME, Records=kinesis_records)

        failed_count = response["FailedRecordCount"]
        if failed_count > 0:
            print(f"Warning: {failed_count} records failed to send in this batch")

        total_sent += len(batch) - failed_count
        print(f"Sent {total_sent} of {len(records)} events so far...")

    return total_sent


if __name__ == "__main__":
    print(f"Generating {NUMBER_OF_SESSIONS} simulated viewing sessions...")

    all_events = []
    for _ in range(NUMBER_OF_SESSIONS):
        all_events.extend(generate_session())

    # Kinesis does not require events to be sent in timestamp order, and
    # shuffling here better simulates many subscribers' sessions arriving
    # interleaved, the way they would from a real streaming application.
    random.shuffle(all_events)

    print(f"Generated {len(all_events)} total events from "
          f"{NUMBER_OF_SESSIONS} sessions. Sending to Kinesis stream "
          f"'{STREAM_NAME}'...")

    sent = send_events(all_events)
    print(f"Done. Successfully sent {sent} of {len(all_events)} events.")
