"""Convert canonical edits to the existing collector envelope and pace delivery."""

import math
import time

from src.kafka_common import build_envelope


def to_event(row: dict) -> dict:
    """Preserve IDs and original event time; ingestion time is set at publication."""
    # PySpark collects timestamp values as naive host-local datetimes, even with
    # a UTC SQL session. Cast to epoch seconds in Spark before crossing into Python
    # so replay times cannot move when the operator's machine uses another zone.
    timestamp = row.get("event_epoch")
    if timestamp is None:
        if row["ts"].tzinfo is None:
            raise ValueError("Canonical replay rows need event_epoch from Spark or an aware timestamp")
        timestamp = int(row["ts"].timestamp())
    event = {
        "id": row["rcid"], "wiki": "enwiki", "type": "edit", "namespace": row["ns"],
        "title": row["title"], "timestamp": timestamp,
        "user": row["user"], "userid": row.get("userid"), "bot": row["is_bot"],
        "minor": row["is_minor"], "tags": row.get("tags") or [],
        "revision": {"old": row.get("old_revid"), "new": row.get("revid")},
        "length": {"old": row.get("oldlen"), "new": row.get("newlen")},
        "comment": row.get("comment"),
    }
    if row["pageid"] > 0:
        event["page_id"] = row["pageid"]
    return event


def paced_envelopes(events, speed=1.0, clock=time.monotonic, sleep=time.sleep):
    """Use elapsed event time, not accumulated sleeps, to avoid playback drift.

    speed=0 is unpaced. Event timestamps are never shifted to the wall clock:
    replay belongs in isolated topics/storage/checkpoints to avoid live watermarks.
    """
    if not math.isfinite(speed) or speed < 0:
        raise ValueError("Replay speed must be finite and nonnegative")
    first_timestamp = previous = None
    started = clock()
    for event in events:
        timestamp = event["timestamp"]
        if previous is not None and timestamp < previous:
            raise ValueError("Replay input must be ordered by event time")
        if first_timestamp is None:
            first_timestamp = timestamp
        if speed:
            target = started + (timestamp - first_timestamp) / speed
            while True:
                remaining = target - clock()
                if remaining <= 0:
                    break
                sleep(min(0.2, remaining))
        previous = timestamp
        yield build_envelope(event)
