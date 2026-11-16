"""Wikimedia source selection and small local capture metadata (no database)."""

import time
import fcntl
import hashlib
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

try:
    from .stream_control import atomic_json, read_json
except ImportError:
    from stream_control import atomic_json, read_json

UTC = timezone.utc
RUNTIME = Path(__file__).resolve().parents[1] / "data/run"


@contextmanager
def collector_lock(identity, runtime=RUNTIME):
    """Keep one publisher per broker/topic; the OS releases ownership on exit."""
    runtime = Path(runtime)
    runtime.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(identity.encode()).hexdigest()[:16]
    # Never unlink an advisory lock file: another process may hold its inode.
    with (runtime / f"collector-{key}.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SystemExit("Another WikiPulse collector already publishes to this broker/topic. "
                             "Use Apply stream source in the managed dashboard to change its date.") from error
        try:
            lock.seek(0)
            lock.truncate()
            lock.write(str(os.getpid()))
            lock.flush()
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def normalize_since(value, now=None):
    if value is None or value == "":
        return None
    try:
        text = str(value).strip()
        parsed = (datetime.fromtimestamp(int(text) / 1000, UTC) if text.isdigit()
                  else datetime.fromisoformat(text.replace("Z", "+00:00")))
    except (ValueError, OverflowError, OSError, TypeError) as error:
        raise ValueError("Choose a valid timestamp with timezone.") from error
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include a timezone (use UTC / Z).")
    parsed = parsed.astimezone(UTC)
    if parsed.timestamp() > (time.time() if now is None else now):
        raise ValueError("Choose a timestamp in the past.")
    return parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def stream_url(url, since=None):
    parts = urlsplit(url)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != "since"]
    if since:
        query.append(("since", since))
    return urlunsplit(parts._replace(query=urlencode(query)))


class CaptureProgress:
    def __init__(self, since=None, runtime=RUNTIME):
        self.runtime = Path(runtime)
        self.since = since
        self.last_write = 0
        self.days = read_json(self.runtime / "capture-days.json")
        self.days_changed = False
        self.state = {"since": since, "state": "connecting", "queued_events": 0,
                      "queued_bytes": 0, "first_event": None, "latest_event": None,
                      "started_at": datetime.now(UTC).isoformat(), "message": "Connecting to Wikipedia"}

    def record(self, event, payload_bytes):
        event_time = datetime.fromtimestamp(event["timestamp"], UTC)
        self.state["queued_events"] += 1
        self.state["queued_bytes"] += payload_bytes
        self.state["first_event"] = self.state["first_event"] or event_time.isoformat()
        previous = self.state["latest_event"]
        self.state["latest_event"] = max(previous or "", event_time.isoformat())
        day = event_time.date().isoformat()
        ingestion_day = datetime.now(UTC).date().isoformat()
        partitions = self.days.setdefault(day, [])
        if ingestion_day not in partitions:
            partitions.append(ingestion_day)
            self.days_changed = True

    def write(self, state=None, message=None, force=False, counts=None):
        if state:
            self.state["state"] = state
        if message:
            self.state["message"] = message
        if force or time.monotonic() - self.last_write >= 2:
            if counts is not None:
                self.state["delivery"] = dict(counts)
            self.state["updated_at"] = datetime.now(UTC).isoformat()
            atomic_json(self.runtime / "collector-source.progress.json", self.state)
            if self.days_changed:
                atomic_json(self.runtime / "capture-days.json", self.days)
                self.days_changed = False
            self.last_write = time.monotonic()
