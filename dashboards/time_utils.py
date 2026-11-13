"""Shared conversion helpers for dashboard timestamps shown in India time."""

from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))


def to_india_time(value) -> datetime:
    """Interpret stored timestamps as UTC and return an aware IST datetime."""
    if isinstance(value, datetime):
        timestamp = value
    else:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(IST)


def to_india_wall_time(value) -> datetime:
    """Return an IST clock value without timezone metadata for Plotly axes."""
    return to_india_time(value).replace(tzinfo=None)


def format_india_time(value) -> str | None:
    if value is None or value == "":
        return None
    return to_india_time(value).strftime("%d %b %Y, %H:%M:%S IST")
