"""Read local stream supervision state and request safe supervised restarts."""

import json
import os
import secrets
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUN_DIR = ROOT / "data/run"
STATE_PATH = RUN_DIR / "local_supervisor.json"
RESTART_PATH = RUN_DIR / "restart_stream.request.json"


def _read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _age_seconds(value, now):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        age = (now - parsed.replace(tzinfo=parsed.tzinfo or timezone.utc)).total_seconds()
        return age if age >= -5 else None
    except (AttributeError, TypeError, ValueError, OverflowError):
        return None


def stream_health() -> dict:
    """Return a small health snapshot without starting Spark or contacting Kafka."""
    state = _read_json(STATE_PATH)
    if not state or state.get("state") in ("stopped", "shutdown"):
        return {
            "managed": False,
            "state": "offline",
            "label": "STREAM OFFLINE · Start with ./run-local.sh",
            "last_event": None,
            "services": {},
        }

    services = state.get("services", {})
    failed = [name for name, service in services.items() if not service.get("running")]
    status = state.get("state", "starting")
    if failed and status == "running":
        status = "broken"

    progress = _read_json(RUN_DIR / "windows-5m.progress.json")
    event_time = (progress.get("eventTime") or {}).get("max")
    if not event_time:
        event_time = progress.get("timestamp")
    now = datetime.now(timezone.utc)
    source = _read_json(RUN_DIR / "stream-source.json")
    capture = _read_json(RUN_DIR / "collector-source.progress.json")
    stale_reason = "No recent Wikipedia events"
    if status == "running" and (source or capture):
        # Event time is intentionally old during backfill. Freshness comes from
        # actual collector activity and raw Spark ingestion, before finalization.
        age = _age_seconds(capture.get("updated_at"), now)
        raw = _read_json(RUN_DIR / "bronze.progress.json")
        raw_age = _age_seconds(raw.get("timestamp"), now)
        matching_source = capture.get("since") == source.get("since")
        if not matching_source:
            status = "reconnecting"
        elif age is None or age > 120:
            status, stale_reason = "stale", "No recent Wikipedia collector activity"
        elif capture.get("state") in ("connecting", "reconnecting"):
            status = "reconnecting"
        elif capture.get("state") != "connected":
            status, stale_reason = "stale", "Wikipedia collector is not connected"
        elif raw_age is None or raw_age > 120:
            status, stale_reason = "stale", "No recent Spark ingestion progress"
        else:
            event_time = capture.get("latest_event")
            event_age = _age_seconds(event_time, now)
            if not event_time:
                status = "reconnecting"
            elif event_age is None:
                status, stale_reason = "stale", "Wikipedia event time is unavailable"
            elif event_age > 120:
                status = "catching_up"
    elif status == "running" and event_time:
        age = _age_seconds(event_time, now)
        if age is None or age > 120:
            status = "stale"
    label = {
        "starting": "STARTING · Local stream services are coming up",
        "restarting": "RESTARTING · Reconnecting stream services",
        "broken": "BROKEN · A stream service stopped",
        "stale": "STALE · " + stale_reason,
        "catching_up": "CATCHING UP · Wikipedia history is arriving",
        "reconnecting": "RECONNECTING · Waiting for the selected Wikipedia source",
        "running": "LIVE · Stream services running",
        "idle": "STREAM STOPPED · Dashboard and stored activity available",
        "draining": "DRAINING · Committing admitted events before stopping",
    }.get(status, "STREAM STATUS · " + status.upper())
    if event_time and status in ("running", "catching_up"):
        label += f" · Latest captured event {event_time}"
    elif failed:
        label += " · " + ", ".join(failed)
    return {
        "managed": True,
        "state": status,
        "label": label,
        "last_event": event_time,
        "services": services,
        "updated_at": state.get("updated_at"),
        "error": state.get("error"),
    }


def request_stream_restart(token: str | None = None) -> dict:
    """Queue a restart for the active local supervisor; never launch unmanaged workers."""
    expected = os.environ.get("WIKIPULSE_CONTROL_TOKEN", "")
    if not expected or not token or not secrets.compare_digest(expected, token):
        return {"ok": False, "message": "Restart is available when the stack is launched with ./run-local.sh."}
    health = stream_health()
    if not health["managed"]:
        return {"ok": False, "message": "The local supervisor is not running. Start with ./run-local.sh."}
    if RESTART_PATH.exists():
        return {"ok": False, "message": "A stream restart is already queued."}
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    temporary = RESTART_PATH.with_suffix(".tmp")
    temporary.write_text(json.dumps({"requested_at": datetime.now(timezone.utc).isoformat()}))
    temporary.replace(RESTART_PATH)
    return {"ok": True, "message": "Stream restart requested. Existing data and checkpoints are preserved."}


def request_restart_from_ui() -> dict:
    """Use the app's in-process token when the dashboard button is clicked."""
    return request_stream_restart(os.environ.get("WIKIPULSE_CONTROL_TOKEN"))
