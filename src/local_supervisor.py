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
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


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
    event_time = progress.get("eventTime", {}).get("max")
    if not event_time:
        event_time = progress.get("timestamp")
    if status == "running" and event_time:
        try:
            parsed_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - parsed_time).total_seconds() > 120:
                status = "stale"
        except ValueError:
            pass
    label = {
        "starting": "STARTING · Local stream services are coming up",
        "restarting": "RESTARTING · Reconnecting stream services",
        "broken": "BROKEN · A stream service stopped",
        "stale": "STALE · No recent Wikipedia events",
        "running": "LIVE · Stream services running",
    }.get(status, "STREAM STATUS · " + status.upper())
    if event_time and status == "running":
        label += f" · Last event {event_time}"
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
