"""Authenticated lifecycle commands for the resident local stream supervisor."""

import fcntl
import json
import os
import secrets
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read_json(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return {}


def atomic_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + secrets.token_hex(4) + ".tmp")
    temporary.write_text(json.dumps(data))
    temporary.replace(path)


def request_action(action, command_id, token=None, root=ROOT):
    if action not in ("start", "stop") or not isinstance(command_id, str) or not 8 <= len(command_id) <= 80:
        return {"ok": False, "message": "Invalid stream command."}
    expected = os.environ.get("WIKIPULSE_CONTROL_TOKEN", "")
    if not expected or not token or not secrets.compare_digest(expected, token):
        return {"ok": False, "message": "Start the managed stack with ./run-local.sh to enable stream controls."}
    runtime = Path(root) / "data/run"
    runtime.mkdir(parents=True, exist_ok=True)
    with (runtime / "stream-control.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        pending = runtime / "stream-control.request.json"
        receipt = read_json(runtime / "stream-control.result.json")
        queued = read_json(pending)
        if command_id in (receipt.get("id"), queued.get("id")):
            return {"ok": True, "id": command_id, "message": "Command already accepted."}
        state = read_json(runtime / "local_supervisor.json")
        pid = state.get("supervisor_pid")
        try:
            if not isinstance(pid, int) or pid <= 1:
                raise ProcessLookupError()
            os.kill(pid, 0)
        except (OSError, ProcessLookupError):
            return {"ok": False, "message": "The resident supervisor is unavailable. Start ./run-local.sh."}
        if pending.exists() or state.get("state") in ("starting", "restarting", "draining"):
            return {"ok": False, "message": "A stream transition is already in progress."}
        if state.get("state") == "stopped":
            return {"ok": False, "message": "Start ./run-local.sh first; the resident controller is stopped."}
        if action == "start" and state.get("state") == "running":
            return {"ok": True, "id": command_id, "message": "Stream is already running."}
        if action == "stop" and state.get("state") == "idle":
            return {"ok": True, "id": command_id, "message": "Stream is already stopped; the dashboard remains available."}
        atomic_json(pending, {"id": command_id, "action": action,
                              "requested_at": datetime.now(timezone.utc).isoformat()})
        return {"ok": True, "id": command_id, "message": f"Stream {action} accepted. Progress appears in the console."}


def committed_files(root):
    metadata = Path(root) / "_spark_metadata"
    logs = sorted((p for p in metadata.glob("*") if p.name.split(".")[0].isdigit()),
                  key=lambda p: int(p.name.split(".")[0]))
    compact = [i for i, p in enumerate(logs) if p.suffix == ".compact"]
    paths = set()
    for p in logs[compact[-1]:] if compact else logs:
        for line in p.read_text().splitlines()[1:]:
            item = json.loads(line)
            if item.get("action") == "delete":
                paths.discard(item["path"])
            else:
                paths.add(item["path"])
    return paths


def committed_offset(checkpoint):
    checkpoint = Path(checkpoint)
    commits = [p for p in (checkpoint / "commits").glob("*") if p.name.isdigit()]
    if not commits:
        return None
    batch = max(commits, key=lambda p: int(p.name)).name
    lines = (checkpoint / "offsets" / batch).read_text().splitlines()[2:]
    return json.loads(lines[0]) if lines else None


def file_stream_drained(source, checkpoint):
    offset = committed_offset(checkpoint)
    if not isinstance(offset, dict) or "logOffset" not in offset:
        return not committed_files(source)
    latest = int(offset["logOffset"])
    logs = sorted((p for p in (Path(checkpoint) / "sources/0").glob("*")
                   if p.name.split(".")[0].isdigit() and int(p.name.split(".")[0]) <= latest),
                  key=lambda p: int(p.name.split(".")[0]))
    compact = [i for i, p in enumerate(logs) if p.suffix == ".compact"]
    seen = set()
    for log in logs[compact[-1]:] if compact else logs:
        for line in log.read_text().splitlines()[1:]:
            seen.add(json.loads(line)["path"])
    return committed_files(source).issubset(seen)


def kafka_targets():
    from confluent_kafka import Consumer, TopicPartition
    from src.kafka_common import KAFKA_SERVERS, KAFKA_TOPIC
    observer = Consumer({"bootstrap.servers": KAFKA_SERVERS, "group.id": "wikipulse-drain-observer",
                         "enable.auto.commit": False, "broker.address.family": "v4"})
    try:
        metadata = observer.list_topics(KAFKA_TOPIC, timeout=5)
        return {str(p): observer.get_watermark_offsets(TopicPartition(KAFKA_TOPIC, p), timeout=3)[1]
                for p in metadata.topics[KAFKA_TOPIC].partitions}
    finally:
        observer.close()


def stop_and_drain(processes, root, timeout=90, sleep=time.sleep):
    """Drain committed input in dependency order, retaining watermark state."""
    from src.kafka_common import KAFKA_TOPIC
    root = Path(root)
    processes.stop_named(("collector",))
    deadline = time.monotonic() + timeout
    warnings = []

    def wait_for(predicate, description):
        while time.monotonic() < deadline:
            try:
                if predicate():
                    return True
            except (OSError, ValueError, KeyError):
                pass
            sleep(1)
        warnings.append(description + " remains pending; compatible checkpoints were retained")
        return False

    try:
        targets = kafka_targets()
        raw_checkpoint = root / os.getenv("RAW_CHECKPOINT_PATH", "data/checkpoints/raw_edits")

        def bronze_drained():
            offsets = (committed_offset(raw_checkpoint) or {}).get(KAFKA_TOPIC, {})
            return all(int(offsets.get(p, -1)) >= end for p, end in targets.items())

        wait_for(bronze_drained, "Kafka-to-Bronze backlog")
    except Exception as error:
        warnings.append(f"Kafka drain could not be verified ({str(error)[:160]}); offsets and checkpoints were retained")
    processes.stop_named(("bronze",))
    raw = root / os.getenv("RAW_PARQUET_PATH", "data/raw/wikipedia_edits")
    for duration in ("5m", "1h"):
        wait_for(lambda d=duration: file_stream_drained(raw, root / f"data/checkpoints/page_activity/windows_{d}"),
                 f"{duration} window input")
    processes.stop_named(("windows-5m", "windows-1h"))
    for duration in ("5m", "1h"):
        wait_for(lambda d=duration: file_stream_drained(root / f"data/live_activity/windows_{d}",
                                                       root / f"data/checkpoints/detection/{d}"),
                 f"{duration} detector input")
    processes.stop_named(("detection-5m", "detection-1h"))
    return {"drained": not warnings, "warnings": warnings,
            "message": "Stream stopped; dashboard and broker remain available. Open event-time windows resume from checkpoints.",
            "source_gap": "Collection was stopped. Resumption can leave a source gap; capture coverage is not guaranteed."}
