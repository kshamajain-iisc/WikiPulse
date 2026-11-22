"""Shared live/replay Kafka configuration and event envelope contract."""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wiki.raw.edits")
KAFKA_ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "wiki.shift.candidates")


def is_file_broker_mode() -> bool:
    """Return True when using the Docker-free local file-backed broker spool."""
    return os.getenv("WIKIPULSE_BROKER_MODE", "").lower() == "file"


def get_local_broker_spool(root: Path | None = None) -> Path:
    base = root or ROOT
    return (base / os.getenv("LOCAL_BROKER_SPOOL_PATH", "data/raw/local_broker_spool")).resolve()


def seed_local_broker_spool(root: Path | None = None) -> Path:
    """Ensure the local file broker spool exists and has an initial valid seed file so
    Spark's file stream source commits batch 0 immediately on startup."""
    spool = get_local_broker_spool(root)
    spool.mkdir(parents=True, exist_ok=True)
    if not any(spool.glob("*.json")):
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        raw_event = {
            "id": 1,
            "wiki": "enwiki",
            "type": "edit",
            "namespace": 0,
            "page_id": 18938200,
            "title": "2026 United States elections",
            "timestamp": int(now.timestamp()),
            "user": "WikiPulseBootstrap",
            "userid": 1,
            "bot": False,
            "minor": True,
            "comment": "bootstrap seed",
            "revision": {"old": 100, "new": 101},
            "length": {"old": 1000, "new": 1000},
        }
        envelope = {
            "event_id": "enwiki:1",
            "event_time": now_iso,
            "ingested_at": now_iso,
            "raw_event": raw_event,
        }
        record = {
            "key": "enwiki:1",
            "value": json.dumps(envelope, ensure_ascii=False),
            "topic": KAFKA_TOPIC,
            "partition": 0,
            "offset": 1,
            "timestamp": now_iso,
        }
        seed = spool / "seed_0.json"
        seed.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")
    return spool


class FileStreamProducer:
    """Drop-in replacement for confluent_kafka.Producer backed by atomic JSONL spool files."""

    def __init__(self, client_id: str = "wikipulse-live-collector", spool_dir: Path | None = None):
        self.client_id = client_id
        self.spool_dir = spool_dir or get_local_broker_spool()
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        self.batch_size = 20
        self.flush_interval = 2.0
        self._buffer: list[tuple[dict, object]] = []
        self._offset = int(time.time() * 1000)
        self._batch_seq = 0
        self._last_flush = time.monotonic()

    def produce(self, topic: str, key=None, value=None, on_delivery=None) -> None:
        key_str = (
            key.decode("utf-8")
            if isinstance(key, (bytes, bytearray))
            else (str(key) if key is not None else None)
        )
        val_str = (
            value.decode("utf-8")
            if isinstance(value, (bytes, bytearray))
            else (str(value) if value is not None else None)
        )
        self._offset += 1
        record = {
            "key": key_str,
            "value": val_str,
            "topic": topic,
            "partition": 0,
            "offset": self._offset,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._buffer.append((record, on_delivery))
        if (
            self._batch_seq == 0
            or len(self._buffer) >= self.batch_size
            or (time.monotonic() - self._last_flush) >= self.flush_interval
        ):
            self._write_batch()

    def _write_batch(self) -> None:
        if not self._buffer:
            self._last_flush = time.monotonic()
            return
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        self._batch_seq += 1
        stamp = int(time.time() * 1000)
        target = self.spool_dir / f"batch_{stamp}_{os.getpid()}_{self._batch_seq}.json"
        tmp = target.with_suffix(".tmp")
        lines = [json.dumps(item[0], ensure_ascii=False) for item in self._buffer]
        tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
        tmp.replace(target)
        callbacks = [item[1] for item in self._buffer if item[1] is not None]
        self._buffer.clear()
        self._last_flush = time.monotonic()
        for callback in callbacks:
            try:
                callback(None, None)
            except Exception:
                pass

    def poll(self, timeout: float = 0) -> int:
        if self._buffer and (time.monotonic() - self._last_flush) >= self.flush_interval:
            self._write_batch()
        if timeout and timeout > 0:
            time.sleep(min(timeout, 0.1))
        return 0

    def flush(self, timeout: float = 30) -> int:
        self._write_batch()
        return len(self._buffer)


def ensure_topic(topic: str) -> None:
    """Create a topic explicitly; Compose disables broker auto-creation."""
    if is_file_broker_mode():
        seed_local_broker_spool()
        return
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": KAFKA_SERVERS})
    future = admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic]
    try:
        future.result(timeout=30)
    except KafkaException as error:
        from confluent_kafka import KafkaError
        if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise


def configure_logging():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def create_producer(client_id="wikipulse-live-collector"):
    if is_file_broker_mode():
        return FileStreamProducer(client_id=client_id)
    return Producer({
        "bootstrap.servers": KAFKA_SERVERS,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 10,
        "compression.type": "snappy",
        "linger.ms": 20,
        "message.timeout.ms": 120000,
    })


def get_event_id(event):
    wiki, change_id = event.get("wiki"), event.get("id")
    return f"{wiki}:{change_id}" if wiki and change_id is not None else None


def event_timestamp(value):
    if isinstance(value, (float, int)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Event timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def is_valid_edit(event):
    if not isinstance(event, dict):
        return False
    if (event.get("wiki") != "enwiki" or event.get("type") != "edit"
            or event.get("namespace") != 0 or not event.get("title")
            or not get_event_id(event)):
        return False
    try:
        event_timestamp(event.get("timestamp"))
    except (ValueError, TypeError, AttributeError, OverflowError, OSError):
        return False
    return True


def build_envelope(event):
    return {
        "event_id": get_event_id(event),
        "event_time": event_timestamp(event["timestamp"]).isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "raw_event": event,
    }
