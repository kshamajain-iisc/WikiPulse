"""Shared live/replay Kafka configuration and event envelope contract."""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wiki.raw.edits")


def configure_logging():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def create_producer(client_id="wikipulse-live-collector"):
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
