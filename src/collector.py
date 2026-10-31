
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
TOPIC = os.getenv("KAFKA_TOPIC", "wiki.raw.edits")
STREAM_URL = os.getenv(
    "WIKIMEDIA_STREAM_URL",
    "https://stream.wikimedia.org/v2/stream/recentchange",
)
USER_AGENT = os.getenv("WIKIMEDIA_USER_AGENT", "")
STATS_INTERVAL = int(
    os.getenv("STATS_INTERVAL_SECONDS", "30")
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("wikipulse.collector")

running = True

stats = {
    "received": 0,
    "filtered": 0,
    "invalid": 0,
    "queued": 0,
    "published": 0,
    "failed": 0,
}


def stop_handler(signum, frame):
    global running
    running = False
    logger.info("Shutdown requested")


signal.signal(signal.SIGINT, stop_handler)
signal.signal(signal.SIGTERM, stop_handler)


def create_producer():
    return Producer({
        "bootstrap.servers": KAFKA_SERVERS,
        "client.id": "wikipulse-live-collector",
        "acks": "all",
        "enable.idempotence": True,
        "retries": 10,
        "compression.type": "snappy",
        "linger.ms": 20,
        "message.timeout.ms": 120000,
    })


def get_event_id(event):
    # Wikimedia recentchange ID is scoped to a wiki.
    wiki = event.get("wiki")
    change_id = event.get("id")

    if not wiki or change_id is None:
        return None

    return f"{wiki}:{change_id}"




def is_valid_edit(event):
    if not isinstance(event, dict):
        return False

    # Only English Wikipedia
    if event.get("wiki") != "enwiki":
        return False

    # Only edit events
    if event.get("type") != "edit":
        return False

    # Only main article namespace
    if event.get("namespace") != 0:
        return False

    # Stable event identifier
    if not get_event_id(event):
        return False

    # Required article information
    if not event.get("title"):
        return False

    # Required event timestamp
    if event.get("timestamp") is None:
        return False

    return True

    if not isinstance(event, dict):
        logger.warning("Invalid: event is not a dictionary")
        return False

    if event.get("wiki") != "enwiki":
        return False

    if event.get("type") != "edit":
        return False

    if event.get("namespace") != 0:
        return False

    checks = {
        "event_id": get_event_id(event),
        "title": event.get("title"),
        "page_id": event.get("page_id"),
        "timestamp": event.get("timestamp"),
    }

    missing = [
        name for name, value in checks.items()
        if value is None or value == ""
    ]

    if missing:
        logger.warning(
            "Rejected event: missing=%s available_keys=%s",
            missing,
            list(event.keys()),
        )
        return False

    return True

    if not isinstance(event, dict):
        return False

    if event.get("wiki") != "enwiki":
        return False

    if event.get("type") != "edit":
        return False

    if event.get("namespace") != 0:
        return False

    if not get_event_id(event):
        return False

    if not event.get("title"):
        return False

    # if not event.get("page_id"):
    #     return False

    if event.get("timestamp") is None:
        return False

    return True


def delivery_callback(error, message):
    if error is not None:
        stats["failed"] += 1
        logger.error("Kafka delivery failed: %s", error)
    else:
        stats["published"] += 1


def print_stats():
    logger.info(
        "received=%d filtered=%d invalid=%d "
        "queued=%d published=%d failed=%d",
        stats["received"],
        stats["filtered"],
        stats["invalid"],
        stats["queued"],
        stats["published"],
        stats["failed"],
    )


def publish_event(producer, event):
    event_id = get_event_id(event)

    record = {
        "event_id": event_id,
        "event_time": datetime.fromtimestamp(
            event["timestamp"], tz=timezone.utc
        ).isoformat(),
        "ingested_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "raw_event": event,
    }

    payload = json.dumps(
        record, ensure_ascii=False
    ).encode("utf-8")

    while running:
        try:
            producer.produce(
                topic=TOPIC,
                key=event_id.encode("utf-8"),
                value=payload,
                on_delivery=delivery_callback,
            )
            stats["queued"] += 1
            producer.poll(0)
            return True

        except BufferError:
            logger.warning(
                "Producer queue full; waiting..."
            )
            producer.poll(1)

        except Exception:
            stats["failed"] += 1
            logger.exception("Unable to enqueue event")
            return False

    return False


def process_event(producer, event):
    stats["received"] += 1

    if not isinstance(event, dict):
        stats["invalid"] += 1
        return

    if (
        event.get("wiki") != "enwiki"
        or event.get("type") != "edit"
        or event.get("namespace") != 0
    ):
        stats["filtered"] += 1
        return

    if not is_valid_edit(event):
        stats["invalid"] += 1
        return

    publish_event(producer, event)


def stream_events(producer):
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
    })

    last_event_id = None
    backoff = 2
    last_stats = time.monotonic()

    while running:
        headers = {}

        if last_event_id:
            headers["Last-Event-ID"] = last_event_id

        try:
            logger.info("Connecting to %s", STREAM_URL)

            with session.get(
                STREAM_URL,
                headers=headers,
                stream=True,
                timeout=(15, 90),
            ) as response:
                response.raise_for_status()
                logger.info("EventStreams connected")
                backoff = 2

                data_lines = []
                sse_id = None

                for line in response.iter_lines(
                    decode_unicode=True
                ):
                    if not running:
                        break

                    if line is None:
                        continue

                    # Blank line marks the end of an SSE event.
                    if line == "":
                        if data_lines:
                            try:
                                event = json.loads(
                                    "\n".join(data_lines)
                                )
                                process_event(producer, event)

                                # Resume only after processing
                                # and enqueueing the event.
                                if sse_id is not None:
                                    last_event_id = sse_id

                            except (
                                ValueError,
                                TypeError,
                                OverflowError,
                            ):
                                stats["invalid"] += 1
                                logger.warning(
                                    "Invalid event payload"
                                )

                        data_lines = []
                        sse_id = None

                    elif line.startswith("data:"):
                        data_lines.append(
                            line[5:].lstrip(" ")
                        )

                    elif line.startswith("id:"):
                        sse_id = line[3:].strip()

                    if (
                        time.monotonic() - last_stats
                        >= STATS_INTERVAL
                    ):
                        producer.poll(0)
                        print_stats()
                        last_stats = time.monotonic()

        except requests.RequestException as exc:
            logger.warning(
                "Stream connection error: %s", exc
            )

        except Exception:
            logger.exception(
                "Unexpected collector error"
            )

        if running:
            logger.info(
                "Reconnecting in %d seconds", backoff
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    logger.info("Flushing Kafka messages...")
    remaining = producer.flush(30)

    if remaining:
        logger.error(
            "%d messages remain undelivered", remaining
        )

    print_stats()


def main():
    if (
        not USER_AGENT
        or "example.com" in USER_AGENT
    ):
        raise SystemExit(
            "Set a real WIKIMEDIA_USER_AGENT in .env"
        )

    producer = create_producer()

    logger.info("Starting WikiPulse collector")
    logger.info("Kafka: %s", KAFKA_SERVERS)
    logger.info("Topic: %s", TOPIC)

    stream_events(producer)


if __name__ == "__main__":
    main()
