
import json
import logging
import os
import signal
import time

import requests
from dotenv import load_dotenv

try:
    from .stream_source import CaptureProgress, normalize_since, stream_url
    from .kafka_common import (
        KAFKA_SERVERS, KAFKA_TOPIC, build_envelope, configure_logging,
        create_producer, get_event_id, is_valid_edit,
    )
except ImportError:  # Support python src/collector.py as well as python -m.
    from stream_source import CaptureProgress, normalize_since, stream_url
    from kafka_common import (
        KAFKA_SERVERS, KAFKA_TOPIC, build_envelope, configure_logging,
        create_producer, get_event_id, is_valid_edit,
    )

load_dotenv()

TOPIC = KAFKA_TOPIC
STREAM_URL = os.getenv(
    "WIKIMEDIA_STREAM_URL",
    "https://stream.wikimedia.org/v2/stream/recentchange",
)
USER_AGENT = (
    os.getenv("WIKIMEDIA_USER_AGENT")
    or os.getenv("WIKIPULSE_USER_AGENT", "WikiPulse/1.0 (Senthil Raj K; mailto:senthilrajk@iisc.ac.in)")
)
STATS_INTERVAL = int(
    os.getenv("STATS_INTERVAL_SECONDS", "30")
)

configure_logging()
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


def publish_event(producer, event, progress=None):
    event_id = get_event_id(event)

    record = build_envelope(event)

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
            if progress is not None:
                progress.record(event, len(payload))
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


def process_event(producer, event, progress=None):
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

    return publish_event(producer, event, progress)


def stream_events(producer, since=None, progress=None):
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
            url = stream_url(STREAM_URL, None if last_event_id else since)
            logger.info("Connecting to %s", url)
            if progress:
                progress.write("connecting", "Reconnecting from the last processed event" if last_event_id else "Connecting to Wikipedia", force=True)

            with session.get(
                url,
                headers=headers,
                stream=True,
                timeout=(15, 90),
            ) as response:
                response.raise_for_status()
                logger.info("EventStreams connected")
                if progress:
                    progress.write("connected", "Wikipedia connected; fetching since " + since if since else "Wikipedia connected; receiving live events", force=True)
                backoff = 2

                data_lines = []
                sse_id = None
                sse_type = "message"

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
                                if sse_type == "error":
                                    raise requests.RequestException("Wikipedia stream returned an error event")
                                published = process_event(producer, event, progress)
                                if published is False:
                                    raise RuntimeError("Kafka enqueue failed; reconnecting without advancing cursor")

                                # Resume only after processing
                                # and enqueueing the event.
                                if sse_id is not None:
                                    last_event_id = sse_id
                                if progress:
                                    progress.write(counts=stats)

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
                        sse_type = "message"

                    elif line.startswith("data:"):
                        data_lines.append(
                            line[5:].lstrip(" ")
                        )

                    elif line.startswith("id:"):
                        sse_id = line[3:].strip()
                    elif line.startswith("event:"):
                        sse_type = line[6:].strip()

                    if (
                        time.monotonic() - last_stats
                        >= STATS_INTERVAL
                    ):
                        producer.poll(0)
                        print_stats()
                        last_stats = time.monotonic()

        except requests.RequestException as exc:
            if progress:
                progress.write("reconnecting", "Wikipedia connection interrupted; retrying from the last processed event", force=True)
            logger.warning(
                "Stream connection error: %s", exc
            )

        except Exception:
            if progress:
                progress.write("reconnecting", "Capture interrupted; retrying without advancing the event cursor", force=True)
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
    session.close()
    if progress:
        progress.write("stopped", "Wikipedia capture stopped", force=True, counts=stats)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Capture Wikipedia events live or from an upstream timestamp")
    parser.add_argument("--since", help="Historical start timestamp with timezone; omitted means live now")
    args = parser.parse_args()
    try:
        since = normalize_since(args.since)
    except ValueError as error:
        parser.error(str(error))
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

    stream_events(producer, since, CaptureProgress(since))


if __name__ == "__main__":
    main()
