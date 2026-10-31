
import argparse
import gzip
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer


TOPIC = "wiki.raw.edits"
BOOTSTRAP_SERVERS = "localhost:9092"


def delivery_callback(error, message):
    if error:
        print(f"Delivery failed: {error}")


def replay(files, rate, limit):
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "wikipulse-replay",
        "enable.idempotence": True,
    })

    stats = {
        "read": 0,
        "filtered": 0,
        "queued": 0,
        "published": 0,
        "failed": 0,
    }

    def on_delivery(error, message):
        if error:
            stats["failed"] += 1
            delivery_callback(error, message)
        else:
            stats["published"] += 1

    for file_path in files:
        print(f"\nReading: {file_path}")

        with gzip.open(file_path, "rt", encoding="utf-8") as file:
            for line in file:
                stats["read"] += 1

                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    stats["filtered"] += 1
                    continue

                if not (
                    event.get("wiki") == "enwiki"
                    and event.get("namespace") == 0
                    and event.get("type") == "edit"
                ):
                    stats["filtered"] += 1
                    continue

                event_id = event.get("meta", {}).get("id")

                if not event_id:
                    stats["filtered"] += 1
                    continue

                payload = {
                    "event_id": event_id,
                    "ingested_at": datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "event": event,
                }

                producer.produce(
                    topic=TOPIC,
                    key=str(event_id).encode("utf-8"),
                    value=json.dumps(payload).encode("utf-8"),
                    on_delivery=on_delivery,
                )

                stats["queued"] += 1
                producer.poll(0)

                if rate > 0:
                    time.sleep(1 / rate)

                if limit and stats["queued"] >= limit:
                    break

            if limit and stats["queued"] >= limit:
                break

    remaining = producer.flush(timeout=60)

    if remaining:
        print(f"WARNING: {remaining} messages remain undelivered")

    print("\n--- Replay Statistics ---")
    for name, value in stats.items():
        print(f"{name}: {value}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="Path to a .jsonl.gz file or directory",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=100,
        help="Maximum messages per second; 0 disables delay",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum matching events; 0 means unlimited",
    )

    args = parser.parse_args()

    input_path = Path(args.input)

    if input_path.is_file():
        files = [input_path]
    else:
        files = sorted(input_path.glob("*.jsonl.gz"))

    if not files:
        raise FileNotFoundError("No .jsonl.gz files found")

    replay(files, args.rate, args.limit)
