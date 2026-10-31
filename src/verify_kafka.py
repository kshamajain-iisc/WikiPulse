
import argparse
import json
import os

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--count", type=int, default=10
    )
    parser.add_argument(
        "--timeout", type=int, default=30
    )
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        ),
        "group.id": "wikipulse-verifier",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    topic = os.getenv(
        "KAFKA_TOPIC", "wiki.raw.edits"
    )

    consumer.subscribe([topic])

    checked = 0
    invalid = 0
    empty_polls = 0

    try:
        while (
            checked < args.count
            and empty_polls < 3
        ):
            message = consumer.poll(args.timeout)

            if message is None:
                empty_polls += 1
                continue

            if message.error():
                if (
                    message.error().code()
                    == KafkaError._PARTITION_EOF
                ):
                    continue
                raise RuntimeError(message.error())

            empty_polls = 0

            try:
                record = json.loads(
                    message.value().decode("utf-8")
                )

                raw = record["raw_event"]
                event_id = record["event_id"]

                assert raw["wiki"] == "enwiki"
                assert raw["type"] == "edit"
                assert raw["namespace"] == 0

                assert event_id == (
                    f"{raw['wiki']}:{raw['id']}"
                )

                assert message.key().decode(
                    "utf-8"
                ) == event_id

                assert record["event_time"]
                assert record["ingested_at"]

                checked += 1

                print(
                    f"[OK] {event_id} | "
                    f"{raw['title']} | "
                    f"partition={message.partition()} | "
                    f"offset={message.offset()}"
                )

            except (
                AssertionError,
                KeyError,
                ValueError,
                TypeError,
                UnicodeDecodeError,
                AttributeError,
            ) as exc:
                invalid += 1
                print(f"[INVALID] {exc}")

    finally:
        consumer.close()

    print()
    print("Validation summary")
    print(f"Valid records: {checked}")
    print(f"Invalid records: {invalid}")

    if checked < args.count or invalid:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
