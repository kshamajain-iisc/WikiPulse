"""Replay historical edits to an isolated Kafka topic at 1x, 10x, 50x, or 100x."""

import argparse
import json
import math
from pathlib import Path

from pyspark.sql import functions as F

from src.kafka_common import KAFKA_TOPIC, create_producer, ensure_topic
from src.page_features import HISTORY_SCHEMA, historical_edits, live_edits
from src.replay.events import paced_envelopes, to_event
from src.spark_common import create_spark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--format", choices=["history-json", "canonical-parquet", "bronze-parquet"], default="history-json")
    parser.add_argument("--topic", default="wiki.replay.edits")
    parser.add_argument("--speed", type=float, default=10.0, help="Event-time multiplier; 0 means unpaced")
    parser.add_argument("--limit", type=int, help="Optional maximum event count")
    parser.add_argument("--dry-run", action="store_true", help="Validate and summarize without Kafka or waiting")
    args = parser.parse_args()
    if not math.isfinite(args.speed) or args.speed < 0 or (args.limit is not None and args.limit < 1):
        parser.error("Speed must be finite and nonnegative; limit must be positive")
    if args.topic == KAFKA_TOPIC:
        parser.error("Replay requires a separate topic from the configured live KAFKA_TOPIC")
    spark = create_spark("WikiPulse-Historical-Replay", with_kafka=False)
    try:
        if args.format == "history-json":
            edits = historical_edits(spark.read.schema(HISTORY_SCHEMA).option("mode", "FAILFAST").json(str(args.input)))
        elif args.format == "bronze-parquet":
            edits = live_edits(spark.read.parquet(str(args.input))).dropDuplicates(["event_id"])
        else:
            edits = spark.read.parquet(str(args.input)).dropDuplicates(["rcid"])
        ordered = edits.withColumn("event_epoch", F.col("ts").cast("long")).orderBy("ts", "rcid")
        if args.limit:
            ordered = ordered.limit(args.limit)
        producer = None
        failures = []
        if not args.dry_run:
            ensure_topic(args.topic)
            producer = create_producer("wikipulse-historical-replay")

        def delivered(error, message):
            if error:
                failures.append(str(error))

        count = 0
        events = (to_event(row.asDict()) for row in ordered.toLocalIterator())
        for envelope in paced_envelopes(events, 0 if args.dry_run else args.speed):
            if producer:
                while True:
                    try:
                        producer.produce(args.topic, key=envelope["event_id"], value=json.dumps(envelope), on_delivery=delivered)
                        break
                    except BufferError:
                        producer.poll(0.1)
                producer.poll(0)
            count += 1
        if producer and (producer.flush(120) or failures):
            raise RuntimeError(f"Replay delivery failed: {failures[:3]}")
        print(json.dumps({"events": count, "topic": args.topic, "speed": args.speed, "dry_run": args.dry_run}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
