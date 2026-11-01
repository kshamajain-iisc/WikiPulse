"""Local immutable microbatch storage and acknowledged Kafka delivery.

Spark may retry foreachBatch after a crash. Reuse the committed local batch and
publish the same alert IDs again; Kafka consumers must deduplicate by alert_id.
"""

import json
import shutil
from pathlib import Path

from pyspark.sql import DataFrame

from src.kafka_common import create_producer


def commit_batch(rows: DataFrame, destination: Path) -> DataFrame:
    """Stage beside the destination, then atomically rename on a local filesystem."""
    if not (destination / "_SUCCESS").is_file():
        if destination.exists():
            raise RuntimeError(f"Incomplete committed batch: {destination}")
        staging = destination.with_name(".pending-" + destination.name)
        shutil.rmtree(staging, ignore_errors=True)
        rows.write.mode("error").parquet(str(staging))
        staging.rename(destination)
    # Read back the committed values, rather than recomputing a retried batch.
    return rows.sparkSession.read.parquet(str(destination))


def publish_alerts(alerts: DataFrame, topic: str) -> None:
    """Fail the batch on any delivery failure; iteration bounds driver memory."""
    producer = create_producer("wikipulse-alerts")
    failures = []

    def delivered(error, message):
        if error is not None:
            failures.append(str(error))

    for payload in alerts.toJSON().toLocalIterator():
        record = json.loads(payload)
        while True:
            try:
                producer.produce(topic, key=record["alert_id"], value=payload, on_delivery=delivered)
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
    remaining = producer.flush(120)
    if remaining or failures:
        raise RuntimeError(f"Kafka alert delivery failed: {remaining} pending; {failures[:3]}")
