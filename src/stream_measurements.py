"""Run checkpointed raw ingestion or event-time page measurements.

Run ``python -m src.stream_measurements bronze`` first, followed by a separate
``python -m src.stream_measurements windows --duration 5m`` process.
"""

import argparse
import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from src.page_features import live_edits, page_windows
from src.spark_common import (
    CHECKPOINT_PATH,
    KAFKA_SERVERS,
    KAFKA_TOPIC,
    RAW_PATH,
    ROOT,
    create_spark,
    parse_kafka_events,
)


def read_kafka_events(spark: SparkSession, starting_offsets: str) -> DataFrame:
    """Read bounded microbatches while failing explicitly if Kafka history is lost."""
    kafka_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", os.getenv("SPARK_MAX_OFFSETS", "50000"))
        .option("failOnDataLoss", "true")
        .load()
    )
    # Reuse the shared parser so inspection and storage enforce the same contract.
    return parse_kafka_events(kafka_events)


def read_page_windows(
    spark: SparkSession,
    schema_path: Path,
    duration: str,
    watermark: str,
) -> DataFrame:
    """Normalize stored events, remove recent duplicates, then aggregate by page."""
    schema = StructType.fromJson(json.loads(schema_path.read_text()))
    bronze_events = spark.readStream.schema(schema).parquet(RAW_PATH)

    # Set the watermark before the stateful operators. This bounds duplicate
    # tracking and lets Spark finalize windows when later event times arrive.
    edits = (
        live_edits(bronze_events)
        .withWatermark("ts", watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    window_duration = "5 minutes" if duration == "5m" else "1 hour"
    return page_windows(edits, window_duration)


def main() -> None:
    """Configure one streaming stage and stop its query cleanly on interruption."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=["bronze", "windows"])
    parser.add_argument("--duration", choices=["5m", "1h"], default="5m")
    parser.add_argument("--output", default=str(ROOT / "data/live_activity"))
    parser.add_argument("--checkpoint-root", default=str(ROOT / "data/checkpoints/page_activity"))
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--ready-file", type=Path, help="Optional startup marker for process supervisors")
    args = parser.parse_args()

    schema_path = Path(args.output) / "bronze_schema.json"
    if args.stage == "windows" and not schema_path.exists():
        parser.error("Start the bronze stage first to create its schema")

    spark = create_spark(
        "WikiPulse-Streaming-" + args.stage,
        with_kafka=args.stage == "bronze",
    )
    query = None
    try:
        if args.stage == "bronze":
            rows = read_kafka_events(spark, args.starting_offsets)
            # File streaming requires a known schema. Publish it alongside the
            # measurement outputs so downstream jobs do not infer it each time.
            schema_path.parent.mkdir(parents=True, exist_ok=True)
            schema_path.write_text(rows.schema.json())
            output_path = RAW_PATH
            checkpoint_path = CHECKPOINT_PATH
            partition_column = "ingestion_date"
        else:
            rows = read_page_windows(spark, schema_path, args.duration, args.watermark)
            output_path = str(Path(args.output) / f"windows_{args.duration}")
            checkpoint_path = str(
                Path(args.checkpoint_root) / f"windows_{args.duration}"
            )
            partition_column = "day"

        # Each stage/duration owns its checkpoint. The append file sink commits
        # finalized batches and uses that checkpoint to resume after a restart.
        writer = (
            rows.writeStream.format("parquet")
            .outputMode("append")
            .partitionBy(partition_column)
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .queryName(f"measurements_{args.stage}_{args.duration}")
        )
        if args.available_now:
            writer = writer.trigger(availableNow=True)
        else:
            trigger_seconds = os.getenv("SPARK_TRIGGER_SECONDS", "10")
            writer = writer.trigger(processingTime=f"{trigger_seconds} seconds")

        query = writer.start()
        if args.ready_file is not None:
            args.ready_file.parent.mkdir(parents=True, exist_ok=True)
            args.ready_file.write_text(str(query.id))
        query.awaitTermination()
    except KeyboardInterrupt:
        # Ctrl+C is a normal shutdown; the checkpoint remains available to resume.
        pass
    finally:
        if query is not None:
            query.stop()
        if args.ready_file is not None:
            args.ready_file.unlink(missing_ok=True)
        spark.stop()


if __name__ == "__main__":
    main()
