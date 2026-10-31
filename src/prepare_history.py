"""Prepare historical page windows and baselines from complete daily collections.

Run with ``python -m src.prepare_history --start YYYY-MM-DD --end YYYY-MM-DD``.
The end date is exclusive. Existing outputs at --output are overwritten.
"""

import argparse
import json
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

from src.page_features import (
    HISTORY_SCHEMA,
    build_baselines,
    historical_edits,
    page_windows,
)
from src.spark_common import ROOT, create_spark


def daily_source_files(input_directory: Path, start: date, end: date) -> list[Path]:
    """Require one collection file for every UTC day in the requested interval."""
    if end <= start:
        raise ValueError("End date must follow start date")

    days = [start + timedelta(days=offset) for offset in range((end - start).days)]
    files = [input_directory / f"rc_{day}.jsonl.gz" for day in days]
    missing_files = [str(path) for path in files if not path.is_file()]
    if missing_files:
        raise ValueError("Missing daily source files: " + ", ".join(missing_files))
    return files


def validate_collection_coverage(input_directory: Path, start: date, end: date) -> None:
    """Reject partial collections before interpreting quiet windows as zero edits."""
    completed_intervals = []
    for checkpoint_path in (input_directory / "checkpoints").glob("*.json"):
        checkpoint = json.loads(checkpoint_path.read_text())
        if checkpoint.get("status") != "complete":
            continue

        interval_start = datetime.fromisoformat(checkpoint["start"].replace("Z", "+00:00"))
        interval_end = datetime.fromisoformat(checkpoint["end"].replace("Z", "+00:00"))
        completed_intervals.append((interval_start, interval_end))

    # Merge overlapping or adjacent complete intervals. A gap prevents the cursor
    # from reaching the requested end, even if later checkpoints are complete.
    covered_until = datetime.combine(start, time.min, timezone.utc)
    requested_end = datetime.combine(end, time.min, timezone.utc)
    for interval_start, interval_end in sorted(completed_intervals):
        if interval_start <= covered_until:
            covered_until = max(covered_until, interval_end)

    if covered_until < requested_end:
        raise ValueError(
            "Completed collector checkpoints do not cover the baseline interval; "
            "finish collection, or attest external full-day files with --trust-complete-files"
        )


def prepare_tables(
    spark: SparkSession,
    source_files: list[Path],
    output_directory: Path,
    start: str,
    end: str,
) -> None:
    """Write canonical edits, then matching five-minute and hourly result tables."""
    raw_events = (
        spark.read.schema(HISTORY_SCHEMA)
        .option("mode", "FAILFAST")
        .json([str(path) for path in source_files])
    )
    edits = historical_edits(raw_events).filter(
        (F.col("day") >= start) & (F.col("day") < end)
    )

    # Both window durations reuse the normalized input. Spill to disk if it does
    # not fit in memory rather than re-reading and re-normalizing the gzip files.
    edits.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        edits.write.mode("overwrite").partitionBy("day").parquet(
            str(output_directory / "edits")
        )
        duration_settings = [(300, "5 minutes", "5m"), (3600, "1 hour", "1h")]
        for duration_seconds, duration, label in duration_settings:
            windows = page_windows(edits, duration)
            windows.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                windows.write.mode("overwrite").partitionBy("day").parquet(
                    str(output_directory / f"windows_{label}")
                )
                baselines = build_baselines(windows, start, end, duration_seconds)
                baselines.write.mode("overwrite").parquet(
                    str(output_directory / f"baselines_{label}")
                )
            finally:
                windows.unpersist()
    finally:
        edits.unpersist()


def main() -> None:
    """Validate the requested collection interval and run the batch Spark job."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(ROOT / "data/enwiki_recentchanges"))
    parser.add_argument("--output", default=str(ROOT / "data/week2/historical"))
    parser.add_argument(
        "--trust-complete-files",
        action="store_true",
        help="For external datasets only: attest that each input day is complete",
    )
    parser.add_argument("--start", required=True, help="Complete UTC date, inclusive")
    parser.add_argument("--end", required=True, help="Complete UTC date, exclusive")
    args = parser.parse_args()

    try:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
        source_files = daily_source_files(Path(args.input), start, end)
        if not args.trust_complete_files:
            validate_collection_coverage(Path(args.input), start, end)
    except ValueError as error:
        parser.error(str(error))

    spark = create_spark("WikiPulse-Historical-Measurements", with_kafka=False)
    try:
        prepare_tables(spark, source_files, Path(args.output), args.start, args.end)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
