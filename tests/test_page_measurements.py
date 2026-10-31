"""Deterministic Spark integration checks using local synthetic inputs."""

import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, functions as F

from src.activity_dashboard import read_activity
from src.page_features import (
    HISTORY_SCHEMA,
    build_baselines,
    compare_history,
    historical_edits,
    live_edits,
    page_windows,
)
from src.prepare_history import validate_collection_coverage
from src.spark_common import create_spark, parse_kafka_events


class PageMeasurementTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark("Page-Measurement-Tests", with_kafka=False)
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def historical_input(self) -> DataFrame:
        """Include one duplicate, two humans, a bot, and missing byte sizes."""
        sample_edits = [
            (1, "A", False, 100, 150),
            (2, "B", False, 150, 120),
            (3, "Robot", True, None, None),
        ]
        records = []
        for change_id, user, is_bot, old_length, new_length in sample_edits:
            event = {
                "rcid": change_id,
                "revid": change_id,
                "pageid": 10,
                "ns": 0,
                "title": "Example",
                "type": "edit",
                "timestamp": "2026-10-01T00:01:00Z",
                "user": user,
                "userid": change_id,
                "bot": is_bot,
                "oldlen": old_length,
                "newlen": new_length,
                "tags": ["mw-undo"] if change_id == 2 else [],
            }
            records.append(json.dumps(event))

        input_lines = self.spark.sparkContext.parallelize(records + records[:1])
        return self.spark.read.schema(HISTORY_SCHEMA).json(input_lines)

    def kafka_input(self, change_id=1, event_time="2026-10-01T00:01:00Z", include_page_id=True) -> DataFrame:
        """Model the Kafka source schema without requiring a running broker."""
        event = {
            "id": change_id,
            "wiki": "enwiki",
            "type": "edit",
            "namespace": 0,
            "page_id": 10,
            "title": "Example",
            "timestamp": 1790812860,
            "user": "A",
            "userid": 5,
            "tags": ["mw-undo"],
            "length": {"old": 100, "new": 150},
        }
        if not include_page_id:
            event.pop("page_id")
        envelope = {
            "event_id": f"enwiki:{change_id}",
            "event_time": event_time,
            "ingested_at": "2026-10-01T00:01:01Z",
            "raw_event": event,
        }
        records = [
            (f"enwiki:{change_id}", json.dumps(envelope), "test", 0, 1, "2026-10-01T00:01:01Z"),
            ("bad", "{broken", "test", 0, 2, "2026-10-01T00:01:01Z"),
        ]
        schema = (
            "key string, value string, topic string, "
            "partition int, offset long, timestamp string"
        )
        return self.spark.createDataFrame(records, schema)

    def run_window_stream(self, edits: DataFrame, root: Path) -> None:
        """Drain available input using the same output and checkpoint on restart."""
        query = (
            page_windows(edits).writeStream.format("parquet")
            .outputMode("append")
            .option("path", str(root / "output"))
            .option("checkpointLocation", str(root / "checkpoint"))
            .trigger(availableNow=True)
            .start()
        )
        try:
            self.assertTrue(query.awaitTermination(60), "Streaming query timed out")
            self.assertFalse(query.isActive)
            self.assertIsNone(query.exception())
        finally:
            query.stop()

    def test_features_baseline_and_no_leakage(self):
        edits = historical_edits(self.historical_input())
        self.assertEqual(edits.count(), 3)
        windows = page_windows(edits)
        row = windows.first()
        self.assertEqual((row.edits_total, row.edits_human, row.edits_bot), (3, 2, 1))
        self.assertEqual((row.gross_bytes, row.net_bytes, row.rewriting_bytes), (80, 20, 60))
        self.assertEqual(row.distinct_human_editors, 2)
        self.assertAlmostEqual(row.size_coverage, 2 / 3)
        self.assertEqual(row.reverts_count, 1)

        # A complete day contains 288 five-minute windows, including quiet ones.
        baseline = build_baselines(windows, "2026-10-01", "2026-10-02", 300)
        self.assertAlmostEqual(baseline.first().avg_edits_human, 2 / 288)
        self.assertFalse(compare_history(windows, baseline).first().has_baseline)
        later_windows = windows.withColumn("day", F.lit("2026-10-02").cast("date"))
        self.assertTrue(compare_history(later_windows, baseline).first().has_baseline)

    def test_shared_parser_and_adapter(self):
        parsed = parse_kafka_events(self.kafka_input())
        self.assertEqual(parsed.filter("is_valid").count(), 1)
        self.assertEqual(parsed.filter("NOT is_valid").count(), 1)
        event = live_edits(parsed).first()
        self.assertEqual(
            (event.rcid, event.user, event.delta, event.is_revert_action),
            (1, "A", 50, True),
        )

    def test_stream_events_without_page_id_are_recovered(self):
        parsed = parse_kafka_events(self.kafka_input(include_page_id=False))
        event = parsed.filter("is_valid").first()
        self.assertIsNotNone(event)
        self.assertLess(event.page_id, 0)
        # Previously stored validation flags must not prevent recovery.
        old_bronze = parsed.withColumn("is_valid", F.lit(False))
        recovered = live_edits(old_bronze)
        self.assertEqual(recovered.count(), 1)
        self.assertEqual(recovered.first().pageid, event.page_id)

    def test_title_fallback_matches_historical_baseline(self):
        baseline_windows = page_windows(historical_edits(self.historical_input()))
        baseline = build_baselines(baseline_windows, "2026-10-01", "2026-10-02", 300)
        live = live_edits(parse_kafka_events(
            self.kafka_input(event_time="2026-10-02T00:01:00Z", include_page_id=False)
        ))
        comparison = compare_history(page_windows(live), baseline).first()
        self.assertTrue(comparison.has_baseline)
        self.assertLess(comparison.pageid, 0)

    def test_live_preview_before_window_finalization(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            now = datetime.now(timezone.utc).isoformat()
            parsed = parse_kafka_events(self.kafka_input(event_time=now, include_page_id=False))
            # Use current ingestion dates so the preview's partition pruning applies.
            parsed = parsed.withColumn("ingestion_date", F.current_date())
            parsed.withColumn("is_valid", F.lit(False)).write.parquet(str(root / "raw"))
            activity = read_activity(
                self.spark, root / "live", root / "history", "5m",
                raw_directory=root / "raw",
            )
            self.assertEqual(activity["view"], "live")
            self.assertEqual(activity["rows"][0]["edits_total"], 1)
            self.assertIn("provisional", activity["status"])

    def test_streaming_file_sink(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            parsed = parse_kafka_events(self.kafka_input())
            parsed.write.parquet(str(root / "input"))
            source = self.spark.readStream.schema(parsed.schema).parquet(str(root / "input"))
            edits = (
                live_edits(source)
                .withWatermark("ts", "10 minutes")
                .dropDuplicatesWithinWatermark(["event_id"])
            )
            self.run_window_stream(edits, root)

            # The initial window stays in state until a newer event advances time.
            self.assertEqual(self.spark.read.parquet(str(root / "output")).count(), 0)
            newer_event = parse_kafka_events(
                self.kafka_input(change_id=2, event_time="2026-10-01T02:00:00Z")
            ).filter("is_valid")
            # Replay the original ID together with the newer event. Checkpointed
            # duplicate tracking must prevent the old window from counting twice.
            parsed.unionByName(newer_event).write.mode("append").parquet(str(root / "input"))
            self.run_window_stream(edits, root)
            saved_windows = self.spark.read.parquet(str(root / "output")).collect()
            self.assertEqual(len(saved_windows), 1)
            self.assertEqual(saved_windows[0].edits_total, 1)

    def test_dashboard_reads_saved_windows(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            waiting = read_activity(self.spark, root / "live", root / "history", "5m", view="finalized")
            self.assertEqual(waiting["rows"], [])

            windows = page_windows(historical_edits(self.historical_input()))
            windows.write.parquet(str(root / "live/windows_5m"))
            activity = read_activity(self.spark, root / "live", root / "history", "5m", view="finalized")
            self.assertEqual(len(activity["rows"]), 1)
            self.assertEqual(activity["rows"][0]["edits_total"], 3)
            self.assertIsNone(activity["rows"][0].get("human_edit_ratio"))

    def test_collection_coverage_rejects_gaps(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            checkpoints = root / "checkpoints"
            checkpoints.mkdir()
            checkpoint_path = checkpoints / "collection.json"
            checkpoint = {
                "start": "2026-10-01T00:00:00Z",
                "end": "2026-10-02T00:00:00Z",
                "status": "complete",
            }
            checkpoint_path.write_text(json.dumps(checkpoint))
            validate_collection_coverage(root, date(2026, 10, 1), date(2026, 10, 2))
            with self.assertRaises(ValueError):
                validate_collection_coverage(root, date(2026, 10, 1), date(2026, 10, 3))


if __name__ == "__main__":
    unittest.main()
