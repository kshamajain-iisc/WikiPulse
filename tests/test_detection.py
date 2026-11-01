"""Detector arithmetic, missing evidence, leakage, and real checkpoint recovery."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from src.dashboard_queries import DashboardPaths, DashboardQueries
from src.detection.ml import compare_methods, distances
from src.detection.pipeline import pin_contract, start_stream
from src.detection.scoring import DetectorConfig, score_windows
from src.detection.storage import commit_batch, publish_alerts
from src.page_features import HISTORY_SCHEMA, build_baselines, historical_edits, page_windows
from src.spark_common import create_spark


class DetectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark("Detection-Tests", with_kafka=False)
        cls.spark.sparkContext.setLogLevel("ERROR")
        # Construct actual shared feature rows, then change measurements explicitly
        # to make expected scores independent of the detector implementation.
        raw = cls.spark.read.schema(HISTORY_SCHEMA).json(cls.spark.sparkContext.parallelize([
            '{"rcid":1,"pageid":10,"ns":0,"title":"Example","type":"edit",'
            '"timestamp":"2026-10-01T00:01:00Z","user":"A","userid":1,'
            '"bot":false,"oldlen":100,"newlen":120}'
        ]))
        cls.training = page_windows(historical_edits(raw)).cache()
        cls.baseline = build_baselines(cls.training, "2026-10-01", "2026-10-02", 300)
        for metric, avg, std in (("edits_human", 2.0, 1.0), ("distinct_human_editors", 2.0, 1.0), ("gross_bytes", 100.0, 100.0)):
            cls.baseline = cls.baseline.withColumn("avg_" + metric, F.lit(avg)).withColumn("std_" + metric, F.lit(std))
        cls.baseline = cls.baseline.cache()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def window(self):
        return (self.training.withColumn("window_start", F.to_timestamp(F.lit("2026-10-02 00:00:00")))
                .withColumn("window_end", F.to_timestamp(F.lit("2026-10-02 00:05:00")))
                .withColumn("day", F.to_date("window_start"))
                .withColumn("window_id", F.lit("evaluation-window"))
                .withColumn("edits_total", F.lit(14).cast("long"))
                .withColumn("edits_human", F.lit(14).cast("long"))
                .withColumn("distinct_human_editors", F.lit(8).cast("long"))
                .withColumn("gross_bytes", F.lit(10000).cast("long")))

    def test_component_arithmetic_and_stable_identity(self):
        scored = score_windows(self.window(), self.baseline)
        row = scored.first()
        self.assertEqual((row.activity_points, row.editor_points, row.content_points, row.revert_points), (35, 20, 20, 0))
        self.assertAlmostEqual(row.historical_score, 7500 / 85)
        self.assertTrue(row.historical_alert)
        self.assertEqual(row.available_weight, 85)
        self.assertIn("pageviews", row.missing_evidence)
        self.assertIsNone(row.related_page_points)
        self.assertEqual(len(row.reasons), 3)
        self.assertEqual(row.alert_id, score_windows(self.window(), self.baseline).first().alert_id)

    def test_missing_baseline_sizes_and_training_period(self):
        cold = score_windows(self.window(), None).first()
        self.assertTrue(cold.is_alert)
        self.assertFalse(cold.historical_alert)
        self.assertEqual(cold.confidence, "low_no_baseline")
        self.assertIsNone(cold.historical_score)
        missing = self.window().withColumn("size_coverage", F.lit(0.5))
        row = score_windows(missing, self.baseline).first()
        self.assertIsNone(row.content_points)
        self.assertAlmostEqual(row.historical_score, 5500 / 65)
        self.assertEqual(row.confidence, "limited_sizes")
        during_training = self.window().withColumn("day", F.lit("2026-10-01").cast("date"))
        row = score_windows(during_training, self.baseline).first()
        self.assertFalse(row.is_alert)
        self.assertIsNone(row.historical_score)

    def test_bot_editor_hourly_and_normal_volume_controls(self):
        cases = [self.window().withColumn("bot_ratio", F.lit(0.8)),
                 self.window().withColumn("distinct_human_editors", F.lit(1)),
                 self.window().withColumn("window_duration", F.lit("1 hour"))]
        for frame in cases:
            self.assertFalse(score_windows(frame, None).first().is_alert)
        normal = self.window().withColumn("edits_human", F.lit(2)).withColumn("distinct_human_editors", F.lit(2)).withColumn("gross_bytes", F.lit(100))
        self.assertEqual(score_windows(normal, self.baseline).first().historical_score, 0)
        revert = self.window().withColumn("revert_ratio", F.lit(0.5))
        row = score_windows(revert, self.baseline).first()
        self.assertEqual(row.revert_points, 10)
        self.assertEqual(row.historical_score, 100)
        self.assertEqual(row.alert_type, "revert_activity")

    def test_zero_variance_and_invalid_configuration(self):
        baseline = self.baseline.withColumn("std_edits_human", F.lit(0.0))
        row = score_windows(self.window(), baseline).first()
        self.assertEqual(row.activity_points, 35)
        for kwargs in ({"threshold": 101}, {"zscore_cap": 0}, {"min_editors": 0}):
            with self.assertRaises(ValueError):
                DetectorConfig(**kwargs)

    def test_local_commit_reuses_saved_values_and_kafka_failures_propagate(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "batch"
            original = score_windows(self.window(), None)
            commit_batch(original, path)
            recovered = commit_batch(original.withColumn("historical_score", F.lit(99.0)), path)
            self.assertIsNone(recovered.first().historical_score)
            producer = Mock()
            producer.flush.return_value = 1
            with patch("src.detection.storage.create_producer", return_value=producer):
                with self.assertRaisesRegex(RuntimeError, "Kafka alert delivery failed"):
                    publish_alerts(recovered, "test-alerts")
                self.assertEqual(producer.produce.call_args.kwargs["key"], recovered.first().alert_id)
            producer.flush.return_value = 0
            producer.produce.side_effect = lambda *args, **kwargs: kwargs["on_delivery"]("delivery error", None)
            with patch("src.detection.storage.create_producer", return_value=producer):
                with self.assertRaisesRegex(RuntimeError, "delivery error"):
                    publish_alerts(recovered, "test-alerts")

    def test_stream_restart_and_dashboard_duration_filter(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.window().write.parquet(str(root / "windows"))
            self.baseline.write.parquet(str(root / "baseline"))

            def drain():
                query = start_stream(self.spark, root / "windows", root / "baseline", root / "results",
                                     root / "checkpoint", "5m", available_now=True)
                try:
                    self.assertTrue(query.awaitTermination(60))
                    self.assertIsNone(query.exception())
                finally:
                    query.stop()

            drain()
            drain()
            queries = DashboardQueries(self.spark, DashboardPaths(history=root / "missing", curated=root / "results"))
            self.assertEqual(len(queries.alerts("5m")), 1)
            self.assertEqual(queries.alerts("1h"), [])
            self.assertEqual(queries.alerts("5m", "different title"), [])
            later = self.window().withColumn("window_id", F.lit("second-window"))
            later.write.mode("append").parquet(str(root / "windows"))
            drain()
            self.assertEqual(len(queries.alerts("5m")), 2)
            with self.assertRaisesRegex(ValueError, "inputs changed"):
                pin_contract(root / "checkpoint", root / "windows", root / "baseline", root / "results", "5m", DetectorConfig(threshold=70), None)

    def test_ml_temporal_split_and_model_roundtrip(self):
        training = self.training
        for index in range(1, 8):
            training = training.unionByName(self.training.withColumn("edits_human", F.lit(index + 1).cast("long"))
                                            .withColumn("gross_bytes", F.lit(index * 100).cast("long")))
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "ml"
            result, metadata = compare_methods(training, self.window(), self.baseline, output)
            row = result.first()
            self.assertGreaterEqual(row.ml_distance, 0)
            self.assertTrue(0 <= row.ml_score <= 100)
            loaded = PipelineModel.load(str(output / "model"))
            self.assertAlmostEqual(distances(loaded, self.window()).first().ml_distance, row.ml_distance)
            self.assertIn("historical_alert_count", metadata["counts"])
            self.assertEqual(metadata["training_end"], "2026-10-01T00:05:00Z")
            with self.assertRaisesRegex(ValueError, "after training"):
                compare_methods(training, self.training, self.baseline, Path(temporary) / "overlap")


if __name__ == "__main__":
    unittest.main()
