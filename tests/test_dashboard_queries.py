"""Spark integration checks for full-scope dashboard aggregates and empty tables."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from src.dashboard_queries import DashboardPaths, DashboardQueries
from src.spark_common import create_spark, parse_kafka_events


class DashboardQueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark("Dashboard-Query-Tests", with_kafka=False)
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_live_totals_deduplication_and_article_filter(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = DashboardPaths(raw=root / "raw", live=root / "live", history=root / "history", curated=root / "curated")
            now = datetime.now(timezone.utc)
            messages = []
            for change_id in range(1, 5):
                event = {"id": change_id, "wiki": "enwiki", "namespace": 0, "type": "edit",
                         "title": "A" if change_id < 4 else "B", "user": "Editor", "bot": change_id == 4,
                         "length": {"old": 100, "new": 110}, "timestamp": int(now.timestamp())}
                envelope = {"event_id": f"enwiki:{change_id}", "event_time": now.isoformat(),
                            "ingested_at": now.isoformat(), "raw_event": event}
                messages.append((f"enwiki:{change_id}", json.dumps(envelope), "test", 0, change_id, now))
            messages.append(messages[0])
            kafka = self.spark.createDataFrame(messages, "key string, value string, topic string, partition int, offset long, timestamp timestamp")
            parsed = parse_kafka_events(kafka)
            parsed.write.partitionBy("ingestion_date").parquet(str(paths.raw))
            queries = DashboardQueries(self.spark, paths)
            activity = queries.activity("5m", "live")
            self.assertEqual(activity["metrics"]["edits"], 4)
            self.assertEqual(activity["metrics"]["pages"], 2)
            self.assertEqual(activity["metrics"]["gross_bytes"], 40)
            self.assertEqual(activity["rankings"][0]["title"], "A")
            article = queries.activity("5m", "live", "A")
            self.assertEqual(article["metrics"]["edits"], 3)
            quality = queries.quality()
            self.assertEqual(quality["raw_records"], 5)
            self.assertEqual(quality["valid_records"], 5)
            with patch("src.dashboard_queries.CURATED_DIR", paths.curated):
                self.assertEqual(queries.historical(None, None)["timeline"], [])
            self.assertEqual(queries.optional_records("alerts"), [])
            self.assertEqual(queries.article_titles("5m", "live"), ["A", "B"])

    def test_historical_rankings_with_optional_ml_score(self):
        queries = DashboardQueries(self.spark, DashboardPaths())
        day = "2026-10-15"
        tables = {
            "churn_bot_summary": self.spark.createDataFrame(
                [(day, 0, 12, 2, 100, 20)],
                "day string, ns int, total_edits int, bot_edits int, gross_churn_bytes int, net_bytes int",
            ),
            "rankings_daily": self.spark.createDataFrame(
                [(day, 1, "Article", 12, 100)],
                "day string, pageid long, title string, edits_total int, gross_churn_bytes int",
            ),
        }
        alerts = self.spark.createDataFrame(
            [(day, "Article", "MAJOR_REWRITE", 82.34, 94.12)],
            "day string, title string, alert_type string, historical_score double, spark_ml_score double",
        )
        with patch.object(queries, "_parquet_files", return_value=[]), \
                patch.object(queries, "historical_table", side_effect=tables.get):
            for has_ml_score in (False, True):
                with self.subTest(has_ml_score=has_ml_score):
                    tables["alerts"] = alerts if has_ml_score else alerts.drop("spark_ml_score")
                    data = queries.historical(day, day)
                    self.assertEqual(data["metrics"]["edits"], 12)
                    self.assertEqual(data["timeline"][0]["day"], day)
                    self.assertEqual(data["daily_alerts"][0]["major_rewrite"], 1)
                    rank = data["rankings"][0]
                    self.assertEqual(rank["detected_shifts"], 1)
                    self.assertEqual(rank["peak_anomaly_score"], 82.34)
                    self.assertEqual(rank["peak_ml_score"], 94.12 if has_ml_score else None)


if __name__ == "__main__":
    unittest.main()
