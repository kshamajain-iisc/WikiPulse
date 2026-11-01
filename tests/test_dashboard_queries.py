"""Spark integration checks for full-scope dashboard aggregates and empty tables."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

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
            self.assertEqual(queries.historical(None, None)["timeline"], [])
            self.assertEqual(queries.optional_records("alerts"), [])
            self.assertEqual(queries.article_titles("5m", "live"), ["A", "B"])


if __name__ == "__main__":
    unittest.main()
