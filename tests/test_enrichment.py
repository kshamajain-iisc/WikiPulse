"""Unit and Spark integration tests for Milestone 6 contextual enrichment & dashboard queries."""

import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from src.dashboard_queries import DashboardPaths, DashboardQueries
from src.enrichment.geospatial_map import (
    _extract_linked_qids,
    _extract_p625_from_entity,
    _valid_lat_lon,
)
from src.enrichment.graph_and_diffs import parse_diff_html
from src.enrichment.revert_risk_monitor import (
    compute_heuristic_risk_and_reasons,
    evaluate_threshold_curve,
)
from src.models.schemas import ALERT_SCHEMA
from src.spark_common import create_spark


class EnrichmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark("Enrichment-Tests", with_kafka=False)
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_parse_diff_html_strips_html_and_counts_lines_and_links(self):
        sample_diff = (
            '<tr><td class="diff-deletedline"><div>Old text <del>removed</del></div></td>'
            '<td class="diff-addedline"><div>New text https://example.org/ref1 and http://test.com</div></td></tr>'
            '<tr><td class="diff-addedline"><div>Second <script>alert(1)</script> line</div></td></tr>'
        )
        added, removed, links, snippet = parse_diff_html(sample_diff)
        self.assertEqual(added, 2)
        self.assertEqual(removed, 1)
        self.assertEqual(links, 2)
        self.assertNotIn("<div", snippet)
        self.assertNotIn("<script", snippet)
        self.assertIn("+ New text", snippet)
        self.assertIn("- Old text", snippet)

    def test_wikidata_coordinate_and_claim_parsers(self):
        self.assertEqual(_valid_lat_lon(12.9716, 77.5946), (12.9716, 77.5946))
        self.assertIsNone(_valid_lat_lon(95.0, 10.0))
        self.assertIsNone(_valid_lat_lon("invalid", 10.0))

        entity = {
            "claims": {
                "P625": [
                    {
                        "mainsnak": {
                            "datavalue": {
                                "value": {"latitude": 35.6895, "longitude": 139.6917}
                            }
                        }
                    }
                ],
                "P17": [
                    {
                        "mainsnak": {
                            "datavalue": {
                                "value": {"id": "Q17"}
                            }
                        }
                    }
                ],
            }
        }
        self.assertEqual(_extract_p625_from_entity(entity), (35.6895, 139.6917))
        self.assertEqual(_extract_linked_qids(entity, "P17"), ["Q17"])

    def test_heuristic_revert_risk_and_threshold_calibration(self):
        high_score, reasons = compute_heuristic_risk_and_reasons(
            is_anon=True,
            is_bot=False,
            oldlen=10000,
            newlen=2000,
            delta=-8000,
            comment="",
            on_alerted_page=True,
            page_revert_Action_rate=0.40,
        )
        self.assertGreaterEqual(high_score, 0.80)
        self.assertGreaterEqual(len(reasons), 3)

        bot_score, bot_reasons = compute_heuristic_risk_and_reasons(
            is_anon=False,
            is_bot=True,
            oldlen=1000,
            newlen=1050,
            delta=50,
            comment="Bot maintenance",
            on_alerted_page=False,
        )
        self.assertLessEqual(bot_score, 0.10)
        self.assertEqual(len(bot_reasons), 1)

        sample_eval = [
            {"ml_revert_risk": 0.91, "is_reverted_actual": True},
            {"ml_revert_risk": 0.72, "is_reverted_actual": True},
            {"ml_revert_risk": 0.25, "is_reverted_actual": False},
            {"ml_revert_risk": 0.85, "is_reverted_actual": False},
        ]
        curve = evaluate_threshold_curve(sample_eval, "ml_revert_risk", [0.50, 0.80])
        self.assertEqual(len(curve), 2)
        self.assertEqual(curve[0]["recall_pct"], 100.0)
        self.assertEqual(curve[1]["tp"], 1)
        self.assertEqual(curve[1]["fp"], 1)

    def test_dashboard_queries_supports_milestone5_alert_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            curated = root / "curated"
            ts_start = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc).replace(tzinfo=None)
            ts_end = datetime(2026, 10, 15, 12, 5, tzinfo=timezone.utc).replace(tzinfo=None)
            rows = [
                (
                    "alt_test1234567890ab",
                    ts_start,
                    ts_end,
                    "5m",
                    date(2026, 10, 15),
                    1001,
                    0,
                    "Example Alert Article",
                    "CONFIRMED_NEWS",
                    True,
                    92.5,
                    True,
                    88.0,
                    True,
                    14,
                    8,
                    12500,
                    11000,
                    0.0,
                    45.2,
                    4.5,
                    True,
                    ["Velocity surge", "Editor arrival"],
                    {
                        "edit_velocity": 35.0,
                        "editor_diversity": 25.0,
                        "byte_churn": 18.0,
                        "consensus_or_dispute": 8.5,
                        "pageview_corroboration": 6.0,
                    },
                    True,
                    False,
                )
            ]
            df = self.spark.createDataFrame(rows, schema=ALERT_SCHEMA)
            df.write.parquet(str(curated / "alerts"))

            queries = DashboardQueries(
                self.spark,
                DashboardPaths(raw=root / "raw", live=root / "live", history=root / "history", curated=curated),
            )
            alerts_5m = queries.alerts("5m")
            self.assertEqual(len(alerts_5m), 1)
            self.assertEqual(alerts_5m[0]["title"], "Example Alert Article")
            self.assertEqual(alerts_5m[0]["activity_points"], 35.0)
            self.assertEqual(alerts_5m[0]["confidence"], "corroborated")
            self.assertEqual(queries.alerts("1h"), [])


if __name__ == "__main__":
    unittest.main()
