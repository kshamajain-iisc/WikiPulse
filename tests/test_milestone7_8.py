"""Unit and integration tests for Milestone 7 (Scaling, Load & Recovery) and Milestone 8 (System UI & Backup Demo)."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from plotly.utils import PlotlyJSONEncoder

from dashboards import charts
from dashboards.data_service import DashboardDataService
from dashboards.pages import system as system_page
from src.config.settings import CURATED_TABLES
from src.dashboard_queries import DashboardPaths


class Milestone7And8Tests(unittest.TestCase):
    def test_scaling_and_load_charts_handle_empty_and_populated_inputs(self):
        empty_scale = charts.scaling_curve([])
        self.assertEqual(len(empty_scale.data), 0)

        pop_scale = charts.scaling_curve([
            {"workers_cores": 1, "runtime_sec": 6.4, "speedup_vs_1core": 1.0},
            {"workers_cores": 2, "runtime_sec": 3.8, "speedup_vs_1core": 1.68},
            {"workers_cores": 4, "runtime_sec": 2.4, "speedup_vs_1core": 2.67},
            {"workers_cores": 6, "runtime_sec": 1.9, "speedup_vs_1core": 3.37},
        ])
        self.assertEqual(len(pop_scale.data), 2)

        empty_load = charts.recovery_and_load([])
        self.assertEqual(len(empty_load.data), 0)

        pop_load = charts.recovery_and_load([
            {
                "replay_speed_multiplier": "1x",
                "target_input_rate_rows_per_sec": 75.0,
                "measured_processing_rate_rows_per_sec": 8500.0,
            },
            {
                "replay_speed_multiplier": "100x",
                "target_input_rate_rows_per_sec": 7500.0,
                "measured_processing_rate_rows_per_sec": 12400.0,
            },
        ])
        self.assertEqual(len(pop_load.data), 2)

    def test_system_health_maps_ingestion_summary_and_milestone7_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bench_dir = root / "curated" / "benchmarks"
            bench_dir.mkdir(parents=True, exist_ok=True)

            (bench_dir / "ingestion_summary.json").write_text(json.dumps({
                "total_raw_events": 14398713,
                "valid_non_categorize_raw_events": 8428021,
                "curated_deduplicated_events": 8428021,
                "distinct_days": 46,
            }))
            (bench_dir / "milestone7_scaling_benchmarks.json").write_text(json.dumps({
                "experiment_4a_strong_scaling": [
                    {"workers_cores": 1, "runtime_sec": 5.0, "speedup_vs_1core": 1.0},
                    {"workers_cores": 6, "runtime_sec": 2.0, "speedup_vs_1core": 2.5},
                ],
                "experiment_4c_shuffle_partition_sweep": [
                    {"shuffle_partitions": 16, "runtime_sec": 2.0},
                ],
            }))
            (bench_dir / "milestone7_load_recovery.json").write_text(json.dumps({
                "load_and_window_latency_benchmark": {
                    "load_tier_evaluations": [
                        {
                            "replay_speed_multiplier": "1x",
                            "target_input_rate_rows_per_sec": 75.0,
                            "measured_processing_rate_rows_per_sec": 9200.0,
                        }
                    ]
                },
                "controlled_stop_restart_recovery_experiment": {
                    "pre_stop_events_processed": 95000,
                    "pre_stop_alerts_emitted": 18,
                    "backlog_events_accumulated_during_outage": 120000,
                    "backlog_recovery_drain_sec": 1.45,
                    "total_unique_day_events": 191829,
                    "final_backlog_after_recovery": 0,
                    "post_restart_distinct_alerts": 39,
                    "duplicate_alerts_after_restart": 0,
                },
            }))

            queries = Mock()
            queries.paths = DashboardPaths(
                raw=root / "raw",
                live=root / "live",
                history=root / "curated",
                curated=root / "curated",
                runtime=root / "run",
            )
            queries.quality.return_value = {}

            service = DashboardDataService(queries)
            health = service.system_health()
            self.assertEqual(health["quality"]["raw_records"], 14398713)
            self.assertEqual(health["quality"]["valid_records"], 8428021)
            self.assertEqual(health["quality"]["invalid_records"], 14398713 - 8428021)
            self.assertEqual(len(health["strong_scaling"]), 2)
            self.assertEqual(len(health["load_tiers"]), 1)
            self.assertEqual(len(health["recovery_summary"]), 3)
            self.assertEqual(health["lag"], 0)

            rendered = system_page.render(health)
            serialized = json.dumps(rendered, cls=PlotlyJSONEncoder)
            self.assertIn("Strong scaling & parallel speedup", serialized)
            self.assertIn("Controlled stop/restart & backlog recovery", serialized)
            self.assertNotIn("NaN", serialized)

    def test_curated_milestone7_and_8_benchmark_artifacts_when_present(self):
        bench_dir = CURATED_TABLES["benchmarks"]
        scale_path = bench_dir / "milestone7_scaling_benchmarks.json"
        if scale_path.is_file():
            scale = json.loads(scale_path.read_text())
            strong = scale["experiment_4a_strong_scaling"]
            self.assertEqual(len(strong), 4)
            for row in strong:
                for req_key in ("runtime_sec", "shuffle_write_mb", "peak_memory_mb", "input_storage_mb", "output_storage_mb"):
                    self.assertIn(req_key, row)
            self.assertTrue(scale["summary_findings"]["all_experiments_report_runtime_shuffle_memory_storage"])

        rec_path = bench_dir / "milestone7_load_recovery.json"
        if rec_path.is_file():
            rec = json.loads(rec_path.read_text())
            self.assertTrue(rec["performance_metric_2_no_duplicate_alerts_after_restart"])
            self.assertTrue(rec["performance_metric_3_closed_5m_window_under_60s"])
            self.assertTrue(rec["performance_metric_4_processing_rate_ge_input_rate"])
            self.assertTrue(rec["performance_metric_4_backlog_returns_to_zero"])
            self.assertEqual(rec["controlled_stop_restart_recovery_experiment"]["duplicate_alerts_after_restart"], 0)
            self.assertEqual(rec["controlled_stop_restart_recovery_experiment"]["final_backlog_after_recovery"], 0)

        demo_path = bench_dir / "milestone8_case_studies_demo.json"
        if demo_path.is_file():
            demo = json.loads(demo_path.read_text())
            self.assertEqual(demo["case_studies_count"], 4)
            self.assertEqual(len({cs["title"] for cs in demo["case_studies"]}), 4)
            for cs in demo["case_studies"]:
                self.assertGreaterEqual(len(cs["reasons"]), 2)


if __name__ == "__main__":
    unittest.main()

