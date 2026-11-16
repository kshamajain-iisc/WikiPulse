import unittest
from unittest.mock import Mock

from dashboards import contextual_charts as ctx
from dashboards.data_service import DashboardDataService

CURVE = [
    {"threshold": t, "tp": tp, "fp": fp, "fn": fn, "tn": tn, "precision_pct": p, "recall_pct": r, "f1_pct": f,
     "false_positive_rate_pct": fpr}
    for t, tp, fp, fn, tn, p, r, f, fpr in (
        (0.5, 50, 41, 4, 16, 54.9, 92.6, 69.0, 71.9),
        (0.65, 48, 28, 6, 29, 63.2, 88.9, 73.9, 49.1),
        (0.8, 41, 12, 13, 45, 77.4, 75.9, 76.6, 21.1),
    )
]
M6 = {"experiment_5_threshold_calibration": {
    "liftwing_ml_curve": CURVE, "composite_risk_curve": CURVE, "heuristic_risk_curve": CURVE,
    "optimal_composite_threshold": CURVE[1], "optimal_ml_threshold": CURVE[2],
}}
EDGES = [
    {"source_title": "A", "target_title": "X", "edge_type": "outlink", "co_edited_in_window": True,
     "target_edits_in_window": 9, "target_distinct_editors": 3},
    {"source_title": "A", "target_title": "Y", "edge_type": "backlink", "co_edited_in_window": False},
    {"source_title": "B", "target_title": "X", "edge_type": "outlink", "co_edited_in_window": True,
     "target_edits_in_window": 4, "target_distinct_editors": 2},
]
RISKS = [
    {"title": "P", "ml_revert_risk": 0.9, "heuristic_risk": 0.5, "composite_risk": 0.85, "is_reverted_actual": True},
    {"title": "Q", "ml_revert_risk": 0.3, "heuristic_risk": 0.2, "composite_risk": 0.27, "is_reverted_actual": False},
    {"title": "R", "ml_revert_risk": None, "heuristic_risk": 0.6, "composite_risk": 0.60, "is_reverted_actual": True},
]


class ContextualChartsTest(unittest.TestCase):
    def test_empty_inputs_render_empty_states(self):
        for figure in (
            ctx.spillover_hub_network([]),
            ctx.spillover_ratio_bars([]),
            ctx.country_topic_bars([]),
            ctx.revision_diff_profile([]),
            ctx.revert_risk_f1_by_threshold({}),
            ctx.revert_risk_band_outcomes([]),
        ):
            self.assertEqual(len(figure.data), 0)
            self.assertTrue(figure.layout.annotations)

    def test_spillover_network_draws_up_to_300_edges_and_marks_bridges(self):
        figure = ctx.spillover_hub_network(EDGES, max_edges=300)
        names = [trace.name for trace in figure.data]
        self.assertIn("Bridge (linked by ≥2 alerts)", names)
        self.assertIn("Alerted page (hub)", names)
        bars = ctx.spillover_ratio_bars(EDGES)
        self.assertEqual([t.name for t in bars.data], ["Co-edited neighbours", "Link only"])

        many_edges = [
            {"source_title": f"Hub{i % 10}", "target_title": f"Target{i}", "edge_type": "outlink",
             "co_edited_in_window": i % 3 == 0, "target_edits_in_window": i % 5}
            for i in range(350)
        ]
        net300 = ctx.spillover_hub_network(many_edges, max_edges=300)
        # 300 edges -> 300 * 3 coords (x0, x1, None) across the two line traces
        line_coords = sum(len(t.x) for t in net300.data if t.mode == "lines")
        self.assertEqual(line_coords, 300 * 3)

    def test_simplified_revert_risk_and_geo_charts(self):
        f1_fig = ctx.revert_risk_f1_by_threshold(M6)
        self.assertEqual(len([t for t in f1_fig.data if t.mode == "lines+markers"]), 3)

        bands_fig = ctx.revert_risk_band_outcomes(RISKS)
        self.assertEqual([t.name for t in bands_fig.data], ["Reverted ≤48h", "Kept"])
        self.assertEqual(sum(bands_fig.data[0].y) + sum(bands_fig.data[1].y), 3)

        countries = ctx.country_topic_bars([{"country_or_region": "India", "active_topics": 9,
                                             "total_edits_human": 187, "max_score": 93.8}])
        self.assertEqual(len(countries.data), 1)

    def test_contextual_features_loads_300_graph_edges(self):
        queries = Mock()
        queries.paths = None
        queries.historical_optional_records.return_value = []
        service = DashboardDataService(queries)
        service._load("contextual_features", "5m", None, None, None, None)
        queries.historical_optional_records.assert_any_call("graph_edges", limit=300)


if __name__ == "__main__":
    unittest.main()
