import unittest

from dashboards import contextual_charts as ctx

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
    {"title": "P", "ml_revert_risk": 0.9, "heuristic_risk": 0.5, "composite_risk": 0.79, "is_reverted_actual": True,
     "time_to_revert_sec": 66, "reverter_is_bot": False, "is_anon": True},
    {"title": "Q", "ml_revert_risk": 0.3, "heuristic_risk": 0.2, "composite_risk": 0.27, "is_reverted_actual": False},
    {"title": "R", "ml_revert_risk": None, "heuristic_risk": 0.6, "composite_risk": 0.6, "is_reverted_actual": True,
     "time_to_revert_sec": 1500, "reverter_is_bot": True},
]


class ContextualChartsTest(unittest.TestCase):
    def test_empty_inputs_render_empty_states(self):
        for figure in (ctx.spillover_hub_network([]), ctx.spillover_ratio_bars([]), ctx.revert_risk_pr_tradeoff({}),
                       ctx.revert_risk_f1_by_threshold({}), ctx.revert_risk_confusion({}),
                       ctx.revert_risk_separation([]), ctx.time_to_revert_ecdf([]), ctx.revision_diff_profile([]),
                       ctx.country_topic_bars([]), ctx.location_provenance_bars({}, {})):
            self.assertEqual(len(figure.data), 0)
            self.assertTrue(figure.layout.annotations)

    def test_spillover_network_marks_bridges_and_hubs(self):
        figure = ctx.spillover_hub_network(EDGES)
        names = [trace.name for trace in figure.data]
        self.assertIn("Bridge (linked by ≥2 alerts)", names)
        self.assertIn("Alerted page (hub)", names)
        bars = ctx.spillover_ratio_bars(EDGES)
        self.assertEqual([t.name for t in bars.data], ["Co-edited neighbours", "Link only"])

    def test_revert_risk_figures(self):
        self.assertGreaterEqual(len(ctx.revert_risk_pr_tradeoff(M6).data), 6)
        self.assertEqual(len([t for t in ctx.revert_risk_f1_by_threshold(M6).data if t.mode == "lines+markers"]), 3)
        self.assertEqual(len(ctx.revert_risk_confusion(M6).data), 2)
        sep = ctx.revert_risk_separation(RISKS, M6)
        self.assertIn("Composite boundary τ=0.65", [t.name for t in sep.data])
        self.assertIn("without a Lift Wing score", sep.layout.annotations[-1].text)

    def test_ttr_and_geo(self):
        ecdf = ctx.time_to_revert_ecdf(RISKS)
        self.assertEqual(ecdf.layout.xaxis.type, "log")
        self.assertEqual(len(ecdf.data), 2)
        countries = ctx.country_topic_bars([{"country_or_region": "India", "active_topics": 9,
                                             "total_edits_human": 187, "max_score": 93.8}])
        self.assertEqual(len(countries.data), 1)
        prov = ctx.location_provenance_bars({"wikipedia_coord": 2, "none": 1}, {"exact_point": 2, "unlocated": 1})
        self.assertEqual(len(prov.data), 4)


if __name__ == "__main__":
    unittest.main()
