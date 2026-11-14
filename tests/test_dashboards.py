"""UI routing, empty states, cache behavior, and the public Dash HTTP contract."""

import json
import unittest
from unittest.mock import Mock

from plotly.utils import PlotlyJSONEncoder

from dashboards.app import create_app
from dashboards.callbacks import PAGES, render_dashboard
from dashboards.charts import (
    alert_type_breakdown,
    anomaly_scatter,
    baseline_comparison,
    baseline_tier_distribution,
    churn_rankings,
    detector_comparison,
    false_alert_comparison_chart,
    geo_country_breakdown,
    historical_alerts_timeline,
    kmeans_clusters,
    m4_engine_and_latency,
    macro_region_distribution,
    mttr_comparison,
    recentchanges_regional_map,
    regional_edits_breakdown,
    regional_edits_timeline,
    relationship_graph,
    revert_risk_calibration,
    revert_risk_multi_curve,
    rf_feature_importances,
    score_component_breakdown,
    subject_map,
    synthetic_patterns_chart,
)
from dashboards.components import table
from dashboards.data_service import DashboardDataService


class DashboardPresentationTests(unittest.TestCase):
    def test_refresh_timer_waits_for_query_and_respects_pause(self):
        client = create_app(Mock()).server.test_client()
        for paused, busy, expected in (([], False, False), ([], True, True),
                                       (["pause"], True, True), (["pause"], False, True)):
            response = client.post("/_dash-update-component", json={
                "output": "refresh.disabled",
                "outputs": {"id": "refresh", "property": "disabled"},
                "inputs": [
                    {"id": "pause", "property": "value", "value": paused},
                    {"id": "refresh-busy", "property": "data", "value": busy},
                    {"id": "view", "property": "value", "value": "live"},
                    {"id": "location", "property": "pathname", "value": "/"},
                ],
                "state": [], "changedPropIds": ["refresh-busy.data"],
            })
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json["response"]["refresh"]["disabled"], expected)

    def setUp(self):
        self.service = Mock()
        self.service.load.return_value = {
            "metrics": {"edits": 4, "bots": 1, "pages": 2, "gross_bytes": 60, "known_sizes": 4},
            "timeline": [{"window_start": "2026-10-31T10:00:00Z", "day": "2026-10-31", "edits": 4, "human": 3, "bots": 1}],
            "rankings": [{"title": "Example", "edits": 4}], "latest": "2026-10-31T10:00:00Z",
        }

    def test_all_proposal_screens_render_without_optional_sources(self):
        for route in PAGES:
            with self.subTest(route=route):
                result = render_dashboard(self.service, route, "", "5m", "live", None, None, None)
                serialized = json.dumps(result, cls=PlotlyJSONEncoder)
                self.assertIn("children", serialized)
                self.assertNotIn("NaN", serialized)

    def test_article_url_reaches_the_backend(self):
        render_dashboard(self.service, "/investigation", "?title=Example%20article", "5m", "live", None, None, None)
        self.assertEqual(self.service.load.call_args.args[3], "Example article")

    def test_http_layout_health_and_callback(self):
        app = create_app(self.service)
        client = app.server.test_client()
        for route in (
            "/",
            "/historical",
            "/anomaly-scoring",
            "/ml-reports",
            "/contextual-features",
            "/geospatial-map",
            "/relationships",
            "/system",
            "/milestone4",
            "/milestone5",
            "/milestone6",
            "/_dash-layout",
            "/_dash-dependencies",
            "/assets/dashboard.css",
        ):
            response = client.get(route)
            self.assertEqual(response.status_code, 200, route)
            response.close()
        self.assertEqual(client.get("/healthz").json["status"], "ready")
        dependencies = client.get("/_dash-dependencies").json
        navigation_callback = next(item for item in dependencies if "location.href" in item["output"])
        ranking_input = next(item for item in navigation_callback["inputs"] if item["id"] == "page-ranking")
        self.assertTrue(ranking_input["allow_optional"])
        output_key = next(key for key in app.callback_map if "page-body.children" in key)
        values = ["/", "", 0, "5m", "live", None, "1m", 0, 1]
        callback = app.callback_map[output_key]
        inputs = [dict(spec, value=value) for spec, value in zip(callback["inputs"], values)]
        response = client.post("/_dash-update-component", json={
            "output": output_key,
            "outputs": [{"id": output.component_id, "property": output.component_property}
                        for output in callback["output"]],
            "inputs": inputs, "state": [], "changedPropIds": ["location.pathname"],
        })
        self.assertEqual(response.status_code, 200)
        self.assertIn("Wikipedia, in motion.", response.get_data(as_text=True))

    def test_every_chart_reserves_space_in_grid_cards(self):
        # The regression affected populated charts and empty-state figures alike.
        def graph_properties(node):
            if isinstance(node, dict):
                if node.get("type") == "Graph":
                    yield node["props"]
                for value in node.values():
                    yield from graph_properties(value)
            elif isinstance(node, list):
                for value in node:
                    yield from graph_properties(value)

        for route in PAGES:
            with self.subTest(route=route):
                result = render_dashboard(self.service, route, "", "5m", "live", None, None, None)
                rendered = json.loads(json.dumps(result, cls=PlotlyJSONEncoder))
                for props in graph_properties(rendered):
                    height = props["style"]["height"]
                    self.assertTrue(height.endswith("px"))
                    self.assertGreaterEqual(float(height[:-2]), 300)
                    self.assertEqual(props["style"]["minHeight"], height)
                    self.assertTrue(props["responsive"])

    def test_investigation_keeps_other_article_choices(self):
        self.service.load.return_value["articles"] = ["A", "B"]
        _, _, options, _ = render_dashboard(
            self.service, "/investigation", "?title=A", "5m", "live", None, None, None
        )
        self.assertEqual([option["value"] for option in options], ["A", "B"])

    def test_article_picker_navigates_with_one_complete_url(self):
        client = create_app(self.service).server.test_client()
        response = client.post("/_dash-update-component", json={
            "output": "location.href",
            "outputs": {"id": "location", "property": "href"},
            "inputs": [
                {"id": "page-ranking", "property": "clickData", "value": None},
                {"id": "article", "property": "value", "value": "Bola (tabloid)"},
            ],
            "state": [], "changedPropIds": ["article.value"],
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["response"]["location"]["href"],
                         "/investigation?title=Bola%20%28tabloid%29")

    def test_api_rejects_invalid_controls(self):
        client = create_app(self.service).server.test_client()
        self.assertEqual(client.get("/api/activity?view=unsupported").status_code, 400)
        self.assertEqual(client.get("/api/activity?duration=1h").status_code, 200)
        self.service.load.assert_called_with("live", "1h", "live")

    def test_figures_use_real_optional_data(self):
        edge = {"source_title": "A", "target_title": "B"}
        self.assertEqual(len(relationship_graph([edge]).data), 2)
        co_edge = {"source_title": "A", "target_title": "C", "co_edited_in_window": True}
        self.assertEqual(len(relationship_graph([edge, co_edge]).data), 3)
        self.assertEqual(len(subject_map([]).data), 0)
        self.assertEqual(len(baseline_comparison([]).data), 0)
        band_fig = baseline_comparison([{
            "window_start": "2026-10-31T10:00:00Z", "edits_human": 10,
            "avg_edits_human": 2.0, "std_edits_human": 1.5,
        }])
        self.assertEqual(len(band_fig.data), 3)
        self.assertEqual(len(churn_rankings([{"title": "A", "gross_bytes": 500, "rewriting_bytes": 200}]).data), 2)
        self.assertEqual(len(detector_comparison({"detector_a_fixed_count": 3, "detector_b_historical_count": 5}).data), 1)
        self.assertEqual(len(revert_risk_calibration([{"threshold": 0.8, "precision_pct": 85.7, "recall_pct": 67.9, "f1_pct": 75.8}]).data), 3)
        self.assertEqual(len(m4_engine_and_latency({"query_latency_benchmark": {"stored_parquet_rankings_top20_sec": 0.22}}).data), 1)
        self.assertEqual(len(rf_feature_importances({"f_distinct_editors": 0.35, "f_edits_human": 0.22}).data), 1)
        self.assertEqual(len(kmeans_clusters({"1": 40554, "3": 528}, normal_cluster_id=1).data), 1)
        self.assertEqual(len(geo_country_breakdown([{"country_or_region": "United States", "active_topics": 63}]).data), 1)
        self.assertEqual(len(alert_type_breakdown({"alert_type_breakdown": {"CONFIRMED_NEWS": 90, "EDIT_WAR_CONTROVERSY": 2125}}).data), 1)
        self.assertEqual(len(score_component_breakdown([{"title": "A", "feature_contributions": {"edit_velocity": 35.0, "editor_diversity": 25.0}}]).data), 5)
        self.assertEqual(len(anomaly_scatter([{"title": "A", "velocity_zscore": 120.0, "historical_score": 95.0, "alert_type": "CONFIRMED_NEWS"}]).data), 1)
        self.assertEqual(len(false_alert_comparison_chart([{"detector": "Detector B: Historical", "precision_pct": 89.28, "false_alert_rate_pct": 10.72}]).data), 2)
        self.assertEqual(len(synthetic_patterns_chart([{"pattern_title": "SYNTH_BREAKING_NEWS_SURGE", "historical_score": 96.3, "expected_positive": True}]).data), 1)
        self.assertEqual(len(revert_risk_multi_curve({"experiment_5_threshold_calibration": {"composite_risk_curve": [{"threshold": 0.65, "f1_pct": 76.92}]}}).data), 1)
        self.assertEqual(len(mttr_comparison({"overall": {"count": 60, "mean_sec": 1388.9, "median_sec": 66.5}}).data), 2)
        self.assertEqual(len(historical_alerts_timeline([{"day": "2026-10-15", "confirmed_news": 2, "breaking_news": 5, "edit_war": 8, "major_rewrite": 1}]).data), 4)
        self.assertEqual(len(baseline_tier_distribution([{"activity_tier": "quiet", "pages_count": 1500000}]).data), 1)
        self.assertGreaterEqual(len(recentchanges_regional_map(
            [{"country_or_region": "United States", "macro_region": "North America", "total_edits": 1200, "human_edits": 1000, "bot_edits": 200, "reverts": 15, "active_articles": 40, "top_article": "New York City"}],
            [{"title": "New York City", "country_or_region": "United States", "macro_region": "North America", "lat": 40.71, "lon": -74.00, "edits": 300, "human": 280, "bots": 20, "reverts": 5, "gross_bytes": 12000}],
        ).data), 2)
        self.assertEqual(len(regional_edits_breakdown([{"country_or_region": "India", "total_edits": 800, "human_edits": 700, "bot_edits": 100}]).data), 2)
        self.assertEqual(len(regional_edits_timeline([{"bucket": "2026-11-04", "edits": 500, "human": 450, "bots": 50, "reverts": 12}], mode="days").data), 3)
        self.assertEqual(len(macro_region_distribution([{"macro_region": "Europe", "total_edits": 900, "active_articles": 45}]).data), 2)

    def test_table_formats_booleans_and_accessibility_attrs(self):
        region = table([{"title": "Alpha", "flag": True, "count": 12}], [("Title", "title"), ("Flag", "flag"), ("Count", "count")], aria_label="Test table")
        serialized = json.dumps(region, cls=PlotlyJSONEncoder)
        self.assertIn('"Yes"', serialized)
        self.assertIn('"12"', serialized)
        self.assertIn('"Test table"', serialized)

    def test_cache_does_not_repeat_the_same_spark_query(self):
        queries = Mock()
        queries.activity.return_value = {"metrics": {}, "rankings": []}
        queries.optional_records.return_value = []
        service = DashboardDataService(queries)
        first = service.load("live")
        second = service.load("live")
        self.assertEqual(first, second)
        queries.activity.assert_called_once()

    def test_new_anomaly_ml_contextual_and_enriched_historical_pages_render_populated_data(self):
        queries = Mock()
        queries.paths = None
        queries.alerts.return_value = [
            {
                "alert_id": "alt_1",
                "title": "Navi Pillay",
                "window_start": "2026-10-09T14:30:00Z",
                "window_duration": "1h",
                "alert_type": "CONFIRMED_NEWS",
                "historical_score": 98.5,
                "spark_ml_score": 95.4,
                "fixed_threshold_flag": True,
                "pageview_corroborated": True,
                "edits_human": 34,
                "distinct_human_editors": 24,
                "revert_ratio": 0.0,
                "velocity_zscore": 226.6,
                "feature_contributions": {
                    "edit_velocity": 35.0,
                    "editor_diversity": 25.0,
                    "byte_churn": 20.0,
                    "consensus_or_dispute": 8.5,
                    "pageview_corroboration": 10.0,
                },
                "reasons": ["Edit velocity surge", "Contributor arrival"],
            }
        ]
        queries.historical_alerts.return_value = queries.alerts.return_value
        queries.optional_records.return_value = []
        queries.historical_optional_records.return_value = []
        queries.geospatial_recentchanges.return_value = {
            "mode": "days",
            "time_range": "55d",
            "time_label": "Full 55-Day Corpus (2026-09-15 – 2026-11-08)",
            "selected_region": "ALL",
            "metrics": {
                "total_edits": 15000,
                "human_edits": 13500,
                "bot_edits": 1500,
                "reverts": 420,
                "gross_bytes": 1250000,
                "located_articles": 210,
                "active_countries": 38,
                "top_region": "United States",
            },
            "country_rollup": [
                {
                    "country_or_region": "United States",
                    "macro_region": "North America",
                    "total_edits": 5400,
                    "human_edits": 4900,
                    "bot_edits": 500,
                    "reverts": 120,
                    "gross_bytes": 450000,
                    "active_articles": 65,
                    "top_article": "2026 New York City mayoral election",
                    "top_article_edits": 420,
                }
            ],
            "macro_regions": [
                {
                    "macro_region": "North America",
                    "total_edits": 6200,
                    "human_edits": 5600,
                    "bot_edits": 600,
                    "reverts": 140,
                    "active_articles": 78,
                    "countries_count": 3,
                }
            ],
            "map_articles": [
                {
                    "title": "2026 New York City mayoral election",
                    "country_or_region": "United States",
                    "macro_region": "North America",
                    "lat": 40.7128,
                    "lon": -74.006,
                    "location_source": "wikidata_P625",
                    "location_quality": "exact_article_coord",
                    "edits": 420,
                    "human": 405,
                    "bots": 15,
                    "reverts": 18,
                    "gross_bytes": 82000,
                    "peak_editors": 64,
                }
            ],
            "timeline": [
                {"bucket": "2026-11-04", "edits": 1200, "human": 1100, "bots": 100, "reverts": 35, "gross_bytes": 95000}
            ],
            "available_regions": ["United States", "India", "United Kingdom"],
            "available_macro_regions": ["North America", "Asia-Pacific", "Europe"],
        }
        queries.historical.return_value = {
            "metrics": {"edits": 1000, "bots": 200, "pages": 150, "gross_bytes": 50000,
                        "first_day": "2026-10-01", "last_day": "2026-10-30", "observed_days": 30},
            "timeline": [{"day": "2026-10-15", "edits": 1000, "bots": 200, "gross_bytes": 50000, "net_bytes": 12000}],
            "rankings": [{
                "title": "Navi Pillay", "edits": 34, "human": 34, "bots": 0,
                "gross_bytes": 6534, "rewriting_bytes": 5048, "net_bytes": 1486,
                "peak_editors": 24, "detected_shifts": 2, "peak_anomaly_score": 98.5,
                "peak_ml_score": 95.4, "subject_location": "South Africa",
            }],
            "baseline": [{"title": "Navi Pillay", "activity_tier": "moderate", "avg_daily_edits": 1.5}],
            "daily_alerts": [{"day": "2026-10-09", "confirmed_news": 1, "breaking_news": 2, "edit_war": 3, "major_rewrite": 0}],
            "baseline_tiers": [{"activity_tier": "quiet", "pages_count": 1500000, "mean_daily_edits": 0.2, "mean_revert_pct": 1.2, "mean_bot_pct": 10.0}],
        }
        service = DashboardDataService(queries)
        for route, expected_heading in (
            ("/anomaly-scoring", "Explainable knowledge-shift scoring."),
            ("/ml-reports", "Spark ML introspection, synthetic injection & false-alert audits."),
            ("/contextual-features", "Topic spillover, subject geography & Lift Wing Revert Risk."),
            ("/geospatial-map", "Regional edits across days and windows."),
            ("/historical", "Historical anomaly timeline & shift density"),
        ):
            rendered = render_dashboard(service, route, "", "5m", "finalized", None, "1m", None)
            serialized = json.dumps(rendered, cls=PlotlyJSONEncoder)
            self.assertIn(expected_heading, serialized)
            self.assertIn("HISTORICAL CURATED LAKE", serialized)
            self.assertNotIn("NaN", serialized)

    def test_geo_map_click_and_backend_loading_icon_callbacks(self):
        client = create_app(self.service).server.test_client()
        # Verify backend-loading-icon toggles active class when refresh-busy is True
        loading_key = next(k for k in create_app(self.service).callback_map if "backend-loading-icon.className" in k)
        resp_busy = client.post("/_dash-update-component", json={
            "output": loading_key,
            "outputs": [
                {"id": "loading-banner", "property": "style"},
                {"id": "loading-title", "property": "children"},
                {"id": "loading-description", "property": "children"},
                {"id": "backend-loading-icon", "property": "className"},
            ],
            "inputs": [
                {"id": "refresh-busy", "property": "data", "value": True},
                {"id": "location", "property": "pathname", "value": "/geospatial-map"},
                {"id": "view", "property": "value", "value": "finalized"},
            ],
            "state": [], "changedPropIds": ["refresh-busy.data"],
        })
        self.assertEqual(resp_busy.status_code, 200)
        self.assertEqual(
            resp_busy.json["response"]["backend-loading-icon"]["className"],
            "backend-loading-pill active",
        )

        # Verify clicking a country polygon on geo-regional-map updates geo-region to that country
        geo_key = next(k for k in create_app(self.service).callback_map if "geo-map-content.children" in k)
        resp_map = client.post("/_dash-update-component", json={
            "output": geo_key,
            "outputs": [
                {"id": "geo-map-content", "property": "children"},
                {"id": "geo-region", "property": "value"},
            ],
            "inputs": [
                {"id": "geo-mode", "property": "value", "value": "days"},
                {"id": "geo-range", "property": "value", "value": "55d"},
                {"id": "geo-region", "property": "value", "value": "ALL"},
                {"id": "geo-projection", "property": "value", "value": "orthographic"},
                {"id": "geo-metric", "property": "value", "value": "total_edits"},
                {"id": "geo-regional-map", "property": "clickData", "value": {"points": [{"customdata": "COUNTRY|India"}]}},
                {"id": "geo-reset-btn", "property": "n_clicks", "value": 0},
            ],
            "state": [], "changedPropIds": ["geo-regional-map.clickData"],
        })
        self.assertEqual(resp_map.status_code, 200)
        self.assertEqual(resp_map.json["response"]["geo-region"]["value"], "India")


if __name__ == "__main__":
    unittest.main()
