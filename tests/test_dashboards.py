"""UI routing, empty states, cache behavior, and the public Dash HTTP contract."""

import json
import unittest
from unittest.mock import Mock

from plotly.utils import PlotlyJSONEncoder

from dashboards.app import create_app
from dashboards.callbacks import PAGES, render_dashboard
from dashboards.charts import baseline_comparison, relationship_graph, subject_map
from dashboards.data_service import DashboardDataService


class DashboardPresentationTests(unittest.TestCase):
    def test_refresh_timer_waits_for_query_and_respects_pause(self):
        client = create_app(Mock()).server.test_client()
        for paused, busy, expected in (([], False, False), ([], True, True),
                                       (["pause"], True, True), (["pause"], False, True)):
            response = client.post("/_dash-update-component", json={
                "output": "refresh.disabled",
                "outputs": {"id": "refresh", "property": "disabled"},
                "inputs": [{"id": "pause", "property": "value", "value": paused},
                           {"id": "refresh-busy", "property": "data", "value": busy}],
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
        for route in ("/", "/historical", "/relationships", "/system", "/_dash-layout", "/_dash-dependencies", "/assets/dashboard.css"):
            response = client.get(route)
            self.assertEqual(response.status_code, 200, route)
            response.close()
        self.assertEqual(client.get("/healthz").json["status"], "ready")
        dependencies = client.get("/_dash-dependencies").json
        navigation_callback = next(item for item in dependencies if "location.href" in item["output"])
        ranking_input = next(item for item in navigation_callback["inputs"] if item["id"] == "page-ranking")
        self.assertTrue(ranking_input["allow_optional"])
        output_key = next(key for key in app.callback_map if "page-body.children" in key)
        values = ["/", "", 0, "5m", "live", None, None, None]
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
        self.assertEqual(len(subject_map([]).data), 0)
        self.assertEqual(len(baseline_comparison([]).data), 0)

    def test_cache_does_not_repeat_the_same_spark_query(self):
        queries = Mock()
        queries.activity.return_value = {"metrics": {}, "rankings": []}
        queries.optional_records.return_value = []
        service = DashboardDataService(queries)
        first = service.load("live")
        second = service.load("live")
        self.assertEqual(first, second)
        queries.activity.assert_called_once()


if __name__ == "__main__":
    unittest.main()
