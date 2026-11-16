"""Activity serving, progress and source boundaries without Spark or a broker."""

import json
import re
import subprocess
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from dashboards.activity_data import ActivityReader, day_bounds, normalize_spec
from dashboards.activity_jobs import ActivityWorkspace, TERMINAL
from dashboards.activity_ui import activity_url, render_core, activity_shell
from dashboards.app import create_app
from src.stream_control import committed_offset, file_stream_drained, request_action, atomic_json, stop_and_drain

UTC = timezone.utc


def event(rcid, ts, title="Example", page=1, **kwargs):
    return {"rcid": rcid, "ts": datetime.fromisoformat(ts), "pageid": page, "title": title,
            "ns": 0, "type": "edit", "is_bot": False, "oldlen": 100, "newlen": 110,
            "user": "Editor", "is_revert_action": False, **kwargs}


class ActivityReaderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.paths = SimpleNamespace(raw=root / "raw", curated=root / "curated", history=root / "history",
                                     live=root / "live", runtime=root / "run")
        self.reader = ActivityReader(self.paths)

    def write(self, root, rows):
        root.mkdir(parents=True, exist_ok=True)
        path = root / "part.parquet"
        pq.write_table(pa.Table.from_pylist(rows), path)
        return path

    def read(self, **overrides):
        spec = normalize_spec({"mode": "stored", "day": "2026-11-01", **overrides})
        progress = []
        result = self.reader.read(spec, lambda *a, **k: progress.append((a, k)), lambda: False)
        return result, progress

    def test_half_open_ist_day_and_deduplication(self):
        self.write(self.paths.curated / "rc_events/day=2026-10-31", [
            event(1, "2026-10-31T18:29:59"), event(2, "2026-10-31T18:30:00"),
            event(2, "2026-10-31T18:30:00")])
        self.write(self.paths.curated / "rc_events/day=2026-11-01", [
            event(3, "2026-11-01T18:29:59", is_bot=True), event(4, "2026-11-01T18:30:00")])
        data, progress = self.read()
        self.assertEqual(data["metrics"]["edits"], 2)
        self.assertEqual(data["metrics"]["bots"], 1)
        self.assertEqual(data["metrics"]["gross_bytes"], 20)
        self.assertEqual(data["range"]["start"], "2026-10-31T18:30:00Z")
        self.assertTrue(any(m.get("duplicates") == 1 for _, m in progress))
        self.assertTrue(any(m.get("arrow_payload_bytes", 0) > 0 for _, m in progress))

    def test_full_totals_before_top_100_and_article_filter(self):
        self.write(self.paths.curated / "rc_events/day=2026-11-01", [
            event(i, "2026-11-01T12:00:00", title=f"Article {i}", page=i) for i in range(1, 151)])
        data, _ = self.read(zone="UTC")
        self.assertEqual(data["metrics"]["edits"], 150)
        self.assertEqual(data["metrics"]["pages"], 150)
        self.assertEqual(len(data["rankings"]), 100)
        article, _ = self.read(page="investigation", title="Article 149", zone="UTC")
        self.assertEqual(article["metrics"]["edits"], 1)
        self.assertEqual(article["selected"], "Article 149")

    def test_replay_rolling_and_cumulative_and_no_auto_article_switch(self):
        self.write(self.paths.curated / "rc_events/day=2026-11-01", [
            event(1, "2026-11-01T00:01:00"), event(2, "2026-11-01T01:01:00"),
            event(3, "2026-11-01T01:02:00", title="Other", page=2)])
        data, _ = self.read(kind="replay", zone="UTC", cursor=3900, page="investigation", title="Example")
        self.assertEqual(data["metrics"]["edits"], 1)
        self.assertEqual(data["day_so_far"], 2)
        self.assertEqual(data["selected"], "Example")
        empty, _ = self.read(kind="replay", zone="UTC", cursor=3900, page="investigation")
        self.assertEqual(empty["metrics"]["edits"], 0)
        self.assertEqual(empty["day_so_far"], 0)
        self.assertIn("Choose an article", empty["empty_message"])

    def test_cold_article_read_filters_before_materializing(self):
        self.write(self.paths.curated / "rc_events/day=2026-11-01", [
            event(1, "2026-11-01T12:00:00"), event(2, "2026-11-01T12:00:01", title="Other", page=2)])
        data, progress = self.read(zone="UTC", page="investigation", title="Example")
        self.assertEqual(data["metrics"]["edits"], 1)
        reads = [m for a, m in progress if a[0] == "READ"]
        self.assertEqual(reads[-1]["rows_fetched"], 1)

    def test_day_index_reused_but_invalidated_after_data_commit(self):
        root = self.paths.curated / "rc_events/day=2026-11-01"
        self.write(root, [event(1, "2026-11-01T12:00:00")])
        self.read(zone="UTC")
        _, progress = self.read(zone="UTC")
        self.assertTrue(any(a[0] == "CACHE" for a, _ in progress))
        self.write(root, [event(1, "2026-11-01T12:00:00"), event(2, "2026-11-01T13:00:00")])
        data, _ = self.read(zone="UTC")
        self.assertEqual(data["metrics"]["edits"], 2)

    def test_only_committed_bronze_files_and_overlap_deduplication(self):
        root = self.paths.raw / "ingestion_date=2026-11-01"
        row = {"event_time": datetime(2026, 11, 1, 12), "event_id": "enwiki:1", "change_id": 1,
               "page_id": 1, "page_title": "Example", "is_bot": False, "namespace": 0,
               "change_type": "edit", "old_length": 100, "new_length": 110, "wiki": "enwiki",
               "raw_event_json": '{"user":"Editor"}'}
        committed = self.write(root, [row])
        pq.write_table(pa.Table.from_pylist([{**row, "event_id": "enwiki:2", "change_id": 2}]), root / "uncommitted.parquet")
        metadata = self.paths.raw / "_spark_metadata"
        metadata.mkdir()
        (metadata / "0").write_text('v1\n' + json.dumps({"path": committed.as_uri(), "action": "add"}))
        self.write(self.paths.curated / "rc_events/day=2026-11-01", [event(1, "2026-11-01T12:00:00")])
        data, _ = self.read(zone="UTC")
        self.assertEqual(data["metrics"]["edits"], 1)
        self.assertEqual(self.reader.files(self.paths.raw), [committed.resolve()])

    def test_optional_evidence_failure_does_not_fail_activity(self):
        self.write(self.paths.curated / "rc_events/day=2026-11-01", [event(1, "2026-11-01T12:00:00")])
        data, _ = self.read(zone="UTC", page="investigation", title="Example")
        self.write(self.paths.curated / "page_baselines", [{"title": "Example", "avg_hourly_edits": 2.0,
                                                           "std_hourly_edits": 1.0}])
        result = self.reader.evidence(normalize_spec({"page": "investigation", "title": "Example"}), data,
                                      lambda *a, **k: None, lambda: False)
        self.assertEqual(len(result["comparisons"]), 1)
        self.assertEqual(result["risks"], [])

    def test_scope_validation_and_utc_duration(self):
        start, end = day_bounds("2026-11-01", "UTC")
        self.assertEqual(end-start, 86400)
        for spec in ({"page": "historical"}, {"cursor": float("nan")}, {"mode": "stored", "day": "invalid"}):
            with self.assertRaises(ValueError):
                normalize_spec(spec)

    def test_article_search_after_cold_investigation_finds_other_articles(self):
        self.write(self.paths.curated / "rc_events/day=2026-11-01", [
            event(1, "2026-11-01T12:00:00"), event(2, "2026-11-01T12:01:00", title="Other article", page=2),
            event(3, "2026-11-01T12:02:00", title="Excluded", page=3, ns=1)])
        self.read(zone="UTC", page="investigation", title="Example")
        titles = self.reader.search_titles({"mode": "stored", "day": "2026-11-01", "zone": "UTC", "horizon": "5m"}, "OTHER")
        self.assertEqual(titles, ["Other article"])

    def test_day_index_hit_reports_zero_fresh_bytes_and_rows(self):
        self.write(self.paths.curated / "rc_events/day=2026-11-01", [event(1, "2026-11-01T12:00:00")])
        self.read(zone="UTC")
        _, progress = self.read(zone="UTC", kind="replay", cursor=45000)
        metric = next(m for args, m in progress if args[0] == "CACHE")
        self.assertEqual(metric["rows_fetched"], 0)
        self.assertEqual(metric["arrow_payload_bytes"], 0)

    def test_article_first_capture_is_correct_when_reusing_full_day_index(self):
        self.write(self.paths.curated / "rc_events/day=2026-11-01", [
            event(1, "2026-11-01T01:00:00", title="Other", page=2),
            event(2, "2026-11-01T02:00:00", title="Example")])
        self.read(zone="UTC")
        article, _ = self.read(zone="UTC", page="investigation", title="Example", kind="replay", cursor=0)
        self.assertEqual(article["first_cursor"], 7201)
        self.assertEqual(article["available_events"], 1)
        empty, _ = self.read(zone="UTC", page="investigation", title="Missing", kind="replay", cursor=0)
        self.assertEqual(empty["available_events"], 0)

    def test_console_source_reports_only_sources_actually_selected(self):
        self.write(self.paths.curated / "rc_events/day=2026-11-01", [event(1, "2026-11-01T12:00:00")])
        _, progress = self.read(zone="UTC")
        source = next(args[1] for args, metric in progress if args[0] == "SOURCE")
        self.assertIn("Saved edit history", source)
        self.assertNotIn("Streamed edits", source)

    def test_since_capture_is_discoverable_by_event_day_despite_later_ingestion(self):
        self.write(self.paths.raw / "ingestion_date=2026-11-16", [{
            "event_time": datetime(2026, 11, 1, 12), "event_id": "enwiki:123", "change_id": 123, "wiki": "enwiki",
            "namespace": 0, "change_type": "edit", "page_id": 123, "page_title": "Backfilled article",
            "is_bot": False, "old_length": 100, "new_length": 110}])
        atomic_json(self.paths.runtime / "capture-days.json", {"2026-11-01": ["2026-11-16"]})
        data, _ = self.read(zone="UTC")
        self.assertEqual(data["metrics"]["edits"], 1)
        self.assertEqual(data["rankings"][0]["title"], "Backfilled article")
        self.assertIn("2026-11-01", [d["value"] for d in self.reader.catalog("UTC")])
        article, _ = self.read(zone="UTC", page="investigation", title="Backfilled article")
        self.assertEqual(article["metrics"]["edits"], 1)

    def test_all_live_horizons_use_event_time(self):
        self.reader.now = lambda: datetime(2026, 11, 1, 12, tzinfo=UTC).timestamp()
        rows = []
        for i, hour in enumerate(("11:59:00", "11:30:00", "07:00:00", "05:00:00"), 1):
            rows.append({"event_time": datetime.fromisoformat("2026-11-01T" + hour), "event_id": f"enwiki:{i}",
                         "change_id": i, "page_id": i, "page_title": f"Article {i}", "namespace": 0,
                         "change_type": "edit", "wiki": "enwiki", "is_bot": False})
        self.write(self.paths.raw / "ingestion_date=2026-11-01", rows)
        for horizon, expected in (("5m", 1), ("1h", 2), ("6h", 3)):
            with self.subTest(horizon=horizon):
                data, _ = self.read(mode="live", horizon=horizon)
                self.assertEqual(data["metrics"]["edits"], expected)

    def test_finalized_metrics_measure_window_rows_and_require_article(self):
        self.reader.now = lambda: datetime(2026, 11, 1, 12, tzinfo=UTC).timestamp()
        self.write(self.paths.live / "windows_5m", [{"window_start": datetime(2026, 11, 1, 11, 55),
            "window_end": datetime(2026, 11, 1, 12), "pageid": 1, "title": "Example", "ns": 0,
            "edits_total": 7, "edits_human": 5, "edits_bot": 2}])
        data, progress = self.read(mode="live", finalized=True)
        self.assertEqual(data["metrics"]["edits"], 7)
        metric = next(m for args, m in progress if args[0] == "RESULT")
        self.assertEqual(metric["rows_fetched"], 1)
        self.assertIsNone(metric["events_analyzed"])
        self.assertGreater(metric["arrow_payload_bytes"], 0)
        empty, _ = self.read(mode="live", finalized=True, page="investigation")
        self.assertEqual(empty["metrics"]["edits"], 0)
        self.assertIn("Choose an article", empty["empty_message"])

    def test_scoped_http_job_and_render_callbacks(self):
        self.write(self.paths.curated / "rc_events/day=2026-11-01", [event(1, "2026-11-01T12:00:00")])
        w = ActivityWorkspace(self.paths, self.reader)
        self.addCleanup(w.close)
        service = Mock()
        app = create_app(service, activity_workspace=w)
        client = app.server.test_client()
        values = {"location.pathname": "/", "location.search": "", "pulse-mode.value": "stored",
                  "pulse-kind.value": "summary", "pulse-date.date": "2026-11-01", "pulse-horizon.value": "5m",
                  "pulse-zone.value": "UTC", "pulse-finalized.value": [], "pulse-frame.data": {"cursor": 0},
                  "pulse-retry.n_clicks": 1, "pulse-kafka-run.data": None, "pulse-freeze.value": [], "pulse-replay-engine.value": "saved",
                  "pulse-owner.data": "http-fixture", "pulse-request.data": None, "pulse-last.data": None,
                  "pulse-tick.n_intervals": 0, "pulse-job-tick.n_intervals": 0, "pulse-article.search_value": "",
                  "pulse-rendered.data": None, "pulse-clock.data": None}

        def invoke(key, changed):
            cb = app.callback_map[key]
            out = cb["output"]
            describe = lambda o: {"id": o.component_id, "property": o.component_property}
            outputs = [describe(o) for o in out] if isinstance(out, list) else describe(out)
            args = lambda specs: [{**s, "value": values[s["id"] + "." + s["property"]]} for s in specs]
            return client.post("/_dash-update-component", json={"output": key, "outputs": outputs,
                               "inputs": args(cb["inputs"]), "state": args(cb["state"]), "changedPropIds": [changed]})

        request = invoke("pulse-request.data", "pulse-retry.n_clicks")
        self.assertEqual(request.status_code, 200, request.get_data(as_text=True))
        data = request.json["response"]["pulse-request"]["data"]
        deadline = time.monotonic() + 2
        while w.get(data["id"])["status"] not in TERMINAL and time.monotonic() < deadline:
            time.sleep(.005)
        values["pulse-request.data"] = data
        key = next(k for k in app.callback_map if "pulse-core.children" in k)
        result = invoke(key, "pulse-request.data")
        self.assertEqual(result.status_code, 200, result.get_data(as_text=True))
        self.assertIn("Refresh complete", result.json["response"]["pulse-console-log"]["children"])
        self.assertEqual(result.json["response"]["pulse-last"]["data"]["status"], "complete")
        service.load.assert_not_called()


class ActivityJobsTests(unittest.TestCase):
    def wait(self, workspace, job_id):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            job = workspace.get(job_id, True)
            if job["status"] in TERMINAL:
                return job
            time.sleep(.005)
        self.fail("Worker did not finish")

    def test_progress_does_not_wait_for_worker_and_cancel_is_cooperative(self):
        entered, release = threading.Event(), threading.Event()
        reader = Mock()

        def read(spec, emit, cancelled):
            entered.set()
            emit("READ", "Waiting on fixture")
            release.wait(2)
            if cancelled():
                raise InterruptedError()
            return {"loaded_at": "fixture"}

        reader.read.side_effect = read
        reader.evidence.return_value = {}
        workspace = ActivityWorkspace(SimpleNamespace(), reader)
        self.addCleanup(workspace.close)
        first = workspace.submit({}, "browser")
        self.assertTrue(entered.wait(1))
        before = time.monotonic()
        self.assertEqual(workspace.get(first["id"])["status"], "running")
        self.assertLess(time.monotonic() - before, .1)
        second = workspace.submit({"horizon": "1h"}, "browser")
        release.set()
        self.assertEqual(self.wait(workspace, first["id"])["status"], "cancelled")
        self.assertEqual(self.wait(workspace, second["id"])["status"], "complete")
        summaries = [r for r in workspace.history.entries if r["stage"] == "SUMMARY"]
        self.assertEqual(len(summaries), 2)
        self.assertIn("Refresh cancelled", summaries[0]["message"])
        cached = workspace.submit({"horizon": "1h"}, "browser")
        self.assertEqual(cached["metrics"]["cache"], "snapshot hit")
        self.assertEqual(cached["metrics"]["events_analyzed"], 0)
        self.assertEqual(cached["metrics"]["rows_fetched"], 0)
        self.assertEqual(cached["metrics"]["arrow_payload_bytes"], 0)

    def test_core_available_while_optional_evidence_is_loading(self):
        waiting, release = threading.Event(), threading.Event()
        reader = Mock()
        reader.read.return_value = {"loaded_at": "fixture", "metrics": {"edits": 2}}

        def evidence(*args):
            waiting.set()
            release.wait(2)
            return {}

        reader.evidence.side_effect = evidence
        w = ActivityWorkspace(SimpleNamespace(), reader)
        self.addCleanup(w.close)
        request = w.submit({}, "browser")
        self.assertTrue(waiting.wait(1))
        state = w.get(request["id"], True)
        self.assertEqual(state["revision"], 1)
        self.assertEqual(state["result"]["metrics"]["edits"], 2)
        release.set()
        self.wait(w, request["id"])

    def test_failed_query_can_be_retried_and_cache_reports_exact_result_size(self):
        reader = Mock()
        reader.read.side_effect = [RuntimeError("fixture failure"), {"loaded_at": "fixture", "metrics": {"edits": 7}}]
        reader.evidence.return_value = {"warnings": []}
        workspace = ActivityWorkspace(SimpleNamespace(), reader)
        self.addCleanup(workspace.close)
        with self.assertLogs(level="ERROR"):
            first = workspace.submit({}, "failure-fixture")
            self.assertEqual(self.wait(workspace, first["id"])["status"], "failed")
        second = workspace.submit({}, "failure-fixture")
        complete = self.wait(workspace, second["id"])
        self.assertEqual(complete["status"], "complete")
        self.assertIn("Refresh failed", workspace.history.text())
        cached = workspace.submit({}, "failure-fixture")
        self.assertEqual(cached["metrics"]["events_matched"], 7)
        self.assertEqual(cached["metrics"]["events_analyzed"], 0)
        self.assertEqual(cached["metrics"]["response_bytes"], len(json.dumps({"activity": complete["result"], "evidence": complete["evidence"]}).encode()))

    def test_routes_and_deep_link_preserve_context(self):
        w = ActivityWorkspace(SimpleNamespace(), Mock())
        self.addCleanup(w.close)
        app = create_app(Mock(), activity_workspace=w)
        client = app.server.test_client()
        self.assertEqual(client.get("/api/activity/jobs/missing").status_code, 404)
        self.assertEqual(client.get("/healthz").status_code, 200)
        self.assertEqual(client.get("/_dash-layout").status_code, 200)
        spec = {"mode": "stored", "kind": "replay", "day": "2026-11-01", "horizon": "1h", "zone": "IST", "cursor": 1234}
        url = activity_url("/investigation", spec, "A & B")
        self.assertIn("cursor=1234", url)
        self.assertIn("title=A+%26+B", url)


class ActivityControlTests(unittest.TestCase):
    """Exercise the real HTTP callback boundary, including selection edge cases."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.paths = SimpleNamespace(raw=root / "raw", curated=root / "curated", history=root / "history",
                                     live=root / "live", runtime=root / "run")
        self.workspace = ActivityWorkspace(self.paths)
        self.addCleanup(self.workspace.close)
        self.app = create_app(Mock(), activity_workspace=self.workspace)
        self.client = self.app.server.test_client()
        self.values = {"location.pathname": "/", "location.search": "", "pulse-mode.value": "stored",
                       "pulse-kind.value": "replay", "pulse-date.date": "2026-11-01", "pulse-horizon.value": "5m",
                       "pulse-zone.value": "UTC", "pulse-finalized.value": [], "pulse-frame.data": {"cursor": 1234, "render": True},
                       "pulse-retry.n_clicks": 1, "pulse-kafka-run.data": None, "pulse-freeze.value": [], "pulse-replay-engine.value": "saved",
                       "pulse-owner.data": "control-fixture", "pulse-request.data": None, "pulse-last.data": None,
                       "pulse-tick.n_intervals": 0, "pulse-job-tick.n_intervals": 0, "pulse-kafka-tick.n_intervals": 0,
                       "pulse-rendered.data": None, "pulse-article.value": None, "pulse-article.search_value": "",
                       "pulse-clock.data": {"cursor": 1234}, "pulse-speed.value": 50, "pulse-date-previous.n_clicks": 0,
                       "pulse-date-next.n_clicks": 0, "pulse-dates.data": ["2026-10-31", "2026-11-01"]}

    def invoke(self, fragment, changed):
        key = next(k for k in self.app.callback_map if fragment in k)
        callback = self.app.callback_map[key]
        output = callback["output"]
        describe = lambda o: {"id": o.component_id, "property": o.component_property}
        outputs = [describe(o) for o in output] if isinstance(output, list) else describe(output)
        args = lambda specs: [{**s, "value": self.values.get(s["id"] + "." + s["property"])} for s in specs]
        response = self.client.post("/_dash-update-component", json={"output": key, "outputs": outputs,
            "inputs": args(callback["inputs"]), "state": args(callback["state"]), "changedPropIds": [changed]})
        self.assertIn(response.status_code, (200, 204), response.get_data(as_text=True))
        return response

    def test_calendar_preserves_unavailable_deep_link_and_cursor(self):
        self.values["location.search"] = "?mode=stored&day=2026-01-01&cursor=42000&zone=IST"
        result = self.invoke("pulse-seed.data", "location.search").json["response"]
        self.assertEqual(result["pulse-date"]["date"], "2026-01-01")
        self.assertEqual(result["pulse-seed"]["data"]["cursor"], 42000)
        self.assertEqual(result["pulse-dates"]["data"], [])

    def test_stream_source_defaults_live_and_restores_active_since_on_navigation(self):
        result = self.invoke("pulse-source-start.value", "location.pathname").json["response"]
        self.assertEqual(result["pulse-source-start"]["value"], "live")
        atomic_json(self.paths.runtime / "stream-source.json", {"mode": "since", "since": "2026-11-01T12:34:56.000Z"})
        result = self.invoke("pulse-source-start.value", "location.pathname").json["response"]
        self.assertEqual(result["pulse-source-date"]["date"], "2026-11-01")
        self.assertEqual(result["pulse-source-time"]["value"], "12:34:56")

    def test_catch_up_health_keeps_start_disabled_and_stop_available(self):
        health = {"state": "catching_up", "label": "CATCHING UP · Wikipedia history is arriving", "managed": True}
        with patch("dashboards.activity_ui.stream_health", return_value=health), \
                patch.object(self.workspace.ingestion, "snapshot", return_value=[]), \
                patch.dict("os.environ", {"WIKIPULSE_CONTROL_TOKEN": "fixture"}):
            response = self.invoke("pulse-stream-metrics.children", "health-refresh.n_intervals").json["response"]
        self.assertTrue(response["pulse-start-stream"]["disabled"])
        self.assertFalse(response["pulse-stop-stream"]["disabled"])
        self.assertFalse(response["pulse-apply-source"]["disabled"])

    def test_apply_source_queues_explicit_utc_timestamp_and_live_discards_timestamp(self):
        self.values.update({"pulse-apply-source.n_clicks": 1, "pulse-source-start.value": "since",
                            "pulse-source-date.date": "2026-11-01", "pulse-source-time.value": "12:34:56"})
        with patch("dashboards.activity_ui.request_action", return_value={"message": "Accepted"}) as action:
            self.invoke("pulse-source-command.children", "pulse-apply-source.n_clicks")
            self.assertEqual(action.call_args.kwargs["source"], {"mode": "since", "since": "2026-11-01T12:34:56Z"})
            self.values["pulse-source-start.value"] = "live"
            self.invoke("pulse-source-command.children", "pulse-apply-source.n_clicks")
            self.assertEqual(action.call_args.kwargs["source"]["mode"], "live")

    def test_calendar_steps_between_captured_dates_only(self):
        with patch.object(self.workspace.reader, "catalog", return_value=[{"value": "2026-10-28"}, {"value": "2026-11-05"}]):
            self.assertEqual(self.invoke("pulse-date.date@", "pulse-date-previous.n_clicks").json["response"]["pulse-date"]["date"], "2026-10-28")
            self.assertEqual(self.invoke("pulse-date.date@", "pulse-date-next.n_clicks").json["response"]["pulse-date"]["date"], "2026-11-05")

    def test_unavailable_date_has_honest_hint_and_boundary_buttons(self):
        self.values["pulse-date.date"] = "2026-12-01"
        result = self.invoke("pulse-date-hint.children", "pulse-date.date").json["response"]
        self.assertIn("No known capture", result["pulse-date-hint"]["children"])
        self.assertTrue(result["pulse-date-next"]["disabled"])
        self.assertFalse(result["pulse-date-previous"]["disabled"])

    def test_freeze_skips_automatic_queries_and_resume_submits(self):
        self.values["pulse-freeze.value"] = ["freeze"]
        with patch.object(self.workspace, "submit", wraps=self.workspace.submit) as submit:
            self.assertEqual(self.invoke("pulse-request.data", "pulse-frame.data").status_code, 204)
            submit.assert_not_called()
            self.values["pulse-freeze.value"] = []
            self.invoke("pulse-request.data", "pulse-freeze.value")
            submit.assert_called_once()

    def test_summary_does_not_requery_on_clock_ticks(self):
        self.values["pulse-kind.value"] = "summary"
        with patch.object(self.workspace, "submit") as submit:
            self.assertEqual(self.invoke("pulse-request.data", "pulse-frame.data").status_code, 204)
            submit.assert_not_called()

    def test_background_kafka_updates_do_not_refresh_saved_summary(self):
        self.values["pulse-kind.value"] = "summary"
        self.values["pulse-kafka-run.data"] = {"id": "a" * 32, "attached": False, "status": "running"}
        with patch.object(self.workspace, "submit") as submit:
            self.assertEqual(self.invoke("pulse-request.data", "pulse-kafka-run.data").status_code, 204)
            submit.assert_not_called()

    def test_detaching_kafka_still_loads_saved_data_once(self):
        self.values["pulse-request.data"] = {"spec": {"run_id": "a" * 32}}
        self.values["pulse-kafka-run.data"] = {"id": "a" * 32, "attached": False, "status": "running"}
        with patch.object(self.workspace, "submit", wraps=self.workspace.submit) as submit:
            self.invoke("pulse-request.data", "pulse-kafka-run.data")
            self.assertNotIn("run_id", submit.call_args.args[0])

    def test_terminal_kafka_status_stops_polling_manager(self):
        self.values["pulse-kafka-run.data"] = {"id": "a" * 32, "status": "cancelled"}
        with patch.object(self.workspace, "kafka_replays") as manager:
            self.assertEqual(self.invoke("pulse-kafka-status.children", "pulse-kafka-tick.n_intervals").status_code, 204)
            manager.assert_not_called()

    def test_attached_kafka_status_does_not_requery_charts(self):
        self.values["pulse-replay-engine.value"] = "kafka"
        self.values["pulse-request.data"] = {"spec": {"run_id": "a" * 32}}
        self.values["pulse-kafka-run.data"] = {"id": "a" * 32, "attached": True,
            "status": "running", "day": "2026-11-01", "zone": "UTC", "cursor": 1300}
        with patch.object(self.workspace, "submit") as submit:
            self.assertEqual(self.invoke("pulse-request.data", "pulse-kafka-run.data").status_code, 204)
            submit.assert_not_called()

    def test_scheduled_kafka_frame_loads_latest_cursor(self):
        self.values["pulse-replay-engine.value"] = "kafka"
        self.values["pulse-kafka-run.data"] = {"id": "a" * 32, "attached": True,
            "status": "running", "day": "2026-11-01", "zone": "UTC", "cursor": 1300}
        with patch.object(self.workspace, "submit", return_value={"id": "scheduled"}) as submit:
            self.invoke("pulse-request.data", "pulse-frame.data")
            self.assertEqual(submit.call_args.args[0]["cursor"], 1300)
            self.assertEqual(submit.call_args.args[0]["run_id"], "a" * 32)

    def test_scheduled_refresh_does_not_cancel_an_inflight_query(self):
        self.values["pulse-frame.data"]["automatic"] = True
        self.values["pulse-request.data"] = {"id": "old", "spec": {}}
        with patch.object(self.workspace, "get", return_value={"status": "running"}), \
                patch.object(self.workspace, "submit") as submit:
            self.assertEqual(self.invoke("pulse-request.data", "pulse-frame.data").status_code, 204)
            submit.assert_not_called()

    def test_manual_refresh_uses_clock_instead_of_old_chart_frame(self):
        self.values["pulse-clock.data"] = {"cursor": 1600}
        with patch.object(self.workspace, "submit", wraps=self.workspace.submit) as submit:
            self.invoke("pulse-request.data", "pulse-retry.n_clicks")
            self.assertEqual(submit.call_args.args[0]["cursor"], 1600)

    def test_cached_refresh_leaves_existing_graphs_untouched(self):
        self.values["pulse-kind.value"] = "summary"
        first = self.invoke("pulse-request.data", "pulse-retry.n_clicks").json["response"]["pulse-request"]["data"]
        deadline = time.monotonic() + 2
        while self.workspace.get(first["id"])["status"] not in TERMINAL and time.monotonic() < deadline:
            time.sleep(.005)
        self.values["pulse-request.data"] = first
        rendered = self.invoke("pulse-core.children", "pulse-request.data").json["response"]
        self.values["pulse-rendered.data"] = rendered["pulse-rendered"]["data"]
        self.values["pulse-last.data"] = rendered["pulse-last"]["data"]
        second = self.invoke("pulse-request.data", "pulse-retry.n_clicks").json["response"]["pulse-request"]["data"]
        self.values["pulse-request.data"] = second
        result = self.invoke("pulse-core.children", "pulse-request.data").json["response"]
        self.assertNotIn("pulse-core", result)
        self.assertNotIn("pulse-evidence", result)
        self.assertIn("cached snapshot", result["pulse-console-summary"]["children"])

    def test_background_refresh_keeps_existing_optional_evidence_visible(self):
        spec = normalize_spec({"mode": "stored", "day": "2026-11-01"})
        data = self.workspace.reader.read(spec, lambda *a, **k: None, lambda: False)
        self.values["pulse-request.data"] = {"id": "new", "scope": "same"}
        self.values["pulse-rendered.data"] = {"scope": "same", "loaded_at": "old", "evidence_token": "prior"}
        job = {"id": "new", "spec": spec, "status": "evidence", "revision": 1, "result": data,
               "evidence": None, "metrics": {}, "logs": [], "elapsed_seconds": .1}
        with patch.object(self.workspace, "get", return_value=job):
            result = self.invoke("pulse-core.children", "pulse-request.data").json["response"]
        self.assertIn("pulse-core", result)
        self.assertNotIn("pulse-evidence", result)

    def test_browser_title_updates_are_disabled_in_served_config(self):
        page = self.client.get("/").get_data(as_text=True)
        config = json.loads(re.search(r'<script id="_dash-config"[^>]*>(.*?)</script>', page, re.S).group(1))
        self.assertIsNone(config["update_title"])

    def test_replay_has_only_start_and_stop_action_buttons(self):
        layout = self.client.get("/_dash-layout").json
        def nodes(value):
            if isinstance(value, dict):
                yield value
                yield from nodes(value.get("props", {}).get("children"))
            elif isinstance(value, list):
                for child in value:
                    yield from nodes(child)
        controls = next(n for n in nodes(layout) if n.get("props", {}).get("id") == "pulse-replay-controls")
        buttons = [n["props"]["children"] for n in nodes(controls) if n.get("type") == "Button"]
        self.assertEqual(buttons, ["Start replay", "Stop replay"])

    def test_saved_start_does_not_launch_kafka(self):
        with patch.object(self.workspace, "kafka_replays") as manager:
            self.invoke("pulse-kafka-status.children", "pulse-play.n_clicks")
            manager.assert_not_called()

    def test_unified_kafka_start_and_stop(self):
        self.values["pulse-replay-engine.value"] = "kafka"
        state = {"id": "b" * 32, "day": "2026-11-01", "zone": "UTC", "speed": 50, "status": "running"}
        manager = Mock()
        manager.start.return_value = dict(state)
        manager.status.return_value = dict(state)
        with patch.object(self.workspace, "kafka_replays", return_value=manager):
            start = self.invoke("pulse-kafka-status.children", "pulse-play.n_clicks").json["response"]
            manager.start.assert_called_once_with("2026-11-01", "UTC", 50)
            self.values["pulse-kafka-run.data"] = start["pulse-kafka-run"]["data"]
            stop = self.invoke("pulse-kafka-status.children", "pulse-cancel.n_clicks").json["response"]
        manager.action.assert_called_once_with(state["id"], "cancel")
        self.assertEqual(stop["pulse-kafka-run"]["data"]["status"], "cancelling")

    def test_scope_change_stops_previous_kafka_run(self):
        self.values["pulse-replay-engine.value"] = "kafka"
        self.values["pulse-kafka-run.data"] = {"id": "c" * 32, "status": "running", "attached": True}
        self.values["pulse-date.date"] = "2026-11-02"
        manager = Mock()
        manager.status.return_value = {"id": "c" * 32, "status": "running", "day": "2026-11-01", "zone": "UTC"}
        with patch.object(self.workspace, "kafka_replays", return_value=manager):
            response = self.invoke("pulse-kafka-status.children", "pulse-date.date").json["response"]
        manager.action.assert_called_once_with("c" * 32, "cancel")
        self.assertFalse(response["pulse-kafka-run"]["data"]["attached"])

    def test_article_selection_carries_replay_context(self):
        self.values["pulse-article.value"] = "A & B"
        result = self.invoke("location.href@", "pulse-article.value").json["response"]["location"]["href"]
        self.assertIn("day=2026-11-01", result)
        self.assertIn("cursor=1234", result)
        self.assertIn("title=A+%26+B", result)

    def test_unicode_article_scope_matches_browser_json_stringify(self):
        self.values["location.pathname"] = "/investigation"
        self.values["location.search"] = "?title=Einstein%E2%80%93Cartan+theory"
        result = self.invoke("pulse-request.data", "location.search").json["response"]["pulse-request"]["data"]
        expected = '["/investigation","stored","replay","2026-11-01","5m","UTC","Einstein–Cartan theory",null]'
        self.assertEqual(result["scope"], expected)

    def test_clearing_article_preserves_scope_and_removes_title(self):
        self.values["location.pathname"] = "/investigation"
        self.values["location.search"] = "?title=Example"
        self.values["pulse-article.value"] = None
        result = self.invoke("location.href@", "pulse-article.value").json["response"]["location"]["href"]
        self.assertTrue(result.startswith("/investigation?"))
        self.assertNotIn("title=", result)
        self.assertIn("cursor=1234", result)
        self.assertIn("day=2026-11-01", result)

    def test_departure_cancels_pending_activity(self):
        self.values["location.pathname"] = "/historical"
        with patch.object(self.workspace, "cancel_owner") as cancel:
            self.invoke("pulse-request.data", "location.pathname")
            cancel.assert_called_once_with("control-fixture")

    def test_log_export_matches_job_and_has_download_header(self):
        job = self.workspace.submit({"mode": "stored", "day": "2026-11-01"}, "download-fixture")
        result = self.client.get(f"/api/activity/jobs/{job['id']}/log")
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json["id"], job["id"])
        self.assertIn("attachment", result.headers["Content-Disposition"])


class StreamControlTests(unittest.TestCase):
    def test_stop_drains_in_dependency_order_and_preserves_pending_windows(self):
        from src.kafka_common import KAFKA_TOPIC
        processes = Mock()
        with tempfile.TemporaryDirectory() as directory, patch("src.stream_control.kafka_targets", return_value={"0": 4}), \
                patch("src.stream_control.committed_offset", return_value={KAFKA_TOPIC: {"0": 4}}), \
                patch("src.stream_control.file_stream_drained", return_value=True):
            result = stop_and_drain(processes, Path(directory))
        self.assertTrue(result["drained"])
        self.assertEqual([c.args[0] for c in processes.stop_named.call_args_list],
                         [("collector",), ("bronze",), ("windows-5m", "windows-1h"), ("detection-5m", "detection-1h")])
        self.assertIn("Open event-time windows", result["message"])

    def test_unverified_drain_is_never_reported_as_drained(self):
        with tempfile.TemporaryDirectory() as directory, patch("src.stream_control.kafka_targets", side_effect=RuntimeError("offline")):
            result = stop_and_drain(Mock(), Path(directory), timeout=0)
        self.assertFalse(result["drained"])
        self.assertTrue(result["warnings"])

    def test_command_auth_idempotency_and_idle_start(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict("os.environ", {"WIKIPULSE_CONTROL_TOKEN": "fixture-token"}):
            root = Path(directory)
            atomic_json(root / "data/run/local_supervisor.json", {"supervisor_pid": 999, "state": "idle"})
            with patch("src.stream_control.os.kill"):
                self.assertFalse(request_action("start", "fixture-command", "wrong", root)["ok"])
                first = request_action("start", "fixture-command", "fixture-token", root)
                self.assertTrue(first["ok"])
                self.assertTrue(request_action("start", "fixture-command", "fixture-token", root)["ok"])
                self.assertFalse(request_action("stop", "another-command", "fixture-token", root)["ok"])

    def test_drain_uses_committed_source_offset_not_discovered_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, checkpoint = root / "source", root / "checkpoint"
            (source / "_spark_metadata").mkdir(parents=True)
            (source / "_spark_metadata/0").write_text('v1\n{"path":"file:///one","action":"add"}\n')
            (checkpoint / "commits").mkdir(parents=True)
            (checkpoint / "offsets").mkdir()
            (checkpoint / "sources/0").mkdir(parents=True)
            (checkpoint / "commits/0").write_text('v1\n{}')
            (checkpoint / "offsets/0").write_text('v1\n{}\n{"logOffset":0}')
            (checkpoint / "sources/0/0").write_text('v1\n{"path":"file:///old"}\n')
            (checkpoint / "sources/0/1").write_text('v1\n{"path":"file:///one"}\n')
            self.assertFalse(file_stream_drained(source, checkpoint))
            (checkpoint / "offsets/0").write_text('v1\n{}\n{"logOffset":1}')
            self.assertTrue(file_stream_drained(source, checkpoint))


class IsolatedReplayControlTests(unittest.TestCase):
    def test_idle_spark_heartbeat_does_not_hide_later_real_batch(self):
        from src.replay.session import accumulate_progress
        state = {"consumed": 0, "committed": 0, "rejected": 0}
        batches = set()
        idle = {"batchId": 3, "numInputRows": 0}
        data = {"batchId": 3, "numInputRows": 7,
                "observedMetrics": {"validation": {"rows": 7, "rejected": 1}, "output": {"rows": 6}}}
        accumulate_progress(state, [idle], batches)
        self.assertNotIn(3, batches)
        accumulate_progress(state, [idle, data, data], batches)
        self.assertEqual((state["consumed"], state["committed"], state["rejected"]), (7, 6, 1))

    def test_run_isolation_publication_controls_and_single_active_run(self):
        from dashboards.kafka_replay import KafkaReplays
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = SimpleNamespace(raw=root / "live-bronze", curated=root / "curated", runtime=root / "run")
            (paths.curated / "rc_events/day=2026-11-01").mkdir(parents=True)
            process = Mock(pid=987654321)
            process.poll.return_value = None
            manager = KafkaReplays(paths)
            with patch("src.kafka_common.is_file_broker_mode", return_value=False), \
                    patch("dashboards.kafka_replay.subprocess.Popen", return_value=process) as launch:
                state = manager.start("2026-11-01", "UTC", 50)
                identity = state["id"]
                with self.assertRaisesRegex(ValueError, "already active"):
                    manager.start("2026-11-01", "UTC", 50)
            invocation = launch.call_args.args[0]
            run = manager.root / identity
            self.assertIn(str(run), invocation)
            self.assertNotEqual(manager.reader(identity).paths.raw, paths.raw)
            manager.action(identity, "pause")
            self.assertTrue(json.loads((run / "control.json").read_text())["paused"])
            manager.action(identity, "pause")
            self.assertFalse(json.loads((run / "control.json").read_text())["paused"])
            manager.action(identity, "cancel")
            self.assertTrue(json.loads((run / "control.json").read_text())["cancelled"])
            process.poll.return_value = 1
            self.assertEqual(manager.status(identity)["status"], "failed")

    def test_pause_excludes_wall_time_and_cancel_interrupts_gate(self):
        from src.replay.session import ReplayGate
        with tempfile.TemporaryDirectory() as directory:
            control = Path(directory) / "control.json"
            now = [0.0]
            with patch("src.replay.session.time.monotonic", side_effect=lambda: now[0]):
                gate = ReplayGate(control)
                now[0] = 5
                self.assertEqual(gate.clock(), 5)
                atomic_json(control, {"paused": True})
                now[0] = 100
                self.assertEqual(gate.clock(), 5)
                atomic_json(control, {"paused": False})
                now[0] = 101
                self.assertEqual(gate.clock(), 6)
                atomic_json(control, {"cancelled": True})
                with self.assertRaises(InterruptedError):
                    gate.wait()


class PlaybackClockTests(unittest.TestCase):
    def test_start_stop_are_idempotent_and_kafka_ready_stays_idle(self):
        script = r'''
const fs=require('fs'),vm=require('vm');let now=0,state=null,engine='saved',seed={first_cursor:1000};
global.window={dash_clientside:{no_update:null,callback_context:{triggered:[]}}};Date.now=()=>now;
vm.runInThisContext(fs.readFileSync('dashboards/assets/activity.js','utf8'));
function assert(ok){if(!ok)throw Error('Start/Stop contract failed');}
function step(trigger){window.dash_clientside.callback_context.triggered=[{prop_id:trigger+'.value'}];
 const out=window.dash_clientside.pulse.clock(0,50,0,0,'stored','replay','2026-11-01','UTC',seed,'/',state,30,engine);state=out[0];return out;}
step('pulse-date');step('pulse-play');assert(state.cursor===1000&&state.playing);
now=2000;step('pulse-play');assert(state.cursor===1100&&state.playing);
now=4000;let stopped=step('pulse-cancel');assert(state.cursor===1200&&!state.playing&&stopped[1]);
now=6000;step('pulse-cancel');assert(state.cursor===1200&&!state.playing);
now=8000;step('pulse-play');assert(state.cursor===1200&&state.playing);
engine='kafka';seed={kafka_cursor:0,kafka_status:'ready'};step('pulse-replay-engine');
now=100000;assert(step('pulse-tick')[1]===null&&!state.playing);
'''
        result = subprocess.run(["node", "-e", script], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_refresh_cadence_is_independent_of_playback_speed(self):
        script = r'''
const fs=require('fs'),vm=require('vm');let now=0;
global.window={dash_clientside:{no_update:null,callback_context:{triggered:[]}}};Date.now=()=>now;
vm.runInThisContext(fs.readFileSync('dashboards/assets/activity.js','utf8'));
function assert(ok,msg){if(!ok)throw Error(msg);}
for(const refresh of [10,30,60]) for(const mode of ['live','stored']){
 let state=null;
 const step=function(trigger){window.dash_clientside.callback_context.triggered=[{prop_id:trigger+'.value'}];
  let out=window.dash_clientside.pulse.clock(0,100,0,0,mode,'replay','2026-11-01','UTC',null,'/',state,refresh);state=out[0];return out;};
 now=0;step('pulse-date');if(mode==='stored')step('pulse-play');
 for(let second=1;second<refresh;second++){now=second*1000;assert(step('pulse-tick')[1]===null,'redrew too early');}
 now=refresh*1000;let out=step('pulse-tick');assert(out[1]&&out[1].automatic,'missed selected cadence');
 if(mode==='stored')assert(state.cursor===refresh*100,'refresh slowed replay clock');
}
let state=null,rate=30,seed={kafka_cursor:100,kafka_status:'running'};
function step(trigger){window.dash_clientside.callback_context.triggered=[{prop_id:trigger+'.value'}];
 let out=window.dash_clientside.pulse.clock(0,50,0,0,'stored','replay','2026-11-01','UTC',seed,'/',state,rate);state=out[0];return out;}
now=0;step('pulse-date');now=2000;seed.kafka_cursor=200;assert(step('pulse-kafka-run')[1]===null,'Kafka status redrew charts');
now=30000;assert(step('pulse-tick')[1]!==null,'Kafka cadence missed');
seed.kafka_status='complete';now=31000;assert(step('pulse-kafka-run')[1]!==null,'final committed frame missing');
now=100000;assert(step('pulse-tick')[1]===null,'terminal Kafka run kept refreshing');
// A rate change resets the wait without changing playback position or redrawing.
seed=null;state=null;now=0;step('pulse-date');step('pulse-play');now=5000;rate=60;
assert(step('pulse-refresh-rate')[1]===null,'rate change needlessly redrew');assert(state.cursor===250,'rate change moved cursor');
now=64999;assert(step('pulse-tick')[1]===null,'new cadence fired early');now=65000;assert(step('pulse-tick')[1]!==null,'new cadence missed');
step('pulse-cancel');now=200000;assert(step('pulse-tick')[1]===null,'paused replay refreshed');
'''
        result = subprocess.run(["node", "-e", script], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_every_speed_first_capture_cancel_and_navigation(self):
        script = r'''
const fs=require('fs'),vm=require('vm'); let now=0;
global.window={dash_clientside:{no_update:null,callback_context:{triggered:[]}}}; Date.now=()=>now;
vm.runInThisContext(fs.readFileSync('dashboards/assets/activity.js','utf8'));
function assert(ok){if(!ok)throw Error('clock contract failed');}
for (const speed of [1,5,20,24,48,50,100]) {
 let state=null,seed={first_cursor:1000},path='/';
 function step(trigger){window.dash_clientside.callback_context.triggered=[{prop_id:trigger+'.value'}];
   const out=window.dash_clientside.pulse.clock(0,speed,0,0,'stored','replay','2026-11-01','IST',seed,path,state);state=out[0];return out;}
 step('pulse-date'); step('pulse-play'); assert(state.cursor===1000);
 now+=2000;step('pulse-tick');assert(state.cursor===1000+speed*2);
 step('pulse-cancel'); const at=state.cursor;now+=2000;step('pulse-tick');assert(state.cursor===at&&!state.playing);
 step('pulse-play');path='/historical';step('location');assert(!state.playing);
}
'''
        result = subprocess.run(["node", "-e", script], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_elapsed_speed_pause_seek_and_completion(self):
        # Execute the production pure clock against a deterministic wall clock.
        script = r'''
const fs = require('fs'), vm = require('vm');
let now = 0;
global.window = {dash_clientside:{no_update:null,callback_context:{triggered:[]}}};
Date.now = () => now;
vm.runInThisContext(fs.readFileSync('dashboards/assets/activity.js','utf8'));
let state=null, speed=50, seek=0;
function step(trigger){ window.dash_clientside.callback_context.triggered=[{prop_id:trigger+'.value'}];
 const out=window.dash_clientside.pulse.clock(0,speed,0,seek,'stored','replay','2026-11-01','UTC',null,'/',state);
 state=out[0]; return out; }
function equal(a,b){ if(a!==b)throw Error(`${a} != ${b}`); }
step('pulse-date');step('pulse-play');now=10000;step('pulse-tick');equal(state.cursor,500);
speed=20;step('pulse-speed');now=20000;step('pulse-tick');equal(state.cursor,700);
step('pulse-cancel');now=40000;step('pulse-tick');equal(state.cursor,700);
seek=86390;step('pulse-seek');step('pulse-play');now=42000;step('pulse-tick');equal(state.cursor,86400);equal(state.playing,false);
step('pulse-play');equal(state.cursor,0);equal(state.playing,true);
'''
        result = subprocess.run(["node", "-e", script], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
