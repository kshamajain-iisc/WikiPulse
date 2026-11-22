"""Upstream timestamp selection, reconnect cursors and checkpoint-safe switching."""

import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

import requests

from src import collector, run_local
from src.stream_source import CaptureProgress, collector_lock, normalize_since, stream_url
from src.stream_control import atomic_json, read_json, request_action


class SourceTests(unittest.TestCase):
    def test_collector_lock_rejects_another_process_and_releases_after_exit(self):
        code = "from src.stream_source import collector_lock; import sys\nwith collector_lock('broker/topic',sys.argv[1]): print('acquired')"
        with tempfile.TemporaryDirectory() as root:
            with collector_lock("broker/topic", root):
                blocked = subprocess.run([sys.executable, "-c", code, root], text=True, capture_output=True, timeout=5)
                self.assertNotEqual(blocked.returncode, 0)
                self.assertIn("Another WikiPulse collector", blocked.stderr)
                with collector_lock("broker/other-topic", root):
                    pass
            acquired = subprocess.run([sys.executable, "-c", code, root], text=True, capture_output=True, timeout=5)
            self.assertEqual(acquired.returncode, 0, acquired.stderr)

    def test_collector_lock_is_released_after_a_process_crash(self):
        code = "from src.stream_source import collector_lock; import sys,os\nwith collector_lock('broker/topic',sys.argv[1]): os._exit(9)"
        with tempfile.TemporaryDirectory() as root:
            crashed = subprocess.run([sys.executable, "-c", code, root], capture_output=True, timeout=5)
            self.assertEqual(crashed.returncode, 9)
            with collector_lock("broker/topic", root):
                pass

    def test_duplicate_main_exits_before_creating_a_producer_or_overwriting_progress(self):
        with tempfile.TemporaryDirectory() as root, collector_lock(f"{collector.KAFKA_SERVERS}/{collector.TOPIC}", root):
            with patch.object(collector, "collector_lock", side_effect=lambda identity: collector_lock(identity, root)), \
                    patch.object(collector, "create_producer") as producer, patch.object(collector, "stream_events") as stream, \
                    patch.object(collector, "USER_AGENT", "WikiPulseTest/1.0"), patch("sys.argv", ["collector"]):
                with self.assertRaisesRegex(SystemExit, "Another WikiPulse collector"):
                    collector.main()
                producer.assert_not_called()
                stream.assert_not_called()
                self.assertFalse((Path(root) / "collector-source.progress.json").exists())

    def test_timestamp_supports_offsets_and_epoch_milliseconds_but_not_future_or_naive(self):
        now = datetime(2026, 11, 16, tzinfo=timezone.utc).timestamp()
        self.assertEqual(normalize_since("2026-11-01T05:30:00+05:30", now), "2026-11-01T00:00:00.000Z")
        self.assertEqual(normalize_since(str(int(now * 1000)), now), "2026-11-16T00:00:00.000Z")
        self.assertIsNone(normalize_since(None, now))
        for value in ("bad", "2026-11-01T00:00:00", "2026-11-17T00:00:00Z"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                normalize_since(value, now)

    def test_live_strips_old_since_and_historical_preserves_other_query_parameters(self):
        url = "https://stream.wikimedia.org/v2/stream/recentchange?foo=bar&since=old&since=older"
        self.assertEqual(parse_qs(urlsplit(stream_url(url)).query), {"foo": ["bar"]})
        self.assertEqual(parse_qs(urlsplit(stream_url(url, "2026-11-01T00:00:00Z")).query),
                         {"foo": ["bar"], "since": ["2026-11-01T00:00:00Z"]})

    def test_resume_uses_last_processed_id_instead_of_restarting_since(self):
        self._reconnect(published=True, event_type="message", expect_cursor=True)

    def test_failed_enqueue_retries_original_since_without_advancing_cursor(self):
        self._reconnect(published=False, event_type="message", expect_cursor=False)

    def test_sse_error_event_does_not_advance_cursor(self):
        self._reconnect(published=True, event_type="error", expect_cursor=False)

    def _reconnect(self, published, event_type, expect_cursor):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        def first():
            yield "event: " + event_type
            yield "id: cursor-1"
            yield 'data: {"wiki":"enwiki"}'
            yield ""
            raise requests.ConnectionError("fixture disconnect")
        def second():
            collector.running = False
            yield ": heartbeat"
        response.iter_lines.side_effect = [first(), second()]
        session = Mock()
        session.get.return_value = response
        producer = Mock()
        producer.flush.return_value = 0
        with patch.object(collector.requests, "Session", return_value=session), patch.object(collector, "running", True), \
                patch.object(collector, "process_event", return_value=published), patch.object(collector.time, "sleep"), \
                patch.object(collector, "STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"), self.assertLogs(collector.logger):
            collector.stream_events(producer, "2026-11-01T00:00:00Z")
        calls = session.get.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertIn("since=", calls[0].args[0])
        self.assertEqual(calls[0].kwargs["headers"], {})
        if expect_cursor:
            self.assertNotIn("since=", calls[1].args[0])
            self.assertEqual(calls[1].kwargs["headers"], {"Last-Event-ID": "cursor-1"})
        else:
            self.assertEqual(calls[0].args[0], calls[1].args[0])
            self.assertEqual(calls[1].kwargs["headers"], {})

    def test_progress_records_actual_queued_payload_and_backfill_ingestion_mapping(self):
        with tempfile.TemporaryDirectory() as root:
            progress = CaptureProgress("2026-11-01T00:00:00Z", root)
            progress.record({"timestamp": datetime(2026, 11, 1, 12, tzinfo=timezone.utc).timestamp()}, 1234)
            progress.write("connected", force=True, counts={"published": 1})
            state = read_json(Path(root) / "collector-source.progress.json")
            self.assertEqual(state["queued_events"], 1)
            self.assertEqual(state["queued_bytes"], 1234)
            self.assertEqual(state["delivery"]["published"], 1)
            mapping = read_json(Path(root) / "capture-days.json")
            self.assertEqual(mapping["2026-11-01"], [datetime.now(timezone.utc).date().isoformat()])

    def test_source_command_validates_before_queuing_and_preserves_authentication(self):
        with tempfile.TemporaryDirectory() as root, patch.dict("os.environ", {"WIKIPULSE_CONTROL_TOKEN": "fixture"}), patch("src.stream_control.os.kill"):
            runtime = Path(root) / "data/run"
            atomic_json(runtime / "local_supervisor.json", {"state": "running", "supervisor_pid": 999})
            source = {"mode": "since", "since": "2026-11-01T00:00:00Z"}
            self.assertFalse(request_action("source", "source-fixture", "wrong", root, source)["ok"])
            self.assertFalse(request_action("source", "source-fixture", "fixture", root, {"mode": "since"})["ok"])
            self.assertFalse((runtime / "stream-control.request.json").exists())
            self.assertTrue(request_action("source", "source-fixture", "fixture", root, source)["ok"])
            command = read_json(runtime / "stream-control.request.json")
            self.assertEqual(command["source"]["since"], "2026-11-01T00:00:00.000Z")
            self.assertTrue(request_action("source", "source-fixture", "fixture", root, source)["ok"])
            self.assertFalse(request_action("stop", "other-fixture", "fixture", root)["ok"])

    def test_source_switch_restarts_only_collector_and_retains_processing_workers(self):
        with tempfile.TemporaryDirectory() as root, patch.object(run_local, "start_collector") as start, patch.object(run_local, "start_processing_services") as processing:
            runtime = Path(root)
            processes = Mock()
            result = run_local.change_stream_source(processes, runtime, {"mode": "since", "since": "2026-11-01T00:00:00Z"}, True)
            processes.stop_named.assert_called_once_with(("collector",))
            start.assert_called_once_with(processes, runtime)
            processing.assert_not_called()
            self.assertEqual(result["source"]["since"], "2026-11-01T00:00:00.000Z")
            self.assertEqual(read_json(runtime / "stream-source.json")["mode"], "since")

    def test_failed_source_switch_restores_previous_live_configuration(self):
        with tempfile.TemporaryDirectory() as root, patch.object(run_local, "start_collector", side_effect=[RuntimeError("unavailable"), None]) as start:
            runtime = Path(root)
            atomic_json(runtime / "stream-source.json", {"mode": "live", "since": None})
            with self.assertRaises(RuntimeError):
                run_local.change_stream_source(Mock(), runtime, {"mode": "since", "since": "2026-11-01T00:00:00Z"}, True)
            self.assertEqual(start.call_count, 2)
            self.assertEqual(read_json(runtime / "stream-source.json"), {"mode": "live", "since": None})
