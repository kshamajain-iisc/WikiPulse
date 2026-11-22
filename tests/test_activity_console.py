"""Console history lifecycle and honest, nonblocking ingestion measurements."""

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from dashboards.activity_console import ConsoleHistory, IngestionMonitor, probe_kafka
from dashboards.activity_jobs import ActivityWorkspace, TERMINAL
from dashboards.app import create_app


class ConsoleHistoryTests(unittest.TestCase):
    def test_rotation_is_bounded_and_restores_order_after_restart(self):
        with tempfile.TemporaryDirectory() as root:
            history = ConsoleHistory(root, limit=4, file_bytes=600)
            for index in range(30):
                history.append(str(index), "SUMMARY", "measured refresh " + str(index))
            paths = list(Path(root).glob("*.jsonl"))
            self.assertLessEqual(len(paths), 3)
            self.assertTrue(all(p.stat().st_size <= 600 for p in paths))
            restored = ConsoleHistory(root, limit=4, file_bytes=600)
            self.assertEqual([e["refresh"] for e in restored.entries], ["26", "27", "28", "29"])
            records = [json.loads(line) for line in restored.download().splitlines()]
            self.assertEqual(records[-1]["refresh"], "29")
            self.assertLess(len(records), 30)

    def test_partial_line_and_storage_failure_preserve_memory_history(self):
        with tempfile.TemporaryDirectory() as root:
            history = ConsoleHistory(root)
            history.append("first", "SUMMARY", "first refresh")
            with (Path(root) / "history.jsonl").open("a") as stream:
                stream.write('{"partial":')
            restored = ConsoleHistory(root)
            restored.append("second", "SUMMARY", "second refresh")
            again = ConsoleHistory(root)
            self.assertEqual([e["refresh"] for e in again.entries], ["first", "second"])
            unavailable = Path(root) / "not-a-directory"
            unavailable.write_text("fixture")
            fallback = ConsoleHistory(unavailable)
            with self.assertLogs(level="WARNING"):
                fallback.append("third", "SUMMARY", "third refresh")
            self.assertIn("third refresh", fallback.text())
            self.assertIn("in memory only", fallback.text())
            self.assertEqual(json.loads(fallback.download())["refresh"], "third")

    def test_each_refresh_appends_measured_summary_and_downloads_prior_requests(self):
        with tempfile.TemporaryDirectory() as root, patch("dashboards.activity_console.probe_kafka", return_value={"message": "Kafka fixture"}):
            def read(spec, emit, cancelled):
                emit("READ", "fixture", rows_fetched=1000, arrow_payload_bytes=2_500_000)
                emit("AGGREGATE", "fixture", events_analyzed=900, events_matched=800)
                return {"metrics": {"edits": 800}}
            reader = Mock()
            reader.read.side_effect = read
            reader.evidence.return_value = {}
            workspace = ActivityWorkspace(SimpleNamespace(runtime=Path(root)), reader)
            try:
                first = workspace.submit({}, "browser")
                deadline = time.monotonic() + 3
                while workspace.get(first["id"])["status"] not in TERMINAL and time.monotonic() < deadline:
                    time.sleep(.005)
                self.assertEqual(workspace.get(first["id"])["status"], "complete")
                initial = workspace.history.text()
                second = workspace.submit({}, "browser")
                self.assertEqual(second["status"], "complete")
                self.assertTrue(workspace.history.text().startswith(initial))
                summaries = [r for r in workspace.history.entries if r["stage"] == "SUMMARY"]
                self.assertEqual(len(summaries), 2)
                self.assertIn("1,000 rows fetched", summaries[0]["message"])
                self.assertIn("900 events analyzed", summaries[0]["message"])
                self.assertIn("2.50 MB decoded", summaries[0]["message"])
                self.assertGreaterEqual(summaries[0]["metrics"]["processing_seconds"], 0)
                self.assertIn("0 rows fetched", summaries[1]["message"])
                self.assertIn("0 events analyzed", summaries[1]["message"])
                self.assertIn("cached result", summaries[1]["message"])
                app = create_app(Mock(), activity_workspace=workspace)
                download = app.server.test_client().get("/api/activity/console/log")
                self.assertEqual(download.status_code, 200)
                self.assertIn(b"attachment", download.headers["Content-Disposition"].encode())
                records = [json.loads(line) for line in download.text.splitlines()]
                self.assertEqual({r["refresh"] for r in records}, {first["id"], second["id"]})
                self.assertEqual(app.server.test_client().get(f"/api/activity/jobs/{first['id']}/log").status_code, 200)
            finally:
                workspace.close()
            restored = ConsoleHistory(Path(root) / "activity-console")
            self.assertEqual(len([r for r in restored.entries if r["stage"] == "SUMMARY"]), 2)


class IngestionMonitorTests(unittest.TestCase):
    def test_probe_does_not_block_refresh_and_samples_actual_batch_once(self):
        entered, release = threading.Event(), threading.Event()
        def probe():
            entered.set()
            release.wait(2)
            return {"message": "Kafka reachable · approximately 42 retained records"}
        with tempfile.TemporaryDirectory() as root:
            runtime = Path(root)
            (runtime / "bronze.progress.json").write_text(json.dumps({"numInputRows": 20, "durationMs": {"triggerExecution": 1250}, "timestamp": "fixture"}))
            monitor = IngestionMonitor(runtime, probe)
            try:
                before = time.monotonic()
                lines = monitor.snapshot()
                self.assertLess(time.monotonic() - before, .1)
                self.assertTrue(entered.wait(1))
                self.assertIn("20 input events in 1.25 s", " ".join(lines))
                self.assertIn("not yet measured", " ".join(lines))
                self.assertNotIn("windows-5m", " ".join(lines))
                release.set()
                monitor._future.result(timeout=1)
                self.assertIn("42 retained records", " ".join(monitor.snapshot()))
                self.assertIsNone(monitor._future)
            finally:
                release.set()
                monitor.close()

    def test_unavailable_broker_does_not_claim_running_or_zero_bytes(self):
        monitor = IngestionMonitor(Path("/nonexistent"), Mock(side_effect=RuntimeError("offline")))
        try:
            monitor.snapshot()
            try:
                monitor._future.result(timeout=1)
            except RuntimeError:
                pass
            message = " ".join(monitor.snapshot())
            self.assertIn("could not be verified", message)
            self.assertIn("bytes are unknown", message)
            self.assertNotIn("Kafka reachable", message)
        finally:
            monitor.close()

    def test_watermark_probe_uses_retained_span_without_consuming_or_creating(self):
        observer = Mock()
        observer.list_topics.return_value.topics = {"wiki.raw.edits": SimpleNamespace(error=None, partitions={0: None, 1: None})}
        observer.get_watermark_offsets.side_effect = [(90, 100), (50, 70)]
        with patch("confluent_kafka.Consumer", return_value=observer) as consumer, patch("src.kafka_common.is_file_broker_mode", return_value=False), patch("src.kafka_common.KAFKA_TOPIC", "wiki.raw.edits"):
            state = probe_kafka()
        self.assertEqual(state["retained_offset_span"], 30)
        self.assertIn("approximately 30 retained records", state["message"])
        self.assertFalse(consumer.call_args.args[0]["allow.auto.create.topics"])
        observer.poll.assert_not_called()
        observer.commit.assert_not_called()
        observer.subscribe.assert_not_called()
        observer.close.assert_called_once()
