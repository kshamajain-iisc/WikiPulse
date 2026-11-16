"""Distinguish event-time catch-up from stalled collector/ingestion activity."""

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from src import local_supervisor

NOW = datetime(2026, 11, 16, 17, tzinfo=timezone.utc)


class StreamHealthTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.run_patch = patch.object(local_supervisor, "RUN_DIR", self.root)
        self.state_patch = patch.object(local_supervisor, "STATE_PATH", self.root / "local_supervisor.json")
        self.run_patch.start()
        self.state_patch.start()
        self.addCleanup(self.run_patch.stop)
        self.addCleanup(self.state_patch.stop)
        self.write("local_supervisor.json", {"state": "running", "services": {"collector": {"running": True}, "bronze": {"running": True}}})
        self.write("stream-source.json", {"mode": "since", "since": "2026-11-04T10:00:00Z"})
        self.write("collector-source.progress.json", {"since": "2026-11-04T10:00:00Z", "state": "connected",
                                                    "updated_at": NOW.isoformat(), "latest_event": "2026-11-09T14:00:00Z"})
        self.write("bronze.progress.json", {"timestamp": NOW.isoformat(), "numInputRows": 100})
        self.write("windows-5m.progress.json", {"eventTime": {"max": "2026-11-09T14:00:00Z"}})

    def write(self, name, value):
        (self.root / name).write_text(json.dumps(value))

    def health(self):
        with patch.object(local_supervisor, "datetime", wraps=datetime) as clock:
            clock.now.return_value = NOW
            return local_supervisor.stream_health()

    def test_fresh_capture_and_spark_ingestion_with_old_events_are_catching_up(self):
        health = self.health()
        self.assertEqual(health["state"], "catching_up")
        self.assertIn("CATCHING UP", health["label"])
        self.assertNotIn("STALE", health["label"])
        self.assertEqual(health["last_event"], "2026-11-09T14:00:00Z")

    def test_recent_source_events_return_to_live_even_if_finalized_windows_are_old(self):
        capture = json.loads((self.root / "collector-source.progress.json").read_text())
        capture["latest_event"] = (NOW - timedelta(seconds=20)).isoformat()
        self.write("collector-source.progress.json", capture)
        self.assertEqual(self.health()["state"], "running")
        self.assertTrue(self.health()["label"].startswith("LIVE"))

    def test_collector_stall_is_stale_even_with_recent_spark_batch(self):
        capture = json.loads((self.root / "collector-source.progress.json").read_text())
        capture["updated_at"] = (NOW - timedelta(seconds=121)).isoformat()
        self.write("collector-source.progress.json", capture)
        self.assertEqual(self.health()["state"], "stale")
        self.assertIn("collector activity", self.health()["label"])

    def test_stalled_spark_does_not_get_hidden_by_fresh_collector_activity(self):
        self.write("bronze.progress.json", {"timestamp": (NOW - timedelta(seconds=121)).isoformat()})
        self.assertEqual(self.health()["state"], "stale")
        self.assertIn("Spark ingestion", self.health()["label"])

    def test_broken_and_paused_states_take_precedence_over_fresh_capture(self):
        self.write("local_supervisor.json", {"state": "running", "services": {"bronze": {"running": False}}})
        self.assertEqual(self.health()["state"], "broken")
        for state in ("idle", "starting", "draining", "restarting"):
            self.write("local_supervisor.json", {"state": state, "services": {}})
            self.assertEqual(self.health()["state"], state)

    def test_reconnecting_does_not_claim_healthy_catch_up(self):
        capture = json.loads((self.root / "collector-source.progress.json").read_text())
        capture["state"] = "reconnecting"
        self.write("collector-source.progress.json", capture)
        self.assertEqual(self.health()["state"], "reconnecting")

    def test_waiting_for_the_first_event_does_not_claim_history_is_arriving(self):
        capture = json.loads((self.root / "collector-source.progress.json").read_text())
        capture["latest_event"] = None
        self.write("collector-source.progress.json", capture)
        self.assertEqual(self.health()["state"], "reconnecting")

    def test_previous_source_progress_cannot_claim_the_new_source_is_active(self):
        self.write("stream-source.json", {"mode": "live", "since": None})
        self.assertEqual(self.health()["state"], "reconnecting")

    def test_missing_or_invalid_progress_remains_unknown_and_stale(self):
        for value in (None, "bad", (NOW + timedelta(days=1)).isoformat()):
            with self.subTest(value=value):
                capture = {"since": "2026-11-04T10:00:00Z", "state": "connected", "updated_at": value}
                self.write("collector-source.progress.json", capture)
                self.assertEqual(self.health()["state"], "stale")

    def test_legacy_event_freshness_is_preserved_when_source_telemetry_is_absent(self):
        (self.root / "stream-source.json").unlink()
        (self.root / "collector-source.progress.json").unlink()
        self.assertEqual(self.health()["state"], "stale")
        self.write("windows-5m.progress.json", {"eventTime": {"max": NOW.isoformat()}})
        self.assertEqual(self.health()["state"], "running")
