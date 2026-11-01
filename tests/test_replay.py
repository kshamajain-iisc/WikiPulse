"""Replay contracts and deterministic synthetic ground truth without Kafka."""

import unittest
from datetime import datetime, timezone

from src.kafka_common import is_valid_edit
from src.replay.events import paced_envelopes, to_event
from src.replay.patterns import controlled_records


class ReplayTests(unittest.TestCase):
    def event(self, timestamp):
        return {"id": timestamp, "wiki": "enwiki", "type": "edit", "namespace": 0,
                "title": "Example", "timestamp": timestamp}

    def test_speed_and_original_timestamps(self):
        for speed in (1, 10, 50, 100):
            now = [0.0]

            def sleep(seconds):
                now[0] += seconds

            records = list(paced_envelopes([self.event(1000), self.event(1010)], speed,
                                          clock=lambda: now[0], sleep=sleep))
            self.assertAlmostEqual(now[0], 10 / speed)
            self.assertEqual(records[1]["event_id"], "enwiki:1010")
            self.assertEqual(records[1]["raw_event"]["timestamp"], 1010)
            self.assertTrue(is_valid_edit(records[0]["raw_event"]))

    def test_unpaced_order_validation_and_invalid_speed(self):
        for speed in (-1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                list(paced_envelopes([], speed))
        with self.assertRaisesRegex(ValueError, "ordered"):
            list(paced_envelopes([self.event(10), self.event(1)], 0))
        self.assertEqual(len(list(paced_envelopes([self.event(10)], 0))), 1)

    def test_canonical_adapter_and_fixture_repeatability(self):
        row = {"rcid": 1, "ts": datetime(2026, 10, 2, tzinfo=timezone.utc), "pageid": 10, "ns": 0, "title": "Example",
               "is_bot": False, "is_minor": False, "user": "Editor", "oldlen": 100, "newlen": 200,
               "tags": ["mw-undo"], "revid": 2, "old_revid": 1}
        event = to_event(row)
        self.assertEqual(event["revision"], {"old": 1, "new": 2})
        self.assertEqual(event["length"], {"old": 100, "new": 200})
        self.assertEqual(event["timestamp"], 1790899200)
        row["ts"] = datetime(2026, 10, 2, 5, 30)
        row["event_epoch"] = 1790899200
        self.assertEqual(to_event(row)["timestamp"], 1790899200)
        del row["event_epoch"]
        with self.assertRaisesRegex(ValueError, "event_epoch"):
            to_event(row)
        self.assertTrue(is_valid_edit(event))
        training, evaluation, labels = controlled_records()
        self.assertEqual((training, evaluation, labels), controlled_records())
        self.assertEqual(len(labels), 9)
        self.assertEqual(sum(label["expected_alert"] for label in labels), 6)
        self.assertEqual(len({r["rcid"] for r in training + evaluation}), len(training) + len(evaluation))
        self.assertFalse(next(label for label in labels if label["pattern"] == "normal_high_volume")["expected_alert"])


if __name__ == "__main__":
    unittest.main()
