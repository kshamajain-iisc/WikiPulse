"""Exercise dashboard routing without running Spark or a real HTTP server."""

import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from src.activity_dashboard import create_request_handler


class DashboardRouteTests(unittest.TestCase):
    def request(self, path):
        handler_class = create_request_handler(None, Path("live"), Path("history"), "5m")
        # Build the handler without opening a socket; its route method is the same
        # one used by HTTPServer, with response output replaced by a recording mock.
        handler = object.__new__(handler_class)
        handler.path = path
        handler.send_payload = Mock()
        return handler

    def test_hourly_api_selects_hourly_tables(self):
        handler = self.request("/api/activity?duration=1h")
        with patch("src.activity_dashboard.read_activity", return_value={"rows": []}) as read:
            handler.do_GET()
        self.assertEqual(read.call_args.args[3], "1h")
        self.assertEqual(handler.send_payload.call_args.args[-1], 200)

    def test_invalid_duration_is_rejected(self):
        handler = self.request("/api/activity?duration=bad")
        handler.do_GET()
        self.assertEqual(handler.send_payload.call_args.kwargs["status"], 400)

    def test_html_carries_the_selected_duration(self):
        handler = self.request("/?duration=1h")
        handler.do_GET()
        page = handler.send_payload.call_args.args[0].decode()
        self.assertIn('data-duration="1h"', page)
        self.assertNotIn("{{DURATION}}", page)


if __name__ == "__main__":
    unittest.main()
