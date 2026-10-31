"""Check launcher ownership and failure cleanup without starting external services."""

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import run_local


class LocalLauncherTests(unittest.TestCase):
    def invoke_launcher(self, broker_already_running=False, startup_failure=False):
        """Simulate readiness, then interrupt the running stack or fail startup."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch.object(run_local, "ROOT", root),
                patch.object(run_local, "LOG_DIRECTORY", root / "logs"),
                patch.object(run_local, "validate_prerequisites"),
                patch.object(run_local, "LocalProcesses") as supervisor_class,
                patch.object(run_local, "run_command") as command,
                patch("sys.argv", ["run_local"]),
            ):
                # Only the first command queries prior ownership of the broker.
                command.side_effect = ["broker-id" if broker_already_running else "", "", "", ""]
                supervisor = supervisor_class.return_value
                if startup_failure:
                    supervisor.wait_for.side_effect = RuntimeError("startup failed")
                else:
                    supervisor.check_running.side_effect = KeyboardInterrupt
                result = run_local.main()
                return result, command.call_args_list, supervisor

    def test_starts_all_services_and_stops_owned_broker(self):
        result, commands, supervisor = self.invoke_launcher()
        self.assertEqual(result, 0)
        self.assertEqual(
            [call.args[0] for call in supervisor.start.call_args_list],
            ["dashboard", "collector", "bronze", "windows-5m", "windows-1h"],
        )
        supervisor.stop.assert_called_once()
        self.assertEqual(commands[-1].args[0], ["docker", "compose", "stop", "kafka"])

    def test_keeps_a_previously_running_broker(self):
        result, commands, supervisor = self.invoke_launcher(broker_already_running=True)
        self.assertEqual(result, 0)
        supervisor.stop.assert_called_once()
        self.assertFalse(any("stop" in call.args[0] for call in commands))

    def test_startup_failure_cleans_up_started_services(self):
        result, commands, supervisor = self.invoke_launcher(startup_failure=True)
        self.assertEqual(result, 1)
        supervisor.stop.assert_called_once()
        self.assertEqual(commands[-1].args[0], ["docker", "compose", "stop", "kafka"])

    def test_raw_readiness_requires_a_committed_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schema = root / "schema.json"
            schema.write_text("{}")
            raw = root / "raw"
            metadata = raw / "_spark_metadata"
            metadata.mkdir(parents=True)
            self.assertFalse(run_local.raw_output_ready(schema, raw))
            (metadata / ".0.crc").touch()
            self.assertFalse(run_local.raw_output_ready(schema, raw))
            (metadata / "0").write_text("committed")
            self.assertTrue(run_local.raw_output_ready(schema, raw))

    def test_setup_command_reports_failure(self):
        failure = subprocess.CompletedProcess(["docker"], 1, stdout="", stderr="engine unavailable")
        with patch.object(run_local.subprocess, "run", return_value=failure):
            with self.assertRaisesRegex(RuntimeError, "engine unavailable"):
                run_local.run_command(["docker", "info"])

    def test_child_failure_is_reported(self):
        from contextlib import ExitStack
        from unittest.mock import Mock

        with ExitStack() as resources:
            supervisor = run_local.LocalProcesses(resources)
            child = Mock()
            child.poll.return_value = 7
            supervisor.children.append(("bronze", child))
            with self.assertRaisesRegex(RuntimeError, "bronze exited with code 7"):
                supervisor.check_running()


if __name__ == "__main__":
    unittest.main()
