"""Supervise Kafka, the collector, Spark measurements, and the local dashboard.

Use ./run-local.sh to bootstrap dependencies and invoke this module. Ctrl+C stops
all child processes and stops Kafka if this invocation started it. Data is retained.
"""

import argparse
import fcntl
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Callable
from urllib.error import URLError
from urllib.request import urlopen

from src.kafka_common import KAFKA_SERVERS, KAFKA_TOPIC

ROOT = Path(__file__).resolve().parents[1]
LIVE_DIRECTORY = ROOT / "data/live_activity"
LOG_DIRECTORY = ROOT / "logs/local"
STARTUP_TIMEOUT_SECONDS = 240


def run_command(arguments: list[str], timeout: int = 30) -> str:
    """Run a setup command and report a concise failure without hiding its cause."""
    result = subprocess.run(
        arguments,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"{' '.join(arguments)} failed:\n{detail}")
    return result.stdout.strip()


def validate_prerequisites(port: int) -> None:
    """Check local configuration before starting any services."""
    for executable in ("docker", "java"):
        if shutil.which(executable) is None:
            raise RuntimeError(f"Install {executable} and ensure it is on PATH before starting.")
    run_command(["java", "-version"])
    run_command(["docker", "compose", "version"])
    try:
        run_command(["docker", "info", "--format", "{{.ServerVersion}}"], timeout=10)
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        raise RuntimeError(
            "The container engine is unavailable. Start Docker Desktop, or run "
            "'podman machine start' if your Docker context uses Podman, then retry."
        ) from error

    # Compose advertises this local address; accepting another broker here would
    # start one Kafka instance while sending data to a different instance.
    if KAFKA_SERVERS not in ("localhost:9092", "127.0.0.1:9092"):
        raise RuntimeError("Local startup requires KAFKA_BOOTSTRAP_SERVERS=localhost:9092.")
    if not os.getenv("SPARK_MASTER", "local[2]").startswith("local"):
        raise RuntimeError("Local startup requires SPARK_MASTER=local[2] (or another local mode).")
    user_agent = os.getenv("WIKIMEDIA_USER_AGENT", "")
    if not user_agent or "example.com" in user_agent:
        raise RuntimeError("Set a real WIKIMEDIA_USER_AGENT contact in .env before starting.")
    with socket.socket() as listener:
        # Match HTTPServer's address reuse behavior so TIME_WAIT from a clean
        # dashboard shutdown does not incorrectly block an immediate restart.
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind(("127.0.0.1", port))
        except OSError as error:
            raise RuntimeError(f"Dashboard port {port} is busy; choose another with --port.") from error


class LocalProcesses:
    """Own process groups so Spark JVMs are stopped along with their Python drivers."""

    def __init__(self, resources: ExitStack):
        self.resources = resources
        self.children: list[tuple[str, subprocess.Popen]] = []

    def start(self, name: str, module: str, *arguments: str) -> None:
        """Start one service with a separate readable log file."""
        log_path = LOG_DIRECTORY / f"{name}.log"
        log_file = self.resources.enter_context(log_path.open("a"))
        environment = os.environ.copy()
        environment["PYTHONUNBUFFERED"] = "1"
        environment.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        process = subprocess.Popen(
            [sys.executable, "-m", module, *arguments],
            cwd=ROOT,
            env=environment,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.children.append((name, process))
        print(f"Started {name}; log: {log_path}", flush=True)

    def check_running(self) -> None:
        """Fail promptly if any required service exits, even with exit code zero."""
        for name, process in self.children:
            exit_code = process.poll()
            if exit_code is not None:
                raise RuntimeError(
                    f"{name} exited with code {exit_code}; check {LOG_DIRECTORY / (name + '.log')}"
                )

    def wait_for(self, description: str, ready: Callable[[], bool]) -> None:
        """Wait for a concrete readiness condition while supervising started children."""
        print(f"Waiting for {description}…", flush=True)
        deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            self.check_running()
            if ready():
                return
            time.sleep(1)
        raise RuntimeError(f"Timed out waiting for {description}; check {LOG_DIRECTORY}.")

    def stop(self) -> None:
        """Stop all owned groups, then escalate only groups that do not exit."""
        # Signal every group first so shutdown time does not grow per service.
        for shutdown_signal, grace_seconds in (
            (signal.SIGINT, 20),
            (signal.SIGTERM, 5),
            (signal.SIGKILL, 5),
        ):
            for _, process in reversed(self.children):
                # A driver may already have exited while its Spark JVM survives.
                try:
                    os.killpg(process.pid, shutdown_signal)
                except ProcessLookupError:
                    pass
            deadline = time.monotonic() + grace_seconds
            for _, process in self.children:
                try:
                    process.wait(timeout=max(0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    pass
            if all(process.poll() is not None for _, process in self.children):
                # Terminate any remaining descendants of an already exited driver.
                for _, process in self.children:
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                return


def raw_output_ready(schema_path: Path, raw_directory: Path) -> bool:
    """Wait for a committed raw batch, ignoring temporary files during writes."""
    metadata_directory = raw_directory / "_spark_metadata"
    if not schema_path.is_file() or not metadata_directory.is_dir():
        return False
    # Spark names committed file-sink logs 0, 1, ... and compacted logs N.compact.
    # The log may commit an empty batch, which is sufficient to start downstream
    # readers because the raw directory and its explicit schema now exist.
    for entry in metadata_directory.iterdir():
        batch_number = entry.name.split(".")[0]
        if batch_number.isdigit() and entry.is_file():
            return True
    return False


def dashboard_ready(port: int) -> bool:
    """Check the lightweight HTML route without triggering a Spark query."""
    try:
        with urlopen(f"http://127.0.0.1:{port}/", timeout=2) as response:
            return response.status == 200
    except (URLError, TimeoutError):
        return False


def main() -> int:
    """Start services in dependency order and retain logs/data after shutdown."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--check", action="store_true", help="Check prerequisites without starting services")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("Port must be between 1 and 65535")

    # A process lock prevents a second launcher from sharing active checkpoints.
    lock_directory = ROOT / "data/run"
    lock_directory.mkdir(parents=True, exist_ok=True)
    with ExitStack() as resources:
        lock_file = resources.enter_context((lock_directory / "local.lock").open("a"))
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("WikiPulse is already running through this launcher.", file=sys.stderr)
            return 1

        # Service managers commonly use SIGTERM; handle it like terminal Ctrl+C.
        def request_shutdown(signum, frame):
            raise KeyboardInterrupt

        previous_handler = signal.signal(signal.SIGTERM, request_shutdown)
        resources.callback(signal.signal, signal.SIGTERM, previous_handler)
        processes = LocalProcesses(resources)
        owns_kafka = False
        exit_code = 0
        try:
            validate_prerequisites(args.port)
            if args.check:
                print("Local prerequisites are ready.")
                return 0
            LOG_DIRECTORY.mkdir(parents=True, exist_ok=True)
            was_running = run_command([
                "docker", "compose", "ps", "--status", "running", "-q", "kafka"
            ])
            owns_kafka = not bool(was_running)
            print("Starting Kafka and waiting for broker health…", flush=True)
            run_command(
                ["docker", "compose", "up", "-d", "--wait", "--wait-timeout", "180", "kafka"],
                timeout=STARTUP_TIMEOUT_SECONDS,
            )
            # Topic auto-creation is disabled in Compose. The command is safe on
            # repeat starts and retains any existing topic and its messages.
            run_command([
                "docker", "compose", "exec", "-T", "kafka",
                "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
                "--create", "--if-not-exists", "--topic", KAFKA_TOPIC,
                "--partitions", "3", "--replication-factor", "1",
            ])

            processes.start("dashboard", "src.activity_dashboard", "--port", str(args.port))
            processes.wait_for("the dashboard server", lambda: dashboard_ready(args.port))
            print(f"Dashboard: http://127.0.0.1:{args.port}/", flush=True)
            processes.start("collector", "src.collector")
            bronze_ready = lock_directory / "bronze.ready"
            bronze_ready.unlink(missing_ok=True)
            processes.start(
                "bronze", "src.stream_measurements", "bronze", "--ready-file", str(bronze_ready)
            )
            processes.wait_for("the raw ingestion query", bronze_ready.is_file)

            raw_path = (ROOT / os.getenv("RAW_PARQUET_PATH", "data/raw/wikipedia_edits")).resolve()
            schema_path = LIVE_DIRECTORY / "bronze_schema.json"
            # Windows need the producer schema and a committed raw batch, not just
            # a started Python process. First-time connector downloads may take time.
            processes.wait_for(
                "the first committed raw Spark batch",
                lambda: raw_output_ready(schema_path, raw_path),
            )
            for duration in ("5m", "1h"):
                ready_file = lock_directory / f"windows-{duration}.ready"
                ready_file.unlink(missing_ok=True)
                processes.start(
                    f"windows-{duration}", "src.stream_measurements", "windows",
                    "--duration", duration, "--ready-file", str(ready_file),
                )
                processes.wait_for(f"the {duration} measurement query", ready_file.is_file)
            print(
                "All services launched. Select 5m or 1h on the dashboard. "
                "Finalized results require later events to advance the watermark.\n"
                "Press Ctrl+C to stop. Logs are in logs/local/.",
                flush=True,
            )
            while True:
                processes.check_running()
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopping local services…", flush=True)
        except (RuntimeError, OSError, subprocess.TimeoutExpired) as error:
            print(f"Startup/runtime error: {error}", file=sys.stderr)
            exit_code = 1
        finally:
            processes.stop()
            if owns_kafka:
                try:
                    run_command(["docker", "compose", "stop", "kafka"], timeout=45)
                except (RuntimeError, OSError, subprocess.TimeoutExpired) as error:
                    print(f"Could not stop Kafka: {error}", file=sys.stderr)
        return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
