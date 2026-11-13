"""Supervise Kafka, the collector, Spark measurements, and the local dashboard.

Use ./run-local.sh to bootstrap dependencies and invoke this module. Ctrl+C stops
all child processes and stops Kafka if this invocation started it. Data is retained.
"""

import argparse
import json
import fcntl
import os
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.error import URLError
from urllib.request import urlopen

from src.kafka_common import KAFKA_ALERT_TOPIC, KAFKA_SERVERS, KAFKA_TOPIC

ROOT = Path(__file__).resolve().parents[1]
LIVE_DIRECTORY = ROOT / "data/live_activity"
LOG_DIRECTORY = ROOT / "logs/local"
SUPERVISOR_STATE = ROOT / "data/run/local_supervisor.json"
RESTART_REQUEST = ROOT / "data/run/restart_stream.request.json"
STREAM_SERVICES = (
    "collector", "bronze", "windows-5m", "windows-1h", "detection-5m", "detection-1h",
)
STARTUP_TIMEOUT_SECONDS = 240


def write_supervisor_state(processes, state: str, error: str | None = None) -> None:
    """Publish process liveness for the local dashboard without exposing credentials."""
    services = {
        name: {"pid": process.pid, "running": process.poll() is None}
        for name, process in processes.children
    }
    payload = {
        "state": state,
        "supervisor_pid": os.getpid(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "services": services,
        "error": error,
    }
    temporary = SUPERVISOR_STATE.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload))
    temporary.replace(SUPERVISOR_STATE)


def start_stream_services(processes, lock_directory: Path) -> None:
    """Start/restart the stream children in checkpoint-safe dependency order."""
    collector_log = LOG_DIRECTORY / "collector.log"
    collector_log_offset = collector_log.stat().st_size if collector_log.exists() else 0
    processes.start("collector", "src.collector")
    processes.wait_for(
        "the Wikimedia EventStreams connection",
        lambda: log_contains_since(collector_log, collector_log_offset, "EventStreams connected"),
    )
    bronze_ready = lock_directory / "bronze.ready"
    bronze_ready.unlink(missing_ok=True)
    processes.start("bronze", "src.stream_measurements", "bronze", "--ready-file", str(bronze_ready))
    processes.wait_for("the raw ingestion query", bronze_ready.is_file)

    raw_path = (ROOT / os.getenv("RAW_PARQUET_PATH", "data/raw/wikipedia_edits")).resolve()
    schema_path = LIVE_DIRECTORY / "bronze_schema.json"
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
    for duration in ("5m", "1h"):
        ready_file = lock_directory / f"detection-{duration}.ready"
        ready_file.unlink(missing_ok=True)
        processes.start(
            f"detection-{duration}", "src.detection", "stream",
            "--duration", duration, "--ready-file", str(ready_file),
        )
        processes.wait_for(f"the {duration} alert query", ready_file.is_file)


def log_contains_since(path: Path, offset: int, text: str) -> bool:
    """Match a fresh service log entry, ignoring messages from older runs."""
    try:
        with path.open("r") as log_file:
            log_file.seek(offset)
            return text in log_file.read()
    except OSError:
        return False


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


def validate_prerequisites(port: int, *, check_port: bool = True) -> None:
    """Check local configuration before starting any services."""
    for executable in ("docker", "java"):
        if shutil.which(executable) is None:
            raise RuntimeError(f"Install {executable} and ensure it is on PATH before starting.")
    run_command(["java", "-version"])
    run_command(["docker", "compose", "version"])
    try:
        run_command(["docker", "info", "--format", "{{.ServerVersion}}"], timeout=10)
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        try:
            context = run_command(["docker", "context", "show"], timeout=5)
        except (RuntimeError, subprocess.TimeoutExpired):
            context = "default"
        if context == "colima":
            recovery = "Restart Colima with 'colima stop && colima start'."
        elif context == "desktop-linux":
            recovery = "Start Docker Desktop and wait for its engine to become ready."
        elif "podman" in context.lower():
            recovery = "Start the Podman VM with 'podman machine start'."
        else:
            recovery = f"Start the Docker engine for context '{context}'."
        raise RuntimeError(
            f"The Docker engine for context '{context}' is unavailable. {recovery} Then retry."
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
    if check_port:
        check_port_available(port)


def check_port_available(port: int) -> None:
    """Fail if a process still owns the dashboard port after cleanup."""
    with socket.socket() as listener:
        # Match HTTPServer's address reuse behavior so TIME_WAIT from a clean
        # dashboard shutdown does not incorrectly block an immediate restart.
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind(("127.0.0.1", port))
        except OSError as error:
            raise RuntimeError(f"Dashboard port {port} is busy; choose another with --port.") from error


def read_supervisor_state() -> dict:
    try:
        return json.loads(SUPERVISOR_STATE.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def lock_is_held(lock_file) -> bool:
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    fcntl.flock(lock_file, fcntl.LOCK_UN)
    return False


def process_parent_pid(pid: int) -> int | None:
    """Return a process's parent PID on macOS/Linux, if it is still running."""
    try:
        result = subprocess.run(
            ["ps", "-o", "ppid=", "-p", str(pid)],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=5,
        )
        return int(result.stdout.strip()) if result.returncode == 0 else None
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return None


def is_wikipulse_service(pid: int) -> bool:
    """Guard stale PID cleanup against signaling an unrelated reused PID."""
    try:
        result = subprocess.run(
            ["ps", "-o", "command=", "-p", str(pid)],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    command = result.stdout.strip()
    return str(ROOT) in command and any(
        marker in command for marker in (
            "src.collector", "src.stream_measurements", "src.detection", "dashboards.app",
        )
    )


def stop_process_group(pid: int, description: str, timeout: float = 25) -> None:
    """Gracefully stop a known WikiPulse process group, then force it if needed."""
    try:
        os.killpg(pid, signal.SIGINT)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.killpg(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.25)
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    time.sleep(1)
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    print(f"Stopped previous {description} (PID {pid}).", flush=True)


def listener_pids(port: int) -> list[int]:
    """Find listener PIDs using lsof when available (macOS/Linux)."""
    if shutil.which("lsof") is None:
        return []
    try:
        result = subprocess.run(
            ["lsof", "-nP", "-t", f"-iTCP:{port}", "-sTCP:LISTEN"],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=5,
        )
        return sorted({int(line) for line in result.stdout.splitlines() if line.strip().isdigit()})
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return []


def listener_is_from_repo(pid: int) -> bool:
    """Use the listener's working directory to identify direct demo launches."""
    if shutil.which("lsof") is None:
        return False
    try:
        result = subprocess.run(
            ["lsof", "-nP", "-p", str(pid)],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return any(
        line.split()[-1] == str(ROOT)
        for line in result.stdout.splitlines()
        if " cwd " in line
    )


def process_descendants(root_pid: int) -> list[int]:
    """Collect a process tree so direct Dash launches do not orphan Spark JVMs."""
    try:
        result = subprocess.run(
            ["ps", "-axo", "pid=,ppid="],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    children: dict[int, list[int]] = {}
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0].isdigit() and fields[1].isdigit():
            children.setdefault(int(fields[1]), []).append(int(fields[0]))
    ordered: list[int] = []
    pending = list(children.get(root_pid, ()))
    while pending:
        pid = pending.pop(0)
        ordered.append(pid)
        pending.extend(children.get(pid, ()))
    return list(reversed(ordered))


def stop_previous_stack(lock_file, port: int) -> None:
    """Stop the prior managed stack and a WikiPulse dashboard left on its port."""
    state = read_supervisor_state()
    if lock_is_held(lock_file):
        supervisor_pid = state.get("supervisor_pid")
        if not isinstance(supervisor_pid, int):
            # Older supervisors did not publish their own PID. Their service
            # children were started by the supervisor, so recover it from one.
            for service in state.get("services", {}).values():
                child_pid = service.get("pid") if isinstance(service, dict) else None
                if isinstance(child_pid, int):
                    supervisor_pid = process_parent_pid(child_pid)
                    if supervisor_pid:
                        break
        if not isinstance(supervisor_pid, int):
            raise RuntimeError(
                "The WikiPulse launcher lock is held, but its process could not be identified safely. "
                "Stop the existing WikiPulse launcher and retry."
            )
        try:
            os.kill(supervisor_pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + 35
        while time.monotonic() < deadline and lock_is_held(lock_file):
            time.sleep(0.25)
        if lock_is_held(lock_file):
            raise RuntimeError(f"Previous WikiPulse launcher (PID {supervisor_pid}) did not stop.")
        print(f"Stopped previous WikiPulse launcher (PID {supervisor_pid}).", flush=True)
    else:
        # Recover orphaned children if an earlier launcher was killed without
        # running its cleanup handler. State contains only this project's PIDs.
        for service in state.get("services", {}).values():
            pid = service.get("pid") if isinstance(service, dict) else None
            if isinstance(pid, int) and pid > 1 and service.get("running"):
                if not is_wikipulse_service(pid):
                    continue
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    continue
                stop_process_group(pid, "WikiPulse service")

    # The backup dashboard is sometimes launched directly, outside the local
    # supervisor. Replace it only when lsof identifies this repo as its cwd.
    for pid in listener_pids(port):
        if not listener_is_from_repo(pid):
            raise RuntimeError(
                f"Dashboard port {port} is occupied by another process (PID {pid}); "
                "stop it before starting WikiPulse."
            )
        descendants = process_descendants(pid)
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
        # A directly launched Dash backup demo can own a separate PySpark JVM.
        # Stop the captured descendants too, since they are outside our process
        # supervisor's session and would otherwise survive the dashboard PID.
        for child_pid in descendants:
            try:
                os.kill(child_pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if pid not in listener_pids(port):
                break
            time.sleep(0.25)
        if pid in listener_pids(port):
            os.kill(pid, signal.SIGKILL)
        print(f"Stopped previous WikiPulse dashboard (PID {pid}).", flush=True)


class LocalProcesses:
    """Own process groups so Spark JVMs are stopped along with their Python drivers."""

    def __init__(self, resources: ExitStack):
        self.resources = resources
        self.children: list[tuple[str, subprocess.Popen]] = []

    def start(self, name: str, module: str, *arguments: str, extra_env: dict | None = None) -> None:
        """Start one service with a separate readable log file."""
        log_path = LOG_DIRECTORY / f"{name}.log"
        log_file = self.resources.enter_context(log_path.open("a"))
        environment = os.environ.copy()
        environment["PYTHONUNBUFFERED"] = "1"
        environment.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        environment.update(extra_env or {})
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

    def stop_named(self, names: tuple[str, ...]) -> None:
        """Stop and forget selected child groups, preserving their output/checkpoints."""
        targets = [(name, process) for name, process in self.children if name in names]
        for shutdown_signal, grace_seconds in (
            (signal.SIGINT, 20), (signal.SIGTERM, 5), (signal.SIGKILL, 5),
        ):
            for _, process in reversed(targets):
                try:
                    os.killpg(process.pid, shutdown_signal)
                except ProcessLookupError:
                    pass
            deadline = time.monotonic() + grace_seconds
            for _, process in targets:
                try:
                    process.wait(timeout=max(0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    pass
            if all(process.poll() is not None for _, process in targets):
                break
        self.children = [(name, process) for name, process in self.children if name not in names]

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
        if args.check:
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
            validate_prerequisites(args.port, check_port=args.check)
            if args.check:
                print("Local prerequisites are ready.")
                return 0
            stop_previous_stack(lock_file, args.port)
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("Another WikiPulse launcher started at the same time; retry.")
            check_port_available(args.port)
            LOG_DIRECTORY.mkdir(parents=True, exist_ok=True)
            was_running = run_command([
                "docker", "compose", "ps", "--status", "running", "-q", "kafka"
            ])
            if was_running:
                print("Stopping the existing WikiPulse Kafka broker before restart…", flush=True)
                run_command(["docker", "compose", "stop", "kafka"], timeout=45)
            owns_kafka = True
            print("Starting Kafka and waiting for broker health…", flush=True)
            run_command(
                ["docker", "compose", "up", "-d", "--wait", "--wait-timeout", "180", "kafka"],
                timeout=STARTUP_TIMEOUT_SECONDS,
            )
            # Topic auto-creation is disabled in Compose. The command is safe on
            # repeat starts and retains any existing topic and its messages.
            for topic in dict.fromkeys((KAFKA_TOPIC, KAFKA_ALERT_TOPIC)):
                run_command([
                    "docker", "compose", "exec", "-T", "kafka",
                    "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
                    "--create", "--if-not-exists", "--topic", topic,
                    "--partitions", "3", "--replication-factor", "1",
                ])

            control_token = secrets.token_urlsafe(32)
            processes.start(
                "dashboard", "dashboards.app", "--port", str(args.port),
                extra_env={"WIKIPULSE_CONTROL_TOKEN": control_token},
            )
            processes.wait_for("the dashboard server", lambda: dashboard_ready(args.port))
            print(f"Dashboard: http://127.0.0.1:{args.port}/", flush=True)
            write_supervisor_state(processes, "starting")
            start_stream_services(processes, lock_directory)
            write_supervisor_state(processes, "running")
            print(
                "All services launched. Select 5m or 1h on the dashboard. "
                "Finalized results require later events to advance the watermark.\n"
                "Press Ctrl+C to stop. Logs are in logs/local/.",
                flush=True,
            )
            while True:
                try:
                    processes.check_running()
                    write_supervisor_state(processes, "running")
                except RuntimeError as error:
                    write_supervisor_state(processes, "broken", str(error))

                if RESTART_REQUEST.exists():
                    RESTART_REQUEST.unlink(missing_ok=True)
                    write_supervisor_state(processes, "restarting")
                    try:
                        processes.stop_named(STREAM_SERVICES)
                        start_stream_services(processes, lock_directory)
                        write_supervisor_state(processes, "running")
                        print("Stream services restarted; checkpoints and stored data were preserved.", flush=True)
                    except (RuntimeError, OSError, subprocess.TimeoutExpired) as error:
                        write_supervisor_state(processes, "broken", str(error))
                        print(f"Stream restart failed: {error}", file=sys.stderr, flush=True)
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopping local services…", flush=True)
        except (RuntimeError, OSError, subprocess.TimeoutExpired) as error:
            print(f"Startup/runtime error: {error}", file=sys.stderr)
            exit_code = 1
        finally:
            processes.stop()
            write_supervisor_state(processes, "stopped")
            if owns_kafka:
                try:
                    run_command(["docker", "compose", "stop", "kafka"], timeout=45)
                except (RuntimeError, OSError, subprocess.TimeoutExpired) as error:
                    print(f"Could not stop Kafka: {error}", file=sys.stderr)
        return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
