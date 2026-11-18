"""Own at most one isolated Kafka replay process without touching the live stack."""

import os
import re
import signal
import subprocess
import sys
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dashboards.activity_data import ActivityReader, day_bounds
from src.stream_control import atomic_json, read_json


class KafkaReplays:
    def __init__(self, paths):
        self.paths = paths
        self.root = paths.runtime.parent / "replays"
        self.runs = {}
        self.lock = threading.Lock()

    def start(self, day, zone, speed):
        from src.kafka_common import is_file_broker_mode
        day_bounds(day, zone)
        if speed not in (1, 5, 20, 24, 48, 50, 100) or is_file_broker_mode():
            raise ValueError("Kafka replay requires the real Kafka broker and a supported speed.")
        with self.lock:
            if any(v["process"].poll() is None for v in self.runs.values()):
                raise ValueError("An isolated Kafka replay is already active. Finish or cancel it first.")
            while len(self.runs) >= 2:
                self.runs.pop(next(iter(self.runs)))
            source = self.paths.curated / "rc_events"
            form = "canonical"
            a, b = day_bounds(day, zone)
            if not any((source / ("day=" + datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d"))).is_dir()
                       for t in (a, b - 1)):
                source, form = self.paths.raw, "bronze"
            identity = uuid.uuid4().hex
            run = self.root / identity
            run.mkdir(parents=True)
            atomic_json(run / "status.json", {"id": identity, "status": "preparing", "day": day, "zone": zone,
                                               "speed": speed, "cursor": 0, "message": "Starting isolated Kafka and Spark replay"})
            atomic_json(run / "control.json", {})
            log = (run / "worker.log").open("w")
            env = {**os.environ, "SPARK_MASTER": "local[1]", "SPARK_DRIVER_MEMORY": "768m", "PYTHONUNBUFFERED": "1"}
            process = subprocess.Popen([sys.executable, "-m", "src.replay.session", "--input", str(source),
                                        "--format", form, "--run-dir", str(run), "--day", day, "--zone", zone,
                                        "--speed", str(speed)], stdout=log, stderr=subprocess.STDOUT, env=env,
                                       cwd=Path(__file__).resolve().parents[1], start_new_session=True)
            log.close()
            from src.dashboard_queries import DashboardPaths
            reader = ActivityReader(DashboardPaths(raw=run / "bronze", curated=run / "unused",
                                                   history=run / "unused", live=run / "unused", runtime=run))
            reader.replay_run = True
            self.runs[identity] = {"process": process, "reader": reader, "path": run}
            return self.status(identity)

    def status(self, identity):
        if not isinstance(identity, str) or not re.fullmatch(r"[0-9a-f]{32}", identity) or identity not in self.runs:
            raise ValueError("Unknown Kafka replay run.")
        run = self.runs[identity]
        state = read_json(run["path"] / "status.json")
        if run["process"].poll() is not None and state.get("status") not in ("complete", "cancelled", "failed"):
            state.update(status="failed", message="Replay worker stopped. See the retained worker.log.")
        return state

    def action(self, identity, action):
        state = self.status(identity)
        control = read_json(self.runs[identity]["path"] / "control.json")
        if action == "cancel":
            control["cancelled"] = True
        elif action == "pause":
            control["paused"] = not control.get("paused", False)
        else:
            raise ValueError("Unknown replay action.")
        atomic_json(self.runs[identity]["path"] / "control.json", control)
        return state

    def reader(self, identity):
        self.status(identity)
        return self.runs[identity]["reader"]

    def close(self):
        for identity, run in self.runs.items():
            if run["process"].poll() is None:
                self.action(identity, "cancel")
                try:
                    os.killpg(run["process"].pid, signal.SIGTERM)
                    run["process"].wait(timeout=20)
                except (OSError, subprocess.TimeoutExpired):
                    try:
                        os.killpg(run["process"].pid, signal.SIGKILL)
                    except OSError:
                        pass
