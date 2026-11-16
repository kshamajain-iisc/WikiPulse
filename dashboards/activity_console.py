"""Bounded local console history and nonblocking, read-only ingestion telemetry."""

import json
import logging
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dashboards.activity_data import stamp
from src.stream_control import read_json


def data_size(value):
    if value is None:
        return "not measured"
    return f"{value / 1_000_000:.2f} MB" if value >= 1_000_000 else f"{value / 1_000:.2f} KB"


class ConsoleHistory:
    """Keep 500 visible events and at most three 1 MB local JSONL files."""

    def __init__(self, directory=None, limit=500, file_bytes=1_000_000):
        self.directory = Path(directory) if directory is not None else None
        self.entries = deque(maxlen=limit)
        self.file_bytes = file_bytes
        self._lock = threading.RLock()
        self.storage_error = False
        if self.directory is not None:
            for path in self._files():
                try:
                    for line in path.read_text(encoding="utf-8").splitlines():
                        try:
                            entry = json.loads(line)
                            if isinstance(entry, dict) and all(isinstance(entry.get(k), str) for k in ("time", "refresh", "stage", "message")):
                                self.entries.append(entry)
                        except (ValueError, TypeError):
                            continue  # A partial last line must not discard the rest.
                except OSError:
                    self.storage_error = True

    def _files(self):
        return [self.directory / name for name in ("history.2.jsonl", "history.1.jsonl", "history.jsonl")
                if (self.directory / name).exists()]

    def append(self, refresh, stage, message, metrics=None):
        entry = {"time": stamp(time.time()), "refresh": refresh, "stage": stage,
                 "message": message, "metrics": dict(metrics or {})}
        encoded = json.dumps(entry, ensure_ascii=False) + "\n"
        with self._lock:
            self.entries.append(entry)
            if self.directory is not None:
                try:
                    self.directory.mkdir(parents=True, exist_ok=True)
                    path = self.directory / "history.jsonl"
                    if path.exists() and path.stat().st_size + len(encoded.encode()) > self.file_bytes:
                        (self.directory / "history.2.jsonl").unlink(missing_ok=True)
                        older = self.directory / "history.1.jsonl"
                        if older.exists():
                            older.replace(self.directory / "history.2.jsonl")
                        path.replace(older)
                    # Separate an interrupted final write from the next full record.
                    partial = False
                    if path.exists() and path.stat().st_size:
                        with path.open("rb") as stream:
                            stream.seek(-1, 2)
                            partial = stream.read(1) != b"\n"
                    with path.open("a", encoding="utf-8") as stream:
                        if partial:
                            stream.write("\n")
                        stream.write(encoded)
                    self.storage_error = False
                except OSError:
                    if not self.storage_error:
                        logging.warning("Activity console file unavailable; retaining history in memory")
                    self.storage_error = True

    def text(self):
        with self._lock:
            lines = [f"{r['time'][:10]} {r['time'][11:19]} UTC · {r['refresh'][:8]} · {r['message']}"
                     for r in self.entries]
            if self.storage_error:
                lines.append("Local log file unavailable. History is currently kept in memory only.")
            return "\n".join(lines)

    def download(self):
        with self._lock:
            if self.directory is not None and not self.storage_error:
                try:
                    return "".join(p.read_text(encoding="utf-8") for p in self._files())
                except OSError:
                    pass
            return "".join(json.dumps(entry) + "\n" for entry in self.entries)


def probe_kafka():
    """Inspect broker metadata/offsets only; never consume, commit or create topics."""
    from confluent_kafka import Consumer, TopicPartition
    from src.kafka_common import KAFKA_SERVERS, KAFKA_TOPIC, is_file_broker_mode
    if is_file_broker_mode():
        return {"message": "Local file ingestion is enabled; Kafka broker is not in use."}
    observer = Consumer({"bootstrap.servers": KAFKA_SERVERS, "group.id": "wikipulse-console-observer",
                         "enable.auto.commit": False, "allow.auto.create.topics": False,
                         "broker.address.family": "v4", "log_level": 0})
    try:
        metadata = observer.list_topics(timeout=3)
        topic = metadata.topics.get(KAFKA_TOPIC)
        if topic is None or topic.error:
            raise RuntimeError("Capture topic unavailable")
        deadline = time.monotonic() + 3
        retained = 0
        for partition in topic.partitions:
            low, high = observer.get_watermark_offsets(TopicPartition(KAFKA_TOPIC, partition),
                                                       timeout=max(.1, deadline - time.monotonic()))
            if low < 0 or high < low:
                raise RuntimeError("Offsets unavailable")
            retained += high - low
        return {"message": f"Kafka reachable · {KAFKA_TOPIC} · {len(topic.partitions):,} partitions · "
                           f"approximately {retained:,} retained records (offset span; bytes not measured).",
                "retained_offset_span": retained, "partitions": len(topic.partitions)}
    finally:
        observer.close()


class IngestionMonitor:
    def __init__(self, runtime, probe=None):
        self.runtime = runtime
        self.probe = probe or probe_kafka
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="activity-telemetry")
        self._future = None
        self._sample = None
        self._next = 0
        self._lock = threading.RLock()

    def close(self):
        self._executor.shutdown(wait=True, cancel_futures=True)

    def _probe(self):
        try:
            sample = self.probe()
        except Exception:
            sample = {"message": "Kafka could not be verified; retained records and bytes are unknown."}
        return dict(sample, checked_at=stamp(time.time()))

    def snapshot(self):
        with self._lock:
            if self._future is not None and self._future.done():
                self._sample = self._future.result()
                self._future = None
                self._next = time.monotonic() + 30
            if self.runtime is not None and self._future is None and time.monotonic() >= self._next:
                self._future = self._executor.submit(self._probe)
            lines = []
            if self._sample:
                lines.append(self._sample["message"] + f" Checked {self._sample['checked_at'][11:19]} UTC.")
            else:
                lines.append("Checking Kafka in the background; retained records and bytes are not yet measured.")
            if self.runtime is not None:
                progress = read_json(self.runtime / "bronze.progress.json")
                state = read_json(self.runtime / "local_supervisor.json")
                lines.insert(0, f"Capture controller: {state.get('state', 'unavailable')}. Dashboard queries read saved event files.")
                if progress:
                    rows = progress.get("numInputRows")
                    ms = (progress.get("durationMs") or {}).get("triggerExecution", progress.get("batchDuration"))
                    row_text = f"{rows:,} input events" if isinstance(rows, (int, float)) else "input count not measured"
                    duration = f" in {ms / 1000:.2f} s" if isinstance(ms, (int, float)) else ""
                    lines.append(f"Latest reported ingestion batch: {row_text}{duration}; "
                                 f"reported at {progress.get('timestamp', 'unknown')}. This is a sampled batch, not a refresh total.")
            return lines
