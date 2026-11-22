"""Small asynchronous request queue and measured progress for activity screens."""

import copy
import json
import logging
import threading
import time
import uuid
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

from dashboards.activity_data import ActivityReader, normalize_spec, stamp
from dashboards.activity_console import ConsoleHistory, IngestionMonitor, data_size

TERMINAL = {"complete", "failed", "cancelled"}


class ActivityWorkspace:
    def __init__(self, paths, reader=None):
        self.reader = reader or ActivityReader(paths)
        self.paths = paths
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="activity-query")
        self._lock = threading.RLock()
        self._jobs = OrderedDict()
        self._owners = {}
        self._cache = {"live": OrderedDict(), "stored": OrderedDict()}
        runtime = getattr(paths, "runtime", None)
        self.history = ConsoleHistory(runtime / "activity-console" if runtime is not None else None)
        self.ingestion = IngestionMonitor(runtime)
        self.replays = None

    def kafka_replays(self):
        with self._lock:
            if self.replays is None:
                from dashboards.kafka_replay import KafkaReplays
                self.replays = KafkaReplays(self.paths)
            return self.replays

    def reader_for(self, spec):
        if spec.get("run_id"):
            return self.kafka_replays().reader(spec["run_id"])
        return self.reader

    def close(self):
        if self.replays is not None:
            self.replays.close()
        with self._lock:
            for job in self._jobs.values():
                if job["status"] not in TERMINAL:
                    job["cancelled"] = True
        self._executor.shutdown(wait=True, cancel_futures=True)
        self.ingestion.close()

    def _key(self, spec):
        scope = dict(spec)
        # A live snapshot has a short serving TTL, rather than a new key per tick.
        scope.pop("request", None)
        reader = self.reader_for(spec)
        version = reader.version() if isinstance(reader, ActivityReader) else ()
        return json.dumps([scope, version], sort_keys=True)

    def _reuse(self, job, cached, message):
        """A snapshot reuses results, so report zero fresh scan/analysis work."""
        data, evidence = cached[1], cached[2]
        job.update(result=data, evidence=evidence, revision=2, status="complete",
                   started=job["started"] or time.monotonic(), finished=time.monotonic())
        self._emit(job, "CACHE", message, cache="snapshot hit", rows_fetched=0,
                   arrow_payload_bytes=0, events_analyzed=0,
                   events_matched=data.get("metrics", {}).get("edits"),
                   result_created_at=data.get("loaded_at"),
                   response_bytes=len(json.dumps({"activity": data, "evidence": evidence}, default=str).encode()),
                   queue_seconds=job["started"] - job["created"])

    def submit(self, raw_spec, owner):
        spec = normalize_spec(raw_spec)
        key = self._key(spec)
        mode = spec["mode"]
        with self._lock:
            previous_id = self._owners.get(owner)
            previous = self._jobs.get(previous_id)
            if previous and previous["status"] not in TERMINAL:
                # Incoming live batches can change the cache version during a
                # day scan. An identical stored selection can use that scan.
                same_selection = mode == "stored" and previous["spec"] == spec
                if not previous["cancelled"] and (previous["key"] == key or same_selection):
                    return self.public(previous)
                previous["cancelled"] = True
            job_id = uuid.uuid4().hex
            job = {"id": job_id, "owner": owner, "key": key, "spec": spec, "status": "queued",
                   "created": time.monotonic(), "started": None, "finished": None,
                   "cancelled": False, "revision": 0, "result": None, "evidence": None,
                   "metrics": {}, "logs": [], "error": None}
            self._jobs[job_id] = job
            self._owners[owner] = job_id
            page = "Page Investigation" if spec["page"] == "investigation" else "Live Pulse"
            selection = "Live" if mode == "live" else f"{spec['day']} {spec['zone']} {spec['kind']}"
            self._emit(job, "REFRESH", f"Refresh started · {page} · {selection}" +
                       (f" · {spec['title']}" if spec.get("title") else ""))
            # Remove finished requests only. Retain at most one queued request
            # per browser owner; stale queued work exits before touching data.
            for old_id in list(self._jobs):
                if len(self._jobs) <= 80:
                    break
                if self._jobs[old_id]["status"] in TERMINAL:
                    removed = self._jobs.pop(old_id)
                    if self._owners.get(removed["owner"]) == old_id:
                        self._owners.pop(removed["owner"], None)
            cached = self._cache[mode].get(key)
            ttl = 10 if mode == "live" else 30
            if cached and time.monotonic() - cached[0] < ttl:
                self._reuse(job, cached, "Saved snapshot served from cache; no event reprocessing")
            else:
                self._emit(job, "QUEUED", "Request accepted; waiting for the bounded activity worker", cache="miss")
                self._executor.submit(self._run, job)
            return self.public(job)

    def _emit(self, job, stage, message, **metrics):
        with self._lock:
            job["logs"].append({"time": stamp(time.time()), "stage": stage, "message": message})
            job["logs"] = job["logs"][-200:]
            job["metrics"].update(metrics)
            # Keep readable operational events across requests, pages and restarts.
            if stage not in {"QUEUED", "CHARTS", "EVIDENCE", "RESULT"}:
                if stage == "READ" and "arrow_payload_bytes" in metrics:
                    message = f"{metrics.get('rows_fetched', 0):,} rows fetched · {data_size(metrics['arrow_payload_bytes'])} decoded for analysis"
                self.history.append(job["id"], stage, message, metrics)
            if (stage in {"COMPLETE", "FAILED", "CANCELLED"} or
                    (stage == "CACHE" and metrics.get("cache") == "snapshot hit")) and not job.get("summarized"):
                job["summarized"] = True
                m = job["metrics"]
                count = lambda key: f"{m[key]:,}" if m.get(key) is not None else "not measured"
                elapsed = (job["finished"] or time.monotonic()) - (job["started"] or job["created"])
                summary = (f"Refresh {job['status']} in {elapsed:.2f} s · {count('rows_fetched')} rows fetched · "
                           f"{count('events_analyzed')} events analyzed · {data_size(m.get('arrow_payload_bytes'))} decoded · "
                           f"{count('events_matched')} matching edits")
                if m.get("cache") == "snapshot hit":
                    summary += " · cached result; no new fetch or analysis"
                elif m.get("cache") == "day index hit":
                    summary += " · analyzed from memory; no file reread"
                self.history.append(job["id"], "SUMMARY", summary, dict(m, processing_seconds=round(elapsed, 3)))

    def _run(self, job):
        cancelled = lambda: job["cancelled"]
        emit = lambda stage, message, **metrics: self._emit(job, stage, message, **metrics)
        try:
            if cancelled():
                raise InterruptedError()
            with self._lock:
                job.update(status="running", started=time.monotonic())
                cached = self._cache[job["spec"]["mode"]].get(job["key"])
                if cached and time.monotonic() - cached[0] < (10 if job["spec"]["mode"] == "live" else 30):
                    self._reuse(job, cached, "Identical queued request reused the completed snapshot; no duplicate scan")
                    return
            emit("START", "Reading filtered activity with the Parquet serving worker",
                 queue_seconds=job["started"] - job["created"])
            for line in self.ingestion.snapshot():
                emit("INGESTION", line)
            reader = self.reader_for(job["spec"])
            if job["spec"].get("run_id"):
                emit("KAFKA", "Reading only this isolated run's Spark-committed replay output")
            data = reader.read(job["spec"], emit, cancelled)
            if cancelled():
                raise InterruptedError()
            with self._lock:
                job.update(result=data, revision=1, status="evidence")
            emit("CHARTS", "Primary activity ready; loading optional evidence independently")
            try:
                evidence = self.reader.evidence(job["spec"], data, emit, cancelled)
            except InterruptedError:
                raise
            except Exception:
                logging.exception("Optional activity evidence failed")
                evidence = {"alerts": [], "comparisons": [], "diffs": [], "risks": [],
                            "warnings": ["Optional evidence is unavailable. Activity is ready."]}
            if cancelled():
                raise InterruptedError()
            response_bytes = len(json.dumps({"activity": data, "evidence": evidence}, default=str).encode())
            with self._lock:
                job.update(evidence=evidence, revision=2, status="complete", finished=time.monotonic())
                cache = self._cache[job["spec"]["mode"]]
                cache[job["key"]] = (time.monotonic(), data, evidence)
                cache.move_to_end(job["key"])
                while len(cache) > (12 if job["spec"]["mode"] == "live" else 24):
                    cache.popitem(last=False)
                emit("COMPLETE", "Activity and available evidence are ready", response_bytes=response_bytes,
                     query_seconds=job["finished"] - job["started"])
        except InterruptedError:
            with self._lock:
                job.update(status="cancelled", finished=time.monotonic())
                emit("CANCELLED", "Request superseded or cancelled; selections are preserved")
        except Exception:
            logging.exception("Activity request failed")
            with self._lock:
                job.update(status="failed", finished=time.monotonic(),
                           error="The activity query failed. Retry this selection; see logs/local/dashboard.log for details.")
                emit("FAILED", job["error"])

    def public(self, job):
        result = {k: job[k] for k in ("id", "spec", "status", "revision", "metrics", "logs", "error")}
        result["elapsed_seconds"] = round((job["finished"] or time.monotonic()) - job["created"], 3)
        return copy.deepcopy(result)

    def get(self, job_id, include_data=False):
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            result = self.public(job)
            if include_data:
                result.update(result=job["result"], evidence=job["evidence"])
            return result

    def cancel_owner(self, owner):
        with self._lock:
            job = self._jobs.get(self._owners.get(owner))
            if job and job["status"] not in TERMINAL:
                job["cancelled"] = True
