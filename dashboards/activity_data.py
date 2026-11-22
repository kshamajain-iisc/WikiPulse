"""Bounded Parquet serving for the two activity screens, independent of Spark jobs."""

import bisect
import json
import math
import re
import threading
import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from dashboards.time_utils import IST

UTC = timezone.utc
HORIZONS = {"5m": 300, "1h": 3600, "6h": 21600}


def stamp(epoch):
    return datetime.fromtimestamp(epoch, UTC).isoformat().replace("+00:00", "Z")


def day_bounds(day, zone="IST"):
    start = datetime.combine(date.fromisoformat(day), datetime.min.time(), IST if zone == "IST" else UTC)
    return start.timestamp(), (start + timedelta(days=1)).timestamp()


def normalize_spec(spec):
    page = spec.get("page", "live")
    if page not in ("live", "investigation"):
        raise ValueError("Choose Live Pulse or Page Investigation.")
    mode = spec.get("mode", "live")
    kind = spec.get("kind", "summary")
    zone = spec.get("zone", "IST")
    horizon = spec.get("horizon", "5m")
    if mode not in ("live", "stored") or kind not in ("summary", "replay"):
        raise ValueError("Invalid activity mode.")
    if zone not in ("IST", "UTC") or horizon not in HORIZONS:
        raise ValueError("Invalid timezone or activity horizon.")
    day = spec.get("day")
    if mode == "stored":
        day_bounds(day or "", zone)
    cursor = float(spec.get("cursor", 0))
    if not math.isfinite(cursor):
        raise ValueError("Invalid replay position.")
    title = str(spec.get("title") or "").strip()[:512] or None
    result = {"page": page, "mode": mode, "kind": kind, "zone": zone,
            "horizon": horizon, "day": day if mode == "stored" else None,
            "cursor": max(0, min(86400, cursor)) if mode == "stored" and kind == "replay" else 0,
            "title": title if page == "investigation" else None,
            "finalized": bool(spec.get("finalized")) if mode == "live" else False}
    if spec.get("run_id"):
        if not isinstance(spec["run_id"], str) or not re.fullmatch(r"[0-9a-f]{32}", spec["run_id"]):
            raise ValueError("Invalid replay run identity.")
        result["run_id"] = spec["run_id"]
    return result


@dataclass(frozen=True, slots=True)
class Event:
    ts: float
    identity: str
    page: int
    title: str
    bot: bool
    delta: int | None
    editor: str | None
    revert: bool


class ActivityReader:
    def __init__(self, paths, now=None):
        self.paths = paths
        self.now = now or (lambda: datetime.now(UTC).timestamp())
        self._manifests = {}
        self._indexes = OrderedDict()
        self._catalog = None
        self._catalog_lock = threading.Lock()
        self._titles = OrderedDict()

    @staticmethod
    def title_scope(spec):
        return tuple(spec.get(k) for k in ("mode", "day", "zone", "horizon"))

    def search_titles(self, spec, query):
        with self._catalog_lock:
            titles = self._titles.get(self.title_scope(spec))
        text = (query or "").casefold()
        if not text:
            return []
        if titles is None:
            # A cold article query projects only that article. Search must still
            # discover the other captured articles without materializing events.
            spec = normalize_spec(spec)
            start, end = day_bounds(spec["day"], spec["zone"]) if spec["mode"] == "stored" else (
                self.now() - HORIZONS[spec["horizon"]], self.now())
            matches = set()
            for kind, files in self._sources(spec, start, end):
                dataset = ds.dataset([str(p) for p in files], format="parquet")
                ts, ns, change, title = ("ts", "ns", "type", "title") if kind == "canonical" else (
                    "event_time", "namespace", "change_type", "page_title")
                if not {ts, ns, change, title}.issubset(dataset.schema.names):
                    continue
                scalar = lambda n: pa.scalar(datetime.fromtimestamp(n, UTC).replace(tzinfo=None), dataset.schema.field(ts).type)
                filt = ((ds.field(ts) >= scalar(start)) & (ds.field(ts) < scalar(end)) &
                        (ds.field(ns) == 0) & (ds.field(change) == "edit") &
                        pc.match_substring(ds.field(title), pattern=text, ignore_case=True))
                for batch in dataset.scanner(columns=[title], filter=filt, batch_size=8192).to_batches():
                    matches.update(t for t in batch.column(0).to_pylist() if t)
                    if len(matches) >= 40:
                        break
                if len(matches) >= 40:
                    break
            titles = matches
        return sorted((t for t in titles if text in t.casefold()), key=lambda t: (not t.casefold().startswith(text), t))[:40]

    def files(self, root):
        """Honor Spark file-sink commits; never read an uncommitted output part."""
        root = Path(root)
        metadata = root / "_spark_metadata"
        if not metadata.is_dir():
            return sorted(root.rglob("*.parquet")) if root.exists() else []
        logs = sorted((p for p in metadata.iterdir() if p.name.split(".")[0].isdigit()),
                      key=lambda p: int(p.name.split(".")[0]))
        signature = tuple((p.name, p.stat().st_mtime_ns) for p in logs)
        cached = self._manifests.get(root)
        if cached and cached[0] == signature:
            return cached[1]
        compact = [i for i, p in enumerate(logs) if p.suffix == ".compact"]
        selected = logs[compact[-1]:] if compact else logs
        paths = set()
        for log in selected:
            for line in log.read_text().splitlines()[1:]:
                if not line:
                    continue
                item = json.loads(line)
                path = Path(unquote(urlparse(item["path"]).path)).resolve()
                if not path.is_relative_to(root.resolve()):
                    continue
                if item.get("action") == "delete":
                    paths.discard(path)
                else:
                    paths.add(path)
        result = sorted(p for p in paths if p.is_file())
        self._manifests[root] = (signature, result)
        return result

    def catalog(self, zone="IST"):
        with self._catalog_lock:
            if self._catalog and time.monotonic() - self._catalog[0] < 30:
                days = self._catalog[1]
            else:
                found = set()
                for root, prefix in ((self.paths.curated / "rc_events", "day="),
                                     (self.paths.history / "edits", "day="),
                                     (self.paths.raw, "ingestion_date=")):
                    if root.is_dir():
                        for part in root.glob(prefix + "*"):
                            try:
                                found.add(date.fromisoformat(part.name.split("=", 1)[1]))
                            except ValueError:
                                continue
                found.update(date.fromisoformat(d) for d in self.capture_days())
                days = sorted(found, reverse=True)
                self._catalog = (time.monotonic(), days)
            # An IST day can overlap the next UTC source date. Mark dates as
            # observed partitions rather than claiming complete capture.
            candidates = set(days)
            if zone == "IST":
                today = datetime.fromtimestamp(self.now(), IST).date()
                candidates.update(d + timedelta(days=1) for d in days if d + timedelta(days=1) <= today)
            return [{"label": f"{d:%d %b %Y} · capture coverage unverified", "value": d.isoformat()}
                    for d in sorted(candidates, reverse=True)]

    def version(self):
        roots = (self.paths.raw / "_spark_metadata", self.paths.curated / "rc_events",
                 self.paths.curated / "alerts", self.paths.curated / "page_baselines")
        return tuple(p.stat().st_mtime_ns if p.exists() else 0 for p in roots)

    def capture_days(self):
        runtime = getattr(self.paths, "runtime", None)
        if runtime is None:
            return {}
        try:
            mapping = json.loads((runtime / "capture-days.json").read_text())
            return {date.fromisoformat(day).isoformat(): [date.fromisoformat(d).isoformat() for d in partitions]
                    for day, partitions in mapping.items() if isinstance(partitions, list)}
        except (OSError, ValueError, TypeError, AttributeError):
            return {}

    def _sources(self, spec, start, end):
        first = datetime.fromtimestamp(start, UTC).date()
        last = datetime.fromtimestamp(max(start, end - 0.001), UTC).date()
        days = {(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)}
        sources = []
        if spec["mode"] == "stored":
            for root in (self.paths.curated / "rc_events", self.paths.history / "edits"):
                files = [p for d in sorted(days) for p in (root / ("day=" + d)).rglob("*.parquet")]
                if files:
                    sources.append(("canonical", files))
                    break
        # Bronze fills event-time gaps and supplies recent dates. Stable event
        # identity deduplicates any overlap with canonical history.
        raw_days = days | {(first - timedelta(days=1)).isoformat(), (last + timedelta(days=1)).isoformat()}
        for day, ingestion_days in self.capture_days().items():
            if day in days:
                raw_days.update(ingestion_days)
        raw = [p for p in self.files(self.paths.raw)
               if getattr(self, "replay_run", False) or any(f"ingestion_date={d}" in str(p) for d in raw_days)]
        if raw:
            sources.append(("bronze", raw))
        return sources

    @staticmethod
    def _signature(sources):
        return tuple((kind, str(p), p.stat().st_size, p.stat().st_mtime_ns)
                     for kind, files in sources for p in files)

    def read(self, spec, emit, cancelled):
        now = self.now()
        if spec["mode"] == "stored":
            start, end = day_bounds(spec["day"], spec["zone"])
        else:
            start, end = now - HORIZONS[spec["horizon"]], now
        if spec["finalized"]:
            return self._finalized(spec, start, end, emit, cancelled)
        sources = self._sources(spec, start, end)
        signature = self._signature(sources)
        article = spec["title"] if spec["page"] == "investigation" else None
        key = (start, end, signature, article) if spec["mode"] == "stored" else None
        full_key = (start, end, signature, None)
        cached_key = full_key if full_key in self._indexes else key
        cached = self._indexes.get(cached_key) if key else None
        footprint = sum(x[2] for x in signature)
        source_names = {"canonical": "Saved edit history", "bronze": "Streamed edits saved by Spark"}
        source_label = " + ".join(source_names[kind] for kind, _ in sources) or "No captured source for this scope"
        if spec.get("run_id"):
            source_label = "Isolated Kafka run " + spec["run_id"][:8] + ": " + source_label
        emit("SOURCE", f"Reading {source_label} · {len(signature):,} selected files ({footprint / 1_000_000:.2f} MB on disk; not bytes fetched)", input_file_bytes=footprint,
             selected_files=len(signature), scope_start=stamp(start), scope_end=stamp(end))
        if cached:
            events = cached
            self._indexes.move_to_end(cached_key)
            emit("CACHE", "Reusing the selected-day event index; no Parquet reread", cache="day index hit",
                 rows_fetched=0, arrow_payload_bytes=0)
        else:
            events = self._events(sources, start, end, emit, cancelled, article)
            # Bound retained indexes independently of snapshot caches. A large
            # day is still served but is not retained in process memory.
            if key and len(events) <= 250_000:
                self._indexes[key] = events
                while len(self._indexes) > 2:
                    self._indexes.popitem(last=False)
        if cancelled():
            raise InterruptedError("A newer selection superseded this request.")
        if not article or cached_key == full_key and cached:
            with self._catalog_lock:
                title_key = self.title_scope(spec)
                self._titles[title_key] = {e.title for e in events}
                self._titles.move_to_end(title_key)
                while len(self._titles) > 4:
                    self._titles.popitem(last=False)
        cursor = end
        view_start = start
        if spec["mode"] == "stored" and spec["kind"] == "replay":
            cursor = start + spec["cursor"]
            view_start = max(start, cursor - HORIZONS[spec["horizon"]])
        times = [e.ts for e in events]
        a = bisect.bisect_left(times, view_start)
        b = bisect.bisect_left(times, cursor)
        # Live includes an event exactly at the request clock. Stored days and
        # replay frames consistently use half-open [start, end) bounds.
        current = events[a:b]
        title = spec["title"]
        if spec["page"] == "investigation" and not title:
            emit("SELECT", "Choose an article or open one from Live Pulse")
            current = []
        elif title:
            current = [e for e in current if e.title == title]
        bucket = 3600 if spec["mode"] == "stored" and spec["kind"] == "summary" else (
            60 if spec["horizon"] == "5m" else 300 if spec["horizon"] == "1h" else 3600)
        emit("AGGREGATE", f"Aggregating {len(current):,} unique matching edits", events_analyzed=len(events),
             events_matched=len(current))
        data = aggregate(current, bucket, cancelled, start if spec["mode"] == "stored" else 0)
        cumulative = events[:b]
        if spec["page"] == "investigation" and not title:
            cumulative = []
        elif title:
            cumulative = [e for e in cumulative if e.title == title]
        data["day_so_far"] = len(cumulative) if spec["mode"] == "stored" and spec["kind"] == "replay" else None
        label = ("Kafka replay (Spark committed)" if spec.get("run_id") else
                 "Live preview" if spec["mode"] == "live" else "Day summary" if spec["kind"] == "summary" else "Saved replay")
        observed = events if spec["page"] == "live" else [e for e in events if title and e.title == title]
        data.update({"selected": title, "view": "live" if spec["mode"] == "live" else spec["kind"],
                     "scope": f"{label} · {spec['zone']} · mainspace edits",
                     "data_source": " / ".join(k for k, _ in sources) + " Parquet" if sources else "No captured source for this day",
                     "range": {"start": stamp(view_start), "end": stamp(cursor)},
                     "bucket": f"{bucket // 60} minutes", "duration": spec["horizon"],
                     "coverage": "Observed capture; full-day completeness is unverified.",
                     "available_events": len(observed), "first_observed": stamp(observed[0].ts) if observed else None,
                     "first_cursor": min(86400, observed[0].ts - start + 1) if observed else 0,
                     "last_observed": stamp(observed[-1].ts) if observed else None,
                     "loaded_at": stamp(now), "alerts": [], "comparisons": [], "risks": [], "diffs": []})
        if not current:
            data["empty_message"] = ("Choose an article to investigate." if not title and spec["page"] == "investigation"
                                     else "No edits match this article and time range." if title
                                     else "No captured edits in this time range. Choose another date or replay position.")
        emit("RESULT", f"{data['metrics']['edits']:,} edits · {data['metrics']['pages']:,} articles · "
             f"{len(data['rankings']):,} ranked rows returned", rows_returned=len(data["rankings"]) + len(data["timeline"]))
        return data

    def _events(self, sources, start, end, emit, cancelled, title=None):
        events, seen = [], set()
        fetched = payload = rejected = 0
        last_emit = time.monotonic()

        def read_progress():
            emit("READ", f"{fetched:,} filtered rows fetched · {payload:,} B decoded Arrow payload",
                 rows_fetched=fetched, arrow_payload_bytes=payload, rejected_rows=rejected,
                 unique_events=len(events), duplicates=fetched - rejected - len(events))
        for kind, files in sources:
            if cancelled():
                raise InterruptedError()
            dataset = ds.dataset([str(p) for p in files], format="parquet")
            names = set(dataset.schema.names)
            ts, ns, change = ("ts", "ns", "type") if kind == "canonical" else ("event_time", "namespace", "change_type")
            if not {ts, ns, change}.issubset(names):
                continue
            filt = ((ds.field(ts) >= pa.scalar(datetime.fromtimestamp(start, UTC).replace(tzinfo=None), dataset.schema.field(ts).type))
                    & (ds.field(ts) < pa.scalar(datetime.fromtimestamp(end, UTC).replace(tzinfo=None), dataset.schema.field(ts).type))
                    & (ds.field(ns) == 0) & (ds.field(change) == "edit"))
            if title:
                filt &= ds.field("title" if kind == "canonical" else "page_title") == title
            columns = (["ts", "rcid", "pageid", "title", "is_bot", "oldlen", "newlen", "user", "is_revert_action"]
                       if kind == "canonical" else ["event_time", "event_id", "change_id", "page_id", "page_title", "is_bot",
                                                   "old_length", "new_length", "raw_event_json", "wiki", "kafka_key"])
            scanner = dataset.scanner(columns=[c for c in columns if c in names], filter=filt, batch_size=8192)
            for batch in scanner.to_batches():
                if cancelled():
                    raise InterruptedError()
                fetched += batch.num_rows
                payload += batch.nbytes
                for r in batch.to_pylist():
                    event = normalize_event(r, kind)
                    if event is None:
                        rejected += 1
                    elif event.identity not in seen:
                        seen.add(event.identity)
                        events.append(event)
                if batch.num_rows and time.monotonic() - last_emit >= .3:
                    read_progress()
                    last_emit = time.monotonic()
        read_progress()
        events.sort(key=lambda e: (e.ts, e.identity))
        return events

    def _finalized(self, spec, start, end, emit, cancelled):
        duration = "5m" if spec["horizon"] == "5m" else "1h"
        files = self.files(self.paths.live / ("windows_" + duration))
        emit("READ", "Reading committed finalized streaming windows", input_file_bytes=sum(p.stat().st_size for p in files))
        rows, payload = [], 0
        needs_article = spec["page"] == "investigation" and not spec["title"]
        if files and not needs_article:
            dataset = ds.dataset([str(p) for p in files], format="parquet")
            filt = ((ds.field("window_end") > datetime.fromtimestamp(start, UTC).replace(tzinfo=None))
                    & (ds.field("window_end") <= datetime.fromtimestamp(end, UTC).replace(tzinfo=None))
                    & (ds.field("ns") == 0))
            if spec["title"]:
                filt &= ds.field("title") == spec["title"]
            for batch in dataset.scanner(filter=filt, batch_size=4096).to_batches():
                if cancelled():
                    raise InterruptedError()
                rows.extend(batch.to_pylist())
                payload += batch.nbytes
        # Aggregate windows directly; do not invent individual events or editors.
        ranking, timeline = {}, {}
        for r in rows:
            pid = r["pageid"]
            target = ranking.setdefault(pid, {"title": r["title"], "edits": 0, "human": 0, "bots": 0,
                                               "gross_bytes": 0, "net_bytes": 0, "reverts": 0, "known_sizes": 0,
                                               "peak_window_editors": 0})
            for out, col in (("edits", "edits_total"), ("human", "edits_human"), ("bots", "edits_bot"),
                             ("gross_bytes", "gross_bytes"), ("net_bytes", "net_bytes"),
                             ("reverts", "reverts_count"), ("known_sizes", "edits_with_sizes")):
                target[out] += r.get(col) or 0
            target["peak_window_editors"] = max(target["peak_window_editors"], r.get("distinct_human_editors") or 0)
            k = r["window_start"].replace(tzinfo=UTC).timestamp()
            t = timeline.setdefault(k, {"window_start": stamp(k), "human": 0, "bots": 0, "gross_bytes": 0, "net_bytes": 0})
            for out, col in (("human", "edits_human"), ("bots", "edits_bot"), ("gross_bytes", "gross_bytes"), ("net_bytes", "net_bytes")):
                t[out] += r.get(col) or 0
        metrics = {k: sum(r[k] for r in ranking.values()) for k in ("edits", "human", "bots", "gross_bytes", "net_bytes", "reverts", "known_sizes")}
        metrics["pages"] = len(ranking)
        emit("RESULT", f"{len(rows):,} finalized page-window rows returned", rows_fetched=len(rows), events_analyzed=None,
             arrow_payload_bytes=payload,
             events_matched=metrics["edits"], rows_returned=len(ranking))
        return {"metrics": metrics, "timeline": [timeline[k] for k in sorted(timeline)],
                "rankings": sorted(ranking.values(), key=lambda r: (-r["edits"], r["title"]))[:100],
                "selected": spec["title"], "view": "finalized", "scope": "Finalized windows · watermark-delayed",
                "bucket": "5 minutes" if duration == "5m" else "1 hour", "duration": spec["horizon"],
                "data_source": "Committed streaming windows", "range": {"start": stamp(start), "end": stamp(end)},
                "latest": stamp(max(r["window_end"].replace(tzinfo=UTC).timestamp() for r in rows)) if rows else None,
                "loaded_at": stamp(self.now()), "alerts": [], "comparisons": [], "risks": [], "diffs": [],
                **({"empty_message": "Choose an article to investigate."} if needs_article else {})}

    def evidence(self, spec, data, emit, cancelled):
        title = spec["title"]
        result = {"alerts": [], "comparisons": [], "diffs": [], "risks": [], "warnings": []}
        for name, key in (("alerts", "alerts"), ("revision_diffs", "diffs"), ("revert_risk_eval", "risks"),
                          ("page_baselines", "baseline")):
            if cancelled():
                raise InterruptedError()
            if not title and name != "alerts":
                continue
            emit("EVIDENCE", f"Looking up {name}" + (f" for {title}" if title else " in the selected range"))
            try:
                files = self.files(self.paths.curated / name)
                if not files:
                    continue
                dataset = ds.dataset([str(p) for p in files], format="parquet")
                if title and "title" not in dataset.schema.names:
                    result["warnings"].append(f"{name} lacks article identity; it was not used for this article.")
                    continue
                filt = ds.field("title") == title if title and "title" in dataset.schema.names else None
                if name == "alerts" and "window_start" in dataset.schema.names:
                    start = datetime.fromisoformat(data["range"]["start"].replace("Z", "+00:00")).replace(tzinfo=None)
                    end = datetime.fromisoformat(data["range"]["end"].replace("Z", "+00:00")).replace(tzinfo=None)
                    bounds = (ds.field("window_start") >= start) & (ds.field("window_start") < end)
                    filt = bounds if filt is None else filt & bounds
                    if "window_duration" in dataset.schema.names:
                        allowed = ["5m", "5 minutes"] if spec["horizon"] == "5m" else ["1h", "1 hour"]
                        filt &= ds.field("window_duration").isin(allowed)
                found = []
                for batch in dataset.scanner(filter=filt, batch_size=512).to_batches():
                    if cancelled():
                        raise InterruptedError()
                    found.extend(batch.to_pylist())
                    if len(found) >= 200:
                        break
                result[key] = json.loads(json.dumps(found[:200], default=lambda x: x.isoformat()))
            except InterruptedError:
                raise
            except Exception:
                result["warnings"].append(f"{name} is unavailable for this selection; activity is unaffected.")
        baseline = result.pop("baseline", [])
        if baseline:
            b = baseline[0]
            bucket_text = data.get("bucket", "5 minutes")
            bucket_seconds = int(bucket_text.split()[0]) * (3600 if "hour" in bucket_text else 60)
            divisor = 3600 / bucket_seconds
            mean = b.get("avg_hourly_edits")
            std = b.get("std_hourly_edits")
            if mean is not None:
                result["comparisons"] = [{**r, "edits_human": r.get("human", 0), "avg_edits_human": mean / divisor,
                                           "std_edits_human": (std or 0) / math.sqrt(divisor)} for r in data["timeline"]]
                result["baseline_period"] = f"Baseline training: {b.get('baseline_start_date', 'unknown')} → {b.get('baseline_end_date', 'unknown')}"
        return result


def normalize_event(row, kind):
    if kind == "canonical":
        ts, rcid, page, title = row.get("ts"), row.get("rcid"), row.get("pageid"), row.get("title")
        old, new = row.get("oldlen"), row.get("newlen")
        editor, revert = row.get("user"), row.get("is_revert_action", False)
    else:
        ts, rcid, page, title = row.get("event_time"), row.get("change_id"), row.get("page_id"), row.get("page_title")
        if (row.get("wiki") != "enwiki" or row.get("event_id") != f"enwiki:{rcid}"
                or (row.get("kafka_key") is not None and row["kafka_key"] != row.get("event_id"))):
            return None
        old, new = row.get("old_length"), row.get("new_length")
        try:
            raw = json.loads(row.get("raw_event_json") or "{}")
        except (TypeError, ValueError):
            raw = {}
        editor = raw.get("user")
        revert = bool(set(raw.get("tags") or []) & {"mw-rollback", "mw-undo", "mw-manual-revert"})
    if ts is None or rcid is None or not page or not title:
        return None
    return Event(ts.replace(tzinfo=UTC).timestamp(), f"enwiki:{rcid}", int(page), title,
                 bool(row.get("is_bot")), int(new) - int(old) if old is not None and new is not None else None,
                 editor, bool(revert))


def aggregate(events, bucket, cancelled=lambda: False, anchor=0):
    ranking, timeline, editors = {}, {}, defaultdict(set)
    for i, e in enumerate(events):
        if i % 8192 == 0 and cancelled():
            raise InterruptedError()
        tick = int((e.ts - anchor) // bucket) * bucket + anchor
        r = ranking.setdefault(e.page, {"title": e.title, "edits": 0, "human": 0, "bots": 0,
                                        "gross_bytes": 0, "net_bytes": 0, "reverts": 0,
                                        "known_sizes": 0, "peak_window_editors": 0})
        r["title"] = e.title
        t = timeline.setdefault(tick, {"window_start": stamp(tick), "human": 0, "bots": 0,
                                       "gross_bytes": 0, "net_bytes": 0})
        r["edits"] += 1
        key = "bots" if e.bot else "human"
        r[key] += 1
        t[key] += 1
        if e.delta is not None:
            r["known_sizes"] += 1
            r["gross_bytes"] += abs(e.delta)
            r["net_bytes"] += e.delta
            t["gross_bytes"] += abs(e.delta)
            t["net_bytes"] += e.delta
        r["reverts"] += e.revert
        if not e.bot and e.editor:
            group = editors[(e.page, tick)]
            group.add(e.editor)
            r["peak_window_editors"] = max(r["peak_window_editors"], len(group))
    metrics = {k: sum(r[k] for r in ranking.values()) for k in ("edits", "human", "bots", "gross_bytes", "net_bytes", "reverts", "known_sizes")}
    metrics["pages"] = len(ranking)
    return {"metrics": metrics, "timeline": [timeline[k] for k in sorted(timeline)],
            "rankings": sorted(ranking.values(), key=lambda r: (-r["edits"], r["title"]))[:100],
            "latest": stamp(events[-1].ts) if events else None}
