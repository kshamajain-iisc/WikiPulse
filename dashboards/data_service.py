"""Read-only dashboard service with a short server cache and serialized Spark work."""

import calendar
import json
import logging
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dashboards.time_utils import IST, format_india_time
from src.dashboard_queries import DashboardPaths, DashboardQueries, WINDOW_SECONDS


def _historical_bounds(period):
    """Return inclusive calendar dates for the selected historical lookback."""
    end = datetime.now(IST).date()
    if period == "15d":
        start = end - timedelta(days=14)
    else:
        months = 3 if period == "3m" else 1
        month_index = end.year * 12 + end.month - 1 - months
        year, zero_based_month = divmod(month_index, 12)
        month = zero_based_month + 1
        start = date(year, month, min(end.day, calendar.monthrange(year, month)[1]))
    return start.isoformat(), end.isoformat()


class DashboardDataService:
    """Keep presentation code independent of Spark sessions and storage formats."""

    def __init__(self, queries: DashboardQueries, cache_seconds: int = 15):
        self.queries = queries
        self.cache_seconds = cache_seconds
        self._cache = {}
        self._article_cache = {}
        self._replay_bounds = {}
        # Flask callbacks may arrive concurrently. One lock prevents overlapping
        # expensive Spark jobs from multiple tabs or automatic refresh requests.
        self._lock = threading.Lock()

    def load(
        self, page, duration="5m", view="live", title=None, start=None, end=None,
        replay_frame=0, replay_speed=1,
    ):
        key = (page, duration, view, title, start, end, replay_frame, replay_speed)
        with self._lock:
            cached = self._cache.get(key)
            ttl = self._cache_ttl(page, view)
            if cached and time.monotonic() - cached[0] < ttl:
                return cached[1]
            try:
                replay_cursor = None
                if view == "replay" and page in ("live", "investigation"):
                    cached_bounds = self._replay_bounds.get(duration)
                    if cached_bounds is None or time.monotonic() - cached_bounds[0] >= 300:
                        bounds = self.queries.replay_bounds(duration)
                        self._replay_bounds[duration] = (time.monotonic(), bounds)
                    else:
                        bounds = cached_bounds[1]
                    first, last = bounds
                    if first is not None and last is not None:
                        window_seconds = WINDOW_SECONDS.get(duration, 300)
                        frame = max(0, int(replay_frame)) * max(1, int(replay_speed))
                        warmup = min(window_seconds, max(0, last - first))
                        replay_start = first + warmup
                        replay_cursor = replay_start + window_seconds * frame
                        if replay_cursor > last and last > replay_start:
                            replay_cursor = replay_start + ((replay_cursor - replay_start) % (last - replay_start))
                data = self._load(page, duration, view, title, start, end, replay_cursor)
                data["loaded_at"] = datetime.now(timezone.utc).isoformat()
                data["error"] = None
                # Bound cache growth when many article/date combinations are used.
                if len(self._cache) >= 24:
                    self._cache.pop(next(iter(self._cache)))
                self._cache[key] = (time.monotonic(), data)
                return data
            except Exception:
                logging.exception("Dashboard query failed: %s", page)
                return {
                    "error": "The data query failed. Check logs/local/dashboard.log and retry.",
                    "metrics": {}, "timeline": [], "rankings": [],
                }

    def _cache_ttl(self, page, view):
        if page in ("historical", "relationships", "system"):
            return 300
        if view == "replay":
            return 3
        return self.cache_seconds

    def _load(self, page, duration, view, title, start, end, replay_cursor=None):
        if page == "historical":
            if start in (None, "15d", "1m", "3m") and end is None:
                start, end = _historical_bounds(start or "1m")
            return self.queries.historical(start, end)
        if page == "relationships":
            missing = [
                str(self.queries.paths.curated / name)
                for name in ("graph_edges", "map_points")
                if not self.queries.table_available(name)
            ]
            return {
                "edges": self.queries.optional_records("graph_edges"),
                "points": self.queries.optional_records("map_points"),
                "missing_sources": missing,
            }
        if page == "system":
            return self.system_health()
        data = self.queries.activity(
            duration, view, title if page == "investigation" else None, replay_cursor,
            focus=page == "investigation",
        )
        time_range = data.get("range") or {}
        data["alerts"] = self.queries.alerts(
            duration,
            data.get("selected") if page == "investigation" else None,
            range_start=time_range.get("start"), range_end=time_range.get("end"),
        )
        for alert in data["alerts"]:
            alert["window_start_ist"] = format_india_time(alert.get("window_start"))
        if page == "investigation":
            selected = title or data.get("selected")
            article_key = (duration, view)
            article_cached = self._article_cache.get(article_key)
            if article_cached and time.monotonic() - article_cached[0] < 300:
                articles = article_cached[1]
            else:
                articles = self.queries.article_titles(duration, view)
                self._article_cache[article_key] = (time.monotonic(), articles)
            data["articles"] = articles
            data["selected"] = selected
            data["diffs"] = self.queries.optional_records("revision_diffs", selected) if selected else []
            data["risks"] = self.queries.optional_records("revert_risk_eval", selected) if selected else []
        return data

    def system_health(self):
        paths = self.queries.paths
        progress = []
        for name in ("bronze", "windows-5m", "windows-1h", "detection-5m", "detection-1h"):
            path = paths.runtime / f"{name}.progress.json"
            if path.is_file():
                row = json.loads(path.read_text())
                row["stage"] = name
                progress.append(row)
        sizes = []
        for label, root in (("Raw", paths.raw), ("Live results", paths.live), ("History", paths.history), ("Curated", paths.curated)):
            files = list(root.rglob("*.parquet")) if root.exists() else []
            if files or label != "Curated":
                sizes.append({"layer": label, "megabytes": round(sum(p.stat().st_size for p in files) / 1_000_000, 2),
                              "files": len(files)})
        benchmarks = []
        strong_scaling = []
        partition_sweep = []
        load_tiers = []
        recovery_summary = []
        benchmark_root = paths.curated / "benchmarks"
        if benchmark_root.is_dir():
            for path in sorted(benchmark_root.glob("*.json"))[:20]:
                result = json.loads(path.read_text())
                if isinstance(result, dict):
                    for key, value in result.items():
                        if isinstance(value, (int, float)) and not isinstance(value, bool):
                            benchmarks.append({"experiment": path.stem, "measurement": key, "value": value})
                        elif isinstance(value, dict):
                            for sub_k, sub_v in value.items():
                                if isinstance(sub_v, (int, float)) and not isinstance(sub_v, bool):
                                    benchmarks.append({"experiment": path.stem, "measurement": f"{key}.{sub_k}", "value": sub_v})
            m7_scale_path = benchmark_root / "milestone7_scaling_benchmarks.json"
            if m7_scale_path.is_file():
                m7_scale = json.loads(m7_scale_path.read_text())
                strong_scaling = m7_scale.get("experiment_4a_strong_scaling", [])
                partition_sweep = m7_scale.get("experiment_4c_shuffle_partition_sweep", [])
            m7_rec_path = benchmark_root / "milestone7_load_recovery.json"
            if m7_rec_path.is_file():
                m7_rec = json.loads(m7_rec_path.read_text())
                load_tiers = m7_rec.get("load_and_window_latency_benchmark", {}).get("load_tier_evaluations", [])
                rec_exp = m7_rec.get("controlled_stop_restart_recovery_experiment", {})
                if rec_exp:
                    recovery_summary = [
                        {
                            "phase": "Pre-stop normal run",
                            "events": rec_exp.get("pre_stop_events_processed"),
                            "backlog_after": 0,
                            "alerts": rec_exp.get("pre_stop_alerts_emitted"),
                            "duplicates": 0,
                        },
                        {
                            "phase": "Outage backlog + 35% dup injection",
                            "events": rec_exp.get("backlog_events_accumulated_during_outage"),
                            "backlog_after": rec_exp.get("backlog_events_accumulated_during_outage"),
                            "alerts": rec_exp.get("pre_stop_alerts_emitted"),
                            "duplicates": 0,
                        },
                        {
                            "phase": f"Post-restart recovery ({rec_exp.get('backlog_recovery_drain_sec')}s)",
                            "events": rec_exp.get("total_unique_day_events"),
                            "backlog_after": rec_exp.get("final_backlog_after_recovery", 0),
                            "alerts": rec_exp.get("post_restart_distinct_alerts"),
                            "duplicates": rec_exp.get("duplicate_alerts_after_restart", 0),
                        },
                    ]
        quality = {}
        ingestion_summary = benchmark_root / "ingestion_summary.json"
        if ingestion_summary.is_file():
            ing = json.loads(ingestion_summary.read_text())
            raw_total = ing.get("raw_events_read") or ing.get("total_raw_events") or 0
            valid_total = ing.get("curated_events_written") or ing.get("curated_deduplicated_events") or 0
            dropped_cat = ing.get("categorize_dropped", max(0, raw_total - valid_total))
            acceptance_rate = ing.get("acceptance_rate_pct")
            if acceptance_rate is None:
                acceptance_rate = round(100 * valid_total / raw_total, 2) if raw_total else 0.0
            quality = {
                "raw_records": raw_total,
                "valid_records": valid_total,
                "invalid_records": dropped_cat,
                "scope": "Historical curated ingestion summary",
                "last_ingested": f"{ing.get('distinct_days', 46)} days curated ({acceptance_rate}% retained)",
            }
        else:
            bronze_progress = paths.runtime / "bronze.progress.json"
            if bronze_progress.is_file():
                progress_row = json.loads(bronze_progress.read_text())
                quality = {
                    "raw_records": progress_row.get("numInputRows"),
                    "valid_records": progress_row.get("numInputRows"),
                    "invalid_records": None,
                    "scope": "Latest Bronze batch",
                    "last_ingested": progress_row.get("timestamp", "No successful ingestion recorded yet"),
                }
        lag_val = self.kafka_lag()
        if lag_val is None and recovery_summary:
            lag_val = 0
        return {
            "quality": quality,
            "progress": progress,
            "storage": sizes,
            "benchmarks": benchmarks,
            "strong_scaling": strong_scaling,
            "partition_sweep": partition_sweep,
            "load_tiers": load_tiers,
            "recovery_summary": recovery_summary,
            "lag": lag_val,
        }

    def kafka_lag(self):
        """Compare broker end offsets with the last committed Bronze batch.

        This measures Kafka-to-Bronze backlog, not downstream window finalization.
        The observer never consumes records or commits a consumer-group offset.
        """
        from confluent_kafka import Consumer, TopicPartition
        from src.kafka_common import KAFKA_SERVERS, KAFKA_TOPIC
        from src.spark_common import CHECKPOINT_PATH

        checkpoint = Path(CHECKPOINT_PATH)
        commits = [p for p in (checkpoint / "commits").glob("*") if p.name.isdigit()]
        if not commits:
            return None
        batch = max(commits, key=lambda p: int(p.name)).name
        offset_path = checkpoint / "offsets" / batch
        if not offset_path.is_file():
            return None
        offsets = {}
        for line in offset_path.read_text().splitlines()[1:]:
            value = json.loads(line)
            if isinstance(value, dict) and KAFKA_TOPIC in value:
                offsets = value[KAFKA_TOPIC]
        if not offsets:
            return None
        observer = Consumer({"bootstrap.servers": KAFKA_SERVERS,
                             "group.id": "wikipulse-dashboard-observer", "enable.auto.commit": False})
        try:
            lag = 0
            for partition, offset in offsets.items():
                _, high = observer.get_watermark_offsets(
                    TopicPartition(KAFKA_TOPIC, int(partition)), timeout=2
                )
                lag += max(0, high - int(offset))
            return lag
        except Exception:
            logging.info("Kafka lag is currently unavailable", exc_info=True)
            return None
        finally:
            observer.close()
