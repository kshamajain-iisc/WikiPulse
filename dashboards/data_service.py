"""Read-only dashboard service with a short server cache and serialized Spark work."""

import json
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from src.dashboard_queries import DashboardPaths, DashboardQueries


class DashboardDataService:
    """Keep presentation code independent of Spark sessions and storage formats."""

    def __init__(self, queries: DashboardQueries, cache_seconds: int = 20):
        self.queries = queries
        self.cache_seconds = cache_seconds
        self._cache = {}
        # Flask callbacks may arrive concurrently. One lock prevents overlapping
        # expensive Spark jobs from multiple tabs or automatic refresh requests.
        self._lock = threading.Lock()

    def load(self, page, duration="5m", view="live", title=None, start=None, end=None):
        key = (page, duration, view, title, start, end)
        with self._lock:
            cached = self._cache.get(key)
            if cached and time.monotonic() - cached[0] < self.cache_seconds:
                return cached[1]
            try:
                data = self._load(page, duration, view, title, start, end)
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

    def _load(self, page, duration, view, title, start, end):
        if page == "historical":
            return self.queries.historical(start, end)
        if page == "relationships":
            return {
                "edges": self.queries.optional_records("graph_edges"),
                "points": self.queries.optional_records("map_points"),
            }
        if page == "system":
            return self.system_health()
        data = self.queries.activity(duration, view, title if page == "investigation" else None)
        data["alerts"] = self.queries.optional_records("alerts", title if page == "investigation" else None)
        if page == "investigation":
            # When no article was selected, investigate the leading observed page.
            selected = title or next(iter(data.get("rankings", [])), {}).get("title")
            if selected and not title:
                data = self.queries.activity(duration, view, selected)
                data["alerts"] = self.queries.optional_records("alerts", selected)
            data["articles"] = self.queries.article_titles(duration, view)
            data["selected"] = selected
            data["diffs"] = self.queries.optional_records("revision_diffs", selected) if selected else []
            data["risks"] = self.queries.optional_records("revert_risk_eval", selected) if selected else []
        return data

    def system_health(self):
        paths = self.queries.paths
        progress = []
        for name in ("bronze", "windows-5m", "windows-1h"):
            path = paths.runtime / f"{name}.progress.json"
            if path.is_file():
                row = json.loads(path.read_text())
                row["stage"] = name
                progress.append(row)
        sizes = []
        for label, root in (("Raw", paths.raw), ("Live results", paths.live), ("History", paths.history)):
            files = list(root.rglob("*.parquet")) if root.exists() else []
            sizes.append({"layer": label, "megabytes": sum(p.stat().st_size for p in files) / 1_000_000,
                          "files": len(files)})
        benchmarks = []
        benchmark_root = paths.curated / "benchmarks"
        for path in sorted(benchmark_root.glob("*.json"))[:20]:
            result = json.loads(path.read_text())
            if isinstance(result, dict):
                for key, value in result.items():
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        benchmarks.append({"experiment": path.stem, "measurement": key, "value": value})
        return {"quality": self.queries.quality(), "progress": progress,
                "storage": sizes, "benchmarks": benchmarks, "lag": self.kafka_lag()}

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
