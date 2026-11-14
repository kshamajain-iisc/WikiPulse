"""Spark query boundary for dashboards; return only bounded, JSON-safe results.

All dataframe filtering, deduplication, grouping, ranking and joins stay in Spark.
Presentation code receives aggregates and capped records, never raw dataframes.
"""

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

try:
    import pyarrow.dataset as pa_ds
except ImportError:
    pa_ds = None

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F

from src.config.settings import CURATED_DIR
from src.page_features import compare_history, live_edits, page_windows
from src.spark_common import RAW_PATH, ROOT, reparse_stored_events


WINDOW_SECONDS = {"5m": 5 * 60, "1h": 60 * 60, "6h": 6 * 60 * 60}
WINDOW_LABELS = {"5m": "5 minutes", "1h": "1 hour", "6h": "6 hours"}
LIVE_BUCKETS = {"5m": "5 minutes", "1h": "5 minutes", "6h": "1 hour"}
LIVE_WINDOW_TABLES = {"5m": "5m", "1h": "1h", "6h": "1h"}

# Subject-geography gazetteer for historical RecentChanges (never editor IPs)
RECENTCHANGES_GEO_GAZETTEER = (
    ("United States", 39.8282, -98.5795, "North America", r"(?i)\b(United States|U\.S\.|US Senate|US House|American|California|Texas|New York|Florida|Washington, D\.C\.|Chicago|Los Angeles|Boston|Pennsylvania|Ohio|Georgia \(U\.S\. state\)|Michigan|Virginia)\b"),
    ("United Kingdom", 54.6000, -2.0000, "Europe", r"(?i)\b(United Kingdom|British|England|English|London|Scotland|Scottish|Wales|Welsh|Northern Ireland|Manchester|Liverpool|Birmingham)\b"),
    ("India", 22.8000, 83.0000, "South Asia", r"(?i)\b(India|Indian|New Delhi|Delhi|Mumbai|Bengaluru|Bangalore|Chennai|Kolkata|Hyderabad|Tamil|Hindi|Maharashtra|Kerala|Gujarat|Punjab|Uttar Pradesh|Karnataka|Rajasthan)\b"),
    ("Canada", 56.0000, -109.0000, "North America", r"(?i)\b(Canada|Canadian|Toronto|Ontario|Quebec|Vancouver|Montreal|Alberta|Ottawa)\b"),
    ("Australia", -25.2744, 133.7751, "Oceania", r"(?i)\b(Australia|Australian|Sydney|Melbourne|Queensland|Brisbane|Perth|New South Wales|Victoria \(state\))\b"),
    ("Japan", 36.2048, 138.2529, "East & Southeast Asia", r"(?i)\b(Japan|Japanese|Tokyo|Osaka|Kyoto|Hokkaido|Yokohama|Nagoya)\b"),
    ("China", 35.8447, 103.4519, "East & Southeast Asia", r"(?i)\b(China|Chinese|Beijing|Shanghai|Hong Kong|Shenzhen|Guangzhou|Sichuan|Taiwan)\b"),
    ("Germany", 51.1657, 10.4515, "Europe", r"(?i)\b(Germany|German|Berlin|Munich|Bavaria|Frankfurt|Hamburg|Saxony|Cologne)\b"),
    ("France", 46.2276, 2.2137, "Europe", r"(?i)\b(France|French|Paris|Marseille|Lyon|Toulouse|Normandy)\b"),
    ("Italy", 42.5000, 12.5000, "Europe", r"(?i)\b(Italy|Italian|Rome|Milan|Naples|Venice|Sicily|Florence|Turin)\b"),
    ("Spain", 40.4637, -3.7492, "Europe", r"(?i)\b(Spain|Spanish|Madrid|Barcelona|Catalonia|Seville|Valencia|Andalusia)\b"),
    ("Brazil", -14.2350, -51.9253, "Latin America", r"(?i)\b(Brazil|Brazilian|São Paulo|Rio de Janeiro|Brasília|Bahia)\b"),
    ("Russia", 61.5240, 105.3188, "Europe", r"(?i)\b(Russia|Russian|Moscow|Saint Petersburg|Soviet|Siberia)\b"),
    ("Ukraine", 48.3794, 31.1656, "Europe", r"(?i)\b(Ukraine|Ukrainian|Kyiv|Kharkiv|Odesa|Donbas|Crimea)\b"),
    ("Indonesia", -0.7893, 113.9213, "East & Southeast Asia", r"(?i)\b(Indonesia|Indonesian|Jakarta|Bali|Java|Sumatra)\b"),
    ("Iran", 32.4279, 53.6880, "Middle East & Africa", r"(?i)\b(Iran|Iranian|Tehran|Persian)\b"),
    ("Israel", 31.0461, 34.8516, "Middle East & Africa", r"(?i)\b(Israel|Israeli|Jerusalem|Tel Aviv|Haifa)\b"),
    ("Philippines", 12.8797, 121.7740, "East & Southeast Asia", r"(?i)\b(Philippines|Philippine|Filipino|Manila|Batangas|Cebu|Mindanao|Luzon)\b"),
    ("Poland", 51.9194, 19.1451, "Europe", r"(?i)\b(Poland|Polish|Warsaw|Kraków|Gdańsk)\b"),
    ("Mexico", 23.6345, -102.5528, "Latin America", r"(?i)\b(Mexico|Mexican|Mexico City|Guadalajara|Monterrey)\b"),
    ("South Korea", 35.9078, 127.7669, "East & Southeast Asia", r"(?i)\b(South Korea|Korean|Seoul|Busan|Incheon)\b"),
    ("Pakistan", 30.3753, 69.3451, "South Asia", r"(?i)\b(Pakistan|Pakistani|Karachi|Lahore|Islamabad)\b"),
    ("Bangladesh", 23.6850, 90.3563, "South Asia", r"(?i)\b(Bangladesh|Bangladeshi|Dhaka|Chittagong)\b"),
    ("Greece", 39.0742, 21.8243, "Europe", r"(?i)\b(Greece|Greek|Athens|Thessaloniki)\b"),
    ("Ireland", 53.1424, -7.6921, "Europe", r"(?i)\b(Ireland|Irish|Dublin|Cork)\b"),
    ("Sweden", 60.1282, 18.6435, "Europe", r"(?i)\b(Sweden|Swedish|Stockholm|Gothenburg)\b"),
    ("New Zealand", -40.9006, 174.8860, "Oceania", r"(?i)\b(New Zealand|Auckland|Wellington|Christchurch)\b"),
    ("Egypt", 26.8206, 30.8025, "Middle East & Africa", r"(?i)\b(Egypt|Egyptian|Cairo|Alexandria)\b"),
    ("Saudi Arabia", 23.8859, 45.0792, "Middle East & Africa", r"(?i)\b(Saudi Arabia|Saudi|Riyadh|Jeddah|Mecca)\b"),
    ("South Africa", -30.5595, 22.9375, "Middle East & Africa", r"(?i)\b(South Africa|South African|Johannesburg|Cape Town|Pretoria)\b"),
    ("Argentina", -38.4161, -63.6167, "Latin America", r"(?i)\b(Argentina|Argentine|Buenos Aires)\b"),
    ("Turkey", 38.9637, 35.2433, "Middle East & Africa", r"(?i)\b(Turkey|Turkish|Istanbul|Ankara)\b"),
    ("Nigeria", 9.0820, 8.6753, "Middle East & Africa", r"(?i)\b(Nigeria|Nigerian|Lagos|Abuja)\b"),
    ("Paraguay", -23.4425, -58.4438, "Latin America", r"(?i)\b(Paraguay|Paraguayan|Asunción)\b"),
    ("Nepal", 28.3949, 84.1240, "South Asia", r"(?i)\b(Nepal|Nepali|Nepalese|Kathmandu)\b"),
    ("Maldives", 3.2028, 73.2207, "South Asia", r"(?i)\b(Maldives|Maldivian)\b"),
    ("Ghana", 7.9465, -1.0232, "Middle East & Africa", r"(?i)\b(Ghana|Ghanaian|Accra)\b"),
    ("Croatia", 45.1000, 15.2000, "Europe", r"(?i)\b(Croatia|Croatian|Zagreb)\b"),
    ("Equatorial Guinea", 1.6508, 10.2679, "Middle East & Africa", r"(?i)\b(Equatorial Guinea)\b"),
)

COUNTRY_ALIASES = {
    "People's Republic of China": "China",
    "Ancient Rome": "Italy",
    "Kingdom of Italy": "Italy",
    "Kingdom of Saxony": "Germany",
    "Russian Empire": "Russia",
    "Soviet Union": "Russia",
}

COUNTRY_TO_MACRO = {row[0]: row[3] for row in RECENTCHANGES_GEO_GAZETTEER}
_COMPILED_GEO_GAZETTEER = [
    (cname, float(c_lat), float(c_lon), macro, re.compile(pattern))
    for cname, c_lat, c_lon, macro, pattern in RECENTCHANGES_GEO_GAZETTEER
]

GEO_TIME_RANGES = {
    "55d": ("2026-09-15", "2026-11-08", None, None, "All 55 Curated Days (2026-09-15 – 2026-11-08)"),
    "30d": ("2026-10-10", "2026-11-08", None, None, "Last 30 Curated Days (2026-10-10 – 2026-11-08)"),
    "15d": ("2026-10-25", "2026-11-08", None, None, "Last 15 Curated Days (2026-10-25 – 2026-11-08)"),
    "8d": ("2026-11-01", "2026-11-08", None, None, "Evaluation 8 Days (2026-11-01 – 2026-11-08)"),
    "baseline_47d": ("2026-09-15", "2026-10-31", None, None, "Baseline 47 Days (2026-09-15 – 2026-10-31)"),
    "day_2026-11-04": ("2026-11-04", "2026-11-04", "2026-11-04T00:00:00Z", "2026-11-04T23:59:59Z", "Peak Activity Day · 2026-11-04 (24h)"),
    "day_2026-11-08": ("2026-11-08", "2026-11-08", "2026-11-08T00:00:00Z", "2026-11-08T23:59:59Z", "Final Curated Day · 2026-11-08 (24h)"),
    "win_6h_peak": ("2026-11-04", "2026-11-04", "2026-11-04T12:00:00Z", "2026-11-04T18:00:00Z", "Peak 6-Hour Window · 2026-11-04 12:00–18:00 UTC"),
    "win_6h_latest": ("2026-11-08", "2026-11-08", "2026-11-08T18:00:00Z", "2026-11-08T23:59:59Z", "Final 6-Hour Window · 2026-11-08 18:00–24:00 UTC"),
    "win_1h_latest": ("2026-11-08", "2026-11-08", "2026-11-08T22:00:00Z", "2026-11-08T23:59:59Z", "Final Hour Window · 2026-11-08 22:00–24:00 UTC"),
}


def _window_seconds(duration: str) -> int:
    return WINDOW_SECONDS.get(duration, WINDOW_SECONDS["5m"])


def _window_table_duration(duration: str) -> str:
    return LIVE_WINDOW_TABLES.get(duration, "5m")


def _utc_timestamp(value) -> str | None:
    if value is None:
        return None
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class DashboardPaths:
    """Locations can be overridden without modifying any presentation component."""

    raw: Path = Path(RAW_PATH)
    live: Path = ROOT / "data/live_activity"
    history: Path = ROOT / "data/week2/historical"
    curated: Path = CURATED_DIR
    runtime: Path = ROOT / "data/run"


def has_table(path: Path) -> bool:
    """An absent dataset is a normal empty state, not a failed Spark query."""
    return path.is_dir() and next(path.rglob("*.parquet"), None) is not None


def records(frame: DataFrame, limit: int = 100) -> list[dict]:
    """Serialize timestamps in the Spark session's UTC timezone before collection."""
    return [json.loads(row) for row in frame.limit(limit).toJSON().collect()]


class DashboardQueries:
    """Reusable read-only queries sharing the existing normalization and features."""

    def __init__(self, spark: SparkSession, paths: DashboardPaths):
        self.spark = spark
        self.paths = paths

    def _range_bounds(self, duration: str) -> tuple[str, str]:
        seconds = _window_seconds(duration)
        row = self.spark.range(1).select(
            F.date_format(
                F.current_timestamp() - F.expr(f"INTERVAL {seconds} SECONDS"),
                "yyyy-MM-dd'T'HH:mm:ss'Z'",
            ).alias("start"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("end"),
        ).first()
        return row.start, row.end

    @staticmethod
    def _empty_activity(duration: str, view: str, data_source: str) -> dict:
        return {
            "metrics": {"edits": 0, "human": 0, "bots": 0, "pages": 0,
                        "gross_bytes": 0, "net_bytes": 0, "reverts": 0, "known_sizes": 0},
            "timeline": [], "rankings": [], "comparisons": [], "latest": None,
            "selected": None, "range": None, "duration": duration,
            "bucket": LIVE_BUCKETS.get(duration, "5 minutes"),
            "scope": f"Past {WINDOW_LABELS.get(duration, '5 minutes')} · IST",
            "data_source": data_source, "view": view,
        }

    def table(self, name: str) -> DataFrame | None:
        """Resolve one optional curated table from the configured roots."""
        for root in (self.paths.history, self.paths.curated):
            path = root / name
            if has_table(path):
                return self.spark.read.option("pathGlobFilter", "*.parquet").parquet(str(path))
        return None

    def _parquet_files(self, name: str, prefer_curated_lake: bool = True) -> list[str]:
        """Return non-empty Parquet file paths for a curated table."""
        roots = [CURATED_DIR, self.paths.curated, self.paths.history]
        if not prefer_curated_lake or self.paths.curated not in (ROOT / "data/curated", CURATED_DIR):
            roots = [self.paths.curated, self.paths.history, CURATED_DIR]
        seen = set()
        for root in roots:
            if root in seen:
                continue
            seen.add(root)
            path = root / name
            if path.is_dir():
                files = sorted(str(f) for f in path.rglob("*.parquet") if f.is_file() and f.stat().st_size > 0)
                if files:
                    return files
        return []

    def historical_table(self, name: str) -> DataFrame | None:
        """Read strictly from the historical curated lake (`ingest_curate`), never live stream outputs."""
        if self.spark is None:
            return None
        roots = [CURATED_DIR, self.paths.curated, self.paths.history]
        if self.paths.curated not in (ROOT / "data/curated", CURATED_DIR):
            roots = [self.paths.curated, self.paths.history, CURATED_DIR]
        seen = set()
        for root in roots:
            if root in seen:
                continue
            seen.add(root)
            path = root / name
            if has_table(path):
                return self.spark.read.option("pathGlobFilter", "*.parquet").parquet(str(path))
        return None

    @staticmethod
    def _normalize_arrow_row(row: dict) -> dict:
        out = {}
        for k, v in row.items():
            if hasattr(v, "strftime"):
                out[k] = v.strftime("%Y-%m-%dT%H:%M:%SZ") if hasattr(v, "hour") else v.strftime("%Y-%m-%d")
            elif isinstance(v, list) and v and isinstance(v[0], tuple) and len(v[0]) == 2:
                out[k] = {str(mk): mv for mk, mv in v}
            else:
                out[k] = v
        return out

    def historical_optional_records(self, name: str, title: str | None = None) -> list[dict]:
        """Return bounded records strictly from the historical curated lake (`ingest_curate`)."""
        if pa_ds is not None and name in ("graph_edges", "revision_diffs", "map_points", "revert_risk_eval"):
            files = self._parquet_files(name, prefer_curated_lake=True)
            if files:
                rows = [
                    self._normalize_arrow_row(r)
                    for r in pa_ds.dataset(files, format="parquet", partitioning="hive").to_table().to_pylist()
                ]
                if title is not None:
                    rows = [r for r in rows if r.get("title") == title]
                if rows and "window_start" in rows[0]:
                    rows.sort(key=lambda r: str(r.get("window_start") or ""), reverse=True)
                return rows[:200]
        frame = self.historical_table(name)
        if frame is None:
            return []
        if title is not None and "title" in frame.columns:
            frame = frame.filter(F.col("title") == title)
        if "window_start" in frame.columns:
            frame = frame.orderBy(F.desc("window_start"))
        return records(frame, 200)

    def historical_alerts(self, duration: str | None = None) -> list[dict]:
        """Read historical curated alerts produced by the batch `ingest_curate` / M5 pipeline."""
        if pa_ds is not None:
            files = self._parquet_files("alerts", prefer_curated_lake=True)
            if files:
                expected_durations = None
                if duration:
                    source_duration = _window_table_duration(duration)
                    expected_durations = (source_duration, "5 minutes" if source_duration == "5m" else "1 hour")
                raw = pa_ds.dataset(files, format="parquet", partitioning="hive").to_table().to_pylist()
                seen_ids: set[str] = set()
                rows: list[dict] = []
                for item in raw:
                    r = self._normalize_arrow_row(item)
                    if r.get("is_injected_synthetic"):
                        continue
                    if expected_durations and r.get("window_duration") not in expected_durations:
                        continue
                    aid = str(r.get("alert_id") or "")
                    if aid and aid in seen_ids:
                        continue
                    if aid:
                        seen_ids.add(aid)
                    fc = r.get("feature_contributions")
                    if isinstance(fc, dict) and "activity_points" not in r:
                        r["activity_points"] = fc.get("edit_velocity")
                        r["editor_points"] = fc.get("editor_diversity")
                        r["content_points"] = fc.get("byte_churn")
                        r["revert_points"] = fc.get("consensus_or_dispute")
                        r["pageview_points"] = fc.get("pageview_corroboration")
                    rows.append(r)
                rows.sort(
                    key=lambda r: (float(r.get("historical_score") or 0.0), str(r.get("window_start") or "")),
                    reverse=True,
                )
                return rows[:200]
        frame = self.historical_table("alerts")
        if frame is None:
            return []
        if "is_injected_synthetic" in frame.columns:
            frame = frame.filter(~F.coalesce(F.col("is_injected_synthetic"), F.lit(False)))
        if duration:
            source_duration = _window_table_duration(duration)
            expected_durations = (source_duration, "5 minutes" if source_duration == "5m" else "1 hour")
            frame = frame.filter(F.col("window_duration").isin(*expected_durations))
        if "feature_contributions" in frame.columns and "activity_points" not in frame.columns:
            frame = (
                frame.withColumn("activity_points", F.col("feature_contributions")["edit_velocity"])
                .withColumn("editor_points", F.col("feature_contributions")["editor_diversity"])
                .withColumn("content_points", F.col("feature_contributions")["byte_churn"])
                .withColumn("revert_points", F.col("feature_contributions")["consensus_or_dispute"])
                .withColumn("pageview_points", F.col("feature_contributions")["pageview_corroboration"])
            )
        return records(frame.dropDuplicates(["alert_id"]).orderBy(F.desc("historical_score"), F.desc("window_start")), 200)

    def table_available(self, name: str) -> bool:
        """Check for a populated table without asking Spark to scan its contents."""
        return any(has_table(root / name) for root in (self.paths.history, self.paths.curated))

    def replay_bounds(self, duration: str) -> tuple[int | None, int | None]:
        """Get the event-time range available for read-only dashboard replay."""
        source_duration = _window_table_duration(duration)
        path = self.paths.live / f"windows_{source_duration}"
        frame = (
            self.spark.read.option("pathGlobFilter", "*.parquet").parquet(str(path))
            if has_table(path) else self.table(f"page_windows_{source_duration}")
        )
        if frame is None or "window_end" not in frame.columns:
            return None, None
        row = frame.agg(
            F.min("window_end").cast("long").alias("first"),
            F.max("window_end").cast("long").alias("last"),
        ).first()
        return row.first, row.last

    def optional_records(self, name: str, title: str | None = None) -> list[dict]:
        frame = self.table(name)
        if frame is None:
            return []
        if title is not None and "title" in frame.columns:
            frame = frame.filter(F.col("title") == title)
        if "window_start" in frame.columns:
            frame = frame.orderBy(F.desc("window_start"))
        return records(frame, 200)

    def alerts(
        self, duration: str, title: str | None = None,
        range_start: str | None = None, range_end: str | None = None,
    ) -> list[dict]:
        """Read finalized alerts for the chosen duration, independently of previews."""
        frame = self.table("alerts")
        if frame is None:
            return []
        source_duration = _window_table_duration(duration)
        expected_durations = (source_duration, "5 minutes" if source_duration == "5m" else "1 hour")
        frame = frame.filter(F.col("window_duration").isin(*expected_durations))
        if range_start and "window_start" in frame.columns:
            window_seconds = _window_seconds(source_duration)
            frame = frame.filter(
                (F.col("window_start") + F.expr(f"INTERVAL {window_seconds} SECONDS")
                 > F.to_timestamp(F.lit(range_start)))
            )
        if range_end and "window_start" in frame.columns:
            frame = frame.filter(F.col("window_start") <= F.to_timestamp(F.lit(range_end)))
        if title:
            frame = frame.filter(F.col("title") == title)
        if "feature_contributions" in frame.columns and "activity_points" not in frame.columns:
            frame = (
                frame.withColumn(
                    "confidence",
                    F.when(F.col("pageview_corroborated"), F.lit("corroborated"))
                    .when(F.col("historical_alert_flag") & F.col("spark_ml_alert_flag"), F.lit("high_consensus"))
                    .otherwise(F.lit("baseline_normalized")),
                )
                .withColumn("activity_points", F.col("feature_contributions")["edit_velocity"])
                .withColumn("editor_points", F.col("feature_contributions")["editor_diversity"])
                .withColumn("content_points", F.col("feature_contributions")["byte_churn"])
                .withColumn("revert_points", F.col("feature_contributions")["consensus_or_dispute"])
                .withColumn("pageview_points", F.col("feature_contributions")["pageview_corroboration"])
                .withColumn(
                    "available_weight",
                    F.when(F.col("pageview_ratio_vs_baseline").isNotNull(), F.lit(100)).otherwise(F.lit(90)),
                )
                .withColumn(
                    "missing_evidence",
                    F.when(F.col("pageview_ratio_vs_baseline").isNull(), F.array(F.lit("pageviews"))).otherwise(
                        F.array().cast("array<string>")
                    ),
                )
            )
        # A consumer/recovery can deliver an alert again. Identity is stable for
        # one page-window and score version; the display shows it only once.
        return records(frame.dropDuplicates(["alert_id"]).orderBy(F.desc("window_start")), 200)

    def recent_edits(self, duration: str) -> tuple[DataFrame | None, str | None, str | None, str | None]:
        """Read exactly the selected rolling horizon from the live Bronze lake.

        The horizon is anchored to the current UTC clock, not the newest event.
        That prevents a stalled stream from making an old chart look current.
        """
        if not has_table(self.paths.raw):
            return None, None, None, None
        bronze = self.spark.read.option("pathGlobFilter", "*.parquet").parquet(str(self.paths.raw)).filter(
            F.col("ingestion_date") >= F.date_sub(F.current_date(), 1)
        )
        # Event-time metadata is typed in Bronze; apply the range before parsing
        # the event JSON so the 6-hour choice does not grow with retained history.
        candidates = bronze.filter(
            (F.col("wiki") == "enwiki") & (F.col("change_type") == "edit")
            & (F.col("namespace") == 0)
        ).withColumn("_event_ts", F.to_timestamp("event_time"))
        seconds = _window_seconds(duration)
        lower_bound = F.expr(f"current_timestamp() - INTERVAL {seconds} SECONDS")
        candidates = candidates.filter(
            (F.col("_event_ts") >= lower_bound)
            & (F.col("_event_ts") <= F.current_timestamp())
        )
        latest = candidates.agg(
            F.date_format(F.max("_event_ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("utc"),
            F.date_format(lower_bound, "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("range_start"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("range_end"),
        ).first()
        edits = live_edits(candidates).dropDuplicates(["event_id"])
        return edits, latest.utc, latest.range_start, latest.range_end

    def activity(
        self, duration: str, view: str, title: str | None = None,
        replay_cursor: int | None = None, focus: bool = False,
    ) -> dict:
        """Aggregate the selected live or finalized event-time horizon."""
        latest = None
        windows = None
        range_start = range_end = None
        data_source = "live Bronze events"
        source_duration = _window_table_duration(duration)
        seconds = _window_seconds(duration)
        if view == "live":
            edits, latest, range_start, range_end = self.recent_edits(duration)
            if edits is not None:
                if latest is None:
                    empty = self._empty_activity(duration, view, "live Bronze events · no edits in range")
                    empty["range"] = {"start": range_start, "end": range_end}
                    empty["view"] = view
                    return empty
                if focus and not title:
                    leader = edits.groupBy("title").agg(F.count("*").alias("edits")) \
                        .orderBy(F.desc("edits"), "title").first()
                    title = leader.title if leader else None
                if title:
                    edits = edits.filter(F.col("title") == title)
                windows = page_windows(edits, LIVE_BUCKETS[duration])
            else:
                # If raw Bronze is unavailable, use only the running stream's
                # window output. Never silently substitute historical tables in
                # Live Preview, since that makes stale data look live.
                path = self.paths.live / f"windows_{source_duration}"
                if not has_table(path):
                    return self._empty_activity(duration, view, data_source="No live source available")
                windows = self.spark.read.option("pathGlobFilter", "*.parquet").parquet(str(path))
                data_source = "live streaming windows"
                range_bounds = self._range_bounds(duration)
                range_start, range_end = range_bounds
                windows = windows.filter(
                    (F.col("window_end") > F.to_timestamp(F.lit(range_start)))
                    & (F.col("window_end") <= F.to_timestamp(F.lit(range_end)))
                )
                if "ns" in windows.columns:
                    windows = windows.filter(F.col("ns") == 0)
        else:
            data_source = "live finalized windows"
            path = self.paths.live / f"windows_{source_duration}"
            if has_table(path):
                windows = self.spark.read.option("pathGlobFilter", "*.parquet").parquet(str(path))
            else:
                windows = self.table(f"page_windows_{source_duration}")
                data_source = "curated finalized windows"
                if windows is not None:
                    windows = windows.filter(F.col("ns") == 0)
            if windows is None:
                return self._empty_activity(duration, view, data_source="No finalized window source available")
            if view == "replay" and replay_cursor is not None:
                range_end = _utc_timestamp(datetime.fromtimestamp(replay_cursor, timezone.utc))
                range_start = _utc_timestamp(datetime.fromtimestamp(replay_cursor - seconds, timezone.utc))
                windows = windows.filter(
                    (F.col("window_end").cast("long") <= replay_cursor)
                    & (F.col("window_end").cast("long") > replay_cursor - seconds)
                )
            elif data_source == "curated finalized windows":
                max_row = windows.agg(F.max("window_end").cast("long").alias("max_end")).first()
                max_end = max_row.max_end if max_row else None
                if max_end is not None:
                    range_end = _utc_timestamp(datetime.fromtimestamp(max_end, timezone.utc))
                    range_start = _utc_timestamp(datetime.fromtimestamp(max_end - seconds, timezone.utc))
                    windows = windows.filter(
                        (F.col("window_end").cast("long") <= max_end)
                        & (F.col("window_end").cast("long") > max_end - seconds)
                    )
                else:
                    range_start, range_end = self._range_bounds(duration)
            else:
                range_start, range_end = self._range_bounds(duration)
                windows = windows.filter(
                    (F.col("window_end") > F.to_timestamp(F.lit(range_start)))
                    & (F.col("window_end") <= F.to_timestamp(F.lit(range_end)))
                )
            if "ns" in windows.columns:
                windows = windows.filter(F.col("ns") == 0)
            latest = windows.agg(
                F.date_format(F.max("window_end"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("utc")
            ).first().utc
            if latest is None:
                empty = self._empty_activity(duration, view, data_source)
                empty["range"] = {"start": range_start, "end": range_end}
                return empty
            if title:
                windows = windows.filter(F.col("title") == title)

        if focus and not title:
            leader = windows.groupBy("title").agg(F.sum("edits_total").alias("edits")) \
                .orderBy(F.desc("edits"), "title").first()
            title = leader.title if leader else None
            if title:
                windows = windows.filter(F.col("title") == title)

        if "edits_with_sizes" not in windows.columns:
            windows = windows.withColumn("edits_with_sizes", F.col("edits_total"))

        windows.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            totals = windows.agg(
                F.sum("edits_total").alias("edits"),
                F.sum("edits_human").alias("human"),
                F.sum("edits_bot").alias("bots"),
                F.countDistinct("pageid").alias("pages"),
                F.sum("gross_bytes").alias("gross_bytes"),
                F.sum("net_bytes").alias("net_bytes"),
                F.sum("reverts_count").alias("reverts"),
                F.sum("edits_with_sizes").alias("known_sizes"),
            )
            metrics = records(totals, 1)[0]
            timeline = windows.groupBy("window_start").agg(
                F.sum("edits_human").alias("human"),
                F.sum("edits_bot").alias("bots"),
                F.sum("gross_bytes").alias("gross_bytes"),
                F.sum("net_bytes").alias("net_bytes"),
            ).orderBy("window_start")
            rankings = windows.groupBy("pageid", "ns").agg(
                F.max_by("title", "window_end").alias("title"),
                F.sum("edits_total").alias("edits"),
                F.sum("edits_human").alias("human"),
                F.sum("edits_bot").alias("bots"),
                F.sum("gross_bytes").alias("gross_bytes"),
                F.sum("net_bytes").alias("net_bytes"),
                F.sum("reverts_count").alias("reverts"),
                F.max("distinct_human_editors").alias("peak_window_editors"),
            ).withColumn(
                "rewriting_bytes", F.col("gross_bytes") - F.abs("net_bytes")
            ).orderBy(F.desc("edits"), "title")
            # Baseline comparisons are kept at matching window granularity.
            if view == "live":
                baseline_duration = "1h" if LIVE_BUCKETS[duration] == "1 hour" else "5m"
            else:
                baseline_duration = source_duration
            baseline = self.table(f"baselines_{baseline_duration}")
            comparisons = []
            if baseline is not None:
                compared = compare_history(windows, baseline).filter("has_baseline")
                comparisons = records(compared.orderBy(F.desc("window_start")), 100)
            else:
                page_base = self.table("page_baselines")
                if page_base is not None:
                    div = 12.0 if LIVE_BUCKETS[duration] == "5 minutes" else 1.0
                    compared = windows.join(
                        page_base.select(
                            "pageid",
                            F.round(F.col("avg_hourly_edits") / F.lit(div), 3).alias("avg_edits_human"),
                            F.round(F.col("std_hourly_edits") / F.lit(div), 3).alias("std_edits_human"),
                        ),
                        on="pageid",
                        how="inner",
                    )
                    comparisons = records(compared.orderBy(F.desc("window_start")), 100)
            return {
                "metrics": metrics,
                "timeline": records(timeline, 300),
                "rankings": records(rankings, 100),
                "comparisons": comparisons,
                "latest": latest,
                "selected": title if focus else None,
                "scope": f"Past {WINDOW_LABELS[duration]} · IST",
                "range": {"start": range_start, "end": range_end},
                "data_source": data_source,
                "view": view,
                "duration": duration,
                "bucket": LIVE_BUCKETS[duration] if view == "live" else WINDOW_LABELS[source_duration],
            }
        finally:
            windows.unpersist()

    def historical(self, start: str | None, end: str | None) -> dict:
        """Aggregate compact canonical summaries; never scan the full event lake interactively."""
        churn_bot = self.historical_table("churn_bot_summary")
        rankings_daily = self.historical_table("rankings_daily")
        missing = [
            name for name, frame in (
                ("churn_bot_summary", churn_bot), ("rankings_daily", rankings_daily)
            ) if frame is None
        ]
        if missing:
            return {
                "metrics": {}, "timeline": [], "rankings": [], "baseline": [],
                "missing_sources": [str(self.paths.curated / name) for name in missing],
            }

        cb = churn_bot.filter(F.col("ns") == 0)
        rk = rankings_daily
        if start:
            cb = cb.filter(F.col("day") >= start)
            rk = rk.filter(F.col("day") >= start)
        if end:
            cb = cb.filter(F.col("day") <= end)
            rk = rk.filter(F.col("day") <= end)
        totals = cb.agg(
            F.sum("total_edits").alias("edits"),
            F.sum("bot_edits").alias("bots"),
            F.sum("gross_churn_bytes").alias("gross_bytes"),
            F.sum("net_bytes").alias("net_bytes"),
            F.min("day").cast("string").alias("first_day"),
            F.max("day").cast("string").alias("last_day"),
            F.countDistinct("day").alias("observed_days"),
        )
        metrics = records(totals, 1)[0]
        pages_row = rk.agg(F.countDistinct("pageid").alias("pages")).first()
        metrics["pages"] = int(pages_row.pages or 0) if pages_row else 0
        timeline = cb.select(
            "day",
            F.col("total_edits").alias("edits"),
            F.col("bot_edits").alias("bots"),
            F.col("gross_churn_bytes").alias("gross_bytes"),
            "net_bytes",
        ).orderBy("day")
        ranking_aggs = [
            F.sum("edits_total").alias("edits"),
            F.sum("gross_churn_bytes").alias("gross_bytes"),
        ]
        if "edits_human" in rk.columns:
            ranking_aggs.append(F.sum("edits_human").alias("human"))
        if "edits_bot" in rk.columns:
            ranking_aggs.append(F.sum("edits_bot").alias("bots"))
        if "net_bytes" in rk.columns:
            ranking_aggs.append(F.sum("net_bytes").alias("net_bytes"))
        if "rewriting_bytes" in rk.columns:
            ranking_aggs.append(F.sum("rewriting_bytes").alias("rewriting_bytes"))
        if "distinct_editors" in rk.columns:
            ranking_aggs.append(F.max("distinct_editors").alias("peak_editors"))
        ranking = rk.groupBy("title").agg(*ranking_aggs).orderBy(F.desc("edits"), "title")
        ranking_rows = records(ranking, 50)

        # Optional contextual & anomaly enrichment for the Historical Intelligence view
        alerts_df = self.historical_table("alerts")
        daily_alerts = []
        alert_stats_by_title = {}
        if alerts_df is not None:
            al = alerts_df
            if "is_injected_synthetic" in al.columns:
                al = al.filter(~F.coalesce(F.col("is_injected_synthetic"), F.lit(False)))
            if start and "day" in al.columns:
                al = al.filter(F.col("day") >= start)
            if end and "day" in al.columns:
                al = al.filter(F.col("day") <= end)
            if "day" in al.columns and "alert_type" in al.columns:
                daily_agg = (
                    al.groupBy(F.col("day").cast("string").alias("day"))
                    .agg(
                        F.count("*").alias("total_alerts"),
                        F.sum(F.when(F.col("alert_type") == "CONFIRMED_NEWS", 1).otherwise(0)).alias("confirmed_news"),
                        F.sum(F.when(F.col("alert_type") == "BREAKING_NEWS_CANDIDATE", 1).otherwise(0)).alias("breaking_news"),
                        F.sum(F.when(F.col("alert_type") == "EDIT_WAR_CONTROVERSY", 1).otherwise(0)).alias("edit_war"),
                        F.sum(F.when(F.col("alert_type") == "MAJOR_REWRITE", 1).otherwise(0)).alias("major_rewrite"),
                    )
                    .orderBy("day")
                )
                daily_alerts = records(daily_agg, 366)
            if ranking_rows and "title" in al.columns:
                top_titles = [r["title"] for r in ranking_rows if r.get("title")]
                per_title = (
                    al.filter(F.col("title").isin(*top_titles))
                    .groupBy("title")
                    .agg(
                        F.count("*").alias("detected_shifts"),
                        F.round(F.max("historical_score"), 2).alias("peak_anomaly_score"),
                        F.round(F.max("spark_ml_score"), 2).alias("peak_ml_score"),
                    )
                )
                for row in records(per_title, 100):
                    alert_stats_by_title[row["title"]] = row

        map_df = self.historical_table("map_points")
        location_by_title = {}
        if map_df is not None and ranking_rows and "title" in map_df.columns:
            top_titles = [r["title"] for r in ranking_rows if r.get("title")]
            loc_rows = (
                map_df.filter(F.col("title").isin(*top_titles))
                .groupBy("title")
                .agg(
                    F.first("country_or_region", ignorenulls=True).alias("country_or_region"),
                    F.first("location_source", ignorenulls=True).alias("location_source"),
                )
            )
            for row in records(loc_rows, 100):
                location_by_title[row["title"]] = row

        for r in ranking_rows:
            t = r.get("title")
            a_stat = alert_stats_by_title.get(t, {})
            l_stat = location_by_title.get(t, {})
            r["detected_shifts"] = a_stat.get("detected_shifts", 0)
            r["peak_anomaly_score"] = a_stat.get("peak_anomaly_score")
            r["peak_ml_score"] = a_stat.get("peak_ml_score")
            r["subject_location"] = l_stat.get("country_or_region") or "—"

        pb_df = self.historical_table("page_baselines")
        baseline_tiers = []
        if pb_df is not None and "activity_tier" in pb_df.columns:
            tier_agg = (
                pb_df.groupBy("activity_tier")
                .agg(
                    F.count("*").alias("pages_count"),
                    F.round(F.avg("avg_daily_edits"), 2).alias("mean_daily_edits"),
                    F.round(F.avg("baseline_revert_ratio") * 100.0, 2).alias("mean_revert_pct"),
                    F.round(F.avg("baseline_bot_ratio") * 100.0, 2).alias("mean_bot_pct"),
                )
                .orderBy(F.desc("pages_count"))
            )
            baseline_tiers = records(tier_agg, 10)

        return {
            "metrics": metrics,
            "timeline": records(timeline, 366),
            "rankings": ranking_rows,
            "baseline": self.historical_optional_records("page_baselines") or self.historical_optional_records("baselines_5m"),
            "daily_alerts": daily_alerts,
            "baseline_tiers": baseline_tiers,
        }

    def geospatial_recentchanges(
        self, mode: str = "days", time_range: str = "55d", region: str = "ALL"
    ) -> dict:
        """Aggregate regional RecentChanges edits across selected days or windows (`ingest_curate`)."""
        mode_norm = mode if mode in ("days", "windows_5m", "windows_1h") else "days"
        range_key = time_range if time_range in GEO_TIME_RANGES else "55d"
        day_start, day_end, ts_start, ts_end, range_label = GEO_TIME_RANGES[range_key]
        selected_region = (region or "ALL").strip()
        available_regions = ["ALL"] + sorted(set(COUNTRY_TO_MACRO.values())) + [
            row[0] for row in RECENTCHANGES_GEO_GAZETTEER[:25]
        ]

        # Fast PyArrow execution over curated Parquet lake (avoids Spark whole-stage codegen overhead)
        if pa_ds is not None:
            effective_mode = mode_norm
            files: list[str] = []
            is_window_table = False
            if mode_norm in ("windows_5m", "windows_1h") and day_end >= "2026-11-01":
                tname = "page_windows_5m" if mode_norm == "windows_5m" else "page_windows_1h"
                files = self._parquet_files(tname, prefer_curated_lake=True)
                if files:
                    is_window_table = True
            if not files:
                effective_mode = "days"
                files = self._parquet_files("rankings_daily", prefer_curated_lake=True)

            if files:
                mp_by_pageid: dict[int, tuple[float, float, str, str, str, str]] = {}
                mp_files = self._parquet_files("map_points", prefer_curated_lake=True)
                if mp_files:
                    mp_rows = pa_ds.dataset(mp_files, format="parquet").to_table().to_pylist()
                    for m in mp_rows:
                        if m.get("has_coordinates") and m.get("lat") is not None and m.get("lon") is not None:
                            pid = int(m.get("pageid") or 0)
                            raw_c = str(m.get("country_or_region") or "").strip()
                            norm_c = COUNTRY_ALIASES.get(raw_c, raw_c)
                            if pid and norm_c:
                                mp_by_pageid[pid] = (
                                    float(m["lat"]),
                                    float(m["lon"]),
                                    norm_c,
                                    str(m.get("location_source") or "map_points"),
                                    str(m.get("location_quality") or "exact_article_coord"),
                                    str(m.get("wikibase_item") or "—"),
                                )

                eff_day_start = max(day_start, "2026-11-01") if is_window_table else day_start
                # Partition-prune files when Hive-partitioned by day=YYYY-MM-DD
                pruned_files = []
                for fp in files:
                    m_day = re.search(r"day=(\d{4}-\d{2}-\d{2})", fp)
                    if m_day:
                        d_val = m_day.group(1)
                        if d_val < eff_day_start or d_val > day_end:
                            continue
                    pruned_files.append(fp)
                cols = (
                    ["pageid", "title", "window_start", "edits_total", "edits_human", "edits_bot", "gross_bytes", "net_bytes", "reverts_count"]
                    if is_window_table
                    else ["pageid", "title", "day", "edits_total", "edits_human", "edits_bot", "gross_churn_bytes", "net_bytes", "reverts_count"]
                )
                raw_rows = []
                if pruned_files:
                    ds = pa_ds.dataset(pruned_files, format="parquet", partitioning="hive")
                    avail_cols = [c for c in cols if c in ds.schema.names]
                    raw_rows = ds.to_table(columns=avail_cols).to_pylist()

                title_geo_cache: dict[str, tuple[float, float, str, str, str, str] | None] = {}

                def resolve_geo(pid: int, title: str):
                    if pid in mp_by_pageid:
                        return mp_by_pageid[pid]
                    if title in title_geo_cache:
                        return title_geo_cache[title]
                    for cname, c_lat, c_lon, _, cregex in _COMPILED_GEO_GAZETTEER:
                        if cregex.search(title):
                            res = (c_lat, c_lon, cname, "recentchanges_subject", "country_centroid", "—")
                            title_geo_cache[title] = res
                            return res
                    title_geo_cache[title] = None
                    return None

                total_edits = 0
                human_edits = 0
                bot_edits = 0
                gross_bytes = 0
                net_bytes = 0
                reverts = 0
                active_pages_set: set[int] = set()
                first_bucket: str | None = None
                last_bucket: str | None = None

                country_map: dict[tuple[str, str], dict] = {}
                macro_map: dict[str, dict] = {}
                article_map: dict[int, dict] = {}
                timeline_map: dict[str, dict] = {}

                for r in raw_rows:
                    if is_window_table:
                        ws = r.get("window_start")
                        bkt = ws.strftime("%Y-%m-%dT%H:%M:%SZ") if hasattr(ws, "strftime") else str(ws or r.get("day") or "")
                        d_str = bkt[:10]
                        if d_str < eff_day_start or d_str > day_end:
                            continue
                        if ts_start and bkt < ts_start:
                            continue
                        if ts_end and bkt > ts_end:
                            continue
                        g_bytes = int(r.get("gross_bytes") or 0)
                    else:
                        d_val = r.get("day")
                        bkt = d_val.strftime("%Y-%m-%d") if hasattr(d_val, "strftime") else str(d_val or "")
                        if bkt < eff_day_start or bkt > day_end:
                            continue
                        g_bytes = int(r.get("gross_churn_bytes") or 0)

                    pid = int(r.get("pageid") or 0)
                    title = str(r.get("title") or "")
                    geo = resolve_geo(pid, title)
                    if geo is None:
                        continue
                    lat, lon, cname, l_source, l_qual, qid = geo
                    macro = COUNTRY_TO_MACRO.get(cname, "Other / Global")
                    if selected_region and selected_region != "ALL":
                        if macro != selected_region and cname != selected_region:
                            continue

                    e_tot = int(r.get("edits_total") or 0)
                    e_hum = int(r.get("edits_human") or 0)
                    e_bot = int(r.get("edits_bot") or 0)
                    n_bytes = int(r.get("net_bytes") or 0)
                    rev = int(r.get("reverts_count") or 0)

                    total_edits += e_tot
                    human_edits += e_hum
                    bot_edits += e_bot
                    gross_bytes += g_bytes
                    net_bytes += n_bytes
                    reverts += rev
                    active_pages_set.add(pid)
                    if first_bucket is None or bkt < first_bucket:
                        first_bucket = bkt
                    if last_bucket is None or bkt > last_bucket:
                        last_bucket = bkt

                    ckey = (cname, macro)
                    c_entry = country_map.get(ckey)
                    if c_entry is None:
                        c_entry = {
                            "country_or_region": cname,
                            "macro_region": macro,
                            "pages": set(),
                            "total_edits": 0,
                            "total_edits_human": 0,
                            "total_edits_bot": 0,
                            "gross_bytes": 0,
                            "reverts": 0,
                            "lat": round(lat, 4),
                            "lon": round(lon, 4),
                            "article_counts": {},
                        }
                        country_map[ckey] = c_entry
                    c_entry["pages"].add(pid)
                    c_entry["total_edits"] += e_tot
                    c_entry["total_edits_human"] += e_hum
                    c_entry["total_edits_bot"] += e_bot
                    c_entry["gross_bytes"] += g_bytes
                    c_entry["reverts"] += rev
                    c_entry["article_counts"][title] = c_entry["article_counts"].get(title, 0) + e_tot

                    m_entry = macro_map.get(macro)
                    if m_entry is None:
                        m_entry = {
                            "macro_region": macro,
                            "countries": set(),
                            "pages": set(),
                            "total_edits": 0,
                            "human_edits": 0,
                            "bot_edits": 0,
                            "gross_bytes": 0,
                            "reverts": 0,
                        }
                        macro_map[macro] = m_entry
                    m_entry["countries"].add(cname)
                    m_entry["pages"].add(pid)
                    m_entry["total_edits"] += e_tot
                    m_entry["human_edits"] += e_hum
                    m_entry["bot_edits"] += e_bot
                    m_entry["gross_bytes"] += g_bytes
                    m_entry["reverts"] += rev

                    a_entry = article_map.get(pid)
                    if a_entry is None:
                        a_entry = {
                            "pageid": pid,
                            "title": title,
                            "country_or_region": cname,
                            "macro_region": macro,
                            "location_source": l_source,
                            "location_quality": l_qual,
                            "wikibase_item": qid,
                            "base_lat": lat,
                            "base_lon": lon,
                            "edits_total": 0,
                            "edits_human": 0,
                            "edits_bot": 0,
                            "gross_bytes": 0,
                            "net_bytes": 0,
                            "reverts": 0,
                        }
                        article_map[pid] = a_entry
                    a_entry["edits_total"] += e_tot
                    a_entry["edits_human"] += e_hum
                    a_entry["edits_bot"] += e_bot
                    a_entry["gross_bytes"] += g_bytes
                    a_entry["net_bytes"] += n_bytes
                    a_entry["reverts"] += rev

                    t_entry = timeline_map.get(bkt)
                    if t_entry is None:
                        t_entry = {
                            "bucket": bkt,
                            "edits": 0,
                            "human": 0,
                            "bots": 0,
                            "gross_bytes": 0,
                            "reverts": 0,
                            "pages": set(),
                        }
                        timeline_map[bkt] = t_entry
                    t_entry["edits"] += e_tot
                    t_entry["human"] += e_hum
                    t_entry["bots"] += e_bot
                    t_entry["gross_bytes"] += g_bytes
                    t_entry["reverts"] += rev
                    t_entry["pages"].add(pid)

                country_rollup = []
                for c_entry in country_map.values():
                    top_art = max(c_entry["article_counts"].items(), key=lambda kv: kv[1])[0] if c_entry["article_counts"] else "—"
                    country_rollup.append({
                        "country_or_region": c_entry["country_or_region"],
                        "macro_region": c_entry["macro_region"],
                        "active_topics": len(c_entry["pages"]),
                        "total_edits": c_entry["total_edits"],
                        "total_edits_human": c_entry["total_edits_human"],
                        "total_edits_bot": c_entry["total_edits_bot"],
                        "gross_bytes": c_entry["gross_bytes"],
                        "reverts": c_entry["reverts"],
                        "lat": c_entry["lat"],
                        "lon": c_entry["lon"],
                        "top_article": top_art,
                    })
                country_rollup.sort(key=lambda x: (-x["total_edits"], -x["active_topics"]))

                macro_regions = [
                    {
                        "macro_region": m_entry["macro_region"],
                        "countries_count": len(m_entry["countries"]),
                        "active_topics": len(m_entry["pages"]),
                        "total_edits": m_entry["total_edits"],
                        "human_edits": m_entry["human_edits"],
                        "bot_edits": m_entry["bot_edits"],
                        "gross_bytes": m_entry["gross_bytes"],
                        "reverts": m_entry["reverts"],
                    }
                    for m_entry in macro_map.values()
                ]
                macro_regions.sort(key=lambda x: -x["total_edits"])

                article_rows = sorted(
                    article_map.values(),
                    key=lambda x: (-x["edits_total"], -x["gross_bytes"]),
                )[:150]
                for idx, a in enumerate(article_rows):
                    b_lat = float(a["base_lat"])
                    b_lon = float(a["base_lon"])
                    if a["location_quality"] == "country_centroid":
                        pid = int(a["pageid"] or idx)
                        d_lat = ((pid % 19) - 9) * 0.14
                        d_lon = (((pid // 19) % 23) - 11) * 0.18
                        a["lat"] = round(max(-85.0, min(85.0, b_lat + d_lat)), 4)
                        a["lon"] = round(max(-179.0, min(179.0, b_lon + d_lon)), 4)
                    else:
                        a["lat"] = round(b_lat, 4)
                        a["lon"] = round(b_lon, 4)
                    a["coordinates"] = f"{a['lat']:.2f}°, {a['lon']:.2f}°"

                timeline_rows = [
                    {
                        "bucket": t_entry["bucket"],
                        "edits": t_entry["edits"],
                        "human": t_entry["human"],
                        "bots": t_entry["bots"],
                        "gross_bytes": t_entry["gross_bytes"],
                        "reverts": t_entry["reverts"],
                        "active_pages": len(t_entry["pages"]),
                    }
                    for t_entry in sorted(timeline_map.values(), key=lambda x: x["bucket"])
                ][:400]

                return {
                    "metrics": {
                        "total_edits": total_edits,
                        "human_edits": human_edits,
                        "bot_edits": bot_edits,
                        "gross_bytes": gross_bytes,
                        "net_bytes": net_bytes,
                        "reverts": reverts,
                        "active_pages": len(active_pages_set),
                        "active_countries": len(country_rollup),
                        "active_macro_regions": len(macro_regions),
                        "first_bucket": first_bucket,
                        "last_bucket": last_bucket,
                    },
                    "country_rollup": country_rollup[:50],
                    "macro_regions": macro_regions[:12],
                    "map_articles": article_rows,
                    "timeline": timeline_rows,
                    "available_regions": available_regions,
                    "mode": effective_mode,
                    "requested_mode": mode_norm,
                    "time_range": range_key,
                    "region": selected_region,
                    "range_label": range_label,
                }

        # Select underlying RecentChanges table based on granularity mode & window availability
        base_df = None
        effective_mode = mode_norm
        if mode_norm in ("windows_5m", "windows_1h") and day_end >= "2026-11-01":
            table_name = "page_windows_5m" if mode_norm == "windows_5m" else "page_windows_1h"
            win_df = self.historical_table(table_name)
            if win_df is not None:
                eff_day_start = max(day_start, "2026-11-01")
                w = win_df.filter((F.col("day") >= eff_day_start) & (F.col("day") <= day_end))
                if ts_start:
                    w = w.filter(F.col("window_start") >= F.to_timestamp(F.lit(ts_start)))
                if ts_end:
                    w = w.filter(F.col("window_start") <= F.to_timestamp(F.lit(ts_end)))
                base_df = w.select(
                    F.col("pageid").cast("long").alias("pageid"),
                    F.col("title").cast("string").alias("title"),
                    F.date_format(F.col("window_start"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("bucket"),
                    F.col("edits_total").cast("long").alias("edits_total"),
                    F.col("edits_human").cast("long").alias("edits_human"),
                    F.col("edits_bot").cast("long").alias("edits_bot"),
                    F.col("gross_bytes").cast("long").alias("gross_bytes"),
                    F.col("net_bytes").cast("long").alias("net_bytes"),
                    F.col("reverts_count").cast("long").alias("reverts_count"),
                )

        if base_df is None:
            effective_mode = "days"
            rk_df = self.historical_table("rankings_daily")
            if rk_df is not None:
                r = rk_df.filter((F.col("day") >= day_start) & (F.col("day") <= day_end))
                base_df = r.select(
                    F.col("pageid").cast("long").alias("pageid"),
                    F.col("title").cast("string").alias("title"),
                    F.col("day").cast("string").alias("bucket"),
                    F.col("edits_total").cast("long").alias("edits_total"),
                    F.col("edits_human").cast("long").alias("edits_human"),
                    F.col("edits_bot").cast("long").alias("edits_bot"),
                    F.col("gross_churn_bytes").cast("long").alias("gross_bytes"),
                    F.col("net_bytes").cast("long").alias("net_bytes"),
                    F.col("reverts_count").cast("long").alias("reverts_count"),
                )
            else:
                rc_df = self.historical_table("rc_events")
                if rc_df is not None:
                    rc = rc_df.filter(
                        (F.col("ns") == 0)
                        & (F.col("type") == "edit")
                        & (F.col("day") >= day_start)
                        & (F.col("day") <= day_end)
                    )
                    base_df = rc.groupBy("pageid", "title", F.col("day").cast("string").alias("bucket")).agg(
                        F.count("*").alias("edits_total"),
                        F.sum(F.when(~F.col("is_bot"), 1).otherwise(0)).alias("edits_human"),
                        F.sum(F.when(F.col("is_bot"), 1).otherwise(0)).alias("edits_bot"),
                        F.sum(F.coalesce(F.col("abs_delta"), F.lit(0))).alias("gross_bytes"),
                        F.sum(F.coalesce(F.col("delta"), F.lit(0))).alias("net_bytes"),
                        F.sum(F.when(F.col("is_revert"), 1).otherwise(0)).alias("reverts_count"),
                    )

        map_df = self.historical_table("map_points")
        if base_df is None and map_df is not None:
            loc_only = map_df.filter(F.col("has_coordinates") == True)
            base_df = loc_only.select(
                F.col("pageid").cast("long").alias("pageid"),
                F.col("title").cast("string").alias("title"),
                F.lit(day_end).alias("bucket"),
                F.col("edits_human").cast("long").alias("edits_total"),
                F.col("edits_human").cast("long").alias("edits_human"),
                F.lit(0).cast("long").alias("edits_bot"),
                F.lit(0).cast("long").alias("gross_bytes"),
                F.lit(0).cast("long").alias("net_bytes"),
                F.lit(0).cast("long").alias("reverts_count"),
            )

        if base_df is None:
            return {
                "metrics": {"total_edits": 0, "human_edits": 0, "bot_edits": 0, "gross_bytes": 0,
                            "net_bytes": 0, "reverts": 0, "active_pages": 0, "active_countries": 0,
                            "active_macro_regions": 0},
                "country_rollup": [], "macro_regions": [], "map_articles": [], "timeline": [],
                "available_regions": ["ALL"], "mode": mode_norm, "time_range": range_key,
                "region": region or "ALL", "range_label": range_label,
            }

        # Build Catalyst regex gazetteer expressions for subject geography resolution
        country_expr = F.lit(None).cast("string")
        lat_expr = F.lit(None).cast("double")
        lon_expr = F.lit(None).cast("double")
        for cname, c_lat, c_lon, _, pattern in reversed(RECENTCHANGES_GEO_GAZETTEER):
            cond = F.col("title").rlike(pattern)
            country_expr = F.when(cond, F.lit(cname)).otherwise(country_expr)
            lat_expr = F.when(cond, F.lit(float(c_lat))).otherwise(lat_expr)
            lon_expr = F.when(cond, F.lit(float(c_lon))).otherwise(lon_expr)

        if map_df is not None:
            mp_loc = (
                map_df.filter(F.col("has_coordinates") == True)
                .select(
                    F.col("pageid").cast("long").alias("pageid"),
                    F.col("lat").cast("double").alias("_mp_lat"),
                    F.col("lon").cast("double").alias("_mp_lon"),
                    F.col("country_or_region").cast("string").alias("_mp_country"),
                    F.col("location_source").cast("string").alias("_mp_source"),
                    F.col("location_quality").cast("string").alias("_mp_quality"),
                    F.col("wikibase_item").cast("string").alias("_mp_qid"),
                )
                .dropDuplicates(["pageid"])
            )
            joined = base_df.join(F.broadcast(mp_loc), on="pageid", how="left")
        else:
            joined = (
                base_df.withColumn("_mp_lat", F.lit(None).cast("double"))
                .withColumn("_mp_lon", F.lit(None).cast("double"))
                .withColumn("_mp_country", F.lit(None).cast("string"))
                .withColumn("_mp_source", F.lit(None).cast("string"))
                .withColumn("_mp_quality", F.lit(None).cast("string"))
                .withColumn("_mp_qid", F.lit(None).cast("string"))
            )

        raw_country = F.coalesce(F.col("_mp_country"), country_expr)
        norm_country = raw_country
        for raw_name, canonical_name in COUNTRY_ALIASES.items():
            norm_country = F.when(norm_country == raw_name, F.lit(canonical_name)).otherwise(norm_country)

        macro_expr = F.lit("Other / Global")
        for cname, mregion in COUNTRY_TO_MACRO.items():
            macro_expr = F.when(F.col("country_or_region") == cname, F.lit(mregion)).otherwise(macro_expr)

        located = (
            joined.withColumn("lat", F.coalesce(F.col("_mp_lat"), lat_expr))
            .withColumn("lon", F.coalesce(F.col("_mp_lon"), lon_expr))
            .withColumn("country_or_region", norm_country)
            .withColumn("location_source", F.coalesce(F.col("_mp_source"), F.lit("recentchanges_subject")))
            .withColumn("location_quality", F.coalesce(F.col("_mp_quality"), F.lit("country_centroid")))
            .withColumn("wikibase_item", F.coalesce(F.col("_mp_qid"), F.lit("—")))
            .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull() & F.col("country_or_region").isNotNull())
            .withColumn("macro_region", macro_expr)
        )

        selected_region = (region or "ALL").strip()
        if selected_region and selected_region != "ALL":
            filtered = located.filter(
                (F.col("macro_region") == selected_region) | (F.col("country_or_region") == selected_region)
            )
        else:
            filtered = located

        filtered.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            totals_row = filtered.agg(
                F.coalesce(F.sum("edits_total"), F.lit(0)).alias("total_edits"),
                F.coalesce(F.sum("edits_human"), F.lit(0)).alias("human_edits"),
                F.coalesce(F.sum("edits_bot"), F.lit(0)).alias("bot_edits"),
                F.coalesce(F.sum("gross_bytes"), F.lit(0)).alias("gross_bytes"),
                F.coalesce(F.sum("net_bytes"), F.lit(0)).alias("net_bytes"),
                F.coalesce(F.sum("reverts_count"), F.lit(0)).alias("reverts"),
                F.countDistinct("pageid").alias("active_pages"),
                F.countDistinct("country_or_region").alias("active_countries"),
                F.countDistinct("macro_region").alias("active_macro_regions"),
                F.min("bucket").alias("first_bucket"),
                F.max("bucket").alias("last_bucket"),
            )
            metrics = records(totals_row, 1)[0]

            country_agg = (
                filtered.groupBy("country_or_region", "macro_region")
                .agg(
                    F.countDistinct("pageid").alias("active_topics"),
                    F.sum("edits_total").alias("total_edits"),
                    F.sum("edits_human").alias("total_edits_human"),
                    F.sum("edits_bot").alias("total_edits_bot"),
                    F.sum("gross_bytes").alias("gross_bytes"),
                    F.sum("reverts_count").alias("reverts"),
                    F.round(F.avg("lat"), 4).alias("lat"),
                    F.round(F.avg("lon"), 4).alias("lon"),
                    F.max_by("title", "edits_total").alias("top_article"),
                )
                .orderBy(F.desc("total_edits"), F.desc("active_topics"))
            )
            country_rollup = records(country_agg, 50)

            macro_agg = (
                filtered.groupBy("macro_region")
                .agg(
                    F.countDistinct("country_or_region").alias("countries_count"),
                    F.countDistinct("pageid").alias("active_topics"),
                    F.sum("edits_total").alias("total_edits"),
                    F.sum("edits_human").alias("human_edits"),
                    F.sum("edits_bot").alias("bot_edits"),
                    F.sum("gross_bytes").alias("gross_bytes"),
                    F.sum("reverts_count").alias("reverts"),
                )
                .orderBy(F.desc("total_edits"))
            )
            macro_regions = records(macro_agg, 12)

            article_agg = (
                filtered.groupBy(
                    "pageid", "title", "country_or_region", "macro_region",
                    "location_source", "location_quality", "wikibase_item"
                )
                .agg(
                    F.first("lat").alias("base_lat"),
                    F.first("lon").alias("base_lon"),
                    F.sum("edits_total").alias("edits_total"),
                    F.sum("edits_human").alias("edits_human"),
                    F.sum("edits_bot").alias("edits_bot"),
                    F.sum("gross_bytes").alias("gross_bytes"),
                    F.sum("net_bytes").alias("net_bytes"),
                    F.sum("reverts_count").alias("reverts"),
                )
                .orderBy(F.desc("edits_total"), F.desc("gross_bytes"))
            )
            article_rows = records(article_agg, 150)
            for idx, r in enumerate(article_rows):
                b_lat = float(r.get("base_lat") or 0.0)
                b_lon = float(r.get("base_lon") or 0.0)
                if r.get("location_quality") == "country_centroid":
                    pid = int(r.get("pageid") or idx)
                    d_lat = ((pid % 19) - 9) * 0.14
                    d_lon = (((pid // 19) % 23) - 11) * 0.18
                    r["lat"] = round(max(-85.0, min(85.0, b_lat + d_lat)), 4)
                    r["lon"] = round(max(-179.0, min(179.0, b_lon + d_lon)), 4)
                else:
                    r["lat"] = round(b_lat, 4)
                    r["lon"] = round(b_lon, 4)
                r["coordinates"] = f"{r['lat']:.2f}°, {r['lon']:.2f}°"

            timeline_agg = (
                filtered.groupBy("bucket")
                .agg(
                    F.sum("edits_total").alias("edits"),
                    F.sum("edits_human").alias("human"),
                    F.sum("edits_bot").alias("bots"),
                    F.sum("gross_bytes").alias("gross_bytes"),
                    F.sum("reverts_count").alias("reverts"),
                    F.countDistinct("pageid").alias("active_pages"),
                )
                .orderBy("bucket")
            )
            timeline_rows = records(timeline_agg, 400)

            available_regions = ["ALL"] + sorted(set(COUNTRY_TO_MACRO.values())) + [
                row[0] for row in RECENTCHANGES_GEO_GAZETTEER[:25]
            ]

            return {
                "metrics": metrics,
                "country_rollup": country_rollup,
                "macro_regions": macro_regions,
                "map_articles": article_rows,
                "timeline": timeline_rows,
                "available_regions": available_regions,
                "mode": effective_mode,
                "requested_mode": mode_norm,
                "time_range": range_key,
                "region": selected_region,
                "range_label": range_label,
            }
        finally:
            filtered.unpersist()

    def article_titles(self, duration: str, view: str) -> list[str]:
        """Provide bounded article choices without computing per-page features again."""
        source = None
        if view == "live":
            if has_table(self.paths.raw):
                seconds = _window_seconds(duration)
                source = self.spark.read.option("pathGlobFilter", "*.parquet").parquet(str(self.paths.raw)).filter(
                    (F.col("ingestion_date") >= F.date_sub(F.current_date(), 1))
                    & (F.col("wiki") == "enwiki") & (F.col("namespace") == 0)
                    & (F.col("change_type") == "edit")
                    & (F.to_timestamp("event_time") >= F.expr(f"current_timestamp() - INTERVAL {seconds} SECONDS"))
                    & (F.to_timestamp("event_time") <= F.current_timestamp())
                ).select(F.col("page_title").alias("title"))
        else:
            source_duration = _window_table_duration(duration)
            path = self.paths.live / f"windows_{source_duration}"
            if has_table(path):
                source = self.spark.read.option("pathGlobFilter", "*.parquet").parquet(str(path)).select("title")
        if source is None:
            if view == "live":
                source_duration = _window_table_duration(duration)
                live_path = self.paths.live / f"windows_{source_duration}"
                if not has_table(live_path):
                    return []
                source = self.spark.read.option("pathGlobFilter", "*.parquet").parquet(str(live_path)).select("title")
            else:
                source_duration = _window_table_duration(duration)
                fallback = self.table("alerts") or self.table(f"page_windows_{source_duration}")
                if fallback is None:
                    return []
                source = fallback.select("title")
        titles = source.filter(F.col("title").isNotNull()).distinct().orderBy("title")
        return [row["title"] for row in records(titles, 500)]

    def quality(self) -> dict:
        """Revalidate original payloads so old rejected flags do not distort counts."""
        if not has_table(self.paths.raw):
            return {}
        raw = self.spark.read.parquet(str(self.paths.raw)).filter(
            F.col("ingestion_date").isNull()
            | (F.col("ingestion_date") >= F.date_sub(F.current_date(), 1))
        )
        parsed = reparse_stored_events(raw)
        return records(parsed.agg(
            F.count("*").alias("raw_records"),
            F.sum(F.col("is_valid").cast("long")).alias("valid_records"),
            F.sum((~F.col("is_valid")).cast("long")).alias("invalid_records"),
            F.countDistinct("event_id").alias("event_ids"),
            F.date_format(F.max("ingested_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("last_ingested"),
        ), 1)[0]
