"""Spark query boundary for dashboards; return only bounded, JSON-safe results.

All dataframe filtering, deduplication, grouping, ranking and joins stay in Spark.
Presentation code receives aggregates and capped records, never raw dataframes.
"""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F

from src.page_features import compare_history, live_edits, page_windows
from src.spark_common import RAW_PATH, ROOT, reparse_stored_events


WINDOW_SECONDS = {"5m": 5 * 60, "1h": 60 * 60, "6h": 6 * 60 * 60}
WINDOW_LABELS = {"5m": "5 minutes", "1h": "1 hour", "6h": "6 hours"}
LIVE_BUCKETS = {"5m": "5 minutes", "1h": "5 minutes", "6h": "1 hour"}
LIVE_WINDOW_TABLES = {"5m": "5m", "1h": "1h", "6h": "1h"}


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
    curated: Path = ROOT / "data/curated"
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
        churn_bot = self.table("churn_bot_summary")
        rankings_daily = self.table("rankings_daily")
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
        return {
            "metrics": metrics,
            "timeline": records(timeline, 366),
            "rankings": records(ranking, 50),
            "baseline": self.optional_records("page_baselines") or self.optional_records("baselines_5m"),
        }

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
