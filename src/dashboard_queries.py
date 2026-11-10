"""Spark query boundary for dashboards; return only bounded, JSON-safe results.

All dataframe filtering, deduplication, grouping, ranking and joins stay in Spark.
Presentation code receives aggregates and capped records, never raw dataframes.
"""

import json
from dataclasses import dataclass
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F

from src.page_features import compare_history, live_edits, page_windows
from src.spark_common import RAW_PATH, ROOT, reparse_stored_events


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

    def table(self, name: str) -> DataFrame | None:
        """Resolve one optional curated table from the configured roots."""
        for root in (self.paths.history, self.paths.curated):
            path = root / name
            if has_table(path):
                return self.spark.read.option("pathGlobFilter", "*.parquet").parquet(str(path))
        return None

    def optional_records(self, name: str, title: str | None = None) -> list[dict]:
        frame = self.table(name)
        if frame is None:
            return []
        if title is not None and "title" in frame.columns:
            frame = frame.filter(F.col("title") == title)
        if "window_start" in frame.columns:
            frame = frame.orderBy(F.desc("window_start"))
        return records(frame, 200)

    def alerts(self, duration: str, title: str | None = None) -> list[dict]:
        """Read finalized alerts for the chosen duration, independently of previews."""
        frame = self.table("alerts")
        if frame is None:
            return []
        expected_durations = (duration, "5 minutes" if duration == "5m" else "1 hour")
        frame = frame.filter(F.col("window_duration").isin(*expected_durations))
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

    def recent_edits(self, duration: str) -> tuple[DataFrame | None, str | None]:
        """Restrict preview reads by ingestion date and latest observed event time."""
        if not has_table(self.paths.raw):
            return None, None
        bronze = self.spark.read.option("pathGlobFilter", "*.parquet").parquet(str(self.paths.raw)).filter(
            F.col("ingestion_date") >= F.date_sub(F.current_date(), 1)
        )
        # Event-time metadata is already stored as a typed Parquet column. Find
        # the horizon before parsing large JSON payloads, then parse only that
        # horizon. This keeps live queries small as retained history grows.
        candidates = bronze.filter(
            (F.col("wiki") == "enwiki") & (F.col("change_type") == "edit")
            & (F.col("namespace") == 0)
        )
        latest = candidates.agg(
            F.max("event_time").cast("long").alias("epoch"),
            F.date_format(F.max("event_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("utc"),
        ).first()
        if latest.epoch is None:
            return None, None
        minutes = 30 if duration == "5m" else 120
        candidates = candidates.filter(F.col("event_time").cast("long") >= latest.epoch - minutes * 60)
        edits = live_edits(candidates).dropDuplicates(["event_id"])
        return edits, latest.utc

    def activity(self, duration: str, view: str, title: str | None = None) -> dict:
        """Compute full-scope KPIs before limiting the displayed article ranking."""
        latest = None
        windows = None
        if view == "live":
            edits, latest = self.recent_edits(duration)
            if edits is not None:
                if title:
                    edits = edits.filter(F.col("title") == title)
                windows = page_windows(edits, "5 minutes" if duration == "5m" else "1 hour")
            else:
                windows = self.table(f"page_windows_{duration}")
                if windows is None:
                    return {"metrics": {}, "timeline": [], "rankings": [], "latest": None}
                windows = windows.filter(F.col("ns") == 0)
                latest = windows.agg(
                    F.date_format(F.max("window_end"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("utc")
                ).first().utc
                if title:
                    windows = windows.filter(F.col("title") == title)
                else:
                    latest_epoch = windows.agg(F.max("window_end").cast("long")).first()[0]
                    if latest_epoch is not None:
                        windows = windows.filter(F.col("window_end").cast("long") >= latest_epoch - 7200)
        else:
            path = self.paths.live / f"windows_{duration}"
            if has_table(path):
                windows = self.spark.read.option("pathGlobFilter", "*.parquet").parquet(str(path))
            else:
                windows = self.table(f"page_windows_{duration}")
                if windows is not None:
                    windows = windows.filter(F.col("ns") == 0)
            if windows is None:
                return {"metrics": {}, "timeline": [], "rankings": [], "latest": None}
            latest = windows.agg(
                F.date_format(F.max("window_end"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("utc")
            ).first().utc
            if title:
                windows = windows.filter(F.col("title") == title)
            else:
                # Keep a bounded, recent event-time horizon for chart interpretation.
                latest_epoch = windows.agg(F.max("window_end").cast("long")).first()[0]
                if latest_epoch is not None:
                    windows = windows.filter(F.col("window_end").cast("long") >= latest_epoch - 7200)

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
            baseline = self.table(f"baselines_{duration}")
            comparisons = []
            if baseline is not None:
                compared = compare_history(windows, baseline).filter("has_baseline")
                comparisons = records(compared.orderBy(F.desc("window_start")), 100)
            else:
                page_base = self.table("page_baselines")
                if page_base is not None:
                    div = 12.0 if duration == "5m" else 1.0
                    compared = windows.join(
                        page_base.select(
                            "pageid",
                            F.round(F.col("avg_hourly_edits") / F.lit(div), 3).alias("avg_edits_human"),
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
                "scope": "Last 30 event-time minutes" if view == "live" and duration == "5m" else "Last 2 event-time hours",
            }
        finally:
            windows.unpersist()

    def historical(self, start: str | None, end: str | None) -> dict:
        """Aggregate available canonical history, preserving missing source dates."""
        churn_bot = self.table("churn_bot_summary")
        rankings_daily = self.table("rankings_daily")
        if churn_bot is not None and rankings_daily is not None:
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
            ranking = rk.groupBy("title").agg(
                F.sum("edits_total").alias("edits"),
                F.sum("gross_churn_bytes").alias("gross_bytes"),
            ).orderBy(F.desc("edits"), "title")
            return {
                "metrics": metrics,
                "timeline": records(timeline, 366),
                "rankings": records(ranking, 20),
                "baseline": self.optional_records("page_baselines") or self.optional_records("baselines_5m"),
            }

        edits = self.table("edits")
        if edits is None:
            edits = self.table("rc_events")
        if edits is None:
            return {"metrics": {}, "timeline": [], "rankings": [], "baseline": []}
        edits = edits.filter((F.col("ns") == 0) & (F.col("type") == "edit"))
        if start:
            edits = edits.filter(F.col("day") >= start)
        if end:
            edits = edits.filter(F.col("day") <= end)
        edits.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            totals = edits.agg(
                F.count("*").alias("edits"),
                F.countDistinct("pageid").alias("pages"),
                F.sum(F.col("is_bot").cast("long")).alias("bots"),
                F.sum("abs_delta").alias("gross_bytes"),
                F.sum("delta").alias("net_bytes"),
                F.min("day").cast("string").alias("first_day"),
                F.max("day").cast("string").alias("last_day"),
                F.countDistinct("day").alias("observed_days"),
            )
            timeline = edits.groupBy("day").agg(
                F.count("*").alias("edits"),
                F.sum(F.col("is_bot").cast("long")).alias("bots"),
                F.sum("abs_delta").alias("gross_bytes"),
                F.sum("delta").alias("net_bytes"),
            ).orderBy("day")
            ranking = edits.groupBy("title").agg(
                F.count("*").alias("edits"), F.sum("abs_delta").alias("gross_bytes")
            ).orderBy(F.desc("edits"), "title")
            return {
                "metrics": records(totals, 1)[0],
                "timeline": records(timeline, 366),
                "rankings": records(ranking, 20),
                "baseline": self.optional_records("page_baselines") or self.optional_records("baselines_5m"),
            }
        finally:
            edits.unpersist()

    def article_titles(self, duration: str, view: str) -> list[str]:
        """Provide bounded article choices without computing per-page features again."""
        source = None
        if view == "live":
            if has_table(self.paths.raw):
                source = self.spark.read.option("pathGlobFilter", "*.parquet").parquet(str(self.paths.raw)).filter(
                    (F.col("ingestion_date") >= F.date_sub(F.current_date(), 1))
                    & (F.col("wiki") == "enwiki") & (F.col("namespace") == 0)
                ).select(F.col("page_title").alias("title"))
        else:
            path = self.paths.live / f"windows_{duration}"
            if has_table(path):
                source = self.spark.read.option("pathGlobFilter", "*.parquet").parquet(str(path)).select("title")
        if source is None:
            fallback = self.table("alerts") or self.table(f"page_windows_{duration}")
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
