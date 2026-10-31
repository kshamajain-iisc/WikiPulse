"""Shared Spark transformations for historical and live page activity.

Both pipelines normalize edits with the existing model helpers before using the
same feature calculations. No dataframe processing is performed in Python.
"""

from datetime import date

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.models.schemas import normalize_recentchanges_df
from src.spark_common import WIKIMEDIA_SCHEMA, reparse_stored_events
from src.page_identity import title_page_id

# Explicit fields keep optional columns available even when every input row
# omits them. Inference would otherwise make the normalizer fail on sparse files.
HISTORY_SCHEMA = StructType([
    StructField("rcid", LongType()),
    StructField("revid", LongType()),
    StructField("old_revid", LongType()),
    StructField("pageid", LongType()),
    StructField("ns", IntegerType()),
    StructField("title", StringType()),
    StructField("type", StringType()),
    StructField("timestamp", StringType()),
    StructField("user", StringType()),
    StructField("userid", LongType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("new", BooleanType()),
    StructField("temp", BooleanType()),
    StructField("oldlen", LongType()),
    StructField("newlen", LongType()),
    StructField("tags", ArrayType(StringType())),
    StructField("comment", StringType()),
    StructField("sha1", StringType()),
])

BASELINE_METRICS = (
    "edits_total",
    "edits_human",
    "gross_bytes",
    "distinct_human_editors",
)


def historical_edits(raw: DataFrame) -> DataFrame:
    """Keep valid, unique mainspace edits matching the live collector's scope."""
    normalized = normalize_recentchanges_df(raw)
    valid_edit = (
        (F.col("ns") == 0)
        & (F.col("type") == "edit")
        & (F.col("pageid") > 0)
        & F.col("rcid").isNotNull()
        & F.col("ts").isNotNull()
        & (F.length("title") > 0)
    )
    return normalized.filter(valid_edit).dropDuplicates(["rcid"])


def live_edits(parsed: DataFrame) -> DataFrame:
    """Adapt shared Kafka parsing to the existing canonical edit normalizer."""
    # Bronze retains raw JSON, so corrected parser rules can recover old events
    # without mutating stored files or trusting an outdated is_valid flag.
    parsed = reparse_stored_events(parsed)
    events = parsed.filter("is_valid").withColumn(
        "event", F.from_json("raw_event_json", WIKIMEDIA_SCHEMA)
    )

    # Rename EventStreams fields to the historical API names expected by the
    # normalizer. This avoids implementing bot, anonymity, and revert logic twice.
    raw = events.select(
        F.col("change_id").alias("rcid"),
        F.col("new_revision_id").alias("revid"),
        F.col("old_revision_id").alias("old_revid"),
        F.col("page_id").alias("pageid"),
        F.col("namespace").alias("ns"),
        F.col("page_title").alias("title"),
        F.col("change_type").alias("type"),
        F.col("event_time").alias("timestamp"),
        F.col("old_length").alias("oldlen"),
        F.col("new_length").alias("newlen"),
        F.col("is_bot").alias("bot"),
        F.col("is_minor").alias("minor"),
        F.col("event.user").alias("user"),
        F.col("event.userid").alias("userid"),
        F.col("event.tags").alias("tags"),
        F.col("event.comment").alias("comment"),
        F.lit(False).alias("new"),
    )
    canonical = normalize_recentchanges_df(raw).withColumn(
        "source_feed", F.lit("eventstream")
    )
    # Reconstruct the validated stable ID after normalization for streaming dedup.
    return canonical.withColumn(
        "event_id", F.concat(F.lit("enwiki:"), F.col("rcid"))
    )


def page_windows(edits: DataFrame, duration: str = "5 minutes") -> DataFrame:
    """Calculate tumbling UTC windows with explicit coverage for missing sizes."""
    is_human = ~F.col("is_bot")
    human_editor = F.when(is_human, F.col("user"))
    grouped = edits.groupBy(F.window("ts", duration), "pageid", "ns").agg(
        # Use page ID as identity; a title change must not split one window.
        F.max_by("title", F.struct("ts", "rcid")).alias("title"),
        F.count("*").alias("edits_total"),
        F.sum(is_human.cast("long")).alias("edits_human"),
        F.sum(F.col("is_bot").cast("long")).alias("edits_bot"),
        F.sum(F.col("is_anon").cast("long")).alias("edits_anon"),
        # Approximate counting bounds per-window state for popular pages.
        F.approx_count_distinct(human_editor, 0.05).alias("distinct_human_editors"),
        F.sum(F.col("is_minor").cast("long")).alias("minor_edits"),
        F.count("delta").alias("edits_with_sizes"),
        F.sum("abs_delta").alias("gross_bytes"),
        F.sum("delta").alias("net_bytes"),
        F.sum(F.col("is_revert_action").cast("long")).alias("reverts_count"),
    )
    window_key = F.concat_ws("::", "pageid", "window_start", "window_duration")
    return (
        grouped.withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("window_duration", F.lit(duration))
        .withColumn("day", F.to_date("window_start"))
        .withColumn("window_id", F.sha2(window_key, 256))
        .withColumn("rewriting_bytes", F.col("gross_bytes") - F.abs("net_bytes"))
        .withColumn("bot_ratio", F.col("edits_bot") / F.col("edits_total"))
        .withColumn("revert_ratio", F.col("reverts_count") / F.col("edits_total"))
        .withColumn("size_coverage", F.col("edits_with_sizes") / F.col("edits_total"))
        .drop("window")
    )


def build_baselines(
    windows: DataFrame,
    start: str,
    end: str,
    duration_seconds: int,
) -> DataFrame:
    """Compute population moments including quiet windows without a time grid.

    The caller must supply windows from complete UTC days in [start, end).
    Otherwise missing collection time would be mistaken for zero activity.
    """
    baseline_days = (date.fromisoformat(end) - date.fromisoformat(start)).days
    window_count = baseline_days * 86400 // duration_seconds
    if window_count <= 0:
        raise ValueError("Baseline end must follow start")

    # Gather the first and second moments for each feature. Quiet windows add
    # zero to both sums, so a full page-by-time cross join is unnecessary.
    moment_expressions = []
    for metric in BASELINE_METRICS:
        moment_expressions.append(F.sum(metric).alias(f"sum_{metric}"))
        moment_expressions.append(
            F.sum(F.pow(F.col(metric), 2)).alias(f"sumsq_{metric}")
        )

    baselines = windows.groupBy("pageid", "ns", "window_duration").agg(
        F.max_by("title", "window_end").alias("baseline_title"),
        F.count("*").alias("active_windows"),
        F.sum("edits_total").alias("total_baseline_edits"),
        F.sum("edits_with_sizes").alias("baseline_edits_with_sizes"),
        *moment_expressions,
    )
    for metric in BASELINE_METRICS:
        mean = F.col(f"sum_{metric}") / window_count
        second_moment = F.col(f"sumsq_{metric}") / window_count
        # Floating-point rounding can make an otherwise zero variance negative.
        variance = F.greatest(F.lit(0.0), second_moment - F.pow(mean, 2))
        baselines = (
            baselines.withColumn(f"avg_{metric}", mean)
            .withColumn(f"std_{metric}", F.sqrt(variance))
            .drop(f"sum_{metric}", f"sumsq_{metric}")
        )

    complete_sizes = (
        F.col("baseline_edits_with_sizes") == F.col("total_baseline_edits")
    )
    return (
        baselines.withColumn("byte_baseline_complete", complete_sizes)
        .withColumn("baseline_start", F.lit(start).cast("date"))
        .withColumn("baseline_end_exclusive", F.lit(end).cast("date"))
        .withColumn("baseline_windows", F.lit(window_count))
    )


def compare_history(windows: DataFrame, baselines: DataFrame) -> DataFrame:
    """Attach historical comparisons only after the baseline training period."""
    # Index each historical page by both its real ID and its internal title key.
    # Title fallback enables comparison when the stream omits numeric IDs.
    lookup = baselines.withColumn(
        "lookup_pageid",
        F.explode(F.array(F.col("pageid"), title_page_id(F.col("baseline_title")))),
    ).drop("pageid").withColumnRenamed("lookup_pageid", "pageid")
    lookup_keys = ["pageid", "ns", "window_duration"]
    key_partition = Window.partitionBy(*lookup_keys)
    lookup = (
        lookup.withColumn("matches", F.count("*").over(key_partition))
        .filter(F.col("matches") == 1)
        .drop("matches")
    )
    # Ambiguous titles are excluded instead of multiplying dashboard rows.
    joined = windows.join(F.broadcast(lookup), lookup_keys, "left")
    available = (
        F.col("baseline_end_exclusive").isNotNull()
        & (F.col("day") >= F.col("baseline_end_exclusive"))
    )

    # Floors keep quiet-page ratios and constant-activity z-scores finite.
    human_edit_ratio = F.col("edits_human") / F.greatest(
        F.col("avg_edits_human"), F.lit(1.0)
    )
    velocity_zscore = (
        F.col("edits_human") - F.col("avg_edits_human")
    ) / F.greatest(F.col("std_edits_human"), F.lit(1.0))
    return (
        joined.withColumn("has_baseline", available)
        .withColumn("human_edit_ratio", F.when(available, human_edit_ratio))
        .withColumn("velocity_zscore", F.when(available, velocity_zscore))
    )
