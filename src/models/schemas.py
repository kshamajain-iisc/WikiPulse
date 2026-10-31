"""Milestone 1: Shared Data Models & PySpark Schema Contracts for WikiPulse.

Covers the 5 core Proposal Data Models plus 3 contextual enrichment tables:
1. Raw edit            -> RAW_EDIT_SCHEMA
2. Page windows        -> PAGE_WINDOW_SCHEMA
3. Historical baseline -> HISTORICAL_BASELINE_SCHEMA
4. Alerts              -> ALERT_SCHEMA
5. Graph edge          -> GRAPH_EDGE_SCHEMA
6. Map point           -> MAP_POINT_SCHEMA
7. Revert risk         -> REVERT_RISK_SCHEMA
8. Revision diff       -> REVISION_DIFF_SCHEMA
"""

import hashlib
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# 1. RAW EDIT SCHEMA (Unifies RecentChanges, Revisions, and Live EventStreams)
# ---------------------------------------------------------------------------
RAW_EDIT_SCHEMA = StructType(
    [
        StructField("rcid", LongType(), nullable=True),
        StructField("revid", LongType(), nullable=True),
        StructField("old_revid", LongType(), nullable=True),
        StructField("pageid", LongType(), nullable=False),
        StructField("ns", IntegerType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("type", StringType(), nullable=False),          # edit | new | log
        StructField("ts", TimestampType(), nullable=False),         # Event UTC timestamp
        StructField("day", DateType(), nullable=False),             # Partition key YYYY-MM-DD
        StructField("hour", TimestampType(), nullable=False),       # Truncated hour UTC
        StructField("user", StringType(), nullable=True),
        StructField("userid", LongType(), nullable=True),
        StructField("is_bot", BooleanType(), nullable=False),
        StructField("is_anon", BooleanType(), nullable=False),      # Temp account (~2026-...) or userid=0
        StructField("is_minor", BooleanType(), nullable=False),
        StructField("is_new_page", BooleanType(), nullable=False),
        StructField("oldlen", LongType(), nullable=True),
        StructField("newlen", LongType(), nullable=True),
        StructField("delta", LongType(), nullable=True),            # newlen - oldlen
        StructField("abs_delta", LongType(), nullable=True),        # |newlen - oldlen|
        StructField("is_reverted", BooleanType(), nullable=False),  # Tagged mw-reverted
        StructField("is_revert_action", BooleanType(), nullable=False),  # mw-rollback | mw-undo | mw-manual-revert
        StructField("is_revert", BooleanType(), nullable=False),    # Any revert tag present
        StructField("sha1", StringType(), nullable=True),
        StructField("comment", StringType(), nullable=True),
        StructField("tags", ArrayType(StringType()), nullable=False),
        StructField("logtype", StringType(), nullable=True),
        StructField("logaction", StringType(), nullable=True),
        StructField("source_feed", StringType(), nullable=False),   # recentchanges | revisions | eventstream
    ]
)

# ---------------------------------------------------------------------------
# 2. PAGE WINDOWS SCHEMA (5-minute & 1-hour windowed activity + baseline z-scores)
# ---------------------------------------------------------------------------
PAGE_WINDOW_SCHEMA = StructType(
    [
        StructField("window_id", StringType(), nullable=False),
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("window_duration", StringType(), nullable=False),  # 5m | 1h
        StructField("day", DateType(), nullable=False),
        StructField("pageid", LongType(), nullable=False),
        StructField("ns", IntegerType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("edits_total", LongType(), nullable=False),
        StructField("edits_human", LongType(), nullable=False),
        StructField("edits_bot", LongType(), nullable=False),
        StructField("edits_anon", LongType(), nullable=False),
        StructField("distinct_human_editors", LongType(), nullable=False),
        StructField("gross_bytes", LongType(), nullable=False),        # sum(|delta|)
        StructField("net_bytes", LongType(), nullable=False),          # sum(delta)
        StructField("rewriting_bytes", LongType(), nullable=False),    # gross_bytes - |net_bytes|
        StructField("reverts_count", LongType(), nullable=False),
        StructField("revert_ratio", DoubleType(), nullable=False),
        StructField("new_page_created", BooleanType(), nullable=False),
        # Baseline-normalized features (joined from HISTORICAL_BASELINE_SCHEMA)
        StructField("velocity_zscore", DoubleType(), nullable=False),
        StructField("editor_surge_ratio", DoubleType(), nullable=False),
        StructField("churn_zscore", DoubleType(), nullable=False),
        StructField("consensus_factor", DoubleType(), nullable=False),  # 1.0 - revert_ratio
    ]
)

# ---------------------------------------------------------------------------
# 3. HISTORICAL BASELINE SCHEMA (Per-page baseline moments & Analytics series)
# ---------------------------------------------------------------------------
HISTORICAL_BASELINE_SCHEMA = StructType(
    [
        StructField("pageid", LongType(), nullable=False),
        StructField("ns", IntegerType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("baseline_start_date", DateType(), nullable=False),
        StructField("baseline_end_date", DateType(), nullable=False),
        StructField("days_observed", IntegerType(), nullable=False),
        StructField("last_available_date", DateType(), nullable=False),
        StructField("has_missing_data", BooleanType(), nullable=False),
        StructField("activity_tier", StringType(), nullable=False),    # quiet | moderate | active | hyperactive
        StructField("total_baseline_edits", LongType(), nullable=False),
        StructField("avg_daily_edits", DoubleType(), nullable=False),
        StructField("std_daily_edits", DoubleType(), nullable=False),
        StructField("avg_hourly_edits", DoubleType(), nullable=False),
        StructField("std_hourly_edits", DoubleType(), nullable=False),
        StructField("p95_hourly_edits", DoubleType(), nullable=False),
        StructField("avg_distinct_editors_daily", DoubleType(), nullable=False),
        StructField("avg_gross_bytes_daily", DoubleType(), nullable=False),
        StructField("std_gross_bytes_daily", DoubleType(), nullable=False),
        StructField("avg_net_bytes_daily", DoubleType(), nullable=False),
        StructField("baseline_revert_ratio", DoubleType(), nullable=False),
        StructField("baseline_bot_ratio", DoubleType(), nullable=False),
        # Wikimedia Analytics API pageview enrichment
        StructField("avg_daily_pageviews", DoubleType(), nullable=True),
        StructField("std_daily_pageviews", DoubleType(), nullable=True),
        StructField("last_pageview_date", DateType(), nullable=True),
        StructField("pageview_missing_flag", BooleanType(), nullable=False),
    ]
)

# ---------------------------------------------------------------------------
# 4. ALERTS SCHEMA (Explainable multi-detector anomaly alerts)
# ---------------------------------------------------------------------------
ALERT_SCHEMA = StructType(
    [
        StructField("alert_id", StringType(), nullable=False),         # Deterministic SHA-256 prefix
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("window_duration", StringType(), nullable=False),  # 5m | 1h
        StructField("day", DateType(), nullable=False),
        StructField("pageid", LongType(), nullable=False),
        StructField("ns", IntegerType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("alert_type", StringType(), nullable=False),       # BREAKING_NEWS_CANDIDATE | CONFIRMED_NEWS | EDIT_WAR_CONTROVERSY | MAJOR_REWRITE
        StructField("fixed_threshold_flag", BooleanType(), nullable=False),
        StructField("historical_score", DoubleType(), nullable=False), # 0.0 to 100.0
        StructField("historical_alert_flag", BooleanType(), nullable=False),
        StructField("spark_ml_score", DoubleType(), nullable=False),
        StructField("spark_ml_alert_flag", BooleanType(), nullable=False),
        StructField("edits_human", LongType(), nullable=False),
        StructField("distinct_human_editors", LongType(), nullable=False),
        StructField("gross_bytes", LongType(), nullable=False),
        StructField("net_bytes", LongType(), nullable=False),
        StructField("revert_ratio", DoubleType(), nullable=False),
        StructField("velocity_zscore", DoubleType(), nullable=False),
        StructField("pageview_ratio_vs_baseline", DoubleType(), nullable=True),
        StructField("pageview_corroborated", BooleanType(), nullable=False),
        StructField("reasons", ArrayType(StringType()), nullable=False),  # >= 2 explainable reasons required
        StructField("feature_contributions", MapType(StringType(), DoubleType()), nullable=False),
        StructField("has_location", BooleanType(), nullable=False),
        StructField("is_injected_synthetic", BooleanType(), nullable=False),
    ]
)

# ---------------------------------------------------------------------------
# 5. GRAPH EDGE SCHEMA (Outgoing links, incoming backlinks & window spillover)
# ---------------------------------------------------------------------------
GRAPH_EDGE_SCHEMA = StructType(
    [
        StructField("edge_id", StringType(), nullable=False),
        StructField("source_pageid", LongType(), nullable=False),
        StructField("source_title", StringType(), nullable=False),
        StructField("target_pageid", LongType(), nullable=True),
        StructField("target_title", StringType(), nullable=False),
        StructField("edge_type", StringType(), nullable=False),        # outlink | backlink
        StructField("window_start", TimestampType(), nullable=True),
        StructField("co_edited_in_window", BooleanType(), nullable=False),
        StructField("target_edits_in_window", LongType(), nullable=False),
        StructField("target_distinct_editors", LongType(), nullable=False),
        StructField("snapshot_ts", TimestampType(), nullable=False),
        StructField("is_truncated_cap", BooleanType(), nullable=False),
    ]
)

# ---------------------------------------------------------------------------
# 6. MAP POINT SCHEMA (Subject location with source & quality; no editor IPs)
# ---------------------------------------------------------------------------
MAP_POINT_SCHEMA = StructType(
    [
        StructField("pageid", LongType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("alert_id", StringType(), nullable=True),
        StructField("wikibase_item", StringType(), nullable=True),     # e.g., Q1355
        StructField("has_coordinates", BooleanType(), nullable=False),
        StructField("lat", DoubleType(), nullable=True),
        StructField("lon", DoubleType(), nullable=True),
        StructField("country_or_region", StringType(), nullable=True),
        StructField("location_source", StringType(), nullable=False),  # wikipedia_coord | wikidata_p625 | wikidata_p276_p17 | none
        StructField("location_quality", StringType(), nullable=False), # exact_point | related_entity_point | country_centroid | unlocated
        StructField("edits_human", LongType(), nullable=False),
        StructField("historical_score", DoubleType(), nullable=True),
        StructField("alert_type", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# 7. REVERT RISK SCHEMA (Lift Wing ML + Heuristic + Delayed Revert Ground Truth)
# ---------------------------------------------------------------------------
REVERT_RISK_SCHEMA = StructType(
    [
        StructField("revid", LongType(), nullable=False),
        StructField("parentid", LongType(), nullable=True),
        StructField("pageid", LongType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("ts", TimestampType(), nullable=False),
        StructField("day", DateType(), nullable=False),
        StructField("user", StringType(), nullable=True),
        StructField("is_anon", BooleanType(), nullable=False),
        StructField("is_bot", BooleanType(), nullable=False),
        StructField("delta", LongType(), nullable=True),
        StructField("ml_revert_risk", DoubleType(), nullable=True),     # Lift Wing probability [0.0, 1.0]
        StructField("heuristic_risk", DoubleType(), nullable=False),    # Simple checks score [0.0, 1.0]
        StructField("composite_risk", DoubleType(), nullable=False),    # Combined score (fallback if API down)
        StructField("api_status", StringType(), nullable=False),        # success | cached | fallback_offline | unsupported
        StructField("heuristic_reasons", ArrayType(StringType()), nullable=False),
        # Ground-truth delayed feedback outcome
        StructField("is_reverted_actual", BooleanType(), nullable=False),
        StructField("reverter_revid", LongType(), nullable=True),
        StructField("reverter_user", StringType(), nullable=True),
        StructField("reverter_is_bot", BooleanType(), nullable=True),
        StructField("time_to_revert_sec", LongType(), nullable=True),
        StructField("observation_window_hours", IntegerType(), nullable=False),
    ]
)

# ---------------------------------------------------------------------------
# 8. REVISION DIFF SCHEMA (MediaWiki action=compare change data)
# ---------------------------------------------------------------------------
REVISION_DIFF_SCHEMA = StructType(
    [
        StructField("revid", LongType(), nullable=False),
        StructField("from_revid", LongType(), nullable=False),
        StructField("pageid", LongType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("ts", TimestampType(), nullable=True),
        StructField("user", StringType(), nullable=True),
        StructField("comment", StringType(), nullable=True),
        StructField("from_size", LongType(), nullable=True),
        StructField("to_size", LongType(), nullable=True),
        StructField("delta_bytes", LongType(), nullable=True),
        StructField("added_lines_count", IntegerType(), nullable=False),
        StructField("removed_lines_count", IntegerType(), nullable=False),
        StructField("external_links_added", IntegerType(), nullable=False),
        StructField("diff_snippet", StringType(), nullable=True),
        StructField("usable_change_data", BooleanType(), nullable=False),
        StructField("compare_status", StringType(), nullable=False),    # success | empty_diff | missing_rev | api_error
    ]
)

ALL_SCHEMAS = {
    "raw_edit": RAW_EDIT_SCHEMA,
    "page_window": PAGE_WINDOW_SCHEMA,
    "historical_baseline": HISTORICAL_BASELINE_SCHEMA,
    "alert": ALERT_SCHEMA,
    "graph_edge": GRAPH_EDGE_SCHEMA,
    "map_point": MAP_POINT_SCHEMA,
    "revert_risk": REVERT_RISK_SCHEMA,
    "revision_diff": REVISION_DIFF_SCHEMA,
}


# ---------------------------------------------------------------------------
# 9. Deterministic ID & Normalization Helpers
# ---------------------------------------------------------------------------
def compute_deterministic_alert_id(pageid: int, window_start_iso: str, window_duration: str) -> str:
    """Create a deterministic, replay-idempotent alert primary key."""
    key = f"{pageid}::{window_start_iso}::{window_duration}"
    return "alt_" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def normalize_recentchanges_df(raw_df: DataFrame, filter_categorize: bool = True) -> DataFrame:
    """Transform a raw `rc_*.jsonl.gz` Spark DataFrame into the canonical RAW_EDIT_SCHEMA."""
    df = raw_df
    if filter_categorize:
        df = df.filter(F.col("type").isin("edit", "new", "log"))

    cols = set(df.columns)
    has_temp = "temp" in cols
    has_logtype = "logtype" in cols
    has_logaction = "logaction" in cols
    has_sha1 = "sha1" in cols

    ts_col = F.to_timestamp(F.col("timestamp"))
    tags_col = F.coalesce(F.col("tags"), F.array().cast(ArrayType(StringType())))

    is_reverted_col = F.coalesce(F.array_contains(tags_col, "mw-reverted"), F.lit(False))
    is_revert_action_col = F.coalesce(
        F.array_contains(tags_col, "mw-rollback")
        | F.array_contains(tags_col, "mw-undo")
        | F.array_contains(tags_col, "mw-manual-revert"),
        F.lit(False),
    )

    temp_expr = F.coalesce(F.col("temp").cast("boolean"), F.lit(False)) if has_temp else F.lit(False)
    is_anon_col = F.coalesce(
        temp_expr
        | F.col("user").startswith("~")
        | F.col("userid").isNull()
        | (F.col("userid") == 0),
        F.lit(False),
    )

    delta_col = (F.col("newlen").cast("long") - F.col("oldlen").cast("long"))

    normalized = df.select(
        F.col("rcid").cast("long").alias("rcid"),
        F.col("revid").cast("long").alias("revid"),
        F.col("old_revid").cast("long").alias("old_revid"),
        F.coalesce(F.col("pageid").cast("long"), F.lit(0)).alias("pageid"),
        F.coalesce(F.col("ns").cast("int"), F.lit(0)).alias("ns"),
        F.coalesce(F.col("title").cast("string"), F.lit("")).alias("title"),
        F.coalesce(F.col("type").cast("string"), F.lit("edit")).alias("type"),
        ts_col.alias("ts"),
        F.to_date(ts_col).alias("day"),
        F.date_trunc("hour", ts_col).alias("hour"),
        F.col("user").cast("string").alias("user"),
        F.col("userid").cast("long").alias("userid"),
        F.coalesce(F.col("bot").cast("boolean"), F.lit(False)).alias("is_bot"),
        is_anon_col.alias("is_anon"),
        F.coalesce(F.col("minor").cast("boolean"), F.lit(False)).alias("is_minor"),
        F.coalesce((F.col("type") == "new") | F.col("new").cast("boolean"), F.lit(False)).alias("is_new_page"),
        F.col("oldlen").cast("long").alias("oldlen"),
        F.col("newlen").cast("long").alias("newlen"),
        delta_col.alias("delta"),
        F.abs(delta_col).alias("abs_delta"),
        is_reverted_col.alias("is_reverted"),
        is_revert_action_col.alias("is_revert_action"),
        (is_reverted_col | is_revert_action_col).alias("is_revert"),
        (F.col("sha1").cast("string") if has_sha1 else F.lit(None).cast("string")).alias("sha1"),
        F.col("comment").cast("string").alias("comment"),
        tags_col.alias("tags"),
        (F.col("logtype").cast("string") if has_logtype else F.lit(None).cast("string")).alias("logtype"),
        (F.col("logaction").cast("string") if has_logaction else F.lit(None).cast("string")).alias("logaction"),
        F.lit("recentchanges").alias("source_feed"),
    )
    return normalized


def normalize_revisions_df(raw_rev_df: DataFrame) -> DataFrame:
    """Transform a raw `revisions_*.jsonl.gz` Spark DataFrame into the canonical RAW_EDIT_SCHEMA."""
    ts_col = F.to_timestamp(F.col("ts"))
    tags_col = F.coalesce(F.col("tags"), F.array().cast(ArrayType(StringType())))

    is_reverted_col = F.coalesce(F.array_contains(tags_col, "mw-reverted"), F.lit(False))
    is_revert_action_col = F.coalesce(
        F.array_contains(tags_col, "mw-rollback")
        | F.array_contains(tags_col, "mw-undo")
        | F.array_contains(tags_col, "mw-manual-revert"),
        F.lit(False),
    )
    is_anon_col = F.coalesce(
        F.col("anon").cast("boolean")
        | F.col("user").startswith("~")
        | F.col("userid").isNull()
        | (F.col("userid") == 0),
        F.lit(False),
    )

    return raw_rev_df.select(
        F.lit(None).cast("long").alias("rcid"),
        F.col("revid").cast("long").alias("revid"),
        F.col("parentid").cast("long").alias("old_revid"),
        F.coalesce(F.col("pageid").cast("long"), F.lit(0)).alias("pageid"),
        F.coalesce(F.col("ns").cast("int"), F.lit(0)).alias("ns"),
        F.coalesce(F.col("title").cast("string"), F.lit("")).alias("title"),
        F.when(F.col("parentid") == 0, F.lit("new")).otherwise(F.lit("edit")).alias("type"),
        ts_col.alias("ts"),
        F.to_date(ts_col).alias("day"),
        F.date_trunc("hour", ts_col).alias("hour"),
        F.col("user").cast("string").alias("user"),
        F.col("userid").cast("long").alias("userid"),
        F.coalesce(F.col("bot_flag").cast("boolean"), F.lit(False)).alias("is_bot"),
        is_anon_col.alias("is_anon"),
        F.coalesce(F.col("minor").cast("boolean"), F.lit(False)).alias("is_minor"),
        F.coalesce(F.col("parentid") == 0, F.lit(False)).alias("is_new_page"),
        F.lit(None).cast("long").alias("oldlen"),
        F.col("size").cast("long").alias("newlen"),
        F.lit(None).cast("long").alias("delta"),
        F.lit(None).cast("long").alias("abs_delta"),
        is_reverted_col.alias("is_reverted"),
        is_revert_action_col.alias("is_revert_action"),
        (is_reverted_col | is_revert_action_col).alias("is_revert"),
        F.lit(None).cast("string").alias("sha1"),
        F.col("comment").cast("string").alias("comment"),
        tags_col.alias("tags"),
        F.lit(None).cast("string").alias("logtype"),
        F.lit(None).cast("string").alias("logaction"),
        F.lit("revisions").alias("source_feed"),
    )

