"""Score finalized Week 2 windows using Spark expressions and historical moments.

These heuristics identify unusual editing, not verified news or vandalism. Keep
the component points and missing evidence on every row so scores are auditable.
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F

from src.page_features import compare_history

SCORE_VERSION = "knowledge-shift-v1"


@dataclass(frozen=True)
class DetectorConfig:
    threshold: float = 65.0
    min_human_5m: int = 6
    min_human_1h: int = 15
    min_editors: int = 3
    min_human_share: float = 0.5
    zscore_cap: float = 6.0

    def __post_init__(self):
        if not 0 < self.threshold <= 100 or self.zscore_cap <= 0:
            raise ValueError("Threshold must be in (0, 100] and z-score cap positive")
        if min(self.min_human_5m, self.min_human_1h, self.min_editors) < 1:
            raise ValueError("Minimum activity counts must be positive")
        if not 0 <= self.min_human_share <= 1:
            raise ValueError("Minimum human share must be in [0, 1]")


def score_windows(
    windows: DataFrame, baselines: DataFrame | None, config: DetectorConfig = DetectorConfig(),
) -> DataFrame:
    """Return every window, including suppressed rows, for detector evaluation.

    Missing baselines produce low-confidence fixed-rule candidates with a null
    historical score. Baseline-period rows never become candidates. Content
    points require complete current and historical size observations.
    """
    if baselines is None:
        rows = windows.withColumn("has_baseline", F.lit(False))
        # Match the joined schema so the expressions below have one code path.
        for name in ("baseline_end_exclusive", "baseline_start"):
            rows = rows.withColumn(name, F.lit(None).cast("date"))
        rows = rows.withColumn("byte_baseline_complete", F.lit(False))
        for metric in ("edits_human", "distinct_human_editors", "gross_bytes"):
            for prefix in ("avg", "std"):
                rows = rows.withColumn(f"{prefix}_{metric}", F.lit(None).cast("double"))
    else:
        rows = compare_history(windows, baselines)

    def points(metric: str, weight: float, floor: float):
        zscore = (F.col(metric) - F.col(f"avg_{metric}")) / F.greatest(
            F.col(f"std_{metric}"), F.lit(floor)
        )
        return weight * F.least(F.lit(1.0), F.greatest(F.lit(0.0), zscore / config.zscore_cap))

    history = F.col("has_baseline")
    complete_bytes = history & F.col("byte_baseline_complete") & (F.col("size_coverage") == 1)
    minimum = F.when(F.col("window_duration") == "1 hour", config.min_human_1h).otherwise(config.min_human_5m)
    after_training = F.col("baseline_end_exclusive").isNull() | (F.col("day") >= F.col("baseline_end_exclusive"))
    eligible = (
        (F.col("edits_human") >= minimum)
        & (F.col("distinct_human_editors") >= config.min_editors)
        & ((1 - F.col("bot_ratio")) >= config.min_human_share)
        & after_training
    )
    rows = (
        rows.withColumn("score_version", F.lit(SCORE_VERSION))
        .withColumn("activity_points", F.when(history, points("edits_human", 35.0, 1.0)))
        .withColumn("editor_points", F.when(history, points("distinct_human_editors", 20.0, 1.0)))
        .withColumn("content_points", F.when(complete_bytes, points("gross_bytes", 20.0, 1024.0)))
        # Tags measure observed revert actions; missing tags are a lower bound.
        .withColumn("revert_points", F.when(history, 10.0 * F.least(F.lit(1.0), F.col("revert_ratio") / 0.30)))
        .withColumn("related_page_points", F.lit(None).cast("double"))
        .withColumn("pageview_points", F.lit(None).cast("double"))
        .withColumn("available_weight", F.when(history, F.when(complete_bytes, 85.0).otherwise(65.0)).otherwise(0.0))
        .withColumn("fixed_rule_alert", F.coalesce(eligible, F.lit(False)))
    )
    total = F.col("activity_points") + F.col("editor_points") + F.coalesce(F.col("content_points"), F.lit(0.0)) + F.col("revert_points")
    rows = (
        rows.withColumn("historical_score", F.when(history, 100.0 * total / F.col("available_weight")))
        .withColumn("historical_alert", F.coalesce(eligible & (F.col("historical_score") >= config.threshold), F.lit(False)))
        .withColumn("is_alert", F.col("historical_alert") | (F.col("fixed_rule_alert") & ~history))
        .withColumn("confidence", F.when(history, F.when(complete_bytes, "historical").otherwise("limited_sizes")).otherwise("low_no_baseline"))
        .withColumn("alert_type", F.when(~history, "activity_candidate")
                    .when(F.col("revert_ratio") >= 0.30, "revert_activity")
                    .when(F.col("net_bytes") <= -10000, "large_deletion")
                    .otherwise("knowledge_shift"))
        .withColumn("alert_id", F.sha2(F.concat_ws("::", F.lit(SCORE_VERSION), "window_id"), 256))
        .withColumn("reasons", F.filter(F.array(
            F.when(F.col("activity_points") > 0, F.format_string("Human edit increase: %.1f / 35 points", F.col("activity_points"))),
            F.when(F.col("editor_points") > 0, F.format_string("Editor increase: %.1f / 20 points", F.col("editor_points"))),
            F.when(F.col("content_points") > 0, F.format_string("Content movement: %.1f / 20 points", F.col("content_points"))),
            F.when(F.col("revert_points") > 0, F.format_string("Observed reverts: %.1f / 10 points", F.col("revert_points"))),
            F.when(~history, F.lit("No usable baseline: fixed-rule candidate only")),
        ), lambda reason: reason.isNotNull()))
        .withColumn("missing_evidence", F.filter(F.array(
            F.lit("related_page_activity"), F.lit("pageviews"), F.lit("revision_risk"),
            F.when(~complete_bytes, F.lit("complete_byte_comparison")),
            F.when(~history, F.lit("historical_baseline")),
        ), lambda reason: reason.isNotNull()))
    )
    return rows
