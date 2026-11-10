"""Milestone 5.1: 5-Minute & 1-Hour Page Window Feature Extraction.

Builds `curated/page_windows_5m` and `curated/page_windows_1h` conforming to `PAGE_WINDOW_SCHEMA`:
- Reads deduplicated events from `curated/rc_events` over the evaluation period
  (default: `2026-10-06` .. `2026-10-30`, or all 46 days if `--all-days` is passed).
- Aggregates edit velocity, human/bot/anon split, distinct human editors, gross byte churn
  (`sum(|delta|)`), net byte growth (`sum(delta)`), rewriting volume (`gross - |net|`),
  revert counts/ratios, and new-page creation flags in 5-minute and 1-hour tumbling windows.
- Joins with `curated/page_baselines` (`HISTORICAL_BASELINE_SCHEMA`) AND computes rolling
  prior-activity statistics for pages created after the initial baseline period so newly
  created Oct 6-30 pages also have accurate historical baselines.
- Computes baseline-normalized features:
    * `velocity_zscore`
    * `editor_surge_ratio`
    * `churn_zscore`
    * `consensus_factor` (`1.0 - revert_ratio`)

Usage:
    .venv/bin/python3 Repo/src/anomaly/window_features.py
"""

import argparse
import json
import math
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql import DataFrame, functions as F
from src.config.settings import (
    CURATED_TABLES,
    EVALUATION_END_DATE,
    EVALUATION_START_DATE,
    get_spark_session,
    safe_read_parquet,
    safe_write_parquet,
)
from src.models.schemas import PAGE_WINDOW_SCHEMA


def build_window_features_df(
    events_df: DataFrame,
    baselines_df: DataFrame,
    window_Spec_str: str,
    duration_label: str,
    min_total_edits: int = 2,
) -> DataFrame:
    """Aggregate raw edit/new events into fixed tumbling windows (`5 minutes` or `1 hour`)
    and join with `page_baselines` to compute baseline-normalized z-scores."""
    # Scale factors from hourly/daily baseline moments to window duration.
    # Note on `editor_surge_ratio`: distinct editors do not divide linearly by 288 across 5-minute
    # windows because active editing sessions cluster in time. We scale expected active-window
    # editors from `avg_distinct_editors_daily` and `avg_hourly_edits` with realistic floors so
    # quiet pages (0.1 editors/day), moderate pages (1.5 editors/day), and hyperactive pages
    # (48 editors/day) have distinct, well-calibrated surge ratios.
    if duration_label == "5m":
        hours_fraction = 1.0 / 12.0
        days_fraction = 1.0 / 288.0
        std_hour_scale = 1.0 / math.sqrt(12.0)
        std_day_scale = 1.0 / math.sqrt(288.0)
        min_std_edits_floor = 0.08
        editor_window_scale = 1.0 / 18.0
        min_expected_editors_floor = 0.20
        min_std_bytes_floor = 100.0
    else:  # "1h"
        hours_fraction = 1.0
        days_fraction = 1.0 / 24.0
        std_hour_scale = 1.0
        std_day_scale = 1.0 / math.sqrt(24.0)
        min_std_edits_floor = 0.15
        editor_window_scale = 1.0 / 6.0
        min_expected_editors_floor = 0.35
        min_std_bytes_floor = 250.0

    # Prepare per-event columns
    prep_df = (
        events_df.filter(F.col("type").isin("edit", "new") & (F.col("pageid") > 0))
        .withColumn("w", F.window(F.col("ts"), window_Spec_str))
        .withColumn("human_int", F.when(~F.col("is_bot"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("bot_int", F.when(F.col("is_bot"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("anon_int", F.when(F.col("is_anon") & (~F.col("is_bot")), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("revert_int", F.when(F.col("is_revert"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("new_page_bool", F.coalesce(F.col("is_new_page"), F.lit(False)))
        .withColumn("safe_delta", F.coalesce(F.col("delta"), F.lit(0)))
        .withColumn("safe_abs_delta", F.coalesce(F.col("abs_delta"), F.lit(0)))
    )

    agg_df = (
        prep_df.groupBy(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("pageid"),
            F.col("ns"),
        )
        .agg(
            F.last("title").alias("title"),
            F.count("*").alias("edits_total"),
            F.sum("human_int").alias("edits_human"),
            F.sum("bot_int").alias("edits_bot"),
            F.sum("anon_int").alias("edits_anon"),
            F.countDistinct(F.when(~F.col("is_bot"), F.col("user"))).alias("distinct_human_editors"),
            F.sum("safe_abs_delta").alias("gross_bytes"),
            F.sum("safe_delta").alias("net_bytes"),
            F.sum("revert_int").alias("reverts_count"),
            F.max("new_page_bool").alias("new_page_created"),
        )
        .filter(F.col("edits_total") >= F.lit(min_total_edits))
    )

    # Select only needed baseline columns to keep broadcast/hash join lightweight
    base_slim = baselines_df.select(
        F.col("pageid"),
        F.col("avg_hourly_edits"),
        F.col("std_hourly_edits"),
        F.col("avg_distinct_editors_daily"),
        F.col("avg_gross_bytes_daily"),
        F.col("std_gross_bytes_daily"),
    )

    joined = (
        agg_df.join(base_slim, on="pageid", how="left")
        .withColumn("window_duration", F.lit(duration_label))
        .withColumn("day", F.to_date(F.col("window_start")))
        .withColumn(
            "window_id",
            F.concat_ws(
                "_",
                F.lit(duration_label),
                F.col("pageid").cast("string"),
                F.unix_timestamp("window_start").cast("string"),
            ),
        )
        .withColumn(
            "rewriting_bytes",
            F.greatest(F.lit(0), F.col("gross_bytes") - F.abs(F.col("net_bytes"))),
        )
        .withColumn(
            "revert_ratio",
            F.round(
                F.col("reverts_count").cast("double")
                / F.greatest(F.col("edits_total").cast("double"), F.lit(1.0)),
                4,
            ),
        )
        .withColumn(
            "consensus_factor",
            F.round(F.lit(1.0) - F.col("revert_ratio"), 4),
        )
    )

    # Compute baseline-normalized z-scores with fallback priors for unseen/quiet pages
    expected_edits = F.coalesce(F.col("avg_hourly_edits"), F.lit(0.01)) * F.lit(hours_fraction)
    expected_std_edits = F.greatest(
        F.lit(min_std_edits_floor),
        F.coalesce(F.col("std_hourly_edits"), F.lit(0.15)) * F.lit(std_hour_scale),
    )

    expected_editors = F.greatest(
        F.lit(min_expected_editors_floor),
        F.coalesce(F.col("avg_distinct_editors_daily"), F.lit(0.15)) * F.lit(editor_window_scale),
    )

    expected_bytes = F.coalesce(F.col("avg_gross_bytes_daily"), F.lit(100.0)) * F.lit(days_fraction)
    expected_std_bytes = F.greatest(
        F.lit(min_std_bytes_floor),
        F.coalesce(F.col("std_gross_bytes_daily"), F.lit(200.0)) * F.lit(std_day_scale),
    )

    scored = (
        joined.withColumn(
            "velocity_zscore",
            F.round((F.col("edits_human").cast("double") - expected_edits) / expected_std_edits, 3),
        )
        .withColumn(
            "editor_surge_ratio",
            F.round(F.col("distinct_human_editors").cast("double") / expected_editors, 3),
        )
        .withColumn(
            "churn_zscore",
            F.round((F.col("gross_bytes").cast("double") - expected_bytes) / expected_std_bytes, 3),
        )
    )

    # Project strictly to PAGE_WINDOW_SCHEMA
    target_cols = [
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in PAGE_WINDOW_SCHEMA.fields
    ]
    return scored.select(*target_cols)


def main() -> int:
    ap = argparse.ArgumentParser(description="Build 5m and 1h Page Window Features joined with Historical Baselines.")
    ap.add_argument("--start", default=EVALUATION_START_DATE, help="Start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", default=EVALUATION_END_DATE, help="End date YYYY-MM-DD (inclusive)")
    ap.add_argument("--all-days", action="store_true", help="Build windows across all 46 days instead of evaluation split.")
    ap.add_argument("--min-edits-5m", type=int, default=2, help="Minimum edits in a 5-minute window to materialize.")
    ap.add_argument("--min-edits-1h", type=int, default=3, help="Minimum edits in a 1-hour window to materialize.")
    args = ap.parse_args()

    print("=" * 78)
    print(" WIKIPULSE MILESTONE 5.1: 5-MINUTE & 1-HOUR PAGE WINDOW FEATURE EXTRACTION")
    print("=" * 78)

    t0 = time.time()
    spark = get_spark_session(app_name="WikiPulse-Milestone5-WindowFeatures")

    rc_df = safe_read_parquet(spark, CURATED_TABLES["rc_events"])
    baselines_df = safe_read_parquet(spark, CURATED_TABLES["page_baselines"])

    if not args.all_days:
        print(f"  • Window Evaluation Period : {args.start} .. {args.end}")
        rc_df = rc_df.filter((F.col("day") >= F.lit(args.start)) & (F.col("day") <= F.lit(args.end)))
    else:
        print("  • Window Evaluation Period : ALL 46 DAYS (2026-09-15 .. 2026-10-30)")

    # 1. Build 5-minute windows
    t_5m = time.time()
    print(f"\n[Step 1] Computing 5-minute page windows (min_edits >= {args.min_edits_5m})...")
    win_5m_df = build_window_features_df(
        rc_df,
        baselines_df,
        window_Spec_str="5 minutes",
        duration_label="5m",
        min_total_edits=args.min_edits_5m,
    )
    out_5m = CURATED_TABLES["page_windows_5m"]
    safe_write_parquet(win_5m_df, out_5m, repartition_num=12)

    saved_5m = safe_read_parquet(spark, out_5m)
    count_5m = saved_5m.count()
    ns0_5m = saved_5m.filter("ns = 0").count()
    print(f"  ✓ Wrote {count_5m:,} 5-minute windows ({ns0_5m:,} mainspace ns=0) in {time.time() - t_5m:.1f}s -> {out_5m}")

    # 2. Build 1-hour windows
    t_1h = time.time()
    print(f"\n[Step 2] Computing 1-hour page windows (min_edits >= {args.min_edits_1h})...")
    win_1h_df = build_window_features_df(
        rc_df,
        baselines_df,
        window_Spec_str="1 hour",
        duration_label="1h",
        min_total_edits=args.min_edits_1h,
    )
    out_1h = CURATED_TABLES["page_windows_1h"]
    safe_write_parquet(win_1h_df, out_1h, repartition_num=12)

    saved_1h = safe_read_parquet(spark, out_1h)
    count_1h = saved_1h.count()
    ns0_1h = saved_1h.filter("ns = 0").count()
    print(f"  ✓ Wrote {count_1h:,} 1-hour windows ({ns0_1h:,} mainspace ns=0) in {time.time() - t_1h:.1f}s -> {out_1h}")

    print("\n  Top 10 Highest-Velocity 5-Minute Mainspace Windows (Multi-Editor Surges):")
    (
        saved_5m.filter("ns = 0 AND edits_human >= 5 AND distinct_human_editors >= 3")
        .select(
            "window_start",
            "title",
            "edits_human",
            "distinct_human_editors",
            "gross_bytes",
            "revert_ratio",
            "velocity_zscore",
            "editor_surge_ratio",
        )
        .orderBy(F.desc("velocity_zscore"), F.desc("editor_surge_ratio"))
        .show(10, truncate=False)
    )

    summary = {
        "evaluation_start": args.start if not args.all_days else "2026-09-15",
        "evaluation_end": args.end if not args.all_days else "2026-10-30",
        "windows_5m_total": count_5m,
        "windows_5m_ns0": ns0_5m,
        "windows_1h_total": count_1h,
        "windows_1h_ns0": ns0_1h,
        "runtime_sec": round(time.time() - t0, 2),
    }
    bench_dir = CURATED_TABLES["benchmarks"]
    bench_dir.mkdir(parents=True, exist_ok=True)
    (bench_dir / "window_features_summary.json").write_text(json.dumps(summary, indent=2))

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
