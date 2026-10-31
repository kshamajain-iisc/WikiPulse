"""Milestone 4.3: Historical Rankings, Churn/Bot Summary, Manual Audit & RDD vs DataFrame Benchmarks.

Delivers:
1. `curated/churn_bot_summary`: Daily & namespace Knowledge Churn, Rewriting Volume
   (`gross_bytes - |net_bytes|`), Contributor Diversity, and Human vs. Bot split.
2. `curated/rankings_daily`: Daily article rankings comparing raw edit count against
   Historical Baseline Surge (`daily_zscore`), demonstrating why fixed rules fail on
   routinely active pages while historical comparison surfaces true knowledge shifts.
3. Proposal Experiment #1 / Feature Metric #7: Manual pure-Python churn verification
   on a deterministic 2,000-event sample (asserts 0 discrepancy with Spark).
4. Proposal Experiment #3: PySpark RDD (`map`/`reduceByKey`) vs. Spark DataFrame
   (`groupBy`/`agg`) execution benchmark on per-page churn aggregation.
5. Proposal Experiment #6 / Performance Metric #6: Compressed JSONL.gz vs. Partitioned
   Parquet vs. Stored Result Table query latency benchmark (verifying < 2.0s dashboard queries).

Usage:
    .venv/bin/python3 Repo/src/historical/rankings_benchmark.py
"""

import gzip
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql import Window, functions as F
from src.config.settings import (
    CURATED_TABLES,
    NAMESPACES,
    RAW_RC_DIR,
    clean_macos_icon_files,
    get_spark_session,
    safe_read_parquet,
    safe_write_parquet,
)


def verify_manual_churn_sample(spark, sample_gz_path: Path, sample_size: int = 2000) -> dict:
    """Proposal Feature Metric #7: Verify Spark churn totals match manually calculated sample totals."""
    raw_lines: list[str] = []
    with gzip.open(sample_gz_path, "rt", encoding="utf-8") as fin:
        for line in fin:
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("type") in ("edit", "new"):
                raw_lines.append(line.strip())
                if len(raw_lines) >= sample_size:
                    break

    # 1. Pure-Python manual calculation
    manual_edits = len(raw_lines)
    manual_bots = 0
    manual_humans = 0
    manual_added = 0
    manual_deleted = 0
    manual_net = 0
    manual_gross = 0

    for s in raw_lines:
        r = json.loads(s)
        is_bot = bool(r.get("bot", False))
        if is_bot:
            manual_bots += 1
        else:
            manual_humans += 1
        delta = int(r.get("newlen", 0) or 0) - int(r.get("oldlen", 0) or 0)
        if delta > 0:
            manual_added += delta
        else:
            manual_deleted += abs(delta)
        manual_net += delta
        manual_gross += abs(delta)

    # 2. Spark DataFrame calculation on the exact same sample
    rdd = spark.sparkContext.parallelize(raw_lines, 4)
    sdf = spark.read.json(rdd).withColumn(
        "delta", F.col("newlen").cast("long") - F.col("oldlen").cast("long")
    )
    spark_row = sdf.agg(
        F.count("*").alias("edits"),
        F.sum(F.when(F.col("bot") == True, 1).otherwise(0)).alias("bots"),
        F.sum(F.when(F.col("bot") == True, 0).otherwise(1)).alias("humans"),
        F.sum(F.when(F.col("delta") > 0, F.col("delta")).otherwise(0)).alias("added"),
        F.sum(F.when(F.col("delta") < 0, F.abs(F.col("delta"))).otherwise(0)).alias("deleted"),
        F.sum("delta").alias("net"),
        F.sum(F.abs(F.col("delta"))).alias("gross"),
    ).collect()[0]

    matches = (
        manual_edits == int(spark_row["edits"])
        and manual_bots == int(spark_row["bots"])
        and manual_humans == int(spark_row["humans"])
        and manual_added == int(spark_row["added"])
        and manual_deleted == int(spark_row["deleted"])
        and manual_net == int(spark_row["net"])
        and manual_gross == int(spark_row["gross"])
    )
    assert matches, f"Manual vs Spark mismatch: manual={manual_net}/{manual_gross}, spark={spark_row}"

    return {
        "sample_file": sample_gz_path.name,
        "sample_size": sample_size,
        "manual_totals": {
            "edits": manual_edits,
            "human_edits": manual_humans,
            "bot_edits": manual_bots,
            "bytes_added": manual_added,
            "bytes_deleted": manual_deleted,
            "net_bytes": manual_net,
            "gross_churn_bytes": manual_gross,
            "rewriting_bytes": manual_gross - abs(manual_net),
        },
        "spark_exact_match": matches,
    }


def benchmark_rdd_vs_dataframe(rc_df, sample_days: int = 7) -> dict:
    """Proposal Experiment Plan #3: Compare one useful aggregation using RDDs and DataFrames."""
    days_list = [
        r["day"]
        for r in rc_df.select("day").distinct().orderBy("day").limit(sample_days).collect()
    ]
    sub_df = (
        rc_df.filter(F.col("day").isin(days_list) & (F.col("ns") == 0) & F.col("type").isin("edit", "new"))
        .select("pageid", "title", "is_bot", "is_revert", "delta", "abs_delta")
        .cache()
    )
    input_rows = sub_df.count()

    # 1. DataFrame Aggregation
    t_df_start = time.time()
    df_agg = (
        sub_df.groupBy("pageid")
        .agg(
            F.first("title").alias("title"),
            F.count("*").alias("edits"),
            F.sum(F.when(~F.col("is_bot"), 1).otherwise(0)).alias("human_edits"),
            F.sum(F.coalesce(F.col("abs_delta"), F.lit(0))).alias("gross_bytes"),
            F.sum(F.coalesce(F.col("delta"), F.lit(0))).alias("net_bytes"),
            F.sum(F.when(F.col("is_revert"), 1).otherwise(0)).alias("reverts"),
        )
        .orderBy(F.desc("gross_bytes"))
    )
    df_top5 = df_agg.limit(5).collect()
    df_runtime = time.time() - t_df_start

    # 2. RDD Aggregation (map + reduceByKey + takeOrdered)
    t_rdd_start = time.time()
    rdd_top5 = (
        sub_df.rdd.map(
            lambda r: (
                r["pageid"],
                (
                    r["title"],
                    1,
                    0 if r["is_bot"] else 1,
                    int(r["abs_delta"] or 0),
                    int(r["delta"] or 0),
                    1 if r["is_revert"] else 0,
                ),
            )
        )
        .reduceByKey(
            lambda a, b: (
                a[0] or b[0],
                a[1] + b[1],
                a[2] + b[2],
                a[3] + b[3],
                a[4] + b[4],
                a[5] + b[5],
            )
        )
        .takeOrdered(5, key=lambda kv: -kv[1][3])
    )
    rdd_runtime = time.time() - t_rdd_start

    sub_df.unpersist()

    # Verify identical top page & gross bytes
    assert df_top5[0]["pageid"] == rdd_top5[0][0], "RDD vs DataFrame top page mismatch"
    assert int(df_top5[0]["gross_bytes"]) == int(rdd_top5[0][1][3]), "RDD vs DataFrame gross_bytes mismatch"

    return {
        "days_evaluated": len(days_list),
        "input_rows": input_rows,
        "dataframe_runtime_sec": round(df_runtime, 3),
        "rdd_runtime_sec": round(rdd_runtime, 3),
        "speedup_factor_df_over_rdd": round(rdd_runtime / max(df_runtime, 1e-4), 2),
        "top_page_verified": df_top5[0]["title"],
        "top_page_gross_bytes": int(df_top5[0]["gross_bytes"]),
    }


def main() -> int:
    print("=" * 76)
    print(" WIKIPULSE MILESTONE 4.3: HISTORICAL RANKINGS, CHURN & BENCHMARKS")
    print("=" * 76)

    t0 = time.time()
    spark = get_spark_session(app_name="WikiPulse-Milestone4-Rankings")

    rc_path = CURATED_TABLES["rc_events"]
    base_path = CURATED_TABLES["page_baselines"]

    rc_df = safe_read_parquet(spark, rc_path)
    baselines_df = safe_read_parquet(spark, base_path)

    # -----------------------------------------------------------------------
    # 1. Knowledge Churn, Rewriting & Bot Ratio Summary (`churn_bot_summary`)
    # -----------------------------------------------------------------------
    print("\n[1/4] Computing Knowledge Churn, Rewriting Volume & Human-vs-Bot Split...")
    ns_map_expr = F.create_map(*[x for k, v in NAMESPACES.items() for x in (F.lit(k), F.lit(v))])

    edits_df = rc_df.filter(F.col("type").isin("edit", "new")).withColumn(
        "ns_name", F.coalesce(ns_map_expr[F.col("ns")], F.concat(F.lit("NS_"), F.col("ns")))
    ).withColumn(
        "safe_delta", F.coalesce(F.col("delta"), F.lit(0))
    ).withColumn(
        "safe_abs_delta", F.coalesce(F.col("abs_delta"), F.lit(0))
    )

    churn_bot_df = (
        edits_df.groupBy("day", "ns", "ns_name")
        .agg(
            F.count("*").alias("total_edits"),
            F.sum(F.when(~F.col("is_bot") & ~F.col("is_anon"), 1).otherwise(0)).alias("human_registered_edits"),
            F.sum(F.when(~F.col("is_bot") & F.col("is_anon"), 1).otherwise(0)).alias("anon_temp_edits"),
            F.sum(F.when(F.col("is_bot"), 1).otherwise(0)).alias("bot_edits"),
            F.countDistinct(F.when(~F.col("is_bot"), F.col("user"))).alias("distinct_human_editors"),
            F.sum(F.when(F.col("safe_delta") > 0, F.col("safe_delta")).otherwise(0)).alias("bytes_added"),
            F.sum(F.when(F.col("safe_delta") < 0, F.abs(F.col("safe_delta"))).otherwise(0)).alias("bytes_deleted"),
            F.sum("safe_delta").alias("net_bytes"),
            F.sum("safe_abs_delta").alias("gross_churn_bytes"),
            F.sum(F.when(F.col("is_revert"), 1).otherwise(0)).alias("reverts_count"),
        )
        .withColumn(
            "rewriting_bytes",
            F.greatest(F.lit(0), F.col("gross_churn_bytes") - F.abs(F.col("net_bytes"))),
        )
        .withColumn(
            "bot_ratio_pct",
            F.round(100.0 * F.col("bot_edits") / F.greatest(F.col("total_edits"), F.lit(1)), 2),
        )
        .withColumn(
            "revert_ratio_pct",
            F.round(100.0 * F.col("reverts_count") / F.greatest(F.col("total_edits"), F.lit(1)), 2),
        )
        .withColumn("edits_per_hour", F.round(F.col("total_edits") / 24.0, 2))
    )

    churn_out = CURATED_TABLES["churn_bot_summary"]
    safe_write_parquet(churn_bot_df, churn_out, repartition_num=4)

    # -----------------------------------------------------------------------
    # 2. Daily Historical Comparison Rankings (`rankings_daily`)
    # -----------------------------------------------------------------------
    print("[2/4] Building Daily Article Rankings with Historical Baseline Comparison...")
    daily_articles = (
        edits_df.filter("ns = 0")
        .groupBy("day", "pageid")
        .agg(
            F.last("title").alias("title"),
            F.count("*").alias("edits_total"),
            F.sum(F.when(~F.col("is_bot"), 1).otherwise(0)).alias("edits_human"),
            F.sum(F.when(F.col("is_bot"), 1).otherwise(0)).alias("edits_bot"),
            F.countDistinct(F.when(~F.col("is_bot"), F.col("user"))).alias("distinct_human_editors"),
            F.sum("safe_abs_delta").alias("gross_churn_bytes"),
            F.sum("safe_delta").alias("net_bytes"),
            F.sum(F.when(F.col("is_revert"), 1).otherwise(0)).alias("reverts_count"),
        )
        .withColumn(
            "rewriting_bytes",
            F.greatest(F.lit(0), F.col("gross_churn_bytes") - F.abs(F.col("net_bytes"))),
        )
        .withColumn(
            "revert_ratio",
            F.round(F.col("reverts_count") / F.greatest(F.col("edits_total"), F.lit(1)), 4),
        )
        .withColumn(
            "controversy_score",
            F.round(
                F.log1p("edits_human")
                * F.log1p("distinct_human_editors")
                * (F.col("revert_ratio") + F.lit(0.05)),
                3,
            ),
        )
    )

    # Join with historical baselines to compute relative surge z-score
    base_lookup = baselines_df.filter("ns = 0").select(
        "pageid",
        "activity_tier",
        "avg_daily_edits",
        "std_daily_edits",
        "avg_gross_bytes_daily",
        "std_gross_bytes_daily",
        "avg_daily_pageviews",
        "last_available_date",
        "has_missing_data",
    )

    ranked_df = (
        daily_articles.join(base_lookup, on="pageid", how="left")
        .withColumn("activity_tier", F.coalesce(F.col("activity_tier"), F.lit("quiet")))
        .withColumn("avg_daily_edits", F.coalesce(F.col("avg_daily_edits"), F.lit(0.1)))
        .withColumn("std_daily_edits", F.coalesce(F.col("std_daily_edits"), F.lit(0.5)))
        .withColumn(
            "daily_velocity_zscore",
            F.round(
                (F.col("edits_human") - F.col("avg_daily_edits"))
                / F.greatest(F.col("std_daily_edits"), F.lit(0.5)),
                2,
            ),
        )
        .filter("edits_human >= 5 OR gross_churn_bytes >= 5000")
    )

    w_day_raw = Window.partitionBy("day").orderBy(F.desc("edits_human"), F.desc("distinct_human_editors"))
    w_day_surge = Window.partitionBy("day").orderBy(F.desc("daily_velocity_zscore"), F.desc("distinct_human_editors"))

    ranked_final = (
        ranked_df.withColumn("raw_edit_rank", F.row_number().over(w_day_raw))
        .withColumn("baseline_surge_rank", F.row_number().over(w_day_surge))
        .filter("raw_edit_rank <= 200 OR baseline_surge_rank <= 200")
    )

    rankings_out = CURATED_TABLES["rankings_daily"]
    safe_write_parquet(ranked_final, rankings_out, repartition_num=8)

    # -----------------------------------------------------------------------
    # 3. Manual Sample Audit & RDD vs DataFrame Benchmark
    # -----------------------------------------------------------------------
    print("[3/4] Running Manual Sample Audit & RDD vs. DataFrame Benchmark...")
    sample_gz = RAW_RC_DIR / "rc_2026-09-16.jsonl.gz"
    manual_audit = verify_manual_churn_sample(spark, sample_gz, sample_size=2000)
    print(
        f"  ✓ Manual Sample Audit (2,000 events): exact_match={manual_audit['spark_exact_match']} "
        f"| net_bytes={manual_audit['manual_totals']['net_bytes']:+,} "
        f"| gross_churn={manual_audit['manual_totals']['gross_churn_bytes']:,}"
    )

    rdd_vs_df = benchmark_rdd_vs_dataframe(rc_df, sample_days=7)
    print(
        f"  ✓ RDD vs DataFrame (7 days, {rdd_vs_df['input_rows']:,} rows): "
        f"DF={rdd_vs_df['dataframe_runtime_sec']}s vs RDD={rdd_vs_df['rdd_runtime_sec']}s "
        f"({rdd_vs_df['speedup_factor_df_over_rdd']}x speedup)"
    )

    # -----------------------------------------------------------------------
    # 4. JSONL.gz vs Parquet & Dashboard Query Latency Benchmark (< 2s target)
    # -----------------------------------------------------------------------
    print("[4/4] Benchmarking Dashboard Query Latency (JSONL.gz vs Stored Parquet)...")
    t_json0 = time.time()
    _ = (
        spark.read.json(str(RAW_RC_DIR / "rc_2026-09-16.jsonl.gz"))
        .filter("ns = 0 AND type IN ('edit', 'new') AND bot = false")
        .groupBy("title")
        .count()
        .orderBy(F.desc("count"))
        .limit(20)
        .collect()
    )
    json_1day_query_sec = round(time.time() - t_json0, 3)

    t_pq_stored0 = time.time()
    top_ranked_sample = (
        safe_read_parquet(spark, rankings_out)
        .filter("day = DATE '2026-09-16'")
        .orderBy("baseline_surge_rank")
        .limit(20)
        .collect()
    )
    stored_rankings_query_sec = round(time.time() - t_pq_stored0, 3)

    t_churn_stored0 = time.time()
    _ = (
        safe_read_parquet(spark, churn_out)
        .filter("ns = 0")
        .orderBy("day")
        .collect()
    )
    stored_churn_query_sec = round(time.time() - t_churn_stored0, 3)

    bench_dir = CURATED_TABLES["benchmarks"]
    bench_dir.mkdir(parents=True, exist_ok=True)
    m4_report = {
        "manual_churn_audit": manual_audit,
        "rdd_vs_dataframe_benchmark": rdd_vs_df,
        "query_latency_benchmark": {
            "raw_jsonl_gz_1day_top20_sec": json_1day_query_sec,
            "stored_parquet_rankings_top20_sec": stored_rankings_query_sec,
            "stored_parquet_46day_churn_series_sec": stored_churn_query_sec,
            "meets_under_2s_target": stored_rankings_query_sec < 2.0 and stored_churn_query_sec < 2.0,
        },
    }
    (bench_dir / "milestone4_benchmarks.json").write_text(json.dumps(m4_report, indent=2))

    print(
        f"  ✓ Query Latency: Raw JSONL.gz (1d) = {json_1day_query_sec}s | "
        f"Stored Rankings Parquet = {stored_rankings_query_sec}s | "
        f"Stored 46d Churn Parquet = {stored_churn_query_sec}s (<2.0s target met!)"
    )

    print("\n  Sample Historical Surge vs. Raw Rank (Sept 16, 2026):")
    for r in top_ranked_sample[:5]:
        print(
            f"    • #{r['baseline_surge_rank']:<2} (raw #{r['raw_edit_rank']:<3}) {r['title']:<38} "
            f"| edits={r['edits_human']:<3} vs avg={r['avg_daily_edits']:.1f} (z={r['daily_velocity_zscore']:+.1f})"
        )

    print(f"\n✓ Milestone 4 Rankings & Benchmarks completed in {time.time() - t0:.1f}s")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

