"""Milestone 4.2: Per-Page Historical Baselines & Wikimedia Analytics Enrichment.

Builds `historical/data/curated/page_baselines` (conforming to HISTORICAL_BASELINE_SCHEMA):
- Computes per-page daily & hourly edit velocity, distinct human editors, gross/net byte
  churn, revert ratio, and bot ratio over the baseline window (default: 2026-09-15 .. 2026-10-05,
  21 full UTC days = 504 hours).
- Uses exact zero-inclusive calendar moments:
    mean_daily = sum(x_d) / D
    std_daily  = sqrt(max(0, sum(x_d^2) / D - mean_daily^2))
  so quiet days/hours with 0 edits are accurately reflected without materializing dense zero grids.
- Applies namespace & activity-tier fallback floors (`std_floor`) so quiet pages never divide by 0.
- Joins daily pageviews from `historical/data/analytics/pageviews_top_daily.json` and any cached
  `page_*_views.json` files (plus optional `--fetch-top-pageviews N` live REST API pull).
- Explicitly records `last_available_date`, `has_missing_data`, `last_pageview_date`, and
  `pageview_missing_flag` per Proposal Feature Metric #8.

Usage:
    .venv/bin/python3 Repo/src/historical/build_baselines.py
    .venv/bin/python3 Repo/src/historical/build_baselines.py --fetch-top-pageviews 30
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql import Window, functions as F
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType
from src.config.settings import (
    BASELINE_END_DATE,
    BASELINE_START_DATE,
    CURATED_TABLES,
    RAW_ANALYTICS_DIR,
    USER_AGENT,
    build_analytics_per_page_urls,
    clean_macos_icon_files,
    get_spark_session,
    safe_read_parquet,
    safe_write_parquet,
)
from src.models.schemas import HISTORICAL_BASELINE_SCHEMA


def fetch_json_polite(url: str, retries: int = 3) -> dict | None:
    """Fetch JSON from Wikimedia REST Analytics API with polite User-Agent and backoff."""
    req = urllib.request.Request(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                time.sleep(2 ** attempt * 1.5)
                continue
            return None
        except Exception:
            if attempt < retries - 1:
                time.sleep(1.5)
                continue
            return None
    return None


def load_and_enrich_pageviews(top_titles: list[str], start_dt: date, end_dt: date, fetch_top_n: int = 0) -> list[tuple]:
    """Load pageview statistics from `data/analytics/` and optionally fetch missing top pages."""
    pages_cache_dir = RAW_ANALYTICS_DIR / "pages"
    pages_cache_dir.mkdir(parents=True, exist_ok=True)

    # 1. Parse `pageviews_top_daily.json`
    per_title_views: dict[str, dict[str, int]] = {}
    top_daily_path = RAW_ANALYTICS_DIR / "pageviews_top_daily.json"
    if top_daily_path.exists():
        try:
            top_data = json.loads(top_daily_path.read_text(encoding="utf-8"))
            for day_str, articles in top_data.items():
                for item in articles:
                    art = item.get("article", "").replace("_", " ")
                    views = item.get("views")
                    if art and views is not None:
                        per_title_views.setdefault(art, {})[day_str] = int(views)
        except Exception as e:
            print(f"  ! Warning reading pageviews_top_daily.json: {e}")

    # 2. Optionally fetch per-article daily pageviews for top N active pages
    s_ymd = start_dt.strftime("%Y%m%d")
    e_ymd = end_dt.strftime("%Y%m%d")
    if fetch_top_n > 0:
        to_fetch = [t for t in top_titles[:fetch_top_n] if t]
        print(f"  • Checking/fetching per-article daily pageviews for top {len(to_fetch)} active pages...")
        for title in to_fetch:
            safe_name = urllib.parse.quote(title.replace(" ", "_"), safe="")
            cache_file = pages_cache_dir / f"views_{safe_name}_{s_ymd}_{e_ymd}.json"
            data = None
            if cache_file.exists():
                try:
                    data = json.loads(cache_file.read_text(encoding="utf-8"))
                except Exception:
                    data = None
            else:
                urls = build_analytics_per_page_urls(safe_name, s_ymd, e_ymd)
                data = fetch_json_polite(urls["views"])
                if data:
                    cache_file.write_text(json.dumps(data, indent=1), encoding="utf-8")
                time.sleep(0.15)

            if data and "items" in data:
                for it in data["items"]:
                    ts_raw = str(it.get("timestamp", ""))[:8]
                    if len(ts_raw) == 8:
                        d_str = f"{ts_raw[:4]}-{ts_raw[4:6]}-{ts_raw[6:8]}"
                        per_title_views.setdefault(title, {})[d_str] = int(it.get("views", 0))

    # 3. Also load any existing `page_*_views.json` in `data/analytics/`
    for pfile in RAW_ANALYTICS_DIR.glob("page_*_views.json"):
        try:
            pdata = json.loads(pfile.read_text(encoding="utf-8"))
            for it in pdata.get("items", []):
                art = it.get("article", "").replace("_", " ")
                ts_raw = str(it.get("timestamp", ""))[:8]
                if art and len(ts_raw) == 8:
                    d_str = f"{ts_raw[:4]}-{ts_raw[4:6]}-{ts_raw[6:8]}"
                    per_title_views.setdefault(art, {})[d_str] = int(it.get("views", 0))
        except Exception:
            pass

    # Compute mean, std, last_pageview_date, and missing flag per title
    expected_days = max(1, (end_dt - start_dt).days + 1)
    rows = []
    for title, day_map in per_title_views.items():
        if not day_map:
            continue
        vals = [float(v) for v in day_map.values()]
        n = len(vals)
        mean_v = sum(vals) / n
        var_v = max(0.0, (sum(v * v for v in vals) / n) - (mean_v * mean_v))
        std_v = var_v ** 0.5
        last_dt_str = max(day_map.keys())
        last_dt = date.fromisoformat(last_dt_str)
        missing_flag = n < expected_days
        rows.append((title, float(round(mean_v, 2)), float(round(std_v, 2)), last_dt, bool(missing_flag)))

    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Build per-page historical baselines in Spark.")
    ap.add_argument("--start", default=BASELINE_START_DATE, help="Baseline start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", default=BASELINE_END_DATE, help="Baseline end date YYYY-MM-DD (inclusive)")
    ap.add_argument(
        "--fetch-top-pageviews",
        type=int,
        default=25,
        help="Number of top active articles to enrich via Wikimedia Pageviews API (0 to use local files only).",
    )
    args = ap.parse_args()

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_dt = datetime.strptime(args.end, "%Y-%m-%d").date()
    calendar_days = max(1, (end_dt - start_dt).days + 1)
    calendar_hours = calendar_days * 24

    print("=" * 76)
    print(" WIKIPULSE MILESTONE 4.2: HISTORICAL BASELINES & ANALYTICS ENRICHMENT")
    print("=" * 76)
    print(f"  • Baseline Window : {args.start} .. {args.end} ({calendar_days} days / {calendar_hours} hours)")

    t0 = time.time()
    spark = get_spark_session(app_name="WikiPulse-Milestone4-Baselines")

    rc_path = CURATED_TABLES["rc_events"]
    rc_df = safe_read_parquet(spark, rc_path)

    # Check overall dataset date coverage to detect any missing source days
    dataset_days_row = (
        rc_df.filter((F.col("day") >= F.lit(args.start)) & (F.col("day") <= F.lit(args.end)))
        .agg(
            F.countDistinct("day").alias("observed_days"),
            F.max("day").alias("max_day"),
        )
        .collect()[0]
    )
    observed_source_days = int(dataset_days_row["observed_days"] or 0)
    source_last_date = dataset_days_row["max_day"] or end_dt
    source_has_missing_days = observed_source_days < calendar_days

    print(
        f"  • Source Coverage : {observed_source_days}/{calendar_days} days present "
        f"(last_available_date={source_last_date}, has_missing_data={source_has_missing_days})"
    )

    # Filter to baseline window and edit/new events
    base_events = rc_df.filter(
        (F.col("day") >= F.lit(args.start))
        & (F.col("day") <= F.lit(args.end))
        & F.col("type").isin("edit", "new")
    ).withColumn(
        "human_edit", F.when(~F.col("is_bot"), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "bot_edit", F.when(F.col("is_bot"), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "revert_int", F.when(F.col("is_revert"), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "safe_delta", F.coalesce(F.col("delta"), F.lit(0))
    ).withColumn(
        "safe_abs_delta", F.coalesce(F.col("abs_delta"), F.lit(0))
    )

    # 1. Per-page daily rollup (over active days, then normalized across all `calendar_days`)
    daily_page = (
        base_events.groupBy("pageid", "ns", "day")
        .agg(
            F.first("title").alias("day_title"),
            F.count("*").alias("d_edits_all"),
            F.sum("human_edit").alias("d_edits_human"),
            F.sum("bot_edit").alias("d_edits_bot"),
            F.sum("revert_int").alias("d_reverts"),
            F.countDistinct(F.when(~F.col("is_bot"), F.col("user"))).alias("d_distinct_human_editors"),
            F.sum("safe_abs_delta").alias("d_gross_bytes"),
            F.sum("safe_delta").alias("d_net_bytes"),
        )
    )

    D = float(max(1, observed_source_days))
    daily_stats = (
        daily_page.groupBy("pageid", "ns")
        .agg(
            F.last("day_title").alias("title"),
            F.count("*").alias("days_active"),
            F.max("day").alias("page_last_edit_date"),
            F.sum("d_edits_all").alias("total_baseline_edits"),
            F.sum("d_edits_human").alias("sum_human_edits"),
            F.sum(F.col("d_edits_human") * F.col("d_edits_human")).alias("sum_sq_human_edits_daily"),
            F.sum("d_edits_bot").alias("sum_bot_edits"),
            F.sum("d_reverts").alias("sum_reverts"),
            F.sum("d_distinct_editors_daily" if "d_distinct_editors_daily" in daily_page.columns else "d_distinct_human_editors").alias("sum_distinct_editors_daily"),
            F.sum("d_gross_bytes").alias("sum_gross_bytes"),
            F.sum(F.col("d_gross_bytes").cast("double") * F.col("d_gross_bytes").cast("double")).alias("sum_sq_gross_bytes_daily"),
            F.sum("d_net_bytes").alias("sum_net_bytes"),
        )
        .withColumn("avg_daily_edits", F.round(F.col("sum_human_edits") / F.lit(D), 4))
        .withColumn(
            "std_daily_edits",
            F.round(
                F.sqrt(
                    F.greatest(
                        F.lit(0.04),  # Floor variance (std >= 0.20) for quiet pages
                        (F.col("sum_sq_human_edits_daily") / F.lit(D)) - F.pow(F.col("avg_daily_edits"), 2),
                    )
                ),
                4,
            ),
        )
        .withColumn("avg_distinct_editors_daily", F.round(F.col("sum_distinct_editors_daily") / F.lit(D), 4))
        .withColumn("avg_gross_bytes_daily", F.round(F.col("sum_gross_bytes") / F.lit(D), 2))
        .withColumn(
            "std_gross_bytes_daily",
            F.round(
                F.sqrt(
                    F.greatest(
                        F.lit(10000.0),  # Floor std >= 100 bytes
                        (F.col("sum_sq_gross_bytes_daily") / F.lit(D)) - F.pow(F.col("avg_gross_bytes_daily"), 2),
                    )
                ),
                2,
            ),
        )
        .withColumn("avg_net_bytes_daily", F.round(F.col("sum_net_bytes") / F.lit(D), 2))
        .withColumn(
            "baseline_revert_ratio",
            F.round(F.col("sum_reverts") / F.greatest(F.col("total_baseline_edits"), F.lit(1)), 4),
        )
        .withColumn(
            "baseline_bot_ratio",
            F.round(F.col("sum_bot_edits") / F.greatest(F.col("total_baseline_edits"), F.lit(1)), 4),
        )
    )

    # 2. Per-page hourly rollup (normalized across all H = D * 24 hours)
    H = float(max(24, observed_source_days * 24))
    hourly_page = (
        base_events.filter(~F.col("is_bot"))
        .groupBy("pageid", "ns", "hour")
        .agg(F.count("*").alias("h_edits_human"))
    )

    hourly_stats = (
        hourly_page.groupBy("pageid", "ns")
        .agg(
            F.sum("h_edits_human").alias("h_sum"),
            F.sum(F.col("h_edits_human") * F.col("h_edits_human")).alias("h_sum_sq"),
            F.expr("percentile_approx(h_edits_human, 0.95)").cast("double").alias("p95_active_hour"),
        )
        .withColumn("avg_hourly_edits", F.round(F.col("h_sum") / F.lit(H), 5))
        .withColumn(
            "std_hourly_edits",
            F.round(
                F.sqrt(
                    F.greatest(
                        F.lit(0.0225),  # Floor std >= 0.15 edits/hr
                        (F.col("h_sum_sq") / F.lit(H)) - F.pow(F.col("avg_hourly_edits"), 2),
                    )
                ),
                5,
            ),
        )
        .withColumn("p95_hourly_edits", F.round(F.coalesce(F.col("p95_active_hour"), F.lit(1.0)), 2))
        .select("pageid", "ns", "avg_hourly_edits", "std_hourly_edits", "p95_hourly_edits")
    )

    # 3. Enrich with Wikimedia Analytics Pageviews
    top_ns0_rows = (
        daily_stats.filter("ns = 0")
        .orderBy(F.desc("total_baseline_edits"))
        .limit(max(50, args.fetch_top_pageviews))
        .select("title")
        .collect()
    )
    top_titles = [r["title"] for r in top_ns0_rows if r["title"]]
    pv_rows = load_and_enrich_pageviews(top_titles, start_dt, end_dt, fetch_top_n=args.fetch_top_pageviews)

    pv_schema = StructType(
        [
            StructField("title", StringType(), False),
            StructField("avg_daily_pageviews", DoubleType(), True),
            StructField("std_daily_pageviews", DoubleType(), True),
            StructField("last_pageview_date", DateType(), True),
            StructField("pv_incomplete", StringType(), True),
        ]
    )
    pv_df = spark.createDataFrame(
        [(r[0], r[1], r[2], r[3], "true" if r[4] else "false") for r in pv_rows],
        schema=pv_schema,
    )

    # 4. Assemble final HISTORICAL_BASELINE_SCHEMA DataFrame
    joined = (
        daily_stats.join(hourly_stats, on=["pageid", "ns"], how="left")
        .join(pv_df, on="title", how="left")
        .withColumn("avg_hourly_edits", F.coalesce(F.col("avg_hourly_edits"), F.lit(0.0)))
        .withColumn("std_hourly_edits", F.coalesce(F.col("std_hourly_edits"), F.lit(0.15)))
        .withColumn("p95_hourly_edits", F.coalesce(F.col("p95_hourly_edits"), F.lit(0.0)))
        .withColumn(
            "activity_tier",
            F.when(F.col("total_baseline_edits") >= 100, F.lit("hyperactive"))
            .when(F.col("total_baseline_edits") >= 20, F.lit("active"))
            .when(F.col("total_baseline_edits") >= 5, F.lit("moderate"))
            .otherwise(F.lit("quiet")),
        )
        .withColumn("baseline_start_date", F.lit(start_dt).cast("date"))
        .withColumn("baseline_end_date", F.lit(end_dt).cast("date"))
        .withColumn("days_observed", F.lit(observed_source_days).cast("int"))
        .withColumn("last_available_date", F.lit(source_last_date).cast("date"))
        .withColumn("has_missing_data", F.lit(source_has_missing_days).cast("boolean"))
        .withColumn(
            "pageview_missing_flag",
            F.when(F.col("avg_daily_pageviews").isNull() | (F.col("pv_incomplete") == "true"), F.lit(True)).otherwise(
                F.lit(False)
            ),
        )
    )

    final_cols = [f.name for f in HISTORICAL_BASELINE_SCHEMA.fields]
    baselines_df = joined.select(*[F.col(c).cast(f.dataType).alias(c) for c, f in zip(final_cols, HISTORICAL_BASELINE_SCHEMA.fields)])

    out_path = CURATED_TABLES["page_baselines"]
    safe_write_parquet(baselines_df, out_path, repartition_num=8)

    # Verify & print summary
    saved_df = safe_read_parquet(spark, out_path)
    total_pages = saved_df.count()
    tier_counts = {
        r["activity_tier"]: r["count"]
        for r in saved_df.groupBy("activity_tier").count().collect()
    }
    pv_enriched_count = saved_df.filter(F.col("avg_daily_pageviews").isNotNull()).count()

    print(f"\n  ✓ Built historical baselines for {total_pages:,} distinct pages in {time.time() - t0:.1f}s")
    print(f"  • Activity Tiers           : {tier_counts}")
    print(f"  • Pages with Pageview Data : {pv_enriched_count:,}")
    print("\n  Top 10 Most Active Mainspace Articles in Baseline Window:")
    (
        saved_df.filter("ns = 0")
        .select(
            "title",
            "activity_tier",
            "total_baseline_edits",
            "avg_daily_edits",
            "std_daily_edits",
            "baseline_revert_ratio",
            "avg_daily_pageviews",
            "last_available_date",
        )
        .orderBy(F.desc("total_baseline_edits"))
        .show(10, truncate=False)
    )

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

