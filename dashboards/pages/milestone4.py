"""Milestone 4: Historical Ingestion, Per-Page Baselines, Churn Rankings & Engine Benchmarks."""

from dash import html

from dashboards import charts
from dashboards.components import graph, heading, metric, notice, number, panel, pill_row, table


def render(data):
    ing = data.get("ingestion_summary", {})
    m4 = data.get("m4_benchmarks", {})
    audit = m4.get("manual_churn_audit", {})
    manual_totals = audit.get("manual_totals", {})
    rdd = m4.get("rdd_vs_dataframe_benchmark", {})
    lat = m4.get("query_latency_benchmark", {})
    rankings = data.get("rankings", [])
    baselines = data.get("baseline", [])

    curated_events = ing.get("curated_events_written") or ing.get("curated_deduplicated_events")
    acceptance = ing.get("acceptance_rate_pct")
    if acceptance is None and curated_events:
        acceptance = 100.0
    speedup = rdd.get("speedup_factor_df_over_rdd")
    parquet_sec = lat.get("stored_parquet_rankings_top20_sec")

    pills = []
    if audit:
        pills.append(("Sample file", audit.get("sample_file", "—")))
        pills.append(("Sample size", f"{audit.get('sample_size', 0):,}"))
        pills.append(("Spark exact match", "Yes" if audit.get("spark_exact_match") else "No"))
    if lat:
        pills.append(("Under 2s SLA", "Pass" if lat.get("meets_under_2s_target") else "Fail"))

    audit_rows = [manual_totals] if manual_totals else []

    baseline_columns = [
        ("Article", "title"),
        ("Activity tier", "activity_tier"),
        ("Mean daily edits", "avg_daily_edits"),
        ("Std daily edits", "std_daily_edits"),
        ("Revert ratio", "baseline_revert_ratio"),
        ("Mean daily views", "avg_daily_pageviews"),
        ("Last source date", "last_available_date"),
        ("Last pageview date", "last_pageview_date"),
    ]
    if baselines and "baseline_title" in baselines[0]:
        baseline_columns = [
            ("Article", "baseline_title"),
            ("Mean human edits / 5m", "avg_edits_human"),
            ("Std dev", "std_edits_human"),
            ("Windows observed", "baseline_windows"),
        ]

    return [
        heading(
            "MILESTONE 4 / BASELINES & RANKINGS",
            "Historical curation, baselines & churn.",
            "Curated Parquet lake ingestion, per-page rolling baselines with Analytics pageviews, churn/bot rankings, and RDD vs DataFrame benchmarks.",
        ),
        html.Div([
            metric("CURATED EVENTS", number(curated_events),
                   f"{acceptance:.1f}% valid non-categorize accepted" if acceptance is not None else "Run ingest_curate.py", True),
            metric("BASELINE PROFILES", number(len(baselines) if baselines else None),
                   "Sampled baseline rows displayed"),
            metric("DF VS RDD SPEEDUP", f"{speedup:.2f}×" if speedup is not None else "—",
                   f"DF {rdd.get('dataframe_runtime_sec', '—')}s vs RDD {rdd.get('rdd_runtime_sec', '—')}s"),
            metric("PARQUET QUERY TIME", f"{parquet_sec:.3f}s" if parquet_sec is not None else "—",
                   f"vs {lat.get('raw_jsonl_gz_1day_top20_sec', '—')}s raw JSONL.gz"),
        ], className="metric-grid"),
        *([pill_row(pills)] if pills else []),
        html.Div([
            panel("RDD vs DataFrame & JSONL.gz vs Parquet",
                  "Milestone 4 engine speedup and sub-2-second stored Parquet query SLA",
                  graph(charts.m4_engine_and_latency(m4))),
            panel("Knowledge churn & rewriting leaders",
                  "Top 10 articles by gross byte churn and rewriting volume",
                  graph(charts.churn_rankings(rankings))),
        ], className="chart-grid"),
        panel("Manual churn audit verification",
              "Deterministic pure-Python calculation vs Spark aggregation",
              table(audit_rows, [
                  ("Edits", "edits"), ("Human", "human_edits"), ("Bot", "bot_edits"),
                  ("Added (B)", "bytes_added"), ("Deleted (B)", "bytes_deleted"),
                  ("Net (B)", "net_bytes"), ("Gross churn (B)", "gross_churn_bytes"),
                  ("Rewriting (B)", "rewriting_bytes"),
              ], aria_label="Manual churn audit")
              if audit_rows else notice("No manual audit recorded", "Run rankings_benchmark.py to verify Spark vs Python churn totals.")),
        panel("Historical churn & contributor leaderboard",
              "Top 50 articles in the selected date range",
              table(rankings, [
                  ("Article", "title"), ("Edits", "edits"), ("Human", "human"), ("Bot", "bots"),
                  ("Gross churn (B)", "gross_bytes"), ("Rewriting (B)", "rewriting_bytes"),
                  ("Net change (B)", "net_bytes"), ("Peak daily editors", "peak_editors"),
              ], article_links=True, aria_label="Milestone 4 churn leaderboard")),
        panel("Page baseline profiles",
              "Per-page historical baselines joined with Wikimedia Analytics pageviews",
              table(baselines, baseline_columns, article_links=True, aria_label="Milestone 4 baseline profiles")),
    ]

