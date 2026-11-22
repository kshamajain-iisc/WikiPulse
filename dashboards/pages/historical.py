"""Historical trends with explicit source coverage, anomaly density, baseline tiers, and contextual leaderboard."""

from datetime import date

from dash import html

from dashboards import charts
from dashboards.components import graph, heading, metric_row, notice, panel, pill_row, table


def render(data):
    metrics = data.get("metrics", {})
    first, last = metrics.get("first_day"), metrics.get("last_day")
    missing_sources = data.get("missing_sources", [])
    ing = data.get("ingestion_summary", {})
    m4 = data.get("m4_benchmarks", {})
    m5 = data.get("m5_summary", {})
    m6_geo = data.get("m6_geospatial", {})
    daily_alerts = data.get("daily_alerts", [])
    baseline_tiers = data.get("baseline_tiers", [])

    coverage = "No canonical historical dataset is available for this date range."
    if missing_sources:
        coverage = (
            "Historical summaries are missing: " + ", ".join(missing_sources)
            + ". Build them with the historical setup steps in DATA_SETUP.md."
        )
    elif first and last:
        expected = (date.fromisoformat(last) - date.fromisoformat(first)).days + 1
        observed = metrics.get("observed_days", 0)
        coverage = f"Source range {first} → {last} · {observed} observed dates / {expected} calendar dates."
        if observed < expected:
            coverage += " Missing dates are not interpreted as zero activity."

    hist_pills = []
    curated_events = ing.get("curated_events_written") or ing.get("curated_deduplicated_events")
    if curated_events:
        hist_pills.append(("Curated events", f"{curated_events:,}"))
    audit = m4.get("manual_churn_audit", {})
    if audit:
        hist_pills.append(("Manual Python/Spark audit", "Exact match" if audit.get("spark_exact_match") else "—"))
    rdd = m4.get("rdd_vs_dataframe_benchmark", {})
    if rdd.get("speedup_factor_df_over_rdd"):
        hist_pills.append(("DF vs RDD speedup", f"{rdd['speedup_factor_df_over_rdd']}×"))
    lat = m4.get("query_latency_benchmark", {})
    if lat.get("stored_parquet_rankings_top20_sec") is not None:
        hist_pills.append(("Parquet query SLA", f"{lat['stored_parquet_rankings_top20_sec']:.3f}s (<2s Pass)"))
    if m5.get("total_alerts_union"):
        hist_pills.append(("Historical alerts detected", f"{m5['total_alerts_union']:,}"))
    if m6_geo.get("located_coverage_pct"):
        hist_pills.append(("Subject geo coverage", f"{m6_geo['located_coverage_pct']}%"))

    baseline_rows = data.get("baseline", [])
    baseline_columns = [
        ("Article", "title"),
        ("Activity tier", "activity_tier"),
        ("Mean daily edits", "avg_daily_edits"),
        ("Std daily edits", "std_daily_edits"),
        ("Revert ratio", "baseline_revert_ratio"),
        ("Mean daily views", "avg_daily_pageviews"),
        ("Last source date", "last_available_date"),
        ("Last pageview date", "last_pageview_date"),
        ("Missing data", "has_missing_data"),
        ("Views missing", "pageview_missing_flag"),
    ]
    if baseline_rows and "baseline_title" in baseline_rows[0]:
        baseline_columns = [
            ("Article", "baseline_title"),
            ("Mean human edits / 5m", "avg_edits_human"),
            ("Standard deviation", "std_edits_human"),
            ("Windows observed", "baseline_windows"),
            ("Training end (exclusive)", "baseline_end_exclusive"),
        ]

    rankings = data.get("rankings", [])
    ranking_columns = [
        ("Article", "title"),
        ("Edits", "edits"),
        ("Human", "human"),
        ("Bot", "bots"),
        ("Gross churn (B)", "gross_bytes"),
        ("Rewriting (B)", "rewriting_bytes"),
        ("Net change (B)", "net_bytes"),
        ("Peak daily editors", "peak_editors"),
    ]
    if rankings and any(r.get("detected_shifts") is not None for r in rankings):
        ranking_columns.extend([
            ("Detected shifts", "detected_shifts"),
            ("Peak anomaly score", "peak_anomaly_score"),
            ("Peak ML score", "peak_ml_score"),
            ("Subject location", "subject_location"),
        ])

    return [
        heading(
            "UNDERSTAND / HISTORICAL INTELLIGENCE",
            "Put today's activity in context.",
            "Explore long-term editing patterns, per-page baseline tiers, historical anomaly density, and context-enriched churn rankings.",
        ),
        notice("Historical coverage", coverage),
        metric_row(metrics, historical=True),
        *([pill_row(hist_pills)] if hist_pills else []),
        html.Div([
            panel(
                "Daily editing rhythm",
                "Available UTC dates; gaps are not zero-filled",
                graph(charts.activity_timeline(data.get("timeline", []), historical=True)),
            ),
            panel(
                "Most active articles",
                "Top articles in the selected date range",
                graph(charts.ranking(rankings)),
            ),
        ], className="chart-grid"),
        html.Div([
            panel(
                "Content growth and rewriting",
                "Absolute byte movement versus net change by day",
                graph(charts.bytes_timeline(data.get("timeline", []), historical=True)),
            ),
            panel(
                "Knowledge churn & rewriting leaders",
                "Top 10 articles by gross churn and rewriting volume",
                graph(charts.churn_rankings(rankings)),
            ),
        ], className="chart-grid"),
        html.Div([
            panel(
                "Historical anomaly timeline & shift density",
                "Daily detected knowledge shifts by classification (Confirmed News, Breaking Candidate, Edit War, Major Rewrite)",
                graph(charts.historical_alerts_timeline(daily_alerts)),
            ),
            panel(
                "Historical baseline activity tier distribution",
                "Per-page rolling baseline profiles categorized into quiet, moderate, active, and hyperactive tiers",
                graph(charts.baseline_tier_distribution(baseline_tiers)),
            ),
        ], className="chart-grid"),
        panel(
            "Historical churn, anomaly & contextual leaderboard",
            "Top 50 articles enriched with detected knowledge shifts, peak anomaly/ML scores, and resolved subject geography",
            table(rankings, ranking_columns, article_links=True, aria_label="Historical churn leaderboard"),
        ),
        *([
            panel(
                "Baseline activity tier summary",
                "Aggregated daily edit velocity, revert rate, and bot share across historical baseline tiers",
                table(baseline_tiers, [
                    ("Activity tier", "activity_tier"),
                    ("Articles", "pages_count"),
                    ("Mean daily edits", "mean_daily_edits"),
                    ("Mean revert rate (%)", "mean_revert_pct"),
                    ("Mean bot share (%)", "mean_bot_pct"),
                ], aria_label="Baseline activity tier summary"),
            )
        ] if baseline_tiers else []),
        panel(
            "Page baseline profiles",
            "Per-page baselines with namespace priors & Analytics pageviews",
            table(baseline_rows, baseline_columns, article_links=True, aria_label="Page baseline profiles"),
        ),
        notice(
            "Pageview confirmation",
            "Daily pageview baselines from Repo/data/analytics/ are joined per article with explicit last_pageview_date and pageview_missing_flag indicators.",
        ),
    ]
