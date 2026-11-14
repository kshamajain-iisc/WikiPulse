"""Historical trends with explicit source coverage rather than fabricated zeros."""

from datetime import date

from dash import html

from dashboards import charts
from dashboards.components import graph, heading, metric_row, notice, panel, table


def render(data):
    metrics = data.get("metrics", {})
    first, last = metrics.get("first_day"), metrics.get("last_day")
    missing_sources = data.get("missing_sources", [])
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
        baseline_columns = [("Article", "baseline_title"), ("Mean human edits / 5m", "avg_edits_human"),
                            ("Standard deviation", "std_edits_human"), ("Windows observed", "baseline_windows"),
                            ("Training end (exclusive)", "baseline_end_exclusive")]
    rankings = data.get("rankings", [])
    return [
        heading("UNDERSTAND / HISTORICAL INTELLIGENCE", "Put today's activity in context.",
                "Explore long-term editing patterns and compare articles against their own history."),
        notice("Historical coverage", coverage),
        metric_row(metrics, historical=True),
        html.Div([
            panel("Daily editing rhythm", "Available UTC dates; gaps are not zero-filled",
                  graph(charts.activity_timeline(data.get("timeline", []), historical=True))),
            panel("Most active articles", "Top articles in the selected date range",
                  graph(charts.ranking(rankings))),
        ], className="chart-grid"),
        html.Div([
            panel("Content growth and rewriting", "Absolute byte movement versus net change by day",
                  graph(charts.bytes_timeline(data.get("timeline", []), historical=True))),
            panel("Knowledge churn & rewriting leaders", "Top 10 articles by gross churn and rewriting volume",
                  graph(charts.churn_rankings(rankings))),
        ], className="chart-grid"),
        panel("Historical churn & contributor leaderboard", "Top 50 articles in the selected date range",
              table(rankings, [
                  ("Article", "title"), ("Edits", "edits"), ("Human", "human"), ("Bot", "bots"),
                  ("Gross churn (B)", "gross_bytes"), ("Rewriting (B)", "rewriting_bytes"),
                  ("Net change (B)", "net_bytes"), ("Peak daily editors", "peak_editors"),
              ], article_links=True, aria_label="Historical churn leaderboard")),
        panel("Page baseline profiles", "Per-page baselines with namespace priors & Analytics pageviews",
              table(baseline_rows, baseline_columns, article_links=True, aria_label="Page baseline profiles")),
        notice("Pageview confirmation", "Daily pageview baselines from Repo/data/analytics/ are joined per article with explicit last_pageview_date and pageview_missing_flag indicators."),
    ]
