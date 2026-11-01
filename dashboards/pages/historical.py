"""Historical trends with explicit source coverage rather than fabricated zeros."""

from datetime import date

from dash import html

from dashboards import charts
from dashboards.components import graph, heading, metric_row, notice, panel, table


def render(data):
    metrics = data.get("metrics", {})
    first, last = metrics.get("first_day"), metrics.get("last_day")
    coverage = "No canonical historical dataset is available for this date range."
    if first and last:
        expected = (date.fromisoformat(last) - date.fromisoformat(first)).days + 1
        observed = metrics.get("observed_days", 0)
        coverage = f"Source range {first} → {last} · {observed} observed dates / {expected} calendar dates."
        if observed < expected:
            coverage += " Missing dates are not interpreted as zero activity."
    baseline_rows = data.get("baseline", [])
    baseline_columns = [("Article", "title"), ("Activity tier", "activity_tier"),
                        ("Mean daily edits", "avg_daily_edits"), ("Last source date", "last_available_date"),
                        ("Missing data", "has_missing_data")]
    if baseline_rows and "baseline_title" in baseline_rows[0]:
        baseline_columns = [("Article", "baseline_title"), ("Mean human edits / 5m", "avg_edits_human"),
                            ("Standard deviation", "std_edits_human"), ("Windows observed", "baseline_windows"),
                            ("Training end (exclusive)", "baseline_end_exclusive")]
    return [
        heading("UNDERSTAND / HISTORICAL INTELLIGENCE", "Put today's activity in context.",
                "Explore long-term editing patterns and compare articles against their own history."),
        notice("Historical coverage", coverage),
        metric_row(metrics, historical=True),
        html.Div([
            panel("Daily editing rhythm", "Available UTC dates; gaps are not zero-filled",
                  graph(charts.activity_timeline(data.get("timeline", []), historical=True))),
            panel("Most active articles", "Top articles in the selected date range",
                  graph(charts.ranking(data.get("rankings", [])))),
        ], className="chart-grid"),
        panel("Content growth and rewriting", "Absolute byte movement versus net change by day",
              graph(charts.bytes_timeline(data.get("timeline", []), historical=True))),
        panel("Page baseline profiles", "Optional milestone baseline table",
              table(baseline_rows, baseline_columns)),
        notice("Pageview confirmation", "Pageviews are delayed supporting evidence. No view counts or correlations are shown until a normalized pageview source is connected."),
    ]
