"""Live activity radar and proposal alert feed."""

from dash import html

from dashboards import charts
from dashboards.components import graph, heading, metric_row, notice, panel, table


def render(data):
    metrics = data.get("metrics", {})
    alerts = data.get("alerts", [])
    time_range = data.get("range")
    scope = data.get("scope", "selected India Standard Time event-time range")
    bucket = data.get("bucket", "5 minutes")
    if data.get("view") == "live":
        description = f"Live event data · {scope}. Charts use {bucket} buckets and are clipped to the selected range."
        alert_description = (
            f"Finalized alert windows overlapping the selected {scope}; charts use current live events."
        )
    else:
        description = f"{data.get('view', 'finalized').title()} activity · {scope}. Source: {data.get('data_source', 'stream windows')}."
        alert_description = "Finalized alerts within the selected event-time range."
    return [
        heading("OBSERVE / LIVE PULSE", "Wikipedia, in motion.",
                description),
        metric_row(metrics),
        html.Div([
            panel("The editing pulse", "Human and bot edits by India Standard Time event-time window",
                  graph(charts.activity_timeline(data.get("timeline", []), time_range=time_range))),
            panel("Who's contributing?", "Human versus automated edits",
                  graph(charts.bot_split(metrics))),
        ], className="chart-grid wide-left"),
        html.Div([
            panel("Articles in focus", "Top 10 articles by edits · click an article to investigate",
                  graph(charts.ranking(data.get("rankings", [])), "page-ranking")),
            panel("Knowledge churn", "Gross byte movement and net content change",
                  graph(charts.bytes_timeline(data.get("timeline", []), time_range=time_range))),
        ], className="chart-grid"),
        panel("Knowledge-shift feed", alert_description,
              table(alerts, [("Article", "title"), ("Type", "alert_type"),
                             ("Historical score", "historical_score"), ("Confidence", "confidence"),
                             ("Reasons", "reasons")], True)
              if alerts else notice("No finalized alerts yet",
                  "Alerts appear after windows finalize. A page without history can produce a low-confidence activity candidate.")),
        panel("Activity ledger", "Top 100 observed articles · totals above include the full selected scope",
              table(data.get("rankings", []), [("Article", "title"), ("Edits", "edits"),
                    ("Human", "human"), ("Bot", "bots"), ("Gross bytes", "gross_bytes"),
                    ("Net bytes", "net_bytes"), ("Revert actions", "reverts"),
                    ("Peak window editors ≈", "peak_window_editors")], True)),
    ]
