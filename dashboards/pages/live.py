"""Live activity radar and proposal alert feed."""

from dash import html

from dashboards import charts
from dashboards.components import graph, heading, metric_row, notice, panel, table


def render(data):
    metrics = data.get("metrics", {})
    alerts = data.get("alerts", [])
    return [
        heading("OBSERVE / LIVE PULSE", "Wikipedia, in motion.",
                "Follow editing activity, contributor patterns, and the pages changing right now."),
        metric_row(metrics),
        html.Div([
            panel("The editing pulse", "Human and bot edits by UTC event-time window",
                  graph(charts.activity_timeline(data.get("timeline", [])))),
            panel("Who's contributing?", "Human versus automated edits",
                  graph(charts.bot_split(metrics))),
        ], className="chart-grid wide-left"),
        html.Div([
            panel("Articles in focus", "Top 10 articles by edits · click an article to investigate",
                  graph(charts.ranking(data.get("rankings", [])), "page-ranking")),
            panel("Knowledge churn", "Gross byte movement and net content change",
                  graph(charts.bytes_timeline(data.get("timeline", [])))),
        ], className="chart-grid"),
        panel("Knowledge-shift feed", "Finalized alerts for the selected duration · recent 200 records · independent of live preview",
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
