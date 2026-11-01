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
        panel("Knowledge-shift feed", "Scored alerts from the detection pipeline",
              table(alerts, [("Article", "title"), ("Type", "alert_type"),
                             ("Historical score", "historical_score"), ("Reasons", "reasons")], True)
              if alerts else notice("Detection pipeline not connected yet",
                  "These are activity measurements. No alert score or confirmed-news label is inferred from edit counts.")),
        panel("Activity ledger", "Top 100 observed articles · totals above include the full selected scope",
              table(data.get("rankings", []), [("Article", "title"), ("Edits", "edits"),
                    ("Human", "human"), ("Bot", "bots"), ("Gross bytes", "gross_bytes"),
                    ("Net bytes", "net_bytes"), ("Revert actions", "reverts"),
                    ("Peak window editors ≈", "peak_window_editors")], True)),
    ]
