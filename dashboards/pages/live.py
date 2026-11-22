"""Live activity radar and proposal alert feed."""

from dash import html

from dashboards import charts
from dashboards.components import graph, heading, metric_row, notice, panel, pill_row, table


def _alert_detector_summary(alerts, m5_summary):
    if alerts:
        type_counts = {}
        for a in alerts:
            atype = a.get("alert_type") or "UNKNOWN"
            type_counts[atype] = type_counts.get(atype, 0) + 1
        summary = {
            "detector_a_fixed_count": sum(1 for a in alerts if a.get("fixed_threshold_flag")),
            "detector_b_historical_count": sum(1 for a in alerts if a.get("historical_alert_flag") or (a.get("historical_score") or 0) >= 50),
            "detector_c_spark_ml_count": sum(1 for a in alerts if a.get("spark_ml_alert_flag") or (a.get("spark_ml_score") or 0) >= 0.5),
            "all_three_consensus_count": sum(
                1 for a in alerts
                if a.get("fixed_threshold_flag")
                and (a.get("historical_alert_flag") or (a.get("historical_score") or 0) >= 50)
                and (a.get("spark_ml_alert_flag") or (a.get("spark_ml_score") or 0) >= 0.5)
            ),
            "pageview_corroborated_count": sum(1 for a in alerts if a.get("pageview_corroborated")),
        }
        return summary, type_counts, "Active window alerts (Fixed vs Historical vs Spark ML)"
    if m5_summary:
        return m5_summary, m5_summary.get("alert_type_breakdown", {}), "Corpus-wide Milestone 5 benchmark (no alerts in current live slice)"
    return {}, {}, "Detector comparison populates when alerts are produced"


def render(data):
    metrics = data.get("metrics", {})
    alerts = data.get("alerts", [])
    time_range = data.get("range")
    scope = data.get("scope", "selected India Standard Time event-time range")
    bucket = data.get("bucket", "5 minutes")
    detector_summary, type_counts, detector_subtitle = _alert_detector_summary(alerts, data.get("m5_summary"))
    type_pills = pill_row([(k.replace("_", " ").title(), f"{v:,}") for k, v in type_counts.items()]) if type_counts else None
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
        panel("Three-detector comparison & classification", detector_subtitle,
              html.Div([
                  *([type_pills] if type_pills else []),
                  graph(charts.detector_comparison(detector_summary)),
              ])),
        panel("Knowledge-shift feed", alert_description,
              table(alerts, [("Article", "title"), ("Type", "alert_type"),
                             ("Historical score", "historical_score"), ("Spark ML", "spark_ml_score"),
                             ("Fixed", "fixed_threshold_flag"), ("Pageviews", "pageview_corroborated"),
                             ("Confidence", "confidence"), ("Reasons", "reasons")], True,
                    aria_label="Knowledge-shift alert feed")
              if alerts else notice("No finalized alerts yet",
                  "Alerts appear after windows finalize. A page without history can produce a low-confidence activity candidate.")),
        panel("Activity ledger", "Top 100 observed articles · totals above include the full selected scope",
              table(data.get("rankings", []), [("Article", "title"), ("Edits", "edits"),
                    ("Human", "human"), ("Bot", "bots"), ("Gross bytes", "gross_bytes"),
                    ("Net bytes", "net_bytes"), ("Revert actions", "reverts"),
                    ("Peak window editors ≈", "peak_window_editors")], True,
                    aria_label="Activity ledger")),
    ]
