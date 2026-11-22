"""Article activity and the supporting evidence available for that article."""

from urllib.parse import quote

from dash import html

from dashboards import charts
from dashboards.components import graph, heading, metric_row, notice, panel, table


def render(data):
    title = data.get("selected")
    time_range = data.get("range")
    scope = data.get("scope", "selected India Standard Time event-time range")
    description = f"Choose an article, click a ranking, or open an article from the activity ledger. Current activity covers the {scope}."
    evidence_description = (
        f"Finalized alerts overlapping the selected {scope}; charts use current live events."
        if data.get("view") == "live"
        else "Finalized alert windows within the selected event-time range."
    )
    return [
        heading("INVESTIGATE / ARTICLE DETAIL", title or "Follow the evidence.",
                description),
        html.A("Open on Wikipedia ↗", href="https://en.wikipedia.org/wiki/" + quote(title or "Main_Page"),
               target="_blank", rel="noopener noreferrer", className="article-link"),
        metric_row(data.get("metrics", {})),
        panel("Knowledge-shift evidence", evidence_description,
              table(data.get("alerts", []), [
                  ("Window (IST)", "window_start_ist"), ("Type", "alert_type"),
                  ("Hist / 100", "historical_score"), ("Spark ML", "spark_ml_score"),
                  ("Fixed", "fixed_threshold_flag"), ("Pageviews", "pageview_corroborated"),
                  ("Confidence", "confidence"), ("Edits / 35", "activity_points"),
                  ("Editors / 25", "editor_points"), ("Content / 20", "content_points"),
                  ("Reverts / 10", "revert_points"), ("Pageviews / 10", "pageview_points"),
                  ("Available weight", "available_weight"),
                  ("Missing evidence", "missing_evidence"), ("Reasons", "reasons"),
              ], aria_label="Knowledge-shift evidence")),
        html.Div([
            panel("Activity over time", f"Selected article · {data.get('bucket', '5 minutes')} IST buckets",
                  graph(charts.activity_timeline(data.get("timeline", []), time_range=time_range))),
            panel("Current versus normal", "Matching historical windows with μ + 2σ baseline band",
                  graph(charts.baseline_comparison(data.get("comparisons", []), time_range=time_range))),
        ], className="chart-grid"),
        html.Div([
            panel("Content movement", "Known byte sizes · selected IST range",
                  graph(charts.bytes_timeline(data.get("timeline", []), time_range=time_range))),
            panel("Contributor balance", "Observed human and bot edit share",
                  graph(charts.bot_split(data.get("metrics", {})))),
        ], className="chart-grid"),
        panel("Edit-risk and reversion evidence", "Lift Wing ML & heuristic revert risk vs actual revert outcomes",
              table(data.get("risks", []), [
                  ("Revision", "revid"), ("User", "user"), ("Anon", "is_anon"),
                  ("Delta (B)", "delta"), ("ML Risk", "ml_revert_risk"),
                  ("Heuristic", "heuristic_risk"), ("Composite", "composite_risk"),
                  ("Reverted", "is_reverted_actual"), ("Time to revert (s)", "time_to_revert_sec"),
                  ("API status", "api_status"),
              ], aria_label="Edit-risk and reversion evidence")
              if data.get("risks") else notice("No revision-risk evidence", "The scoring/enrichment producer has not supplied risk records for this article.")),
        panel("Revision changes", "MediaWiki compare diffs; external diff HTML is never executed",
              table(data.get("diffs", []), [
                  ("Revision", "revid"), ("User", "user"), ("Delta (B)", "bytes_delta"),
                  ("Added lines", "added_lines_count"), ("Removed lines", "removed_lines_count"),
                  ("Status", "compare_status"), ("Comment", "comment"), ("Text snippet", "diff_snippet"),
              ], aria_label="Revision changes")
              if data.get("diffs") else notice("No revision comparisons", "Revision-difference records will appear here when enrichment is connected.")),
    ]
