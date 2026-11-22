"""Milestone 6: Contextual Enrichment (Graph, Diffs, Geospatial Map & Lift Wing Revert Risk)."""

from dash import html

from dashboards import charts
from dashboards.components import graph, heading, metric, notice, number, panel, pill_row, table


def render(data):
    edges = data.get("edges", [])
    points = data.get("points", [])
    diffs = data.get("diffs", [])
    risks = data.get("risks", [])
    m6_graph = data.get("m6_graph_diffs", {})
    m6_geo = data.get("m6_geospatial", {})
    m6_risk = data.get("m6_revert_risk", {})

    g_enr = m6_graph.get("graph_enrichment", {})
    d_enr = m6_graph.get("revision_diff_enrichment", {})
    r_cov = m6_risk.get("coverage_metrics", {})
    r_pre = m6_risk.get("prefilter_efficiency", {})
    curve = m6_risk.get("experiment_5_threshold_calibration", {}).get("liftwing_ml_curve", [])
    countries = m6_geo.get("top_countries_or_regions", [])

    pills = [
        ("Alerted pages enriched", g_enr.get("alerted_pages_enriched", "—")),
        ("Co-edited spillover edges", g_enr.get("co_edited_spillover_edges", "—")),
        ("Live compare API success", f"{d_enr.get('live_compare_api_success_pct', '—')}%"),
        ("Editor IP geolocation", "Never used" if m6_geo.get("editor_ip_geolocation_used") is False else "—"),
        ("Unlocated alerts preserved", "Yes" if m6_geo.get("unlocated_alerts_preserved_valid") else "—"),
        ("Lift Wing prefilter reduction", f"{r_pre.get('prefilter_reduction_pct', '—')}%"),
    ]

    return [
        heading(
            "MILESTONE 6 / CONTEXTUAL ENRICHMENT & RISK",
            "Graph spillover, subject map & Lift Wing risk.",
            "End-to-end contextual investigation combining link/backlink co-edit spillover, MediaWiki revision diffs, subject geography, and Lift Wing Revert Risk calibration.",
        ),
        html.Div([
            metric("GRAPH EDGES", number(g_enr.get("total_graph_edges") or len(edges)),
                   f"{g_enr.get('co_edited_spillover_edges', 0)} co-edited spillover edges", True),
            metric("USABLE REVISION DIFFS", f"{d_enr.get('usable_change_data_pct', 0):.1f}%" if d_enr else "—",
                   f"{d_enr.get('total_revisions_compared', len(diffs))} revisions compared (≥90% target)"),
            metric("SUBJECT GEO COVERAGE", f"{m6_geo.get('located_coverage_pct', 0):.1f}%" if m6_geo else "—",
                   f"{m6_geo.get('located_subjects_count', 0)} located · {m6_geo.get('unlocated_subjects_count', 0)} unlocated valid"),
            metric("REVERT RISK COVERAGE", f"{r_cov.get('valid_probability_or_fallback_pct', 0):.1f}%" if r_cov else "—",
                   f"{r_cov.get('liftwing_ml_direct_count', 0)}/{r_cov.get('total_revisions_scored', len(risks))} direct Lift Wing ML"),
        ], className="metric-grid"),
        pill_row(pills),
        html.Div([
            panel("Related-page & co-edit spillover network",
                  "Amber edges indicate neighbors co-edited in the same alert window",
                  graph(charts.relationship_graph(edges))),
            panel("Global article-subject map",
                  "Wikipedia coordinates + Wikidata P625/P276/P17 (no editor IP geolocation)",
                  graph(charts.subject_map(points))),
        ], className="chart-grid"),
        html.Div([
            panel("Lift Wing Revert Risk threshold calibration",
                  "Precision, Recall & F1 across thresholds 0.20–0.90 vs actual later reverts",
                  graph(charts.revert_risk_calibration(curve))),
            panel("Top countries & regions by alerted topics",
                  "Subject geography aggregated from curated map_points",
                  graph(charts.geo_country_breakdown(countries))),
        ], className="chart-grid"),
        panel("Threshold calibration table (Lift Wing ML)",
              "TP / FP / FN / TN and Precision / Recall / F1 across candidate thresholds",
              table(curve, [
                  ("Threshold", "threshold"), ("TP", "tp"), ("FP", "fp"), ("FN", "fn"), ("TN", "tn"),
                  ("Precision (%)", "precision_pct"), ("Recall (%)", "recall_pct"),
                  ("F1 (%)", "f1_pct"), ("FPR (%)", "false_positive_rate_pct"),
              ], aria_label="Lift Wing calibration table")
              if curve else notice("No calibration curve", "Run revert_risk_monitor.py to populate threshold calibration.")),
        panel("Co-edit spillover & link evidence",
              "Directional outlinks/backlinks and same-window co-edit counts",
              table(edges, [
                  ("Source article", "source_title"), ("Target article", "target_title"),
                  ("Relationship", "edge_type"), ("Co-edited", "co_edited_in_window"),
                  ("Target window edits", "target_edits_in_window"), ("Snapshot", "snapshot_ts"),
              ], aria_label="Milestone 6 link evidence")),
        panel("Scored revisions & revert outcomes",
              "Lift Wing ML probabilities vs heuristic risk and actual revert outcomes",
              table(risks, [
                  ("Article", "title"), ("Revision", "revid"), ("User", "user"),
                  ("Delta (B)", "delta"), ("ML Risk", "ml_revert_risk"),
                  ("Heuristic", "heuristic_risk"), ("Composite", "composite_risk"),
                  ("Reverted", "is_reverted_actual"), ("API status", "api_status"),
              ], article_links=True, aria_label="Milestone 6 revert risk sample")),
    ]

