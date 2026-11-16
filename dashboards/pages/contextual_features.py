"""Dedicated UI Page 3: Contextual Features (Topic Graph, Subject Geography, Revision Diffs & Lift Wing Revert Risk)."""

from dash import html

from dashboards import charts
from dashboards import contextual_charts as ctx
from dashboards.components import graph, heading, metric, notice, number, panel, pill_row, table


def render(data):
    edges = data.get("edges", [])
    points = data.get("points", [])
    diffs = data.get("diffs", [])
    risks = data.get("risks", [])
    m6_graph = data.get("m6_graph_diffs", {})
    m6_geo = data.get("m6_geospatial", {})
    m6_risk = data.get("m6_revert_risk", {})
    case_demo = data.get("case_studies_demo", {})

    g_enr = m6_graph.get("graph_enrichment", {})
    d_enr = m6_graph.get("revision_diff_enrichment", {})
    r_cov = m6_risk.get("coverage_metrics", {})
    r_pre = m6_risk.get("prefilter_efficiency", {})
    exp5 = m6_risk.get("experiment_5_threshold_calibration", {})
    curve = exp5.get("liftwing_ml_curve", [])
    comp_curve = exp5.get("composite_risk_curve", [])
    cal_finding = exp5.get("calibration_finding")
    mttr = m6_risk.get("mttr_breakdown", {})
    countries = m6_geo.get("top_countries_or_regions", [])
    loc_src = m6_geo.get("location_source_breakdown", {})
    loc_qual = m6_geo.get("location_quality_breakdown", {})
    rev_highlights = case_demo.get("liftwing_reverted_revision_highlights", [])

    pills = [
        ("Data source", "ingest_curate.py (Strictly Historical Curated Lake)"),
        ("Alerted pages enriched", g_enr.get("alerted_pages_enriched", "—")),
        ("Co-edited spillover edges", g_enr.get("co_edited_spillover_edges", "—")),
        ("Outlinks / Backlinks", f"{g_enr.get('outlink_edges', '—')} / {g_enr.get('backlink_edges', '—')}"),
        ("Live compare API success", f"{d_enr.get('live_compare_api_success_pct', '—')}%"),
        ("Editor IP geolocation", "Never used" if m6_geo.get("editor_ip_geolocation_used") is False else "—"),
        ("Unlocated alerts preserved", "Yes" if m6_geo.get("unlocated_alerts_preserved_valid") else "—"),
        ("Lift Wing prefilter reduction", f"{r_pre.get('prefilter_reduction_pct', '—')}%"),
    ]
    if loc_src:
        pills.append((
            "Geo sources (Wiki / P625 / P276·P17)",
            f"{loc_src.get('wikipedia_coord', 0)} / {loc_src.get('wikidata_p625', 0)} / {loc_src.get('wikidata_p276_p17', 0)}",
        ))
    if loc_qual:
        pills.append((
            "Geo quality (Exact / Related / Centroid)",
            f"{loc_qual.get('exact_point', 0)} / {loc_qual.get('related_entity_point', 0)} / {loc_qual.get('country_centroid', 0)}",
        ))

    return [
        heading(
            "ENRICH / HISTORICAL CONTEXTUAL FEATURES (INGEST_CURATE)",
            "Topic spillover, subject geography & Lift Wing Revert Risk.",
            "Strictly historical curated enrichments from ingest_curate.py (2026-09-15 – 2026-11-15): investigate co-edited link/backlink spillover networks, MediaWiki revision diffs, subject location provenance, and Lift Wing ML Revert Risk calibration. For interactive regional RecentChanges exploration by [days / windows], open the Geospatial Map page.",
        ),
        html.Div([
            metric(
                "GRAPH & SPILLOVER EDGES",
                number(g_enr.get("total_graph_edges") or len(edges)),
                f"{g_enr.get('co_edited_spillover_edges', 0)} same-window co-edited spillover edges",
                True,
            ),
            metric(
                "USABLE REVISION DIFFS",
                f"{d_enr.get('usable_change_data_pct', 0):.1f}%" if d_enr else "—",
                f"{d_enr.get('total_revisions_compared', len(diffs))} revisions compared (≥90% target)",
            ),
            metric(
                "SUBJECT GEO COVERAGE",
                f"{m6_geo.get('located_coverage_pct', 0):.1f}%" if m6_geo else "—",
                f"{m6_geo.get('located_subjects_count', 0)} located · {m6_geo.get('unlocated_subjects_count', 0)} unlocated valid",
            ),
            metric(
                "REVERT RISK COVERAGE",
                f"{r_cov.get('valid_probability_or_fallback_pct', 0):.1f}%" if r_cov else "—",
                f"{r_cov.get('liftwing_ml_direct_count', 0)}/{r_cov.get('total_revisions_scored', len(risks))} direct Lift Wing ML (≥95% target)",
            ),
        ], className="metric-grid"),
        pill_row(pills),
        *([notice("Empirical Revert-Risk Calibration Finding", cal_finding)] if cal_finding else []),
        html.Div([
            panel(
                "Related-page & co-edit spillover network",
                "Alerted pages are hubs (sized by co-edited neighbours); amber neighbours were co-edited in the "
                "same alert window and are sized by their in-window edits; blue bridges are linked from ≥2 alerts. "
                "Hover any node for edit and editor counts.",
                graph(ctx.spillover_hub_network(edges), show_modebar=True),
            ),
            panel(
                "Spillover share by alerted page",
                f"Co-edited vs link-only neighbours per hub ({len(edges)} of "
                f"{g_enr.get('total_graph_edges', len(edges))} edges loaded); % = share of neighbours that "
                "also changed during the alert",
                graph(ctx.spillover_ratio_bars(edges)),
            ),
        ], className="chart-grid wide-left"),
        html.Div([
            panel(
                "Precision–recall trade-off (Composite vs Lift Wing ML vs Heuristic)",
                "Each point is a threshold τ (labelled); stars mark operating points; dotted arcs are iso-F1 "
                "guides vs actual 48h reverts",
                graph(ctx.revert_risk_pr_tradeoff(m6_risk)),
            ),
            panel(
                "F1 by decision threshold",
                "Stars mark each scorer's peak F1; hover shows precision, recall and false-positive rate at every τ",
                graph(ctx.revert_risk_f1_by_threshold(m6_risk)),
            ),
        ], className="chart-grid"),
        html.Div([
            panel(
                "Where reverted edits sit: Lift Wing ML vs heuristic risk",
                "Each scored revision coloured by its actual 48h outcome (◆ = anonymous editor); the shaded area "
                "is what the calibrated composite rule flags",
                graph(ctx.revert_risk_separation(risks, m6_risk)),
            ),
            panel(
                "Confusion matrices at the operating points",
                "Counts and row-normalised rates: how many real reverts are caught vs how many kept edits are "
                "falsely flagged",
                graph(ctx.revert_risk_confusion(m6_risk)),
            ),
        ], className="chart-grid"),
        html.Div([
            panel(
                "Time-to-revert distribution",
                (f"Cumulative share of reverts done within a given time (log scale); summary: median "
                 f"{(mttr.get('overall') or {}).get('median_sec', '—')}s vs mean "
                 f"{(mttr.get('overall') or {}).get('mean_sec', '—')}s — the mean is dominated by a slow tail")
                if mttr else "Cumulative share of reverts done within a given time (log scale)",
                graph(ctx.time_to_revert_ecdf(risks, mttr) if risks else charts.mttr_comparison(mttr)),
            ),
            panel(
                "Revision diff profile",
                "Lines added vs removed per compared revision (log–log); marker size = external links added, "
                "✕ = compare not successful",
                graph(ctx.revision_diff_profile(diffs)),
            ),
        ], className="chart-grid"),
        html.Div([
            panel(
                "Top countries & regions by alerted topics",
                "Subject geography (never editor IP); bar colour = peak anomaly score, label = human edits",
                graph(ctx.country_topic_bars(countries)),
            ),
            panel(
                "Subject location provenance & precision",
                f"How {m6_geo.get('total_alert_subjects_evaluated', '—')} alerted subjects were located and how "
                "precise each location is; unlocated subjects remain valid alerts",
                graph(ctx.location_provenance_bars(loc_src, loc_qual)),
            ),
        ], className="chart-grid"),
        panel(
            "Empirical threshold calibration table (Lift Wing ML & Composite Risk)",
            "TP / FP / FN / TN and Precision / Recall / F1 across candidate thresholds",
            table(comp_curve or curve, [
                ("Threshold (τ)", "threshold"),
                ("TP", "tp"),
                ("FP", "fp"),
                ("FN", "fn"),
                ("TN", "tn"),
                ("Precision (%)", "precision_pct"),
                ("Recall (%)", "recall_pct"),
                ("F1 (%)", "f1_pct"),
                ("Accuracy (%)", "accuracy_pct"),
                ("FPR (%)", "false_positive_rate_pct"),
            ], aria_label="Revert risk calibration table")
            if (comp_curve or curve) else notice("No calibration curve", "Run revert_risk_monitor.py to populate threshold calibration."),
        ),
        panel(
            "Scored revisions & Lift Wing Revert Risk outcomes",
            "Lift Wing ML probabilities vs low-cost heuristic risk, composite risk, and actual 48h revert outcomes",
            table(risks or rev_highlights, [
                ("Article", "title"),
                ("Revision", "revid"),
                ("User", "user"),
                ("Anon", "is_anon"),
                ("Delta (B)", "delta"),
                ("Lift Wing ML", "ml_revert_risk"),
                ("Heuristic", "heuristic_risk"),
                ("Composite", "composite_risk"),
                ("Reverted (48h)", "is_reverted_actual"),
                ("TTR (s)", "time_to_revert_sec"),
                ("API status", "api_status"),
            ], article_links=True, aria_label="Scored revisions and revert outcomes"),
        ),
        panel(
            "MediaWiki revision diff comparisons (action=compare)",
            "Extracted line additions/removals, byte deltas, and safe text snippets for alerted revisions",
            table(diffs, [
                ("Article", "title"),
                ("From rev", "from_revid"),
                ("To rev", "revid"),
                ("User", "user"),
                ("Delta (B)", "delta_bytes"),
                ("+Lines", "added_lines_count"),
                ("-Lines", "removed_lines_count"),
                ("+Links", "external_links_added"),
                ("Status", "compare_status"),
                ("Edit comment", "comment"),
                ("Diff snippet", "diff_snippet"),
            ], article_links=True, aria_label="MediaWiki revision diff comparisons")
            if diffs else notice("No revision diffs loaded", "Run graph_and_diffs.py to populate curated/revision_diffs."),
        ),
        html.Div([
            panel(
                "Co-edit spillover & link evidence",
                "Directional outlinks/backlinks and same-window co-edit counts",
                table(edges, [
                    ("Source article", "source_title"),
                    ("Target article", "target_title"),
                    ("Relationship", "edge_type"),
                    ("Co-edited", "co_edited_in_window"),
                    ("Target edits", "target_edits_in_window"),
                    ("Target editors", "target_distinct_editors"),
                ], aria_label="Co-edit spillover and link evidence"),
            ),
            panel(
                "Subject location provenance",
                "Wikibase QID, country/region, coordinate source, and resolution quality",
                table(points, [
                    ("Article", "title"),
                    ("Wikibase QID", "wikibase_item"),
                    ("Country / region", "country_or_region"),
                    ("Source", "location_source"),
                    ("Quality", "location_quality"),
                    ("Hist score", "historical_score"),
                ], article_links=True, aria_label="Subject location provenance"),
            ),
        ], className="chart-grid"),
    ]
