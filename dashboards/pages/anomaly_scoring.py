"""Dedicated UI Page 1: Multi-Detector Anomaly Scoring & Knowledge-Shift Classification."""

from dash import html

from dashboards import charts
from dashboards.components import graph, heading, metric, notice, number, panel, pill_row, table


def render(data):
    ing = data.get("ingestion_summary", {})
    m5 = data.get("m5_summary", {})
    win_feat = data.get("window_features", {})
    case_demo = data.get("case_studies_demo", {})
    case_studies = case_demo.get("case_studies", [])
    alerts = data.get("alerts", [])

    dur_breakdown = m5.get("window_duration_breakdown", {})
    type_breakdown = m5.get("alert_type_breakdown", {})
    total_5m_win = win_feat.get("windows_5m_total")
    total_1h_win = win_feat.get("windows_1h_total")

    scoring_pills = [
        ("Data source", "ingest_curate.py (Strictly Historical Curated Lake)"),
    ]
    if ing:
        scoring_pills.append((
            "Curated rc_events",
            f"{ing.get('curated_deduplicated_events', 10190119):,} ({ing.get('distinct_days', 55)} days: 2026-09-15 – 2026-11-08)",
        ))
    if total_5m_win is not None and total_1h_win is not None:
        scoring_pills.append(("5m windows evaluated", f"{total_5m_win:,}"))
        scoring_pills.append(("1h windows evaluated", f"{total_1h_win:,}"))
    if dur_breakdown:
        scoring_pills.append(("5m alerts", f"{dur_breakdown.get('5m', 0):,}"))
        scoring_pills.append(("1h alerts", f"{dur_breakdown.get('1h', 0):,}"))
    for atype, cnt in type_breakdown.items():
        scoring_pills.append((atype.replace("_", " ").title(), f"{cnt:,}"))

    case_rows = []
    for cs in case_studies:
        fc = cs.get("feature_contributions", {})
        case_rows.append({
            "title": cs.get("title"),
            "alert_type": cs.get("alert_type"),
            "window_duration": cs.get("window_duration"),
            "historical_score": cs.get("detector_b_historical_score"),
            "spark_ml_score": cs.get("detector_c_spark_ml_score"),
            "fixed_threshold_flag": cs.get("detector_a_fixed_flag"),
            "pageview_corroborated": cs.get("pageview_corroborated"),
            "velocity_zscore": cs.get("velocity_zscore"),
            "vel_pts": fc.get("edit_velocity"),
            "editor_pts": fc.get("editor_diversity"),
            "churn_pts": fc.get("byte_churn"),
            "consensus_pts": fc.get("consensus_or_dispute"),
            "pv_pts": fc.get("pageview_corroboration"),
            "reasons": cs.get("reasons", []),
        })

    return [
        heading(
            "DETECT / HISTORICAL ANOMALY SCORING (INGEST_CURATE)",
            "Explainable knowledge-shift scoring.",
            "Strictly historical curated evaluation from ingest_curate.py (2026-09-15 – 2026-11-08): compare Fixed Threshold (Detector A), Explainable 0–100 Historical Score (Detector B), and Spark ML (Detector C) across 5m and 1h curated windows.",
        ),
        html.Div([
            metric(
                "TOTAL ALERTS (UNION)",
                number(m5.get("total_alerts_union")),
                f"{dur_breakdown.get('5m', 0):,} in 5m · {dur_breakdown.get('1h', 0):,} in 1h windows"
                if dur_breakdown else "Flagged by Fixed, Historical, or Spark ML",
                True,
            ),
            metric(
                "HISTORICAL SCORE (B)",
                number(m5.get("detector_b_historical_count")),
                f"{m5.get('explainability_rate_pct', 100.0):.0f}% with ≥2 human-readable reasons",
            ),
            metric(
                "ALL-3 CONSENSUS",
                number(m5.get("all_three_consensus_count")),
                f"Simultaneously flagged by A, B, and C ({m5.get('detector_a_fixed_count', 0):,} Fixed A)",
            ),
            metric(
                "PAGEVIEW CORROBORATED",
                number(m5.get("pageview_corroborated_count")),
                "Confirmed by ≥2.5× daily Analytics pageview surge",
            ),
        ], className="metric-grid"),
        *([pill_row(scoring_pills)] if scoring_pills else []),
        html.Div([
            panel(
                "Three-detector comparison (A vs B vs C)",
                "Alert volume across Fixed Threshold, Historical Score, Spark ML, All-3 Consensus, and Pageviews",
                graph(charts.detector_comparison(m5)),
            ),
            panel(
                "Knowledge-shift classification breakdown",
                "Distribution across Edit War Controversy, Breaking News Candidate, Major Rewrite, and Confirmed News",
                graph(charts.alert_type_breakdown(m5)),
            ),
        ], className="chart-grid"),
        html.Div([
            panel(
                "Additive 100-point Historical Score decomposition",
                "Exact component points: Velocity (/35) + Editors (/25) + Churn (/20) + Consensus/Dispute (/10) + Pageviews (/10)",
                graph(charts.score_component_breakdown(alerts or case_rows)),
            ),
            panel(
                "Velocity Z-score vs Historical Score",
                "Baseline-normalized edit velocity surge vs 0–100 anomaly score colored by shift class",
                graph(charts.anomaly_scatter(alerts)),
            ),
        ], className="chart-grid"),
        panel(
            "Canonical knowledge-shift archetypes (Case Studies)",
            "Four verified real-world archetypes illustrating Confirmed News, Breaking Candidate, Edit War, and Major Rewrite",
            table(case_rows, [
                ("Article", "title"),
                ("Shift class", "alert_type"),
                ("Win", "window_duration"),
                ("Hist / 100", "historical_score"),
                ("Spark ML", "spark_ml_score"),
                ("Fixed (A)", "fixed_threshold_flag"),
                ("Pageviews", "pageview_corroborated"),
                ("Vel (/35)", "vel_pts"),
                ("Edit (/25)", "editor_pts"),
                ("Churn (/20)", "churn_pts"),
                ("Cons (/10)", "consensus_pts"),
                ("PV (/10)", "pv_pts"),
                ("Explainable reasons", "reasons"),
            ], article_links=True, aria_label="Canonical knowledge-shift case studies")
            if case_rows else notice("No case studies loaded", "Run backup_demo.py to generate milestone8_case_studies_demo.json."),
        ),
        panel(
            "Multi-detector scored alerts ledger",
            "Top curated alerts ordered by Historical Knowledge-Shift Score and Spark ML Score",
            table(alerts, [
                ("Article", "title"),
                ("Window (IST)", "window_start_ist"),
                ("Dur", "window_duration"),
                ("Shift class", "alert_type"),
                ("Hist / 100", "historical_score"),
                ("Spark ML", "spark_ml_score"),
                ("Fixed (A)", "fixed_threshold_flag"),
                ("Pageviews", "pageview_corroborated"),
                ("Human edits", "edits_human"),
                ("Editors", "distinct_human_editors"),
                ("Revert ratio", "revert_ratio"),
                ("Velocity z", "velocity_zscore"),
                ("Reasons (≥2 required)", "reasons"),
            ], article_links=True, aria_label="Multi-detector scored alerts ledger"),
        ),
    ]
