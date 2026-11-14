"""Plotly Graph Objects only: figures accept lists, never pandas dataframes."""

import math
from datetime import timedelta

import plotly.graph_objects as go

from dashboards.time_utils import format_india_time, to_india_wall_time

GREEN = "#536dfe"
BLUE = "#13a8d4"
AMBER = "#e69a35"
CORAL = "#b63a2b"
INK = "#25385f"

def _india_range(time_range):
    if not time_range:
        return None
    return [to_india_wall_time(time_range["start"]), to_india_wall_time(time_range["end"])]


def _india_axis(figure, time_range=None):
    figure.update_xaxes(tickformat="%d %b\n%H:%M", hoverformat="%d %b %Y, %H:%M")
    if not time_range:
        return
    start, end = _india_range(time_range)
    seconds = max(1, int((end - start).total_seconds()))
    step = 60 if seconds <= 10 * 60 else 15 * 60 if seconds <= 2 * 60 * 60 else 2 * 60 * 60
    first_tick = start.replace(second=0, microsecond=0)
    wall_seconds = first_tick.hour * 3600 + first_tick.minute * 60
    offset = (step - (wall_seconds % step)) % step
    first_tick += timedelta(seconds=offset)
    ticks = []
    tick = first_tick
    while tick < end:
        ticks.append(tick)
        tick += timedelta(seconds=step)
    ticks.append(end)
    figure.update_xaxes(
        range=[start, end], tickmode="array", tickvals=ticks,
        ticktext=[tick.strftime("%d %b<br>%H:%M") for tick in ticks],
    )


def _india_hover_labels(rows, key):
    return [to_india_wall_time(row[key]).strftime("%d %b %Y, %H:%M") for row in rows]


def style(figure, height=300):
    figure.update_layout(
        template="plotly_white", height=height,
        margin=dict(l=12, r=16, t=22, b=12),
        font=dict(family="Arial, sans-serif", size=13, color="#66758d"),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=1.16, x=0),
        hovermode="x unified",
        colorway=[GREEN, BLUE, AMBER],
    )
    figure.update_xaxes(showgrid=False, zeroline=False)
    figure.update_yaxes(gridcolor="#e5eaf3", zeroline=False)
    return figure


def empty(message="No observations available yet"):
    figure = go.Figure()
    figure.add_annotation(text=message, x=0.5, y=0.5, xref="paper", yref="paper",
                          showarrow=False, font=dict(size=14, color="#7b8799"))
    figure.update_xaxes(visible=False)
    figure.update_yaxes(visible=False)
    return style(figure)


def activity_timeline(rows, historical=False, time_range=None):
    if not rows:
        return empty()
    time_key = "day" if historical else "window_start"
    figure = go.Figure()
    x = [row[time_key] if historical else to_india_wall_time(row[time_key]) for row in rows]
    for name, key, color in (("Human edits", "human", GREEN), ("Bot edits", "bots", BLUE)):
        values = [(row.get("edits", 0) - row.get("bots", 0)) if historical and key == "human"
                  else row.get(key, 0) for row in rows]
        figure.add_trace(go.Scatter(x=x, y=values, name=name, mode="lines+markers",
                                   line=dict(color=color, width=2.5), marker=dict(size=5),
                                   fill="tozeroy" if key == "human" else None))
    if not historical:
        _india_axis(figure, time_range)
        labels = _india_hover_labels(rows, time_key)
        for trace in figure.data:
            trace.customdata = labels
            trace.hovertemplate = "%{customdata} IST<br>%{y:,}<extra>%{fullData.name}</extra>"
    return style(figure)


def ranking(rows, metric="edits", label="Edits"):
    if not rows:
        return empty()
    top = sorted(rows, key=lambda row: row.get(metric) or 0, reverse=True)[:10][::-1]
    figure = go.Figure(go.Bar(
        x=[row.get(metric) for row in top],
        y=list(range(len(top))), orientation="h", marker_color=GREEN,
        customdata=[row["title"] for row in top],
        hovertemplate="%{customdata}<br>" + label + ": %{x:,}<extra></extra>",
    ))
    # Numeric positions keep distinct articles separate even if shortened labels
    # share a prefix. The complete title remains available in the hover and click.
    labels = [row["title"] if len(row["title"]) <= 28 else row["title"][:27] + "…" for row in top]
    figure.update_yaxes(automargin=True, tickfont=dict(size=11), tickmode="array",
                        tickvals=list(range(len(top))), ticktext=labels)
    return style(figure, 340)


def bot_split(metrics):
    edits = metrics.get("edits") or 0
    bots = metrics.get("bots") or 0
    if not edits:
        return empty()
    figure = go.Figure(go.Pie(
        labels=["Human", "Bot"], values=[edits - bots, bots], hole=0.78,
        marker=dict(colors=[GREEN, "#b8d6e6"]), textinfo="none", sort=False,
        hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
    ))
    figure.add_annotation(text=f"{100 * (edits - bots) / edits:.0f}%<br>human",
                          showarrow=False, font=dict(size=21, color=INK))
    figure.update_layout(showlegend=True)
    return style(figure)


def bytes_timeline(rows, historical=False, time_range=None):
    if not rows:
        return empty()
    figure = go.Figure()
    x = [row["day"] if historical else to_india_wall_time(row["window_start"]) for row in rows]
    for label, key, color in (("Gross movement", "gross_bytes", BLUE), ("Net change", "net_bytes", GREEN)):
        figure.add_trace(go.Bar(x=x, y=[row.get(key) for row in rows], name=label, marker_color=color))
    figure.update_layout(barmode="group")
    if not historical:
        _india_axis(figure, time_range)
        labels = _india_hover_labels(rows, "window_start")
        for trace in figure.data:
            trace.customdata = labels
            trace.hovertemplate = "%{customdata} IST<br>%{y:,}<extra>%{fullData.name}</extra>"
    return style(figure)


def baseline_comparison(rows, time_range=None):
    if not rows:
        return empty("No matching historical baseline for these windows")
    rows = sorted(rows, key=lambda row: row["window_start"])
    figure = go.Figure()
    x_vals = [to_india_wall_time(r["window_start"]) for r in rows]
    for label, key in (("Observed human edits", "edits_human"), ("Historical mean", "avg_edits_human")):
        figure.add_trace(go.Scatter(x=x_vals, y=[r.get(key) for r in rows],
                                   name=label, mode="lines+markers"))
    if any(r.get("std_edits_human") is not None and r.get("avg_edits_human") is not None for r in rows):
        upper_band = [
            round((r.get("avg_edits_human") or 0) + 2.0 * (r.get("std_edits_human") or 0), 2)
            for r in rows
        ]
        figure.add_trace(go.Scatter(
            x=x_vals, y=upper_band, name="Baseline μ + 2σ",
            mode="lines", line=dict(color=AMBER, width=1.8, dash="dash"),
        ))
    _india_axis(figure, time_range)
    labels = _india_hover_labels(rows, "window_start")
    for trace in figure.data:
        trace.customdata = labels
        trace.hovertemplate = "%{customdata} IST<br>%{y:,}<extra>%{fullData.name}</extra>"
    return style(figure)


def relationship_graph(edges):
    if not edges:
        return empty("Link enrichment has not produced graph edges yet")
    names = sorted({edge[key] for edge in edges for key in ("source_title", "target_title")})[:60]
    positions = {name: (math.cos(2 * math.pi * i / len(names)), math.sin(2 * math.pi * i / len(names)))
                 for i, name in enumerate(names)}
    x, y = [], []
    co_x, co_y = [], []
    for edge in edges:
        source, target = positions.get(edge["source_title"]), positions.get(edge["target_title"])
        if source and target:
            if edge.get("co_edited_in_window"):
                co_x.extend([source[0], target[0], None])
                co_y.extend([source[1], target[1], None])
            else:
                x.extend([source[0], target[0], None])
                y.extend([source[1], target[1], None])
    figure = go.Figure(go.Scatter(x=x, y=y, mode="lines", line=dict(color="#b7c4d9", width=1),
                                  name="Wiki link", hoverinfo="skip"))
    if co_x:
        figure.add_trace(go.Scatter(x=co_x, y=co_y, mode="lines", line=dict(color=AMBER, width=2.2),
                                    name="Co-edited spillover", hoverinfo="skip"))
    figure.add_trace(go.Scatter(x=[positions[n][0] for n in names], y=[positions[n][1] for n in names],
                               text=names, mode="markers", marker=dict(size=14, color=GREEN),
                               name="Article", hovertemplate="%{text}<extra></extra>"))
    figure.update_layout(showlegend=bool(co_x), hovermode="closest")
    figure.update_xaxes(visible=False)
    figure.update_yaxes(visible=False, scaleanchor="x")
    return style(figure, 380)


def subject_map(points):
    located = [row for row in points if row.get("has_coordinates") and row.get("lat") is not None
               and row.get("lon") is not None and -90 <= row["lat"] <= 90 and -180 <= row["lon"] <= 180]
    if not located:
        return empty("No article-subject coordinates available yet")
    figure = go.Figure(go.Scattergeo(
        lat=[row["lat"] for row in located], lon=[row["lon"] for row in located],
        text=[f"{row['title']} · {row.get('location_quality', 'unknown')}" for row in located],
        marker=dict(size=10, color=GREEN, opacity=0.8), hovertemplate="%{text}<extra></extra>",
    ))
    figure.update_geos(showland=True, landcolor="#e8efea", showcountries=True,
                       countrycolor="white", showcoastlines=False, projection_type="natural earth")
    return style(figure, 380)


def storage(rows):
    if not rows:
        return empty()
    return style(go.Figure(go.Bar(x=[r["layer"] for r in rows], y=[r["megabytes"] for r in rows],
                                 marker_color=[GREEN, BLUE, AMBER], name="Parquet MB")))


def scaling_curve(rows):
    if not rows:
        return empty("Strong scaling measurements appear after running Milestone 7 benchmarks")
    x = [f"{r.get('workers_cores', 1)} cores" for r in rows]
    figure = go.Figure()
    figure.add_trace(go.Bar(
        x=x,
        y=[r.get("runtime_sec", 0) for r in rows],
        name="Runtime (s)",
        marker_color=BLUE,
    ))
    figure.add_trace(go.Scatter(
        x=x,
        y=[r.get("speedup_vs_1core", 1.0) for r in rows],
        name="Speedup (x)",
        mode="lines+markers",
        line=dict(color=GREEN, width=2.5),
        marker=dict(size=7),
    ))
    return style(figure)


def recovery_and_load(rows):
    if not rows:
        return empty("Streaming load measurements appear after running Milestone 7 benchmarks")
    x = [str(r.get("replay_speed_multiplier", "")) for r in rows]
    figure = go.Figure()
    figure.add_trace(go.Bar(
        x=x,
        y=[r.get("target_input_rate_rows_per_sec", 0) for r in rows],
        name="Input rate (rows/s)",
        marker_color=AMBER,
    ))
    figure.add_trace(go.Bar(
        x=x,
        y=[r.get("measured_processing_rate_rows_per_sec", 0) for r in rows],
        name="Processing capacity (rows/s)",
        marker_color=GREEN,
    ))
    figure.update_layout(barmode="group")
    return style(figure)


def churn_rankings(rows):
    """Top 10 articles by gross byte churn and rewriting volume (Milestone 4)."""
    if not rows:
        return empty("No churn rankings available yet")
    top = sorted(rows, key=lambda r: r.get("gross_bytes") or 0, reverse=True)[:10][::-1]
    labels = [r["title"] if len(r["title"]) <= 28 else r["title"][:27] + "…" for r in top]
    y_idx = list(range(len(top)))
    figure = go.Figure()
    figure.add_trace(go.Bar(
        x=[r.get("gross_bytes") or 0 for r in top], y=y_idx, orientation="h",
        name="Gross churn (B)", marker_color=BLUE, customdata=[r["title"] for r in top],
        hovertemplate="%{customdata}<br>Gross churn: %{x:,} B<extra></extra>",
    ))
    if any(r.get("rewriting_bytes") is not None for r in top):
        figure.add_trace(go.Bar(
            x=[r.get("rewriting_bytes") or 0 for r in top], y=y_idx, orientation="h",
            name="Rewriting (B)", marker_color=AMBER, customdata=[r["title"] for r in top],
            hovertemplate="%{customdata}<br>Rewriting: %{x:,} B<extra></extra>",
        ))
    figure.update_layout(barmode="group")
    figure.update_yaxes(automargin=True, tickfont=dict(size=11), tickmode="array",
                        tickvals=y_idx, ticktext=labels)
    return style(figure, 340)


def detector_comparison(summary):
    """Compare Detector A (Fixed), B (Historical), C (Spark ML), Consensus & Pageviews (Milestone 5)."""
    if not summary:
        return empty("No detector comparison counts available yet")
    labels = ["Fixed (A)", "Historical (B)", "Spark ML (C)", "All-3 Consensus", "Pageview Corrob."]
    values = [
        summary.get("detector_a_fixed_count", 0),
        summary.get("detector_b_historical_count", 0),
        summary.get("detector_c_spark_ml_count", 0),
        summary.get("all_three_consensus_count", 0),
        summary.get("pageview_corroborated_count", 0),
    ]
    if not any(values):
        return empty("No alerts triggered in the selected scope")
    figure = go.Figure(go.Bar(
        x=labels, y=values,
        marker_color=[AMBER, GREEN, BLUE, INK, "#06a6be"],
        hovertemplate="%{x}: %{y:,} alerts<extra></extra>",
    ))
    return style(figure)


def revert_risk_calibration(curve_rows):
    """Precision, Recall, and F1 vs Lift Wing threshold (Milestone 6)."""
    if not curve_rows:
        return empty("Revert-risk calibration curve appears after running Milestone 6 evaluation")
    thresholds = [r.get("threshold") for r in curve_rows]
    figure = go.Figure()
    for label, key, color in (
        ("Precision (%)", "precision_pct", GREEN),
        ("Recall (%)", "recall_pct", BLUE),
        ("F1 (%)", "f1_pct", AMBER),
    ):
        figure.add_trace(go.Scatter(
            x=thresholds, y=[r.get(key) for r in curve_rows],
            name=label, mode="lines+markers", line=dict(color=color, width=2.5),
        ))
    figure.add_vline(x=0.8, line_dash="dash", line_color="#98a4b7", annotation_text="0.80 target")
    figure.update_xaxes(title_text="ML Revert Risk Threshold")
    figure.update_yaxes(title_text="Score (%)", range=[0, 105])
    return style(figure)


def m4_engine_and_latency(m4_data):
    """Compare RDD vs DataFrame runtime and Raw JSONL.gz vs Parquet query latency (Milestone 4)."""
    if not m4_data:
        return empty("Milestone 4 engine benchmarks appear after running rankings_benchmark.py")
    rdd_bench = m4_data.get("rdd_vs_dataframe_benchmark", {})
    lat_bench = m4_data.get("query_latency_benchmark", {})
    labels = [
        "RDD (7d agg)",
        "DataFrame (7d agg)",
        "Raw JSONL.gz (1d)",
        "Parquet rankings",
        "Parquet 46d series",
    ]
    values = [
        rdd_bench.get("rdd_runtime_sec", 0),
        rdd_bench.get("dataframe_runtime_sec", 0),
        lat_bench.get("raw_jsonl_gz_1day_top20_sec", 0),
        lat_bench.get("stored_parquet_rankings_top20_sec", 0),
        lat_bench.get("stored_parquet_46day_churn_series_sec", 0),
    ]
    if not any(values):
        return empty("Milestone 4 engine benchmarks appear after running rankings_benchmark.py")
    figure = go.Figure(go.Bar(
        x=labels, y=values,
        marker_color=[AMBER, GREEN, AMBER, BLUE, GREEN],
        hovertemplate="%{x}: %{y:.3f}s<extra></extra>",
    ))
    figure.add_hline(y=2.0, line_dash="dash", line_color="#d94b6a", annotation_text="2.0s SLA")
    figure.update_yaxes(title_text="Seconds")
    return style(figure)


def rf_feature_importances(importances):
    """Horizontal bar chart of Random Forest feature importances learned by Spark ML."""
    if not importances:
        return empty("Random Forest feature importances appear after running Milestone 5 Spark ML")
    pretty = {
        "f_distinct_editors": "Distinct human editors",
        "f_edits_human": "Human edit count",
        "f_editor_surge": "Editor surge ratio",
        "f_log_gross_bytes": "Log gross byte churn",
        "f_velocity_z": "Edit velocity z-score",
        "f_revert_ratio": "Revert ratio",
        "f_log_rewriting_bytes": "Log rewriting bytes",
        "f_bot_share": "Bot edit share",
    }
    items = sorted(importances.items(), key=lambda kv: kv[1])
    labels = [pretty.get(k, k) for k, _ in items]
    values = [round(v * 100, 2) for _, v in items]
    figure = go.Figure(go.Bar(
        x=values, y=labels, orientation="h", marker_color=GREEN,
        hovertemplate="%{y}: %{x:.2f}%<extra></extra>",
    ))
    figure.update_xaxes(title_text="Importance (%)")
    figure.update_yaxes(automargin=True)
    return style(figure, 320)


def kmeans_clusters(cluster_sizes, normal_cluster_id=None):
    """Bar chart of KMeans (k=8) window cluster distribution learned by Spark ML."""
    if not cluster_sizes:
        return empty("KMeans cluster distribution appears after running Milestone 5 Spark ML")
    items = sorted(cluster_sizes.items(), key=lambda kv: int(kv[0]))
    labels = [
        f"Cluster {cid} (baseline)" if str(cid) == str(normal_cluster_id) else f"Cluster {cid}"
        for cid, _ in items
    ]
    colors = [
        BLUE if str(cid) == str(normal_cluster_id) else AMBER if count < 2000 else GREEN
        for cid, count in items
    ]
    figure = go.Figure(go.Bar(
        x=labels, y=[count for _, count in items], marker_color=colors,
        hovertemplate="%{x}: %{y:,} windows<extra></extra>",
    ))
    figure.update_yaxes(title_text="Page windows")
    return style(figure, 320)


def geo_country_breakdown(countries):
    """Top countries/regions by alerted Wikipedia topics (Milestone 6)."""
    if not countries:
        return empty("Country breakdown appears after running Milestone 6 geospatial enrichment")
    top = countries[:10][::-1]
    figure = go.Figure(go.Bar(
        x=[r.get("active_topics", 0) for r in top],
        y=[r.get("country_or_region", "Unknown") for r in top],
        orientation="h", marker_color=BLUE,
        hovertemplate="%{y}: %{x:,} alerted topics<extra></extra>",
    ))
    figure.update_xaxes(title_text="Alerted topics")
    figure.update_yaxes(automargin=True)
    return style(figure, 320)


def alert_type_breakdown(m5_summary):
    """Breakdown of detected knowledge-shift alerts by alert_type and window_duration."""
    if not m5_summary:
        return empty("Alert type breakdown appears after running Milestone 5 scoring")
    type_counts = m5_summary.get("alert_type_breakdown", {})
    if not type_counts:
        return empty("No alert type breakdown available yet")
    items = sorted(type_counts.items(), key=lambda kv: kv[1], reverse=True)
    labels = [k.replace("_", " ").title() for k, _ in items]
    values = [v for _, v in items]
    figure = go.Figure(go.Bar(
        x=labels, y=values,
        marker_color=[AMBER, GREEN, BLUE, INK][:len(labels)],
        hovertemplate="%{x}: %{y:,} alerts<extra></extra>",
    ))
    figure.update_yaxes(title_text="Flagged alerts")
    return style(figure, 320)


def score_component_breakdown(alerts):
    """Stacked horizontal bar chart of additive 100-point Historical Score components."""
    if not alerts:
        return empty("Score component decomposition appears when alerts are available")
    with_contrib = [r for r in alerts if isinstance(r.get("feature_contributions"), dict) and r.get("feature_contributions")]
    if not with_contrib:
        return empty("Feature contribution breakdown appears when alerts are available")
    top = with_contrib[:10][::-1]
    labels = [r["title"] if len(r.get("title", "")) <= 26 else r.get("title", "")[:25] + "…" for r in top]
    y_idx = list(range(len(top)))
    components = [
        ("Edit velocity (/35)", "edit_velocity", GREEN),
        ("Editor diversity (/25)", "editor_diversity", BLUE),
        ("Byte churn (/20)", "byte_churn", AMBER),
        ("Consensus / dispute (/10)", "consensus_or_dispute", INK),
        ("Pageview corrob. (/10)", "pageview_corroboration", "#06a6be"),
    ]
    figure = go.Figure()
    for name, key, color in components:
        figure.add_trace(go.Bar(
            x=[float((r.get("feature_contributions") or {}).get(key, 0.0)) for r in top],
            y=y_idx, orientation="h", name=name, marker_color=color,
            customdata=[r.get("title", "") for r in top],
            hovertemplate="%{customdata}<br>" + name + ": %{x:.1f} pts<extra></extra>",
        ))
    figure.update_layout(barmode="stack")
    figure.update_xaxes(title_text="Historical Knowledge-Shift Score (0–100 pts)", range=[0, 105])
    figure.update_yaxes(automargin=True, tickfont=dict(size=11), tickmode="array",
                        tickvals=y_idx, ticktext=labels)
    return style(figure, 340)


def anomaly_scatter(alerts):
    """Velocity Z-Score vs Historical Score scatter across detected alerts."""
    if not alerts:
        return empty("Anomaly score distribution appears when alerts are available")
    valid = [r for r in alerts if r.get("velocity_zscore") is not None and r.get("historical_score") is not None][:80]
    if not valid:
        return empty("No velocity z-score and historical score pairs available")
    figure = go.Figure()
    color_map = {
        "CONFIRMED_NEWS": GREEN,
        "BREAKING_NEWS_CANDIDATE": BLUE,
        "EDIT_WAR_CONTROVERSY": AMBER,
        "MAJOR_REWRITE": INK,
    }
    by_type = {}
    for r in valid:
        atype = r.get("alert_type") or "BREAKING_NEWS_CANDIDATE"
        by_type.setdefault(atype, []).append(r)
    for atype, rows in by_type.items():
        figure.add_trace(go.Scatter(
            x=[min(300.0, float(r.get("velocity_zscore") or 0)) for r in rows],
            y=[float(r.get("historical_score") or 0) for r in rows],
            mode="markers",
            name=atype.replace("_", " ").title(),
            marker=dict(
                size=[min(22, max(8, int(r.get("distinct_human_editors") or 3) + 5)) for r in rows],
                color=color_map.get(atype, GREEN),
                opacity=0.82,
            ),
            customdata=[r.get("title", "") for r in rows],
            hovertemplate="%{customdata}<br>Velocity z: %{x:.1f}<br>Historical score: %{y:.1f}<extra></extra>",
        ))
    figure.update_layout(hovermode="closest")
    figure.update_xaxes(title_text="Velocity Z-Score (capped at 300)")
    figure.update_yaxes(title_text="Historical Score (0–100)")
    return style(figure, 340)


def false_alert_comparison_chart(full_comp, sample_comp=None):
    """Grouped bar chart comparing Precision (%) and False-Alert Rate (%) across Detectors A, B, and C."""
    rows = full_comp or sample_comp or []
    if not rows:
        return empty("Detector false-alert evaluation appears after running evaluate_alerts.py")
    short_labels = []
    for r in rows:
        det = r.get("detector", "")
        if "Naive" in det:
            short_labels.append("Det A (Naive Fixed)")
        elif "All Shift" in det or "Fixed" in det:
            short_labels.append("Det A (Fixed All)")
        elif "Historical" in det:
            short_labels.append("Det B (Historical)")
        elif "Spark ML" in det:
            short_labels.append("Det C (Spark ML)")
        else:
            short_labels.append(det[:22])
    figure = go.Figure()
    figure.add_trace(go.Bar(
        x=short_labels,
        y=[r.get("precision_pct", 0) for r in rows],
        name="Precision (%)",
        marker_color=GREEN,
        hovertemplate="%{x}<br>Precision: %{y:.2f}%<extra></extra>",
    ))
    figure.add_trace(go.Bar(
        x=short_labels,
        y=[r.get("false_alert_rate_pct", 0) for r in rows],
        name="False-Alert Rate (%)",
        marker_color=AMBER,
        hovertemplate="%{x}<br>False-Alert Rate: %{y:.2f}%<extra></extra>",
    ))
    figure.update_layout(barmode="group")
    figure.update_yaxes(title_text="Percentage (%)", range=[0, 108])
    return style(figure, 320)


def synthetic_patterns_chart(pattern_details):
    """Bar chart of synthetic knowledge-shift patterns (5 positive + 2 negative controls) vs alert threshold."""
    if not pattern_details:
        return empty("Synthetic pattern injection results appear after running evaluate_alerts.py")
    labels = [r.get("pattern_title", "").replace("SYNTH_", "").replace("CTRL_", "CTRL: ").replace("_", " ").title()
              for r in pattern_details]
    scores = [r.get("historical_score", 0) for r in pattern_details]
    colors = [GREEN if r.get("expected_positive") else AMBER for r in pattern_details]
    figure = go.Figure(go.Bar(
        x=scores[::-1], y=labels[::-1], orientation="h",
        marker_color=colors[::-1],
        hovertemplate="%{y}: %{x:.1f} pts<extra></extra>",
    ))
    figure.add_vline(x=65.0, line_dash="dash", line_color="#d94b6a", annotation_text="Alert threshold (65)")
    figure.update_xaxes(title_text="Historical Score (0–100)", range=[0, 105])
    figure.update_yaxes(automargin=True)
    return style(figure, 320)


def revert_risk_multi_curve(m6_revert_risk):
    """Compare F1 score curves across Lift Wing ML, Composite Risk, and Heuristic Risk (Milestone 6)."""
    exp5 = (m6_revert_risk or {}).get("experiment_5_threshold_calibration", {})
    ml_curve = exp5.get("liftwing_ml_curve", [])
    comp_curve = exp5.get("composite_risk_curve", [])
    heur_curve = exp5.get("heuristic_risk_curve", [])
    if not (ml_curve or comp_curve or heur_curve):
        return empty("Multi-model F1 calibration appears after running revert_risk_monitor.py")
    figure = go.Figure()
    for name, curve, color in (
        ("Composite Risk F1 (%)", comp_curve, GREEN),
        ("Lift Wing ML F1 (%)", ml_curve, BLUE),
        ("Heuristic Risk F1 (%)", heur_curve, AMBER),
    ):
        if curve:
            figure.add_trace(go.Scatter(
                x=[r.get("threshold") for r in curve],
                y=[r.get("f1_pct") for r in curve],
                name=name, mode="lines+markers", line=dict(color=color, width=2.5),
            ))
    figure.add_vline(x=0.65, line_dash="dot", line_color=GREEN, annotation_text="0.65 Calibrated")
    figure.add_vline(x=0.80, line_dash="dash", line_color="#98a4b7", annotation_text="0.80 Static")
    figure.update_xaxes(title_text="Revert Risk Decision Threshold (τ)")
    figure.update_yaxes(title_text="F1 Score (%)", range=[0, 100])
    return style(figure, 320)


def mttr_comparison(mttr_breakdown):
    """Mean vs Median Time-to-Revert (seconds) across Overall, Human, and Bot reverters."""
    if not mttr_breakdown:
        return empty("Time-to-Revert (MTTR) breakdown appears after running revert_risk_monitor.py")
    categories = [
        ("Overall Reverted", mttr_breakdown.get("overall", {})),
        ("Human Reverters", mttr_breakdown.get("human_reverters", {})),
        ("Bot Reverters", mttr_breakdown.get("bot_reverters", {})),
    ]
    labels = [label for label, d in categories if d.get("count")]
    if not labels:
        return empty("No reverted revisions with TTR recorded yet")
    means = [d.get("mean_sec") or 0 for _, d in categories if d.get("count")]
    medians = [d.get("median_sec") or 0 for _, d in categories if d.get("count")]
    figure = go.Figure()
    figure.add_trace(go.Bar(
        x=labels, y=medians, name="Median TTR (s)", marker_color=GREEN,
        hovertemplate="%{x}<br>Median TTR: %{y:.1f}s<extra></extra>",
    ))
    figure.add_trace(go.Bar(
        x=labels, y=means, name="Mean TTR (s)", marker_color=BLUE,
        hovertemplate="%{x}<br>Mean TTR: %{y:.1f}s<extra></extra>",
    ))
    figure.update_layout(barmode="group")
    figure.update_yaxes(title_text="Seconds to revert")
    return style(figure, 320)


def historical_alerts_timeline(daily_alerts):
    """Stacked daily count of detected historical knowledge shifts by alert_type."""
    if not daily_alerts:
        return empty("Historical anomaly timeline populates when curated/alerts covers the selected dates")
    x = [r.get("day") for r in daily_alerts]
    figure = go.Figure()
    series = [
        ("Confirmed news", "confirmed_news", GREEN),
        ("Breaking candidate", "breaking_news", BLUE),
        ("Edit war / dispute", "edit_war", AMBER),
        ("Major rewrite", "major_rewrite", INK),
    ]
    for label, key, color in series:
        figure.add_trace(go.Bar(
            x=x, y=[r.get(key, 0) for r in daily_alerts],
            name=label, marker_color=color,
        ))
    figure.update_layout(barmode="stack")
    figure.update_yaxes(title_text="Detected shifts / day")
    return style(figure, 300)


def baseline_tier_distribution(tier_rows):
    """Distribution of per-page historical baselines across activity tiers (quiet/moderate/active/hyperactive)."""
    if not tier_rows:
        return empty("Baseline tier distribution populates when page_baselines is available")
    labels = [str(r.get("activity_tier", "unknown")).title() for r in tier_rows]
    counts = [r.get("pages_count", 0) for r in tier_rows]
    figure = go.Figure(go.Bar(
        x=labels, y=counts, marker_color=[BLUE, GREEN, AMBER, INK][:len(labels)],
        hovertemplate="%{x}: %{y:,} article baselines<extra></extra>",
    ))
    figure.update_yaxes(title_text="Articles (log scale)", type="log")
    return style(figure, 300)


def recentchanges_regional_map(
    country_rollup,
    map_articles,
    selected_region="ALL",
    projection="natural earth",
    metric_key="total_edits",
):
    """Dual-layer Choropleth (country RecentChanges density) + Scattergeo (article bubbles)."""
    if not country_rollup and not map_articles:
        return empty("Regional RecentChanges map populates when curated tables are available")
    metric_meta = {
        "total_edits": ("Total Edits", "total_edits", "edits_total", 0.42, ""),
        "human_edits": ("Human Edits", "total_edits_human", "edits_human", 0.42, ""),
        "gross_bytes": ("Gross Churn (B)", "gross_bytes", "gross_bytes", 0.25, " B"),
        "reverts": ("Reverts", "reverts", "reverts", 0.50, ""),
    }
    m_label, country_col, art_col, size_exp, unit = metric_meta.get(
        metric_key, metric_meta["total_edits"]
    )
    figure = go.Figure()
    if country_rollup:
        figure.add_trace(go.Choropleth(
            locations=[r.get("country_or_region") for r in country_rollup],
            locationmode="country names",
            z=[r.get(country_col) or r.get("total_edits") or 0 for r in country_rollup],
            customdata=[f"COUNTRY|{r.get('country_or_region')}" for r in country_rollup],
            text=[
                f"<b>{r.get('country_or_region')}</b> ({r.get('macro_region', '')})<br>"
                f"Active articles: {r.get('active_topics', r.get('active_articles', 0)):,}<br>"
                f"Human edits: {r.get('total_edits_human', r.get('human_edits', 0)):,} · Bot edits: {r.get('total_edits_bot', r.get('bot_edits', 0)):,}<br>"
                f"Gross churn: {r.get('gross_bytes', 0):,} B · Reverts: {r.get('reverts', 0):,}<br>"
                f"Top article: {r.get('top_article', '—')}<br>"
                f"<i>Click country to filter dashboard to {r.get('country_or_region')}</i>"
                for r in country_rollup
            ],
            colorscale=[[0.0, "#dce8f8"], [0.35, "#7ea8e8"], [0.7, "#1f5eff"], [1.0, "#0b2878"]],
            showscale=True,
            colorbar=dict(title=m_label, thickness=14, len=0.72, x=0.985),
            hovertemplate=f"%{{text}}<br>{m_label}: %{{z:,}}{unit}<extra></extra>",
            name="Country density",
        ))
    if map_articles:
        macro_colors = {
            "North America": BLUE,
            "Europe": GREEN,
            "South Asia": AMBER,
            "East & Southeast Asia": "#7c3aed",
            "Middle East & Africa": "#db2777",
            "Latin America": "#0891b2",
            "Oceania": "#ea580c",
        }
        figure.add_trace(go.Scattergeo(
            lat=[r.get("lat") for r in map_articles],
            lon=[r.get("lon") for r in map_articles],
            text=[
                f"<b>{r.get('title')}</b><br>"
                f"Region: {r.get('country_or_region')} ({r.get('macro_region')})<br>"
                f"Edits: {r.get('edits_total', r.get('edits', 0)):,} "
                f"(Human: {r.get('edits_human', r.get('human', 0)):,}, Bot: {r.get('edits_bot', r.get('bots', 0)):,})<br>"
                f"Gross churn: {r.get('gross_bytes', 0):,} B · Reverts: {r.get('reverts', 0):,}<br>"
                f"Source: {r.get('location_source', 'wikidata')} ({r.get('location_quality', 'exact')})<br>"
                f"<i>Click bubble to inspect article below</i>"
                for r in map_articles
            ],
            customdata=[
                f"ARTICLE|{r.get('title')}|{r.get('country_or_region', '')}"
                for r in map_articles
            ],
            mode="markers",
            name="Edited articles",
            hovertemplate="%{text}<extra></extra>",
            marker=dict(
                size=[
                    min(30, max(8, 6 + (max(1, int(r.get(art_col) or r.get("edits_total") or r.get("edits") or 1)) ** size_exp)))
                    for r in map_articles
                ],
                color=[macro_colors.get(r.get("macro_region"), CORAL) for r in map_articles],
                line=dict(color="#ffffff", width=1.25),
                opacity=0.90,
            ),
        ))
    proj_norm = projection if projection in (
        "natural earth", "orthographic", "equirectangular", "mercator", "robinson"
    ) else "natural earth"
    scope_map = {
        "North America": "north america",
        "Europe": "europe",
        "Latin America": "south america",
    }
    geo_scope = scope_map.get(selected_region, "world") if proj_norm != "orthographic" else "world"
    geo_kwargs = dict(
        scope=geo_scope,
        showcountries=True, countrycolor="#b9c5d8", countrywidth=0.8,
        showsubunits=True, subunitcolor="#d6dfec",
        showland=True, landcolor="#eef2f7",
        showocean=True, oceancolor="#f7f9fc",
        showlakes=True, lakecolor="#e3eefc",
        showcoastlines=True, coastlinecolor="#b9c5d8", coastlinewidth=0.7,
        projection_type=proj_norm,
        lonaxis=dict(showgrid=True, gridcolor="rgba(160,175,200,0.18)"),
        lataxis=dict(showgrid=True, gridcolor="rgba(160,175,200,0.18)"),
    )
    # Auto-center and zoom when a country or non-scope macro-region is selected
    if selected_region and selected_region != "ALL" and geo_scope == "world":
        lats = [float(r["lat"]) for r in (country_rollup or map_articles) if r.get("lat") is not None]
        lons = [float(r["lon"]) for r in (country_rollup or map_articles) if r.get("lon") is not None]
        if lats and lons:
            c_lat = sum(lats) / len(lats)
            c_lon = sum(lons) / len(lons)
            is_macro = selected_region in (
                "South Asia", "East & Southeast Asia", "Middle East & Africa", "Oceania",
                "North America", "Europe", "Latin America",
            )
            geo_kwargs["center"] = dict(lat=c_lat, lon=c_lon)
            if proj_norm == "orthographic":
                geo_kwargs["projection_rotation"] = dict(lat=c_lat, lon=c_lon, roll=0)
            else:
                geo_kwargs["projection_scale"] = 1.9 if is_macro else 3.2
    figure.update_geos(**geo_kwargs)
    styled = style(figure, 620)
    styled.update_layout(
        margin=dict(l=0, r=0, t=8, b=8),
        legend=dict(
            orientation="h", yanchor="bottom", y=0.01, xanchor="left", x=0.01,
            bgcolor="rgba(255,255,255,0.82)", bordercolor="#dfe6f2", borderwidth=1,
        ),
    )
    return styled


def regional_edits_breakdown(country_rollup):
    """Horizontal stacked bar chart of Human vs Bot RecentChanges edits by country/region."""
    if not country_rollup:
        return empty("No regional edits found for the selected filter")
    top = country_rollup[:15][::-1]
    countries = [r.get("country_or_region", "Unknown") for r in top]
    human = [r.get("total_edits_human") or 0 for r in top]
    bots = [r.get("total_edits_bot") or 0 for r in top]
    figure = go.Figure()
    figure.add_trace(go.Bar(
        y=countries, x=human, orientation="h", name="Human edits", marker_color=BLUE,
        hovertemplate="%{y}<br>Human edits: %{x:,}<extra></extra>",
    ))
    figure.add_trace(go.Bar(
        y=countries, x=bots, orientation="h", name="Bot edits", marker_color=AMBER,
        hovertemplate="%{y}<br>Bot edits: %{x:,}<extra></extra>",
    ))
    figure.update_layout(barmode="stack")
    figure.update_xaxes(title_text="RecentChanges edits in selected range")
    return style(figure, 360)


def regional_edits_timeline(timeline, mode="days"):
    """Time-series of Regional RecentChanges Human edits, Bot edits, and Active Pages."""
    if not timeline:
        return empty("Regional timeline populates when RecentChanges rows match the time range")
    x = [
        format_india_time(r.get("bucket")) if "T" in str(r.get("bucket", "")) else r.get("bucket")
        for r in timeline
    ]
    figure = go.Figure()
    figure.add_trace(go.Scatter(
        x=x, y=[r.get("human") or 0 for r in timeline],
        name="Human edits", mode="lines+markers", line=dict(color=BLUE, width=2.4),
        fill="tozeroy", fillcolor="rgba(31,94,255,0.12)",
    ))
    figure.add_trace(go.Scatter(
        x=x, y=[r.get("bots") or 0 for r in timeline],
        name="Bot edits", mode="lines", line=dict(color=AMBER, width=1.8, dash="dot"),
    ))
    figure.add_trace(go.Scatter(
        x=x, y=[r.get("reverts") or 0 for r in timeline],
        name="Reverts", mode="lines", line=dict(color=CORAL, width=1.6),
    ))
    figure.update_xaxes(title_text="Window (IST)" if mode != "days" else "Curated Day (UTC)")
    figure.update_yaxes(title_text="Edits per bucket")
    return style(figure, 340)


def macro_region_distribution(macro_regions):
    """Comparison of Macro-Regions by Total Edits and Active Articles."""
    if not macro_regions:
        return empty("Macro-region breakdown populates when located RecentChanges are available")
    labels = [r.get("macro_region", "Other") for r in macro_regions]
    edits = [r.get("total_edits") or 0 for r in macro_regions]
    topics = [r.get("active_topics") or 0 for r in macro_regions]
    figure = go.Figure()
    figure.add_trace(go.Bar(
        x=labels, y=edits, name="Total edits", marker_color=BLUE,
        hovertemplate="%{x}<br>Total edits: %{y:,}<extra></extra>",
    ))
    figure.add_trace(go.Bar(
        x=labels, y=topics, name="Active articles", marker_color=GREEN,
        hovertemplate="%{x}<br>Active articles: %{y:,}<extra></extra>",
    ))
    figure.update_layout(barmode="group")
    figure.update_yaxes(title_text="Count (log scale)", type="log")
    return style(figure, 340)

