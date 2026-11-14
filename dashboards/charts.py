"""Plotly Graph Objects only: figures accept lists, never pandas dataframes."""

import math
from datetime import timedelta

import plotly.graph_objects as go

from dashboards.time_utils import to_india_wall_time

GREEN = "#536dfe"
BLUE = "#13a8d4"
AMBER = "#e69a35"
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
