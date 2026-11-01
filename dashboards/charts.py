"""Plotly Graph Objects only: figures accept lists, never pandas dataframes."""

import math

import plotly.graph_objects as go

GREEN = "#16836f"
BLUE = "#5976bb"
AMBER = "#d69e45"
INK = "#203c37"


def style(figure, height=300):
    figure.update_layout(
        template="plotly_white", height=height,
        margin=dict(l=12, r=16, t=22, b=12),
        font=dict(family="Arial, sans-serif", size=12, color="#71817b"),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=1.16, x=0),
        hovermode="x unified",
        colorway=[GREEN, BLUE, AMBER],
    )
    figure.update_xaxes(showgrid=False, zeroline=False)
    figure.update_yaxes(gridcolor="#edf1ee", zeroline=False)
    return figure


def empty(message="No observations available yet"):
    figure = go.Figure()
    figure.add_annotation(text=message, x=0.5, y=0.5, xref="paper", yref="paper",
                          showarrow=False, font=dict(size=13, color="#89968e"))
    figure.update_xaxes(visible=False)
    figure.update_yaxes(visible=False)
    return style(figure)


def activity_timeline(rows, historical=False):
    if not rows:
        return empty()
    time_key = "day" if historical else "window_start"
    figure = go.Figure()
    x = [row[time_key] for row in rows]
    for name, key, color in (("Human edits", "human", GREEN), ("Bot edits", "bots", BLUE)):
        values = [(row.get("edits", 0) - row.get("bots", 0)) if historical and key == "human"
                  else row.get(key, 0) for row in rows]
        figure.add_trace(go.Scatter(x=x, y=values, name=name, mode="lines+markers",
                                   line=dict(color=color, width=2.5), marker=dict(size=5),
                                   fill="tozeroy" if key == "human" else None))
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
        marker=dict(colors=[GREEN, "#c8d9e5"]), textinfo="none", sort=False,
        hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
    ))
    figure.add_annotation(text=f"{100 * (edits - bots) / edits:.0f}%<br>human",
                          showarrow=False, font=dict(size=21, color=INK))
    figure.update_layout(showlegend=True)
    return style(figure)


def bytes_timeline(rows, historical=False):
    if not rows:
        return empty()
    figure = go.Figure()
    x = [row["day" if historical else "window_start"] for row in rows]
    for label, key, color in (("Gross movement", "gross_bytes", BLUE), ("Net change", "net_bytes", GREEN)):
        figure.add_trace(go.Bar(x=x, y=[row.get(key) for row in rows], name=label, marker_color=color))
    figure.update_layout(barmode="group")
    return style(figure)


def baseline_comparison(rows):
    if not rows:
        return empty("No matching historical baseline for these windows")
    rows = sorted(rows, key=lambda row: row["window_start"])
    figure = go.Figure()
    for label, key in (("Observed human edits", "edits_human"), ("Historical mean", "avg_edits_human")):
        figure.add_trace(go.Scatter(x=[r["window_start"] for r in rows], y=[r.get(key) for r in rows],
                                   name=label, mode="lines+markers"))
    return style(figure)


def relationship_graph(edges):
    if not edges:
        return empty("Link enrichment has not produced graph edges yet")
    names = sorted({edge[key] for edge in edges for key in ("source_title", "target_title")})[:60]
    positions = {name: (math.cos(2 * math.pi * i / len(names)), math.sin(2 * math.pi * i / len(names)))
                 for i, name in enumerate(names)}
    x, y = [], []
    for edge in edges:
        source, target = positions.get(edge["source_title"]), positions.get(edge["target_title"])
        if source and target:
            x.extend([source[0], target[0], None])
            y.extend([source[1], target[1], None])
    figure = go.Figure(go.Scatter(x=x, y=y, mode="lines", line=dict(color="#c4d6ce", width=1), hoverinfo="skip"))
    figure.add_trace(go.Scatter(x=[positions[n][0] for n in names], y=[positions[n][1] for n in names],
                               text=names, mode="markers", marker=dict(size=14, color=GREEN),
                               hovertemplate="%{text}<extra></extra>"))
    figure.update_layout(showlegend=False, hovermode="closest")
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
