"""Figures for the Contextual Features page (/contextual-features) only.

Kept separate from ``dashboards.charts`` so the shared figures used by the
Milestone 6, Relationships, and System pages stay untouched. Figures accept
plain lists/dicts (never pandas) and degrade to an explanatory empty state.
"""

import math
from collections import defaultdict

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from dashboards.charts import AMBER, BLUE, CORAL, GREEN, INK, empty, style

MUTED = "#98a4b7"
LINK_GREY = "#c9d3e3"
KEPT = "#8fa3c2"
# composite_risk = 0.72 * ml_revert_risk + 0.28 * heuristic_risk (src/enrichment/revert_risk_monitor.py)
COMPOSITE_ML_WEIGHT = 0.72
COMPOSITE_HEURISTIC_WEIGHT = 0.28
_TTR_TICKS = [(10, "10s"), (60, "1m"), (300, "5m"), (600, "10m"), (1800, "30m"),
              (3600, "1h"), (6 * 3600, "6h"), (24 * 3600, "24h"), (48 * 3600, "48h")]


def _short(text, limit=26):
    text = str(text or "—")
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _truthy(value):
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    return bool(value)


def _num(value, default=0.0):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return default if math.isnan(out) else out


# ---------------------------------------------------------------------------
# Topic graph & spillover
# ---------------------------------------------------------------------------

@@NETWORK@@


def spillover_ratio_bars(edges, top=12):
    """Per alerted page: co-edited vs link-only neighbours, sorted by spillover."""
    if not edges:
        return empty("Spillover ratios appear after graph enrichment runs")
    stats = defaultdict(lambda: {"co": 0, "link": 0, "edits": 0})
    for edge in edges:
        source = edge.get("source_title")
        if not source:
            continue
        if _truthy(edge.get("co_edited_in_window")):
            stats[source]["co"] += 1
            stats[source]["edits"] += int(_num(edge.get("target_edits_in_window")))
        else:
            stats[source]["link"] += 1
    rows = sorted(stats.items(), key=lambda kv: (kv[1]["co"], kv[1]["co"] + kv[1]["link"]), reverse=True)[:top][::-1]
    if not rows:
        return empty("Spillover ratios appear after graph enrichment runs")
    labels = [_short(name, 28) for name, _ in rows]
    custom = [[name, s["co"], s["link"], 100 * s["co"] / max(1, s["co"] + s["link"]), s["edits"]] for name, s in rows]
    template = ("<b>%{customdata[0]}</b><br>Co-edited: %{customdata[1]} · Link only: %{customdata[2]}"
                "<br>Spillover share: %{customdata[3]:.0f}%<br>Neighbour edits in window: %{customdata[4]:,}<extra></extra>")
    figure = go.Figure()
    figure.add_trace(go.Bar(
        y=list(range(len(rows))), x=[s["co"] for _, s in rows], orientation="h", name="Co-edited neighbours",
        marker_color=AMBER, customdata=custom, hovertemplate=template,
        text=[f"{c[3]:.0f}%" if c[1] else "" for c in custom], textposition="inside", insidetextanchor="start",
        textfont=dict(color="white", size=11),
    ))
    figure.add_trace(go.Bar(
        y=list(range(len(rows))), x=[s["link"] for _, s in rows], orientation="h", name="Link only",
        marker_color=LINK_GREY, customdata=custom, hovertemplate=template,
    ))
    figure.update_layout(barmode="stack", hovermode="closest", bargap=0.28)
    figure.update_yaxes(tickmode="array", tickvals=list(range(len(rows))), ticktext=labels,
                        automargin=True, tickfont=dict(size=11), gridcolor="rgba(0,0,0,0)")
    figure.update_xaxes(title_text="Linked neighbours", showgrid=True, gridcolor="#e5eaf3")
    return style(figure, 520)


@@RISK@@


# ---------------------------------------------------------------------------
# Revision diffs & subject geography
# ---------------------------------------------------------------------------

_REVERT_WORDS = ("revert", "undid", "rv ", "rvv", "vandal", "restor")


def revision_diff_profile(diffs):
    """Lines added vs removed per compared revision (log–log), sized by external links added."""
    usable = [d for d in (diffs or []) if d.get("added_lines_count") is not None]
    if not usable:
        return empty("Revision diff profile appears after graph_and_diffs.py populates revision_diffs")
    figure = go.Figure()
    split = (
        ("Revert / vandalism-fix comment", [d for d in usable if any(w in str(d.get("comment") or "").lower() for w in _REVERT_WORDS)], CORAL),
        ("Other edits", [d for d in usable if not any(w in str(d.get("comment") or "").lower() for w in _REVERT_WORDS)], BLUE),
    )
    for label, rows, color in split:
        if not rows:
            continue
        figure.add_trace(go.Scatter(
            x=[int(_num(d.get("added_lines_count"))) + 1 for d in rows],
            y=[int(_num(d.get("removed_lines_count"))) + 1 for d in rows],
            mode="markers", name=f"{label} ({len(rows)})",
            marker=dict(color=color, opacity=0.72, line=dict(color="white", width=0.8),
                        size=[8 + 3 * math.sqrt(_num(d.get("external_links_added"))) for d in rows],
                        symbol=["circle" if d.get("compare_status") in (None, "success") else "x" for d in rows]),
            customdata=[[d.get("title"), d.get("user"), int(_num(d.get("delta_bytes"))),
                         int(_num(d.get("added_lines_count"))), int(_num(d.get("removed_lines_count"))),
                         int(_num(d.get("external_links_added"))), d.get("compare_status") or "—",
                         _short(d.get("comment"), 80)] for d in rows],
            hovertemplate=("<b>%{customdata[0]}</b> · %{customdata[1]}<br>+%{customdata[3]} / −%{customdata[4]} lines · "
                           "Δ %{customdata[2]:,} B · %{customdata[5]} ext. links<br>Status: %{customdata[6]}"
                           "<br><i>%{customdata[7]}</i><extra></extra>"),
        ))
    figure.add_trace(go.Scatter(x=[1, 1000], y=[1, 1000], mode="lines", line=dict(color="#dfe5ef", dash="dot"),
                                hoverinfo="skip", showlegend=False))
    figure.update_layout(hovermode="closest")
    figure.update_xaxes(type="log", title_text="Lines added (+1, log)", showgrid=True, gridcolor="#eef2f8")
    figure.update_yaxes(type="log", title_text="Lines removed (+1, log)")
    return style(figure, 360)


def country_topic_bars(countries, top=12):
    """Top countries by alerted topics; colour = peak anomaly score, label = human edits."""
    if not countries:
        return empty("Country breakdown appears after running Milestone 6 geospatial enrichment")
    rows = sorted(countries, key=lambda r: (_num(r.get("active_topics")), _num(r.get("total_edits_human"))),
                  reverse=True)[:top][::-1]
    scores = [_num(r.get("max_score")) for r in rows]
    figure = go.Figure(go.Bar(
        x=[_num(r.get("active_topics")) for r in rows], y=list(range(len(rows))), orientation="h",
        marker=dict(color=scores, colorscale=[[0, "#c5cffc"], [1, GREEN]],
                    cmin=min(scores) - 1, cmax=max(scores) + 0.1,
                    colorbar=dict(title=dict(text="Peak<br>score", font=dict(size=11)), thickness=10, len=0.8)),
        text=[f"{int(_num(r.get('total_edits_human'))):,} edits" for r in rows], textposition="outside",
        textfont=dict(size=10, color=MUTED), cliponaxis=False,
        customdata=[[r.get("country_or_region"), int(_num(r.get("total_edits_human"))), _num(r.get("max_score"))] for r in rows],
        hovertemplate=("<b>%{customdata[0]}</b><br>%{x:,} alerted topics · %{customdata[1]:,} human edits"
                       "<br>Peak anomaly score %{customdata[2]:.1f}<extra></extra>"),
    ))
    figure.update_yaxes(tickmode="array", tickvals=list(range(len(rows))),
                        ticktext=[_short(r.get("country_or_region"), 24) for r in rows], automargin=True,
                        tickfont=dict(size=11), gridcolor="rgba(0,0,0,0)")
    figure.update_xaxes(title_text="Alerted topics", showgrid=True, gridcolor="#eef2f8",
                        range=[0, max(_num(r.get("active_topics")) for r in rows) * 1.22])
    figure.update_layout(hovermode="closest")
    return style(figure, 380)
