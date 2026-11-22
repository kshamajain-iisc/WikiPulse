"""Figures for the Contextual Features page (/contextual-features) only.

Kept separate from ``dashboards.charts`` so the shared figures used by the
Milestone 6, Relationships, and System pages stay untouched. Figures accept
plain lists/dicts (never pandas) and degrade to an explanatory empty state.
"""

import math
from collections import defaultdict

import plotly.graph_objects as go

from dashboards.charts import AMBER, BLUE, CORAL, GREEN, INK, empty, style

MUTED = "#98a4b7"
LINK_GREY = "#c9d3e3"
KEPT = "#9fb0c9"


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

def spillover_hub_network(edges, max_edges=300):
    """Full-width hub-and-spoke spillover network displaying up to 300 edges.

    Alerted (source) pages sit along a wide ellipse; their linked neighbours fan
    outward. Neighbours co-edited in the same alert window are amber and sized by
    their in-window edits; neighbours linked from multiple hubs are drawn as blue
    bridge nodes between those hubs.
    """
    valid = [e for e in (edges or []) if e.get("source_title") and e.get("target_title")][:max_edges]
    if not valid:
        return empty("Link enrichment has not produced graph edges yet")

    by_source = defaultdict(list)
    for edge in valid:
        by_source[edge["source_title"]].append(edge)

    def hub_rank(item):
        rows = item[1]
        return (sum(1 for r in rows if _truthy(r.get("co_edited_in_window"))), len(rows))

    hubs = [name for name, _ in sorted(by_source.items(), key=hub_rank, reverse=True)]
    rx, ry = 2.8, 1.25
    hub_pos = {}
    for i, name in enumerate(hubs):
        angle = math.pi / 2 - 2 * math.pi * i / max(1, len(hubs))
        hx, hy = rx * math.cos(angle), ry * math.sin(angle)
        # Outward normal of the ellipse at angle so neighbour fans point cleanly outward.
        nx, ny = math.cos(angle) / rx, math.sin(angle) / ry
        out_angle = math.atan2(ny, nx)
        hub_pos[name] = (hx, hy, out_angle)

    target_hubs = defaultdict(set)
    target_info = {}
    for hub, rows in by_source.items():
        for r in rows:
            target = r["target_title"]
            if target in hub_pos:
                continue
            target_hubs[target].add(hub)
            info = target_info.setdefault(target, {"co": False, "edits": 0, "editors": 0, "types": set()})
            info["co"] = info["co"] or _truthy(r.get("co_edited_in_window"))
            info["edits"] = max(info["edits"], int(_num(r.get("target_edits_in_window"))))
            info["editors"] = max(info["editors"], int(_num(r.get("target_distinct_editors"))))
            info["types"].add(str(r.get("edge_type") or "link"))

    target_pos = {}
    fan_members = defaultdict(list)
    for target, owners in target_hubs.items():
        if len(owners) > 1:
            xs = [hub_pos[h][0] for h in owners]
            ys = [hub_pos[h][1] for h in owners]
            target_pos[target] = (0.78 * sum(xs) / len(xs), 0.78 * sum(ys) / len(ys))
        else:
            fan_members[next(iter(owners))].append(target)

    for hub, members in fan_members.items():
        hx, hy, out_angle = hub_pos[hub]
        members.sort(key=lambda t: (not target_info[t]["co"], -target_info[t]["edits"]))
        k = len(members)
        spread = min(math.pi * 1.15, 0.24 * max(1, k - 1))
        for j, target in enumerate(members):
            offset = 0 if k == 1 else (j / (k - 1) - 0.5) * spread
            radius = 0.34 if j % 2 == 0 else 0.50
            target_pos[target] = (hx + radius * math.cos(out_angle + offset), hy + radius * math.sin(out_angle + offset))

    link_x, link_y, co_x, co_y = [], [], [], []
    for hub, rows in by_source.items():
        hx, hy, _ = hub_pos[hub]
        for r in rows:
            pos = target_pos.get(r["target_title"]) or hub_pos.get(r["target_title"])
            if not pos:
                continue
            xs, ys = (co_x, co_y) if _truthy(r.get("co_edited_in_window")) else (link_x, link_y)
            xs.extend([hx, pos[0], None])
            ys.extend([hy, pos[1], None])

    figure = go.Figure()
    figure.add_trace(go.Scatter(x=link_x, y=link_y, mode="lines", line=dict(color=LINK_GREY, width=1),
                                name="Link only (no co-edit)", hoverinfo="skip"))
    figure.add_trace(go.Scatter(x=co_x, y=co_y, mode="lines", line=dict(color=AMBER, width=2),
                                name="Co-edited spillover", hoverinfo="skip"))

    def node_trace(names, name, color, line_color="white"):
        if not names:
            return
        figure.add_trace(go.Scatter(
            x=[target_pos[n][0] for n in names], y=[target_pos[n][1] for n in names], mode="markers",
            name=name,
            marker=dict(size=[7 + 2.2 * math.sqrt(target_info[n]["edits"]) for n in names],
                        color=color, line=dict(color=line_color, width=1), opacity=0.92),
            customdata=[[n, target_info[n]["edits"], target_info[n]["editors"],
                         " / ".join(sorted(target_info[n]["types"])),
                         ", ".join(_short(h, 30) for h in sorted(target_hubs[n]))] for n in names],
            hovertemplate=("<b>%{customdata[0]}</b><br>Edits in alert window: %{customdata[1]}"
                           "<br>Distinct editors: %{customdata[2]}<br>Relation: %{customdata[3]}"
                           "<br>Linked from: %{customdata[4]}<extra></extra>"),
        ))

    single = [t for t in target_pos if len(target_hubs[t]) == 1]
    node_trace([t for t in single if not target_info[t]["co"]], "Neighbour (quiet)", LINK_GREY, "#aab6c9")
    node_trace([t for t in single if target_info[t]["co"]], "Neighbour (co-edited)", AMBER)
    node_trace([t for t in target_pos if len(target_hubs[t]) > 1], "Bridge (linked by ≥2 alerts)", BLUE)

    hub_stats = {h: (sum(1 for r in by_source[h] if _truthy(r.get("co_edited_in_window"))), len(by_source[h]))
                 for h in hubs}
    figure.add_trace(go.Scatter(
        x=[hub_pos[h][0] for h in hubs], y=[hub_pos[h][1] for h in hubs], mode="markers+text",
        name="Alerted page (hub)", text=[_short(h, 22) for h in hubs],
        textposition=["top center" if hub_pos[h][1] >= 0 else "bottom center" for h in hubs],
        textfont=dict(size=10, color=INK),
        marker=dict(size=[16 + 2.5 * math.sqrt(hub_stats[h][0]) for h in hubs], color=GREEN,
                    line=dict(color="white", width=2), symbol="circle"),
        customdata=[[h, hub_stats[h][0], hub_stats[h][1],
                     100 * hub_stats[h][0] / hub_stats[h][1] if hub_stats[h][1] else 0] for h in hubs],
        hovertemplate=("<b>%{customdata[0]}</b> (alerted)<br>Co-edited neighbours: %{customdata[1]} of "
                       "%{customdata[2]} (%{customdata[3]:.0f}%)<extra></extra>"),
    ))
    figure.update_layout(hovermode="closest")
    figure.update_xaxes(visible=False)
    figure.update_yaxes(visible=False, scaleanchor="x")
    figure = style(figure, 600)
    figure.update_layout(legend=dict(orientation="h", y=-0.02, x=0, yanchor="top", font=dict(size=11)),
                         margin=dict(l=8, r=8, t=8, b=42))
    return figure


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
    return style(figure, 380)


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


# ---------------------------------------------------------------------------
# Simplified Lift Wing Revert Risk (2 charts)
# ---------------------------------------------------------------------------

def revert_risk_f1_by_threshold(m6_revert_risk):
    """F1 vs decision threshold for Composite, Lift Wing ML, and Heuristic scorers."""
    exp5 = (m6_revert_risk or {}).get("experiment_5_threshold_calibration", {}) or {}
    series = []
    for name, key, color, op_key in (
        ("Composite", "composite_risk_curve", GREEN, "optimal_composite_threshold"),
        ("Lift Wing ML", "liftwing_ml_curve", BLUE, "optimal_ml_threshold"),
        ("Heuristic", "heuristic_risk_curve", AMBER, None),
    ):
        curve = sorted(exp5.get(key) or [], key=lambda r: _num(r.get("threshold")))
        if curve:
            best = max(curve, key=lambda r: _num(r.get("f1_pct")))
            series.append((name, curve, color, exp5.get(op_key) or best, best))
    if not series:
        return empty("Multi-model F1 calibration appears after running revert_risk_monitor.py")

    figure = go.Figure()
    for name, curve, color, _, best in series:
        figure.add_trace(go.Scatter(
            x=[_num(r.get("threshold")) for r in curve], y=[_num(r.get("f1_pct")) for r in curve],
            mode="lines+markers", name=name, line=dict(color=color, width=2.6), marker=dict(size=7),
            customdata=[[_num(r.get("precision_pct")), _num(r.get("recall_pct"))] for r in curve],
            hovertemplate=f"{name}: F1 %{{y:.1f}}% (Precision %{{customdata[0]:.0f}}% · Recall %{{customdata[1]:.0f}}%)<extra></extra>",
        ))
        figure.add_trace(go.Scatter(
            x=[_num(best.get("threshold"))], y=[_num(best.get("f1_pct"))], mode="markers+text",
            marker=dict(symbol="star", size=15, color=color, line=dict(color="white", width=1.5)),
            text=[f"{_num(best.get('f1_pct')):.1f}%"], textposition="top center",
            textfont=dict(size=11, color=color), showlegend=False, hoverinfo="skip",
        ))
    tau = _num((exp5.get("optimal_composite_threshold") or {}).get("threshold"), 0.65)
    figure.add_vline(x=tau, line_dash="dot", line_color=GREEN,
                     annotation_text=f"Calibrated τ={tau:.2f}", annotation_position="bottom left",
                     annotation_font=dict(size=11, color=GREEN))
    figure.add_vline(x=0.80, line_dash="dash", line_color=MUTED, annotation_text="Static τ=0.80",
                     annotation_position="bottom right", annotation_font=dict(size=11, color=MUTED))
    figure.update_xaxes(title_text="Decision threshold (τ)", range=[0.15, 0.95], dtick=0.15)
    figure.update_yaxes(title_text="F1 score (%)", range=[0, 100])
    return style(figure, 360)


def revert_risk_band_outcomes(risks):
    """Kept vs actually reverted (48h) counts across composite risk bands, with revert rate labels."""
    scored = [r for r in (risks or []) if r.get("composite_risk") is not None]
    if not scored:
        return empty("Per-revision risk outcomes appear after revert_risk_monitor.py writes revert_risk_eval")
    bands = [
        ("Low (<0.35)", 0.0, 0.35),
        ("Moderate (0.35–0.50)", 0.35, 0.50),
        ("Borderline (0.50–0.65)", 0.50, 0.65),
        ("Calibrated (0.65–0.80)", 0.65, 0.80),
        ("High (≥0.80)", 0.80, 1.01),
    ]
    labels, reverted_counts, kept_counts, totals, rates = [], [], [], [], []
    for label, lo, hi in bands:
        rows = [r for r in scored if lo <= _num(r.get("composite_risk")) < hi]
        rev = sum(1 for r in rows if _truthy(r.get("is_reverted_actual")))
        kept = len(rows) - rev
        labels.append(label)
        reverted_counts.append(rev)
        kept_counts.append(kept)
        totals.append(len(rows))
        rates.append(100 * rev / len(rows) if rows else 0.0)

    figure = go.Figure()
    figure.add_trace(go.Bar(
        x=labels, y=reverted_counts, name="Reverted ≤48h", marker_color=CORAL,
        customdata=[[t, r] for t, r in zip(totals, rates)],
        hovertemplate="<b>%{x}</b><br>Reverted: %{y} of %{customdata[0]} (%{customdata[1]:.1f}%)<extra></extra>",
    ))
    figure.add_trace(go.Bar(
        x=labels, y=kept_counts, name="Kept", marker_color=KEPT,
        text=[f"{r:.0f}% rev" if t else "" for t, r in zip(totals, rates)],
        textposition="outside", textfont=dict(size=11, color=INK), cliponaxis=False,
        customdata=[[t, r] for t, r in zip(totals, rates)],
        hovertemplate="<b>%{x}</b><br>Kept: %{y} of %{customdata[0]}<extra></extra>",
    ))
    figure.update_layout(barmode="stack", hovermode="closest", bargap=0.32)
    figure.update_yaxes(title_text="Scored revisions", range=[0, max(totals or [1]) * 1.18])
    figure.update_xaxes(tickfont=dict(size=11))
    return style(figure, 360)
