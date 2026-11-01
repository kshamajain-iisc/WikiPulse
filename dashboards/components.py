"""Small shared presentation components; no data processing or Spark imports."""

from urllib.parse import quote

from dash import dcc, html


def number(value, suffix=""):
    if value is None:
        return "—"
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}m{suffix}"
    if abs(value) >= 10_000:
        return f"{value / 1000:.1f}k{suffix}"
    return f"{value:,.0f}{suffix}"


def heading(eyebrow, title, description):
    return html.Div([html.Div(eyebrow, className="eyebrow"), html.H1(title), html.P(description)],
                    className="page-heading")


def metric(label, value, note, accent=False):
    return html.Div([html.Div(label, className="metric-label"),
                     html.Div(value, className="metric-value"),
                     html.Div(note, className="metric-note")],
                    className="metric-card" + (" accent-card" if accent else ""))


def metric_row(metrics, historical=False):
    edits = metrics.get("edits")
    bots = metrics.get("bots") or 0
    coverage = (metrics.get("known_sizes", 0) / edits * 100) if edits else None
    return html.Div([
        metric("ARTICLE EDITS", number(edits), "Across the selected data scope", True),
        metric("ACTIVE ARTICLES", number(metrics.get("pages")), "Distinct page identities"),
        metric("HUMAN ACTIVITY", f"{100 * (edits - bots) / edits:.1f}%" if edits else "—",
               "Share of observed edits"),
        metric("CONTENT MOVEMENT", number(metrics.get("gross_bytes"), " B"),
               "Known sizes only" if historical else f"{coverage:.0f}% size coverage" if coverage is not None else "Waiting for known sizes"),
    ], className="metric-grid")


def panel(title, subtitle, content, class_name=""):
    return html.Section([html.Div([html.H3(title), html.P(subtitle)], className="panel-heading"), content],
                        className="panel " + class_name)


def graph(figure, graph_id=None):
    """Reserve chart space explicitly while allowing the width to follow its card.

    Dash's default 100% graph height cannot resolve against an auto-height grid
    card. Set the container height from the figure so charts and empty states
    participate in normal layout instead of collapsing underneath the heading.
    """
    height = figure.layout.height or 300
    options = {
        "figure": figure,
        "responsive": True,
        "className": "dashboard-graph",
        "style": {"height": f"{height}px", "minHeight": f"{height}px", "width": "100%"},
        "config": {"displayModeBar": False},
    }
    if graph_id:
        options["id"] = graph_id
    return dcc.Graph(**options)


def notice(title, description):
    return html.Div([html.Span("○", className="notice-icon"),
                     html.Div([html.Strong(title), html.P(description)])], className="notice")


def table(rows, columns, article_links=False):
    """Render bounded server results safely as text; no untrusted HTML."""
    if not rows:
        return notice("No records yet", "This section populates when its source table is available.")
    body = []
    for row in rows:
        cells = []
        for label, key in columns:
            value = row.get(key)
            if isinstance(value, list):
                value = "; ".join(str(item) for item in value)
            elif isinstance(value, dict):
                value = str(value)
            elif isinstance(value, float):
                value = f"{value:,.2f}"
            elif isinstance(value, int):
                value = f"{value:,}"
            if value is None:
                value = "—"
            if article_links and key == "title":
                value = dcc.Link(value, href="/investigation?title=" + quote(row["title"], safe=""))
            cells.append(html.Td(value))
        body.append(html.Tr(cells))
    return html.Div(html.Table([
        html.Thead(html.Tr([html.Th(label) for label, _ in columns])), html.Tbody(body)
    ]), className="table-scroll")
