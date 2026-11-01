"""Application shell, navigation, and shared controls."""

from dash import dcc, html

ROUTES = [
    ("/", "01", "Live pulse"),
    ("/investigation", "02", "Page investigation"),
    ("/historical", "03", "Historical intelligence"),
    ("/relationships", "04", "Relationships & map"),
    ("/system", "05", "Engineering & health"),
]


def navigation(pathname="/"):
    return [dcc.Link([html.Span(number, className="nav-number"), html.Span(label)],
                     href=path, className="nav-link active" if path == pathname else "nav-link")
            for path, number, label in ROUTES]


def shell():
    return html.Div([
        dcc.Location(id="location", refresh="callback-nav"),
        dcc.Interval(id="refresh", interval=30000, n_intervals=0),
        html.A("Skip to dashboard", href="#page-body", className="skip-link"),
        html.Aside([
            html.Div([html.Span("W", className="brand-mark"),
                      html.Div([html.Strong("WikiPulse"), html.Small("KNOWLEDGE INTELLIGENCE")])], className="brand"),
            html.Div("WORKSPACE", className="sidebar-label"),
            html.Nav(navigation(), id="navigation"),
            html.Div([html.Div("A living knowledge system", className="sidebar-caption"),
                      html.P("Observe the edits. Understand the change."),
                      html.Div([html.Span(className="status-dot"), " English Wikipedia"], className="sidebar-source")],
                     className="sidebar-bottom"),
        ], className="sidebar"),
        html.Main([
            html.Header([
                html.Div([html.Span(className="status-dot"), html.Span("ENWIKI / OBSERVATORY")], className="workspace-name"),
                html.Div([html.Span("Spark processing"), html.Span("UTC timestamps"),
                          html.Span("Dash + Plotly", className="header-badge")], className="header-meta"),
            ], className="topbar"),
            html.Div([
                html.Div([
                    html.Div([html.Label("WINDOW"), dcc.Dropdown(id="duration", value="5m", clearable=False,
                        options=[{"label": "Five minutes", "value": "5m"}, {"label": "One hour", "value": "1h"}])], className="control"),
                    html.Div([html.Label("RESULTS"), dcc.Dropdown(id="view", value="live", clearable=False,
                        options=[{"label": "Live preview", "value": "live"}, {"label": "Finalized windows", "value": "finalized"}])], className="control"),
                    html.Div([html.Label("ARTICLE"), dcc.Dropdown(id="article", placeholder="Leading article / choose…",
                        options=[], clearable=True)], className="control article-control"),
                    html.Div([html.Label("HISTORY DATE RANGE"), dcc.DatePickerRange(id="dates", clearable=True,
                        display_format="DD MMM YYYY", start_date_placeholder_text="Start", end_date_placeholder_text="End")], className="control dates-control"),
                    dcc.Checklist(id="pause", options=[{"label": "Pause refresh", "value": "pause"}], value=[], className="pause-control"),
                ], className="controls"),
                html.Div("Connecting to measured activity…", id="source-status", className="source-status", role="status"),
                dcc.Loading(html.Div(id="page-body", tabIndex=-1), type="circle", color="#16836f", delay_show=300),
                html.Footer("WIKIPULSE  /  DATA ENGINEERING AT SCALE  ·  Evidence first. All times UTC."),
            ], className="workspace"),
        ], className="main"),
    ], className="app-shell")
