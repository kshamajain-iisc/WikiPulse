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
        # Poll live views often enough to feel live; static routes disable it.
        dcc.Interval(id="refresh", interval=15000, n_intervals=0),
        dcc.Interval(id="health-refresh", interval=10000, n_intervals=0),
        dcc.Interval(id="replay-clock", interval=15000, n_intervals=0, disabled=True),
        dcc.Store(id="refresh-busy", data=False),
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
            html.Div([
                html.Span(className="loading-spinner"),
                html.Div([
                    html.Strong("Loading dashboard", id="loading-title"),
                    html.Span("Preparing the requested view…", id="loading-description"),
                ], className="loading-copy"),
            ], id="loading-banner", className="loading-banner", role="status",
               **{"aria-live": "polite"}, style={"display": "none"}),
            html.Header([
                html.Div([html.Span(className="status-dot"), html.Span("ENWIKI / OBSERVATORY")], className="workspace-name"),
                html.Div([
                    html.Div([
                        html.Span("Checking stream…", id="stream-health-label", className="health-tag health-unknown"),
                        html.Button("Restart stream", id="restart-stream", n_clicks=0, disabled=True,
                                    className="restart-button"),
                        html.Span("", id="restart-feedback", role="status", className="restart-feedback"),
                    ], className="stream-actions"),
                    dcc.Checklist(
                        id="theme-toggle", options=[{"label": "Dark mode", "value": "dark"}], value=[],
                        persistence=True, persistence_type="local", className="theme-toggle",
                    ),
                ], className="header-actions"),
            ], className="topbar"),
            html.Div([
                html.Div([
                    html.Div([html.Label("ACTIVITY WINDOW"), dcc.RadioItems(
                        id="duration", value="5m", inline=True, className="window-picker",
                        options=[{"label": "5 min", "value": "5m"},
                                 {"label": "1 hour", "value": "1h"},
                                 {"label": "6 hours", "value": "6h"}],
                    )], className="control window-control"),
                    html.Div([html.Label("RESULTS"), dcc.Dropdown(id="view", value="live", clearable=False,
                        options=[{"label": "Live preview", "value": "live"},
                                 {"label": "Finalized windows", "value": "finalized"},
                                 {"label": "Replay stored activity", "value": "replay"}])], className="control"),
                    html.Div([
                        html.Label("REPLAY"),
                        dcc.Checklist(id="replay-playing", options=[{"label": "Play", "value": "play"}],
                                      value=[], className="replay-play"),
                    ], id="replay-controls", className="control replay-control", style={"display": "none"}),
                    html.Div([
                        html.Label("SPEED"),
                        dcc.Dropdown(id="replay-speed", value=1, clearable=False,
                            options=[{"label": "1×", "value": 1}, {"label": "5×", "value": 5},
                                     {"label": "20×", "value": 20}]),
                    ], id="replay-speed-control", className="control replay-control", style={"display": "none"}),
                    html.Div([html.Label("ARTICLE"), dcc.Dropdown(id="article", placeholder="Leading article / choose…",
                        options=[], clearable=True)], className="control article-control"),
                    html.Div([
                        html.Label("HISTORICAL RANGE"),
                        dcc.Dropdown(id="historical-range", value="1m", clearable=False,
                            options=[{"label": "15 days", "value": "15d"},
                                     {"label": "1 month", "value": "1m"},
                                     {"label": "3 months", "value": "3m"}]),
                    ], className="control historical-range-control"),
                    dcc.Checklist(id="pause", options=[{"label": "Pause refresh", "value": "pause"}], value=[], className="pause-control"),
                ], className="controls"),
                html.Div("Starting dashboard queries…", id="source-status", className="source-status", role="status"),
                html.Div(id="page-body", tabIndex=-1),
                html.Footer("WIKIPULSE  /  DATA ENGINEERING AT SCALE  ·  Evidence first. Live times shown in India Standard Time (IST)."),
            ], className="workspace"),
        ], className="main"),
    ], className="app-shell", id="app-shell")
