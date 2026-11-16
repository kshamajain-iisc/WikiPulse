"""Controls and asynchronous rendering scoped to Pulse and Investigation."""

import json
import hashlib
import os
import uuid
from bisect import bisect_left, bisect_right
from datetime import date
from urllib.parse import parse_qs, urlencode, quote

from dash import Input, Output, State, ctx, dcc, html, no_update
from dash.exceptions import PreventUpdate

from dashboards import charts
from dashboards.activity_jobs import TERMINAL
from dashboards.components import graph, heading, metric_row, notice, panel, pill_row
from dashboards.pages import investigation, live
from dashboards.time_utils import format_india_time
from src.local_supervisor import stream_health
from src.stream_control import read_json, request_action

ROUTES = ("/", "/investigation")


def activity_shell():
    return html.Div([
        dcc.Store(id="pulse-owner", data=uuid.uuid4().hex, storage_type="session"),
        dcc.Store(id="pulse-request"), dcc.Store(id="pulse-rendered"),
        dcc.Store(id="pulse-clock"), dcc.Store(id="pulse-frame"), dcc.Store(id="pulse-seed"),
        dcc.Store(id="pulse-last"), dcc.Store(id="pulse-dates"),
        dcc.Store(id="pulse-follow-state"),
        dcc.Store(id="pulse-kafka-run"),
        dcc.Store(id="pulse-console-preference", data=False),
        dcc.Interval(id="pulse-tick", interval=1000, disabled=True),
        dcc.Interval(id="pulse-job-tick", interval=1000, disabled=True),
        dcc.Interval(id="pulse-kafka-tick", interval=2000, disabled=True),
        html.Div([
            html.Div([html.Label("SOURCE"), dcc.RadioItems(id="pulse-mode", value="live", inline=True,
                options=[{"label": "Live", "value": "live"}, {"label": "Stored activity", "value": "stored"}])], className="pulse-source-control"),
            html.Div([html.Label("ACTIVITY HORIZON"), dcc.RadioItems(id="pulse-horizon", value="5m", inline=True,
                options=[{"label": "5 min", "value": "5m"}, {"label": "1 hour", "value": "1h"}, {"label": "6 hours", "value": "6h"}])], className="pulse-horizon-control"),
            html.Div([html.Label("ARTICLE"), dcc.Dropdown(id="pulse-article", options=[], placeholder="Search or select an article…",
                                                       clearable=True)], className="pulse-article-control"),
            html.Div([html.Label("CHART REFRESH"), dcc.Dropdown(id="pulse-refresh-rate", value=30, clearable=False,
                options=[{"label": "10 seconds", "value": 10}, {"label": "30 seconds", "value": 30},
                         {"label": "1 minute", "value": 60}], persistence=True, persistence_type="local"),
                dcc.Checklist(id="pulse-freeze", options=[{"label": "Freeze display", "value": "freeze"}], value=[])],
                className="pulse-display-control", title="Refreshes charts while Live or replay is running. Capture and replay speed continue independently; day summaries refresh on selection or retry."),
            html.Div([
            html.Div([html.Label("CAPTURE DATE", htmlFor="pulse-date"), html.Div([
                html.Button("‹", id="pulse-date-previous", n_clicks=0, className="pulse-date-step", title="Previous captured date", **{"aria-label": "Previous captured date"}),
                dcc.DatePickerSingle(id="pulse-date", date=None, display_format="DD MMM YYYY", first_day_of_week=1,
                                     placeholder="Choose date", clearable=False, day_size=34, show_outside_days=False),
                html.Button("›", id="pulse-date-next", n_clicks=0, className="pulse-date-step", title="Next captured date", **{"aria-label": "Next captured date"}),
            ], className="pulse-date-picker"), html.Small(id="pulse-date-hint", className="pulse-date-hint")], className="pulse-date-control"),
            html.Div([html.Label("VIEW"), dcc.RadioItems(id="pulse-kind", value="summary", inline=True,
                options=[{"label": "Day summary", "value": "summary"}, {"label": "Replay", "value": "replay"}])], className="pulse-kind-control"),
            html.Div([html.Label("DAY TIMEZONE"), dcc.RadioItems(id="pulse-zone", value="IST", inline=True,
                options=[{"label": "IST", "value": "IST"}, {"label": "UTC", "value": "UTC"}])], className="pulse-zone-control"),
            ], id="pulse-stored-controls", style={"display": "none"}),
        ], id="pulse-toolbar", className="pulse-toolbar is-live"),
        html.Div([
            html.Button("Start replay", id="pulse-play", n_clicks=0, className="pulse-button primary"),
            html.Button("Stop replay", id="pulse-cancel", n_clicks=0, className="pulse-button"),
            html.Span("Ready", id="pulse-replay-state", role="status", className="pulse-replay-state"),
            html.Div([html.Strong("00:00:00", id="pulse-clock-label"), html.Small("", id="pulse-eta")], className="pulse-clock-copy"),
            html.Details([html.Summary("Replay settings"),
                html.Div([
                    html.Div([html.Label("PROCESSING"), dcc.Dropdown(id="pulse-replay-engine", value="saved", clearable=False,
                        options=[{"label": "Saved playback", "value": "saved"},
                                 {"label": "Kafka & Spark demo", "value": "kafka"}])], className="pulse-engine"),
                    html.Div([html.Label("PLAYBACK SPEED"), dcc.Dropdown(id="pulse-speed", value=50, clearable=False,
                        options=[{"label": f"{n}×", "value": n} for n in (1, 5, 20, 24, 48, 50, 100)])], className="pulse-speed"),
                ], className="pulse-replay-settings-row"),
                html.P("Start begins at the first captured event, or resumes your position. Stop keeps that position. Kafka & Spark uses an isolated pipeline; Stop drains its admitted events. Stop before changing processing."),
                html.Div([html.Label("SEEK WITHIN SELECTED DAY"), dcc.Slider(id="pulse-seek", min=0, max=86400, step=1, value=0,
                    marks={0: "00:00", 21600: "06:00", 43200: "12:00", 64800: "18:00", 86400: "24:00"}, updatemode="mouseup")], className="pulse-seek"),
            ], className="pulse-kafka-panel"),
            html.Div(id="pulse-kafka-status", role="status", className="pulse-kafka-status"),
        ], id="pulse-replay-controls", className="pulse-replay-bar", style={"display": "none"}),
        html.Div([
            html.Div([
                html.Label("WIKIPEDIA STREAM START"),
                dcc.RadioItems(id="pulse-source-start", value="live", inline=True,
                    options=[{"label": "Live now", "value": "live"}, {"label": "Since timestamp", "value": "since"}]),
                html.Div([html.Label("SINCE (UTC)"),
                    dcc.DatePickerSingle(id="pulse-source-date", display_format="DD MMM YYYY", placeholder="Choose UTC date", clearable=False),
                    html.Label(["UTC time", dcc.Input(id="pulse-source-time", type="text", value="00:00:00", placeholder="HH:MM:SS", maxLength=8)])],
                    id="pulse-source-since-control", style={"display": "none"}),
                html.Button("Apply stream source", id="pulse-apply-source", n_clicks=0, className="pulse-button"),
                html.Small("Changes shared Wikipedia capture. Since fetches available history, then continues live. Charts keep the selected activity horizon; past dates appear in Stored activity. Upstream retention varies (typically 7–31 days)."),
                html.Div(id="pulse-source-command", role="status"),
                html.Div(id="pulse-source-status", role="status"),
            ], className="pulse-source-start"),
            dcc.Checklist(id="pulse-finalized", value=[], options=[{"label": "Show finalized windows (watermark-delayed)", "value": "yes"}]),
        ], id="pulse-live-controls", className="pulse-advanced"),
        html.Div("Display frozen. Capture and processing continue. Clear Freeze display to load the latest snapshot.",
                 id="pulse-frozen-note", className="notice", role="status", style={"display": "none"}),
        html.Div([html.Span("Preparing activity…", id="pulse-status", role="status"),
                  html.Button("Refresh / Retry", id="pulse-retry", n_clicks=0, className="pulse-button")], className="pulse-status-line"),
        html.Div([html.Div([
        html.Div([html.Div(className="pulse-skeleton-title"),
                  html.Div([html.Div(className="pulse-skeleton-card") for _ in range(4)], className="metric-grid"),
                  html.Div([html.Div(className="pulse-skeleton-chart") for _ in range(2)], className="chart-grid")],
                 id="pulse-skeleton", className="pulse-skeleton", role="status", **{"aria-label": "Loading activity charts"}),
        html.Div(id="pulse-core"),
        html.Div(id="pulse-evidence"),
        ], id="pulse-content", className="pulse-content"),
        html.Details([
            html.Summary([html.Span(className="pulse-terminal-dot"), html.Strong("Processing console"),
                          html.Span("Ready", id="pulse-console-summary")], id="pulse-console-toggle", n_clicks=0),
            html.Div(id="pulse-measurements", className="pulse-measurements"),
            html.P("Latest refresh above; refresh history below. Data decoded is the measured event payload used for analysis, not disk or network traffic. History is shared by local pages: latest 500 entries shown, up to 3 MB saved on this computer across restarts.", className="pulse-console-explainer"),
            dcc.Checklist(id="pulse-follow", value=["follow"], options=[{"label": "Follow latest log entries", "value": "follow"}], className="pulse-follow"),
            html.Pre("Select a view to inspect measured processing activity.", id="pulse-console-log", tabIndex=0),
            html.Div([
                html.Button("Start stream", id="pulse-start-stream", n_clicks=0, className="pulse-button"),
                html.Button("Stop stream", id="pulse-stop-stream", n_clicks=0, className="pulse-button"),
                html.A("Download history", id="pulse-log-download", href="/api/activity/console/log", target="_blank", className="pulse-button"),
                html.Small("Start/Stop affects the shared capture pipeline. Freeze display only pauses this view."),
            ], className="pulse-stream-actions"),
            html.Div(id="pulse-command-status", role="status"),
            html.Div(id="pulse-stream-metrics", className="pulse-stream-metrics"),
        ], id="pulse-console", className="pulse-console", open=False),
        ], id="pulse-results", className="pulse-results"),
    ], id="pulse-host", className="pulse-workspace", style={"display": "none"})


def activity_url(path, spec, title=None):
    query = {k: spec[k] for k in ("mode", "kind", "horizon", "zone") if k in spec}
    if spec.get("mode") == "stored":
        query.update(day=spec.get("day"), cursor=round(spec.get("cursor", 0)))
    if title:
        query["title"] = title
    return path + "?" + urlencode({k: v for k, v in query.items() if v is not None})


def decorate(node, spec):
    """Rewrite scoped links only; other screens retain their existing URLs."""
    if isinstance(node, (list, tuple)):
        for child in node:
            decorate(child, spec)
    elif hasattr(node, "to_plotly_json"):
        if isinstance(node, dcc.Link) and getattr(node, "href", "").startswith("/investigation?"):
            title = parse_qs(node.href.split("?", 1)[1]).get("title", [None])[0]
            node.href = activity_url("/investigation", spec, title)
        if isinstance(node, dcc.Graph):
            if getattr(node, "id", None) == "page-ranking":
                node.id = "pulse-ranking"
            node.figure.update_layout(uirevision=json.dumps({k: v for k, v in spec.items() if k != "cursor"}, sort_keys=True))
        decorate(getattr(node, "children", None), spec)
    return node


def render_core(data, spec):
    if spec["page"] == "live":
        original = live.render(data)
        result = original[:4] + [original[-1]]
    else:
        title = data.get("selected")
        result = [dcc.Link("← Back to Live Pulse", href=activity_url("/", spec), className="pulse-back"),
                  heading("INVESTIGATE / ARTICLE DETAIL", title or "Choose an article to investigate",
                          f"{data.get('scope', '')}. The selected article stays fixed during replay."),
                  *([html.A("Open on Wikipedia ↗", href="https://en.wikipedia.org/wiki/" + quote(title, safe=""),
                            target="_blank", rel="noopener noreferrer", className="article-link")] if title else []),
                  metric_row(data.get("metrics", {})),
                  html.Div([
                      panel("Activity over time", f"{data.get('bucket', '5 minutes')} buckets · timestamps shown in IST",
                            graph(charts.activity_timeline(data.get("timeline", []), time_range=data.get("range")))),
                      panel("Content movement", "Known byte sizes in the selected range",
                            graph(charts.bytes_timeline(data.get("timeline", []), time_range=data.get("range")))),
                  ], className="chart-grid")]
    notes = [notice("Selected scope", data["empty_message"])] if data.get("empty_message") else []
    if data.get("day_so_far") is not None:
        notes.append(pill_row([("Day total so far", f"{data['day_so_far']:,} edits"),
                              ("Rolling activity", f"{data['metrics']['edits']:,} edits")]))
    if spec["mode"] == "stored":
        notes.append(notice("Capture coverage", f"{data.get('coverage', '')} First observed: "
                            f"{format_india_time(data.get('first_observed')) or 'unavailable'}; last observed: "
                            f"{format_india_time(data.get('last_observed')) or 'unavailable'}."))
    return decorate(notes + result, spec)


def render_evidence(data, evidence, spec):
    merged = {**data, **evidence}
    merged["alerts"] = [{**r, "window_start_ist": format_india_time(r.get("window_start")),
                         "fixed_threshold_flag": r.get("fixed_threshold_flag", r.get("fixed_rule_alert")),
                         "historical_alert_flag": r.get("historical_alert_flag", r.get("historical_alert"))}
                        for r in evidence.get("alerts", [])]
    warnings = [notice("Optional evidence", message) for message in evidence.get("warnings", [])]
    if spec["page"] == "live":
        original = live.render(merged)
        return decorate(warnings + original[4:6], spec)
    original = investigation.render(merged)
    return warnings + [html.Div([panel("Current versus normal", evidence.get("baseline_period", "Matching historical baseline when available"),
                             graph(charts.baseline_comparison(evidence.get("comparisons", []), time_range=data.get("range")))),
                                  panel("Contributor balance", "Human and bot edits in the selected scope", graph(charts.bot_split(data.get("metrics", {}))))], className="chart-grid"),
                       original[3], original[-2], original[-1]]


def pretty_bytes(value):
    if value is None:
        return "unknown"
    for scale, unit in ((1_000_000, "MB"), (1000, "KB")):
        if value >= scale:
            return f"{value / scale:,.2f} {unit}"
    return f"{value:,} B"


def register_activity_callbacks(app, workspace):
    @app.callback(Output("pulse-source-start", "value"), Output("pulse-source-date", "date"),
                  Output("pulse-source-time", "value"), Input("location", "pathname"))
    def hydrate_source(path):
        if path not in ROUTES:
            raise PreventUpdate
        source = read_json(workspace.paths.runtime / "stream-source.json")
        since = source.get("since")
        return ("since", since[:10], since[11:19]) if since else ("live", None, "00:00:00")

    @app.callback(Output("pulse-source-since-control", "style"), Input("pulse-source-start", "value"))
    def source_fields(mode):
        return {"display": "flex" if mode == "since" else "none"}

    @app.callback(Output("pulse-source-command", "children"), Input("pulse-apply-source", "n_clicks"),
                  State("pulse-source-start", "value"), State("pulse-source-date", "date"),
                  State("pulse-source-time", "value"), State("location", "pathname"), prevent_initial_call=True)
    def configure_source(clicks, mode, day, clock, path):
        if not clicks or path not in ROUTES:
            raise PreventUpdate
        source = {"mode": mode, "since": f"{day}T{clock}Z" if day and clock else None}
        result = request_action("source", uuid.uuid4().hex, os.environ.get("WIKIPULSE_CONTROL_TOKEN"), source=source)
        workspace.history.append("source", "SOURCE", result["message"])
        return result["message"]

    @app.callback(
        Output("pulse-dates", "data"), Output("pulse-date", "date"),
        Output("pulse-mode", "value"), Output("pulse-kind", "value"), Output("pulse-horizon", "value"),
        Output("pulse-zone", "value"), Output("pulse-article", "value"), Output("pulse-seed", "data"),
        Input("location", "pathname"), Input("location", "search"),
        State("pulse-date", "date"), State("pulse-mode", "value"), State("pulse-kind", "value"),
        State("pulse-horizon", "value"), State("pulse-zone", "value"),
    )
    def hydrate(path, search, day, mode, kind, horizon, zone):
        if path not in ROUTES:
            raise PreventUpdate
        q = parse_qs((search or "").lstrip("?"))
        values = {"mode": (q.get("mode") or [mode or "live"])[0],
                  "kind": (q.get("kind") or [kind or "summary"])[0],
                  "horizon": (q.get("horizon") or [horizon or "5m"])[0],
                  "zone": (q.get("zone") or [zone or "IST"])[0]}
        for name, allowed in (("mode", ("live", "stored")), ("kind", ("summary", "replay")),
                              ("horizon", ("5m", "1h", "6h")), ("zone", ("IST", "UTC"))):
            if values[name] not in allowed:
                values[name] = allowed[0]
        options = workspace.reader.catalog(values["zone"])
        selected = (q.get("day") or [day])[0]
        try:
            if selected:
                date.fromisoformat(selected)
        except (TypeError, ValueError):
            selected = None
        if not selected:
            selected = options[0]["value"] if options else None
        try:
            cursor = max(0, min(86400, float(q.get("cursor", [0])[0])))
        except ValueError:
            cursor = 0
        return ([o["value"] for o in options], selected, values["mode"], values["kind"], values["horizon"], values["zone"],
                q.get("title", [None])[0], {"cursor": cursor, "url": (path or "/") + (search or "")})

    @app.callback(Output("pulse-date", "date", allow_duplicate=True),
                  Input("pulse-date-previous", "n_clicks"), Input("pulse-date-next", "n_clicks"),
                  State("pulse-date", "date"), State("pulse-zone", "value"), prevent_initial_call=True)
    def step_date(previous, following, day, zone):
        days = sorted(o["value"] for o in workspace.reader.catalog(zone))
        index = bisect_left(days, day or "") - 1 if ctx.triggered_id == "pulse-date-previous" else bisect_right(days, day or "")
        return days[index] if 0 <= index < len(days) else no_update

    @app.callback(Output("pulse-dates", "data", allow_duplicate=True), Input("pulse-zone", "value"),
                  Input("pulse-mode", "value"), Input("health-refresh", "n_intervals"), State("location", "pathname"),
                  prevent_initial_call=True)
    def captured_dates(zone, mode, tick, path):
        if path not in ROUTES or mode != "stored":
            raise PreventUpdate
        return [o["value"] for o in workspace.reader.catalog(zone)]

    @app.callback(Output("pulse-date-previous", "disabled"), Output("pulse-date-next", "disabled"),
                  Output("pulse-date-hint", "children"), Input("pulse-dates", "data"), Input("pulse-date", "date"))
    def date_hint(days, day):
        days = sorted(days or [])
        observed = day in days
        hint = f"{len(days)} captured dates · coverage unverified" if observed else "No known capture · choose another date"
        return (not any(d < (day or "") for d in days), not any(d > (day or "") for d in days), hint)

    app.clientside_callback(
        """function(path,mode,kind,day,horizon,zone,search,request,rendered,kafka,freeze,engine){
          const active = path === '/' || path === '/investigation';
          const q = new URLSearchParams(search || '');
          const run = engine==='kafka'&&kafka&&kafka.attached&&mode==='stored'&&kind==='replay'&&kafka.day===day&&kafka.zone===zone ? kafka.id : null;
          const scope = JSON.stringify([path,mode,kind,day,horizon,zone,q.get('title'),run]);
          const same = rendered && rendered.scope === scope;
          return [active?{display:'block'}:{display:'none'},!active,
            mode==='stored'?{display:'contents'}:{display:'none'},mode==='stored'&&kind==='replay'?{}:{display:'none'},
            mode==='live'?{}:{display:'none'},same?{}:{display:'none'},same?{display:'none'}:{},
            same?{}:{display:'none'},(freeze||[]).includes('freeze')?{}:{display:'none'},
            'pulse-toolbar '+(mode==='stored'?'is-stored':'is-live')];
        }""",
        Output("pulse-host", "style"), Output("pulse-tick", "disabled"),
        Output("pulse-stored-controls", "style"), Output("pulse-replay-controls", "style"),
        Output("pulse-live-controls", "style"), Output("pulse-core", "style"),
        Output("pulse-skeleton", "style"), Output("pulse-evidence", "style"),
        Output("pulse-frozen-note", "style"),
        Output("pulse-toolbar", "className"),
        Input("location", "pathname"), Input("pulse-mode", "value"), Input("pulse-kind", "value"),
        Input("pulse-date", "date"), Input("pulse-horizon", "value"), Input("pulse-zone", "value"),
        Input("location", "search"), Input("pulse-request", "data"), Input("pulse-rendered", "data"), Input("pulse-kafka-run", "data"), Input("pulse-freeze", "value"),
        Input("pulse-replay-engine", "value"),
    )
    app.clientside_callback(
        """function(tick,speed,start,seek,mode,kind,day,zone,seed,path,stop,kafka,refresh,engine,rendered,previous){
          if(rendered && rendered.first_cursor !== undefined) seed=Object.assign({},seed||{},{first_cursor:rendered.first_cursor});
          if(engine==='kafka'){
            const attached=kafka&&kafka.attached&&kafka.day===day&&kafka.zone===zone;
            seed=Object.assign({},seed||{},{kafka_cursor:attached?kafka.cursor||0:0,kafka_status:attached?kafka.status:'ready'});
          }
          return window.dash_clientside.pulse.clock(tick,speed,start,seek,mode,kind,day,zone,seed,path,previous,refresh,engine);
        }""",
        Output("pulse-clock", "data"), Output("pulse-frame", "data"), Output("pulse-clock-label", "children"),
        Output("pulse-replay-state", "children"), Output("pulse-seek", "value"), Output("pulse-eta", "children"),
        Input("pulse-tick", "n_intervals"), Input("pulse-speed", "value"), Input("pulse-play", "n_clicks"),
        Input("pulse-seek", "value"), Input("pulse-mode", "value"),
        Input("pulse-kind", "value"), Input("pulse-date", "date"), Input("pulse-zone", "value"),
        Input("pulse-seed", "data"), Input("location", "pathname"), Input("pulse-cancel", "n_clicks"),
        Input("pulse-kafka-run", "data"), Input("pulse-refresh-rate", "value"), Input("pulse-replay-engine", "value"),
        State("pulse-rendered", "data"), State("pulse-clock", "data"),
    )
    app.clientside_callback(
        """function(path,request,last,run){
          const active=path==='/'||path==='/investigation';
          const terminal=['complete','failed','cancelled'];
          return [!active||!request||!!request.error||!!(last&&last.id===request.id&&terminal.includes(last.status)),
                  !active||!run||terminal.includes(run.status)];
        }""", Output("pulse-job-tick", "disabled"), Output("pulse-kafka-tick", "disabled"),
        Input("location", "pathname"), Input("pulse-request", "data"), Input("pulse-last", "data"),
        Input("pulse-kafka-run", "data"),
    )
    app.clientside_callback(
        """function(run,day,zone,mode,kind,engine,clock,rendered){
          const pending=!!(run&&!['complete','cancelled','failed'].includes(run.status));
          const kafka=engine==='kafka';
          const playing=!!(clock&&clock.playing);
          const ready=kafka?!!day:!!(rendered&&rendered.loaded_at&&rendered.can_replay!==false);
          return [pending||playing||!ready, kafka?(!pending||run.status==='cancelling'):!playing,
                  kafka,pending,pending||playing];
        }""", Output("pulse-play", "disabled"), Output("pulse-cancel", "disabled"),
        Output("pulse-seek", "disabled"), Output("pulse-speed", "disabled"), Output("pulse-replay-engine", "disabled"),
        Input("pulse-kafka-run", "data"), Input("pulse-date", "date"), Input("pulse-zone", "value"),
        Input("pulse-mode", "value"), Input("pulse-kind", "value"), Input("pulse-replay-engine", "value"),
        Input("pulse-clock", "data"), Input("pulse-rendered", "data"),
    )
    app.clientside_callback(
        """function(clicks,previous){
          const open=clicks>0 ? !previous : false;
          return [open,open,'pulse-results'+(open?' is-console-open':'')];
        }""", Output("pulse-console", "open"), Output("pulse-console-preference", "data"),
        Output("pulse-results", "className"),
        Input("pulse-console-toggle", "n_clicks"), State("pulse-console-preference", "data"),
    )
    app.clientside_callback(
        """function(log,follow){
          if ((follow||[]).includes('follow')) window.requestAnimationFrame(function(){
            const el=document.getElementById('pulse-console-log'); if(el) el.scrollTop=el.scrollHeight;
          });
          return null;
        }""", Output("pulse-follow-state", "data"), Input("pulse-console-log", "children"), Input("pulse-follow", "value"),
    )

    @app.callback(Output("pulse-request", "data"),
                  Input("location", "pathname"), Input("location", "search"), Input("pulse-mode", "value"),
                  Input("pulse-kind", "value"), Input("pulse-date", "date"), Input("pulse-horizon", "value"),
                  Input("pulse-zone", "value"), Input("pulse-finalized", "value"), Input("pulse-frame", "data"),
                  Input("pulse-retry", "n_clicks"), Input("pulse-kafka-run", "data"), Input("pulse-freeze", "value"),
                  Input("pulse-replay-engine", "value"), State("pulse-owner", "data"),
                  State("pulse-request", "data"), State("pulse-clock", "data"))
    def submit(path, search, mode, kind, day, horizon, zone, finalized, frame, retry, kafka_run, freeze, engine, owner, previous, clock):
        if path not in ROUTES:
            workspace.cancel_owner(owner)
            return None
        if "freeze" in (freeze or []) and ctx.triggered_id in ("pulse-frame", "pulse-kafka-run", "pulse-freeze"):
            raise PreventUpdate
        if mode == "stored" and not day:
            raise PreventUpdate
        q = parse_qs((search or "").lstrip("?"))
        spec = {"page": "live" if path == "/" else "investigation", "mode": mode, "kind": kind,
                "day": day, "horizon": horizon, "zone": zone, "title": q.get("title", [None])[0],
                "cursor": ((clock or {}) if ctx.triggered_id != "pulse-frame" else (frame or {})).get("cursor", (frame or {}).get("cursor", 0)),
                "finalized": "yes" in (finalized or [])}
        kafka_active = (engine == "kafka" and kafka_run and kafka_run.get("attached") and mode == "stored" and kind == "replay"
                        and kafka_run.get("day") == day and kafka_run.get("zone") == zone)
        if (ctx.triggered_id == "pulse-kafka-run" and not kafka_active
                and not (previous or {}).get("spec", {}).get("run_id")):
            # Status-only updates for a detached/background run must not refresh
            # Live, day summaries, or already-detached saved playback.
            raise PreventUpdate
        if kafka_active:
            if ctx.triggered_id == "pulse-kafka-run" and (previous or {}).get("spec", {}).get("run_id") == kafka_run["id"]:
                # Status counters can update without querying or rebuilding charts.
                raise PreventUpdate
            spec.update(run_id=kafka_run["id"], cursor=kafka_run.get("cursor", 0))
        if ctx.triggered_id == "pulse-frame":
            # Clock continues locally, but paused/summary views do not query.
            if mode == "stored" and (kind != "replay" or not (frame or {}).get("render")):
                raise PreventUpdate
            if (frame or {}).get("automatic") and previous and previous.get("id"):
                pending = workspace.get(previous["id"])
                if pending and pending["status"] not in TERMINAL:
                    raise PreventUpdate
        try:
            request = workspace.submit(spec, owner)
        except ValueError as error:
            return {"error": str(error), "spec": spec}
        # Match JSON.stringify in the browser, including literal Unicode titles.
        request["scope"] = json.dumps([path, mode, kind, day, horizon, zone, spec["title"], spec.get("run_id")],
                                      separators=(",", ":"), ensure_ascii=False)
        return request

    @app.callback(Output("pulse-kafka-run", "data"), Output("pulse-kafka-status", "children"),
                  Input("pulse-play", "n_clicks"), Input("pulse-cancel", "n_clicks"),
                  Input("pulse-replay-engine", "value"), Input("pulse-kafka-tick", "n_intervals"),
                  Input("pulse-mode", "value"), Input("pulse-kind", "value"),
                  Input("pulse-date", "date"), Input("pulse-zone", "value"),
                  State("pulse-kafka-run", "data"), State("pulse-speed", "value"),
                  State("location", "pathname"), prevent_initial_call=True)
    def kafka_run(start, stop, engine, tick, mode, kind, day, zone, current, speed, path):
        if path not in ROUTES:
            raise PreventUpdate
        trigger = ctx.triggered_id
        selected = engine == "kafka" and mode == "stored" and kind == "replay"
        starting = selected and trigger == "pulse-play"
        if not current and not starting:
            return no_update, "Choose Start replay to run the selected day through Kafka & Spark." if selected else ""
        if trigger == "pulse-kafka-tick" and current.get("status") in TERMINAL:
            raise PreventUpdate
        manager = workspace.kafka_replays()
        try:
            if starting:
                if current and current.get("status") not in TERMINAL:
                    raise PreventUpdate
                state = manager.start(day, zone, speed)
                attached = True
            else:
                state = manager.status(current["id"])
                matching = selected and state.get("day") == day and state.get("zone") == zone
                if (trigger == "pulse-cancel" or not matching) and state["status"] not in TERMINAL:
                    manager.action(current["id"], "cancel")
                    state.update(status="cancelling", message="Stopping replay; draining admitted events. Partial results are retained.")
                attached = bool(matching and current.get("attached", True))
            state["attached"] = attached
            label = "stopped" if state["status"] == "cancelled" else "stopping" if state["status"] == "cancelling" else state["status"]
            details = [html.Strong(f"Kafka & Spark: {label} · {state.get('day')} · {state.get('speed')}×"),
                       html.P(state.get("message", "")),
                       pill_row([("Selected", state.get("selected", "preparing")), ("Published", state.get("published", 0)),
                                 ("Consumed", state.get("consumed", 0)), ("Spark committed", state.get("committed", 0)),
                                 ("Pending", state.get("pending", 0)), ("Rejected", state.get("rejected", 0))])]
            if selected and not attached:
                details = "Stopping the previous replay…" if state["status"] not in TERMINAL else "Choose Start replay to process this date through Kafka & Spark."
            return state, details if selected else ""
        except (ValueError, OSError) as error:
            return no_update, notice("Replay", str(error))

    @app.callback(
        Output("pulse-core", "children"), Output("pulse-evidence", "children"),
        Output("pulse-status", "children"), Output("pulse-article", "options"),
        Output("pulse-console-log", "children"), Output("pulse-console-summary", "children"),
        Output("pulse-measurements", "children"), Output("pulse-rendered", "data"),
        Output("pulse-last", "data"), Output("pulse-log-download", "href"),
        Input("pulse-job-tick", "n_intervals"), Input("pulse-request", "data"),
        State("pulse-last", "data"), State("location", "pathname"), State("pulse-article", "search_value"),
        State("pulse-rendered", "data"),
    )
    def poll(tick, request, previous, path, search_text, displayed):
        if path not in ROUTES or not request:
            raise PreventUpdate
        if request.get("error"):
            return (notice("Invalid selection", request["error"]), [], request["error"], [], workspace.history.text(), "Invalid selection", [],
                    {"scope": request.get("scope")}, None, "/api/activity/console/log")
        if previous and previous.get("id") == request["id"] and previous.get("status") in TERMINAL:
            raise PreventUpdate
        job = workspace.get(request["id"], include_data=True)
        if job is None:
            return (notice("Request expired", "Use Refresh / Retry to reload this selection."), [], "Request expired", [],
                    workspace.history.text(), "Expired", [], {"scope": request["scope"]}, None, "/api/activity/console/log")
        prior_core = previous and previous.get("id") == job["id"] and previous.get("revision", 0) >= 1
        core = no_update
        evidence = no_update
        rendered = no_update
        options = no_update
        data = job.get("result")
        same_scope = displayed and displayed.get("scope") == request["scope"]
        if data is not None and not prior_core:
            if not same_scope or displayed.get("loaded_at") != data.get("loaded_at"):
                core = render_core(data, job["spec"])
            if not same_scope:
                evidence = notice("Loading optional evidence", "Activity is ready. Baselines, alerts and revision evidence load independently.")
            rendered = {"scope": request["scope"], "first_cursor": data.get("first_cursor", 0), "loaded_at": data.get("loaded_at"),
                        "can_replay": bool(data.get("available_events")),
                        "evidence_token": displayed.get("evidence_token") if same_scope else None}
            titles = [r["title"] for r in data.get("rankings", [])]
            if job["spec"].get("title") not in titles and job["spec"].get("title"):
                titles.append(job["spec"]["title"])
            options = no_update if search_text else [{"label": title, "value": title} for title in titles]
        if job.get("evidence") is not None:
            evidence_token = hashlib.sha256(json.dumps([data.get("range"), data.get("metrics"), job["evidence"]],
                                                       sort_keys=True, default=str).encode()).hexdigest()
            if not same_scope or displayed.get("evidence_token") != evidence_token:
                evidence = render_evidence(data, job["evidence"], job["spec"])
            if rendered is no_update:
                rendered = dict(displayed or {}, scope=request["scope"])
            rendered["evidence_token"] = evidence_token
        if job["status"] == "failed":
            core = notice("Unable to load this selection", job["error"])
            evidence = []
            rendered = {"scope": request["scope"]}
        if job["status"] == "cancelled" and data is None:
            core = notice("Replay request cancelled", "Choose a position and press Play, or use Refresh / Retry.")
            evidence = []
            rendered = {"scope": request["scope"]}
        elif job["status"] == "cancelled":
            evidence = notice("Evidence request cancelled", "Primary activity remains available. Use Refresh / Retry to load evidence again.")
        m = job["metrics"]
        metrics = [("Rows fetched", f"{m['rows_fetched']:,}" if "rows_fetched" in m else "pending"),
                   ("Events analyzed", f"{m['events_analyzed']:,}" if m.get("events_analyzed") is not None else "window aggregates" if data and data.get("view") == "finalized" else "pending"),
                   ("Data decoded", pretty_bytes(m.get("arrow_payload_bytes"))),
                   ("Processing time", f"{m['query_seconds']:.2f} s" if "query_seconds" in m else f"{job['elapsed_seconds']:.2f} s elapsed"),
                   ("Result", "cached snapshot" if m.get("cache") == "snapshot hit" else "analyzed from memory" if m.get("cache") == "day index hit" else "file query")]
        log = workspace.history.text()
        summary = f"{job['status'].title()} · {job['elapsed_seconds']:.1f} s"
        status = summary
        if m.get("events_matched") is not None:
            summary += f" · {m['events_matched']:,} matching edits"
        if m.get("cache") == "snapshot hit":
            summary += " · cached snapshot"
        elif m.get("arrow_payload_bytes") is not None:
            summary += f" · {pretty_bytes(m['arrow_payload_bytes'])} decoded"
        if data:
            status += f" · {data.get('scope')} · Latest matching event: {format_india_time(data.get('latest')) or 'none'}"
            status += f" · Snapshot: {format_india_time(data.get('loaded_at'))}"
        last = {"id": job["id"], "revision": job["revision"], "status": job["status"]}
        return (core, evidence, status, options, log, summary, pill_row(metrics), rendered, last,
                "/api/activity/console/log")

    @app.callback(Output("pulse-article", "options", allow_duplicate=True), Input("pulse-article", "search_value"),
                  State("pulse-mode", "value"), State("pulse-date", "date"), State("pulse-zone", "value"),
                  State("pulse-horizon", "value"), State("pulse-article", "value"), prevent_initial_call=True)
    def find_article(query, mode, day, zone, horizon, selected):
        if not query:
            raise PreventUpdate
        titles = workspace.reader.search_titles({"mode": mode, "day": day if mode == "stored" else None,
                                                "zone": zone, "horizon": horizon}, query)
        if selected and selected not in titles:
            titles.append(selected)
        return [{"label": title, "value": title} for title in titles]

    @app.callback(Output("location", "href", allow_duplicate=True),
                  Input("pulse-ranking", "clickData", allow_optional=True), Input("pulse-article", "value"),
                  State("location", "pathname"), State("location", "search"), State("pulse-mode", "value"),
                  State("pulse-kind", "value"), State("pulse-date", "date"), State("pulse-horizon", "value"),
                  State("pulse-zone", "value"), State("pulse-clock", "data"), prevent_initial_call=True)
    def open_article(click, title, path, search, mode, kind, day, horizon, zone, clock):
        if path not in ROUTES:
            raise PreventUpdate
        if ctx.triggered_id == "pulse-ranking":
            title = (click or {}).get("points", [{}])[0].get("customdata")
        current = parse_qs((search or "").lstrip("?")).get("title", [None])[0]
        clearing = not title and current and path == "/investigation" and ctx.triggered_id == "pulse-article"
        if (not title and not clearing) or (path == "/investigation" and title == current):
            raise PreventUpdate
        return activity_url("/investigation", {"mode": mode, "kind": kind, "day": day, "horizon": horizon,
                                               "zone": zone, "cursor": (clock or {}).get("cursor", 0)}, title)

    @app.callback(Output("pulse-command-status", "children"),
                  Input("pulse-start-stream", "n_clicks"), Input("pulse-stop-stream", "n_clicks"), prevent_initial_call=True)
    def command(start, stop):
        result = request_action("start" if ctx.triggered_id == "pulse-start-stream" else "stop", uuid.uuid4().hex,
                                os.environ.get("WIKIPULSE_CONTROL_TOKEN"))
        return result["message"]

    @app.callback(Output("pulse-stream-metrics", "children"), Output("pulse-start-stream", "disabled"),
                  Output("pulse-stop-stream", "disabled"), Output("pulse-source-status", "children"),
                  Output("pulse-apply-source", "disabled"), Input("health-refresh", "n_intervals"),
                  Input("location", "pathname"))
    def stage_health(tick, path):
        if path not in ROUTES:
            raise PreventUpdate
        health = stream_health()
        rows = [html.P(line) for line in workspace.ingestion.snapshot()]
        receipt = read_json(workspace.paths.runtime / "stream-control.result.json")
        pending = read_json(workspace.paths.runtime / "stream-control.request.json")
        busy = bool(pending) or health["state"] in ("starting", "restarting", "draining")
        details = [html.Strong(health["label"]), *rows]
        if receipt:
            details.append(html.P(str(receipt.get("message", "")) + " " + " ".join(receipt.get("warnings", []))))
            if receipt.get("source_gap"):
                details.append(html.P(receipt["source_gap"]))
        managed = health["managed"] and bool(os.environ.get("WIKIPULSE_CONTROL_TOKEN"))
        source = read_json(workspace.paths.runtime / "stream-source.json")
        progress = read_json(workspace.paths.runtime / "collector-source.progress.json")
        source_label = "Since " + source["since"] if source.get("since") else "Live now"
        source_status = [html.Strong("Active Wikipedia source: " + source_label)]
        if progress.get("since") == source.get("since"):
            source_status.append(html.Span(f" · {progress.get('state', 'waiting')} · {progress.get('queued_events', 0):,} events / "
                                          f"{pretty_bytes(progress.get('queued_bytes', 0))} queued to Kafka"))
            if source.get("since") and progress.get("first_event"):
                source_status.append(html.Span(" · First captured event: " + progress["first_event"]))
            if progress.get("latest_event"):
                source_status.append(html.Span(" · Latest captured event: " + progress["latest_event"]))
        if source.get("since"):
            source_status.append(html.Small("Capture coverage depends on upstream retention. Live charts use the current activity horizon; use Stored activity to view past dates. Historical arrivals do not rebuild finalized anomaly scores."))
        return (details, busy or not managed or health["state"] in ("running", "catching_up", "reconnecting", "stale"),
                busy or not managed or health["state"] == "idle", source_status, busy or not managed)

