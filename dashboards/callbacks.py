"""Dash callbacks connect user controls to cached backend snapshots."""

from urllib.parse import parse_qs, quote

from dash import Input, Output, ctx
from dash.exceptions import PreventUpdate

from dashboards.components import notice
from dashboards.layout import navigation
from dashboards.pages import historical, investigation, live, relationships, system
from dashboards.time_utils import format_india_time
from src.local_supervisor import request_restart_from_ui, stream_health

PAGES = {
    "/": ("live", live),
    "/investigation": ("investigation", investigation),
    "/historical": ("historical", historical),
    "/relationships": ("relationships", relationships),
    "/system": ("system", system),
}

def render_dashboard(service, pathname, search, duration, view, title, start, end,
                     replay_frame=0, replay_speed=1):
    """Keep routing callable independently for meaningful UI integration tests."""
    route = PAGES.get(pathname)
    if route is None:
        return [notice("Page not found", "Choose a dashboard from the sidebar.")], "Unknown route", [], navigation()
    page_name, page_module = route
    # Article links are ordinary shareable URLs; dropdown selection is also supported.
    linked_title = parse_qs((search or "").lstrip("?")).get("title", [None])[0]
    selected_title = linked_title or title
    data = service.load(
        page_name, duration, view, selected_title, start, end,
        replay_frame=replay_frame, replay_speed=replay_speed,
    )
    if data.get("error"):
        return [notice("Unable to refresh data", data["error"])], "Refresh failed", [], navigation(pathname)
    latest = data.get("latest") or data.get("metrics", {}).get("last_day")
    if page_name in ("live", "investigation"):
        mode = {"live": "LIVE PREVIEW", "finalized": "FINALIZED", "replay": "REPLAY"}.get(view, view.upper())
        source = data.get("data_source", "live activity")
        latest_label = format_india_time(latest) or latest
        selected_range = data.get("range") or {}
        range_label = ""
        if selected_range.get("start") and selected_range.get("end"):
            range_label = (
                f" · Range: {format_india_time(selected_range['start'])}"
                f" – {format_india_time(selected_range['end'])}"
            )
        if view == "live" and not latest:
            health = stream_health()
            status = (
                f"LIVE PREVIEW · {data.get('scope', 'Selected range')}{range_label} · No events in range. {health['label']}. "
                "Choose Replay stored activity to browse saved windows."
            )
        elif view == "replay":
            status = f"REPLAY · {latest_label or 'Waiting for stored windows'} · {data.get('scope', 'Stored activity')}{range_label}"
        else:
            status = (
                f"{mode} · {data.get('scope', 'Waiting for observations')}{range_label} · "
                f"Source: {source} · Latest event: {latest_label or 'unavailable'}"
            )
    else:
        status = "READ-ONLY EVIDENCE · Last refresh: " + data.get("loaded_at", "pending")
    titles = data.get("articles") or [row["title"] for row in data.get("rankings", [])]
    options = [{"label": article, "value": article} for article in titles]
    if selected_title and not any(option["value"] == selected_title for option in options):
        options.append({"label": selected_title, "value": selected_title})
    return page_module.render(data), status, options, navigation(pathname)


def register_callbacks(app, service):
    @app.callback(
        Output("page-body", "children"), Output("source-status", "children"),
        Output("article", "options"),
        Input("location", "pathname"), Input("location", "search"), Input("refresh", "n_intervals"),
        Input("duration", "value"), Input("view", "value"), Input("article", "value"),
        Input("historical-range", "value"),
        Input("replay-clock", "n_intervals"), Input("replay-speed", "value"),
        running=[(Output("refresh-busy", "data"), True, False)],
    )
    def refresh_page(pathname, search, tick, duration, view, title, historical_range, replay_frame, replay_speed):
        path = pathname or "/"
        # Historical evidence is driven by its date controls; relationship and
        # health views change only on navigation. Ignore irrelevant global
        # controls and timer ticks so they cannot queue expensive Spark work.
        triggered = set(ctx.triggered_prop_ids)
        if path in ("/historical", "/relationships", "/system") and triggered:
            allowed = {"location.pathname"}
            if path == "/historical":
                allowed.add("historical-range.value")
            if not triggered.intersection(allowed):
                raise PreventUpdate
        if path in ("/", "/investigation"):
            if triggered == {"historical-range.value"}:
                raise PreventUpdate
            if path == "/" and "article.value" in triggered:
                raise PreventUpdate
        children, status, options, _ = render_dashboard(
            service, path, search, duration, view, title, historical_range, None,
            replay_frame, replay_speed,
        )
        return children, status, options

    @app.callback(Output("navigation", "children"), Input("location", "pathname"))
    def update_navigation(pathname):
        # Highlight the destination as soon as the URL changes, independently
        # of the data query that fills the page body.
        return navigation(pathname or "/")

    @app.callback(Output("refresh", "disabled"), Input("pause", "value"), Input("refresh-busy", "data"),
                  Input("view", "value"), Input("location", "pathname"))
    def pause_refresh(value, busy, view=None, pathname="/"):
        # A slow query must finish before the timer can request another snapshot.
        # Keep the user's pause choice when the running callback finishes.
        live_route = pathname in (None, "/", "/investigation")
        return bool(busy) or "pause" in (value or []) or view == "replay" or not live_route

    @app.callback(
        Output("loading-banner", "style"),
        Output("loading-title", "children"), Output("loading-description", "children"),
        Input("refresh-busy", "data"), Input("location", "pathname"), Input("view", "value"),
    )
    def loading_copy(busy, pathname, view):
        if view == "replay" and pathname in ("/", "/investigation"):
            title, description = "Selecting replay window", "Reading the stored activity around this replay point…"
        else:
            copy = {
                "/": ("Refreshing live pulse", "Reading recent edits and activity trends…"),
                "/investigation": ("Loading article evidence", "Filtering recent activity and saved context for this page…"),
                "/historical": ("Loading historical intelligence", "Reading daily rankings and baseline coverage…"),
                "/relationships": ("Loading relationship evidence", "Reading co-edit links and available locations…"),
                "/system": ("Loading pipeline health", "Checking stream progress, storage, and benchmark summaries…"),
            }
            title, description = copy.get(pathname or "/", ("Loading dashboard", "Preparing the requested view…"))
        return {"display": "flex" if busy else "none"}, title, description

    @app.callback(
        Output("replay-clock", "disabled"),
        Output("replay-controls", "style"),
        Output("replay-speed-control", "style"),
        Input("view", "value"), Input("replay-playing", "value"),
    )
    def replay_controls(view, playing):
        visible = view == "replay"
        return not (visible and "play" in (playing or [])), (
            {"display": "block"} if visible else {"display": "none"}
        ), ({"display": "block"} if visible else {"display": "none"})

    @app.callback(
        Output("stream-health-label", "children"),
        Output("stream-health-label", "className"),
        Output("restart-stream", "disabled"),
        Input("health-refresh", "n_intervals"),
    )
    def update_stream_health(_):
        health = stream_health()
        state = health["state"]
        disabled = not health["managed"] or state in ("starting", "restarting")
        return health["label"], f"health-tag health-{state}", disabled

    @app.callback(
        Output("restart-feedback", "children"),
        Input("restart-stream", "n_clicks"),
        prevent_initial_call=True,
    )
    def restart_stream(n_clicks):
        result = request_restart_from_ui()
        return result["message"]

    @app.callback(
        Output("app-shell", "className"),
        Input("theme-toggle", "value"),
    )
    def set_theme(value):
        dark = "dark" in (value or [])
        return "app-shell theme-dark" if dark else "app-shell"

    # One href update avoids a race between separate pathname/search updates.
    @app.callback(Output("location", "href"),
                  Input("page-ranking", "clickData", allow_optional=True), Input("article", "value"),
                  prevent_initial_call=True)
    def investigate_article(click, selected_title):
        if ctx.triggered_id == "article":
            title = selected_title
        else:
            title = click.get("points", [{}])[0].get("customdata") if click else None
        if not title:
            raise PreventUpdate
        return "/investigation?title=" + quote(title, safe="")
