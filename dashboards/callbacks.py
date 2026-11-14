"""Dash callbacks connect user controls to cached backend snapshots."""

from urllib.parse import parse_qs, quote

from dash import Input, Output, ctx
from dash.exceptions import PreventUpdate

from dashboards.components import notice
from dashboards.layout import navigation
from dashboards.pages import (
    anomaly_scoring,
    contextual_features,
    geospatial_map,
    historical,
    investigation,
    live,
    milestone4,
    milestone5,
    milestone6,
    ml_reports,
    relationships,
    system,
)
from dashboards.time_utils import format_india_time
from src.local_supervisor import request_restart_from_ui, stream_health

PAGES = {
    "/": ("live", live),
    "/investigation": ("investigation", investigation),
    "/historical": ("historical", historical),
    "/anomaly-scoring": ("anomaly_scoring", anomaly_scoring),
    "/ml-reports": ("ml_reports", ml_reports),
    "/contextual-features": ("contextual_features", contextual_features),
    "/geospatial-map": ("geospatial_map", geospatial_map),
    "/relationships": ("relationships", relationships),
    "/system": ("system", system),
    "/milestone4": ("milestone4", milestone4),
    "/milestone5": ("milestone5", milestone5),
    "/milestone6": ("milestone6", milestone6),
}

def render_dashboard(service, pathname, search, duration, view, title, start, end,
                     replay_frame=0, replay_speed=1):
    """Keep routing callable independently for meaningful UI integration tests."""
    route = PAGES.get(pathname)
    if route is None:
        return [notice("Page not found", "Choose a dashboard from the sidebar.")], "Unknown route", [], navigation()
    page_name, page_module = route
    # Article links are ordinary shareable URLs; dropdown selection is also supported.
    query = parse_qs((search or "").lstrip("?"))
    linked_title = query.get("title", [None])[0]
    selected_title = linked_title or title
    load_view = view
    load_start = start
    if page_name == "geospatial_map":
        load_view = query.get("mode", ["days"])[0]
        load_start = query.get("range", ["55d"])[0]
        selected_title = query.get("region", ["ALL"])[0]
    data = service.load(
        page_name, duration, load_view, selected_title, load_start, end,
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
    elif page_name in (
        "historical",
        "anomaly_scoring",
        "ml_reports",
        "contextual_features",
        "geospatial_map",
        "milestone4",
        "milestone5",
        "milestone6",
    ):
        status = (
            "HISTORICAL CURATED LAKE (ingest_curate · 2026-09-15 – 2026-11-08) · Last refresh: "
            + data.get("loaded_at", "pending")
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
        if path in (
            "/historical",
            "/anomaly-scoring",
            "/ml-reports",
            "/contextual-features",
            "/geospatial-map",
            "/relationships",
            "/system",
            "/milestone4",
            "/milestone5",
            "/milestone6",
        ) and triggered:
            allowed = {"location.pathname"}
            if path in ("/historical", "/milestone4"):
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
        Output("backend-loading-icon", "className"),
        Input("refresh-busy", "data"), Input("location", "pathname"), Input("view", "value"),
    )
    def loading_copy(busy, pathname, view):
        if view == "replay" and pathname in ("/", "/investigation"):
            title, description = "Selecting replay window", "Reading the stored activity around this replay point…"
        else:
            copy = {
                "/": ("Refreshing live pulse", "Reading recent edits and activity trends…"),
                "/investigation": ("Loading article evidence", "Filtering recent activity and saved context for this page…"),
                "/historical": ("Loading historical intelligence", "Reading daily rankings, anomaly density, and baseline coverage…"),
                "/anomaly-scoring": ("Loading anomaly scoring", "Reading multi-detector scores, component weights, and case studies…"),
                "/ml-reports": ("Loading ML tests & reports", "Reading Spark ML models, synthetic injection tests, and false-alert audits…"),
                "/contextual-features": ("Loading contextual features", "Reading topic graph spillover, revision diffs, subject map, and Lift Wing risk…"),
                "/geospatial-map": ("Loading geospatial map", "Aggregating regional recentchanges edits across days and windows…"),
                "/relationships": ("Loading relationship evidence", "Reading co-edit links and available locations…"),
                "/system": ("Loading pipeline health", "Checking stream progress, storage, and benchmark summaries…"),
                "/milestone4": ("Loading Milestone 4", "Reading curated ingestion, baselines, churn rankings, and benchmarks…"),
                "/milestone5": ("Loading Milestone 5", "Reading Spark ML outcomes, detector comparisons, and evaluation…"),
                "/milestone6": ("Loading Milestone 6", "Reading graph spillover, revision diffs, subject map, and Lift Wing calibration…"),
            }
            title, description = copy.get(pathname or "/", ("Loading dashboard", "Preparing the requested view…"))
        icon_cls = "backend-loading-pill active" if busy else "backend-loading-pill"
        return {"display": "flex" if busy else "none"}, title, description, icon_cls

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

    @app.callback(
        Output("geo-map-content", "children"),
        Output("geo-region", "value"),
        Input("geo-mode", "value"),
        Input("geo-range", "value"),
        Input("geo-region", "value"),
        Input("geo-projection", "value"),
        Input("geo-metric", "value"),
        Input("geo-regional-map", "clickData", allow_optional=True),
        Input("geo-reset-btn", "n_clicks"),
        running=[(Output("refresh-busy", "data"), True, False)],
        prevent_initial_call=True,
    )
    def update_geospatial_map(
        geo_mode, geo_range, geo_region, geo_projection, geo_metric, map_click, reset_clicks
    ):
        target_region = geo_region or "ALL"
        selected_article_title = None
        trig = ctx.triggered_id
        if trig == "geo-reset-btn":
            target_region = "ALL"
        elif trig == "geo-regional-map" and map_click:
            pt = (map_click.get("points") or [{}])[0]
            cdata = str(pt.get("customdata") or "")
            if cdata.startswith("COUNTRY|"):
                clicked_country = cdata.split("|", 1)[1].strip()
                if clicked_country:
                    target_region = clicked_country
            elif cdata.startswith("ARTICLE|"):
                parts = cdata.split("|")
                if len(parts) >= 2:
                    selected_article_title = parts[1]
            elif pt.get("location"):
                target_region = str(pt["location"])

        data = service.load(
            "geospatial_map", "5m", geo_mode or "days", target_region, geo_range or "55d", None
        )
        return (
            geospatial_map.render_content(
                data,
                projection=geo_projection or "natural earth",
                metric_key=geo_metric or "total_edits",
                selected_article_title=selected_article_title,
            ),
            target_region,
        )

    @app.callback(
        Output("top-controls", "style"),
        Output("live-window-control", "style"),
        Output("live-view-control", "style"),
        Output("live-article-control", "style"),
        Output("historical-range-wrapper", "style"),
        Output("pause", "style"),
        Input("location", "pathname"),
    )
    def toggle_top_controls(pathname):
        path = pathname or "/"
        if path in ("/", "/investigation"):
            return (
                {"display": "flex"},
                {"display": "block"},
                {"display": "block"},
                {"display": "block"},
                {"display": "none"},
                {"display": "block"},
            )
        if path in ("/historical", "/milestone4"):
            return (
                {"display": "flex"},
                {"display": "none"},
                {"display": "none"},
                {"display": "none"},
                {"display": "block"},
                {"display": "none"},
            )
        # Strictly historical / static pages with their own dedicated controls or full corpus scope
        return (
            {"display": "none"},
            {"display": "none"},
            {"display": "none"},
            {"display": "none"},
            {"display": "none"},
            {"display": "none"},
        )
