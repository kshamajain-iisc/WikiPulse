"""Dash callbacks connect user controls to cached backend snapshots."""

from urllib.parse import parse_qs, quote

from dash import Input, Output, ctx
from dash.exceptions import PreventUpdate

from dashboards.components import notice
from dashboards.layout import navigation
from dashboards.pages import historical, investigation, live, relationships, system

PAGES = {
    "/": ("live", live),
    "/investigation": ("investigation", investigation),
    "/historical": ("historical", historical),
    "/relationships": ("relationships", relationships),
    "/system": ("system", system),
}


def render_dashboard(service, pathname, search, duration, view, title, start, end):
    """Keep routing callable independently for meaningful UI integration tests."""
    route = PAGES.get(pathname)
    if route is None:
        return [notice("Page not found", "Choose a dashboard from the sidebar.")], "Unknown route", [], navigation()
    page_name, page_module = route
    # Article links are ordinary shareable URLs; dropdown selection is also supported.
    linked_title = parse_qs((search or "").lstrip("?")).get("title", [None])[0]
    selected_title = linked_title or title
    data = service.load(page_name, duration, view, selected_title, start, end)
    if data.get("error"):
        return [notice("Unable to refresh data", data["error"])], "Refresh failed", [], navigation(pathname)
    latest = data.get("latest") or data.get("metrics", {}).get("last_day")
    if page_name in ("live", "investigation"):
        mode = "PROVISIONAL" if view == "live" else "FINALIZED"
        status = f"{mode} · {data.get('scope', 'Waiting for observations')} · Latest source: {latest or 'unavailable'}"
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
        Output("article", "options"), Output("navigation", "children"),
        Input("location", "pathname"), Input("location", "search"), Input("refresh", "n_intervals"),
        Input("duration", "value"), Input("view", "value"), Input("article", "value"),
        Input("dates", "start_date"), Input("dates", "end_date"),
    )
    def refresh_page(pathname, search, tick, duration, view, title, start, end):
        return render_dashboard(service, pathname or "/", search, duration, view, title, start, end)

    @app.callback(Output("refresh", "disabled"), Input("pause", "value"))
    def pause_refresh(value):
        return "pause" in (value or [])

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
