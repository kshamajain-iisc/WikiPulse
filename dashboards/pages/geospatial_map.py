"""Dedicated UI Page: RecentChanges Geospatial Regional Edit Map with [Days / Windows] Time Range Picker."""

from urllib.parse import quote

from dash import dcc, html

from dashboards import charts
from dashboards.components import graph, heading, metric, notice, number, panel, pill_row, table

MODE_OPTIONS = [
    {"label": "Days (Daily RecentChanges)", "value": "days"},
    {"label": "5m Windows (page_windows_5m)", "value": "windows_5m"},
    {"label": "1h Windows (page_windows_1h)", "value": "windows_1h"},
]

RANGE_OPTIONS = [
    {"label": "[Days] All 55 Curated Days (Sep 15 – Nov 08)", "value": "55d"},
    {"label": "[Days] Last 30 Curated Days (Oct 10 – Nov 08)", "value": "30d"},
    {"label": "[Days] Last 15 Curated Days (Oct 25 – Nov 08)", "value": "15d"},
    {"label": "[Days / Windows] Evaluation 8 Days (Nov 01 – Nov 08)", "value": "8d"},
    {"label": "[Days] Baseline 47 Days (Sep 15 – Oct 31)", "value": "baseline_47d"},
    {"label": "[Windows / Day] Peak Day · Nov 04, 2026 (24h)", "value": "day_2026-11-04"},
    {"label": "[Windows / Day] Final Day · Nov 08, 2026 (24h)", "value": "day_2026-11-08"},
    {"label": "[Windows] Peak 6-Hour Window (Nov 04 12:00–18:00 UTC)", "value": "win_6h_peak"},
    {"label": "[Windows] Final 6-Hour Window (Nov 08 18:00–24:00 UTC)", "value": "win_6h_latest"},
    {"label": "[Windows] Final 1-Hour Window (Nov 08 22:00–24:00 UTC)", "value": "win_1h_latest"},
]

PROJECTION_OPTIONS = [
    {"label": "Natural Earth (Wide)", "value": "natural earth"},
    {"label": "3D Interactive Globe", "value": "orthographic"},
    {"label": "Robinson Atlas", "value": "robinson"},
    {"label": "Equirectangular Flat", "value": "equirectangular"},
    {"label": "Mercator Zoom", "value": "mercator"},
]

METRIC_OPTIONS = [
    {"label": "Total Edits", "value": "total_edits"},
    {"label": "Human Edits", "value": "human_edits"},
    {"label": "Gross Byte Churn", "value": "gross_bytes"},
    {"label": "Reverts Count", "value": "reverts"},
]

DEFAULT_REGIONS = [
    "ALL",
    "North America",
    "Europe",
    "South Asia",
    "East & Southeast Asia",
    "Middle East & Africa",
    "Latin America",
    "Oceania",
    "United States",
    "United Kingdom",
    "India",
    "Canada",
    "Australia",
    "China",
    "Japan",
    "Germany",
    "France",
    "Italy",
    "Spain",
    "Brazil",
    "Russia",
    "Indonesia",
    "Iran",
    "Philippines",
    "Poland",
]


def render_inspector(selected_article_row, selected_region="ALL"):
    """Render an interactive inspection bar below the full-width map."""
    if selected_article_row:
        title = selected_article_row.get("title", "Selected Article")
        country = selected_article_row.get("country_or_region", "—")
        macro = selected_article_row.get("macro_region", "—")
        e_tot = selected_article_row.get("edits_total", selected_article_row.get("edits", 0))
        e_hum = selected_article_row.get("edits_human", selected_article_row.get("human", 0))
        e_bot = selected_article_row.get("edits_bot", selected_article_row.get("bots", 0))
        g_bytes = selected_article_row.get("gross_bytes", 0)
        revs = selected_article_row.get("reverts", 0)
        coords = selected_article_row.get("coordinates") or (
            f"{selected_article_row.get('lat', 0):.2f}°, {selected_article_row.get('lon', 0):.2f}°"
        )
        return html.Div([
            html.Div([
                html.Span("SELECTED MAP BUBBLE", className="eyebrow", style={"marginBottom": "4px"}),
                html.Strong(title, style={"fontSize": "18px", "display": "block"}),
                html.Span(
                    f"{country} ({macro}) · Coordinates: {coords} · Source: {selected_article_row.get('location_source', 'wikidata')}",
                    style={"color": "var(--muted)", "fontSize": "13px"},
                ),
            ], className="geo-inspector-main"),
            html.Div([
                pill_row([
                    ("Total edits", f"{e_tot:,}"),
                    ("Human edits", f"{e_hum:,}"),
                    ("Bot edits", f"{e_bot:,}"),
                    ("Gross churn", number(g_bytes, " B")),
                    ("Reverts", f"{revs:,}"),
                ]),
                dcc.Link(
                    "Open Page Investigation →",
                    href="/investigation?title=" + quote(str(title), safe=""),
                    className="restart-button",
                    style={"textDecoration": "none", "display": "inline-flex", "alignItems": "center"},
                ),
            ], className="geo-inspector-actions"),
        ], className="geo-inspector-bar")

    scope_text = "All Regions (Global)" if selected_region == "ALL" else f"Active filter: {selected_region}"
    return html.Div([
        html.Div([
            html.Strong(f"Interactive Map Controls · {scope_text}"),
            html.Span(
                "Scroll to zoom · Drag to pan/rotate · Click any country polygon on the map to filter the entire page to that country · Click any article bubble to inspect its telemetry here.",
                style={"color": "var(--muted)", "fontSize": "13px", "display": "block", "marginTop": "3px"},
            ),
        ]),
    ], className="geo-inspector-bar")


def render_content(data, projection="natural earth", metric_key="total_edits", selected_article_title=None):
    """Render the dynamic KPI cards, full-width interactive map, regional charts, and tables."""
    metrics = data.get("metrics", {})
    country_rollup = data.get("country_rollup", [])
    macro_regions = data.get("macro_regions", [])
    map_articles = data.get("map_articles", [])
    timeline = data.get("timeline", [])
    ing = data.get("ingestion_summary", {})
    m6_geo = data.get("m6_geospatial", {})

    mode = data.get("mode", "days")
    requested_mode = data.get("requested_mode", mode)
    selected_region = data.get("region", "ALL") or "ALL"
    range_label = data.get("range_label", "All 55 Curated Days (2026-09-15 – 2026-11-08)")

    # Fallback to m6_geospatial if unit test mock returns empty country_rollup
    if not country_rollup and m6_geo.get("top_countries_or_regions"):
        country_rollup = [
            {
                "country_or_region": r.get("country_or_region"),
                "macro_region": "Global",
                "active_topics": r.get("active_topics", 0),
                "total_edits": r.get("total_edits_human", 0),
                "total_edits_human": r.get("total_edits_human", 0),
                "total_edits_bot": 0,
                "gross_bytes": 0,
                "reverts": 0,
                "top_article": "—",
            }
            for r in m6_geo.get("top_countries_or_regions", [])
        ]

    total_edits = metrics.get("total_edits") or sum(r.get("total_edits", 0) for r in country_rollup)
    human_edits = metrics.get("human_edits") or sum(r.get("total_edits_human", 0) for r in country_rollup)
    bot_edits = metrics.get("bot_edits") or 0
    active_pages = metrics.get("active_pages") or sum(r.get("active_topics", 0) for r in country_rollup)
    active_countries = metrics.get("active_countries") or len(country_rollup)
    active_macros = metrics.get("active_macro_regions") or len(macro_regions)
    gross_bytes = metrics.get("gross_bytes") or 0
    reverts = metrics.get("reverts") or 0

    mode_label = {
        "days": "Daily RecentChanges (rankings_daily / rc_events)",
        "windows_5m": "5-Minute Windows (page_windows_5m)",
        "windows_1h": "1-Hour Windows (page_windows_1h)",
    }.get(mode, mode)

    pills = [
        ("Data source", "ingest_curate.py (curated/rc_events + map_points)"),
        ("Granularity", mode_label),
        ("Selected range", range_label),
        ("Region filter", "All Regions (Global)" if selected_region == "ALL" else selected_region),
        ("First bucket", metrics.get("first_bucket") or "2026-09-15"),
        ("Last bucket", metrics.get("last_bucket") or "2026-11-08"),
        ("Privacy policy", "Subject geography only (0 editor IPs)"),
    ]
    if ing:
        pills.append((
            "Total lake corpus",
            f"{ing.get('curated_deduplicated_events', 10190119):,} rc_events ({ing.get('distinct_days', 55)} days)",
        ))

    fallback_notice = []
    if requested_mode != mode:
        fallback_notice = [
            notice(
                "Window horizon note",
                "Pre-computed 5m and 1h window tables cover the Nov 01 – Nov 08 evaluation period. "
                "Showing daily RecentChanges aggregation for the selected pre-November baseline dates.",
            )
        ]

    selected_article_row = None
    if selected_article_title and map_articles:
        selected_article_row = next(
            (r for r in map_articles if r.get("title") == selected_article_title),
            None,
        )

    return [
        *fallback_notice,
        html.Div([
            metric(
                "REGIONAL EDITS (RECENTCHANGES)",
                number(total_edits),
                f"{number(human_edits)} human · {number(bot_edits)} bot edits in range",
                True,
            ),
            metric(
                "ACTIVE COUNTRIES & REGIONS",
                number(active_countries),
                f"Across {number(active_macros)} macro-regions ({'Global' if selected_region == 'ALL' else selected_region})",
            ),
            metric(
                "LOCATED ARTICLES EDITED",
                number(active_pages),
                "Wikipedia coordinates + Wikidata P625/P276/P17 + subject gazetteer",
            ),
            metric(
                "REGIONAL CHURN & REVERTS",
                number(gross_bytes, " B"),
                f"{number(reverts)} reverts recorded in selected region & time range",
            ),
        ], className="metric-grid"),
        pill_row(pills),
        panel(
            f"Interactive Full-Width RecentChanges Geospatial Map ({'All Regions' if selected_region == 'ALL' else selected_region})",
            f"{range_label} · Scroll to zoom, drag to pan, click any country polygon to filter region, or click any article bubble to inspect",
            html.Div([
                graph(
                    charts.recentchanges_regional_map(
                        country_rollup,
                        map_articles,
                        selected_region=selected_region,
                        projection=projection,
                        metric_key=metric_key,
                    ),
                    graph_id="geo-regional-map",
                    show_modebar=True,
                    scroll_zoom=True,
                ),
                html.Div(
                    render_inspector(selected_article_row, selected_region),
                    id="geo-selected-inspector",
                ),
            ]),
            class_name="geo-hero-panel",
        ),
        html.Div([
            panel(
                "Top countries & regions by RecentChanges edits (Human vs Bot)",
                "Stacked human and bot edit counts in the selected days/windows range",
                graph(charts.regional_edits_breakdown(country_rollup)),
            ),
            panel(
                f"Regional edit velocity & revert timeline ({'Days' if mode == 'days' else 'Windows'})",
                f"Human edits, bot edits, and reverts across {range_label}",
                graph(charts.regional_edits_timeline(timeline, mode)),
            ),
        ], className="chart-grid"),
        panel(
            "Macro-region comparison (Total Edits vs Active Articles)",
            "Geographic distribution across global macro-regions for the selected time range",
            graph(charts.macro_region_distribution(macro_regions)),
        ),
        panel(
            "Regional RecentChanges summary leaderboard",
            "Countries and regions ranked by RecentChanges edit activity in the selected [days / windows] range",
            table(country_rollup, [
                ("Country / region", "country_or_region"),
                ("Macro-region", "macro_region"),
                ("Active articles", "active_topics"),
                ("Total edits", "total_edits"),
                ("Human edits", "total_edits_human"),
                ("Bot edits", "total_edits_bot"),
                ("Gross bytes", "gross_bytes"),
                ("Reverts", "reverts"),
                ("Most edited article in region", "top_article"),
            ], aria_label="Regional RecentChanges summary leaderboard")
            if country_rollup else notice("No regional rows", "Select another time range or region filter."),
        ),
        panel(
            f"Top edited articles in region ({'All Regions' if selected_region == 'ALL' else selected_region})",
            "Click any article title to open its detailed Page Investigation view",
            table(map_articles, [
                ("Article", "title"),
                ("Country / region", "country_or_region"),
                ("Macro-region", "macro_region"),
                ("Edits (range)", "edits_total"),
                ("Human", "edits_human"),
                ("Bots", "edits_bot"),
                ("Gross bytes", "gross_bytes"),
                ("Net bytes", "net_bytes"),
                ("Reverts", "reverts"),
                ("Geo source", "location_source"),
                ("Coordinates", "coordinates"),
            ], article_links=True, aria_label="Top edited articles in region")
            if map_articles else notice("No located articles in range", "Choose a broader time range or All Regions."),
        ),
    ]


def render(data):
    """Render the RecentChanges Geospatial Map page with interactive [Days / Windows] and Region controls."""
    mode = data.get("requested_mode") or data.get("mode") or "days"
    time_range = data.get("time_range") or "55d"
    selected_region = data.get("region") or "ALL"
    available_regions = data.get("available_regions") or DEFAULT_REGIONS
    region_options = [
        {"label": "All Regions (Global)" if r == "ALL" else r, "value": r}
        for r in available_regions
    ]

    return [
        heading(
            "GEOSPATIAL / RECENTCHANGES REGIONAL MAP",
            "Regional edits across days and windows.",
            "Explore historical RecentChanges edit volume, human vs bot activity, byte churn, and reverts by region. "
            "Switch between Daily (55 days: Sep 15 – Nov 08) and Windowed (5m / 1h) granularity from the curated ingest_curate lake.",
        ),
        html.Div([
            html.Div([
                html.Label("GRANULARITY [DAYS / WINDOWS]"),
                dcc.RadioItems(
                    id="geo-mode",
                    value=mode if mode in ("days", "windows_5m", "windows_1h") else "days",
                    inline=True,
                    className="window-picker",
                    options=MODE_OPTIONS,
                ),
            ], className="control window-control"),
            html.Div([
                html.Label("TIME RANGE (DAYS / WINDOWS)"),
                dcc.Dropdown(
                    id="geo-range",
                    value=time_range,
                    clearable=False,
                    options=RANGE_OPTIONS,
                ),
            ], className="control", style={"minWidth": "295px"}),
            html.Div([
                html.Label("REGION / COUNTRY"),
                dcc.Dropdown(
                    id="geo-region",
                    value=selected_region,
                    clearable=False,
                    options=region_options,
                ),
            ], className="control", style={"minWidth": "205px"}),
            html.Div([
                html.Label("MAP PROJECTION"),
                dcc.Dropdown(
                    id="geo-projection",
                    value="natural earth",
                    clearable=False,
                    options=PROJECTION_OPTIONS,
                ),
            ], className="control", style={"minWidth": "185px"}),
            html.Div([
                html.Label("SHADING & BUBBLE METRIC"),
                dcc.Dropdown(
                    id="geo-metric",
                    value="total_edits",
                    clearable=False,
                    options=METRIC_OPTIONS,
                ),
            ], className="control", style={"minWidth": "175px"}),
            html.Button(
                "Reset to Global",
                id="geo-reset-btn",
                n_clicks=0,
                className="restart-button",
                style={"height": "42px"},
            ),
        ], className="controls", style={"marginBottom": "18px"}),
        html.Div(render_content(data), id="geo-map-content"),
    ]
