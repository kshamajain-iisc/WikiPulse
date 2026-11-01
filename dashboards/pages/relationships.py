"""Related-page links and article-subject geography from curated enrichment."""

from dash import html

from dashboards import charts
from dashboards.components import graph, heading, metric, number, panel, table


def render(data):
    edges, points = data.get("edges", []), data.get("points", [])
    located = [point for point in points if point.get("has_coordinates")]
    return [
        heading("EXPLORE / RELATIONSHIPS & PLACES", "Knowledge doesn't change in isolation.",
                "Explore recorded article relationships and the geography of their subjects, never editor locations."),
        html.Div([
            metric("DISPLAYED LINKS", number(len(edges)), "Up to 200 recorded edges", True),
            metric("DISPLAYED SUBJECTS", number(len(points)), "Up to 200 location records"),
            metric("WITH COORDINATES", number(len(located)), "Recorded location provenance required"),
            metric("WITHOUT COORDINATES", number(len(points) - len(located)), "Unlocated topics remain valid"),
        ], className="metric-grid"),
        html.Div([
            panel("Related-page network", "Circular layout · up to 60 nodes · links do not establish causation",
                  graph(charts.relationship_graph(edges))),
            panel("Article-subject map", "Wikipedia / Wikidata enrichment; no editor IP geolocation",
                  graph(charts.subject_map(points))),
        ], className="chart-grid"),
        panel("Link evidence", "Source and target labels preserve the recorded edge direction",
              table(edges, [("Source article", "source_title"), ("Target article", "target_title"),
                            ("Relationship", "edge_type"), ("Snapshot", "snapshot_ts")])),
        panel("Location provenance", "Source and quality for every displayed subject record",
              table(points, [("Article", "title"), ("Country / region", "country_or_region"),
                    ("Source", "location_source"), ("Quality", "location_quality")], True)),
    ]
