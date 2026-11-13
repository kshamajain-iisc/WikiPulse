"""Milestone 6.2: Global Geospatial Subject Map (`curated/map_points`, `MAP_POINT_SCHEMA`).

Resolves article-subject locations (never editor IP addresses) for alerted Wikipedia pages using a
2-tier spatial resolution pipeline with local disk caching (`curated/cache/geo/`):
- **Tier 1 (`wikipedia_coord` -> `exact_point`)**:
    * MediaWiki `action=query&prop=coordinates|pageprops` primary article coordinates.
- **Tier 2 (`wikidata_p625` / `wikidata_p276_p17` -> `exact_point` | `related_entity_point` | `country_centroid`)**:
    * Wikidata `wbgetentities` via `wikibase_item` QID:
      - Direct `P625` coordinate statement on the article subject.
      - Second-hop `P625` lookup on linked spatial entities (`P276` location, `P131` administrative
        territory, `P159` headquarters) -> `related_entity_point`.
      - Second-hop `P625` lookup on linked country entities (`P17` country, `P27` citizenship,
        `P495` country of origin, `P1001` jurisdiction) -> `country_centroid`.
- **Unlocated (`none` -> `unlocated`)**:
    * Topics without physical coordinates remain valid alerts (`has_coordinates = False`).

Also updates `has_location` on `curated/alerts` so alerts reflect resolved subject coordinates.

Usage:
    .venv/bin/python3 Repo/src/enrichment/geospatial_map.py
"""

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql import DataFrame, functions as F
from src.config.settings import (
    CURATED_TABLES,
    USER_AGENT,
    WIKIMEDIA_APIS,
    build_page_links_coords_params,
    build_wikidata_entities_params,
    get_spark_session,
    safe_read_parquet,
    safe_write_parquet,
)
from src.models.schemas import ALERT_SCHEMA, MAP_POINT_SCHEMA
from src.enrichment.alert_flags import historical_alert_filter

ACTION_API_URL = WIKIMEDIA_APIS["mediawiki_action_api"]["url"]
WIKIDATA_API_URL = WIKIMEDIA_APIS["wikidata_action_api"]["url"]
GEO_CACHE_DIR = CURATED_TABLES["api_cache"] / "geo"

# Wikidata property priority for spatial resolution
RELATED_ENTITY_PROPS = ("P276", "P131", "P159", "P291")
COUNTRY_PROPS = ("P17", "P495", "P1001", "P27")


def _geo_cache_file(prefix: str, key: str) -> Path:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:20]
    return GEO_CACHE_DIR / f"{prefix}_{digest}.json"


def _valid_lat_lon(lat: Any, lon: Any) -> tuple[float, float] | None:
    try:
        lat_f = float(lat)
        lon_f = float(lon)
        if -90.0 <= lat_f <= 90.0 and -180.0 <= lon_f <= 180.0:
            return round(lat_f, 5), round(lon_f, 5)
    except (TypeError, ValueError):
        pass
    return None


def fetch_wikipedia_coords_and_qids(
    session: requests.Session,
    titles: list[str],
    offline: bool = False,
) -> dict[str, dict[str, Any]]:
    """Batch query MediaWiki `prop=coordinates|pageprops` for up to 50 titles per request."""
    GEO_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    out: dict[str, dict[str, Any]] = {}

    batch_size = 45
    for idx in range(0, len(titles), batch_size):
        batch = titles[idx : idx + batch_size]
        cache_file = _geo_cache_file("wiki_coords", "|".join(sorted(batch)))
        payload = None
        if cache_file.exists():
            try:
                payload = json.loads(cache_file.read_text(encoding="utf-8"))
            except Exception:
                payload = None

        if payload is None and not offline:
            params = {
                "action": "query",
                "titles": "|".join(batch),
                "prop": "coordinates|pageprops",
                "ppprop": "wikibase_item",
                "coprimary": "all",
                "redirects": "1",
                "format": "json",
                "formatversion": "2",
                "maxlag": "5",
            }
            try:
                resp = session.get(ACTION_API_URL, params=params, timeout=15)
                if resp.status_code == 200:
                    payload = resp.json()
                    cache_file.write_text(json.dumps(payload), encoding="utf-8")
                time.sleep(0.2)
            except Exception:
                payload = None

        if not payload:
            continue

        query = payload.get("query", {})
        redirect_map: dict[str, str] = {}
        for r in query.get("redirects", []):
            if r.get("from") and r.get("to"):
                redirect_map[r["to"]] = r["from"]

        for page in query.get("pages", []):
            if page.get("missing"):
                continue
            p_title = page.get("title", "")
            coords_list = page.get("coordinates", [])
            chosen_coord = None
            if coords_list:
                # Prefer primary coordinate if marked, else first valid coordinate
                primaries = [c for c in coords_list if c.get("primary")]
                cand = primaries[0] if primaries else coords_list[0]
                chosen_coord = _valid_lat_lon(cand.get("lat"), cand.get("lon"))

            qid = page.get("pageprops", {}).get("wikibase_item")
            record = {
                "pageid": int(page.get("pageid") or 0),
                "title": p_title,
                "wikibase_item": qid,
                "wiki_coord": chosen_coord,
            }
            out[p_title] = record
            orig_title = redirect_map.get(p_title)
            if orig_title:
                out[orig_title] = record

    return out


def _extract_p625_from_entity(entity: dict[str, Any]) -> tuple[float, float] | None:
    """Extract `(lat, lon)` from Wikidata `P625` claim on an entity."""
    claims = entity.get("claims", {})
    for stmt in claims.get("P625", []):
        mainsnak = stmt.get("mainsnak", {})
        datavalue = mainsnak.get("datavalue", {})
        val = datavalue.get("value", {})
        if isinstance(val, dict):
            coord = _valid_lat_lon(val.get("latitude"), val.get("longitude"))
            if coord is not None:
                return coord
    return None


def _extract_linked_qids(entity: dict[str, Any], prop_id: str) -> list[str]:
    """Extract target entity QIDs (`Q...`) for a given Wikidata property (`P17`, `P276`, `P131`, etc.)."""
    claims = entity.get("claims", {})
    qids: list[str] = []
    for stmt in claims.get(prop_id, []):
        mainsnak = stmt.get("mainsnak", {})
        datavalue = mainsnak.get("datavalue", {})
        val = datavalue.get("value", {})
        if isinstance(val, dict) and val.get("id", "").startswith("Q"):
            qids.append(str(val["id"]))
    return qids


def _extract_label(entity: dict[str, Any]) -> str | None:
    labels = entity.get("labels", {})
    en_obj = labels.get("en")
    if isinstance(en_obj, dict) and en_obj.get("value"):
        return str(en_obj["value"])
    return None


def fetch_wikidata_entities(
    session: requests.Session,
    qids: list[str],
    offline: bool = False,
) -> dict[str, dict[str, Any]]:
    """Batch fetch Wikidata entities by QID list (up to 45 per request) with disk caching."""
    GEO_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    unique_qids = sorted({q for q in qids if q and q.startswith("Q")})
    entities_map: dict[str, dict[str, Any]] = {}

    batch_size = 45
    for idx in range(0, len(unique_qids), batch_size):
        batch = unique_qids[idx : idx + batch_size]
        cache_file = _geo_cache_file("wikidata_qids", "|".join(batch))
        payload = None
        if cache_file.exists():
            try:
                payload = json.loads(cache_file.read_text(encoding="utf-8"))
            except Exception:
                payload = None

        if payload is None and not offline:
            params = build_wikidata_entities_params(ids=batch)
            try:
                resp = session.get(WIKIDATA_API_URL, params=params, timeout=18)
                if resp.status_code == 200:
                    payload = resp.json()
                    cache_file.write_text(json.dumps(payload), encoding="utf-8")
                time.sleep(0.25)
            except Exception:
                payload = None

        if not payload:
            continue

        for qid, ent in payload.get("entities", {}).items():
            if isinstance(ent, dict) and "missing" not in ent:
                entities_map[qid] = ent

    return entities_map


def resolve_alert_locations(
    spark,
    alerts_df: DataFrame,
    session: requests.Session,
    max_pages: int = 160,
    offline: bool = False,
) -> tuple[DataFrame, dict[str, Any]]:
    """Resolve subject locations for top alerted pages and build `MAP_POINT_SCHEMA` DataFrame."""
    # Group alerts by pageid/title to get peak alert per article
    grouped_alerts = (
        alerts_df.groupBy("pageid", "title")
        .agg(
            F.max_by("alert_id", "historical_score").alias("alert_id"),
            F.max("historical_score").alias("historical_score"),
            F.max_by("alert_type", "historical_score").alias("alert_type"),
            F.max("edits_human").alias("edits_human"),
            F.max(historical_alert_filter(alerts_df).cast("int")).alias("is_hist_alert"),
        )
        .orderBy(F.desc("is_hist_alert"), F.desc("historical_score"), F.desc("edits_human"))
        .limit(max_pages)
        .collect()
    )

    titles = [str(r["title"]) for r in grouped_alerts]
    wiki_meta = fetch_wikipedia_coords_and_qids(session, titles, offline=offline)

    # First-hop Wikidata QIDs
    first_hop_qids = [
        meta["wikibase_item"]
        for meta in wiki_meta.values()
        if meta.get("wikibase_item")
    ]
    first_hop_entities = fetch_wikidata_entities(session, first_hop_qids, offline=offline)

    # Collect second-hop QIDs for related spatial entities (`P276`, `P131`, `P159`) and countries (`P17`, `P27`, `P495`)
    second_hop_qids: set[str] = set()
    for ent in first_hop_entities.values():
        for p in RELATED_ENTITY_PROPS + COUNTRY_PROPS:
            for target_qid in _extract_linked_qids(ent, p)[:2]:
                second_hop_qids.add(target_qid)

    second_hop_entities = fetch_wikidata_entities(session, list(second_hop_qids), offline=offline)
    all_wd_entities = {**second_hop_entities, **first_hop_entities}

    # Third-hop country QIDs from related spatial entities (so if an event has P276 -> Venue, we also get Venue's P17 -> Country label)
    third_hop_qids: set[str] = set()
    for qid in second_hop_qids:
        ent = all_wd_entities.get(qid)
        if ent:
            for c_qid in _extract_linked_qids(ent, "P17")[:1]:
                if c_qid not in all_wd_entities:
                    third_hop_qids.add(c_qid)
    if third_hop_qids:
        third_hop_entities = fetch_wikidata_entities(session, list(third_hop_qids), offline=offline)
        all_wd_entities.update(third_hop_entities)

    map_rows: list[tuple[Any, ...]] = []
    source_counts: dict[str, int] = {
        "wikipedia_coord": 0,
        "wikidata_p625": 0,
        "wikidata_p276_p17": 0,
        "none": 0,
    }
    quality_counts: dict[str, int] = {
        "exact_point": 0,
        "related_entity_point": 0,
        "country_centroid": 0,
        "unlocated": 0,
    }
    country_rollup: dict[str, dict[str, Any]] = {}

    for row in grouped_alerts:
        pageid = int(row["pageid"])
        title = str(row["title"])
        alert_id = str(row["alert_id"]) if row["alert_id"] else None
        hist_score = round(float(row["historical_score"] or 0.0), 2)
        alert_type = str(row["alert_type"]) if row["alert_type"] else None
        edits_human = int(row["edits_human"] or 0)

        meta = wiki_meta.get(title, {})
        qid = meta.get("wikibase_item")
        wiki_coord = meta.get("wiki_coord")

        lat: float | None = None
        lon: float | None = None
        country_or_region: str | None = None
        location_source = "none"
        location_quality = "unlocated"

        ent = all_wd_entities.get(qid) if qid else None

        # Resolve country/region label from Wikidata if available
        if ent:
            for c_prop in COUNTRY_PROPS:
                for c_qid in _extract_linked_qids(ent, c_prop):
                    c_ent = all_wd_entities.get(c_qid)
                    if c_ent and _extract_label(c_ent):
                        country_or_region = _extract_label(c_ent)
                        break
                if country_or_region:
                    break

        # Tier 1: Direct Wikipedia primary coordinate
        if wiki_coord is not None:
            lat, lon = wiki_coord
            location_source = "wikipedia_coord"
            location_quality = "exact_point"
            if not country_or_region and ent:
                for r_prop in RELATED_ENTITY_PROPS:
                    for r_qid in _extract_linked_qids(ent, r_prop):
                        r_ent = all_wd_entities.get(r_qid)
                        if r_ent and _extract_label(r_ent):
                            country_or_region = _extract_label(r_ent)
                            break
                    if country_or_region:
                        break

        # Tier 2A: Direct Wikidata P625 coordinate on the item itself
        elif ent is not None:
            direct_p625 = _extract_p625_from_entity(ent)
            if direct_p625 is not None:
                lat, lon = direct_p625
                location_source = "wikidata_p625"
                location_quality = "exact_point"
            else:
                # Tier 2B: Related spatial entity (`P276` location, `P131` admin territory, `P159` HQ)
                for r_prop in RELATED_ENTITY_PROPS:
                    for r_qid in _extract_linked_qids(ent, r_prop):
                        r_ent = all_wd_entities.get(r_qid)
                        if not r_ent:
                            continue
                        r_coord = _extract_p625_from_entity(r_ent)
                        if r_coord is not None:
                            lat, lon = r_coord
                            location_source = "wikidata_p276_p17"
                            location_quality = "related_entity_point"
                            r_label = _extract_label(r_ent)
                            if not country_or_region:
                                # Check if the related entity has a P17 country
                                for rc_qid in _extract_linked_qids(r_ent, "P17"):
                                    rc_ent = all_wd_entities.get(rc_qid)
                                    if rc_ent and _extract_label(rc_ent):
                                        country_or_region = _extract_label(rc_ent)
                                        break
                                if not country_or_region and r_label:
                                    country_or_region = r_label
                            break
                    if lat is not None:
                        break

                # Tier 2C: Country centroid (`P17` country, `P495` origin, `P1001` jurisdiction, `P27` citizenship)
                if lat is None:
                    for c_prop in COUNTRY_PROPS:
                        for c_qid in _extract_linked_qids(ent, c_prop):
                            c_ent = all_wd_entities.get(c_qid)
                            if not c_ent:
                                continue
                            c_coord = _extract_p625_from_entity(c_ent)
                            if c_coord is not None:
                                lat, lon = c_coord
                                location_source = "wikidata_p276_p17"
                                location_quality = "country_centroid"
                                if not country_or_region:
                                    country_or_region = _extract_label(c_ent)
                                break
                        if lat is not None:
                            break

        has_coords = bool(lat is not None and lon is not None)
        if has_coords and not country_or_region:
            country_or_region = "Global / Specified Coordinates"

        source_counts[location_source] = source_counts.get(location_source, 0) + 1
        quality_counts[location_quality] = quality_counts.get(location_quality, 0) + 1

        if country_or_region:
            entry = country_rollup.setdefault(
                country_or_region,
                {"country_or_region": country_or_region, "active_topics": 0, "total_edits_human": 0, "max_score": 0.0},
            )
            entry["active_topics"] += 1
            entry["total_edits_human"] += edits_human
            entry["max_score"] = round(max(entry["max_score"], hist_score), 2)

        map_rows.append(
            (
                pageid,
                title,
                alert_id,
                qid,
                has_coords,
                lat,
                lon,
                country_or_region,
                location_source,
                location_quality,
                edits_human,
                hist_score,
                alert_type,
            )
        )

    map_df = spark.createDataFrame(map_rows, schema=MAP_POINT_SCHEMA).dropDuplicates(["pageid"])
    located_count = sum(1 for r in map_rows if r[4])
    unlocated_count = len(map_rows) - located_count
    top_countries = sorted(
        country_rollup.values(),
        key=lambda x: (-x["active_topics"], -x["total_edits_human"], -x["max_score"]),
    )[:15]

    stats = {
        "total_alert_subjects_evaluated": len(map_rows),
        "located_subjects_count": located_count,
        "unlocated_subjects_count": unlocated_count,
        "located_coverage_pct": round(100.0 * located_count / max(1, len(map_rows)), 2),
        "location_source_breakdown": source_counts,
        "location_quality_breakdown": quality_counts,
        "unlocated_alerts_preserved_valid": True,
        "editor_ip_geolocation_used": False,
        "top_countries_or_regions": top_countries,
    }
    return map_df, stats


def update_alerts_has_location(spark, alerts_df: DataFrame, map_df: DataFrame) -> int:
    """Update `has_location` in `curated/alerts` based on resolved coordinates in `map_points`."""
    loc_lookup = (
        map_df.filter("has_coordinates = true")
        .select(F.col("pageid"), F.lit(True).alias("_resolved_has_loc"))
        .dropDuplicates(["pageid"])
    )
    updated = (
        alerts_df.join(loc_lookup, on="pageid", how="left")
        .withColumn("has_location", F.coalesce(F.col("_resolved_has_loc"), F.col("has_location"), F.lit(False)))
        .drop("_resolved_has_loc")
    )
    alert_cols = [
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in ALERT_SCHEMA.fields
    ]
    final_alerts = updated.select(*alert_cols)
    safe_write_parquet(final_alerts, CURATED_TABLES["alerts"], repartition_num=8)
    saved = safe_read_parquet(spark, CURATED_TABLES["alerts"])
    return int(saved.filter("has_location = true").count())


def main() -> int:
    ap = argparse.ArgumentParser(description="Milestone 6.2: Build Global Geospatial Subject Map (`curated/map_points`).")
    ap.add_argument("--max-pages", type=int, default=160, help="Number of alerted article subjects to resolve.")
    ap.add_argument("--offline", action="store_true", help="Use cached responses only.")
    ap.add_argument("--skip-alert-update", action="store_true", help="Build map_points without rewriting the alerts table.")
    args = ap.parse_args()

    print("=" * 78)
    print(" WIKIPULSE MILESTONE 6.2: GLOBAL GEOSPATIAL SUBJECT MAP ENRICHMENT")
    print("=" * 78)

    t0 = time.time()
    spark = get_spark_session(app_name="WikiPulse-Milestone6-GeospatialMap")

    alerts_df = safe_read_parquet(spark, CURATED_TABLES["alerts"])

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

    print(f"\n[Step 1] Resolving Tier-1 (Wikipedia Coordinates) & Tier-2 (Wikidata P625/P276/P131/P17) subject locations for top {args.max_pages} alerted pages...")
    map_df, geo_stats = resolve_alert_locations(
        spark,
        alerts_df,
        session,
        max_pages=args.max_pages,
        offline=args.offline,
    )

    out_map = CURATED_TABLES["map_points"]
    safe_write_parquet(map_df, out_map, repartition_num=2)
    saved_map = safe_read_parquet(spark, out_map)

    print(f"  ✓ Wrote {saved_map.count():,} subject map records -> {out_map}")
    print(f"    • Located subjects         : {geo_stats['located_subjects_count']} / {geo_stats['total_alert_subjects_evaluated']} ({geo_stats['located_coverage_pct']}%)")
    print(f"    • Unlocated (kept valid)   : {geo_stats['unlocated_subjects_count']}")
    print(f"    • Location source breakdown: {geo_stats['location_source_breakdown']}")
    print(f"    • Location quality tags    : {geo_stats['location_quality_breakdown']}")

    alerts_with_loc = 0
    if not args.skip_alert_update:
        print("\n[Step 2] Updating `has_location` flag on `curated/alerts`...")
        alerts_with_loc = update_alerts_has_location(spark, alerts_df, saved_map)
        print(f"  ✓ Updated `curated/alerts` (`{alerts_with_loc:,}` alert windows now linked to resolved subject coordinates)")
    else:
        print("\n[Step 2] Skipping alerts rewrite; the live alert table is left intact.")

    print("\n  Sample Located Article Subjects (Sorted by Historical Score):")
    (
        saved_map.filter("has_coordinates = true")
        .orderBy(F.desc("historical_score"), F.desc("edits_human"))
        .select(
            "title",
            "wikibase_item",
            "lat",
            "lon",
            "country_or_region",
            "location_source",
            "location_quality",
            "historical_score",
        )
        .show(12, truncate=30)
    )

    print("  Sample Unlocated Article Subjects (Preserved as Valid Non-Spatial Alerts):")
    (
        saved_map.filter("has_coordinates = false")
        .orderBy(F.desc("historical_score"))
        .select("title", "wikibase_item", "location_source", "location_quality", "historical_score", "alert_type")
        .show(6, truncate=34)
    )

    geo_stats["alerts_windows_with_location"] = alerts_with_loc
    geo_stats["runtime_sec"] = round(time.time() - t0, 2)
    bench_dir = CURATED_TABLES["benchmarks"]
    bench_dir.mkdir(parents=True, exist_ok=True)
    (bench_dir / "milestone6_geospatial_summary.json").write_text(json.dumps(geo_stats, indent=2))

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

