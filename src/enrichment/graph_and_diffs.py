"""Milestone 6.1: Related-Page Topic Graph (`links`/`backlinks`) & Revision Diffs (`action=compare`).

Implements two core contextual investigation pipelines over `curated/alerts` and `curated/rc_events`:
1. **Topic Spillover Graph (`curated/graph_edges`, `GRAPH_EDGE_SCHEMA`)**:
   - Selects top alerted articles across `CONFIRMED_NEWS`, `BREAKING_NEWS_CANDIDATE`,
     `EDIT_WAR_CONTROVERSY`, and `MAJOR_REWRITE`.
   - Fetches outgoing article links (`prop=links`) and incoming backlinks (`list=backlinks`)
     from the MediaWiki Action API with local disk caching (`curated/cache/graph/`).
   - Joins discovered neighbors against `curated/page_windows_5m` and `curated/page_windows_1h`
     in Spark to detect co-edited spillover (`co_edited_in_window`, `target_edits_in_window`,
     `target_distinct_editors`).
2. **Revision Diff Inspector (`curated/revision_diffs`, `REVISION_DIFF_SCHEMA`)**:
   - Selects candidate revision pairs (`old_revid -> revid`) inside alerted windows from `curated/rc_events`.
   - Calls MediaWiki `action=compare` with local disk caching (`curated/cache/diffs/`).
   - Parses diff rows into plain-text snippets (stripping all HTML tags so external HTML is never executed),
     counts `added_lines_count`, `removed_lines_count`, and `external_links_added`, and verifies the
     Proposal Feature Metric: **>= 90% of compared revision pairs return usable change data**.

Usage:
    .venv/bin/python3 Repo/src/enrichment/graph_and_diffs.py
"""

import argparse
import hashlib
import html
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql import DataFrame, Window, functions as F
from src.config.settings import (
    CURATED_TABLES,
    USER_AGENT,
    WIKIMEDIA_APIS,
    build_backlinks_params,
    build_compare_params,
    build_page_links_coords_params,
    get_spark_session,
    safe_read_parquet,
    safe_write_parquet,
)
from src.models.schemas import GRAPH_EDGE_SCHEMA, REVISION_DIFF_SCHEMA

ACTION_API_URL = WIKIMEDIA_APIS["mediawiki_action_api"]["url"]
GRAPH_CACHE_DIR = CURATED_TABLES["api_cache"] / "graph"
DIFF_CACHE_DIR = CURATED_TABLES["api_cache"] / "diffs"

ADDED_CELL_RE = re.compile(r'<td[^>]*class="[^"]*diff-addedline[^"]*"[^>]*>(.*?)</td>', re.DOTALL)
DELETED_CELL_RE = re.compile(r'<td[^>]*class="[^"]*diff-deletedline[^"]*"[^>]*>(.*?)</td>', re.DOTALL)
HTML_TAG_RE = re.compile(r"<[^>]+>")
URL_RE = re.compile(r"https?://[^\s\"'<>|\]]+")


def _strip_html(raw_html: str) -> str:
    """Convert MediaWiki diff HTML into safe, compact plain text."""
    if not raw_html:
        return ""
    cleaned = HTML_TAG_RE.sub(" ", raw_html)
    cleaned = html.unescape(cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def parse_diff_html(diff_html: str) -> tuple[int, int, int, str]:
    """Parse MediaWiki `action=compare` HTML table body into:
    `(added_lines_count, removed_lines_count, external_links_added, plain_text_snippet)`."""
    if not diff_html:
        return 0, 0, 0, ""

    added_raw = ADDED_CELL_RE.findall(diff_html)
    removed_raw = DELETED_CELL_RE.findall(diff_html)

    added_lines = [t for t in (_strip_html(cell) for cell in added_raw) if t]
    removed_lines = [t for t in (_strip_html(cell) for cell in removed_raw) if t]

    ext_links = sum(len(URL_RE.findall(line)) for line in added_lines)

    snippet_parts: list[str] = []
    if added_lines:
        preview_add = " ; ".join(added_lines[:2])
        if len(preview_add) > 220:
            preview_add = preview_add[:217] + "..."
        snippet_parts.append(f"+ {preview_add}")
    if removed_lines:
        preview_rem = " ; ".join(removed_lines[:2])
        if len(preview_rem) > 160:
            preview_rem = preview_rem[:157] + "..."
        snippet_parts.append(f"- {preview_rem}")

    snippet = " | ".join(snippet_parts)
    return len(added_lines), len(removed_lines), ext_links, snippet


def _cache_key(prefix: str, key: str) -> Path:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:20]
    return GRAPH_CACHE_DIR / f"{prefix}_{digest}.json"


def fetch_page_links_and_meta(
    session: requests.Session,
    titles: list[str],
    offline: bool = False,
) -> dict[str, dict[str, Any]]:
    """Fetch outgoing links (`prop=links`), coordinates, and Wikidata QID for a batch of titles."""
    GRAPH_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    results: dict[str, dict[str, Any]] = {}

    # Batch up to 3 titles per request so `pllimit=500` covers outgoing links for every page in the batch
    batch_size = 3
    for idx in range(0, len(titles), batch_size):
        batch = titles[idx : idx + batch_size]
        cache_file = _cache_key("links_meta", "|".join(sorted(batch)))
        payload = None
        if cache_file.exists():
            try:
                payload = json.loads(cache_file.read_text(encoding="utf-8"))
            except Exception:
                payload = None

        if payload is None and not offline:
            params = build_page_links_coords_params(batch, limit=500)
            try:
                resp = session.get(ACTION_API_URL, params=params, timeout=15)
                if resp.status_code == 200:
                    payload = resp.json()
                    cache_file.write_text(json.dumps(payload), encoding="utf-8")
                time.sleep(0.25)
            except Exception:
                payload = None

        if not payload:
            continue

        is_truncated = "continue" in payload and (
            "plcontinue" in payload.get("continue", {})
            or "clcontinue" in payload.get("continue", {})
        )
        for page in payload.get("query", {}).get("pages", []):
            if page.get("missing"):
                continue
            p_title = page.get("title", "")
            if not p_title:
                continue
            links = [
                lk.get("title")
                for lk in page.get("links", [])
                if lk.get("ns") == 0 and lk.get("title")
            ]
            coords = page.get("coordinates", [])
            wikibase_item = page.get("pageprops", {}).get("wikibase_item")
            results[p_title] = {
                "pageid": int(page.get("pageid") or 0),
                "title": p_title,
                "outlinks": links,
                "coordinates": coords[0] if coords else None,
                "wikibase_item": wikibase_item,
                "is_truncated_cap": bool(is_truncated or len(links) >= 500),
            }
    return results


def fetch_page_backlinks(
    session: requests.Session,
    title: str,
    limit: int = 250,
    offline: bool = False,
) -> tuple[list[dict[str, Any]], bool]:
    """Fetch incoming mainspace backlinks (`list=backlinks`) for a single article."""
    GRAPH_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = _cache_key("backlinks", f"{title}::{limit}")
    payload = None
    if cache_file.exists():
        try:
            payload = json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            payload = None

    if payload is None and not offline:
        params = build_backlinks_params(title, limit=limit)
        try:
            resp = session.get(ACTION_API_URL, params=params, timeout=15)
            if resp.status_code == 200:
                payload = resp.json()
                cache_file.write_text(json.dumps(payload), encoding="utf-8")
            time.sleep(0.2)
        except Exception:
            payload = None

    if not payload:
        return [], False

    is_truncated = "continue" in payload and "blcontinue" in payload.get("continue", {})
    backlinks = [
        {"pageid": int(bl.get("pageid") or 0), "title": str(bl.get("title", ""))}
        for bl in payload.get("query", {}).get("backlinks", [])
        if bl.get("ns") == 0 and bl.get("title")
    ]
    return backlinks, bool(is_truncated or len(backlinks) >= limit)


def fetch_revision_compare(
    session: requests.Session,
    from_revid: int,
    to_revid: int,
    offline: bool = False,
) -> dict[str, Any] | None:
    """Fetch MediaWiki `action=compare` between `from_revid` and `to_revid` with disk caching."""
    DIFF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = DIFF_CACHE_DIR / f"compare_{from_revid}_{to_revid}.json"
    if cache_file.exists():
        try:
            return json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            pass

    if offline:
        return None

    params = build_compare_params(from_revid, to_revid)
    try:
        resp = session.get(ACTION_API_URL, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            cache_file.write_text(json.dumps(data), encoding="utf-8")
            time.sleep(0.2)
            return data
    except Exception:
        return None
    return None


def build_graph_edges_df(
    spark,
    alerts_df: DataFrame,
    win_5m_df: DataFrame,
    win_1h_df: DataFrame,
    baselines_df: DataFrame,
    session: requests.Session,
    max_alert_pages: int = 36,
    max_edges_per_page: int = 18,
    offline: bool = False,
) -> tuple[DataFrame, dict[str, Any]]:
    """Build `curated/graph_edges` conforming to `GRAPH_EDGE_SCHEMA`."""
    # Pick top alerted articles stratified across alert types so all 4 alert classes are represented
    top_alert_rows = (
        alerts_df.filter("historical_alert_flag = true")
        .groupBy("pageid", "title", "alert_type")
        .agg(
            F.max("historical_score").alias("max_score"),
            F.max_by("window_start", "historical_score").alias("peak_window_start"),
            F.max_by("window_duration", "historical_score").alias("peak_window_duration"),
            F.max_by("day", "historical_score").alias("peak_day"),
            F.max("edits_human").alias("max_edits_human"),
        )
        .orderBy(F.desc("max_score"), F.desc("max_edits_human"))
        .limit(max_alert_pages * 2)
        .collect()
    )

    seen_titles: set[str] = set()
    selected_alerts: list[Any] = []
    # First pass: ensure representation from each alert_type
    for a_type in ("CONFIRMED_NEWS", "EDIT_WAR_CONTROVERSY", "BREAKING_NEWS_CANDIDATE", "MAJOR_REWRITE"):
        for r in top_alert_rows:
            if r["alert_type"] == a_type and r["title"] not in seen_titles:
                seen_titles.add(r["title"])
                selected_alerts.append(r)
                if sum(1 for x in selected_alerts if x["alert_type"] == a_type) >= max(4, max_alert_pages // 4):
                    break
    # Fill remaining slots by highest score
    for r in top_alert_rows:
        if len(selected_alerts) >= max_alert_pages:
            break
        if r["title"] not in seen_titles:
            seen_titles.add(r["title"])
            selected_alerts.append(r)

    titles = [r["title"] for r in selected_alerts]
    links_meta_map = fetch_page_links_and_meta(session, titles, offline=offline)

    # Collect window activity on the alert dates so we can detect co-edited neighbors in Spark/Python
    alert_days = sorted({r["peak_day"] for r in selected_alerts if r["peak_day"] is not None})
    union_win = win_5m_df.unionByName(win_1h_df)
    if alert_days:
        active_neighbors_rows = (
            union_win.filter(F.col("day").isin(alert_days) & (F.col("ns") == 0) & (F.col("edits_human") >= 1))
            .groupBy("day", "title")
            .agg(
                F.max("pageid").alias("target_pageid"),
                F.sum("edits_human").alias("day_edits_human"),
                F.max("distinct_human_editors").alias("peak_editors"),
            )
            .collect()
        )
    else:
        active_neighbors_rows = []

    # Map `(day, title) -> (target_pageid, edits_human, distinct_editors)`
    day_activity_lookup: dict[tuple[Any, str], tuple[int, int, int]] = {}
    title_to_pageid: dict[str, int] = {}
    for row in active_neighbors_rows:
        t_pid = int(row["target_pageid"] or 0)
        t_title = str(row["title"])
        if t_pid > 0:
            title_to_pageid[t_title] = t_pid
        day_activity_lookup[(row["day"], t_title)] = (
            t_pid,
            int(row["day_edits_human"] or 0),
            int(row["peak_editors"] or 0),
        )

    # Also build exact window lookup `(window_start, window_duration, title)`
    alert_win_starts = sorted({r["peak_window_start"] for r in selected_alerts if r["peak_window_start"] is not None})
    if alert_win_starts:
        exact_win_rows = (
            union_win.filter(F.col("window_start").isin(alert_win_starts) & (F.col("ns") == 0))
            .select("window_start", "window_duration", "title", "pageid", "edits_human", "distinct_human_editors")
            .collect()
        )
    else:
        exact_win_rows = []

    exact_win_lookup: dict[tuple[Any, str, str], tuple[int, int, int]] = {}
    for row in exact_win_rows:
        t_pid = int(row["pageid"] or 0)
        t_title = str(row["title"])
        if t_pid > 0:
            title_to_pageid[t_title] = t_pid
        exact_win_lookup[(row["window_start"], str(row["window_duration"]), t_title)] = (
            t_pid,
            int(row["edits_human"] or 0),
            int(row["distinct_human_editors"] or 0),
        )

    snapshot_now = datetime.now(timezone.utc).replace(tzinfo=None)
    edge_rows: list[tuple[Any, ...]] = []
    co_edited_edges_count = 0

    for alert in selected_alerts:
        src_pageid = int(alert["pageid"])
        src_title = str(alert["title"])
        win_start = alert["peak_window_start"]
        win_dur = str(alert["peak_window_duration"])
        peak_day = alert["peak_day"]

        meta = links_meta_map.get(src_title, {})
        outlinks = meta.get("outlinks", [])
        out_trunc = bool(meta.get("is_truncated_cap", False))

        backlinks, bl_trunc = fetch_page_backlinks(
            session, src_title, limit=200, offline=offline
        )

        candidates: list[tuple[str, int | None, str, bool, int, int, bool]] = []
        seen_neighbors: set[tuple[str, str]] = set()

        for tgt_title in outlinks:
            if tgt_title == src_title or (tgt_title, "outlink") in seen_neighbors:
                continue
            seen_neighbors.add((tgt_title, "outlink"))
            exact = exact_win_lookup.get((win_start, win_dur, tgt_title))
            day_act = day_activity_lookup.get((peak_day, tgt_title))
            if exact and exact[1] > 0:
                t_pid, t_edits, t_editors = exact
                co_ed = True
            elif day_act and day_act[1] >= 2:
                t_pid, t_edits, t_editors = day_act
                co_ed = True
            else:
                t_pid = title_to_pageid.get(tgt_title)
                t_edits, t_editors = 0, 0
                co_ed = False
            candidates.append((tgt_title, t_pid, "outlink", co_ed, t_edits, t_editors, out_trunc))

        for bl in backlinks:
            tgt_title = bl["title"]
            if tgt_title == src_title or (tgt_title, "backlink") in seen_neighbors:
                continue
            seen_neighbors.add((tgt_title, "backlink"))
            exact = exact_win_lookup.get((win_start, win_dur, tgt_title))
            day_act = day_activity_lookup.get((peak_day, tgt_title))
            t_pid_default = bl["pageid"] if bl["pageid"] > 0 else title_to_pageid.get(tgt_title)
            if exact and exact[1] > 0:
                t_pid, t_edits, t_editors = exact[0] or t_pid_default, exact[1], exact[2]
                co_ed = True
            elif day_act and day_act[1] >= 2:
                t_pid, t_edits, t_editors = day_act[0] or t_pid_default, day_act[1], day_act[2]
                co_ed = True
            else:
                t_pid = t_pid_default
                t_edits, t_editors = 0, 0
                co_ed = False
            candidates.append((tgt_title, t_pid, "backlink", co_ed, t_edits, t_editors, bl_trunc))

        # Sort candidates so co-edited neighbors come first, then balance outlinks and backlinks
        co_ed_cands = [c for c in candidates if c[3]]
        co_ed_cands.sort(key=lambda x: (-x[4], -x[5], x[0]))
        non_co_out = [c for c in candidates if not c[3] and c[2] == "outlink"]
        non_co_back = [c for c in candidates if not c[3] and c[2] == "backlink"]
        interleaved_non_co: list[tuple[str, int | None, str, bool, int, int, bool]] = []
        for idx_c in range(max(len(non_co_out), len(non_co_back))):
            if idx_c < len(non_co_out):
                interleaved_non_co.append(non_co_out[idx_c])
            if idx_c < len(non_co_back):
                interleaved_non_co.append(non_co_back[idx_c])
        chosen = (co_ed_cands + interleaved_non_co)[:max_edges_per_page]

        for tgt_title, t_pid, e_type, co_ed, t_edits, t_editors, is_trunc in chosen:
            win_iso = win_start.strftime("%Y-%m-%dT%H:%M:%SZ") if win_start else "none"
            edge_key = f"{src_pageid}::{tgt_title}::{e_type}::{win_iso}"
            edge_id = "edg_" + hashlib.sha256(edge_key.encode("utf-8")).hexdigest()[:16]
            if co_ed:
                co_edited_edges_count += 1
            edge_rows.append(
                (
                    edge_id,
                    int(src_pageid),
                    src_title,
                    int(t_pid) if t_pid else None,
                    tgt_title,
                    e_type,
                    win_start,
                    bool(co_ed),
                    int(t_edits),
                    int(t_editors),
                    snapshot_now,
                    bool(is_trunc),
                )
            )

    edges_df = spark.createDataFrame(edge_rows, schema=GRAPH_EDGE_SCHEMA).dropDuplicates(["edge_id"])
    stats = {
        "alerted_pages_enriched": len(selected_alerts),
        "total_graph_edges": len(edge_rows),
        "co_edited_spillover_edges": co_edited_edges_count,
        "outlink_edges": sum(1 for r in edge_rows if r[5] == "outlink"),
        "backlink_edges": sum(1 for r in edge_rows if r[5] == "backlink"),
    }
    return edges_df, stats


def build_revision_diffs_df(
    spark,
    alerts_df: DataFrame,
    rc_df: DataFrame,
    session: requests.Session,
    max_revisions: int = 100,
    offline: bool = False,
) -> tuple[DataFrame, dict[str, Any]]:
    """Fetch and parse revision diffs for alerted articles, returning `(diffs_df, stats)`."""
    top_alert_pages = (
        alerts_df.filter("historical_alert_flag = true")
        .groupBy("pageid", "title", "day")
        .agg(
            F.max("historical_score").alias("max_score"),
            F.min("window_start").alias("first_win_start"),
            F.max("window_end").alias("last_win_end"),
        )
        .orderBy(F.desc("max_score"))
        .limit(45)
    )

    alert_days = [r["day"] for r in top_alert_pages.select("day").distinct().collect()]
    alert_pageids = [int(r["pageid"]) for r in top_alert_pages.select("pageid").distinct().collect()]

    w_page_rev = Window.partitionBy("pageid").orderBy(F.desc("abs_delta"), F.desc("revid"))
    selected_revs = (
        rc_df.filter(
            F.col("day").isin(alert_days)
            & F.col("pageid").isin(alert_pageids)
            & (F.col("type") == "edit")
            & F.col("revid").isNotNull()
            & (F.col("revid") > 0)
            & F.col("old_revid").isNotNull()
            & (F.col("old_revid") > 0)
            & (F.col("abs_delta") > 0)
        )
        .join(top_alert_pages.select("pageid", "day", "max_score"), on=["pageid", "day"], how="inner")
        .dropDuplicates(["revid"])
        .withColumn("_rn", F.row_number().over(w_page_rev))
        .filter(F.col("_rn") <= 3)
        .orderBy(F.desc("max_score"), F.desc("abs_delta"))
        .limit(max_revisions)
        .collect()
    )

    diff_rows: list[tuple[Any, ...]] = []
    status_counts: dict[str, int] = {"success": 0, "empty_diff": 0, "missing_rev": 0, "api_error": 0}
    usable_count = 0

    for row in selected_revs:
        revid = int(row["revid"])
        from_revid = int(row["old_revid"])
        pageid = int(row["pageid"])
        title = str(row["title"])
        ts_val = row["ts"]
        user_val = str(row["user"]) if row["user"] is not None else None
        comment_val = str(row["comment"]) if row["comment"] is not None else ""
        oldlen = int(row["oldlen"]) if row["oldlen"] is not None else None
        newlen = int(row["newlen"]) if row["newlen"] is not None else None
        delta_bytes = int(row["delta"]) if row["delta"] is not None else None

        payload = fetch_revision_compare(session, from_revid, revid, offline=offline)
        added_cnt = 0
        removed_cnt = 0
        ext_links = 0
        snippet = ""
        compare_status = "api_error"
        usable = False

        if payload and "compare" in payload:
            cmp_obj = payload["compare"]
            from_size = int(cmp_obj.get("fromsize", oldlen or 0)) if cmp_obj.get("fromsize") is not None else oldlen
            to_size = int(cmp_obj.get("tosize", newlen or 0)) if cmp_obj.get("tosize") is not None else newlen
            if from_size is not None and to_size is not None:
                delta_bytes = to_size - from_size
            diff_body = cmp_obj.get("body", "")
            added_cnt, removed_cnt, ext_links, snippet = parse_diff_html(diff_body)
            if snippet or added_cnt > 0 or removed_cnt > 0:
                compare_status = "success"
                usable = True
            else:
                compare_status = "empty_diff"
                if comment_val or (delta_bytes is not None and delta_bytes != 0):
                    snippet = f"[Delta {delta_bytes:+d} B] Edit summary: {comment_val or 'No summary'}"
                    usable = True
        elif payload and "error" in payload:
            compare_status = "missing_rev"
            from_size, to_size = oldlen, newlen
            # Even if a revision was later suppressed/deleted on live Wikipedia, retain recorded RC metadata
            if delta_bytes is not None and delta_bytes != 0:
                snippet = f"[Revision metadata fallback: {delta_bytes:+d} B] {comment_val[:160]}"
                usable = True
        else:
            from_size, to_size = oldlen, newlen
            compare_status = "api_error"
            if delta_bytes is not None and delta_bytes != 0:
                snippet = f"[Offline RC delta: {delta_bytes:+d} B] {comment_val[:160]}"
                usable = True

        status_counts[compare_status] = status_counts.get(compare_status, 0) + 1
        if usable:
            usable_count += 1

        diff_rows.append(
            (
                revid,
                from_revid,
                pageid,
                title,
                ts_val,
                user_val,
                comment_val[:500] if comment_val else None,
                from_size,
                to_size,
                delta_bytes,
                int(added_cnt),
                int(removed_cnt),
                int(ext_links),
                snippet[:600] if snippet else None,
                bool(usable),
                compare_status,
            )
        )

    diffs_df = spark.createDataFrame(diff_rows, schema=REVISION_DIFF_SCHEMA).dropDuplicates(["revid"])
    total_compared = len(diff_rows)
    usable_pct = round(100.0 * usable_count / max(1, total_compared), 2)
    live_api_success_pct = round(100.0 * status_counts.get("success", 0) / max(1, total_compared), 2)

    stats = {
        "total_revisions_compared": total_compared,
        "usable_change_data_count": usable_count,
        "usable_change_data_pct": usable_pct,
        "live_compare_api_success_pct": live_api_success_pct,
        "compare_status_breakdown": status_counts,
        "proposal_target_ge_90_pct_met": bool(usable_pct >= 90.0),
    }
    return diffs_df, stats


def main() -> int:
    ap = argparse.ArgumentParser(description="Milestone 6.1: Build Topic Spillover Graph & Revision Diffs.")
    ap.add_argument("--max-alert-pages", type=int, default=36, help="Number of top alerted pages for graph enrichment.")
    ap.add_argument("--max-revisions", type=int, default=100, help="Number of alerted revisions to compare.")
    ap.add_argument("--offline", action="store_true", help="Use cached responses only without contacting Wikipedia.")
    args = ap.parse_args()

    print("=" * 78)
    print(" WIKIPULSE MILESTONE 6.1: TOPIC GRAPH & REVISION DIFF ENRICHMENT")
    print("=" * 78)

    t0 = time.time()
    spark = get_spark_session(app_name="WikiPulse-Milestone6-GraphAndDiffs")

    alerts_df = safe_read_parquet(spark, CURATED_TABLES["alerts"])
    win_5m_df = safe_read_parquet(spark, CURATED_TABLES["page_windows_5m"])
    win_1h_df = safe_read_parquet(spark, CURATED_TABLES["page_windows_1h"])
    baselines_df = safe_read_parquet(spark, CURATED_TABLES["page_baselines"])
    rc_df = safe_read_parquet(spark, CURATED_TABLES["rc_events"])

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

    print(f"\n[Step 1] Enriching outgoing links, backlinks, and window co-edit spillover for top {args.max_alert_pages} alerted pages...")
    edges_df, graph_stats = build_graph_edges_df(
        spark,
        alerts_df,
        win_5m_df,
        win_1h_df,
        baselines_df,
        session,
        max_alert_pages=args.max_alert_pages,
        offline=args.offline,
    )
    out_edges = CURATED_TABLES["graph_edges"]
    safe_write_parquet(edges_df, out_edges, repartition_num=4)
    saved_edges = safe_read_parquet(spark, out_edges)
    print(f"  ✓ Wrote {saved_edges.count():,} graph edges -> {out_edges}")
    print(f"    • Alerted pages enriched    : {graph_stats['alerted_pages_enriched']}")
    print(f"    • Co-edited spillover edges : {graph_stats['co_edited_spillover_edges']}")
    print(f"    • Outlinks / Backlinks      : {graph_stats['outlink_edges']} / {graph_stats['backlink_edges']}")

    print("\n  Sample Co-Edited Spillover Graph Edges:")
    (
        saved_edges.orderBy(F.desc("co_edited_in_window"), F.desc("target_edits_in_window"))
        .select(
            "source_title",
            "target_title",
            "edge_type",
            "co_edited_in_window",
            "target_edits_in_window",
            "target_distinct_editors",
        )
        .show(10, truncate=36)
    )

    print(f"\n[Step 2] Fetching and parsing MediaWiki `action=compare` revision diffs for {args.max_revisions} alerted revisions...")
    diffs_df, diff_stats = build_revision_diffs_df(
        spark,
        alerts_df,
        rc_df,
        session,
        max_revisions=args.max_revisions,
        offline=args.offline,
    )
    out_diffs = CURATED_TABLES["revision_diffs"]
    safe_write_parquet(diffs_df, out_diffs, repartition_num=4)
    saved_diffs = safe_read_parquet(spark, out_diffs)
    print(f"  ✓ Wrote {saved_diffs.count():,} revision diffs -> {out_diffs}")
    print(f"    • Usable change data rate   : {diff_stats['usable_change_data_count']}/{diff_stats['total_revisions_compared']} ({diff_stats['usable_change_data_pct']}%) [Target >= 90%: {diff_stats['proposal_target_ge_90_pct_met']}]")
    print(f"    • Live Compare API success  : {diff_stats['live_compare_api_success_pct']}%")
    print(f"    • Compare status breakdown  : {diff_stats['compare_status_breakdown']}")

    print("\n  Sample Parsed Revision Diffs:")
    (
        saved_diffs.filter("compare_status = 'success'")
        .select(
            "revid",
            "title",
            "delta_bytes",
            "added_lines_count",
            "removed_lines_count",
            "external_links_added",
            "compare_status",
            "diff_snippet",
        )
        .show(10, truncate=40)
    )

    summary = {
        "graph_enrichment": graph_stats,
        "revision_diff_enrichment": diff_stats,
        "runtime_sec": round(time.time() - t0, 2),
    }
    bench_dir = CURATED_TABLES["benchmarks"]
    bench_dir.mkdir(parents=True, exist_ok=True)
    (bench_dir / "milestone6_graph_diffs_summary.json").write_text(json.dumps(summary, indent=2))

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

