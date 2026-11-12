"""Milestone 8.1: Turnkey Reproducible Backup Demo & Real Case-Study Walkthrough.

Provides a zero-external-dependency, presentation-ready backup demo runner over the 46-day
WikiPulse Curated Parquet Lake (`historical/data/curated/`):
1. **Pre-Flight Readiness Check (`--verify-only`)**:
   Audits all 11 Curated Parquet tables, all 11 Milestone 4–7 benchmark JSON artifacts, and
   all 5 Dash HTTP routes (`/`, `/investigation`, `/historical`, `/relationships`, `/system`).
2. **Real Case-Study Walkthrough (`--walkthrough-only`)**:
   Extracts and prints a presentation-ready tour of 4 representative real Wikipedia Knowledge-Shift
   events (`CONFIRMED_NEWS`, `BREAKING_NEWS_CANDIDATE`, `EDIT_WAR_CONTROVERSY`, `MAJOR_REWRITE`),
   joining alerts with link-graph spillover edges, revision diffs, subject coordinates, and
   Lift Wing revert-risk calibration. Saves `curated/benchmarks/milestone8_case_studies_demo.json`.
3. **Pre-Warmed Interactive Demo Server (default / `--serve`)**:
   Runs the pre-flight check + case-study tour, pre-warms the Dash server cache for instant
   sub-100ms tab switching during live presentations, and serves `http://127.0.0.1:8080`.

Usage:
    # Run verification + case-study walkthrough and exit (CI / pre-presentation check):
    .venv/bin/python3 Repo/src/demo/backup_demo.py --verify-only

    # Print the 4-case-study terminal walkthrough and exit:
    .venv/bin/python3 Repo/src/demo/backup_demo.py --walkthrough-only

    # Launch the full pre-warmed interactive backup demo server on port 8080:
    .venv/bin/python3 Repo/src/demo/backup_demo.py --port 8080
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql import functions as F
from dashboards.app import create_app
from dashboards.data_service import DashboardDataService
from src.config.settings import (
    CURATED_DIR,
    CURATED_TABLES,
    EVALUATION_END_DATE,
    EVALUATION_START_DATE,
    get_spark_session,
    safe_read_parquet,
)
from src.dashboard.app import run_end_to_end_dashboard_benchmark
from src.dashboard_queries import DashboardPaths, DashboardQueries

EXPECTED_BENCHMARK_FILES = (
    "ingestion_summary.json",
    "milestone4_benchmarks.json",
    "window_features_summary.json",
    "milestone5_scoring_summary.json",
    "milestone5_evaluation.json",
    "milestone6_graph_diffs_summary.json",
    "milestone6_geospatial_summary.json",
    "milestone6_revert_risk_summary.json",
    "milestone6_dashboard_benchmark.json",
    "milestone7_scaling_benchmarks.json",
    "milestone7_load_recovery.json",
)


def extract_case_studies_walkthrough(spark, curated_dir: Path) -> dict[str, Any]:
    """Extract 4 real-world Wikipedia Knowledge-Shift case studies across all 4 alert classes
    (`CONFIRMED_NEWS`, `BREAKING_NEWS_CANDIDATE`, `EDIT_WAR_CONTROVERSY`, `MAJOR_REWRITE`),
    enriched with graph spillover, revision diffs, geospatial coordinates, and revert risk."""
    alerts_df = safe_read_parquet(spark, curated_dir / "alerts")
    edges_df = safe_read_parquet(spark, curated_dir / "graph_edges")
    diffs_df = safe_read_parquet(spark, curated_dir / "revision_diffs")
    map_df = safe_read_parquet(spark, curated_dir / "map_points")
    risk_df = safe_read_parquet(spark, curated_dir / "revert_risk_eval")

    edge_titles = {str(r["source_title"]) for r in edges_df.select("source_title").distinct().collect()}
    diff_titles = {
        str(r["title"])
        for r in diffs_df.filter("usable_change_data = true").select("title").distinct().collect()
    }

    target_types = (
        "CONFIRMED_NEWS",
        "BREAKING_NEWS_CANDIDATE",
        "EDIT_WAR_CONTROVERSY",
        "MAJOR_REWRITE",
    )

    case_studies: list[dict[str, Any]] = []
    used_titles: set[str] = set()
    print("\n" + "=" * 80)
    print(" WIKIPULSE PRESENTATION WALKTHROUGH: 4 REAL WIKIPEDIA KNOWLEDGE-SHIFT CASE STUDIES")
    print("=" * 80)

    for idx, a_type in enumerate(target_types, start=1):
        candidates = (
            alerts_df.filter(
                (F.col("alert_type") == a_type) & (F.col("historical_alert_flag") == F.lit(True))
            )
            .orderBy(
                F.desc("has_location"),
                F.desc("historical_score"),
                F.desc("distinct_human_editors"),
            )
            .limit(60)
            .collect()
        )
        distinct_candidates = [row for row in candidates if str(row["title"]) not in used_titles] or candidates
        if not distinct_candidates:
            continue
        # Prefer a distinct candidate that has both graph spillover edges and revision diffs when available
        r = max(
            distinct_candidates,
            key=lambda row: (
                (str(row["title"]) in edge_titles) and (str(row["title"]) in diff_titles),
                (str(row["title"]) in edge_titles) or (str(row["title"]) in diff_titles),
                bool(row["has_location"]),
                float(row["historical_score"]),
            ),
        )
        title = str(r["title"])
        used_titles.add(title)
        pageid = int(r["pageid"])

        # Related-page spillover edges for this article (or fallback top spillover example)
        art_edges = (
            edges_df.filter(F.col("source_title") == title)
            .orderBy(F.desc("co_edited_in_window"), F.desc("target_edits_in_window"))
            .limit(4)
            .collect()
        )
        spillover_neighbors = [
            {
                "neighbor_title": str(e["target_title"]),
                "edge_type": str(e["edge_type"]),
                "co_edited_in_window": bool(e["co_edited_in_window"]),
                "target_edits_in_window": int(e["target_edits_in_window"] or 0),
            }
            for e in art_edges
        ]

        # Revision diff snippet for this article (or top available diff)
        art_diffs = (
            diffs_df.filter((F.col("title") == title) & (F.col("usable_change_data") == F.lit(True)))
            .orderBy(F.desc(F.abs(F.col("delta_bytes"))))
            .limit(1)
            .collect()
        )
        diff_info = None
        if art_diffs:
            d = art_diffs[0]
            diff_info = {
                "from_revid": int(d["from_revid"]),
                "to_revid": int(d["revid"]),
                "user": str(d["user"]),
                "bytes_delta": int(d["delta_bytes"] or 0),
                "added_snippet": str(d["diff_snippet"] or "")[:160],
                "comment": str(d["comment"] or "")[:120],
            }

        # Geospatial subject mapping for this article
        art_map = map_df.filter(F.col("title") == title).limit(1).collect()
        geo_info = None
        if art_map:
            m = art_map[0]
            geo_info = {
                "has_coordinates": bool(m["has_coordinates"]),
                "lat": float(m["lat"]) if m["lat"] is not None else None,
                "lon": float(m["lon"]) if m["lon"] is not None else None,
                "country": str(m["country_or_region"] or "Unlocated / Conceptual"),
                "location_source": str(m["location_source"]),
                "location_quality": str(m["location_quality"]),
            }

        reasons_list = [str(x) for x in (r["reasons"] or [])]
        contrib_map = {str(k): round(float(v), 2) for k, v in (r["feature_contributions"] or {}).items()}

        case = {
            "case_number": idx,
            "alert_type": a_type,
            "title": title,
            "pageid": pageid,
            "window_start": r["window_start"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "window_duration": str(r["window_duration"]),
            "edits_human": int(r["edits_human"]),
            "distinct_human_editors": int(r["distinct_human_editors"]),
            "gross_bytes": int(r["gross_bytes"]),
            "net_bytes": int(r["net_bytes"]),
            "rewriting_bytes": max(0, int(r["gross_bytes"]) - abs(int(r["net_bytes"]))),
            "revert_ratio": round(float(r["revert_ratio"]), 4),
            "velocity_zscore": round(float(r["velocity_zscore"]), 2),
            "detector_a_fixed_flag": bool(r["fixed_threshold_flag"]),
            "detector_b_historical_score": round(float(r["historical_score"]), 2),
            "detector_c_spark_ml_score": round(float(r["spark_ml_score"]), 2),
            "pageview_corroborated": bool(r["pageview_corroborated"]),
            "feature_contributions": contrib_map,
            "reasons": reasons_list,
            "spillover_neighbors": spillover_neighbors,
            "sample_revision_diff": diff_info,
            "geospatial_resolution": geo_info,
        }
        case_studies.append(case)

        print(f"\n[Case Study #{idx}] {a_type} -> Article: '{title}' ({case['window_duration']} window @ {case['window_start']})")
        print(
            f"  • Activity   : {case['edits_human']} human edits by {case['distinct_human_editors']} distinct editors "
            f"(velocity z={case['velocity_zscore']:+.1f}, gross={case['gross_bytes']:,} B, net={case['net_bytes']:+,} B, "
            f"rewriting={case['rewriting_bytes']:,} B, revert_ratio={case['revert_ratio']*100:.1f}%)"
        )
        print(
            f"  • Detectors  : Detector A (Fixed)={case['detector_a_fixed_flag']} | "
            f"Detector B (Historical)={case['detector_b_historical_score']}/100 | "
            f"Detector C (Spark ML)={case['detector_c_spark_ml_score']}/100"
        )
        print(f"  • Points     : {contrib_map}")
        for r_idx, reason_txt in enumerate(reasons_list, start=1):
            print(f"  • Reason #{r_idx}  : {reason_txt}")
        if geo_info:
            print(
                f"  • Subject Geo: source={geo_info['location_source']} | quality={geo_info['location_quality']} | "
                f"country={geo_info['country']} | coords=({geo_info['lat']}, {geo_info['lon']})"
            )
        if spillover_neighbors:
            neigh_str = ", ".join(
                f"{n['neighbor_title']} ({n['edge_type']}, co_edited={n['co_edited_in_window']})"
                for n in spillover_neighbors[:3]
            )
            print(f"  • Graph Links: {neigh_str}")
        if diff_info:
            print(
                f"  • Sample Diff: rev {diff_info['from_revid']} -> {diff_info['to_revid']} "
                f"({diff_info['bytes_delta']:+,} B by {diff_info['user']}): \"{diff_info['added_snippet'][:90]}...\""
            )

    # Top high-risk reverted revision from Lift Wing Revert Risk monitor
    top_risk_rows = (
        risk_df.filter("is_reverted_actual = true")
        .orderBy(F.desc("ml_revert_risk"), F.asc("time_to_revert_sec"))
        .limit(3)
        .collect()
    )
    revert_highlights = [
        {
            "revid": int(r["revid"]),
            "title": str(r["title"]),
            "user": str(r["user"]),
            "is_anon": bool(r["is_anon"]),
            "delta": int(r["delta"]),
            "ml_revert_risk": round(float(r["ml_revert_risk"]), 4),
            "composite_risk": round(float(r["composite_risk"]), 4),
            "time_to_revert_sec": int(r["time_to_revert_sec"] or 0),
            "reverter_is_bot": bool(r["reverter_is_bot"]),
        }
        for r in top_risk_rows
    ]

    # Verify benchmark JSON inventory
    bench_dir = CURATED_TABLES["benchmarks"]
    present_benchmarks = [f for f in EXPECTED_BENCHMARK_FILES if (bench_dir / f).is_file()]

    demo_summary = {
        "case_studies_count": len(case_studies),
        "benchmark_files_expected": len(EXPECTED_BENCHMARK_FILES),
        "benchmark_files_present": len(present_benchmarks),
        "all_benchmark_files_ready": len(present_benchmarks) == len(EXPECTED_BENCHMARK_FILES),
        "case_studies": case_studies,
        "liftwing_reverted_revision_highlights": revert_highlights,
    }
    bench_dir.mkdir(parents=True, exist_ok=True)
    (bench_dir / "milestone8_case_studies_demo.json").write_text(json.dumps(demo_summary, indent=2))
    return demo_summary


def main() -> int:
    ap = argparse.ArgumentParser(description="Milestone 8.1: Reproducible Backup Demo & Case-Study Walkthrough.")
    ap.add_argument("--port", type=int, default=8080, help="Port for interactive Dash backup demo server.")
    ap.add_argument("--curated", type=Path, default=CURATED_DIR, help="Path to curated Parquet lake.")
    ap.add_argument("--verify-only", action="store_true", help="Run full table/benchmark/route verification and exit.")
    ap.add_argument("--walkthrough-only", action="store_true", help="Print the 4 case-study walkthrough and exit.")
    args = ap.parse_args()

    t0 = time.time()
    spark = get_spark_session(app_name="WikiPulse-Milestone8-BackupDemo")
    try:
        demo_summary = extract_case_studies_walkthrough(spark, args.curated)
        if args.walkthrough_only:
            print(
                f"\n  ✓ Case-study walkthrough complete ({demo_summary['case_studies_count']} case studies, "
                f"{demo_summary['benchmark_files_present']}/{demo_summary['benchmark_files_expected']} benchmark artifacts ready)."
            )
            return 0

        bench_report = run_end_to_end_dashboard_benchmark(spark, args.curated)
        all_ready = (
            bench_report["all_11_tables_valid"]
            and bench_report["all_views_under_2s_target"]
            and demo_summary["all_benchmark_files_ready"]
        )
        print(
            f"\n[Backup Demo Readiness Summary] "
            f"all_11_tables_valid={bench_report['all_11_tables_valid']} | "
            f"max_query_latency={bench_report['max_stored_query_latency_sec']:.4f}s (<2.0s={bench_report['all_views_under_2s_target']}) | "
            f"benchmarks={demo_summary['benchmark_files_present']}/{demo_summary['benchmark_files_expected']} | "
            f"elapsed={time.time() - t0:.2f}s"
        )
        if args.verify_only:
            return 0 if all_ready else 1

        # Pre-warm Dash service cache across all 5 screens before starting HTTP server
        paths = DashboardPaths(
            raw=REPO_ROOT / "data/raw/wikipedia_edits",
            live=REPO_ROOT / "data/live_activity",
            history=args.curated,
            curated=args.curated,
        )
        service = DashboardDataService(DashboardQueries(spark, paths), cache_seconds=120)
        for screen in ("live", "investigation", "historical", "relationships", "system"):
            _ = service.load(screen, "5m", "finalized", None, EVALUATION_START_DATE, EVALUATION_END_DATE)

        app = create_app(service)
        print(f"\nLaunching Pre-Warmed WikiPulse Backup Demo on http://127.0.0.1:{args.port} ...", flush=True)
        app.run(host="127.0.0.1", port=args.port, debug=False, use_reloader=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
