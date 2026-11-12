"""Milestone 6.4: End-to-End Investigation Dashboard & Sub-2s Query Latency Benchmark.

Integrates the Milestone 4, 5, and 6 curated Parquet lake (`historical/data/curated/`) with the
proposal-aligned Dash + Plotly investigation frontend (`Repo/dashboards/`), and provides an
automated verification & query-latency benchmark across all 4 Proposal Dashboard Views:
- **View 1 (Live & Replay Alert Radar)**: `curated/alerts`, `curated/page_windows_5m`, `curated/page_windows_1h`
- **View 2 (Historical Baselines, Churn & Bot Rankings)**: `curated/page_baselines`, `curated/rankings_daily`, `curated/churn_bot_summary`
- **View 3 (Graph, Map & Revert-Risk Investigation)**: `curated/graph_edges`, `curated/revision_diffs`, `curated/map_points`, `curated/revert_risk_eval`
- **View 4 (Engineering & Scale Benchmarks)**: `curated/benchmarks/*.json`

Usage:
    # Run full end-to-end verification & sub-2s query latency benchmark:
    .venv/bin/python3 Repo/src/dashboard/app.py --benchmark-only

    # Launch interactive Dash + Plotly server on port 8080:
    .venv/bin/python3 Repo/src/dashboard/app.py --port 8080
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

from plotly.utils import PlotlyJSONEncoder
from pyspark.sql import functions as F
from dashboards.app import create_app
from dashboards.callbacks import render_dashboard
from dashboards.data_service import DashboardDataService
from src.config.settings import (
    CURATED_DIR,
    CURATED_TABLES,
    EVALUATION_END_DATE,
    EVALUATION_START_DATE,
    get_spark_session,
    safe_read_parquet,
)
from src.dashboard_queries import DashboardPaths, DashboardQueries
from src.models.schemas import (
    ALERT_SCHEMA,
    GRAPH_EDGE_SCHEMA,
    HISTORICAL_BASELINE_SCHEMA,
    MAP_POINT_SCHEMA,
    PAGE_WINDOW_SCHEMA,
    RAW_EDIT_SCHEMA,
    REVERT_RISK_SCHEMA,
    REVISION_DIFF_SCHEMA,
)

EXPECTED_TABLE_SCHEMAS = {
    "rc_events": RAW_EDIT_SCHEMA,
    "page_baselines": HISTORICAL_BASELINE_SCHEMA,
    "page_windows_5m": PAGE_WINDOW_SCHEMA,
    "page_windows_1h": PAGE_WINDOW_SCHEMA,
    "alerts": ALERT_SCHEMA,
    "graph_edges": GRAPH_EDGE_SCHEMA,
    "revision_diffs": REVISION_DIFF_SCHEMA,
    "map_points": MAP_POINT_SCHEMA,
    "revert_risk_eval": REVERT_RISK_SCHEMA,
}


def run_end_to_end_dashboard_benchmark(spark, curated_dir: Path) -> dict[str, Any]:
    """Audit all 11 curated tables, measure stored-result query latencies for all 4 Proposal
    Dashboard Views (<2.0s target), and verify all 5 Dash HTTP routes."""
    print("\n[Step 1] Auditing all 11 Curated Parquet Tables across Milestones 4, 5 & 6...")
    table_audit: dict[str, dict[str, Any]] = {}
    all_tables_order = [
        "rc_events",
        "page_baselines",
        "churn_bot_summary",
        "rankings_daily",
        "page_windows_5m",
        "page_windows_1h",
        "alerts",
        "graph_edges",
        "revision_diffs",
        "map_points",
        "revert_risk_eval",
    ]

    for name in all_tables_order:
        t_path = CURATED_TABLES[name]
        df = safe_read_parquet(spark, t_path)
        row_count = int(df.count())
        expected_schema = EXPECTED_TABLE_SCHEMAS.get(name)
        if expected_schema is not None:
            expected_cols = {f.name for f in expected_schema.fields}
            actual_cols = set(df.columns)
            schema_ok = expected_cols.issubset(actual_cols)
        else:
            schema_ok = len(df.columns) > 0
        table_audit[name] = {
            "rows": row_count,
            "columns": len(df.columns),
            "schema_valid": bool(schema_ok and row_count > 0),
        }
        print(f"  • {name:<20}: {row_count:>10,} rows | {len(df.columns):>2} cols | schema_valid={table_audit[name]['schema_valid']}")

    # Open stored Parquet DataFrame handles before measuring query execution latency
    print("\n[Step 2] Measuring Stored-Parquet Query Latency Across All 4 Proposal Dashboard Views (<2.0s Target)...")
    alerts_df = safe_read_parquet(spark, CURATED_TABLES["alerts"])
    rankings_df = safe_read_parquet(spark, CURATED_TABLES["rankings_daily"])
    churn_df = safe_read_parquet(spark, CURATED_TABLES["churn_bot_summary"])
    baselines_df = safe_read_parquet(spark, CURATED_TABLES["page_baselines"])
    edges_df = safe_read_parquet(spark, CURATED_TABLES["graph_edges"])
    diffs_df = safe_read_parquet(spark, CURATED_TABLES["revision_diffs"])
    map_df = safe_read_parquet(spark, CURATED_TABLES["map_points"])
    risk_df = safe_read_parquet(spark, CURATED_TABLES["revert_risk_eval"])

    latencies: dict[str, float] = {}

    # View 1: Live & Replay Alert Radar (Top 50 alerts + detector breakdown)
    t_v1 = time.perf_counter()
    v1_rows = (
        alerts_df.filter("historical_alert_flag = true")
        .select(
            "alert_id",
            "window_start",
            "window_duration",
            "title",
            "alert_type",
            "historical_score",
            "spark_ml_score",
            "edits_human",
            "distinct_human_editors",
            "revert_ratio",
            "reasons",
        )
        .orderBy(F.desc("historical_score"), F.desc("window_start"))
        .limit(50)
        .collect()
    )
    latencies["view1_alert_radar_query_sec"] = round(time.perf_counter() - t_v1, 4)
    top_article = str(v1_rows[0]["title"]) if v1_rows else "Earth"

    # View 2: Historical Baselines, Churn & Bot Rankings
    t_v2 = time.perf_counter()
    _ = rankings_df.filter("baseline_surge_rank <= 20").orderBy(F.desc("day"), "baseline_surge_rank").limit(40).collect()
    _ = churn_df.filter("ns = 0").orderBy("day").collect()
    _ = baselines_df.filter(F.col("title") == top_article).limit(5).collect()
    latencies["view2_historical_rankings_query_sec"] = round(time.perf_counter() - t_v2, 4)

    # View 3: Contextual Investigation (Graph Edges, Revision Diffs, Global Subject Map, Revert Risk)
    t_v3 = time.perf_counter()
    _ = edges_df.orderBy(F.desc("co_edited_in_window"), F.desc("target_edits_in_window")).limit(100).collect()
    _ = diffs_df.filter("usable_change_data = true").limit(50).collect()
    _ = map_df.filter("has_coordinates = true").limit(100).collect()
    _ = risk_df.orderBy(F.desc("composite_risk")).limit(50).collect()
    latencies["view3_graph_map_risk_investigation_sec"] = round(time.perf_counter() - t_v3, 4)

    # View 4: Engineering & Scale Benchmarks
    t_v4 = time.perf_counter()
    bench_files = sorted((CURATED_TABLES["benchmarks"]).glob("*.json"))
    bench_loaded = [json.loads(p.read_text(encoding="utf-8")) for p in bench_files]
    latencies["view4_engineering_benchmarks_sec"] = round(time.perf_counter() - t_v4, 4)

    max_query_sec = max(latencies.values())
    under_2s_all = all(sec < 2.0 for sec in latencies.values())

    for k, v in latencies.items():
        print(f"  • {k:<42}: {v:.4f}s (< 2.0s: {v < 2.0})")

    # Step 3: Verify Dash + Plotly UI rendering across all 5 routes
    print("\n[Step 3] Verifying Dash + Plotly Frontend Rendering Across All 5 Application Routes...")
    paths = DashboardPaths(
        raw=REPO_ROOT / "data/raw/wikipedia_edits",
        live=REPO_ROOT / "data/live_activity",
        history=curated_dir,
        curated=curated_dir,
    )
    service = DashboardDataService(DashboardQueries(spark, paths))
    app = create_app(service)
    client = app.server.test_client()

    route_checks: dict[str, dict[str, Any]] = {}
    for route in ("/", "/investigation", "/historical", "/relationships", "/system"):
        t_r = time.perf_counter()
        http_resp = client.get(route)
        search = f"?title={top_article}" if route == "/investigation" else ""
        rendered = render_dashboard(
            service, route, search, "5m", "finalized", None, EVALUATION_START_DATE, EVALUATION_END_DATE
        )
        serialized = json.dumps(rendered, cls=PlotlyJSONEncoder)
        elapsed_r = round(time.perf_counter() - t_r, 3)
        route_ok = (http_resp.status_code == 200) and ("children" in serialized) and ("NaN" not in serialized)
        route_checks[route] = {
            "http_status": http_resp.status_code,
            "rendered_payload_bytes": len(serialized),
            "render_sec": elapsed_r,
            "passed": route_ok,
        }
        http_resp.close()
        print(f"  • Route {route:<16}: HTTP {route_checks[route]['http_status']} | payload={len(serialized):>8,} B | render={elapsed_r:.3f}s | passed={route_ok}")

    report = {
        "curated_tables_verified": len(table_audit),
        "all_11_tables_valid": all(v["schema_valid"] for v in table_audit.values()),
        "max_stored_query_latency_sec": max_query_sec,
        "all_views_under_2s_target": under_2s_all,
        "view_query_latencies_sec": latencies,
        "dash_route_verification": route_checks,
        "table_inventory": table_audit,
        "benchmark_files_loaded": len(bench_loaded),
    }

    bench_dir = CURATED_TABLES["benchmarks"]
    bench_dir.mkdir(parents=True, exist_ok=True)
    (bench_dir / "milestone6_dashboard_benchmark.json").write_text(json.dumps(report, indent=2))
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description="Milestone 6.4: Unified WikiPulse Dashboard & Benchmark Verifier.")
    ap.add_argument("--port", type=int, default=8080, help="Port for interactive Dash server.")
    ap.add_argument("--curated", type=Path, default=CURATED_DIR, help="Path to curated Parquet lake.")
    ap.add_argument("--benchmark-only", action="store_true", help="Run end-to-end verification and exit.")
    args = ap.parse_args()

    print("=" * 78)
    print(" WIKIPULSE MILESTONE 6.4: END-TO-END INVESTIGATION DASHBOARD & BENCHMARK")
    print("=" * 78)

    spark = get_spark_session(app_name="WikiPulse-Milestone6-Dashboard")
    try:
        report = run_end_to_end_dashboard_benchmark(spark, args.curated)
        if args.benchmark_only:
            print(
                f"\n  ✓ Milestone 6.4 verification complete: all_11_tables_valid={report['all_11_tables_valid']}, "
                f"max_query_latency={report['max_stored_query_latency_sec']:.4f}s (<2.0s={report['all_views_under_2s_target']})"
            )
            return 0 if (report["all_11_tables_valid"] and report["all_views_under_2s_target"]) else 1

        paths = DashboardPaths(
            raw=REPO_ROOT / "data/raw/wikipedia_edits",
            live=REPO_ROOT / "data/live_activity",
            history=args.curated,
            curated=args.curated,
        )
        service = DashboardDataService(DashboardQueries(spark, paths))
        app = create_app(service)
        print(f"\nLaunching WikiPulse Dashboards on http://127.0.0.1:{args.port} ...", flush=True)
        app.run(host="127.0.0.1", port=args.port, debug=False, use_reloader=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

