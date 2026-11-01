"""Run the proposal-aligned WikiPulse dashboards: python -m dashboards.app."""

import argparse
from pathlib import Path

from dash import Dash
from flask import jsonify

from dashboards.callbacks import register_callbacks
from dashboards.data_service import DashboardDataService
from dashboards.layout import shell
from src.dashboard_queries import DashboardPaths, DashboardQueries
from src.spark_common import ROOT, create_spark


def create_app(service):
    """Build the UI without creating Spark; tests can inject a small fake service."""
    app = Dash(
        __name__, assets_folder=str(Path(__file__).parent / "assets"),
        suppress_callback_exceptions=True, title="WikiPulse · Knowledge intelligence",
        update_title="Refreshing observations…",
    )
    app.layout = shell()
    register_callbacks(app, service)

    @app.server.get("/healthz")
    def health():
        # Readiness must not wait for a heavy Spark aggregation.
        return jsonify({"status": "ready", "frontend": "Dash + Plotly"})

    @app.server.get("/api/activity")
    def activity():
        from flask import request
        duration = request.args.get("duration", "5m")
        view = request.args.get("view", "live")
        if duration not in ("5m", "1h") or view not in ("live", "finalized"):
            return jsonify({"error": "Invalid duration or view"}), 400
        data = service.load("live", duration, view)
        return jsonify(data), 503 if data.get("error") else 200

    return app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--live", type=Path, default=ROOT / "data/live_activity")
    parser.add_argument("--history", type=Path, default=ROOT / "data/week2/historical")
    parser.add_argument("--curated", type=Path, default=ROOT / "data/curated")
    args = parser.parse_args()
    spark = create_spark("WikiPulse-Dashboards", with_kafka=False)
    try:
        paths = DashboardPaths(live=args.live, history=args.history, curated=args.curated)
        service = DashboardDataService(DashboardQueries(spark, paths))
        app = create_app(service)
        print(f"WikiPulse dashboards: http://127.0.0.1:{args.port}", flush=True)
        app.run(host="127.0.0.1", port=args.port, debug=False, use_reloader=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
