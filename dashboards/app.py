"""Run the proposal-aligned WikiPulse dashboards: python -m dashboards.app."""

import argparse
from pathlib import Path

from dash import Dash
from flask import Response, jsonify, request

from dashboards.callbacks import register_callbacks
from dashboards.data_service import DashboardDataService
from dashboards.layout import shell
from src.config.settings import CURATED_DIR
from src.dashboard_queries import DashboardPaths, DashboardQueries
from src.spark_common import ROOT, create_spark
from src.local_supervisor import request_stream_restart, stream_health


def create_app(service, default_view="live", activity_workspace=None):
    """Build the UI without creating Spark; tests can inject a small fake service."""
    app = Dash(
        __name__, assets_folder=str(Path(__file__).parent / "assets"),
        suppress_callback_exceptions=True, title="WikiPulse · Knowledge intelligence",
        update_title=None,
    )
    app.layout = lambda: shell(default_view=default_view, activity=activity_workspace is not None)
    register_callbacks(app, service, activity_workspace)
    if activity_workspace is not None:
        from dashboards.activity_ui import register_activity_callbacks
        register_activity_callbacks(app, activity_workspace)

        @app.server.get("/api/activity/jobs/<job_id>")
        def activity_job(job_id):
            result = activity_workspace.get(job_id)
            return jsonify(result or {"error": "Request not found"}), 200 if result else 404

        @app.server.get("/api/activity/jobs/<job_id>/log")
        def activity_job_log(job_id):
            result = activity_workspace.get(job_id)
            if result is None:
                return jsonify({"error": "Request not found"}), 404
            import json
            return Response(json.dumps(result, indent=2), mimetype="application/json",
                            headers={"Content-Disposition": f'attachment; filename="activity-{result["id"]}.json"'})

        @app.server.get("/api/activity/console/log")
        def activity_console_log():
            return Response(activity_workspace.history.download(), mimetype="application/x-ndjson",
                            headers={"Content-Disposition": 'attachment; filename="wikipulse-processing-history.jsonl"'})

    @app.server.get("/healthz")
    def health():
        # Readiness must not wait for a heavy Spark aggregation.
        return jsonify({"status": "ready", "frontend": "Dash + Plotly"})

    @app.server.get("/api/stream/health")
    def stream_health_route():
        return jsonify(stream_health())

    @app.server.post("/api/stream/restart")
    def stream_restart_route():
        result = request_stream_restart(request.headers.get("X-WikiPulse-Control-Token"))
        return jsonify(result), 202 if result["ok"] else 409

    @app.server.get("/api/activity")
    def activity():
        duration = request.args.get("duration", "5m")
        view = request.args.get("view", "live")
        if duration not in ("5m", "1h", "6h") or view not in ("live", "finalized"):
            return jsonify({"error": "Invalid duration or view"}), 400
        data = service.load("live", duration, view)
        return jsonify(data), 503 if data.get("error") else 200

    return app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--live", type=Path, default=ROOT / "data/live_activity")
    parser.add_argument("--history", type=Path, default=ROOT / "data/week2/historical")
    parser.add_argument("--curated", type=Path, default=CURATED_DIR)
    args = parser.parse_args()
    spark = create_spark("WikiPulse-Dashboards", with_kafka=False)
    try:
        paths = DashboardPaths(live=args.live, history=args.history, curated=args.curated)
        service = DashboardDataService(DashboardQueries(spark, paths))
        from dashboards.activity_jobs import ActivityWorkspace
        workspace = ActivityWorkspace(paths)
        app = create_app(service, activity_workspace=workspace)
        print(f"WikiPulse dashboards: http://127.0.0.1:{args.port}", flush=True)
        app.run(host="127.0.0.1", port=args.port, debug=False, use_reloader=False)
    finally:
        if "workspace" in locals():
            workspace.close()
        spark.stop()


if __name__ == "__main__":
    main()
