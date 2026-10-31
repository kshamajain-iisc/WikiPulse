"""Serve a local activity dashboard backed by Spark page measurements.

Run with ``python -m src.activity_dashboard`` and open http://127.0.0.1:8080.
Spark performs all table processing; only the bounded display result reaches Python.
"""

import argparse
import json
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from pyspark.sql import SparkSession, Window, functions as F

from src.page_features import compare_history, live_edits, page_windows
from src.spark_common import ROOT, RAW_PATH, create_spark

TEMPLATE_PATH = Path(__file__).parent / "templates" / "activity.html"
DISPLAY_ROW_LIMIT = 100


def read_activity(
    spark: SparkSession,
    live_directory: Path,
    history_directory: Path,
    duration: str,
    view: str = "live",
    raw_directory: Path | None = None,
) -> dict:
    """Return recent provisional or finalized page windows, limited for display."""
    if view == "live":
        raw_path = raw_directory if raw_directory is not None else Path(RAW_PATH)
        if not any(raw_path.rglob("*.parquet")):
            return {"status": "Waiting for raw events from the collector.", "rows": [], "view": view}
        # Preview aggregates are recomputed in Spark from retained raw events.
        # They are provisional and do not alter finalized streaming results.
        bronze = spark.read.parquet(str(raw_path))
        recent = bronze.filter(F.col("ingestion_date") >= F.date_sub(F.current_date(), 1))
        edits = live_edits(recent).dropDuplicates(["event_id"])
        # Collect epoch seconds and a Spark-formatted UTC string. Collecting a
        # Python datetime would otherwise format it in the host's local timezone.
        latest_event = edits.agg(
            F.max("ts").cast("long").alias("epoch_seconds"),
            F.date_format(F.max("ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("utc_time"),
        ).first()
        if latest_event.epoch_seconds is None:
            return {
                "status": "Raw events received, but none pass edit validation.",
                "rows": [],
                "view": view,
            }
        lookback_minutes = 10 if duration == "5m" else 120
        recent_edits = edits.filter(
            F.col("ts").cast("long") >= latest_event.epoch_seconds - lookback_minutes * 60
        )
        windows = page_windows(recent_edits, "5 minutes" if duration == "5m" else "1 hour")
        status = (
            "Live preview · provisional counts may change as events arrive · "
            f"latest event: {latest_event.utc_time}"
        )
    else:
        window_path = live_directory / f"windows_{duration}"
        if not any(window_path.rglob("*.parquet")):
            return {
                "status": (
                    "Waiting for finalized windows (window end + event-time delay). "
                    "Use Live preview to see incoming edits."
                ),
                "rows": [],
                "view": view,
            }
        windows = spark.read.parquet(str(window_path))
        status = "Finalized activity · includes the configured event-time delay"

    baseline_path = history_directory / f"baselines_{duration}"
    if any(baseline_path.rglob("*.parquet")):
        baselines = spark.read.parquet(str(baseline_path))
        windows = compare_history(windows, baselines)
    else:
        # A new installation can display live counts before history is prepared.
        windows = windows.withColumn("human_edit_ratio", F.lit(None).cast("double"))

    latest_per_page = Window.partitionBy("pageid").orderBy(F.desc("window_end"))
    ranked_windows = windows.withColumn(
        "row_number", F.row_number().over(latest_per_page)
    )
    display_rows = (
        ranked_windows.filter(F.col("row_number") == 1)
        .orderBy(F.desc("window_end"), F.desc("edits_human"))
        .limit(DISPLAY_ROW_LIMIT)
        .drop("row_number")
    )
    # Serialize timestamps with Spark's JSON encoder. Never collect the full lake.
    return {
        "status": status,
        "view": view,
        "rows": [json.loads(row) for row in display_rows.toJSON().collect()],
    }


def create_request_handler(
    spark: SparkSession,
    live_directory: Path,
    history_directory: Path,
    duration: str,
) -> type[BaseHTTPRequestHandler]:
    """Bind the HTTP routes to one Spark session and its configured table paths."""
    class ActivityRequestHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            """Serve the static page or the current Spark query result."""
            requested_url = urlsplit(self.path)
            requested_duration = parse_qs(requested_url.query).get("duration", [duration])[0]
            requested_view = parse_qs(requested_url.query).get("view", ["live"])[0]
            if requested_view not in ("live", "finalized"):
                self.send_payload(b"View must be live or finalized", "text/plain", status=400)
                return
            if requested_duration not in ("5m", "1h"):
                self.send_payload(b"Duration must be 5m or 1h", "text/plain", status=400)
                return
            if requested_url.path == "/":
                # Only validated duration labels enter the static template.
                page = (
                    TEMPLATE_PATH.read_text()
                    .replace("{{DURATION}}", requested_duration)
                    .replace("{{VIEW}}", requested_view)
                )
                self.send_payload(page.encode(), "text/html; charset=utf-8")
            elif requested_url.path == "/api/activity":
                self.send_activity(requested_duration, requested_view)
            else:
                self.send_payload(b"Not found", "text/plain", status=404)

        def send_activity(self, requested_duration: str, requested_view: str) -> None:
            """Log query failures on the server and return a readable client status."""
            try:
                activity = read_activity(
                    spark, live_directory, history_directory,
                    requested_duration, requested_view,
                )
                status = 200
            except Exception:
                logging.exception("Spark activity query failed")
                activity = {
                    "status": "Activity query failed; check server logs",
                    "rows": [],
                }
                status = 503
            self.send_payload(json.dumps(activity).encode(), "application/json", status)

        def send_payload(
            self, payload: bytes, content_type: str, status: int = 200
        ) -> None:
            """Write a complete HTTP response with an explicit content length."""
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return ActivityRequestHandler


def main() -> None:
    """Start the loopback-only viewer and release Spark when it exits."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", default=str(ROOT / "data/live_activity"))
    parser.add_argument("--history", default=str(ROOT / "data/week2/historical"))
    parser.add_argument("--duration", choices=["5m", "1h"], default="5m")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    spark = create_spark("WikiPulse-Activity-Dashboard", with_kafka=False)
    server = None
    try:
        handler = create_request_handler(
            spark, Path(args.live), Path(args.history), args.duration
        )
        server = HTTPServer(("127.0.0.1", args.port), handler)
        print(f"Activity dashboard: http://127.0.0.1:{args.port}", flush=True)
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if server is not None:
            server.server_close()
        spark.stop()


if __name__ == "__main__":
    main()
