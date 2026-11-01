"""Observed pipeline measurements and engineering experiment evidence."""

from datetime import datetime, timezone

from dash import html

from dashboards import charts
from dashboards.components import graph, heading, metric, number, notice, panel, table


def render(data):
    quality = data.get("quality", {})
    progress = data.get("progress", [])
    progress_rows = []
    for row in progress:
        timestamp = row.get("timestamp")
        age = None
        if timestamp:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(timestamp.replace("Z", "+00:00"))).total_seconds()
        progress_rows.append({
            "stage": row["stage"], "timestamp": timestamp,
            "input_rate": row.get("inputRowsPerSecond"),
            "processing_rate": row.get("processedRowsPerSecond"),
            "duration": row.get("durationMs", {}).get("triggerExecution"),
            "state": "Stale (> 2 min)" if age is not None and age > 120 else "Recent progress",
        })
    return [
        heading("OPERATE / ENGINEERING & HEALTH", "A clear view of the pipeline.",
                "Track observed ingestion quality, streaming progress, storage, and reproducible experiment evidence."),
        html.Div([
            metric("RAW RECORDS", number(quality.get("raw_records")), "Current / prior UTC ingestion day", True),
            metric("ACCEPTED EDITS", number(quality.get("valid_records")), "Revalidated from original payloads"),
            metric("REJECTED RECORDS", number(quality.get("invalid_records")), "Includes malformed test messages"),
            metric("KAFKA → BRONZE LAG", number(data.get("lag")), "Records beyond last committed raw batch"),
        ], className="metric-grid"),
        panel("Streaming progress", "Actual Spark progress; unavailable until the updated jobs emit a batch",
              table(progress_rows, [("Stage", "stage"), ("State", "state"), ("Input rows/s", "input_rate"),
                    ("Processed rows/s", "processing_rate"), ("Batch ms", "duration"), ("Batch timestamp", "timestamp")])),
        html.Div([
            panel("Lake footprint", "Measured Parquet storage, in decimal MB",
                  graph(charts.storage(data.get("storage", [])))),
            panel("Storage inventory", "Actual files in the configured data layers",
                  table(data.get("storage", []), [("Layer", "layer"), ("MB", "megabytes"), ("Files", "files")])),
        ], className="chart-grid"),
        panel("Experiment evidence", "Stored numeric benchmark results; units are retained in measurement names",
              table(data.get("benchmarks", []), [("Experiment", "experiment"), ("Measurement", "measurement"), ("Value", "value")])
              if data.get("benchmarks") else notice("No benchmark results recorded", "RDD/DataFrame, scaling, shuffle, replay, and recovery measurements appear when their experiment reports are produced.")),
        notice("Latest raw ingestion", quality.get("last_ingested", "No successful ingestion recorded yet")),
    ]
