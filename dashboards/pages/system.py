"""Observed pipeline measurements and engineering experiment evidence."""

from datetime import datetime, timezone

from dash import html

from dashboards import charts
from dashboards.components import graph, heading, metric, number, notice, panel, pill_row, table


def render(data):
    quality = data.get("quality", {})
    progress = data.get("progress", [])
    m4 = data.get("m4_benchmarks", {})
    m5 = data.get("m5_summary", {})
    m5_eval = data.get("m5_evaluation", {})
    m6_risk = data.get("m6_revert_risk", {})
    m6_graph = data.get("m6_graph_diffs", {})
    m6_geo = data.get("m6_geospatial", {})

    m4_pills = []
    if m4:
        audit = m4.get("manual_churn_audit", {})
        rdd = m4.get("rdd_vs_dataframe_benchmark", {})
        lat = m4.get("query_latency_benchmark", {})
        m4_pills = [
            ("Manual audit match", "Exact" if audit.get("spark_exact_match") else "—"),
            ("DF vs RDD speedup", f"{rdd.get('speedup_factor_df_over_rdd', '—')}×"),
            ("Parquet < 2s SLA", "Pass" if lat.get("meets_under_2s_target") else "—"),
        ]

    m5_m6_pills = []
    if m5_eval:
        synth = m5_eval.get("experiment_2_synthetic_pattern_injection", {})
        if synth:
            m5_m6_pills.append(("Synthetic recall (B)", synth.get("detector_b_positive_recall", "—")))
            m5_m6_pills.append(("Negative false alerts", synth.get("detector_b_negative_control_false_alerts", "—")))
    if m6_graph:
        diff_enr = m6_graph.get("revision_diff_enrichment", {})
        m5_m6_pills.append(("Diff usable rate", f"{diff_enr.get('usable_change_data_pct', '—')}%"))
    if m6_geo:
        m5_m6_pills.append(("Geo coverage", f"{m6_geo.get('located_coverage_pct', '—')}%"))
    if m6_risk:
        cov = m6_risk.get("coverage_metrics", {})
        m5_m6_pills.append(("Revert risk coverage", f"{cov.get('valid_probability_or_fallback_pct', '—')}%"))

    curve_rows = m6_risk.get("experiment_5_threshold_calibration", {}).get("liftwing_ml_curve", [])

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
            metric("RAW RECORDS", number(quality.get("raw_records")), quality.get("scope", "Stored ingestion snapshot"), True),
            metric("ACCEPTED EDITS", number(quality.get("valid_records")), quality.get("valid_note", "Retained after category filtering")),
            metric("FILTERED EVENTS", number(quality.get("invalid_records")), quality.get("filtered_note", "May include non-edit categories excluded from curated data")),
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
        html.Div([
            panel("Milestone 4: Engine & query latency", "RDD vs DataFrame runtime & JSONL.gz vs Parquet (<2s SLA)",
                  html.Div([
                      *([pill_row(m4_pills)] if m4_pills else []),
                      graph(charts.m4_engine_and_latency(m4)),
                  ])),
            panel("Milestone 6: Lift Wing threshold calibration", "Precision, Recall & F1 across ML revert-risk thresholds",
                  html.Div([
                      *([pill_row(m5_m6_pills)] if m5_m6_pills else []),
                      graph(charts.revert_risk_calibration(curve_rows)),
                  ])),
        ], className="chart-grid"),
        html.Div([
            panel("Strong scaling & parallel speedup", "Fixed 14-day window + baseline join + scoring across worker cores",
                  graph(charts.scaling_curve(data.get("strong_scaling", [])))),
            panel("Streaming load vs. processing capacity", "Measured throughput across 1x, 10x, 50x, and 100x Wikipedia replay loads",
                  graph(charts.recovery_and_load(data.get("load_tiers", [])))),
        ], className="chart-grid"),
        panel("Controlled stop/restart & backlog recovery", "Spark checkpoint recovery draining accumulated outage backlog with zero duplicate alerts",
              table(data.get("recovery_summary", []), [
                  ("Stage", "phase"),
                  ("Events", "events"),
                  ("Remaining backlog", "backlog_after"),
                  ("Distinct alerts", "alerts"),
                  ("Duplicate alerts", "duplicates"),
              ]) if data.get("recovery_summary") else notice("No recovery run recorded yet", "Run Repo/src/benchmarks/load_and_recovery_benchmark.py to populate checkpoint recovery results.")),
        panel("Experiment evidence", "Stored numeric benchmark results; units are retained in measurement names",
              table(data.get("benchmarks", []), [("Experiment", "experiment"), ("Measurement", "measurement"), ("Value", "value")])
              if data.get("benchmarks") else notice("No benchmark results recorded", "RDD/DataFrame, scaling, shuffle, replay, and recovery measurements appear when their experiment reports are produced.")),
        notice("Latest raw ingestion", quality.get("last_ingested", "No successful ingestion recorded yet")),
    ]
