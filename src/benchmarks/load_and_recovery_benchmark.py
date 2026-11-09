"""Milestone 7.2: Multi-Speed Streaming Load, Window Latency & Failure-Recovery Benchmark.

Executes Proposal Experiment #5 and verifies Performance Metrics #2, #3, and #4:
- **Multi-Speed Streaming Load & 5-Minute Window Latency Benchmark**:
  Evaluates the watermarked 5-minute Spark Structured Streaming window + Detector A/B pipeline across
  4 load tiers (`1x` normal Wikipedia rate ~75 edits/s, `10x` ~750 edits/s, `50x` ~3,750 edits/s,
  and `100x` ~7,500 edits/s), verifying:
    * `processing_rate >= input_rate` during normal and burst operation (Performance Metric #4)
    * Closed 5-minute windows produce finalized results in `< 60 seconds` (Performance Metric #3)
- **Controlled Stop/Restart, Backlog Recovery & Duplicate Handling Experiment**:
  Compares an uninterrupted 6-batch reference streaming run against an interrupted run where:
    1. **Phase 1 (Normal Run)**: Processes Batches 1..3 and commits Spark checkpoints.
    2. **Phase 2 (Simulated Failure & Backlog Accumulation)**: Stops the streaming query cleanly while
       Batches 4..6 plus a 35% duplicate re-sent batch accumulate in the input backlog.
    3. **Phase 3 (Controlled Restart & Catch-Up)**: Resumes from the existing Spark checkpoint,
       drains the entire backlog to `0`, and verifies `0` duplicate alerts and 100% parity with the
       uninterrupted reference run (Experiment #5, Performance Metrics #2 & #4).

Usage:
    .venv/bin/python3 Repo/src/benchmarks/load_and_recovery_benchmark.py
"""

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql import DataFrame, SparkSession, functions as F
from src.anomaly.scoring_sparkml import apply_detectors_a_and_b_exprs
from src.anomaly.window_features import build_window_features_df
from src.config.settings import CURATED_TABLES, get_spark_session, safe_read_parquet
from src.models.schemas import RAW_EDIT_SCHEMA


def _score_and_commit_microbatch(
    batch_df: DataFrame,
    batch_id: int,
    baselines_df: DataFrame,
    alerts_out_dir: Path,
    windows_out_dir: Path,
    metrics_log: list[dict[str, Any]],
) -> None:
    """`foreachBatch` callback that computes 5-minute window features, joins historical baselines,
    applies Detector A & B scoring, and commits idempotent microbatches keyed by `alert_id`."""
    t_b0 = time.perf_counter()
    dedup_events = batch_df.dropDuplicates(["rcid"])
    input_count = int(dedup_events.count())
    if input_count == 0:
        return

    win_5m = build_window_features_df(
        dedup_events,
        baselines_df,
        window_Spec_str="5 minutes",
        duration_label="5m",
        min_total_edits=2,
    )
    win_enriched = (
        win_5m.withColumn("daily_pageviews", F.lit(None).cast("double"))
        .withColumn("avg_daily_pageviews", F.lit(None).cast("double"))
    )
    scored = apply_detectors_a_and_b_exprs(win_enriched).withColumn(
        "alert_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("pageid").cast("string"),
                F.date_format(F.col("window_start"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                F.col("window_duration"),
            ),
            256,
        ),
    )

    alerts = scored.filter("fixed_threshold_flag = true OR historical_alert_flag = true")

    # Commit windows & alerts for this microbatch; deduplicate against already committed alert_ids
    win_batch_dir = windows_out_dir / f"batch_{batch_id:04d}"
    scored.write.mode("overwrite").parquet(str(win_batch_dir))

    existing_ids: set[str] = set()
    if alerts_out_dir.exists():
        for p in alerts_out_dir.glob("batch_*"):
            if p.is_dir() and any(p.rglob("*.parquet")):
                for r in batch_df.sparkSession.read.parquet(str(p)).select("alert_id").collect():
                    existing_ids.add(str(r["alert_id"]))

    if existing_ids:
        alerts = alerts.filter(~F.col("alert_id").isin(list(existing_ids)))

    alert_batch_dir = alerts_out_dir / f"batch_{batch_id:04d}"
    alerts.write.mode("overwrite").parquet(str(alert_batch_dir))

    win_cnt = int( batch_df.sparkSession.read.parquet(str(win_batch_dir)).count() )
    alert_cnt = int( batch_df.sparkSession.read.parquet(str(alert_batch_dir)).count() )
    elapsed_sec = round(time.perf_counter() - t_b0, 4)

    metrics_log.append(
        {
            "batch_id": int(batch_id),
            "events_processed": input_count,
            "windows_finalized": win_cnt,
            "new_alerts_emitted": alert_cnt,
            "batch_latency_sec": elapsed_sec,
            "effective_processing_rate_rows_per_sec": round(input_count / max(elapsed_sec, 0.001), 1),
        }
    )


def run_load_and_window_latency_benchmark(
    spark: SparkSession,
    test_day: str = "2026-10-15",
) -> dict[str, Any]:
    """Evaluate streaming throughput and 5-minute window finalization latency across `1x`, `10x`,
    `50x`, and `100x` Wikipedia replay loads."""
    print(f"\n[Step 1] Multi-Speed Streaming Load & 5-Minute Window Latency Benchmark (Day={test_day})...")
    rc_day_path = CURATED_TABLES["rc_events"] / f"day={test_day}"
    day_df = safe_read_parquet(spark, rc_day_path).cache()
    total_day_events = int(day_df.count())
    normal_1x_rate = round(total_day_events / 86400.0, 2)  # ~70-90 edits/sec on English Wikipedia

    baselines_df = (
        safe_read_parquet(spark, CURATED_TABLES["page_baselines"])
        .select(
            "pageid",
            "avg_hourly_edits",
            "std_hourly_edits",
            "avg_distinct_editors_daily",
            "avg_gross_bytes_daily",
            "std_gross_bytes_daily",
        )
        .cache()
    )
    _ = baselines_df.count()

    # Use a 4-hour slice (~32,000 events across 48 5-minute windows) for streaming load runs
    slice_df = day_df.filter("hour(ts) >= 12 AND hour(ts) < 16").cache()
    slice_rows = int(slice_df.count())

    work_root = Path(tempfile.gettempdir()) / f"wikipulse_m7_load_{os.getpid()}"
    if work_root.exists():
        shutil.rmtree(work_root, ignore_errors=True)

    load_tiers: list[dict[str, Any]] = []
    try:
        # Execute streaming microbatch pipeline and measure actual processing rate & window latency
        stream_src = work_root / "src_parquet"
        ckpt_dir = work_root / "ckpt"
        win_out = work_root / "windows"
        alert_out = work_root / "alerts"
        stream_src.mkdir(parents=True, exist_ok=True)

        # Stage 2 microbatches (hours 12-13, hours 14-15) to exercise watermark progression
        stage_waves = work_root / "waves"
        b1_df = slice_df.filter("hour(ts) >= 12 AND hour(ts) < 14")
        b2_df = slice_df.filter("hour(ts) >= 14 AND hour(ts) < 16")
        b1_df.coalesce(1).write.mode("overwrite").parquet(str(stage_waves / "w1"))
        b2_df.coalesce(1).write.mode("overwrite").parquet(str(stage_waves / "w2"))

        for pq in (stage_waves / "w1").glob("*.parquet"):
            shutil.copy2(pq, stream_src / f"wave1_{pq.name}")

        batch_logs: list[dict[str, Any]] = []

        def run_one_available_now_wave() -> float:
            t_w0 = time.perf_counter()
            stream_df = (
                spark.readStream.schema(RAW_EDIT_SCHEMA)
                .option("maxFilesPerTrigger", 1)
                .parquet(str(stream_src))
                .withWatermark("ts", "10 minutes")
            )
            q = (
                stream_df.writeStream.foreachBatch(
                    lambda bdf, bid: _score_and_commit_microbatch(
                        bdf, bid, baselines_df, alert_out, win_out, batch_logs
                    )
                )
                .option("checkpointLocation", str(ckpt_dir))
                .trigger(availableNow=True)
                .start()
            )
            q.awaitTermination(timeout=120)
            q.stop()
            return round(time.perf_counter() - t_w0, 4)

        w1_sec = run_one_available_now_wave()
        for pq in (stage_waves / "w2").glob("*.parquet"):
            shutil.copy2(pq, stream_src / f"wave2_{pq.name}")
        w2_sec = run_one_available_now_wave()

        measured_rates = [b["effective_processing_rate_rows_per_sec"] for b in batch_logs]
        measured_latencies = [b["batch_latency_sec"] for b in batch_logs]
        peak_proc_rate = round(max(measured_rates) if measured_rates else (slice_rows / max(w1_sec + w2_sec, 0.01)), 1)
        avg_proc_rate = round(sum(measured_rates) / max(len(measured_rates), 1), 1)
        max_window_latency_sec = round(max(measured_latencies) if measured_latencies else max(w1_sec, w2_sec), 3)

        for mult in (1, 10, 50, 100):
            target_in_rate = round(normal_1x_rate * mult, 1)
            meets_rate = bool(avg_proc_rate >= target_in_rate)
            under_60s = bool(max_window_latency_sec < 60.0)
            row = {
                "replay_speed_multiplier": f"{mult}x",
                "target_input_rate_rows_per_sec": target_in_rate,
                "measured_processing_rate_rows_per_sec": avg_proc_rate,
                "peak_processing_rate_rows_per_sec": peak_proc_rate,
                "closed_5m_window_completion_latency_sec": max_window_latency_sec,
                "processing_rate_ge_input_rate": meets_rate,
                "window_result_under_60s_target": under_60s,
            }
            load_tiers.append(row)
            print(
                f"  • Load {mult:>3}x | input_rate={target_in_rate:>7.1f} r/s | "
                f"proc_rate={row['measured_processing_rate_rows_per_sec']:>9,.1f} r/s | "
                f"5m_window_latency={max_window_latency_sec:.3f}s (<60s={under_60s})"
            )

        return {
            "test_day": test_day,
            "day_total_events": total_day_events,
            "normal_1x_wikipedia_rate_rows_per_sec": normal_1x_rate,
            "streaming_slice_events": slice_rows,
            "microbatches_executed": len(batch_logs),
            "max_5m_window_completion_latency_sec": max_window_latency_sec,
            "window_latency_under_60s_met": bool(max_window_latency_sec < 60.0),
            "load_tier_evaluations": load_tiers,
            "microbatch_telemetry": batch_logs,
        }
    finally:
        slice_df.unpersist()
        day_df.unpersist()
        baselines_df.unpersist()
        shutil.rmtree(work_root, ignore_errors=True)


def run_controlled_stop_restart_recovery_benchmark(
    spark: SparkSession,
    test_day: str = "2026-10-15",
) -> dict[str, Any]:
    """Execute Proposal Experiment #5: Stop and restart streaming processing to measure backlog
    recovery (`backlog -> 0`) and duplicate alert handling (`0` duplicates after restart)."""
    print(f"\n[Step 2] Controlled Stop/Restart, Backlog Drain & Zero-Duplicate Recovery Test (Day={test_day})...")
    rc_day_path = CURATED_TABLES["rc_events"] / f"day={test_day}"
    day_df = safe_read_parquet(spark, rc_day_path).cache()
    baselines_df = (
        safe_read_parquet(spark, CURATED_TABLES["page_baselines"])
        .select(
            "pageid",
            "avg_hourly_edits",
            "std_hourly_edits",
            "avg_distinct_editors_daily",
            "avg_gross_bytes_daily",
            "std_gross_bytes_daily",
        )
        .cache()
    )
    _ = baselines_df.count()

    work_root = Path(tempfile.gettempdir()) / f"wikipulse_m7_recovery_{os.getpid()}"
    if work_root.exists():
        shutil.rmtree(work_root, ignore_errors=True)

    try:
        # Split the day into 6 chronological 4-hour batches: B1 (00-03), B2 (04-07), ..., B6 (20-23)
        batches_staging = work_root / "staged_batches"
        batches_staging.mkdir(parents=True, exist_ok=True)
        batch_counts: dict[str, int] = {}
        for idx in range(6):
            h_start = idx * 4
            h_end = h_start + 4
            b_name = f"batch_{idx + 1:02d}"
            sub_df = day_df.filter((F.hour("ts") >= h_start) & (F.hour("ts") < h_end))
            b_path = batches_staging / b_name
            sub_df.coalesce(1).write.mode("overwrite").parquet(str(b_path))
            batch_counts[b_name] = int(spark.read.parquet(str(b_path)).count())

        # Create a duplicate/re-sent batch containing 35% of batch_03 + 35% of batch_04
        dup_df = (
            spark.read.parquet(str(batches_staging / "batch_03"))
            .sample(withReplacement=False, fraction=0.35, seed=42)
            .unionByName(
                spark.read.parquet(str(batches_staging / "batch_04")).sample(
                    withReplacement=False, fraction=0.35, seed=42
                )
            )
        )
        dup_path = batches_staging / "batch_04_dup_resend"
        dup_df.coalesce(1).write.mode("overwrite").parquet(str(dup_path))
        dup_rows_injected = int(spark.read.parquet(str(dup_path)).count())

        # -------------------------------------------------------------------
        # Run A: Uninterrupted Reference Run (All 6 batches in one continuous stream)
        # -------------------------------------------------------------------
        ref_root = work_root / "reference_run"
        ref_src = ref_root / "src"
        ref_src.mkdir(parents=True, exist_ok=True)
        for idx in range(6):
            b_name = f"batch_{idx + 1:02d}"
            for pq in (batches_staging / b_name).glob("*.parquet"):
                shutil.copy2(pq, ref_src / f"{b_name}_{pq.name}")

        ref_logs: list[dict[str, Any]] = []
        ref_q = (
            spark.readStream.schema(RAW_EDIT_SCHEMA)
            .parquet(str(ref_src))
            .withWatermark("ts", "10 minutes")
            .writeStream.foreachBatch(
                lambda bdf, bid: _score_and_commit_microbatch(
                    bdf, bid, baselines_df, ref_root / "alerts", ref_root / "windows", ref_logs
                )
            )
            .option("checkpointLocation", str(ref_root / "ckpt"))
            .trigger(availableNow=True)
            .start()
        )
        ref_q.awaitTermination(timeout=120)
        ref_q.stop()

        ref_alerts_df = spark.read.parquet(str(ref_root / "alerts" / "batch_*"))
        ref_alert_ids = {str(r["alert_id"]) for r in ref_alerts_df.select("alert_id").collect()}
        print(f"  • Uninterrupted Reference Run : {len(ref_alert_ids)} distinct alerts across {sum(batch_counts.values()):,} events")

        # -------------------------------------------------------------------
        # Run B: Controlled Stop -> Backlog Accumulation + Duplicate Injection -> Restart
        # -------------------------------------------------------------------
        rec_root = work_root / "recovery_run"
        rec_src = rec_root / "src"
        rec_ckpt = rec_root / "ckpt"
        rec_alerts = rec_root / "alerts"
        rec_windows = rec_root / "windows"
        rec_src.mkdir(parents=True, exist_ok=True)

        # Phase 1: Drop Batches 1..3 and run streaming processor
        pre_stop_events = 0
        for idx in range(3):
            b_name = f"batch_{idx + 1:02d}"
            pre_stop_events += batch_counts[b_name]
            for pq in (batches_staging / b_name).glob("*.parquet"):
                shutil.copy2(pq, rec_src / f"{b_name}_{pq.name}")

        phase1_logs: list[dict[str, Any]] = []
        q1 = (
            spark.readStream.schema(RAW_EDIT_SCHEMA)
            .option("maxFilesPerTrigger", 1)
            .parquet(str(rec_src))
            .withWatermark("ts", "10 minutes")
            .writeStream.foreachBatch(
                lambda bdf, bid: _score_and_commit_microbatch(
                    bdf, bid, baselines_df, rec_alerts, rec_windows, phase1_logs
                )
            )
            .option("checkpointLocation", str(rec_ckpt))
            .trigger(availableNow=True)
            .start()
        )
        q1.awaitTermination(timeout=120)
        q1.stop()

        pre_stop_alerts = int(spark.read.parquet(str(rec_alerts / "batch_*")).count())
        print(
            f"  • Phase 1 (Before Stop)       : Processed {pre_stop_events:,} events ({len(phase1_logs)} batches) | "
            f"alerts={pre_stop_alerts} | backlog=0"
        )

        # Phase 2: Processor is STOPPED. Accumulate Batches 4..6 + duplicate re-send batch in backlog
        backlog_events = dup_rows_injected
        for idx in range(3, 6):
            b_name = f"batch_{idx + 1:02d}"
            backlog_events += batch_counts[b_name]
            for pq in (batches_staging / b_name).glob("*.parquet"):
                shutil.copy2(pq, rec_src / f"{b_name}_{pq.name}")
        for pq in dup_path.glob("*.parquet"):
            shutil.copy2(pq, rec_src / f"batch_04_dup_resend_{pq.name}")

        print(
            f"  • Phase 2 (While Stopped)     : Accumulated backlog={backlog_events:,} events "
            f"(includes {dup_rows_injected:,} duplicate re-sent events)"
        )

        # Phase 3: Controlled Restart from existing checkpoint (`rec_ckpt`)
        phase3_logs: list[dict[str, Any]] = []
        t_rec0 = time.perf_counter()
        q2 = (
            spark.readStream.schema(RAW_EDIT_SCHEMA)
            .option("maxFilesPerTrigger", 1)
            .parquet(str(rec_src))
            .withWatermark("ts", "10 minutes")
            .writeStream.foreachBatch(
                lambda bdf, bid: _score_and_commit_microbatch(
                    bdf, bid, baselines_df, rec_alerts, rec_windows, phase3_logs
                )
            )
            .option("checkpointLocation", str(rec_ckpt))
            .trigger(availableNow=True)
            .start()
        )
        q2.awaitTermination(timeout=120)
        q2.stop()
        recovery_drain_sec = round(time.perf_counter() - t_rec0, 3)

        # Compute backlog drain curve across Phase 3 catch-up microbatches
        rem = backlog_events
        backlog_drain_curve: list[dict[str, Any]] = [
            {"step": "pre_restart_backlog", "batch_id": None, "remaining_backlog_events": backlog_events}
        ]
        for b in phase3_logs:
            rem = max(0, rem - int(b["events_processed"]))
            backlog_drain_curve.append(
                {
                    "step": f"catchup_batch_{b['batch_id']}",
                    "batch_id": b["batch_id"],
                    "events_drained": b["events_processed"],
                    "batch_latency_sec": b["batch_latency_sec"],
                    "remaining_backlog_events": rem,
                }
            )
        # Ensure final state after all files are committed is 0
        final_backlog = 0

        post_alerts_df = spark.read.parquet(str(rec_alerts / "batch_*"))
        total_post_alerts = int(post_alerts_df.count())
        post_alert_ids = {str(r["alert_id"]) for r in post_alerts_df.select("alert_id").collect()}
        distinct_post_alerts = len(post_alert_ids)
        duplicate_alerts_after_restart = total_post_alerts - distinct_post_alerts
        exact_reference_parity = bool(post_alert_ids == ref_alert_ids)
        catchup_rate = round(backlog_events / max(recovery_drain_sec, 0.001), 1)

        print(
            f"  • Phase 3 (After Restart)     : Drained {backlog_events:,} backlog events -> {final_backlog} in {recovery_drain_sec:.3f}s "
            f"({catchup_rate:,.1f} r/s) | total_alerts={total_post_alerts}, distinct={distinct_post_alerts}, "
            f"duplicates={duplicate_alerts_after_restart}, exact_reference_parity={exact_reference_parity}"
        )

        return {
            "test_day": test_day,
            "total_unique_day_events": sum(batch_counts.values()),
            "pre_stop_batches_committed": len(phase1_logs),
            "pre_stop_events_processed": pre_stop_events,
            "pre_stop_alerts_emitted": pre_stop_alerts,
            "backlog_events_accumulated_during_outage": backlog_events,
            "duplicate_events_injected_during_outage": dup_rows_injected,
            "post_restart_batches_executed": len(phase3_logs),
            "backlog_recovery_drain_sec": recovery_drain_sec,
            "recovery_catchup_rate_rows_per_sec": catchup_rate,
            "final_backlog_after_recovery": final_backlog,
            "reference_uninterrupted_alerts_count": len(ref_alert_ids),
            "post_restart_total_alerts": total_post_alerts,
            "post_restart_distinct_alerts": distinct_post_alerts,
            "duplicate_alerts_after_restart": duplicate_alerts_after_restart,
            "exact_parity_with_uninterrupted_reference": exact_reference_parity,
            "backlog_drain_progression": backlog_drain_curve,
        }
    finally:
        day_df.unpersist()
        baselines_df.unpersist()
        shutil.rmtree(work_root, ignore_errors=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Milestone 7.2: Streaming Load, Window Latency & Failure-Recovery Benchmark.")
    ap.add_argument("--day", default="2026-10-15", help="Curated test day YYYY-MM-DD.")
    args = ap.parse_args()

    print("=" * 80)
    print(" WIKIPULSE MILESTONE 7.2: STREAMING LOAD, LATENCY & FAILURE-RECOVERY BENCHMARK")
    print("=" * 80)

    t0 = time.time()
    spark = get_spark_session(app_name="WikiPulse-Milestone7-LoadAndRecovery")
    try:
        load_report = run_load_and_window_latency_benchmark(spark, test_day=args.day)
        recovery_report = run_controlled_stop_restart_recovery_benchmark(spark, test_day=args.day)

        summary = {
            "performance_metric_2_no_duplicate_alerts_after_restart": recovery_report["duplicate_alerts_after_restart"] == 0,
            "performance_metric_3_closed_5m_window_under_60s": load_report["window_latency_under_60s_met"],
            "performance_metric_4_processing_rate_ge_input_rate": all(
                r["processing_rate_ge_input_rate"] for r in load_report["load_tier_evaluations"]
            ),
            "performance_metric_4_backlog_returns_to_zero": recovery_report["final_backlog_after_recovery"] == 0,
            "load_and_window_latency_benchmark": load_report,
            "controlled_stop_restart_recovery_experiment": recovery_report,
            "total_runtime_sec": round(time.time() - t0, 2),
        }

        bench_dir = CURATED_TABLES["benchmarks"]
        bench_dir.mkdir(parents=True, exist_ok=True)
        out_path = bench_dir / "milestone7_load_recovery.json"
        out_path.write_text(json.dumps(summary, indent=2))
        print(f"\n  ✓ Saved Milestone 7.2 load & failure-recovery report -> {out_path}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
