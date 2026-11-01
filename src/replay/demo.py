"""Generate controlled fixtures, score them, and optionally run Spark ML offline."""

import argparse
import gzip
import json
from pathlib import Path

from pyspark.sql import functions as F

from src.detection.ml import compare_methods
from src.detection.scoring import score_windows
from src.page_features import HISTORY_SCHEMA, build_baselines, historical_edits, page_windows
from src.replay.patterns import controlled_records
from src.spark_common import ROOT, create_spark


def run_demo(spark, output: Path, with_ml=False):
    if output.exists():
        raise ValueError(f"Choose a new demo output: {output}")
    output.mkdir(parents=True)
    training, evaluation, labels = controlled_records()
    for name, events in (("training", training), ("evaluation", evaluation)):
        with gzip.open(output / f"{name}.jsonl.gz", "wt", encoding="utf-8") as stream:
            for event in events:
                stream.write(json.dumps(event) + "\n")
    (output / "labels.json").write_text(json.dumps(labels, indent=2))
    windows = {}
    for name in ("training", "evaluation"):
        raw = spark.read.schema(HISTORY_SCHEMA).json(str(output / f"{name}.jsonl.gz"))
        page_windows(historical_edits(raw)).write.parquet(str(output / f"windows_{name}"))
        windows[name] = spark.read.parquet(str(output / f"windows_{name}"))
    baseline = build_baselines(windows["training"], "2026-10-01", "2026-10-02", 300)
    baseline.write.parquet(str(output / "baselines_5m"))
    baseline = spark.read.parquet(str(output / "baselines_5m"))
    if with_ml:
        scored, _ = compare_methods(windows["training"], windows["evaluation"], baseline, output / "ml")
    else:
        scored = score_windows(windows["evaluation"], baseline)
    scored.write.parquet(str(output / "scored_windows"))
    scored.filter("is_alert").write.parquet(str(output / "alerts"))
    # Labels live outside feature/model input, so neither method can train on them.
    label_frame = spark.createDataFrame(labels)
    evaluated = scored.join(label_frame, "title")
    methods = ["fixed_rule_alert", "historical_alert"] + (["ml_alert"] if with_ml else [])
    summaries = {}
    for method in methods:
        summaries[method] = evaluated.agg(*[
            F.sum(condition.cast("long")).alias(name) for name, condition in (
                ("true_positives", F.col(method) & F.col("expected_alert")),
                ("false_positives", F.col(method) & ~F.col("expected_alert")),
                ("false_negatives", ~F.col(method) & F.col("expected_alert")),
                ("true_negatives", ~F.col(method) & ~F.col("expected_alert")),
            )
        ]).first().asDict()
    evaluated.select("pattern", "expected_alert", *methods, "historical_score", "reasons").write.json(str(output / "evaluation"))
    (output / "summary.json").write_text(json.dumps(summaries, indent=2))
    return summaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "data/experiments/week3")
    parser.add_argument("--with-ml", action="store_true")
    args = parser.parse_args()
    spark = create_spark("WikiPulse-Controlled-Detection", with_kafka=False)
    try:
        print(json.dumps(run_demo(spark, args.output, args.with_ml), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
