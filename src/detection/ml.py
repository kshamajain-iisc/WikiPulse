"""Initial Spark ML comparison: scaled log features and K-means distance.

Fit only on earlier historical windows. The distance threshold is the training
99th percentile, not a calibrated probability or a claim of detection accuracy.
"""

import argparse
import json
import time
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import functions as F

from src.detection.scoring import score_windows
from src.spark_common import create_spark


def feature_columns(windows):
    """Log counts limit scale dominance; coverage exposes missing size evidence."""
    rows = windows
    for name in ("edits_human", "distinct_human_editors", "gross_bytes"):
        rows = rows.withColumn("ml_" + name, F.log1p(F.coalesce(F.col(name), F.lit(0.0))))
    return rows.withColumn("ml_minor_ratio", F.col("minor_edits") / F.col("edits_total"))


def distances(model, windows):
    transformed = model.transform(feature_columns(windows))
    centers = model.stages[-1].clusterCenters()
    center_map = F.create_map(*[item for index, center in enumerate(centers)
                                for item in (F.lit(index), F.array(*[F.lit(float(v)) for v in center]))])
    scaler = model.stages[1]
    inputs = model.stages[0].getInputCols()
    center = center_map[F.col("ml_cluster")]
    # Expand the fitted scaler over scalar columns. This keeps distance arithmetic
    # in Spark SQL without importing pandas via pyspark.ml.functions. Spark maps
    # a zero-variance dimension to zero, which we must match here as well.
    squared = F.lit(0.0)
    for index, name in enumerate(inputs):
        scaled = ((F.col(name) - float(scaler.mean[index])) / float(scaler.std[index])
                  if scaler.std[index] else F.lit(0.0))
        squared = squared + F.pow(scaled - center[index], 2)
    return transformed.withColumn("ml_distance", F.sqrt(squared))


def compare_methods(training, evaluation, baselines, output: Path, k=2, seed=42):
    """Persist a fitted pipeline, threshold metadata, and per-window comparisons."""
    if output.exists():
        raise ValueError(f"Choose a new ML experiment output: {output}")
    if k < 2:
        raise ValueError("K-means requires k >= 2")
    # Compare epoch values and serialize metadata in Spark's UTC session. Python
    # timestamp collection otherwise converts them to naive host-local times.
    train_bounds = training.agg(
        F.max("window_end").cast("long").alias("end_epoch"),
        F.date_format(F.max("window_end"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("end_utc"),
        F.count("*").alias("count"),
    ).first()
    eval_start = evaluation.agg(F.min("window_start").cast("long")).first()[0]
    durations = training.select("window_duration").union(evaluation.select("window_duration")).distinct().collect()
    if len(durations) != 1:
        raise ValueError("Training and evaluation must use one matching window duration")
    if train_bounds["count"] < k or eval_start is None or eval_start < train_bounds.end_epoch:
        raise ValueError("Need at least k training rows and nonempty evaluation strictly after training windows")
    inputs = ["ml_edits_human", "ml_distinct_human_editors", "ml_gross_bytes",
              "bot_ratio", "revert_ratio", "ml_minor_ratio", "size_coverage"]
    prepared = feature_columns(training)
    if prepared.select(*inputs).distinct().limit(k).count() < k:
        raise ValueError("Need at least k distinct training feature vectors")
    pipeline = Pipeline(stages=[
        VectorAssembler(inputCols=inputs, outputCol="ml_unscaled"),
        StandardScaler(inputCol="ml_unscaled", outputCol="ml_features", withMean=True, withStd=True),
        KMeans(k=k, seed=seed, maxIter=30, featuresCol="ml_features", predictionCol="ml_cluster"),
    ])
    started = time.monotonic()
    model = pipeline.fit(prepared)
    train_distance = distances(model, training)
    threshold = max(1e-6, train_distance.approxQuantile("ml_distance", [0.99], 0.001)[0])
    training_seconds = time.monotonic() - started
    started = time.monotonic()
    compared = distances(model, score_windows(evaluation, baselines)).withColumn(
        "ml_alert", F.col("ml_distance") > threshold
    ).withColumn("ml_score", 100 * F.col("ml_distance") / (F.col("ml_distance") + threshold))
    # ML vectors are internal model inputs; keep the public comparison table scalar.
    compared = compared.drop("ml_unscaled", "ml_features")
    compared.write.parquet(str(output / "comparison"))
    result = evaluation.sparkSession.read.parquet(str(output / "comparison"))
    counts = result.agg(F.count("*").alias("windows"), *[
        F.sum(F.col(name).cast("long")).alias(name + "_count")
        for name in ("fixed_rule_alert", "historical_alert", "ml_alert")
    ]).first().asDict()
    metadata = {"method": "StandardScaler + seeded KMeans / nearest-center distance", "k": k,
                "seed": seed, "features": inputs, "training_end": train_bounds.end_utc,
                "distance_threshold": threshold, "threshold_quantile": 0.99,
                "training_seconds": training_seconds, "comparison_seconds": time.monotonic() - started,
                "counts": counts}
    model.write().save(str(output / "model"))
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2))
    return result, metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--training", type=Path, required=True)
    parser.add_argument("--evaluation", type=Path, required=True)
    parser.add_argument("--baselines", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--k", type=int, default=2)
    args = parser.parse_args()
    spark = create_spark("WikiPulse-ML-Comparison", with_kafka=False)
    try:
        _, metadata = compare_methods(spark.read.parquet(str(args.training)), spark.read.parquet(str(args.evaluation)),
                                      spark.read.parquet(str(args.baselines)), args.output, args.k)
        print(json.dumps(metadata, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
