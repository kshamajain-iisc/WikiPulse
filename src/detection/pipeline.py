"""Run the same detector in batch or against finalized streaming page windows."""

import hashlib
import json
import os
from dataclasses import asdict
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F

from src.detection.scoring import SCORE_VERSION, DetectorConfig, score_windows
from src.detection.storage import commit_batch, publish_alerts
from src.page_features import HISTORY_SCHEMA, historical_edits, page_windows


def load_baselines(spark, path: Path):
    """An absent initial baseline is supported; corrupt existing data fails loudly."""
    if not path.exists():
        return None
    return spark.read.parquet(str(path))


def run_batch(spark, windows: Path, baselines: Path, output: Path, config=DetectorConfig()):
    """Write a new experiment directory; refuse to overwrite prior results."""
    if output.exists():
        raise ValueError(f"Choose a new experiment output: {output}")
    scored = score_windows(spark.read.parquet(str(windows)), load_baselines(spark, baselines), config)
    scored.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        scored.write.parquet(str(output / "scored_windows"))
        scored.filter("is_alert").write.parquet(str(output / "alerts"))
    finally:
        scored.unpersist()


def pin_contract(checkpoint: Path, source: Path, baselines: Path, output: Path, duration: str, config, topic):
    """Refuse changed detector inputs with a checkpoint that has committed offsets."""
    files = sorted((str(p.relative_to(baselines)), p.stat().st_size, p.stat().st_mtime_ns)
                   for p in baselines.rglob("*.parquet"))
    contract = {
        "version": SCORE_VERSION, "config": asdict(config), "source": str(source.resolve()),
        "baseline": str(baselines.resolve()), "baseline_signature": hashlib.sha256(json.dumps(files).encode()).hexdigest(),
        "output": str(output.resolve()), "duration": duration, "topic": topic,
    }
    manifest = checkpoint / "detector.json"
    if manifest.exists():
        if json.loads(manifest.read_text()) != contract:
            raise ValueError("Detector inputs changed; use a new checkpoint and output directory")
    else:
        if (output / "scored_windows" / f"duration={duration}").exists():
            raise ValueError("Output already belongs to another checkpoint; choose a new output")
        checkpoint.mkdir(parents=True, exist_ok=True)
        manifest.write_text(json.dumps(contract, indent=2))


def start_stream(spark, source: Path, baselines: Path, output: Path, checkpoint: Path,
                 duration: str, config=DetectorConfig(), topic=None, available_now=False):
    """Score only append-only, watermark-finalized windows, never live previews."""
    pin_contract(checkpoint, source, baselines, output, duration, config, topic)
    baseline = load_baselines(spark, baselines)
    # Derive the explicit sink schema from the shared feature transform. This
    # allows startup before the first watermark has finalized a window.
    schema = page_windows(historical_edits(spark.createDataFrame([], HISTORY_SCHEMA)),
                          "5 minutes" if duration == "5m" else "1 hour").schema
    source.mkdir(parents=True, exist_ok=True)
    windows = spark.readStream.schema(schema).option("maxFilesPerTrigger", 100).parquet(str(source))

    def process_batch(batch, batch_id):
        destination = output / "scored_windows" / f"duration={duration}" / f"batch_id={batch_id:020d}"
        scored = score_windows(batch.dropDuplicates(["window_id"]), baseline, config)
        saved = commit_batch(scored, destination)
        alerts_path = output / "alerts" / f"duration={duration}" / f"batch_id={batch_id:020d}"
        alerts = commit_batch(saved.filter(F.col("is_alert")), alerts_path)
        if topic:
            publish_alerts(alerts, topic)

    writer = windows.writeStream.foreachBatch(process_batch).option("checkpointLocation", str(checkpoint))
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{os.getenv('SPARK_TRIGGER_SECONDS', '10')} seconds")
    return writer.start()
