"""Batch or streaming knowledge-shift detection: python -m src.detection --help."""

import argparse
import json
from pathlib import Path

from src.detection.pipeline import run_batch, start_stream
from src.detection.scoring import DetectorConfig
from src.kafka_common import KAFKA_ALERT_TOPIC, ensure_topic
from src.spark_common import ROOT, create_spark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["batch", "stream"])
    parser.add_argument("--duration", choices=["5m", "1h"], default="5m")
    parser.add_argument("--windows", type=Path, help="Override the finalized window input")
    parser.add_argument("--baselines", type=Path, help="Override the matching baseline table")
    parser.add_argument("--output", type=Path, default=ROOT / "data/curated")
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument("--threshold", type=float, default=65.0)
    parser.add_argument("--no-kafka", action="store_true", help="Streaming: persist locally without Kafka publication")
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--ready-file", type=Path)
    args = parser.parse_args()
    try:
        config = DetectorConfig(threshold=args.threshold)
    except ValueError as error:
        parser.error(str(error))
    baselines = args.baselines or ROOT / f"data/week2/historical/baselines_{args.duration}"
    windows = args.windows or ROOT / f"data/live_activity/windows_{args.duration}"
    checkpoint = args.checkpoint or ROOT / f"data/checkpoints/detection/{args.duration}"
    topic = None if args.no_kafka else KAFKA_ALERT_TOPIC
    if args.mode == "stream" and topic:
        ensure_topic(topic)
    spark = create_spark("WikiPulse-Detection-" + args.mode, with_kafka=False)
    query = None
    try:
        if args.mode == "batch":
            run_batch(spark, windows, baselines, args.output, config)
        else:
            query = start_stream(spark, windows, baselines, args.output, checkpoint,
                                 args.duration, config, topic, args.available_now)
            if args.ready_file:
                args.ready_file.parent.mkdir(parents=True, exist_ok=True)
                args.ready_file.write_text(str(query.id))
            while query.isActive:
                query.awaitTermination(5)
                if args.ready_file and query.lastProgress:
                    path = args.ready_file.with_suffix(".progress.json")
                    temporary = path.with_suffix(".tmp")
                    temporary.write_text(json.dumps(query.lastProgress))
                    temporary.replace(path)
            if query.exception():
                raise RuntimeError(str(query.exception()))
    except KeyboardInterrupt:
        pass
    finally:
        if query:
            query.stop()
        if args.ready_file:
            args.ready_file.unlink(missing_ok=True)
        spark.stop()


if __name__ == "__main__":
    main()
