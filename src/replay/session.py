"""One isolated, observable Kafka and Spark replay run for the activity boards."""

import argparse
import json
import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from dashboards.activity_data import day_bounds
from src.kafka_common import create_producer, ensure_topic
from src.page_features import live_edits
from src.replay.events import paced_envelopes, to_event
from src.spark_common import create_spark, parse_kafka_events, KAFKA_SERVERS
from src.stream_control import atomic_json, read_json


class ReplayGate:
    def __init__(self, control):
        self.control = control
        self.last = time.monotonic()
        self.elapsed = 0

    def clock(self):
        now = time.monotonic()
        command = read_json(self.control)
        if command.get("cancelled"):
            raise InterruptedError("Replay cancelled")
        if not command.get("paused"):
            self.elapsed += now - self.last
        self.last = now
        return self.elapsed

    def wait(self):
        while read_json(self.control).get("paused"):
            self.clock()
            time.sleep(.1)
        self.clock()


def accumulate_progress(state, progress_rows, batches):
    """Count committed data batches, not idle progress heartbeats.

    Spark can report a no-data heartbeat using the next batch ID before a real
    data batch commits with that same ID. Marking the heartbeat as counted would
    discard the later batch's real validation and output counts.
    """
    for progress in progress_rows:
        batch = progress["batchId"]
        if batch in batches or not progress.get("numInputRows", 0):
            continue
        observed = progress.get("observedMetrics", {})
        validation, output = observed.get("validation", {}), observed.get("output", {})
        validation = validation.asDict() if hasattr(validation, "asDict") else validation
        output = output.asDict() if hasattr(output, "asDict") else output
        if "rows" not in validation or "rows" not in output:
            continue
        batches.add(batch)
        state["consumed"] += validation["rows"]
        state["rejected"] += validation.get("rejected", 0) or 0
        state["committed"] += output["rows"]
        state["batch_duration_ms"] = progress.get("batchDuration")
        state["processed_rows_per_second"] = progress.get("processedRowsPerSecond")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--format", choices=("canonical", "bronze"), required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--day", required=True)
    parser.add_argument("--zone", choices=("IST", "UTC"), default="IST")
    parser.add_argument("--speed", type=int, choices=(1, 5, 20, 24, 48, 50, 100), default=50)
    parser.add_argument("--limit", type=int, help="Integration verification only: bounded event count")
    args = parser.parse_args()
    run = args.run_dir
    run.mkdir(parents=True, exist_ok=True)
    if not run.name.isalnum():
        parser.error("Invalid run identity")
    topic = "wiki.replay." + run.name
    status_path, control = run / "status.json", run / "control.json"
    state = {"id": run.name, "status": "preparing", "day": args.day, "zone": args.zone,
             "speed": args.speed, "topic": topic, "selected": None, "published": 0,
             "consumed": 0, "committed": 0, "rejected": 0, "cursor": 0,
             "message": "Preparing the selected day in Spark", "paused": False}
    lock = threading.RLock()
    spark, query, producer = None, None, None
    published_done = threading.Event()
    failures = []

    def snapshot():
        with lock:
            state["updated_at"] = datetime.now(timezone.utc).isoformat()
            atomic_json(status_path, state)

    def cancel(signum, frame):
        atomic_json(control, {"cancelled": True})

    signal.signal(signal.SIGTERM, cancel)
    signal.signal(signal.SIGINT, cancel)
    try:
        snapshot()
        start, end = day_bounds(args.day, args.zone)
        spark = create_spark("WikiPulse-Isolated-Replay-" + run.name[:8], with_kafka=True)
        edits = spark.read.option("pathGlobFilter", "*.parquet").parquet(str(args.input))
        if args.format == "bronze":
            edits = live_edits(edits)
        edits = edits.withColumn("ts", F.col("ts").cast("timestamp"))
        edits = edits.filter((F.col("ns") == 0) & (F.col("type") == "edit")
                             & (F.col("ts") >= F.lit(datetime.fromtimestamp(start, timezone.utc)))
                             & (F.col("ts") < F.lit(datetime.fromtimestamp(end, timezone.utc))))
        ordered = edits.dropDuplicates(["rcid"]).withColumn("event_epoch", F.col("ts").cast("long")).orderBy("ts", "rcid")
        if args.limit:
            ordered = ordered.limit(args.limit)
        state["selected"] = ordered.count()
        if read_json(control).get("cancelled"):
            raise InterruptedError("Replay cancelled while preparing")
        ensure_topic(topic)
        producer = create_producer("wikipulse-isolated-replay-" + run.name[:8])
        messages = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_SERVERS)
                    .option("subscribe", topic).option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "true").option("maxOffsetsPerTrigger", 10000).load())
        rows = parse_kafka_events(messages).observe("validation", F.count(F.lit(1)).alias("rows"),
                    F.sum((~F.col("is_valid")).cast("long")).alias("rejected"))
        rows = (rows.filter("is_valid").withWatermark("event_time", "1 day")
                .dropDuplicatesWithinWatermark(["event_id"]).observe("output", F.count(F.lit(1)).alias("rows")))
        query = (rows.writeStream.format("parquet").outputMode("append")
                 .option("checkpointLocation", str(run / "checkpoint"))
                 .option("path", str(run / "bronze")).partitionBy("ingestion_date")
                 .trigger(processingTime="2 seconds").start())
        gate = ReplayGate(control)
        state.update(status="running", message="Publishing to isolated Kafka; Spark validates, deduplicates and commits Bronze")

        def delivered(error, message):
            with lock:
                if error:
                    failures.append(str(error))
                else:
                    state["published"] += 1

        def publish():
            try:
                events = (to_event(row.asDict()) for row in ordered.toLocalIterator())
                for envelope in paced_envelopes(events, args.speed, clock=gate.clock):
                    while True:
                        gate.wait()
                        try:
                            producer.produce(topic, key=envelope["event_id"], value=json.dumps(envelope), on_delivery=delivered)
                            break
                        except BufferError:
                            producer.poll(.1)
                    producer.poll(0)
                    with lock:
                        state["cursor"] = max(0, min(86400, envelope["raw_event"]["timestamp"] - start + 1))
            except InterruptedError:
                pass
            except Exception as error:
                failures.append(str(error))
            finally:
                if producer.flush(15):
                    failures.append("Some replay messages were not delivered")
                published_done.set()

        thread = threading.Thread(target=publish, daemon=True, name="replay-publication")
        thread.start()
        batches = set()
        drain_started = None
        while True:
            # Delivery callbacks must keep advancing even when publication is
            # paused or waiting through an event-time gap.
            producer.poll(0)
            command = read_json(control)
            with lock:
                state["paused"] = bool(command.get("paused"))
                if command.get("cancelled"):
                    state.update(status="cancelling", message="Publication stopped; draining acknowledged messages before closing the isolated run")
                accumulate_progress(state, query.recentProgress, batches)
                state["pending"] = max(0, state["published"] - state["consumed"])
            if not query.isActive:
                raise RuntimeError(str(query.exception() or "Replay processing stopped"))
            snapshot()
            if published_done.is_set():
                if state["consumed"] >= state["published"]:
                    break
                drain_started = drain_started or time.monotonic()
                if time.monotonic() - drain_started > 90:
                    raise RuntimeError("Replay processing did not drain within 90 seconds; outputs and checkpoints retained")
            time.sleep(.5)
        cancelled = read_json(control).get("cancelled", False)
        state.update(status="cancelled" if cancelled else "complete", paused=False,
                     message="Replay cancelled; partial output retained" if cancelled else "Kafka replay complete; selected, delivered and committed counts reconciled")
        if failures:
            raise RuntimeError("; ".join(failures[:3]))
        if not cancelled and not (state["selected"] == state["published"] == state["consumed"] == state["committed"]):
            raise RuntimeError("Replay counts do not reconcile; inspect the retained run")
    except InterruptedError as error:
        state.update(status="cancelled", message=str(error))
    except Exception as error:
        state.update(status="failed", message=str(error))
        atomic_json(control, {"cancelled": True})
    finally:
        if query is not None:
            query.stop()
        snapshot()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
