
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

try:
    from .page_identity import title_page_id
except ImportError:  # Support existing direct script entry points.
    from page_identity import title_page_id
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, IntegerType, BooleanType, ArrayType,
)

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent

KAFKA_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC", "wiki.raw.edits"
)
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[2]")
KAFKA_PACKAGE = os.getenv(
    "SPARK_KAFKA_PACKAGE",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
)

RAW_PATH = str(
    (ROOT / os.getenv(
        "RAW_PARQUET_PATH", "data/raw/wikipedia_edits"
    )).resolve()
)

CHECKPOINT_PATH = str(
    (ROOT / os.getenv(
        "RAW_CHECKPOINT_PATH",
        "data/checkpoints/raw_edits"
    )).resolve()
)


def create_spark(app_name, with_kafka=True):
    """Create a UTC Spark session; offline jobs can skip Kafka jar resolution."""
    # Local workers must use the driver's Python version, not another python3 on PATH.
    if SPARK_MASTER.startswith("local"):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = SparkSession.builder
    if with_kafka:
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    return (
        builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.streaming.forceDeleteTempCheckpointLocation",
            "true",
        )
        .getOrCreate()
    )


# Schema of the message published by Task 1.
ENVELOPE_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("ingested_at", StringType()),
])

# Wikimedia fields needed by the first pipeline.
WIKIMEDIA_SCHEMA = StructType([
    StructField("user", StringType()),
    StructField("userid", LongType()),
    StructField("tags", ArrayType(StringType())),
    StructField("comment", StringType()),
    StructField("id", LongType()),
    StructField("wiki", StringType()),
    StructField("type", StringType()),
    StructField("namespace", IntegerType()),
    StructField("page_id", LongType()),
    StructField("title", StringType()),
    StructField("timestamp", LongType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField(
        "revision",
        StructType([
            StructField("old", LongType()),
            StructField("new", LongType()),
        ]),
    ),
    StructField(
        "length",
        StructType([
            StructField("old", LongType()),
            StructField("new", LongType()),
        ]),
    ),
])


def parse_kafka_events(kafka_df):
    # Preserve Kafka's original message.
    base = kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

    # Parse the Task 1 envelope.
    parsed = base.withColumn(
        "envelope",
        F.from_json(
            F.col("raw_json"),
            ENVELOPE_SCHEMA,
            {"mode": "PERMISSIVE"},
        ),
    )

    # Convert the nested raw_event back to JSON.
    # The Task 1 producer stores it as a JSON object.
    parsed = parsed.withColumn(
        "raw_event_json",
        F.get_json_object(
            F.col("raw_json"), "$.raw_event"
        ),
    )

    parsed = parsed.withColumn(
        "wiki_event",
        F.from_json(
            F.col("raw_event_json"),
            WIKIMEDIA_SCHEMA,
            {"mode": "PERMISSIVE"},
        ),
    )

    result = parsed.select(
        "raw_json",
        "raw_event_json",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",

        F.col("envelope.event_id").alias("event_id"),
        F.to_timestamp(
            F.col("envelope.event_time")
        ).alias("event_time"),
        F.to_timestamp(
            F.col("envelope.ingested_at")
        ).alias("ingested_at"),

        F.col("wiki_event.wiki").alias("wiki"),
        F.col("wiki_event.id").alias("change_id"),
        # EventStreams commonly omits the numeric page ID. Negative values
        # are internal title keys; actual MediaWiki page IDs remain positive.
        F.coalesce(
            F.when(F.col("wiki_event.page_id") > 0, F.col("wiki_event.page_id")),
            F.when(F.length(F.col("wiki_event.title")) > 0,
                   title_page_id(F.col("wiki_event.title"))),
        ).alias("page_id"),
        F.col("wiki_event.title").alias("page_title"),
        F.col("wiki_event.namespace").alias("namespace"),
        F.col("wiki_event.type").alias("change_type"),

        F.col("wiki_event.revision.old").alias(
            "old_revision_id"
        ),
        F.col("wiki_event.revision.new").alias(
            "new_revision_id"
        ),

        F.col("wiki_event.length.old").alias(
            "old_length"
        ),
        F.col("wiki_event.length.new").alias(
            "new_length"
        ),

        F.col("wiki_event.bot").alias("is_bot"),
        F.col("wiki_event.minor").alias("is_minor"),
    )

    # Mark invalid records rather than silently dropping them.
    result = result.withColumn(
        "is_valid",
        F.coalesce((
            F.col("event_id").isNotNull()
            & F.col("event_time").isNotNull()
            & F.col("ingested_at").isNotNull()
            & (F.col("wiki") == "enwiki")
            & (F.col("change_type") == "edit")
            & (F.col("namespace") == 0)
            & F.col("change_id").isNotNull()
            & F.col("page_id").isNotNull()
            & (F.length(F.col("page_title")) > 0)
            & (
                F.col("event_id")
                == F.concat(
                    F.lit("enwiki:"),
                    F.col("change_id").cast("string"),
                )
            )
            & (F.col("kafka_key") == F.col("event_id"))
        ), F.lit(False)),
    )

    # Useful for storage partitioning.
    result = result.withColumn(
        "ingestion_date",
        F.to_date(F.col("ingested_at")),
    )

    return result


def reparse_stored_events(stored_events):
    """Apply the current parser to preserved Bronze messages without rewriting them.

    This also repairs previously rejected records when parser rules are corrected.
    Kafka coordinates and original payloads remain the source of truth.
    """
    kafka_events = stored_events.select(
        F.col("kafka_key").alias("key"),
        F.col("raw_json").alias("value"),
        F.col("kafka_topic").alias("topic"),
        F.col("kafka_partition").alias("partition"),
        F.col("kafka_offset").alias("offset"),
        F.col("kafka_timestamp").alias("timestamp"),
    )
    return parse_kafka_events(kafka_events)
