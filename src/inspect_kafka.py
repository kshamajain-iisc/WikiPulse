
import argparse

from pyspark.sql import functions as F

from spark_common import (
    create_spark,
    parse_kafka_events,
    KAFKA_SERVERS,
    KAFKA_TOPIC,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit", type=int, default=10
    )
    args = parser.parse_args()

    spark = create_spark("WikiPulse-Kafka-Inspection")
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Connecting to Kafka...")
        print("Broker:", KAFKA_SERVERS)
        print("Topic:", KAFKA_TOPIC)

        kafka_df = (
            spark.read
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                KAFKA_SERVERS,
            )
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        print("\nKafka source schema:")
        kafka_df.printSchema()

        parsed = parse_kafka_events(kafka_df)

        print("\nParsed event schema:")
        parsed.printSchema()

        print("\nSample valid Wikipedia edits:")

        (
            parsed
            .filter(F.col("is_valid"))
            .select(
                "event_id",
                "page_title",
                "event_time",
                "old_revision_id",
                "new_revision_id",
                "kafka_partition",
                "kafka_offset",
            )
            .show(args.limit, truncate=False)
        )

        print("\nValidation counts:")

        parsed.groupBy("is_valid").count().show()

        print("\nTotal Kafka records:")
        print(parsed.count())

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
