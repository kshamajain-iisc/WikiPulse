
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = {
    "wiki.raw.edits": {
        "partitions": 3,
        "replication_factor": 1,
    }
}


def create_topics():
    admin = AdminClient({
        "bootstrap.servers": BOOTSTRAP_SERVERS
    })

    metadata = admin.list_topics(timeout=10)
    existing_topics = set(metadata.topics.keys())

    new_topics = []

    for name, config in TOPICS.items():
        if name in existing_topics:
            print(f"Topic already exists: {name}")
            continue

        new_topics.append(
            NewTopic(
                topic=name,
                num_partitions=config["partitions"],
                replication_factor=config["replication_factor"],
            )
        )

    if not new_topics:
        print("All required topics already exist.")
        return

    futures = admin.create_topics(new_topics)

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Created topic: {topic}")
        except Exception as error:
            print(f"Failed to create {topic}: {error}")
            raise


if __name__ == "__main__":
    create_topics()
