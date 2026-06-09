#!/usr/bin/env python3

import json
import os

from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wearable.sensor.raw")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "wearable-sensor-consumer")


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print(f"Consuming Kafka topic={KAFKA_TOPIC} servers={KAFKA_BOOTSTRAP_SERVERS}", flush=True)

    for message in consumer:
        print(
            json.dumps(
                {
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "key": message.key,
                    "value": message.value,
                },
                indent=2,
            ),
            flush=True,
        )


if __name__ == "__main__":
    main()
