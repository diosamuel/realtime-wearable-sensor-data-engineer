#!/usr/bin/env python3
"""Bridge simulated MQTT sensor messages into Kafka.

Run after Mosquitto and Kafka are available:
    python3 /opt/spark-apps/mqtt_to_kafka.py

The simulator publishes MQTT messages like:
    T_xacc=8.1305;T_yacc=1.0349;...

This bridge writes JSON messages to Kafka topic `wearable.sensor.raw`.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

import paho.mqtt.client as mqtt
from kafka import KafkaProducer

from utils import SENSOR_COLUMNS


MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "simulate/sensor")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wearable.sensor.raw")


def parse_sensor_payload(payload: str):
    values: dict[str, float] = {}

    for item in payload.split(";"):
        if not item:
            continue

        column, separator, raw_value = item.partition("=")
        if not separator:
            raise ValueError(f"Invalid sensor item without '=': {item}")
        if column not in SENSOR_COLUMNS:
            raise ValueError(f"Unknown sensor column: {column}")

        values[column] = float(raw_value)

    missing_columns = [column for column in SENSOR_COLUMNS if column not in values]
    if missing_columns:
        raise ValueError(f"Missing sensor columns: {', '.join(missing_columns)}")

    return values


def build_kafka_record(topic: str, payload: bytes):
    raw_payload = payload.decode("utf-8")

    return {
        "source": "mqtt",
        "mqtt_topic": topic,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "sensor_count": len(SENSOR_COLUMNS),
        "values": parse_sensor_payload(raw_payload),
    }


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        retries=5,
        linger_ms=10,
    )


def main():
    producer = create_kafka_producer()
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    running = True

    def stop(_signum: int, _frame: Any):
        nonlocal running
        running = False
        mqtt_client.disconnect()

    def on_connect(
        client: mqtt.Client,
        _userdata: Any,
        _flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        _properties: mqtt.Properties | None,
    ):
        if reason_code != 0:
            raise RuntimeError(f"MQTT connection failed with reason code {reason_code}")

        client.subscribe(MQTT_TOPIC)
        print(f"Subscribed MQTT {MQTT_HOST}:{MQTT_PORT} topic={MQTT_TOPIC}", flush=True)

    def on_message(_client: mqtt.Client, _userdata: Any, message: mqtt.MQTTMessage):
        try:
            record = build_kafka_record(message.topic, message.payload)
            producer.send(KAFKA_TOPIC, key=message.topic, value=record)
            producer.flush(timeout=5)
            print(f"Kafka topic={KAFKA_TOPIC} sensor_count={record['sensor_count']}", flush=True)
        except Exception as exc:
            print(f"Failed to bridge MQTT message to Kafka: {exc}", file=sys.stderr, flush=True)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()

    try:
        while running:
            time.sleep(1)
    finally:
        mqtt_client.loop_stop()
        producer.flush(timeout=10)
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
