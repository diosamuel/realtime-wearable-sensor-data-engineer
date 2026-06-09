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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt
from kafka import KafkaProducer

SPARK_APPS_DIR = Path(__file__).resolve().parents[1]
if str(SPARK_APPS_DIR) not in sys.path:
    sys.path.insert(0, str(SPARK_APPS_DIR))

from model.utils_spark import SENSOR_COLUMNS


MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "simulate/sensor")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wearable.sensor.raw")


@dataclass(frozen=True)
class BridgeConfig:
    mqtt_host: str = MQTT_HOST
    mqtt_port: int = MQTT_PORT
    mqtt_topic: str = MQTT_TOPIC
    kafka_bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS
    kafka_topic: str = KAFKA_TOPIC

    @classmethod
    def from_environment(cls):
        return cls(
            mqtt_host=os.getenv("MQTT_HOST", cls.mqtt_host),
            mqtt_port=int(os.getenv("MQTT_PORT", str(cls.mqtt_port))),
            mqtt_topic=os.getenv("MQTT_TOPIC", cls.mqtt_topic),
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                cls.kafka_bootstrap_servers,
            ),
            kafka_topic=os.getenv("KAFKA_TOPIC", cls.kafka_topic),
        )


class SensorPayloadParser:
    def __init__(self, sensor_columns: list[str]):
        self.sensor_columns = sensor_columns
        self.sensor_column_set = set(sensor_columns)

    def parse(self, payload: str):
        values: dict[str, float] = {}

        for item in payload.split(";"):
            if not item:
                continue

            column, separator, raw_value = item.partition("=")
            if not separator:
                raise ValueError(f"Invalid sensor item without '=': {item}")
            if column not in self.sensor_column_set:
                raise ValueError(f"Unknown sensor column: {column}")

            values[column] = float(raw_value)

        missing_columns = [column for column in self.sensor_columns if column not in values]
        if missing_columns:
            raise ValueError(f"Missing sensor columns: {', '.join(missing_columns)}")

        return values


class KafkaRecordBuilder:
    def __init__(self, parser: SensorPayloadParser):
        self.parser = parser

    def build(self, topic: str, payload: bytes):
        raw_payload = payload.decode("utf-8")

        return {
            "source": "mqtt",
            "mqtt_topic": topic,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "sensor_count": len(self.parser.sensor_columns),
            "values": self.parser.parse(raw_payload),
        }


class MqttToKafkaBridge:
    def __init__(self, config: BridgeConfig):
        self.config = config
        self.running = False
        self.record_builder = KafkaRecordBuilder(SensorPayloadParser(SENSOR_COLUMNS))
        self.producer = self._create_kafka_producer()
        self.mqtt_client = self._create_mqtt_client()

    def _create_kafka_producer(self):
        return create_kafka_producer(self.config)

    def _create_mqtt_client(self):
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        client.on_connect = self.on_connect
        client.on_message = self.on_message
        return client

    def start(self):
        self.running = True
        self.mqtt_client.connect(self.config.mqtt_host, self.config.mqtt_port, keepalive=60)
        self.mqtt_client.loop_start()

        try:
            while self.running:
                time.sleep(1)
        finally:
            self.close()

    def stop(self, _signum: int | None = None, _frame: Any | None = None):
        self.running = False
        self.mqtt_client.disconnect()

    def close(self):
        self.mqtt_client.loop_stop()
        self.producer.flush(timeout=10)
        self.producer.close(timeout=10)

    def on_connect(
        self,
        client: mqtt.Client,
        _userdata: Any,
        _flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        _properties: mqtt.Properties | None,
    ):
        if reason_code != 0:
            raise RuntimeError(f"MQTT connection failed with reason code {reason_code}")

        client.subscribe(self.config.mqtt_topic)
        print(
            f"Subscribed MQTT {self.config.mqtt_host}:{self.config.mqtt_port} "
            f"topic={self.config.mqtt_topic}",
            flush=True,
        )

    def on_message(
        self,
        _client: mqtt.Client,
        _userdata: Any,
        message: mqtt.MQTTMessage,
    ):
        try:
            self.publish_message(message)
        except Exception as exc:
            print(f"Failed to bridge MQTT message to Kafka: {exc}", file=sys.stderr, flush=True)

    def publish_message(self, message: mqtt.MQTTMessage):
        record = self.record_builder.build(message.topic, message.payload)
        self.producer.send(self.config.kafka_topic, key=message.topic, value=record)
        self.producer.flush(timeout=5)
        print(
            f"Kafka topic={self.config.kafka_topic} sensor_count={record['sensor_count']}",
            flush=True,
        )


def parse_sensor_payload(payload: str):
    return SensorPayloadParser(SENSOR_COLUMNS).parse(payload)


def build_kafka_record(topic: str, payload: bytes):
    return KafkaRecordBuilder(SensorPayloadParser(SENSOR_COLUMNS)).build(topic, payload)


def create_kafka_producer(config: BridgeConfig | None = None):
    producer_config = config or BridgeConfig.from_environment()
    return KafkaProducer(
        bootstrap_servers=producer_config.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        retries=5,
        linger_ms=10,
    )


def main():
    bridge = MqttToKafkaBridge(BridgeConfig.from_environment())

    signal.signal(signal.SIGINT, bridge.stop)
    signal.signal(signal.SIGTERM, bridge.stop)

    bridge.start()


if __name__ == "__main__":
    main()
