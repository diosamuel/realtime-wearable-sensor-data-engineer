#!/usr/bin/env python3
"""Publish simulated wearable sensor readings to a local MQTT broker.

Example subscriber:
    mosquitto_sub -h localhost -t simulate/sensor
"""

from __future__ import annotations

import math
import os
import random
import time

import paho.mqtt.publish as mqtt_publish


MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "simulate/sensor")
PUBLISH_INTERVAL_SECONDS = float(os.getenv("PUBLISH_INTERVAL_SECONDS", "5"))

SENSOR_COLUMNS = [
    "T_xacc", "T_yacc", "T_zacc", "T_xgyro", "T_ygyro", "T_zgyro", "T_xmag", "T_ymag", "T_zmag",
    "RA_xacc", "RA_yacc", "RA_zacc", "RA_xgyro", "RA_ygyro", "RA_zgyro", "RA_xmag", "RA_ymag", "RA_zmag",
    "LA_xacc", "LA_yacc", "LA_zacc", "LA_xgyro", "LA_ygyro", "LA_zgyro", "LA_xmag", "LA_ymag", "LA_zmag",
    "RL_xacc", "RL_yacc", "RL_zacc", "RL_xgyro", "RL_ygyro", "RL_zgyro", "RL_xmag", "RL_ymag", "RL_zmag",
    "LL_xacc", "LL_yacc", "LL_zacc", "LL_xgyro", "LL_ygyro", "LL_zgyro", "LL_xmag", "LL_ymag", "LL_zmag",
]


def simulated_value(column: str, sample_index: int):
    """Generate values that roughly match accelerometer, gyro, and magnetometer ranges."""
    phase = sample_index / 12.0
    body_offset = (sum(ord(char) for char in column.split("_", 1)[0]) % 7) * 0.07

    if column.endswith("acc"):
        axis_bias = {"xacc": 0.0, "yacc": 0.8, "zacc": 8.7}[column.rsplit("_", 1)[1]]
        return axis_bias + math.sin(phase + body_offset) * 1.4 + random.uniform(-0.35, 0.35)

    if column.endswith("gyro"):
        return math.sin(phase * 1.7 + body_offset) * 0.08 + random.uniform(-0.015, 0.015)

    if column.endswith("mag"):
        return math.sin(phase * 0.8 + body_offset) * 6.5 + random.uniform(-1.5, 1.5)

    return random.uniform(-1.0, 1.0)


def build_payload(sample_index: int):
    return ";".join(f"{column}={simulated_value(column, sample_index):.6f}" for column in SENSOR_COLUMNS)


def publish(host: str, port: int, topic: str, payload: str):
    mqtt_publish.single(
        topic,
        payload=payload,
        hostname=host,
        port=port,
    )


def main():
    sample_index = 0

    while True:
        payload = build_payload(sample_index)
        try:
            publish(MQTT_HOST, MQTT_PORT, MQTT_TOPIC, payload)
            print(payload, flush=True)
            sample_index += 1
        except Exception as exc:
            print(f"Failed to publish simulated sensor payload: {exc}", flush=True)
        time.sleep(PUBLISH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
