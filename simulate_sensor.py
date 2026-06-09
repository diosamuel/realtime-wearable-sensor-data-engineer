#!/usr/bin/env python3
"""Publish simulated wearable sensor readings to a local MQTT broker or Kafka.

Example publisher (Kafka):
    PUBLISH_TARGET=kafka KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python simulate_sensor.py
"""

from __future__ import annotations

import json
import math
import os
import random
import shutil
import subprocess
import time
from datetime import datetime, timezone

PUBLISH_TARGET = os.getenv("PUBLISH_TARGET", "mqtt")
MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "simulate/sensor"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wearable.sensor.raw")
PUBLISH_INTERVAL_SECONDS = 1

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
    payload = {
        "person_id": "P1",
        # Use Spark's default timestamp format: yyyy-MM-dd HH:mm:ss.SSS
        "sensor_event_time": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    }
    
    # 1. Base 45 sensor columns
    base_vals = {}
    for column in SENSOR_COLUMNS:
        val = round(simulated_value(column, sample_index), 6)
        base_vals[column] = val
        payload[column] = val

    # 2. Add 15 magnitude columns
    body_parts = {
        'T': ('T_xacc', 'T_yacc', 'T_zacc', 'T_xgyro', 'T_ygyro', 'T_zgyro', 'T_xmag', 'T_ymag', 'T_zmag'),
        'RA': ('RA_xacc', 'RA_yacc', 'RA_zacc', 'RA_xgyro', 'RA_ygyro', 'RA_zgyro', 'RA_xmag', 'RA_ymag', 'RA_zmag'),
        'LA': ('LA_xacc', 'LA_yacc', 'LA_zacc', 'LA_xgyro', 'LA_ygyro', 'LA_zgyro', 'LA_xmag', 'LA_ymag', 'LA_zmag'),
        'RL': ('RL_xacc', 'RL_yacc', 'RL_zacc', 'RL_xgyro', 'RL_ygyro', 'RL_zgyro', 'RL_xmag', 'RL_ymag', 'RL_zmag'),
        'LL': ('LL_xacc', 'LL_yacc', 'LL_zacc', 'LL_xgyro', 'LL_ygyro', 'LL_zgyro', 'LL_xmag', 'LL_ymag', 'LL_zmag'),
    }
    
    mag_vals = {}
    for part, cols in body_parts.items():
        xacc, yacc, zacc, xgyro, ygyro, zgyro, xmag, ymag, zmag = cols
        mag_vals[f'{part}_acc_mag'] = math.sqrt(base_vals[xacc]**2 + base_vals[yacc]**2 + base_vals[zacc]**2)
        mag_vals[f'{part}_gyro_mag'] = math.sqrt(base_vals[xgyro]**2 + base_vals[ygyro]**2 + base_vals[zgyro]**2)
        mag_vals[f'{part}_mag_mag'] = math.sqrt(base_vals[xmag]**2 + base_vals[ymag]**2 + base_vals[zmag]**2)
        
    all_vals = {**base_vals, **mag_vals}
    
    # 3. Add 360 statistical features expected by the Random Forest Model
    for col, val in all_vals.items():
        payload[f"{col}_mean"] = round(val, 6)
        payload[f"{col}_std"] = round(abs(val * 0.1), 6)
        payload[f"{col}_min"] = round(val - abs(val * 0.1), 6)
        payload[f"{col}_max"] = round(val + abs(val * 0.1), 6)
        payload[f"{col}_skew"] = 0.0
        payload[f"{col}_kurtosis"] = 0.0

    return json.dumps(payload)


def publish_mqtt(host: str, port: int, topic: str, payload: str):
    if shutil.which("mosquitto_pub") is None:
        raise RuntimeError("mosquitto_pub is not installed or is not available on PATH")

    subprocess.run(
        ["mosquitto_pub", "-h", host, "-p", str(port), "-t", topic, "-m", payload],
        check=True,
    )


def main():
    sample_index = 0
    producer = None

    if PUBLISH_TARGET == "kafka":
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        print(f"Publishing to Kafka: {KAFKA_BOOTSTRAP_SERVERS} -> {KAFKA_TOPIC}")
    else:
        print(f"Publishing to MQTT: {MQTT_HOST}:{MQTT_PORT} -> {MQTT_TOPIC}")

    while True:
        payload = build_payload(sample_index)
        
        if PUBLISH_TARGET == "kafka":
            producer.send(KAFKA_TOPIC, payload.encode('utf-8'))
            producer.flush()
        else:
            publish_mqtt(MQTT_HOST, MQTT_PORT, MQTT_TOPIC, payload)
            
        print(payload, flush=True)
        sample_index += 1
        time.sleep(PUBLISH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()