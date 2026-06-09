#!/usr/bin/env python3
"""Read bridged wearable sensor messages from Kafka with Spark Structured Streaming.

Example:
    /opt/spark/bin/spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      --master spark://spark-master:7077 \
      /opt/spark-apps/kafka_sensor_stream.py
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from utils import SENSOR_COLUMNS


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wearable.sensor.raw")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/wearable-sensor-kafka")


def sensor_schema():
    return StructType([StructField(column, DoubleType(), nullable=False) for column in SENSOR_COLUMNS])


def kafka_value_schema():
    return StructType(
        [
            StructField("source", StringType(), nullable=False),
            StructField("mqtt_topic", StringType(), nullable=False),
            StructField("ingested_at", StringType(), nullable=False),
            StructField("sensor_count", IntegerType(), nullable=False),
            StructField("values", sensor_schema(), nullable=False),
        ]
    )


def main():
    spark = (
        SparkSession.builder
        .appName("wearable-sensor-kafka-stream")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_messages = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_messages = raw_messages.select(
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), kafka_value_schema()).alias("record"),
    )

    sensor_rows = parsed_messages.select(
        col("kafka_timestamp"),
        col("record.source"),
        col("record.mqtt_topic"),
        col("record.ingested_at"),
        *[col(f"record.values.{column}").alias(column) for column in SENSOR_COLUMNS],
    )

    query = (
        sensor_rows.writeStream
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
