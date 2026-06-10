#!/usr/bin/env python3

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


SPARK_APPS_DIR = Path(__file__).resolve().parents[1]
if str(SPARK_APPS_DIR) not in sys.path:
    sys.path.insert(0, str(SPARK_APPS_DIR))

from model.utilsSpark import SENSOR_COLUMNS


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wearable.sensor.raw")
CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "/opt/spark-data/checkpoints/consumeSpark",
)


def sensor_schema():
    return StructType([
        StructField(column, DoubleType(), nullable=False)
        for column in SENSOR_COLUMNS
    ])


def message_schema():
    return StructType([
        StructField("source", StringType(), nullable=False),
        StructField("mqtt_topic", StringType(), nullable=False),
        StructField("ingested_at", StringType(), nullable=False),
        StructField("sensor_count", IntegerType(), nullable=False),
        StructField("values", sensor_schema(), nullable=False),
    ])


def main():
    spark = (
        SparkSession.builder
        .appName("consumeSpark")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("key").cast("string").alias("message_key"),
            from_json(col("value").cast("string"), message_schema()).alias("data"),
        )
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("message_key"),
            col("data.source").alias("source"),
            col("data.mqtt_topic").alias("mqtt_topic"),
            col("data.ingested_at").alias("ingested_at"),
            col("data.sensor_count").alias("sensor_count"),
            col("data.values").alias("sensor_values"),
        )
    )

    query = (
        parsed_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 20)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
