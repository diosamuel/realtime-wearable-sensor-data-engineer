#!/usr/bin/env python3

import os
import sys
from functools import reduce
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, lit, sum as spark_sum
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


SPARK_APPS_DIR = Path(__file__).resolve().parents[1]
if str(SPARK_APPS_DIR) not in sys.path:
    sys.path.insert(0, str(SPARK_APPS_DIR))

from model.utils_spark import SENSOR_COLUMNS


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wearable.sensor.raw")
CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "/opt/spark-data/checkpoints/consume-spark",
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
        .appName("consume-spark")
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

    sensor_df = (
        kafka_df
        .select(
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), message_schema()).alias("data"),
        )
        .select(
            col("kafka_timestamp"),
            col("data.source"),
            col("data.mqtt_topic"),
            col("data.ingested_at"),
            col("data.sensor_count"),
            *[col(f"data.values.{column}").alias(column) for column in SENSOR_COLUMNS],
        )
    )

    total_sensor_value = reduce(
        lambda left, right: left + right,
        [col(column) for column in SENSOR_COLUMNS],
        lit(0.0),
    )

    sum_df = (
        sensor_df
        .withColumn("total_sensor_value", total_sensor_value)
        .agg(
            count("*").alias("received_rows"),
            spark_sum("total_sensor_value").alias("sum_all_sensor_values"),
            *[
                spark_sum(column).alias(f"{column}_sum")
                for column in SENSOR_COLUMNS
            ],
        )
    )

    query = (
        sum_df.writeStream
        .format("console")
        .outputMode("complete")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
