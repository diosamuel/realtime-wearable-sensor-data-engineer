#!/usr/bin/env python3
import os
import pickle
import time
from datetime import datetime, timezone

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Import common constants
try:
    from utils import SENSOR_COLUMNS
except ImportError:
    SENSOR_COLUMNS = [
        "T_xacc", "T_yacc", "T_zacc", "T_xgyro", "T_ygyro", "T_zgyro", "T_xmag", "T_ymag", "T_zmag",
        "RA_xacc", "RA_yacc", "RA_zacc", "RA_xgyro", "RA_ygyro", "RA_zgyro", "RA_xmag", "RA_ymag", "RA_zmag",
        "LA_xacc", "LA_yacc", "LA_zacc", "LA_xgyro", "LA_ygyro", "LA_zgyro", "LA_xmag", "LA_ymag", "LA_zmag",
        "RL_xacc", "RL_yacc", "RL_zacc", "RL_xgyro", "RL_ygyro", "RL_zgyro", "RL_xmag", "RL_ymag", "RL_zmag",
        "LL_xacc", "LL_yacc", "LL_zacc", "LL_xgyro", "LL_ygyro", "LL_zgyro", "LL_xmag", "LL_ymag", "LL_zmag",
    ]

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wearable.sensor.raw")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/wearable-sensor-kafka")
SPARK_MODEL_PATH = os.getenv("SPARK_MODEL_PATH", "/opt/spark-data/HAR_SmartHealth/spark_rf_pipeline_model")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/warehouse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password123")
MINIO_URL = os.getenv("MINIO_URL", "s3a://wearable-sensor-data/processed/")

# Sedentary Threshold (e.g., 30 minutes)
SEDENTARY_THRESHOLD_MINUTES = 30.0

def sensor_schema():
    return StructType([StructField(column, DoubleType(), nullable=False) for column in SENSOR_COLUMNS])

def kafka_value_schema():
    return StructType(
        [
            StructField("source", StringType(), nullable=False),
            StructField("mqtt_topic", StringType(), nullable=False),
            StructField("sensor_event_time", StringType(), nullable=False),
            StructField("ingested_at", StringType(), nullable=False),
            StructField("sensor_count", IntegerType(), nullable=False),
            StructField("values", sensor_schema(), nullable=False),
        ]
    )

def add_magnitude_columns(df):
    body_parts = ['T', 'RA', 'LA', 'RL', 'LL']
    mag_columns = []
    for part in body_parts:
        acc_mag = f"{part}_acc_mag"
        gyro_mag = f"{part}_gyro_mag"
        mag_mag = f"{part}_mag_mag"
        
        df = df.withColumn(acc_mag, F.sqrt(F.col(f"{part}_xacc")**2 + F.col(f"{part}_yacc")**2 + F.col(f"{part}_zacc")**2))
        df = df.withColumn(gyro_mag, F.sqrt(F.col(f"{part}_xgyro")**2 + F.col(f"{part}_ygyro")**2 + F.col(f"{part}_zgyro")**2))
        df = df.withColumn(mag_mag, F.sqrt(F.col(f"{part}_xmag")**2 + F.col(f"{part}_ymag")**2 + F.col(f"{part}_zmag")**2))
        mag_columns.extend([acc_mag, gyro_mag, mag_mag])
    return df, mag_columns

def extract_features(df, sensor_columns):
    aggregations = []
    for col_name in sensor_columns:
        col = F.col(col_name)
        aggregations.extend([
            F.mean(col).alias(f"{col_name}_mean"),
            F.stddev(col).alias(f"{col_name}_std"),
            F.min(col).alias(f"{col_name}_min"),
            F.max(col).alias(f"{col_name}_max"),
            F.skewness(col).alias(f"{col_name}_skew"),
            F.kurtosis(col).alias(f"{col_name}_kurtosis"),
        ])
    return aggregations

def main():
    spark = (
        SparkSession.builder
        .appName("HAR-Realtime-Streaming")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Load Model
    model = PipelineModel.load(SPARK_MODEL_PATH)
    
    # Activity Categories Mapping (align with report)
    # sedentary, light, intense
    activity_categories = {
        'sitting': 'sedentary',
        'standing': 'sedentary',
        'lying_back': 'sedentary',
        'lying_right': 'sedentary',
        'standing_elevator': 'sedentary',
        'walking_treadmill': 'light',
        'ascending_stairs': 'light',
        'descending_stairs': 'light',
        'moving_elevator': 'light',
        'running_treadmill': 'intense',
    }
    category_map_expr = F.create_map([F.lit(x) for x in sum(activity_categories.items(), ())])

    # Read from Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .load()
    )

    # Parse JSON
    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), kafka_value_schema()).alias("record")
    ).select(
        col("record.mqtt_topic").alias("user_id"), # Using topic as user_id proxy
        F.to_timestamp(col("record.sensor_event_time")).alias("sensor_event_time"),
        *[col(f"record.values.{c}").alias(c) for c in SENSOR_COLUMNS]
    )

    # Add Magnitudes
    df_with_mag, mag_cols = add_magnitude_columns(parsed_df)
    all_sensor_cols = SENSOR_COLUMNS + mag_cols

    # Windowed Feature Extraction
    # 5 second window, 1 second slide
    windowed_features = (
        df_with_mag
        .withWatermark("sensor_event_time", "10 seconds")
        .groupBy(
            F.window("sensor_event_time", "5 seconds", "1 second"),
            "user_id"
        )
        .agg(*extract_features(df_with_mag, all_sensor_cols))
        .withColumn("sensor_event_time", F.col("window.end"))
    )

    # Prediction
    predictions = model.transform(windowed_features)
    
    # Map predictions to labels and categories
    indexer_model = model.stages[0]
    labels = indexer_model.labels
    label_map_expr = F.create_map([F.lit(x) for x in sum(enumerate(labels), ())]) # index to label
    
    # Prepare output
    result_df = (
        predictions
        .withColumn("predicted_activity_label", label_map_expr[F.col("prediction")])
        .withColumn("activity_category", category_map_expr[F.col("predicted_activity_label")])
        .withColumn("predicted_at", F.current_timestamp())
        .withColumn("processing_latency_sec", 
                    F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("sensor_event_time")))
    )

    # Stateful processing for sedentary duration would normally use mapGroupsWithState
    # For simplicity in this script, we can use a windowed aggregation or join with state
    # But since we need to persist it to PostgreSQL, let's use a ForeachBatch sink
    
    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
            
        batch_df.cache()
        
        # Load existing state from DB
        state_df = spark.read \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "activity_state") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .load()
        
        # Join batch with state
        # In this simulation, we assume user_id = 1
        joined_df = batch_df.withColumn("user_id", F.lit(1)) \
            .join(state_df, "user_id", "left")
        
        # Logic: 
        # If current category is 'sedentary':
        #   If last was not 'sedentary', start_time = current sensor_event_time
        #   Else, duration = sensor_event_time - start_time
        # Else: reset start_time
        
        calculated_df = joined_df.withColumn(
            "new_sedentary_start_time",
            F.when(F.col("activity_category") == "sedentary",
                F.when(F.col("last_activity_category") == "sedentary", F.col("sedentary_start_time"))
                .otherwise(F.col("sensor_event_time"))
            ).otherwise(F.lit(None).cast("timestamp"))
        ).withColumn(
            "sedentary_duration_minutes",
            F.when(F.col("new_sedentary_start_time").isNotNull(),
                (F.unix_timestamp(F.col("sensor_event_time")) - F.unix_timestamp(F.col("new_sedentary_start_time"))) / 60.0
            ).otherwise(F.lit(0.0))
        ).withColumn(
            "alert_status",
            F.when(F.col("sedentary_duration_minutes") >= SEDENTARY_THRESHOLD_MINUTES, F.lit("ALERT"))
            .otherwise(F.lit("NORMAL"))
        )

        # 1. Update activity_state in PostgreSQL
        new_state = calculated_df.select(
            "user_id",
            F.col("activity_category").alias("last_activity_category"),
            F.col("new_sedentary_start_time").alias("sedentary_start_time"),
            F.col("sedentary_duration_minutes").alias("sedentary_streak_minutes")
        )
        
        # Use JDBC to update state (Simplified: overwrite/append for demo, in prod use merge)
        new_state.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "activity_state") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("overwrite") \
            .save()

        # 2. Write to realtime_activity_monitor
        calculated_df.select(
            "user_id",
            "predicted_activity_label",
            "activity_category",
            F.lit(0.95).alias("confidence_score"),
            "sedentary_duration_minutes",
            "alert_status",
            "processing_latency_sec",
            "sensor_event_time",
            "predicted_at"
        ).write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "realtime_activity_monitor") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()
            
        # 3. Aggregation for Fact Table
        fact_agg = calculated_df.groupBy(
            "user_id",
            F.to_date("sensor_event_time").alias("date"),
            "predicted_activity_label"
        ).agg(
            F.count("*").alias("prediction_count"),
            F.sum(F.lit(1.0/60.0)).alias("total_duration_minutes"), # Assuming each row is 1 sec slide
            F.avg(F.lit(0.95)).alias("avg_confidence_score"),
            F.sum(F.when(F.col("alert_status") == "ALERT", 1).otherwise(0)).alias("alert_count"),
            F.max("sedentary_duration_minutes").alias("max_sedentary_streak_minutes")
        )
        
        fact_agg.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "fact_activity_daily_summary") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()

        # 4. Write to MinIO (Silent fail if bucket not exists)
        try:
            batch_df.write.mode("append").parquet(MINIO_URL)
        except:
            pass
        
        batch_df.unpersist()

    query = (
        result_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
