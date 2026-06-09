#!/usr/bin/env python3
"""Read bridged wearable sensor messages from Kafka, run ML inference, and write to PostgreSQL.

Example:
    /opt/spark/bin/spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      --master spark://spark-master:7077 \
      /opt/spark-apps/process/kafka_sensor_stream.py
"""

from __future__ import annotations
import os
import pickle

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit, udf, create_map
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType
from pyspark.ml import PipelineModel

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wearable.sensor.raw")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/wearable-sensor-kafka")

SAVE_DIR = os.getenv('HAR_SAVE_DIR', '/opt/spark-data/HAR_SmartHealth/')
SPARK_MODEL_PATH = os.path.join(SAVE_DIR, 'spark_rf_pipeline_model')
LABEL_MAPPING_PATH = os.path.join(SAVE_DIR, 'label_mapping.pkl')
FEATURE_META_PATH = os.path.join(SAVE_DIR, 'feature_cols.pkl')

DB_URL = "jdbc:postgresql://postgres:5432/warehouse"
DB_USER = "admin"
DB_PASS = "password123"

@udf(returnType=DoubleType())
def extract_confidence(prob_vector):
    """Extracts the maximum probability score from MLlib's DenseVector."""
    if prob_vector is not None:
        return float(prob_vector.toArray().max())
    return 0.0

def write_to_postgres(df, epoch_id):
    """JDBC Sink to write inference outputs into PostgreSQL realtime_activity_monitor."""
    df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "realtime_activity_monitor") \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

def main():
    spark = (
        SparkSession.builder
        .appName("wearable-sensor-inference-stream")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. Audit check: Ensure Model Artifacts exist
    missing_artifacts = []
    if not os.path.exists(FEATURE_META_PATH): missing_artifacts.append(FEATURE_META_PATH)
    if not os.path.exists(LABEL_MAPPING_PATH): missing_artifacts.append(LABEL_MAPPING_PATH)
    if not os.path.exists(SPARK_MODEL_PATH): missing_artifacts.append(SPARK_MODEL_PATH)
    
    if missing_artifacts:
        raise FileNotFoundError(f"Model artifacts missing: {missing_artifacts}. Please run train.py first!")
        
    with open(FEATURE_META_PATH, 'rb') as f:
        feature_cols = pickle.load(f)
        
    with open(LABEL_MAPPING_PATH, 'rb') as f:
        label_map = pickle.load(f)

    # 2. Build Kafka JSON Schema (person_id + timestamp + 360 features)
    fields = [
        StructField("person_id", StringType(), nullable=False),
        StructField("sensor_event_time", TimestampType(), nullable=False)
    ]
    fields.extend([StructField(c, DoubleType(), nullable=True) for c in feature_cols])
    kafka_value_schema = StructType(fields)

    # 3. Load MLlib PipelineModel
    model = PipelineModel.load(SPARK_MODEL_PATH)

    # 4. Prepare Label and Category Mapping Expressions
    index_to_label_items = []
    for idx, label in label_map['index_to_label'].items():
        index_to_label_items.extend([lit(float(idx)), lit(label)])
    index_to_label_expr = create_map(*index_to_label_items)

    ACTIVITY_CATEGORIES = {
        'sitting': 'sedentary', 'standing': 'sedentary', 'lying_back': 'sedentary', 
        'lying_right': 'sedentary', 'standing_elevator': 'sedentary',
        'moving_elevator': 'light', 'walking_treadmill': 'light', 
        'descending_stairs': 'light', 'ascending_stairs': 'intense', 
        'running_treadmill': 'intense'
    }
    category_items = []
    for label, category in ACTIVITY_CATEGORIES.items():
        category_items.extend([lit(label), lit(category)])
    category_expr = create_map(*category_items)

    # 5. Read Stream
    raw_messages = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_stream = raw_messages.select(
        from_json(col("value").cast("string"), kafka_value_schema).alias("record")
    ).select("record.*")

    # 6. Transform / Predict
    predictions = model.transform(parsed_stream)

    # 7. Extract Columns and Calculate Latency
    final_df = predictions.withColumn("predicted_activity_label", index_to_label_expr[col("prediction")]) \
        .withColumn("activity_category", category_expr[col("predicted_activity_label")]) \
        .withColumn("confidence_score", extract_confidence(col("probability"))) \
        .withColumn("predicted_at", current_timestamp()) \
        .withColumn("prediction_latency_sec", col("predicted_at").cast("double") - col("sensor_event_time").cast("double")) \
        .withColumn("sedentary_start_time", lit(None).cast(TimestampType())) \
        .withColumn("sedentary_streak_sec", lit(0).cast(DoubleType())) \
        .withColumn("sedentary_streak_min", lit(0).cast(DoubleType())) \
        .withColumn("is_alert", lit(False))
        
    output_df = final_df.select(
        "person_id", "sensor_event_time", "predicted_at", 
        "predicted_activity_label", "activity_category", "confidence_score",
        "sedentary_start_time", "sedentary_streak_sec", "sedentary_streak_min",
        "is_alert", "prediction_latency_sec"
    )

    # 8. Write to PostgreSQL using foreachBatch
    query = (
        output_df.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()