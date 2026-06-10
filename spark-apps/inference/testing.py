import os
import pickle
import sys
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


SPARK_APPS_DIR = Path(__file__).resolve().parents[1]
if str(SPARK_APPS_DIR) not in sys.path:
    sys.path.insert(0, str(SPARK_APPS_DIR))

from process.setupMinio import MinioConfig, MinioCrud
from transformation import SensorTransformer


MODEL_KEY = os.getenv("MODEL_KEY", "model_artifact/spark_rf_pipeline_model")
LABEL_MAPPING_KEY = os.getenv("LABEL_MAPPING_KEY", "model_artifact/label_mapping.pkl")
FEATURE_COLS_KEY = os.getenv("FEATURE_COLS_KEY", "model_artifact/feature_cols.pkl")
INPUT_PATH = os.getenv("INPUT_PATH", "data/custom_sensor.csv")


def create_spark():
    config = MinioConfig()
    spark = SparkSession.builder.appName("HAR-Inference-Testing").getOrCreate()

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", config.endpoint)
    hadoop_conf.set("fs.s3a.access.key", config.access_key)
    hadoop_conf.set("fs.s3a.secret.key", config.secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set(
        "fs.s3a.connection.ssl.enabled",
        "true" if config.endpoint.startswith("https://") else "false",
    )
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    return spark


def load_artifacts():
    config = MinioConfig()
    minio = MinioCrud(config)

    model = PipelineModel.load(config.s3a_uri(MODEL_KEY))
    label_map = pickle.loads(minio.read_bytes(LABEL_MAPPING_KEY))
    feature_columns = pickle.loads(minio.read_bytes(FEATURE_COLS_KEY))

    return model, label_map, feature_columns


def read_sensor_data(spark):
    schema = T.StructType([
        T.StructField(column, T.DoubleType(), nullable=True)
        for column in SensorTransformer.SENSOR_COLUMNS
    ])

    return (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(INPUT_PATH)
    )


def add_prediction_label(result_df, label_map):
    mapping = []
    for index, label in label_map["index_to_label"].items():
        mapping.extend([F.lit(float(index)), F.lit(label)])

    label_expr = F.create_map(*mapping)
    return result_df.withColumn("predicted_label", label_expr[F.col("prediction")])


def run():
    spark = create_spark()
    model, label_map, feature_columns = load_artifacts()

    sensor_df = read_sensor_data(spark)
    feature_df = SensorTransformer(feature_columns=feature_columns).transform(sensor_df)
    result_df = add_prediction_label(model.transform(feature_df), label_map)

    result_df.select("predicted_label", "prediction", "probability").show(truncate=False)
    return result_df


if __name__ == "__main__":
    run()
