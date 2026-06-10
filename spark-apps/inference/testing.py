import os
import pickle
import sys
from pathlib import Path
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


INFERENCE_DIR = Path(__file__).resolve().parent
SPARK_APPS_DIR = INFERENCE_DIR.parent
if str(INFERENCE_DIR) not in sys.path:
    sys.path.insert(0, str(INFERENCE_DIR))
if str(SPARK_APPS_DIR) not in sys.path:
    sys.path.insert(0, str(SPARK_APPS_DIR))

from process.setupMinio import MinioConfig, MinioCrud
from transformation import SensorTransformer

MODEL_KEY = os.getenv("MODEL_KEY", "model_artifact/spark_rf_pipeline_model")
LABEL_MAPPING_KEY = os.getenv("LABEL_MAPPING_KEY", "model_artifact/label_mapping.pkl")
FEATURE_COLS_KEY = os.getenv("FEATURE_COLS_KEY", "model_artifact/feature_cols.pkl")
INPUT_PATH = os.getenv("INPUT_PATH", "data/custom_sensor.csv")
MIN_FEATURE_ROWS = 10


def configure_minio_s3(spark, config):
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


def create_spark():
    spark = (
        SparkSession.builder
        .appName("HAR-Inference-Testing")
        .getOrCreate()
    )
    return configure_minio_s3(spark, MinioConfig())


def load_artifacts():
    config = MinioConfig()
    minio = MinioCrud(config)

    model = PipelineModel.load(config.s3a_uri(MODEL_KEY))
    label_map = pickle.loads(minio.read_bytes(LABEL_MAPPING_KEY))
    feature_columns = pickle.loads(minio.read_bytes(FEATURE_COLS_KEY))

    print("Semua Spark artefak berhasil di-load")
    print(f"  Spark model : {config.s3_uri(MODEL_KEY)}")
    print(f"  Label map   : {minio.config.s3_uri(LABEL_MAPPING_KEY)}")
    print(f"  Feature cols: {minio.config.s3_uri(FEATURE_COLS_KEY)}")
    print(f"  Jumlah fitur : {len(feature_columns)}")
    print(f"  Jumlah kelas : {len(label_map['labels'])}")
    print()

    return model, label_map, feature_columns


def sensor_schema():
    return T.StructType([
        T.StructField(column, T.DoubleType(), nullable=True)
        for column in SensorTransformer.SENSOR_COLUMNS
    ])


def read_sensor_data(spark, input_path=INPUT_PATH):
    return (
        spark.read
        .option("header", "true")
        .schema(sensor_schema())
        .csv(input_path)
    )


def add_segment_id(df, total_segments=MIN_FEATURE_ROWS):
    window = Window.orderBy(F.monotonically_increasing_id())

    return (
        df
        .withColumn("row_number", F.row_number().over(window))
        .withColumn("segment_id", ((F.col("row_number") - 1) % total_segments) + 1)
        .drop("row_number")
    )


def make_feature_df(sensor_df, feature_columns):
    sensor_df = add_segment_id(sensor_df)
    transformer = SensorTransformer(feature_columns=feature_columns)
    feature_df = transformer.transform(sensor_df)

    total_rows = feature_df.count()
    if total_rows < MIN_FEATURE_ROWS:
        raise ValueError(
            f"feature_df harus minimal {MIN_FEATURE_ROWS} rows, "
            f"tapi hasil transform hanya {total_rows} rows."
        )

    return feature_df


def predict(model, feature_df, label_map):
    result = model.transform(feature_df)
    index_to_label = label_map["index_to_label"]

    mapping = []
    for index, label in index_to_label.items():
        mapping.extend([F.lit(float(index)), F.lit(label)])

    label_expr = F.create_map(*mapping)
    return result.withColumn("predicted_label", label_expr[F.col("prediction")])

def run(spark=None, sensor_df=None):
    spark = spark or create_spark()
    model, label_map, feature_columns = load_artifacts()

    if sensor_df is None:
        sensor_df = read_sensor_data(spark)

    feature_df = make_feature_df(sensor_df, feature_columns)
    result_df = predict(model, feature_df, label_map)

    print("=====RESULT=====")
    result_df.select("predicted_label", "prediction").show(truncate=False)
    return result_df


if __name__ == "__main__":
    run()
