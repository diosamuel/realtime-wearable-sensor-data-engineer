#!/usr/bin/env python3

import os
import pickle
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pandas as pd
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    from pyspark.ml.functions import vector_to_array
except ImportError:
    vector_to_array = None


ROOT_DIR = Path(__file__).resolve().parent
SERVING_PATH = ROOT_DIR / "spark-apps" / "process" / "serving.py"

SPARK_MODEL_DIR = Path(
    os.getenv(
        "SPARK_MODEL_DIR",
        str(ROOT_DIR / "outputs" / "spark_rf_pipeline_model"),
    )
)
MODEL_DIR = Path(os.getenv("MODEL_DIR", str(ROOT_DIR / "outputs" / "model")))
LABEL_MAPPING_PATH = MODEL_DIR / "label_mapping.pkl"
FEATURE_COLS_PATH = MODEL_DIR / "feature_cols.pkl"

SENSOR_COLUMNS = [
    "T_xacc", "T_yacc", "T_zacc", "T_xgyro", "T_ygyro", "T_zgyro", "T_xmag", "T_ymag", "T_zmag",
    "RA_xacc", "RA_yacc", "RA_zacc", "RA_xgyro", "RA_ygyro", "RA_zgyro", "RA_xmag", "RA_ymag", "RA_zmag",
    "LA_xacc", "LA_yacc", "LA_zacc", "LA_xgyro", "LA_ygyro", "LA_zgyro", "LA_xmag", "LA_ymag", "LA_zmag",
    "RL_xacc", "RL_yacc", "RL_zacc", "RL_xgyro", "RL_ygyro", "RL_zgyro", "RL_xmag", "RL_ymag", "RL_zmag",
    "LL_xacc", "LL_yacc", "LL_zacc", "LL_xgyro", "LL_ygyro", "LL_zgyro", "LL_xmag", "LL_ymag", "LL_zmag",
]

BODY_PARTS = {
    "T": ("T_xacc", "T_yacc", "T_zacc", "T_xgyro", "T_ygyro", "T_zgyro", "T_xmag", "T_ymag", "T_zmag"),
    "RA": ("RA_xacc", "RA_yacc", "RA_zacc", "RA_xgyro", "RA_ygyro", "RA_zgyro", "RA_xmag", "RA_ymag", "RA_zmag"),
    "LA": ("LA_xacc", "LA_yacc", "LA_zacc", "LA_xgyro", "LA_ygyro", "LA_zgyro", "LA_xmag", "LA_ymag", "LA_zmag"),
    "RL": ("RL_xacc", "RL_yacc", "RL_zacc", "RL_xgyro", "RL_ygyro", "RL_zgyro", "RL_xmag", "RL_ymag", "RL_zmag"),
    "LL": ("LL_xacc", "LL_yacc", "LL_zacc", "LL_xgyro", "LL_ygyro", "LL_zgyro", "LL_xmag", "LL_ymag", "LL_zmag"),
}


def resolve_path(path):
    path = Path(path)
    return path if path.is_absolute() else ROOT_DIR / path


def require_path(path, description):
    path = resolve_path(path)
    if not path.exists():
        raise FileNotFoundError(f"{description} not found: {path}")
    return path


def load_pickle(path):
    path = require_path(path, "Pickle artifact")
    with path.open("rb") as file:
        return pickle.load(file)


def create_spark():
    builder = SparkSession.builder.appName("HAR-PySpark-Index-Inference")
    spark_master = os.getenv("SPARK_MASTER")
    if spark_master:
        builder = builder.master(spark_master)
    return builder.getOrCreate()


def load_spark_model():
    model_dir = require_path(SPARK_MODEL_DIR, "Spark PipelineModel")
    full_model = PipelineModel.load(str(model_dir))
    stages = list(full_model.stages)

    if stages and getattr(stages[0], "getInputCol", lambda: None)() == "activity_label":
        stages = stages[1:]

    model = PipelineModel(stages=stages)
    print(f"Spark model loaded: {model_dir}")
    print(f"Inference stages  : {[stage.__class__.__name__ for stage in stages]}")
    return model


def load_model_artifacts():
    model = load_spark_model()
    label_map = load_pickle(LABEL_MAPPING_PATH)
    feature_columns = load_pickle(FEATURE_COLS_PATH)

    if "index_to_label" not in label_map:
        raise ValueError("label_mapping.pkl must contain an 'index_to_label' mapping")

    print(f"Model metadata loaded: {resolve_path(MODEL_DIR)}")
    print(f"Feature columns      : {len(feature_columns)}")
    print(f"Classes              : {len(label_map.get('labels', []))}")
    return model, label_map, feature_columns


def load_serving_module():
    spec = spec_from_file_location("serving", SERVING_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load serving module from {SERVING_PATH}")

    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def capture_sensor_data(serving, conn):
    return conn.execute(
        f"""
        SELECT ingested_at, unnest(sensor_values)
        FROM read_parquet(
            {serving.sql_quote(serving.S3_PARQUET_PATH)},
            hive_partitioning = true,
            union_by_name = true
        )
        ORDER BY ingested_at DESC
        LIMIT {serving.LIMIT}
        """
    ).fetchdf()


def latest_sensor_data():
    serving = load_serving_module()
    conn = serving.main()

    try:
        return capture_sensor_data(serving, conn)
    finally:
        conn.close()


def prepare_sensor_data(sensor_data):
    if not isinstance(sensor_data, pd.DataFrame):
        raise TypeError("sensor_data must be a pandas.DataFrame")
    if sensor_data.empty:
        raise ValueError("No sensor rows were provided for inference.")

    missing_columns = [column for column in SENSOR_COLUMNS if column not in sensor_data.columns]
    if missing_columns:
        raise ValueError(f"Missing sensor columns: {missing_columns}")

    prepared = sensor_data.copy()
    if "segment_id" not in prepared.columns:
        prepared["segment_id"] = 1

    prepared["segment_id"] = pd.to_numeric(prepared["segment_id"], errors="coerce").fillna(1).astype("int64")
    prepared[SENSOR_COLUMNS] = prepared[SENSOR_COLUMNS].apply(pd.to_numeric, errors="raise")
    return prepared[["segment_id", *SENSOR_COLUMNS]]


def sensor_schema():
    return T.StructType([
        T.StructField("segment_id", T.LongType(), nullable=False),
        *[
            T.StructField(column, T.DoubleType(), nullable=True)
            for column in SENSOR_COLUMNS
        ],
    ])


def read_sensor_data(spark, sensor_data):
    prepared = prepare_sensor_data(sensor_data)
    return spark.createDataFrame(
        prepared.to_dict("records"),
        schema=sensor_schema(),
    )


def add_magnitude_columns(df):
    magnitude_columns = []

    for part, columns in BODY_PARTS.items():
        xacc, yacc, zacc, xgyro, ygyro, zgyro, xmag, ymag, zmag = columns

        acc_mag = f"{part}_acc_mag"
        gyro_mag = f"{part}_gyro_mag"
        mag_mag = f"{part}_mag_mag"

        df = (
            df
            .withColumn(acc_mag, F.sqrt(F.col(xacc) ** 2 + F.col(yacc) ** 2 + F.col(zacc) ** 2))
            .withColumn(gyro_mag, F.sqrt(F.col(xgyro) ** 2 + F.col(ygyro) ** 2 + F.col(zgyro) ** 2))
            .withColumn(mag_mag, F.sqrt(F.col(xmag) ** 2 + F.col(ymag) ** 2 + F.col(zmag) ** 2))
        )
        magnitude_columns.extend([acc_mag, gyro_mag, mag_mag])

    return df, magnitude_columns


def feature_formulas(columns):
    formulas = []

    for column in columns:
        value = F.col(column)
        formulas.extend([
            F.mean(value).alias(f"{column}_mean"),
            F.stddev(value).alias(f"{column}_std"),
            F.min(value).alias(f"{column}_min"),
            F.max(value).alias(f"{column}_max"),
            F.skewness(value).alias(f"{column}_skew"),
            F.kurtosis(value).alias(f"{column}_kurtosis"),
        ])

    return formulas


def build_feature_frame(sensor_df, feature_columns):
    sensor_df, magnitude_columns = add_magnitude_columns(sensor_df)
    all_columns = SENSOR_COLUMNS + magnitude_columns

    feature_df = sensor_df.groupBy("segment_id").agg(*feature_formulas(all_columns))
    missing_features = [column for column in feature_columns if column not in feature_df.columns]
    if missing_features:
        raise ValueError(f"Missing extracted features: {missing_features}")

    return feature_df.select(
        "segment_id",
        *[
            F.coalesce(F.col(column).cast("double"), F.lit(0.0)).alias(column)
            for column in feature_columns
        ],
    )


def add_prediction_label(result_df, label_map):
    mapping = []
    for index, label in label_map["index_to_label"].items():
        mapping.extend([F.lit(float(index)), F.lit(label)])

    label_expr = F.create_map(*mapping)
    return result_df.withColumn(
        "predicted_label",
        label_expr[F.col("prediction").cast("double")],
    )


def add_confidence(result_df):
    if vector_to_array is not None:
        return result_df.withColumn("confidence", F.array_max(vector_to_array("probability")))

    confidence_udf = F.udf(lambda probability: float(max(probability)) if probability is not None else None, T.DoubleType())
    return result_df.withColumn("confidence", confidence_udf(F.col("probability")))


def predict(sensor_data, spark=None, model=None, label_map=None, feature_columns=None):
    spark = spark or create_spark()
    if model is None or label_map is None or feature_columns is None:
        model, label_map, feature_columns = load_model_artifacts()

    sensor_df = read_sensor_data(spark, sensor_data)
    feature_df = build_feature_frame(sensor_df, feature_columns)
    result_df = add_confidence(add_prediction_label(model.transform(feature_df), label_map))

    return result_df.select(
        "segment_id",
        "predicted_label",
        "prediction",
        "confidence",
        "probability",
    ).orderBy("segment_id")


def run(sensor_data=None, spark=None):
    spark = spark or create_spark()
    model, label_map, feature_columns = load_model_artifacts()
    sensor_data = latest_sensor_data() if sensor_data is None else sensor_data
    return predict(sensor_data, spark, model, label_map, feature_columns)


def main():
    spark = create_spark()
    try:
        result_df = run(spark=spark)
        result_pdf = result_df.toPandas()

        print("=" * 65)
        print("HASIL INFERENSI SENSOR TERBARU")
        print("=" * 65)
        print(result_pdf.to_string(index=False))
        return result_pdf
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
