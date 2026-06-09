from pyspark.sql.types import *
id_cols = [
    StructField("activity_id",IntegerType()),
    StructField("activity_label",StringType()),
    StructField("person_id",IntegerType()),
    StructField("segment_id",IntegerType()),
    StructField("activity_category",StringType())
]
feature_columns = []

positions = ["T", "RA", "LA", "RL", "LL"]
sensors = ["acc", "gyro", "mag"]
axes = ["x", "y", "z"]
stats = ["mean", "std", "min", "max", "skew", "kurtosis"]

# Setup feature columns (360 columns)
# x/y/z features
for pos in positions:
    for sensor in sensors:
        for axis in axes:
            for stat in stats:
                feature_columns.append(
                    f"{pos}_{axis}{sensor}_{stat}"
                )

# magnitude features
for pos in positions:
    for sensor in sensors:
        for stat in stats:
            feature_columns.append(
                f"{pos}_{sensor}_mag_{stat}"
            )

feature_cols = [
    StructField(c, DoubleType()) for c in feature_columns
]

SchemaOnWrite = StructType(
    id_cols + feature_cols
)
