import pickle

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils_spark import (
    FEATURE_META_PATH,
    LABEL_MAPPING_PATH,
    SAVE_DIR,
    SPARK_MODEL_PATH,
    TEST_PARQUET_PATH,
)


spark = (
    SparkSession.builder
    .appName('HAR-RandomForest-SparkMLlib-Inference')
    .config('spark.driver.memory', '4g')
    .getOrCreate()
)

loaded_spark_model = PipelineModel.load(SPARK_MODEL_PATH)

with open(FEATURE_META_PATH, 'rb') as f:
    loaded_feature_cols = pickle.load(f)

with open(LABEL_MAPPING_PATH, 'rb') as f:
    loaded_label_map = pickle.load(f)

test_df = spark.read.parquet(TEST_PARQUET_PATH)
dummy_samples = (
    test_df
    .select(*loaded_feature_cols, 'activity_label')
    .orderBy(F.rand(seed=99))
    .limit(5)
)

result = loaded_spark_model.transform(dummy_samples)

index_to_label_items = []
for idx, label in loaded_label_map['index_to_label'].items():
    index_to_label_items.extend([F.lit(float(idx)), F.lit(label)])

index_to_label_expr = F.create_map(*index_to_label_items)

result_with_labels = (
    result
    .withColumn('predicted_label', index_to_label_expr[F.col('prediction')])
    .withColumn(
        'status',
        F.when(F.col('activity_label') == F.col('predicted_label'), F.lit('BENAR'))
        .otherwise(F.lit('SALAH'))
    )
)

print('DEMO HASIL INFERENSI (5 sampel dari test parquet)')
result_with_labels.select('activity_label', 'predicted_label', 'status').show(truncate=False)

correct = result_with_labels.filter(F.col('status') == 'BENAR').count()
print(f'Hasil: {correct}/5 prediksi benar')

print('Model artifacts:')
print(f'  {SPARK_MODEL_PATH}  -> Spark PipelineModel (native)')
print(f'  {FEATURE_META_PATH}  -> daftar {len(loaded_feature_cols)} fitur')
print(f'  {TEST_PARQUET_PATH}  -> sumber sample inference')
print(f'Semua artefak tersimpan di: {SAVE_DIR}')


