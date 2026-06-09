import os
import pickle
import time as _time

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler as SparkScaler
from pyspark.ml.classification import RandomForestClassifier as SparkRF
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline as SparkPipeline
from utils_spark import (
    FEATURE_META_PATH,
    LABEL_MAPPING_PATH,
    N_ESTIMATORS,
    SAVE_DIR,
    SPARK_MODEL_PATH,
    prepareFeatureDatasets,
)

spark = SparkSession.builder \
    .appName('HAR-RandomForest-SparkMLlib') \
    .config('spark.driver.memory', '4g') \
    .getOrCreate()

print(f'Initiated Spark version: {spark.version}')

prepared = prepareFeatureDatasets(spark, write_parquet=True)
feature_cols = prepared['feature_cols']
spark_train = spark.read.parquet(prepared['train_path'])
spark_test = spark.read.parquet(prepared['test_path'])

print(f"Train parquet: {prepared['train_path']} ({spark_train.count()} rows)")
print(f"Test parquet : {prepared['test_path']} ({spark_test.count()} rows)")

indexer = StringIndexer(
    inputCol='activity_label',
    outputCol='label',
    handleInvalid='error'
)

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol='raw_features'
)

spark_scaler = SparkScaler(
    inputCol='raw_features',
    outputCol='features',
    withMean=True,
    withStd=True
)

spark_rf = SparkRF(
    featuresCol='features',
    labelCol='label',
    numTrees=N_ESTIMATORS,
    maxDepth=20,
    minInstancesPerNode=1,
    featureSubsetStrategy='sqrt',
    seed=42
)

spark_pipeline = SparkPipeline(stages=[indexer, assembler, spark_scaler, spark_rf])
t0_spark = _time.time()
spark_model = spark_pipeline.fit(spark_train)
spark_train_time = _time.time() - t0_spark
print(f'Training selesai dalam {spark_train_time:.1f} detik!')

predictions = spark_model.transform(spark_test)
evaluator_f1 = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='f1')
evaluator_acc = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
evaluator_prec = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='weightedPrecision')
evaluator_rec = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='weightedRecall')

f1_spark   = evaluator_f1.evaluate(predictions)
acc_spark  = evaluator_acc.evaluate(predictions)
prec_spark = evaluator_prec.evaluate(predictions)
rec_spark  = evaluator_rec.evaluate(predictions)
indexer_model = spark_model.stages[0]
labels_list = indexer_model.labels

label_mapping_expr = spark.createDataFrame(
    [(float(idx), label) for idx, label in enumerate(labels_list)],
    ['prediction', 'predicted_label']
)
confusion_matrix_df = (
    predictions
    .select('activity_label', 'prediction')
    .join(label_mapping_expr, on='prediction', how='left')
    .groupBy('activity_label')
    .pivot('predicted_label', labels_list)
    .count()
    .fillna(0)
)
confusion_matrix_df.show(truncate=False)

predictions.select('activity_label', 'label', 'prediction').show(10, truncate=False)
os.makedirs(SAVE_DIR, exist_ok=True)

# Write to local
spark_model_path = SPARK_MODEL_PATH
spark_model.write().overwrite().save(spark_model_path)
print(f'Spark PipelineModel disimpan ke : {spark_model_path}')

indexer_model = spark_model.stages[0]
label_mapping = {
    'label_to_index': {label: i for i, label in enumerate(indexer_model.labels)},
    'index_to_label': {i: label for i, label in enumerate(indexer_model.labels)},
    'labels': list(indexer_model.labels)
}

label_mapping_path = LABEL_MAPPING_PATH
with open(label_mapping_path, 'wb') as f:
    pickle.dump(label_mapping, f)
print(f'Label mapping (PKL) disimpan    : {label_mapping_path}')

feature_meta_path = FEATURE_META_PATH
with open(feature_meta_path, 'wb') as f:
    pickle.dump(feature_cols, f)
print(f'Feature cols (PKL) disimpan     : {feature_meta_path}')

print('Semua Spark artefak berhasil disimpan!')
print(f'Direktori : {SAVE_DIR}')
for f in os.listdir(SAVE_DIR):
    full_p = os.path.join(SAVE_DIR, f)
    if os.path.isfile(full_p):
        size_kb = os.path.getsize(full_p) / 1024
        print(f'  {f:<40} {size_kb:.1f} KB')
    else:
        print(f'  {f:<40} [folder]')
