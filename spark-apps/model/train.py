import os
import pickle
import time as _time

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler as SparkScaler
from pyspark.ml.classification import RandomForestClassifier as SparkRF
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline as SparkPipeline
from preprocessing import feature_cols, test_df, train_df
from utils import (
    FEATURE_META_PATH,
    LABEL_MAPPING_PATH,
    N_ESTIMATORS,
    SAVE_DIR,
    SPARK_MODEL_PATH,
)

spark = SparkSession.builder \
    .appName('HAR-RandomForest-SparkMLlib') \
    .config('spark.driver.memory', '4g') \
    .getOrCreate()

print(f'Spark version: {spark.version}')

spark_train = spark.createDataFrame(train_df)
spark_test  = spark.createDataFrame(test_df)

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

eval_pd = predictions.select('activity_label', 'prediction').toPandas()
eval_pd['predicted_label'] = eval_pd['prediction'].astype(int).map(lambda idx: labels_list[idx])

confusion_matrix_df = pd.crosstab(
    eval_pd['activity_label'],
    eval_pd['predicted_label'],
    rownames=['Actual'],
    colnames=['Predicted']
)

predictions.select('activity_label', 'label', 'prediction').show(10, truncate=False)
os.makedirs(SAVE_DIR, exist_ok=True)
spark_model_path = SPARK_MODEL_PATH
spark_model.write().overwrite().save(spark_model_path)
print(f'[1] Spark PipelineModel disimpan ke : {spark_model_path}')

indexer_model = spark_model.stages[0]
label_mapping = {
    'label_to_index': {label: i for i, label in enumerate(indexer_model.labels)},
    'index_to_label': {i: label for i, label in enumerate(indexer_model.labels)},
    'labels': list(indexer_model.labels)
}

label_mapping_path = LABEL_MAPPING_PATH
with open(label_mapping_path, 'wb') as f:
    pickle.dump(label_mapping, f)
print(f'[2] Label mapping (PKL) disimpan    : {label_mapping_path}')

feature_meta_path = FEATURE_META_PATH
with open(feature_meta_path, 'wb') as f:
    pickle.dump(feature_cols, f)
print(f'[3] Feature cols (PKL) disimpan     : {feature_meta_path}')

print('Semua Spark artefak berhasil disimpan!')
print(f'  Direktori : {SAVE_DIR}')
for f in os.listdir(SAVE_DIR):
    full_p = os.path.join(SAVE_DIR, f)
    if os.path.isfile(full_p):
        size_kb = os.path.getsize(full_p) / 1024
        print(f'  {f:<40} {size_kb:.1f} KB')
    else:
        print(f'  {f:<40} [folder]')
