import os
import pickle
import time as _time

from pyspark.ml import Pipeline as SparkPipeline
from pyspark.ml.classification import RandomForestClassifier as SparkRF
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StandardScaler as SparkScaler, StringIndexer, VectorAssembler
from pyspark.sql import SparkSession

from preprocessing import SparkPreprocessing
from utils_spark import SparkHARUtils


class SparkTrainer:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.preprocessor = SparkPreprocessing(self.spark, write_parquet=True)

        self.prepared = None
        self.feature_cols = []
        self.spark_train = None
        self.spark_test = None
        self.indexer = None
        self.assembler = None
        self.spark_scaler = None
        self.spark_rf = None
        self.spark_pipeline = None
        self.spark_model = None
        self.predictions = None
        self.metrics = {}

    def _create_spark_session(self):
        return (
            SparkSession.builder
            .appName('HAR-RandomForest-SparkMLlib')
            .config('spark.driver.memory', '4g')
            .getOrCreate()
        )

    def preprocess(self):
        print(f'Initiated Spark version: {self.spark.version}')
        self.prepared = self.preprocessor.run()
        self.feature_cols = self.prepared['feature_cols']
        self.spark_train = self.spark.read.parquet(self.prepared['train_path'])
        self.spark_test = self.spark.read.parquet(self.prepared['test_path'])
        print(f"Train parquet: {self.prepared['train_path']} ({self.spark_train.count()} rows)")
        print(f"Test parquet : {self.prepared['test_path']} ({self.spark_test.count()} rows)")

    def build_pipeline(self):
        self.indexer = StringIndexer(
            inputCol='activity_label',
            outputCol='label',
            handleInvalid='error',
        )
        self.assembler = VectorAssembler(
            inputCols=self.feature_cols,
            outputCol='raw_features',
        )
        self.spark_scaler = SparkScaler(
            inputCol='raw_features',
            outputCol='features',
            withMean=True,
            withStd=True,
        )
        self.spark_rf = SparkRF(
            featuresCol='features',
            labelCol='label',
            numTrees=SparkHARUtils.N_ESTIMATORS,
            maxDepth=20,
            minInstancesPerNode=1,
            featureSubsetStrategy='sqrt',
            seed=42,
        )
        self.spark_pipeline = SparkPipeline(stages=[
            self.indexer,
            self.assembler,
            self.spark_scaler,
            self.spark_rf,
        ])

    def train(self):
        t0_spark = _time.time()
        self.spark_model = self.spark_pipeline.fit(self.spark_train)
        spark_train_time = _time.time() - t0_spark
        print(f'Training selesai dalam {spark_train_time:.1f} detik!')

    def evaluate(self):
        self.predictions = self.spark_model.transform(self.spark_test)

        evaluators = {
            'f1': MulticlassClassificationEvaluator(
                labelCol='label', predictionCol='prediction', metricName='f1'
            ),
            'accuracy': MulticlassClassificationEvaluator(
                labelCol='label', predictionCol='prediction', metricName='accuracy'
            ),
            'weightedPrecision': MulticlassClassificationEvaluator(
                labelCol='label', predictionCol='prediction', metricName='weightedPrecision'
            ),
            'weightedRecall': MulticlassClassificationEvaluator(
                labelCol='label', predictionCol='prediction', metricName='weightedRecall'
            ),
        }
        self.metrics = {
            name: evaluator.evaluate(self.predictions)
            for name, evaluator in evaluators.items()
        }

        indexer_model = self.spark_model.stages[0]
        labels_list = indexer_model.labels
        label_mapping_expr = self.spark.createDataFrame(
            [(float(idx), label) for idx, label in enumerate(labels_list)],
            ['prediction', 'predicted_label'],
        )
        confusion_matrix_df = (
            self.predictions
            .select('activity_label', 'prediction')
            .join(label_mapping_expr, on='prediction', how='left')
            .groupBy('activity_label')
            .pivot('predicted_label', labels_list)
            .count()
            .fillna(0)
        )

        print('Confusion matrix:')
        confusion_matrix_df.show(truncate=False)
        print('Sample prediction:')
        self.predictions.select('activity_label', 'label', 'prediction').show(10, truncate=False)

    def save_artifacts(self):
        os.makedirs(SparkHARUtils.SAVE_DIR, exist_ok=True)

        self.spark_model.write().overwrite().save(SparkHARUtils.SPARK_MODEL_PATH)
        print(f'Spark PipelineModel disimpan ke : {SparkHARUtils.SPARK_MODEL_PATH}')

        indexer_model = self.spark_model.stages[0]
        label_mapping = {
            'label_to_index': {label: i for i, label in enumerate(indexer_model.labels)},
            'index_to_label': {i: label for i, label in enumerate(indexer_model.labels)},
            'labels': list(indexer_model.labels),
        }

        with open(SparkHARUtils.LABEL_MAPPING_PATH, 'wb') as f:
            pickle.dump(label_mapping, f)
        print(f'Label mapping (PKL) disimpan    : {SparkHARUtils.LABEL_MAPPING_PATH}')

        with open(SparkHARUtils.FEATURE_META_PATH, 'wb') as f:
            pickle.dump(self.feature_cols, f)
        print(f'Feature cols (PKL) disimpan     : {SparkHARUtils.FEATURE_META_PATH}')

        print('Semua Spark artefak berhasil disimpan!')
        print(f'Direktori : {SparkHARUtils.SAVE_DIR}')
        for f_name in os.listdir(SparkHARUtils.SAVE_DIR):
            full_p = os.path.join(SparkHARUtils.SAVE_DIR, f_name)
            if os.path.isfile(full_p):
                size_kb = os.path.getsize(full_p) / 1024
                print(f'  {f_name:<40} {size_kb:.1f} KB')
            else:
                print(f'  {f_name:<40} [folder]')

    def run(self):
        self.preprocess()
        self.build_pipeline()
        self.train()
        self.evaluate()
        self.save_artifacts()
        return self


if __name__ == '__main__':
    SparkTrainer().run()
