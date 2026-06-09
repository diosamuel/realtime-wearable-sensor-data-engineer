import pickle

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils_spark import SparkHARUtils


class SparkInference:
    def __init__(self, spark=None, sample_size=5, sample_seed=99):
        self.spark = spark or self._create_spark_session()
        self.sample_size = sample_size
        self.sample_seed = sample_seed

        self.loaded_spark_model = None
        self.loaded_feature_cols = []
        self.loaded_label_map = {}
        self.test_df = None
        self.dummy_samples = None
        self.result = None
        self.result_with_labels = None

    def _create_spark_session(self):
        return (
            SparkSession.builder
            .appName('HAR-RandomForest-SparkMLlib-Inference')
            .config('spark.driver.memory', '4g')
            .getOrCreate()
        )

    def load_artifacts(self):
        self.loaded_spark_model = PipelineModel.load(SparkHARUtils.SPARK_MODEL_PATH)

        with open(SparkHARUtils.FEATURE_META_PATH, 'rb') as f:
            self.loaded_feature_cols = pickle.load(f)

        with open(SparkHARUtils.LABEL_MAPPING_PATH, 'rb') as f:
            self.loaded_label_map = pickle.load(f)

        return self.loaded_spark_model, self.loaded_feature_cols, self.loaded_label_map

    def load_test_data(self):
        self.test_df = self.spark.read.parquet(SparkHARUtils.TEST_PARQUET_PATH)
        return self.test_df

    def prepare_samples(self):
        if self.test_df is None:
            raise ValueError('test_df belum ada. Panggil load_test_data() dulu.')

        self.dummy_samples = (
            self.test_df
            .select(*self.loaded_feature_cols, 'activity_label')
            .orderBy(F.rand(seed=self.sample_seed))
            .limit(self.sample_size)
        )
        return self.dummy_samples

    def build_prediction_label_expr(self):
        index_to_label_items = []
        for idx, label in self.loaded_label_map['index_to_label'].items():
            index_to_label_items.extend([F.lit(float(idx)), F.lit(label)])
        return F.create_map(*index_to_label_items)

    def run_prediction(self):
        if self.dummy_samples is None:
            raise ValueError('dummy_samples belum ada. Panggil prepare_samples() dulu.')

        self.result = self.loaded_spark_model.transform(self.dummy_samples)
        index_to_label_expr = self.build_prediction_label_expr()
        self.result_with_labels = (
            self.result
            .withColumn('predicted_label', index_to_label_expr[F.col('prediction')])
            .withColumn(
                'status',
                F.when(F.col('activity_label') == F.col('predicted_label'), F.lit('BENAR'))
                .otherwise(F.lit('SALAH'))
            )
        )
        return self.result_with_labels

    def show_results(self):
        print('DEMO HASIL INFERENSI (5 sampel dari test parquet)')
        self.result_with_labels.select('activity_label', 'predicted_label', 'status').show(truncate=False)

        correct = self.result_with_labels.filter(F.col('status') == 'BENAR').count()
        print(f'Hasil: {correct}/{self.sample_size} prediksi benar')

    def show_artifacts(self):
        print('Model artifacts:')
        print(f'  {SparkHARUtils.SPARK_MODEL_PATH}  -> Spark PipelineModel (native)')
        print(f'  {SparkHARUtils.FEATURE_META_PATH}  -> daftar {len(self.loaded_feature_cols)} fitur')
        print(f'  {SparkHARUtils.TEST_PARQUET_PATH}  -> sumber sample inference')
        print(f'Semua artefak tersimpan di: {SparkHARUtils.SAVE_DIR}')

    def run(self):
        self.load_artifacts()
        self.load_test_data()
        self.prepare_samples()
        self.run_prediction()
        self.show_results()
        self.show_artifacts()
        return self.result_with_labels


if __name__ == '__main__':
    SparkInference().run()
