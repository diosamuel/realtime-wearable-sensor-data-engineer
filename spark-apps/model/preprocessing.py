import os
import sys

from pyspark.sql import functions as F

from utilsSpark import SparkHARUtils

SPARK_APPS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if SPARK_APPS_DIR not in sys.path:
    sys.path.insert(0, SPARK_APPS_DIR)

from process.setupMinio import MinioConfig, MinioCrud

class SparkPreprocessing:
    def __init__(self, spark, write_parquet=True):
        self.spark = spark
        self.write_parquet = write_parquet

        self.raw_df = None
        self.feature_df = None
        self.train_df = None
        self.test_df = None
        self.feature_cols = []
        self.mag_columns = []
        self.minio_paths = {}

    def load_raw_data(self):
        self.raw_df = SparkHARUtils.loadAllData(
            self.spark,
            SparkHARUtils.DATA_PATH,
            SparkHARUtils.SELECTED_ACTIVITIES,
            SparkHARUtils.SENSOR_COLUMNS,
        )
        return self.raw_df

    def add_magnitude_columns(self):
        if self.raw_df is None:
            raise ValueError('raw_df belum ada. Panggil load_raw_data() dulu.')

        self.raw_df, self.mag_columns = SparkHARUtils.addMagnitudeColumns(self.raw_df)
        return self.raw_df, self.mag_columns

    def extract_features(self):
        if self.raw_df is None:
            raise ValueError('raw_df belum ada. Panggil load_raw_data() dulu.')

        all_columns = SparkHARUtils.SENSOR_COLUMNS + self.mag_columns
        self.feature_df = (
            SparkHARUtils.extractFeaturesPerSegment(self.raw_df, all_columns)
            .withColumn('activity_category', SparkHARUtils.activity_category_expr()[F.col('activity_label')])
        )
        meta_cols = ['activity_id', 'activity_label', 'activity_category', 'person_id', 'segment_id']
        self.feature_cols = [col for col in self.feature_df.columns if col not in meta_cols]
        return self.feature_df, self.feature_cols

    def split_datasets(self):
        if self.feature_df is None:
            raise ValueError('feature_df belum ada. Panggil extract_features() dulu.')

        self.train_df = self.feature_df.filter(
            F.col('person_id').isin(SparkHARUtils.TRAIN_PERSONS)
        )
        self.test_df = self.feature_df.filter(
            F.col('person_id').isin(SparkHARUtils.TEST_PERSONS)
        )
        return self.train_df, self.test_df

    def write_outputs(self):
        if not self.write_parquet:
            return

        os.makedirs(SparkHARUtils.SAVE_DIR, exist_ok=True)
        self.raw_df.write.mode('overwrite').parquet(SparkHARUtils.RAW_PARQUET_PATH)
        self.feature_df.write.mode('overwrite').parquet(SparkHARUtils.FEATURE_PARQUET_PATH)
        self.train_df.write.mode('overwrite').parquet(SparkHARUtils.TRAIN_PARQUET_PATH)
        self.test_df.write.mode('overwrite').parquet(SparkHARUtils.TEST_PARQUET_PATH)
        parquet_paths = {
            'raw_path': SparkHARUtils.RAW_PARQUET_PATH,
            'feature_path': SparkHARUtils.FEATURE_PARQUET_PATH,
            'train_path': SparkHARUtils.TRAIN_PARQUET_PATH,
            'test_path': SparkHARUtils.TEST_PARQUET_PATH,
        }
        self.minio_paths = MinioCrud(MinioConfig()).upload_parquet_outputs(parquet_paths)

    def run(self):
        self.load_raw_data()
        self.add_magnitude_columns()
        self.extract_features()
        self.split_datasets()
        self.write_outputs()

        return {
            'feature_cols': self.feature_cols,
            'raw_df': self.raw_df,
            'feature_df': self.feature_df,
            'train_df': self.train_df,
            'test_df': self.test_df,
            'raw_path': SparkHARUtils.RAW_PARQUET_PATH,
            'feature_path': SparkHARUtils.FEATURE_PARQUET_PATH,
            'train_path': SparkHARUtils.TRAIN_PARQUET_PATH,
            'test_path': SparkHARUtils.TEST_PARQUET_PATH,
            'minio_paths': self.minio_paths,
        }
