import os
import sys

from pyspark.sql import functions as F
from pyspark.sql import types as T

SPARK_APPS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if SPARK_APPS_DIR not in sys.path:
    sys.path.insert(0, SPARK_APPS_DIR)

from process.setupMinio import MinioConfig, MinioCrud


class SparkHARUtils:
    N_ESTIMATORS = 300
    SENSOR_COLUMNS = [
        'T_xacc', 'T_yacc', 'T_zacc', 'T_xgyro', 'T_ygyro', 'T_zgyro', 'T_xmag', 'T_ymag', 'T_zmag',
        'RA_xacc', 'RA_yacc', 'RA_zacc', 'RA_xgyro', 'RA_ygyro', 'RA_zgyro', 'RA_xmag', 'RA_ymag', 'RA_zmag',
        'LA_xacc', 'LA_yacc', 'LA_zacc', 'LA_xgyro', 'LA_ygyro', 'LA_zgyro', 'LA_xmag', 'LA_ymag', 'LA_zmag',
        'RL_xacc', 'RL_yacc', 'RL_zacc', 'RL_xgyro', 'RL_ygyro', 'RL_zgyro', 'RL_xmag', 'RL_ymag', 'RL_zmag',
        'LL_xacc', 'LL_yacc', 'LL_zacc', 'LL_xgyro', 'LL_ygyro', 'LL_zgyro', 'LL_xmag', 'LL_ymag', 'LL_zmag',
    ]
    SELECTED_ACTIVITIES = {
        'a01': 'sitting',
        'a02': 'standing',
        'a03': 'lying_back',
        'a04': 'lying_right',
        'a05': 'ascending_stairs',
        'a06': 'descending_stairs',
        'a07': 'standing_elevator',
        'a08': 'moving_elevator',
        'a10': 'walking_treadmill',
        'a12': 'running_treadmill',
    }
    ACTIVITY_CATEGORIES = {
        'sitting': 'sedentary',
        'standing': 'sedentary',
        'lying_back': 'sedentary',
        'lying_right': 'sedentary',
        'standing_elevator': 'sedentary',
        'walking_treadmill': 'light_activity',
        'ascending_stairs': 'light_activity',
        'descending_stairs': 'light_activity',
        'moving_elevator': 'light_activity',
        'running_treadmill': 'intense_activity',
    }
    TRAIN_PERSONS = [1, 2, 3, 4, 5, 6]
    TEST_PERSONS = [7, 8]
    MINIO_CONFIG = MinioConfig()
    DATA_PATH = os.getenv('HAR_DATA_PATH', '/data/data')
    SAVE_PREFIX = os.getenv('HAR_SAVE_PREFIX', 'model_artifact').strip('/')
    SAVE_DIR = os.getenv('HAR_SAVE_DIR', MINIO_CONFIG.s3a_uri(SAVE_PREFIX)).rstrip('/')
    S3_SAVE_DIR = MINIO_CONFIG.s3_uri(SAVE_PREFIX)
    SPARK_MODEL_PATH = f'{SAVE_DIR}/spark_rf_pipeline_model'
    LABEL_MAPPING_KEY = f'{SAVE_PREFIX}/label_mapping.pkl'
    FEATURE_META_KEY = f'{SAVE_PREFIX}/feature_cols.pkl'
    LABEL_MAPPING_PATH = f'{SAVE_DIR}/label_mapping.pkl'
    FEATURE_META_PATH = f'{SAVE_DIR}/feature_cols.pkl'
    RAW_PARQUET_PATH = f'{SAVE_DIR}/raw_dataset.parquet'
    FEATURE_PARQUET_PATH = f'{SAVE_DIR}/feature_dataset.parquet'
    TRAIN_PARQUET_PATH = f'{SAVE_DIR}/train_set.parquet'
    TEST_PARQUET_PATH = f'{SAVE_DIR}/test_set.parquet'

    @staticmethod
    def minio():
        return MinioCrud(SparkHARUtils.MINIO_CONFIG)

    @staticmethod
    def configureMinioS3(spark):
        config = SparkHARUtils.MINIO_CONFIG
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set('fs.s3a.endpoint', config.endpoint)
        hadoop_conf.set('fs.s3a.access.key', config.access_key)
        hadoop_conf.set('fs.s3a.secret.key', config.secret_key)
        hadoop_conf.set('fs.s3a.path.style.access', 'true')
        hadoop_conf.set(
            'fs.s3a.connection.ssl.enabled',
            'true' if config.endpoint.startswith('https://') else 'false',
        )
        hadoop_conf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        return spark

    @staticmethod
    def activity_label_expr():
        mapping_items = []
        for activity_id, activity_label in SparkHARUtils.SELECTED_ACTIVITIES.items():
            mapping_items.extend([F.lit(activity_id), F.lit(activity_label)])
        return F.create_map(*mapping_items)

    @staticmethod
    def activity_category_expr():
        mapping_items = []
        for activity_label, activity_category in SparkHARUtils.ACTIVITY_CATEGORIES.items():
            mapping_items.extend([F.lit(activity_label), F.lit(activity_category)])
        return F.create_map(*mapping_items)

    @staticmethod
    def dataset_file_paths(data_path, selected_activities):
        file_paths = []

        for act_id in selected_activities:
            act_path = os.path.join(data_path, act_id)

            if not os.path.exists(act_path):
                print(f'WARNING: Folder tidak ditemukan: {act_path}')
                continue

            for p_num in range(1, 9):
                person_path = os.path.join(act_path, f'p{p_num}')

                if not os.path.exists(person_path):
                    continue

                for s_num in range(1, 61):
                    file_path = os.path.join(person_path, f's{s_num:02d}.txt')
                    if os.path.exists(file_path):
                        file_paths.append(file_path)

        if not file_paths:
            raise ValueError('Tidak ada data yang berhasil diload!')

        return file_paths

    @staticmethod
    def loadAllData(spark, data_path, selected_activities, sensor_columns):
        """
        Load semua file dataset sebagai Spark DataFrame.
        Struktur folder: data/a{activity}/p{person}/s{segment}.txt
        """
        schema = T.StructType([
            T.StructField(column_name, T.DoubleType(), nullable=True)
            for column_name in sensor_columns
        ])
        file_paths = SparkHARUtils.dataset_file_paths(data_path, selected_activities)
        activity_label_map = SparkHARUtils.activity_label_expr()

        return (
            spark.read
            .option('header', 'false')
            .schema(schema)
            .csv(file_paths)
            .withColumn('source_file', F.input_file_name())
            .withColumn('activity_id', F.regexp_extract('source_file', r'[\\/](a\d{2})[\\/]p\d+[\\/]s\d{2}\.txt$', 1))
            .withColumn('activity_label', activity_label_map[F.col('activity_id')])
            .withColumn('person_id', F.regexp_extract('source_file', r'[\\/]a\d{2}[\\/]p(\d+)[\\/]s\d{2}\.txt$', 1).cast('int'))
            .withColumn('segment_id', F.regexp_extract('source_file', r'[\\/]a\d{2}[\\/]p\d+[\\/]s(\d{2})\.txt$', 1).cast('int'))
            .drop('source_file')
        )

    @staticmethod
    def addMagnitudeColumns(df):
        body_parts = {
            'T': ('T_xacc', 'T_yacc', 'T_zacc', 'T_xgyro', 'T_ygyro', 'T_zgyro', 'T_xmag', 'T_ymag', 'T_zmag'),
            'RA': ('RA_xacc', 'RA_yacc', 'RA_zacc', 'RA_xgyro', 'RA_ygyro', 'RA_zgyro', 'RA_xmag', 'RA_ymag', 'RA_zmag'),
            'LA': ('LA_xacc', 'LA_yacc', 'LA_zacc', 'LA_xgyro', 'LA_ygyro', 'LA_zgyro', 'LA_xmag', 'LA_ymag', 'LA_zmag'),
            'RL': ('RL_xacc', 'RL_yacc', 'RL_zacc', 'RL_xgyro', 'RL_ygyro', 'RL_zgyro', 'RL_xmag', 'RL_ymag', 'RL_zmag'),
            'LL': ('LL_xacc', 'LL_yacc', 'LL_zacc', 'LL_xgyro', 'LL_ygyro', 'LL_zgyro', 'LL_xmag', 'LL_ymag', 'LL_zmag'),
        }
        mag_columns = []

        for part, cols in body_parts.items():
            xacc, yacc, zacc, xgyro, ygyro, zgyro, xmag, ymag, zmag = cols
            acc_col = f'{part}_acc_mag'
            gyro_col = f'{part}_gyro_mag'
            mag_col = f'{part}_mag_mag'

            df = (
                df
                .withColumn(acc_col, F.sqrt(F.col(xacc) ** 2 + F.col(yacc) ** 2 + F.col(zacc) ** 2))
                .withColumn(gyro_col, F.sqrt(F.col(xgyro) ** 2 + F.col(ygyro) ** 2 + F.col(zgyro) ** 2))
                .withColumn(mag_col, F.sqrt(F.col(xmag) ** 2 + F.col(ymag) ** 2 + F.col(zmag) ** 2))
            )
            mag_columns.extend([acc_col, gyro_col, mag_col])

        return df, mag_columns

    @staticmethod
    def extractFeaturesPerSegment(df, sensor_columns):
        """
        Ekstrak fitur statistik per segmen sebagai Spark DataFrame.
        Output: 1 baris per segmen dengan fitur mean, std, min, max, skew, kurtosis
        """
        group_cols = ['activity_id', 'activity_label', 'person_id', 'segment_id']
        aggregations = []

        for col_name in sensor_columns:
            col = F.col(col_name)
            aggregations.extend([
                F.mean(col).alias(f'{col_name}_mean'),
                F.stddev(col).alias(f'{col_name}_std'),
                F.min(col).alias(f'{col_name}_min'),
                F.max(col).alias(f'{col_name}_max'),
                F.skewness(col).alias(f'{col_name}_skew'),
                F.kurtosis(col).alias(f'{col_name}_kurtosis'),
            ])

        return df.groupBy(*group_cols).agg(*aggregations)

    @staticmethod
    def prepareFeatureDatasets(spark, write_parquet=True):
        raw_df = SparkHARUtils.loadAllData(
            spark,
            SparkHARUtils.DATA_PATH,
            SparkHARUtils.SELECTED_ACTIVITIES,
            SparkHARUtils.SENSOR_COLUMNS,
        )
        raw_df, mag_columns = SparkHARUtils.addMagnitudeColumns(raw_df)
        all_columns = SparkHARUtils.SENSOR_COLUMNS + mag_columns

        feature_df = (
            SparkHARUtils.extractFeaturesPerSegment(raw_df, all_columns)
            .withColumn('activity_category', SparkHARUtils.activity_category_expr()[F.col('activity_label')])
        )
        meta_cols = ['activity_id', 'activity_label', 'activity_category', 'person_id', 'segment_id']
        feature_cols = [col for col in feature_df.columns if col not in meta_cols]
        train_df = feature_df.filter(F.col('person_id').isin(SparkHARUtils.TRAIN_PERSONS))
        test_df = feature_df.filter(F.col('person_id').isin(SparkHARUtils.TEST_PERSONS))

        if write_parquet:
            SparkHARUtils.configureMinioS3(spark)
            SparkHARUtils.minio().ensure_bucket_ready()
            raw_df.write.mode('overwrite').parquet(SparkHARUtils.RAW_PARQUET_PATH)
            feature_df.write.mode('overwrite').parquet(SparkHARUtils.FEATURE_PARQUET_PATH)
            train_df.write.mode('overwrite').parquet(SparkHARUtils.TRAIN_PARQUET_PATH)
            test_df.write.mode('overwrite').parquet(SparkHARUtils.TEST_PARQUET_PATH)

        return {
            'feature_cols': feature_cols,
            'raw_df': raw_df,
            'feature_df': feature_df,
            'train_df': train_df,
            'test_df': test_df,
            'raw_path': SparkHARUtils.RAW_PARQUET_PATH,
            'feature_path': SparkHARUtils.FEATURE_PARQUET_PATH,
            'train_path': SparkHARUtils.TRAIN_PARQUET_PATH,
            'test_path': SparkHARUtils.TEST_PARQUET_PATH,
        }


SENSOR_COLUMNS = SparkHARUtils.SENSOR_COLUMNS
