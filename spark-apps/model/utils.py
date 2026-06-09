import os

import pandas as pd


N_ESTIMATORS = 300
SENSOR_COLUMNS = [
    'T_xacc',  'T_yacc',  'T_zacc',  'T_xgyro',  'T_ygyro',  'T_zgyro',  'T_xmag',  'T_ymag',  'T_zmag',
    'RA_xacc', 'RA_yacc', 'RA_zacc', 'RA_xgyro', 'RA_ygyro', 'RA_zgyro', 'RA_xmag', 'RA_ymag', 'RA_zmag',
    'LA_xacc', 'LA_yacc', 'LA_zacc', 'LA_xgyro', 'LA_ygyro', 'LA_zgyro', 'LA_xmag', 'LA_ymag', 'LA_zmag',
    'RL_xacc', 'RL_yacc', 'RL_zacc', 'RL_xgyro', 'RL_ygyro', 'RL_zgyro', 'RL_xmag', 'RL_ymag', 'RL_zmag',
    'LL_xacc', 'LL_yacc', 'LL_zacc', 'LL_xgyro', 'LL_ygyro', 'LL_zgyro', 'LL_xmag', 'LL_ymag', 'LL_zmag'
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
    'a12': 'running_treadmill'
}
ACTIVITY_CATEGORIES = {
    'sitting':'sedentary',
    'standing':'sedentary',
    'lying_back':'sedentary',
    'lying_right':'sedentary',
    'standing_elevator':'sedentary',
    'walking_treadmill':'light_activity',
    'ascending_stairs':'light_activity',
    'descending_stairs':'light_activity',
    'moving_elevator':'light_activity',
    'running_treadmill':'intense_activity'
}
TRAIN_PERSONS = [1, 2, 3, 4, 5, 6]
TEST_PERSONS = [7, 8]
DATA_PATH = os.getenv('HAR_DATA_PATH', '/data/data')
SAVE_DIR = os.getenv('HAR_SAVE_DIR', '/opt/spark-data/HAR_SmartHealth/')
SPARK_MODEL_PATH = os.path.join(SAVE_DIR, 'spark_rf_pipeline_model')
LABEL_MAPPING_PATH = os.path.join(SAVE_DIR, 'label_mapping.pkl')
FEATURE_META_PATH = os.path.join(SAVE_DIR, 'feature_cols.pkl')


def loadAllData(data_path, selected_activities, sensor_columns):
    """
    Load semua file dari dataset.
    Struktur folder: data/a{activity}/p{person}/s{segment}.txt
    """
    all_records = []

    for act_id, act_label in selected_activities.items():
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

                if not os.path.exists(file_path):
                    continue

                try:
                    df = pd.read_csv(file_path, header=None, names=sensor_columns)
                    df['activity_id']    = act_id
                    df['activity_label'] = act_label
                    df['person_id']      = p_num
                    df['segment_id']     = s_num
                    df['timestep']       = range(len(df))
                    all_records.append(df)
                except Exception as e:
                    print(f'ERROR loading {file_path}: {e}')

    if not all_records:
        raise ValueError('Tidak ada data yang berhasil diload!')

    result = pd.concat(all_records, ignore_index=True)
    return result


def extractFeaturesPerSegment(df, sensor_columns):
    """
    Ekstrak fitur statistik per segmen dari setiap sensor.
    Output: 1 baris per segmen dengan fitur mean, std, min, max, skew, kurtosis
    """
    group_cols = ['activity_id', 'activity_label', 'person_id', 'segment_id']

    feature_dfs = []

    grouped = df.groupby(group_cols)

    for name, group in grouped:
        feat = {}
        feat['activity_id']    = name[0]
        feat['activity_label'] = name[1]
        feat['person_id']      = name[2]
        feat['segment_id']     = name[3]

        sensor_data = group[sensor_columns]

        for col in sensor_columns:
            series = sensor_data[col]
            feat[f'{col}_mean']     = series.mean()
            feat[f'{col}_std']      = series.std()
            feat[f'{col}_min']      = series.min()
            feat[f'{col}_max']      = series.max()
            feat[f'{col}_skew']     = series.skew()
            feat[f'{col}_kurtosis'] = series.kurtosis()

        feature_dfs.append(feat)

    return pd.DataFrame(feature_dfs)

