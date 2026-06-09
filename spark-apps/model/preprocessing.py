import numpy as np
import warnings
warnings.filterwarnings('ignore')
from utils import (
    ACTIVITY_CATEGORIES,
    DATA_PATH,
    SELECTED_ACTIVITIES,
    SENSOR_COLUMNS,
    TEST_PERSONS,
    TRAIN_PERSONS,
    extractFeaturesPerSegment,
    loadAllData,
)

print("Initated preprocessing")

raw_df = loadAllData(DATA_PATH, SELECTED_ACTIVITIES, SENSOR_COLUMNS)
body_parts = {
    'T':  ('T_xacc',  'T_yacc',  'T_zacc',  'T_xgyro',  'T_ygyro',  'T_zgyro',  'T_xmag',  'T_ymag',  'T_zmag'),
    'RA': ('RA_xacc', 'RA_yacc', 'RA_zacc', 'RA_xgyro', 'RA_ygyro', 'RA_zgyro', 'RA_xmag', 'RA_ymag', 'RA_zmag'),
    'LA': ('LA_xacc', 'LA_yacc', 'LA_zacc', 'LA_xgyro', 'LA_ygyro', 'LA_zgyro', 'LA_xmag', 'LA_ymag', 'LA_zmag'),
    'RL': ('RL_xacc', 'RL_yacc', 'RL_zacc', 'RL_xgyro', 'RL_ygyro', 'RL_zgyro', 'RL_xmag', 'RL_ymag', 'RL_zmag'),
    'LL': ('LL_xacc', 'LL_yacc', 'LL_zacc', 'LL_xgyro', 'LL_ygyro', 'LL_zgyro', 'LL_xmag', 'LL_ymag', 'LL_zmag'),
}
MAG_COLUMNS = []

for part, cols in body_parts.items():
    xacc, yacc, zacc, xgyro, ygyro, zgyro, xmag, ymag, zmag = cols
    raw_df[f'{part}_acc_mag']  = np.sqrt(raw_df[xacc]**2  + raw_df[yacc]**2  + raw_df[zacc]**2)
    raw_df[f'{part}_gyro_mag'] = np.sqrt(raw_df[xgyro]**2 + raw_df[ygyro]**2 + raw_df[zgyro]**2)
    raw_df[f'{part}_mag_mag']  = np.sqrt(raw_df[xmag]**2  + raw_df[ymag]**2  + raw_df[zmag]**2)
    MAG_COLUMNS += [f'{part}_acc_mag', f'{part}_gyro_mag', f'{part}_mag_mag']
    print(f"magnitude col: {MAG_COLUMNS}")

ALL_COLUMNS = SENSOR_COLUMNS + MAG_COLUMNS
feature_df = extractFeaturesPerSegment(raw_df, ALL_COLUMNS)
n_feature_cols = len([c for c in feature_df.columns if c not in ['activity_id', 'activity_label', 'person_id', 'segment_id']])
feature_df['activity_category'] = feature_df['activity_label'].map(ACTIVITY_CATEGORIES)
meta_cols    = ['activity_id', 'activity_label', 'activity_category', 'person_id', 'segment_id']
feature_cols = [c for c in feature_df.columns if c not in meta_cols]
train_df = feature_df[feature_df['person_id'].isin(TRAIN_PERSONS)].copy()
test_df = feature_df[feature_df['person_id'].isin(TEST_PERSONS)].copy()

print(f"Train: {len(train_df)}")
print(f"Test: {len(test_df)}")