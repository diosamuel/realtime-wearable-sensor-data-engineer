from pyspark.ml import PipelineModel
import pickle
from preprocessing import feature_cols, test_df
from train import acc_spark, f1_spark, spark, spark_train_time
from utils import (
    FEATURE_META_PATH,
    LABEL_MAPPING_PATH,
    N_ESTIMATORS,
    SAVE_DIR,
    SELECTED_ACTIVITIES,
    SPARK_MODEL_PATH,
    TEST_PERSONS,
    TRAIN_PERSONS,
)

spark_model_path = SPARK_MODEL_PATH
label_mapping_path = LABEL_MAPPING_PATH
feature_meta_path = FEATURE_META_PATH
loaded_spark_model  = PipelineModel.load(spark_model_path)

with open(label_mapping_path, 'rb') as f:
    loaded_label_map = pickle.load(f)
with open(feature_meta_path, 'rb') as f:
    loaded_feature_cols = pickle.load(f)


# Test samples
dummy_samples = test_df[feature_cols].sample(5, random_state=99).copy()
dummy_labels  = test_df.loc[dummy_samples.index, 'activity_label'].values
dummy_samples['activity_label'] = dummy_labels
dummy_samples = dummy_samples.reset_index(drop=True)

dummy_spark = spark.createDataFrame(dummy_samples)
result = loaded_spark_model.transform(dummy_spark)
index_to_label = loaded_label_map['index_to_label']
result_pd = result.select('activity_label', 'prediction').toPandas()
result_pd['predicted_label'] = result_pd['prediction'].astype(int).map(lambda x: index_to_label[x])
result_pd['status'] = result_pd.apply(
    lambda r: 'BENAR v' if r['activity_label'] == r['predicted_label'] else 'SALAH x', axis=1
)

print('DEMO HASIL INFERENSI (5 sampel data baru dari sensor)')
print(result_pd[['activity_label', 'predicted_label', 'status']].to_string(index=False))
correct = (result_pd['status'] == 'BENAR v').sum()
print(f'Hasil: {correct}/5 prediksi benar')

print(f'Model artifacts :')
print(f'  spark_rf_pipeline_model/  -> Spark PipelineModel (native)')
print(f'  label_mapping.pkl         -> index <-> label mapping')
print(f'  feature_cols.pkl          -> daftar {len(feature_cols)} fitur')
print(f'Semua artefak tersimpan di: {SAVE_DIR}')
