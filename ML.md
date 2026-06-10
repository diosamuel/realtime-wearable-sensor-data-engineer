# Machine Learning
## Apa yang dilakukan random forest?

Di sini random forest dipakai sebagai **model klasifikasi multi-kelas** untuk menebak aktivitas dari data sensor wearable.

Secara praktis:
- Input berupa vektor fitur hasil ekstraksi dari sinyal sensor.
- Model membangun banyak decision tree (`numTrees = 300`).
- Setiap tree dilatih dengan subset fitur acak (`featureSubsetStrategy='sqrt'`).
- Prediksi akhir diambil lewat voting dari semua tree.

Jadi random forest tidak melakukan preprocessing sinyal mentah. Tugasnya hanya belajar pola dari fitur numerik yang sudah disiapkan.

## Apa input modelnya?

Input model adalah kolom `features` yang dibuat dari pipeline:
1. `activity_label` diubah menjadi label numerik dengan `StringIndexer`.
2. Semua fitur numerik digabung oleh `VectorAssembler` ke kolom `raw_features`.
3. `StandardScaler` mengubah `raw_features` menjadi `features`.

Secara sumber data, fitur berasal dari:
- 45 kolom sensor utama:
  - `T_*`, `RA_*`, `LA_*`, `RL_*`, `LL_*`
  - masing-masing berisi axis accelerometer, gyroscope, dan magnetometer
- 15 kolom magnitude tambahan:
  - `T_acc_mag`, `T_gyro_mag`, `T_mag_mag`
  - `RA_acc_mag`, `RA_gyro_mag`, `RA_mag_mag`
  - `LA_acc_mag`, `LA_gyro_mag`, `LA_mag_mag`
  - `RL_acc_mag`, `RL_gyro_mag`, `RL_mag_mag`
  - `LL_acc_mag`, `LL_gyro_mag`, `LL_mag_mag`

Total fitur dasar = 60 kolom, lalu tiap kolom diekstrak menjadi statistik per segmen.

Jumlah fitur yang akhirnya dipakai model:
- 45 kolom sensor utama
- 15 kolom magnitude tambahan
- total kolom yang masuk `extractFeaturesPerSegment` = 60 kolom
- tiap kolom diekstrak menjadi 6 statistik: `mean`, `std`, `min`, `max`, `skew`, `kurtosis`

Jadi total fitur numerik untuk training:

`60 kolom x 6 statistik = 360 fitur`

Kolom-kolom inilah yang masuk ke `VectorAssembler` sebagai `feature_cols`.

## Apa targetnya?

Target yang diprediksi adalah `activity_label`.

Label ini berasal dari mapping `activity_id -> activity_label` di `utils_spark.py`, misalnya:
- `a01 -> sitting`
- `a02 -> standing`
- `a03 -> lying_back`
- `a04 -> lying_right`
- `a05 -> ascending_stairs`
- `a06 -> descending_stairs`
- `a07 -> standing_elevator`
- `a08 -> moving_elevator`
- `a10 -> walking_treadmill`
- `a12 -> running_treadmill`

Di pipeline training:
- `activity_label` dijadikan `label` oleh `StringIndexer`
- model mempelajari kelas aktivitas ini

Jadi target akhirnya adalah **jenis aktivitas manusia**, bukan `activity_category`.
`activity_category` memang dibuat saat preprocessing, tetapi tidak dipakai sebagai target training.

## Preprocessing yang dilakukan

Preprocessing terjadi di `SparkPreprocessing` dan utilitas `SparkHARUtils`.

### 1. Load data mentah

Data dibaca dari struktur folder:
- `data/aXX/pY/sZZ.txt`

Hanya aktivitas yang ada di `SELECTED_ACTIVITIES` yang dipakai.

Setiap file dibaca sebagai CSV tanpa header dengan schema 45 kolom sensor.

### 2. Tambah metadata

Dari nama file, sistem mengekstrak:
- `activity_id`
- `activity_label`
- `person_id`
- `segment_id`

Ini penting supaya tiap baris tahu berasal dari aktivitas, orang, dan segmen mana.

### 3. Tambah fitur magnitude

Untuk setiap body part (`T`, `RA`, `LA`, `RL`, `LL`), dibuat 3 fitur magnitude:
- magnitude accelerometer
- magnitude gyroscope
- magnitude magnetometer

Rumusnya bentuk Euclidean norm:

`sqrt(x^2 + y^2 + z^2)`

### 4. Ekstraksi fitur statistik per segmen

Data digroup berdasarkan:
- `activity_id`
- `activity_label`
- `person_id`
- `segment_id`

Lalu untuk setiap kolom sensor dan magnitude dihitung:
- mean
- standard deviation
- min
- max
- skewness
- kurtosis

Hasilnya adalah 1 baris per segmen, bukan 1 baris per sampel mentah.

Output dari `extractFeaturesPerSegment` menjadi basis dataset training, tetapi tidak semua kolomnya dipakai sebagai input model.

Kolom metadata berikut dikeluarkan dari `feature_cols`:
- `activity_id`
- `activity_label`
- `activity_category`
- `person_id`
- `segment_id`

Sisa kolom statistik numerik, yaitu 360 fitur, dipakai sebagai input model.
`activity_label` tetap dipakai, tetapi sebagai target/label yang diubah menjadi kolom `label` oleh `StringIndexer`.

### 5. Split train-test berdasarkan orang

Pembagian data tidak random.

- Train: `person_id` 1 sampai 6
- Test: `person_id` 7 dan 8

Ini lebih realistis karena menguji generalisasi ke subjek yang belum pernah dilihat model.

### 6. Scaling

Semua fitur numerik dinormalisasi dengan `StandardScaler`:
- `withMean=True`
- `withStd=True`

Tujuannya agar skala fitur lebih seragam sebelum masuk ke random forest.
Secara teori random forest tidak terlalu bergantung pada skala fitur, jadi langkah ini lebih bersifat konsistensi pipeline daripada kebutuhan wajib model.

## Apakah 360 fitur logis untuk random forest?

Ya, 360 fitur masih logis untuk `RandomForestClassifier`, terutama karena fiturnya berbentuk data tabular hasil ekstraksi statistik per segmen.
Random forest cukup umum dipakai untuk kasus seperti ini karena mampu menangani banyak fitur numerik dan hubungan non-linear.

Yang perlu diperhatikan adalah rasio jumlah sampel terhadap jumlah fitur.
Dalam struktur dataset ini, satu baris training adalah satu segmen.
Estimasi maksimal data training:

`10 aktivitas x 6 train persons x 60 segmen = 3.600 baris training`

Dengan 360 fitur, rasionya sekitar:

`3.600 sampel / 360 fitur = 10 sampel per fitur`

Rasio ini masih masuk akal, tetapi tidak terlalu besar.
Artinya model bisa dilatih, tetapi risiko overfitting tetap perlu diperhatikan.

Konfigurasi random forest saat ini:
- `numTrees=300`
- `maxDepth=20`
- `featureSubsetStrategy='sqrt'`

`featureSubsetStrategy='sqrt'` membantu karena pada setiap split tree model hanya mempertimbangkan sekitar:

`sqrt(360) ~= 19 fitur`

Ini mengurangi risiko setiap tree terlalu bergantung pada semua fitur sekaligus.
Namun `maxDepth=20` cukup dalam untuk dataset berukuran ribuan baris, sehingga perlu dicek apakah model terlalu overfit.

Secara eksperimen, konfigurasi yang layak dibandingkan:
- `maxDepth`: 8, 12, 16, 20
- `numTrees`: 100, 200, 300

Jika skor train sangat tinggi tetapi skor test turun, kemungkinan model terlalu kompleks.
Karena split dilakukan berdasarkan `person_id`, evaluasi test pada person 7 dan 8 cukup penting untuk melihat generalisasi ke orang yang belum pernah dilihat model.

## Ringkasan alur training

Alurnya:
1. Load file sensor mentah
2. Tambah metadata aktivitas dan subjek
3. Tambah magnitude feature
4. Ekstrak statistik per segmen
5. Ambil `activity_label` sebagai target
6. Gabungkan semua fitur ke vektor
7. Standardisasi fitur
8. Latih `RandomForestClassifier`
9. Evaluasi dengan F1, accuracy, precision, recall
10. Simpan model dan artefak mapping label/fitur

## Kesimpulan singkat

Model ini adalah pipeline klasifikasi aktivitas manusia berbasis fitur statistik dari sensor wearable.

- **Random forest**: classifier ensemble yang mempelajari pola aktivitas dari fitur numerik
- **Input**: 360 fitur statistik hasil ekstraksi dari 60 sinyal sensor dasar + magnitude
- **Target**: `activity_label`
- **Preprocessing**: load file per segmen, tambah magnitude, ekstrak statistik, split by person, lalu scaling
