# Struktur Dashboard
## Halaman 1: Realtime Sensor Analytics

`INPUT`
- Data sensor dikirim ke PostgreSQL setiap 1 detik.
- Data berasal dari 1 orang yang sedang dipantau.
- Data dipakai untuk membaca kondisi aktivitas secara real-time.

`OUTPUT`
- Prediksi orang sedang melakukan kegiatan apa.
- Status aktivitas terbaru yang sedang berjalan.
- Informasi update terakhir dari sensor.

`CHART`
- Time series sensor.
- Card aktivitas orang.
- Optional: timestamp last update dan status prediksi terakhir.

## Halaman 2: Aggregation Dashboard

`INPUT`
- Data gold layer yang sudah dibersihkan.
- Data ini bukan real-time.
- Data berasal dari dataset yang sudah diproses dan dipakai untuk modelling.

`OUTPUT`
- Ringkasan agregasi dari data historis.
- Insight per aktivitas, per person, atau per waktu.
- Jawaban untuk desain star schema, grain fakta, dan dimensi.

`CHART`
- Bar chart untuk perbandingan antar aktivitas.
- Pie chart atau stacked bar untuk distribusi kategori.
- Table summary untuk ringkasan agregat.
- Optional: line chart untuk tren waktu.

## Halaman 3: Model Monitoring

`INPUT`
- Hasil evaluasi model.
- Metrik training/testing model.

`OUTPUT`
- Kualitas model secara sederhana.
- Nilai performa utama seperti Confusion Matrix.
- Keputusan cepat apakah model masih layak dipakai.

`CHART`
- KPI card untuk Confusion Matrix.
- Card tambahan untuk accuracy jika diperlukan.
- Optional: bar chart untuk membandingkan metrik model.

## Ringkasan Singkat

- Halaman 1: realtime sensor analytics untuk 1 orang.
- Halaman 2: aggregation dashboard dari gold layer data.
- Halaman 3: model monitoring dengan metrik sederhana.
