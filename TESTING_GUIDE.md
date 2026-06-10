# Panduan Pengujian Sistem Monitoring Aktivitas Real-time (Kelompok 21)

Dokumen ini menjelaskan langkah-langkah detail untuk menjalankan dan menguji sistem klasifikasi aktivitas fisik dan pemantauan durasi sedentari sesuai dengan laporan.

## Prasyarat
- Docker & Docker Compose
- Python 3.10+
- MQTT Broker (Mosquitto) terinstal secara lokal (jika tidak menggunakan Docker untuk MQTT)

---

## Langkah 1: Persiapan Infrastruktur
Jalankan seluruh layanan *stack* menggunakan Docker Compose:
```bash
docker compose up -d
```
Tunggu hingga seluruh container (Kafka, Spark, Postgres, MinIO, Airflow) berstatus `Running`.

## Langkah 2: Inisialisasi Database
Langkah ini wajib dilakukan untuk membuat tabel operasional dan tabel fakta sesuai desain *star schema*:
```bash
docker exec -i tubes-postgres-olap psql -U admin -d warehouse < postgres_init.sql
```

## Langkah 3: Pelatihan Model (Training)
Pastikan dataset UCI (folder `a01`, `a02`, dsb) sudah ada di folder `./data`. Jalankan proses training untuk menghasilkan model Random Forest:
```bash
bash train.sh
```
Langkah ini akan menghasilkan folder `spark_rf_pipeline_model` di dalam `/opt/spark-data/HAR_SmartHealth/`.

## Langkah 4: Menjalankan Pipeline Streaming
Buka terminal baru dan jalankan engine streaming Spark:
```bash
bash run_streaming.sh
```
Pipeline ini akan mulai mendengarkan topik Kafka `wearable.sensor.raw`, melakukan ekstraksi fitur windowed, dan melakukan prediksi.

## Langkah 5: Simulasi Pengiriman Data
Buka dua terminal baru untuk menjalankan simulator sensor dan jembatan MQTT-to-Kafka:

1.  **Terminal Simulator:** (Mengirim data sensor dengan timestamp asli)
    ```bash
    python simulate_sensor.py
    ```
2.  **Terminal Bridge:** (Meneruskan data dari MQTT ke Kafka)
    ```bash
    python spark-apps/ingest/mqttToKafka.py
    ```

## Langkah 6: Verifikasi Hasil
Anda dapat memantau hasil secara real-time melalui PostgreSQL:

1.  **Cek Monitoring Real-time:**
    ```sql
    SELECT * FROM realtime_activity_monitor ORDER BY sensor_event_time DESC LIMIT 10;
    ```
    Perhatikan kolom `sedentary_duration_minutes` dan `alert_status`.

2.  **Cek Agregasi Historis (Fact Table):**
    ```sql
    SELECT * FROM fact_activity_daily_summary;
    ```

3.  **Cek State Sedentari:**
    ```sql
    SELECT * FROM activity_state;
    ```

---

## Troubleshooting
- **Error Kafka:** Jika Spark gagal terhubung ke Kafka, pastikan container `tubes-kafka-broker` sudah benar-benar siap (cek logs: `docker logs tubes-kafka-broker`).
- **Error JDBC:** Jika muncul error driver PostgreSQL, script `run_streaming.sh` akan otomatis mencoba mengunduh package `org.postgresql:postgresql:42.7.3`.
- **Reset Data:** Untuk memulai dari awal, hapus volume Postgres: `docker compose down -v`.
