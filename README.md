# Realtime Wearable Sensor Pipeline

Simple realtime data pipeline for wearable sensor activity recognition.

The project simulates wearable sensor data, sends it through MQTT and Kafka, processes it with Spark, stores artifacts in MinIO, and runs Spark MLlib inference from a custom CSV sample.

Dataset reference: [Daily and Sports Activities - UCI ML Repository](https://archive.ics.uci.edu/dataset/256/daily+and+sports+activities)

## Stack

| Layer | Tool |
|---|---|
| Sensor simulation | Python |
| MQTT broker | Eclipse Mosquitto |
| Streaming broker | Kafka KRaft + Schema Registry |
| Stream processing / ML | Apache Spark 3.5.1 + MLlib |
| Object storage | MinIO |
| Warehouse | PostgreSQL |
| Orchestration | Airflow |
| Dashboard | Tableau |

## Main Services

| Service | URL / Port | Notes |
|---|---|---|
| Kafka UI | http://localhost:8080 | Kafka topics and messages |
| MinIO Console | http://localhost:9002 | `admin / password123` |
| Spark Master UI | http://localhost:8081 | Spark cluster UI |
| Airflow | http://localhost:8084 | `admin / admin` |
| PostgreSQL | `localhost:5432` | `admin / password123`, DB `warehouse` |
| Tableau | http://localhost:8501 | Dashboard |

## Start Services

```bash
docker compose up -d
```

This builds the Spark image, starts the services, and mounts:

| Local path | Container path |
|---|---|
| `./spark-apps` | `/opt/spark-apps` |
| `./data` | `/data` |
| `./spark-data` | `/opt/spark-data` |

## 1. Simulate Realtime Sensor Data

Start the required services and run the Spark streaming consumer:

```bash
docker compose up -d \
  mqtt-broker \
  kafka-broker \
  kafka-schema-registry \
  minio \
  spark-master \
  spark-worker \
  mqtt-to-kafka \
  sensor-simulator

docker compose exec -T spark-master \
  sh -lc 'cd /opt/spark-apps/ingest && KAFKA_BOOTSTRAP_SERVERS=broker:29092 /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --master spark://spark-master:7077 \
    consumeSpark.py'
```

Shortcut:

```bash
./capture_sensor.sh
```

## 2. Train Model

Run model training through Spark submit inside the Spark master container:

```bash
docker compose up -d

docker compose exec -T spark-master \
  sh -lc 'cd /opt/spark-apps/model && /opt/spark/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=admin \
    --conf spark.hadoop.fs.s3a.secret.key=password123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --master spark://spark-master:7077 \
    train_model.py'
```

Shortcut:

```bash
./train.sh
```

Training writes model artifacts to MinIO under:

```text
model_artifact/spark_rf_pipeline_model
model_artifact/label_mapping.pkl
model_artifact/feature_cols.pkl
```

## 3. Run Inference Example

The example reads simulated custom sensor data from:

```text
data/custom_sensor.csv
```

Inside Docker this file is available as:

```text
/data/custom_sensor.csv
```

Run inference with `spark-submit`:

```bash
docker compose up -d

docker compose exec -T spark-master \
  sh -lc 'cd /opt/spark-apps/inference && INPUT_PATH=/data/custom_sensor.csv /opt/spark/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=admin \
    --conf spark.hadoop.fs.s3a.secret.key=password123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --master spark://spark-master:7077 \
    testing.py'
```

The script prints:

```text
predicted_label
prediction
probability
```

## Stop Services

```bash
docker compose down
```
