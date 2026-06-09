# Realtime Wearable Sensor Data Engineering Pipeline

A real-time data engineering pipeline that ingests wearable sensor data via MQTT, processes it with Spark MLlib for activity recognition, and serves analytics through a Tableau dashboard backed by PostgreSQL.

Dataset: [Daily and Sports Activities — UCI ML Repository](https://archive.ics.uci.edu/dataset/256/daily+and+sports+activities)

---

## Architecture

> **Note:** For detailed information on the database schema, table grains, and dashboard mapping, please refer to the [Data Contract (Phase 1)](./DATA_CONTRACT.md).

![Architecture](./architecture.png)

---

## Stack

| Layer | Technology |
|---|---|
| Ingestion | MQTT (Mosquitto — external) |
| Message Broker | Apache Kafka (KRaft mode) + Schema Registry |
| Object Storage | MinIO (S3-compatible) |
| Stream Processing | Apache Spark 3.5.1 + MLlib |
| Data Warehouse | PostgreSQL 15 |
| Orchestration | Apache Airflow 2.9.1 |
| Dashboard | Tableau |

---

## Services

| Service | Port | Description |
|---|---|---|
| Kafka Broker | `9092` | Message broker (KRaft, no Zookeeper) |
| Schema Registry | `8083` | Avro schema management |
| Kafka UI | `8080` | Web UI for Kafka topics and messages |
| MinIO | `9000` / `9002` | S3-compatible object storage + console |
| Spark Master | `8081` / `7077` | Spark cluster master UI and submit port |
| PostgreSQL (OLAP) | `5432` | Data warehouse for aggregated results |
| Tableau | `8501` | Analytics dashboard |
| Airflow Webserver | `8084` | DAG management and monitoring |

> **Note:** Mosquitto MQTT broker runs as an external service on port `1883`.

## Database Schema Initialization

The PostgreSQL Data Warehouse schema is defined in Phase 1 Data Contract and implemented via an init script.

### 1. Schema Location
The DDL script is located at: `sql/init.sql`. 
It contains both the operational table (`realtime_activity_monitor`) and the Data Warehouse schema (`fact_activity_daily_summary`, dimensions, etc.).

### 2. How to Initialize
The schema is automatically created the *first time* the `postgres` container starts. 
If you need to recreate the database to apply new schema changes, because we use a local bind mount, you must manually clear the data folder before restarting:

```bash
docker compose down -v

# Windows (PowerShell):
Remove-Item -Recurse -Force .\postgres\data\*
# Linux/macOS/Git Bash:
# rm -rf ./postgres/data/*

docker compose up -d postgres
docker compose logs -f postgres
```
*(Wait until you see "database system is ready to accept connections")*

### 3. Verify the Schema
You can verify the tables were created successfully by running:

```bash
docker exec -it tubes-postgres-olap psql -U admin -d warehouse -c "\dt"
```

To verify the `dim_activity` seed data:
```bash
docker exec -it tubes-postgres-olap psql -U admin -d warehouse -c "SELECT * FROM dim_activity;"
```

## Phase 3B: Streaming Inference Test

This section outlines the verification steps for the teammate testing the end-to-end inference and PostgreSQL write layer.

### 1. Start Infrastructure & Train Model
Make sure your Kafka broker, PostgreSQL, and Spark master are running:
```bash
docker compose up -d
```
Since inference requires a trained model, ensure you train the model once via Spark before starting the stream:
```bash
docker exec -it tubes-spark-worker /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/model/train.py
```

### 2. Run the Simulator
Run the Python script on your host machine to simulate sensor data and publish directly to Kafka:
```bash
# Ensure kafka-python is installed (pip install kafka-python)
PUBLISH_TARGET=kafka KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python simulate_sensor.py
```
> **Disclaimer on Streaming ML:** For this demo phase (fastest route), `simulate_sensor.py` generates dummy values and *mocks* the 360 statistical window features (`_mean`, `_std`, etc.) directly per second. In a true production environment, the raw sensor data would be sent, and Spark Structured Streaming would use stateful window aggregations (e.g., tumbling windows) to compute these 360 features before inference.

*(PowerShell users: `$env:PUBLISH_TARGET="kafka"; $env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"; python simulate_sensor.py`)*

### 3. Run Spark Streaming Inference
In a separate terminal, submit the Spark job inside the Spark worker container to consume the Kafka topic. Note that we add the PostgreSQL JDBC driver dependency:
```bash
docker exec -it tubes-spark-worker /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --master spark://spark-master:7077 \
  /opt/spark-apps/process/kafka_sensor_stream.py
```

### 4. Verify in PostgreSQL
Connect to PostgreSQL and query the `realtime_activity_monitor` table:
```bash
docker exec -it tubes-postgres-olap psql -U admin -d warehouse -c "SELECT person_id, sensor_event_time, predicted_activity_label, activity_category, confidence_score, predicted_at, stored_at FROM realtime_activity_monitor ORDER BY stored_at DESC LIMIT 10;"
```
Check the total row count:
```bash
docker exec -it tubes-postgres-olap psql -U admin -d warehouse -c "SELECT COUNT(*) FROM realtime_activity_monitor;"
```

### Teammate Troubleshooting Checklist
- [ ] **Kafka:** Is `simulate_sensor.py` successfully printing JSON payloads to the console?
- [ ] **Spark Parse:** Does the Spark stream start without errors regarding `feature_cols.pkl` missing?
- [ ] **Model Loading:** Are there any errors about `PipelineModel.load`?
- [ ] **Spark Inference:** Are there any schema mismatch errors during `model.transform()`?
- [ ] **PostgreSQL Sink:** Does the stream process micro-batches without JDBC connection refused errors?
- [ ] **Output:** Does the SQL query return rows with valid `predicted_activity_label` and `confidence_score` (between 0-1)?

---

## Getting Started

### Prerequisites
- Docker and Docker Compose installed
- Mosquitto running externally on port `1883`

### Run the stack

```bash
docker compose up -d
```

### Access services

- Kafka UI: http://localhost:8080
- MinIO Console: http://localhost:9002 — `admin / password123`
- Spark Master UI: http://localhost:8081
- Airflow: http://localhost:8084 — `admin / admin`
- Tableau Dashboard: http://localhost:8501

---

## Pipeline Flow

```
Wearable Sensors
      │
      ▼
MQTT Broker (external)
      │
      ▼
Kafka Topic (raw sensor data)
      │
      ├──► MinIO (raw data lake)
      │
      ▼
Spark Streaming + MLlib
(activity classification)
      │
      ├──► MinIO (processed data)
      │
      ▼
PostgreSQL (aggregated results)
      │
      ▼
Tableau Dashboard
```

---

## Dataset

The pipeline simulates 19 daily and sports activities (sitting, walking, running, cycling, etc.) recorded by 45 sensors across 5 body units at 25 Hz.

Source: [UCI Daily and Sports Activities Dataset](https://archive.ics.uci.edu/dataset/256/daily+and+sports+activities)

| Abbr | Long Name | Lokasi       |
| ---- | --------- | ------------ |
| T    | Torso     | Badan / dada |
| RA   | Right Arm | Lengan kanan |
| LA   | Left Arm  | Lengan kiri  |
| RL   | Right Leg | Kaki kanan   |
| LL   | Left Leg  | Kaki kiri    |
