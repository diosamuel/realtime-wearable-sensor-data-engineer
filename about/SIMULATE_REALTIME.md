# Simulate Realtime Pipeline

Pipeline flow:

`simulate mqtt -> transform mqtt to topic kafka -> capture topic to spark streaming`

## Approach

The clean way to run these three stages together is:

1. Put MQTT, Kafka, and Spark on the same Docker Compose network.
2. Run the simulator as a container that publishes to MQTT service hostname `mqtt-broker`.
3. Run the MQTT-to-Kafka bridge as a container that reads from `mqtt-broker` and writes to Kafka service hostname `broker:29092`.
4. Run `consumeSpark.py` through `spark-submit` inside the Spark cluster.

This repo now supports that layout with:

- `mqtt-broker` service in `docker-compose.yaml`
- `sensor-simulator` service in `docker-compose.yaml`
- `mqtt-to-kafka` service in `docker-compose.yaml`
- `run_realtime_pipeline.sh` to start the pipeline

## Step by Step

1. Start the required infrastructure first.

   This project now runs MQTT, Kafka, and Spark in Docker Compose on one network.

   Start the realtime infrastructure:

   ```bash
   docker compose up -d mqtt-broker kafka-broker kafka-schema-registry spark-master spark-worker
   ```

2. Run the sensor simulator.

   The simulator publishes wearable sensor data into MQTT topic `simulate/sensor`.

   Local run:

   ```bash
   python3 simulate_sensor.py
   ```

   Container run:

   ```bash
   docker compose up -d sensor-simulator
   ```

3. Run the MQTT to Kafka bridge.

   This service subscribes to MQTT, transforms the payload into JSON, then writes it to Kafka topic `wearable.sensor.raw`.

   Local run:

   ```bash
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
   MQTT_HOST=localhost \
   python3 spark-apps/ingest/mqttToKafka.py
   ```

   Container run:

   ```bash
   docker compose up -d mqtt-to-kafka
   ```

4. Run the Spark Structured Streaming consumer.

   This reads Kafka topic `wearable.sensor.raw`, parses the JSON record, prints the streamed sensor data,
   writes parquet batches locally, and uploads each batch to MinIO through `process/setupMinio.py`.

   ```bash
   docker compose exec -T spark-master \
     sh -lc 'cd /opt/spark-apps/ingest && KAFKA_BOOTSTRAP_SERVERS=broker:29092 /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --master spark://spark-master:7077 consumeSpark.py'
   ```

   Default parquet locations:

   - Local: `/data/raw/current_sensor/batch_id=<id>`
   - MinIO: `s3://wearable-sensor-demo/model_artifact/current_sensor/batch_id=<id>`

5. Run the whole pipeline with one command.

   ```bash
   bash run_realtime_pipeline.sh
   ```

## Execution Order

1. `simulate_sensor.py` sends sensor data to MQTT.
2. `spark-apps/ingest/mqttToKafka.py` reads MQTT and writes Kafka topic `wearable.sensor.raw`.
3. `spark-apps/ingest/consumeSpark.py` reads Kafka with Spark Structured Streaming and prints the stream.

## Container Mapping

- MQTT broker: `mqtt-broker`
- Simulator publisher: `sensor-simulator`
- MQTT to Kafka bridge: `mqtt-to-kafka`
- Kafka broker inside network: `broker:29092`
- Spark master: `spark-master`

## Recommended Path

Use Docker Compose for all networked components, and keep Spark consumption submitted through `spark-submit`.

The practical command is:

```bash
bash run_realtime_pipeline.sh
```
