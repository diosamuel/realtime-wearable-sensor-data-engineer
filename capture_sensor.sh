#!/usr/bin/env bash
set -euo pipefail

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
  sh -lc 'cd /opt/spark-apps/ingest && KAFKA_BOOTSTRAP_SERVERS=broker:29092 /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --master spark://spark-master:7077 consumeSpark.py'
