#!/usr/bin/env bash
set -euo pipefail

docker compose exec -T spark-master \
  sh -lc 'cd /opt/spark-apps/process && /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
    --master spark://spark-master:7077 \
    kafka_sensor_stream.py'
