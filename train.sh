#!/usr/bin/env bash
set -euo pipefail

docker compose up -d
docker compose -f docker-compose.yaml exec -T spark-master \
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
