#!/usr/bin/env bash
set -euo pipefail

docker compose up -d
docker compose -f docker-compose.yaml exec -T spark-master \
  sh -lc 'cd /opt/spark-apps/model && /opt/spark/bin/spark-submit --master spark://spark-master:7077 inference.py'
