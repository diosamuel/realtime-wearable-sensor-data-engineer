#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.yaml"

docker compose -f "${COMPOSE_FILE}" exec -T spark-master \
  sh -lc 'cd /opt/spark-apps/model && /opt/spark/bin/spark-submit --master spark://spark-master:7077 inference.py'
