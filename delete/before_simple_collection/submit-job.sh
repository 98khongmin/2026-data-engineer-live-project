#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 첫 번째 인자는 Python 파일, 나머지는 해당 작업의 인자
JOB_SCRIPT="${1:?사용법: ./submit-job.sh jobs/파일.py [추가 인자]}"
JOB_SCRIPT="${JOB_SCRIPT#./}"
JOB_SCRIPT="${JOB_SCRIPT#jobs/}"
shift

JOB_NAME="${JOB_NAME:-$(basename "$JOB_SCRIPT" .py)}"
SPARK_CONTAINER="${SPARK_CONTAINER:-amazon-review-pipeline-spark-master-1}"

echo "Submitting $JOB_SCRIPT in $SPARK_CONTAINER"

exec docker exec \
  --env-file "$PROJECT_ROOT/.env" \
  -e "JOB_NAME=$JOB_NAME" \
  -e "SPARK_DRIVER_HOST=${SPARK_DRIVER_HOST:-spark-master}" \
  -e "SPARK_MASTER_URL=${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  -e POSTGRES_HOST=postgres \
  -e POSTGRES_PORT=5432 \
  "$SPARK_CONTAINER" \
  /bin/bash /opt/spark/scripts/run-job.sh "$JOB_SCRIPT" "$@"