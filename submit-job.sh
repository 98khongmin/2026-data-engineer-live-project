#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 첫 번째 인자는 Python 파일, 나머지는 해당 작업의 인자
JOB_SCRIPT="${1:?사용법: ./submit-job.sh jobs/파일.py [추가 인자]}"
JOB_SCRIPT="${JOB_SCRIPT#./}"
JOB_SCRIPT="${JOB_SCRIPT#jobs/}"
shift

SPARK_CONTAINER="${SPARK_CONTAINER:-amazon-review-pipeline-spark-master-1}"

echo "Submitting $JOB_SCRIPT in $SPARK_CONTAINER"

# 실제 터미널에서만 TTY를 전달해야 tqdm이 한 줄로 갱신됩니다.
# 로그 파일로 보내거나 Airflow에서 실행하면 TTY 없이 실행합니다.
DOCKER_COMMAND=(docker exec)
if [[ -t 1 && -t 2 ]]; then
  DOCKER_COMMAND+=(-t -e SHOW_PROGRESS=1)
else
  DOCKER_COMMAND+=(-e SHOW_PROGRESS=0)
fi

exec "${DOCKER_COMMAND[@]}" \
  --env-file "$PROJECT_ROOT/.env" \
  -e POSTGRES_HOST=postgres \
  -e POSTGRES_PORT=5432 \
  -e "SPARK_DRIVER_HOST=${SPARK_DRIVER_HOST:-spark-master}" \
  -e "SPARK_MASTER_URL=${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  "$SPARK_CONTAINER" \
  /bin/bash /opt/spark/scripts/run-job.sh "$JOB_SCRIPT" "$@"
