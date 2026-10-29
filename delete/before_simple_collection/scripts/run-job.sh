#!/usr/bin/env bash
# Container entry point shared by Compose and a future Airflow DockerOperator.
set -euo pipefail

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  echo "Usage: bash /opt/spark/scripts/run-job.sh [script.py] [job arguments ...]"
  exit 0
fi

JOB_SCRIPT="${1:-${JOB_SCRIPT:-amazon_reviews_etl.py}}"
if [[ $# -gt 0 ]]; then shift; fi
JOB_SCRIPT="${JOB_SCRIPT#./}"
JOB_SCRIPT="${JOB_SCRIPT#jobs/}"
JOB_SCRIPT="${JOB_SCRIPT#/opt/spark/app/}"
APP_DIR="${SPARK_APP_DIR:-/opt/spark/app}"

case "/${JOB_SCRIPT}/" in
  *"/../"*|*"//"*)
    echo "Error: specify a relative Python script inside the app directory." >&2
    exit 2
    ;;
esac
if [[ "$JOB_SCRIPT" != *.py || ! -f "$APP_DIR/$JOB_SCRIPT" ]]; then
  echo "Error: Python job not found: $APP_DIR/$JOB_SCRIPT" >&2
  exit 2
fi

JOB_NAME="${JOB_NAME:-$(basename -- "$JOB_SCRIPT" .py)}"
echo "Submitting $APP_DIR/$JOB_SCRIPT (name: $JOB_NAME)"

# Resource settings live in spark-defaults.conf. PostgreSQL JDBC is in the image.
# A future Airflow container must have a DNS name reachable from both workers;
# set SPARK_DRIVER_HOST to that container's name on the shared Docker network.
# exec preserves Spark's exit status and forwards termination signals to it.
exec "${SPARK_HOME:-/opt/spark}/bin/spark-submit" \
  --name "$JOB_NAME" \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --deploy-mode client \
  --conf spark.dynamicAllocation.enabled=false \
  --conf "spark.driver.host=${SPARK_DRIVER_HOST:-spark-job}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  "$APP_DIR/$JOB_SCRIPT" "$@"
