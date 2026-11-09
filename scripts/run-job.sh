#!/usr/bin/env bash
set -euo pipefail

JOB_SCRIPT="${1:-collector.py}"
JOB_SCRIPT="${JOB_SCRIPT#jobs/}"
if [[ $# -gt 0 ]]; then shift; fi

# 수집은 일반 Python, Spark 작업은 클러스터로 제출합니다.
if [[ "$JOB_SCRIPT" == "refine_reviews.py" || "$JOB_SCRIPT" == "product_analytics.py" || "$JOB_SCRIPT" == "sentiment_analysis.py" || "$JOB_SCRIPT" == *"spark"* ]]; then
  exec /opt/spark/bin/spark-submit \
    --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
    --deploy-mode client \
    --executor-memory 1600m \
    --driver-memory 1600m \
    --conf spark.sql.shuffle.partitions=32 \
    --conf "spark.driver.host=${SPARK_DRIVER_HOST:-spark-master}" \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.ui.showConsoleProgress=false \
    --conf spark.speculation=false \
    "/opt/spark/app/$JOB_SCRIPT" "$@"
fi

exec python3 -u "/opt/spark/app/$JOB_SCRIPT" "$@"
