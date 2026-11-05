#!/usr/bin/env bash
# ==============================================================================
# Spark 컨테이너 장애 인가 및 재시도 멱등성/정합성 검증 테스트 스크립트
# ==============================================================================
set -e

# 항상 프로젝트 루트 디렉터리 기준으로 작업 수행
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${PROJECT_ROOT}"

export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"

CATEGORY="CDs_and_Vinyl"
TARGET_YM="2023-01"

echo "======================================================================"
echo "[사전 점검] 정제 Parquet 및 기본 요약 테이블 준비 상태 확인"
echo "======================================================================"
if [ ! -d "data/processed/${CATEGORY}/review_year_month=${TARGET_YM}" ]; then
  echo "  -> [알림] 정제 Parquet 데이터가 없어 refine_store_parquet(${TARGET_YM})를 1회 선행 생성합니다..."
  docker exec airflow-worker /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --executor-memory 1600m \
    --driver-memory 1600m \
    --conf spark.sql.shuffle.partitions=32 \
    --conf spark.driver.host=airflow-worker \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.ui.showConsoleProgress=false \
    /opt/airflow/jobs/refine_reviews.py \
    --step refine_store_parquet \
    --year-month "${TARGET_YM}" \
    --category "${CATEGORY}" \
    --partitions 32 \
    --raw-dir /opt/airflow/data/raw/${CATEGORY}.jsonl \
    --raw-parquet-dir /opt/airflow/data/raw_parquet/${CATEGORY} \
    --processed-dir /opt/airflow/data/processed/${CATEGORY}
  echo "  -> [완료] 정제 Parquet 데이터 생성 완료!"
else
  echo "  -> [확인] 정제 Parquet(${CATEGORY}, ${TARGET_YM}) 준비 완료."
fi

SUMMARY_COUNT=$(docker exec postgres psql -U postgres -d postgres -t -A -c \
  "SELECT count(*) FROM monthly_review_summary WHERE category='${CATEGORY}' AND review_year_month='${TARGET_YM}';" || echo "0")
if [ "${SUMMARY_COUNT}" = "0" ] || [ -z "${SUMMARY_COUNT}" ]; then
  echo "  -> [알림] monthly_review_summary에 데이터가 없어 aggregate_monthly(${TARGET_YM})를 선행 실행합니다..."
  docker exec airflow-worker /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --executor-memory 1600m \
    --driver-memory 1600m \
    --conf spark.sql.shuffle.partitions=32 \
    --conf spark.driver.host=airflow-worker \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.ui.showConsoleProgress=false \
    --packages org.postgresql:postgresql:42.7.4 \
    /opt/airflow/jobs/refine_reviews.py \
    --step aggregate_monthly \
    --year-month "${TARGET_YM}" \
    --category "${CATEGORY}" \
    --partitions 32 \
    --processed-dir /opt/airflow/data/processed/${CATEGORY} \
    --summary-table public.monthly_review_summary > /dev/null 2>&1
  echo "  -> [완료] monthly_review_summary 초기화 완료!"
else
  echo "  -> [확인] monthly_review_summary(${CATEGORY}, ${TARGET_YM}) 준비 완료."
fi

echo ""
echo "======================================================================"
echo "[Step 0] 장애 테스트 전 현재 PostgreSQL 데이터 상태 확인"
echo "======================================================================"
BEFORE_COUNT=$(docker exec postgres psql -U postgres -d postgres -t -A -c \
  "SELECT count(*) FROM product_monthly_metrics WHERE category='${CATEGORY}' AND review_year_month='${TARGET_YM}';")
BEFORE_SUM=$(docker exec postgres psql -U postgres -d postgres -t -A -c \
  "SELECT COALESCE(sum(total_reviews), 0) FROM product_monthly_metrics WHERE category='${CATEGORY}' AND review_year_month='${TARGET_YM}';")

if [ "${BEFORE_COUNT}" -eq "0" ]; then
  echo "  -> [사전 적재] 마트 데이터가 비어있어 1차 적재를 진행합니다..."
  docker exec airflow-worker /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --executor-memory 1600m \
    --driver-memory 1600m \
    --conf spark.sql.shuffle.partitions=32 \
    --conf spark.driver.host=airflow-worker \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.ui.showConsoleProgress=false \
    --packages org.postgresql:postgresql:42.7.4 \
    /opt/airflow/jobs/product_analytics.py \
    --step all \
    --year-month "${TARGET_YM}" \
    --category "${CATEGORY}" \
    --partitions 32 \
    --processed-dir /opt/airflow/data/processed/${CATEGORY} \
    --metrics-table public.product_monthly_metrics \
    --segment-table public.product_segment_comparison > /dev/null 2>&1
  BEFORE_COUNT=$(docker exec postgres psql -U postgres -d postgres -t -A -c \
    "SELECT count(*) FROM product_monthly_metrics WHERE category='${CATEGORY}' AND review_year_month='${TARGET_YM}';")
  BEFORE_SUM=$(docker exec postgres psql -U postgres -d postgres -t -A -c \
    "SELECT COALESCE(sum(total_reviews), 0) FROM product_monthly_metrics WHERE category='${CATEGORY}' AND review_year_month='${TARGET_YM}';")
fi

echo "  - 대상 카테고리 / 년월: ${CATEGORY} / ${TARGET_YM}"
echo "  - 초기 상품 수: ${BEFORE_COUNT}개, 총 리뷰 수: ${BEFORE_SUM}건"
echo "----------------------------------------------------------------------"

echo ""
echo "======================================================================"
echo "[Step 1] Spark 집계 작업 실행 및 백그라운드 전환..."
echo "======================================================================"
docker exec airflow-worker /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --executor-memory 1600m \
  --driver-memory 1600m \
  --conf spark.sql.shuffle.partitions=32 \
  --conf spark.driver.host=airflow-worker \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.ui.showConsoleProgress=false \
  --packages org.postgresql:postgresql:42.7.4 \
  /opt/airflow/jobs/product_analytics.py \
  --step all \
  --year-month "${TARGET_YM}" \
  --category "${CATEGORY}" \
  --partitions 32 \
  --processed-dir /opt/airflow/data/processed/${CATEGORY} \
  --metrics-table public.product_monthly_metrics \
  --segment-table public.product_segment_comparison > /tmp/spark_fail_test.log 2>&1 &
SPARK_PID=$!
echo "  - Spark Job PID (호스트 서브셸): ${SPARK_PID}"

echo ""
echo "======================================================================"
echo "[Step 2] 5초 대기 후 Spark 워커(spark-worker-2) 강제 중단 (장애 발생!)"
echo "======================================================================"
sleep 5
echo "  -> [장애 인가] docker stop spark-worker-2 실행!"
docker stop spark-worker-2

echo "  -> Spark 작업 종료 대기 중 (장애 감지 또는 Failover 확인)..."
wait ${SPARK_PID} || true
echo "  -> 1차 작업 종료 (결과 로그 요약):"
tail -n 10 /tmp/spark_fail_test.log

echo ""
echo "======================================================================"
echo "[Step 3] 장애 직후 PostgreSQL 메인 마트 무결성 확인 (오염 여부 점검)"
echo "======================================================================"
MID_COUNT=$(docker exec postgres psql -U postgres -d postgres -t -A -c \
  "SELECT count(*) FROM product_monthly_metrics WHERE category='${CATEGORY}' AND review_year_month='${TARGET_YM}';")
MID_SUM=$(docker exec postgres psql -U postgres -d postgres -t -A -c \
  "SELECT COALESCE(sum(total_reviews), 0) FROM product_monthly_metrics WHERE category='${CATEGORY}' AND review_year_month='${TARGET_YM}';")
echo "  - 장애 발생 직후 상품 수: ${MID_COUNT}개, 총 리뷰 수: ${MID_SUM}건"
if [ "${BEFORE_COUNT}" -eq "${MID_COUNT}" ] && [ "${BEFORE_SUM}" -eq "${MID_SUM}" ]; then
  echo "  [검증 성공] Staging 테이블 격리 덕분에 메인 테이블에 불완전 데이터가 유입되지 않았습니다!"
else
  echo "  [주의] 데이터 수치 변경 감지됨"
fi

echo ""
echo "======================================================================"
echo "[Step 4] Spark 워커 컨테이너 복구 (docker start spark-worker-2)"
echo "======================================================================"
docker start spark-worker-2
echo "  -> 워커 재기동 및 클러스터 마스터 재등록 대기 (5초)..."
sleep 5

echo ""
echo "======================================================================"
echo "[Step 5] 재시도(Retry) 실행 - 복구 후 정상 완료 확인"
echo "======================================================================"
docker exec airflow-worker /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --executor-memory 1600m \
  --driver-memory 1600m \
  --conf spark.sql.shuffle.partitions=32 \
  --conf spark.driver.host=airflow-worker \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.ui.showConsoleProgress=false \
  --packages org.postgresql:postgresql:42.7.4 \
  /opt/airflow/jobs/product_analytics.py \
  --step all \
  --year-month "${TARGET_YM}" \
  --category "${CATEGORY}" \
  --partitions 32 \
  --processed-dir /opt/airflow/data/processed/${CATEGORY} \
  --metrics-table public.product_monthly_metrics \
  --segment-table public.product_segment_comparison
echo "  -> 재시도 작업 정상 완료!"

echo ""
echo "======================================================================"
echo "[Step 6] 최종 데이터 정합성 및 멱등성 검증 (Airflow validate_data)"
echo "======================================================================"
AFTER_COUNT=$(docker exec postgres psql -U postgres -d postgres -t -A -c \
  "SELECT count(*) FROM product_monthly_metrics WHERE category='${CATEGORY}' AND review_year_month='${TARGET_YM}';")
AFTER_SUM=$(docker exec postgres psql -U postgres -d postgres -t -A -c \
  "SELECT COALESCE(sum(total_reviews), 0) FROM product_monthly_metrics WHERE category='${CATEGORY}' AND review_year_month='${TARGET_YM}';")

echo "  - 재시도 완료 후 상품 수: ${AFTER_COUNT}개 (초기값: ${BEFORE_COUNT}개)"
echo "  - 재시도 완료 후 리뷰 합: ${AFTER_SUM}건 (초기값: ${BEFORE_SUM}건)"

if [ "${BEFORE_COUNT}" -eq "${AFTER_COUNT}" ] && [ "${BEFORE_SUM}" -eq "${AFTER_SUM}" ]; then
  echo "  [최종 판정: SUCCESS] 중복 데이터 없이 100% 멱등성과 정합성이 완벽히 유지되었습니다!"
else
  echo "  [최종 판정: MISMATCH] 수치 불일치 발생!"
fi

echo "----------------------------------------------------------------------"
echo "Airflow 품질 검증 함수(validate_postgres_summary) 실행 결과:"
docker exec airflow-worker python3 -c "
from dags.dag import validate_postgres_summary
validate_postgres_summary(params={'category': '${CATEGORY}', 'year_month': '${TARGET_YM}'})
"
echo "======================================================================"
