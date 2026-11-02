"""5회차 과제: Airflow DAG 설계 및 구현 - Amazon 리뷰 월별 요약 집계 파이프라인.

DAG 구조 (단일 DAG 4단계 순차 체이닝):
    download_raw_data >> refine_store_parquet >> aggregate_monthly >> validate_data

스케줄 & 증분 처리 (사전 감지 스킵 탑재):
    - 매월 1일 (schedule="@monthly") - 논리적 날짜(data_interval_start) 기준 전월 1달 치 데이터 처리
    - 이미 대상 월의 Parquet 파티션이 존재하는 경우 원본 JSONL 정제를 자동으로 스킵(0.1초)하여 백필 속도 극대화
"""
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def notify_failure(context):
    """태스크 실패 시 호출되는 알림 콜백 (현재는 콘솔 [ALERT] 로그로 기록되며, 향후 웹훅 연동 지점)."""
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown_task"
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    print(f"[ALERT] Task '{task_id}' 실패 감지! 실행일자: {execution_date}, 오류 내용: {exception}")


def validate_postgres_summary(**context):
    """PostgreSQL monthly_review_summary 테이블 데이터 품질 및 멱등성 검증."""
    params = context.get("params", {})
    target_ym = params.get("year_month")
    if not target_ym or target_ym.lower() in ("auto", ""):
        data_interval_start = context.get("data_interval_start")
        if data_interval_start:
            target_ym = data_interval_start.strftime("%Y-%m")
        else:
            logical_date = context.get("logical_date")
            target_ym = logical_date.strftime("%Y-%m") if logical_date else None

    logical_date = context.get("logical_date")
    data_interval_start = context.get("data_interval_start")
    data_interval_end = context.get("data_interval_end")
    print(f"[Airflow 실행 날짜 정보] logical_date={logical_date}, data_interval_start={data_interval_start}, data_interval_end={data_interval_end}, target_ym={target_ym}")

    import psycopg2
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )
    try:
        with conn.cursor() as cur:
            # 1. 전체 집계 현황 검증
            cur.execute("""
                SELECT 
                    count(*) AS total_months,
                    min(review_year_month) AS earliest_month,
                    max(review_year_month) AS latest_month,
                    count(*) FILTER (WHERE total_reviews <= 0) AS invalid_reviews_count,
                    count(*) FILTER (WHERE avg_rating < 1.0 OR avg_rating > 5.0) AS invalid_rating_count,
                    max(updated_at) AS last_updated_at
                FROM public.monthly_review_summary;
            """)
            row = cur.fetchone()
            total_months, earliest, latest, invalid_reviews, invalid_ratings, last_updated = row
            if total_months == 0:
                raise ValueError("monthly_review_summary 테이블에 적재된 데이터가 없습니다!")
            if invalid_reviews > 0:
                raise ValueError(f"비정상 리뷰 건수: {invalid_reviews}건 (리뷰 수 <= 0인 데이터 발견)")
            if invalid_ratings > 0:
                raise ValueError(f"평점 범위(1.0~5.0)를 벗어난 데이터가 {invalid_ratings}건 존재합니다!")

            print(f"[품질 검증] 누적 총 집계 월수: {total_months:,}개월 (집계 기간: {earliest} ~ {latest})")
            print(f"[품질 검증] 비정상 평점 건수: {invalid_ratings}건, 최근 갱신 시각: {last_updated}")

            # 2. 지정된 대상 월 증분 검증 (특정 월 실행 시)
            if target_ym and target_ym.lower() not in ("all", "auto", ""):
                cur.execute("""
                    SELECT total_reviews, avg_rating, verified_reviews, unique_products, unique_users, updated_at
                    FROM public.monthly_review_summary
                    WHERE review_year_month = %s;
                """, (target_ym,))
                month_row = cur.fetchone()
                if not month_row:
                    raise ValueError(
                        f"대상 월({target_ym}) 데이터가 monthly_review_summary에 존재하지 않습니다! "
                        f"(참고: 원본 데이터셋은 2000-11 ~ 2023-09 구간의 데이터만 포함하고 있습니다)"
                    )

                m_revs, m_rating, m_verif, m_prods, m_users, m_updated = month_row
                print(f"[증분 적재 검증 성공] 대상 월: {target_ym}")
                print(f"  - 리뷰 수: {m_revs:,}건, 평균 평점: {m_rating}점 (구매 인증: {m_verif:,}건)")
                print(f"  - 상품 수: {m_prods:,}개, 작성자 수: {m_users:,}명, 최종 갱신: {m_updated}")
    finally:
        conn.close()


default_args = {
    "owner": "data_engineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="monthly_amazon_reviews_pipeline",
    default_args=default_args,
    description="월별 Amazon 리뷰 다운로드, Spark 정제(Parquet), 월별 요약 집계(PostgreSQL UPSERT) 및 품질 검증 파이프라인",
    schedule="@monthly",  # 매월 1일 정기 자동 실행
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 10, 1),  # 데이터셋 최종 월(2023-09) 마감 시점 (현재 시점 불필요한 자동 스케줄 방지)
    catchup=False,
    max_active_runs=1,
    params={
        "year_month": Param(
            default="auto",
            type="string",
            description="처리할 대상 년월 (YYYY-MM). 'auto'는 선택한 논리적 날짜/백필 구간의 월을 자동 처리합니다. 특정 월 강제 지정 시 '2023-01' 또는 전체 실행 시 'all'을 입력하세요.",
        ),
    },
    tags=["amazon", "reviews", "spark", "monthly", "incremental", "aggregate"],
) as dag:

    # 1. 원본 데이터 다운로드 (외부 네트워크 의존성 -> 3회 재시도 + 지수 백오프)
    download_raw_data = BashOperator(
        task_id="download_raw_data",
        bash_command="python3 -u /opt/airflow/jobs/collector.py --raw-dir /opt/airflow/data/raw --states-dir /opt/airflow/states",
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
        doc_md="""
        ### download_raw_data
        * **역할**: Hugging Face 외부 원본 JSONL 데이터를 원본 저장소(`data/raw/`) 디렉토리에 스트리밍 다운로드 (이미 존재 시 자동 스킵)
        * **재시도 전략**: 외부 네트워크 통신 일시 장애 대비 3회 재시도 및 지수 백오프(2분 -> 4분 -> 8분)
        """,
    )

    # 2. Spark 분산 정제 및 월별 Parquet 저장 (사전 감지 스킵: 이미 파티션 존재 시 0.1초 스킵)
    refine_store_parquet = BashOperator(
        task_id="refine_store_parquet",
        bash_command="""
        {% if params.year_month and params.year_month not in ('auto', '') %}
            {% set ym = params.year_month %}
        {% elif data_interval_start %}
            {% set ym = data_interval_start.strftime('%Y-%m') %}
        {% elif logical_date %}
            {% set ym = logical_date.strftime('%Y-%m') %}
        {% else %}
            {% set ym = '2023-01' %}
        {% endif %}
        echo "[Airflow 실행 날짜 정보] logical_date={{ logical_date }}, data_interval_start={{ data_interval_start }}, target_ym={{ ym }}"
        /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --conf spark.driver.host=airflow-worker \
            --conf spark.driver.bindAddress=0.0.0.0 \
            --conf spark.ui.showConsoleProgress=false \
            /opt/airflow/jobs/refine_reviews.py \
            --step refine_store_parquet \
            --year-month "{{ ym }}" \
            --raw-dir /opt/airflow/data/raw \
            --processed-dir /opt/airflow/data/processed/reviews
        """,
        retries=1,
        retry_delay=timedelta(minutes=5),
        doc_md="""
        ### refine_store_parquet
        * **역할**: 원본 JSONL에서 대상 년월의 데이터를 선별하여 결측치 필터링, UUID5 중복 제거 후 월별 파티션 Parquet 형식으로 저장 (이미 존재 시 자동 스킵)
        * **재시도 전략**: 일시적 클러스터 리소스 부족(OOM) 대비 1회 재시도, 반복 실패 시 로직/데이터 이슈로 판단하여 즉시 중단 및 알림
        """,
    )

    # 3. 월별 요약 집계 및 PostgreSQL UPSERT 적재 (해당 월 1개 행 증분 반영)
    aggregate_monthly = BashOperator(
        task_id="aggregate_monthly",
        bash_command="""
        {% if params.year_month and params.year_month not in ('auto', '') %}
            {% set ym = params.year_month %}
        {% elif data_interval_start %}
            {% set ym = data_interval_start.strftime('%Y-%m') %}
        {% elif logical_date %}
            {% set ym = logical_date.strftime('%Y-%m') %}
        {% else %}
            {% set ym = '2023-01' %}
        {% endif %}
        echo "[Airflow 실행 날짜 정보] logical_date={{ logical_date }}, data_interval_start={{ data_interval_start }}, target_ym={{ ym }}"
        /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --conf spark.driver.host=airflow-worker \
            --conf spark.driver.bindAddress=0.0.0.0 \
            --conf spark.ui.showConsoleProgress=false \
            --packages org.postgresql:postgresql:42.7.4 \
            /opt/airflow/jobs/refine_reviews.py \
            --step aggregate_monthly \
            --year-month "{{ ym }}" \
            --processed-dir /opt/airflow/data/processed/reviews \
            --summary-table public.monthly_review_summary
        """,
        retries=2,
        retry_delay=timedelta(minutes=3),
        doc_md="""
        ### aggregate_monthly
        * **역할**: 정제된 Parquet 파티션에서 대상 년월의 리뷰 수, 평균 평점, 구매인증 수 등을 집계하여 PostgreSQL `monthly_review_summary`에 UPSERT 병합
        * **재시도 전략**: DB 커넥션 일시 지연 또는 트랜잭션 락 대비 2회 재시도 (3분 간격)
        """,
    )

    # 4. 데이터 품질 및 멱등성 검증 (데이터 정합성 이슈 -> 재시도 없이 즉시 알림)
    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=validate_postgres_summary,
        retries=0,
        doc_md="""
        ### validate_data
        * **역할**: PostgreSQL `monthly_review_summary` 테이블의 누적 월수 및 대상 월의 데이터 적재 정합성 검증
        * **재시도 전략**: 재시도 0회. 품질 검증 실패는 비즈니스/데이터 결함이므로 즉시 알림 발송 후 파이프라인 차단
        """,
    )

    # Task 실행 순서 및 의존성 정의 (단일 파이프라인 4단계 순차 체이닝)
    download_raw_data >> refine_store_parquet >> aggregate_monthly >> validate_data

