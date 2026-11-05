"""5회차 과제: Airflow DAG 설계 및 구현 - Amazon 리뷰 월별 요약 및 상품별 분석 마트 파이프라인.

DAG 구조 (병렬 집계 분기 및 수렴):
    download_raw_data >> prepare_raw_parquet >> refine_store_parquet >> [aggregate_monthly, aggregate_product] >> validate_data

스케줄 & 증분 처리 (사전 감지 스킵 탑재):
    - 매월 1일 (schedule="@monthly") - 논리적 날짜(data_interval_start) 기준 전월 1달 치 데이터 처리
    - Raw Parquet 준비 단계(prepare_raw_parquet)에서 전체 JSONL을 1회 변환 후 매월 재사용 (완전 시 Spark 로딩 생략)
    - 월별 정제 작업(refine_store_parquet)은 대상 월의 Raw Parquet 파티션만 선별 로딩하여 전체 JSONL 스캔 제거
    - 이미 대상 월의 Parquet 파티션이 존재하는 경우 정제를 자동으로 스킵하여 백필 속도 극대화
    - 정제 Parquet 완료 후 aggregate_monthly(월별 요약)와 aggregate_product(상품별 마트)를 병렬 분기 실행하여 총 수행시간 단축
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
    """PostgreSQL monthly_review_summary 및 product_monthly_metrics 데이터 품질 및 멱등성 검증."""
    params = context.get("params", {})
    category = params.get("category") or os.getenv("DEFAULT_CATEGORY", "All_Beauty")
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
    print(
        f"[Airflow 실행 날짜 정보] logical_date={logical_date}, "
        f"data_interval_start={data_interval_start}, target_ym={target_ym}, category={category}"
    )

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
            # 1. 전체 집계 현황 검증 (현재 카테고리 기준)
            cur.execute("""
                SELECT
                    count(*) AS total_months,
                    min(review_year_month) AS earliest_month,
                    max(review_year_month) AS latest_month,
                    count(*) FILTER (WHERE total_reviews <= 0) AS invalid_reviews_count,
                    count(*) FILTER (WHERE avg_rating < 1.0 OR avg_rating > 5.0) AS invalid_rating_count,
                    max(updated_at) AS last_updated_at
                FROM public.monthly_review_summary
                WHERE category = %s;
            """, (category,))
            row = cur.fetchone()
            total_months, earliest, latest, invalid_reviews, invalid_ratings, last_updated = row
            if total_months == 0:
                raise ValueError(f"monthly_review_summary 테이블에 [{category}] 카테고리로 적재된 데이터가 없습니다!")
            if invalid_reviews > 0:
                raise ValueError(f"[{category}] 비정상 리뷰 건수: {invalid_reviews}건 (리뷰 수 <= 0인 데이터 발견)")
            if invalid_ratings > 0:
                raise ValueError(f"[{category}] 평점 범위(1.0~5.0)를 벗어난 데이터가 {invalid_ratings}건 존재합니다!")

            print(f"[품질 검증] ({category}) 누적 총 집계 월수: {total_months:,}개월 (집계 기간: {earliest} ~ {latest})")
            print(f"[품질 검증] ({category}) 비정상 평점 건수: {invalid_ratings}건, 최근 갱신 시각: {last_updated}")

            # 2. 지정된 대상 월 증분 검증 (특정 월 실행 시)
            if target_ym and target_ym.lower() not in ("all", "auto", ""):
                cur.execute("""
                    SELECT total_reviews, avg_rating, verified_reviews, unique_products, unique_users, updated_at
                    FROM public.monthly_review_summary
                    WHERE category = %s AND review_year_month = %s;
                """, (category, target_ym))
                month_row = cur.fetchone()
                if not month_row:
                    raise ValueError(
                        f"대상 월({target_ym}, 카테고리: {category}) 데이터가 monthly_review_summary에 존재하지 않습니다! "
                        f"(참고: 원본 데이터셋은 2000-11 ~ 2023-09 구간의 데이터만 포함하고 있습니다)"
                    )

                m_revs, m_rating, m_verif, m_prods, m_users, m_updated = month_row
                print(f"[증분 적재 검증 성공] 카테고리: {category}, 대상 월: {target_ym}")
                print(f"  - 리뷰 수: {m_revs:,}건, 평균 평점: {m_rating}점 (구매 인증: {m_verif:,}건)")
                print(f"  - 상품 수: {m_prods:,}개, 작성자 수: {m_users:,}명, 최종 갱신: {m_updated}")

            # 3. 상품별 분석 마트 및 뷰 검증
            cur.execute("""
                SELECT count(*), count(DISTINCT product_id), min(review_year_month), max(review_year_month)
                FROM public.product_monthly_metrics
                WHERE category = %s;
            """, (category,))
            pm_row = cur.fetchone()
            total_pm_rows, unique_pm_prods, pm_min_ym, pm_max_ym = pm_row
            print(
                f"[상품 마트 품질 검증] ({category}) 총 레코드 수: {total_pm_rows:,}건, 고유 상품 수: {unique_pm_prods:,}개 "
                f"(기간: {pm_min_ym} ~ {pm_max_ym})"
            )

            # 4. 분석 뷰(v_product_quarterly_metrics, v_product_review_surge) 정상 조회 검증
            cur.execute("SELECT count(*) FROM public.v_product_quarterly_metrics WHERE category = %s;", (category,))
            q_cnt = cur.fetchone()[0]
            print(f"[분기 뷰 검증] ({category}) 상품-분기별 집계 레코드 수: {q_cnt:,}건 (가중 평균 평점 산출 정상)")

            cur.execute("SELECT count(*) FROM public.v_product_review_surge WHERE category = %s;", (category,))
            surge_cnt = cur.fetchone()[0]
            print(f"[급증 상품 뷰 검증] ({category}) 리뷰 급증(100%+ & 연속월) 포착 건수: {surge_cnt:,}건")

            # 5. 저리뷰 vs 다리뷰 세그먼트 비교 테이블 검증
            cur.execute("""
                SELECT segment_rule, analysis_start_month, analysis_end_month, segment_type, product_count, total_reviews_sum
                FROM public.product_segment_comparison
                WHERE category = %s
                ORDER BY segment_type;
            """, (category,))
            segments = cur.fetchall()
            if segments:
                print(f"[세그먼트 비교 마트 검증] ({category}) 등록된 세그먼트 수: {len(segments)}개 (기준값 및 산출기간 보존 확인)")
                for seg in segments:
                    print(f"  - [{seg[3]}] 상품 수: {seg[4]:,}개, 누적 리뷰: {seg[5]:,}건 (규칙: {seg[0]}, 기간: {seg[1]}~{seg[2]})")
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
    description="월별 Amazon 리뷰 다운로드, Spark 정제(Parquet), 월별 요약 및 상품별 분석 마트(PostgreSQL UPSERT), 품질 검증 파이프라인",
    schedule="@monthly",  # 매월 1일 정기 자동 실행
    start_date=datetime(2000, 1, 1),  # 원본 데이터셋 시작 연도(2000년)부터 백필/수동 트리거 지원
    end_date=datetime(2023, 10, 1),  # 데이터셋 최종 월(2023-09) 마감 시점 (현재 시점 불필요한 자동 스케줄 방지)
    catchup=False,
    max_active_runs=1,
    params={
        "year_month": Param(
            default="auto",
            type="string",
            description=(
                "처리할 대상 년월 (YYYY-MM). 'auto'는 선택한 논리적 날짜/백필 구간의 월을 자동 처리합니다. "
                "특정 월 강제 지정 시 '2023-01' 또는 전체 실행 시 'all'을 입력하세요."
            ),
        ),
        "category": Param(
            default=os.getenv("DEFAULT_CATEGORY", "All_Beauty"),
            type="string",
            description=(
                "수집 및 처리할 Amazon 리뷰 카테고리/데이터셋 이름 (예: All_Beauty, Appliances, Gift_Cards 등). "
                "data/processed/<category> 폴더로 분리 저장됩니다."
            ),
        ),
        "category_url": Param(
            default=(
                "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/"
                "resolve/main/raw/review_categories/{category}.jsonl"
            ),
            type="string",
            description=(
                "다운로드할 원본 파일의 URL. 비어있을 경우 로컬 data/raw/{category}.jsonl 파일을 사용하며, "
                "로컬에도 파일이 없으면 파이프라인이 즉시 중단됩니다. '{category}'는 선택된 카테고리명으로 자동 치환됩니다."
            ),
        ),
    },
    tags=["amazon", "reviews", "spark", "monthly", "incremental", "aggregate"],
) as dag:

    # 1. 원본 데이터 다운로드 (외부 네트워크 의존성 -> 3회 재시도 + 지수 백오프)
    download_raw_data = BashOperator(
        task_id="download_raw_data",
        bash_command="""
        {% set cat = params.category if (params.category and params.category != '') else 'All_Beauty' %}
        {% set raw_file = '/opt/airflow/data/raw/' + cat + '.jsonl' %}
        {% set raw_url = params.category_url | default('', true) | trim %}
        {% if '{category}' in raw_url %}
            {% set raw_url = raw_url.replace('{category}', cat) %}
        {% endif %}

        echo "[수집 단계] 카테고리: {{ cat }}"
        echo "[수집 단계] 입력 URL: '{{ raw_url }}'"

        if [ -n "{{ raw_url }}" ]; then
            echo "[수집 단계] URL이 지정되어 다운로드/수집을 진행합니다: {{ raw_url }}"
            python3 -u /opt/airflow/jobs/collector.py \
                --category "{{ cat }}" \
                --input "{{ raw_url }}" \
                --raw-dir /opt/airflow/data/raw \
                --states-dir /opt/airflow/states
        else
            echo "[수집 단계] category_url이 비어있으므로 로컬 파일 검증을 수행합니다: {{ raw_file }}"
            if [ -f "{{ raw_file }}" ] && [ -s "{{ raw_file }}" ]; then
                echo "[수집 단계 확인 성공] 로컬에 {{ cat }}.jsonl 파일이 유효하게 존재합니다. 다운로드를 생략합니다."
            else
                echo "[수집 단계 오류] category_url이 비어있고, 로컬에 {{ raw_file }} 파일도 존재하지 않습니다! 파이프라인을 중단합니다." >&2
                exit 1
            fi
        fi
        """,
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

    # 2. Raw Parquet 월별 파티션 변환 및 준비 (이미 완전하게 준비된 경우 Spark 로딩 없이 즉시 스킵)
    prepare_raw_parquet = BashOperator(
        task_id="prepare_raw_parquet",
        bash_command="""
        {% set cat = params.category if (params.category and params.category != '') else 'All_Beauty' %}
        echo "[Raw Parquet 준비 단계] 카테고리: {{ cat }}"
        /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --executor-memory 1600m \
            --driver-memory 1600m \
            --conf spark.sql.shuffle.partitions=32 \
            --conf spark.driver.host=airflow-worker \
            --conf spark.driver.bindAddress=0.0.0.0 \
            --conf spark.ui.showConsoleProgress=false \
            /opt/airflow/jobs/refine_reviews.py \
            --step convert_raw_to_parquet \
            --partitions 32 \
            --category "{{ cat }}" \
            --raw-dir /opt/airflow/data/raw/{{ cat }}.jsonl \
            --raw-parquet-dir /opt/airflow/data/raw_parquet/{{ cat }}
        """,
        retries=1,
        retry_delay=timedelta(minutes=5),
        doc_md="""
        ### prepare_raw_parquet
        * **역할**: 원본 JSONL을 월별 파티션 Raw Parquet으로 변환 및 무결성 검증 (이미 완전하게 준비된 경우 Spark 로딩 없이 즉시 종료)
        * **재시도 전략**: 일시적 클러스터 리소스 부족(OOM) 대비 1회 재시도
        """,
    )

    # 3. Spark 분산 정제 및 월별 Parquet 저장 (사전 감지 스킵: 이미 파티션 존재 시 스킵)
    refine_store_parquet = BashOperator(
        task_id="refine_store_parquet",
        bash_command="""
        {% set cat = params.category if (params.category and params.category != '') else 'All_Beauty' %}
        {% if params.year_month and params.year_month not in ('auto', '') %}
            {% set ym = params.year_month %}
        {% elif data_interval_start %}
            {% set ym = data_interval_start.strftime('%Y-%m') %}
        {% elif logical_date %}
            {% set ym = logical_date.strftime('%Y-%m') %}
        {% else %}
            {% set ym = '2023-01' %}
        {% endif %}
        echo "[Airflow 실행 날짜 정보] logical_date={{ logical_date }}, target_ym={{ ym }}, category={{ cat }}"
        /opt/spark/bin/spark-submit \
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
            --year-month "{{ ym }}" \
            --category "{{ cat }}" \
            --partitions 32 \
            --raw-dir /opt/airflow/data/raw/{{ cat }}.jsonl \
            --raw-parquet-dir /opt/airflow/data/raw_parquet/{{ cat }} \
            --processed-dir /opt/airflow/data/processed/{{ cat }}
        """,
        retries=1,
        retry_delay=timedelta(minutes=5),
        doc_md="""
        ### refine_store_parquet
        * **역할**: 대상 년월의 Raw Parquet 파티션(부재 시 JSONL 폴백)에서 데이터를 선별하여 결측치 필터링, UUID5 중복 제거 후 월별 파티션 Parquet 형식으로 저장 (이미 존재 시 자동 스킵)
        * **재시도 전략**: 일시적 클러스터 리소스 부족(OOM) 대비 1회 재시도, 반복 실패 시 로직/데이터 이슈로 판단하여 즉시 중단 및 알림
        """,
    )

    # 4. 월별 요약 집계 및 PostgreSQL UPSERT 적재 (해당 월 1개 행 증분 반영)
    aggregate_monthly = BashOperator(
        task_id="aggregate_monthly",
        bash_command="""
        {% set cat = params.category if (params.category and params.category != '') else 'All_Beauty' %}
        {% if params.year_month and params.year_month not in ('auto', '') %}
            {% set ym = params.year_month %}
        {% elif data_interval_start %}
            {% set ym = data_interval_start.strftime('%Y-%m') %}
        {% elif logical_date %}
            {% set ym = logical_date.strftime('%Y-%m') %}
        {% else %}
            {% set ym = '2023-01' %}
        {% endif %}
        echo "[Airflow 실행 날짜 정보] logical_date={{ logical_date }}, target_ym={{ ym }}, category={{ cat }}"
        /opt/spark/bin/spark-submit \
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
            --year-month "{{ ym }}" \
            --category "{{ cat }}" \
            --partitions 32 \
            --processed-dir /opt/airflow/data/processed/{{ cat }} \
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

    # 5. 상품별 월별 분석 마트 집계 및 저리뷰/다리뷰 세그먼트 분석 (PostgreSQL UPSERT)
    aggregate_product = BashOperator(
        task_id="aggregate_product",
        bash_command="""
        {% set cat = params.category if (params.category and params.category != '') else 'All_Beauty' %}
        {% if params.year_month and params.year_month not in ('auto', '') %}
            {% set ym = params.year_month %}
        {% elif data_interval_start %}
            {% set ym = data_interval_start.strftime('%Y-%m') %}
        {% elif logical_date %}
            {% set ym = logical_date.strftime('%Y-%m') %}
        {% else %}
            {% set ym = '2023-01' %}
        {% endif %}
        echo "[Airflow 실행 날짜 정보] logical_date={{ logical_date }}, target_ym={{ ym }}, category={{ cat }}"
        /opt/spark/bin/spark-submit \
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
            --year-month "{{ ym }}" \
            --category "{{ cat }}" \
            --partitions 32 \
            --processed-dir /opt/airflow/data/processed/{{ cat }} \
            --metrics-table public.product_monthly_metrics \
            --segment-table public.product_segment_comparison
        """,
        retries=2,
        retry_delay=timedelta(minutes=3),
        doc_md="""
        ### aggregate_product
        * **역할**: 상품별 월별 지표(review_quarter 포함) 집계, PostgreSQL `product_monthly_metrics` UPSERT 및 저/다리뷰 세그먼트 비교 마트(`product_segment_comparison`) 갱신
        * **재시도 전략**: DB 커넥션 일시 지연 또는 트랜잭션 락 대비 2회 재시도 (3분 간격)
        """,
    )

    # 6. 데이터 품질 및 멱등성 검증 (데이터 정합성 이슈 -> 재시도 없이 즉시 알림)
    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=validate_postgres_summary,
        retries=0,
        doc_md="""
        ### validate_data
        * **역할**: PostgreSQL `monthly_review_summary`, `product_monthly_metrics`, 뷰 및 세그먼트 마트 데이터 정합성 검증
        * **재시도 전략**: 재시도 0회. 품질 검증 실패는 비즈니스/데이터 결함이므로 즉시 알림 발송 후 파이프라인 차단
        """,
    )

    # Task 실행 순서 및 의존성 정의 (정제 Parquet 완료 후 월별 요약과 상품별 마트를 병렬 실행 후 품질 검증으로 수렴)
    download_raw_data >> prepare_raw_parquet >> refine_store_parquet >> [aggregate_monthly, aggregate_product] >> validate_data

