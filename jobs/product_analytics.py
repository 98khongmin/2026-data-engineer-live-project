"""Spark 기반 상품별 월/분기별 리뷰 수·평점 집계 및 분석 마트 파이프라인.

기능 요약:
1. 상품별 월별 지표 마트 집계 (review_quarter 포함) 및 PostgreSQL UPSERT 적재
2. 분기별 지표 요약 뷰 (리뷰 수 가중 평균 평점) 및 연속월 검증 기반 리뷰 급증 상품 뷰 연동
3. 장기간 저리뷰 vs 다리뷰 상품 비교 마트 산출 (기준 규칙, 분석 기간, 분위수 임계값 보존)
"""
import argparse
from contextlib import contextmanager
import os
import time
from urllib.parse import quote
import uuid

try:
    import psycopg2
except ImportError:
    psycopg2 = None
from pyspark.sql import SparkSession, functions as F


@contextmanager
def _dummy_measure(stage_name, component="SPARK", record_count=None):
    yield {"stage": stage_name, "component": component, "count": record_count}


class StageProfiler:
    """작업 단계별 소요 시간, 처리량, 병목 구간을 정밀 측정하는 프로파일러."""

    def __init__(self, job_name="aggregate_product"):
        self.job_name = job_name
        self.records = []
        self.start_total = time.perf_counter()

    @contextmanager
    def measure(self, stage_name, component="SPARK", record_count=None):
        t0 = time.perf_counter()
        print(f"\n[프로파일링 시작] ⏱️  {stage_name}...", flush=True)
        info = {"stage": stage_name, "component": component, "count": record_count}
        try:
            yield info
        finally:
            dur = time.perf_counter() - t0
            info["duration"] = dur
            self.records.append(info)
            cnt = info.get("count")
            cnt_str = f" ({cnt:,}건, {int(cnt / dur):,}건/초)" if cnt and dur > 0 else ""
            print(f"[프로파일링 완료] ✅ {stage_name} 완료: {dur:.2f}초{cnt_str}", flush=True)

    def print_summary(self, category="All_Beauty", year_month="auto"):
        total_dur = time.perf_counter() - self.start_total
        if not self.records:
            return

        print("\n" + "=" * 105, flush=True)
        print(f"📊 [{self.job_name} 실행 성능 및 병목 분석 리포트]", flush=True)
        print(f" - 카테고리: {category} | 대상 년월: {year_month} | 전체 소요 시간: {total_dur:.2f}초", flush=True)
        print("=" * 105, flush=True)
        print(f"{'단계 구분':<18} {'세부 작업 내용':<42} {'소요 시간(초)':>12} {'비중(%)':>10} {'처리량(건/초)':>16}", flush=True)
        print("-" * 105, flush=True)

        comp_totals = {}
        max_record = None

        for r in self.records:
            dur = r["duration"]
            pct = (dur / total_dur * 100) if total_dur > 0 else 0
            comp = f"[{r['component']}]"
            cnt = r.get("count")
            tps = f"{int(cnt / dur):,} 건/초" if cnt and dur > 0 else "-"
            print(f"{comp:<18} {r['stage']:<42} {dur:>10.2f}초 {pct:>9.1f}% {tps:>16}", flush=True)

            comp_totals[r["component"]] = comp_totals.get(r["component"], 0.0) + dur
            if max_record is None or dur > max_record["duration"]:
                max_record = r

        print("-" * 105, flush=True)
        print(f"{'총 소요 시간':<60} {total_dur:>10.2f}초 {'100.0%':>10}", flush=True)
        print("=" * 105, flush=True)

        if max_record:
            max_pct = (max_record["duration"] / total_dur * 100) if total_dur > 0 else 0
            print(f"🚨 [병목 분석 결과]", flush=True)
            print(f"   - 최대 병목 단계: '{max_record['stage']}' ({max_record['duration']:.2f}초, 전체의 {max_pct:.1f}%)", flush=True)
            comp_summary = " vs ".join([f"{k} {v:.2f}초 ({(v / total_dur * 100):.1f}%)" for k, v in comp_totals.items()])
            print(f"   - 컴포넌트별 소요: {comp_summary}", flush=True)
            if max_record["component"] in ("POSTGRES_WRITE", "POSTGRES_UPSERT", "POSTGRES_READ"):
                print(f"   -> 💡 분석 결과: Spark 집계 연산보다 PostgreSQL I/O 및 쿼리 실행에서 주요 병목이 발생하고 있습니다.", flush=True)
            elif max_record["component"] == "SPARK_COMPUTE":
                print(f"   -> 💡 분석 결과: Spark 셔플 집계(groupBy/agg) 연산이 병목입니다. 파티션 수 또는 클러스터 코어 확장을 고려하세요.", flush=True)
            elif max_record["component"] == "SPARK_IO":
                print(f"   -> 💡 분석 결과: Parquet 파일 디스크 읽기 I/O가 병목입니다. 파티션 프루닝 또는 디스크 I/O를 점검하세요.", flush=True)
            print("=" * 105 + "\n", flush=True)


def send_slack_alert(job_name, error_message, category=None, year_month=None, step=None):
    """Spark 작업/워커 실패 시 Slack Webhook으로 장애 알림을 전송합니다."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    try:
        import json
        import urllib.request

        err_str = str(error_message)
        if len(err_str) > 800:
            err_str = err_str[:800] + "\n... (생략)"

        fields = [
            {"title": "Spark Job", "value": f"`{job_name}`", "short": True},
            {"title": "실행 단계 (Step)", "value": f"`{step or 'all'}`", "short": True},
        ]
        if category:
            fields.append({"title": "카테고리", "value": f"`{category}`", "short": True})
        if year_month:
            fields.append({"title": "대상 년월", "value": f"`{year_month}`", "short": True})

        fields.append({"title": "오류/장애 상세 내용", "value": f"```{err_str}```", "short": False})

        payload = {
            "text": f"🚨 [Spark 장애 알림] {job_name} 작업 실패 감지",
            "attachments": [
                {
                    "color": "#E01E5A",
                    "title": f"🚨 Spark 작업/워커 장애 감지: {job_name}",
                    "fields": fields,
                    "mrkdwn_in": ["text", "fields"],
                }
            ],
        }

        req = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                print(f"[Slack 알림 전송 완료] Spark 장애 알림이 성공적으로 전송되었습니다.", flush=True)
            else:
                print(f"[Slack 알림 응답 경고] HTTP {response.status}", flush=True)
    except Exception as notify_err:
        print(f"[Slack 알림 전송 실패] 알림 전송 중 오류: {notify_err}", flush=True)


def get_db_config():
    """환경 변수로부터 PostgreSQL 접속 설정을 로드합니다."""
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "postgres"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }


def ensure_database_objects(db_config):
    """분석 마트 테이블 및 뷰가 존재하지 않을 경우 자동 생성합니다."""
    ddl_sql = """
    CREATE TABLE IF NOT EXISTS public.product_monthly_metrics (
        category VARCHAR(100) NOT NULL,
        product_id VARCHAR(50) NOT NULL,
        review_year_month VARCHAR(7) NOT NULL,
        review_quarter VARCHAR(7) NOT NULL,
        total_reviews INTEGER NOT NULL,
        avg_rating DOUBLE PRECISION NOT NULL,
        verified_reviews INTEGER NOT NULL,
        helpful_votes_sum BIGINT NOT NULL DEFAULT 0,
        positive_reviews INTEGER NOT NULL,
        negative_reviews INTEGER NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (category, product_id, review_year_month)
    );

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'product_monthly_metrics' AND column_name = 'category'
        ) THEN
            ALTER TABLE public.product_monthly_metrics ADD COLUMN IF NOT EXISTS category VARCHAR(100) DEFAULT 'All_Beauty' NOT NULL;
            ALTER TABLE public.product_monthly_metrics DROP CONSTRAINT IF EXISTS product_monthly_metrics_pkey;
            ALTER TABLE public.product_monthly_metrics ADD PRIMARY KEY (category, product_id, review_year_month);
        END IF;
    END $$;

    CREATE INDEX IF NOT EXISTS idx_prod_metrics_cat_ym
        ON public.product_monthly_metrics(category, review_year_month);
    CREATE INDEX IF NOT EXISTS idx_prod_metrics_cat_prod
        ON public.product_monthly_metrics(category, product_id);
    CREATE INDEX IF NOT EXISTS idx_prod_metrics_quarter
        ON public.product_monthly_metrics(review_quarter);

    DROP VIEW IF EXISTS public.v_product_review_surge CASCADE;
    DROP VIEW IF EXISTS public.v_product_quarterly_metrics CASCADE;

    CREATE OR REPLACE VIEW public.v_product_quarterly_metrics AS
    SELECT
        category,
        product_id,
        review_quarter,
        SUM(total_reviews) AS total_reviews,
        ROUND((SUM(avg_rating * total_reviews) / NULLIF(SUM(total_reviews), 0))::numeric, 2) AS avg_rating,
        SUM(verified_reviews) AS verified_reviews,
        SUM(helpful_votes_sum) AS helpful_votes_sum,
        SUM(positive_reviews) AS positive_reviews,
        SUM(negative_reviews) AS negative_reviews,
        MAX(updated_at) AS updated_at
    FROM public.product_monthly_metrics
    GROUP BY category, product_id, review_quarter;

    CREATE OR REPLACE VIEW public.v_product_review_surge AS
    WITH lag_metrics AS (
        SELECT
            category,
            product_id,
            review_year_month,
            review_quarter,
            total_reviews,
            avg_rating,
            LAG(review_year_month, 1) OVER (PARTITION BY category, product_id ORDER BY review_year_month) AS prev_ym,
            LAG(total_reviews, 1) OVER (PARTITION BY category, product_id ORDER BY review_year_month) AS raw_prev_reviews,
            LAG(avg_rating, 1) OVER (PARTITION BY category, product_id ORDER BY review_year_month) AS raw_prev_rating,
            TO_CHAR(TO_DATE(review_year_month || '-01', 'YYYY-MM-DD') - INTERVAL '1 month', 'YYYY-MM') AS expected_prev_ym
        FROM public.product_monthly_metrics
    ),
    consecutive_metrics AS (
        SELECT
            category,
            product_id,
            review_year_month,
            review_quarter,
            total_reviews,
            avg_rating,
            CASE
                WHEN prev_ym = expected_prev_ym THEN raw_prev_reviews
                ELSE NULL
            END AS prev_month_reviews,
            CASE
                WHEN prev_ym = expected_prev_ym THEN raw_prev_rating
                ELSE NULL
            END AS prev_month_rating,
            CASE
                WHEN prev_ym = expected_prev_ym THEN true
                ELSE false
            END AS is_consecutive_month
        FROM lag_metrics
    )
    SELECT
        category,
        product_id,
        review_year_month,
        review_quarter,
        prev_month_reviews,
        total_reviews,
        (total_reviews - COALESCE(prev_month_reviews, 0)) AS review_growth_cnt,
        ROUND(
            CASE 
                WHEN COALESCE(prev_month_reviews, 0) > 0 
                THEN ((total_reviews - prev_month_reviews)::numeric / prev_month_reviews::numeric) * 100
                ELSE 100.0 
            END, 2
        ) AS mom_growth_rate_pct,
        prev_month_rating,
        avg_rating,
        ROUND((avg_rating - COALESCE(prev_month_rating, avg_rating))::numeric, 2) AS rating_delta,
        is_consecutive_month
    FROM consecutive_metrics
    WHERE total_reviews >= 10
      AND (
        prev_month_reviews IS NULL
        OR prev_month_reviews = 0
        OR (total_reviews - prev_month_reviews)::numeric / prev_month_reviews >= 1.0
      )
    ORDER BY review_growth_cnt DESC;

    CREATE TABLE IF NOT EXISTS public.product_segment_comparison (
        category VARCHAR(100) NOT NULL,
        segment_rule VARCHAR(50) NOT NULL,
        analysis_start_month VARCHAR(7) NOT NULL,
        analysis_end_month VARCHAR(7) NOT NULL,
        segment_type VARCHAR(20) NOT NULL,
        threshold_p20_reviews INTEGER NOT NULL,
        threshold_p80_reviews INTEGER NOT NULL,
        product_count BIGINT NOT NULL,
        total_reviews_sum BIGINT NOT NULL,
        avg_product_reviews DOUBLE PRECISION NOT NULL,
        weighted_avg_rating DOUBLE PRECISION NOT NULL,
        verified_reviews_ratio DOUBLE PRECISION NOT NULL,
        positive_reviews_ratio DOUBLE PRECISION NOT NULL,
        negative_reviews_ratio DOUBLE PRECISION NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (category, segment_rule, analysis_start_month, analysis_end_month, segment_type)
    );

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'product_segment_comparison' AND column_name = 'category'
        ) THEN
            ALTER TABLE public.product_segment_comparison ADD COLUMN IF NOT EXISTS category VARCHAR(100) DEFAULT 'All_Beauty' NOT NULL;
            ALTER TABLE public.product_segment_comparison DROP CONSTRAINT IF EXISTS product_segment_comparison_pkey;
            ALTER TABLE public.product_segment_comparison ADD PRIMARY KEY (category, segment_rule, analysis_start_month, analysis_end_month, segment_type);
        END IF;
    END $$;

    CREATE INDEX IF NOT EXISTS idx_segment_comp_period
        ON public.product_segment_comparison(category, analysis_start_month, analysis_end_month);
    """
    try:
        conn = psycopg2.connect(**db_config)
        with conn.cursor() as cur:
            cur.execute(ddl_sql)
        conn.commit()
        conn.close()
        print("[DB 초기화] 상품 분석 마트 테이블 및 뷰 스키마가 정상 확인되었습니다.", flush=True)
    except Exception as e:
        print(f"[DB 초기화 경고] DDL 실행 중 오류 발생 (이미 생성된 경우 무시 가능): {e}", flush=True)


def upsert_staging_to_metrics(db_config, staging_table, target_table):
    """PostgreSQL staging 테이블의 상품 월별 지표를 target 마트 테이블에 원자적 UPSERT 병합합니다."""
    upsert_sql = f"""
    SET LOCAL synchronous_commit = 'off';
    INSERT INTO {target_table} (
        category, product_id, review_year_month, review_quarter, total_reviews,
        avg_rating, verified_reviews, helpful_votes_sum, positive_reviews, negative_reviews, updated_at
    )
    SELECT
        category, product_id, review_year_month, review_quarter, total_reviews,
        avg_rating, verified_reviews, helpful_votes_sum, positive_reviews, negative_reviews, updated_at
    FROM {staging_table}
    ON CONFLICT (category, product_id, review_year_month) DO UPDATE SET
        review_quarter = EXCLUDED.review_quarter,
        total_reviews = EXCLUDED.total_reviews,
        avg_rating = EXCLUDED.avg_rating,
        verified_reviews = EXCLUDED.verified_reviews,
        helpful_votes_sum = EXCLUDED.helpful_votes_sum,
        positive_reviews = EXCLUDED.positive_reviews,
        negative_reviews = EXCLUDED.negative_reviews,
        updated_at = EXCLUDED.updated_at;
    """
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            cur.execute(upsert_sql)
            cur.execute(f"DROP TABLE IF EXISTS {staging_table};")
        conn.commit()
        print(f"[DB 적재 완료] {staging_table} -> {target_table} UPSERT 완료 및 staging 삭제 완료", flush=True)
    finally:
        conn.close()


def upsert_staging_to_segments(db_config, staging_table, target_table):
    """저리뷰 vs 다리뷰 세그먼트 비교 결과를 PostgreSQL target 테이블에 UPSERT 병합합니다."""
    upsert_sql = f"""
    SET LOCAL synchronous_commit = 'off';
    INSERT INTO {target_table} (
        category, segment_rule, analysis_start_month, analysis_end_month, segment_type,
        threshold_p20_reviews, threshold_p80_reviews, product_count, total_reviews_sum,
        avg_product_reviews, weighted_avg_rating, verified_reviews_ratio,
        positive_reviews_ratio, negative_reviews_ratio, created_at
    )
    SELECT
        category, segment_rule, analysis_start_month, analysis_end_month, segment_type,
        threshold_p20_reviews, threshold_p80_reviews, product_count, total_reviews_sum,
        avg_product_reviews, weighted_avg_rating, verified_reviews_ratio,
        positive_reviews_ratio, negative_reviews_ratio, created_at
    FROM {staging_table}
    ON CONFLICT (category, segment_rule, analysis_start_month, analysis_end_month, segment_type) DO UPDATE SET
        threshold_p20_reviews = EXCLUDED.threshold_p20_reviews,
        threshold_p80_reviews = EXCLUDED.threshold_p80_reviews,
        product_count = EXCLUDED.product_count,
        total_reviews_sum = EXCLUDED.total_reviews_sum,
        avg_product_reviews = EXCLUDED.avg_product_reviews,
        weighted_avg_rating = EXCLUDED.weighted_avg_rating,
        verified_reviews_ratio = EXCLUDED.verified_reviews_ratio,
        positive_reviews_ratio = EXCLUDED.positive_reviews_ratio,
        negative_reviews_ratio = EXCLUDED.negative_reviews_ratio,
        created_at = EXCLUDED.created_at;
    """
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            cur.execute(upsert_sql)
            cur.execute(f"DROP TABLE IF EXISTS {staging_table};")
        conn.commit()
        print(f"[DB 적재 완료] {staging_table} -> {target_table} 세그먼트 UPSERT 완료 및 staging 삭제 완료", flush=True)
    finally:
        conn.close()


def run_aggregate_metrics(spark, args, db_config, profiler=None):
    """정제된 Parquet으로부터 상품별 월별 지표(review_quarter 포함) 집계 및 적재."""
    category = args.category or "All_Beauty"
    print("=" * 70, flush=True)
    print(f"[상품 마트 집계] ({category}) {args.processed_dir} 로딩 시작...", flush=True)

    target_ym = args.year_month
    is_incremental = bool(target_ym and target_ym.lower() not in ("all", "auto", ""))
    measure_ctx = profiler.measure if profiler else _dummy_measure

    with measure_ctx("1. Parquet 데이터 로딩", component="SPARK_IO") as m:
        if is_incremental:
            partition_path = os.path.join(args.processed_dir, f"review_year_month={target_ym}")
            if os.path.exists(partition_path):
                print(f"[상품 마트 집계] 대상 월({target_ym}) 파티션 직접 로딩: {partition_path}", flush=True)
                df = spark.read.parquet(partition_path).withColumn("review_year_month", F.lit(target_ym))
            else:
                print(f"[상품 마트 집계] 대상 월({target_ym}) 파티션 경로가 없어 상위 디렉터리 필터 로딩 시도", flush=True)
                df = spark.read.parquet(args.processed_dir).filter(F.col("review_year_month") == target_ym)
        else:
            print("[상품 마트 집계] 전체 파티션 Parquet 로딩", flush=True)
            df = spark.read.parquet(args.processed_dir)

        record_count = df.count()
        m["count"] = record_count
        print(f"[상품 마트 집계] 읽어들인 리뷰 레코드 수: {record_count:,}건", flush=True)

    if record_count == 0:
        print("[상품 마트 집계 알림] 처리할 리뷰 데이터가 없어 작업을 종료합니다.", flush=True)
        return

    # 분기 식별자 컬럼 (YYYY-Q#)
    quarter_expr = F.concat(
        F.substring(F.col("review_year_month"), 1, 4),
        F.lit("-Q"),
        F.when(F.substring(F.col("review_year_month"), 6, 2).isin("01", "02", "03"), "1")
         .when(F.substring(F.col("review_year_month"), 6, 2).isin("04", "05", "06"), "2")
         .when(F.substring(F.col("review_year_month"), 6, 2).isin("07", "08", "09"), "3")
         .otherwise("4"),
    )

    helpful_expr = (
        F.coalesce(F.col("helpful_vote"), F.lit(0)).cast("long")
        if "helpful_vote" in df.columns
        else F.lit(0).cast("long")
    )

    with measure_ctx("2. 상품-월별 집계 연산", component="SPARK_COMPUTE") as m:
        print(f"[상품 마트 집계] ({category}) 상품별(product_id) x 월별(review_year_month) 집계 수행...", flush=True)
        metrics_df = (
            df.groupBy("product_id", "review_year_month")
            .agg(
                F.count("*").cast("int").alias("total_reviews"),
                F.round(F.avg("rating"), 2).alias("avg_rating"),
                F.sum(F.when(F.col("verified_purchase") == True, 1).otherwise(0)).cast("int").alias("verified_reviews"),
                F.sum(helpful_expr).alias("helpful_votes_sum"),
                F.sum(F.when(F.col("rating") >= 4.0, 1).otherwise(0)).cast("int").alias("positive_reviews"),
                F.sum(F.when(F.col("rating") <= 2.0, 1).otherwise(0)).cast("int").alias("negative_reviews"),
            )
            .withColumn("category", F.lit(category))
            .withColumn("review_quarter", quarter_expr)
            .withColumn("updated_at", F.current_timestamp())
        )
        metrics_df = metrics_df.cache()
        metrics_count = metrics_df.count()
        m["count"] = metrics_count
        print(f"[상품 마트 집계] 산출된 상품-월별 마트 레코드 수: {metrics_count:,}건", flush=True)

    jdbc_url = (
        f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{quote(db_config['dbname'], safe='')}"
        "?reWriteBatchedInserts=true"
    )
    run_id = uuid.uuid4().hex[:8]
    staging_table = f"product_monthly_metrics_staging_{run_id}"

    with measure_ctx("3. PostgreSQL JDBC Staging 적재", component="POSTGRES_WRITE") as m:
        m["count"] = metrics_count
        print(f"[상품 마트 DB 적재] PostgreSQL 임시 테이블({staging_table})에 분산 적재 중 (batchsize=10000, Multi-row rewrite)...", flush=True)
        target_write_parts = min(max(4, metrics_df.rdd.getNumPartitions()), 8)
        (
            metrics_df.coalesce(target_write_parts)
            .write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", staging_table)
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", "10000")
            .mode("overwrite")
            .save()
        )

    metrics_df.unpersist()

    with measure_ctx("4. PostgreSQL 마트 UPSERT 병합", component="POSTGRES_UPSERT") as m:
        m["count"] = metrics_count
        upsert_staging_to_metrics(db_config, staging_table, args.metrics_table)


def run_analyze_segments(spark, args, db_config, profiler=None):
    """장기간 저리뷰 vs 다리뷰 상품 비교 마트 산출 및 적재."""
    category = args.category or "All_Beauty"
    print("=" * 70, flush=True)
    print(f"[저리뷰 vs 다리뷰 세그먼트 분석] ({category}) {args.metrics_table} 기반 분석 시작...", flush=True)

    measure_ctx = profiler.measure if profiler else _dummy_measure
    jdbc_url = (
        f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{quote(db_config['dbname'], safe='')}"
        "?reWriteBatchedInserts=true"
    )

    with measure_ctx("5. 세그먼트용 마트 JDBC 로드", component="POSTGRES_READ") as m:
        cols_query = (
            f"(SELECT category, product_id, review_year_month, total_reviews, avg_rating, "
            f"verified_reviews, positive_reviews, negative_reviews "
            f"FROM {args.metrics_table} WHERE category = '{category}') AS metrics_sub"
        )
        metrics_df = (
            spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", cols_query)
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", "10000")
            .load()
        )

        metrics_count = metrics_df.count()
        m["count"] = metrics_count

    if metrics_count == 0:
        print("[세그먼트 분석 알림] 상품 마트 테이블에 데이터가 없어 세그먼트 분석을 생략합니다.", flush=True)
        return

    with measure_ctx("6. 분위수 및 세그먼트 집계", component="SPARK_COMPUTE") as m:
        # 분석 대상 기간(시작월, 마감월) 동적 추출
        period_row = metrics_df.select(
            F.min("review_year_month").alias("start_ym"),
            F.max("review_year_month").alias("end_ym"),
        ).first()
        start_ym = period_row["start_ym"] or "2000-01"
        end_ym = period_row["end_ym"] or "2023-12"

        print(f"[세그먼트 분석] 분석 대상 기간: {start_ym} ~ {end_ym}", flush=True)

        # 상품별 전체 누적 지표 집계
        prod_totals = (
            metrics_df.groupBy("product_id")
            .agg(
                F.sum("total_reviews").cast("long").alias("lifetime_reviews"),
                (F.sum(F.col("avg_rating") * F.col("total_reviews")) / F.sum("total_reviews")).alias("lifetime_avg_rating"),
                F.sum("verified_reviews").cast("long").alias("lifetime_verified"),
                F.sum("positive_reviews").cast("long").alias("lifetime_positive"),
                F.sum("negative_reviews").cast("long").alias("lifetime_negative"),
            )
        )

        prod_count = prod_totals.count()
        print(f"[세그먼트 분석] 분석 대상 고유 상품 수: {prod_count:,}개", flush=True)

        # 하위 20%(p20) 및 상위 20%(p80) 분위수 임계치 산출
        quantiles = prod_totals.approxQuantile("lifetime_reviews", [0.20, 0.80], 0.01)
        p20 = int(quantiles[0])
        p80 = int(quantiles[1])
        if p80 <= p20:
            p80 = p20 + 1

        print(f"[세그먼트 분석] 사분위 임계치 산출: p20(저리뷰 경계)={p20}건 이하, p80(다리뷰 경계)={p80}건 이상", flush=True)

        # 상품별 세그먼트 분류
        segmented = prod_totals.withColumn(
            "segment_type",
            F.when(F.col("lifetime_reviews") <= p20, F.lit("LOW_REVIEWS"))
             .when(F.col("lifetime_reviews") >= p80, F.lit("HIGH_REVIEWS"))
             .otherwise(F.lit("MID_REVIEWS")),
        )

        # 세그먼트별 비교 지표 산출
        segment_summary = (
            segmented.groupBy("segment_type")
            .agg(
                F.count("*").cast("long").alias("product_count"),
                F.sum("lifetime_reviews").cast("long").alias("total_reviews_sum"),
                F.round(F.avg("lifetime_reviews"), 2).alias("avg_product_reviews"),
                F.round(F.sum(F.col("lifetime_avg_rating") * F.col("lifetime_reviews")) / F.sum("lifetime_reviews"), 2).alias("weighted_avg_rating"),
                F.round(F.sum("lifetime_verified") / F.sum("lifetime_reviews"), 4).alias("verified_reviews_ratio"),
                F.round(F.sum("lifetime_positive") / F.sum("lifetime_reviews"), 4).alias("positive_reviews_ratio"),
                F.round(F.sum("lifetime_negative") / F.sum("lifetime_reviews"), 4).alias("negative_reviews_ratio"),
            )
            .withColumn("category", F.lit(category))
            .withColumn("segment_rule", F.lit("percentile_p20_p80"))
            .withColumn("analysis_start_month", F.lit(start_ym))
            .withColumn("analysis_end_month", F.lit(end_ym))
            .withColumn("threshold_p20_reviews", F.lit(p20))
            .withColumn("threshold_p80_reviews", F.lit(p80))
            .withColumn("created_at", F.current_timestamp())
        )
        segment_summary = segment_summary.cache()
        seg_count = segment_summary.count()
        m["count"] = seg_count

    print("[세그먼트 분석 결과 미리보기]", flush=True)
    segment_summary.show(truncate=False)

    run_id = uuid.uuid4().hex[:8]
    staging_table = f"product_segment_comparison_staging_{run_id}"

    with measure_ctx("7. 세그먼트 Staging 및 UPSERT", component="POSTGRES_WRITE") as m:
        m["count"] = seg_count
        (
            segment_summary.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", staging_table)
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", "10000")
            .mode("overwrite")
            .save()
        )
        upsert_staging_to_segments(db_config, staging_table, args.segment_table)

    segment_summary.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--step",
        choices=["all", "aggregate_metrics", "analyze_segments"],
        default="all",
        help="실행할 파이프라인 단계: aggregate_metrics, analyze_segments, all",
    )
    parser.add_argument("--year-month", default=None,
                        help="처리할 대상 년월 (YYYY-MM). 지정 시 해당 월만 증분 추출/적재, 'all' 또는 미지정 시 전체 처리")
    parser.add_argument("--category", default=os.getenv("CATEGORY", None),
                        help="처리할 대상 카테고리/데이터셋 이름 (예: All_Beauty, Appliances 등)")
    parser.add_argument("--processed-dir", default=os.getenv("PROCESSED_DIR", "/opt/spark/data/processed/reviews"),
                        help="정제 완료 Parquet 디렉터리 경로")
    parser.add_argument("--metrics-table", default="public.product_monthly_metrics",
                        help="상품별 월별 지표 마트 PostgreSQL 테이블명")
    parser.add_argument("--segment-table", default="public.product_segment_comparison",
                        help="저리뷰 vs 다리뷰 비교 마트 PostgreSQL 테이블명")
    parser.add_argument("--partitions", type=int, default=16, help="Spark 처리 파티션 수")
    args = parser.parse_args()

    # 카테고리 결정
    category = args.category
    if not category:
        category = "All_Beauty"

    if args.processed_dir.endswith("/reviews"):
        args.processed_dir = os.path.join(os.path.dirname(args.processed_dir), category)
    elif not args.processed_dir.endswith(f"/{category}") and os.path.basename(args.processed_dir) != category:
        args.processed_dir = os.path.join(args.processed_dir, category)

    db_config = get_db_config()
    ensure_database_objects(db_config)

    spark = (
        SparkSession.builder
        .appName(f"AmazonReview_ProductAnalytics_{category}")
        .config("spark.sql.shuffle.partitions", str(args.partitions))
        .getOrCreate()
    )

    profiler = StageProfiler("aggregate_product")

    try:
        if args.step in ("all", "aggregate_metrics"):
            run_aggregate_metrics(spark, args, db_config, profiler=profiler)
        if args.step in ("all", "analyze_segments"):
            run_analyze_segments(spark, args, db_config, profiler=profiler)
        profiler.print_summary(category=category, year_month=args.year_month or "auto")
    except Exception as e:
        print(f"[Spark 작업 실패 감지] 오류 발생: {e}", flush=True)
        send_slack_alert(
            job_name=f"AmazonReview_ProductAnalytics_{category}",
            error_message=e,
            category=category,
            year_month=args.year_month,
            step=args.step,
        )
        raise e
    finally:
        spark.stop()
        print("[작업 종료] Spark 세션이 안전하게 종료되었습니다.", flush=True)


if __name__ == "__main__":
    main()
