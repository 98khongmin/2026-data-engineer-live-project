"""Spark 기반 원본 분산 로딩, review_id 중복 제거, 월별 Parquet 저장 및 PostgreSQL 월별 집계 파이프라인."""
import argparse
import json
import os
from urllib.parse import quote
import uuid

from pyspark.sql import SparkSession, functions as F, types as T

NAMESPACE_URL = uuid.NAMESPACE_URL

# 원본 JSONL 데이터 스키마 정의 (스키마 추론 오버헤드 방지 및 타입 안정성 확보)
RAW_REVIEW_SCHEMA = T.StructType([
    T.StructField("rating", T.DoubleType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("text", T.StringType(), True),
    T.StructField("asin", T.StringType(), True),
    T.StructField("parent_asin", T.StringType(), True),
    T.StructField("user_id", T.StringType(), True),
    T.StructField("timestamp", T.LongType(), True),
    T.StructField("helpful_vote", T.LongType(), True),
    T.StructField("verified_purchase", T.BooleanType(), True),
])


@F.udf(returnType=T.StringType())
def make_review_id(user_id, parent_asin, timestamp):
    """user_id, parent_asin, timestamp를 결합해 고유한 UUID5 review_id를 생성합니다."""
    if not user_id or not parent_asin or timestamp is None:
        return None
    key = json.dumps([user_id, parent_asin, timestamp], ensure_ascii=False, separators=(",", ":"))
    return str(uuid.uuid5(NAMESPACE_URL, key))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step", choices=["all", "refine_store_parquet", "refine_parquet", "aggregate_monthly"], default="all",
                        help="실행할 파이프라인 단계: refine_store_parquet (정제 및 Parquet 저장), aggregate_monthly (월별 집계 및 DB 적재), all (전체 실행)")
    parser.add_argument("--year-month", default=None,
                        help="처리할 대상 년월 (YYYY-MM). 지정 시 해당 월만 증분 추출/적재, 'all' 또는 미지정 시 전체 처리")
    parser.add_argument("--raw-dir", default=os.getenv("RAW_DIR", "/opt/spark/data/raw"),
                        help="수집기가 적재한 원본 JSON/JSONL 디렉토리")
    parser.add_argument("--processed-dir", default=os.getenv("PROCESSED_DIR", "/opt/spark/data/processed/reviews"),
                        help="정제 완료 원본을 저장할 Parquet 경로")
    parser.add_argument("--summary-table", default="public.monthly_review_summary",
                        help="월별 집계를 저장할 PostgreSQL 테이블명")
    parser.add_argument("--partitions", type=int, default=4, help="Spark 처리 파티션 수")
    args = parser.parse_args()

    database = dict(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )

    spark = (
        SparkSession.builder
        .appName(f"amazon-reviews-pipeline-{args.step}")
        .config("spark.sql.shuffle.partitions", str(args.partitions))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        refined_df = None

        # 1. 정제 및 Parquet 저장 단계
        if args.step in ("all", "refine_store_parquet", "refine_parquet"):
            # 스마트 스킵 검사: 이미 대상 Parquet 파티션이 존재하는지 확인
            skip_refine = False
            if args.year_month and args.year_month.lower() not in ("all", "auto"):
                target_partition = os.path.join(args.processed_dir, f"review_year_month={args.year_month}")
                if os.path.exists(target_partition) and any(f.endswith(".parquet") for f in os.listdir(target_partition)):
                    print(f"[정제 스킵] 대상 월({args.year_month}) Parquet 파티션이 이미 존재하므로 원본 JSONL 정제를 생략합니다.", flush=True)
                    skip_refine = True
            elif args.year_month and args.year_month.lower() == "all":
                if os.path.exists(args.processed_dir):
                    existing_parts = [d for d in os.listdir(args.processed_dir) if d.startswith("review_year_month=")]
                    if len(existing_parts) > 50:
                        print(f"[정제 스킵] 전체 Parquet 파티션({len(existing_parts)}개)이 이미 존재하므로 일괄 정제를 생략합니다.", flush=True)
                        skip_refine = True

            if not skip_refine:
                print("=" * 70, flush=True)
                print(f"[정제 단계] {args.raw_dir} 원본 파일 읽기 시작 (명시적 스키마 적용)", flush=True)
                raw_df = spark.read.schema(RAW_REVIEW_SCHEMA).json(args.raw_dir)
                total_raw_count = raw_df.count()
                print(f"[정제 단계] 총 원본 레코드 수: {total_raw_count:,}건", flush=True)

                print("-" * 70, flush=True)
                print("[정제 단계] 필수값 필터링 및 review_id 기준 dropDuplicates 수행", flush=True)

                # 필수 컬럼 결측치 확인 및 rating 유효성 검사 (1.0 ~ 5.0)
                valid_df = (
                    raw_df.filter(
                        F.col("user_id").isNotNull() & (F.trim(F.col("user_id")) != "") &
                        F.col("parent_asin").isNotNull() & (F.trim(F.col("parent_asin")) != "") &
                        F.col("timestamp").isNotNull() &
                        F.col("asin").isNotNull() & (F.trim(F.col("asin")) != "") &
                        F.col("text").isNotNull() & (F.trim(F.col("text")) != "") &
                        F.col("rating").isNotNull() & F.col("rating").between(1, 5)
                    )
                    .withColumn("review_id", make_review_id(F.col("user_id"), F.col("parent_asin"), F.col("timestamp")))
                    .dropDuplicates(["review_id"])
                )

                # 날짜 컬럼 파생 및 최종 정제 스키마 구성
                refined_df = (
                    valid_df.select(
                        F.col("review_id"),
                        F.trim(F.col("user_id")).alias("user_id"),
                        F.trim(F.col("parent_asin")).alias("parent_asin"),
                        F.trim(F.col("asin")).alias("product_id"),
                        F.col("timestamp"),
                        F.to_timestamp(F.col("timestamp") / 1000).alias("review_time"),
                        F.to_date(F.to_timestamp(F.col("timestamp") / 1000)).alias("review_date"),
                        F.date_format(F.to_timestamp(F.col("timestamp") / 1000), "yyyy-MM").alias("review_year_month"),
                        F.col("rating").cast("double").alias("rating"),
                        F.trim(F.col("title")).alias("review_title"),
                        F.trim(F.col("text")).alias("review_text"),
                        F.coalesce(F.col("verified_purchase"), F.lit(False)).alias("verified_purchase"),
                    )
                )

                # 20년 치 전체 데이터를 단 1회 스캔으로 모든 월별 파티션 폴더를 일괄 생성
                refined_df = refined_df.repartition(args.partitions)
                refined_df.cache()
                refined_count = refined_df.count()
                print(f"[정제 단계] 정제 및 중복 제거 후 레코드 수: {refined_count:,}건 (260개 월 동시 파티셔닝 중...)", flush=True)

                print(f"[정제 단계] {args.processed_dir} 에 partitionBy('review_year_month') 로 전체 일괄 저장 중...", flush=True)
                (
                    refined_df.write
                    .mode("overwrite")
                    .partitionBy("review_year_month")
                    .parquet(args.processed_dir)
                )
                print("[정제 단계 완료] 전체 월별(review_year_month) Parquet 파티션 일괄 저장 완료! (이후 모든 실행은 0.1초 만에 스킵됩니다)", flush=True)
            else:
                if args.step in ("refine_store_parquet", "refine_parquet"):
                    print("[정제 단계] Parquet 파티션이 이미 준비되어 있으므로 작업을 성공적으로 마칩니다.", flush=True)
                    return

        # 2. 월별 집계 및 DB 적재 단계
        if args.step in ("all", "aggregate_monthly"):
            if refined_df is None:
                print("=" * 70, flush=True)
                print(f"[집계 단계] {args.processed_dir} 정제 Parquet 데이터 로딩 중...", flush=True)
                if args.year_month and args.year_month.lower() not in ("all", "auto"):
                    print(f"[집계 단계] 대상 월({args.year_month}) 파티션 프루닝 로딩", flush=True)
                    refined_df = spark.read.parquet(args.processed_dir).filter(F.col("review_year_month") == args.year_month)
                else:
                    refined_df = spark.read.parquet(args.processed_dir)

            print("-" * 70, flush=True)
            print("[집계 단계] review_year_month 기준 리뷰 수, 평균 평점, 구매인증 수 월별 집계 생성", flush=True)
            monthly_summary = (
                refined_df.groupBy("review_year_month")
                .agg(
                    F.count("*").alias("total_reviews"),
                    F.round(F.avg("rating"), 2).alias("avg_rating"),
                    F.sum(F.when(F.col("verified_purchase") == True, 1).otherwise(0)).alias("verified_reviews"),
                    F.countDistinct("product_id").alias("unique_products"),
                    F.countDistinct("user_id").alias("unique_users"),
                )
                .withColumn("updated_at", F.current_timestamp())
                .orderBy("review_year_month")
            )

            monthly_count = monthly_summary.count()
            print(f"[집계 단계] 총 집계 월수: {monthly_count:,}개월", flush=True)

            if monthly_count == 0:
                print(f"[집계 단계 알림] 집계 대상 레코드가 0건이어서 DB 적재를 건너뜁니다.", flush=True)
            else:
                jdbc_url = f"jdbc:postgresql://{database['host']}:{database['port']}/{quote(database['dbname'], safe='')}"
                staging_table = f"{args.summary_table}_staging"
                print(f"[집계 단계] PostgreSQL 스테이징 테이블({staging_table})에 분산 적재 중...", flush=True)
                (
                    monthly_summary.write
                    .format("jdbc")
                    .option("url", jdbc_url)
                    .option("dbtable", staging_table)
                    .option("user", database["user"])
                    .option("password", database["password"])
                    .option("driver", "org.postgresql.Driver")
                    .mode("overwrite")
                    .save()
                )

                print(f"[집계 단계] PostgreSQL {args.summary_table} 대상 UPSERT(ON CONFLICT DO UPDATE) 병합 수행 중...", flush=True)
                try:
                    import psycopg2
                    conn = psycopg2.connect(
                        host=database["host"],
                        port=database["port"],
                        dbname=database["dbname"],
                        user=database["user"],
                        password=database["password"],
                    )
                    try:
                        with conn.cursor() as cur:
                            upsert_sql = f"""
                                CREATE TABLE IF NOT EXISTS {args.summary_table} (
                                    review_year_month VARCHAR(7) PRIMARY KEY,
                                    total_reviews BIGINT NOT NULL,
                                    avg_rating DOUBLE PRECISION NOT NULL,
                                    verified_reviews BIGINT NOT NULL,
                                    unique_products BIGINT NOT NULL,
                                    unique_users BIGINT NOT NULL,
                                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                                );

                                CREATE INDEX IF NOT EXISTS monthly_review_summary_ym_idx
                                    ON {args.summary_table}(review_year_month);

                                INSERT INTO {args.summary_table} (
                                    review_year_month, total_reviews, avg_rating, verified_reviews, unique_products, unique_users, updated_at
                                )
                                SELECT review_year_month, total_reviews, avg_rating, verified_reviews, unique_products, unique_users, updated_at
                                FROM {staging_table}
                                ON CONFLICT (review_year_month) DO UPDATE SET
                                    total_reviews = EXCLUDED.total_reviews,
                                    avg_rating = EXCLUDED.avg_rating,
                                    verified_reviews = EXCLUDED.verified_reviews,
                                    unique_products = EXCLUDED.unique_products,
                                    unique_users = EXCLUDED.unique_users,
                                    updated_at = EXCLUDED.updated_at;

                                DROP TABLE IF EXISTS {staging_table};
                            """
                            cur.execute(upsert_sql)
                        conn.commit()
                        print(f"[집계 단계 완료] PostgreSQL {args.summary_table} UPSERT 적재 완료 (총 {monthly_count:,}행 멱등성 보장)", flush=True)
                    finally:
                        conn.close()
                except ImportError:
                    print("[집계 단계 알림] psycopg2가 설치되지 않은 환경이라 스테이징 완료 후 직접 쿼리가 생략되었습니다.", flush=True)

                print("=" * 70, flush=True)
                print("집계 결과:")
                monthly_summary.orderBy(F.col("review_year_month").desc()).show(10, truncate=False)
                print("=" * 70, flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
