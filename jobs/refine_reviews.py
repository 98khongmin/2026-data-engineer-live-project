"""Spark 기반 원본 분산 로딩, review_id 중복 제거, 날짜별 Parquet 저장 및 PostgreSQL 일별 집계 파이프라인."""
import argparse
import json
import os
from urllib.parse import quote
import uuid

from pyspark.sql import SparkSession, functions as F, types as T

NAMESPACE_URL = uuid.NAMESPACE_URL


@F.udf(returnType=T.StringType())
def make_review_id(user_id, parent_asin, timestamp):
    """user_id, parent_asin, timestamp를 결합해 고유한 UUID5 review_id를 생성합니다."""
    if not user_id or not parent_asin or timestamp is None:
        return None
    key = json.dumps([user_id, parent_asin, timestamp], ensure_ascii=False, separators=(",", ":"))
    return str(uuid.uuid5(NAMESPACE_URL, key))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", default=os.getenv("RAW_DIR", "/opt/spark/data/raw"),
                        help="수집기가 적재한 원본 JSON/JSONL 디렉토리")
    parser.add_argument("--processed-dir", default=os.getenv("PROCESSED_DIR", "/opt/spark/data/processed/reviews"),
                        help="정제 완료 원본을 저장할 Parquet 경로")
    parser.add_argument("--summary-table", default="public.daily_review_summary",
                        help="일별 집계를 저장할 PostgreSQL 테이블명")
    parser.add_argument("--partitions", type=int, default=4, help="Spark 처리 파티션 수")
    args = parser.parse_args()

    database = dict(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "reviews"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )

    spark = (
        SparkSession.builder
        .appName("amazon-reviews-parquet-and-aggregation")
        .config("spark.sql.shuffle.partitions", str(args.partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("=" * 70, flush=True)
        print(f"[1단계: Raw 로딩] {args.raw_dir} 원본 파일 일괄 읽기 시작", flush=True)
        # raw/ 디렉토리 내의 모든 JSON/JSONL 파일을 한 번에 읽음
        raw_df = spark.read.json(args.raw_dir)
        total_raw_count = raw_df.count()
        print(f"[1단계: Raw 로딩 완료] 총 원본 레코드 수: {total_raw_count:,}건", flush=True)

        print("-" * 70, flush=True)
        print("[2단계: 정제 & 중복 제거] 필수값 필터링 및 review_id 기준 dropDuplicates 수행", flush=True)

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
                F.col("rating").cast("double").alias("rating"),
                F.trim(F.col("title")).alias("review_title"),
                F.trim(F.col("text")).alias("review_text"),
                F.coalesce(F.col("verified_purchase"), F.lit(False)).alias("verified_purchase"),
            )
        ).repartition(args.partitions)

        refined_df.cache()
        refined_count = refined_df.count()
        dedup_count = total_raw_count - refined_count
        print(f"[2단계: 정제 완료] 정제 및 중복 제거 후 레코드 수: {refined_count:,}건 (제외/중복: {dedup_count:,}건)", flush=True)

        # 2단계 저장: 날짜별 파티션 Parquet으로 저장 (Silver Layer)
        print(f"[2단계: Parquet 저장] {args.processed_dir} 에 partitionBy('review_date') 로 저장 중...", flush=True)
        (
            refined_df.write
            .mode("overwrite")
            .partitionBy("review_date")
            .parquet(args.processed_dir)
        )
        print("[2단계: Parquet 저장 완료] 날짜별 폴더 구조 생성 완료", flush=True)

        # 3단계: 일별 집계 생성 (Gold Layer / Mart)
        print("-" * 70, flush=True)
        print("[3단계: 일별 집계] review_date 기준 리뷰 수, 평균 평점, 구매인증 수 집계", flush=True)
        daily_summary = (
            refined_df.groupBy("review_date")
            .agg(
                F.count("*").alias("total_reviews"),
                F.round(F.avg("rating"), 2).alias("avg_rating"),
                F.sum(F.when(F.col("verified_purchase") == True, 1).otherwise(0)).alias("verified_reviews"),
                F.countDistinct("product_id").alias("unique_products"),
                F.countDistinct("user_id").alias("unique_users"),
            )
            .withColumn("updated_at", F.current_timestamp())
            .orderBy("review_date")
        )

        daily_count = daily_summary.count()
        print(f"[3단계: 집계 완료] 총 집계 일수: {daily_count:,}일", flush=True)

        # 3단계 저장: PostgreSQL 테이블로 JDBC 적재
        jdbc_url = f"jdbc:postgresql://{database['host']}:{database['port']}/{quote(database['dbname'], safe='')}"
        print(f"[3단계: DB 적재] PostgreSQL {args.summary_table} 테이블에 적재 중...", flush=True)
        (
            daily_summary.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", args.summary_table)
            .option("user", database["user"])
            .option("password", database["password"])
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )
        print(f"[3단계: DB 적재 완료] PostgreSQL {args.summary_table} 적재 완료 (총 {daily_count:,}행)", flush=True)

        print("=" * 70, flush=True)
        print("최근 5일간 집계 결과 샘플:")
        daily_summary.orderBy(F.col("review_date").desc()).show(5, truncate=False)
        print("=" * 70, flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
