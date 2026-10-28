"""Spark 워커가 raw_reviews를 나눠 읽고 정제·UUID5 생성·DB 저장을 수행합니다."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import os
import sys
import socket
import time
from urllib.parse import quote
import uuid

import psycopg2
from psycopg2.extras import execute_values
from tqdm import tqdm


def make_review_id(user_id, parent_asin, timestamp):
    # 고정 namespace + 같은 세 값 = 매번 같은 UUID5.
    # 단순 문자열 연결 대신 JSON 배열을 써서 컬럼 경계를 구분합니다.
    key = json.dumps([user_id, parent_asin, timestamp], ensure_ascii=False, separators=(",", ":"))
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


def transform(row):
    """필수값·평점을 확인하고 본문/제목 공백과 시간을 정리합니다."""
    user_id, parent_asin, timestamp, asin, text, rating, title, verified = row
    if not user_id or not user_id.strip() or not parent_asin or not parent_asin.strip():
        return None
    if timestamp is None or not asin or not asin.strip() or not text or not text.strip():
        return None
    if rating is None or not 1 <= rating <= 5:
        return None

    # ID에는 정제 전 원본 키를 그대로 사용합니다. timestamp는 원본 밀리초입니다.
    review_id = make_review_id(user_id, parent_asin, timestamp)
    review_time = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=timestamp)
    review_title = None
    if title is not None:
        review_title = title.strip() or None
    return (
        review_id, user_id, parent_asin, timestamp,
        asin.strip(), text.strip(), rating,
        review_title,
        review_time, verified,
    )


def save_batch(connection, batch):
    """같은 UUID5가 이미 있으면 건너뛰고, 새 정제 결과만 저장합니다."""
    sql = '''
        INSERT INTO amazon_reviews (
            review_id, user_id, parent_asin, "timestamp", product_id,
            review_text, rating, review_title, review_time, verified_purchase
        ) VALUES %s
        ON CONFLICT (review_id) DO NOTHING
    '''
    with connection.cursor() as cursor:
        execute_values(cursor, sql, batch, page_size=len(batch))
        inserted = cursor.rowcount
    connection.commit()
    return inserted



def process_partition(rows, database, batch_size):
    """각 Spark 태스크가 담당 구간을 정제하고 DB에 직접 배치 저장합니다."""
    from pyspark import TaskContext
    connection = psycopg2.connect(**database)
    read_count = valid_count = inserted_count = 0
    batch = []
    try:
        for row in rows:
            read_count += 1
            review = transform(row)
            if review is None:
                continue
            valid_count += 1
            batch.append(review)
            if len(batch) == batch_size:
                inserted_count += save_batch(connection, batch)
                batch = []
        if batch:
            inserted_count += save_batch(connection, batch)
    finally:
        connection.close()
    # 전체 리뷰를 driver로 가져오지 않고 파티션별 집계만 반환합니다.
    yield {"partition": TaskContext.get().partitionId(), "worker": socket.gethostname(),
           "read": read_count, "valid": valid_count, "inserted": inserted_count}


def collect_with_progress(spark, results, partitions):
    """실제 분산 작업은 동시에 실행하고 완료된 태스크 수를 표시합니다."""
    context = spark.sparkContext
    tracker = context.statusTracker()
    group = "refine-reviews"

    def run_job():
        context.setJobGroup(group, "원본 리뷰 분산 정제", interruptOnCancel=True)
        return results.collect()

    with ThreadPoolExecutor(max_workers=1) as pool, tqdm(
        total=partitions, desc="Spark 정제", unit="파티션", dynamic_ncols=True,
        # spark-submit 내부 Python 출력은 파이프이므로 호스트의 TTY 여부도 전달받습니다.
        disable=not (sys.stderr.isatty() or os.getenv("SHOW_PROGRESS") == "1"),
    ) as progress:
        future = pool.submit(run_job)
        try:
            while not future.done():
                stages = set()
                for job_id in tracker.getJobIdsForGroup(group):
                    job = tracker.getJobInfo(job_id)
                    if job is not None:
                        stages.update(job.stageIds)
                completed = 0
                for stage_id in stages:
                    stage = tracker.getStageInfo(stage_id)
                    if stage is not None:
                        completed += stage.numCompletedTasks
                progress.update(max(0, min(completed, partitions) - progress.n))
                time.sleep(0.3)
            counts = future.result()
            progress.update(partitions - progress.n)
            return counts
        except KeyboardInterrupt:
            context.cancelJobGroup(group)
            raise


def main():
    from pyspark.sql import SparkSession
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--partitions", type=int, default=4, help="DB를 나눠 읽고 처리할 태스크 수")
    args = parser.parse_args()
    if args.batch_size < 1 or args.partitions < 1:
        parser.error("배치 크기와 파티션 수는 1 이상이어야 합니다.")

    database = dict(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "reviews"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    spark = SparkSession.builder.appName("amazon-reviews-distributed-refinement").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    connection = None
    try:
        connection = psycopg2.connect(**database)
        with connection.cursor() as cursor:
            cursor.execute('SELECT count(*), min(source_line), max(source_line) FROM raw_reviews')
            total, lower, upper = cursor.fetchone()
            cursor.execute('SELECT count(*) FROM amazon_reviews')
            before = cursor.fetchone()[0]
        connection.commit()
        if total == 0:
            print("원본 테이블이 비어 있습니다. 수집 작업을 먼저 실행하세요.", flush=True)
            return

        url = f"jdbc:postgresql://{database['host']}:{database['port']}/{quote(database['dbname'], safe='')}"
        # source_line 구간을 나누므로 각 워커가 DB에서 자기 구간을 직접 읽습니다.
        raw = (spark.read.format("jdbc")
               .option("url", url).option("dbtable", "public.raw_reviews")
               .option("user", database["user"]).option("password", database["password"])
               .option("driver", "org.postgresql.Driver").option("fetchsize", args.batch_size)
               .option("partitionColumn", "source_line").option("lowerBound", lower)
               .option("upperBound", upper + 1).option("numPartitions", args.partitions)
               .load()
               .select("user_id", "parent_asin", "timestamp", "asin", "text", "rating", "title", "verified_purchase"))
        rows = raw.rdd
        batch_size = args.batch_size
        results = rows.mapPartitions(lambda partition: process_partition(partition, database, batch_size))
        print(f"Spark 마스터: {spark.sparkContext.master}; 읽기/처리 파티션: {rows.getNumPartitions()}", flush=True)
        counts = collect_with_progress(spark, results, rows.getNumPartitions())

        with connection.cursor() as cursor:
            cursor.execute('SELECT count(*) FROM amazon_reviews')
            after = cursor.fetchone()[0]
        # 재시도 중 커밋된 행까지 포함하기 위해 실제 DB 증가분을 사용합니다.
        # 같은 정제 테이블에 쓰는 다른 작업은 동시에 실행하지 않습니다.
        read_count = sum(part["read"] for part in counts)
        valid_count = sum(part["valid"] for part in counts)
        inserted_count = after - before
        for part in sorted(counts, key=lambda part: part["partition"]):
            print(f"파티션 {part['partition']}: {part['worker']} / 원본 {part['read']:,}건 / 유효 {part['valid']:,}건", flush=True)
        print(f"정제 완료: 원본 {read_count:,}건, 신규 저장 {inserted_count:,}건, "
              f"제외 {read_count - valid_count:,}건, 기존 ID 스킵 {valid_count - inserted_count:,}건", flush=True)
    finally:
        if connection is not None:
            connection.close()
        spark.stop()


if __name__ == "__main__":
    main()
