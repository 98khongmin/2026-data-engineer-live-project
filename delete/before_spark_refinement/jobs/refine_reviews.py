"""raw_reviews를 읽어 정제한 뒤 UUID5 review_id와 함께 amazon_reviews에 저장합니다."""
import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import sys
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("배치 크기는 1 이상이어야 합니다.")

    database = dict(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "reviews"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    # 읽기와 쓰기 연결을 나눠 저장 커밋이 읽기 커서를 닫지 않게 합니다.
    reader = psycopg2.connect(**database)
    writer = None
    read_count = valid_count = inserted_count = 0
    try:
        writer = psycopg2.connect(**database)
        with reader.cursor() as counter:
            counter.execute("SELECT count(*) FROM raw_reviews")
            total = counter.fetchone()[0]
        # 터미널에서는 한 줄을 갱신하고, 파일/Airflow 로그에는 완료 요약만 남깁니다.
        with reader.cursor(name="raw_review_reader") as cursor, tqdm(
            total=total, desc="정제", unit="건", dynamic_ncols=True,
            mininterval=0.5, disable=not sys.stderr.isatty(),
        ) as progress:
            cursor.execute('''
                SELECT user_id, parent_asin, "timestamp", asin, text,
                       rating, title, verified_purchase
                FROM raw_reviews
            ''')
            while True:
                rows = cursor.fetchmany(args.batch_size)
                if not rows:
                    break
                batch = []
                for row in rows:
                    review = transform(row)
                    if review is not None:
                        batch.append(review)
                read_count += len(rows)
                valid_count += len(batch)
                if batch:
                    inserted_count += save_batch(writer, batch)
                progress.set_postfix({"유효": f"{valid_count:,}", "신규": f"{inserted_count:,}"}, refresh=False)
                progress.update(len(rows))
        print(f"정제 완료: 원본 {read_count:,}건, 신규 저장 {inserted_count:,}건, "
              f"제외 {read_count - valid_count:,}건, 기존 ID 스킵 {valid_count - inserted_count:,}건", flush=True)
    finally:
        reader.close()
        if writer is not None:
            writer.close()


if __name__ == "__main__":
    main()
