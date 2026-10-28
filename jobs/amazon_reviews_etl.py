"""JSONL을 1,000건씩 PostgreSQL에 원본 그대로 저장하는 수집 코드."""
import argparse
import json
import os
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, execute_values
from tqdm import tqdm


REVIEW_COLUMNS = {
    "user_id", "parent_asin", "timestamp", "rating", "title", "text",
    "images", "asin", "helpful_vote", "verified_purchase",
}


def load_state(state_file, input_file):
    """이전에 저장을 완료한 줄 번호를 읽습니다."""
    if not state_file.exists():
        return 0

    state = json.loads(state_file.read_text(encoding="utf-8"))
    if (state["source_file"] != input_file.name
            or state["source_size"] != input_file.stat().st_size
            or state["database"] != os.getenv("POSTGRES_DB", "reviews")):
        raise ValueError("입력 파일 또는 DB가 변경되었습니다. 상태를 확인하거나 --reset-state를 사용하세요.")
    return state["last_line"]


def save_state(state_file, input_file, last_line, completed=False):
    """임시 파일에 쓴 뒤 교체하여 상태 파일이 반쪽만 저장되는 것을 방지합니다."""
    state = {
        "source_file": input_file.name,
        "source_size": input_file.stat().st_size,
        "database": os.getenv("POSTGRES_DB", "reviews"),
        "last_line": last_line,
        "completed": completed,
    }
    temporary = state_file.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(state_file)


def save_batch(connection, batch, state_file, input_file):
    """DB 저장에 성공한 다음에만 진행 상태를 갱신합니다."""
    sql = '''
        INSERT INTO raw_reviews
            (user_id, parent_asin, "timestamp", rating, title, text,
             images, asin, helpful_vote, verified_purchase, source_file, source_line)
        VALUES %s
        ON CONFLICT (user_id, parent_asin, "timestamp") DO NOTHING
    '''
    with connection.cursor() as cursor:
        execute_values(cursor, sql, batch, page_size=len(batch))
        inserted = cursor.rowcount
    connection.commit()

    last_line = batch[-1][-1]
    save_state(state_file, input_file, last_line)
    return inserted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=os.getenv("INPUT_PATH", "/opt/spark/data/All_Beauty.jsonl"))
    parser.add_argument("--states-dir", default=os.getenv("STATES_DIR", "/opt/spark/states"))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("COLLECT_BATCH_SIZE", "1000")))
    parser.add_argument("--max-batches", type=int, help="지정한 배치 수만 저장하고 중단")
    parser.add_argument("--reset-state", action="store_true", help="DB는 유지하고 처음부터 다시 읽기")
    args = parser.parse_args()
    if args.batch_size < 1 or (args.max_batches is not None and args.max_batches < 1):
        parser.error("배치 크기와 배치 수는 1 이상이어야 합니다.")

    input_file = Path(args.input)
    states_dir = Path(args.states_dir)
    states_dir.mkdir(parents=True, exist_ok=True)
    state_file = states_dir / f"{input_file.stem}.json"
    last_line = 0 if args.reset_state else load_state(state_file, input_file)

    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "reviews"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        # DB를 초기화해도 states 파일은 남을 수 있습니다.
        with connection.cursor() as cursor:
            cursor.execute("SELECT EXISTS (SELECT 1 FROM raw_reviews)")
            has_data = cursor.fetchone()[0]
        if last_line > 0 and not has_data:
            print("DB가 비어 있어 체크포인트 대신 처음부터 수집합니다.", flush=True)
            last_line = 0
        print(f"수집 시작: {input_file.name}, {last_line + 1:,}행부터", flush=True)

        batch = []
        batches = 0
        inserted_count = skipped_count = 0
        with input_file.open(encoding="utf-8", newline="") as counter:
            total_lines = sum(1 for _ in counter)
        # JSONL의 필드를 읽어 같은 이름의 DB 컬럼에 넣습니다.
        with input_file.open(encoding="utf-8", newline="") as source, tqdm(
            total=total_lines, initial=last_line, desc="수집", unit="행",
            dynamic_ncols=True, mininterval=0.5, disable=not sys.stderr.isatty(),
        ) as progress:
            for line_number, raw_line in enumerate(source, start=1):
                if line_number <= last_line:
                    continue

                review = json.loads(raw_line)
                unknown_columns = set(review) - REVIEW_COLUMNS
                if unknown_columns:
                    raise ValueError(f"DB 컬럼을 추가해야 하는 필드가 있습니다: {unknown_columns}")
                user_id = review.get("user_id")
                parent_asin = review.get("parent_asin")
                timestamp = review.get("timestamp")
                if (not isinstance(user_id, str) or not isinstance(parent_asin, str)
                        or type(timestamp) is not int):
                    raise ValueError(f"{line_number}행의 기본 키가 누락되었거나 형식이 잘못되었습니다.")

                # NULL은 None으로 유지합니다. 본문 자르기나 공백 제거는 하지 않습니다.
                # images는 리스트이므로 Json으로 감싸 JSONB 컬럼에 저장합니다.
                images = review.get("images")
                batch.append((
                    user_id, parent_asin, timestamp,
                    review.get("rating"),
                    review.get("title"),
                    review.get("text"),
                    Json(images) if images is not None else None,
                    review.get("asin"),
                    review.get("helpful_vote"),
                    review.get("verified_purchase"),
                    input_file.name, line_number,
                ))
                if len(batch) == args.batch_size:
                    inserted = save_batch(connection, batch, state_file, input_file)
                    inserted_count += inserted
                    skipped_count += len(batch) - inserted
                    progress.set_postfix({"저장": f"{inserted_count:,}", "중복": f"{skipped_count:,}"}, refresh=False)
                    progress.update(len(batch))
                    last_line = line_number
                    batch = []
                    batches += 1
                    if args.max_batches is not None and batches >= args.max_batches:
                        progress.close()
                        print(f"수집 중단: {last_line:,}행까지, 저장 {inserted_count:,}건, 키 중복 스킵 {skipped_count:,}건", flush=True)
                        return

            if batch:
                inserted = save_batch(connection, batch, state_file, input_file)
                inserted_count += inserted
                skipped_count += len(batch) - inserted
                progress.set_postfix({"저장": f"{inserted_count:,}", "중복": f"{skipped_count:,}"}, refresh=False)
                progress.update(len(batch))
                last_line = batch[-1][-1]
        save_state(state_file, input_file, last_line, completed=True)
        print(f"수집 완료: {last_line:,}행, 이번 실행 저장 {inserted_count:,}건, 키 중복 스킵 {skipped_count:,}건", flush=True)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
