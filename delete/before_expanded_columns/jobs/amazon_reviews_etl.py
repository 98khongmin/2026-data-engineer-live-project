"""JSONL을 1,000건씩 PostgreSQL에 원본 그대로 저장하는 수집 코드."""
import argparse
import json
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values


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
            (user_id, parent_asin, "timestamp", raw_payload, source_file, source_line)
        VALUES %s
        ON CONFLICT (user_id, parent_asin, "timestamp") DO NOTHING
    '''
    with connection.cursor() as cursor:
        execute_values(cursor, sql, batch, page_size=len(batch))
        inserted = cursor.rowcount
    connection.commit()

    last_line = batch[-1][-1]
    save_state(state_file, input_file, last_line)
    print(f"{last_line:,}행까지 완료: 저장 {inserted}건, 중복 스킵 {len(batch) - inserted}건", flush=True)


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
    print(f"수집 시작: {input_file.name}, {last_line + 1:,}행부터", flush=True)

    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "reviews"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        batch = []
        batches = 0
        # newline=""으로 원본의 줄바꿈 표현까지 보존합니다.
        with input_file.open(encoding="utf-8", newline="") as source:
            for line_number, raw_line in enumerate(source, start=1):
                if line_number <= last_line:
                    continue

                review = json.loads(raw_line)
                user_id = review.get("user_id")
                parent_asin = review.get("parent_asin")
                timestamp = review.get("timestamp")
                if (not isinstance(user_id, str) or not isinstance(parent_asin, str)
                        or type(timestamp) is not int):
                    raise ValueError(f"{line_number}행의 기본 키가 누락되었거나 형식이 잘못되었습니다.")

                batch.append((user_id, parent_asin, timestamp, raw_line, input_file.name, line_number))
                if len(batch) == args.batch_size:
                    save_batch(connection, batch, state_file, input_file)
                    last_line = line_number
                    batch = []
                    batches += 1
                    if args.max_batches is not None and batches >= args.max_batches:
                        print("지정한 배치 수까지 저장했습니다. 다음 실행에서 이어집니다.", flush=True)
                        return

        if batch:
            save_batch(connection, batch, state_file, input_file)
            last_line = batch[-1][-1]
        save_state(state_file, input_file, last_line, completed=True)
        print(f"수집 완료: {last_line:,}행", flush=True)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
