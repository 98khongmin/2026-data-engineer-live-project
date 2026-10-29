# JSONL 원본 수집 — 필드별 컬럼 저장

실행 명령:

```bash
./submit-job.sh jobs/amazon_reviews_etl.py
```

일반 Python으로 JSONL을 읽어 1,000건씩 `raw_reviews`에 저장합니다.
Spark 정제, NULL 제거, 본문 공백 제거, timestamp 단위 변환은 하지 않습니다.

## 원본 필드 → DB 컬럼

| 원본 필드 | PostgreSQL 타입 |
| --- | --- |
| user_id | TEXT |
| parent_asin | TEXT |
| timestamp | BIGINT |
| rating | DOUBLE PRECISION |
| title | TEXT |
| text | TEXT |
| images | JSONB |
| asin | TEXT |
| helpful_vote | BIGINT |
| verified_purchase | BOOLEAN |

위 10개는 실제 All_Beauty.jsonl 전체에서 확인한 원본 필드입니다.
`images`는 리스트이므로 해당 컬럼만 JSONB로 보관합니다.
추가로 `source_file`, `source_line`, `collected_at`을 관리용으로 저장합니다.

`main()`의 `batch.append(...)`에서 필드 순서대로 값을 모으고,
`save_batch()`의 `INSERT` 컬럼 목록과 같은 순서로 저장합니다.
본문·제목은 자르거나 trim하지 않습니다. 빈 문자열, 0, False, NULL도 구분합니다.
키가 아닌 필드가 누락되었거나 NULL이면 SQL NULL로 저장합니다.
미리 정하지 않은 새 필드가 나타나면 누락시키지 않고 오류로 중단합니다.
이 경우 테이블 컬럼과 Python 매핑에 새 필드를 추가해야 합니다.

JSON의 공백·키 순서·줄바꿈 표현을 통째로 보존하는 방식은 아닙니다.
각 필드의 값을 보존하는 방식이며 원문 JSONL 파일은 `data/`에 보관합니다.

## 중복과 진행 상태

기본 키는 `(user_id, parent_asin, timestamp)`이며 충돌 시 건너뜁니다.
같은 키지만 본문이 달라도 최초 저장본을 유지합니다.

```text
states에서 마지막 줄 읽기 → JSONL 읽기 → 1,000건 DB 저장·커밋 → states 갱신
```

`states/All_Beauty.json`의 `last_line`에 완료 위치를 저장합니다.
DB 커밋 후 상태 기록 전에 중단되어도, 재실행 시 기본 키가 중복 저장을 막습니다.
원본 파일과 DB를 유지하고 한 번에 하나의 수집 작업만 실행하는 과제용 코드입니다.
DB 테이블이 완전히 비어 있으면 남아 있는 checkpoint 대신 처음부터 자동 수집합니다.
파일을 수정하거나 DB 일부 행만 삭제했다면 상태를 확인하고 `--reset-state`를 사용하세요.

```bash
# 1,000건 저장 후 중단 / 다음 실행에서 재개
./submit-job.sh jobs/amazon_reviews_etl.py --max-batches 1
./submit-job.sh jobs/amazon_reviews_etl.py

# 처음부터 다시 읽기. DB는 삭제하지 않고 같은 키는 스킵
./submit-job.sh jobs/amazon_reviews_etl.py --reset-state
```

## 기존 DB 변경

기존 `raw_payload` 테이블은 `raw_reviews_payload_backup`으로 보관하고,
그 안의 기존 행을 새 컬럼 구조의 `raw_reviews`로 옮겼습니다.
DB의 행 수와 내용을 확인한 뒤 기존 완료 checkpoint를 그대로 사용합니다.
마이그레이션 SQL은 `postgres/migrations/002_expand_raw_reviews.sql`입니다.
새 DB는 `postgres/init/001_schema.sql`로 처음부터 컬럼 구조를 생성합니다.

```sql
SELECT user_id, parent_asin, "timestamp", rating, title, text,
       images, asin, helpful_vote, verified_purchase
FROM raw_reviews
LIMIT 5;
```

코드는 호스트 폴더를 마운트하므로 기존 컨테이너에서도 수정 즉시 적용됩니다.
이미지를 새로 배포할 때는 다음 명령으로 빌드합니다.

```bash
docker build -t amazon-review-spark:3.5.9 .
docker compose up -d --wait
```
