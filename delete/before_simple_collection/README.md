# JSONL 원본 수집 → PostgreSQL

현재 실행은 정제가 아닌 원본 수집입니다. 기존 파일명 `jobs/amazon_reviews_etl.py`는 실행 명령 호환을 위해 유지했습니다. Spark 세션과 이미지에 이미 설치된 JDBC를 사용하지만 수집은 driver에서 순차 배치로 실행합니다. 워커에서 분산 정제하거나 행을 필터링하지 않습니다.

## 실행

클러스터가 실행 중이면:

```bash
./submit-job.sh jobs/amazon_reviews_etl.py
```

`submit-job.sh`는 `docker exec`로 기존 마스터 안에서 작업을 실행합니다. `.env`의 `INPUT_PATH`, `COLLECT_BATCH_SIZE`(기본 1000), `STATES_DIR`을 사용합니다.

처음 기동하거나 states 마운트를 적용할 때:

```bash
mkdir -p states
docker compose up -d --wait
```

이미지 재빌드가 필요한 경우:

```bash
docker build -t amazon-review-spark:3.5.9 .
```

## 저장 정책

- 대상: `public.raw_reviews`. 기존 정제 테이블 `public.amazon_reviews`는 보존합니다.
- 기본 키: `(user_id, parent_asin, timestamp)`. 원본 정수 timestamp를 BIGINT로 저장하며 초/밀리초 변환은 하지 않습니다.
- `raw_payload` TEXT에는 JSONL 한 줄을 UTF-8 문자열로 그대로 보관합니다. 앞뒤 공백, 키 순서, 줄바꿈, 추가 필드, NULL 값을 유지합니다. JSON을 다시 직렬화하거나 JSONB로 변환하지 않습니다.
- 키 세 컬럼만 별도로 추출합니다. `source_file`, `source_line`, `collected_at`은 관리용 컬럼으로 원문 밖에 저장합니다.
- 같은 키가 다시 들어오면 `ON CONFLICT DO NOTHING`으로 건너뜁니다. 같은 키지만 본문이 다른 행도 최초 저장본을 유지합니다. 따라서 동일 키를 가진 원본의 모든 버전을 저장하는 정책은 아닙니다.
- 키 누락/NULL, 문자열이 아닌 사용자·상품 ID, 정수가 아닌 timestamp, 잘못된 JSON 등은 오류로 중단합니다. 실패한 배치는 저장/진행 처리하지 않습니다. 입력은 수정하지 않습니다.
- 입력은 UTF-8 일반 JSONL 파일 하나를 사용합니다. gzip, 디렉터리, glob 입력은 지원하지 않습니다.

## 재개와 중복 방지

```text
최대 1,000행 읽기 → PostgreSQL 트랜잭션 커밋 → 상태 파일 원자적 갱신
```

호스트 `states/`는 Docker `/opt/spark/states`에 읽기·쓰기로 마운트됩니다. 예를 들어 입력이 `All_Beauty.jsonl`이면 `states/All_Beauty.json`을 사용합니다.

상태에는 원본 이름·크기·수정시각·SHA-256, DB 대상, 다음 바이트 위치, 커밋한 줄 수, 삽입/스킵 집계, 완료 여부가 들어갑니다. SHA-256은 입력 파일 변경 확인용이며 리뷰 ID를 생성하지 않습니다.

- 시작할 때 입력 파일 지문을 확인하고 마지막 위치부터 읽습니다. 진행률 확인을 위해 입력 전체를 다시 읽어 지문은 계산하지만, 완료 구간을 DB에 다시 저장하지 않습니다.
- DB 커밋 후 상태 기록 전에 중단됐다면 같은 배치를 다시 읽습니다. DB 기본 키가 중복을 방지합니다.
- 상태 파일은 임시 파일에 저장 후 fsync·rename합니다. 파일 잠금으로 같은 상태 파일에 대한 동시 수집을 막습니다. `.lock` 파일은 잠금용이므로 남아 있어도 정상입니다.
- 상태 파일 삽입/스킵 집계는 checkpoint로 확인된 처리 집계입니다. DB 커밋 직후 장애로 checkpoint가 유실되면 재시도에서 스킵으로 계산될 수 있으므로 전체 DB 행 수와 같은 의미는 아닙니다.
- 기본 제출 방식은 마스터 driver 포트를 공유하므로 작업을 한 번에 하나씩 실행하세요.
- 원본 파일, DB, 테이블이 바뀌면 기존 상태를 자동 재사용하지 않습니다. 상태 파일명은 파일명 기준이므로 같은 이름의 다른 데이터는 별도 `--states-dir`을 사용하세요.
- DB 볼륨을 삭제하면 상태도 함께 초기화하거나 `--reset-state`로 처음부터 재수집해야 합니다. 마지막 키가 DB에 없는 상태는 오류로 안내합니다. 임의로 DB의 일부 행을 삭제한 상황까지 상태 파일이 모두 복구해 주지는 않습니다.

재개 시연(1,000행 저장 후 정상 중단):

```bash
./submit-job.sh jobs/amazon_reviews_etl.py --max-batches 1
./submit-job.sh jobs/amazon_reviews_etl.py
```

처음부터 재읽기(DB 데이터는 삭제하지 않음):

```bash
./submit-job.sh jobs/amazon_reviews_etl.py --reset-state
```

특정 파일/배치 크기:

```bash
./submit-job.sh jobs/amazon_reviews_etl.py --input /opt/spark/data/All_Beauty.jsonl --batch-size 1000
```

## 결과 확인

```sql
SELECT count(*) FROM public.raw_reviews;
SELECT user_id, parent_asin, "timestamp", raw_payload
FROM public.raw_reviews LIMIT 1;
```

기존 정제 코드는 `delete/before_raw_collection/jobs/amazon_reviews_etl.py`에 보관했습니다. 검증용 코드·기록은 `delete/raw_collection_validation/`에 보관합니다.
