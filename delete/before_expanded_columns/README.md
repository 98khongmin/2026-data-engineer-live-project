# JSONL 원본 수집

JSONL을 읽어 1,000건씩 PostgreSQL에 저장하는 일반 Python 코드입니다.
`jobs/amazon_reviews_etl.py`라는 기존 파일명은 유지했지만 Spark 정제는 실행하지 않습니다.

## 실행

```bash
./submit-job.sh jobs/amazon_reviews_etl.py
```

흐름은 다음과 같습니다.

```text
states에서 마지막 줄 번호 읽기
→ JSONL을 읽으며 이미 완료한 줄 건너뛰기
→ 1,000건씩 PostgreSQL에 저장·커밋
→ states 갱신
```

- `raw_reviews.raw_payload`: 원본 JSON 문자열. 공백·추가 필드·줄바꿈을 유지합니다.
- 기본 키: `user_id + parent_asin + timestamp`. 같은 키는 `ON CONFLICT DO NOTHING`으로 건너뜁니다. 내용이 달라도 키가 같으면 먼저 저장한 행을 유지합니다.
- `states/All_Beauty.json`: 마지막으로 DB 저장을 완료한 줄 번호와 완료 여부를 기록합니다. 컨테이너 안에서는 `/opt/spark/states`로 접근합니다.
- DB 커밋 후 상태 기록 전에 중단되면 같은 구간을 다시 읽지만, 기본 키가 중복 저장을 막습니다.
- 상태는 임시 파일에 쓴 뒤 교체합니다. 여러 수집 작업을 동시에 실행하지 마세요.

핵심 함수:

| 함수 | 역할 |
| --- | --- |
| `load_state` | 마지막 저장 위치 읽기 |
| `save_state` | 진행 상태 파일 저장 |
| `save_batch` | 배치 DB 저장 → 커밋 → 상태 갱신 |
| `main` | 파일을 읽고 1,000건씩 모으기 |

## 처음 준비하거나 이미지가 바뀌었을 때

이미지에 PostgreSQL 연결 라이브러리를 설치합니다. 코드에서 Java/JDBC를 호출하지 않습니다.

```bash
docker build -t amazon-review-spark:3.5.9 .
docker compose up -d --wait
```

Spark 기반 이미지는 기존 실행 환경을 유지하기 위해 남겼습니다. 수집 자체는 Python으로만 실행하므로 워커에 작업을 분배하지 않습니다.
DB 초기화 SQL은 처음 DB 볼륨을 만들 때 `raw_reviews`를 생성합니다. 기존 DB에도 이 테이블이 있어야 합니다.
`.env`의 `INPUT_PATH`, `COLLECT_BATCH_SIZE`, `STATES_DIR`, DB 설정을 사용합니다.

## 중간 재개 시연

```bash
# 기본 1,000건 저장 후 중단
./submit-job.sh jobs/amazon_reviews_etl.py --max-batches 1

# 저장했던 줄 다음부터 이어서 실행
./submit-job.sh jobs/amazon_reviews_etl.py
```

이미 전체 수집이 끝난 파일은 처음부터 다시 읽도록 지정할 수 있습니다. DB 행은 삭제하지 않으며 같은 키는 모두 건너뜁니다.

```bash
./submit-job.sh jobs/amazon_reviews_etl.py --reset-state
```

## 간단한 구현의 전제

원본 파일을 수정하지 않고, 같은 DB에 한 번에 한 작업만 실행하는 과제용 코드입니다.
파일 이름·크기·DB 이름은 확인하지만 전체 파일 해시나 바이트 위치는 관리하지 않습니다.
재개할 때 파일 앞부분도 읽어서 완료한 줄을 건너뜁니다.
파일을 바꾸거나 DB 데이터를 삭제했다면 checkpoint를 확인하고 `--reset-state`를 사용하세요.
키 NULL/누락·잘못된 JSON·DB 오류가 나면 중단하며, 실패한 배치는 완료 처리하지 않습니다.
이전의 복잡한 구현과 상태 백업은 `delete/before_simple_collection/`에 보관했습니다.
