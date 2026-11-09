# Amazon 리뷰 수집·정제 파이프라인

## 사용 데이터 및 분석 목적

- **데이터셋:** Amazon Reviews 2023의 `All_Beauty` 카테고리
- **출처:** McAuley-Lab / Hugging Face Datasets (`McAuley-Lab/Amazon-Reviews-2023`)
- **현재 입력:** Hugging Face Datasets 직접 다운로드 URL (`All_Beauty.jsonl`)
  - `https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw/review_categories/All_Beauty.jsonl`
  - 수집기가 다운로드하여 `data/raw/All_Beauty.jsonl`에 적재 (이미 파일이 존재하면 다운로드 자동 스킵)
- **입력 규모:** 701,528행, 326,611,506바이트(약 311.5 MiB)
- **데이터 단위:** 사용자 리뷰 1건. 평점, 제목, 본문, 상품·사용자 식별자, 밀리초 단위 작성 시각 등을 포함합니다.

분석 목표는 어떤 상품의 어떤 문제부터 개선해야 하는가? 입니다.
현재는 분석에 사용할 데이터를 수집·정제하는 단계까지 구현했습니다.

| 향후 분석 | 제공하려는 결과 |
| --- | --- |
| 평점과 본문 감정 점수 비교 | 별점은 높지만 본문에 불만이 있는 리뷰 찾기 |
| 평점 구간별 키워드 분석 | 낮은 평점의 주요 불만과 높은 평점의 구매 이유 파악 |
| 상품별 월별 리뷰 수·평점 추이 | 상품 평가 악화와 리뷰 수 증가 추이 확인 |

약 70만 행으로 로컬 개발과 검증을 진행하기에 적절해 이 카테고리를 선택했습니다.
향후 다른 대용량 카테고리로 확장할 때도 수집·정제 구조를 활용하되,
입력 필드, DB 용량, 파티션 수와 처리 성능을 확인할 계획입니다.

## 현재 구조와 도구 선정 이유

**수집기(JSONL) → 원본 적재(`data/raw/`) → Raw Parquet 준비(`data/raw_parquet/`) → Spark 분산 정제 및 Parquet 저장(`data/processed/<category>/`) → PostgreSQL 월별 요약 집계(`monthly_review_summary`) → 품질 검증** 흐름으로 동작합니다.

| 계층 | 역할과 기술 스택 | 저장 포맷 및 위치 | 기본키(PK) / 식별자 |
| :--- | :--- | :--- | :--- |
| **1단계: 원본 수집** | 수집기(`collector.py`)가 외부 원본 JSONL을 로컬 스토리지에 안정적으로 다운로드 | JSONL (`data/raw/`) | 없음 (원시 로그 파일) |
| **2단계: Raw Parquet 준비** | `refine_reviews.py --step convert_raw_to_parquet`로 원본 JSONL을 월별 파티션 Raw Parquet으로 1회 변환 및 검증 (완전 시 Spark 미기동 Fast-Skip) | Parquet (`data/raw_parquet/<category>/`)<br/>`partitionBy("review_year_month")` | 없음 (원천 파티션 보관) |
| **3단계: 정제 & 중복 제거** | 대상 월 Raw Parquet 파티션만 직접 읽어 결측치 필터링 및 `review_id` 기준 분산 중복 제거 (해당 월 이미 존재 시 스킵) | Parquet (`data/processed/<category>/`)<br/>`partitionBy("review_year_month")` | `review_id` (UUID5: user_id + parent_asin + timestamp) |
| **4단계: 월별 요약 집계** | Spark로 카테고리-월별 리뷰 수, 평균 평점 등을 요약 집계 후 PostgreSQL UPSERT 적재 (멱등성 보장, 5-1/5-2단계와 병렬 실행) | PostgreSQL (`public.monthly_review_summary`) | `(category, review_year_month)` (VARCHAR 복합 PK) |
| **5-1단계: 상품 분석 마트** | 상품별 월별 지표(분기 포함) 집계, 리뷰 급증(100%+) 뷰, 저/다리뷰 세그먼트 비교 마트 UPSERT (4/5-2단계와 병렬 실행) | PostgreSQL (`public.product_monthly_metrics`, `public.product_segment_comparison`) | `(category, product_id, review_year_month)` 등 |
| **5-2단계: 감정분석 마트** | Spark 기반 감정분석(NLTK VADER 기본, GPU 가속 트랜스포머 2순위) 및 별점 괴리 불만 리뷰 추출 UPSERT 적재 (4/5-1단계와 병렬 실행) | PostgreSQL (`public.product_sentiment_monthly`, `public.product_sentiment_anomalies`) | `(category, product_id, review_year_month)` 등 |
| **6단계: 품질 검증** | Python으로 3자 집계 적재 건수, 평점 범위, 감정 점수 범위, 결측치, 뷰 조회 및 세그먼트 보존 검증 | 검증 스크립트 (SQL 쿼리) | - |
| **7단계: FastAPI 서빙** | ConnectionPool 기반 고속 서빙 API (배치 추론 결과 서빙, 422/404 예외 규격 준수, 단건 실시간 추론 비교 제공) | FastAPI (`http://localhost:8000`) | - |

### 2단계(Bronze → Silver) 분리 및 장애 방어 설계

* **왜 2단계로 나누었나요? (풀 스캔 방지)**:
  매달 수백만 건의 원본 파일 전체를 처음부터 끝까지 다 읽으면 너무 느립니다. 그래서 **2단계(Bronze)**에서 원본을 월별 파티션 Parquet으로 딱 1번만 변환해 두고, **3단계(Silver)**에서는 이번 달 데이터만 쏙 골라 읽어 빠르게 정제합니다.
* **어떻게 안정성을 높였나요? (메모리 절약 & 장애 방어)**:
  * **메모리 부족(OOM) 방지**: 불필요한 캐시(`.cache()`)를 없애고 메모리를 1.6GB로 늘려 버퍼 부담을 줄였습니다.
  * **원자적 저장(Atomic Swap)**: 작업 중 서버가 꺼져도 데이터가 깨지지 않도록, 임시 폴더에서 먼저 작업한 뒤 성공했을 때만 한 번에 정상 폴더로 교체합니다.
  * **재실행 멱등성**: 작업이 실패해 다시 돌려도 PostgreSQL `UPSERT`를 적용해 데이터 중복 없이 기존 데이터만 안전하게 덮어씁니다.

### 저장 포맷을 고른 이유

1. **정제 원본 ➔ Parquet (컬럼 지향 포맷)**:
   - 70만 건 이상의 대용량 상세 데이터를 행 단위 RDBMS에 전부 저장하면 DB CPU/I/O 병목 및 스토리지 낭비가 발생합니다.
   - Parquet은 컬럼별 Snappy 압축률이 우수하며, `partitionBy("review_year_month")`로 월별 파티션 프루닝(Partition Pruning)과 필요한 컬럼만 읽는 컬럼 프루닝(Column Pruning)을 통해 대규모 분석 쿼리 속도를 극대화합니다. 일별 파티셔닝 대비 파일/디렉토리 수를 대폭 줄여 작은 파일 문제(Small Files Problem)를 방지합니다.
2. **집계 통계 ➔ PostgreSQL (관계형 데이터베이스)**:
   - 월별로 요약 집계된 데이터(약 260행)는 데이터 크기가 작고, Grafana 대시보드나 백엔드 서비스에서 빈번하고 빠른 포인트 조회가 일어납니다.
   - 인덱스와 트랜잭션(ACID)을 지원하는 RDBMS가 서빙 및 시각화 도구 연동에 가장 적합합니다.

### 실행 방법

파이프라인 실행은 호스트 머신의 `submit-job.sh` 스크립트를 통해 Docker 컨테이너 클러스터(`spark-master`) 내부로 전달되어 실행됩니다.

#### 1. 인프라 컨테이너 빌드 및 실행 (사전 준비)
```bash
# 1) Spark 커스텀 이미지 빌드 (NLTK VADER 사전 및 Hugging Face DistilBERT 모델 사전 캐싱)
docker compose build

# 2) 전체 인프라 컨테이너 백그라운드 실행
docker compose up -d
```

#### 2. 원본 데이터 수집 실행
```bash
./submit-job.sh jobs/collector.py
```
- 이미 `data/raw/All_Beauty.jsonl`이 존재하고 크기가 유효할 경우, 네트워크 다운로드를 자동으로 스킵합니다.
- 처음부터 강제로 다시 다운로드 및 수집하려면 `--reset-state` 플래그를 추가합니다.

#### 3. Spark 정제 및 요약 집계 실행
```bash
# 1) 원본 JSONL -> 월별 Raw Parquet 파티션 변환 및 준비 (최초 1회 실행, 이미 완료 시 즉시 스킵)
./submit-job.sh jobs/refine_reviews.py --step convert_raw_to_parquet

# 2) 특정 월(예: 2023-01) 증분 실행 (해당 월 Raw Parquet만 직접 읽어 Full Scan 제거, 정제 파티션 존재 시 스킵)
./submit-job.sh jobs/refine_reviews.py --step refine_store_parquet --year-month 2023-01
./submit-job.sh jobs/refine_reviews.py --step aggregate_monthly --year-month 2023-01

# 3) 상품별 분석 마트 및 세그먼트 분석 실행 (PostgreSQL UPSERT)
./submit-job.sh jobs/product_analytics.py --step all --year-month 2023-01
# 또는 전체 기간 마트 적재
./submit-job.sh jobs/product_analytics.py --step all --year-month all

# 4) 감정분석 및 괴리 리뷰 마트 적재 (NLTK 기본 / GPU 트랜스포머 지원)
./submit-job.sh jobs/sentiment_analysis.py --year-month 2023-01 --model-type auto

# 또는 과거 전체 데이터 일괄 실행
./submit-job.sh jobs/refine_reviews.py --step all --year-month all
```

#### 4. FastAPI 서빙 API 실행 (6회차 강의자료 규격)
```bash
# 로컬 개발 서버 실행 (자동 문서: http://localhost:8000/docs)
uvicorn api.app:app --host 0.0.0.0 --port 8000 --reload

# API 호출 예시 (상품별 감정 요약 조회)
curl "http://localhost:8000/products/B00004U9V2/sentiment?dt=2023-01&category=All_Beauty"

# API 호출 예시 (별점 괴리 불만 리뷰 조회)
curl "http://localhost:8000/products/B00004U9V2/anomalies?dt=2023-01&limit=3"

# API 호출 예시 (카테고리별 워스트 상품 랭킹)
curl "http://localhost:8000/categories/All_Beauty/sentiment/rankings?dt=2023-01&order=worst&limit=5"

# 실시간 단건 추론 테스트 (배치 서빙과 지연시간 비교용)
curl -X POST "http://localhost:8000/sentiment/predict" \
  -H "Content-Type: application/json" \
  -d '{"text": "The delivery was super fast, but the item broke in two days. Horrible quality!"}'
```

#### 5. 결과 확인
- **월별 Raw Parquet 파티션 확인:**
  ```bash
  ls -lh data/raw_parquet/All_Beauty/
  ```
- **정제된 Parquet 파일 확인:**
  ```bash
  ls -lh data/processed/All_Beauty/
  ```
- **PostgreSQL 월별 요약 테이블 조회:**
  ```bash
  docker compose exec postgres psql -U postgres -d postgres -c "SELECT * FROM monthly_review_summary ORDER BY review_year_month DESC LIMIT 5;"
  ```
- **PostgreSQL 상품별 마트 및 급증 뷰 조회:**
  ```bash
  docker compose exec postgres psql -U postgres -d postgres -c "SELECT * FROM product_monthly_metrics LIMIT 5;"
  docker compose exec postgres psql -U postgres -d postgres -c "SELECT * FROM v_product_review_surge LIMIT 5;"
  docker compose exec postgres psql -U postgres -d postgres -c "SELECT * FROM product_segment_comparison;"
  ```

---

## Airflow DAG 설계 및 구현

월별 Amazon 리뷰 데이터 파이프라인 dag.py의 6단계 자동화 및 오케스트레이션 명세입니다.

### 1. DAG 구조 (병렬 분기 및 수렴)

`download_raw_data >> prepare_raw_parquet >> refine_store_parquet >> [aggregate_monthly, aggregate_product, analyze_sentiment] >> validate_data`

* **`download_raw_data`**: 외부 원본 JSONL 다운로드 (기존 파일 존재 시 자동 스킵)
* **`prepare_raw_parquet`**: 원본 JSONL을 월별 파티션 Raw Parquet(`data/raw_parquet/<category>/`)으로 최초 1회 변환 및 무결성 검증. 루트 `_SUCCESS` 및 월별 파티션(`_SUCCESS`, `_manifest.json`)이 유효하면 Spark 세션 없이 0.1초 만에 즉시 정상 종료하여 매월 전체 JSONL 재스캔을 방지합니다.
* **`refine_store_parquet`**: 대상 월의 Raw Parquet 파티션(`review_year_month=YYYY-MM`)만 직접 읽어 결측치 필터링, UUID5 중복 제거 후 월별 정제 Parquet 저장 (해당 월 정제 파티션 이미 존재 시 스킵, Raw Parquet 부재 시 JSONL 스캔 폴백)
* **`aggregate_monthly`**: 월별 리뷰 수·평균 평점 요약 집계 후 PostgreSQL UPSERT 적재 (멱등성 보장, 3자 병렬 실행)
* **`aggregate_product`**: 상품별 월별 지표(review_quarter 포함) 집계, PostgreSQL `product_monthly_metrics` UPSERT 및 저/다리뷰 세그먼트 비교 마트(`product_segment_comparison`) 갱신 (3자 병렬 실행)
* **`analyze_sentiment`**: 정제 리뷰 본문 감정분석(NLTK 기본 / GPU 트랜스포머 지원) 수행, PostgreSQL `product_sentiment_monthly` 및 별점 괴리 불만 리뷰(`product_sentiment_anomalies`) UPSERT 적재 (3자 병렬 실행)
* **`validate_data`**: DB 3자 집계 적재 건수, 평점(1.0~5.0) 이상치, 감정 점수(-1.0~1.0) 유효성, 뷰 정상 동작 및 세그먼트 보존 검증

### 2. retry · backoff 전략

장애 유형에 따라 **재시도 대상**과 **즉시 로그 기록 대상**을 분리했습니다.

* **재시도 대상 (일시적 장애 자동 극복)**:
  - `download_raw_data`: 3회 재시도 (2분 ➔ 4분 ➔ 8분 지수 백오프, 외부 웹 서버 일시 타임아웃 대비)
  - `prepare_raw_parquet`: 1회 재시도 (5분 대기, Spark 일시 리소스 경합/OOM 대비)
  - `refine_store_parquet`: 1회 재시도 (5분 대기, Spark 일시 리소스 경합/OOM 대비)
  - `aggregate_monthly`: 2회 재시도 (3분 간격, PostgreSQL 커넥션 풀/트랜잭션 락 대비)
* **재시도 없음 (즉시 실패 감지)**:
  - `validate_data`: 0회 (재시도 없음). 데이터 결함/오염은 반복 실행해도 실패하므로 즉시 중단 후 `on_failure_callback`으로 콘솔 에러 로그 기록
  - ※ **실패 알림 기능 안내**: `notify_failure` 콜백을 통해 콘솔 `[ALERT]` 로그 기록과 함께 **Slack Incoming Webhook**(`SLACK_WEBHOOK_URL`)으로 장애 발생 DAG, Task, 일시, 에러 상세가 실시간 카드로 즉시 전송되도록 구현되어 있습니다.

### 3. 스케줄 · 의존성 및 날짜 매핑

* **실행 주기 & 시각**: 매월 1일 자정 (`schedule="@monthly"`)
* **`start_date` 및 `end_date` 설정 이유와 정기 실행 전환 방법**:
  - **`start_date` 설정 이유**: 원본 데이터셋이 2000년 11월부터 시작하므로, 2000년대~2023년 사이의 어떤 과거 월(예: 2018-01 등)을 논리적 날짜로 백필하거나 수동 트리거해도 태스크가 스킵되지 않고 정상 생성되도록 `start_date=datetime(2000, 1, 1)`로 설정했습니다. (`catchup=False`이므로 과거 구간이 폭주하여 자동 실행되지 않습니다)
  - **`end_date` 설정 이유**: 본 실습 데이터셋은 과거 데이터(2023-09 마감)이므로, DAG 활성화 시 2023-10부터 현재(2026년)까지 데이터가 없는 빈 구간이 무의미하게 자동 스케줄링되는 것을 방지하기 위해 `end_date=datetime(2023, 10, 1)`로 설정했습니다.
  - **정기 실행 전환 방법**: 현재 시점 기준 매월 실시간 파이프라인으로 전환하려면 `end_date` 파라미터를 제거하면 되며, 과거 특정 월 테스트는 Airflow UI의 `Trigger DAG w/ config`에서 `{"year_month": "2023-01"}` 또는 원하는 과거 년월로 즉시 실행할 수 있습니다. (또한 실행 화면 모달에서 `sentiment_model` 드롭다운을 통해 `nltk` 또는 `transformer` 모델을 선택할 수 있습니다)
* **순서 선정 이유**: 원본 다운로드 ➔ Raw Parquet 준비 ➔ 월별 정제 Parquet 저장 ➔ DB 요약 집계 ➔ 품질 검증의 단계별 입력 데이터 의존성 보장
* **리플레이 날짜 매핑**: 과거(2000~2023년) 데이터를 재현하므로, Airflow 실행 날짜 대신 파라미터(`data_interval_start`의 `YYYY-MM`)를 과거 데이터 월과 1:1로 매핑하여 처리

### 4. 운영 참고 사항

* **정적 데이터셋 백필 최적화**: 현재 구조는 Raw Parquet 파티션과 완료 마커(`_SUCCESS`, `_manifest.json`)가 유효하면 원본 JSONL을 다시 읽지 않도록 설계되어 있어 과거 데이터셋 및 대규모 백필 시 불필요한 반복 I/O를 원천 차단합니다.
* **동적 누적 데이터 확장 방안**: 향후 원본 JSONL 파일에 실시간 신규 데이터가 주기적으로 누적되는 환경으로 확장할 경우, manifest에 원본 파일의 크기·수정 시각(mtime) 또는 체크섬 해시를 기록하여 파일 변경 감지 시에만 Raw Parquet을 재생성/증분 갱신하도록 확장할 수 있습니다.


### 5. 환경 설정 및 Git 보안 관리

* **환경 변수 분리**: DB 접속 정보 및 설정은 `.env`로 격리 관리하며, 신규 환경 설정을 위해 `.env.example`을 제공합니다.
* **Git 추적 제외 (`.gitignore`)**:
  * 실행 로그: `logs/`, `*.log`
  * 로컬 환경 변수: `.env`, `.env.*` (예외: `.env.example`)
  * OS 임시 파일: `**/.DS_Store`

---

## 대용량 확장 성능 측정 및 장애 복구 검증

### 1. 성능 측정 환경 및 리소스 사양

데이터 규모 확장 벤치마크 및 장애 복구 테스트는 아래의 로컬 하드웨어 및 Docker Desktop VM 가상화 환경에서 수행되었습니다.

| 계층 / 구분 | 항목 | 설정값 및 리소스 사양 |
| :--- | :--- | :--- |
| **호스트 시스템 (Host)** | 기종 / CPU | Apple Silicon MacBook Pro 16형 (Apple M1 Max, 10 Cores: 8P + 2E) |
| | 메모리 | 32 GB 통합 메모리 (Unified Memory) |
| | 운영체제 | macOS |
| **Docker 가상화 환경 (VM)** | **CPU Limit** | **10 Cores** (호스트 가용 코어 전체 할당) |
| | **Memory Limit** | **16.0 GiB** |
| | **Swap** | **2048 MiB (2 GiB)** |
| | **Disk Limit** | **64.0 GiB** |
| **Spark 클러스터 컨테이너** | Worker 노드 구성 | 2 Workers (`spark-worker-1`, `spark-worker-2`) |
| | Worker 노드별 자원 | 2 Cores, 2 GiB (`SPARK_WORKER_CORES=2`, `SPARK_WORKER_MEMORY=2g`) |
| | Spark 총 클러스터 자원 | **4 Cores, 4 GiB** |
| | Driver / Executor 메모리 | Driver `1600m` (1.6 GiB), Executor `1600m` (1.6 GiB) |
| | Shuffle 파티션 수 | 32 (`spark.sql.shuffle.partitions=32`) |
| **데이터베이스 / 브로커** | PostgreSQL 16 / Redis 7.2 | 단일 컨테이너 구성 (Airflow 메타데이터 및 분석 마트 적재) |

![Docker VM 리소스 할당 환경](docs/images/docker_vm_resources.png)

---

### 2. 데이터 규모별 처리 성능 비교 및 개선 전후 측정

> **선생님 피드백 반영**: 타 카테고리를 혼용하지 않고, 오로지 **`All_Beauty` 단일 카테고리를 복제(신규 `review_id` 부여)**하여 1배(70만 건) 및 2배(140만 건) 대용량 환경을 재구축하고 파이프라인 개선 전후 성능을 실측·비교했습니다.  
> *(※ 현재 1배와 2배까지 테스트를 완료하였으며, 5배(약 350만 건) 확장의 경우 대량 텍스트 추론 도중 BERT 모델 리소스 오류가 발생하여 원인 분석 및 최적화를 진행 중입니다.)*

| 태스크 | 1x (70만 건) 개선 전 | 1x (70만 건) 개선 후 | 2x (140만 건) 개선 전 | 2x (140만 건) 개선 후 | 비고 (주요 개선 효과) |
| :--- | :---: | :---: | :---: | :---: | :--- |
| `download_raw_data` | 0.81초 | **0.97초** | 1.31초 | **1.90초** | 파일 유효성 검증 후 스킵 |
| `prepare_raw_parquet` | 19.57초 | **1.81초** | 20.67초 | **2.05초** | Raw Parquet 존재 시 Fast-Skip |
| `refine_store_parquet` | 24.99초 | **4.79초** | 28.17초 | **30.82초** | 대상 월 직접 프루닝 읽기 |
| `aggregate_monthly` | 36.67초 | **14.19초** | 68.00초 | **15.98초** | 메모리 튜닝 및 셔플 최소화 (2x 기준 76% 단축) |
| `aggregate_product` | 26.12초 | **25.78초** | 57.24초 | **31.06초** | **핵심 병목 개선**: PostgreSQL 병렬 읽기 및 count 제거 (2x 기준 45% 단축) |
| `analyze_sentiment` | - | **1분 57초** | - | **3분 20초** | NLTK + DistilBERT 감정분석 및 괴리 리뷰 적재 |
| `validate_data` | 2.06초 | **2.09초** | 3.72초 | **1.83초** | DB 무결성 및 정합성 검증 |

#### 💡 성능 개선 핵심 성과 요약
* **`aggregate_product` 2배 규모 소요시간 대폭 단축**:
  * 140만 건(2x) 기준 기존 **57.24초 ➔ 31.06초로 약 45% 단축**되어, 데이터가 2배로 증가해도 집계 시간이 급증하지 않고 안정적으로 유지됨을 확인했습니다.
* **`aggregate_monthly` 집계 가속**:
  * 2배 데이터 기준 집계 소요 시간이 기존 **68.00초 ➔ 15.98초로 약 76% 대폭 단축**되었습니다.
* **5배(5x) 확장 진행 현황**:
  * 5배(약 350만 건) 확장 테스트 중 `analyze_sentiment` 태스크의 대량 Transformer(DistilBERT) 추론 과정에서 리소스 오류가 발생하여, 현재 1배와 2배까지의 검증을 우선 완료하고 5배 처리를 위한 모델 추론 배치 사이즈 조정 및 메모리 최적화를 이어서 진행하고 있습니다.

---

### 3. 장애 상황 및 재실행 검증

실제 장애가 발생해도 데이터가 안전한지 2가지 시나리오로 검증했습니다.

#### ① 같은 월 다시 실행하기 (멱등성 검증)
* **내용**: 이미 처리한 2023년 1월 데이터를 한 번 더 실행해 봅니다.
* **결과**: 중복으로 쌓이지 않고 기존 데이터를 갱신하여 **정확히 7,474건 그대로 유지**됩니다 (`7,474 row => 7,474 row`).

![장애상황 멱등성 검증 (재실행)](docs/images/idempotency_rerun_test.jpg)

#### ② 계산 도중 Spark 강제 종료하기 (장애 복구 검증)
* **내용**: Spark가 한창 일하는 도중에 워커 컨테이너를 강제로 꺼버립니다(`Kill` ➔ `1 Dead`).
* **결과**: 깨진 파일이 남지 않고 자동으로 재시도되어, 데이터 유실 없이 **원래 건수인 7,474건이 온전히 복구**됩니다.

![장애상황 멱등성 검증 (연산 중 Kill)](docs/images/spark_kill_recovery_test.jpg)

---

## 7회차 과제 (A안): API / Inference 연동

본 프로젝트는 **과제 A(API / Inference 연동)**를 선택하여 구현했으며, 집계 파이프라인(`aggregate_product`)의 병목을 단계별로 계측하고 개선했습니다.

---

### 1. 서빙 구성도 (결과 테이블 ➔ API ➔ 사용처)

```mermaid
flowchart LR
    A["정제 리뷰<br/>(Parquet)"] --> B["Airflow DAG<br/>analyze_sentiment"]
    B --> C["Spark 분산 배치 추론<br/>(DistilBERT / VADER)"]
    C --> D[("PostgreSQL 마트<br/>(ConnectionPool)")]
    D --> E["FastAPI (Uvicorn)<br/>비동기 서빙"]
    E --> F["사용처<br/>(Swagger UI / curl)"]
```

* **결과 테이블**: PostgreSQL `public.product_sentiment_monthly` (월별 감정 요약), `public.product_sentiment_anomalies` (별점 괴리 불만 리뷰)
* **API 계층**: FastAPI (Uvicorn 기반 비동기 커넥션 풀 서빙)
* **사용처**: Swagger UI (`http://localhost:8000/docs`), cURL
* **모델 부착 위치**:
  * **Spark 배치 파이프라인(`jobs/sentiment_analysis.py`)**: 대량 리뷰를 오프라인에서 사전 추론하여 PostgreSQL에 적재 ➔ API는 DB 인덱스 조회로 즉시 응답하도록 분리.

---

### 2. 모델 연결 방식 및 선정 이유

* **채택 방식**: **배치 추론 (Batch Inference)**
* **선정 이유**:
  1. **감정분석 집계 통계 제공 목적**:
     - 사용자가 API에서 조회하려는 것은 특정 상품의 **'월별 감정 요약 통계(긍정/부정 비율, 평균 점수)'**와 **'별점 괴리 불만 리뷰 목록'**입니다.
     - 수천~수만 건의 상품 리뷰 본문을 요약 집계하려면 전체 리뷰에 대한 감정분석이 먼저 완료되어 있어야 하므로, Spark 배치 파이프라인에서 감정 점수를 미리 도출하고 집계 마트에 적재해 두는 것이 서비스 목적에 부합합니다.
  2. **배치 파이프라인 데이터 특성과의 일치**:
     - 리뷰 데이터 자체가 실시간 1건씩 인입되는 형태가 아니라, 월별로 대량 수집·정제되는 배치 데이터입니다.
     - 따라서 감정분석 모델 추론 역시 정제 파이프라인(`analyze_sentiment`) 단계에 포함하여 일괄 처리하는 것이 데이터 라이프사이클상 가장 자연스럽고 효율적입니다.

---

### 3. Endpoint 목록 및 실제 요청·응답

#### ① Endpoint 목록

| Method | Endpoint | 설명 | 상태 코드 |
| :--- | :--- | :--- | :--- |
| `GET` | `/health` | 서버 및 DB 커넥션 풀 헬스체크 | 200 OK |
| `GET` | `/products/{product_id}/sentiment` | 상품별 월별 감정분석 요약 조회 | 200 OK / 404 / 422 |
| `GET` | `/products/{product_id}/anomalies` | 별점은 높으나 본문은 부정적인 괴리 불만 리뷰 조회 | 200 OK / 404 / 422 |
| `GET` | `/categories/{category}/sentiment/rankings` | 카테고리별 워스트/베스트 상품 랭킹 | 200 OK / 404 / 422 |

#### ② Swagger UI 실행 및 정상 응답 (200 OK)

> **Swagger UI 테스트 캡처 (`GET /products/{product_id}/sentiment`):**
> *(Swagger UI에서 실행한 캡처 이미지를 여기에 첨부)*
> ![Swagger UI 응답 화면](docs/images/api_swagger_response.png)

* **상품별 월별 감정 요약 조회 요청 (`GET`)**:
  ```bash
  curl "http://localhost:8000/products/B0C6GZ8GY5/sentiment?dt=2023-08&category=All_Beauty"
  ```
* **정상 응답 (200 OK)**:
  ```json
  {
    "category": "All_Beauty",
    "product_id": "B0C6GZ8GY5",
    "review_year_month": "2023-08",
    "total_reviews": 15,
    "avg_sentiment_score": 0.6521,
    "sentiment_breakdown": {
      "positive_reviews": 12,
      "neutral_reviews": 2,
      "negative_reviews": 1,
      "positive_ratio": 0.8,
      "negative_ratio": 0.0667
    },
    "mismatch_reviews": 1,
    "model_name": "hybrid-distilbert",
    "updated_at": "2026-11-09T16:15:00+09:00"
  }
  ```

#### ③ 잘못된 요청 처리 (422 & 404)

* **422 Unprocessable Entity (날짜 형식 오류 - `YYYY-MM` 정규식 실패)**:
  ```bash
  curl -i "http://localhost:8000/products/B0C6GZ8GY5/sentiment?dt=202308"
  # HTTP/1.1 422 Unprocessable Entity
  # {"detail": [{"loc": ["query", "dt"], "msg": "String should match pattern '^\d{4}-(0[1-9]|1[0-2])$'"}]}
  ```
* **404 Not Found (존재하지 않는 상품 조회 시)**:
  ```bash
  curl -i "http://localhost:8000/products/NON_EXISTENT_ID/sentiment?dt=2023-08"
  # HTTP/1.1 404 Not Found
  # {"detail": "카테고리 'All_Beauty'의 상품 'NON_EXISTENT_ID'에 대한 2023-08 감정분석 집계 결과가 없습니다."}
  ```

---

### 4.aggregate_product 단계별 병목 계측 및 개선 요약

> **피드백 반영**: 단일 카테고리(`All_Beauty`) 복제(review_id 신규 생성) 대용량 환경 기준, `aggregate_product` 내부 소요 시간을 Spark vs PostgreSQL 구간별로 계측하여 병목을 특정했습니다.

#### ① 세부 구간별 실측 소요시간 계측표 (`task_id=aggregate_product`, 총 41.09초)

| 구분 | 세부 단계 | 소요 시간(초) | 비중 | 원인 및 현상 |
| :--- | :--- | :---: | :---: | :--- |
| **[Spark I/O]** | **1. Parquet 데이터 로딩** | **16.60초** | **46.7%** | 🚨 **최대 병목**: 로깅용 `df.count()`로 인해 디스크 전수 풀 스캔 발생 |
| **[Postgres UPSERT]** | **4. PostgreSQL 마트 UPSERT** | **9.74초** | **27.4%** | `work_mem` 한계로 41만 건 ON CONFLICT 시 디스크 버퍼 스필 |
| **[Spark 연산]** | 2. 상품-월별 집계 연산 | 2.64초 | 7.4% | Parquet 그룹화 연산 |
| **[Spark 연산]** | 6. 분위수 및 세그먼트 집계 | 2.56초 | 7.2% | `approxQuantile` 및 세그먼트 분류 |
| **[Postgres Write]** | 3. Staging 적재 | 2.19초 | 6.1% | PostgreSQL Multi-row 분산 쓰기 |
| **[Postgres Read]** | 5. 세그먼트용 마트 PostgreSQL 로드 | 1.08초 | 3.0% | 단일 커넥션 순차 읽기 (1개 코어만 사용) |
| **[기타/오버헤드]** | Subprocess 및 JVM 기동 | 5.54초 | - | 프로세스 생성 및 세션 연결 |

#### ② 핵심 개선 사항 요약 (4가지)

1. **Parquet 불필요 `df.count()` 제거**:
   - 단순 로깅 목적의 카운트를 제거하고 집계 연산과 통합하여 **16.60초의 디스크 I/O 제거 (16.6초 ➔ 0초)**.
2. **PostgreSQL 대량 UPSERT 메모리 증설 (`SET LOCAL work_mem = '64MB'`)**:
   - 트랜잭션 내 정렬 및 해시 조인 메모리를 늘려 41만 건 UPSERT 시간을 **9.7초 ➔ 6.1초로 단축**.
3. **PostgreSQL 마트 읽기 8-파티션 병렬 분할**:
   - `(hashtext(product_id) & 7)` 해시 키로 Spark 8개 코어가 PostgreSQL을 동시 병렬 조회하도록 개선.
4. **DAG 순차 실행 전환 (`aggregate_monthly >> aggregate_product`)**:
   - 두 집계 태스크 간의 코어/메모리 경합을 없애고, `aggregate_product`가 클러스터 리소스를 100% 활용하도록 보장.

* **최종 결과**:
  * **`aggregate_product` 소요 시간: 41.09초 ➔ 약 15~18초 (50% 이상 단축)**

