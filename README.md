# Amazon 리뷰 수집·정제 파이프라인

## 사용 데이터 및 분석 목적

- **데이터셋:** Amazon Reviews 2023의 `All_Beauty` 카테고리
- **출처:** McAuley-Lab / Hugging Face Datasets (`McAuley-Lab/Amazon-Reviews-2023`)
- **현재 입력:** 내려받은 정적 배치 파일 `data/All_Beauty.jsonl`
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

현재 DB 테이블명은 원본 `raw_reviews`, 정제 결과 `amazon_reviews`입니다.
리뷰의 `rating`은 개별 리뷰 평점이며, `average_rating`은 현재 리뷰 파일에 포함되지 않습니다.
상품 메타데이터 수집을 추가하면 `parent_asin`으로 연결해 `average_rating`, `store`,
상품명 등의 정보를 함께 분석할 계획입니다. 상품 메타데이터 수집·저장은 아직 구현하지 않았습니다.

## 현재 구조와 도구 선정 이유

데이터 레이크의 표준인 **메달리온 아키텍처(Medallion Architecture: Bronze → Silver → Gold)** 구조를 따릅니다.
**수집기(JSONL) → Raw 파일 적재(`data/raw/`) → Spark 분산 정제/중복제거 → Processed Parquet(`data/processed/`) + PostgreSQL 일별 집계(`daily_review_summary`)**로 동작합니다.

| 계층 | 역할과 기술 스택 | 저장 포맷 및 위치 | 기본키(PK) / 식별자 |
| :--- | :--- | :--- | :--- |
| **1단계: Raw (Bronze)** | 수집기(`collector.py`)가 원본 JSONL을 Landing Zone에 보관 | JSONL (`data/raw/`) | 없음 (원시 로그 파일) |
| **2단계: Processed (Silver)** | Spark가 읽어 결측치 정제 및 `review_id` 기준 분산 중복 제거 | Parquet (`data/processed/reviews/`)<br/>`partitionBy("review_date")` | `review_id` (UUID5: user_id + parent_asin + timestamp) |
| **3단계: Aggregation (Gold)** | Spark가 일자별 리뷰 수, 평균 평점, 구매인증 수를 요약 집계하여 DB 적재 | PostgreSQL (`public.daily_review_summary`) | `review_date` (DATE PRIMARY KEY) |

### 저장 포맷을 고른 이유

1. **정제 원본 ➔ Parquet (컬럼 지향 포맷)**:
   - 70만 건 이상의 대용량 상세 데이터를 행 단위 RDBMS에 전부 저장하면 DB CPU/I/O 병목 및 스토리지 낭비가 발생합니다.
   - Parquet은 컬럼별 Snappy 압축률이 우수하며, `partitionBy("review_date")`로 날짜별 파티션 프루닝(Partition Pruning)과 필요한 컬럼만 읽는 컬럼 프루닝(Column Pruning)을 통해 대규모 분석 쿼리 속도를 극대화합니다.
2. **집계 통계 ➔ PostgreSQL (관계형 데이터베이스)**:
   - 일자별로 요약 집계된 데이터(약 6,500행)는 데이터 크기가 작고, Grafana 대시보드나 백엔드 서비스에서 빈번하고 빠른 포인트 조회가 일어납니다.
   - 인덱스와 트랜잭션(ACID)을 지원하는 RDBMS가 서빙 및 시각화 도구 연동에 가장 적합합니다.

### 실행 명령

1. **수집기 실행 (원본 파일을 data/raw/ 에 적재)**:
   ```bash
   ./submit-job.sh collector.py
   ```
2. **Spark 정제 및 적재 실행**:
   ```bash
   ./submit-job.sh refine_reviews.py
   ```

### 실행 및 멱등성(동일 파일 중복 투입) 검증 결과

`data/raw/` 폴더에 원본 파일 1개(`All_Beauty.jsonl`, 701,528건)를 넣고 실행했을 때와, **동일한 파일을 2개(`All_Beauty.jsonl` + `All_Beauty_duplicate.jsonl`, 1,403,056건) 넣고 실행했을 때의 결과 비교**입니다:

| 검증 단계 | 1차 실행 (정상 파일 1개) | 2차 실행 (동일 파일 2개 투입) | 검증 결과 |
| :--- | :---: | :---: | :---: |
| Spark 원본 로딩 건수 (`data/raw/`) | 701,528건 | 1,403,056건 | 2배 입력 로드 확인 |
| `review_id` 기준 중복 제거 및 제외 | 7,981건 | 709,509건 | 복사본 701,528건 자동 제거 |
| **정제 완료 Parquet 건수 (`data/processed/`)** | **693,547건** | **693,547건** | **완벽 일치 (건수 불변)** |
| **PostgreSQL 일별 집계 (`daily_review_summary`)** | **6,557일** (총 693,547건) | **6,557일** (총 693,547건) | **완벽 일치 (건수 불변)** |

Spark의 `dropDuplicates(["review_id"])`와 파티션 Overwrite 모드를 통해 동일 파일이 중복 수집되거나 재실행되어도 **결과 건수가 100% 동일하게 유지되는 멱등성(Idempotency)**을 검증 완료했습니다.
