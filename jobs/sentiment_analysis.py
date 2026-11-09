"""Spark 기반 Amazon 리뷰 감정분석 및 이상 리뷰(괴리 리뷰) 분석 마트 파이프라인.

기능 요약:
1. 정제된 Parquet 리뷰 제목과 본문(review_title + review_text)에 대해 감정분석(Sentiment Analysis) 수행
   - 기본 (1순위): NLTK VADER (경량, CPU 고속 처리)
   - 2순위 (GPU 가속 / 하이브리드 정밀검증): Hugging Face Transformers (distilbert-base-uncased-finetuned-sst-2-english 경량 모델)
   - 자동 환경 감지(CUDA / MPS / CPU) 및 Graceful Fallback 지원
2. 상품별-월별 감정 분석 마트 집계 (평균 감정 점수, 긍정/중립/부정/괴리 리뷰 건수)
3. 별점-감정 괴리 이상 리뷰(rating >= 4.0 이나 본문은 부정적) 추출 및 마트 적재
4. PostgreSQL 원자적 UPSERT 적재 (멱등성 보장)
"""
import argparse
import os
import sys
from urllib.parse import quote
import uuid

try:
    import psycopg2
except ImportError:
    psycopg2 = None

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)
from pyspark.storagelevel import StorageLevel

# 기본 내장 감정 단어 사전 (오프라인 환경 / NLTK vader_lexicon 다운로드 불가 시 절대 크래시 방지용)
# 뷰티/제품 리뷰 도메인 특성 반영: "fake nail", "smells good", "not cheap", "no allergic reaction" 등의
# 오탐을 방지하기 위해 단어 매칭 왜곡 가능성이 높은 어휘(smells, fake, cheap, allergic)를 배제합니다.
_FALLBACK_POS_WORDS = {
    "good", "great", "excellent", "amazing", "love", "best", "fantastic", "perfect",
    "wonderful", "happy", "awesome", "nice", "beautiful", "recommend", "effective",
    "favorite", "glad", "satisfied", "pleased", "impressive", "superb", "convenient", "handy"
}
_FALLBACK_NEG_WORDS = {
    "bad", "terrible", "horrible", "awful", "worst", "hate", "poor", "broken",
    "disappointed", "disappointing", "useless", "waste", "defective",
    "hurts", "burns", "irritation", "garbage"
}


def send_slack_alert(job_name, error_message, category=None, year_month=None, step=None):
    """Spark 작업/워커 실패 시 Slack Webhook으로 장애 알림을 전송합니다."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    try:
        import json
        import urllib.request

        err_str = str(error_message)
        if len(err_str) > 800:
            err_str = err_str[:800] + "\n... (생략)"

        fields = [
            {"title": "Spark Job", "value": f"`{job_name}`", "short": True},
            {"title": "실행 단계 (Step)", "value": f"`{step or 'sentiment_analysis'}`", "short": True},
        ]
        if category:
            fields.append({"title": "카테고리", "value": f"`{category}`", "short": True})
        if year_month:
            fields.append({"title": "대상 년월", "value": f"`{year_month}`", "short": True})

        fields.append({"title": "오류/장애 상세 내용", "value": f"```{err_str}```", "short": False})

        payload = {
            "text": f"🚨 [Spark 장애 알림] {job_name} 작업 실패 감지",
            "attachments": [
                {
                    "color": "#E01E5A",
                    "title": f"🚨 Spark 작업/워커 장애 감지: {job_name}",
                    "fields": fields,
                    "mrkdwn_in": ["text", "fields"],
                }
            ],
        }

        req = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                print(f"[Slack 알림 전송 완료] {job_name} 실패 알림 전송됨.", flush=True)
            else:
                print(f"[Slack 알림 응답 경고] HTTP {response.status}", flush=True)
    except Exception as notify_err:
        print(f"[Slack 알림 전송 실패] {notify_err}", flush=True)


def get_db_config():
    """환경 변수로부터 PostgreSQL 접속 설정을 로드합니다."""
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "postgres"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }


def ensure_database_objects(db_config, sentiment_table, anomaly_table):
    """감정 분석 결과 마트 테이블 및 인덱스가 존재하지 않을 경우 자동 생성합니다."""
    ddl_sql = f"""
    CREATE TABLE IF NOT EXISTS {sentiment_table} (
        category VARCHAR(100) NOT NULL,
        product_id VARCHAR(50) NOT NULL,
        review_year_month VARCHAR(7) NOT NULL,
        total_reviews INTEGER NOT NULL,
        avg_sentiment_score DOUBLE PRECISION NOT NULL,
        positive_reviews INTEGER NOT NULL,
        neutral_reviews INTEGER NOT NULL,
        negative_reviews INTEGER NOT NULL,
        mismatch_reviews INTEGER NOT NULL,
        model_name VARCHAR(50) NOT NULL DEFAULT 'vader',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (category, product_id, review_year_month)
    );

    CREATE INDEX IF NOT EXISTS idx_prod_sent_cat_ym
        ON {sentiment_table}(category, review_year_month);
    CREATE INDEX IF NOT EXISTS idx_prod_sent_cat_prod
        ON {sentiment_table}(category, product_id);

    CREATE TABLE IF NOT EXISTS {anomaly_table} (
        category VARCHAR(100) NOT NULL,
        product_id VARCHAR(50) NOT NULL,
        review_id VARCHAR(100) NOT NULL,
        review_year_month VARCHAR(7) NOT NULL,
        rating DOUBLE PRECISION NOT NULL,
        sentiment_score DOUBLE PRECISION NOT NULL,
        sentiment_label VARCHAR(20) NOT NULL,
        review_title TEXT,
        review_text TEXT,
        anomaly_type VARCHAR(50) NOT NULL DEFAULT 'HIGH_RATING_NEGATIVE_TEXT',
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (category, product_id, review_id)
    );

    CREATE INDEX IF NOT EXISTS idx_sent_anom_cat_prod_ym
        ON {anomaly_table}(category, product_id, review_year_month);
    """
    if psycopg2 is None:
        print("[DB 초기화 경고] psycopg2가 설치되어 있지 않아 DDL 사전 생성을 건너뜁니다.", flush=True)
        return

    try:
        conn = psycopg2.connect(**db_config)
        with conn.cursor() as cur:
            cur.execute(ddl_sql)
        conn.commit()
        conn.close()
        print(f"[DB 초기화] 감정 분석 테이블({sentiment_table}, {anomaly_table}) 스키마 확인 완료.", flush=True)
    except Exception as e:
        print(f"[DB 초기화 경고] DDL 실행 중 오류 (이미 존재할 시 무시 가능): {e}", flush=True)


def upsert_staging_to_metrics(db_config, staging_table, target_table):
    """PostgreSQL staging 테이블의 감정 집계를 target 테이블에 원자적 UPSERT 병합합니다."""
    upsert_sql = f"""
    SET LOCAL synchronous_commit = 'off';
    INSERT INTO {target_table} (
        category, product_id, review_year_month, total_reviews, avg_sentiment_score,
        positive_reviews, neutral_reviews, negative_reviews, mismatch_reviews, model_name, updated_at
    )
    SELECT
        category, product_id, review_year_month, total_reviews, avg_sentiment_score,
        positive_reviews, neutral_reviews, negative_reviews, mismatch_reviews, model_name, updated_at
    FROM {staging_table}
    ON CONFLICT (category, product_id, review_year_month) DO UPDATE SET
        total_reviews = EXCLUDED.total_reviews,
        avg_sentiment_score = EXCLUDED.avg_sentiment_score,
        positive_reviews = EXCLUDED.positive_reviews,
        neutral_reviews = EXCLUDED.neutral_reviews,
        negative_reviews = EXCLUDED.negative_reviews,
        mismatch_reviews = EXCLUDED.mismatch_reviews,
        model_name = EXCLUDED.model_name,
        updated_at = EXCLUDED.updated_at;
    """
    if psycopg2 is None:
        raise RuntimeError("PostgreSQL UPSERT 병합을 위해 psycopg2 패키지가 필요합니다.")

    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            cur.execute(upsert_sql)
            cur.execute(f"DROP TABLE IF EXISTS {staging_table};")
        conn.commit()
        print(f"[DB 적재 완료] {staging_table} -> {target_table} UPSERT 및 임시 테이블 정리 완료", flush=True)
    finally:
        conn.close()


def upsert_staging_to_anomalies(db_config, staging_table, target_table):
    """PostgreSQL staging 테이블의 괴리 이상 리뷰를 target 테이블에 원자적 UPSERT 병합합니다."""
    upsert_sql = f"""
    SET LOCAL synchronous_commit = 'off';
    INSERT INTO {target_table} (
        category, product_id, review_id, review_year_month, rating, sentiment_score,
        sentiment_label, review_title, review_text, anomaly_type, created_at
    )
    SELECT
        category, product_id, review_id, review_year_month, rating, sentiment_score,
        sentiment_label, review_title, review_text, anomaly_type, created_at
    FROM {staging_table}
    ON CONFLICT (category, product_id, review_id) DO UPDATE SET
        rating = EXCLUDED.rating,
        sentiment_score = EXCLUDED.sentiment_score,
        sentiment_label = EXCLUDED.sentiment_label,
        review_title = EXCLUDED.review_title,
        review_text = EXCLUDED.review_text,
        anomaly_type = EXCLUDED.anomaly_type,
        created_at = EXCLUDED.created_at;
    """
    if psycopg2 is None:
        raise RuntimeError("PostgreSQL UPSERT 병합을 위해 psycopg2 패키지가 필요합니다.")

    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            cur.execute(upsert_sql)
            cur.execute(f"DROP TABLE IF EXISTS {staging_table};")
        conn.commit()
        print(f"[DB 적재 완료] {staging_table} -> {target_table} 괴리 리뷰 적재 및 임시 테이블 정리 완료", flush=True)
    finally:
        conn.close()


# ==============================================================================
# 감정 분석 모델 인스턴스 (Worker 프로세스 레벨 캐싱)
# ==============================================================================
_GLOBAL_VADER_ANALYZER = None
_GLOBAL_TRANSFORMER_ANALYZER = None


def _get_vader_analyzer():
    """워커 프로세스당 단 1회 VADER(또는 내장사전 fallback) 분석기를 로드하여 재사용합니다."""
    global _GLOBAL_VADER_ANALYZER
    if _GLOBAL_VADER_ANALYZER is not None:
        return _GLOBAL_VADER_ANALYZER

    # 1. NLTK VADER 로드 시도
    try:
        import nltk
        from nltk.sentiment.vader import SentimentIntensityAnalyzer

        nltk_custom_data = os.getenv("NLTK_DATA", "/opt/nltk_data")
        if os.path.exists(nltk_custom_data) and nltk_custom_data not in nltk.data.path:
            nltk.data.path.insert(0, nltk_custom_data)

        try:
            nltk.data.find("sentiment/vader_lexicon.zip")
        except LookupError:
            try:
                nltk.download("vader_lexicon", quiet=True)
            except Exception:
                pass

        sia = SentimentIntensityAnalyzer()

        def nltk_score_fn(text: str):
            cleaned = (text or "").strip()
            if not cleaned:
                return 0.0, "NEUTRAL"
            score = sia.polarity_scores(cleaned)["compound"]
            lbl = "POSITIVE" if score >= 0.1 else ("NEGATIVE" if score <= -0.1 else "NEUTRAL")
            return round(float(score), 4), lbl

        _GLOBAL_VADER_ANALYZER = (nltk_score_fn, "vader")
        return _GLOBAL_VADER_ANALYZER
    except Exception:
        # 2. 오프라인 내장 사전 Fallback
        import re

        def fallback_score_fn(text: str):
            cleaned = (text or "").lower()
            if not cleaned:
                return 0.0, "NEUTRAL"
            tokens = set(re.findall(r"\b[a-z]+\b", cleaned))
            pos_cnt = len(tokens & _FALLBACK_POS_WORDS)
            neg_cnt = len(tokens & _FALLBACK_NEG_WORDS)
            total = pos_cnt + neg_cnt
            if total == 0:
                return 0.0, "NEUTRAL"
            score = (pos_cnt - neg_cnt) / total
            lbl = "POSITIVE" if score >= 0.1 else ("NEGATIVE" if score <= -0.1 else "NEUTRAL")
            return round(float(score), 4), lbl

        _GLOBAL_VADER_ANALYZER = (fallback_score_fn, "lexicon_fallback")
        return _GLOBAL_VADER_ANALYZER


def _get_transformer_analyzer():
    """워커 프로세스당 단 1회 HuggingFace Transformers 감정분석 파이프라인을 로드하여 재사용합니다."""
    global _GLOBAL_TRANSFORMER_ANALYZER
    if _GLOBAL_TRANSFORMER_ANALYZER is not None:
        return _GLOBAL_TRANSFORMER_ANALYZER

    try:
        import torch
        from transformers import pipeline

        device = -1
        if torch.cuda.is_available():
            device = 0
        elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            device = "mps"

        model_name = os.getenv("TRANSFORMER_MODEL_NAME", "distilbert-base-uncased-finetuned-sst-2-english")
        pipe = pipeline(
            "sentiment-analysis",
            model=model_name,
            device=device,
            truncation=True,
            max_length=512,
        )

        def transformer_score_fn(text: str):
            cleaned = (text or "").strip()
            if not cleaned:
                return 0.0, "NEUTRAL"
            try:
                # nlptown 모델 1~5 stars 다분류 모델은 top_k=None으로 전체 확률을 얻어 기대 별점(Expected Star) 산출
                res = pipe(cleaned[:512], top_k=None)
                if isinstance(res, list) and len(res) > 0 and isinstance(res[0], list):
                    res = res[0]
                has_stars = any("label" in item and item["label"][0].isdigit() for item in res)
                if has_stars:
                    expected_star = sum(
                        int(item["label"][0]) * float(item["score"])
                        for item in res
                        if "label" in item and item["label"][0].isdigit()
                    )
                    # 1~5점 별점을 -1.0 ~ +1.0 감정 점수로 선형 정규화
                    signed_score = (expected_star - 3.0) / 2.0
                else:
                    top_item = max(res, key=lambda x: float(x.get("score", 0.0)))
                    lbl = str(top_item.get("label", "")).upper()
                    sc = float(top_item.get("score", 1.0))
                    signed_score = sc if "POS" in lbl else -sc
            except Exception:
                res = pipe(cleaned[:512])[0]
                lbl = str(res.get("label", ""))
                if lbl and lbl[0].isdigit():
                    star = int(lbl[0])
                    signed_score = (star - 3.0) / 2.0
                elif "POS" in lbl.upper():
                    signed_score = float(res.get("score", 1.0))
                elif "NEG" in lbl.upper():
                    signed_score = -float(res.get("score", 1.0))
                else:
                    signed_score = 0.0

            assigned_lbl = "POSITIVE" if signed_score >= 0.1 else ("NEGATIVE" if signed_score <= -0.1 else "NEUTRAL")
            return round(float(signed_score), 4), assigned_lbl

        _GLOBAL_TRANSFORMER_ANALYZER = (transformer_score_fn, "transformer")
        print(f"[Worker Transformer 활성화 완료] {model_name} 탑재 (Device: {device})", flush=True)
        return _GLOBAL_TRANSFORMER_ANALYZER
    except Exception as e:
        print(f"[Worker Transformer 로드 실패] 사유: {e} -> VADER로 Fallback", flush=True)
        _GLOBAL_TRANSFORMER_ANALYZER = _get_vader_analyzer()
        return _GLOBAL_TRANSFORMER_ANALYZER


def _get_worker_analyzer(model_type="hybrid"):
    """기존 호환용 분석기 획득 함수."""
    if model_type == "transformer":
        return _get_transformer_analyzer()
    return _get_vader_analyzer()


def run_sentiment_analysis(spark, args, db_config):
    """정제된 Parquet 리뷰 데이터를 기반으로 감정분석 수행 및 PostgreSQL 적재."""
    category = args.category or "All_Beauty"
    target_ym = args.year_month
    is_incremental = bool(target_ym and target_ym.lower() not in ("all", "auto", ""))

    print("=" * 70, flush=True)
    print(f"[감정분석 파이프라인] ({category}) {args.processed_dir} 로딩 시작...", flush=True)
    print(f"  - 모델 설정: {args.model_type} (기본: NLTK VADER / GPU 지원 시 트랜스포머)", flush=True)

    if is_incremental:
        partition_path = os.path.join(args.processed_dir, f"review_year_month={target_ym}")
        if os.path.exists(partition_path):
            print(f"[감정분석] 대상 월({target_ym}) 파티션 직접 로딩: {partition_path}", flush=True)
            df = spark.read.parquet(partition_path).withColumn("review_year_month", F.lit(target_ym))
        else:
            print(f"[감정분석] 대상 월({target_ym}) 상위 디렉터리 필터 로딩 시도", flush=True)
            df = spark.read.parquet(args.processed_dir).filter(F.col("review_year_month") == target_ym)
    else:
        print("[감정분석] 전체 파티션 Parquet 로딩", flush=True)
        df = spark.read.parquet(args.processed_dir)

    record_count = df.count()
    print(f"[감정분석] 읽어들인 리뷰 레코드 수: {record_count:,}건", flush=True)
    if record_count == 0:
        print("[감정분석 알림] 처리할 리뷰 데이터가 없어 작업을 정상 종료합니다.", flush=True)
        return

    # 필수 컬럼 정규화 (review_text 및 review_title)
    if "review_text" in df.columns:
        text_col = F.coalesce(F.col("review_text"), F.lit(""))
    elif "text" in df.columns:
        text_col = F.coalesce(F.col("text"), F.lit(""))
    else:
        text_col = F.lit("")

    if "review_title" in df.columns:
        title_col = F.coalesce(F.col("review_title"), F.lit(""))
    elif "title" in df.columns:
        title_col = F.coalesce(F.col("title"), F.lit(""))
    else:
        title_col = F.lit("")

    # HTML 태그 제거 및 공백 정규화
    cleaned_title = F.trim(F.regexp_replace(F.regexp_replace(title_col, r"<[^>]+>", " "), r"\s+", " "))
    cleaned_text = F.trim(F.regexp_replace(F.regexp_replace(text_col, r"<[^>]+>", " "), r"\s+", " "))

    # 제목 + 본문 결합 컬럼 생성 (제목의 명확한 긍/부정 어조를 본문 분석에 유기적으로 반영)
    combined_text = (
        F.when((cleaned_title != "") & (cleaned_text != ""),
               F.concat(cleaned_title, F.lit(". "), cleaned_text))
        .when(cleaned_title != "", cleaned_title)
        .otherwise(cleaned_text)
    )

    df = (
        df.withColumn("cleaned_review_text", cleaned_text)
          .withColumn("text_for_sentiment", combined_text)
          .withColumn("title_for_sentiment", cleaned_title)
    )

    # [최적화 1] 싱글 파일 로딩으로 인한 단일 코어 병목 방지:
    # 전체 워커 코어가 100% 동시에 가동되도록 파티션을 분할합니다.
    current_partitions = df.rdd.getNumPartitions()
    target_partitions = max(args.partitions, 8)
    if current_partitions < target_partitions:
        print(
            f"[감정분석 최적화] 파티션 재분할: {current_partitions} -> {target_partitions}개 파티션 (전체 코어 병렬화)",
            flush=True,
        )
        df = df.repartition(target_partitions)

    # UDF 스키마 정의 (점수, 감정 라벨, 워커에서 실제 사용된 모델명)
    output_schema = StructType([
        StructField("score", DoubleType(), False),
        StructField("label", StringType(), False),
        StructField("model", StringType(), False),
    ])

    @F.udf(returnType=output_schema)
    def vader_udf(val):
        fn, model_name = _get_vader_analyzer()
        s, l = fn(str(val or ""))
        return {"score": float(s), "label": str(l), "model": str(model_name)}

    @F.udf(returnType=output_schema)
    def transformer_udf(val):
        fn, model_name = _get_transformer_analyzer()
        s, l = fn(str(val or ""))
        return {"score": float(s), "label": str(l), "model": str(model_name)}

    requested_mode = (args.model_type or "hybrid").lower()
    suspicious_count = 0

    if requested_mode == "transformer":
        # 1. 순수 트랜스포머 모드 (전체 대상 BERT 추론)
        print("[감정분석] 전체 리뷰 대상 Transformer(BERT) 추론 실행...", flush=True)
        scored_df = df.withColumn("sent_struct", transformer_udf(F.col("text_for_sentiment")))
        scored_df = (
            scored_df
            .withColumn("sentiment_score", F.col("sent_struct.score"))
            .withColumn("sentiment_label", F.col("sent_struct.label"))
            .withColumn("model_name", F.col("sent_struct.model"))
            .drop("sent_struct")
        )
    elif requested_mode == "nltk":
        # 2. 순수 NLTK VADER 모드 (초고속 CPU)
        print("[감정분석] 전체 리뷰 대상 NLTK VADER 추론 실행...", flush=True)
        scored_df = df.withColumn("sent_struct", vader_udf(F.col("text_for_sentiment")))
        scored_df = (
            scored_df
            .withColumn("sentiment_score", F.col("sent_struct.score"))
            .withColumn("sentiment_label", F.col("sent_struct.label"))
            .withColumn("model_name", F.col("sent_struct.model"))
            .drop("sent_struct")
        )
    else:
        # 3. 하이브리드 2단계 모드 (기본/권장):
        # 1단계 NLTK 초고속 스캔 -> 평점 4.0 이상 부정 의심군만 2단계 Transformer 문맥 정밀 검증
        print("=" * 70, flush=True)
        print("[하이브리드 감정분석 1단계] 전체 리뷰 대상 경량(VADER) 1차 고속 스캔 시작...", flush=True)
        stage1_df = df.withColumn("vader_struct", vader_udf(F.col("text_for_sentiment")))
        stage1_df = (
            stage1_df
            .withColumn("sentiment_score", F.col("vader_struct.score"))
            .withColumn("sentiment_label", F.col("vader_struct.label"))
            .withColumn("model_name", F.lit("vader"))
            .drop("vader_struct")
        )

        # 의심군 조건: 별점은 높은데(rating >= 4.0) 1차 단어 매칭 오판으로 NEGATIVE로 분류된 데이터
        suspicious_filter = (F.col("rating") >= 4.0) & (F.col("sentiment_label") == "NEGATIVE")
        normal_df = stage1_df.filter(~suspicious_filter)
        suspicious_df = stage1_df.filter(suspicious_filter)

        suspicious_count = suspicious_df.count()
        print(
            f"[하이브리드 1단계 완료] 별점-감정 괴리 의심군(rating >= 4.0 & 1차 부정): {suspicious_count:,}건 감지",
            flush=True,
        )

        if suspicious_count > 0:
            print(
                f"[하이브리드 2단계] 의심 리뷰 {suspicious_count:,}건에 대해 Transformer(BERT) 문맥 정밀 재검증 시작...",
                flush=True,
            )
            sub_parts = max(1, min(args.partitions, suspicious_count // 50 + 1))
            suspicious_df = suspicious_df.repartition(sub_parts)

            refined_df = suspicious_df.withColumn("tf_struct", transformer_udf(F.col("text_for_sentiment")))
            refined_df = (
                refined_df
                .withColumn("sentiment_score", F.col("tf_struct.score"))
                .withColumn("sentiment_label", F.col("tf_struct.label"))
                .withColumn("model_name", F.lit("hybrid-transformer"))
                .drop("tf_struct")
            )

            scored_df = normal_df.unionByName(refined_df)
        else:
            print("[하이브리드 알림] 괴리 의심 리뷰가 없어 2단계를 건너뜁니다.", flush=True)
            scored_df = normal_df

    # [최적화 2] UDF 연산 결과 캐싱:
    # 이후 metrics_df와 anomalies_df의 count(), write() 실행 시 무거운 감정분석이 3~4회 중복 반복되지 않도록 방지
    scored_df = scored_df.persist(StorageLevel.MEMORY_AND_DISK)

    # --------------------------------------------------------------------------
    # 1. 상품별-월별 감정 요약 집계 마트 산출
    # --------------------------------------------------------------------------
    print(f"[감정분석 집계] 상품별(product_id) x 월별(review_year_month) 감정 지표 산출 중...", flush=True)
    metrics_df = (
        scored_df.groupBy("product_id", "review_year_month")
        .agg(
            F.count("*").cast("int").alias("total_reviews"),
            F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment_score"),
            F.sum(F.when(F.col("sentiment_label") == "POSITIVE", 1).otherwise(0)).cast("int").alias("positive_reviews"),
            F.sum(F.when(F.col("sentiment_label") == "NEUTRAL", 1).otherwise(0)).cast("int").alias("neutral_reviews"),
            F.sum(F.when(F.col("sentiment_label") == "NEGATIVE", 1).otherwise(0)).cast("int").alias("negative_reviews"),
            # 평점은 높으나(>= 4.0) 본문 감정은 부정(NEGATIVE)인 "숨은 불만 리뷰" 집계
            F.sum(
                F.when((F.col("rating") >= 4.0) & (F.col("sentiment_label") == "NEGATIVE"), 1).otherwise(0)
            ).cast("int").alias("mismatch_reviews"),
            F.first("model_name").alias("model_name"),
        )
        .withColumn("category", F.lit(category))
        .withColumn("updated_at", F.current_timestamp())
    )

    metrics_count = metrics_df.count()
    print(f"[감정분석 집계] 산출된 상품-월별 레코드 수: {metrics_count:,}건", flush=True)

    # --------------------------------------------------------------------------
    # 2. 별점-본문 감정 괴리(이상) 리뷰 데이터 추출
    # --------------------------------------------------------------------------
    anomalies_df = (
        scored_df
        .filter((F.col("rating") >= 4.0) & (F.col("sentiment_label") == "NEGATIVE"))
        .select(
            F.lit(category).alias("category"),
            F.col("product_id"),
            F.col("review_id"),
            F.col("review_year_month"),
            F.col("rating").cast("double").alias("rating"),
            F.col("sentiment_score").cast("double").alias("sentiment_score"),
            F.col("sentiment_label"),
            F.substring(F.col("title_for_sentiment"), 1, 300).alias("review_title"),
            F.substring(F.col("cleaned_review_text"), 1, 1000).alias("review_text"),
            F.lit("HIGH_RATING_NEGATIVE_TEXT").alias("anomaly_type"),
            F.current_timestamp().alias("created_at"),
        )
    )
    anomalies_count = anomalies_df.count()
    if requested_mode in ("hybrid", "auto") and suspicious_count > 0:
        recovered_count = max(0, suspicious_count - anomalies_count)
        print(
            f"[하이브리드 정밀 검증 완료] 진짜 괴리 불만 리뷰: {anomalies_count:,}건 확정 "
            f"(가짜 이상치 {recovered_count:,}건이 Transformer 문맥 판정으로 POSITIVE/NEUTRAL로 정상 복구됨)",
            flush=True,
        )
    else:
        print(f"[이상 리뷰 감지] 별점 괴리 불만 리뷰 추출 건수: {anomalies_count:,}건", flush=True)

    # --------------------------------------------------------------------------
    # 3. PostgreSQL 원자적 UPSERT 적재
    # --------------------------------------------------------------------------
    jdbc_url = (
        f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{quote(db_config['dbname'], safe='')}"
        "?reWriteBatchedInserts=true"
    )
    run_id = uuid.uuid4().hex[:8]

    # 3-1. 감정 요약 마트 적재
    staging_metrics_table = f"product_sentiment_monthly_staging_{run_id}"
    print(f"[DB 적재] 요약 마트 임시 테이블({staging_metrics_table}) 저장 중 (batchsize=10000, Multi-row rewrite)...", flush=True)
    target_write_parts = min(max(4, metrics_df.rdd.getNumPartitions()), 8)
    (
        metrics_df.coalesce(target_write_parts)
        .write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", staging_metrics_table)
        .option("user", db_config["user"])
        .option("password", db_config["password"])
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "10000")
        .mode("overwrite")
        .save()
    )
    upsert_staging_to_metrics(db_config, staging_metrics_table, args.sentiment_table)

    # 3-2. 이상 리뷰 마트 적재 (건수가 있을 경우)
    if anomalies_count > 0:
        staging_anom_table = f"product_sentiment_anomalies_staging_{run_id}"
        print(f"[DB 적재] 괴리 리뷰 임시 테이블({staging_anom_table}) 저장 중 (batchsize=10000)...", flush=True)
        (
            anomalies_df.coalesce(target_write_parts)
            .write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", staging_anom_table)
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", "10000")
            .mode("overwrite")
            .save()
        )
        upsert_staging_to_anomalies(db_config, staging_anom_table, args.anomaly_table)

    # 캐시 메모리 해제
    try:
        scored_df.unpersist()
    except Exception:
        pass

    print("=" * 70, flush=True)
    print(f"[감정분석 파이프라인 완료] 요약 {metrics_count:,}건, 이상 리뷰 {anomalies_count:,}건 적재 완료", flush=True)
    print("=" * 70, flush=True)


def parse_arguments():
    """CLI 인자 파싱."""
    parser = argparse.ArgumentParser(description="Spark Amazon 리뷰 감정분석 및 이상 리뷰 마트 파이프라인")
    parser.add_argument("--category", default=os.getenv("DEFAULT_CATEGORY", "All_Beauty"), help="상품 카테고리")
    parser.add_argument("--year-month", default="2023-01", help="분석 대상 연월 (예: 2023-01 또는 all)")
    parser.add_argument(
        "--processed-dir",
        default=os.getenv("PROCESSED_DIR", "/opt/spark/data/processed/All_Beauty"),
        help="정제 Parquet 저장 경로",
    )
    parser.add_argument(
        "--sentiment-table",
        default="public.product_sentiment_monthly",
        help="월별 감정 요약 적재 대상 테이블",
    )
    parser.add_argument(
        "--anomaly-table",
        default="public.product_sentiment_anomalies",
        help="괴리 이상 리뷰 적재 대상 테이블",
    )
    parser.add_argument(
        "--model-type",
        choices=["hybrid", "nltk", "transformer", "auto"],
        default="hybrid",
        help="감정분석 모델 (기본: hybrid[1차 VADER + 2차 Transformer 정밀검증], nltk: 경량, transformer: 전체 BERT)",
    )
    parser.add_argument("--partitions", type=int, default=32, help="Spark 셔플 파티션 수")
    return parser.parse_args()


def main():
    args = parse_arguments()
    db_config = get_db_config()

    try:
        ensure_database_objects(db_config, args.sentiment_table, args.anomaly_table)

        spark = (
            SparkSession.builder
            .appName(f"amazon-reviews-sentiment-{args.category}")
            .config("spark.sql.shuffle.partitions", str(args.partitions))
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        run_sentiment_analysis(spark, args, db_config)
    except Exception as e:
        print(f"[감정분석 작업 실패] 치명적 오류 발생: {e}", file=sys.stderr, flush=True)
        send_slack_alert(
            job_name="sentiment_analysis.py",
            error_message=e,
            category=args.category,
            year_month=args.year_month,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()

