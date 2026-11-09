"""FastAPI 기반 Amazon 리뷰 분석 마트 서빙 API.

6회차 강의자료(7회차 과제 A) 구현 사양:
1. 서빙 구성: PostgreSQL 결과 마트(product_sentiment_monthly, product_sentiment_anomalies) -> FastAPI -> Client
2. ConnectionPool 적용: 요청마다 새 DB 커넥션을 생성하지 않고 커넥션 풀 재사용
3. 예외 및 유효성 검사 (강의자료 규격):
   - 422 Unprocessable Entity: 날짜 형식(YYYY-MM) 불일치, limit 범위(ge=1, le=100) 초과 시 자동 검증
   - 404 Not Found: 해당 조건의 집계 결과 부재 시 명확한 에러 반환
4. 배치 서빙 및 단건 실시간 추론 비교 엔드포인트 제공
"""
from contextlib import asynccontextmanager
import os
import re
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Path, Query
from pydantic import BaseModel, Field

# PostgreSQL 연결 설정
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "postgres")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DEFAULT_DSN = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_DSN = os.getenv("PG_DSN", DEFAULT_DSN)

# psycopg_pool 로드 (미설치 시 psycopg2 fallback 지원)
pool = None
try:
    from psycopg_pool import ConnectionPool
    pool = ConnectionPool(PG_DSN, open=False, min_size=2, max_size=10)
except ImportError:
    pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """서버 기동 시 DB 연결 풀을 열고 종료 시 안전하게 회수합니다."""
    if pool is not None:
        pool.open()
        print(f"[API 서버 기동] PostgreSQL 커넥션 풀 오픈 완료 (DSN: {PG_HOST}:{PG_PORT}/{PG_DB})", flush=True)
    yield
    if pool is not None:
        pool.close()
        print("[API 서버 종료] PostgreSQL 커넥션 풀 정상 회수 완료", flush=True)


app = FastAPI(
    title="Amazon Reviews Pipeline Serving API",
    description="배치 파이프라인 집계 결과 서빙 및 감정분석/이상 리뷰 조회 API",
    version="1.0.0",
    lifespan=lifespan,
)


def execute_query(sql: str, params: tuple) -> List[tuple]:
    """커넥션 풀 또는 직접 연결을 통해 쿼리를 실행하고 결과를 반환합니다."""
    if pool is not None:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
    else:
        import psycopg2
        conn = psycopg2.connect(
            host=PG_HOST,
            port=int(PG_PORT),
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        finally:
            conn.close()


# ==============================================================================
# API 엔드포인트 정의
# ==============================================================================

@app.get("/health", summary="헬스체크", tags=["시스템"])
def health():
    """서버 및 데이터베이스 연결 상태를 확인합니다."""
    db_status = "unknown"
    try:
        rows = execute_query("SELECT 1;", ())
        if rows and rows[0][0] == 1:
            db_status = "connected"
    except Exception as e:
        db_status = f"disconnected: {e}"

    return {
        "status": "ok",
        "database": db_status,
        "connection_pool": "enabled" if pool is not None else "psycopg2_direct",
    }


@app.get(
    "/products/{product_id}/sentiment",
    summary="상품별 월별 감정분석 요약 조회 (배치 추론 서빙)",
    tags=["감정분석 마트"],
)
def get_product_sentiment(
    product_id: str = Path(..., description="상품 고유 식별자 (ASIN)", example="B00004U9V2"),
    dt: str = Query(
        ...,
        pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
        description="조회 대상 년월 (형식: YYYY-MM)",
        example="2023-01",
    ),
    category: str = Query("All_Beauty", description="카테고리명"),
):
    """지정된 상품의 특정 월 감정분석 집계 결과를 고속 조회합니다.

    - 422: dt 형식이 YYYY-MM이 아닌 경우 자동 발생
    - 404: 해당 상품/월의 집계 데이터가 없는 경우
    """
    sql = """
        SELECT
            category, product_id, review_year_month, total_reviews,
            avg_sentiment_score, positive_reviews, neutral_reviews, negative_reviews,
            mismatch_reviews, model_name, updated_at
        FROM public.product_sentiment_monthly
        WHERE category = %s AND product_id = %s AND review_year_month = %s;
    """
    rows = execute_query(sql, (category, product_id, dt))
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"카테고리 '{category}'의 상품 '{product_id}'에 대한 {dt} 감정분석 집계 결과가 없습니다.",
        )

    r = rows[0]
    return {
        "category": r[0],
        "product_id": r[1],
        "review_year_month": r[2],
        "total_reviews": r[3],
        "avg_sentiment_score": float(r[4]),
        "sentiment_breakdown": {
            "positive_reviews": r[5],
            "neutral_reviews": r[6],
            "negative_reviews": r[7],
            "positive_ratio": round(r[5] / r[3], 4) if r[3] > 0 else 0.0,
            "negative_ratio": round(r[7] / r[3], 4) if r[3] > 0 else 0.0,
        },
        "mismatch_reviews": r[8],
        "model_name": r[9],
        "updated_at": r[10].isoformat() if r[10] else None,
    }


@app.get(
    "/products/{product_id}/anomalies",
    summary="별점-본문 감정 괴리 이상 리뷰 조회",
    tags=["감정분석 마트"],
)
def get_product_anomalies(
    product_id: str = Path(..., description="상품 고유 식별자 (ASIN)", example="B00004U9V2"),
    dt: str = Query(
        ...,
        pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
        description="조회 대상 년월 (형식: YYYY-MM)",
        example="2023-01",
    ),
    category: str = Query("All_Beauty", description="카테고리명"),
    limit: int = Query(5, ge=1, le=50, description="반환할 최대 리뷰 건수 (1~50)"),
):
    """별점은 4~5점으로 높으나 본문 감정은 부정적인 '숨은 불만 리뷰'를 조회합니다."""
    sql = """
        SELECT
            review_id, rating, sentiment_score, sentiment_label,
            review_title, review_text, anomaly_type, created_at
        FROM public.product_sentiment_anomalies
        WHERE category = %s AND product_id = %s AND review_year_month = %s
        ORDER BY sentiment_score ASC
        LIMIT %s;
    """
    rows = execute_query(sql, (category, product_id, dt, limit))
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"상품 '{product_id}'의 {dt} 괴리 이상 리뷰 내역이 없습니다.",
        )

    anomalies = [
        {
            "review_id": r[0],
            "rating": float(r[1]),
            "sentiment_score": float(r[2]),
            "sentiment_label": r[3],
            "review_title": r[4],
            "review_text": r[5],
            "anomaly_type": r[6],
            "created_at": r[7].isoformat() if r[7] else None,
        }
        for r in rows
    ]

    return {
        "category": category,
        "product_id": product_id,
        "review_year_month": dt,
        "count": len(anomalies),
        "anomalies": anomalies,
    }


@app.get(
    "/categories/{category}/sentiment/rankings",
    summary="카테고리별 감정 랭킹 조회 (워스트/베스트 상품)",
    tags=["카테고리 마트"],
)
def get_sentiment_rankings(
    category: str = Path(..., description="카테고리명 (예: All_Beauty, CDs_and_Vinyl)"),
    dt: str = Query(
        ...,
        pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
        description="조회 대상 년월 (형식: YYYY-MM)",
        example="2023-01",
    ),
    order: str = Query(
        "worst",
        pattern=r"^(worst|best)$",
        description="정렬 순서: worst(부정적 점수 순) 또는 best(긍정적 점수 순)",
    ),
    min_reviews: int = Query(3, ge=1, le=100, description="최소 리뷰 수 필터"),
    limit: int = Query(10, ge=1, le=100, description="조회 건수 제한 (1~100)"),
):
    """특정 월 카테고리 내에서 불만율이 가장 높은(worst) 혹은 만족도가 가장 높은(best) 상품 목록을 반환합니다."""
    order_clause = "ASC" if order == "worst" else "DESC"
    sql = f"""
        SELECT
            product_id, total_reviews, avg_sentiment_score,
            positive_reviews, negative_reviews, mismatch_reviews
        FROM public.product_sentiment_monthly
        WHERE category = %s AND review_year_month = %s AND total_reviews >= %s
        ORDER BY avg_sentiment_score {order_clause}
        LIMIT %s;
    """
    rows = execute_query(sql, (category, dt, min_reviews, limit))
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"카테고리 '{category}'의 {dt} 조건에 부합하는 상품 랭킹 데이터가 없습니다.",
        )

    ranking_items = [
        {
            "rank": idx + 1,
            "product_id": r[0],
            "total_reviews": r[1],
            "avg_sentiment_score": float(r[2]),
            "positive_reviews": r[3],
            "negative_reviews": r[4],
            "mismatch_reviews": r[5],
        }
        for idx, r in enumerate(rows)
    ]

    return {
        "category": category,
        "review_year_month": dt,
        "order": order,
        "count": len(ranking_items),
        "rankings": ranking_items,
    }


# ==============================================================================
# 실시간 요청 시 추론 (On-demand Inference) 엔드포인트 - 비교 실험용
# ==============================================================================

class TextInferenceRequest(BaseModel):
    text: str = Field(..., min_length=1, max_length=2000, description="분석할 리뷰 텍스트")


@app.post(
    "/sentiment/predict",
    summary="단건 텍스트 실시간 감정 추론 (배치 서빙과 지연시간 비교용)",
    tags=["실시간 추론"],
)
def predict_sentiment(req: TextInferenceRequest):
    """임의의 리뷰 텍스트에 대해 API 서버에서 실시간으로 감정 점수를 산출합니다.

    배치 서빙(p95 수 ms) vs 실시간 추론(p95 수백 ms)의 지연시간 비교 및 데모용입니다.
    """
    from jobs.sentiment_analysis import _get_worker_analyzer

    analyzer, model_name = _get_worker_analyzer("auto")
    score, label = analyzer(req.text)

    return {
        "model_name": model_name,
        "text": req.text,
        "sentiment_score": score,
        "sentiment_label": label,
    }

