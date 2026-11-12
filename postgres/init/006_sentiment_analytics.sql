-- 6단계: 감정 분석 마트 및 이상 리뷰 마트 테이블/인덱스 정의

-- 1. 상품별 월별 감정 집계 마트 테이블
CREATE TABLE IF NOT EXISTS public.product_sentiment_monthly (
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
    ON public.product_sentiment_monthly(category, review_year_month);

CREATE INDEX IF NOT EXISTS idx_prod_sent_cat_prod
    ON public.product_sentiment_monthly(category, product_id);

-- 카테고리별 월별 감정 랭킹 조회(ORDER BY avg_sentiment_score) 가속 인덱스
CREATE INDEX IF NOT EXISTS idx_prod_sent_ranking
    ON public.product_sentiment_monthly(category, review_year_month, avg_sentiment_score);

COMMENT ON TABLE public.product_sentiment_monthly IS '상품별 월별 리뷰 감정분석 요약 마트 테이블';
COMMENT ON COLUMN public.product_sentiment_monthly.avg_sentiment_score IS '평균 감정 점수 (-1.0 ~ +1.0)';
COMMENT ON COLUMN public.product_sentiment_monthly.mismatch_reviews IS '별점 괴리 불만 리뷰 수 (rating >= 4.0 & sentiment NEGATIVE)';


-- 2. 별점-본문 감정 괴리(이상) 리뷰 테이블
CREATE TABLE IF NOT EXISTS public.product_sentiment_anomalies (
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
    ON public.product_sentiment_anomalies(category, product_id, review_year_month);

-- 별점 괴리 리뷰 조회(ORDER BY sentiment_score ASC LIMIT N) 가속을 위한 복합 인덱스 (Sort 단계 제거)
CREATE INDEX IF NOT EXISTS idx_sent_anom_cat_prod_ym_score
    ON public.product_sentiment_anomalies(category, product_id, review_year_month, sentiment_score ASC);

COMMENT ON TABLE public.product_sentiment_anomalies IS '별점은 높으나 본문은 부정적인 괴리 불만 리뷰 테이블';

