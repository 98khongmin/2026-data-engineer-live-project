-- 5단계: 상품별 월/분기별 분석 마트 및 뷰 정의

-- 1. 상품별 월별 지표 마트 테이블 (review_quarter 및 category 포함)
CREATE TABLE IF NOT EXISTS public.product_monthly_metrics (
    category VARCHAR(100) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    review_year_month VARCHAR(7) NOT NULL,
    review_quarter VARCHAR(7) NOT NULL,  -- 예: '2023-Q1'
    total_reviews INTEGER NOT NULL,
    avg_rating DOUBLE PRECISION NOT NULL,
    verified_reviews INTEGER NOT NULL,
    helpful_votes_sum BIGINT NOT NULL DEFAULT 0,
    positive_reviews INTEGER NOT NULL,    -- 4~5점 긍정 리뷰
    negative_reviews INTEGER NOT NULL,    -- 1~2점 부정 리뷰
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (category, product_id, review_year_month)
);

CREATE INDEX IF NOT EXISTS idx_prod_metrics_cat_ym
    ON public.product_monthly_metrics(category, review_year_month);

CREATE INDEX IF NOT EXISTS idx_prod_metrics_cat_prod
    ON public.product_monthly_metrics(category, product_id);

CREATE INDEX IF NOT EXISTS idx_prod_metrics_quarter
    ON public.product_monthly_metrics(review_quarter);

COMMENT ON TABLE public.product_monthly_metrics IS '상품별 월별 리뷰 지표 집계 마트 (카테고리 및 분기 컬럼 포함)';
COMMENT ON COLUMN public.product_monthly_metrics.category IS '데이터셋/상품 카테고리 (예: All_Beauty, Appliances, PK)';
COMMENT ON COLUMN public.product_monthly_metrics.product_id IS '상품 식별자 (asin, PK)';
COMMENT ON COLUMN public.product_monthly_metrics.review_year_month IS '리뷰 작성 년월 (YYYY-MM, PK)';
COMMENT ON COLUMN public.product_monthly_metrics.review_quarter IS '리뷰 작성 분기 (YYYY-Q#)';
COMMENT ON COLUMN public.product_monthly_metrics.total_reviews IS '해당 월의 총 리뷰 수';
COMMENT ON COLUMN public.product_monthly_metrics.avg_rating IS '해당 월의 평균 평점';
COMMENT ON COLUMN public.product_monthly_metrics.verified_reviews IS '실구매 인증 리뷰 수';
COMMENT ON COLUMN public.product_monthly_metrics.helpful_votes_sum IS '추천(helpful_vote) 합계';
COMMENT ON COLUMN public.product_monthly_metrics.positive_reviews IS '긍정 리뷰 수 (4.0 ~ 5.0점)';
COMMENT ON COLUMN public.product_monthly_metrics.negative_reviews IS '부정 리뷰 수 (1.0 ~ 2.0점)';


-- 2. 분기별 지표 요약 뷰 (리뷰 수 가중 평균 평점 적용, 카테고리별 분할)
CREATE OR REPLACE VIEW public.v_product_quarterly_metrics AS
SELECT
    category,
    product_id,
    review_quarter,
    SUM(total_reviews) AS total_reviews,
    ROUND((SUM(avg_rating * total_reviews) / NULLIF(SUM(total_reviews), 0))::numeric, 2) AS avg_rating,
    SUM(verified_reviews) AS verified_reviews,
    SUM(helpful_votes_sum) AS helpful_votes_sum,
    SUM(positive_reviews) AS positive_reviews,
    SUM(negative_reviews) AS negative_reviews,
    MAX(updated_at) AS updated_at
FROM public.product_monthly_metrics
GROUP BY category, product_id, review_quarter;

COMMENT ON VIEW public.v_product_quarterly_metrics IS '카테고리-상품별 분기별 지표 집계 뷰 (가중 평균 평점 산출)';


-- 3. 리뷰 수 급증 상품 뷰 (연속 월 검증 + 100% 이상 급증 조건, 카테고리별 파티셔닝)
CREATE OR REPLACE VIEW public.v_product_review_surge AS
WITH lag_metrics AS (
    SELECT
        category,
        product_id,
        review_year_month,
        review_quarter,
        total_reviews,
        avg_rating,
        LAG(review_year_month, 1) OVER (PARTITION BY category, product_id ORDER BY review_year_month) AS prev_ym,
        LAG(total_reviews, 1) OVER (PARTITION BY category, product_id ORDER BY review_year_month) AS raw_prev_reviews,
        LAG(avg_rating, 1) OVER (PARTITION BY category, product_id ORDER BY review_year_month) AS raw_prev_rating,
        TO_CHAR(TO_DATE(review_year_month || '-01', 'YYYY-MM-DD') - INTERVAL '1 month', 'YYYY-MM') AS expected_prev_ym
    FROM public.product_monthly_metrics
),
consecutive_metrics AS (
    SELECT
        category,
        product_id,
        review_year_month,
        review_quarter,
        total_reviews,
        avg_rating,
        -- 이전 달이 정확히 직전 달(1개월 전)인 경우에만 직전 월 리뷰 수 바인딩, 단절된 경우 NULL 처리
        CASE
            WHEN prev_ym = expected_prev_ym THEN raw_prev_reviews
            ELSE NULL
        END AS prev_month_reviews,
        CASE
            WHEN prev_ym = expected_prev_ym THEN raw_prev_rating
            ELSE NULL
        END AS prev_month_rating,
        CASE
            WHEN prev_ym = expected_prev_ym THEN true
            ELSE false
        END AS is_consecutive_month
    FROM lag_metrics
)
SELECT
    category,
    product_id,
    review_year_month,
    review_quarter,
    prev_month_reviews,
    total_reviews,
    (total_reviews - COALESCE(prev_month_reviews, 0)) AS review_growth_cnt,
    ROUND(
        CASE 
            WHEN COALESCE(prev_month_reviews, 0) > 0 
            THEN ((total_reviews - prev_month_reviews)::numeric / prev_month_reviews::numeric) * 100
            ELSE 100.0 
        END, 2
    ) AS mom_growth_rate_pct,
    prev_month_rating,
    avg_rating,
    ROUND((avg_rating - COALESCE(prev_month_rating, avg_rating))::numeric, 2) AS rating_delta,
    is_consecutive_month
FROM consecutive_metrics
WHERE total_reviews >= 10
  AND (
    prev_month_reviews IS NULL
    OR prev_month_reviews = 0
    OR (total_reviews - prev_month_reviews)::numeric / prev_month_reviews >= 1.0
  )
ORDER BY review_growth_cnt DESC;

COMMENT ON VIEW public.v_product_review_surge IS '월별 리뷰 수 급증(100% 이상 증가) 상품 탐지 뷰 (연속월 방어 로직 탑재)';


-- 4. 장기간 저리뷰 vs 다리뷰 상품 비교 마트 테이블 (기준값 및 산출 기간 보존, 카테고리별 보존)
CREATE TABLE IF NOT EXISTS public.product_segment_comparison (
    category VARCHAR(100) NOT NULL,
    segment_rule VARCHAR(50) NOT NULL,
    analysis_start_month VARCHAR(7) NOT NULL,
    analysis_end_month VARCHAR(7) NOT NULL,
    segment_type VARCHAR(20) NOT NULL,  -- LOW_REVIEWS, MID_REVIEWS, HIGH_REVIEWS
    threshold_p20_reviews INTEGER NOT NULL,
    threshold_p80_reviews INTEGER NOT NULL,
    product_count BIGINT NOT NULL,
    total_reviews_sum BIGINT NOT NULL,
    avg_product_reviews DOUBLE PRECISION NOT NULL,
    weighted_avg_rating DOUBLE PRECISION NOT NULL,
    verified_reviews_ratio DOUBLE PRECISION NOT NULL,
    positive_reviews_ratio DOUBLE PRECISION NOT NULL,
    negative_reviews_ratio DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (category, segment_rule, analysis_start_month, analysis_end_month, segment_type)
);

CREATE INDEX IF NOT EXISTS idx_segment_comp_period
    ON public.product_segment_comparison(category, analysis_start_month, analysis_end_month);

COMMENT ON TABLE public.product_segment_comparison IS '장기간 저리뷰 vs 다리뷰 상품 비교 마트 (재현성을 위한 기준값 및 분석기간 보존)';
COMMENT ON COLUMN public.product_segment_comparison.category IS '데이터셋/상품 카테고리 (예: All_Beauty, Appliances, PK)';
COMMENT ON COLUMN public.product_segment_comparison.segment_rule IS '분위수 분할 규칙 (예: percentile_p20_p80)';
COMMENT ON COLUMN public.product_segment_comparison.analysis_start_month IS '분석 대상 시작 년월';
COMMENT ON COLUMN public.product_segment_comparison.analysis_end_month IS '분석 대상 마감 년월';
COMMENT ON COLUMN public.product_segment_comparison.segment_type IS '세그먼트 그룹 구분 (LOW_REVIEWS / MID_REVIEWS / HIGH_REVIEWS)';
COMMENT ON COLUMN public.product_segment_comparison.threshold_p20_reviews IS '저리뷰 경계 기준 리뷰 수 (하위 20%)';
COMMENT ON COLUMN public.product_segment_comparison.threshold_p80_reviews IS '다리뷰 경계 기준 리뷰 수 (상위 20%)';

