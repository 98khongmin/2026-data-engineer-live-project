-- 3단계: 월별 리뷰 요약 집계 테이블
CREATE TABLE IF NOT EXISTS public.monthly_review_summary (
    category VARCHAR(100) NOT NULL,
    review_year_month VARCHAR(7) NOT NULL,
    total_reviews BIGINT NOT NULL,
    avg_rating DOUBLE PRECISION NOT NULL,
    verified_reviews BIGINT NOT NULL,
    unique_products BIGINT NOT NULL,
    unique_users BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (category, review_year_month)
);

CREATE INDEX IF NOT EXISTS monthly_review_summary_cat_ym_idx
    ON public.monthly_review_summary(category, review_year_month);

COMMENT ON TABLE public.monthly_review_summary IS '카테고리-월별 리뷰 요약 집계 테이블';
COMMENT ON COLUMN public.monthly_review_summary.category IS '데이터셋/상품 카테고리 (예: All_Beauty, Appliances, PK)';
COMMENT ON COLUMN public.monthly_review_summary.review_year_month IS '리뷰 작성 년월 (YYYY-MM, PK)';
COMMENT ON COLUMN public.monthly_review_summary.total_reviews IS '해당 월의 총 리뷰 수';
COMMENT ON COLUMN public.monthly_review_summary.avg_rating IS '해당 월의 평균 평점 (1.0 ~ 5.0)';
COMMENT ON COLUMN public.monthly_review_summary.verified_reviews IS '인증 구매(verified purchase) 리뷰 수';
COMMENT ON COLUMN public.monthly_review_summary.unique_products IS '리뷰가 작성된 고유 상품(asin) 수';
COMMENT ON COLUMN public.monthly_review_summary.unique_users IS '리뷰를 작성한 고유 사용자(user_id) 수';

