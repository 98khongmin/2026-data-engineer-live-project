-- 3단계: 일별 리뷰 집계 테이블 (Gold Layer / Mart)
CREATE TABLE IF NOT EXISTS public.daily_review_summary (
    review_date DATE PRIMARY KEY,
    total_reviews BIGINT NOT NULL,
    avg_rating DOUBLE PRECISION NOT NULL,
    verified_reviews BIGINT NOT NULL,
    unique_products BIGINT NOT NULL,
    unique_users BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS daily_review_summary_date_idx
    ON public.daily_review_summary(review_date);

COMMENT ON TABLE public.daily_review_summary IS '일별 리뷰 집계 요약 테이블 (Gold Layer / Mart)';
COMMENT ON COLUMN public.daily_review_summary.review_date IS '리뷰 작성 일자 (PK)';
COMMENT ON COLUMN public.daily_review_summary.total_reviews IS '해당 일자의 총 리뷰 수';
COMMENT ON COLUMN public.daily_review_summary.avg_rating IS '해당 일자의 평균 평점 (1.0 ~ 5.0)';
COMMENT ON COLUMN public.daily_review_summary.verified_reviews IS '인증 구매(verified purchase) 리뷰 수';
COMMENT ON COLUMN public.daily_review_summary.unique_products IS '리뷰가 작성된 고유 상품(asin) 수';
COMMENT ON COLUMN public.daily_review_summary.unique_users IS '리뷰를 작성한 고유 사용자(user_id) 수';

