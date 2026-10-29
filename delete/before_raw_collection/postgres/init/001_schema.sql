-- Safe to reapply: preserves existing rows and creates missing objects only.
CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE IF NOT EXISTS public.amazon_reviews (
    review_id CHAR(64) PRIMARY KEY,
    user_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    review_text TEXT NOT NULL,
    rating DOUBLE PRECISION NOT NULL CHECK (rating BETWEEN 1 AND 5),
    review_title TEXT,
    review_time TIMESTAMP WITHOUT TIME ZONE,
    verified_purchase BOOLEAN,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS amazon_reviews_product_idx
    ON public.amazon_reviews(product_id);

COMMENT ON COLUMN public.amazon_reviews.review_time IS
    'UTC; Amazon epoch converted by Spark';
COMMENT ON COLUMN public.amazon_reviews.review_id IS
    'SHA-256 of ordered normalized HASH_COLUMNS JSON';
