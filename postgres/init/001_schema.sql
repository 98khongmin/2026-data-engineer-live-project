-- Safe to reapply: preserves existing rows and creates missing objects only.
CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE IF NOT EXISTS public.amazon_reviews (
    review_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    parent_asin TEXT NOT NULL,
    "timestamp" BIGINT NOT NULL,
    product_id TEXT NOT NULL,
    review_text TEXT NOT NULL,
    rating DOUBLE PRECISION NOT NULL CHECK (rating BETWEEN 1 AND 5),
    review_title TEXT,
    review_time TIMESTAMPTZ NOT NULL,
    verified_purchase BOOLEAN,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS amazon_reviews_product_idx
    ON public.amazon_reviews(product_id);

COMMENT ON COLUMN public.amazon_reviews.review_time IS
    'UTC derived from the original integer milliseconds';
COMMENT ON COLUMN public.amazon_reviews.review_id IS
    'UUID5: NAMESPACE_URL and compact JSON array [user_id, parent_asin, timestamp]';

-- Raw collection is separate from existing refined results. Do not drop either.
CREATE TABLE IF NOT EXISTS public.raw_reviews (
    user_id TEXT NOT NULL,
    parent_asin TEXT NOT NULL,
    "timestamp" BIGINT NOT NULL,
    rating DOUBLE PRECISION,
    title TEXT,
    text TEXT,
    images JSONB,
    asin TEXT,
    helpful_vote BIGINT,
    verified_purchase BOOLEAN,
    source_file TEXT NOT NULL,
    source_line BIGINT NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, parent_asin, "timestamp")
);

COMMENT ON COLUMN public.raw_reviews.images IS
    'Original image array stored as JSONB; other review fields have separate columns';
COMMENT ON COLUMN public.raw_reviews."timestamp" IS
    'Original integer timestamp; no seconds/milliseconds conversion';

-- Spark JDBC partitions read source_line ranges in parallel.
CREATE INDEX IF NOT EXISTS raw_reviews_source_line_idx
    ON public.raw_reviews(source_line);
