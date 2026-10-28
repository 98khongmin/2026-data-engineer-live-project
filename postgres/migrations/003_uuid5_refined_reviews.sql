-- Run with psql --single-transaction. Old SHA-256 results remain in a backup.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'amazon_reviews'
          AND column_name = 'review_id' AND data_type <> 'uuid'
    ) THEN
        IF to_regclass('public.amazon_reviews_legacy_backup') IS NOT NULL THEN
            RAISE EXCEPTION 'Legacy backup already exists; inspect it before migrating';
        END IF;
        ALTER TABLE public.amazon_reviews RENAME TO amazon_reviews_legacy_backup;
        ALTER INDEX IF EXISTS public.amazon_reviews_product_idx
            RENAME TO amazon_reviews_legacy_product_idx;
    END IF;
END $$;

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
COMMENT ON COLUMN public.amazon_reviews.review_id IS
    'UUID5: NAMESPACE_URL and compact JSON array [user_id, parent_asin, timestamp]';
COMMENT ON COLUMN public.amazon_reviews.review_time IS
    'UTC derived from the original integer milliseconds';
