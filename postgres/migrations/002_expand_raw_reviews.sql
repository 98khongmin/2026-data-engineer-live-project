-- Run against an existing DB using psql --single-transaction.
-- Preserve the previous table before creating the expanded column layout.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'raw_reviews'
          AND column_name = 'raw_payload'
    ) THEN
        IF to_regclass('public.raw_reviews_payload_backup') IS NOT NULL THEN
            RAISE EXCEPTION 'Backup table already exists; inspect it before migrating';
        END IF;
        ALTER TABLE public.raw_reviews RENAME TO raw_reviews_payload_backup;
    END IF;
END $$;

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

-- Populate from the backup once; reruns safely skip already-migrated keys.
DO $$
BEGIN
    IF to_regclass('public.raw_reviews_payload_backup') IS NOT NULL THEN
        INSERT INTO public.raw_reviews (
            user_id, parent_asin, "timestamp", rating, title, text,
            images, asin, helpful_vote, verified_purchase,
            source_file, source_line, collected_at
        )
        SELECT old.user_id, old.parent_asin, old."timestamp",
               review.rating, review.title, review.text, review.images,
               review.asin, review.helpful_vote, review.verified_purchase,
               old.source_file, old.source_line, old.collected_at
        FROM public.raw_reviews_payload_backup AS old
        CROSS JOIN LATERAL json_to_record(old.raw_payload::json) AS review(
            rating DOUBLE PRECISION, title TEXT, text TEXT, images JSONB,
            asin TEXT, helpful_vote BIGINT, verified_purchase BOOLEAN
        )
        WHERE TRUE
        ON CONFLICT (user_id, parent_asin, "timestamp") DO NOTHING;
    END IF;
END $$;
