CREATE TABLE IF NOT EXISTS public.page_content (
    url_hash text NOT NULL,
    content text DEFAULT ''::text NOT NULL,
    updated_at double precision DEFAULT EXTRACT(epoch FROM now()) NOT NULL,
    CONSTRAINT page_content_pkey PRIMARY KEY (url_hash),
    CONSTRAINT page_content_url_hash_fkey
        FOREIGN KEY (url_hash) REFERENCES public.pages(url_hash) ON DELETE CASCADE
);

ALTER TABLE public.pages
    ADD COLUMN IF NOT EXISTS content_type text,
    ADD COLUMN IF NOT EXISTS storage_tier text DEFAULT 'metadata_only'::text NOT NULL,
    ADD COLUMN IF NOT EXISTS storage_reason text DEFAULT 'metadata_only'::text NOT NULL,
    ADD COLUMN IF NOT EXISTS stored_content_bytes integer DEFAULT 0 NOT NULL,
    ADD COLUMN IF NOT EXISTS content_truncated boolean DEFAULT false NOT NULL,
    ADD COLUMN IF NOT EXISTS outlink_count integer DEFAULT 0 NOT NULL,
    ADD COLUMN IF NOT EXISTS stored_outlink_count integer DEFAULT 0 NOT NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'pages'
          AND column_name = 'content'
    ) THEN
        EXECUTE $sql$
            INSERT INTO public.page_content (url_hash, content, updated_at)
            SELECT url_hash, content, EXTRACT(epoch FROM now())
            FROM public.pages
            WHERE content IS NOT NULL
              AND content <> ''
            ON CONFLICT (url_hash) DO UPDATE
            SET content = EXCLUDED.content,
                updated_at = EXCLUDED.updated_at
        $sql$;

        EXECUTE $sql$
            UPDATE public.pages
            SET storage_tier = CASE
                    WHEN content IS NULL OR content = '' THEN 'metadata_only'
                    ELSE 'legacy'
                END,
                storage_reason = 'legacy_migration',
                stored_content_bytes = CASE
                    WHEN content IS NULL THEN 0
                    ELSE octet_length(content)
                END,
                content_truncated = false,
                outlink_count = COALESCE(array_length(outlinks, 1), 0),
                stored_outlink_count = COALESCE(array_length(outlinks, 1), 0)
        $sql$;

        ALTER TABLE public.pages DROP COLUMN content;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_pages_storage_tier
    ON public.pages(storage_tier);
