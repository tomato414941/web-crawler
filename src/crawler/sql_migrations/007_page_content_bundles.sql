CREATE TABLE IF NOT EXISTS public.content_bundles (
    bundle_key text NOT NULL,
    stored_bytes bigint NOT NULL,
    record_count integer NOT NULL,
    live_records integer NOT NULL,
    live_bytes bigint NOT NULL,
    created_at double precision DEFAULT EXTRACT(epoch FROM now()) NOT NULL,
    CONSTRAINT content_bundles_pkey PRIMARY KEY (bundle_key)
);

CREATE INDEX IF NOT EXISTS idx_content_bundles_released
    ON public.content_bundles(created_at)
    WHERE live_records <= 0;

ALTER TABLE public.pages
    ADD COLUMN IF NOT EXISTS content_bundle_key text,
    ADD COLUMN IF NOT EXISTS content_offset bigint,
    ADD COLUMN IF NOT EXISTS content_record_bytes integer;

CREATE INDEX IF NOT EXISTS idx_pages_content_bundle_key
    ON public.pages(content_bundle_key)
    WHERE content_bundle_key IS NOT NULL;

-- Keep bundle liveness exact for every page update or delete, including manual cleanup.
CREATE OR REPLACE FUNCTION public.release_page_content_record() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE public.content_bundles
    SET live_records = live_records - 1,
        live_bytes = live_bytes - COALESCE(OLD.content_record_bytes, 0)
    WHERE bundle_key = OLD.content_bundle_key;
    RETURN NULL;
END
$$;

CREATE OR REPLACE TRIGGER pages_release_content_record_on_update
    AFTER UPDATE OF content_bundle_key, content_offset ON public.pages
    FOR EACH ROW
    WHEN (
        OLD.content_bundle_key IS NOT NULL
        AND (
            NEW.content_bundle_key IS DISTINCT FROM OLD.content_bundle_key
            OR NEW.content_offset IS DISTINCT FROM OLD.content_offset
        )
    )
    EXECUTE FUNCTION public.release_page_content_record();

CREATE OR REPLACE TRIGGER pages_release_content_record_on_delete
    AFTER DELETE ON public.pages
    FOR EACH ROW
    WHEN (OLD.content_bundle_key IS NOT NULL)
    EXECUTE FUNCTION public.release_page_content_record();
