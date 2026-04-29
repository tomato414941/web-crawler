ALTER TABLE public.url_ledger
    ADD COLUMN IF NOT EXISTS url_hash text,
    ADD COLUMN IF NOT EXISTS url_length integer,
    ADD COLUMN IF NOT EXISTS url_identity_version integer DEFAULT 1 NOT NULL;

UPDATE public.url_ledger
SET url_hash = md5(url),
    url_length = octet_length(url),
    url_identity_version = 1
WHERE url_hash IS NULL
   OR url_length IS NULL
   OR url_identity_version IS DISTINCT FROM 1;

CREATE INDEX IF NOT EXISTS idx_url_ledger_url_hash
    ON public.url_ledger(url_hash);

CREATE INDEX IF NOT EXISTS idx_url_ledger_url_length
    ON public.url_ledger(url_length);
