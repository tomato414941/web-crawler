ALTER TABLE public.url_ledger
    ADD COLUMN IF NOT EXISTS current_intent text;

UPDATE public.url_ledger
SET current_intent = CASE
    WHEN terminal_reason IS NOT NULL THEN NULL
    WHEN EXISTS (
        SELECT 1
        FROM public.scheduler_queue_retry_quarantine AS blocked
        WHERE blocked.url = public.url_ledger.url
    ) THEN 'retry'
    WHEN EXISTS (
        SELECT 1
        FROM public.scheduler_queue_refresh AS refresh
        WHERE refresh.url = public.url_ledger.url
    ) THEN 'refresh'
    WHEN last_error IS NOT NULL THEN 'retry'
    ELSE COALESCE(current_intent, 'explore')
END
WHERE current_intent IS NULL;

CREATE INDEX IF NOT EXISTS idx_url_ledger_current_intent
    ON public.url_ledger(current_intent);
