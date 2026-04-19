DO $$
BEGIN
    IF to_regclass('public.host_state') IS NULL
       AND to_regclass('public.domain_state') IS NOT NULL THEN
        ALTER TABLE public.domain_state RENAME TO host_state;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'domain_state_pkey'
          AND conrelid = 'public.host_state'::regclass
    ) THEN
        ALTER TABLE public.host_state
            RENAME CONSTRAINT domain_state_pkey TO host_state_pkey;
    END IF;
END $$;

DROP INDEX IF EXISTS public.idx_domain_state_next_request_at;
DROP INDEX IF EXISTS public.idx_domain_state_backoff_until;

CREATE INDEX IF NOT EXISTS idx_host_state_next_request_at
    ON public.host_state(next_request_at);

CREATE INDEX IF NOT EXISTS idx_host_state_backoff_until
    ON public.host_state(backoff_until);

DO $$
DECLARE
    target_table text;
BEGIN
    FOREACH target_table IN ARRAY ARRAY[
        'url_ledger',
        'pages',
        'scheduler_queue_frontline',
        'scheduler_queue_deferred',
        'scheduler_queue_refresh',
        'scheduler_queue_retry_quarantine',
        'active_leases'
    ]
    LOOP
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = target_table
              AND column_name = 'domain'
        ) AND NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = target_table
              AND column_name = 'host'
        ) THEN
            EXECUTE format('ALTER TABLE public.%I RENAME COLUMN domain TO host', target_table);
        END IF;
    END LOOP;
END $$;

DROP INDEX IF EXISTS public.idx_url_ledger_domain;
DROP INDEX IF EXISTS public.idx_pages_domain;
DROP INDEX IF EXISTS public.idx_scheduler_queue_frontline_domain;
DROP INDEX IF EXISTS public.idx_scheduler_queue_deferred_domain;
DROP INDEX IF EXISTS public.idx_scheduler_queue_refresh_domain;
DROP INDEX IF EXISTS public.idx_scheduler_queue_retry_quarantine_domain;
DROP INDEX IF EXISTS public.idx_active_leases_domain;

CREATE INDEX IF NOT EXISTS idx_url_ledger_host
    ON public.url_ledger(host);

CREATE INDEX IF NOT EXISTS idx_pages_host
    ON public.pages(host);

CREATE INDEX IF NOT EXISTS idx_scheduler_queue_frontline_host
    ON public.scheduler_queue_frontline(host);

CREATE INDEX IF NOT EXISTS idx_scheduler_queue_deferred_host
    ON public.scheduler_queue_deferred(host);

CREATE INDEX IF NOT EXISTS idx_scheduler_queue_refresh_host
    ON public.scheduler_queue_refresh(host);

CREATE INDEX IF NOT EXISTS idx_scheduler_queue_retry_quarantine_host
    ON public.scheduler_queue_retry_quarantine(host);

CREATE INDEX IF NOT EXISTS idx_active_leases_host
    ON public.active_leases(host);
