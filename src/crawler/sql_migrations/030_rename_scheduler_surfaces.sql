DO $$
BEGIN
    IF to_regclass('public.scheduler_queue_runnable') IS NULL
       AND to_regclass('public.scheduler_queue_frontline') IS NOT NULL THEN
        ALTER TABLE public.scheduler_queue_frontline RENAME TO scheduler_queue_runnable;
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('public.scheduler_queue_scheduled') IS NULL
       AND to_regclass('public.scheduler_queue_deferred') IS NOT NULL THEN
        ALTER TABLE public.scheduler_queue_deferred RENAME TO scheduler_queue_scheduled;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'scheduler_queue_frontline_pkey'
    ) THEN
        ALTER TABLE public.scheduler_queue_runnable
            RENAME CONSTRAINT scheduler_queue_frontline_pkey TO scheduler_queue_runnable_pkey;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'scheduler_queue_frontline_url_fkey'
    ) THEN
        ALTER TABLE public.scheduler_queue_runnable
            RENAME CONSTRAINT scheduler_queue_frontline_url_fkey TO scheduler_queue_runnable_url_fkey;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'scheduler_queue_deferred_pkey'
    ) THEN
        ALTER TABLE public.scheduler_queue_scheduled
            RENAME CONSTRAINT scheduler_queue_deferred_pkey TO scheduler_queue_scheduled_pkey;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'scheduler_queue_deferred_url_fkey'
    ) THEN
        ALTER TABLE public.scheduler_queue_scheduled
            RENAME CONSTRAINT scheduler_queue_deferred_url_fkey TO scheduler_queue_scheduled_url_fkey;
    END IF;
END $$;

ALTER INDEX IF EXISTS public.idx_scheduler_queue_frontline_host
    RENAME TO idx_scheduler_queue_runnable_host;
ALTER INDEX IF EXISTS public.idx_scheduler_queue_frontline_ready
    RENAME TO idx_scheduler_queue_runnable_ready;
ALTER INDEX IF EXISTS public.idx_scheduler_queue_frontline_branch
    RENAME TO idx_scheduler_queue_runnable_branch;

ALTER INDEX IF EXISTS public.idx_scheduler_queue_deferred_host
    RENAME TO idx_scheduler_queue_scheduled_host;
ALTER INDEX IF EXISTS public.idx_scheduler_queue_deferred_ready
    RENAME TO idx_scheduler_queue_scheduled_ready;
ALTER INDEX IF EXISTS public.idx_scheduler_queue_deferred_branch
    RENAME TO idx_scheduler_queue_scheduled_branch;

UPDATE public.active_leases
SET physical_queue = CASE physical_queue
    WHEN 'exploration' THEN 'runnable'
    WHEN 'frontline' THEN 'runnable'
    WHEN 'backlog' THEN 'scheduled'
    WHEN 'deferred' THEN 'scheduled'
    ELSE physical_queue
END
WHERE physical_queue IN ('exploration', 'frontline', 'backlog', 'deferred');

UPDATE public.scheduler_queue_retry_quarantine
SET physical_queue = CASE physical_queue
    WHEN 'exploration' THEN 'runnable'
    WHEN 'frontline' THEN 'runnable'
    WHEN 'backlog' THEN 'scheduled'
    WHEN 'deferred' THEN 'scheduled'
    ELSE physical_queue
END
WHERE physical_queue IN ('exploration', 'frontline', 'backlog', 'deferred');

UPDATE public.host_runnable_heads
SET physical_queue = CASE physical_queue
    WHEN 'exploration' THEN 'runnable'
    WHEN 'frontline' THEN 'runnable'
    WHEN 'backlog' THEN 'scheduled'
    WHEN 'deferred' THEN 'scheduled'
    ELSE physical_queue
END
WHERE physical_queue IN ('exploration', 'frontline', 'backlog', 'deferred');
