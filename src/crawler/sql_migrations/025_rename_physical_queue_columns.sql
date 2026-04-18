DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'scheduler_queue_retry_quarantine'
          AND column_name = 'queue_class'
    ) THEN
        ALTER TABLE public.scheduler_queue_retry_quarantine
            RENAME COLUMN queue_class TO physical_queue;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'active_leases'
          AND column_name = 'queue_class'
    ) THEN
        ALTER TABLE public.active_leases
            RENAME COLUMN queue_class TO physical_queue;
    END IF;
END $$;

ALTER INDEX IF EXISTS public.idx_scheduler_queue_retry_quarantine_queue_class
    RENAME TO idx_scheduler_queue_retry_quarantine_physical_queue;

ALTER INDEX IF EXISTS public.idx_active_leases_queue_class
    RENAME TO idx_active_leases_physical_queue;
