DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'frontier_lease_active_pkey'
    ) THEN
        ALTER TABLE public.active_leases
            RENAME CONSTRAINT frontier_lease_active_pkey TO active_leases_pkey;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'frontier_lease_active_url_fkey'
    ) THEN
        ALTER TABLE public.active_leases
            RENAME CONSTRAINT frontier_lease_active_url_fkey TO active_leases_url_fkey;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'frontier_queue_exploration_pkey'
    ) THEN
        ALTER TABLE public.scheduler_queue_frontline
            RENAME CONSTRAINT frontier_queue_exploration_pkey TO scheduler_queue_frontline_pkey;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'frontier_queue_exploration_url_fkey'
    ) THEN
        ALTER TABLE public.scheduler_queue_frontline
            RENAME CONSTRAINT frontier_queue_exploration_url_fkey TO scheduler_queue_frontline_url_fkey;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'frontier_queue_backlog_pkey'
    ) THEN
        ALTER TABLE public.scheduler_queue_deferred
            RENAME CONSTRAINT frontier_queue_backlog_pkey TO scheduler_queue_deferred_pkey;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'frontier_queue_backlog_url_fkey'
    ) THEN
        ALTER TABLE public.scheduler_queue_deferred
            RENAME CONSTRAINT frontier_queue_backlog_url_fkey TO scheduler_queue_deferred_url_fkey;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'frontier_queue_recrawl_pkey'
    ) THEN
        ALTER TABLE public.scheduler_queue_refresh
            RENAME CONSTRAINT frontier_queue_recrawl_pkey TO scheduler_queue_refresh_pkey;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'frontier_queue_recrawl_url_fkey'
    ) THEN
        ALTER TABLE public.scheduler_queue_refresh
            RENAME CONSTRAINT frontier_queue_recrawl_url_fkey TO scheduler_queue_refresh_url_fkey;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'frontier_queue_blocked_domain_backoff_pkey'
    ) THEN
        ALTER TABLE public.scheduler_queue_retry_quarantine
            RENAME CONSTRAINT frontier_queue_blocked_domain_backoff_pkey TO scheduler_queue_retry_quarantine_pkey;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'frontier_queue_blocked_domain_backoff_url_fkey'
    ) THEN
        ALTER TABLE public.scheduler_queue_retry_quarantine
            RENAME CONSTRAINT frontier_queue_blocked_domain_backoff_url_fkey TO scheduler_queue_retry_quarantine_url_fkey;
    END IF;
END $$;
