DO $$
BEGIN
    IF to_regclass('public.scheduler_queue_frontline') IS NULL
       AND to_regclass('public.frontier_queue_exploration') IS NOT NULL THEN
        ALTER TABLE public.frontier_queue_exploration RENAME TO scheduler_queue_frontline;
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('public.scheduler_queue_deferred') IS NULL
       AND to_regclass('public.frontier_queue_backlog') IS NOT NULL THEN
        ALTER TABLE public.frontier_queue_backlog RENAME TO scheduler_queue_deferred;
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('public.scheduler_queue_refresh') IS NULL
       AND to_regclass('public.frontier_queue_recrawl') IS NOT NULL THEN
        ALTER TABLE public.frontier_queue_recrawl RENAME TO scheduler_queue_refresh;
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('public.scheduler_queue_retry_quarantine') IS NULL
       AND to_regclass('public.frontier_queue_blocked_domain_backoff') IS NOT NULL THEN
        ALTER TABLE public.frontier_queue_blocked_domain_backoff RENAME TO scheduler_queue_retry_quarantine;
    END IF;
END $$;

ALTER INDEX IF EXISTS idx_frontier_queue_exploration_domain
    RENAME TO idx_scheduler_queue_frontline_domain;
ALTER INDEX IF EXISTS idx_frontier_queue_backlog_domain
    RENAME TO idx_scheduler_queue_deferred_domain;
ALTER INDEX IF EXISTS idx_frontier_queue_recrawl_domain
    RENAME TO idx_scheduler_queue_refresh_domain;

ALTER INDEX IF EXISTS idx_frontier_queue_exploration_ready
    RENAME TO idx_scheduler_queue_frontline_ready;
ALTER INDEX IF EXISTS idx_frontier_queue_backlog_ready
    RENAME TO idx_scheduler_queue_deferred_ready;
ALTER INDEX IF EXISTS idx_frontier_queue_recrawl_ready
    RENAME TO idx_scheduler_queue_refresh_ready;

ALTER INDEX IF EXISTS idx_frontier_queue_exploration_branch
    RENAME TO idx_scheduler_queue_frontline_branch;
ALTER INDEX IF EXISTS idx_frontier_queue_backlog_branch
    RENAME TO idx_scheduler_queue_deferred_branch;
ALTER INDEX IF EXISTS idx_frontier_queue_recrawl_branch
    RENAME TO idx_scheduler_queue_refresh_branch;

ALTER INDEX IF EXISTS idx_frontier_queue_blocked_domain_backoff_domain
    RENAME TO idx_scheduler_queue_retry_quarantine_domain;
ALTER INDEX IF EXISTS idx_frontier_queue_blocked_domain_backoff_queue_class
    RENAME TO idx_scheduler_queue_retry_quarantine_queue_class;
ALTER INDEX IF EXISTS idx_frontier_queue_blocked_domain_backoff_branch
    RENAME TO idx_scheduler_queue_retry_quarantine_branch;
ALTER INDEX IF EXISTS idx_frontier_queue_blocked_domain_backoff_quarantined_at
    RENAME TO idx_scheduler_queue_retry_quarantine_quarantined_at;
