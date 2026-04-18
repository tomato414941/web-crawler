DO $$
BEGIN
    IF to_regclass('public.active_leases') IS NULL
       AND to_regclass('public.frontier_lease_active') IS NOT NULL THEN
        ALTER TABLE public.frontier_lease_active RENAME TO active_leases;
    END IF;
END $$;

ALTER INDEX IF EXISTS idx_frontier_lease_active_domain
    RENAME TO idx_active_leases_domain;
ALTER INDEX IF EXISTS idx_frontier_lease_active_queue_class
    RENAME TO idx_active_leases_queue_class;
ALTER INDEX IF EXISTS idx_frontier_lease_active_expiry
    RENAME TO idx_active_leases_expiry;
