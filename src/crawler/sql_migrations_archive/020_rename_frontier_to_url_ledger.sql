DO $$
BEGIN
    IF to_regclass('public.frontier') IS NOT NULL
       AND to_regclass('public.url_ledger') IS NULL THEN
        ALTER TABLE public.frontier RENAME TO url_ledger;
    END IF;
END $$;

ALTER INDEX IF EXISTS frontier_pkey
    RENAME TO url_ledger_pkey;
ALTER INDEX IF EXISTS idx_frontier_domain
    RENAME TO idx_url_ledger_domain;
