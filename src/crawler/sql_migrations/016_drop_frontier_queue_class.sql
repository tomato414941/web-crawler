DROP INDEX IF EXISTS idx_url_ledger_pending_queue_class;
DROP INDEX IF EXISTS idx_url_ledger_pending_queue_domain;

ALTER TABLE url_ledger
    DROP COLUMN IF EXISTS queue_class;
