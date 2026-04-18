DROP INDEX IF EXISTS idx_url_ledger_status;
DROP INDEX IF EXISTS idx_url_ledger_pending;
DROP INDEX IF EXISTS idx_url_ledger_pending_domain;
DROP INDEX IF EXISTS idx_url_ledger_leased_expiry;

ALTER TABLE url_ledger
    DROP COLUMN IF EXISTS status;
