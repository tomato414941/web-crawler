DROP INDEX IF EXISTS idx_frontier_status;
DROP INDEX IF EXISTS idx_frontier_pending;
DROP INDEX IF EXISTS idx_frontier_pending_domain;
DROP INDEX IF EXISTS idx_frontier_leased_expiry;

ALTER TABLE frontier
    DROP COLUMN IF EXISTS status;
