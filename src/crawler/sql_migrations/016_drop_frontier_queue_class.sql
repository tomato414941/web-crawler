DROP INDEX IF EXISTS idx_frontier_pending_queue_class;
DROP INDEX IF EXISTS idx_frontier_pending_queue_domain;

ALTER TABLE frontier
    DROP COLUMN IF EXISTS queue_class;
