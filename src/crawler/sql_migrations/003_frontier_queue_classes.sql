ALTER TABLE frontier
    ADD COLUMN IF NOT EXISTS queue_class TEXT;

UPDATE frontier
SET queue_class = CASE
    WHEN status = 'done' THEN 'recrawl'
    WHEN discovery_kind = 'seed' THEN 'exploration'
    WHEN depth <= 1 THEN 'exploration'
    ELSE 'backlog'
END
WHERE queue_class IS NULL;

ALTER TABLE frontier
    ALTER COLUMN queue_class SET DEFAULT 'backlog';

ALTER TABLE frontier
    ALTER COLUMN queue_class SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_frontier_pending_queue_class
    ON frontier(queue_class, priority DESC, next_fetch_at ASC, added_at ASC)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_frontier_pending_queue_domain
    ON frontier(queue_class, domain)
    WHERE status = 'pending';
