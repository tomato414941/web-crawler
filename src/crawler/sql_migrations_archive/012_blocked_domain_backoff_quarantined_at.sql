ALTER TABLE frontier_queue_blocked_domain_backoff
    ADD COLUMN IF NOT EXISTS quarantined_at DOUBLE PRECISION NOT NULL DEFAULT 0;

UPDATE frontier_queue_blocked_domain_backoff
SET quarantined_at = CASE
    WHEN quarantined_at > 0 THEN quarantined_at
    WHEN added_at > 0 THEN added_at
    ELSE EXTRACT(EPOCH FROM NOW())
END;

ALTER TABLE frontier_queue_blocked_domain_backoff
    ALTER COLUMN quarantined_at SET DEFAULT EXTRACT(EPOCH FROM NOW());

CREATE INDEX IF NOT EXISTS idx_frontier_queue_blocked_domain_backoff_quarantined_at
    ON frontier_queue_blocked_domain_backoff(quarantined_at);
