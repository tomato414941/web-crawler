ALTER TABLE url_ledger
    ADD COLUMN IF NOT EXISTS queue_class TEXT;

UPDATE url_ledger
SET queue_class = CASE
    WHEN status = 'done' THEN 'recrawl'
    ELSE 'backlog'
END
WHERE queue_class IS NULL;

ALTER TABLE url_ledger
    ALTER COLUMN queue_class SET DEFAULT 'backlog';

ALTER TABLE url_ledger
    ALTER COLUMN queue_class SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_url_ledger_pending_queue_class
    ON url_ledger(queue_class, priority DESC, next_fetch_at ASC, added_at ASC)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_url_ledger_pending_queue_domain
    ON url_ledger(queue_class, domain)
    WHERE status = 'pending';
