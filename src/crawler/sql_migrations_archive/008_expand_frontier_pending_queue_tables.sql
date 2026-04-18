ALTER TABLE frontier_queue_exploration
    ADD COLUMN IF NOT EXISTS priority REAL NOT NULL DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS next_fetch_at DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS added_at DOUBLE PRECISION NOT NULL DEFAULT 0;

ALTER TABLE frontier_queue_backlog
    ADD COLUMN IF NOT EXISTS priority REAL NOT NULL DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS next_fetch_at DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS added_at DOUBLE PRECISION NOT NULL DEFAULT 0;

ALTER TABLE frontier_queue_recrawl
    ADD COLUMN IF NOT EXISTS priority REAL NOT NULL DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS next_fetch_at DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS added_at DOUBLE PRECISION NOT NULL DEFAULT 0;

UPDATE frontier_queue_exploration AS queue
SET priority = url_ledger.priority,
    next_fetch_at = url_ledger.next_fetch_at,
    added_at = url_ledger.added_at
FROM url_ledger
WHERE url_ledger.url = queue.url;

UPDATE frontier_queue_backlog AS queue
SET priority = url_ledger.priority,
    next_fetch_at = url_ledger.next_fetch_at,
    added_at = url_ledger.added_at
FROM url_ledger
WHERE url_ledger.url = queue.url;

UPDATE frontier_queue_recrawl AS queue
SET priority = url_ledger.priority,
    next_fetch_at = url_ledger.next_fetch_at,
    added_at = url_ledger.added_at
FROM url_ledger
WHERE url_ledger.url = queue.url;

CREATE INDEX IF NOT EXISTS idx_frontier_queue_exploration_ready
    ON frontier_queue_exploration(priority DESC, next_fetch_at ASC, added_at ASC);
CREATE INDEX IF NOT EXISTS idx_frontier_queue_backlog_ready
    ON frontier_queue_backlog(priority DESC, next_fetch_at ASC, added_at ASC);
CREATE INDEX IF NOT EXISTS idx_frontier_queue_recrawl_ready
    ON frontier_queue_recrawl(priority DESC, next_fetch_at ASC, added_at ASC);
