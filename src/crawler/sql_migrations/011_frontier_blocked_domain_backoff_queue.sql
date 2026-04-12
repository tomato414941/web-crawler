CREATE TABLE IF NOT EXISTS frontier_queue_blocked_domain_backoff (
    url TEXT PRIMARY KEY REFERENCES frontier(url) ON DELETE CASCADE,
    domain TEXT NOT NULL,
    queue_class TEXT NOT NULL,
    priority REAL NOT NULL DEFAULT 1.0,
    next_fetch_at DOUBLE PRECISION NOT NULL DEFAULT 0,
    added_at DOUBLE PRECISION NOT NULL DEFAULT 0,
    branch_key TEXT NOT NULL DEFAULT '/'
);

CREATE INDEX IF NOT EXISTS idx_frontier_queue_blocked_domain_backoff_domain
    ON frontier_queue_blocked_domain_backoff(domain);
CREATE INDEX IF NOT EXISTS idx_frontier_queue_blocked_domain_backoff_queue_class
    ON frontier_queue_blocked_domain_backoff(queue_class);
CREATE INDEX IF NOT EXISTS idx_frontier_queue_blocked_domain_backoff_branch
    ON frontier_queue_blocked_domain_backoff(domain, branch_key);

INSERT INTO frontier_queue_blocked_domain_backoff (
    url,
    domain,
    queue_class,
    priority,
    next_fetch_at,
    added_at,
    branch_key
)
SELECT queue.url, queue.domain, 'exploration', queue.priority, queue.next_fetch_at, queue.added_at, queue.branch_key
FROM frontier_queue_exploration AS queue
JOIN domain_state ON domain_state.host_key = queue.domain
WHERE domain_state.backoff_until > EXTRACT(EPOCH FROM NOW())
ON CONFLICT (url) DO UPDATE
SET domain = EXCLUDED.domain,
    queue_class = EXCLUDED.queue_class,
    priority = EXCLUDED.priority,
    next_fetch_at = EXCLUDED.next_fetch_at,
    added_at = EXCLUDED.added_at,
    branch_key = EXCLUDED.branch_key;

INSERT INTO frontier_queue_blocked_domain_backoff (
    url,
    domain,
    queue_class,
    priority,
    next_fetch_at,
    added_at,
    branch_key
)
SELECT queue.url, queue.domain, 'backlog', queue.priority, queue.next_fetch_at, queue.added_at, queue.branch_key
FROM frontier_queue_backlog AS queue
JOIN domain_state ON domain_state.host_key = queue.domain
WHERE domain_state.backoff_until > EXTRACT(EPOCH FROM NOW())
ON CONFLICT (url) DO UPDATE
SET domain = EXCLUDED.domain,
    queue_class = EXCLUDED.queue_class,
    priority = EXCLUDED.priority,
    next_fetch_at = EXCLUDED.next_fetch_at,
    added_at = EXCLUDED.added_at,
    branch_key = EXCLUDED.branch_key;

INSERT INTO frontier_queue_blocked_domain_backoff (
    url,
    domain,
    queue_class,
    priority,
    next_fetch_at,
    added_at,
    branch_key
)
SELECT queue.url, queue.domain, 'recrawl', queue.priority, queue.next_fetch_at, queue.added_at, queue.branch_key
FROM frontier_queue_recrawl AS queue
JOIN domain_state ON domain_state.host_key = queue.domain
WHERE domain_state.backoff_until > EXTRACT(EPOCH FROM NOW())
ON CONFLICT (url) DO UPDATE
SET domain = EXCLUDED.domain,
    queue_class = EXCLUDED.queue_class,
    priority = EXCLUDED.priority,
    next_fetch_at = EXCLUDED.next_fetch_at,
    added_at = EXCLUDED.added_at,
    branch_key = EXCLUDED.branch_key;

DELETE FROM frontier_queue_exploration AS queue
USING domain_state
WHERE domain_state.host_key = queue.domain
  AND domain_state.backoff_until > EXTRACT(EPOCH FROM NOW());

DELETE FROM frontier_queue_backlog AS queue
USING domain_state
WHERE domain_state.host_key = queue.domain
  AND domain_state.backoff_until > EXTRACT(EPOCH FROM NOW());

DELETE FROM frontier_queue_recrawl AS queue
USING domain_state
WHERE domain_state.host_key = queue.domain
  AND domain_state.backoff_until > EXTRACT(EPOCH FROM NOW());
