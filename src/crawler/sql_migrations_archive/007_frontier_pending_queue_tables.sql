CREATE TABLE IF NOT EXISTS frontier_queue_exploration (
    url TEXT PRIMARY KEY REFERENCES url_ledger(url) ON DELETE CASCADE,
    domain TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS frontier_queue_backlog (
    url TEXT PRIMARY KEY REFERENCES url_ledger(url) ON DELETE CASCADE,
    domain TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS frontier_queue_recrawl (
    url TEXT PRIMARY KEY REFERENCES url_ledger(url) ON DELETE CASCADE,
    domain TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_frontier_queue_exploration_domain
    ON frontier_queue_exploration(domain);
CREATE INDEX IF NOT EXISTS idx_frontier_queue_backlog_domain
    ON frontier_queue_backlog(domain);
CREATE INDEX IF NOT EXISTS idx_frontier_queue_recrawl_domain
    ON frontier_queue_recrawl(domain);

DELETE FROM frontier_queue_exploration;
DELETE FROM frontier_queue_backlog;
DELETE FROM frontier_queue_recrawl;

INSERT INTO frontier_queue_exploration (url, domain)
SELECT url, domain
FROM url_ledger
WHERE status = 'pending'
  AND queue_class = 'exploration'
ON CONFLICT (url) DO UPDATE
SET domain = EXCLUDED.domain;

INSERT INTO frontier_queue_backlog (url, domain)
SELECT url, domain
FROM url_ledger
WHERE status = 'pending'
  AND queue_class = 'backlog'
ON CONFLICT (url) DO UPDATE
SET domain = EXCLUDED.domain;

INSERT INTO frontier_queue_recrawl (url, domain)
SELECT url, domain
FROM url_ledger
WHERE status = 'pending'
  AND queue_class = 'recrawl'
ON CONFLICT (url) DO UPDATE
SET domain = EXCLUDED.domain;
