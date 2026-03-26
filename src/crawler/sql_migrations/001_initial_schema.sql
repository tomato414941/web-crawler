CREATE TABLE IF NOT EXISTS pages (
    url_hash TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    domain TEXT NOT NULL,
    title TEXT,
    content TEXT,
    status INTEGER,
    content_length INTEGER,
    depth INTEGER,
    source_url TEXT,
    outlinks TEXT[],
    crawled_at DOUBLE PRECISION NOT NULL,
    created_at DOUBLE PRECISION NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())
);

CREATE INDEX IF NOT EXISTS idx_pages_domain ON pages(domain);
CREATE INDEX IF NOT EXISTS idx_pages_crawled_at ON pages(crawled_at);

CREATE TABLE IF NOT EXISTS frontier (
    url TEXT PRIMARY KEY,
    domain TEXT NOT NULL,
    depth INTEGER NOT NULL,
    priority REAL NOT NULL DEFAULT 1.0,
    discovery_kind TEXT NOT NULL DEFAULT 'seed',
    archetype TEXT NOT NULL DEFAULT 'generic_page',
    source_url TEXT,
    added_at DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    next_fetch_at DOUBLE PRECISION NOT NULL DEFAULT 0,
    last_success_at DOUBLE PRECISION,
    fail_streak INTEGER NOT NULL DEFAULT 0,
    lease_token TEXT,
    lease_expires_at DOUBLE PRECISION,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_frontier_status ON frontier(status);
CREATE INDEX IF NOT EXISTS idx_frontier_domain ON frontier(domain);
CREATE INDEX IF NOT EXISTS idx_frontier_pending
    ON frontier(priority DESC, next_fetch_at ASC, added_at ASC) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_frontier_pending_domain
    ON frontier(domain) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_frontier_leased_expiry
    ON frontier(lease_expires_at) WHERE status = 'leased';

CREATE TABLE IF NOT EXISTS domain_state (
    host_key TEXT PRIMARY KEY,
    crawl_delay_seconds DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    next_request_at DOUBLE PRECISION NOT NULL DEFAULT 0,
    backoff_until DOUBLE PRECISION NOT NULL DEFAULT 0,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    robots_checked_at DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_domain_state_next_request_at
    ON domain_state(next_request_at);
CREATE INDEX IF NOT EXISTS idx_domain_state_backoff_until
    ON domain_state(backoff_until);
