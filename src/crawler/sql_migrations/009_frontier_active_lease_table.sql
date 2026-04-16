CREATE TABLE IF NOT EXISTS frontier_lease_active (
    url TEXT PRIMARY KEY REFERENCES url_ledger(url) ON DELETE CASCADE,
    domain TEXT NOT NULL,
    queue_class TEXT NOT NULL,
    lease_token TEXT NOT NULL,
    lease_expires_at DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_frontier_lease_active_domain
    ON frontier_lease_active(domain);
CREATE INDEX IF NOT EXISTS idx_frontier_lease_active_queue_class
    ON frontier_lease_active(queue_class);
CREATE INDEX IF NOT EXISTS idx_frontier_lease_active_expiry
    ON frontier_lease_active(lease_expires_at);

DELETE FROM frontier_lease_active;

INSERT INTO frontier_lease_active (url, domain, queue_class, lease_token, lease_expires_at)
SELECT url, domain, queue_class, lease_token, lease_expires_at
FROM url_ledger
WHERE status = 'leased'
  AND lease_token IS NOT NULL
  AND lease_expires_at IS NOT NULL
ON CONFLICT (url) DO UPDATE
SET domain = EXCLUDED.domain,
    queue_class = EXCLUDED.queue_class,
    lease_token = EXCLUDED.lease_token,
    lease_expires_at = EXCLUDED.lease_expires_at;
