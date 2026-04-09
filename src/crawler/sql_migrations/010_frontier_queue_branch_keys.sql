ALTER TABLE frontier_queue_exploration
    ADD COLUMN IF NOT EXISTS branch_key TEXT NOT NULL DEFAULT '/';

ALTER TABLE frontier_queue_backlog
    ADD COLUMN IF NOT EXISTS branch_key TEXT NOT NULL DEFAULT '/';

ALTER TABLE frontier_queue_recrawl
    ADD COLUMN IF NOT EXISTS branch_key TEXT NOT NULL DEFAULT '/';

UPDATE frontier_queue_exploration AS queue
SET branch_key = COALESCE(NULLIF('/' || array_to_string((regexp_match(frontier.url, '^https?://[^/]+/([^/?#]+)(?:/([^/?#]+))?'))[1:2], '/'), '/'), '/')
FROM frontier
WHERE frontier.url = queue.url;

UPDATE frontier_queue_backlog AS queue
SET branch_key = COALESCE(NULLIF('/' || array_to_string((regexp_match(frontier.url, '^https?://[^/]+/([^/?#]+)(?:/([^/?#]+))?'))[1:2], '/'), '/'), '/')
FROM frontier
WHERE frontier.url = queue.url;

UPDATE frontier_queue_recrawl AS queue
SET branch_key = COALESCE(NULLIF('/' || array_to_string((regexp_match(frontier.url, '^https?://[^/]+/([^/?#]+)(?:/([^/?#]+))?'))[1:2], '/'), '/'), '/')
FROM frontier
WHERE frontier.url = queue.url;

CREATE INDEX IF NOT EXISTS idx_frontier_queue_exploration_branch
    ON frontier_queue_exploration(domain, branch_key);
CREATE INDEX IF NOT EXISTS idx_frontier_queue_backlog_branch
    ON frontier_queue_backlog(domain, branch_key);
CREATE INDEX IF NOT EXISTS idx_frontier_queue_recrawl_branch
    ON frontier_queue_recrawl(domain, branch_key);
