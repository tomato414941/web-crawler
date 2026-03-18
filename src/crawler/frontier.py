"""URL Frontier with PostgreSQL persistence."""

import logging
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import psycopg2.extras

from .urls import normalize_url

logger = logging.getLogger(__name__)

FRONTIER_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS frontier (
    url TEXT PRIMARY KEY,
    domain TEXT NOT NULL,
    depth INTEGER NOT NULL,
    priority REAL NOT NULL DEFAULT 1.0,
    source_url TEXT,
    added_at DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
);

CREATE INDEX IF NOT EXISTS idx_frontier_status ON frontier(status);
CREATE INDEX IF NOT EXISTS idx_frontier_domain ON frontier(domain);
CREATE INDEX IF NOT EXISTS idx_frontier_pending
    ON frontier(priority DESC, added_at ASC) WHERE status = 'pending';
"""


@dataclass
class CrawlTask:
    """A URL to crawl with metadata."""

    url: str
    depth: int
    priority: float = 1.0
    source_url: str | None = None
    added_at: float = 0.0

    def __post_init__(self):
        if self.added_at == 0.0:
            self.added_at = time.time()


class Frontier:
    """URL frontier with PostgreSQL persistence. Dedup via ON CONFLICT."""

    def __init__(self, conn):
        self._conn = conn
        self._init_schema()

    def _init_schema(self):
        with self._conn.cursor() as cur:
            cur.execute(FRONTIER_SCHEMA_SQL)
        self._conn.commit()
        logger.info("Frontier schema initialized")

    def add(self, task: CrawlTask) -> bool:
        """Add a URL to the frontier. Returns False if already exists."""
        normalized_url = normalize_url(task.url)
        task.url = normalized_url
        domain = urlparse(normalized_url).netloc

        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO frontier (url, domain, depth, priority, source_url, added_at)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (url) DO NOTHING""",
                    (task.url, domain, task.depth, task.priority, task.source_url, task.added_at),
                )
                inserted = cur.rowcount > 0
            self._conn.commit()
            return inserted
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to add %s", task.url)
            return False

    def add_many(self, tasks: list[CrawlTask]) -> int:
        """Add multiple URLs in a single transaction. Returns count of new URLs added."""
        if not tasks:
            return 0

        rows = []
        for task in tasks:
            normalized_url = normalize_url(task.url)
            task.url = normalized_url
            domain = urlparse(normalized_url).netloc
            rows.append((task.url, domain, task.depth, task.priority, task.source_url, task.added_at))

        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO frontier (url, domain, depth, priority, source_url, added_at)
                       VALUES %s
                       ON CONFLICT (url) DO NOTHING""",
                    rows,
                    page_size=200,
                )
                inserted = cur.rowcount
            self._conn.commit()
            return inserted
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to add batch of %d URLs", len(tasks))
            return 0

    def get_next(self, domain: str | None = None) -> CrawlTask | None:
        """Get next URL to crawl, optionally filtered by domain."""
        where = "status = 'pending'"
        params: list = []

        if domain:
            where += " AND domain = %s"
            params.append(domain)

        with self._conn.cursor() as cur:
            cur.execute(
                f"""UPDATE frontier SET status = 'processing'
                    WHERE url = (
                        SELECT url FROM frontier
                        WHERE {where}
                        ORDER BY priority DESC, added_at ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING url, depth, priority, source_url, added_at""",
                params,
            )
            row = cur.fetchone()
        self._conn.commit()

        if row:
            url, depth, priority, source_url, added_at = row
            return CrawlTask(
                url=url, depth=depth, priority=priority,
                source_url=source_url, added_at=added_at,
            )
        return None

    def get_batch(self, count: int = 10, domain: str | None = None) -> list[CrawlTask]:
        """Get a batch of URLs to crawl."""
        where = "status = 'pending'"
        params: list = []

        if domain:
            where += " AND domain = %s"
            params.append(domain)

        params.append(count)

        with self._conn.cursor() as cur:
            cur.execute(
                f"""UPDATE frontier SET status = 'processing'
                    WHERE url IN (
                        SELECT url FROM frontier
                        WHERE {where}
                        ORDER BY priority DESC, added_at ASC
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING url, depth, priority, source_url, added_at""",
                params,
            )
            rows = cur.fetchall()
        self._conn.commit()

        return [
            CrawlTask(url=url, depth=depth, priority=priority,
                       source_url=source_url, added_at=added_at)
            for url, depth, priority, source_url, added_at in rows
        ]

    def mark_done(self, url: str):
        """Mark a URL as successfully crawled."""
        with self._conn.cursor() as cur:
            cur.execute("UPDATE frontier SET status = 'done' WHERE url = %s", (url,))
        self._conn.commit()

    def mark_failed(self, url: str):
        """Mark a URL as failed."""
        with self._conn.cursor() as cur:
            cur.execute("UPDATE frontier SET status = 'failed' WHERE url = %s", (url,))
        self._conn.commit()

    def requeue_failed(self) -> int:
        """Requeue failed URLs for retry."""
        with self._conn.cursor() as cur:
            cur.execute("UPDATE frontier SET status = 'pending' WHERE status = 'failed'")
            count = cur.rowcount
        self._conn.commit()
        return count

    def stats(self) -> dict:
        """Get queue statistics."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM frontier GROUP BY status")
            stats = dict(cur.fetchall())
        stats["total"] = sum(stats.values())
        return stats

    def pending_count(self) -> int:
        """Get count of pending URLs."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM frontier WHERE status = 'pending'")
            return cur.fetchone()[0]

    def is_seen(self, url: str) -> bool:
        """Check if URL exists in frontier."""
        normalized = normalize_url(url)
        with self._conn.cursor() as cur:
            cur.execute("SELECT 1 FROM frontier WHERE url = %s LIMIT 1", (normalized,))
            return cur.fetchone() is not None
