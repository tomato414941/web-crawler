"""URL Frontier with SQLite persistence and Bloom filter for deduplication."""

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from pybloom_live import ScalableBloomFilter


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
    """URL frontier with SQLite persistence and Bloom filter deduplication."""

    def __init__(self, db_path: str | Path = ":memory:", capacity: int = 100000):
        self.db_path = str(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.seen = ScalableBloomFilter(initial_capacity=capacity, error_rate=0.001)
        self._init_db()

    def _init_db(self):
        """Initialize SQLite tables."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS queue (
                id INTEGER PRIMARY KEY,
                url TEXT UNIQUE NOT NULL,
                domain TEXT NOT NULL,
                depth INTEGER NOT NULL,
                priority REAL NOT NULL,
                source_url TEXT,
                added_at REAL NOT NULL,
                status TEXT DEFAULT 'pending'
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON queue(status)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_domain ON queue(domain)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_priority ON queue(priority DESC)")
        self.conn.commit()

    def add(self, task: CrawlTask) -> bool:
        """Add a URL to the frontier. Returns False if already seen."""
        if task.url in self.seen:
            return False

        self.seen.add(task.url)
        domain = urlparse(task.url).netloc

        try:
            self.conn.execute(
                """INSERT INTO queue (url, domain, depth, priority, source_url, added_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (task.url, domain, task.depth, task.priority, task.source_url, task.added_at)
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def add_many(self, tasks: list[CrawlTask]) -> int:
        """Add multiple URLs. Returns count of new URLs added."""
        added = 0
        for task in tasks:
            if self.add(task):
                added += 1
        return added

    def get_next(self, domain: str | None = None) -> CrawlTask | None:
        """Get next URL to crawl, optionally filtered by domain."""
        query = "SELECT url, depth, priority, source_url, added_at FROM queue WHERE status = 'pending'"
        params = []

        if domain:
            query += " AND domain = ?"
            params.append(domain)

        query += " ORDER BY priority DESC, added_at ASC LIMIT 1"

        cursor = self.conn.execute(query, params)
        row = cursor.fetchone()

        if row:
            url, depth, priority, source_url, added_at = row
            self.conn.execute("UPDATE queue SET status = 'processing' WHERE url = ?", (url,))
            self.conn.commit()
            return CrawlTask(
                url=url,
                depth=depth,
                priority=priority,
                source_url=source_url,
                added_at=added_at,
            )

        return None

    def get_batch(self, count: int = 10, domain: str | None = None) -> list[CrawlTask]:
        """Get a batch of URLs to crawl."""
        query = "SELECT url, depth, priority, source_url, added_at FROM queue WHERE status = 'pending'"
        params = []

        if domain:
            query += " AND domain = ?"
            params.append(domain)

        query += " ORDER BY priority DESC, added_at ASC LIMIT ?"
        params.append(count)

        cursor = self.conn.execute(query, params)
        rows = cursor.fetchall()

        tasks = []
        for url, depth, priority, source_url, added_at in rows:
            self.conn.execute("UPDATE queue SET status = 'processing' WHERE url = ?", (url,))
            tasks.append(CrawlTask(
                url=url,
                depth=depth,
                priority=priority,
                source_url=source_url,
                added_at=added_at,
            ))

        self.conn.commit()
        return tasks

    def mark_done(self, url: str):
        """Mark a URL as successfully crawled."""
        self.conn.execute("UPDATE queue SET status = 'done' WHERE url = ?", (url,))
        self.conn.commit()

    def mark_failed(self, url: str):
        """Mark a URL as failed."""
        self.conn.execute("UPDATE queue SET status = 'failed' WHERE url = ?", (url,))
        self.conn.commit()

    def requeue_failed(self) -> int:
        """Requeue failed URLs for retry."""
        cursor = self.conn.execute(
            "UPDATE queue SET status = 'pending' WHERE status = 'failed'"
        )
        self.conn.commit()
        return cursor.rowcount

    def stats(self) -> dict:
        """Get queue statistics."""
        cursor = self.conn.execute(
            "SELECT status, COUNT(*) FROM queue GROUP BY status"
        )
        stats = dict(cursor.fetchall())
        stats['total'] = sum(stats.values())
        return stats

    def pending_count(self) -> int:
        """Get count of pending URLs."""
        cursor = self.conn.execute("SELECT COUNT(*) FROM queue WHERE status = 'pending'")
        return cursor.fetchone()[0]

    def is_seen(self, url: str) -> bool:
        """Check if URL was already seen."""
        return url in self.seen

    def close(self):
        """Close database connection."""
        self.conn.close()
