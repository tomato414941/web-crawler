"""Postgres storage for crawl results."""

import hashlib
import logging
import time
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
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
"""


def _url_hash(url: str) -> str:
    return hashlib.blake2b(url.encode(), digest_size=8).hexdigest()


class PgStorage:
    """Store crawl results in Postgres."""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = False
        self._init_schema()
        self._count = 0

    def _init_schema(self):
        with self._conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        self._conn.commit()
        logger.info("Postgres storage initialized")

    def save(self, result: dict) -> bool:
        """Save a single crawl result. Returns True if inserted."""
        if result.get("error"):
            return False

        url = result["url"]
        url_hash = _url_hash(url)

        from urllib.parse import urlparse

        domain = urlparse(url).netloc

        # Extract title from content
        title = None
        content = result.get("content", "")
        if content:
            import re

            m = re.search(r"<title[^>]*>(.*?)</title>", content, re.IGNORECASE | re.DOTALL)
            if m:
                title = m.group(1).strip()[:500]

        outlinks = result.get("outlinks", [])

        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO pages (url_hash, url, domain, title, content, status,
                           content_length, depth, source_url, outlinks, crawled_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (url_hash) DO UPDATE SET
                           content = EXCLUDED.content,
                           title = EXCLUDED.title,
                           status = EXCLUDED.status,
                           content_length = EXCLUDED.content_length,
                           outlinks = EXCLUDED.outlinks,
                           crawled_at = EXCLUDED.crawled_at""",
                    (
                        url_hash,
                        url,
                        domain,
                        title,
                        content,
                        result.get("status"),
                        result.get("content_length"),
                        result.get("depth"),
                        result.get("source_url"),
                        outlinks,
                        result.get("timestamp", time.time()),
                    ),
                )
            self._conn.commit()
            self._count += 1
            return True
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to save %s", url)
            return False

    @property
    def count(self) -> int:
        return self._count

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
