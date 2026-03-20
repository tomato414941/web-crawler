"""Postgres storage for crawl results."""

from collections.abc import Mapping
import hashlib
import logging
import re
import time
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras

from .result import CrawlResult, result_to_dict

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


_TITLE_PATTERN = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)


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

    def _finish_read(self) -> None:
        """Close read-only transactions so API requests do not hold relation locks."""
        self._conn.commit()

    def save(self, result: CrawlResult | Mapping[str, object]) -> bool:
        """Save a single crawl result. Returns True if inserted."""
        data = result_to_dict(result)
        if data.get("error"):
            return False

        url = data["url"]
        url_hash = _url_hash(url)
        domain = urlparse(url).netloc

        title = None
        content = data.get("content", "")
        if content:
            m = _TITLE_PATTERN.search(content)
            if m:
                title = m.group(1).strip()[:500]

        outlinks = data.get("outlinks", [])

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
                        data.get("status"),
                        data.get("content_length"),
                        data.get("depth"),
                        data.get("source_url"),
                        outlinks,
                        data.get("timestamp", time.time()),
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

    @property
    def conn(self):
        """Expose connection for frontier (which shares the same Postgres)."""
        return self._conn

    def list_pages(
        self,
        since: float = 0,
        limit: int = 100,
        offset: int = 0,
        domain: str | None = None,
    ) -> list[dict]:
        """List crawled pages with optional filters."""
        conditions = ["crawled_at > %s"]
        params: list = [since]

        if domain:
            conditions.append("domain = %s")
            params.append(domain)

        where = " AND ".join(conditions)
        params.extend([limit, offset])

        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""SELECT url_hash, url, domain, title, status, content_length,
                               outlinks, crawled_at
                        FROM pages WHERE {where}
                        ORDER BY crawled_at ASC
                        LIMIT %s OFFSET %s""",
                    params,
                )
                pages = [dict(row) for row in cur.fetchall()]
            self._finish_read()
            return pages
        except Exception:
            self._conn.rollback()
            raise

    def get_page(self, url_hash: str) -> dict | None:
        """Get a single page with full content."""
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """SELECT url_hash, url, domain, title, content, status,
                              content_length, depth, source_url, outlinks, crawled_at
                       FROM pages WHERE url_hash = %s""",
                    (url_hash,),
                )
                row = cur.fetchone()
            self._finish_read()
            return dict(row) if row else None
        except Exception:
            self._conn.rollback()
            raise

    def get_stats(self) -> dict:
        """Get crawl statistics."""
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """SELECT
                         count(*) as total_pages,
                         count(DISTINCT domain) as domains,
                         min(crawled_at) as oldest,
                         max(crawled_at) as newest,
                         sum(content_length) as total_bytes
                       FROM pages"""
                )
                row = cur.fetchone()

                cur.execute("SELECT to_regclass('public.frontier')")
                frontier_exists = cur.fetchone()[0] is not None

                frontier_status: dict[str, int] = {}
                discovery_kinds: dict[str, int] = {}
                archetypes: dict[str, int] = {}
                top_pending_domains: list[dict[str, object]] = []
                if frontier_exists:
                    cur.execute("SELECT status, COUNT(*) FROM frontier GROUP BY status")
                    frontier_status = {status: count for status, count in cur.fetchall()}

                    cur.execute(
                        """SELECT discovery_kind, COUNT(*)
                           FROM frontier
                           GROUP BY discovery_kind"""
                    )
                    discovery_kinds = {kind: count for kind, count in cur.fetchall()}

                    cur.execute(
                        """SELECT archetype, COUNT(*)
                           FROM frontier
                           GROUP BY archetype"""
                    )
                    archetypes = {archetype: count for archetype, count in cur.fetchall()}

                    cur.execute(
                        """SELECT domain, COUNT(*)
                           FROM frontier
                           WHERE status = 'pending'
                           GROUP BY domain
                           ORDER BY COUNT(*) DESC, domain ASC
                           LIMIT 10"""
                    )
                    top_pending_domains = [
                        {"domain": domain, "count": count}
                        for domain, count in cur.fetchall()
                    ]

                cur.execute(
                    """SELECT domain, COUNT(*)
                       FROM pages
                       GROUP BY domain
                       ORDER BY COUNT(*) DESC, domain ASC
                       LIMIT 10"""
                )
                top_page_domains = [
                    {"domain": domain, "count": count}
                    for domain, count in cur.fetchall()
                ]
            self._finish_read()
        except Exception:
            self._conn.rollback()
            raise

        return {
            "total_pages": row[0],
            "domains": row[1],
            "oldest_crawl": row[2],
            "newest_crawl": row[3],
            "total_bytes": row[4],
            "frontier_status": frontier_status,
            "discovery_kinds": discovery_kinds,
            "archetypes": archetypes,
            "top_page_domains": top_page_domains,
            "top_pending_domains": top_pending_domains,
        }

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
