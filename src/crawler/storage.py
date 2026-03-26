"""Postgres storage for crawl results."""

from collections import Counter
from collections.abc import Mapping
import hashlib
import logging
import re
import time
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras

from .error_stats import categorize_crawl_error
from .result import CrawlResult, result_to_dict
from .schema import assert_public_table_columns

logger = logging.getLogger(__name__)
PAGES_REQUIRED_COLUMNS = {
    "url_hash",
    "url",
    "domain",
    "title",
    "content",
    "status",
    "content_length",
    "depth",
    "source_url",
    "outlinks",
    "crawled_at",
    "created_at",
}
FRONTIER_STATS_REQUIRED_COLUMNS = {
    "status",
    "discovery_kind",
    "archetype",
    "domain",
    "last_error",
}


_TITLE_PATTERN = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)


def _url_hash(url: str) -> str:
    return hashlib.blake2b(url.encode(), digest_size=8).hexdigest()


def _sanitize_stored_content(content: object) -> str:
    """Drop content that cannot be represented safely in the TEXT storage column."""
    if not isinstance(content, str):
        return ""
    if "\x00" in content:
        return ""
    return content


class PgStorage:
    """Store crawl results in Postgres."""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = False
        assert_public_table_columns(self._conn, "pages", PAGES_REQUIRED_COLUMNS)
        self._count = 0

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
        content = _sanitize_stored_content(data.get("content", ""))
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
                active_error_breakdown: dict[str, int] = {}
                top_error_domains: list[dict[str, object]] = []
                if frontier_exists:
                    assert_public_table_columns(
                        self._conn,
                        "frontier",
                        FRONTIER_STATS_REQUIRED_COLUMNS,
                    )

                    cur.execute("SELECT status, COUNT(*) FROM public.frontier GROUP BY status")
                    frontier_status = {status: count for status, count in cur.fetchall()}

                    cur.execute(
                        """SELECT discovery_kind, COUNT(*)
                           FROM public.frontier
                           GROUP BY discovery_kind"""
                    )
                    discovery_kinds = {kind: count for kind, count in cur.fetchall()}

                    cur.execute(
                        """SELECT archetype, COUNT(*)
                           FROM public.frontier
                           GROUP BY archetype"""
                    )
                    archetypes = {archetype: count for archetype, count in cur.fetchall()}

                    cur.execute(
                        """SELECT domain, COUNT(*)
                           FROM public.frontier
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
                        """SELECT last_error, COUNT(*)
                           FROM public.frontier
                           WHERE last_error IS NOT NULL
                             AND status IN ('pending', 'failed')
                           GROUP BY last_error"""
                    )
                    error_counts = Counter()
                    for error, count in cur.fetchall():
                        category = categorize_crawl_error(error)
                        if category:
                            error_counts[category] += count
                    active_error_breakdown = {
                        category: error_counts[category]
                        for category in (
                            "http_4xx",
                            "http_5xx",
                            "timeout",
                            "connection_error",
                            "http_other",
                            "other",
                        )
                        if error_counts.get(category)
                    }

                    cur.execute(
                        """SELECT domain, COUNT(*)
                           FROM public.frontier
                           WHERE last_error IS NOT NULL
                             AND status IN ('pending', 'failed')
                           GROUP BY domain
                           ORDER BY COUNT(*) DESC, domain ASC
                           LIMIT 10"""
                    )
                    top_error_domains = [
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
            "active_error_breakdown": active_error_breakdown,
            "top_error_domains": top_error_domains,
        }

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
