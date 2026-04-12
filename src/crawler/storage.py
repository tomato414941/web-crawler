"""Postgres storage for crawl results."""

from collections import Counter
from collections.abc import Mapping
import hashlib
import json
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
    "queue_class",
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

    def upsert_runtime_stats(self, component: str, payload: Mapping[str, object]) -> None:
        """Store runtime crawler stats for API consumption."""
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO crawler_runtime_stats (component, payload, updated_at)
                       VALUES (%s, %s::jsonb, %s)
                       ON CONFLICT (component) DO UPDATE SET
                           payload = EXCLUDED.payload,
                           updated_at = EXCLUDED.updated_at""",
                    (component, json.dumps(dict(payload)), time.time()),
                )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to update runtime stats for %s", component)

    def get_runtime_stats(self, component: str | None = None) -> dict[str, object]:
        """Fetch runtime crawler stats snapshots."""
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT to_regclass('public.crawler_runtime_stats')")
                exists = cur.fetchone()[0] is not None
                if not exists:
                    self._finish_read()
                    return {}

                if component is None:
                    cur.execute("SELECT component, payload, updated_at FROM crawler_runtime_stats")
                    rows = cur.fetchall()
                    self._finish_read()
                    return {
                        row["component"]: {
                            "payload": row["payload"],
                            "updated_at": row["updated_at"],
                        }
                        for row in rows
                    }

                cur.execute(
                    "SELECT payload, updated_at FROM crawler_runtime_stats WHERE component = %s",
                    (component,),
                )
                row = cur.fetchone()
            self._finish_read()
            if not row:
                return {}
            return {
                "payload": row["payload"],
                "updated_at": row["updated_at"],
            }
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
                cur.execute("SELECT to_regclass('public.frontier_lease_active')")
                lease_table_exists = cur.fetchone()[0] is not None

                frontier_status: dict[str, int] = {}
                legacy_frontier_status: dict[str, int] = {}
                queue_classes: dict[str, int] = {}
                pending_queue_classes: dict[str, int] = {}
                blocked_queue_classes: dict[str, int] = {}
                readiness: dict[str, object] = {}
                discovery_kinds: dict[str, int] = {}
                archetypes: dict[str, int] = {}
                top_pending_domains: list[dict[str, object]] = []
                top_blocked_domains: list[dict[str, object]] = []
                active_error_breakdown: dict[str, int] = {}
                top_error_domains: list[dict[str, object]] = []
                runtime: dict[str, object] = {}
                pending_queue_sql = """SELECT queue_class, url, domain, next_fetch_at FROM (
                        SELECT 'exploration' AS queue_class, url, domain, next_fetch_at FROM public.frontier_queue_exploration
                        UNION ALL
                        SELECT 'backlog' AS queue_class, url, domain, next_fetch_at FROM public.frontier_queue_backlog
                        UNION ALL
                        SELECT 'recrawl' AS queue_class, url, domain, next_fetch_at FROM public.frontier_queue_recrawl
                    ) AS pending_queue_rows"""
                blocked_queue_sql = """SELECT queue_class, url, domain, next_fetch_at
                    FROM public.frontier_queue_blocked_domain_backoff"""
                cur.execute("SELECT to_regclass('public.crawler_runtime_stats')")
                runtime_exists = cur.fetchone()[0] is not None
                if runtime_exists:
                    cur.execute(
                        "SELECT payload, updated_at FROM crawler_runtime_stats WHERE component = 'crawler'"
                    )
                    runtime_row = cur.fetchone()
                    if runtime_row:
                        runtime = {
                            "payload": runtime_row[0],
                            "updated_at": runtime_row[1],
                        }

                if frontier_exists:
                    assert_public_table_columns(
                        self._conn,
                        "frontier",
                        FRONTIER_STATS_REQUIRED_COLUMNS,
                    )

                    cur.execute("SELECT status, COUNT(*) FROM public.frontier GROUP BY status")
                    legacy_frontier_status = {status: count for status, count in cur.fetchall()}
                    frontier_status = {
                        status: count
                        for status, count in legacy_frontier_status.items()
                        if status not in {'pending', 'leased'}
                    }
                    if lease_table_exists:
                        cur.execute("SELECT COUNT(*) FROM public.frontier_lease_active")
                        frontier_status['leased'] = cur.fetchone()[0]

                    cur.execute(
                        """SELECT queue_class, COUNT(*)
                           FROM public.frontier
                           GROUP BY queue_class"""
                    )
                    queue_classes = {queue_class: count for queue_class, count in cur.fetchall()}

                    cur.execute(
                        f"""SELECT queue_class, COUNT(*)
                           FROM ({pending_queue_sql}) AS pending_queue_rows
                           GROUP BY queue_class"""
                    )
                    pending_queue_classes = {queue_class: count for queue_class, count in cur.fetchall()}
                    cur.execute(
                        f"""SELECT queue_class, COUNT(*)
                           FROM ({blocked_queue_sql}) AS blocked_queue_rows
                           GROUP BY queue_class"""
                    )
                    blocked_queue_classes = {queue_class: count for queue_class, count in cur.fetchall()}
                    frontier_status['pending'] = sum(pending_queue_classes.values()) + sum(
                        blocked_queue_classes.values()
                    )

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
                        f"""SELECT domain, COUNT(*)
                           FROM ({pending_queue_sql}) AS pending_queue_rows
                           GROUP BY domain
                           ORDER BY COUNT(*) DESC, domain ASC
                           LIMIT 10"""
                    )
                    top_pending_domains = [
                        {"domain": domain, "count": count} for domain, count in cur.fetchall()
                    ]

                    now = time.time()
                    cur.execute(
                        f"""WITH pending_entries AS (
                                {pending_queue_sql}
                            ), blocked_entries AS (
                                {blocked_queue_sql}
                            ), readiness_entries AS (
                                SELECT
                                    pending_queue_rows.next_fetch_at > %(now)s AS blocked_next_fetch,
                                    COALESCE(domain_state.next_request_at, 0) > %(now)s AS blocked_domain_next_request,
                                    COALESCE(domain_state.backoff_until, 0) > %(now)s AS blocked_domain_backoff,
                                    GREATEST(
                                        pending_queue_rows.next_fetch_at,
                                        COALESCE(domain_state.next_request_at, 0),
                                        COALESCE(domain_state.backoff_until, 0)
                                    ) AS ready_at
                                FROM pending_entries AS pending_queue_rows
                                LEFT JOIN public.domain_state ON domain_state.host_key = pending_queue_rows.domain
                                UNION ALL
                                SELECT
                                    FALSE AS blocked_next_fetch,
                                    FALSE AS blocked_domain_next_request,
                                    TRUE AS blocked_domain_backoff,
                                    NULL::DOUBLE PRECISION AS ready_at
                                FROM blocked_entries
                            )
                            SELECT
                                COUNT(*) AS pending,
                        COUNT(*) FILTER (
                            WHERE NOT blocked_next_fetch
                              AND NOT blocked_domain_next_request
                              AND NOT blocked_domain_backoff
                        ) AS ready,
                        MIN(ready_at) AS next_ready_at,
                        COUNT(*) FILTER (WHERE blocked_next_fetch) AS blocked_next_fetch,
                        COUNT(*) FILTER (WHERE blocked_domain_next_request) AS blocked_domain_next_request,
                        COUNT(*) FILTER (WHERE blocked_domain_backoff) AS blocked_domain_backoff,
                        COUNT(*) FILTER (WHERE blocked_domain_backoff) AS state_blocked_domain_backoff,
                        COUNT(*) FILTER (
                            WHERE NOT blocked_domain_backoff
                              AND blocked_domain_next_request
                        ) AS state_blocked_domain_next_request,
                        COUNT(*) FILTER (
                            WHERE NOT blocked_domain_backoff
                              AND NOT blocked_domain_next_request
                              AND blocked_next_fetch
                        ) AS state_scheduled,
                        COUNT(*) FILTER (
                            WHERE NOT blocked_domain_backoff
                              AND NOT blocked_domain_next_request
                              AND NOT blocked_next_fetch
                        ) AS state_ready
                            FROM readiness_entries""",
                        {"now": now},
                    )
                    (
                        readiness_pending,
                        readiness_ready,
                        next_ready_at,
                        blocked_next_fetch,
                        blocked_domain_next_request,
                        blocked_domain_backoff,
                        state_blocked_domain_backoff,
                        state_blocked_domain_next_request,
                        state_scheduled,
                        state_ready,
                    ) = cur.fetchone()
                    readiness = {
                        "pending": readiness_pending or 0,
                        "ready": readiness_ready or 0,
                        "next_ready_delay": (
                            None if next_ready_at is None else max(0.0, next_ready_at - now)
                        ),
                        "blocked": {
                            "next_fetch_at": blocked_next_fetch or 0,
                            "domain_next_request": blocked_domain_next_request or 0,
                            "domain_backoff": blocked_domain_backoff or 0,
                        },
                        "state_counts": {
                            "ready": state_ready or 0,
                            "scheduled": state_scheduled or 0,
                            "blocked_domain_next_request": state_blocked_domain_next_request or 0,
                            "blocked_domain_backoff": state_blocked_domain_backoff or 0,
                        },
                    }

                    cur.execute(
                        f"""WITH pending_entries AS (
                                SELECT queue_class, url, domain, next_fetch_at, FALSE AS forced_blocked_domain_backoff
                                FROM ({pending_queue_sql}) AS pending_queue_rows
                                UNION ALL
                                SELECT queue_class, url, domain, next_fetch_at, TRUE AS forced_blocked_domain_backoff
                                FROM ({blocked_queue_sql}) AS blocked_queue_rows
                            )
                            SELECT
                                pending_queue_rows.domain,
                                COUNT(*) AS pending_count,
                                COUNT(*) FILTER (
                                    WHERE COALESCE(domain_state.next_request_at, 0) > %(now)s
                                ) AS blocked_domain_next_request,
                                COUNT(*) FILTER (
                                    WHERE pending_queue_rows.forced_blocked_domain_backoff
                                       OR COALESCE(domain_state.backoff_until, 0) > %(now)s
                                ) AS blocked_domain_backoff,
                                GREATEST(
                                    MAX(COALESCE(domain_state.next_request_at, 0)) - %(now)s,
                                    0
                                ) AS next_request_wait_seconds,
                                GREATEST(
                                    MAX(COALESCE(domain_state.backoff_until, 0)) - %(now)s,
                                    0
                                ) AS backoff_wait_seconds,
                                COALESCE(MAX(domain_state.consecutive_failures), 0) AS consecutive_failures
                            FROM pending_entries AS pending_queue_rows
                            LEFT JOIN public.domain_state ON domain_state.host_key = pending_queue_rows.domain
                            WHERE pending_queue_rows.forced_blocked_domain_backoff
                               OR COALESCE(domain_state.next_request_at, 0) > %(now)s
                               OR COALESCE(domain_state.backoff_until, 0) > %(now)s
                            GROUP BY pending_queue_rows.domain
                            ORDER BY
                                COUNT(*) FILTER (
                                    WHERE pending_queue_rows.forced_blocked_domain_backoff
                                       OR COALESCE(domain_state.backoff_until, 0) > %(now)s
                                ) DESC,
                                GREATEST(
                                    MAX(COALESCE(domain_state.backoff_until, 0)) - %(now)s,
                                    0
                                ) DESC,
                                COUNT(*) DESC,
                                pending_queue_rows.domain ASC
                            LIMIT 10""",
                        {"now": now},
                    )
                    top_blocked_domains = []
                    for blocked_row in cur.fetchall():
                        (
                            domain,
                            pending_count,
                            blocked_next_request_count,
                            blocked_backoff_count,
                            next_request_wait_seconds,
                            backoff_wait_seconds,
                            consecutive_failures,
                        ) = blocked_row
                        dominant_reason = "domain_backoff"
                        wait_seconds = backoff_wait_seconds
                        if blocked_backoff_count == 0 and blocked_next_request_count > 0:
                            dominant_reason = "domain_next_request"
                            wait_seconds = next_request_wait_seconds
                        top_blocked_domains.append(
                            {
                                "domain": domain,
                                "pending_count": pending_count,
                                "blocked_counts": {
                                    "domain_next_request": blocked_next_request_count,
                                    "domain_backoff": blocked_backoff_count,
                                },
                                "wait_seconds": round(wait_seconds, 3),
                                "dominant_reason": dominant_reason,
                                "consecutive_failures": consecutive_failures,
                            }
                        )

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
                        {"domain": domain, "count": count} for domain, count in cur.fetchall()
                    ]

                cur.execute(
                    """SELECT domain, COUNT(*)
                       FROM pages
                       GROUP BY domain
                       ORDER BY COUNT(*) DESC, domain ASC
                       LIMIT 10"""
                )
                top_page_domains = [
                    {"domain": domain, "count": count} for domain, count in cur.fetchall()
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
            "legacy_frontier_status": legacy_frontier_status,
            "queue_classes": queue_classes,
            "pending_queue_classes": pending_queue_classes,
            "blocked_queue_classes": blocked_queue_classes,
            "readiness": readiness,
            "discovery_kinds": discovery_kinds,
            "archetypes": archetypes,
            "top_page_domains": top_page_domains,
            "top_pending_domains": top_pending_domains,
            "top_blocked_domains": top_blocked_domains,
            "active_error_breakdown": active_error_breakdown,
            "top_error_domains": top_error_domains,
            "runtime": runtime,
        }

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
