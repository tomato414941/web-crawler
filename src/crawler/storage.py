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
from .domain_manager import compute_host_budget
from .frontier import (
    BLOCKED_DOMAIN_BACKOFF_TABLE,
    LEASE_TABLE,
    QUEUE_CLASS_ORDER,
    QUEUE_TABLE_BY_CLASS,
)
from .frontier_observability import FrontierObservability
from .config import settings
from .result import CrawlResult, result_to_dict
from .schema import assert_public_table_columns

logger = logging.getLogger(__name__)
PAGES_REQUIRED_COLUMNS = {
    "url_hash",
    "url",
    "domain",
    "title",
    "content",
    "content_length",
    "depth",
    "source_url",
    "outlinks",
    "crawled_at",
    "created_at",
}
FRONTIER_STATS_REQUIRED_COLUMNS = {
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


def _build_operator_summary(
    frontier_status: Mapping[str, object],
    readiness: Mapping[str, object],
    runtime: Mapping[str, object],
    active_error_breakdown: Mapping[str, int],
    host_budget_summary: Mapping[str, object],
) -> dict[str, object]:
    """Build a compact operator-facing metrics surface from detailed stats."""
    runtime_payload = runtime.get("payload") if isinstance(runtime.get("payload"), Mapping) else {}
    state_counts = readiness.get("state_counts") if isinstance(readiness.get("state_counts"), Mapping) else {}
    cycle_errors = runtime_payload.get("errors")
    if not isinstance(cycle_errors, Mapping):
        cycle_errors = runtime_payload.get("failure_breakdown")
    if not isinstance(cycle_errors, Mapping):
        cycle_errors = active_error_breakdown

    return {
        "scheduler_state": {
            "pending": int(readiness.get("pending", 0) or 0),
            "ready": int(state_counts.get("ready", 0) or 0),
            "scheduled": int(state_counts.get("scheduled", 0) or 0),
            "blocked_domain_next_request": int(
                state_counts.get("blocked_domain_next_request", 0) or 0
            ),
            "blocked_host_backoff": int(state_counts.get("blocked_host_backoff", 0) or 0),
            "retry_quarantine": int(state_counts.get("retry_quarantine", 0) or 0),
            "leased": int(frontier_status.get("leased", 0) or 0),
        },
        "throughput": {
            "pages_per_second": runtime_payload.get("pages_per_second"),
            "cycle_pages": runtime_payload.get("pages"),
            "active_hosts": int(runtime_payload.get("active_hosts", 0) or 0),
            "active_branches": int(runtime_payload.get("active_branches", 0) or 0),
            "errors": dict(cycle_errors),
        },
        "backpressure": {
            "parse_queue_size": int(runtime_payload.get("parse_queue_size", 0) or 0),
            "finalize_queue_size": int(runtime_payload.get("finalize_queue_size", 0) or 0),
            "publish_queue_size": int(runtime_payload.get("publish_queue_size", 0) or 0),
            "parse_queue_wait_max_ms": runtime_payload.get("parse_queue_wait_max_ms", 0.0),
            "finalize_queue_wait_max_ms": runtime_payload.get("finalize_queue_wait_max_ms", 0.0),
            "publish_queue_wait_max_ms": runtime_payload.get("publish_queue_wait_max_ms", 0.0),
        },
        "adaptive_budget": {
            "observed_hosts": int(host_budget_summary.get("observed_hosts", 0) or 0),
            "eligible_hosts": int(host_budget_summary.get("eligible_hosts", 0) or 0),
            "eligible_pending": int(host_budget_summary.get("eligible_pending", 0) or 0),
            "ineligible_due_to_failures": int(
                host_budget_summary.get("ineligible_due_to_failures", 0) or 0
            ),
            "ineligible_due_to_latency": int(
                host_budget_summary.get("ineligible_due_to_latency", 0) or 0
            ),
            "max_budget": int(host_budget_summary.get("max_budget", 0) or 0),
        },
    }


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

                frontier_status: dict[str, int] = {}
                pending_queue_classes: dict[str, int] = {}
                blocked_queue_classes: dict[str, int] = {}
                readiness: dict[str, object] = {}
                top_pending_domains: list[dict[str, object]] = []
                top_blocked_domains: list[dict[str, object]] = []
                top_slow_domains: list[dict[str, object]] = []
                top_budget_domains: list[dict[str, object]] = []
                active_error_breakdown: dict[str, int] = {}
                top_error_domains: list[dict[str, object]] = []
                runtime: dict[str, object] = {}
                operator_summary: dict[str, object] = {}
                host_budget_summary: dict[str, object] = {}
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

                    observability = FrontierObservability(
                        self._conn,
                        queue_table_by_class=QUEUE_TABLE_BY_CLASS,
                        queue_class_order=QUEUE_CLASS_ORDER,
                        blocked_queue_table=BLOCKED_DOMAIN_BACKOFF_TABLE,
                        lease_table=LEASE_TABLE,
                    )

                    frontier_status = observability.status_counts()
                    pending_queue_classes = dict(frontier_status.get("pending_queue_tables", {}))
                    blocked_queue_classes = dict(frontier_status.get("blocked_queue_classes", {}))

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
                    readiness_snapshot = observability.readiness(now=now)
                    readiness = {
                        "pending": readiness_snapshot.pending,
                        "ready": readiness_snapshot.ready,
                        "ready_domains": readiness_snapshot.ready_domains,
                        "ready_domain_branches": readiness_snapshot.ready_domain_branches,
                        "next_ready_delay": readiness_snapshot.next_ready_delay,
                        "blocked": dict(readiness_snapshot.blocked),
                        "state_counts": dict(readiness_snapshot.state_counts),
                    }

                    cur.execute(
                        f"""WITH pending_entries AS (
                                SELECT queue_class, url, domain, next_fetch_at, FALSE AS forced_retry_quarantine
                                FROM ({pending_queue_sql}) AS pending_queue_rows
                                UNION ALL
                                SELECT queue_class, url, domain, next_fetch_at, TRUE AS forced_retry_quarantine
                                FROM ({blocked_queue_sql}) AS blocked_queue_rows
                            )
                            SELECT
                                pending_queue_rows.domain,
                                COUNT(*) AS pending_count,
                                COUNT(*) FILTER (
                                    WHERE NOT pending_queue_rows.forced_retry_quarantine
                                      AND COALESCE(domain_state.next_request_at, 0) > %(now)s
                                      AND COALESCE(domain_state.backoff_until, 0) <= %(now)s
                                ) AS blocked_domain_next_request,
                                COUNT(*) FILTER (
                                    WHERE NOT pending_queue_rows.forced_retry_quarantine
                                      AND COALESCE(domain_state.backoff_until, 0) > %(now)s
                                ) AS blocked_host_backoff,
                                COUNT(*) FILTER (
                                    WHERE pending_queue_rows.forced_retry_quarantine
                                ) AS retry_quarantine,
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
                            WHERE pending_queue_rows.forced_retry_quarantine
                               OR COALESCE(domain_state.next_request_at, 0) > %(now)s
                               OR COALESCE(domain_state.backoff_until, 0) > %(now)s
                            GROUP BY pending_queue_rows.domain
                            ORDER BY
                                COUNT(*) FILTER (
                                    WHERE pending_queue_rows.forced_retry_quarantine
                                ) DESC,
                                COUNT(*) FILTER (
                                    WHERE NOT pending_queue_rows.forced_retry_quarantine
                                      AND COALESCE(domain_state.backoff_until, 0) > %(now)s
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
                            blocked_host_backoff_count,
                            retry_quarantine_count,
                            next_request_wait_seconds,
                            backoff_wait_seconds,
                            consecutive_failures,
                        ) = blocked_row
                        dominant_reason = "retry_quarantine"
                        wait_seconds = backoff_wait_seconds
                        if retry_quarantine_count == 0 and blocked_host_backoff_count > 0:
                            dominant_reason = "host_backoff"
                        elif (
                            retry_quarantine_count == 0
                            and blocked_host_backoff_count == 0
                            and blocked_next_request_count > 0
                        ):
                            dominant_reason = "domain_next_request"
                            wait_seconds = next_request_wait_seconds
                        top_blocked_domains.append(
                            {
                                "domain": domain,
                                "pending_count": pending_count,
                                "blocked_counts": {
                                    "domain_next_request": blocked_next_request_count,
                                    "host_backoff": blocked_host_backoff_count,
                                    "retry_quarantine": retry_quarantine_count,
                                },
                                "wait_seconds": round(wait_seconds, 3),
                                "dominant_reason": dominant_reason,
                                "consecutive_failures": consecutive_failures,
                            }
                        )

                    cur.execute(
                        f"""WITH pending_entries AS (
                                SELECT queue_class, url, domain
                                FROM ({pending_queue_sql}) AS pending_queue_rows
                                UNION ALL
                                SELECT queue_class, url, domain
                                FROM ({blocked_queue_sql}) AS blocked_queue_rows
                            )
                            SELECT
                                pending_entries.domain,
                                COUNT(*) AS pending_count,
                                MAX(COALESCE(domain_state.latency_ewma_ms, 0)) AS latency_ewma_ms,
                                COALESCE(MAX(domain_state.consecutive_failures), 0) AS consecutive_failures,
                                COUNT(*) FILTER (
                                    WHERE pending_entries.queue_class = 'exploration'
                                ) AS exploration_count,
                                COUNT(*) FILTER (
                                    WHERE pending_entries.queue_class = 'backlog'
                                ) AS backlog_count,
                                COUNT(*) FILTER (
                                    WHERE pending_entries.queue_class = 'recrawl'
                                ) AS recrawl_count
                            FROM pending_entries
                            JOIN public.domain_state ON domain_state.host_key = pending_entries.domain
                            WHERE COALESCE(domain_state.latency_ewma_ms, 0) > 0
                            GROUP BY pending_entries.domain
                            ORDER BY
                                MAX(COALESCE(domain_state.latency_ewma_ms, 0)) DESC,
                                COUNT(*) DESC,
                                pending_entries.domain ASC
                            LIMIT 10"""
                    )
                    top_slow_domains = [
                        {
                            "domain": domain,
                            "pending_count": pending_count,
                            "latency_ewma_ms": round(latency_ewma_ms, 1),
                            "consecutive_failures": consecutive_failures,
                            "queue_counts": {
                                "exploration": exploration_count,
                                "backlog": backlog_count,
                                "recrawl": recrawl_count,
                            },
                        }
                        for (
                            domain,
                            pending_count,
                            latency_ewma_ms,
                            consecutive_failures,
                            exploration_count,
                            backlog_count,
                            recrawl_count,
                        ) in cur.fetchall()
                    ]
                    elevated_budget_domains = []
                    observed_hosts = 0
                    ineligible_due_to_failures = 0
                    ineligible_due_to_latency = 0
                    for domain_entry in top_slow_domains:
                        observed_hosts += 1
                        host_budget = compute_host_budget(
                            latency_ewma_ms=float(domain_entry["latency_ewma_ms"]),
                            consecutive_failures=int(domain_entry["consecutive_failures"]),
                            default_budget=settings.max_inflight_requests_per_host,
                        )
                        if host_budget <= settings.max_inflight_requests_per_host:
                            if int(domain_entry["consecutive_failures"]) > 0:
                                ineligible_due_to_failures += 1
                            else:
                                ineligible_due_to_latency += 1
                            continue
                        elevated_budget_domains.append(
                            {
                                **domain_entry,
                                "host_budget": host_budget,
                            }
                        )
                    top_budget_domains = elevated_budget_domains[:10]
                    host_budget_summary = {
                        "observed_hosts": observed_hosts,
                        "eligible_hosts": len(elevated_budget_domains),
                        "eligible_pending": sum(
                            int(domain_entry["pending_count"])
                            for domain_entry in elevated_budget_domains
                        ),
                        "ineligible_due_to_failures": ineligible_due_to_failures,
                        "ineligible_due_to_latency": ineligible_due_to_latency,
                        "max_budget": max(
                            (int(domain_entry["host_budget"]) for domain_entry in elevated_budget_domains),
                            default=settings.max_inflight_requests_per_host,
                        ),
                    }

                    cur.execute(
                        """WITH retry_surface AS (
                               SELECT url FROM public.frontier_queue_exploration
                               UNION
                               SELECT url FROM public.frontier_queue_backlog
                               UNION
                               SELECT url FROM public.frontier_queue_recrawl
                           )
                           SELECT frontier.last_error, COUNT(*)
                           FROM public.frontier AS frontier
                           LEFT JOIN retry_surface ON retry_surface.url = frontier.url
                           WHERE frontier.last_error IS NOT NULL
                             AND (frontier.terminal_reason IS NOT NULL OR retry_surface.url IS NOT NULL)
                           GROUP BY frontier.last_error"""
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
                        """WITH retry_surface AS (
                               SELECT url FROM public.frontier_queue_exploration
                               UNION
                               SELECT url FROM public.frontier_queue_backlog
                               UNION
                               SELECT url FROM public.frontier_queue_recrawl
                           )
                           SELECT frontier.domain, COUNT(*)
                           FROM public.frontier AS frontier
                           LEFT JOIN retry_surface ON retry_surface.url = frontier.url
                           WHERE frontier.last_error IS NOT NULL
                             AND (frontier.terminal_reason IS NOT NULL OR retry_surface.url IS NOT NULL)
                           GROUP BY frontier.domain
                           ORDER BY COUNT(*) DESC, frontier.domain ASC
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

        operator_summary = _build_operator_summary(
            frontier_status=frontier_status,
            readiness=readiness,
            runtime=runtime,
            active_error_breakdown=active_error_breakdown,
            host_budget_summary=host_budget_summary,
        )

        return {
            "total_pages": row[0],
            "domains": row[1],
            "oldest_crawl": row[2],
            "newest_crawl": row[3],
            "total_bytes": row[4],
            "frontier_status": frontier_status,
            "pending_queue_classes": pending_queue_classes,
            "blocked_queue_classes": blocked_queue_classes,
            "readiness": readiness,
            "top_page_domains": top_page_domains,
            "top_pending_domains": top_pending_domains,
            "top_blocked_domains": top_blocked_domains,
            "top_slow_domains": top_slow_domains,
            "top_budget_domains": top_budget_domains,
            "active_error_breakdown": active_error_breakdown,
            "top_error_domains": top_error_domains,
            "runtime": runtime,
            "operator_summary": operator_summary,
        }

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
