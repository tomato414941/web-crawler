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
from .host_manager import compute_host_budget
from .url_ledger import (
    BLOCKED_HOST_BACKOFF_TABLE,
    LEASE_TABLE,
    PHYSICAL_QUEUE_DEFAULT_RUNNABLE_SURFACE,
    PHYSICAL_QUEUE_ORDER,
    PHYSICAL_QUEUE_TABLES,
    URL_LEDGER_TABLE,
)
from .scheduler_observability import SchedulerObservability
from .config import settings
from .result import CrawlResult, result_to_dict
from .schema import assert_public_table_columns

logger = logging.getLogger(__name__)
PAGES_REQUIRED_COLUMNS = {
    "url_hash",
    "url",
    "host",
    "title",
    "content",
    "content_length",
    "source_url",
    "outlinks",
    "crawled_at",
    "created_at",
}
URL_LEDGER_STATS_REQUIRED_COLUMNS = {
    "host",
    "current_intent",
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


def _pending_queue_sql() -> str:
    queue_union = "\n                        UNION ALL\n                        ".join(
        f"SELECT '{physical_queue}' AS physical_queue, url, host, next_fetch_at FROM public.{PHYSICAL_QUEUE_TABLES[physical_queue]}"
        for physical_queue in PHYSICAL_QUEUE_ORDER
    )
    return f"SELECT physical_queue, url, host, next_fetch_at FROM (\n                        {queue_union}\n                    ) AS pending_queue_rows"


def _retry_surface_sql() -> str:
    retry_union = "\n                               UNION\n                               ".join(
        f"SELECT url FROM public.{PHYSICAL_QUEUE_TABLES[physical_queue]}"
        for physical_queue in PHYSICAL_QUEUE_ORDER
    )
    return f"WITH retry_surface AS (\n                               {retry_union}\n                           )"


def _physical_queue_count_projection_sql(physical_queue_expr: str) -> str:
    return ",\n                                ".join(
        f"COUNT(*) FILTER (WHERE {physical_queue_expr} = '{physical_queue}') AS queue_count_{index}"
        for index, physical_queue in enumerate(PHYSICAL_QUEUE_ORDER)
    )


def _surface_counts_from_physical_queue_count_values(
    queue_count_values: tuple[int, ...],
) -> dict[str, int]:
    surface_counts: dict[str, int] = {}
    for physical_queue, count in zip(PHYSICAL_QUEUE_ORDER, queue_count_values, strict=True):
        surface = PHYSICAL_QUEUE_DEFAULT_RUNNABLE_SURFACE[physical_queue]
        surface_counts[surface] = surface_counts.get(surface, 0) + int(count or 0)
    return surface_counts


def _build_operator_summary(
    scheduler_status: Mapping[str, object],
    readiness: Mapping[str, object],
    effective_state_counts: Mapping[str, int],
    runtime: Mapping[str, object],
    active_error_breakdown: Mapping[str, int],
    host_budget_summary: Mapping[str, object],
) -> dict[str, object]:
    """Build a compact operator-facing metrics surface from detailed stats."""
    runtime_payload = runtime.get("payload") if isinstance(runtime.get("payload"), Mapping) else {}
    scheduler_state_views = _scheduler_state_views(
        scheduler_status=scheduler_status,
        readiness=readiness,
        effective_state_counts=effective_state_counts,
    )
    scheduler_state_snapshot = scheduler_state_views["scheduler_state_snapshot"]
    state_counts = scheduler_state_views["readiness_state_counts"]
    cycle_errors = runtime_payload.get("errors")
    if not isinstance(cycle_errors, Mapping):
        cycle_errors = runtime_payload.get("failure_breakdown")
    if not isinstance(cycle_errors, Mapping):
        cycle_errors = active_error_breakdown

    scheduler_readiness_states = {
        "pending": int(readiness.get("pending", 0) or 0),
        "runnable": int(state_counts.get("runnable", 0) or 0),
        "scheduled": int(state_counts.get("scheduled", 0) or 0),
        "blocked_host_next_request": int(state_counts.get("blocked_host_next_request", 0) or 0),
        "blocked_host_backoff": int(state_counts.get("blocked_host_backoff", 0) or 0),
        "retry_quarantine": int(state_counts.get("retry_quarantine", 0) or 0),
        "leased": int(scheduler_status.get("leased", 0) or 0),
    }

    return {
        "scheduler_state": dict(scheduler_readiness_states),
        "scheduler_readiness_states": dict(scheduler_readiness_states),
        "scheduler_state_snapshot": dict(scheduler_state_snapshot),
        "scheduler_durable_states": dict(scheduler_state_views["durable_state_counts"]),
        "scheduler_effective_states": dict(scheduler_state_views["effective_state_counts"]),
        "scheduler_intents": dict(scheduler_status.get("intent_counts", {})),
        "scheduler_blocked_reasons": dict(scheduler_state_views["blocked_reason_counts"]),
        "throughput": {
            "pages_per_second": runtime_payload.get("pages_per_second"),
            "cycle_pages": runtime_payload.get("pages"),
            "active_hosts": int(runtime_payload.get("active_hosts", 0) or 0),
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


def _scheduler_state_views(
    *,
    scheduler_status: Mapping[str, object],
    readiness: Mapping[str, object],
    effective_state_counts: Mapping[str, int],
) -> dict[str, dict[str, int]]:
    """Resolve scheduler-facing state aliases from the canonical state snapshot first."""
    scheduler_state_snapshot = (
        scheduler_status.get("scheduler_state_snapshot")
        if isinstance(scheduler_status.get("scheduler_state_snapshot"), Mapping)
        else {}
    )
    durable_state_counts = (
        scheduler_state_snapshot.get("durable_state_counts")
        if isinstance(scheduler_state_snapshot.get("durable_state_counts"), Mapping)
        else scheduler_status.get("durable_state_counts")
        if isinstance(scheduler_status.get("durable_state_counts"), Mapping)
        else {}
    )
    readiness_state_counts = (
        scheduler_state_snapshot.get("readiness_state_counts")
        if isinstance(scheduler_state_snapshot.get("readiness_state_counts"), Mapping)
        else scheduler_status.get("readiness_state_counts")
        if isinstance(scheduler_status.get("readiness_state_counts"), Mapping)
        else readiness.get("state_counts")
        if isinstance(readiness.get("state_counts"), Mapping)
        else {}
    )
    effective_state_counts_view = (
        scheduler_state_snapshot.get("effective_state_counts")
        if isinstance(scheduler_state_snapshot.get("effective_state_counts"), Mapping)
        else effective_state_counts
    )
    blocked_reason_counts = (
        scheduler_state_snapshot.get("blocked_reason_counts")
        if isinstance(scheduler_state_snapshot.get("blocked_reason_counts"), Mapping)
        else scheduler_status.get("blocked_reason_counts")
        if isinstance(scheduler_status.get("blocked_reason_counts"), Mapping)
        else {}
    )
    return {
        "scheduler_state_snapshot": dict(scheduler_state_snapshot),
        "durable_state_counts": dict(durable_state_counts),
        "readiness_state_counts": dict(readiness_state_counts),
        "effective_state_counts": dict(effective_state_counts_view),
        "blocked_reason_counts": dict(blocked_reason_counts),
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
        host = urlparse(url).netloc

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
                    """INSERT INTO pages (url_hash, url, host, title, content, status,
                           content_length, source_url, outlinks, crawled_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        host,
                        title,
                        content,
                        data.get("status"),
                        data.get("content_length"),
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
        """Expose connection for the URL ledger (which shares the same Postgres)."""
        return self._conn

    def list_pages(
        self,
        since: float = 0,
        limit: int = 100,
        offset: int = 0,
        host: str | None = None,
    ) -> list[dict]:
        """List crawled pages with optional filters."""
        conditions = ["crawled_at > %s"]
        params: list = [since]

        if host:
            conditions.append("host = %s")
            params.append(host)

        where = " AND ".join(conditions)
        params.extend([limit, offset])

        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""SELECT url_hash, url, host, title, status, content_length,
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
                    """SELECT url_hash, url, host, title, content, status,
                              content_length, source_url, outlinks, crawled_at
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
                         count(DISTINCT host) as hosts,
                         min(crawled_at) as oldest,
                         max(crawled_at) as newest,
                         sum(content_length) as total_bytes
                       FROM pages"""
                )
                page_stats_row = cur.fetchone()

                cur.execute(f"SELECT to_regclass('public.{URL_LEDGER_TABLE}')")
                url_ledger_exists = cur.fetchone()[0] is not None

                scheduler_status: dict[str, int] = {}
                pending_surfaces: dict[str, int] = {}
                blocked_surfaces: dict[str, int] = {}
                readiness: dict[str, object] = {}
                top_pending_hosts: list[dict[str, object]] = []
                top_blocked_hosts: list[dict[str, object]] = []
                top_slow_hosts: list[dict[str, object]] = []
                top_budget_hosts: list[dict[str, object]] = []
                active_error_breakdown: dict[str, int] = {}
                top_error_hosts: list[dict[str, object]] = []
                runtime: dict[str, object] = {}
                operator_summary: dict[str, object] = {}
                effective_state_counts: dict[str, int] = {}
                host_budget_summary: dict[str, object] = {}
                pending_queue_sql = _pending_queue_sql()
                blocked_queue_sql = f"""SELECT physical_queue, url, host, next_fetch_at
                    FROM public.{BLOCKED_HOST_BACKOFF_TABLE}"""
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

                if url_ledger_exists:
                    assert_public_table_columns(
                        self._conn,
                        URL_LEDGER_TABLE,
                        URL_LEDGER_STATS_REQUIRED_COLUMNS,
                    )

                    observability = SchedulerObservability(
                        self._conn,
                        physical_queue_tables=PHYSICAL_QUEUE_TABLES,
                        physical_queue_order=PHYSICAL_QUEUE_ORDER,
                        physical_queue_default_runnable_surface=PHYSICAL_QUEUE_DEFAULT_RUNNABLE_SURFACE,
                        blocked_queue_table=BLOCKED_HOST_BACKOFF_TABLE,
                        lease_table=LEASE_TABLE,
                    )

                    scheduler_status = observability.status_counts()
                    pending_surfaces = dict(scheduler_status.get("pending_surfaces", {}))
                    blocked_surfaces = dict(scheduler_status.get("blocked_surfaces", {}))

                    cur.execute(
                        f"""SELECT host, COUNT(*)
                           FROM ({pending_queue_sql}) AS pending_queue_rows
                           GROUP BY host
                           ORDER BY COUNT(*) DESC, host ASC
                           LIMIT 10"""
                    )
                    top_pending_hosts = [
                        {"host": host, "count": count} for host, count in cur.fetchall()
                    ]

                    now = time.time()
                    readiness_snapshot = observability.readiness(now=now)
                    readiness = {
                        "pending": readiness_snapshot.pending,
                        "runnable": readiness_snapshot.runnable,
                        "runnable_hosts": readiness_snapshot.runnable_hosts,
                        "next_runnable_delay": readiness_snapshot.next_runnable_delay,
                        "blocked": dict(readiness_snapshot.blocked),
                        "state_counts": dict(
                            scheduler_status.get(
                                "readiness_state_counts", readiness_snapshot.state_counts
                            )
                        ),
                    }
                    effective_state_counts = dict(
                        scheduler_status.get("effective_state_counts", {})
                    )

                    cur.execute(
                        f"""WITH pending_entries AS (
                                SELECT physical_queue, url, host, next_fetch_at, FALSE AS forced_retry_quarantine
                                FROM ({pending_queue_sql}) AS pending_queue_rows
                                UNION ALL
                                SELECT physical_queue, url, host, next_fetch_at, TRUE AS forced_retry_quarantine
                                FROM ({blocked_queue_sql}) AS blocked_queue_rows
                            )
                            SELECT
                                pending_queue_rows.host,
                                COUNT(*) AS pending_count,
                                COUNT(*) FILTER (
                                    WHERE NOT pending_queue_rows.forced_retry_quarantine
                                      AND COALESCE(host_state.next_request_at, 0) > %(now)s
                                      AND COALESCE(host_state.backoff_until, 0) <= %(now)s
                                ) AS blocked_host_next_request,
                                COUNT(*) FILTER (
                                    WHERE NOT pending_queue_rows.forced_retry_quarantine
                                      AND COALESCE(host_state.backoff_until, 0) > %(now)s
                                ) AS blocked_host_backoff,
                                COUNT(*) FILTER (
                                    WHERE pending_queue_rows.forced_retry_quarantine
                                ) AS retry_quarantine,
                                GREATEST(
                                    MAX(COALESCE(host_state.next_request_at, 0)) - %(now)s,
                                    0
                                ) AS next_request_wait_seconds,
                                GREATEST(
                                    MAX(COALESCE(host_state.backoff_until, 0)) - %(now)s,
                                    0
                                ) AS backoff_wait_seconds,
                                COALESCE(MAX(host_state.consecutive_failures), 0) AS consecutive_failures
                            FROM pending_entries AS pending_queue_rows
                            LEFT JOIN public.host_state ON host_state.host_key = pending_queue_rows.host
                            WHERE pending_queue_rows.forced_retry_quarantine
                               OR COALESCE(host_state.next_request_at, 0) > %(now)s
                               OR COALESCE(host_state.backoff_until, 0) > %(now)s
                            GROUP BY pending_queue_rows.host
                            ORDER BY
                                COUNT(*) FILTER (
                                    WHERE pending_queue_rows.forced_retry_quarantine
                                ) DESC,
                                COUNT(*) FILTER (
                                    WHERE NOT pending_queue_rows.forced_retry_quarantine
                                      AND COALESCE(host_state.backoff_until, 0) > %(now)s
                                ) DESC,
                                GREATEST(
                                    MAX(COALESCE(host_state.backoff_until, 0)) - %(now)s,
                                    0
                                ) DESC,
                                COUNT(*) DESC,
                                pending_queue_rows.host ASC
                            LIMIT 10""",
                        {"now": now},
                    )
                    top_blocked_hosts = []
                    for blocked_row in cur.fetchall():
                        (
                            host,
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
                            dominant_reason = "host_next_request"
                            wait_seconds = next_request_wait_seconds
                        top_blocked_hosts.append(
                            {
                                "host": host,
                                "pending_count": pending_count,
                                "blocked_counts": {
                                    "host_next_request": blocked_next_request_count,
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
                                SELECT physical_queue, url, host
                                FROM ({pending_queue_sql}) AS pending_queue_rows
                                UNION ALL
                                SELECT physical_queue, url, host
                                FROM ({blocked_queue_sql}) AS blocked_queue_rows
                            )
                            SELECT
                                pending_entries.host,
                                COUNT(*) AS pending_count,
                                MAX(COALESCE(host_state.latency_ewma_ms, 0)) AS latency_ewma_ms,
                                COALESCE(MAX(host_state.consecutive_failures), 0) AS consecutive_failures,
                                {_physical_queue_count_projection_sql("pending_entries.physical_queue")}
                            FROM pending_entries
                            JOIN public.host_state ON host_state.host_key = pending_entries.host
                            WHERE COALESCE(host_state.latency_ewma_ms, 0) > 0
                            GROUP BY pending_entries.host
                            ORDER BY
                                MAX(COALESCE(host_state.latency_ewma_ms, 0)) DESC,
                                COUNT(*) DESC,
                                pending_entries.host ASC
                            LIMIT 10"""
                    )
                    top_slow_hosts = []
                    for row in cur.fetchall():
                        (
                            host,
                            pending_count,
                            latency_ewma_ms,
                            consecutive_failures,
                            *queue_count_values,
                        ) = row
                        top_slow_hosts.append(
                            {
                                "host": host,
                                "pending_count": pending_count,
                                "latency_ewma_ms": round(latency_ewma_ms, 1),
                                "consecutive_failures": consecutive_failures,
                                "surface_counts": _surface_counts_from_physical_queue_count_values(
                                    tuple(queue_count_values)
                                ),
                            }
                        )
                    elevated_budget_hosts = []
                    observed_hosts = 0
                    ineligible_due_to_failures = 0
                    ineligible_due_to_latency = 0
                    for host_entry in top_slow_hosts:
                        observed_hosts += 1
                        host_budget = compute_host_budget(
                            latency_ewma_ms=float(host_entry["latency_ewma_ms"]),
                            consecutive_failures=int(host_entry["consecutive_failures"]),
                            default_budget=settings.max_inflight_requests_per_host,
                        )
                        if host_budget <= settings.max_inflight_requests_per_host:
                            if int(host_entry["consecutive_failures"]) > 0:
                                ineligible_due_to_failures += 1
                            else:
                                ineligible_due_to_latency += 1
                            continue
                        elevated_budget_hosts.append(
                            {
                                **host_entry,
                                "host_budget": host_budget,
                            }
                        )
                    top_budget_hosts = elevated_budget_hosts[:10]
                    host_budget_summary = {
                        "observed_hosts": observed_hosts,
                        "eligible_hosts": len(elevated_budget_hosts),
                        "eligible_pending": sum(
                            int(host_entry["pending_count"]) for host_entry in elevated_budget_hosts
                        ),
                        "ineligible_due_to_failures": ineligible_due_to_failures,
                        "ineligible_due_to_latency": ineligible_due_to_latency,
                        "max_budget": max(
                            (
                                int(host_entry["host_budget"])
                                for host_entry in elevated_budget_hosts
                            ),
                            default=settings.max_inflight_requests_per_host,
                        ),
                    }

                    cur.execute(
                        _retry_surface_sql()
                        + """
                           SELECT url_ledger.last_error, COUNT(*)
                           FROM public.url_ledger AS url_ledger
                           LEFT JOIN retry_surface ON retry_surface.url = url_ledger.url
                           WHERE url_ledger.last_error IS NOT NULL
                             AND (url_ledger.terminal_reason IS NOT NULL OR retry_surface.url IS NOT NULL)
                           GROUP BY url_ledger.last_error"""
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
                        _retry_surface_sql()
                        + """
                           SELECT url_ledger.host, COUNT(*)
                           FROM public.url_ledger AS url_ledger
                           LEFT JOIN retry_surface ON retry_surface.url = url_ledger.url
                           WHERE url_ledger.last_error IS NOT NULL
                             AND (url_ledger.terminal_reason IS NOT NULL OR retry_surface.url IS NOT NULL)
                           GROUP BY url_ledger.host
                           ORDER BY COUNT(*) DESC, url_ledger.host ASC
                           LIMIT 10"""
                    )
                    top_error_hosts = [
                        {"host": host, "count": count} for host, count in cur.fetchall()
                    ]

                cur.execute(
                    """SELECT host, COUNT(*)
                       FROM pages
                       GROUP BY host
                       ORDER BY COUNT(*) DESC, host ASC
                       LIMIT 10"""
                )
                top_page_hosts = [{"host": host, "count": count} for host, count in cur.fetchall()]
            self._finish_read()
        except Exception:
            self._conn.rollback()
            raise

        operator_summary = _build_operator_summary(
            scheduler_status=scheduler_status,
            readiness=readiness,
            effective_state_counts=effective_state_counts,
            runtime=runtime,
            active_error_breakdown=active_error_breakdown,
            host_budget_summary=host_budget_summary,
        )

        scheduler_state_views = _scheduler_state_views(
            scheduler_status=scheduler_status,
            readiness=readiness,
            effective_state_counts=effective_state_counts,
        )
        return {
            "total_pages": page_stats_row[0],
            "hosts": page_stats_row[1],
            "oldest_crawl": page_stats_row[2],
            "newest_crawl": page_stats_row[3],
            "total_bytes": page_stats_row[4],
            "scheduler_status": scheduler_status,
            "scheduler_state_snapshot": dict(scheduler_state_views["scheduler_state_snapshot"]),
            "intent_counts": dict(scheduler_status.get("intent_counts", {})),
            "durable_state_counts": dict(scheduler_state_views["durable_state_counts"]),
            "readiness_state_counts": dict(scheduler_state_views["readiness_state_counts"]),
            "effective_state_counts": dict(scheduler_state_views["effective_state_counts"]),
            "blocked_reason_counts": dict(scheduler_state_views["blocked_reason_counts"]),
            "pending_surfaces": pending_surfaces,
            "blocked_surfaces": blocked_surfaces,
            "readiness": readiness,
            "top_page_hosts": top_page_hosts,
            "top_pending_hosts": top_pending_hosts,
            "top_blocked_hosts": top_blocked_hosts,
            "top_slow_hosts": top_slow_hosts,
            "top_budget_hosts": top_budget_hosts,
            "active_error_breakdown": active_error_breakdown,
            "top_error_hosts": top_error_hosts,
            "runtime": runtime,
            "operator_summary": operator_summary,
        }

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
