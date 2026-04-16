"""URL ledger with PostgreSQL-backed scheduler state."""

from __future__ import annotations

import logging
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import psycopg2.extras

from .config import settings
from .frontier_observability import FrontierObservability, FrontierReadiness
from .frontier_quarantine import FrontierQuarantine
from .schema import assert_public_table_columns
from .urls import normalize_url, url_branch_key

if TYPE_CHECKING:
    from .domain_store import DomainStore

logger = logging.getLogger(__name__)

URL_LEDGER_TABLE = "url_ledger"
QUEUE_EXPLORATION = "exploration"
QUEUE_BACKLOG = "backlog"
QUEUE_RECRAWL = "recrawl"

EXPLORATION_DOMAIN_BUDGET = 8
DEFAULT_LEASE_SECONDS = 300.0
DEFAULT_RETRY_BACKOFF_SECONDS = 30.0
MAX_RETRY_BACKOFF_SECONDS = 1800.0
RETRY_PRIORITY_DECAY = 0.6
MIN_RETRY_PRIORITY = 0.25
LATENCY_BUCKET_FAST_MS = 150.0
LATENCY_BUCKET_SLOW_MS = 400.0
LATENCY_BUCKET_VERY_SLOW_MS = 1000.0
URL_LEDGER_REQUIRED_COLUMNS = {
    "url",
    "domain",
    "priority",
    "source_url",
    "added_at",
    "next_fetch_at",
    "last_success_at",
    "fail_streak",
    "lease_token",
    "lease_expires_at",
    "last_error",
    "terminal_reason",
    "terminalized_at",
}
FRONTIER_ALLOWED_QUEUE_CLASSES = {
    QUEUE_EXPLORATION,
    QUEUE_BACKLOG,
    QUEUE_RECRAWL,
}
QUEUE_TABLE_BY_CLASS = {
    QUEUE_EXPLORATION: "frontier_queue_exploration",
    QUEUE_BACKLOG: "frontier_queue_backlog",
    QUEUE_RECRAWL: "frontier_queue_recrawl",
}
QUEUE_TABLES = tuple(QUEUE_TABLE_BY_CLASS.values())
BLOCKED_DOMAIN_BACKOFF_TABLE = "frontier_queue_blocked_domain_backoff"
QUEUE_CLASS_ORDER = (
    QUEUE_EXPLORATION,
    QUEUE_BACKLOG,
    QUEUE_RECRAWL,
)
QUEUE_REQUIRED_COLUMNS = {
    "url",
    "domain",
    "priority",
    "next_fetch_at",
    "added_at",
    "branch_key",
}
LEASE_TABLE = "active_leases"
LEASE_REQUIRED_COLUMNS = {
    "url",
    "domain",
    "queue_class",
    "lease_token",
    "lease_expires_at",
}
BLOCKED_QUEUE_REQUIRED_COLUMNS = {
    "url",
    "domain",
    "queue_class",
    "priority",
    "next_fetch_at",
    "added_at",
    "quarantined_at",
    "branch_key",
}

@dataclass
class CrawlTask:
    """A URL to crawl with metadata."""

    url: str
    priority: float = 1.0
    queue_class: str | None = None
    source_url: str | None = None
    added_at: float = 0.0
    next_fetch_at: float = 0.0
    lease_token: str | None = None
    lease_expires_at: float | None = None

    def __post_init__(self):
        if self.added_at == 0.0:
            self.added_at = time.time()
        if self.next_fetch_at == 0.0:
            self.next_fetch_at = self.added_at


@dataclass(frozen=True)
class _ReadySql:
    """SQL fragments for pending URL readiness checks."""

    where: str
    params: tuple[object, ...]
    ready_at: str


class UrlLedger:
    """Durable URL ledger with PostgreSQL persistence. Dedup via ON CONFLICT."""

    def __init__(
        self,
        conn,
        lease_seconds: float | None = None,
        retry_backoff_seconds: float | None = None,
        max_retry_backoff_seconds: float | None = None,
    ):
        self._conn = conn
        self._lease_seconds = settings.frontier_lease_seconds if lease_seconds is None else lease_seconds
        self._retry_backoff_seconds = (
            settings.frontier_retry_backoff_seconds
            if retry_backoff_seconds is None else retry_backoff_seconds
        )
        self._max_retry_backoff_seconds = (
            settings.frontier_max_retry_backoff_seconds
            if max_retry_backoff_seconds is None else max_retry_backoff_seconds
        )
        self._domain_store: DomainStore | None = None
        self._observability = FrontierObservability(
            conn,
            queue_table_by_class=QUEUE_TABLE_BY_CLASS,
            queue_class_order=QUEUE_CLASS_ORDER,
            blocked_queue_table=BLOCKED_DOMAIN_BACKOFF_TABLE,
            lease_table=LEASE_TABLE,
        )
        self._quarantine = FrontierQuarantine(
            conn,
            queue_exploration=QUEUE_EXPLORATION,
            queue_backlog=QUEUE_BACKLOG,
            queue_recrawl=QUEUE_RECRAWL,
            blocked_queue_table=BLOCKED_DOMAIN_BACKOFF_TABLE,
            queue_table_sql=self._queue_table_sql,
            delete_queue_entries=self._delete_queue_entries,
            insert_blocked_rows=self._insert_blocked_domain_backoff_rows,
            insert_pending_rows=self._insert_pending_queue_rows,
        )
        self._assert_current_schema()

    def attach_domain_store(self, domain_store: "DomainStore | None") -> None:
        """Attach the persistent host scheduler used for lease selection."""
        self._domain_store = domain_store

    def _compute_retry_backoff(self, fail_streak: int) -> float:
        """Compute exponential retry backoff for a failed URL."""
        base = max(self._retry_backoff_seconds, 0.0)
        if fail_streak <= 1:
            return base
        delay = base * (2 ** (fail_streak - 1))
        return min(delay, self._max_retry_backoff_seconds)

    def _compute_retry_priority(self, priority: float, fail_streak: int) -> float:
        """Lower retry priority so repeatedly failing URLs do not dominate the queue."""
        if fail_streak <= 0:
            return priority
        return max(MIN_RETRY_PRIORITY, round(priority * (RETRY_PRIORITY_DECAY ** fail_streak), 2))

    def _lease_match_sql(self, table_alias: str, lease_token: str | None) -> tuple[str, tuple]:
        """Build an optional lease-table predicate for completion updates."""
        if lease_token is None:
            return "", ()
        return (
            " AND EXISTS ("
            f"SELECT 1 FROM {LEASE_TABLE} AS active "
            f"WHERE active.url = {table_alias}.url AND active.lease_token = %s"
            ")",
            (lease_token,),
        )

    def _normalized_queue_classes(self, queue_classes: list[str] | None) -> list[str]:
        """Return queue classes in stable scheduler order."""
        if queue_classes:
            allowed = {self._normalize_queue_class(queue_class) for queue_class in queue_classes}
            return [queue_class for queue_class in QUEUE_CLASS_ORDER if queue_class in allowed]
        return list(QUEUE_CLASS_ORDER)

    def _queue_ready_sql(
        self,
        *,
        alias: str,
        now: float,
        domain: str | None = None,
        exclude_domains: list[str] | None = None,
    ) -> _ReadySql:
        """Build readiness SQL fragments for physical pending queue tables."""
        next_request_sql = "0"
        backoff_sql = "0"

        conditions = [f"{alias}.next_fetch_at <= %s"]
        params: list[object] = [now]

        if self._domain_store is not None:
            next_request_sql = (
                "COALESCE(("
                "SELECT ds.next_request_at "
                "FROM domain_state AS ds "
                f"WHERE ds.host_key = {alias}.domain"
                "), 0)"
            )
            backoff_sql = (
                "COALESCE(("
                "SELECT ds.backoff_until "
                "FROM domain_state AS ds "
                f"WHERE ds.host_key = {alias}.domain"
                "), 0)"
            )
            conditions.append(
                "NOT EXISTS ("
                "SELECT 1 FROM domain_state AS gated "
                f"WHERE gated.host_key = {alias}.domain "
                "AND (gated.next_request_at > %s OR gated.backoff_until > %s)"
                ")"
            )
            params.extend([now, now])

        if domain:
            conditions.append(f"{alias}.domain = %s")
            params.append(domain)

        if exclude_domains:
            conditions.append(f"NOT ({alias}.domain = ANY(%s))")
            params.append(exclude_domains)

        return _ReadySql(
            where=" AND ".join(conditions),
            params=tuple(params),
            ready_at=f"GREATEST({alias}.next_fetch_at, {next_request_sql}, {backoff_sql})",
        )

    def _recover_leased_locked(self, now: float, expired_only: bool) -> int:
        """Reset leased URLs back to pending inside an open transaction."""
        if expired_only:
            where = "lease_expires_at <= %s"
            params = (now,)
        else:
            where = "TRUE"
            params = ()

        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH recovered AS (
                        DELETE FROM {LEASE_TABLE}
                        WHERE {where}
                        RETURNING url, domain, queue_class
                    )
                    SELECT ledger.url,
                           ledger.domain,
                           ledger.priority,
                           ledger.next_fetch_at,
                           ledger.added_at,
                           recovered.queue_class
                    FROM {URL_LEDGER_TABLE} AS ledger
                    JOIN recovered ON recovered.url = ledger.url""",
                params,
            )
            rows = cur.fetchall()
            self._replace_pending_queue_rows(cur, self._project_pending_queue_rows(rows))
            return len(rows)

    def _assert_current_schema(self) -> None:
        assert_public_table_columns(self._conn, URL_LEDGER_TABLE, URL_LEDGER_REQUIRED_COLUMNS)

        with self._conn.cursor() as cur:
            for table_name in QUEUE_TABLES:
                cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
                if cur.fetchone()[0] is None:
                    raise RuntimeError(f"missing frontier queue table: {table_name}")
                assert_public_table_columns(self._conn, table_name, QUEUE_REQUIRED_COLUMNS)
            cur.execute("SELECT to_regclass(%s)", (f"public.{BLOCKED_DOMAIN_BACKOFF_TABLE}",))
            if cur.fetchone()[0] is None:
                raise RuntimeError(
                    f"missing frontier blocked queue table: {BLOCKED_DOMAIN_BACKOFF_TABLE}"
                )
            assert_public_table_columns(
                self._conn,
                BLOCKED_DOMAIN_BACKOFF_TABLE,
                BLOCKED_QUEUE_REQUIRED_COLUMNS,
            )
            cur.execute("SELECT to_regclass(%s)", (f"public.{LEASE_TABLE}",))
            if cur.fetchone()[0] is None:
                raise RuntimeError(f"missing frontier lease table: {LEASE_TABLE}")
            assert_public_table_columns(self._conn, LEASE_TABLE, LEASE_REQUIRED_COLUMNS)

    def _normalize_queue_class(self, queue_class: str | None) -> str:
        """Return a supported frontier queue class."""
        if queue_class in FRONTIER_ALLOWED_QUEUE_CLASSES:
            return queue_class
        return QUEUE_BACKLOG

    def _classify_queue(self, task: CrawlTask, *, known_count: int = 0) -> str:
        """Map a task into the queue class used by the scheduler."""
        if task.queue_class in FRONTIER_ALLOWED_QUEUE_CLASSES:
            return task.queue_class
        if known_count >= EXPLORATION_DOMAIN_BUDGET:
            return QUEUE_BACKLOG
        return QUEUE_BACKLOG

    def _merge_queue_class(self, current: str | None, candidate: str | None) -> str:
        """Prefer the more urgent queue class when duplicate URLs merge."""
        current = self._normalize_queue_class(current)
        candidate = self._normalize_queue_class(candidate)
        if QUEUE_EXPLORATION in {current, candidate}:
            return QUEUE_EXPLORATION
        if QUEUE_BACKLOG in {current, candidate}:
            return QUEUE_BACKLOG
        return QUEUE_RECRAWL

    def _merge_task(self, current: CrawlTask, candidate: CrawlTask) -> CrawlTask:
        """Merge duplicate task metadata before bulk upsert."""
        return CrawlTask(
            url=current.url,
            priority=max(current.priority, candidate.priority),
            queue_class=self._merge_queue_class(current.queue_class, candidate.queue_class),
            source_url=current.source_url or candidate.source_url,
            added_at=min(current.added_at, candidate.added_at),
            next_fetch_at=min(current.next_fetch_at, candidate.next_fetch_at),
        )

    def _prepare_tasks(self, tasks: list[CrawlTask]) -> list[CrawlTask]:
        """Normalize and deduplicate tasks before writing to Postgres."""
        merged: dict[str, CrawlTask] = {}
        for task in tasks:
            normalized_url = normalize_url(task.url)
            normalized = CrawlTask(
                url=normalized_url,
                priority=task.priority,
                queue_class=task.queue_class,
                source_url=task.source_url,
                added_at=task.added_at,
                next_fetch_at=task.next_fetch_at,
            )
            existing = merged.get(normalized.url)
            if existing is None:
                merged[normalized.url] = normalized
            else:
                merged[normalized.url] = self._merge_task(existing, normalized)

        domain_counts = self.get_domain_known_counts({urlparse(task.url).netloc for task in merged.values()})
        batch_counts: Counter[str] = Counter()
        prepared: list[CrawlTask] = []
        for task in merged.values():
            domain = urlparse(task.url).netloc
            known_count = domain_counts.get(domain, 0) + batch_counts[domain]
            prepared.append(
                CrawlTask(
                    url=task.url,
                    priority=task.priority,
                    queue_class=self._classify_queue(task, known_count=known_count),
                    source_url=task.source_url,
                    added_at=task.added_at,
                    next_fetch_at=task.next_fetch_at,
                )
            )
            batch_counts[domain] += 1
        return prepared

    def get_domain_known_counts(self, domains: set[str]) -> dict[str, int]:
        """Return known URL counts per domain from the URL ledger."""
        known_domains = {domain for domain in domains if domain}
        if not known_domains:
            return {}
        with self._conn.cursor() as cur:
            cur.execute(
                f"""SELECT domain, COUNT(*)
                   FROM {URL_LEDGER_TABLE}
                   WHERE domain = ANY(%s)
                   GROUP BY domain""",
                (sorted(known_domains),),
            )
            return {domain: count for domain, count in cur.fetchall()}

    def _lease_order_by_sql(self, alias: str, prioritize_breadth: bool) -> str:
        """Return the ORDER BY clause used for lease selection."""
        latency_penalty = self._latency_penalty_sql(alias)
        if prioritize_breadth:
            return (
                f"{alias}.next_fetch_at ASC, "
                f"{latency_penalty} ASC, "
                f"{alias}.added_at ASC, "
                f"{alias}.priority DESC"
            )

        return (
            f"{alias}.priority DESC, "
            f"{latency_penalty} ASC, "
            f"{alias}.next_fetch_at ASC, "
            f"{alias}.added_at ASC"
        )

    def _latency_penalty_sql(self, alias: str) -> str:
        """Return a small host-latency bucket used as a lease tiebreaker."""
        latency_ms = (
            "COALESCE((SELECT ds.latency_ewma_ms "
            f"FROM domain_state AS ds WHERE ds.host_key = {alias}.domain), 0)"
        )
        return (
            "CASE "
            f"WHEN {latency_ms} >= {LATENCY_BUCKET_VERY_SLOW_MS} THEN 3 "
            f"WHEN {latency_ms} >= {LATENCY_BUCKET_SLOW_MS} THEN 2 "
            f"WHEN {latency_ms} >= {LATENCY_BUCKET_FAST_MS} THEN 1 "
            "ELSE 0 END"
        )

    def _host_head_order_by_sql(self, alias: str) -> str:
        """Return ORDER BY used to compare the best ready URL for each host."""
        latency_penalty = self._latency_penalty_sql(alias)
        return (
            f"{alias}.next_fetch_at ASC, "
            f"{latency_penalty} ASC, "
            f"{alias}.added_at ASC, "
            f"{alias}.priority DESC, "
            f"{alias}.url ASC"
        )

    def _host_first_domain_selector_sql(
        self,
        *,
        queue_class: str,
        ready_sql: _ReadySql,
    ) -> str:
        """Return SQL that selects the next host to lease from before choosing a URL."""
        table_name = self._queue_table_sql(queue_class)
        host_head_order = self._host_head_order_by_sql("host_source")
        return f"""candidate.domain = (
                    SELECT selected.domain
                    FROM (
                        SELECT DISTINCT ON (host_source.domain)
                            host_source.domain,
                            host_source.next_fetch_at,
                            {self._latency_penalty_sql("host_source")} AS latency_penalty,
                            host_source.added_at,
                            host_source.priority,
                            host_source.url
                        FROM {table_name} AS host_source
                        WHERE {ready_sql.where}
                        ORDER BY host_source.domain, {host_head_order}
                    ) AS selected
                    ORDER BY
                        selected.next_fetch_at ASC,
                        selected.latency_penalty ASC,
                        selected.added_at ASC,
                        selected.priority DESC,
                        selected.url ASC
                    LIMIT 1
                )"""

    def _queue_table_sql(self, queue_class: str) -> str:
        """Return the physical queue table name for a queue class."""
        return QUEUE_TABLE_BY_CLASS[self._normalize_queue_class(queue_class)]

    def _delete_queue_entries(self, cur, urls: list[str]) -> None:
        """Remove URLs from all physical scheduler queue tables."""
        if not urls:
            return
        for table_name in QUEUE_TABLES:
            cur.execute(f"DELETE FROM {table_name} WHERE url = ANY(%s)", (urls,))
        cur.execute(f"DELETE FROM {BLOCKED_DOMAIN_BACKOFF_TABLE} WHERE url = ANY(%s)", (urls,))

    def _insert_pending_queue_rows(
        self,
        cur,
        rows: list[tuple[str, str, float, float, float, str]],
    ) -> None:
        """Insert scheduler-pending rows into the appropriate physical queue tables."""
        grouped: dict[str, list[tuple[str, str, float, float, float, str]]] = {
            queue_class: [] for queue_class in FRONTIER_ALLOWED_QUEUE_CLASSES
        }
        for url, domain, priority, next_fetch_at, added_at, queue_class in rows:
            normalized_url = normalize_url(url)
            grouped[self._normalize_queue_class(queue_class)].append(
                (normalized_url, domain, priority, next_fetch_at, added_at, url_branch_key(normalized_url))
            )

        for queue_class, pending_rows in grouped.items():
            if not pending_rows:
                continue
            psycopg2.extras.execute_values(
                cur,
                f"""INSERT INTO {self._queue_table_sql(queue_class)}
                        (url, domain, priority, next_fetch_at, added_at, branch_key)
                    VALUES %s
                    ON CONFLICT (url) DO UPDATE
                    SET domain = EXCLUDED.domain,
                        priority = EXCLUDED.priority,
                        next_fetch_at = EXCLUDED.next_fetch_at,
                        added_at = EXCLUDED.added_at,
                        branch_key = EXCLUDED.branch_key""",
                pending_rows,
                page_size=200,
            )

    def _insert_blocked_domain_backoff_rows(
        self,
        cur,
        rows: list[tuple[str, str, float, float, float, str]],
        *,
        quarantined_at: float | None = None,
    ) -> None:
        """Insert URLs into the blocked-domain-backoff physical queue."""
        now = time.time() if quarantined_at is None else quarantined_at
        blocked_rows = [
            (
                normalize_url(url),
                domain,
                self._normalize_queue_class(queue_class),
                priority,
                next_fetch_at,
                added_at,
                now,
                url_branch_key(normalize_url(url)),
            )
            for url, domain, priority, next_fetch_at, added_at, queue_class in rows
        ]
        if not blocked_rows:
            return
        psycopg2.extras.execute_values(
            cur,
            f"""INSERT INTO {BLOCKED_DOMAIN_BACKOFF_TABLE}
                    (url, domain, queue_class, priority, next_fetch_at, added_at, quarantined_at, branch_key)
                VALUES %s
                ON CONFLICT (url) DO UPDATE
                SET domain = EXCLUDED.domain,
                    queue_class = EXCLUDED.queue_class,
                    priority = EXCLUDED.priority,
                    next_fetch_at = EXCLUDED.next_fetch_at,
                    added_at = EXCLUDED.added_at,
                    quarantined_at = EXCLUDED.quarantined_at,
                    branch_key = EXCLUDED.branch_key""",
            blocked_rows,
            page_size=200,
        )

    def _delete_active_leases(self, cur, urls: list[str]) -> None:
        """Remove URLs from the physical active lease table."""
        if not urls:
            return
        cur.execute(f"DELETE FROM {LEASE_TABLE} WHERE url = ANY(%s)", (urls,))

    def _upsert_active_leases(
        self,
        cur,
        rows: list[tuple[str, str, str, str, float]],
    ) -> None:
        """Replace active lease rows using explicit lease-table state."""
        normalized_urls = sorted({normalize_url(url) for url, *_ in rows if url})
        if not normalized_urls:
            return
        self._delete_active_leases(cur, normalized_urls)
        psycopg2.extras.execute_values(
            cur,
            f"""INSERT INTO {LEASE_TABLE}
                    (url, domain, queue_class, lease_token, lease_expires_at)
                VALUES %s
                ON CONFLICT (url) DO UPDATE
                SET domain = EXCLUDED.domain,
                    queue_class = EXCLUDED.queue_class,
                    lease_token = EXCLUDED.lease_token,
                    lease_expires_at = EXCLUDED.lease_expires_at""",
            [
                (
                    normalize_url(url),
                    domain,
                    self._normalize_queue_class(queue_class),
                    lease_token,
                    lease_expires_at,
                )
                for url, domain, queue_class, lease_token, lease_expires_at in rows
            ],
            page_size=200,
        )

    def _replace_pending_queue_rows(
        self,
        cur,
        rows: list[tuple[str, str, float, float, float, str]],
    ) -> None:
        """Replace physical pending queue rows using returned frontier state."""
        normalized_urls = sorted({normalize_url(url) for url, *_ in rows if url})
        if not normalized_urls:
            return
        self._delete_queue_entries(cur, normalized_urls)
        self._insert_pending_queue_rows(cur, rows)

    def _project_pending_queue_rows(
        self,
        rows: list[tuple[object, ...]],
    ) -> list[tuple[str, str, float, float, float, str]]:
        """Project URL ledger rows into the queue-table row shape."""
        projected: list[tuple[str, str, float, float, float, str]] = []
        for row in rows:
            if len(row) == 6:
                projected.append(row)  # type: ignore[arg-type]
                continue
            if len(row) >= 8:
                projected.append((row[0], row[1], row[2], row[3], row[4], row[5]))  # type: ignore[arg-type]
                continue
            raise ValueError(f"unexpected url ledger row shape: {len(row)}")
        return projected

    def requeue_urls(
        self,
        urls: list[str],
        *,
        queue_class: str,
        next_fetch_at: float | None = None,
        current_statuses: list[str] | None = None,
    ) -> int:
        """Move known URLs back into a pending queue class and synchronize scheduler state."""
        normalized_urls = sorted({normalize_url(url) for url in urls if url})
        if not normalized_urls:
            return 0

        scheduled_at = time.time() if next_fetch_at is None else next_fetch_at
        normalized_queue_class = self._normalize_queue_class(queue_class)

        with self._conn.cursor() as cur:
            if current_statuses is None:
                cur.execute(
                    f"""UPDATE {URL_LEDGER_TABLE}
                        SET next_fetch_at = %s,
                            terminal_reason = NULL,
                            terminalized_at = NULL
                        WHERE url = ANY(%s)
                        RETURNING url, domain, priority, next_fetch_at, added_at""",
                    (scheduled_at, normalized_urls),
                )
            else:
                cur.execute(
                    f"""UPDATE {URL_LEDGER_TABLE}
                        SET next_fetch_at = %s,
                            terminal_reason = NULL,
                            terminalized_at = NULL
                        WHERE url = ANY(%s)
                        RETURNING url, domain, priority, next_fetch_at, added_at""",
                    (scheduled_at, normalized_urls),
                )
            rows = cur.fetchall()
            pending_rows = [
                (url, domain, priority, next_fetch_at, added_at, normalized_queue_class)
                for url, domain, priority, next_fetch_at, added_at in rows
            ]
            self._replace_pending_queue_rows(cur, pending_rows)
            self._delete_active_leases(cur, [row[0] for row in pending_rows])
            count = len(pending_rows)

        self._conn.commit()
        return count

    def _upsert_tasks(self, tasks: list[CrawlTask]) -> int:
        """Insert new tasks and promote existing metadata when a better discovery wins."""
        if not tasks:
            return 0

        prepared_tasks = self._prepare_tasks(tasks)
        prepared_by_url = {task.url: task for task in prepared_tasks}
        rows = []
        for task in prepared_tasks:
            domain = urlparse(task.url).netloc
            next_fetch_at = task.next_fetch_at or task.added_at or time.time()
            rows.append(
                (
                    task.url,
                    domain,
                    task.priority,
                    task.source_url,
                    task.added_at,
                    next_fetch_at,
                )
            )

        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    f"""INSERT INTO {URL_LEDGER_TABLE} (
                           url, domain, priority, source_url, added_at, next_fetch_at
                       )
                       VALUES %s
                       ON CONFLICT (url) DO UPDATE SET
                           priority = GREATEST({URL_LEDGER_TABLE}.priority, EXCLUDED.priority),
                           source_url = COALESCE({URL_LEDGER_TABLE}.source_url, EXCLUDED.source_url),
                           added_at = LEAST({URL_LEDGER_TABLE}.added_at, EXCLUDED.added_at),
                           next_fetch_at = LEAST({URL_LEDGER_TABLE}.next_fetch_at, EXCLUDED.next_fetch_at)
                       WHERE
                           EXCLUDED.priority > {URL_LEDGER_TABLE}.priority
                           OR ({URL_LEDGER_TABLE}.source_url IS NULL AND EXCLUDED.source_url IS NOT NULL)
                           OR EXCLUDED.next_fetch_at < {URL_LEDGER_TABLE}.next_fetch_at
                       RETURNING url, domain, priority, next_fetch_at, added_at""",
                    rows,
                    page_size=200,
                )
                ledger_rows = cur.fetchall()
                pending_rows = [
                    (
                        url,
                        domain,
                        priority,
                        next_fetch_at,
                        added_at,
                        prepared_by_url[url].queue_class or QUEUE_BACKLOG,
                    )
                    for url, domain, priority, next_fetch_at, added_at in ledger_rows
                ]
                self._replace_pending_queue_rows(cur, pending_rows)
                self._delete_active_leases(cur, [row[0] for row in pending_rows])
                return len(ledger_rows)
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to upsert batch of %d URLs", len(tasks))
            return 0

    def preview_tasks(self, tasks: list[CrawlTask]) -> list[CrawlTask]:
        """Return normalized tasks with queue classes applied without writing them."""
        return self._prepare_tasks(tasks)

    def place(self, task: CrawlTask) -> bool:
        """Place one discovered URL candidate into scheduler storage."""
        return self._upsert_tasks([task]) > 0

    def place_many(self, tasks: list[CrawlTask]) -> int:
        """Place multiple discovered URL candidates into scheduler storage."""
        return self._upsert_tasks(tasks)

    def add(self, task: CrawlTask) -> bool:
        """Backward-compatible alias for place()."""
        return self.place(task)

    def add_many(self, tasks: list[CrawlTask]) -> int:
        """Backward-compatible alias for place_many()."""
        return self.place_many(tasks)

    def lease_next(
        self,
        domain: str | None = None,
        lease_seconds: float | None = None,
        prioritize_breadth: bool = False,
        exclude_domains: list[str] | None = None,
        queue_classes: list[str] | None = None,
    ) -> CrawlTask | None:
        """Lease the next ready URL, optionally filtered by domain."""
        normalized_queue_classes = self._normalized_queue_classes(queue_classes)
        if len(normalized_queue_classes) != 1:
            for queue_class in normalized_queue_classes:
                task = self.lease_next(
                    domain=domain,
                    lease_seconds=lease_seconds,
                    prioritize_breadth=prioritize_breadth,
                    exclude_domains=exclude_domains,
                    queue_classes=[queue_class],
                )
                if task is not None:
                    return task
            return None

        now = time.time()
        lease_token = uuid.uuid4().hex
        duration = self._lease_seconds if lease_seconds is None else lease_seconds
        lease_expires_at = now + duration
        candidate_from_params: list[object] = []
        ready_sql = self._queue_ready_sql(
            alias="candidate",
            now=now,
            domain=domain,
            exclude_domains=exclude_domains,
        )
        where_sql = ready_sql.where
        if prioritize_breadth:
            host_ready_sql = self._queue_ready_sql(
                alias="host_source",
                now=now,
                domain=domain,
                exclude_domains=exclude_domains,
            )
            candidate_from = f"FROM {self._queue_table_sql(normalized_queue_classes[0])} AS candidate"
            where_sql = (
                f"{ready_sql.where} AND "
                f"{self._host_first_domain_selector_sql(queue_class=normalized_queue_classes[0], ready_sql=host_ready_sql)}"
            )
            candidate_from_params = list(host_ready_sql.params)
            order_by = f"{self._lease_order_by_sql('candidate', prioritize_breadth=True)}, candidate.url ASC"
        else:
            candidate_from = f"FROM {self._queue_table_sql(normalized_queue_classes[0])} AS candidate"
            order_by = self._lease_order_by_sql("candidate", prioritize_breadth=prioritize_breadth)
        params: list[object] = [*candidate_from_params, *ready_sql.params]

        try:
            self._recover_leased_locked(now, expired_only=True)
            with self._conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE {URL_LEDGER_TABLE}
                        SET next_fetch_at = next_fetch_at
                        WHERE url = (
                            SELECT candidate.url
                            {candidate_from}
                            WHERE {where_sql}
                            ORDER BY {order_by}
                            LIMIT 1
                            FOR UPDATE OF candidate SKIP LOCKED
                        )
                        RETURNING
                            url,
                            priority,
                            source_url,
                            added_at,
                            next_fetch_at,
                            domain""",
                    params,
                )
                row = cur.fetchone()
                if row:
                    self._delete_queue_entries(cur, [row[0]])
                    self._upsert_active_leases(
                        cur,
                        [(row[0], row[5], normalized_queue_classes[0], lease_token, lease_expires_at)],
                    )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to lease next URL")
            return None

        if row:
            (
                url,
                priority,
                source_url,
                added_at,
                next_fetch_at,
                _domain,
            ) = row
            return CrawlTask(
                url=url, priority=priority,
                source_url=source_url, added_at=added_at,
                next_fetch_at=next_fetch_at,
                lease_token=lease_token, lease_expires_at=lease_expires_at,
            )
        return None

    def lease_batch(
        self,
        count: int = 10,
        domain: str | None = None,
        lease_seconds: float | None = None,
        prioritize_breadth: bool = False,
        exclude_domains: list[str] | None = None,
        queue_classes: list[str] | None = None,
    ) -> list[CrawlTask]:
        """Lease a batch of ready URLs."""
        normalized_queue_classes = self._normalized_queue_classes(queue_classes)
        if len(normalized_queue_classes) != 1:
            tasks: list[CrawlTask] = []
            while len(tasks) < count:
                task = self.lease_next(
                    domain=domain,
                    lease_seconds=lease_seconds,
                    prioritize_breadth=prioritize_breadth,
                    exclude_domains=exclude_domains,
                    queue_classes=normalized_queue_classes,
                )
                if task is None:
                    break
                tasks.append(task)
            return tasks

        now = time.time()
        lease_token = uuid.uuid4().hex
        duration = self._lease_seconds if lease_seconds is None else lease_seconds
        lease_expires_at = now + duration
        candidate_from_params: list[object] = []
        ready_sql = self._queue_ready_sql(
            alias="candidate",
            now=now,
            domain=domain,
            exclude_domains=exclude_domains,
        )
        candidate_from = f"FROM {self._queue_table_sql(normalized_queue_classes[0])} AS candidate"
        if prioritize_breadth:
            order_by = f"{self._lease_order_by_sql('candidate', prioritize_breadth=True)}, candidate.url ASC"
        else:
            order_by = self._lease_order_by_sql("candidate", prioritize_breadth=prioritize_breadth)
        params: list[object] = [*candidate_from_params, *ready_sql.params, count]

        try:
            self._recover_leased_locked(now, expired_only=True)
            with self._conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE {URL_LEDGER_TABLE}
                        SET next_fetch_at = next_fetch_at
                        WHERE url IN (
                            SELECT candidate.url
                            {candidate_from}
                            WHERE {ready_sql.where}
                            ORDER BY {order_by}
                            LIMIT %s
                            FOR UPDATE OF candidate SKIP LOCKED
                        )
                        RETURNING
                            url,
                            priority,
                            source_url,
                            added_at,
                            next_fetch_at,
                            domain""",
                    params,
                )
                rows = cur.fetchall()
                if rows:
                    self._delete_queue_entries(cur, [row[0] for row in rows])
                    self._upsert_active_leases(
                        cur,
                        [(row[0], row[5], normalized_queue_classes[0], lease_token, lease_expires_at) for row in rows],
                    )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to lease batch of URLs")
            return []

        return [
            CrawlTask(
                url=url,
                priority=priority,
                source_url=source_url,
                added_at=added_at,
                next_fetch_at=next_fetch_at,
                lease_token=lease_token,
                lease_expires_at=lease_expires_at,
            )
            for (
                url,
                priority,
                source_url,
                added_at,
                next_fetch_at,
                _domain,
            ) in rows
        ]

    def mark_done(self, url: str, lease_token: str | None = None) -> bool:
        """Mark a URL as successfully crawled."""
        normalized = normalize_url(url)
        now = time.time()
        lease_sql, lease_params = self._lease_match_sql("ledger", lease_token)

        with self._conn.cursor() as cur:
            cur.execute(
                f"""UPDATE {URL_LEDGER_TABLE} AS ledger
                    SET next_fetch_at = %s,
                        last_success_at = %s,
                        fail_streak = 0,
                        last_error = NULL,
                        terminal_reason = NULL,
                        terminalized_at = NULL
                    WHERE url = %s{lease_sql}
                    RETURNING url""",
                (now, now, normalized, *lease_params),
            )
            rows = cur.fetchall()
            self._delete_queue_entries(cur, [row[0] for row in rows])
            self._delete_active_leases(cur, [row[0] for row in rows])
            updated = bool(rows)
        self._conn.commit()
        return updated

    def mark_failed(
        self,
        url: str,
        retryable: bool = False,
        error: str | None = None,
        backoff_seconds: float | None = None,
        lease_token: str | None = None,
    ) -> bool:
        """Mark a URL as failed, optionally scheduling a retry."""
        normalized = normalize_url(url)
        now = time.time()
        lease_sql, lease_params = self._lease_match_sql("ledger", lease_token)

        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT fail_streak, priority FROM {URL_LEDGER_TABLE} AS ledger WHERE url = %s{lease_sql} FOR UPDATE",
                (normalized, *lease_params),
            )
            row = cur.fetchone()
            if row is None:
                self._conn.rollback()
                return False

            next_fail_streak = row[0] + 1
            next_priority = self._compute_retry_priority(row[1], next_fail_streak)
            retry_delay = backoff_seconds
            if retryable and retry_delay is None:
                retry_delay = self._compute_retry_backoff(next_fail_streak)

            if retryable:
                next_fetch_at = now + (retry_delay or 0.0)
                cur.execute(
                    f"""UPDATE {URL_LEDGER_TABLE}
                        SET next_fetch_at = %s,
                            fail_streak = %s,
                            priority = %s,
                            last_error = %s,
                            terminal_reason = NULL,
                            terminalized_at = NULL
                        WHERE url = %s{lease_sql}
                        RETURNING url, domain, priority, next_fetch_at, added_at""",
                    (
                        next_fetch_at,
                        next_fail_streak,
                        next_priority,
                        error,
                        normalized,
                        *lease_params,
                    ),
                )
                rows = cur.fetchall()
                pending_rows = [
                    (url, domain, priority, next_fetch_at, added_at, QUEUE_BACKLOG)
                    for url, domain, priority, next_fetch_at, added_at in rows
                ]
                self._replace_pending_queue_rows(cur, pending_rows)
                self._delete_active_leases(cur, [row[0] for row in pending_rows])
                updated = bool(rows)
            else:
                cur.execute(
                    f"""UPDATE {URL_LEDGER_TABLE}
                        SET next_fetch_at = %s,
                            fail_streak = %s,
                            priority = %s,
                            last_error = %s,
                            terminal_reason = %s,
                            terminalized_at = %s
                        WHERE url = %s{lease_sql}
                        RETURNING url""",
                    (
                        now,
                        next_fail_streak,
                        next_priority,
                        error,
                        error or "failed",
                        now,
                        normalized,
                        *lease_params,
                    ),
                )
                rows = cur.fetchall()
                urls = [row[0] for row in rows]
                self._delete_queue_entries(cur, urls)
                self._delete_active_leases(cur, urls)
                updated = bool(rows)
        self._conn.commit()
        return updated

    def requeue_failed(self) -> int:
        """Requeue failed URLs for retry."""
        now = time.time()
        with self._conn.cursor() as cur:
            cur.execute(
                f"""UPDATE {URL_LEDGER_TABLE}
                   SET next_fetch_at = %s,
                       terminal_reason = NULL,
                       terminalized_at = NULL
                   WHERE terminal_reason IS NOT NULL
                   RETURNING url, domain, priority, next_fetch_at, added_at""",
                (now,),
            )
            rows = cur.fetchall()
            pending_rows = [
                (url, domain, priority, next_fetch_at, added_at, QUEUE_BACKLOG)
                for url, domain, priority, next_fetch_at, added_at in rows
            ]
            self._replace_pending_queue_rows(cur, pending_rows)
            self._delete_active_leases(cur, [row[0] for row in pending_rows])
            count = len(pending_rows)
        self._conn.commit()
        return count

    def rebalance_blocked_domain_backoff(self, now: float | None = None) -> tuple[int, int]:
        """Move backoff-blocked URLs out of the normal scheduler queues."""
        return self._quarantine.rebalance(now=now)

    def retire_blocked_domain_backoff(
        self,
        *,
        min_consecutive_failures: int,
        min_quarantine_seconds: float,
        limit: int = 256,
        now: float | None = None,
    ) -> int:
        """Retire long-stuck blocked URLs out of pending scheduler state."""
        return self._quarantine.retire(
            min_consecutive_failures=min_consecutive_failures,
            min_quarantine_seconds=min_quarantine_seconds,
            limit=limit,
            now=now,
        )

    def restore_recovered_blocked_domain_backoff(
        self,
        *,
        limit: int,
        per_domain: int,
        now: float | None = None,
    ) -> int:
        """Restore blocked URLs whose domains have already recovered."""
        return self._quarantine.restore_recovered(limit=limit, per_domain=per_domain, now=now)

    def promote_blocked_domain_backoff(
        self,
        limit: int,
        *,
        per_domain: int = 1,
        max_consecutive_failures: int | None = None,
        now: float | None = None,
    ) -> int:
        """Promote a small cooled-down subset from blocked queue back into normal queues."""
        return self._quarantine.promote(
            limit,
            per_domain=per_domain,
            max_consecutive_failures=max_consecutive_failures,
            now=now,
        )

    def recover_leased(self, expired_only: bool = True) -> int:
        """Reset leased URLs back to pending."""
        count = self._recover_leased_locked(time.time(), expired_only=expired_only)
        self._conn.commit()
        return count

    def defer_overcrowded_backlog(
        self,
        *,
        keep_ready_per_domain: int = 128,
        keep_ready_per_branch: int = 16,
        defer_seconds: float = 1800.0,
    ) -> int:
        """Delay excess ready backlog so one host or branch cannot dominate ready work."""
        if keep_ready_per_domain <= 0 or keep_ready_per_branch <= 0:
            return 0

        now = time.time()
        deferred_until = now + defer_seconds
        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH ranked AS (
                        SELECT
                            queue.url,
                            ROW_NUMBER() OVER (
                                PARTITION BY queue.domain
                                ORDER BY queue.priority DESC, queue.next_fetch_at ASC, queue.added_at ASC, queue.url ASC
                            ) AS domain_rownum,
                            ROW_NUMBER() OVER (
                                PARTITION BY queue.domain, queue.branch_key
                                ORDER BY queue.priority DESC, queue.next_fetch_at ASC, queue.added_at ASC, queue.url ASC
                            ) AS branch_rownum
                        FROM {self._queue_table_sql(QUEUE_BACKLOG)} AS queue
                        WHERE queue.next_fetch_at <= %s
                    ), deferred AS (
                        SELECT ranked.url
                        FROM ranked
                        WHERE ranked.domain_rownum > %s
                           OR ranked.branch_rownum > %s
                    )
                    UPDATE {URL_LEDGER_TABLE}
                    SET next_fetch_at = GREATEST(next_fetch_at, %s)
                    WHERE url IN (SELECT url FROM deferred)
                    RETURNING url, domain, priority, next_fetch_at, added_at""",
                (
                    now,
                    keep_ready_per_domain,
                    keep_ready_per_branch,
                    deferred_until,
                ),
            )
            rows = cur.fetchall()
            pending_rows = [
                (url, domain, priority, next_fetch_at, added_at, QUEUE_BACKLOG)
                for url, domain, priority, next_fetch_at, added_at in rows
            ]
            self._replace_pending_queue_rows(cur, pending_rows)
            count = len(rows)
        self._conn.commit()
        return count

    def promote_backlog_host_heads(
        self,
        target_pending: int,
        *,
        per_domain: int = 1,
        candidate_limit: int = 200,
    ) -> int:
        """Promote one backlog head per host into exploration before using seed refill."""
        if target_pending <= 0 or per_domain <= 0 or candidate_limit <= 0:
            return 0

        current_exploration = self.pending_count(queue_classes=[QUEUE_EXPLORATION])
        needed = target_pending - current_exploration
        if needed <= 0:
            return 0

        with self._conn.cursor() as cur:
            cur.execute(
                f"""SELECT DISTINCT domain
                    FROM {self._queue_table_sql(QUEUE_EXPLORATION)}"""
            )
            existing_domains = {domain for (domain,) in cur.fetchall()}

            cur.execute(
                f"""SELECT url, domain
                    FROM {self._queue_table_sql(QUEUE_BACKLOG)}
                    ORDER BY priority DESC, added_at ASC, url ASC
                    LIMIT %s""",
                (max(candidate_limit, needed * 20),),
            )
            candidates = cur.fetchall()

            promoted_urls: list[str] = []
            domain_counts: Counter[str] = Counter()
            for url, domain in candidates:
                if domain in existing_domains:
                    continue
                if domain_counts[domain] >= per_domain:
                    continue
                promoted_urls.append(normalize_url(url))
                existing_domains.add(domain)
                domain_counts[domain] += 1
                if len(promoted_urls) >= needed:
                    break

            if not promoted_urls:
                return 0

            cur.execute(
                f"""SELECT url, domain, priority, next_fetch_at, added_at
                    FROM {self._queue_table_sql(QUEUE_BACKLOG)}
                    WHERE url = ANY(%s)""",
                (promoted_urls,),
            )
            rows = [
                (url, domain, priority, next_fetch_at, added_at, QUEUE_EXPLORATION)
                for url, domain, priority, next_fetch_at, added_at in cur.fetchall()
            ]
            self._delete_queue_entries(cur, [row[0] for row in rows])
            self._insert_pending_queue_rows(cur, rows)
            count = len(rows)

        self._conn.commit()
        return count

    def upsert_seeds(self, urls: list[str], priority: float = 2.0) -> int:
        """Insert or requeue seed URLs."""
        if not urls:
            return 0

        rows = []
        now = time.time()
        for url in urls:
            normalized = normalize_url(url)
            domain = urlparse(normalized).netloc
            rows.append((
                normalized,
                domain,
                priority,
                now,
                now,
            ))

        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"""INSERT INTO {URL_LEDGER_TABLE} (
                       url, domain, priority, source_url, added_at, next_fetch_at
                   )
                   VALUES %s
                   ON CONFLICT (url) DO UPDATE SET
                       added_at = EXCLUDED.added_at,
                       next_fetch_at = EXCLUDED.next_fetch_at,
                       priority = EXCLUDED.priority,
                       fail_streak = 0,
                       last_error = NULL,
                       terminal_reason = NULL,
                       terminalized_at = NULL
                   RETURNING url, domain, priority, next_fetch_at, added_at""",
                rows,
                template="(%s, %s, %s, NULL, %s, %s)",
                page_size=200,
            )
            ledger_rows = cur.fetchall()
            pending_rows = [
                (url, domain, priority, next_fetch_at, added_at, QUEUE_EXPLORATION)
                for url, domain, priority, next_fetch_at, added_at in ledger_rows
            ]
            self._replace_pending_queue_rows(cur, pending_rows)
            self._delete_active_leases(cur, [row[0] for row in pending_rows])
            affected = len(ledger_rows)
        self._conn.commit()
        return affected

    def stats(self) -> dict:
        """Get queue statistics."""
        return self._observability.status_counts()

    def pending_count(self, queue_classes: list[str] | None = None) -> int:
        """Get count of pending URLs, optionally filtered by queue class."""
        normalized_queue_classes = self._normalized_queue_classes(queue_classes)
        return self._observability.pending_count(normalized_queue_classes)

    def pending_domain_count(self, queue_classes: list[str] | None = None) -> int:
        """Get count of distinct pending domains, optionally filtered by queue class."""
        normalized_queue_classes = self._normalized_queue_classes(queue_classes)
        return self._observability.pending_domain_count(normalized_queue_classes)

    def ready_domain_count(
        self,
        now: float | None = None,
        queue_classes: list[str] | None = None,
    ) -> int:
        """Get count of distinct domains that are leaseable right now."""
        normalized_queue_classes = self._normalized_queue_classes(queue_classes)
        return self._observability.ready_domain_count(now=now, queue_classes=normalized_queue_classes)

    def blocked_domain_backoff_count(self) -> int:
        """Return count of URLs isolated due to host backoff."""
        return self._observability.blocked_count()

    def readiness(
        self,
        now: float | None = None,
        queue_classes: list[str] | None = None,
    ) -> FrontierReadiness:
        """Return a single snapshot of pending and leaseable queue state."""
        normalized_queue_classes = self._normalized_queue_classes(queue_classes)
        return self._observability.readiness(now=now, queue_classes=normalized_queue_classes)

    def ready_count(
        self,
        now: float | None = None,
        queue_classes: list[str] | None = None,
    ) -> int:
        """Get count of pending URLs that are leaseable right now."""
        return self.readiness(now=now, queue_classes=queue_classes).ready

    def next_ready_delay(
        self,
        now: float | None = None,
        queue_classes: list[str] | None = None,
    ) -> float | None:
        """Return seconds until the next pending URL becomes leaseable."""
        return self.readiness(now=now, queue_classes=queue_classes).next_ready_delay

    def is_seen(self, url: str) -> bool:
        """Check if URL exists in the URL ledger."""
        normalized = normalize_url(url)
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {URL_LEDGER_TABLE} WHERE url = %s LIMIT 1", (normalized,))
            return cur.fetchone() is not None


# Temporary compatibility alias while callers migrate away from the frontier name.
Frontier = UrlLedger
