"""URL Frontier with PostgreSQL persistence."""

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
from .discovery import (
    ARCHETYPE_GENERIC_PAGE,
    ARCHETYPE_REDIRECT_HUB,
    ARCHETYPE_REGISTRY_LISTING,
    DISCOVERY_EXTERNAL,
    DISCOVERY_SAME_HOST,
    DISCOVERY_SEED,
    DISCOVERY_SEED_HOST,
    discovery_rank,
)
from .schema import assert_public_table_columns
from .urls import normalize_url, url_branch_key

if TYPE_CHECKING:
    from .domain_store import DomainStore

logger = logging.getLogger(__name__)

PENDING_STATUS = "pending"
LEASED_STATUS = "leased"
DONE_STATUS = "done"
FAILED_STATUS = "failed"
QUEUE_EXPLORATION = "exploration"
QUEUE_BACKLOG = "backlog"
QUEUE_RECRAWL = "recrawl"

EXPLORATION_DOMAIN_BUDGET = 8
DEFAULT_LEASE_SECONDS = 300.0
DEFAULT_RETRY_BACKOFF_SECONDS = 30.0
MAX_RETRY_BACKOFF_SECONDS = 1800.0
RETRY_PRIORITY_DECAY = 0.6
MIN_RETRY_PRIORITY = 0.25
FRONTIER_REQUIRED_COLUMNS = {
    "url",
    "domain",
    "depth",
    "priority",
    "queue_class",
    "discovery_kind",
    "archetype",
    "source_url",
    "added_at",
    "status",
    "next_fetch_at",
    "last_success_at",
    "fail_streak",
    "lease_token",
    "lease_expires_at",
    "last_error",
}
FRONTIER_ALLOWED_STATUSES = {
    PENDING_STATUS,
    LEASED_STATUS,
    DONE_STATUS,
    FAILED_STATUS,
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
LEASE_TABLE = "frontier_lease_active"
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
    "branch_key",
}

@dataclass
class CrawlTask:
    """A URL to crawl with metadata."""

    url: str
    depth: int
    priority: float = 1.0
    queue_class: str | None = None
    discovery_kind: str = DISCOVERY_SEED
    archetype: str = ARCHETYPE_GENERIC_PAGE
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
class FrontierReadiness:
    """Summary of the current pending queue readiness."""

    pending: int
    ready: int
    next_ready_delay: float | None
    blocked: dict[str, int]
    state_counts: dict[str, int]


@dataclass(frozen=True)
class _ReadySql:
    """SQL fragments for pending URL readiness checks."""

    where: str
    params: tuple[object, ...]
    ready_at: str


class Frontier:
    """URL frontier with PostgreSQL persistence. Dedup via ON CONFLICT."""

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
        exclude_branch_keys: list[str] | None = None,
        exclude_domain_branches: list[tuple[str, str]] | None = None,
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

        if exclude_branch_keys:
            conditions.append(f"NOT ({alias}.branch_key = ANY(%s))")
            params.append(exclude_branch_keys)

        if exclude_domain_branches:
            placeholders = ", ".join(["(%s, %s)"] * len(exclude_domain_branches))
            conditions.append(
                "NOT EXISTS ("
                f"SELECT 1 FROM (VALUES {placeholders}) AS active(domain, branch_key) "
                f"WHERE active.domain = {alias}.domain "
                f"AND active.branch_key = {alias}.branch_key"
                ")"
            )
            for active_domain, active_branch in exclude_domain_branches:
                params.extend([active_domain, active_branch])

        return _ReadySql(
            where=" AND ".join(conditions),
            params=tuple(params),
            ready_at=f"GREATEST({alias}.next_fetch_at, {next_request_sql}, {backoff_sql})",
        )

    def _pending_queue_union_sql(self, queue_classes: list[str] | None = None) -> str:
        """Return UNION ALL SQL across the selected physical pending queue tables."""
        normalized_queue_classes = self._normalized_queue_classes(queue_classes)
        selects = [
            f"SELECT url, domain, next_fetch_at FROM {self._queue_table_sql(queue_class)}"
            for queue_class in normalized_queue_classes
        ]
        return "\nUNION ALL\n".join(selects)

    def _blocked_queue_sql(self, queue_classes: list[str] | None = None) -> tuple[str, tuple[object, ...]]:
        """Return SQL for blocked-domain-backoff rows, optionally filtered by queue class."""
        normalized_queue_classes = self._normalized_queue_classes(queue_classes)
        sql = f"SELECT url, domain, next_fetch_at FROM {BLOCKED_DOMAIN_BACKOFF_TABLE}"
        if queue_classes:
            sql += " WHERE queue_class = ANY(%s)"
            return sql, (normalized_queue_classes,)
        return sql, ()

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
                f"""WITH active_leases AS (
                        SELECT url
                        FROM {LEASE_TABLE}
                        WHERE {where}
                    )
                    UPDATE frontier
                    SET status = '{PENDING_STATUS}',
                        lease_token = NULL,
                        lease_expires_at = NULL
                    WHERE url IN (SELECT url FROM active_leases)
                    RETURNING url, domain, priority, next_fetch_at, added_at, queue_class, lease_token, lease_expires_at, status""",
                params,
            )
            rows = cur.fetchall()
            self._replace_pending_queue_rows(cur, self._project_pending_queue_rows(rows))
            self._replace_active_lease_rows(cur, [(row[0], row[1], row[5], row[6], row[7], row[8]) for row in rows])
            return len(rows)

    def _assert_current_schema(self) -> None:
        assert_public_table_columns(self._conn, "frontier", FRONTIER_REQUIRED_COLUMNS)

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

        with self._conn.cursor() as cur:
            cur.execute("SELECT DISTINCT status FROM frontier")
            invalid_statuses = sorted(
                status
                for (status,) in cur.fetchall()
                if status not in FRONTIER_ALLOWED_STATUSES
            )
        if invalid_statuses:
            invalid = ", ".join(invalid_statuses)
            raise RuntimeError(f"frontier contains unsupported statuses: {invalid}")

        with self._conn.cursor() as cur:
            cur.execute("SELECT DISTINCT queue_class FROM frontier")
            invalid_queue_classes = sorted(
                queue_class
                for (queue_class,) in cur.fetchall()
                if queue_class not in FRONTIER_ALLOWED_QUEUE_CLASSES
            )
        if invalid_queue_classes:
            invalid = ", ".join(invalid_queue_classes)
            raise RuntimeError(f"frontier contains unsupported queue classes: {invalid}")

    def _normalize_queue_class(self, queue_class: str | None) -> str:
        """Return a supported frontier queue class."""
        if queue_class in FRONTIER_ALLOWED_QUEUE_CLASSES:
            return queue_class
        return QUEUE_BACKLOG

    def _classify_queue(self, task: CrawlTask, *, known_count: int = 0) -> str:
        """Map a task into the queue class used by the scheduler."""
        if task.queue_class in FRONTIER_ALLOWED_QUEUE_CLASSES:
            return task.queue_class
        if task.discovery_kind == DISCOVERY_SEED:
            return QUEUE_EXPLORATION
        if task.archetype in {ARCHETYPE_REGISTRY_LISTING, ARCHETYPE_REDIRECT_HUB}:
            return QUEUE_BACKLOG
        if known_count >= EXPLORATION_DOMAIN_BUDGET:
            return QUEUE_BACKLOG
        if task.discovery_kind == DISCOVERY_SAME_HOST:
            return QUEUE_EXPLORATION if task.depth <= 2 else QUEUE_BACKLOG
        if task.discovery_kind in {DISCOVERY_SEED_HOST, DISCOVERY_EXTERNAL}:
            return QUEUE_EXPLORATION if task.depth <= 3 else QUEUE_BACKLOG
        if task.depth <= 1:
            return QUEUE_EXPLORATION
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

    def _is_better_task(self, candidate: CrawlTask, current: CrawlTask) -> bool:
        """Return True when candidate should replace current task metadata."""
        if candidate.priority != current.priority:
            return candidate.priority > current.priority
        return discovery_rank(candidate.discovery_kind) > discovery_rank(current.discovery_kind)

    def _merge_task(self, current: CrawlTask, candidate: CrawlTask) -> CrawlTask:
        """Merge duplicate task metadata before bulk upsert."""
        preferred = candidate if self._is_better_task(candidate, current) else current
        return CrawlTask(
            url=preferred.url,
            depth=min(current.depth, candidate.depth),
            priority=preferred.priority,
            queue_class=self._merge_queue_class(current.queue_class, candidate.queue_class),
            discovery_kind=preferred.discovery_kind,
            archetype=preferred.archetype,
            source_url=preferred.source_url or current.source_url or candidate.source_url,
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
                depth=task.depth,
                priority=task.priority,
                queue_class=task.queue_class,
                discovery_kind=task.discovery_kind,
                archetype=task.archetype,
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
                    depth=task.depth,
                    priority=task.priority,
                    queue_class=self._classify_queue(task, known_count=known_count),
                    discovery_kind=task.discovery_kind,
                    archetype=task.archetype,
                    source_url=task.source_url,
                    added_at=task.added_at,
                    next_fetch_at=task.next_fetch_at,
                )
            )
            batch_counts[domain] += 1
        return prepared

    def _discovery_rank_sql(self, column: str) -> str:
        """Return SQL that maps discovery kind to a comparable rank."""
        return (
            f"CASE {column} "
            f"WHEN 'external' THEN 1 "
            f"WHEN 'seed_host' THEN 2 "
            f"WHEN 'same_host' THEN 3 "
            f"WHEN 'seed' THEN 4 "
            f"ELSE 0 END"
        )

    def get_domain_known_counts(self, domains: set[str]) -> dict[str, int]:
        """Return known URL counts per domain from the frontier."""
        known_domains = {domain for domain in domains if domain}
        if not known_domains:
            return {}
        with self._conn.cursor() as cur:
            cur.execute(
                """SELECT domain, COUNT(*)
                   FROM frontier
                   WHERE domain = ANY(%s)
                   GROUP BY domain""",
                (sorted(known_domains),),
            )
            return {domain: count for domain, count in cur.fetchall()}

    def _lease_order_by_sql(self, alias: str, prioritize_breadth: bool) -> str:
        """Return the ORDER BY clause used for lease selection."""
        if prioritize_breadth:
            return (
                f"{alias}.next_fetch_at ASC, "
                f"{alias}.added_at ASC, "
                f"{alias}.priority DESC"
            )

        return (
            f"{alias}.priority DESC, "
            f"{alias}.next_fetch_at ASC, "
            f"{alias}.added_at ASC"
        )

    def _branch_breadth_candidate_from_sql(
        self,
        *,
        queue_class: str,
        ranked_ready_sql: _ReadySql,
    ) -> str:
        """Return a queue FROM clause with domain/branch breadth ranks attached."""
        table_name = self._queue_table_sql(queue_class)
        ranked_order = self._lease_order_by_sql("ranked_source", prioritize_breadth=True)
        return f"""FROM {table_name} AS candidate
                    JOIN (
                        SELECT
                            ranked_source.url,
                            ROW_NUMBER() OVER (
                                PARTITION BY ranked_source.domain
                                ORDER BY {ranked_order}, ranked_source.url ASC
                            ) AS domain_rownum,
                            ROW_NUMBER() OVER (
                                PARTITION BY ranked_source.domain, ranked_source.branch_key
                                ORDER BY {ranked_order}, ranked_source.url ASC
                            ) AS branch_rownum
                        FROM {table_name} AS ranked_source
                        WHERE {ranked_ready_sql.where}
                    ) AS ranked ON ranked.url = candidate.url"""

    def _branch_breadth_order_by_sql(self, alias: str) -> str:
        """Return ORDER BY that takes the first ready URL from each branch before repeats."""
        return (
            "ranked.branch_rownum ASC, "
            "ranked.domain_rownum ASC, "
            f"{self._lease_order_by_sql(alias, prioritize_breadth=True)}, "
            f"{alias}.url ASC"
        )

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
        rows: list[tuple[str, str, float, float, float, str, str]],
    ) -> None:
        """Insert scheduler-pending rows into the appropriate physical queue tables."""
        grouped: dict[str, list[tuple[str, str, float, float, float, str]]] = {
            queue_class: [] for queue_class in FRONTIER_ALLOWED_QUEUE_CLASSES
        }
        for url, domain, priority, next_fetch_at, added_at, queue_class, _status in rows:
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
        rows: list[tuple[str, str, float, float, float, str, str]],
    ) -> None:
        """Insert URLs into the blocked-domain-backoff physical queue."""
        blocked_rows = [
            (
                normalize_url(url),
                domain,
                self._normalize_queue_class(queue_class),
                priority,
                next_fetch_at,
                added_at,
                url_branch_key(normalize_url(url)),
            )
            for url, domain, priority, next_fetch_at, added_at, queue_class, status in rows
            if status == PENDING_STATUS
        ]
        if not blocked_rows:
            return
        psycopg2.extras.execute_values(
            cur,
            f"""INSERT INTO {BLOCKED_DOMAIN_BACKOFF_TABLE}
                    (url, domain, queue_class, priority, next_fetch_at, added_at, branch_key)
                VALUES %s
                ON CONFLICT (url) DO UPDATE
                SET domain = EXCLUDED.domain,
                    queue_class = EXCLUDED.queue_class,
                    priority = EXCLUDED.priority,
                    next_fetch_at = EXCLUDED.next_fetch_at,
                    added_at = EXCLUDED.added_at,
                    branch_key = EXCLUDED.branch_key""",
            blocked_rows,
            page_size=200,
        )

    def _delete_active_leases(self, cur, urls: list[str]) -> None:
        """Remove URLs from the physical active lease table."""
        if not urls:
            return
        cur.execute(f"DELETE FROM {LEASE_TABLE} WHERE url = ANY(%s)", (urls,))

    def _replace_active_lease_rows(
        self,
        cur,
        rows: list[tuple[str, str, str, str | None, float | None, str]],
    ) -> None:
        """Replace active lease rows using returned frontier state."""
        normalized_urls = sorted({normalize_url(url) for url, *_ in rows if url})
        if not normalized_urls:
            return
        self._delete_active_leases(cur, normalized_urls)

        active_rows: list[tuple[str, str, str, str, float]] = []
        for url, domain, queue_class, lease_token, lease_expires_at, status in rows:
            if status != LEASED_STATUS or not lease_token or lease_expires_at is None:
                continue
            active_rows.append((
                normalize_url(url),
                domain,
                self._normalize_queue_class(queue_class),
                lease_token,
                lease_expires_at,
            ))

        if not active_rows:
            return
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
            active_rows,
            page_size=200,
        )

    def _replace_pending_queue_rows(
        self,
        cur,
        rows: list[tuple[str, str, float, float, float, str, str]],
    ) -> None:
        """Replace physical pending queue rows using returned frontier state."""
        normalized_urls = sorted({normalize_url(url) for url, *_ in rows if url})
        if not normalized_urls:
            return
        self._delete_queue_entries(cur, normalized_urls)
        self._insert_pending_queue_rows(
            cur,
            [row for row in rows if row[6] == PENDING_STATUS],
        )

    def _project_pending_queue_rows(
        self,
        rows: list[tuple[object, ...]],
    ) -> list[tuple[str, str, float, float, float, str, str]]:
        """Project frontier rows into the queue-table row shape."""
        projected: list[tuple[str, str, float, float, float, str, str]] = []
        for row in rows:
            if len(row) == 7:
                projected.append(row)  # type: ignore[arg-type]
                continue
            if len(row) >= 9:
                projected.append((row[0], row[1], row[2], row[3], row[4], row[5], row[8]))  # type: ignore[arg-type]
                continue
            raise ValueError(f"unexpected frontier row shape: {len(row)}")
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
        statuses = current_statuses or [DONE_STATUS, FAILED_STATUS]

        with self._conn.cursor() as cur:
            cur.execute(
                f"""UPDATE frontier
                    SET status = '{PENDING_STATUS}',
                        queue_class = %s,
                        next_fetch_at = %s,
                        lease_token = NULL,
                        lease_expires_at = NULL
                    WHERE url = ANY(%s)
                      AND status = ANY(%s)
                    RETURNING url, domain, priority, next_fetch_at, added_at, queue_class, lease_token, lease_expires_at, status""",
                (queue_class, scheduled_at, normalized_urls, statuses),
            )
            rows = cur.fetchall()
            self._replace_pending_queue_rows(cur, self._project_pending_queue_rows(rows))
            self._replace_active_lease_rows(cur, [(row[0], row[1], row[5], row[6], row[7], row[8]) for row in rows])
            count = len(rows)

        self._conn.commit()
        return count

    def _upsert_tasks(self, tasks: list[CrawlTask]) -> int:
        """Insert new tasks and promote existing metadata when a better discovery wins."""
        if not tasks:
            return 0

        rows = []
        for task in self._prepare_tasks(tasks):
            domain = urlparse(task.url).netloc
            next_fetch_at = task.next_fetch_at or task.added_at or time.time()
            rows.append(
                (
                    task.url,
                    domain,
                    task.depth,
                    task.priority,
                    task.queue_class,
                    task.discovery_kind,
                    task.archetype,
                    task.source_url,
                    task.added_at,
                    next_fetch_at,
                )
            )

        existing_rank = self._discovery_rank_sql("frontier.discovery_kind")
        new_rank = self._discovery_rank_sql("EXCLUDED.discovery_kind")
        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    f"""INSERT INTO frontier (
                           url, domain, depth, priority, queue_class, discovery_kind, archetype, source_url, added_at, next_fetch_at
                       )
                       VALUES %s
                       ON CONFLICT (url) DO UPDATE SET
                           priority = GREATEST(frontier.priority, EXCLUDED.priority),
                           queue_class = CASE
                               WHEN frontier.queue_class = '{QUEUE_EXPLORATION}'
                                    OR EXCLUDED.queue_class = '{QUEUE_EXPLORATION}'
                                   THEN '{QUEUE_EXPLORATION}'
                               WHEN frontier.queue_class = '{QUEUE_BACKLOG}'
                                    OR EXCLUDED.queue_class = '{QUEUE_BACKLOG}'
                                   THEN '{QUEUE_BACKLOG}'
                               ELSE '{QUEUE_RECRAWL}'
                           END,
                           discovery_kind = CASE
                               WHEN {new_rank} > {existing_rank}
                                   THEN EXCLUDED.discovery_kind
                               ELSE frontier.discovery_kind
                           END,
                           archetype = CASE
                               WHEN EXCLUDED.priority > frontier.priority
                                   THEN EXCLUDED.archetype
                               WHEN {new_rank} > {existing_rank}
                                   THEN EXCLUDED.archetype
                               ELSE frontier.archetype
                           END,
                           source_url = COALESCE(frontier.source_url, EXCLUDED.source_url),
                           depth = LEAST(frontier.depth, EXCLUDED.depth),
                           added_at = LEAST(frontier.added_at, EXCLUDED.added_at),
                           next_fetch_at = LEAST(frontier.next_fetch_at, EXCLUDED.next_fetch_at)
                       WHERE
                           EXCLUDED.priority > frontier.priority
                           OR {new_rank} > {existing_rank}
                           OR EXCLUDED.depth < frontier.depth
                           OR (frontier.source_url IS NULL AND EXCLUDED.source_url IS NOT NULL)
                           OR EXCLUDED.next_fetch_at < frontier.next_fetch_at
                       RETURNING url, domain, priority, next_fetch_at, added_at, queue_class, lease_token, lease_expires_at, status""",
                    rows,
                    page_size=200,
                )
                frontier_rows = cur.fetchall()
                self._replace_pending_queue_rows(cur, self._project_pending_queue_rows(frontier_rows))
                self._replace_active_lease_rows(cur, [(row[0], row[1], row[5], row[6], row[7], row[8]) for row in frontier_rows])
                return len(frontier_rows)
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to upsert batch of %d URLs", len(tasks))
            return 0

    def preview_tasks(self, tasks: list[CrawlTask]) -> list[CrawlTask]:
        """Return normalized tasks with queue classes applied without writing them."""
        return self._prepare_tasks(tasks)

    def add(self, task: CrawlTask) -> bool:
        """Add a URL to the frontier. Returns True if inserted or metadata improved."""
        return self._upsert_tasks([task]) > 0

    def add_many(self, tasks: list[CrawlTask]) -> int:
        """Add multiple URLs. Existing rows are promoted when a better discovery wins."""
        return self._upsert_tasks(tasks)

    def lease_next(
        self,
        domain: str | None = None,
        lease_seconds: float | None = None,
        prioritize_breadth: bool = False,
        exclude_domains: list[str] | None = None,
        exclude_branch_keys: list[str] | None = None,
        exclude_domain_branches: list[tuple[str, str]] | None = None,
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
                    exclude_branch_keys=exclude_branch_keys,
                    exclude_domain_branches=exclude_domain_branches,
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
            exclude_branch_keys=exclude_branch_keys,
            exclude_domain_branches=exclude_domain_branches,
        )
        if prioritize_breadth:
            ranked_ready_sql = self._queue_ready_sql(
                alias="ranked_source",
                now=now,
                domain=domain,
                exclude_domains=exclude_domains,
                exclude_branch_keys=exclude_branch_keys,
                exclude_domain_branches=exclude_domain_branches,
            )
            candidate_from = self._branch_breadth_candidate_from_sql(
                queue_class=normalized_queue_classes[0],
                ranked_ready_sql=ranked_ready_sql,
            )
            candidate_from_params = list(ranked_ready_sql.params)
        else:
            candidate_from = f"FROM {self._queue_table_sql(normalized_queue_classes[0])} AS candidate"
        if prioritize_breadth:
            order_by = self._branch_breadth_order_by_sql("candidate")
        else:
            order_by = self._lease_order_by_sql("candidate", prioritize_breadth=prioritize_breadth)
        params: list[object] = [lease_token, lease_expires_at, *candidate_from_params, *ready_sql.params]

        try:
            self._recover_leased_locked(now, expired_only=True)
            with self._conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE frontier
                        SET status = '{LEASED_STATUS}',
                            lease_token = %s,
                            lease_expires_at = %s
                        WHERE url = (
                            SELECT candidate.url
                            {candidate_from}
                            WHERE {ready_sql.where}
                            ORDER BY {order_by}
                            LIMIT 1
                            FOR UPDATE OF candidate SKIP LOCKED
                        )
                        RETURNING
                            url,
                            depth,
                            priority,
                            discovery_kind,
                            archetype,
                            source_url,
                            added_at,
                            next_fetch_at,
                            queue_class,
                            lease_token,
                            lease_expires_at,
                            status""",
                    params,
                )
                row = cur.fetchone()
                if row:
                    self._delete_queue_entries(cur, [row[0]])
                    self._replace_active_lease_rows(cur, [(row[0], row[1], row[8], row[9], row[10], row[11])])
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to lease next URL")
            return None

        if row:
            (
                url,
                depth,
                priority,
                discovery_kind,
                archetype,
                source_url,
                added_at,
                next_fetch_at,
                _queue_class,
                lease_token,
                lease_expires_at,
                _status,
            ) = row
            return CrawlTask(
                url=url, depth=depth, priority=priority,
                discovery_kind=discovery_kind,
                archetype=archetype,
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
        exclude_branch_keys: list[str] | None = None,
        exclude_domain_branches: list[tuple[str, str]] | None = None,
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
                    exclude_branch_keys=exclude_branch_keys,
                    exclude_domain_branches=exclude_domain_branches,
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
            exclude_branch_keys=exclude_branch_keys,
            exclude_domain_branches=exclude_domain_branches,
        )
        if prioritize_breadth:
            ranked_ready_sql = self._queue_ready_sql(
                alias="ranked_source",
                now=now,
                domain=domain,
                exclude_domains=exclude_domains,
                exclude_branch_keys=exclude_branch_keys,
                exclude_domain_branches=exclude_domain_branches,
            )
            candidate_from = self._branch_breadth_candidate_from_sql(
                queue_class=normalized_queue_classes[0],
                ranked_ready_sql=ranked_ready_sql,
            )
            candidate_from_params = list(ranked_ready_sql.params)
        else:
            candidate_from = f"FROM {self._queue_table_sql(normalized_queue_classes[0])} AS candidate"
        if prioritize_breadth:
            order_by = self._branch_breadth_order_by_sql("candidate")
        else:
            order_by = self._lease_order_by_sql("candidate", prioritize_breadth=prioritize_breadth)
        params: list[object] = [lease_token, lease_expires_at, *candidate_from_params, *ready_sql.params, count]

        try:
            self._recover_leased_locked(now, expired_only=True)
            with self._conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE frontier
                        SET status = '{LEASED_STATUS}',
                            lease_token = %s,
                            lease_expires_at = %s
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
                            depth,
                            priority,
                            discovery_kind,
                            archetype,
                            source_url,
                            added_at,
                            next_fetch_at,
                            queue_class,
                            lease_token,
                            lease_expires_at,
                            status""",
                    params,
                )
                rows = cur.fetchall()
                if rows:
                    self._delete_queue_entries(cur, [row[0] for row in rows])
                    self._replace_active_lease_rows(cur, [(row[0], row[1], row[8], row[9], row[10], row[11]) for row in rows])
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to lease batch of URLs")
            return []

        return [
            CrawlTask(
                url=url,
                depth=depth,
                priority=priority,
                discovery_kind=discovery_kind,
                archetype=archetype,
                source_url=source_url,
                added_at=added_at,
                next_fetch_at=next_fetch_at,
                lease_token=row_lease_token,
                lease_expires_at=row_lease_expires_at,
            )
            for (
                url,
                depth,
                priority,
                discovery_kind,
                archetype,
                source_url,
                added_at,
                next_fetch_at,
                _queue_class,
                row_lease_token,
                row_lease_expires_at,
                _status,
            ) in rows
        ]

    def mark_done(self, url: str, lease_token: str | None = None) -> bool:
        """Mark a URL as successfully crawled."""
        normalized = normalize_url(url)
        now = time.time()
        lease_sql, lease_params = self._lease_match_sql("frontier", lease_token)

        with self._conn.cursor() as cur:
            cur.execute(
                f"""UPDATE frontier
                    SET status = '{DONE_STATUS}',
                        next_fetch_at = %s,
                        last_success_at = %s,
                        fail_streak = 0,
                        lease_token = NULL,
                        lease_expires_at = NULL,
                        last_error = NULL
                    WHERE url = %s{lease_sql}
                    RETURNING url, domain, queue_class, lease_token, lease_expires_at, status""",
                (now, now, normalized, *lease_params),
            )
            rows = cur.fetchall()
            self._delete_queue_entries(cur, [row[0] for row in rows])
            self._replace_active_lease_rows(cur, rows)
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
        lease_sql, lease_params = self._lease_match_sql("frontier", lease_token)

        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT fail_streak, priority FROM frontier WHERE url = %s{lease_sql} FOR UPDATE",
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

            status = PENDING_STATUS if retryable else FAILED_STATUS
            next_fetch_at = now + (retry_delay or 0.0) if retryable else now
            cur.execute(
                f"""UPDATE frontier
                    SET status = %s,
                        next_fetch_at = %s,
                        fail_streak = %s,
                        priority = %s,
                        last_error = %s,
                        lease_token = NULL,
                        lease_expires_at = NULL
                    WHERE url = %s{lease_sql}
                    RETURNING url, domain, priority, next_fetch_at, added_at, queue_class, lease_token, lease_expires_at, status""",
                (
                    status,
                    next_fetch_at,
                    next_fail_streak,
                    next_priority,
                    error,
                    normalized,
                    *lease_params,
                ),
            )
            rows = cur.fetchall()
            self._replace_pending_queue_rows(cur, self._project_pending_queue_rows(rows))
            self._replace_active_lease_rows(cur, [(row[0], row[1], row[5], row[6], row[7], row[8]) for row in rows])
            updated = bool(rows)
        self._conn.commit()
        return updated

    def requeue_failed(self) -> int:
        """Requeue failed URLs for retry."""
        now = time.time()
        with self._conn.cursor() as cur:
            cur.execute(
                """UPDATE frontier
                   SET status = %s,
                       next_fetch_at = %s,
                       lease_token = NULL,
                       lease_expires_at = NULL
                   WHERE status = %s
                   RETURNING url, domain, priority, next_fetch_at, added_at, queue_class, lease_token, lease_expires_at, status""",
                (PENDING_STATUS, now, FAILED_STATUS),
            )
            rows = cur.fetchall()
            self._replace_pending_queue_rows(cur, self._project_pending_queue_rows(rows))
            self._replace_active_lease_rows(cur, [(row[0], row[1], row[5], row[6], row[7], row[8]) for row in rows])
            count = len(rows)
        self._conn.commit()
        return count

    def rebalance_blocked_domain_backoff(self, now: float | None = None) -> tuple[int, int]:
        """Move backoff-blocked URLs out of ready queues and restore cooled-down URLs."""
        now = time.time() if now is None else now
        restored = 0
        quarantined = 0

        with self._conn.cursor() as cur:
            cur.execute(
                f"""DELETE FROM {BLOCKED_DOMAIN_BACKOFF_TABLE} AS blocked
                    WHERE COALESCE((
                            SELECT domain_state.backoff_until
                            FROM domain_state
                            WHERE domain_state.host_key = blocked.domain
                        ), 0) <= %s
                    RETURNING
                        blocked.url,
                        blocked.domain,
                        blocked.priority,
                        blocked.next_fetch_at,
                        blocked.added_at,
                        blocked.queue_class,
                        '{PENDING_STATUS}' AS status""",
                (now,),
            )
            restore_rows = cur.fetchall()
            if restore_rows:
                self._insert_pending_queue_rows(cur, restore_rows)
                restored = len(restore_rows)

            cur.execute(
                f"""SELECT queue.url, queue.domain, queue.priority, queue.next_fetch_at, queue.added_at,
                           %s AS queue_class, '{PENDING_STATUS}' AS status
                    FROM {self._queue_table_sql(QUEUE_EXPLORATION)} AS queue
                    JOIN domain_state ON domain_state.host_key = queue.domain
                    WHERE domain_state.backoff_until > %s
                    UNION ALL
                    SELECT queue.url, queue.domain, queue.priority, queue.next_fetch_at, queue.added_at,
                           %s AS queue_class, '{PENDING_STATUS}' AS status
                    FROM {self._queue_table_sql(QUEUE_BACKLOG)} AS queue
                    JOIN domain_state ON domain_state.host_key = queue.domain
                    WHERE domain_state.backoff_until > %s
                    UNION ALL
                    SELECT queue.url, queue.domain, queue.priority, queue.next_fetch_at, queue.added_at,
                           %s AS queue_class, '{PENDING_STATUS}' AS status
                    FROM {self._queue_table_sql(QUEUE_RECRAWL)} AS queue
                    JOIN domain_state ON domain_state.host_key = queue.domain
                    WHERE domain_state.backoff_until > %s""",
                (
                    QUEUE_EXPLORATION,
                    now,
                    QUEUE_BACKLOG,
                    now,
                    QUEUE_RECRAWL,
                    now,
                ),
            )
            blocked_rows = cur.fetchall()
            if blocked_rows:
                urls = [row[0] for row in blocked_rows]
                self._delete_queue_entries(cur, urls)
                self._insert_blocked_domain_backoff_rows(cur, blocked_rows)
                quarantined = len(blocked_rows)

        self._conn.commit()
        return quarantined, restored

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
        low_priority_threshold: float = 0.75,
        defer_seconds: float = 1800.0,
    ) -> int:
        """Delay excess low-priority backlog so one host or branch cannot dominate ready work."""
        if keep_ready_per_domain <= 0 or keep_ready_per_branch <= 0:
            return 0

        now = time.time()
        deferred_until = now + defer_seconds
        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH ranked AS (
                        SELECT
                            frontier.url,
                            ROW_NUMBER() OVER (
                                PARTITION BY queue.domain
                                ORDER BY queue.priority DESC, queue.next_fetch_at ASC, queue.added_at ASC, queue.url ASC
                            ) AS domain_rownum,
                            ROW_NUMBER() OVER (
                                PARTITION BY queue.domain, queue.branch_key
                                ORDER BY queue.priority DESC, queue.next_fetch_at ASC, queue.added_at ASC, queue.url ASC
                            ) AS branch_rownum
                        FROM {self._queue_table_sql(QUEUE_BACKLOG)} AS queue
                        JOIN frontier ON frontier.url = queue.url
                        WHERE frontier.status = '{PENDING_STATUS}'
                          AND frontier.queue_class = '{QUEUE_BACKLOG}'
                          AND queue.next_fetch_at <= %s
                          AND queue.priority <= %s
                    ), deferred AS (
                        SELECT ranked.url
                        FROM ranked
                        WHERE ranked.domain_rownum > %s
                           OR ranked.branch_rownum > %s
                    )
                    UPDATE frontier
                    SET next_fetch_at = GREATEST(next_fetch_at, %s)
                    WHERE url IN (SELECT url FROM deferred)
                    RETURNING url, domain, priority, next_fetch_at, added_at, queue_class, lease_token, lease_expires_at, status""",
                (
                    now,
                    low_priority_threshold,
                    keep_ready_per_domain,
                    keep_ready_per_branch,
                    deferred_until,
                ),
            )
            rows = cur.fetchall()
            self._replace_pending_queue_rows(cur, self._project_pending_queue_rows(rows))
            self._replace_active_lease_rows(cur, [(row[0], row[1], row[5], row[6], row[7], row[8]) for row in rows])
            count = len(rows)
        self._conn.commit()
        return count

    def promote_seed_host_exploration(
        self,
        seed_hosts: list[str],
        per_host: int = 1,
        max_depth: int = 2,
    ) -> int:
        """Promote a small number of shallow seed-host pages back into exploration."""
        if not seed_hosts or per_host <= 0:
            return 0

        now = time.time()
        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH ranked AS (
                        SELECT url,
                               ROW_NUMBER() OVER (
                                   PARTITION BY domain
                                   ORDER BY COALESCE(last_success_at, added_at) ASC, added_at ASC, url ASC
                               ) AS rownum
                        FROM frontier
                        WHERE domain = ANY(%s)
                          AND status = '{DONE_STATUS}'
                          AND depth <= %s
                          AND discovery_kind IN ('seed_host', 'same_host')
                    )
                    UPDATE frontier
                    SET status = '{PENDING_STATUS}',
                        queue_class = %s,
                        next_fetch_at = %s,
                        fail_streak = 0,
                        last_error = NULL,
                        lease_token = NULL,
                        lease_expires_at = NULL
                    WHERE url IN (
                        SELECT url
                        FROM ranked
                        WHERE rownum <= %s
                    )
                    RETURNING url, domain, priority, next_fetch_at, added_at, queue_class, lease_token, lease_expires_at, status""",
                (seed_hosts, max_depth, QUEUE_EXPLORATION, now, per_host),
            )
            rows = cur.fetchall()
            self._replace_pending_queue_rows(cur, self._project_pending_queue_rows(rows))
            self._replace_active_lease_rows(cur, [(row[0], row[1], row[5], row[6], row[7], row[8]) for row in rows])
            count = len(rows)
        self._conn.commit()
        return count

    def promote_branch_novelty_exploration(
        self,
        target_pending: int,
        *,
        per_domain: int = 1,
        candidate_limit: int = 200,
    ) -> int:
        """Promote branch-diverse backlog URLs into exploration."""
        if target_pending <= 0 or per_domain <= 0 or candidate_limit <= 0:
            return 0

        current_exploration = self.pending_count(queue_classes=[QUEUE_EXPLORATION])
        needed = target_pending - current_exploration
        if needed <= 0:
            return 0

        with self._conn.cursor() as cur:
            cur.execute(
                f"""SELECT domain, branch_key
                    FROM {self._queue_table_sql(QUEUE_EXPLORATION)}"""
            )
            existing_branches = {(domain, branch_key) for domain, branch_key in cur.fetchall()}

            cur.execute(
                f"""SELECT url, domain, branch_key
                    FROM {self._queue_table_sql(QUEUE_BACKLOG)}
                    ORDER BY priority DESC, added_at ASC, url ASC
                    LIMIT %s""",
                (max(candidate_limit, needed * 20),),
            )
            candidates = cur.fetchall()

            promoted_urls: list[str] = []
            domain_counts: Counter[str] = Counter()
            for url, domain, branch in candidates:
                key = (domain, branch)
                if key in existing_branches:
                    continue
                if domain_counts[domain] >= per_domain:
                    continue
                promoted_urls.append(normalize_url(url))
                existing_branches.add(key)
                domain_counts[domain] += 1
                if len(promoted_urls) >= needed:
                    break

            if not promoted_urls:
                return 0

            cur.execute(
                f"""UPDATE frontier
                    SET queue_class = %s
                    WHERE url = ANY(%s)
                      AND status = '{PENDING_STATUS}'
                      AND queue_class = '{QUEUE_BACKLOG}'
                    RETURNING url, domain, priority, next_fetch_at, added_at, queue_class, lease_token, lease_expires_at, status""",
                (QUEUE_EXPLORATION, promoted_urls),
            )
            rows = cur.fetchall()
            self._replace_pending_queue_rows(cur, self._project_pending_queue_rows(rows))
            self._replace_active_lease_rows(cur, [(row[0], row[1], row[5], row[6], row[7], row[8]) for row in rows])
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
                0,
                priority,
                QUEUE_EXPLORATION,
                DISCOVERY_SEED,
                ARCHETYPE_GENERIC_PAGE,
                now,
                now,
            ))

        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO frontier (
                       url, domain, depth, priority, queue_class, discovery_kind, archetype, source_url, added_at, next_fetch_at, status
                   )
                   VALUES %s
                   ON CONFLICT (url) DO UPDATE SET
                       status = 'pending',
                       queue_class = EXCLUDED.queue_class,
                       added_at = EXCLUDED.added_at,
                       next_fetch_at = EXCLUDED.next_fetch_at,
                       priority = EXCLUDED.priority,
                       fail_streak = 0,
                       last_error = NULL,
                       lease_token = NULL,
                       lease_expires_at = NULL
                   RETURNING url, domain, priority, next_fetch_at, added_at, queue_class, lease_token, lease_expires_at, status""",
                rows,
                template="(%s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, 'pending')",
                page_size=200,
            )
            frontier_rows = cur.fetchall()
            self._replace_pending_queue_rows(cur, self._project_pending_queue_rows(frontier_rows))
            self._replace_active_lease_rows(cur, [(row[0], row[1], row[5], row[6], row[7], row[8]) for row in frontier_rows])
            affected = len(frontier_rows)
        self._conn.commit()
        return affected

    def stats(self) -> dict:
        """Get queue statistics."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM frontier GROUP BY status")
            stats = dict(cur.fetchall())
            cur.execute(f"SELECT COUNT(*) FROM {LEASE_TABLE}")
            stats[LEASED_STATUS] = cur.fetchone()[0]
            cur.execute(
                """SELECT queue_class, count(*)
                   FROM (
                       SELECT %s AS queue_class, url FROM frontier_queue_exploration
                       UNION ALL
                       SELECT %s AS queue_class, url FROM frontier_queue_backlog
                       UNION ALL
                       SELECT %s AS queue_class, url FROM frontier_queue_recrawl
                   ) AS pending_queues
                   GROUP BY queue_class""",
                (QUEUE_EXPLORATION, QUEUE_BACKLOG, QUEUE_RECRAWL),
            )
            stats["pending_queue_tables"] = dict(cur.fetchall())
            cur.execute(f"SELECT COUNT(*) FROM {BLOCKED_DOMAIN_BACKOFF_TABLE}")
            stats["blocked_domain_backoff_queue"] = cur.fetchone()[0]
        stats["total"] = sum(value for value in stats.values() if isinstance(value, int))
        return stats

    def pending_count(self, queue_classes: list[str] | None = None) -> int:
        """Get count of pending URLs, optionally filtered by queue class."""
        if queue_classes:
            with self._conn.cursor() as cur:
                total = 0
                for queue_class in queue_classes:
                    cur.execute(f"SELECT COUNT(*) FROM {self._queue_table_sql(queue_class)}")
                    total += cur.fetchone()[0]
                return total

        with self._conn.cursor() as cur:
            total = 0
            for queue_class in (QUEUE_EXPLORATION, QUEUE_BACKLOG, QUEUE_RECRAWL):
                cur.execute(f"SELECT COUNT(*) FROM {self._queue_table_sql(queue_class)}")
                total += cur.fetchone()[0]
            return total

    def blocked_domain_backoff_count(self) -> int:
        """Return count of URLs isolated due to host backoff."""
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {BLOCKED_DOMAIN_BACKOFF_TABLE}")
            return cur.fetchone()[0]

    def readiness(
        self,
        now: float | None = None,
        queue_classes: list[str] | None = None,
    ) -> FrontierReadiness:
        """Return a single snapshot of pending and leaseable queue state."""
        now = time.time() if now is None else now
        pending_queue_sql = self._pending_queue_union_sql(queue_classes)
        blocked_queue_sql, blocked_queue_params = self._blocked_queue_sql(queue_classes)
        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH pending_entries AS (
                        {pending_queue_sql}
                    ), blocked_entries AS (
                        {blocked_queue_sql}
                    ), readiness_entries AS (
                        SELECT
                            queue_entry.url,
                            queue_entry.domain,
                            queue_entry.next_fetch_at,
                            queue_entry.next_fetch_at > %s AS blocked_next_fetch,
                            COALESCE(domain_state.next_request_at, 0) > %s AS blocked_domain_next_request,
                            COALESCE(domain_state.backoff_until, 0) > %s AS blocked_domain_backoff,
                            GREATEST(
                                queue_entry.next_fetch_at,
                                COALESCE(domain_state.next_request_at, 0),
                                COALESCE(domain_state.backoff_until, 0)
                            ) AS ready_at
                        FROM pending_entries AS queue_entry
                        LEFT JOIN domain_state ON domain_state.host_key = queue_entry.domain
                        UNION ALL
                        SELECT
                            blocked_entry.url,
                            blocked_entry.domain,
                            blocked_entry.next_fetch_at,
                            FALSE AS blocked_next_fetch,
                            FALSE AS blocked_domain_next_request,
                            TRUE AS blocked_domain_backoff,
                            NULL::DOUBLE PRECISION AS ready_at
                        FROM blocked_entries AS blocked_entry
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
                (*blocked_queue_params, now, now, now),
            )
            (
                pending,
                ready,
                next_ready_at,
                blocked_next_fetch,
                blocked_domain_next_request,
                blocked_domain_backoff,
                state_blocked_domain_backoff,
                state_blocked_domain_next_request,
                state_scheduled,
                state_ready,
            ) = cur.fetchone()

        next_ready_delay = None if next_ready_at is None else max(0.0, next_ready_at - now)
        return FrontierReadiness(
            pending=pending or 0,
            ready=ready or 0,
            next_ready_delay=next_ready_delay,
            blocked={
                "next_fetch_at": blocked_next_fetch or 0,
                "domain_next_request": blocked_domain_next_request or 0,
                "domain_backoff": blocked_domain_backoff or 0,
            },
            state_counts={
                "ready": state_ready or 0,
                "scheduled": state_scheduled or 0,
                "blocked_domain_next_request": state_blocked_domain_next_request or 0,
                "blocked_domain_backoff": state_blocked_domain_backoff or 0,
            },
        )

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
        """Check if URL exists in frontier."""
        normalized = normalize_url(url)
        with self._conn.cursor() as cur:
            cur.execute("SELECT 1 FROM frontier WHERE url = %s LIMIT 1", (normalized,))
            return cur.fetchone() is not None
