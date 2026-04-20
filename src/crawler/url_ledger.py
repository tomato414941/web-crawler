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
from .host_runnable_heads import HostRunnableHead, HostRunnableHeadStore
from .host_ledger import HostLedgerStore
from .scheduler_membership import (
    PHYSICAL_QUEUE_DEFAULT_SCHEDULER_SURFACE,
    PHYSICAL_QUEUE_ORDER,
    PHYSICAL_QUEUE_TABLES,
    QUEUE_RECRAWL,
    QUEUE_RUNNABLE,
    QUEUE_SCHEDULED,
    QUEUE_TABLES,
    SCHEDULER_SURFACE_NORMAL,
    SCHEDULER_SURFACE_PRIORITY,
    SCHEDULER_SURFACE_REFRESH,
    SCHEDULER_SURFACE_RUNNABLE,
    SCHEDULER_SURFACE_SCHEDULED,
    SchedulerMembershipStore,
)
from .scheduler_observability import SchedulerObservability, SchedulerReadiness
from .scheduler_quarantine import SchedulerQuarantine
from .schema import assert_public_table_columns
from .urls import normalize_url, url_branch_key

if TYPE_CHECKING:
    from .host_store import HostStore

logger = logging.getLogger(__name__)

URL_LEDGER_TABLE = "url_ledger"
INTENT_EXPLORE = "explore"
INTENT_REFRESH = "refresh"
INTENT_RETRY = "retry"
LEASE_STRATEGY_URL_ORDER = "url_order"
LEASE_STRATEGY_HOST_FIRST = "host_first"

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
    "host",
    "priority",
    "source_url",
    "added_at",
    "next_fetch_at",
    "current_intent",
    "last_success_at",
    "fail_streak",
    "last_error",
    "terminal_reason",
    "terminalized_at",
}
BLOCKED_HOST_BACKOFF_TABLE = "scheduler_queue_retry_quarantine"
QUEUE_REQUIRED_COLUMNS = {
    "url",
    "host",
    "priority",
    "next_fetch_at",
    "added_at",
    "branch_key",
}
LEASE_TABLE = "active_leases"
HOST_RUNNABLE_HEADS_TABLE = "host_runnable_heads"
LEASE_REQUIRED_COLUMNS = {
    "url",
    "host",
    "physical_queue",
    "lease_token",
    "lease_expires_at",
}
HOST_RUNNABLE_HEADS_REQUIRED_COLUMNS = {
    "physical_queue",
    "host",
    "head_url",
    "head_next_fetch_at",
    "head_added_at",
    "head_priority",
    "runnable_url_count",
    "latency_penalty",
    "runnable_at",
    "refreshed_at",
}
BLOCKED_QUEUE_REQUIRED_COLUMNS = {
    "url",
    "host",
    "physical_queue",
    "priority",
    "next_fetch_at",
    "added_at",
    "quarantined_at",
    "branch_key",
}
LEASE_STRATEGIES = {
    LEASE_STRATEGY_URL_ORDER,
    LEASE_STRATEGY_HOST_FIRST,
}
SCHEDULER_SURFACE_DEFAULT_INTENT = {
    SCHEDULER_SURFACE_RUNNABLE: INTENT_EXPLORE,
    SCHEDULER_SURFACE_SCHEDULED: INTENT_EXPLORE,
    SCHEDULER_SURFACE_REFRESH: INTENT_REFRESH,
}
INTENT_DEFAULT_SCHEDULER_SURFACE = {
    INTENT_EXPLORE: SCHEDULER_SURFACE_SCHEDULED,
    INTENT_REFRESH: SCHEDULER_SURFACE_REFRESH,
    INTENT_RETRY: SCHEDULER_SURFACE_SCHEDULED,
}
HOST_HEAD_LOOKAHEAD = 32
HOST_HEAD_READ_MODEL_LOOKAHEAD = HOST_HEAD_LOOKAHEAD * 4
__all__ = [
    "BLOCKED_HOST_BACKOFF_TABLE",
    "CrawlTask",
    "HOST_RUNNABLE_HEADS_TABLE",
    "INTENT_EXPLORE",
    "INTENT_REFRESH",
    "INTENT_RETRY",
    "LEASE_TABLE",
    "PHYSICAL_QUEUE_DEFAULT_SCHEDULER_SURFACE",
    "PHYSICAL_QUEUE_ORDER",
    "PHYSICAL_QUEUE_TABLES",
    "QUEUE_RECRAWL",
    "QUEUE_RUNNABLE",
    "QUEUE_SCHEDULED",
    "SCHEDULER_SURFACE_NORMAL",
    "SCHEDULER_SURFACE_REFRESH",
    "SCHEDULER_SURFACE_RUNNABLE",
    "SCHEDULER_SURFACE_SCHEDULED",
    "URL_LEDGER_TABLE",
    "UrlLedger",
]


@dataclass(init=False)
class CrawlTask:
    """A URL to crawl with metadata."""

    url: str
    priority: float = 1.0
    runnable_surface: str | None = None
    intent: str | None = None
    source_url: str | None = None
    added_at: float = 0.0
    next_fetch_at: float = 0.0
    lease_token: str | None = None
    lease_expires_at: float | None = None

    def __init__(
        self,
        url: str,
        priority: float = 1.0,
        *,
        runnable_surface: str | None = None,
        intent: str | None = None,
        source_url: str | None = None,
        added_at: float = 0.0,
        next_fetch_at: float = 0.0,
        lease_token: str | None = None,
        lease_expires_at: float | None = None,
    ):
        self.url = url
        self.priority = priority
        self.runnable_surface = runnable_surface
        self.intent = intent
        self.source_url = source_url
        self.added_at = added_at
        self.next_fetch_at = next_fetch_at
        self.lease_token = lease_token
        self.lease_expires_at = lease_expires_at
        self.__post_init__()

    def __post_init__(self):
        if self.added_at == 0.0:
            self.added_at = time.time()
        if self.next_fetch_at == 0.0:
            self.next_fetch_at = self.added_at


@dataclass(frozen=True)
class RunnableHostHead:
    """Derived host-level runnable head used by host-first lease selection."""

    host_key: str
    url: str
    next_fetch_at: float
    added_at: float
    priority: float
    latency_penalty: int
    host_pending_count: int


@dataclass(frozen=True)
class _RunnableSql:
    """SQL fragments for pending URL readiness checks."""

    where: str
    params: tuple[object, ...]
    runnable_at: str
    join_sql: str = ""
    latency_ms_sql: str = "0"


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
        self._lease_seconds = (
            settings.scheduler_lease_seconds if lease_seconds is None else lease_seconds
        )
        self._retry_backoff_seconds = (
            settings.scheduler_retry_backoff_seconds
            if retry_backoff_seconds is None
            else retry_backoff_seconds
        )
        self._max_retry_backoff_seconds = (
            settings.scheduler_max_retry_backoff_seconds
            if max_retry_backoff_seconds is None
            else max_retry_backoff_seconds
        )
        self._host_store: HostStore | None = None
        self._host_ledger = HostLedgerStore(conn)
        self._membership = SchedulerMembershipStore(
            conn,
            blocked_queue_table=BLOCKED_HOST_BACKOFF_TABLE,
            host_runnable_heads_table=HOST_RUNNABLE_HEADS_TABLE,
        )
        self._host_heads = HostRunnableHeadStore(
            conn,
            table_name=HOST_RUNNABLE_HEADS_TABLE,
            queue_table_sql=self._queue_table_sql,
            normalize_physical_queue=self._normalize_physical_queue,
            normalized_surface_queues=self._normalized_surface_queues,
            latency_penalty_sql=self._latency_penalty_sql,
        )
        self._observability = SchedulerObservability(
            conn,
            physical_queue_tables=PHYSICAL_QUEUE_TABLES,
            physical_queue_order=PHYSICAL_QUEUE_ORDER,
            physical_queue_default_runnable_surface=PHYSICAL_QUEUE_DEFAULT_SCHEDULER_SURFACE,
            blocked_queue_table=BLOCKED_HOST_BACKOFF_TABLE,
            lease_table=LEASE_TABLE,
        )
        self._quarantine = SchedulerQuarantine(
            conn,
            queue_runnable=self._single_physical_queue_for_surface(SCHEDULER_SURFACE_RUNNABLE),
            queue_scheduled=self._single_physical_queue_for_surface(SCHEDULER_SURFACE_SCHEDULED),
            queue_refresh=self._single_physical_queue_for_surface(SCHEDULER_SURFACE_REFRESH),
            blocked_queue_table=BLOCKED_HOST_BACKOFF_TABLE,
            queue_table_sql=self._queue_table_sql,
            delete_queue_entries=self._delete_queue_entries,
            insert_blocked_rows=self._insert_blocked_host_backoff_rows,
            insert_pending_rows=self._insert_pending_queue_rows,
        )
        self._assert_current_schema()

    def attach_host_store(self, host_store: "HostStore | None") -> None:
        """Attach the persistent host scheduler used for lease selection."""
        self._host_store = host_store

    @property
    def host_ledger_store(self) -> HostLedgerStore:
        """Return the durable host identity/history store."""
        return self._host_ledger

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
        return max(MIN_RETRY_PRIORITY, round(priority * (RETRY_PRIORITY_DECAY**fail_streak), 2))

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

    def _physical_queues(self) -> list[str]:
        """Return physical queues in stable scheduler order."""
        return self._membership.physical_queues()

    def _queue_membership_join_sql(self, *, ledger_alias: str) -> tuple[str, str]:
        """Build LEFT JOIN and absence SQL for physical pending queue membership."""
        physical_queues = self._physical_queues()
        joins = "\n                ".join(
            f"LEFT JOIN {PHYSICAL_QUEUE_TABLES[physical_queue]} AS {physical_queue} ON {physical_queue}.url = {ledger_alias}.url"
            for physical_queue in physical_queues
        )
        absence = "\n                  AND ".join(
            f"{physical_queue}.url IS NULL" for physical_queue in physical_queues
        )
        return joins, absence

    def _normalized_physical_queues(self, physical_queues: list[str] | None) -> list[str]:
        """Return physical queues in stable scheduler order."""
        return self._membership.normalized_physical_queues(physical_queues)

    def _normalized_surface_queues(
        self,
        *,
        runnable_surface: str | None,
        physical_queues: list[str] | None,
    ) -> list[str]:
        """Resolve runnable-surface filters into physical queues."""
        return self._membership.normalized_surface_queues(
            scheduler_surface=runnable_surface,
            physical_queues=physical_queues,
        )

    def _single_physical_queue_for_surface(self, runnable_surface: str) -> str:
        """Resolve one physical queue for a single runnable surface."""
        return self._membership.single_physical_queue_for_surface(runnable_surface)

    def _pending_rows_for_physical_queue(
        self,
        rows: list[tuple[str, str, float, float, float]],
        physical_queue: str,
    ) -> list[tuple[str, str, float, float, float, str]]:
        """Project ledger rows into one pending physical queue."""
        return [
            (url, host, priority, next_fetch_at, added_at, physical_queue)
            for url, host, priority, next_fetch_at, added_at in rows
        ]

    def _normalize_intent(self, intent: str | None) -> str | None:
        """Return a normalized scheduling intent when present."""
        if intent is None:
            return None
        normalized = str(intent).strip().lower()
        if normalized not in {INTENT_EXPLORE, INTENT_REFRESH, INTENT_RETRY}:
            raise ValueError(f"Unknown intent: {intent}")
        return normalized

    def _default_runnable_surface_for_physical_queue(
        self, physical_queue: str | None
    ) -> str | None:
        """Map one physical queue back to its conceptual runnable surface."""
        if physical_queue is None:
            return None
        normalized = self._normalize_physical_queue(physical_queue)
        return PHYSICAL_QUEUE_DEFAULT_SCHEDULER_SURFACE[normalized]

    def _default_scheduled_physical_queue(self) -> str:
        """Return the physical queue behind the scheduled runnable surface."""
        return self._single_physical_queue_for_surface(SCHEDULER_SURFACE_SCHEDULED)

    def _resolve_admission_physical_queue(
        self,
        *,
        physical_queue: str | None,
        runnable_surface: str | None,
        intent: str | None,
    ) -> str:
        """Resolve one physical queue for admission or requeue entrypoints."""
        normalized_intent = self._normalize_intent(intent)
        if physical_queue is not None:
            return self._normalize_physical_queue(physical_queue)

        resolved_surface = runnable_surface
        if resolved_surface is None and normalized_intent is not None:
            resolved_surface = INTENT_DEFAULT_SCHEDULER_SURFACE[normalized_intent]
        if resolved_surface is None:
            return self._default_scheduled_physical_queue()

        physical_queues = self._normalized_surface_queues(
            runnable_surface=resolved_surface,
            physical_queues=None,
        )
        if len(physical_queues) != 1:
            raise ValueError(
                f"Admission surface must resolve to one physical queue: {resolved_surface}"
            )
        return physical_queues[0]

    def _queue_runnable_sql(
        self,
        *,
        alias: str,
        now: float,
        host: str | None = None,
        exclude_hosts: list[str] | None = None,
    ) -> _RunnableSql:
        """Build readiness SQL fragments for physical pending queue tables."""
        next_request_sql = "0"
        backoff_sql = "0"
        join_sql = ""
        latency_ms_sql = "0"

        conditions = [f"{alias}.next_fetch_at <= %s"]
        params: list[object] = [now]

        if self._host_store is not None:
            host_state_alias = f"{alias}_host_state"
            join_sql = (
                f"LEFT JOIN host_state AS {host_state_alias} "
                f"ON {host_state_alias}.host_key = {alias}.host"
            )
            next_request_sql = f"COALESCE({host_state_alias}.next_request_at, 0)"
            backoff_sql = f"COALESCE({host_state_alias}.backoff_until, 0)"
            latency_ms_sql = f"COALESCE({host_state_alias}.latency_ewma_ms, 0)"
            conditions.append(f"{next_request_sql} <= %s")
            conditions.append(f"{backoff_sql} <= %s")
            params.extend([now, now])

        if host:
            conditions.append(f"{alias}.host = %s")
            params.append(host)

        if exclude_hosts:
            conditions.append(f"NOT ({alias}.host = ANY(%s))")
            params.append(exclude_hosts)

        return _RunnableSql(
            where=" AND ".join(conditions),
            params=tuple(params),
            runnable_at=f"GREATEST({alias}.next_fetch_at, {next_request_sql}, {backoff_sql})",
            join_sql=join_sql,
            latency_ms_sql=latency_ms_sql,
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
                        RETURNING url, host, physical_queue
                    )
                    SELECT ledger.url,
                           ledger.host,
                           ledger.priority,
                           ledger.next_fetch_at,
                           ledger.added_at,
                           recovered.physical_queue
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
                    raise RuntimeError(f"missing scheduler queue table: {table_name}")
                assert_public_table_columns(self._conn, table_name, QUEUE_REQUIRED_COLUMNS)
            cur.execute("SELECT to_regclass(%s)", (f"public.{BLOCKED_HOST_BACKOFF_TABLE}",))
            if cur.fetchone()[0] is None:
                raise RuntimeError(
                    f"missing scheduler blocked queue table: {BLOCKED_HOST_BACKOFF_TABLE}"
                )
            assert_public_table_columns(
                self._conn,
                BLOCKED_HOST_BACKOFF_TABLE,
                BLOCKED_QUEUE_REQUIRED_COLUMNS,
            )
            cur.execute("SELECT to_regclass(%s)", (f"public.{LEASE_TABLE}",))
            if cur.fetchone()[0] is None:
                raise RuntimeError(f"missing scheduler lease table: {LEASE_TABLE}")
            assert_public_table_columns(self._conn, LEASE_TABLE, LEASE_REQUIRED_COLUMNS)
            cur.execute("SELECT to_regclass(%s)", (f"public.{HOST_RUNNABLE_HEADS_TABLE}",))
            if cur.fetchone()[0] is None:
                raise RuntimeError(
                    f"missing scheduler host runnable-head table: {HOST_RUNNABLE_HEADS_TABLE}"
                )
            assert_public_table_columns(
                self._conn,
                HOST_RUNNABLE_HEADS_TABLE,
                HOST_RUNNABLE_HEADS_REQUIRED_COLUMNS,
            )

    def _normalize_physical_queue(self, physical_queue: str | None) -> str:
        """Return a supported scheduler physical queue."""
        if not hasattr(self, "_membership"):
            if physical_queue in PHYSICAL_QUEUE_TABLES:
                return physical_queue
            return QUEUE_SCHEDULED
        return self._membership.normalize_physical_queue(physical_queue)

    def _physical_queue_for_model(self, *, runnable_surface: str | None, intent: str | None) -> str:
        """Resolve surface-and-intent metadata into one physical queue."""
        return self._resolve_admission_physical_queue(
            physical_queue=None,
            runnable_surface=runnable_surface,
            intent=intent,
        )

    def _intent_for_model(self, *, runnable_surface: str | None, intent: str | None) -> str | None:
        """Resolve surface-and-intent metadata into one current scheduler intent."""
        normalized_intent = self._normalize_intent(intent)
        if normalized_intent is not None:
            return normalized_intent
        resolved_surface = runnable_surface
        if resolved_surface is None:
            resolved_surface = SCHEDULER_SURFACE_SCHEDULED
        return SCHEDULER_SURFACE_DEFAULT_INTENT.get(str(resolved_surface).strip().lower())

    def _task_runnable_surface(self, task: CrawlTask) -> str:
        """Resolve one task into a conceptual runnable surface."""
        if task.runnable_surface is not None:
            return str(task.runnable_surface).strip().lower()
        normalized_intent = self._normalize_intent(task.intent)
        if normalized_intent is not None:
            return INTENT_DEFAULT_SCHEDULER_SURFACE[normalized_intent]
        return SCHEDULER_SURFACE_SCHEDULED

    def _merge_runnable_surface(self, current: CrawlTask, candidate: CrawlTask) -> str:
        """Prefer the more urgent runnable surface when duplicate URLs merge."""
        current_surface = self._task_runnable_surface(current)
        candidate_surface = self._task_runnable_surface(candidate)
        if (
            SCHEDULER_SURFACE_PRIORITY[current_surface]
            <= SCHEDULER_SURFACE_PRIORITY[candidate_surface]
        ):
            return current_surface
        return candidate_surface

    def _merge_task(self, current: CrawlTask, candidate: CrawlTask) -> CrawlTask:
        """Merge duplicate task metadata before bulk upsert."""
        merged_surface = self._merge_runnable_surface(current, candidate)
        merged_intent = (
            current.intent
            or candidate.intent
            or SCHEDULER_SURFACE_DEFAULT_INTENT.get(merged_surface)
        )
        return CrawlTask(
            url=current.url,
            priority=max(current.priority, candidate.priority),
            runnable_surface=merged_surface,
            intent=merged_intent,
            source_url=current.source_url or candidate.source_url,
            added_at=min(current.added_at, candidate.added_at),
            next_fetch_at=min(current.next_fetch_at, candidate.next_fetch_at),
        )

    def _normalize_task_metadata(
        self, task: CrawlTask, *, normalized_url: str | None = None
    ) -> CrawlTask:
        """Project one task into the surface-and-intent model."""
        resolved_surface = self._task_runnable_surface(task)
        resolved_intent = self._normalize_intent(
            task.intent
        ) or SCHEDULER_SURFACE_DEFAULT_INTENT.get(resolved_surface)
        return CrawlTask(
            url=normalized_url or task.url,
            priority=task.priority,
            runnable_surface=resolved_surface,
            intent=resolved_intent,
            source_url=task.source_url,
            added_at=task.added_at,
            next_fetch_at=task.next_fetch_at,
        )

    def _task_intent_rows(self, tasks: list[CrawlTask]) -> list[tuple[str, str]]:
        """Project normalized tasks into url-to-intent rows for bulk ledger updates."""
        rows: list[tuple[str, str]] = []
        for task in tasks:
            normalized_intent = self._normalize_intent(task.intent)
            if normalized_intent is None:
                continue
            rows.append((task.url, normalized_intent))
        return rows

    def _update_task_intents(self, cur, tasks: list[CrawlTask]) -> None:
        """Persist current scheduler intent for the given normalized tasks."""
        rows = self._task_intent_rows(tasks)
        if not rows:
            return
        psycopg2.extras.execute_values(
            cur,
            f"""UPDATE {URL_LEDGER_TABLE} AS ledger
                SET current_intent = payload.current_intent
                FROM (VALUES %s) AS payload(url, current_intent)
                WHERE ledger.url = payload.url""",
            rows,
            template="(%s, %s)",
            page_size=200,
        )

    def _prepare_tasks(self, tasks: list[CrawlTask]) -> list[CrawlTask]:
        """Normalize and deduplicate tasks before writing to Postgres."""
        merged: dict[str, CrawlTask] = {}
        for task in tasks:
            normalized = self._normalize_task_metadata(task, normalized_url=normalize_url(task.url))
            existing = merged.get(normalized.url)
            if existing is None:
                merged[normalized.url] = normalized
            else:
                merged[normalized.url] = self._merge_task(existing, normalized)

        prepared: list[CrawlTask] = []
        for task in merged.values():
            prepared.append(
                CrawlTask(
                    url=task.url,
                    priority=task.priority,
                    runnable_surface=task.runnable_surface,
                    intent=task.intent,
                    source_url=task.source_url,
                    added_at=task.added_at,
                    next_fetch_at=task.next_fetch_at,
                )
            )
        return prepared

    def _normalize_lease_strategy(
        self,
        lease_strategy: str | None,
    ) -> str:
        """Resolve the named lease strategy."""
        if lease_strategy is None:
            return LEASE_STRATEGY_URL_ORDER
        normalized = str(lease_strategy).strip().lower()
        if normalized not in LEASE_STRATEGIES:
            raise ValueError(f"Unknown lease strategy: {lease_strategy}")
        return normalized

    def _lease_order_by_sql(
        self,
        alias: str,
        lease_strategy: str,
        *,
        latency_ms_sql: str | None = None,
    ) -> str:
        """Return the ORDER BY clause used for lease selection."""
        latency_penalty = self._latency_penalty_sql(alias, latency_ms_sql=latency_ms_sql)
        if lease_strategy == LEASE_STRATEGY_HOST_FIRST:
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

    def _latency_penalty_sql(self, alias: str, *, latency_ms_sql: str | None = None) -> str:
        """Return a small host-latency bucket used as a lease tiebreaker."""
        latency_ms = latency_ms_sql or "0"
        return (
            "CASE "
            f"WHEN {latency_ms} >= {LATENCY_BUCKET_VERY_SLOW_MS} THEN 3 "
            f"WHEN {latency_ms} >= {LATENCY_BUCKET_SLOW_MS} THEN 2 "
            f"WHEN {latency_ms} >= {LATENCY_BUCKET_FAST_MS} THEN 1 "
            "ELSE 0 END"
        )

    def _host_head_order_by_sql(self, alias: str, *, latency_ms_sql: str | None = None) -> str:
        """Return ORDER BY used to compare the best runnable URL for each host."""
        latency_penalty = self._latency_penalty_sql(alias, latency_ms_sql=latency_ms_sql)
        return (
            f"{alias}.next_fetch_at ASC, "
            f"{latency_penalty} ASC, "
            f"{alias}.added_at ASC, "
            f"{alias}.priority DESC, "
            f"{alias}.url ASC"
        )

    def _runnable_host_heads_sql(
        self,
        *,
        physical_queue: str,
        runnable_sql: _RunnableSql,
    ) -> tuple[str, tuple[object, ...]]:
        """Return SQL that derives one ready head URL per host."""
        table_name = self._queue_table_sql(physical_queue)
        host_head_order = self._host_head_order_by_sql(
            "candidate",
            latency_ms_sql=runnable_sql.latency_ms_sql,
        )
        latency_penalty = self._latency_penalty_sql(
            "candidate",
            latency_ms_sql=runnable_sql.latency_ms_sql,
        )
        sql = f"""SELECT selected.host,
                         selected.url,
                         selected.next_fetch_at,
                         selected.added_at,
                         selected.priority,
                         selected.latency_penalty,
                         selected.host_pending_count
                  FROM (
                      SELECT DISTINCT ON (candidate.host)
                          candidate.host,
                          candidate.url,
                          candidate.next_fetch_at,
                          candidate.added_at,
                          candidate.priority,
                          {latency_penalty} AS latency_penalty,
                          COUNT(*) OVER (PARTITION BY candidate.host) AS host_pending_count
                      FROM {table_name} AS candidate
                      {runnable_sql.join_sql}
                      WHERE {runnable_sql.where}
                      ORDER BY candidate.host, {host_head_order}
                  ) AS selected
                  ORDER BY
                      selected.host_pending_count ASC,
                      selected.latency_penalty ASC,
                      selected.next_fetch_at ASC,
                      selected.added_at ASC,
                      selected.priority DESC,
                      selected.url ASC"""
        return sql, runnable_sql.params

    def _runnable_host_head_sort_key(
        self, head: RunnableHostHead
    ) -> tuple[int, int, float, float, float, str]:
        """Return the canonical host-first comparison key for runnable host heads."""
        return (
            head.host_pending_count,
            head.latency_penalty,
            head.next_fetch_at,
            head.added_at,
            -head.priority,
            head.url,
        )

    def runnable_host_heads(
        self,
        *,
        limit: int = HOST_HEAD_LOOKAHEAD,
        host: str | None = None,
        exclude_hosts: list[str] | None = None,
        runnable_surface: str | None = None,
        physical_queues: list[str] | None = None,
        now: float | None = None,
    ) -> list[RunnableHostHead]:
        """Return one ready runnable head per host as a derived read model."""
        if limit <= 0:
            return []

        runnable_at = time.time() if now is None else now
        normalized_physical_queues = self._normalized_surface_queues(
            runnable_surface=runnable_surface,
            physical_queues=physical_queues,
        )
        heads: list[RunnableHostHead] = []

        with self._conn.cursor() as cur:
            for physical_queue in normalized_physical_queues:
                runnable_sql = self._queue_runnable_sql(
                    alias="candidate",
                    now=runnable_at,
                    host=host,
                    exclude_hosts=exclude_hosts,
                )
                sql, params = self._runnable_host_heads_sql(
                    physical_queue=physical_queue,
                    runnable_sql=runnable_sql,
                )
                cur.execute(f"{sql} LIMIT %s", (*params, limit))
                heads.extend(
                    RunnableHostHead(
                        host_key=host_key,
                        url=url,
                        next_fetch_at=next_fetch_at,
                        added_at=added_at,
                        priority=priority,
                        latency_penalty=latency_penalty,
                        host_pending_count=host_pending_count,
                    )
                    for (
                        host_key,
                        url,
                        next_fetch_at,
                        added_at,
                        priority,
                        latency_penalty,
                        host_pending_count,
                    ) in cur.fetchall()
                )

        heads.sort(key=self._runnable_host_head_sort_key)
        return heads[:limit]

    def select_runnable_host_head(
        self,
        *,
        host: str | None = None,
        exclude_hosts: list[str] | None = None,
        runnable_surface: str | None = None,
        physical_queues: list[str] | None = None,
        now: float | None = None,
    ) -> RunnableHostHead | None:
        """Return the next host-level runnable head for host-first leasing."""
        heads = self.runnable_host_heads(
            limit=HOST_HEAD_LOOKAHEAD,
            host=host,
            exclude_hosts=exclude_hosts,
            runnable_surface=runnable_surface,
            physical_queues=physical_queues,
            now=now,
        )
        if not heads:
            return None
        return min(heads, key=self._runnable_host_head_sort_key)

    def rebuild_host_runnable_heads(
        self,
        *,
        runnable_surface: str | None = None,
        physical_queues: list[str] | None = None,
        now: float | None = None,
    ) -> int:
        """Rebuild the loose host-head read model from scheduler queue membership."""
        return self._host_heads.rebuild(
            runnable_surface=runnable_surface,
            physical_queues=physical_queues,
            now=now,
        )

    def host_runnable_heads_from_read_model(
        self,
        *,
        limit: int = HOST_HEAD_LOOKAHEAD,
        host: str | None = None,
        exclude_hosts: list[str] | None = None,
        runnable_surface: str | None = None,
        physical_queues: list[str] | None = None,
        now: float | None = None,
    ) -> list[HostRunnableHead]:
        """Read ready host-head candidates from the loose read model."""
        return self._host_heads.read(
            limit=limit,
            host=host,
            exclude_hosts=exclude_hosts,
            runnable_surface=runnable_surface,
            physical_queues=physical_queues,
            now=now,
        )

    def _queue_table_sql(self, physical_queue: str) -> str:
        """Return the table name for one physical queue."""
        if not hasattr(self, "_membership"):
            return PHYSICAL_QUEUE_TABLES[self._normalize_physical_queue(physical_queue)]
        return self._membership.queue_table_sql(physical_queue)

    def _delete_queue_entries(self, cur, urls: list[str]) -> None:
        """Remove URLs from all physical scheduler queue tables."""
        self._membership.delete_queue_entries(cur, urls)

    def _delete_host_runnable_head_candidate(self, *, physical_queue: str, url: str) -> None:
        """Drop a stale read-model candidate after source-of-truth revalidation misses."""
        self._host_heads.delete_candidate(physical_queue=physical_queue, url=url)

    def _insert_pending_queue_rows(
        self,
        cur,
        rows: list[tuple[str, str, float, float, float, str]],
    ) -> None:
        """Insert scheduler-pending rows into the appropriate physical queue tables."""
        self._membership.insert_pending_rows(cur, rows)

    def _insert_blocked_host_backoff_rows(
        self,
        cur,
        rows: list[tuple[str, str, float, float, float, str]],
        *,
        quarantined_at: float | None = None,
    ) -> None:
        """Insert URLs into the blocked-host-backoff physical queue."""
        now = time.time() if quarantined_at is None else quarantined_at
        blocked_rows = [
            (
                normalize_url(url),
                host,
                self._normalize_physical_queue(physical_queue),
                priority,
                next_fetch_at,
                added_at,
                now,
                url_branch_key(normalize_url(url)),
            )
            for url, host, priority, next_fetch_at, added_at, physical_queue in rows
        ]
        if not blocked_rows:
            return
        psycopg2.extras.execute_values(
            cur,
            f"""INSERT INTO {BLOCKED_HOST_BACKOFF_TABLE}
                    (url, host, physical_queue, priority, next_fetch_at, added_at, quarantined_at, branch_key)
                VALUES %s
                ON CONFLICT (url) DO UPDATE
                SET host = EXCLUDED.host,
                    physical_queue = EXCLUDED.physical_queue,
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
                    (url, host, physical_queue, lease_token, lease_expires_at)
                VALUES %s
                ON CONFLICT (url) DO UPDATE
                SET host = EXCLUDED.host,
                    physical_queue = EXCLUDED.physical_queue,
                    lease_token = EXCLUDED.lease_token,
                    lease_expires_at = EXCLUDED.lease_expires_at""",
            [
                (
                    normalize_url(url),
                    host,
                    self._normalize_physical_queue(physical_queue),
                    lease_token,
                    lease_expires_at,
                )
                for url, host, physical_queue, lease_token, lease_expires_at in rows
            ],
            page_size=200,
        )

    def _replace_pending_queue_rows(
        self,
        cur,
        rows: list[tuple[str, str, float, float, float, str]],
    ) -> None:
        """Replace physical pending queue rows using returned scheduler state."""
        self._membership.replace_pending_rows(cur, rows)

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

    def _fetch_pending_rows_for_tasks(
        self,
        cur,
        tasks: list[CrawlTask],
    ) -> list[tuple[str, str, float, float, float, str]]:
        """Load queue-table rows for known ledger URLs using admission tasks."""
        prepared_tasks = self._prepare_tasks(tasks)
        normalized_urls = sorted({task.url for task in prepared_tasks if task.url})
        if not normalized_urls:
            return []
        physical_queue_by_url = {
            url: self._physical_queue_for_model(
                runnable_surface=task.runnable_surface,
                intent=task.intent,
            )
            for url, task in ((task.url, task) for task in prepared_tasks)
        }
        cur.execute(
            f"""SELECT url, host, priority, next_fetch_at, added_at
                FROM {URL_LEDGER_TABLE}
                WHERE url = ANY(%s)""",
            (normalized_urls,),
        )
        pending_rows: list[tuple[str, str, float, float, float, str]] = []
        for url, host, priority, next_fetch_at, added_at in cur.fetchall():
            physical_queue = physical_queue_by_url.get(url, self._default_scheduled_physical_queue())
            pending_rows.append(
                (
                    url,
                    host,
                    priority,
                    next_fetch_at,
                    added_at,
                    self._normalize_physical_queue(physical_queue),
                )
            )
        return pending_rows

    def _new_host_counts_for_tasks(self, cur, tasks: list[CrawlTask]) -> Counter[str]:
        """Return per-host counts for task URLs that are not already known."""
        normalized_urls = sorted({task.url for task in tasks if task.url})
        if not normalized_urls:
            return Counter()
        cur.execute(
            f"SELECT url FROM {URL_LEDGER_TABLE} WHERE url = ANY(%s)",
            (normalized_urls,),
        )
        existing_urls = {url for (url,) in cur.fetchall()}
        return Counter(
            urlparse(task.url).netloc for task in tasks if task.url not in existing_urls
        )

    def requeue_urls(
        self,
        urls: list[str],
        *,
        runnable_surface: str | None = None,
        intent: str | None = None,
        next_fetch_at: float | None = None,
        current_statuses: list[str] | None = None,
    ) -> int:
        """Move known URLs back into a pending physical queue and synchronize scheduler state."""
        normalized_urls = sorted({normalize_url(url) for url in urls if url})
        if not normalized_urls:
            return 0

        scheduled_at = time.time() if next_fetch_at is None else next_fetch_at
        normalized_physical_queue = self._physical_queue_for_model(
            runnable_surface=runnable_surface,
            intent=intent,
        )
        normalized_intent = self._intent_for_model(
            runnable_surface=runnable_surface,
            intent=intent,
        )

        with self._conn.cursor() as cur:
            if current_statuses is None:
                cur.execute(
                    f"""UPDATE {URL_LEDGER_TABLE} AS ledger
                        SET next_fetch_at = %s,
                            current_intent = %s,
                            terminal_reason = NULL,
                            terminalized_at = NULL
                        WHERE url = ANY(%s)
                        RETURNING url, host, priority, next_fetch_at, added_at""",
                    (scheduled_at, normalized_intent, normalized_urls),
                )
            else:
                cur.execute(
                    f"""UPDATE {URL_LEDGER_TABLE} AS ledger
                        SET next_fetch_at = %s,
                            current_intent = %s,
                            terminal_reason = NULL,
                            terminalized_at = NULL
                        WHERE url = ANY(%s)
                        RETURNING url, host, priority, next_fetch_at, added_at""",
                    (scheduled_at, normalized_intent, normalized_urls),
                )
            rows = cur.fetchall()
            pending_rows = [
                (url, host, priority, next_fetch_at, added_at, normalized_physical_queue)
                for url, host, priority, next_fetch_at, added_at in rows
            ]
            self._replace_pending_queue_rows(cur, pending_rows)
            self._delete_active_leases(cur, [row[0] for row in pending_rows])
            count = len(pending_rows)

        self._conn.commit()
        return count

    def requeue_refresh_urls(
        self,
        urls: list[str],
        *,
        next_fetch_at: float | None = None,
        current_statuses: list[str] | None = None,
    ) -> int:
        """Requeue known URLs for refresh intent on the refresh runnable surface."""
        return self.requeue_urls(
            urls,
            runnable_surface=SCHEDULER_SURFACE_REFRESH,
            intent=INTENT_REFRESH,
            next_fetch_at=next_fetch_at,
            current_statuses=current_statuses,
        )

    def _upsert_ledger_tasks(self, tasks: list[CrawlTask]) -> tuple[list[CrawlTask], int]:
        """Insert ledger rows and return normalized tasks plus changed-row count."""
        if not tasks:
            return [], 0

        prepared_tasks = self._prepare_tasks(tasks)
        rows = []
        for task in prepared_tasks:
            host = urlparse(task.url).netloc
            next_fetch_at = task.next_fetch_at or task.added_at or time.time()
            rows.append(
                (
                    task.url,
                    host,
                    task.priority,
                    task.source_url,
                    task.added_at,
                    next_fetch_at,
                    task.intent,
                )
            )

        try:
            with self._conn.cursor() as cur:
                new_host_counts = self._new_host_counts_for_tasks(cur, prepared_tasks)
                psycopg2.extras.execute_values(
                    cur,
                    f"""INSERT INTO {URL_LEDGER_TABLE} (
                           url, host, priority, source_url, added_at, next_fetch_at, current_intent
                       )
                       VALUES %s
                       ON CONFLICT (url) DO UPDATE SET
                           priority = GREATEST({URL_LEDGER_TABLE}.priority, EXCLUDED.priority),
                           source_url = COALESCE({URL_LEDGER_TABLE}.source_url, EXCLUDED.source_url),
                           added_at = LEAST({URL_LEDGER_TABLE}.added_at, EXCLUDED.added_at),
                           next_fetch_at = LEAST({URL_LEDGER_TABLE}.next_fetch_at, EXCLUDED.next_fetch_at),
                           current_intent = COALESCE(EXCLUDED.current_intent, {URL_LEDGER_TABLE}.current_intent)
                       WHERE
                           EXCLUDED.priority > {URL_LEDGER_TABLE}.priority
                           OR ({URL_LEDGER_TABLE}.source_url IS NULL AND EXCLUDED.source_url IS NOT NULL)
                           OR EXCLUDED.next_fetch_at < {URL_LEDGER_TABLE}.next_fetch_at
                           OR EXCLUDED.current_intent IS DISTINCT FROM {URL_LEDGER_TABLE}.current_intent
                       RETURNING url""",
                    rows,
                    template="(%s, %s, %s, %s, %s, %s, %s)",
                    page_size=200,
                )
                changed_rows = cur.fetchall()
                changed = len(changed_rows)
                seen_hosts = {urlparse(task.url).netloc for task in prepared_tasks if task.url}
                host_counts = Counter({host: 0 for host in seen_hosts})
                host_counts.update(new_host_counts)
                self._host_ledger.record_discovered_urls_in_tx(cur, host_counts)
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to upsert batch of %d URLs", len(tasks))
            return [], 0

        self._conn.commit()
        return prepared_tasks, changed

    def discover(self, task: CrawlTask) -> bool:
        """Insert one discovered URL into the ledger without scheduler membership."""
        _prepared, changed = self._upsert_ledger_tasks([task])
        return changed > 0

    def discover_many(self, tasks: list[CrawlTask]) -> int:
        """Insert discovered URLs into the ledger without placing them into queue tables."""
        _prepared, changed = self._upsert_ledger_tasks(tasks)
        return changed

    def _admit_queue_membership(self, tasks: list[CrawlTask]) -> int:
        """Assign scheduler membership for known ledger URLs."""
        if not tasks:
            return 0

        prepared_tasks = self._prepare_tasks(tasks)
        try:
            with self._conn.cursor() as cur:
                self._update_task_intents(cur, prepared_tasks)
                pending_rows = self._fetch_pending_rows_for_tasks(cur, prepared_tasks)
                self._replace_pending_queue_rows(cur, pending_rows)
                self._delete_active_leases(cur, [row[0] for row in pending_rows])
                count = len(pending_rows)
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to admit %d URLs", len(tasks))
            return 0

        self._conn.commit()
        return count

    def admit_discovered_tasks(self, tasks: list[CrawlTask]) -> int:
        """Assign scheduler membership to discovered URLs using task admission metadata."""
        return self._admit_queue_membership(tasks)

    def admit_urls(
        self,
        urls: list[str],
        *,
        runnable_surface: str | None = None,
        intent: str | None = None,
    ) -> int:
        """Assign scheduler membership to known ledger URLs using surface and intent."""
        normalized_urls = sorted({normalize_url(url) for url in urls if url})
        if not normalized_urls:
            return 0

        admission_tasks = [
            CrawlTask(
                url=url,
                runnable_surface=runnable_surface,
                intent=intent,
            )
            for url in normalized_urls
        ]
        return self._admit_queue_membership(admission_tasks)

    def _select_admission_candidate_rows(
        self,
        cur,
        *,
        limit: int,
    ) -> list[tuple[str, str, float, float, float]]:
        """Return ledger rows that are known but lack current scheduler membership."""
        queue_joins, queue_absence = self._queue_membership_join_sql(ledger_alias="ledger")
        cur.execute(
            f"""SELECT ledger.url,
                       ledger.host,
                       ledger.priority,
                       ledger.next_fetch_at,
                       ledger.added_at
                FROM {URL_LEDGER_TABLE} AS ledger
                {queue_joins}
                LEFT JOIN {BLOCKED_HOST_BACKOFF_TABLE} AS blocked
                    ON blocked.url = ledger.url
                LEFT JOIN {LEASE_TABLE} AS lease
                    ON lease.url = ledger.url
                WHERE {queue_absence}
                  AND blocked.url IS NULL
                  AND lease.url IS NULL
                  AND ledger.last_success_at IS NULL
                  AND ledger.terminal_reason IS NULL
                ORDER BY ledger.priority DESC,
                         ledger.next_fetch_at ASC,
                         ledger.added_at ASC,
                         ledger.url ASC
                LIMIT %s
                FOR UPDATE OF ledger SKIP LOCKED""",
            (limit,),
        )
        return list(cur.fetchall())

    def admit_discovered_urls(
        self,
        limit: int,
        *,
        runnable_surface: str | None = None,
        intent: str | None = None,
    ) -> int:
        """Assign scheduler membership to discovered ledger rows without task metadata."""
        if limit <= 0:
            return 0

        normalized_physical_queue = self._physical_queue_for_model(
            runnable_surface=runnable_surface,
            intent=intent,
        )
        resolved_intent = self._intent_for_model(
            runnable_surface=runnable_surface,
            intent=intent,
        )

        try:
            with self._conn.cursor() as cur:
                candidate_rows = self._select_admission_candidate_rows(cur, limit=limit)
                if resolved_intent is not None and candidate_rows:
                    cur.execute(
                        f"""UPDATE {URL_LEDGER_TABLE}
                            SET current_intent = %s
                            WHERE url = ANY(%s)""",
                        (resolved_intent, [row[0] for row in candidate_rows]),
                    )
                pending_rows = [
                    (url, host, priority, next_fetch_at, added_at, normalized_physical_queue)
                    for url, host, priority, next_fetch_at, added_at in candidate_rows
                ]
                self._replace_pending_queue_rows(cur, pending_rows)
                count = len(pending_rows)
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to admit discovered URLs (limit=%d)", limit)
            return 0

        self._conn.commit()
        return count

    def preview_tasks(self, tasks: list[CrawlTask]) -> list[CrawlTask]:
        """Return normalized tasks with physical queues implied without writing them."""
        return self._prepare_tasks(tasks)

    def place(self, task: CrawlTask) -> bool:
        """Place one discovered URL candidate into scheduler storage."""
        return self.place_many([task]) > 0

    def place_many(self, tasks: list[CrawlTask]) -> int:
        """Place multiple discovered URL candidates into scheduler storage."""
        prepared_tasks, changed = self._upsert_ledger_tasks(tasks)
        if not prepared_tasks:
            return changed
        admitted = self.admit_discovered_tasks(prepared_tasks)
        if admitted == 0 and changed > 0:
            return 0
        return changed

    def lease_next(
        self,
        host: str | None = None,
        lease_seconds: float | None = None,
        lease_strategy: str | None = None,
        exclude_hosts: list[str] | None = None,
        runnable_surface: str | None = None,
    ) -> CrawlTask | None:
        """Lease the next runnable URL, optionally filtered by host."""
        normalized_physical_queues = self._normalized_surface_queues(
            runnable_surface=runnable_surface,
            physical_queues=None,
        )
        normalized_lease_strategy = self._normalize_lease_strategy(lease_strategy)
        if len(normalized_physical_queues) != 1:
            for physical_queue in normalized_physical_queues:
                if normalized_lease_strategy == LEASE_STRATEGY_HOST_FIRST:
                    task = self._lease_next_host_first(
                        host=host,
                        lease_seconds=lease_seconds,
                        exclude_hosts=exclude_hosts,
                        physical_queue=physical_queue,
                    )
                else:
                    task = self._lease_next_url_order(
                        host=host,
                        lease_seconds=lease_seconds,
                        exclude_hosts=exclude_hosts,
                        physical_queue=physical_queue,
                    )
                if task is not None:
                    return task
            return None

        physical_queue = normalized_physical_queues[0]
        if normalized_lease_strategy == LEASE_STRATEGY_HOST_FIRST:
            return self._lease_next_host_first(
                host=host,
                lease_seconds=lease_seconds,
                exclude_hosts=exclude_hosts,
                physical_queue=physical_queue,
            )

        return self._lease_next_url_order(
            host=host,
            lease_seconds=lease_seconds,
            exclude_hosts=exclude_hosts,
            physical_queue=physical_queue,
        )

    def _lease_next_host_first(
        self,
        *,
        host: str | None,
        lease_seconds: float | None,
        exclude_hosts: list[str] | None,
        physical_queue: str,
    ) -> CrawlTask | None:
        """Lease from the next selected runnable host head."""
        now = time.time()
        self._recover_leased_locked(now, expired_only=True)
        self._conn.commit()

        try:
            task = self._lease_next_host_first_from_read_model(
                host=host,
                lease_seconds=lease_seconds,
                exclude_hosts=exclude_hosts,
                physical_queue=physical_queue,
                now=now,
            )
        except Exception:
            self._conn.rollback()
            logger.debug(
                "Failed to lease from host runnable-head read model; falling back",
                exc_info=True,
            )
            task = None
        if task is not None:
            return task

        return self._lease_next_host_first_from_derived_query(
            host=host,
            lease_seconds=lease_seconds,
            exclude_hosts=exclude_hosts,
            physical_queue=physical_queue,
            now=now,
        )

    def _lease_next_host_first_from_read_model(
        self,
        *,
        host: str | None,
        lease_seconds: float | None,
        exclude_hosts: list[str] | None,
        physical_queue: str,
        now: float,
    ) -> CrawlTask | None:
        """Lease host-first candidates from the loose read model first."""
        candidate_urls = [
            head.url
            for head in self.host_runnable_heads_from_read_model(
                limit=HOST_HEAD_READ_MODEL_LOOKAHEAD,
                host=host,
                exclude_hosts=exclude_hosts,
                physical_queues=[physical_queue],
                now=now,
            )
        ]
        for candidate_url in candidate_urls:
            task = self._lease_candidate_url(
                candidate_url=candidate_url,
                physical_queue=physical_queue,
                lease_seconds=lease_seconds,
                host=host,
                exclude_hosts=exclude_hosts,
                now=now,
            )
            if task is not None:
                return task
            self._delete_host_runnable_head_candidate(
                physical_queue=physical_queue,
                url=candidate_url,
            )
        return None

    def _lease_next_host_first_from_derived_query(
        self,
        *,
        host: str | None,
        lease_seconds: float | None,
        exclude_hosts: list[str] | None,
        physical_queue: str,
        now: float,
    ) -> CrawlTask | None:
        """Lease host-first candidates by deriving host heads from queue tables."""
        candidate_urls = [
            head.url
            for head in self.runnable_host_heads(
                limit=HOST_HEAD_LOOKAHEAD,
                host=host,
                exclude_hosts=exclude_hosts,
                physical_queues=[physical_queue],
                now=now,
            )
        ]
        for candidate_url in candidate_urls:
            task = self._lease_candidate_url(
                candidate_url=candidate_url,
                physical_queue=physical_queue,
                lease_seconds=lease_seconds,
                host=host,
                exclude_hosts=exclude_hosts,
                now=now,
            )
            if task is not None:
                return task
        return None

    def _lease_next_url_order(
        self,
        *,
        host: str | None,
        lease_seconds: float | None,
        exclude_hosts: list[str] | None,
        physical_queue: str,
    ) -> CrawlTask | None:
        """Lease using URL-order selection from one physical queue."""
        now = time.time()
        runnable_sql = self._queue_runnable_sql(
            alias="candidate",
            now=now,
            host=host,
            exclude_hosts=exclude_hosts,
        )
        candidate_from = (
            f"FROM {self._queue_table_sql(physical_queue)} AS candidate {runnable_sql.join_sql}"
        )
        order_by = self._lease_order_by_sql(
            "candidate",
            LEASE_STRATEGY_URL_ORDER,
            latency_ms_sql=runnable_sql.latency_ms_sql,
        )
        with self._conn.cursor() as cur:
            cur.execute(
                f"""SELECT candidate.url
                    {candidate_from}
                    WHERE candidate.url = (
                        SELECT candidate.url
                        {candidate_from}
                        WHERE {runnable_sql.where}
                        ORDER BY {order_by}
                        LIMIT 1
                        FOR UPDATE OF candidate SKIP LOCKED
                    )""",
                runnable_sql.params,
            )
            row = cur.fetchone()
        if row is None:
            self._conn.commit()
            return None
        return self._lease_candidate_url(
            candidate_url=row[0],
            physical_queue=physical_queue,
            lease_seconds=lease_seconds,
            host=host,
            exclude_hosts=exclude_hosts,
            now=now,
        )

    def _lease_candidate_url(
        self,
        *,
        candidate_url: str,
        physical_queue: str,
        lease_seconds: float | None,
        host: str | None,
        exclude_hosts: list[str] | None,
        now: float,
    ) -> CrawlTask | None:
        """Lease one concrete candidate URL when it is still runnable and unlocked."""
        lease_token = uuid.uuid4().hex
        duration = self._lease_seconds if lease_seconds is None else lease_seconds
        lease_expires_at = now + duration
        runnable_sql = self._queue_runnable_sql(
            alias="candidate",
            now=now,
            host=host,
            exclude_hosts=exclude_hosts,
        )
        candidate_from = (
            f"FROM {self._queue_table_sql(physical_queue)} AS candidate {runnable_sql.join_sql}"
        )

        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    f"""SELECT
                            candidate.url,
                            candidate.priority,
                            ledger.source_url,
                            candidate.added_at,
                            candidate.next_fetch_at,
                            candidate.host
                        {candidate_from}
                        JOIN {URL_LEDGER_TABLE} AS ledger ON ledger.url = candidate.url
                        WHERE candidate.url = %s
                          AND {runnable_sql.where}
                        FOR UPDATE OF candidate SKIP LOCKED""",
                    (candidate_url, *runnable_sql.params),
                )
                row = cur.fetchone()
                if row:
                    self._delete_queue_entries(cur, [row[0]])
                    self._upsert_active_leases(
                        cur,
                        [(row[0], row[5], physical_queue, lease_token, lease_expires_at)],
                    )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to lease next URL")
            return None

        if row is None:
            return None

        url, priority, source_url, added_at, next_fetch_at, _host = row
        return CrawlTask(
            url=url,
            priority=priority,
            source_url=source_url,
            added_at=added_at,
            next_fetch_at=next_fetch_at,
            lease_token=lease_token,
            lease_expires_at=lease_expires_at,
        )

    def lease_batch(
        self,
        count: int = 10,
        host: str | None = None,
        lease_seconds: float | None = None,
        lease_strategy: str | None = None,
        exclude_hosts: list[str] | None = None,
        runnable_surface: str | None = None,
    ) -> list[CrawlTask]:
        """Lease a batch of runnable URLs."""
        normalized_physical_queues = self._normalized_surface_queues(
            runnable_surface=runnable_surface,
            physical_queues=None,
        )
        normalized_lease_strategy = self._normalize_lease_strategy(lease_strategy)
        if len(normalized_physical_queues) != 1:
            tasks: list[CrawlTask] = []
            while len(tasks) < count:
                task = self.lease_next(
                    host=host,
                    lease_seconds=lease_seconds,
                    lease_strategy=normalized_lease_strategy,
                    exclude_hosts=exclude_hosts,
                    runnable_surface=runnable_surface,
                )
                if task is None:
                    break
                tasks.append(task)
            return tasks

        if normalized_lease_strategy == LEASE_STRATEGY_HOST_FIRST:
            tasks: list[CrawlTask] = []
            while len(tasks) < count:
                task = self.lease_next(
                    host=host,
                    lease_seconds=lease_seconds,
                    lease_strategy=normalized_lease_strategy,
                    exclude_hosts=exclude_hosts,
                    runnable_surface=runnable_surface,
                )
                if task is None:
                    break
                tasks.append(task)
            return tasks

        now = time.time()
        lease_token = uuid.uuid4().hex
        duration = self._lease_seconds if lease_seconds is None else lease_seconds
        lease_expires_at = now + duration
        runnable_sql = self._queue_runnable_sql(
            alias="candidate",
            now=now,
            host=host,
            exclude_hosts=exclude_hosts,
        )
        candidate_from = (
            f"FROM {self._queue_table_sql(normalized_physical_queues[0])} AS candidate "
            f"{runnable_sql.join_sql}"
        )
        if normalized_lease_strategy == LEASE_STRATEGY_HOST_FIRST:
            lease_order = self._lease_order_by_sql(
                "candidate",
                normalized_lease_strategy,
                latency_ms_sql=runnable_sql.latency_ms_sql,
            )
            order_by = f"{lease_order}, candidate.url ASC"
        else:
            order_by = self._lease_order_by_sql(
                "candidate",
                normalized_lease_strategy,
                latency_ms_sql=runnable_sql.latency_ms_sql,
            )
        params: list[object] = [*runnable_sql.params, count]

        try:
            self._recover_leased_locked(now, expired_only=True)
            with self._conn.cursor() as cur:
                cur.execute(
                    f"""SELECT
                            candidate.url,
                            candidate.priority,
                            ledger.source_url,
                            candidate.added_at,
                            candidate.next_fetch_at,
                            candidate.host
                        {candidate_from}
                        JOIN {URL_LEDGER_TABLE} AS ledger ON ledger.url = candidate.url
                        WHERE candidate.url IN (
                            SELECT candidate.url
                            {candidate_from}
                            WHERE {runnable_sql.where}
                            ORDER BY {order_by}
                            LIMIT %s
                            FOR UPDATE OF candidate SKIP LOCKED
                        )
                        ORDER BY {order_by}, candidate.url ASC
                        FOR UPDATE OF candidate""",
                    params,
                )
                rows = cur.fetchall()
                if rows:
                    self._delete_queue_entries(cur, [row[0] for row in rows])
                    self._upsert_active_leases(
                        cur,
                        [
                            (
                                row[0],
                                row[5],
                                normalized_physical_queues[0],
                                lease_token,
                                lease_expires_at,
                            )
                            for row in rows
                        ],
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
                _host,
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
                        current_intent = NULL,
                        last_success_at = %s,
                        fail_streak = 0,
                        last_error = NULL,
                        terminal_reason = NULL,
                        terminalized_at = NULL
                    WHERE url = %s{lease_sql}
                    RETURNING url, host""",
                (now, now, normalized, *lease_params),
            )
            rows = cur.fetchall()
            self._delete_queue_entries(cur, [row[0] for row in rows])
            self._delete_active_leases(cur, [row[0] for row in rows])
            for _url, host in rows:
                self._host_ledger.record_success_in_tx(cur, host, at=now)
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
                f"SELECT fail_streak, priority, host FROM {URL_LEDGER_TABLE} AS ledger WHERE url = %s{lease_sql} FOR UPDATE",
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
                    f"""UPDATE {URL_LEDGER_TABLE} AS ledger
                        SET next_fetch_at = %s,
                            current_intent = %s,
                            fail_streak = %s,
                            priority = %s,
                            last_error = %s,
                            terminal_reason = NULL,
                            terminalized_at = NULL
                        WHERE url = %s{lease_sql}
                        RETURNING url, host, priority, next_fetch_at, added_at""",
                    (
                        next_fetch_at,
                        INTENT_RETRY,
                        next_fail_streak,
                        next_priority,
                        error,
                        normalized,
                        *lease_params,
                    ),
                )
                rows = cur.fetchall()
                for _url, host, *_rest in rows:
                    self._host_ledger.record_failure_in_tx(cur, host, at=now)
                pending_rows = self._pending_rows_for_physical_queue(
                    rows,
                    self._single_physical_queue_for_surface(SCHEDULER_SURFACE_SCHEDULED),
                )
                self._replace_pending_queue_rows(cur, pending_rows)
                self._delete_active_leases(cur, [row[0] for row in pending_rows])
                updated = bool(rows)
            else:
                cur.execute(
                    f"""UPDATE {URL_LEDGER_TABLE} AS ledger
                        SET next_fetch_at = %s,
                            current_intent = NULL,
                            fail_streak = %s,
                            priority = %s,
                            last_error = %s,
                            terminal_reason = %s,
                            terminalized_at = %s
                        WHERE url = %s{lease_sql}
                        RETURNING url, host""",
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
                for _url, host in rows:
                    self._host_ledger.record_failure_in_tx(cur, host, at=now)
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
                       current_intent = %s,
                       terminal_reason = NULL,
                       terminalized_at = NULL
                   WHERE terminal_reason IS NOT NULL
                   RETURNING url, host, priority, next_fetch_at, added_at""",
                (now, INTENT_RETRY),
            )
            rows = cur.fetchall()
            pending_rows = self._pending_rows_for_physical_queue(
                rows,
                self._single_physical_queue_for_surface(SCHEDULER_SURFACE_SCHEDULED),
            )
            self._replace_pending_queue_rows(cur, pending_rows)
            self._delete_active_leases(cur, [row[0] for row in pending_rows])
            count = len(pending_rows)
        self._conn.commit()
        return count

    def rebalance_blocked_host_backoff(self, now: float | None = None) -> tuple[int, int]:
        """Move backoff-blocked URLs out of the normal scheduler queues."""
        return self._quarantine.rebalance(now=now)

    def retire_blocked_host_backoff(
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

    def restore_recovered_blocked_host_backoff(
        self,
        *,
        limit: int,
        per_host: int,
        now: float | None = None,
    ) -> int:
        """Restore blocked URLs whose hosts have already recovered."""
        return self._quarantine.restore_recovered(limit=limit, per_host=per_host, now=now)

    def promote_blocked_host_backoff(
        self,
        limit: int,
        *,
        per_host: int = 1,
        max_consecutive_failures: int | None = None,
        now: float | None = None,
    ) -> int:
        """Promote a small cooled-down subset from blocked queue back into normal queues."""
        return self._quarantine.promote(
            limit,
            per_host=per_host,
            max_consecutive_failures=max_consecutive_failures,
            now=now,
        )

    def recover_leased(self, expired_only: bool = True) -> int:
        """Reset leased URLs back to pending."""
        count = self._recover_leased_locked(time.time(), expired_only=expired_only)
        self._conn.commit()
        return count

    def delay_overcrowded_scheduled_surface(
        self,
        *,
        keep_runnable_per_host: int = 128,
        keep_runnable_per_branch: int = 16,
        limit: int | None = None,
        delay_seconds: float = 1800.0,
    ) -> int:
        """Delay excess runnable work on the scheduled surface so one host or branch cannot dominate."""
        if keep_runnable_per_host <= 0 or keep_runnable_per_branch <= 0:
            return 0
        if limit is not None and limit <= 0:
            return 0

        now = time.time()
        delayed_until = now + delay_seconds
        limit_sql = "" if limit is None else "\n                        LIMIT %s"
        params: tuple[object, ...] = (
            now,
            keep_runnable_per_host,
            keep_runnable_per_branch,
            *((limit,) if limit is not None else ()),
            delayed_until,
        )
        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH ranked AS (
                        SELECT
                            queue.url,
                            ROW_NUMBER() OVER (
                                PARTITION BY queue.host
                                ORDER BY queue.priority DESC, queue.next_fetch_at ASC, queue.added_at ASC, queue.url ASC
                            ) AS host_rownum,
                            ROW_NUMBER() OVER (
                                PARTITION BY queue.host, queue.branch_key
                                ORDER BY queue.priority DESC, queue.next_fetch_at ASC, queue.added_at ASC, queue.url ASC
                            ) AS branch_rownum
                        FROM {self._queue_table_sql(self._single_physical_queue_for_surface(SCHEDULER_SURFACE_SCHEDULED))} AS queue
                        WHERE queue.next_fetch_at <= %s
                    ), scheduled AS (
                        SELECT ranked.url
                        FROM ranked
                        WHERE ranked.host_rownum > %s
                           OR ranked.branch_rownum > %s
                        ORDER BY ranked.host_rownum DESC, ranked.branch_rownum DESC, ranked.url ASC
                        {limit_sql}
                    )
                    UPDATE {URL_LEDGER_TABLE}
                    SET next_fetch_at = GREATEST(next_fetch_at, %s)
                    WHERE url IN (SELECT url FROM scheduled)
                    RETURNING url, host, priority, next_fetch_at, added_at""",
                params,
            )
            rows = cur.fetchall()
            pending_rows = self._pending_rows_for_physical_queue(
                rows,
                self._single_physical_queue_for_surface(SCHEDULER_SURFACE_SCHEDULED),
            )
            self._replace_pending_queue_rows(cur, pending_rows)
            count = len(rows)
        self._conn.commit()
        return count

    def promote_scheduled_host_heads(
        self,
        target_pending: int,
        *,
        per_host: int = 1,
        candidate_limit: int = 200,
    ) -> int:
        """Promote one scheduled-surface head per host into the runnable surface."""
        if target_pending <= 0 or per_host <= 0 or candidate_limit <= 0:
            return 0

        current_runnable = self.pending_count(runnable_surface=SCHEDULER_SURFACE_RUNNABLE)
        needed = target_pending - current_runnable
        if needed <= 0:
            return 0

        with self._conn.cursor() as cur:
            cur.execute(
                f"""SELECT DISTINCT host
                    FROM {self._queue_table_sql(self._single_physical_queue_for_surface(SCHEDULER_SURFACE_RUNNABLE))}"""
            )
            existing_hosts = {host for (host,) in cur.fetchall()}

            cur.execute(
                f"""SELECT url, host
                    FROM {self._queue_table_sql(self._single_physical_queue_for_surface(SCHEDULER_SURFACE_SCHEDULED))}
                    ORDER BY priority DESC, added_at ASC, url ASC
                    LIMIT %s""",
                (max(candidate_limit, needed * 20),),
            )
            candidates = cur.fetchall()

            promoted_urls: list[str] = []
            host_counts: Counter[str] = Counter()
            for url, host in candidates:
                if host in existing_hosts:
                    continue
                if host_counts[host] >= per_host:
                    continue
                promoted_urls.append(normalize_url(url))
                existing_hosts.add(host)
                host_counts[host] += 1
                if len(promoted_urls) >= needed:
                    break

            if not promoted_urls:
                return 0

            cur.execute(
                f"""SELECT url, host, priority, next_fetch_at, added_at
                    FROM {self._queue_table_sql(self._single_physical_queue_for_surface(SCHEDULER_SURFACE_SCHEDULED))}
                    WHERE url = ANY(%s)""",
                (promoted_urls,),
            )
            rows = self._pending_rows_for_physical_queue(
                cur.fetchall(),
                self._single_physical_queue_for_surface(SCHEDULER_SURFACE_RUNNABLE),
            )
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
            host = urlparse(normalized).netloc
            rows.append(
                (
                    normalized,
                    host,
                    priority,
                    now,
                    now,
                )
            )

        with self._conn.cursor() as cur:
            normalized_urls = [row[0] for row in rows]
            cur.execute(
                f"SELECT url FROM {URL_LEDGER_TABLE} WHERE url = ANY(%s)",
                (normalized_urls,),
            )
            existing_urls = {url for (url,) in cur.fetchall()}
            new_host_counts = Counter(host for url, host, *_ in rows if url not in existing_urls)
            psycopg2.extras.execute_values(
                cur,
                f"""INSERT INTO {URL_LEDGER_TABLE} (
                       url, host, priority, source_url, added_at, next_fetch_at, current_intent
                   )
                   VALUES %s
                   ON CONFLICT (url) DO UPDATE SET
                       added_at = EXCLUDED.added_at,
                       next_fetch_at = EXCLUDED.next_fetch_at,
                       current_intent = EXCLUDED.current_intent,
                       priority = EXCLUDED.priority,
                       fail_streak = 0,
                       last_error = NULL,
                       terminal_reason = NULL,
                       terminalized_at = NULL
                   RETURNING url, host, priority, next_fetch_at, added_at""",
                rows,
                template=f"(%s, %s, %s, NULL, %s, %s, '{INTENT_EXPLORE}')",
                page_size=200,
            )
            ledger_rows = cur.fetchall()
            seen_hosts = {host for _url, host, *_ in rows if host}
            host_counts = Counter({host: 0 for host in seen_hosts})
            host_counts.update(new_host_counts)
            self._host_ledger.record_discovered_urls_in_tx(cur, host_counts, seen_at=now)
            pending_rows = self._pending_rows_for_physical_queue(
                ledger_rows,
                self._single_physical_queue_for_surface(SCHEDULER_SURFACE_RUNNABLE),
            )
            self._replace_pending_queue_rows(cur, pending_rows)
            self._delete_active_leases(cur, [row[0] for row in pending_rows])
            affected = len(ledger_rows)
        self._conn.commit()
        return affected

    def stats(self) -> dict:
        """Get queue statistics."""
        return self._observability.status_counts()

    def effective_state_counts(self, now: float | None = None) -> dict[str, int]:
        """Return the current runtime-facing scheduler-state breakdown."""
        return self._observability.effective_state_counts(now=now)

    def scheduler_state_snapshot(
        self,
        now: float | None = None,
        *,
        runnable_surface: str | None = None,
    ) -> dict[str, dict[str, int]]:
        """Return the bundled runtime-facing scheduler state snapshot."""
        return self._observability.scheduler_state_snapshot(
            now=now,
            runnable_surface=runnable_surface,
        )

    def blocked_reason_counts(
        self,
        now: float | None = None,
        *,
        runnable_surface: str | None = None,
    ) -> dict[str, int]:
        """Return the current blocked breakdown by scheduler reason."""
        return self._observability.blocked_reason_counts(
            now=now,
            runnable_surface=runnable_surface,
        )

    def readiness_state_counts(
        self,
        now: float | None = None,
        *,
        runnable_surface: str | None = None,
    ) -> dict[str, int]:
        """Return the current readiness-derived scheduler state breakdown."""
        return self._observability.readiness_state_counts(
            now=now,
            runnable_surface=runnable_surface,
        )

    def pending_count(
        self,
        *,
        runnable_surface: str | None = None,
    ) -> int:
        """Get count of pending URLs, optionally filtered by runnable surface."""
        return self._observability.pending_count(runnable_surface=runnable_surface)

    def pending_host_count(
        self,
        *,
        runnable_surface: str | None = None,
    ) -> int:
        """Get count of distinct pending hosts, optionally filtered by runnable surface."""
        return self._observability.pending_host_count(runnable_surface=runnable_surface)

    def runnable_host_count(
        self,
        now: float | None = None,
        *,
        runnable_surface: str | None = None,
    ) -> int:
        """Get count of distinct hosts that are runnable right now."""
        return self._observability.runnable_host_count(
            now=now,
            runnable_surface=runnable_surface,
        )

    def blocked_host_backoff_count(self) -> int:
        """Return count of URLs isolated due to host backoff."""
        return self._observability.blocked_count()

    def readiness(
        self,
        now: float | None = None,
        *,
        runnable_surface: str | None = None,
    ) -> SchedulerReadiness:
        """Return a single snapshot of pending and leaseable queue state."""
        return self._observability.readiness(now=now, runnable_surface=runnable_surface)

    def runnable_count(
        self,
        now: float | None = None,
        *,
        runnable_surface: str | None = None,
    ) -> int:
        """Get count of pending URLs that are runnable right now."""
        return self.readiness(
            now=now,
            runnable_surface=runnable_surface,
        ).runnable

    def scheduled_count(
        self,
        now: float | None = None,
        *,
        runnable_surface: str | None = None,
    ) -> int:
        """Get count of scheduled-but-not-yet-runnable pending URLs."""
        return self.readiness(
            now=now,
            runnable_surface=runnable_surface,
        ).scheduled

    def next_runnable_delay(
        self,
        now: float | None = None,
        *,
        runnable_surface: str | None = None,
    ) -> float | None:
        """Return seconds until the next pending URL becomes leaseable."""
        return self.readiness(
            now=now,
            runnable_surface=runnable_surface,
        ).next_runnable_delay

    def is_seen(self, url: str) -> bool:
        """Check if URL exists in the URL ledger."""
        normalized = normalize_url(url)
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {URL_LEDGER_TABLE} WHERE url = %s LIMIT 1", (normalized,))
            return cur.fetchone() is not None
