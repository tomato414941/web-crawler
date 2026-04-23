"""URL ledger with PostgreSQL-backed scheduler state."""

from __future__ import annotations

import logging
import time
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import psycopg2.extras

from .config import settings
from .host_runnable_heads import (
    HostRunnableHead,
    HostRunnableHeadDirtyRefreshSummary,
    HostRunnableHeadRepairSummary,
    HostRunnableHeadStore,
)
from .host_ledger import HostLedgerStore
from .scheduler_lease_telemetry import HostFirstLeaseTelemetry
from .scheduler_membership import (
    PHYSICAL_QUEUE_DEFAULT_SCHEDULER_SURFACE,
    PHYSICAL_QUEUE_ORDER,
    PHYSICAL_QUEUE_TABLES,
    QUEUE_REFRESH,
    QUEUE_RUNNABLE,
    QUEUE_SCHEDULED,
    QUEUE_TABLES,
    HOST_HEAD_UPDATE_DIRTY,
    SCHEDULER_SURFACE_NORMAL,
    SCHEDULER_SURFACE_URGENCY,
    SCHEDULER_SURFACE_REFRESH,
    SCHEDULER_SURFACE_RUNNABLE,
    SCHEDULER_SURFACE_SCHEDULED,
    SchedulerMembershipStore,
    SchedulerQueueRow,
    SchedulerQueueRowInput,
)
from .scheduler_leases import (
    ACTIVE_LEASES_TABLE,
    LEASE_REQUIRED_COLUMNS,
    ExecutionLeaseStore,
)
from .scheduler_admission import SchedulerAdmissionService
from .scheduler_requeue import SchedulerRequeueService
from .scheduler_selection import SchedulerLeaseSelector
from .scheduler_observability import SchedulerObservability, SchedulerReadiness
from .scheduler_quarantine import SchedulerQuarantine
from .scheduler_retry_policy import SchedulerRetryPolicy
from .schema import assert_public_table_columns
from .urls import normalize_url

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
LATENCY_BUCKET_FAST_MS = 150.0
LATENCY_BUCKET_SLOW_MS = 400.0
LATENCY_BUCKET_VERY_SLOW_MS = 1000.0
ADMISSION_DIAGNOSTIC_FIELDS = (
    "admit_update_intents_ms",
    "admit_fetch_rows_ms",
    "admit_delete_membership_ms",
    "admit_insert_membership_ms",
    "admit_host_heads_ms",
    "admit_delete_leases_ms",
    "admit_commit_ms",
)
URL_LEDGER_REQUIRED_COLUMNS = {
    "url",
    "host",
    "discovery_value",
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
    "scheduler_score",
    "next_fetch_at",
    "added_at",
    "branch_key",
}
LEASE_TABLE = ACTIVE_LEASES_TABLE
HOST_RUNNABLE_HEADS_TABLE = "host_runnable_heads"
HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE = "host_runnable_head_dirty_hosts"
HOST_RUNNABLE_HEADS_REQUIRED_COLUMNS = {
    "physical_queue",
    "host",
    "head_url",
    "head_next_fetch_at",
    "head_added_at",
    "head_scheduler_score",
    "runnable_url_count",
    "execution_tier",
    "latency_penalty",
    "runnable_at",
    "refreshed_at",
}
HOST_RUNNABLE_HEAD_DIRTY_HOSTS_REQUIRED_COLUMNS = {
    "physical_queue",
    "host",
    "marked_at",
}
BLOCKED_QUEUE_REQUIRED_COLUMNS = {
    "url",
    "host",
    "physical_queue",
    "scheduler_score",
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
    "HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE",
    "HOST_RUNNABLE_HEADS_TABLE",
    "INTENT_EXPLORE",
    "INTENT_REFRESH",
    "INTENT_RETRY",
    "LEASE_TABLE",
    "PHYSICAL_QUEUE_DEFAULT_SCHEDULER_SURFACE",
    "PHYSICAL_QUEUE_ORDER",
    "PHYSICAL_QUEUE_TABLES",
    "QUEUE_REFRESH",
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
    discovery_value: float = 1.0
    scheduler_score: float = 1.0
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
        discovery_value: float = 1.0,
        *,
        scheduler_score: float | None = None,
        runnable_surface: str | None = None,
        intent: str | None = None,
        source_url: str | None = None,
        added_at: float = 0.0,
        next_fetch_at: float = 0.0,
        lease_token: str | None = None,
        lease_expires_at: float | None = None,
    ):
        self.url = url
        self.discovery_value = discovery_value
        self.scheduler_score = discovery_value if scheduler_score is None else scheduler_score
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
    scheduler_score: float
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


@dataclass(frozen=True)
class _HostFirstReadModelResult:
    """Result of one host-head read-model lease attempt."""

    task: CrawlTask | None
    read_model: str
    candidates: int = 0
    stale_candidates: int = 0
    execution_tier: int | None = None


def _elapsed_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 1)


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
        self._retry_policy = SchedulerRetryPolicy(
            retry_backoff_seconds=self._retry_backoff_seconds,
            max_retry_backoff_seconds=self._max_retry_backoff_seconds,
            retry_intent=INTENT_RETRY,
        )
        self._host_store: HostStore | None = None
        self._host_ledger = HostLedgerStore(conn)
        self._membership = SchedulerMembershipStore(
            conn,
            blocked_queue_table=BLOCKED_HOST_BACKOFF_TABLE,
            host_runnable_heads_table=HOST_RUNNABLE_HEADS_TABLE,
            host_runnable_head_dirty_hosts_table=HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE,
        )
        self._leases = ExecutionLeaseStore(
            conn,
            normalize_physical_queue=self._normalize_physical_queue,
        )
        self._host_heads = HostRunnableHeadStore(
            conn,
            table_name=HOST_RUNNABLE_HEADS_TABLE,
            queue_table_sql=self._queue_table_sql,
            normalize_physical_queue=self._normalize_physical_queue,
            normalized_surface_queues=self._normalized_surface_queues,
            latency_penalty_sql=self._latency_penalty_sql,
            dirty_hosts_table_name=HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE,
        )
        self._membership.attach_host_heads(self._host_heads)
        self._lease_telemetry = HostFirstLeaseTelemetry()
        self._selection = SchedulerLeaseSelector(
            self,
            task_cls=CrawlTask,
            runnable_host_head_cls=RunnableHostHead,
            url_ledger_table=URL_LEDGER_TABLE,
            lease_strategy_url_order=LEASE_STRATEGY_URL_ORDER,
            lease_strategy_host_first=LEASE_STRATEGY_HOST_FIRST,
            host_head_lookahead=HOST_HEAD_LOOKAHEAD,
            host_head_read_model_lookahead=HOST_HEAD_READ_MODEL_LOOKAHEAD,
        )
        self._admission = SchedulerAdmissionService(
            self,
            task_cls=CrawlTask,
            url_ledger_table=URL_LEDGER_TABLE,
            blocked_host_backoff_table=BLOCKED_HOST_BACKOFF_TABLE,
            lease_table=LEASE_TABLE,
            host_head_update_dirty=HOST_HEAD_UPDATE_DIRTY,
        )
        self._requeue = SchedulerRequeueService(
            self,
            url_ledger_table=URL_LEDGER_TABLE,
            blocked_host_backoff_table=BLOCKED_HOST_BACKOFF_TABLE,
            intent_explore=INTENT_EXPLORE,
            intent_retry=INTENT_RETRY,
            intent_refresh=INTENT_REFRESH,
            scheduler_surface_runnable=SCHEDULER_SURFACE_RUNNABLE,
            scheduler_surface_refresh=SCHEDULER_SURFACE_REFRESH,
            scheduler_surface_scheduled=SCHEDULER_SURFACE_SCHEDULED,
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
            insert_blocked_rows=self._requeue_service().insert_blocked_host_backoff_rows,
            insert_pending_rows=self._insert_pending_queue_rows,
        )
        self._last_admission_diagnostics = self._empty_admission_diagnostics()
        self._assert_current_schema()

    def reset_host_first_fallback_stats(self) -> None:
        """Reset cycle-local host-first fallback counters."""
        self._host_first_lease_telemetry().reset()

    def host_first_fallback_stats(self) -> dict[str, int]:
        """Return cycle-local host-first fallback counters."""
        return self._host_first_lease_telemetry().fallback_stats()

    def _record_host_first_fallback(self, task: CrawlTask | None) -> None:
        """Record that host-first leasing fell back to bounded scanning."""
        self._host_first_lease_telemetry().record_fallback(hit=task is not None)

    def _record_host_first_read_model(self, status: str) -> None:
        """Record the read-model result for one host-first lease attempt."""
        self._host_first_lease_telemetry().record_read_model(status)

    def _host_first_lease_telemetry(self) -> HostFirstLeaseTelemetry:
        telemetry = getattr(self, "_lease_telemetry", None)
        if telemetry is None:
            telemetry = HostFirstLeaseTelemetry()
            self._lease_telemetry = telemetry
        return telemetry

    def _set_last_lease_diagnostics(
        self,
        *,
        read_model: str,
        fallback: str,
        read_model_candidates: int = 0,
        stale_candidates: int = 0,
        execution_tier: int | None = None,
    ) -> None:
        """Store per-lease scheduler diagnostics for the crawler telemetry layer."""
        self._host_first_lease_telemetry().set_last_lease_diagnostics(
            read_model=read_model,
            fallback=fallback,
            read_model_candidates=read_model_candidates,
            stale_candidates=stale_candidates,
            execution_tier=execution_tier,
        )

    def last_lease_diagnostics(self) -> dict[str, object]:
        """Return diagnostics for the most recent lease_next call on this ledger."""
        return self._host_first_lease_telemetry().last_lease_diagnostics()

    def _empty_admission_diagnostics(self) -> dict[str, float]:
        return {field: 0.0 for field in ADMISSION_DIAGNOSTIC_FIELDS}

    def last_admission_diagnostics(self) -> dict[str, float]:
        """Return timing diagnostics for the most recent scheduler admission."""
        return dict(self._last_admission_diagnostics)

    def attach_host_store(self, host_store: "HostStore | None") -> None:
        """Attach the persistent host scheduler used for lease selection."""
        self._host_store = host_store

    @property
    def host_ledger_store(self) -> HostLedgerStore:
        """Return the durable host identity/history store."""
        return self._host_ledger

    def _compute_retry_backoff(self, fail_streak: int) -> float:
        """Compute exponential retry backoff for a failed URL."""
        return self._scheduler_retry_policy().compute_backoff(fail_streak)

    def _compute_retry_scheduler_score(self, discovery_value: float, fail_streak: int) -> float:
        """Lower retry score so repeatedly failing URLs do not dominate the queue."""
        return self._scheduler_retry_policy().compute_scheduler_score(discovery_value, fail_streak)

    def _scheduler_retry_policy(self) -> SchedulerRetryPolicy:
        policy = getattr(self, "_retry_policy", None)
        if policy is None:
            policy = SchedulerRetryPolicy(
                retry_backoff_seconds=getattr(
                    self, "_retry_backoff_seconds", settings.scheduler_retry_backoff_seconds
                ),
                max_retry_backoff_seconds=getattr(
                    self,
                    "_max_retry_backoff_seconds",
                    settings.scheduler_max_retry_backoff_seconds,
                ),
                retry_intent=INTENT_RETRY,
            )
            self._retry_policy = policy
        return policy

    def _lease_selector(self) -> SchedulerLeaseSelector:
        selector = getattr(self, "_selection", None)
        if selector is None:
            selector = SchedulerLeaseSelector(
                self,
                task_cls=CrawlTask,
                runnable_host_head_cls=RunnableHostHead,
                url_ledger_table=URL_LEDGER_TABLE,
                lease_strategy_url_order=LEASE_STRATEGY_URL_ORDER,
                lease_strategy_host_first=LEASE_STRATEGY_HOST_FIRST,
                host_head_lookahead=HOST_HEAD_LOOKAHEAD,
                host_head_read_model_lookahead=HOST_HEAD_READ_MODEL_LOOKAHEAD,
            )
            self._selection = selector
        return selector

    def _admission_service(self) -> SchedulerAdmissionService:
        admission = getattr(self, "_admission", None)
        if admission is None:
            admission = SchedulerAdmissionService(
                self,
                task_cls=CrawlTask,
                url_ledger_table=URL_LEDGER_TABLE,
                blocked_host_backoff_table=BLOCKED_HOST_BACKOFF_TABLE,
                lease_table=LEASE_TABLE,
                host_head_update_dirty=HOST_HEAD_UPDATE_DIRTY,
            )
            self._admission = admission
        return admission

    def _requeue_service(self) -> SchedulerRequeueService:
        requeue = getattr(self, "_requeue", None)
        if requeue is None:
            requeue = SchedulerRequeueService(
                self,
                url_ledger_table=URL_LEDGER_TABLE,
                blocked_host_backoff_table=BLOCKED_HOST_BACKOFF_TABLE,
                intent_explore=INTENT_EXPLORE,
                intent_retry=INTENT_RETRY,
                intent_refresh=INTENT_REFRESH,
                scheduler_surface_runnable=SCHEDULER_SURFACE_RUNNABLE,
                scheduler_surface_refresh=SCHEDULER_SURFACE_REFRESH,
                scheduler_surface_scheduled=SCHEDULER_SURFACE_SCHEDULED,
            )
            self._requeue = requeue
        return requeue

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
        if not hasattr(self, "_membership"):
            if not physical_queues:
                return list(PHYSICAL_QUEUE_ORDER)
            allowed = {
                self._normalize_physical_queue(physical_queue) for physical_queue in physical_queues
            }
            return [
                physical_queue
                for physical_queue in PHYSICAL_QUEUE_ORDER
                if physical_queue in allowed
            ]
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
    ) -> list[SchedulerQueueRow]:
        """Project ledger rows into one pending physical queue."""
        return self._membership.rows_for_physical_queue(rows, physical_queue)

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
        return self._requeue_service().recover_leased_locked(now, expired_only)

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
            cur.execute(
                "SELECT to_regclass(%s)",
                (f"public.{HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE}",),
            )
            if cur.fetchone()[0] is None:
                raise RuntimeError(
                    "missing scheduler host runnable-head dirty table: "
                    f"{HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE}"
                )
            assert_public_table_columns(
                self._conn,
                HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE,
                HOST_RUNNABLE_HEAD_DIRTY_HOSTS_REQUIRED_COLUMNS,
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
            SCHEDULER_SURFACE_URGENCY[current_surface]
            <= SCHEDULER_SURFACE_URGENCY[candidate_surface]
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
            discovery_value=max(current.discovery_value, candidate.discovery_value),
            scheduler_score=max(current.scheduler_score, candidate.scheduler_score),
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
            discovery_value=task.discovery_value,
            scheduler_score=task.scheduler_score,
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
                    discovery_value=task.discovery_value,
                    scheduler_score=task.scheduler_score,
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
                f"{alias}.scheduler_score DESC"
            )

        return (
            f"{alias}.scheduler_score DESC, "
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
        return self._lease_selector().host_head_order_by_sql(alias, latency_ms_sql=latency_ms_sql)

    def _runnable_host_heads_sql(
        self,
        *,
        physical_queue: str,
        runnable_sql: _RunnableSql,
    ) -> tuple[str, tuple[object, ...]]:
        """Return SQL that derives one ready head URL per host."""
        return self._lease_selector().runnable_host_heads_sql(
            physical_queue=physical_queue,
            runnable_sql=runnable_sql,
        )

    def _runnable_host_head_sort_key(
        self, head: RunnableHostHead
    ) -> tuple[int, int, float, float, float, str]:
        """Return the canonical host-first comparison key for runnable host heads."""
        return self._lease_selector().runnable_host_head_sort_key(head)

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
        return self._lease_selector().runnable_host_heads(
            limit=limit,
            host=host,
            exclude_hosts=exclude_hosts,
            runnable_surface=runnable_surface,
            physical_queues=physical_queues,
            now=now,
        )

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
        return self._lease_selector().select_runnable_host_head(
            host=host,
            exclude_hosts=exclude_hosts,
            runnable_surface=runnable_surface,
            physical_queues=physical_queues,
            now=now,
        )

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

    def repair_host_runnable_heads(
        self,
        *,
        limit: int,
        runnable_surface: str | None = None,
        physical_queues: list[str] | None = None,
        now: float | None = None,
    ) -> HostRunnableHeadRepairSummary:
        """Repair a bounded sample of the loose host-head read model."""
        return self._host_heads.repair(
            limit=limit,
            runnable_surface=runnable_surface,
            physical_queues=physical_queues,
            now=now,
        )

    def refresh_dirty_host_runnable_heads(
        self,
        *,
        limit: int,
        runnable_surface: str | None = None,
        physical_queues: list[str] | None = None,
        now: float | None = None,
    ) -> HostRunnableHeadDirtyRefreshSummary:
        """Refresh a bounded batch of dirty host-head read-model rows."""
        return self._host_heads.refresh_dirty_hosts(
            limit=limit,
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
        execution_tiers: list[int] | None = None,
        now: float | None = None,
    ) -> list[HostRunnableHead]:
        """Read ready host-head candidates from the loose read model."""
        return self._host_heads.read(
            limit=limit,
            host=host,
            exclude_hosts=exclude_hosts,
            runnable_surface=runnable_surface,
            physical_queues=physical_queues,
            execution_tiers=execution_tiers,
            now=now,
        )

    def daemon_readiness(self, now: float | None = None) -> SchedulerReadiness:
        """Return loose readiness for daemon cycle gating without live queue scans."""
        now = time.time() if now is None else now
        head_summary = self._host_heads.readiness_summary(now=now)
        retry_quarantine = self.blocked_host_backoff_count()
        pending_from_heads = head_summary.pending_urls
        if pending_from_heads == 0 and retry_quarantine == 0:
            pending_from_heads = self.pending_count()

        pending = pending_from_heads + retry_quarantine
        runnable = head_summary.ready_urls
        scheduled = max(0, pending_from_heads - runnable)
        next_runnable_delay = (
            None
            if head_summary.next_runnable_at is None
            else max(0.0, head_summary.next_runnable_at - now)
        )
        return SchedulerReadiness(
            pending=pending,
            runnable=runnable,
            runnable_hosts=head_summary.ready_hosts,
            next_runnable_delay=next_runnable_delay,
            blocked={
                "next_fetch_at": scheduled,
                "host_next_request": 0,
                "host_backoff": 0,
                "retry_quarantine": retry_quarantine,
            },
            state_counts={
                "runnable": runnable,
                "scheduled": scheduled,
                "blocked_host_next_request": 0,
                "blocked_host_backoff": 0,
                "retry_quarantine": retry_quarantine,
            },
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
        rows: list[SchedulerQueueRowInput],
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
        self._requeue_service().insert_blocked_host_backoff_rows(
            cur,
            rows,
            quarantined_at=quarantined_at,
        )

    def _admission_physical_queue_by_url(
        self,
        tasks: list[CrawlTask],
    ) -> dict[str, str]:
        return self._admission_service().admission_physical_queue_by_url(tasks)

    def _fetch_admission_ledger_rows_for_tasks(
        self,
        cur,
        tasks: list[CrawlTask],
    ) -> list[tuple[str, str, float, float, float]]:
        """Load known ledger rows used by scheduler admission."""
        return self._admission_service().fetch_admission_ledger_rows_for_tasks(
            cur,
            tasks,
        )

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
        return Counter(urlparse(task.url).netloc for task in tasks if task.url not in existing_urls)

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
        return self._requeue_service().requeue_urls(
            urls,
            runnable_surface=runnable_surface,
            intent=intent,
            next_fetch_at=next_fetch_at,
            current_statuses=current_statuses,
        )

    def requeue_refresh_urls(
        self,
        urls: list[str],
        *,
        next_fetch_at: float | None = None,
        current_statuses: list[str] | None = None,
    ) -> int:
        """Requeue known URLs for refresh intent on the refresh runnable surface."""
        return self._requeue_service().requeue_refresh_urls(
            urls,
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
                    task.discovery_value,
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
                           url, host, discovery_value, source_url, added_at, next_fetch_at, current_intent
                       )
                       VALUES %s
                       ON CONFLICT (url) DO UPDATE SET
                           discovery_value = GREATEST({URL_LEDGER_TABLE}.discovery_value, EXCLUDED.discovery_value),
                           source_url = COALESCE({URL_LEDGER_TABLE}.source_url, EXCLUDED.source_url),
                           added_at = LEAST({URL_LEDGER_TABLE}.added_at, EXCLUDED.added_at),
                           next_fetch_at = LEAST({URL_LEDGER_TABLE}.next_fetch_at, EXCLUDED.next_fetch_at),
                           current_intent = COALESCE(EXCLUDED.current_intent, {URL_LEDGER_TABLE}.current_intent)
                       WHERE
                           EXCLUDED.discovery_value > {URL_LEDGER_TABLE}.discovery_value
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
        return self._admission_service().admit_queue_membership(tasks)

    def admit_discovered_tasks(self, tasks: list[CrawlTask]) -> int:
        """Assign scheduler membership to discovered URLs using task admission metadata."""
        return self._admission_service().admit_discovered_tasks(tasks)

    def admit_urls(
        self,
        urls: list[str],
        *,
        runnable_surface: str | None = None,
        intent: str | None = None,
    ) -> int:
        """Assign scheduler membership to known ledger URLs using surface and intent."""
        return self._admission_service().admit_urls(
            urls,
            runnable_surface=runnable_surface,
            intent=intent,
        )

    def _select_admission_candidate_rows(
        self,
        cur,
        *,
        limit: int,
    ) -> list[tuple[str, str, float, float, float]]:
        """Return ledger rows that are known but lack current scheduler membership."""
        return self._admission_service().select_admission_candidate_rows(cur, limit=limit)

    def admit_discovered_urls(
        self,
        limit: int,
        *,
        runnable_surface: str | None = None,
        intent: str | None = None,
    ) -> int:
        """Assign scheduler membership to discovered ledger rows without task metadata."""
        return self._admission_service().admit_discovered_urls(
            limit,
            runnable_surface=runnable_surface,
            intent=intent,
        )

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
        execution_tiers: list[int] | None = None,
    ) -> CrawlTask | None:
        """Lease the next runnable URL, optionally filtered by host."""
        return self._lease_selector().lease_next(
            host=host,
            lease_seconds=lease_seconds,
            lease_strategy=lease_strategy,
            exclude_hosts=exclude_hosts,
            runnable_surface=runnable_surface,
            execution_tiers=execution_tiers,
        )

    def _lease_next_host_first(
        self,
        *,
        host: str | None,
        lease_seconds: float | None,
        exclude_hosts: list[str] | None,
        physical_queue: str | None = None,
        physical_queues: list[str] | None = None,
        execution_tiers: list[int] | None = None,
    ) -> CrawlTask | None:
        """Lease from the next selected runnable host head."""
        return self._lease_selector().lease_next_host_first(
            host=host,
            lease_seconds=lease_seconds,
            exclude_hosts=exclude_hosts,
            physical_queue=physical_queue,
            physical_queues=physical_queues,
            execution_tiers=execution_tiers,
        )

    def _lease_next_host_first_from_read_model(
        self,
        *,
        host: str | None,
        lease_seconds: float | None,
        exclude_hosts: list[str] | None,
        physical_queues: list[str],
        execution_tiers: list[int] | None,
        now: float,
    ) -> _HostFirstReadModelResult:
        """Lease host-first candidates from the loose read model first."""
        return self._lease_selector().lease_next_host_first_from_read_model(
            host=host,
            lease_seconds=lease_seconds,
            exclude_hosts=exclude_hosts,
            physical_queues=physical_queues,
            execution_tiers=execution_tiers,
            now=now,
        )

    def _lease_next_host_first_from_bounded_scan(
        self,
        *,
        host: str | None,
        lease_seconds: float | None,
        exclude_hosts: list[str] | None,
        physical_queues: list[str],
        now: float,
    ) -> CrawlTask | None:
        """Lease from a bounded queue scan when the host-head cache misses."""
        return self._lease_selector().lease_next_host_first_from_bounded_scan(
            host=host,
            lease_seconds=lease_seconds,
            exclude_hosts=exclude_hosts,
            physical_queues=physical_queues,
            now=now,
        )

    def _lease_next_host_first_from_bounded_scan_queue(
        self,
        *,
        host: str | None,
        lease_seconds: float | None,
        exclude_hosts: list[str] | None,
        physical_queue: str,
        now: float,
    ) -> CrawlTask | None:
        """Lease from one physical queue using a bounded host-first scan."""
        return self._lease_selector().lease_next_host_first_from_bounded_scan_queue(
            host=host,
            lease_seconds=lease_seconds,
            exclude_hosts=exclude_hosts,
            physical_queue=physical_queue,
            now=now,
        )

    def _lease_next_url_order(
        self,
        *,
        host: str | None,
        lease_seconds: float | None,
        exclude_hosts: list[str] | None,
        physical_queue: str,
    ) -> CrawlTask | None:
        """Lease using URL-order selection from one physical queue."""
        return self._lease_selector().lease_next_url_order(
            host=host,
            lease_seconds=lease_seconds,
            exclude_hosts=exclude_hosts,
            physical_queue=physical_queue,
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
        return self._lease_selector().lease_candidate_url(
            candidate_url=candidate_url,
            physical_queue=physical_queue,
            lease_seconds=lease_seconds,
            host=host,
            exclude_hosts=exclude_hosts,
            now=now,
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
        return self._lease_selector().lease_batch(
            count=count,
            host=host,
            lease_seconds=lease_seconds,
            lease_strategy=lease_strategy,
            exclude_hosts=exclude_hosts,
            runnable_surface=runnable_surface,
        )

    def mark_done(self, url: str, lease_token: str | None = None) -> bool:
        """Mark a URL as successfully crawled."""
        normalized = normalize_url(url)
        now = time.time()
        lease_sql, lease_params = self._leases.match_sql("ledger", lease_token)

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
            self._leases.delete(cur, [row[0] for row in rows])
            for _url, host in rows:
                self._host_ledger.record_success_in_tx(cur, host, at=now)
            updated = bool(rows)
        self._conn.commit()
        return updated

    def mark_done_many(self, tasks: list[CrawlTask]) -> int:
        """Mark multiple leased URLs as successfully crawled in one transaction."""
        rows_by_url: dict[str, tuple[str, str | None]] = {}
        for task in tasks:
            if task.url:
                normalized = normalize_url(task.url)
                rows_by_url[normalized] = (normalized, task.lease_token)
        now = time.time()
        rows = [
            (normalized, lease_token, now, now)
            for normalized, lease_token in rows_by_url.values()
        ]
        if not rows:
            return 0

        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"""WITH incoming(url, lease_token, next_fetch_at, last_success_at) AS (VALUES %s),
                    updated AS (
                        UPDATE {URL_LEDGER_TABLE} AS ledger
                        SET next_fetch_at = incoming.next_fetch_at,
                            current_intent = NULL,
                            last_success_at = incoming.last_success_at,
                            fail_streak = 0,
                            last_error = NULL,
                            terminal_reason = NULL,
                            terminalized_at = NULL
                        FROM incoming
                        WHERE ledger.url = incoming.url
                          AND (
                              incoming.lease_token IS NULL
                              OR EXISTS (
                                  SELECT 1
                                  FROM {LEASE_TABLE} AS active
                                  WHERE active.url = ledger.url
                                    AND active.lease_token = incoming.lease_token
                              )
                          )
                        RETURNING ledger.url, ledger.host
                    )
                    SELECT url, host FROM updated""",
                rows,
                template="(%s, %s, %s, %s)",
                page_size=200,
            )
            updated_rows = list(cur.fetchall())
            updated_urls = [row[0] for row in updated_rows]
            self._delete_queue_entries(cur, updated_urls)
            self._leases.delete(cur, updated_urls)
            for _url, host in updated_rows:
                self._host_ledger.record_success_in_tx(cur, host, at=now)
        self._conn.commit()
        return len(updated_rows)

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
        lease_sql, lease_params = self._leases.match_sql("ledger", lease_token)

        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT fail_streak, discovery_value, host FROM {URL_LEDGER_TABLE} AS ledger WHERE url = %s{lease_sql} FOR UPDATE",
                (normalized, *lease_params),
            )
            row = cur.fetchone()
            if row is None:
                self._conn.rollback()
                return False

            transition = self._scheduler_retry_policy().failure_transition(
                fail_streak=row[0],
                discovery_value=row[1],
                retryable=retryable,
                error=error,
                backoff_seconds=backoff_seconds,
                now=now,
            )

            if transition.retryable:
                cur.execute(
                    f"""UPDATE {URL_LEDGER_TABLE} AS ledger
                        SET next_fetch_at = %s,
                            current_intent = %s,
                            fail_streak = %s,
                            last_error = %s,
                            terminal_reason = NULL,
                            terminalized_at = NULL
                        WHERE url = %s{lease_sql}
                        RETURNING url, host, %s::real AS scheduler_score, next_fetch_at, added_at""",
                    (
                        transition.next_fetch_at,
                        transition.current_intent,
                        transition.next_fail_streak,
                        transition.last_error,
                        normalized,
                        *lease_params,
                        transition.next_scheduler_score,
                    ),
                )
                rows = cur.fetchall()
                for _url, host, *_rest in rows:
                    self._host_ledger.record_failure_in_tx(cur, host, at=now)
                pending_rows = self._pending_rows_for_physical_queue(
                    rows,
                    self._single_physical_queue_for_surface(SCHEDULER_SURFACE_SCHEDULED),
                )
                self._membership.replace_pending_rows(cur, pending_rows)
                self._leases.delete(cur, self._membership.row_urls(pending_rows))
                updated = bool(rows)
            else:
                cur.execute(
                    f"""UPDATE {URL_LEDGER_TABLE} AS ledger
                        SET next_fetch_at = %s,
                            current_intent = NULL,
                            fail_streak = %s,
                            last_error = %s,
                            terminal_reason = %s,
                            terminalized_at = %s
                        WHERE url = %s{lease_sql}
                        RETURNING url, host""",
                    (
                        transition.next_fetch_at,
                        transition.next_fail_streak,
                        transition.last_error,
                        transition.terminal_reason,
                        transition.terminalized_at,
                        normalized,
                        *lease_params,
                    ),
                )
                rows = cur.fetchall()
                urls = [row[0] for row in rows]
                self._delete_queue_entries(cur, urls)
                self._leases.delete(cur, urls)
                for _url, host in rows:
                    self._host_ledger.record_failure_in_tx(cur, host, at=now)
                updated = bool(rows)
        self._conn.commit()
        return updated

    def requeue_failed(self) -> int:
        """Requeue failed URLs for retry."""
        return self._requeue_service().requeue_failed()

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
        return self._requeue_service().recover_leased(expired_only=expired_only)

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
                                ORDER BY queue.scheduler_score DESC, queue.next_fetch_at ASC, queue.added_at ASC, queue.url ASC
                            ) AS host_rownum,
                            ROW_NUMBER() OVER (
                                PARTITION BY queue.host, queue.branch_key
                                ORDER BY queue.scheduler_score DESC, queue.next_fetch_at ASC, queue.added_at ASC, queue.url ASC
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
                    RETURNING url, host, discovery_value, next_fetch_at, added_at""",
                params,
            )
            rows = cur.fetchall()
            pending_rows = self._pending_rows_for_physical_queue(
                rows,
                self._single_physical_queue_for_surface(SCHEDULER_SURFACE_SCHEDULED),
            )
            self._membership.replace_pending_rows(cur, pending_rows)
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
                    ORDER BY scheduler_score DESC, added_at ASC, url ASC
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
                f"""SELECT url, host, scheduler_score, next_fetch_at, added_at
                    FROM {self._queue_table_sql(self._single_physical_queue_for_surface(SCHEDULER_SURFACE_SCHEDULED))}
                    WHERE url = ANY(%s)""",
                (promoted_urls,),
            )
            rows = self._pending_rows_for_physical_queue(
                cur.fetchall(),
                self._single_physical_queue_for_surface(SCHEDULER_SURFACE_RUNNABLE),
            )
            self._membership.delete_queue_entries(cur, self._membership.row_urls(rows))
            self._membership.insert_pending_rows(cur, rows)
            count = len(rows)

        self._conn.commit()
        return count

    def upsert_seeds(self, urls: list[str], discovery_value: float = 2.0) -> int:
        """Insert or requeue seed URLs."""
        return self._requeue_service().upsert_seeds(urls, discovery_value=discovery_value)

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
