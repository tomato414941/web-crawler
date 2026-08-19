"""PostgreSQL-backed crawl scheduler."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING
from .config import settings
from .host_runnable_heads import (
    HOST_RUNNABLE_HEADS_TABLE as _HOST_RUNNABLE_HEADS_TABLE,
    HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE as _HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE,
    HostRunnableHead,
    HostRunnableHeadDirtyRefreshSummary,
    HostRunnableHeadRepairSummary,
    HostRunnableHeadStore,
)
from .host_ledger import HostLedgerStore
from .scheduler_lease_telemetry import HostFirstLeaseTelemetry
from .scheduler_membership import (
    PHYSICAL_QUEUE_DEFAULT_SCHEDULER_SURFACE as _PHYSICAL_QUEUE_DEFAULT_SCHEDULER_SURFACE,
    PHYSICAL_QUEUE_ORDER as _PHYSICAL_QUEUE_ORDER,
    PHYSICAL_QUEUE_TABLES as _PHYSICAL_QUEUE_TABLES,
    QUEUE_TABLES,
    SCHEDULER_SURFACE_REFRESH as _SCHEDULER_SURFACE_REFRESH,
    SCHEDULER_SURFACE_RUNNABLE as _SCHEDULER_SURFACE_RUNNABLE,
    SCHEDULER_SURFACE_SCHEDULED as _SCHEDULER_SURFACE_SCHEDULED,
    SchedulerMembershipStore,
)
from .scheduler_leases import (
    ACTIVE_LEASES_TABLE,
    LEASE_REQUIRED_COLUMNS,
    ExecutionLeaseStore,
)
from .scheduler_admission import (
    SchedulerAdmissionService,
)
from .scheduler_completion import SchedulerCompletionService
from .scheduler_requeue import SchedulerRequeueService
from .scheduler_lease_service import SchedulerLeaseService
from .scheduler_queue_policy import (
    LEASE_STRATEGY_HOST_FIRST as _LEASE_STRATEGY_HOST_FIRST,
    LEASE_STRATEGY_URL_ORDER as _LEASE_STRATEGY_URL_ORDER,
    SchedulerQueuePolicy,
)
from .scheduler_observability import SchedulerObservability, SchedulerReadiness
from .scheduler_quarantine import (
    BLOCKED_HOST_BACKOFF_TABLE as _BLOCKED_HOST_BACKOFF_TABLE,
    SchedulerQuarantineService,
)
from .scheduler_retry_policy import SchedulerRetryPolicy
from .scheduler_task import (
    CrawlTask as _CrawlTask,
    INTENT_RETRY as _INTENT_RETRY,
)
from .schema import assert_public_table_columns
from .url_ledger_store import URL_LEDGER_TABLE, UrlLedgerStore

if TYPE_CHECKING:
    from .host_store import HostStore

logger = logging.getLogger(__name__)

URL_LEDGER_REQUIRED_COLUMNS = {
    "url",
    "url_hash",
    "url_length",
    "url_identity_version",
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
QUEUE_REQUIRED_COLUMNS = {
    "url",
    "host",
    "scheduler_score",
    "next_fetch_at",
    "added_at",
    "branch_key",
}
_LEASE_TABLE = ACTIVE_LEASES_TABLE
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
HOST_HEAD_LOOKAHEAD = 32
HOST_HEAD_READ_MODEL_LOOKAHEAD = HOST_HEAD_LOOKAHEAD * 4


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

    task: _CrawlTask | None
    read_model: str
    candidates: int = 0
    stale_candidates: int = 0
    execution_tier: int | None = None


def _elapsed_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 1)


class Scheduler:
    """Coordinate durable crawl scheduling services."""

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
        retry_backoff_seconds = (
            settings.scheduler_retry_backoff_seconds
            if retry_backoff_seconds is None
            else retry_backoff_seconds
        )
        max_retry_backoff_seconds = (
            settings.scheduler_max_retry_backoff_seconds
            if max_retry_backoff_seconds is None
            else max_retry_backoff_seconds
        )
        self._retry_policy = SchedulerRetryPolicy(
            retry_backoff_seconds=retry_backoff_seconds,
            max_retry_backoff_seconds=max_retry_backoff_seconds,
            retry_intent=_INTENT_RETRY,
        )
        self._host_ledger = HostLedgerStore(conn)
        self._membership = SchedulerMembershipStore(
            conn,
            blocked_queue_table=_BLOCKED_HOST_BACKOFF_TABLE,
            host_runnable_heads_table=_HOST_RUNNABLE_HEADS_TABLE,
            host_runnable_head_dirty_hosts_table=_HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE,
        )
        self._queue_policy = SchedulerQueuePolicy(self._membership)
        self._leases = ExecutionLeaseStore(
            conn,
            normalize_physical_queue=self._membership.normalize_physical_queue,
        )
        self._host_heads = HostRunnableHeadStore(
            conn,
            table_name=_HOST_RUNNABLE_HEADS_TABLE,
            queue_table_sql=self._membership.queue_table_sql,
            normalize_physical_queue=self._membership.normalize_physical_queue,
            normalized_surface_queues=self._queue_policy.normalized_surface_queues,
            latency_penalty_sql=lambda _alias, *, latency_ms_sql=None: (
                self._queue_policy.latency_penalty_sql(latency_ms_sql=latency_ms_sql)
            ),
            dirty_hosts_table_name=_HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE,
        )
        self._membership.attach_host_heads(self._host_heads)
        self._lease_telemetry = HostFirstLeaseTelemetry()
        self._ledger_store = UrlLedgerStore(
            conn,
            host_ledger=self._host_ledger,
            queue_policy=self._queue_policy,
        )
        self._requeue = SchedulerRequeueService(
            conn,
            membership=self._membership,
            leases=self._leases,
            host_ledger=self._host_ledger,
            retry_policy=self._retry_policy,
            queue_policy=self._queue_policy,
            url_ledger_table=URL_LEDGER_TABLE,
            blocked_host_backoff_table=_BLOCKED_HOST_BACKOFF_TABLE,
        )
        self._selection = SchedulerLeaseService(
            conn,
            membership=self._membership,
            leases=self._leases,
            host_heads=self._host_heads,
            requeue=self._requeue,
            telemetry=self._lease_telemetry,
            queue_policy=self._queue_policy,
            lease_seconds=self._lease_seconds,
            task_cls=_CrawlTask,
            runnable_host_head_cls=RunnableHostHead,
            url_ledger_table=URL_LEDGER_TABLE,
            lease_strategy_url_order=_LEASE_STRATEGY_URL_ORDER,
            lease_strategy_host_first=_LEASE_STRATEGY_HOST_FIRST,
            host_head_lookahead=HOST_HEAD_LOOKAHEAD,
            host_head_read_model_lookahead=HOST_HEAD_READ_MODEL_LOOKAHEAD,
        )
        self._admission = SchedulerAdmissionService(
            conn,
            ledger_store=self._ledger_store,
            membership=self._membership,
            leases=self._leases,
            queue_policy=self._queue_policy,
            url_ledger_table=URL_LEDGER_TABLE,
            blocked_host_backoff_table=_BLOCKED_HOST_BACKOFF_TABLE,
            lease_table=_LEASE_TABLE,
        )
        self._completion = SchedulerCompletionService(
            conn,
            membership=self._membership,
            leases=self._leases,
            host_ledger=self._host_ledger,
            retry_policy=self._retry_policy,
            queue_policy=self._queue_policy,
        )
        self._observability = SchedulerObservability(
            conn,
            physical_queue_tables=_PHYSICAL_QUEUE_TABLES,
            physical_queue_order=_PHYSICAL_QUEUE_ORDER,
            physical_queue_default_runnable_surface=_PHYSICAL_QUEUE_DEFAULT_SCHEDULER_SURFACE,
            blocked_queue_table=_BLOCKED_HOST_BACKOFF_TABLE,
            lease_table=_LEASE_TABLE,
        )
        self._quarantine = SchedulerQuarantineService(
            conn,
            queue_runnable=self._queue_policy.single_physical_queue_for_surface(
                _SCHEDULER_SURFACE_RUNNABLE
            ),
            queue_scheduled=self._queue_policy.single_physical_queue_for_surface(
                _SCHEDULER_SURFACE_SCHEDULED
            ),
            queue_refresh=self._queue_policy.single_physical_queue_for_surface(
                _SCHEDULER_SURFACE_REFRESH
            ),
            blocked_queue_table=_BLOCKED_HOST_BACKOFF_TABLE,
            queue_table_sql=self._membership.queue_table_sql,
            delete_queue_entries=self._membership.delete_queue_entries,
            insert_blocked_rows=self._requeue.insert_blocked_host_backoff_rows,
            insert_pending_rows=self._membership.insert_pending_rows,
        )
        self._assert_current_schema()

    def reset_host_first_fallback_stats(self) -> None:
        """Reset cycle-local host-first fallback counters."""
        self._lease_telemetry.reset()

    def host_first_fallback_stats(self) -> dict[str, int]:
        """Return cycle-local host-first fallback counters."""
        return self._lease_telemetry.fallback_stats()

    def last_lease_diagnostics(self) -> dict[str, object]:
        """Return diagnostics for the most recent lease_next call on this ledger."""
        return self._lease_telemetry.last_lease_diagnostics()

    def last_admission_diagnostics(self) -> dict[str, float]:
        """Return timing diagnostics for the most recent scheduler admission."""
        return self._admission.last_diagnostics()

    def attach_host_store(self, host_store: "HostStore | None") -> None:
        """Attach the persistent host scheduler used for lease selection."""
        self._queue_policy.attach_host_store(host_store)

    @property
    def host_ledger_store(self) -> HostLedgerStore:
        """Return the durable host identity/history store."""
        return self._host_ledger

    def _assert_current_schema(self) -> None:
        assert_public_table_columns(self._conn, URL_LEDGER_TABLE, URL_LEDGER_REQUIRED_COLUMNS)

        with self._conn.cursor() as cur:
            for table_name in QUEUE_TABLES:
                cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
                if cur.fetchone()[0] is None:
                    raise RuntimeError(f"missing scheduler queue table: {table_name}")
                assert_public_table_columns(self._conn, table_name, QUEUE_REQUIRED_COLUMNS)
            cur.execute("SELECT to_regclass(%s)", (f"public.{_BLOCKED_HOST_BACKOFF_TABLE}",))
            if cur.fetchone()[0] is None:
                raise RuntimeError(
                    f"missing scheduler blocked queue table: {_BLOCKED_HOST_BACKOFF_TABLE}"
                )
            assert_public_table_columns(
                self._conn,
                _BLOCKED_HOST_BACKOFF_TABLE,
                BLOCKED_QUEUE_REQUIRED_COLUMNS,
            )
            cur.execute("SELECT to_regclass(%s)", (f"public.{_LEASE_TABLE}",))
            if cur.fetchone()[0] is None:
                raise RuntimeError(f"missing scheduler lease table: {_LEASE_TABLE}")
            assert_public_table_columns(self._conn, _LEASE_TABLE, LEASE_REQUIRED_COLUMNS)
            cur.execute("SELECT to_regclass(%s)", (f"public.{_HOST_RUNNABLE_HEADS_TABLE}",))
            if cur.fetchone()[0] is None:
                raise RuntimeError(
                    f"missing scheduler host runnable-head table: {_HOST_RUNNABLE_HEADS_TABLE}"
                )
            assert_public_table_columns(
                self._conn,
                _HOST_RUNNABLE_HEADS_TABLE,
                HOST_RUNNABLE_HEADS_REQUIRED_COLUMNS,
            )
            cur.execute(
                "SELECT to_regclass(%s)",
                (f"public.{_HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE}",),
            )
            if cur.fetchone()[0] is None:
                raise RuntimeError(
                    "missing scheduler host runnable-head dirty table: "
                    f"{_HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE}"
                )
            assert_public_table_columns(
                self._conn,
                _HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE,
                HOST_RUNNABLE_HEAD_DIRTY_HOSTS_REQUIRED_COLUMNS,
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
        return self._selection.runnable_host_heads(
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
        return self._selection.select_runnable_host_head(
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
        return self._requeue.requeue_urls(
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
        return self._requeue.requeue_refresh_urls(
            urls,
            next_fetch_at=next_fetch_at,
            current_statuses=current_statuses,
        )

    def discover(self, task: _CrawlTask) -> bool:
        """Insert one discovered URL into the ledger without scheduler membership."""
        _prepared, changed = self._ledger_store.upsert_tasks([task])
        return changed > 0

    def discover_many(self, tasks: list[_CrawlTask]) -> int:
        """Insert discovered URLs into the ledger without placing them into queue tables."""
        _prepared, changed = self._ledger_store.upsert_tasks(tasks)
        return changed

    def admit_discovered_tasks(self, tasks: list[_CrawlTask]) -> int:
        """Assign scheduler membership to discovered URLs using task admission metadata."""
        return self._admission.admit_discovered_tasks(tasks)

    def admit_urls(
        self,
        urls: list[str],
        *,
        runnable_surface: str | None = None,
        intent: str | None = None,
    ) -> int:
        """Assign scheduler membership to known ledger URLs using surface and intent."""
        return self._admission.admit_urls(
            urls,
            runnable_surface=runnable_surface,
            intent=intent,
        )

    def admit_discovered_urls(
        self,
        limit: int,
        *,
        runnable_surface: str | None = None,
        intent: str | None = None,
    ) -> int:
        """Assign scheduler membership to discovered ledger rows without task metadata."""
        return self._admission.admit_discovered_urls(
            limit,
            runnable_surface=runnable_surface,
            intent=intent,
        )

    def preview_tasks(self, tasks: list[_CrawlTask]) -> list[_CrawlTask]:
        """Return normalized tasks with physical queues implied without writing them."""
        return self._ledger_store.prepare_tasks(tasks)

    def place(self, task: _CrawlTask) -> bool:
        """Place one discovered URL candidate into scheduler storage."""
        return self.place_many([task]) > 0

    def place_many(self, tasks: list[_CrawlTask]) -> int:
        """Place multiple discovered URL candidates into scheduler storage."""
        prepared_tasks, changed = self._ledger_store.upsert_tasks(tasks)
        if not prepared_tasks:
            return changed
        admitted = self._admission.admit_discovered_tasks(prepared_tasks)
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
    ) -> _CrawlTask | None:
        """Lease the next runnable URL, optionally filtered by host."""
        return self._selection.lease_next(
            host=host,
            lease_seconds=lease_seconds,
            lease_strategy=lease_strategy,
            exclude_hosts=exclude_hosts,
            runnable_surface=runnable_surface,
            execution_tiers=execution_tiers,
        )

    def lease_batch(
        self,
        count: int = 10,
        host: str | None = None,
        lease_seconds: float | None = None,
        lease_strategy: str | None = None,
        exclude_hosts: list[str] | None = None,
        runnable_surface: str | None = None,
    ) -> list[_CrawlTask]:
        """Lease a batch of runnable URLs."""
        return self._selection.lease_batch(
            count=count,
            host=host,
            lease_seconds=lease_seconds,
            lease_strategy=lease_strategy,
            exclude_hosts=exclude_hosts,
            runnable_surface=runnable_surface,
        )

    def mark_done(self, url: str, lease_token: str | None = None) -> bool:
        """Mark a URL as successfully crawled."""
        return self._completion.mark_done(url, lease_token=lease_token)

    def mark_done_many(self, tasks: list[_CrawlTask]) -> int:
        """Mark multiple leased URLs as successfully crawled in one transaction."""
        return self._completion.mark_done_many(tasks)

    def mark_failed(
        self,
        url: str,
        retryable: bool = False,
        error: str | None = None,
        backoff_seconds: float | None = None,
        lease_token: str | None = None,
    ) -> bool:
        """Mark a URL as failed, optionally scheduling a retry."""
        return self._completion.mark_failed(
            url,
            retryable=retryable,
            error=error,
            backoff_seconds=backoff_seconds,
            lease_token=lease_token,
        )

    def requeue_failed(self) -> int:
        """Requeue failed URLs for retry."""
        return self._requeue.requeue_failed()

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
        return self._requeue.recover_leased(expired_only=expired_only)

    def delay_overcrowded_scheduled_surface(
        self,
        *,
        keep_runnable_per_host: int = 128,
        keep_runnable_per_branch: int = 16,
        limit: int | None = None,
        delay_seconds: float = 1800.0,
    ) -> int:
        """Delay excess scheduled work from overrepresented hosts and branches."""
        return self._requeue.delay_overcrowded_scheduled_surface(
            keep_runnable_per_host=keep_runnable_per_host,
            keep_runnable_per_branch=keep_runnable_per_branch,
            limit=limit,
            delay_seconds=delay_seconds,
        )

    def promote_scheduled_host_heads(
        self,
        target_pending: int,
        *,
        per_host: int = 1,
        candidate_limit: int = 200,
    ) -> int:
        """Promote one scheduled head per host into the runnable queue."""
        return self._requeue.promote_scheduled_host_heads(
            target_pending,
            current_runnable=self.pending_count(runnable_surface=_SCHEDULER_SURFACE_RUNNABLE),
            per_host=per_host,
            candidate_limit=candidate_limit,
        )

    def upsert_seeds(self, urls: list[str], discovery_value: float = 2.0) -> int:
        """Insert or requeue seed URLs."""
        return self._requeue.upsert_seeds(urls, discovery_value=discovery_value)

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
        return self._observability.readiness(
            now=now,
            runnable_surface=runnable_surface,
        )

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
        return self._ledger_store.is_seen(url)
