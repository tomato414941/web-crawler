"""Scheduler queue topology and ordering policy."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .scheduler_membership import (
    PHYSICAL_QUEUE_TABLES,
    SCHEDULER_SURFACE_REFRESH,
    SCHEDULER_SURFACE_RUNNABLE,
    SCHEDULER_SURFACE_SCHEDULED,
    SCHEDULER_SURFACE_URGENCY,
    SchedulerMembershipStore,
)
from .scheduler_task import CrawlTask, INTENT_EXPLORE, INTENT_REFRESH, INTENT_RETRY

if TYPE_CHECKING:
    from .host_store import HostStore


LEASE_STRATEGY_URL_ORDER = "url_order"
LEASE_STRATEGY_HOST_FIRST = "host_first"
LATENCY_BUCKET_FAST_MS = 150.0
LATENCY_BUCKET_SLOW_MS = 400.0
LATENCY_BUCKET_VERY_SLOW_MS = 1000.0

LEASE_STRATEGIES = {LEASE_STRATEGY_URL_ORDER, LEASE_STRATEGY_HOST_FIRST}
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


@dataclass(frozen=True)
class RunnableSql:
    """SQL fragments for pending URL readiness checks."""

    where: str
    params: tuple[object, ...]
    runnable_at: str
    join_sql: str = ""
    latency_ms_sql: str = "0"


class SchedulerQueuePolicy:
    """Resolve scheduler queue topology, readiness, and ordering."""

    def __init__(self, membership: SchedulerMembershipStore) -> None:
        self._membership = membership
        self._host_store: HostStore | None = None

    def attach_host_store(self, host_store: HostStore | None) -> None:
        self._host_store = host_store

    def queue_membership_join_sql(self, *, ledger_alias: str) -> tuple[str, str]:
        physical_queues = self._membership.physical_queues()
        joins = "\n                ".join(
            f"LEFT JOIN {PHYSICAL_QUEUE_TABLES[queue]} AS {queue} ON {queue}.url = {ledger_alias}.url"
            for queue in physical_queues
        )
        absence = "\n                  AND ".join(
            f"{queue}.url IS NULL" for queue in physical_queues
        )
        return joins, absence

    def normalized_physical_queues(self, physical_queues: list[str] | None) -> list[str]:
        return self._membership.normalized_physical_queues(physical_queues)

    def normalized_surface_queues(
        self,
        *,
        runnable_surface: str | None,
        physical_queues: list[str] | None,
    ) -> list[str]:
        return self._membership.normalized_surface_queues(
            scheduler_surface=runnable_surface,
            physical_queues=physical_queues,
        )

    def single_physical_queue_for_surface(self, runnable_surface: str) -> str:
        return self._membership.single_physical_queue_for_surface(runnable_surface)

    def normalize_intent(self, intent: str | None) -> str | None:
        if intent is None:
            return None
        normalized = str(intent).strip().lower()
        if normalized not in {INTENT_EXPLORE, INTENT_REFRESH, INTENT_RETRY}:
            raise ValueError(f"Unknown intent: {intent}")
        return normalized

    def default_scheduled_physical_queue(self) -> str:
        return self.single_physical_queue_for_surface(SCHEDULER_SURFACE_SCHEDULED)

    def physical_queue_for_model(self, *, runnable_surface: str | None, intent: str | None) -> str:
        normalized_intent = self.normalize_intent(intent)
        resolved_surface = runnable_surface
        if resolved_surface is None and normalized_intent is not None:
            resolved_surface = INTENT_DEFAULT_SCHEDULER_SURFACE[normalized_intent]
        if resolved_surface is None:
            return self.default_scheduled_physical_queue()
        physical_queues = self.normalized_surface_queues(
            runnable_surface=resolved_surface,
            physical_queues=None,
        )
        if len(physical_queues) != 1:
            raise ValueError(
                f"Admission surface must resolve to one physical queue: {resolved_surface}"
            )
        return physical_queues[0]

    def intent_for_model(self, *, runnable_surface: str | None, intent: str | None) -> str | None:
        normalized_intent = self.normalize_intent(intent)
        if normalized_intent is not None:
            return normalized_intent
        resolved_surface = runnable_surface or SCHEDULER_SURFACE_SCHEDULED
        return SCHEDULER_SURFACE_DEFAULT_INTENT.get(str(resolved_surface).strip().lower())

    def task_runnable_surface(self, task: CrawlTask) -> str:
        if task.runnable_surface is not None:
            return str(task.runnable_surface).strip().lower()
        normalized_intent = self.normalize_intent(task.intent)
        if normalized_intent is not None:
            return INTENT_DEFAULT_SCHEDULER_SURFACE[normalized_intent]
        return SCHEDULER_SURFACE_SCHEDULED

    def normalize_task_metadata(
        self, task: CrawlTask, *, normalized_url: str | None = None
    ) -> CrawlTask:
        resolved_surface = self.task_runnable_surface(task)
        resolved_intent = self.normalize_intent(
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

    def merge_tasks(self, current: CrawlTask, candidate: CrawlTask) -> CrawlTask:
        current_surface = self.task_runnable_surface(current)
        candidate_surface = self.task_runnable_surface(candidate)
        merged_surface = (
            current_surface
            if SCHEDULER_SURFACE_URGENCY[current_surface]
            <= SCHEDULER_SURFACE_URGENCY[candidate_surface]
            else candidate_surface
        )
        return CrawlTask(
            url=current.url,
            discovery_value=max(current.discovery_value, candidate.discovery_value),
            scheduler_score=max(current.scheduler_score, candidate.scheduler_score),
            runnable_surface=merged_surface,
            intent=current.intent
            or candidate.intent
            or SCHEDULER_SURFACE_DEFAULT_INTENT.get(merged_surface),
            source_url=current.source_url or candidate.source_url,
            added_at=min(current.added_at, candidate.added_at),
            next_fetch_at=min(current.next_fetch_at, candidate.next_fetch_at),
        )

    def normalize_lease_strategy(self, lease_strategy: str | None) -> str:
        if lease_strategy is None:
            return LEASE_STRATEGY_URL_ORDER
        normalized = str(lease_strategy).strip().lower()
        if normalized not in LEASE_STRATEGIES:
            raise ValueError(f"Unknown lease strategy: {lease_strategy}")
        return normalized

    def latency_penalty_sql(self, *, latency_ms_sql: str | None = None) -> str:
        latency_ms = latency_ms_sql or "0"
        return (
            "CASE "
            f"WHEN {latency_ms} >= {LATENCY_BUCKET_VERY_SLOW_MS} THEN 3 "
            f"WHEN {latency_ms} >= {LATENCY_BUCKET_SLOW_MS} THEN 2 "
            f"WHEN {latency_ms} >= {LATENCY_BUCKET_FAST_MS} THEN 1 "
            "ELSE 0 END"
        )

    def lease_order_by_sql(
        self,
        alias: str,
        lease_strategy: str,
        *,
        latency_ms_sql: str | None = None,
    ) -> str:
        latency_penalty = self.latency_penalty_sql(latency_ms_sql=latency_ms_sql)
        if lease_strategy == LEASE_STRATEGY_HOST_FIRST:
            return (
                f"{alias}.next_fetch_at ASC, {latency_penalty} ASC, "
                f"{alias}.added_at ASC, {alias}.scheduler_score DESC"
            )
        return (
            f"{alias}.scheduler_score DESC, {latency_penalty} ASC, "
            f"{alias}.next_fetch_at ASC, {alias}.added_at ASC"
        )

    def queue_runnable_sql(
        self,
        *,
        alias: str,
        now: float,
        host: str | None = None,
        exclude_hosts: list[str] | None = None,
    ) -> RunnableSql:
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
            conditions.extend([f"{next_request_sql} <= %s", f"{backoff_sql} <= %s"])
            params.extend([now, now])
        if host:
            conditions.append(f"{alias}.host = %s")
            params.append(host)
        if exclude_hosts:
            conditions.append(f"NOT ({alias}.host = ANY(%s))")
            params.append(exclude_hosts)
        return RunnableSql(
            where=" AND ".join(conditions),
            params=tuple(params),
            runnable_at=f"GREATEST({alias}.next_fetch_at, {next_request_sql}, {backoff_sql})",
            join_sql=join_sql,
            latency_ms_sql=latency_ms_sql,
        )
