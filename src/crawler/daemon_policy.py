"""Pre-cycle scheduler policy for the crawl daemon."""

from __future__ import annotations

from collections.abc import Callable

from .url_ledger import SCHEDULER_SURFACE_SCHEDULED, SCHEDULER_SURFACE_RUNNABLE


class DaemonSchedulerPolicy:
    """Thin policy layer for scheduler maintenance between crawl cycles."""

    def __init__(
        self,
        *,
        cycle_pages: int,
        min_runnable_supply_count: int,
        min_runnable_supply_hosts: int,
        blocked_retry_budget: int,
        blocked_retry_per_host: int,
        blocked_retry_max_consecutive_failures: int,
        quarantine_retire_min_consecutive_failures: int,
        quarantine_retire_after_seconds: float,
        scheduled_runnable_per_host: int,
        scheduled_runnable_per_branch: int,
        scheduled_surface_delay_limit: int,
        scheduled_surface_delay_seconds: float,
    ):
        self._cycle_pages = cycle_pages
        self._min_runnable_supply_count = min_runnable_supply_count
        self._min_runnable_supply_hosts = min_runnable_supply_hosts
        self._blocked_retry_budget = blocked_retry_budget
        self._blocked_retry_per_host = blocked_retry_per_host
        self._blocked_retry_max_consecutive_failures = blocked_retry_max_consecutive_failures
        self._quarantine_retire_min_consecutive_failures = (
            quarantine_retire_min_consecutive_failures
        )
        self._quarantine_retire_after_seconds = quarantine_retire_after_seconds
        self._scheduled_runnable_per_host = scheduled_runnable_per_host
        self._scheduled_runnable_per_branch = scheduled_runnable_per_branch
        self._scheduled_surface_delay_limit = scheduled_surface_delay_limit
        self._scheduled_surface_delay_seconds = scheduled_surface_delay_seconds

    def prepare_scheduler(
        self, scheduler, *, refresh_stale: Callable[[], None] | None = None
    ) -> dict[str, int]:
        """Apply queue maintenance before deciding whether a crawl cycle can run."""
        metrics = {
            "admitted": 0,
            "rebalanced_before": 0,
            "scheduled": 0,
            "rebalanced_after": 0,
            "restored": 0,
            "retired": 0,
            "promoted": 0,
        }

        metrics["admitted"] = self.admit_discovered(scheduler)
        metrics["rebalanced_before"] = self._rebalance_blocked(scheduler)
        self.ensure_runnable_supply(scheduler)
        if refresh_stale is not None:
            refresh_stale()
        metrics["scheduled"] = scheduler.delay_overcrowded_scheduled_surface(
            keep_runnable_per_host=self._scheduled_runnable_per_host,
            keep_runnable_per_branch=self._scheduled_runnable_per_branch,
            limit=self._scheduled_surface_delay_limit,
            delay_seconds=self._scheduled_surface_delay_seconds,
        )
        metrics["rebalanced_after"] = self._rebalance_blocked(scheduler)
        metrics["restored"] = self.restore_recovered_blocked_retry(scheduler)
        metrics["retired"] = self.retire_blocked_retry(scheduler)
        metrics["promoted"] = self.promote_blocked_retry(scheduler)
        return metrics

    def prime_scheduler(self, scheduler) -> dict[str, int]:
        """Apply lightweight maintenance when a fresh DB connection is established."""
        admitted = self.admit_discovered(scheduler)
        rebalanced = self._rebalance_blocked(scheduler)
        scheduled = scheduler.delay_overcrowded_scheduled_surface(
            keep_runnable_per_host=self._scheduled_runnable_per_host,
            keep_runnable_per_branch=self._scheduled_runnable_per_branch,
            limit=self._scheduled_surface_delay_limit,
            delay_seconds=self._scheduled_surface_delay_seconds,
        )
        promoted = self.promote_blocked_retry(scheduler)
        return {
            "admitted": admitted,
            "rebalanced": rebalanced,
            "scheduled": scheduled,
            "promoted": promoted,
        }

    def admit_discovered(self, scheduler) -> int:
        """Admit discovered ledger rows only when the pending scheduler surface is thin."""
        if not hasattr(scheduler, "admit_discovered_urls"):
            return 0
        pending = scheduler.pending_count()
        deficit = max(0, self._cycle_pages - pending)
        if deficit <= 0:
            return 0
        return scheduler.admit_discovered_urls(
            deficit,
            runnable_surface=SCHEDULER_SURFACE_SCHEDULED,
            intent="explore",
        )

    def ensure_runnable_supply(self, scheduler) -> None:
        """Keep runnable host diversity supplied from existing scheduler state only."""
        runnable_pending = scheduler.pending_count(runnable_surface=SCHEDULER_SURFACE_RUNNABLE)
        runnable_hosts = self._runnable_supply_hosts(scheduler)
        if runnable_hosts >= self._min_runnable_supply_hosts:
            return

        needed_hosts = self._min_runnable_supply_hosts - runnable_hosts
        if hasattr(scheduler, "promote_scheduled_host_heads"):
            scheduler.promote_scheduled_host_heads(
                max(
                    runnable_pending + max(needed_hosts, 0),
                    self._min_runnable_supply_hosts,
                ),
                per_host=1,
            )

    def promote_blocked_retry(self, scheduler) -> int:
        """Restore a small cooled-down subset from blocked retry queue when runnable work is thin."""
        if not hasattr(scheduler, "promote_blocked_host_backoff"):
            return 0
        if self._blocked_retry_budget <= 0:
            return 0
        runnable_count = self._runnable_supply_count(scheduler)
        runnable_deficit = max(0, self._min_runnable_supply_count - runnable_count)
        runnable_hosts = self._runnable_supply_hosts(scheduler)
        host_deficit = max(0, self._min_runnable_supply_hosts - runnable_hosts)
        if runnable_deficit <= 0 and host_deficit <= 0:
            return 0

        retry_quarantine = self._retry_quarantine_count(scheduler)
        if retry_quarantine is not None and retry_quarantine <= 0:
            return 0

        deficit = max(1, runnable_deficit, host_deficit)
        limit = max(self._blocked_retry_budget, deficit)
        if retry_quarantine is not None:
            limit = min(limit, retry_quarantine)
        per_host = self._blocked_retry_per_host if runnable_count > 0 else limit
        return scheduler.promote_blocked_host_backoff(
            limit,
            per_host=per_host,
            max_consecutive_failures=self._blocked_retry_max_consecutive_failures,
        )

    def retire_blocked_retry(self, scheduler) -> int:
        """Retire long-stuck blocked retry URLs out of pending scheduler state."""
        if not hasattr(scheduler, "retire_blocked_host_backoff"):
            return 0
        retry_quarantine = self._retry_quarantine_count(scheduler)
        if retry_quarantine is not None and retry_quarantine <= 0:
            return 0
        return scheduler.retire_blocked_host_backoff(
            min_consecutive_failures=self._quarantine_retire_min_consecutive_failures,
            min_quarantine_seconds=self._quarantine_retire_after_seconds,
        )

    def restore_recovered_blocked_retry(self, scheduler) -> int:
        """Restore healthy blocked retry hosts before using bounded retry promotion."""
        if not hasattr(scheduler, "restore_recovered_blocked_host_backoff"):
            return 0
        retry_quarantine = self._retry_quarantine_count(scheduler)
        if retry_quarantine is not None and retry_quarantine <= 0:
            return 0
        return scheduler.restore_recovered_blocked_host_backoff(
            limit=max(self._cycle_pages, self._min_runnable_supply_count),
            per_host=max(self._blocked_retry_budget, self._min_runnable_supply_count),
        )

    def _retry_quarantine_count(self, scheduler) -> int | None:
        if hasattr(scheduler, "blocked_host_backoff_count"):
            return int(scheduler.blocked_host_backoff_count() or 0)
        if hasattr(scheduler, "blocked_reason_counts"):
            blocked_reason_counts = scheduler.blocked_reason_counts()
            return int(blocked_reason_counts.get("retry_quarantine", 0) or 0)
        return None

    def _rebalance_blocked(self, scheduler) -> int:
        if not hasattr(scheduler, "rebalance_blocked_host_backoff"):
            return 0
        quarantined, _restored = scheduler.rebalance_blocked_host_backoff()
        return quarantined

    def _runnable_supply_hosts(self, scheduler) -> int:
        if hasattr(scheduler, "runnable_host_count"):
            return scheduler.runnable_host_count(runnable_surface=SCHEDULER_SURFACE_RUNNABLE)
        if hasattr(scheduler, "ready_host_count"):
            return scheduler.ready_host_count(runnable_surface=SCHEDULER_SURFACE_RUNNABLE)
        if hasattr(scheduler, "pending_host_count"):
            return scheduler.pending_host_count(runnable_surface=SCHEDULER_SURFACE_RUNNABLE)
        if hasattr(scheduler, "runnable_count"):
            return scheduler.runnable_count(runnable_surface=SCHEDULER_SURFACE_RUNNABLE)
        readiness = scheduler.readiness() if hasattr(scheduler, "readiness") else None
        return int(getattr(readiness, "runnable_hosts", 0) or 0)

    def _runnable_supply_count(self, scheduler) -> int:
        if hasattr(scheduler, "pending_count"):
            return scheduler.pending_count(runnable_surface=SCHEDULER_SURFACE_RUNNABLE)
        if hasattr(scheduler, "runnable_count"):
            return scheduler.runnable_count(runnable_surface=SCHEDULER_SURFACE_RUNNABLE)
        readiness = scheduler.readiness() if hasattr(scheduler, "readiness") else None
        return int(getattr(readiness, "runnable", 0) or 0)
