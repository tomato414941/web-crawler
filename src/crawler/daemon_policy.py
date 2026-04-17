"""Pre-cycle scheduler policy for the crawl daemon."""

from __future__ import annotations

from collections.abc import Callable

from .url_ledger import RUNNABLE_SURFACE_DEFERRED, RUNNABLE_SURFACE_FRONTLINE


class DaemonSchedulerPolicy:
    """Thin policy layer for scheduler maintenance between crawl cycles."""

    def __init__(
        self,
        *,
        cycle_pages: int,
        min_frontline_runnable: int,
        min_frontline_hosts: int,
        blocked_retry_budget: int,
        blocked_retry_per_domain: int,
        blocked_retry_max_consecutive_failures: int,
        quarantine_retire_min_consecutive_failures: int,
        quarantine_retire_after_seconds: float,
        deferred_runnable_per_domain: int,
        deferred_runnable_per_branch: int,
        deferred_surface_defer_seconds: float,
    ):
        self._cycle_pages = cycle_pages
        self._min_frontline_runnable = min_frontline_runnable
        self._min_frontline_hosts = min_frontline_hosts
        self._blocked_retry_budget = blocked_retry_budget
        self._blocked_retry_per_domain = blocked_retry_per_domain
        self._blocked_retry_max_consecutive_failures = blocked_retry_max_consecutive_failures
        self._quarantine_retire_min_consecutive_failures = quarantine_retire_min_consecutive_failures
        self._quarantine_retire_after_seconds = quarantine_retire_after_seconds
        self._deferred_runnable_per_domain = deferred_runnable_per_domain
        self._deferred_runnable_per_branch = deferred_runnable_per_branch
        self._deferred_surface_defer_seconds = deferred_surface_defer_seconds

    def prepare_scheduler(self, scheduler, *, refresh_stale: Callable[[], None] | None = None) -> dict[str, int]:
        """Apply queue maintenance before deciding whether a crawl cycle can run."""
        metrics = {
            "admitted": 0,
            "rebalanced_before": 0,
            "deferred": 0,
            "rebalanced_after": 0,
            "restored": 0,
            "retired": 0,
            "promoted": 0,
        }

        metrics["admitted"] = self.admit_discovered(scheduler)
        metrics["rebalanced_before"] = self._rebalance_blocked(scheduler)
        self.ensure_frontline_supply(scheduler)
        if refresh_stale is not None:
            refresh_stale()
        metrics["deferred"] = scheduler.defer_overcrowded_deferred_surface(
            keep_runnable_per_domain=self._deferred_runnable_per_domain,
            keep_runnable_per_branch=self._deferred_runnable_per_branch,
            defer_seconds=self._deferred_surface_defer_seconds,
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
        deferred = scheduler.defer_overcrowded_deferred_surface(
            keep_runnable_per_domain=self._deferred_runnable_per_domain,
            keep_runnable_per_branch=self._deferred_runnable_per_branch,
            defer_seconds=self._deferred_surface_defer_seconds,
        )
        promoted = self.promote_blocked_retry(scheduler)
        return {
            "admitted": admitted,
            "rebalanced": rebalanced,
            "deferred": deferred,
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
            runnable_surface=RUNNABLE_SURFACE_DEFERRED,
            intent="explore",
        )

    def ensure_frontline_supply(self, scheduler) -> None:
        """Keep frontline host diversity supplied from existing scheduler state only."""
        frontline_pending = scheduler.pending_count(runnable_surface=RUNNABLE_SURFACE_FRONTLINE)
        frontline_hosts = self._frontline_runnable_hosts(scheduler)
        if frontline_hosts >= self._min_frontline_hosts:
            return

        needed_hosts = self._min_frontline_hosts - frontline_hosts
        if hasattr(scheduler, "promote_deferred_host_heads"):
            scheduler.promote_deferred_host_heads(
                max(
                    frontline_pending + max(needed_hosts, 0),
                    self._min_frontline_hosts,
                ),
                per_domain=1,
            )

    def promote_blocked_retry(self, scheduler) -> int:
        """Restore a small cooled-down subset from blocked retry queue when runnable work is thin."""
        if not hasattr(scheduler, "promote_blocked_domain_backoff"):
            return 0
        if self._blocked_retry_budget <= 0:
            return 0
        runnable_count = scheduler.runnable_count() if hasattr(scheduler, "runnable_count") else scheduler.runnable_count()
        runnable_deficit = max(0, self._min_frontline_runnable - runnable_count)
        frontline_hosts = self._frontline_runnable_hosts(scheduler)
        host_deficit = max(0, self._min_frontline_hosts - frontline_hosts)
        if runnable_deficit <= 0 and host_deficit <= 0:
            return 0
        deficit = max(1, runnable_deficit, host_deficit)
        limit = max(self._blocked_retry_budget, deficit)
        per_domain = self._blocked_retry_per_domain if runnable_count > 0 else limit
        return scheduler.promote_blocked_domain_backoff(
            limit,
            per_domain=per_domain,
            max_consecutive_failures=self._blocked_retry_max_consecutive_failures,
        )

    def retire_blocked_retry(self, scheduler) -> int:
        """Retire long-stuck blocked retry URLs out of pending scheduler state."""
        if not hasattr(scheduler, "retire_blocked_domain_backoff"):
            return 0
        return scheduler.retire_blocked_domain_backoff(
            min_consecutive_failures=self._quarantine_retire_min_consecutive_failures,
            min_quarantine_seconds=self._quarantine_retire_after_seconds,
        )

    def restore_recovered_blocked_retry(self, scheduler) -> int:
        """Restore healthy blocked retry domains before using bounded retry promotion."""
        if not hasattr(scheduler, "restore_recovered_blocked_domain_backoff"):
            return 0
        return scheduler.restore_recovered_blocked_domain_backoff(
            limit=max(self._cycle_pages, self._min_frontline_runnable),
            per_domain=max(self._blocked_retry_budget, self._min_frontline_runnable),
        )

    def _rebalance_blocked(self, scheduler) -> int:
        if not hasattr(scheduler, "rebalance_blocked_domain_backoff"):
            return 0
        quarantined, _restored = scheduler.rebalance_blocked_domain_backoff()
        return quarantined

    def _frontline_runnable_hosts(self, scheduler) -> int:
        if hasattr(scheduler, "runnable_domain_count"):
            return scheduler.runnable_domain_count(runnable_surface=RUNNABLE_SURFACE_FRONTLINE)
        if hasattr(scheduler, "ready_domain_count"):
            return scheduler.runnable_domain_count(runnable_surface=RUNNABLE_SURFACE_FRONTLINE)
        if hasattr(scheduler, "pending_domain_count"):
            return scheduler.pending_domain_count(runnable_surface=RUNNABLE_SURFACE_FRONTLINE)
        return scheduler.runnable_count(runnable_surface=RUNNABLE_SURFACE_FRONTLINE)
