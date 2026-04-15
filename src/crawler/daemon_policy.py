"""Pre-cycle scheduler policy for the crawl daemon."""

from __future__ import annotations

from collections.abc import Callable


class DaemonSchedulerPolicy:
    """Thin policy layer for frontier maintenance between crawl cycles."""

    def __init__(
        self,
        *,
        seeds: list[str],
        seed_hosts: list[str],
        cycle_pages: int,
        min_exploration_ready: int,
        min_exploration_hosts: int,
        blocked_retry_budget: int,
        blocked_retry_per_domain: int,
        blocked_retry_max_consecutive_failures: int,
        quarantine_retire_min_consecutive_failures: int,
        quarantine_retire_after_seconds: float,
        backlog_ready_per_domain: int,
        backlog_ready_per_branch: int,
        backlog_low_priority: float,
        backlog_defer_seconds: float,
    ):
        self._seeds = seeds
        self._seed_hosts = seed_hosts
        self._cycle_pages = cycle_pages
        self._min_exploration_ready = min_exploration_ready
        self._min_exploration_hosts = min_exploration_hosts
        self._blocked_retry_budget = blocked_retry_budget
        self._blocked_retry_per_domain = blocked_retry_per_domain
        self._blocked_retry_max_consecutive_failures = blocked_retry_max_consecutive_failures
        self._quarantine_retire_min_consecutive_failures = quarantine_retire_min_consecutive_failures
        self._quarantine_retire_after_seconds = quarantine_retire_after_seconds
        self._backlog_ready_per_domain = backlog_ready_per_domain
        self._backlog_ready_per_branch = backlog_ready_per_branch
        self._backlog_low_priority = backlog_low_priority
        self._backlog_defer_seconds = backlog_defer_seconds

    def prepare_frontier(self, frontier, *, recrawl_stale: Callable[[], None] | None = None) -> dict[str, int]:
        """Apply queue maintenance before deciding whether a crawl cycle can run."""
        metrics = {
            "rebalanced_before": 0,
            "deferred": 0,
            "rebalanced_after": 0,
            "restored": 0,
            "retired": 0,
            "promoted": 0,
        }

        metrics["rebalanced_before"] = self._rebalance_blocked(frontier)
        self.ensure_seeds(frontier)
        if recrawl_stale is not None:
            recrawl_stale()
        metrics["deferred"] = frontier.defer_overcrowded_backlog(
            keep_ready_per_domain=self._backlog_ready_per_domain,
            keep_ready_per_branch=self._backlog_ready_per_branch,
            low_priority_threshold=self._backlog_low_priority,
            defer_seconds=self._backlog_defer_seconds,
        )
        metrics["rebalanced_after"] = self._rebalance_blocked(frontier)
        metrics["restored"] = self.restore_recovered_blocked_retry(frontier)
        metrics["retired"] = self.retire_blocked_retry(frontier)
        metrics["promoted"] = self.promote_blocked_retry(frontier)
        return metrics

    def prime_frontier(self, frontier) -> dict[str, int]:
        """Apply lightweight maintenance when a fresh DB connection is established."""
        rebalanced = self._rebalance_blocked(frontier)
        deferred = frontier.defer_overcrowded_backlog(
            keep_ready_per_domain=self._backlog_ready_per_domain,
            keep_ready_per_branch=self._backlog_ready_per_branch,
            low_priority_threshold=self._backlog_low_priority,
            defer_seconds=self._backlog_defer_seconds,
        )
        promoted = self.promote_blocked_retry(frontier)
        return {
            "rebalanced": rebalanced,
            "deferred": deferred,
            "promoted": promoted,
        }

    def ensure_seeds(self, frontier) -> None:
        """Bootstrap seeds, then top up exploration from novel backlog branches."""
        if frontier.pending_count() == 0:
            frontier.upsert_seeds(self._seeds, priority=2.0)
            return

        exploration_ready = frontier.ready_count(queue_classes=["exploration"])
        exploration_pending = frontier.pending_count(queue_classes=["exploration"])
        exploration_hosts = self._exploration_pending_hosts(frontier, exploration_pending)
        if (
            exploration_ready >= self._min_exploration_ready
            and exploration_hosts >= self._min_exploration_hosts
        ):
            return

        needed = self._min_exploration_ready - exploration_ready
        needed_hosts = self._min_exploration_hosts - exploration_hosts
        host_promoted = 0
        if hasattr(frontier, "promote_backlog_host_heads"):
            host_promoted = frontier.promote_backlog_host_heads(
                max(
                    exploration_pending + max(needed, 0) + max(needed_hosts, 0),
                    self._min_exploration_ready,
                    self._min_exploration_hosts,
                ),
                per_domain=1,
            )

        if host_promoted < needed and hasattr(frontier, "promote_seed_host_exploration") and self._seed_hosts:
            frontier.promote_seed_host_exploration(self._seed_hosts, per_host=1, max_depth=2)

    def promote_blocked_retry(self, frontier) -> int:
        """Restore a small cooled-down subset from blocked retry queue when ready work is thin."""
        if not hasattr(frontier, "promote_blocked_domain_backoff"):
            return 0
        if self._blocked_retry_budget <= 0:
            return 0
        ready_count = frontier.ready_count()
        ready_deficit = max(0, self._min_exploration_ready - ready_count)
        exploration_hosts = self._exploration_pending_hosts(frontier)
        host_deficit = max(0, self._min_exploration_hosts - exploration_hosts)
        if ready_deficit <= 0 and host_deficit <= 0:
            return 0
        deficit = max(1, ready_deficit, host_deficit)
        limit = max(self._blocked_retry_budget, deficit)
        per_domain = self._blocked_retry_per_domain if ready_count > 0 else limit
        return frontier.promote_blocked_domain_backoff(
            limit,
            per_domain=per_domain,
            max_consecutive_failures=self._blocked_retry_max_consecutive_failures,
        )

    def retire_blocked_retry(self, frontier) -> int:
        """Retire long-stuck blocked retry URLs out of pending scheduler state."""
        if not hasattr(frontier, "retire_blocked_domain_backoff"):
            return 0
        return frontier.retire_blocked_domain_backoff(
            min_consecutive_failures=self._quarantine_retire_min_consecutive_failures,
            min_quarantine_seconds=self._quarantine_retire_after_seconds,
        )

    def restore_recovered_blocked_retry(self, frontier) -> int:
        """Restore healthy blocked retry domains before using bounded retry promotion."""
        if not hasattr(frontier, "restore_recovered_blocked_domain_backoff"):
            return 0
        return frontier.restore_recovered_blocked_domain_backoff(
            limit=max(self._cycle_pages, self._min_exploration_ready),
            per_domain=max(self._blocked_retry_budget, self._min_exploration_ready),
        )

    def _rebalance_blocked(self, frontier) -> int:
        if not hasattr(frontier, "rebalance_blocked_domain_backoff"):
            return 0
        quarantined, _restored = frontier.rebalance_blocked_domain_backoff()
        return quarantined

    def _exploration_pending_hosts(self, frontier, pending_count: int | None = None) -> int:
        if pending_count is None:
            pending_count = frontier.pending_count(queue_classes=["exploration"])
        if hasattr(frontier, "pending_domain_count"):
            return frontier.pending_domain_count(queue_classes=["exploration"])
        return pending_count
