"""Read-only scheduler invariant checks."""

from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any

from .url_ledger import (
    BLOCKED_HOST_BACKOFF_TABLE,
    HOST_RUNNABLE_HEADS_TABLE,
    LEASE_TABLE,
    PHYSICAL_QUEUE_TABLES,
    URL_LEDGER_TABLE,
)


@dataclass(frozen=True, slots=True)
class SchedulerInvariantReport:
    """Summary of scheduler-state invariant violations."""

    checked_at: float
    duplicate_memberships: int = 0
    terminal_in_live_queue: int = 0
    expired_leases: int = 0
    orphan_host_heads: int = 0
    host_head_mismatches: int = 0
    samples: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    @property
    def violations_total(self) -> int:
        """Return the total violation count across checked invariant classes."""
        return (
            self.duplicate_memberships
            + self.terminal_in_live_queue
            + self.expired_leases
            + self.orphan_host_heads
            + self.host_head_mismatches
        )

    @property
    def ok(self) -> bool:
        """Return True when no checked invariant violations were found."""
        return self.violations_total == 0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable report."""
        return {
            "ok": self.ok,
            "checked_at": self.checked_at,
            "violations_total": self.violations_total,
            "duplicate_memberships": self.duplicate_memberships,
            "terminal_in_live_queue": self.terminal_in_live_queue,
            "expired_leases": self.expired_leases,
            "orphan_host_heads": self.orphan_host_heads,
            "host_head_mismatches": self.host_head_mismatches,
            "samples": self.samples,
        }


class SchedulerInvariantChecker:
    """Check scheduler invariants without mutating scheduler state."""

    def __init__(self, conn):
        self._conn = conn

    def check(
        self,
        *,
        now: float | None = None,
        sample_limit: int = 5,
    ) -> SchedulerInvariantReport:
        """Run read-only invariant checks and return a compact report."""
        checked_at = time.time() if now is None else now
        limit = max(0, int(sample_limit))
        duplicate_count, duplicate_samples = self._duplicate_memberships(limit=limit)
        terminal_count, terminal_samples = self._terminal_in_live_queue(limit=limit)
        expired_count, expired_samples = self._expired_leases(now=checked_at, limit=limit)
        orphan_count, orphan_samples = self._orphan_host_heads(limit=limit)
        mismatch_count, mismatch_samples = self._host_head_mismatches(limit=limit)
        samples = {
            "duplicate_memberships": duplicate_samples,
            "terminal_in_live_queue": terminal_samples,
            "expired_leases": expired_samples,
            "orphan_host_heads": orphan_samples,
            "host_head_mismatches": mismatch_samples,
        }
        return SchedulerInvariantReport(
            checked_at=checked_at,
            duplicate_memberships=duplicate_count,
            terminal_in_live_queue=terminal_count,
            expired_leases=expired_count,
            orphan_host_heads=orphan_count,
            host_head_mismatches=mismatch_count,
            samples=samples,
        )

    def _live_membership_union_sql(self) -> str:
        queue_selects = [
            f"SELECT url, %s AS membership FROM {table_name}"
            for table_name in PHYSICAL_QUEUE_TABLES.values()
        ]
        queue_selects.extend(
            [
                f"SELECT url, physical_queue || ':blocked' AS membership FROM {BLOCKED_HOST_BACKOFF_TABLE}",
                f"SELECT url, physical_queue || ':leased' AS membership FROM {LEASE_TABLE}",
            ]
        )
        return "\nUNION ALL\n".join(queue_selects)

    def _live_membership_params(self) -> tuple[str, ...]:
        return tuple(PHYSICAL_QUEUE_TABLES.keys())

    def _duplicate_memberships(self, *, limit: int) -> tuple[int, list[dict[str, Any]]]:
        union_sql = self._live_membership_union_sql()
        params = self._live_membership_params()
        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH memberships AS (
                        {union_sql}
                    ), duplicate_urls AS (
                        SELECT url, COUNT(*) AS membership_count
                        FROM memberships
                        GROUP BY url
                        HAVING COUNT(*) > 1
                    )
                    SELECT COUNT(*) FROM duplicate_urls""",
                params,
            )
            count = int(cur.fetchone()[0] or 0)
            samples: list[dict[str, Any]] = []
            if limit:
                cur.execute(
                    f"""WITH memberships AS (
                            {union_sql}
                        )
                        SELECT url, array_agg(membership ORDER BY membership) AS memberships
                        FROM memberships
                        GROUP BY url
                        HAVING COUNT(*) > 1
                        ORDER BY url
                        LIMIT %s""",
                    (*params, limit),
                )
                samples = [
                    {"url": url, "memberships": list(memberships)}
                    for url, memberships in cur.fetchall()
                ]
        self._conn.commit()
        return count, samples

    def _terminal_in_live_queue(self, *, limit: int) -> tuple[int, list[dict[str, Any]]]:
        union_sql = self._live_membership_union_sql()
        params = self._live_membership_params()
        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH memberships AS (
                        {union_sql}
                    )
                    SELECT COUNT(*)
                    FROM memberships
                    JOIN {URL_LEDGER_TABLE} AS ledger ON ledger.url = memberships.url
                    WHERE ledger.terminal_reason IS NOT NULL""",
                params,
            )
            count = int(cur.fetchone()[0] or 0)
            samples: list[dict[str, Any]] = []
            if limit:
                cur.execute(
                    f"""WITH memberships AS (
                            {union_sql}
                        )
                        SELECT memberships.url, memberships.membership, ledger.terminal_reason
                        FROM memberships
                        JOIN {URL_LEDGER_TABLE} AS ledger ON ledger.url = memberships.url
                        WHERE ledger.terminal_reason IS NOT NULL
                        ORDER BY memberships.url, memberships.membership
                        LIMIT %s""",
                    (*params, limit),
                )
                samples = [
                    {
                        "url": url,
                        "membership": membership,
                        "terminal_reason": terminal_reason,
                    }
                    for url, membership, terminal_reason in cur.fetchall()
                ]
        self._conn.commit()
        return count, samples

    def _expired_leases(
        self,
        *,
        now: float,
        limit: int,
    ) -> tuple[int, list[dict[str, Any]]]:
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {LEASE_TABLE} WHERE lease_expires_at < %s",
                (now,),
            )
            count = int(cur.fetchone()[0] or 0)
            samples: list[dict[str, Any]] = []
            if limit:
                cur.execute(
                    f"""SELECT url, physical_queue, lease_expires_at
                        FROM {LEASE_TABLE}
                        WHERE lease_expires_at < %s
                        ORDER BY lease_expires_at ASC, url ASC
                        LIMIT %s""",
                    (now, limit),
                )
                samples = [
                    {
                        "url": url,
                        "physical_queue": physical_queue,
                        "lease_expires_at": lease_expires_at,
                    }
                    for url, physical_queue, lease_expires_at in cur.fetchall()
                ]
        self._conn.commit()
        return count, samples

    def _host_head_queue_join_sql(self) -> str:
        joins = [
            f"""SELECT heads.physical_queue,
                      heads.host,
                      heads.head_url,
                      queue.url AS queue_url,
                      queue.host AS queue_host
               FROM {HOST_RUNNABLE_HEADS_TABLE} AS heads
               LEFT JOIN {table_name} AS queue
                 ON queue.url = heads.head_url
                AND heads.physical_queue = %s
               WHERE heads.physical_queue = %s"""
            for table_name in PHYSICAL_QUEUE_TABLES.values()
        ]
        return "\nUNION ALL\n".join(joins)

    def _host_head_queue_join_params(self) -> tuple[str, ...]:
        params: list[str] = []
        for physical_queue in PHYSICAL_QUEUE_TABLES:
            params.extend([physical_queue, physical_queue])
        return tuple(params)

    def _orphan_host_heads(self, *, limit: int) -> tuple[int, list[dict[str, Any]]]:
        join_sql = self._host_head_queue_join_sql()
        params = self._host_head_queue_join_params()
        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH head_targets AS (
                        {join_sql}
                    )
                    SELECT COUNT(*) FROM head_targets WHERE queue_url IS NULL""",
                params,
            )
            count = int(cur.fetchone()[0] or 0)
            samples: list[dict[str, Any]] = []
            if limit:
                cur.execute(
                    f"""WITH head_targets AS (
                            {join_sql}
                        )
                        SELECT physical_queue, host, head_url
                        FROM head_targets
                        WHERE queue_url IS NULL
                        ORDER BY physical_queue, host
                        LIMIT %s""",
                    (*params, limit),
                )
                samples = [
                    {"physical_queue": physical_queue, "host": host, "head_url": head_url}
                    for physical_queue, host, head_url in cur.fetchall()
                ]
        self._conn.commit()
        return count, samples

    def _host_head_mismatches(self, *, limit: int) -> tuple[int, list[dict[str, Any]]]:
        join_sql = self._host_head_queue_join_sql()
        params = self._host_head_queue_join_params()
        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH head_targets AS (
                        {join_sql}
                    )
                    SELECT COUNT(*)
                    FROM head_targets
                    WHERE queue_url IS NOT NULL
                      AND queue_host <> host""",
                params,
            )
            count = int(cur.fetchone()[0] or 0)
            samples: list[dict[str, Any]] = []
            if limit:
                cur.execute(
                    f"""WITH head_targets AS (
                            {join_sql}
                        )
                        SELECT physical_queue, host, head_url, queue_host
                        FROM head_targets
                        WHERE queue_url IS NOT NULL
                          AND queue_host <> host
                        ORDER BY physical_queue, host
                        LIMIT %s""",
                    (*params, limit),
                )
                samples = [
                    {
                        "physical_queue": physical_queue,
                        "host": host,
                        "head_url": head_url,
                        "queue_host": queue_host,
                    }
                    for physical_queue, host, head_url, queue_host in cur.fetchall()
                ]
        self._conn.commit()
        return count, samples
