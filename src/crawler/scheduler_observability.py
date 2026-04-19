"""Read-only scheduler observability helpers."""

from __future__ import annotations

from dataclasses import dataclass
import time


@dataclass(frozen=True)
class SchedulerReadiness:
    """Summary of the current pending queue readiness."""

    pending: int
    runnable: int
    runnable_hosts: int
    next_runnable_delay: float | None
    blocked: dict[str, int]
    state_counts: dict[str, int]

    @property
    def scheduled(self) -> int:
        """Return scheduled-but-not-yet-runnable work count."""
        return int(self.state_counts.get("scheduled", 0))


class SchedulerObservability:
    """Read-only queue and lease snapshots for scheduler-facing metrics."""

    def __init__(
        self,
        conn,
        *,
        physical_queue_tables: dict[str, str],
        physical_queue_order: tuple[str, ...],
        physical_queue_default_runnable_surface: dict[str, str],
        blocked_queue_table: str,
        lease_table: str,
    ):
        self._conn = conn
        self._physical_queue_tables = physical_queue_tables
        self._physical_queue_order = physical_queue_order
        self._physical_queue_default_runnable_surface = physical_queue_default_runnable_surface
        self._blocked_queue_table = blocked_queue_table
        self._lease_table = lease_table

    def _normalized_physical_queues(self, runnable_surface: str | None = None) -> list[str]:
        if runnable_surface is not None:
            return [
                physical_queue
                for physical_queue in self._physical_queue_order
                if self._physical_queue_default_runnable_surface[physical_queue] == runnable_surface
            ]
        return list(self._physical_queue_order)

    def _pending_queue_union_sql(self, runnable_surface: str | None = None) -> str:
        normalized_physical_queues = self._normalized_physical_queues(runnable_surface)
        selects = [
            f"SELECT url, host, branch_key, next_fetch_at FROM {self._physical_queue_tables[physical_queue]}"
            for physical_queue in normalized_physical_queues
        ]
        return "\nUNION ALL\n".join(selects)

    def _blocked_queue_sql(
        self, runnable_surface: str | None = None
    ) -> tuple[str, tuple[object, ...]]:
        sql = f"SELECT url, host, branch_key, next_fetch_at FROM {self._blocked_queue_table}"
        physical_queues = self._normalized_physical_queues(runnable_surface)
        if runnable_surface is not None:
            sql += " WHERE physical_queue = ANY(%s)"
            return sql, (physical_queues,)
        return sql, ()

    def pending_queue_counts(self) -> dict[str, int]:
        union_sql = "\n                       UNION ALL\n                       ".join(
            f"SELECT %s AS physical_queue, url FROM {self._physical_queue_tables[physical_queue]}"
            for physical_queue in self._physical_queue_order
        )
        with self._conn.cursor() as cur:
            cur.execute(
                f"""SELECT physical_queue, count(*)
                   FROM (
                       {union_sql}
                   ) AS pending_queues
                   GROUP BY physical_queue""",
                self._physical_queue_order,
            )
            return dict(cur.fetchall())

    def blocked_queue_counts(self) -> dict[str, int]:
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT physical_queue, COUNT(*) FROM {self._blocked_queue_table} GROUP BY physical_queue"
            )
            return dict(cur.fetchall())

    def intent_counts(self) -> dict[str, int]:
        counts = {"explore": 0, "refresh": 0, "retry": 0}
        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH active_urls AS (
                        SELECT url FROM {self._lease_table}
                        UNION
                        SELECT url FROM {self._blocked_queue_table}
                        UNION
                        SELECT url FROM ({self._pending_queue_union_sql()}) AS pending_entries
                    )
                    SELECT ledger.current_intent, COUNT(*)
                    FROM active_urls
                    JOIN url_ledger AS ledger ON ledger.url = active_urls.url
                    WHERE ledger.current_intent IS NOT NULL
                    GROUP BY ledger.current_intent"""
            )
            for current_intent, count in cur.fetchall():
                if current_intent in counts:
                    counts[current_intent] = int(count)
        return counts

    def durable_state_counts(self) -> dict[str, int]:
        counts = {
            "discovered": 0,
            "scheduled": 0,
            "leased": 0,
            "blocked": 0,
            "terminal": 0,
        }
        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH pending_urls AS (
                        SELECT url FROM ({self._pending_queue_union_sql()}) AS pending_entries
                    ), blocked_urls AS (
                        SELECT url FROM {self._blocked_queue_table}
                    ), leased_urls AS (
                        SELECT url FROM {self._lease_table}
                    )
                    SELECT
                        COUNT(*) FILTER (
                            WHERE ledger.last_success_at IS NULL
                              AND ledger.terminal_reason IS NULL
                              AND pending_urls.url IS NULL
                              AND blocked_urls.url IS NULL
                              AND leased_urls.url IS NULL
                        ) AS discovered_count,
                        COUNT(*) FILTER (WHERE pending_urls.url IS NOT NULL) AS scheduled_count,
                        COUNT(*) FILTER (WHERE blocked_urls.url IS NOT NULL) AS blocked_count,
                        COUNT(*) FILTER (WHERE leased_urls.url IS NOT NULL) AS leased_count,
                        COUNT(*) FILTER (
                            WHERE ledger.last_success_at IS NOT NULL
                               OR ledger.terminal_reason IS NOT NULL
                        ) AS terminal_count
                    FROM url_ledger AS ledger
                    LEFT JOIN pending_urls ON pending_urls.url = ledger.url
                    LEFT JOIN blocked_urls ON blocked_urls.url = ledger.url
                    LEFT JOIN leased_urls ON leased_urls.url = ledger.url"""
            )
            row = cur.fetchone()
        if row is None:
            return counts
        (
            counts["discovered"],
            counts["scheduled"],
            counts["blocked"],
            counts["leased"],
            counts["terminal"],
        ) = (int(value or 0) for value in row)
        return counts

    def _effective_state_counts_from(
        self,
        *,
        durable_state_counts: dict[str, int],
        readiness: SchedulerReadiness,
    ) -> dict[str, int]:
        state_counts = readiness.state_counts
        return {
            "discovered": int(durable_state_counts.get("discovered", 0) or 0),
            "scheduled": int(state_counts.get("scheduled", 0) or 0),
            "runnable": int(state_counts.get("runnable", 0) or 0),
            "blocked": int(state_counts.get("blocked_host_next_request", 0) or 0)
            + int(state_counts.get("blocked_host_backoff", 0) or 0)
            + int(state_counts.get("retry_quarantine", 0) or 0),
            "leased": int(durable_state_counts.get("leased", 0) or 0),
            "terminal": int(durable_state_counts.get("terminal", 0) or 0),
        }

    def _scheduler_state_snapshot_from(
        self,
        *,
        durable_state_counts: dict[str, int],
        readiness: SchedulerReadiness,
    ) -> dict[str, dict[str, int]]:
        readiness_state_counts = dict(readiness.state_counts)
        blocked_reason_counts = dict(readiness.blocked)
        return {
            "durable_state_counts": dict(durable_state_counts),
            "readiness_state_counts": readiness_state_counts,
            "effective_state_counts": self._effective_state_counts_from(
                durable_state_counts=durable_state_counts,
                readiness=readiness,
            ),
            "blocked_reason_counts": blocked_reason_counts,
        }

    def scheduler_state_snapshot(
        self,
        *,
        now: float | None = None,
        runnable_surface: str | None = None,
    ) -> dict[str, dict[str, int]]:
        """Return a bundled runtime-facing scheduler state snapshot."""
        readiness = self.readiness(now=now, runnable_surface=runnable_surface)
        durable_state_counts = self.durable_state_counts()
        return self._scheduler_state_snapshot_from(
            durable_state_counts=durable_state_counts,
            readiness=readiness,
        )

    def effective_state_counts(
        self,
        *,
        now: float | None = None,
        runnable_surface: str | None = None,
    ) -> dict[str, int]:
        """Return a single effective scheduler-state view for runtime-facing APIs."""
        return dict(
            self.scheduler_state_snapshot(
                now=now,
                runnable_surface=runnable_surface,
            )["effective_state_counts"]
        )

    def blocked_reason_counts(
        self,
        *,
        now: float | None = None,
        runnable_surface: str | None = None,
    ) -> dict[str, int]:
        """Return the current blocked breakdown by scheduler reason."""
        return dict(
            self.scheduler_state_snapshot(
                now=now,
                runnable_surface=runnable_surface,
            )["blocked_reason_counts"]
        )

    def readiness_state_counts(
        self,
        *,
        now: float | None = None,
        runnable_surface: str | None = None,
    ) -> dict[str, int]:
        """Return the current readiness-derived scheduler state breakdown."""
        return dict(
            self.scheduler_state_snapshot(
                now=now,
                runnable_surface=runnable_surface,
            )["readiness_state_counts"]
        )

    def _surface_counts(self, queue_counts: dict[str, int]) -> dict[str, int]:
        surface_counts: dict[str, int] = {}
        for physical_queue, count in queue_counts.items():
            surface = self._physical_queue_default_runnable_surface[physical_queue]
            surface_counts[surface] = surface_counts.get(surface, 0) + count
        return surface_counts

    def status_counts(self) -> dict[str, int | dict[str, int]]:
        pending_queue_counts = self.pending_queue_counts()
        blocked_queue_counts = self.blocked_queue_counts()
        pending_surfaces = self._surface_counts(pending_queue_counts)
        blocked_surfaces = self._surface_counts(blocked_queue_counts)
        intent_counts = self.intent_counts()
        state_snapshot = self.scheduler_state_snapshot()

        with self._conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {self._lease_table}")
            leased = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM url_ledger WHERE last_success_at IS NOT NULL")
            done = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM url_ledger WHERE terminal_reason IS NOT NULL")
            failed = cur.fetchone()[0]

        pending = sum(pending_queue_counts.values()) + sum(blocked_queue_counts.values())
        return {
            "leased": leased,
            "done": done,
            "failed": failed,
            "intent_counts": intent_counts,
            "scheduler_state_snapshot": {key: dict(value) for key, value in state_snapshot.items()},
            **state_snapshot,
            "pending_surfaces": pending_surfaces,
            "blocked_surfaces": blocked_surfaces,
            "pending": pending,
            "total": done + failed + pending + leased,
        }

    def pending_count(self, *, runnable_surface: str | None = None) -> int:
        total = 0
        with self._conn.cursor() as cur:
            for physical_queue in self._normalized_physical_queues(runnable_surface):
                cur.execute(f"SELECT COUNT(*) FROM {self._physical_queue_tables[physical_queue]}")
                total += cur.fetchone()[0]
        return total

    def pending_host_count(self, *, runnable_surface: str | None = None) -> int:
        pending_queue_sql = self._pending_queue_union_sql(runnable_surface)
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(DISTINCT host) FROM ({pending_queue_sql}) AS pending_entries"
            )
            value = cur.fetchone()[0]
        return int(value or 0)

    def runnable_count(
        self,
        *,
        now: float | None = None,
        runnable_surface: str | None = None,
    ) -> int:
        return self.readiness(now=now, runnable_surface=runnable_surface).runnable

    def scheduled_count(
        self,
        *,
        now: float | None = None,
        runnable_surface: str | None = None,
    ) -> int:
        return self.readiness(now=now, runnable_surface=runnable_surface).scheduled

    def runnable_host_count(
        self,
        *,
        now: float | None = None,
        runnable_surface: str | None = None,
    ) -> int:
        return self.readiness(now=now, runnable_surface=runnable_surface).runnable_hosts

    def blocked_count(self) -> int:
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {self._blocked_queue_table}")
            return cur.fetchone()[0]

    def readiness(
        self,
        *,
        now: float | None = None,
        runnable_surface: str | None = None,
    ) -> SchedulerReadiness:
        now = time.time() if now is None else now
        pending_queue_sql = self._pending_queue_union_sql(runnable_surface)
        blocked_queue_sql, blocked_queue_params = self._blocked_queue_sql(runnable_surface)
        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH pending_entries AS (
                        {pending_queue_sql}
                    ), blocked_entries AS (
                        {blocked_queue_sql}
                    ), readiness_entries AS (
                        SELECT
                            queue_entry.url,
                            queue_entry.host,
                            queue_entry.branch_key,
                            queue_entry.next_fetch_at,
                            queue_entry.next_fetch_at > %s AS blocked_next_fetch,
                            COALESCE(host_state.next_request_at, 0) > %s AS blocked_host_next_request,
                            COALESCE(host_state.backoff_until, 0) > %s AS blocked_host_backoff,
                            FALSE AS retry_quarantine,
                            GREATEST(
                                queue_entry.next_fetch_at,
                                COALESCE(host_state.next_request_at, 0),
                                COALESCE(host_state.backoff_until, 0)
                            ) AS ready_at
                        FROM pending_entries AS queue_entry
                        LEFT JOIN host_state ON host_state.host_key = queue_entry.host
                        UNION ALL
                        SELECT
                            blocked_entry.url,
                            blocked_entry.host,
                            blocked_entry.branch_key,
                            blocked_entry.next_fetch_at,
                            FALSE AS blocked_next_fetch,
                            FALSE AS blocked_host_next_request,
                            FALSE AS blocked_host_backoff,
                            TRUE AS retry_quarantine,
                            NULL::DOUBLE PRECISION AS ready_at
                        FROM blocked_entries AS blocked_entry
                    )
                    SELECT
                        COUNT(*) AS pending,
                        COUNT(*) FILTER (
                            WHERE NOT blocked_next_fetch
                              AND NOT blocked_host_next_request
                              AND NOT blocked_host_backoff
                              AND NOT retry_quarantine
                        ) AS runnable,
                        COUNT(DISTINCT host) FILTER (
                            WHERE NOT blocked_next_fetch
                              AND NOT blocked_host_next_request
                              AND NOT blocked_host_backoff
                              AND NOT retry_quarantine
                        ) AS runnable_hosts,
                        MIN(ready_at) AS next_ready_at,
                        COUNT(*) FILTER (WHERE blocked_next_fetch) AS blocked_next_fetch,
                        COUNT(*) FILTER (WHERE blocked_host_next_request) AS blocked_host_next_request,
                        COUNT(*) FILTER (WHERE blocked_host_backoff) AS blocked_host_backoff,
                        COUNT(*) FILTER (WHERE retry_quarantine) AS retry_quarantine,
                        COUNT(*) FILTER (
                            WHERE NOT blocked_host_backoff
                              AND NOT retry_quarantine
                              AND blocked_host_next_request
                        ) AS state_blocked_host_next_request,
                        COUNT(*) FILTER (
                            WHERE NOT blocked_host_backoff
                              AND NOT retry_quarantine
                              AND NOT blocked_host_next_request
                              AND blocked_next_fetch
                        ) AS state_scheduled,
                        COUNT(*) FILTER (
                            WHERE NOT blocked_host_backoff
                              AND NOT retry_quarantine
                              AND NOT blocked_host_next_request
                              AND NOT blocked_next_fetch
                        ) AS state_runnable
                    FROM readiness_entries""",
                (*blocked_queue_params, now, now, now),
            )
            (
                pending,
                runnable,
                runnable_hosts,
                next_ready_at,
                blocked_next_fetch,
                blocked_host_next_request,
                blocked_host_backoff,
                retry_quarantine,
                state_blocked_host_next_request,
                state_scheduled,
                state_runnable,
            ) = cur.fetchone()
        return SchedulerReadiness(
            pending=pending or 0,
            runnable=runnable or 0,
            runnable_hosts=runnable_hosts or 0,
            next_runnable_delay=None if next_ready_at is None else max(0.0, next_ready_at - now),
            blocked={
                "next_fetch_at": blocked_next_fetch or 0,
                "host_next_request": blocked_host_next_request or 0,
                "host_backoff": blocked_host_backoff or 0,
                "retry_quarantine": retry_quarantine or 0,
            },
            state_counts={
                "runnable": state_runnable or 0,
                "scheduled": state_scheduled or 0,
                "blocked_host_next_request": state_blocked_host_next_request or 0,
                "blocked_host_backoff": blocked_host_backoff or 0,
                "retry_quarantine": retry_quarantine or 0,
            },
        )
