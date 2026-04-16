"""Read-only scheduler observability helpers."""

from __future__ import annotations

from dataclasses import dataclass
import time


@dataclass(frozen=True)
class FrontierReadiness:
    """Summary of the current pending queue readiness."""

    pending: int
    ready: int
    ready_domains: int
    next_ready_delay: float | None
    blocked: dict[str, int]
    state_counts: dict[str, int]


class FrontierObservability:
    """Read-only queue and lease snapshots for scheduler-facing metrics."""

    def __init__(
        self,
        conn,
        *,
        queue_table_by_class: dict[str, str],
        queue_class_order: tuple[str, ...],
        blocked_queue_table: str,
        lease_table: str,
    ):
        self._conn = conn
        self._queue_table_by_class = queue_table_by_class
        self._queue_class_order = queue_class_order
        self._blocked_queue_table = blocked_queue_table
        self._lease_table = lease_table

    def _normalized_queue_classes(self, queue_classes: list[str] | None) -> list[str]:
        if queue_classes:
            selected = set(queue_classes)
            return [queue_class for queue_class in self._queue_class_order if queue_class in selected]
        return list(self._queue_class_order)

    def _pending_queue_union_sql(self, queue_classes: list[str] | None = None) -> str:
        normalized_queue_classes = self._normalized_queue_classes(queue_classes)
        selects = [
            f"SELECT url, domain, branch_key, next_fetch_at FROM {self._queue_table_by_class[queue_class]}"
            for queue_class in normalized_queue_classes
        ]
        return "\nUNION ALL\n".join(selects)

    def _blocked_queue_sql(self, queue_classes: list[str] | None = None) -> tuple[str, tuple[object, ...]]:
        sql = f"SELECT url, domain, branch_key, next_fetch_at FROM {self._blocked_queue_table}"
        if queue_classes:
            sql += " WHERE queue_class = ANY(%s)"
            return sql, (self._normalized_queue_classes(queue_classes),)
        return sql, ()

    def pending_queue_counts(self) -> dict[str, int]:
        with self._conn.cursor() as cur:
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
                self._queue_class_order,
            )
            return dict(cur.fetchall())

    def blocked_queue_counts(self) -> dict[str, int]:
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT queue_class, COUNT(*) FROM {self._blocked_queue_table} GROUP BY queue_class"
            )
            return dict(cur.fetchall())

    def status_counts(self) -> dict[str, int | dict[str, int]]:
        pending_queue_tables = self.pending_queue_counts()
        blocked_queue_classes = self.blocked_queue_counts()

        with self._conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {self._lease_table}")
            leased = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM url_ledger WHERE last_success_at IS NOT NULL")
            done = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM url_ledger WHERE terminal_reason IS NOT NULL")
            failed = cur.fetchone()[0]

        pending = sum(pending_queue_tables.values()) + sum(blocked_queue_classes.values())
        return {
            "leased": leased,
            "done": done,
            "failed": failed,
            "pending_queue_tables": pending_queue_tables,
            "blocked_queue_classes": blocked_queue_classes,
            "pending": pending,
            "total": done + failed + pending + leased,
        }

    def pending_count(self, queue_classes: list[str] | None = None) -> int:
        total = 0
        with self._conn.cursor() as cur:
            for queue_class in self._normalized_queue_classes(queue_classes):
                cur.execute(f"SELECT COUNT(*) FROM {self._queue_table_by_class[queue_class]}")
                total += cur.fetchone()[0]
        return total

    def pending_domain_count(self, queue_classes: list[str] | None = None) -> int:
        pending_queue_sql = self._pending_queue_union_sql(queue_classes)
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(DISTINCT domain) FROM ({pending_queue_sql}) AS pending_entries"
            )
            value = cur.fetchone()[0]
        return int(value or 0)

    def ready_domain_count(
        self,
        *,
        now: float | None = None,
        queue_classes: list[str] | None = None,
    ) -> int:
        return self.readiness(now=now, queue_classes=queue_classes).ready_domains

    def blocked_count(self) -> int:
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {self._blocked_queue_table}")
            return cur.fetchone()[0]

    def readiness(
        self,
        *,
        now: float | None = None,
        queue_classes: list[str] | None = None,
    ) -> FrontierReadiness:
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
                            queue_entry.branch_key,
                            queue_entry.next_fetch_at,
                            queue_entry.next_fetch_at > %s AS blocked_next_fetch,
                            COALESCE(domain_state.next_request_at, 0) > %s AS blocked_domain_next_request,
                            COALESCE(domain_state.backoff_until, 0) > %s AS blocked_host_backoff,
                            FALSE AS retry_quarantine,
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
                            blocked_entry.branch_key,
                            blocked_entry.next_fetch_at,
                            FALSE AS blocked_next_fetch,
                            FALSE AS blocked_domain_next_request,
                            FALSE AS blocked_host_backoff,
                            TRUE AS retry_quarantine,
                            NULL::DOUBLE PRECISION AS ready_at
                        FROM blocked_entries AS blocked_entry
                    )
                    SELECT
                        COUNT(*) AS pending,
                        COUNT(*) FILTER (
                            WHERE NOT blocked_next_fetch
                              AND NOT blocked_domain_next_request
                              AND NOT blocked_host_backoff
                              AND NOT retry_quarantine
                        ) AS ready,
                        COUNT(DISTINCT domain) FILTER (
                            WHERE NOT blocked_next_fetch
                              AND NOT blocked_domain_next_request
                              AND NOT blocked_host_backoff
                              AND NOT retry_quarantine
                        ) AS ready_domains,
                        MIN(ready_at) AS next_ready_at,
                        COUNT(*) FILTER (WHERE blocked_next_fetch) AS blocked_next_fetch,
                        COUNT(*) FILTER (WHERE blocked_domain_next_request) AS blocked_domain_next_request,
                        COUNT(*) FILTER (WHERE blocked_host_backoff) AS blocked_host_backoff,
                        COUNT(*) FILTER (WHERE retry_quarantine) AS retry_quarantine,
                        COUNT(*) FILTER (
                            WHERE NOT blocked_host_backoff
                              AND NOT retry_quarantine
                              AND blocked_domain_next_request
                        ) AS state_blocked_domain_next_request,
                        COUNT(*) FILTER (
                            WHERE NOT blocked_host_backoff
                              AND NOT retry_quarantine
                              AND NOT blocked_domain_next_request
                              AND blocked_next_fetch
                        ) AS state_scheduled,
                        COUNT(*) FILTER (
                            WHERE NOT blocked_host_backoff
                              AND NOT retry_quarantine
                              AND NOT blocked_domain_next_request
                              AND NOT blocked_next_fetch
                        ) AS state_ready
                    FROM readiness_entries""",
                (*blocked_queue_params, now, now, now),
            )
            (
                pending,
                ready,
                ready_domains,
                next_ready_at,
                blocked_next_fetch,
                blocked_domain_next_request,
                blocked_host_backoff,
                retry_quarantine,
                state_blocked_domain_next_request,
                state_scheduled,
                state_ready,
            ) = cur.fetchone()
        return FrontierReadiness(
            pending=pending or 0,
            ready=ready or 0,
            ready_domains=ready_domains or 0,
            next_ready_delay=None if next_ready_at is None else max(0.0, next_ready_at - now),
            blocked={
                "next_fetch_at": blocked_next_fetch or 0,
                "domain_next_request": blocked_domain_next_request or 0,
                "host_backoff": blocked_host_backoff or 0,
                "retry_quarantine": retry_quarantine or 0,
            },
            state_counts={
                "ready": state_ready or 0,
                "scheduled": state_scheduled or 0,
                "blocked_domain_next_request": state_blocked_domain_next_request or 0,
                "blocked_host_backoff": blocked_host_backoff or 0,
                "retry_quarantine": retry_quarantine or 0,
            },
        )
