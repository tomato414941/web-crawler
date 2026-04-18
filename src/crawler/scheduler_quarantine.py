"""Quarantine policy helpers for blocked scheduler queues."""

from __future__ import annotations

import time
from collections.abc import Callable


class SchedulerQuarantine:
    """State transitions for host-backoff quarantine queues."""

    def __init__(
        self,
        conn,
        *,
        queue_frontline: str,
        queue_deferred: str,
        queue_refresh: str,
        blocked_queue_table: str,
        queue_table_sql: Callable[[str], str],
        delete_queue_entries: Callable,
        insert_blocked_rows: Callable,
        insert_pending_rows: Callable,
    ):
        self._conn = conn
        self._queue_frontline = queue_frontline
        self._queue_deferred = queue_deferred
        self._queue_refresh = queue_refresh
        self._blocked_queue_table = blocked_queue_table
        self._queue_table_sql = queue_table_sql
        self._delete_queue_entries = delete_queue_entries
        self._insert_blocked_rows = insert_blocked_rows
        self._insert_pending_rows = insert_pending_rows

    def rebalance(self, *, now: float | None = None) -> tuple[int, int]:
        """Move backoff-blocked URLs out of the normal scheduler queues."""
        now = time.time() if now is None else now
        quarantined = 0

        with self._conn.cursor() as cur:
            cur.execute(
                f"""SELECT queue.url, queue.domain, queue.priority, queue.next_fetch_at, queue.added_at,
                           %s AS physical_queue
                    FROM {self._queue_table_sql(self._queue_frontline)} AS queue
                    JOIN domain_state ON domain_state.host_key = queue.domain
                    WHERE domain_state.backoff_until > %s
                    UNION ALL
                    SELECT queue.url, queue.domain, queue.priority, queue.next_fetch_at, queue.added_at,
                           %s AS physical_queue
                    FROM {self._queue_table_sql(self._queue_deferred)} AS queue
                    JOIN domain_state ON domain_state.host_key = queue.domain
                    WHERE domain_state.backoff_until > %s
                    UNION ALL
                    SELECT queue.url, queue.domain, queue.priority, queue.next_fetch_at, queue.added_at,
                           %s AS physical_queue
                    FROM {self._queue_table_sql(self._queue_refresh)} AS queue
                    JOIN domain_state ON domain_state.host_key = queue.domain
                    WHERE domain_state.backoff_until > %s""",
                (
                    self._queue_frontline,
                    now,
                    self._queue_deferred,
                    now,
                    self._queue_refresh,
                    now,
                ),
            )
            blocked_rows = cur.fetchall()
            if blocked_rows:
                urls = [row[0] for row in blocked_rows]
                self._delete_queue_entries(cur, urls)
                self._insert_blocked_rows(cur, blocked_rows, quarantined_at=now)
                quarantined = len(blocked_rows)

        self._conn.commit()
        return quarantined, 0

    def retire(
        self,
        *,
        min_consecutive_failures: int,
        min_quarantine_seconds: float,
        limit: int = 256,
        now: float | None = None,
    ) -> int:
        """Retire long-stuck blocked URLs out of pending scheduler state."""
        if min_consecutive_failures < 0 or min_quarantine_seconds < 0 or limit <= 0:
            return 0

        now = time.time() if now is None else now
        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH doomed AS (
                        SELECT blocked.url
                        FROM {self._blocked_queue_table} AS blocked
                        JOIN domain_state ON domain_state.host_key = blocked.domain
                        WHERE COALESCE(domain_state.consecutive_failures, 0) >= %s
                          AND blocked.quarantined_at <= %s
                        ORDER BY
                            COALESCE(domain_state.consecutive_failures, 0) DESC,
                            blocked.quarantined_at ASC,
                            blocked.added_at ASC,
                            blocked.url ASC
                        LIMIT %s
                    ), removed AS (
                        DELETE FROM {self._blocked_queue_table} AS blocked
                        USING doomed
                        WHERE blocked.url = doomed.url
                        RETURNING blocked.url
                    )
                    UPDATE url_ledger
                    SET next_fetch_at = %s,
                        last_error = COALESCE(last_error, 'retry_quarantine_retired'),
                        terminal_reason = COALESCE(terminal_reason, last_error, 'retry_quarantine_retired'),
                        terminalized_at = COALESCE(terminalized_at, %s)
                    WHERE url IN (SELECT url FROM removed)
                    RETURNING url""",
                (
                    min_consecutive_failures,
                    now - min_quarantine_seconds,
                    limit,
                    now,
                    now,
                ),
            )
            retired = len(cur.fetchall())
        self._conn.commit()
        return retired

    def restore_recovered(
        self,
        *,
        limit: int,
        per_domain: int,
        now: float | None = None,
    ) -> int:
        """Return recovered blocked URLs to the deferred runnable surface."""
        if limit <= 0 or per_domain <= 0:
            return 0

        now = time.time() if now is None else now
        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH ranked AS (
                        SELECT
                            blocked.url,
                            blocked.domain,
                            blocked.priority,
                            blocked.next_fetch_at,
                            blocked.added_at,
                            %s AS physical_queue,
                            ROW_NUMBER() OVER (
                                PARTITION BY blocked.domain
                                ORDER BY blocked.next_fetch_at ASC, blocked.added_at ASC, blocked.url ASC
                            ) AS domain_rownum
                        FROM {self._blocked_queue_table} AS blocked
                        LEFT JOIN domain_state ON domain_state.host_key = blocked.domain
                        WHERE COALESCE(domain_state.backoff_until, 0) <= %s
                          AND COALESCE(domain_state.consecutive_failures, 0) = 0
                    ), picked AS (
                        SELECT url
                        FROM ranked
                        WHERE domain_rownum <= %s
                        ORDER BY priority DESC, next_fetch_at ASC, added_at ASC, url ASC
                        LIMIT %s
                    )
                    DELETE FROM {self._blocked_queue_table} AS blocked
                    USING picked
                    WHERE blocked.url = picked.url
                    RETURNING
                        blocked.url,
                        blocked.domain,
                        blocked.priority,
                        blocked.next_fetch_at,
                        blocked.added_at,
                        %s AS physical_queue""",
                (self._queue_deferred, now, per_domain, limit, self._queue_deferred),
            )
            rows = cur.fetchall()
            if rows:
                self._insert_pending_rows(cur, rows)
        self._conn.commit()
        return len(rows)

    def promote(
        self,
        limit: int,
        *,
        per_domain: int = 1,
        max_consecutive_failures: int | None = None,
        now: float | None = None,
    ) -> int:
        """Return a small cooled-down subset from blocked queue back into the deferred runnable surface."""
        if limit <= 0 or per_domain <= 0:
            return 0

        now = time.time() if now is None else now
        with self._conn.cursor() as cur:
            effective_max_failures = (
                None
                if max_consecutive_failures is None or max_consecutive_failures < 0
                else max_consecutive_failures
            )
            cur.execute(
                f"""WITH ranked_candidates AS (
                        SELECT
                            blocked.url,
                            blocked.domain,
                            blocked.priority,
                            blocked.next_fetch_at,
                            blocked.added_at,
                            %s AS physical_queue,
                            COALESCE(domain_state.consecutive_failures, 0) AS failure_count,
                            ROW_NUMBER() OVER (
                                PARTITION BY blocked.domain
                                ORDER BY
                                    COALESCE(domain_state.consecutive_failures, 0) ASC,
                                    blocked.next_fetch_at ASC,
                                    blocked.added_at ASC,
                                    blocked.url ASC
                            ) AS domain_rownum
                        FROM {self._blocked_queue_table} AS blocked
                        LEFT JOIN domain_state ON domain_state.host_key = blocked.domain
                        WHERE COALESCE(domain_state.backoff_until, 0) <= %s
                          AND (
                                %s IS NULL
                                OR COALESCE(domain_state.consecutive_failures, 0) <= %s
                          )
                    ), picked AS (
                        SELECT url
                        FROM ranked_candidates
                        WHERE domain_rownum <= %s
                        ORDER BY
                            failure_count ASC,
                            next_fetch_at ASC,
                            added_at ASC,
                            url ASC
                        LIMIT %s
                    )
                    DELETE FROM {self._blocked_queue_table} AS blocked
                    USING picked
                    WHERE blocked.url = picked.url
                    RETURNING
                        blocked.url,
                        blocked.domain,
                        blocked.priority,
                        blocked.next_fetch_at,
                        blocked.added_at,
                        %s AS physical_queue""",
                (
                    self._queue_deferred,
                    now,
                    effective_max_failures,
                    effective_max_failures,
                    per_domain,
                    limit,
                    self._queue_deferred,
                ),
            )
            rows = cur.fetchall()
            if rows:
                self._insert_pending_rows(cur, rows)
        self._conn.commit()
        return len(rows)
