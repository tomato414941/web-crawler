"""Persistent host-level scheduling state."""

from __future__ import annotations

import logging
import time

import psycopg2.extras

from .host_state import PersistedHostState
from .schema import assert_public_table_columns

logger = logging.getLogger(__name__)
HOST_STATE_REQUIRED_COLUMNS = {
    "host_key",
    "crawl_delay_seconds",
    "next_request_at",
    "backoff_until",
    "consecutive_failures",
    "latency_ewma_ms",
    "latency_last_ms",
    "latency_observed_at",
    "latency_sample_count",
    "robots_checked_at",
    "updated_at",
}

_LATENCY_EWMA_ALPHA = 0.2
_HOST_STATE_SELECT_COLUMNS = """
                       host_key,
                       crawl_delay_seconds,
                       next_request_at,
                       backoff_until,
                       consecutive_failures,
                       latency_ewma_ms,
                       latency_last_ms,
                       latency_observed_at,
                       latency_sample_count,
                       robots_checked_at,
                       updated_at"""


class HostStore:
    """Postgres-backed storage for host scheduling state."""

    def __init__(self, conn, default_delay: float = 1.0):
        self._conn = conn
        self._default_delay = default_delay
        assert_public_table_columns(self._conn, "host_state", HOST_STATE_REQUIRED_COLUMNS)

    def _row_to_state(self, row: tuple) -> PersistedHostState:
        return PersistedHostState(
            host_key=row[0],
            crawl_delay_seconds=row[1],
            next_request_at=row[2],
            backoff_until=row[3],
            consecutive_failures=row[4],
            latency_ewma_ms=row[5],
            latency_last_ms=row[6],
            latency_observed_at=row[7],
            latency_sample_count=row[8],
            robots_checked_at=row[9],
            updated_at=row[10],
        )

    def get_or_create(self, host_key: str) -> PersistedHostState:
        """Return the persistent state for a host key, creating it if needed."""
        now = time.time()
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO host_state (host_key, crawl_delay_seconds, updated_at)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (host_key) DO NOTHING""",
                (host_key, self._default_delay, now),
            )
            cur.execute(
                f"""SELECT{_HOST_STATE_SELECT_COLUMNS}
                   FROM host_state
                   WHERE host_key = %s""",
                (host_key,),
            )
            row = cur.fetchone()
        self._conn.commit()
        return self._row_to_state(row)

    def update_robots(
        self,
        host_key: str,
        *,
        crawl_delay_seconds: float,
        checked_at: float | None = None,
    ) -> PersistedHostState:
        """Persist the latest robots check time and crawl delay."""
        checked_at = time.time() if checked_at is None else checked_at
        with self._conn.cursor() as cur:
            cur.execute(
                f"""INSERT INTO host_state (
                       host_key,
                       crawl_delay_seconds,
                       robots_checked_at,
                       updated_at
                   )
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (host_key) DO UPDATE SET
                       crawl_delay_seconds = EXCLUDED.crawl_delay_seconds,
                       robots_checked_at = EXCLUDED.robots_checked_at,
                       updated_at = EXCLUDED.updated_at
                   RETURNING{_HOST_STATE_SELECT_COLUMNS}""",
                (host_key, crawl_delay_seconds, checked_at, checked_at),
            )
            row = cur.fetchone()
        self._conn.commit()
        return self._row_to_state(row)

    def reserve_request_slot(
        self,
        host_key: str,
        *,
        crawl_delay_seconds: float,
        now: float | None = None,
    ) -> tuple[float, PersistedHostState]:
        """Reserve the next request slot and return the required wait time."""
        now = time.time() if now is None else now
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO host_state (host_key, crawl_delay_seconds, updated_at)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (host_key) DO NOTHING""",
                (host_key, crawl_delay_seconds, now),
            )
            cur.execute(
                """SELECT
                       crawl_delay_seconds,
                       next_request_at,
                       backoff_until,
                       consecutive_failures,
                       latency_ewma_ms,
                       robots_checked_at
                   FROM host_state
                   WHERE host_key = %s
                   FOR UPDATE""",
                (host_key,),
            )
            row = cur.fetchone()
            ready_at = max(row[1], row[2])
            wait_seconds = max(0.0, ready_at - now)
            next_request_at = max(now, ready_at) + crawl_delay_seconds
            cur.execute(
                """UPDATE host_state
                   SET crawl_delay_seconds = %s,
                       next_request_at = %s,
                       updated_at = %s
                   WHERE host_key = %s
                   RETURNING
                       host_key,
                       crawl_delay_seconds,
                       next_request_at,
                       backoff_until,
                       consecutive_failures,
                       latency_ewma_ms,
                       latency_last_ms,
                       latency_observed_at,
                       latency_sample_count,
                       robots_checked_at,
                       updated_at""",
                (crawl_delay_seconds, next_request_at, now, host_key),
            )
            updated_row = cur.fetchone()
        self._conn.commit()
        return wait_seconds, self._row_to_state(updated_row)

    def record_success(
        self,
        host_key: str,
        *,
        now: float | None = None,
        request_latency_ms: float | None = None,
    ) -> PersistedHostState:
        """Reset failure-related state after a successful request."""
        now = time.time() if now is None else now
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO host_state (host_key, crawl_delay_seconds, updated_at)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (host_key) DO NOTHING""",
                (host_key, self._default_delay, now),
            )
            cur.execute(
                f"""UPDATE host_state
                   SET consecutive_failures = 0,
                       backoff_until = 0,
                       latency_ewma_ms = CASE
                           WHEN %s IS NULL THEN latency_ewma_ms
                           WHEN latency_ewma_ms <= 0 THEN %s
                           ELSE latency_ewma_ms + ((%s - latency_ewma_ms) * %s)
                       END,
                       latency_last_ms = CASE
                           WHEN %s IS NULL THEN latency_last_ms
                           ELSE %s
                       END,
                       latency_observed_at = CASE
                           WHEN %s IS NULL THEN latency_observed_at
                           ELSE %s
                       END,
                       latency_sample_count = CASE
                           WHEN %s IS NULL THEN latency_sample_count
                           ELSE latency_sample_count + 1
                       END,
                       updated_at = %s
                   WHERE host_key = %s
                   RETURNING{_HOST_STATE_SELECT_COLUMNS}""",
                (
                    request_latency_ms,
                    request_latency_ms,
                    request_latency_ms,
                    _LATENCY_EWMA_ALPHA,
                    request_latency_ms,
                    request_latency_ms,
                    request_latency_ms,
                    now,
                    request_latency_ms,
                    now,
                    host_key,
                ),
            )
            row = cur.fetchone()
        self._conn.commit()
        return self._row_to_state(row)

    def record_success_many(
        self,
        records: list[tuple[str, float | None]],
        *,
        now: float | None = None,
    ) -> int:
        """Reset failure-related state for multiple hosts in one transaction."""
        timestamp = time.time() if now is None else now
        grouped: dict[str, tuple[float | None, int]] = {}
        for host_key, request_latency_ms in records:
            if not host_key:
                continue
            _latency, count = grouped.get(host_key, (None, 0))
            grouped[host_key] = (request_latency_ms, count + 1)
        if not grouped:
            return 0

        host_rows = [
            (host_key, self._default_delay, timestamp)
            for host_key in sorted(grouped)
        ]
        update_rows = [
            (host_key, request_latency_ms, sample_count, timestamp, timestamp)
            for host_key, (request_latency_ms, sample_count) in sorted(grouped.items())
        ]
        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO host_state (host_key, crawl_delay_seconds, updated_at)
                   VALUES %s
                   ON CONFLICT (host_key) DO NOTHING""",
                host_rows,
                page_size=200,
            )
            psycopg2.extras.execute_values(
                cur,
                f"""UPDATE host_state AS state
                    SET consecutive_failures = 0,
                        backoff_until = 0,
                        latency_ewma_ms = CASE
                            WHEN incoming.request_latency_ms IS NULL THEN state.latency_ewma_ms
                            WHEN state.latency_ewma_ms <= 0 THEN incoming.request_latency_ms
                            ELSE state.latency_ewma_ms
                                + ((incoming.request_latency_ms - state.latency_ewma_ms)
                                   * {_LATENCY_EWMA_ALPHA})
                        END,
                        latency_last_ms = CASE
                            WHEN incoming.request_latency_ms IS NULL THEN state.latency_last_ms
                            ELSE incoming.request_latency_ms
                        END,
                        latency_observed_at = CASE
                            WHEN incoming.request_latency_ms IS NULL THEN state.latency_observed_at
                            ELSE incoming.observed_at
                        END,
                        latency_sample_count = CASE
                            WHEN incoming.request_latency_ms IS NULL THEN state.latency_sample_count
                            ELSE state.latency_sample_count + incoming.sample_count
                        END,
                        updated_at = incoming.updated_at
                    FROM (VALUES %s) AS incoming(
                        host_key,
                        request_latency_ms,
                        sample_count,
                        observed_at,
                        updated_at
                    )
                    WHERE state.host_key = incoming.host_key""",
                update_rows,
                template="(%s, %s, %s, %s, %s)",
                page_size=200,
            )
        self._conn.commit()
        return len(grouped)

    def record_failure(
        self,
        host_key: str,
        *,
        backoff_seconds: float,
        now: float | None = None,
    ) -> PersistedHostState:
        """Advance failure streak and cooldown after a failed request."""
        now = time.time() if now is None else now
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO host_state (host_key, crawl_delay_seconds, updated_at)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (host_key) DO NOTHING""",
                (host_key, self._default_delay, now),
            )
            cur.execute(
                """SELECT backoff_until, consecutive_failures
                   FROM host_state
                   WHERE host_key = %s
                   FOR UPDATE""",
                (host_key,),
            )
            backoff_until, consecutive_failures = cur.fetchone()
            next_backoff_until = max(backoff_until, now + max(backoff_seconds, 0.0))
            cur.execute(
                """UPDATE host_state
                   SET consecutive_failures = %s,
                       backoff_until = %s,
                       updated_at = %s
                   WHERE host_key = %s
                   RETURNING
                       host_key,
                       crawl_delay_seconds,
                       next_request_at,
                       backoff_until,
                       consecutive_failures,
                       latency_ewma_ms,
                       latency_last_ms,
                       latency_observed_at,
                       latency_sample_count,
                       robots_checked_at,
                       updated_at""",
                (consecutive_failures + 1, next_backoff_until, now, host_key),
            )
            row = cur.fetchone()
        self._conn.commit()
        return self._row_to_state(row)
