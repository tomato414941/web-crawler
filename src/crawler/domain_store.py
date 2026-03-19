"""Persistent host-level scheduling state."""

from __future__ import annotations

import logging
import time

from .domain_state import PersistedDomainState

logger = logging.getLogger(__name__)

DOMAIN_STATE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS domain_state (
    host_key TEXT PRIMARY KEY,
    crawl_delay_seconds DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    next_request_at DOUBLE PRECISION NOT NULL DEFAULT 0,
    backoff_until DOUBLE PRECISION NOT NULL DEFAULT 0,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    robots_checked_at DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_domain_state_next_request_at
    ON domain_state(next_request_at);
CREATE INDEX IF NOT EXISTS idx_domain_state_backoff_until
    ON domain_state(backoff_until);
"""


class DomainStore:
    """Postgres-backed storage for host scheduling state."""

    def __init__(self, conn, default_delay: float = 1.0):
        self._conn = conn
        self._default_delay = default_delay
        self._init_schema()

    def _init_schema(self):
        with self._conn.cursor() as cur:
            cur.execute(DOMAIN_STATE_SCHEMA_SQL)
        self._conn.commit()
        logger.info("Domain state schema initialized")

    def _row_to_state(self, row: tuple) -> PersistedDomainState:
        return PersistedDomainState(
            host_key=row[0],
            crawl_delay_seconds=row[1],
            next_request_at=row[2],
            backoff_until=row[3],
            consecutive_failures=row[4],
            robots_checked_at=row[5],
            updated_at=row[6],
        )

    def get_or_create(self, host_key: str) -> PersistedDomainState:
        """Return the persistent state for a host key, creating it if needed."""
        now = time.time()
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO domain_state (host_key, crawl_delay_seconds, updated_at)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (host_key) DO NOTHING""",
                (host_key, self._default_delay, now),
            )
            cur.execute(
                """SELECT
                       host_key,
                       crawl_delay_seconds,
                       next_request_at,
                       backoff_until,
                       consecutive_failures,
                       robots_checked_at,
                       updated_at
                   FROM domain_state
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
    ) -> PersistedDomainState:
        """Persist the latest robots check time and crawl delay."""
        checked_at = time.time() if checked_at is None else checked_at
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO domain_state (
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
                   RETURNING
                       host_key,
                       crawl_delay_seconds,
                       next_request_at,
                       backoff_until,
                       consecutive_failures,
                       robots_checked_at,
                       updated_at""",
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
    ) -> tuple[float, PersistedDomainState]:
        """Reserve the next request slot and return the required wait time."""
        now = time.time() if now is None else now
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO domain_state (host_key, crawl_delay_seconds, updated_at)
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
                       robots_checked_at
                   FROM domain_state
                   WHERE host_key = %s
                   FOR UPDATE""",
                (host_key,),
            )
            row = cur.fetchone()
            ready_at = max(row[1], row[2])
            wait_seconds = max(0.0, ready_at - now)
            next_request_at = max(now, ready_at) + crawl_delay_seconds
            cur.execute(
                """UPDATE domain_state
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
                       robots_checked_at,
                       updated_at""",
                (crawl_delay_seconds, next_request_at, now, host_key),
            )
            updated_row = cur.fetchone()
        self._conn.commit()
        return wait_seconds, self._row_to_state(updated_row)

    def record_success(self, host_key: str, *, now: float | None = None) -> PersistedDomainState:
        """Reset failure-related state after a successful request."""
        now = time.time() if now is None else now
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO domain_state (host_key, crawl_delay_seconds, updated_at)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (host_key) DO NOTHING""",
                (host_key, self._default_delay, now),
            )
            cur.execute(
                """UPDATE domain_state
                   SET consecutive_failures = 0,
                       backoff_until = 0,
                       updated_at = %s
                   WHERE host_key = %s
                   RETURNING
                       host_key,
                       crawl_delay_seconds,
                       next_request_at,
                       backoff_until,
                       consecutive_failures,
                       robots_checked_at,
                       updated_at""",
                (now, host_key),
            )
            row = cur.fetchone()
        self._conn.commit()
        return self._row_to_state(row)

    def record_failure(
        self,
        host_key: str,
        *,
        backoff_seconds: float,
        now: float | None = None,
    ) -> PersistedDomainState:
        """Advance failure streak and cooldown after a failed request."""
        now = time.time() if now is None else now
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO domain_state (host_key, crawl_delay_seconds, updated_at)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (host_key) DO NOTHING""",
                (host_key, self._default_delay, now),
            )
            cur.execute(
                """SELECT backoff_until, consecutive_failures
                   FROM domain_state
                   WHERE host_key = %s
                   FOR UPDATE""",
                (host_key,),
            )
            backoff_until, consecutive_failures = cur.fetchone()
            next_backoff_until = max(backoff_until, now + max(backoff_seconds, 0.0))
            cur.execute(
                """UPDATE domain_state
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
                       robots_checked_at,
                       updated_at""",
                (consecutive_failures + 1, next_backoff_until, now, host_key),
            )
            row = cur.fetchone()
        self._conn.commit()
        return self._row_to_state(row)
