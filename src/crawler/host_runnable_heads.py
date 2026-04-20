"""Runtime read model for host-first scheduler execution."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import logging
import time

import psycopg2.extras

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HostRunnableHead:
    """Loose host-head read model row used for host-first performance evaluation."""

    physical_queue: str
    host_key: str
    url: str
    next_fetch_at: float
    added_at: float
    priority: float
    runnable_url_count: int
    latency_penalty: int
    runnable_at: float
    refreshed_at: float


@dataclass(frozen=True)
class HostRunnableHeadReadiness:
    """Loose readiness summary derived from host-head rows."""

    pending_urls: int
    ready_urls: int
    ready_hosts: int
    next_runnable_at: float | None


class HostRunnableHeadStore:
    """Owns the loose host-level runnable-head projection."""

    def __init__(
        self,
        conn,
        *,
        table_name: str,
        queue_table_sql: Callable[[str], str],
        normalize_physical_queue: Callable[[str | None], str],
        normalized_surface_queues: Callable[..., list[str]],
        latency_penalty_sql: Callable[..., str],
    ):
        self._conn = conn
        self._table_name = table_name
        self._queue_table_sql = queue_table_sql
        self._normalize_physical_queue = normalize_physical_queue
        self._normalized_surface_queues = normalized_surface_queues
        self._latency_penalty_sql = latency_penalty_sql

    def _head_sql(self, *, physical_queue: str) -> tuple[str, str, str]:
        normalized_physical_queue = self._normalize_physical_queue(physical_queue)
        queue_table = self._queue_table_sql(normalized_physical_queue)
        latency_penalty = self._latency_penalty_sql(
            "candidate",
            latency_ms_sql="COALESCE(candidate_host_state.latency_ewma_ms, 0)",
        )
        runnable_at = (
            "GREATEST("
            "candidate.next_fetch_at, "
            "COALESCE(candidate_host_state.next_request_at, 0), "
            "COALESCE(candidate_host_state.backoff_until, 0)"
            ")"
        )
        return queue_table, latency_penalty, runnable_at

    def refresh_host_in_tx(
        self,
        cur,
        *,
        physical_queue: str,
        host: str,
        refreshed_at: float | None = None,
    ) -> int:
        """Refresh one host-head row from source queue membership inside an open transaction."""
        if not host:
            return 0

        normalized_physical_queue = self._normalize_physical_queue(physical_queue)
        queue_table, latency_penalty, runnable_at = self._head_sql(
            physical_queue=normalized_physical_queue
        )
        timestamp = time.time() if refreshed_at is None else refreshed_at

        cur.execute(
            f"""WITH selected AS (
                    SELECT
                        candidate.host,
                        candidate.url,
                        candidate.next_fetch_at,
                        candidate.added_at,
                        candidate.priority,
                        (
                            SELECT COUNT(*)
                            FROM {queue_table} AS host_rows
                            WHERE host_rows.host = candidate.host
                        ) AS runnable_url_count,
                        {latency_penalty} AS latency_penalty,
                        {runnable_at} AS runnable_at
                    FROM {queue_table} AS candidate
                    LEFT JOIN host_state AS candidate_host_state
                        ON candidate_host_state.host_key = candidate.host
                    WHERE candidate.host = %s
                    ORDER BY
                        runnable_at ASC,
                        latency_penalty ASC,
                        candidate.added_at ASC,
                        candidate.priority DESC,
                        candidate.url ASC
                    LIMIT 1
                )
                INSERT INTO {self._table_name} (
                    physical_queue,
                    host,
                    head_url,
                    head_next_fetch_at,
                    head_added_at,
                    head_priority,
                    runnable_url_count,
                    latency_penalty,
                    runnable_at,
                    refreshed_at
                )
                SELECT
                    %s,
                    host,
                    url,
                    next_fetch_at,
                    added_at,
                    priority,
                    runnable_url_count,
                    latency_penalty,
                    runnable_at,
                    %s
                FROM selected
                ON CONFLICT (physical_queue, host) DO UPDATE SET
                    head_url = EXCLUDED.head_url,
                    head_next_fetch_at = EXCLUDED.head_next_fetch_at,
                    head_added_at = EXCLUDED.head_added_at,
                    head_priority = EXCLUDED.head_priority,
                    runnable_url_count = EXCLUDED.runnable_url_count,
                    latency_penalty = EXCLUDED.latency_penalty,
                    runnable_at = EXCLUDED.runnable_at,
                    refreshed_at = EXCLUDED.refreshed_at""",
            (host, normalized_physical_queue, timestamp),
        )
        if cur.rowcount:
            return cur.rowcount

        cur.execute(
            f"""DELETE FROM {self._table_name}
                WHERE physical_queue = %s
                  AND host = %s""",
            (normalized_physical_queue, host),
        )
        return 0

    def refresh_hosts_in_tx(
        self,
        cur,
        pairs: list[tuple[str, str]],
        *,
        refreshed_at: float | None = None,
    ) -> int:
        """Refresh multiple host-head rows from source queue membership in one transaction."""
        timestamp = time.time() if refreshed_at is None else refreshed_at
        refreshed = 0
        for physical_queue, host in sorted(
            {
                (self._normalize_physical_queue(physical_queue), host)
                for physical_queue, host in pairs
                if host
            }
        ):
            refreshed += self.refresh_host_in_tx(
                cur,
                physical_queue=physical_queue,
                host=host,
                refreshed_at=timestamp,
            )
        return refreshed

    def upsert_candidates_in_tx(
        self,
        cur,
        rows: list[tuple[str, str, float, float, float, str]],
        *,
        refreshed_at: float | None = None,
    ) -> int:
        """Offer inserted queue rows as host heads without host-local rescans."""
        if not rows:
            return 0

        timestamp = time.time() if refreshed_at is None else refreshed_at
        grouped: dict[tuple[str, str], list[tuple[str, str, float, float, float, str]]] = {}
        for row in rows:
            url, host, priority, next_fetch_at, added_at, physical_queue = row
            if not url or not host:
                continue
            key = (self._normalize_physical_queue(physical_queue), host)
            grouped.setdefault(key, []).append(
                (url, host, priority, next_fetch_at, added_at, key[0])
            )

        candidate_rows = []
        for (physical_queue, host), host_rows in grouped.items():
            best = min(
                host_rows,
                key=lambda row: (row[3], row[4], -row[2], row[0]),
            )
            url, _host, priority, next_fetch_at, added_at, _physical_queue = best
            candidate_rows.append(
                (
                    physical_queue,
                    host,
                    url,
                    next_fetch_at,
                    added_at,
                    priority,
                    len(host_rows),
                    timestamp,
                )
            )

        if not candidate_rows:
            return 0

        psycopg2.extras.execute_values(
            cur,
            f"""WITH incoming (
                    physical_queue,
                    host,
                    head_url,
                    head_next_fetch_at,
                    head_added_at,
                    head_priority,
                    runnable_url_count,
                    refreshed_at
                ) AS (VALUES %s)
                INSERT INTO {self._table_name} (
                    physical_queue,
                    host,
                    head_url,
                    head_next_fetch_at,
                    head_added_at,
                    head_priority,
                    runnable_url_count,
                    latency_penalty,
                    runnable_at,
                    refreshed_at
                )
                SELECT
                    incoming.physical_queue,
                    incoming.host,
                    incoming.head_url,
                    incoming.head_next_fetch_at,
                    incoming.head_added_at,
                    incoming.head_priority,
                    incoming.runnable_url_count,
                    CASE
                        WHEN COALESCE(host_state.latency_ewma_ms, 0) >= 1000.0 THEN 3
                        WHEN COALESCE(host_state.latency_ewma_ms, 0) >= 400.0 THEN 2
                        WHEN COALESCE(host_state.latency_ewma_ms, 0) >= 150.0 THEN 1
                        ELSE 0
                    END AS latency_penalty,
                    GREATEST(
                        incoming.head_next_fetch_at,
                        COALESCE(host_state.next_request_at, 0),
                        COALESCE(host_state.backoff_until, 0)
                    ) AS runnable_at,
                    incoming.refreshed_at
                FROM incoming
                LEFT JOIN host_state ON host_state.host_key = incoming.host
                ON CONFLICT (physical_queue, host) DO UPDATE SET
                    head_url = EXCLUDED.head_url,
                    head_next_fetch_at = EXCLUDED.head_next_fetch_at,
                    head_added_at = EXCLUDED.head_added_at,
                    head_priority = EXCLUDED.head_priority,
                    runnable_url_count = EXCLUDED.runnable_url_count,
                    latency_penalty = EXCLUDED.latency_penalty,
                    runnable_at = EXCLUDED.runnable_at,
                    refreshed_at = EXCLUDED.refreshed_at
                WHERE (
                    EXCLUDED.runnable_at,
                    EXCLUDED.latency_penalty,
                    EXCLUDED.head_added_at,
                    0 - EXCLUDED.head_priority,
                    EXCLUDED.head_url
                ) < (
                    {self._table_name}.runnable_at,
                    {self._table_name}.latency_penalty,
                    {self._table_name}.head_added_at,
                    0 - {self._table_name}.head_priority,
                    {self._table_name}.head_url
                )""",
            candidate_rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s)",
            page_size=200,
            fetch=False,
        )
        return len(candidate_rows)

    def rebuild(
        self,
        *,
        runnable_surface: str | None = None,
        physical_queues: list[str] | None = None,
        now: float | None = None,
    ) -> int:
        """Rebuild the loose host-head read model from scheduler queue membership."""
        refreshed_at = time.time() if now is None else now
        normalized_physical_queues = self._normalized_surface_queues(
            runnable_surface=runnable_surface,
            physical_queues=physical_queues,
        )
        rebuilt = 0

        try:
            with self._conn.cursor() as cur:
                for physical_queue in normalized_physical_queues:
                    rebuilt += self._rebuild_queue(
                        cur,
                        physical_queue=physical_queue,
                        refreshed_at=refreshed_at,
                    )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to rebuild host runnable heads")
            raise
        return rebuilt

    def _rebuild_queue(self, cur, *, physical_queue: str, refreshed_at: float) -> int:
        normalized_physical_queue = self._normalize_physical_queue(physical_queue)
        queue_table, latency_penalty, runnable_at = self._head_sql(
            physical_queue=normalized_physical_queue
        )

        cur.execute(
            f"DELETE FROM {self._table_name} WHERE physical_queue = %s",
            (normalized_physical_queue,),
        )
        cur.execute(
            f"""WITH candidate_rows AS (
                    SELECT
                        candidate.host,
                        candidate.url,
                        candidate.next_fetch_at,
                        candidate.added_at,
                        candidate.priority,
                        COUNT(*) OVER (PARTITION BY candidate.host) AS runnable_url_count,
                        {latency_penalty} AS latency_penalty,
                        {runnable_at} AS runnable_at
                    FROM {queue_table} AS candidate
                    LEFT JOIN host_state AS candidate_host_state
                        ON candidate_host_state.host_key = candidate.host
                ),
                selected AS (
                    SELECT DISTINCT ON (host)
                        host,
                        url,
                        next_fetch_at,
                        added_at,
                        priority,
                        runnable_url_count,
                        latency_penalty,
                        runnable_at
                    FROM candidate_rows
                    ORDER BY
                        host,
                        runnable_at ASC,
                        latency_penalty ASC,
                        added_at ASC,
                        priority DESC,
                        url ASC
                )
                INSERT INTO {self._table_name} (
                    physical_queue,
                    host,
                    head_url,
                    head_next_fetch_at,
                    head_added_at,
                    head_priority,
                    runnable_url_count,
                    latency_penalty,
                    runnable_at,
                    refreshed_at
                )
                SELECT
                    %s,
                    host,
                    url,
                    next_fetch_at,
                    added_at,
                    priority,
                    runnable_url_count,
                    latency_penalty,
                    runnable_at,
                    %s
                FROM selected""",
            (normalized_physical_queue, refreshed_at),
        )
        return cur.rowcount

    def read(
        self,
        *,
        limit: int,
        host: str | None = None,
        exclude_hosts: list[str] | None = None,
        runnable_surface: str | None = None,
        physical_queues: list[str] | None = None,
        now: float | None = None,
    ) -> list[HostRunnableHead]:
        """Read ready host-head candidates from the loose read model."""
        if limit <= 0:
            return []

        runnable_at = time.time() if now is None else now
        normalized_physical_queues = self._normalized_surface_queues(
            runnable_surface=runnable_surface,
            physical_queues=physical_queues,
        )
        effective_runnable_at = (
            "GREATEST("
            "heads.runnable_at, "
            "COALESCE(host_state.next_request_at, 0), "
            "COALESCE(host_state.backoff_until, 0)"
            ")"
        )
        latency_penalty = self._latency_penalty_sql(
            "heads",
            latency_ms_sql="COALESCE(host_state.latency_ewma_ms, 0)",
        )
        warm_robots_penalty = (
            "CASE WHEN GREATEST("
            "COALESCE(host_state.robots_checked_at, 0), "
            "COALESCE(host_ledger.robots_last_checked_at, 0)"
            ") > 0 THEN 0 ELSE 1 END"
        )
        warm_success_penalty = (
            "CASE WHEN COALESCE(host_ledger.success_count, 0) > 0 "
            "OR COALESCE(host_ledger.last_success_at, 0) > 0 "
            "THEN 0 ELSE 1 END"
        )
        conditions = ["heads.physical_queue = ANY(%s)", f"{effective_runnable_at} <= %s"]
        params: list[object] = [normalized_physical_queues, runnable_at]

        if host:
            conditions.append("heads.host = %s")
            params.append(host)
        if exclude_hosts:
            conditions.append("NOT (heads.host = ANY(%s))")
            params.append(exclude_hosts)

        with self._conn.cursor() as cur:
            cur.execute(
                f"""SELECT
                        heads.physical_queue,
                        heads.host,
                        heads.head_url,
                        heads.head_next_fetch_at,
                        heads.head_added_at,
                        heads.head_priority,
                        heads.runnable_url_count,
                        {latency_penalty} AS effective_latency_penalty,
                        {effective_runnable_at} AS effective_runnable_at,
                        heads.refreshed_at
                    FROM {self._table_name} AS heads
                    LEFT JOIN host_state ON host_state.host_key = heads.host
                    LEFT JOIN host_ledger ON host_ledger.host = heads.host
                    WHERE {" AND ".join(conditions)}
                    ORDER BY
                        {warm_robots_penalty} ASC,
                        {warm_success_penalty} ASC,
                        COALESCE(host_state.consecutive_failures, 0) ASC,
                        {latency_penalty} ASC,
                        heads.runnable_url_count ASC,
                        heads.head_next_fetch_at ASC,
                        heads.head_added_at ASC,
                        heads.head_priority DESC,
                        heads.physical_queue ASC,
                        heads.head_url ASC
                    LIMIT %s""",
                (*params, limit),
            )
            rows = cur.fetchall()

        return [
            HostRunnableHead(
                physical_queue=physical_queue,
                host_key=host_key,
                url=url,
                next_fetch_at=next_fetch_at,
                added_at=added_at,
                priority=priority,
                runnable_url_count=runnable_url_count,
                latency_penalty=latency_penalty,
                runnable_at=row_runnable_at,
                refreshed_at=refreshed_at,
            )
            for (
                physical_queue,
                host_key,
                url,
                next_fetch_at,
                added_at,
                priority,
                runnable_url_count,
                latency_penalty,
                row_runnable_at,
                refreshed_at,
            ) in rows
        ]

    def readiness_summary(
        self,
        *,
        runnable_surface: str | None = None,
        physical_queues: list[str] | None = None,
        now: float | None = None,
    ) -> HostRunnableHeadReadiness:
        """Return a loose queue readiness summary from the host-head projection."""
        runnable_at = time.time() if now is None else now
        normalized_physical_queues = self._normalized_surface_queues(
            runnable_surface=runnable_surface,
            physical_queues=physical_queues,
        )
        effective_runnable_at = (
            "GREATEST("
            "heads.runnable_at, "
            "COALESCE(host_state.next_request_at, 0), "
            "COALESCE(host_state.backoff_until, 0)"
            ")"
        )
        with self._conn.cursor() as cur:
            cur.execute(
                f"""SELECT
                        COALESCE(SUM(heads.runnable_url_count), 0) AS pending_urls,
                        COALESCE(
                            SUM(heads.runnable_url_count)
                                FILTER (WHERE {effective_runnable_at} <= %s),
                            0
                        ) AS ready_urls,
                        COUNT(*) FILTER (WHERE {effective_runnable_at} <= %s) AS ready_hosts,
                        MIN({effective_runnable_at})
                            FILTER (WHERE {effective_runnable_at} > %s) AS next_runnable_at
                    FROM {self._table_name} AS heads
                    LEFT JOIN host_state ON host_state.host_key = heads.host
                    WHERE heads.physical_queue = ANY(%s)""",
                (runnable_at, runnable_at, runnable_at, normalized_physical_queues),
            )
            pending_urls, ready_urls, ready_hosts, next_runnable_at = cur.fetchone()
        return HostRunnableHeadReadiness(
            pending_urls=int(pending_urls or 0),
            ready_urls=int(ready_urls or 0),
            ready_hosts=int(ready_hosts or 0),
            next_runnable_at=next_runnable_at,
        )

    def delete_candidate(self, *, physical_queue: str, url: str) -> None:
        """Drop a stale read-model candidate and refresh its host from queue membership."""
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    f"""DELETE FROM {self._table_name}
                        WHERE physical_queue = %s
                          AND head_url = %s
                        RETURNING physical_queue, host""",
                    (self._normalize_physical_queue(physical_queue), url),
                )
                pairs = [
                    (self._normalize_physical_queue(row_physical_queue), host)
                    for row_physical_queue, host in cur.fetchall()
                ]
                self.refresh_hosts_in_tx(cur, pairs)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            logger.debug("Failed to delete stale host runnable-head candidate", exc_info=True)
