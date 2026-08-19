"""Scheduler lease selection services."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

LEASE_STRATEGY_URL_ORDER = "url_order"
LEASE_STRATEGY_HOST_FIRST = "host_first"


@dataclass(frozen=True)
class HostFirstReadModelResult:
    """Result of one host-head read-model lease attempt."""

    task: Any | None
    read_model: str
    candidates: int = 0
    stale_candidates: int = 0
    execution_tier: int | None = None


class SchedulerLeaseSelector:
    """Owns scheduler lease selection strategies."""

    def __init__(
        self,
        ledger: Any,
        *,
        task_cls: type,
        runnable_host_head_cls: type,
        url_ledger_table: str,
        lease_strategy_url_order: str,
        lease_strategy_host_first: str,
        host_head_lookahead: int,
        host_head_read_model_lookahead: int,
    ) -> None:
        self._ledger = ledger
        self._task_cls = task_cls
        self._runnable_host_head_cls = runnable_host_head_cls
        self._url_ledger_table = url_ledger_table
        self._lease_strategy_url_order = lease_strategy_url_order
        self._lease_strategy_host_first = lease_strategy_host_first
        self._host_head_lookahead = host_head_lookahead
        self._host_head_read_model_lookahead = host_head_read_model_lookahead

    def host_head_order_by_sql(
        self, alias: str, *, latency_ms_sql: str | None = None
    ) -> str:
        """Return ORDER BY used to compare the best runnable URL for each host."""
        latency_penalty = self._ledger._latency_penalty_sql(
            alias,
            latency_ms_sql=latency_ms_sql,
        )
        return (
            f"{alias}.next_fetch_at ASC, "
            f"{latency_penalty} ASC, "
            f"{alias}.added_at ASC, "
            f"{alias}.scheduler_score DESC, "
            f"{alias}.url ASC"
        )

    def runnable_host_heads_sql(
        self,
        *,
        physical_queue: str,
        runnable_sql: Any,
    ) -> tuple[str, tuple[object, ...]]:
        """Return SQL that derives one ready head URL per host."""
        table_name = self._ledger._queue_table_sql(physical_queue)
        host_head_order = self.host_head_order_by_sql(
            "candidate",
            latency_ms_sql=runnable_sql.latency_ms_sql,
        )
        latency_penalty = self._ledger._latency_penalty_sql(
            "candidate",
            latency_ms_sql=runnable_sql.latency_ms_sql,
        )
        sql = f"""SELECT selected.host,
                         selected.url,
                         selected.next_fetch_at,
                         selected.added_at,
                         selected.scheduler_score,
                         selected.latency_penalty,
                         selected.host_pending_count
                  FROM (
                      SELECT DISTINCT ON (candidate.host)
                          candidate.host,
                          candidate.url,
                          candidate.next_fetch_at,
                          candidate.added_at,
                          candidate.scheduler_score,
                          {latency_penalty} AS latency_penalty,
                          COUNT(*) OVER (PARTITION BY candidate.host) AS host_pending_count
                      FROM {table_name} AS candidate
                      {runnable_sql.join_sql}
                      WHERE {runnable_sql.where}
                      ORDER BY candidate.host, {host_head_order}
                  ) AS selected
                  ORDER BY
                      selected.host_pending_count ASC,
                      selected.latency_penalty ASC,
                      selected.next_fetch_at ASC,
                      selected.added_at ASC,
                      selected.scheduler_score DESC,
                      selected.url ASC"""
        return sql, runnable_sql.params

    def runnable_host_head_sort_key(
        self, head: Any
    ) -> tuple[int, int, float, float, float, str]:
        """Return the canonical host-first comparison key for runnable host heads."""
        return (
            head.host_pending_count,
            head.latency_penalty,
            head.next_fetch_at,
            head.added_at,
            -head.scheduler_score,
            head.url,
        )

    def runnable_host_heads(
        self,
        *,
        limit: int,
        host: str | None = None,
        exclude_hosts: list[str] | None = None,
        runnable_surface: str | None = None,
        physical_queues: list[str] | None = None,
        now: float | None = None,
    ) -> list[Any]:
        """Return one ready runnable head per host as a derived read model."""
        if limit <= 0:
            return []

        runnable_at = time.time() if now is None else now
        normalized_physical_queues = self._ledger._normalized_surface_queues(
            runnable_surface=runnable_surface,
            physical_queues=physical_queues,
        )
        heads: list[Any] = []

        with self._ledger._conn.cursor() as cur:
            for physical_queue in normalized_physical_queues:
                runnable_sql = self._ledger._queue_runnable_sql(
                    alias="candidate",
                    now=runnable_at,
                    host=host,
                    exclude_hosts=exclude_hosts,
                )
                sql, params = self.runnable_host_heads_sql(
                    physical_queue=physical_queue,
                    runnable_sql=runnable_sql,
                )
                cur.execute(f"{sql} LIMIT %s", (*params, limit))
                heads.extend(
                    self._runnable_host_head_cls(
                        host_key=host_key,
                        url=url,
                        next_fetch_at=next_fetch_at,
                        added_at=added_at,
                        scheduler_score=scheduler_score,
                        latency_penalty=latency_penalty,
                        host_pending_count=host_pending_count,
                    )
                    for (
                        host_key,
                        url,
                        next_fetch_at,
                        added_at,
                        scheduler_score,
                        latency_penalty,
                        host_pending_count,
                    ) in cur.fetchall()
                )

        heads.sort(key=self.runnable_host_head_sort_key)
        return heads[:limit]

    def select_runnable_host_head(
        self,
        *,
        host: str | None = None,
        exclude_hosts: list[str] | None = None,
        runnable_surface: str | None = None,
        physical_queues: list[str] | None = None,
        now: float | None = None,
    ) -> Any | None:
        """Return the next host-level runnable head for host-first leasing."""
        heads = self.runnable_host_heads(
            limit=self._host_head_lookahead,
            host=host,
            exclude_hosts=exclude_hosts,
            runnable_surface=runnable_surface,
            physical_queues=physical_queues,
            now=now,
        )
        if not heads:
            return None
        return min(heads, key=self.runnable_host_head_sort_key)

    def lease_next(
        self,
        *,
        host: str | None = None,
        lease_seconds: float | None = None,
        lease_strategy: str | None = None,
        exclude_hosts: list[str] | None = None,
        runnable_surface: str | None = None,
        execution_tiers: list[int] | None = None,
    ) -> Any | None:
        """Lease the next runnable URL, optionally filtered by host."""
        normalized_physical_queues = self._ledger._normalized_surface_queues(
            runnable_surface=runnable_surface,
            physical_queues=None,
        )
        normalized_lease_strategy = self._ledger._normalize_lease_strategy(lease_strategy)
        if len(normalized_physical_queues) != 1:
            if normalized_lease_strategy == self._lease_strategy_host_first:
                return self.lease_next_host_first(
                    host=host,
                    lease_seconds=lease_seconds,
                    exclude_hosts=exclude_hosts,
                    physical_queues=normalized_physical_queues,
                    execution_tiers=execution_tiers,
                )

            for physical_queue in normalized_physical_queues:
                task = self.lease_next_url_order(
                    host=host,
                    lease_seconds=lease_seconds,
                    exclude_hosts=exclude_hosts,
                    physical_queue=physical_queue,
                )
                if task is not None:
                    return task
            return None

        physical_queue = normalized_physical_queues[0]
        if normalized_lease_strategy == self._lease_strategy_host_first:
            return self.lease_next_host_first(
                host=host,
                lease_seconds=lease_seconds,
                exclude_hosts=exclude_hosts,
                physical_queue=physical_queue,
                execution_tiers=execution_tiers,
            )

        return self.lease_next_url_order(
            host=host,
            lease_seconds=lease_seconds,
            exclude_hosts=exclude_hosts,
            physical_queue=physical_queue,
        )

    def lease_next_host_first(
        self,
        *,
        host: str | None,
        lease_seconds: float | None,
        exclude_hosts: list[str] | None,
        physical_queue: str | None = None,
        physical_queues: list[str] | None = None,
        execution_tiers: list[int] | None = None,
    ) -> Any | None:
        """Lease from the next selected runnable host head."""
        if physical_queues is None:
            if physical_queue is None:
                physical_queues = [self._ledger._default_scheduled_physical_queue()]
            else:
                physical_queues = [physical_queue]
        normalized_physical_queues = self._ledger._normalized_physical_queues(physical_queues)

        now = time.time()
        self._ledger._recover_leased_locked(now, expired_only=True)
        self._ledger._conn.commit()

        try:
            read_model_result = self.lease_next_host_first_from_read_model(
                host=host,
                lease_seconds=lease_seconds,
                exclude_hosts=exclude_hosts,
                physical_queues=normalized_physical_queues,
                execution_tiers=execution_tiers,
                now=now,
            )
        except Exception:
            self._ledger._conn.rollback()
            logger.debug(
                "Failed to lease from host runnable-head read model; using bounded fallback",
                exc_info=True,
            )
            read_model_result = HostFirstReadModelResult(
                task=None,
                read_model="error",
            )

        self._ledger._record_host_first_read_model(read_model_result.read_model)
        if read_model_result.task is not None:
            self._ledger._set_last_lease_diagnostics(
                read_model=read_model_result.read_model,
                fallback="none",
                read_model_candidates=read_model_result.candidates,
                stale_candidates=read_model_result.stale_candidates,
                execution_tier=getattr(read_model_result, "execution_tier", None),
            )
            return read_model_result.task

        if execution_tiers:
            self._ledger._set_last_lease_diagnostics(
                read_model=read_model_result.read_model,
                fallback="tier_filtered",
                read_model_candidates=read_model_result.candidates,
                stale_candidates=read_model_result.stale_candidates,
                execution_tier=getattr(read_model_result, "execution_tier", None),
            )
            return None

        fallback_task = self.lease_next_host_first_from_bounded_scan(
            host=host,
            lease_seconds=lease_seconds,
            exclude_hosts=exclude_hosts,
            physical_queues=normalized_physical_queues,
            now=now,
        )
        self._ledger._record_host_first_fallback(fallback_task)
        fallback = "hit" if fallback_task is not None else "miss"
        self._ledger._set_last_lease_diagnostics(
            read_model=read_model_result.read_model,
            fallback=fallback,
            read_model_candidates=read_model_result.candidates,
            stale_candidates=read_model_result.stale_candidates,
            execution_tier=getattr(read_model_result, "execution_tier", None),
        )
        return fallback_task

    def lease_next_host_first_from_read_model(
        self,
        *,
        host: str | None,
        lease_seconds: float | None,
        exclude_hosts: list[str] | None,
        physical_queues: list[str],
        execution_tiers: list[int] | None,
        now: float,
    ) -> HostFirstReadModelResult:
        """Lease host-first candidates from the loose read model first."""
        candidate_heads = self._ledger.host_runnable_heads_from_read_model(
            limit=self._host_head_read_model_lookahead,
            host=host,
            exclude_hosts=exclude_hosts,
            physical_queues=physical_queues,
            execution_tiers=execution_tiers,
            now=now,
        )
        stale_candidates = 0
        for head in candidate_heads:
            task = self.lease_candidate_url(
                candidate_url=head.url,
                physical_queue=head.physical_queue,
                lease_seconds=lease_seconds,
                host=host,
                exclude_hosts=exclude_hosts,
                now=now,
            )
            if task is not None:
                return HostFirstReadModelResult(
                    task=task,
                    read_model="hit",
                    candidates=len(candidate_heads),
                    stale_candidates=stale_candidates,
                    execution_tier=head.execution_tier,
                )
            stale_candidates += 1
            self._ledger._delete_host_runnable_head_candidate(
                physical_queue=head.physical_queue,
                url=head.url,
            )

        return HostFirstReadModelResult(
            task=None,
            read_model="stale" if stale_candidates else "miss",
            candidates=len(candidate_heads),
            stale_candidates=stale_candidates,
        )

    def lease_next_host_first_from_bounded_scan(
        self,
        *,
        host: str | None,
        lease_seconds: float | None,
        exclude_hosts: list[str] | None,
        physical_queues: list[str],
        now: float,
    ) -> Any | None:
        """Lease from a bounded queue scan when the host-head cache misses."""
        for physical_queue in physical_queues:
            task = self.lease_next_host_first_from_bounded_scan_queue(
                host=host,
                lease_seconds=lease_seconds,
                exclude_hosts=exclude_hosts,
                physical_queue=physical_queue,
                now=now,
            )
            if task is not None:
                return task
        return None

    def lease_next_host_first_from_bounded_scan_queue(
        self,
        *,
        host: str | None,
        lease_seconds: float | None,
        exclude_hosts: list[str] | None,
        physical_queue: str,
        now: float,
    ) -> Any | None:
        """Lease from one physical queue using a bounded host-first scan."""
        started_at = time.perf_counter()
        runnable_sql = self._ledger._queue_runnable_sql(
            alias="candidate",
            now=now,
            host=host,
            exclude_hosts=exclude_hosts,
        )
        order_by = self._ledger._lease_order_by_sql(
            "candidate",
            self._lease_strategy_host_first,
            latency_ms_sql=runnable_sql.latency_ms_sql,
        )
        candidate_from = (
            f"FROM {self._ledger._queue_table_sql(physical_queue)} AS candidate "
            f"{runnable_sql.join_sql}"
        )
        try:
            with self._ledger._conn.cursor() as cur:
                cur.execute(
                    f"""SELECT candidate.url
                        {candidate_from}
                        WHERE {runnable_sql.where}
                        ORDER BY {order_by}, candidate.url ASC
                        LIMIT %s""",
                    (*runnable_sql.params, self._host_head_lookahead),
                )
                candidate_urls = [url for (url,) in cur.fetchall()]
            self._ledger._conn.commit()
        except Exception:
            self._ledger._conn.rollback()
            logger.debug("Failed bounded host-first fallback scan", exc_info=True)
            return None
        logger.debug(
            "Host runnable-head cache miss; queue=%s bounded fallback candidates=%d elapsed=%0.1fms",
            physical_queue,
            len(candidate_urls),
            (time.perf_counter() - started_at) * 1000,
        )
        for candidate_url in candidate_urls:
            task = self.lease_candidate_url(
                candidate_url=candidate_url,
                physical_queue=physical_queue,
                lease_seconds=lease_seconds,
                host=host,
                exclude_hosts=exclude_hosts,
                now=now,
            )
            if task is not None:
                return task
        return None

    def lease_next_url_order(
        self,
        *,
        host: str | None,
        lease_seconds: float | None,
        exclude_hosts: list[str] | None,
        physical_queue: str,
    ) -> Any | None:
        """Lease using URL-order selection from one physical queue."""
        now = time.time()
        runnable_sql = self._ledger._queue_runnable_sql(
            alias="candidate",
            now=now,
            host=host,
            exclude_hosts=exclude_hosts,
        )
        candidate_from = (
            f"FROM {self._ledger._queue_table_sql(physical_queue)} AS candidate "
            f"{runnable_sql.join_sql}"
        )
        order_by = self._ledger._lease_order_by_sql(
            "candidate",
            self._lease_strategy_url_order,
            latency_ms_sql=runnable_sql.latency_ms_sql,
        )
        with self._ledger._conn.cursor() as cur:
            cur.execute(
                f"""SELECT candidate.url
                    {candidate_from}
                    WHERE candidate.url = (
                        SELECT candidate.url
                        {candidate_from}
                        WHERE {runnable_sql.where}
                        ORDER BY {order_by}
                        LIMIT 1
                        FOR UPDATE OF candidate SKIP LOCKED
                    )""",
                runnable_sql.params,
            )
            row = cur.fetchone()
        if row is None:
            self._ledger._conn.commit()
            return None
        return self.lease_candidate_url(
            candidate_url=row[0],
            physical_queue=physical_queue,
            lease_seconds=lease_seconds,
            host=host,
            exclude_hosts=exclude_hosts,
            now=now,
        )

    def lease_candidate_url(
        self,
        *,
        candidate_url: str,
        physical_queue: str,
        lease_seconds: float | None,
        host: str | None,
        exclude_hosts: list[str] | None,
        now: float,
    ) -> Any | None:
        """Lease one concrete candidate URL when it is still runnable and unlocked."""
        lease_token = self._ledger._leases.new_token()
        duration = self._ledger._lease_seconds if lease_seconds is None else lease_seconds
        lease_expires_at = now + duration
        runnable_sql = self._ledger._queue_runnable_sql(
            alias="candidate",
            now=now,
            host=host,
            exclude_hosts=exclude_hosts,
        )
        candidate_from = (
            f"FROM {self._ledger._queue_table_sql(physical_queue)} AS candidate "
            f"{runnable_sql.join_sql}"
        )

        try:
            with self._ledger._conn.cursor() as cur:
                cur.execute(
                    f"""SELECT
                            candidate.url,
                            candidate.scheduler_score,
                            ledger.discovery_value,
                            ledger.source_url,
                            candidate.added_at,
                            candidate.next_fetch_at,
                            candidate.host
                        {candidate_from}
                        JOIN {self._url_ledger_table} AS ledger ON ledger.url = candidate.url
                        WHERE candidate.url = %s
                          AND ledger.terminal_reason IS NULL
                          AND {runnable_sql.where}
                        FOR UPDATE OF candidate SKIP LOCKED""",
                    (candidate_url, *runnable_sql.params),
                )
                row = cur.fetchone()
                if row:
                    self._ledger._delete_queue_entries(cur, [row[0]])
                    self._ledger._leases.upsert(
                        cur,
                        [(row[0], row[6], physical_queue, lease_token, lease_expires_at)],
                    )
            self._ledger._conn.commit()
        except Exception:
            self._ledger._conn.rollback()
            logger.exception("Failed to lease next URL")
            return None

        if row is None:
            return None

        url, scheduler_score, discovery_value, source_url, added_at, next_fetch_at, _host = row
        return self._task_cls(
            url=url,
            discovery_value=discovery_value,
            scheduler_score=scheduler_score,
            source_url=source_url,
            added_at=added_at,
            next_fetch_at=next_fetch_at,
            lease_token=lease_token,
            lease_expires_at=lease_expires_at,
        )

    def lease_batch(
        self,
        *,
        count: int,
        host: str | None = None,
        lease_seconds: float | None = None,
        lease_strategy: str | None = None,
        exclude_hosts: list[str] | None = None,
        runnable_surface: str | None = None,
    ) -> list[Any]:
        """Lease a batch of runnable URLs."""
        normalized_physical_queues = self._ledger._normalized_surface_queues(
            runnable_surface=runnable_surface,
            physical_queues=None,
        )
        normalized_lease_strategy = self._ledger._normalize_lease_strategy(lease_strategy)
        if len(normalized_physical_queues) != 1:
            tasks: list[Any] = []
            while len(tasks) < count:
                task = self.lease_next(
                    host=host,
                    lease_seconds=lease_seconds,
                    lease_strategy=normalized_lease_strategy,
                    exclude_hosts=exclude_hosts,
                    runnable_surface=runnable_surface,
                )
                if task is None:
                    break
                tasks.append(task)
            return tasks

        if normalized_lease_strategy == self._lease_strategy_host_first:
            tasks = []
            while len(tasks) < count:
                task = self.lease_next(
                    host=host,
                    lease_seconds=lease_seconds,
                    lease_strategy=normalized_lease_strategy,
                    exclude_hosts=exclude_hosts,
                    runnable_surface=runnable_surface,
                )
                if task is None:
                    break
                tasks.append(task)
            return tasks

        now = time.time()
        lease_token = self._ledger._leases.new_token()
        duration = self._ledger._lease_seconds if lease_seconds is None else lease_seconds
        lease_expires_at = now + duration
        runnable_sql = self._ledger._queue_runnable_sql(
            alias="candidate",
            now=now,
            host=host,
            exclude_hosts=exclude_hosts,
        )
        candidate_from = (
            f"FROM {self._ledger._queue_table_sql(normalized_physical_queues[0])} AS candidate "
            f"{runnable_sql.join_sql}"
        )
        order_by = self._ledger._lease_order_by_sql(
            "candidate",
            normalized_lease_strategy,
            latency_ms_sql=runnable_sql.latency_ms_sql,
        )
        params: list[object] = [*runnable_sql.params, count]

        try:
            self._ledger._recover_leased_locked(now, expired_only=True)
            with self._ledger._conn.cursor() as cur:
                cur.execute(
                    f"""SELECT
                            candidate.url,
                            candidate.scheduler_score,
                            ledger.discovery_value,
                            ledger.source_url,
                            candidate.added_at,
                            candidate.next_fetch_at,
                            candidate.host
                        {candidate_from}
                        JOIN {self._url_ledger_table} AS ledger ON ledger.url = candidate.url
                        WHERE candidate.url IN (
                            SELECT candidate.url
                            {candidate_from}
                            WHERE {runnable_sql.where}
                            ORDER BY {order_by}
                            LIMIT %s
                            FOR UPDATE OF candidate SKIP LOCKED
                        )
                        ORDER BY {order_by}, candidate.url ASC
                        FOR UPDATE OF candidate""",
                    params,
                )
                rows = cur.fetchall()
                if rows:
                    self._ledger._delete_queue_entries(cur, [row[0] for row in rows])
                    self._ledger._leases.upsert(
                        cur,
                        [
                            (
                                row[0],
                                row[6],
                                normalized_physical_queues[0],
                                lease_token,
                                lease_expires_at,
                            )
                            for row in rows
                        ],
                    )
            self._ledger._conn.commit()
        except Exception:
            self._ledger._conn.rollback()
            logger.exception("Failed to lease batch of URLs")
            return []

        return [
            self._task_cls(
                url=url,
                discovery_value=discovery_value,
                scheduler_score=scheduler_score,
                source_url=source_url,
                added_at=added_at,
                next_fetch_at=next_fetch_at,
                lease_token=lease_token,
                lease_expires_at=lease_expires_at,
            )
            for (
                url,
                scheduler_score,
                discovery_value,
                source_url,
                added_at,
                next_fetch_at,
                _host,
            ) in rows
        ]
