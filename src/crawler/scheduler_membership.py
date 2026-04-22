"""Scheduler membership storage and surface naming."""

from __future__ import annotations

from dataclasses import dataclass

import psycopg2.extras

from .urls import normalize_url, url_branch_key

QUEUE_RUNNABLE = "runnable"
QUEUE_SCHEDULED = "scheduled"
QUEUE_REFRESH = "refresh"
SCHEDULER_SURFACE_RUNNABLE = "runnable"
SCHEDULER_SURFACE_SCHEDULED = "scheduled"
SCHEDULER_SURFACE_NORMAL = "normal"
SCHEDULER_SURFACE_REFRESH = "refresh"

PHYSICAL_QUEUE_NAMES = {
    QUEUE_RUNNABLE,
    QUEUE_SCHEDULED,
    QUEUE_REFRESH,
}
PHYSICAL_QUEUE_TABLES = {
    QUEUE_RUNNABLE: "scheduler_queue_runnable",
    QUEUE_SCHEDULED: "scheduler_queue_scheduled",
    QUEUE_REFRESH: "scheduler_queue_refresh",
}
QUEUE_TABLES = tuple(PHYSICAL_QUEUE_TABLES.values())
PHYSICAL_QUEUE_ORDER = (
    QUEUE_RUNNABLE,
    QUEUE_SCHEDULED,
    QUEUE_REFRESH,
)
SCHEDULER_SURFACE_PHYSICAL_QUEUES = {
    SCHEDULER_SURFACE_RUNNABLE: (QUEUE_RUNNABLE,),
    SCHEDULER_SURFACE_SCHEDULED: (QUEUE_SCHEDULED,),
    SCHEDULER_SURFACE_NORMAL: (QUEUE_RUNNABLE, QUEUE_SCHEDULED),
    SCHEDULER_SURFACE_REFRESH: (QUEUE_REFRESH,),
}
SCHEDULER_SURFACE_URGENCY = {
    SCHEDULER_SURFACE_RUNNABLE: 0,
    SCHEDULER_SURFACE_SCHEDULED: 1,
    SCHEDULER_SURFACE_REFRESH: 2,
}
PHYSICAL_QUEUE_DEFAULT_SCHEDULER_SURFACE = {
    QUEUE_RUNNABLE: SCHEDULER_SURFACE_RUNNABLE,
    QUEUE_SCHEDULED: SCHEDULER_SURFACE_SCHEDULED,
    QUEUE_REFRESH: SCHEDULER_SURFACE_REFRESH,
}


@dataclass(frozen=True)
class SchedulerQueueRow:
    url: str
    host: str
    scheduler_score: float
    next_fetch_at: float
    added_at: float
    physical_queue: str

    def as_tuple(self) -> tuple[str, str, float, float, float, str]:
        return (
            self.url,
            self.host,
            self.scheduler_score,
            self.next_fetch_at,
            self.added_at,
            self.physical_queue,
        )


SchedulerQueueRowInput = SchedulerQueueRow | tuple[str, str, float, float, float, str]


class SchedulerMembershipStore:
    """Owns live scheduler queue membership rows."""

    def __init__(
        self,
        conn,
        *,
        blocked_queue_table: str,
        host_runnable_heads_table: str,
    ):
        self._conn = conn
        self._blocked_queue_table = blocked_queue_table
        self._host_runnable_heads_table = host_runnable_heads_table
        self._host_heads = None

    def attach_host_heads(self, host_heads) -> None:
        self._host_heads = host_heads

    def physical_queues(self) -> list[str]:
        return list(PHYSICAL_QUEUE_ORDER)

    def normalize_physical_queue(self, physical_queue: str | None) -> str:
        if physical_queue in PHYSICAL_QUEUE_NAMES:
            return physical_queue
        return QUEUE_SCHEDULED

    def normalized_physical_queues(self, physical_queues: list[str] | None) -> list[str]:
        if physical_queues:
            allowed = {
                self.normalize_physical_queue(physical_queue)
                for physical_queue in physical_queues
            }
            return [
                physical_queue
                for physical_queue in self.physical_queues()
                if physical_queue in allowed
            ]
        return self.physical_queues()

    def normalized_surface_queues(
        self,
        *,
        scheduler_surface: str | None,
        physical_queues: list[str] | None,
    ) -> list[str]:
        if scheduler_surface is not None and physical_queues is not None:
            raise ValueError("Specify either scheduler_surface or physical_queues, not both")
        if scheduler_surface is None:
            return self.normalized_physical_queues(physical_queues)
        normalized_surface = str(scheduler_surface).strip().lower()
        resolved = SCHEDULER_SURFACE_PHYSICAL_QUEUES.get(normalized_surface)
        if resolved is None:
            raise ValueError(f"Unknown scheduler surface: {scheduler_surface}")
        return list(resolved)

    def single_physical_queue_for_surface(self, scheduler_surface: str) -> str:
        physical_queues = self.normalized_surface_queues(
            scheduler_surface=scheduler_surface,
            physical_queues=None,
        )
        if len(physical_queues) != 1:
            raise ValueError(
                f"Scheduler surface must resolve to one physical queue: {scheduler_surface}"
            )
        return physical_queues[0]

    def queue_table_sql(self, physical_queue: str) -> str:
        return PHYSICAL_QUEUE_TABLES[self.normalize_physical_queue(physical_queue)]

    def _row_tuple(
        self, row: SchedulerQueueRowInput
    ) -> tuple[str, str, float, float, float, str]:
        if isinstance(row, SchedulerQueueRow):
            return row.as_tuple()
        return row

    def _row_tuples(
        self, rows: list[SchedulerQueueRowInput]
    ) -> list[tuple[str, str, float, float, float, str]]:
        return [self._row_tuple(row) for row in rows]

    def row_urls(self, rows: list[SchedulerQueueRowInput]) -> list[str]:
        return [url for url, *_rest in self._row_tuples(rows)]

    def rows_for_physical_queue(
        self,
        rows: list[tuple[str, str, float, float, float]],
        physical_queue: str,
    ) -> list[SchedulerQueueRow]:
        return [
            SchedulerQueueRow(
                url=url,
                host=host,
                scheduler_score=scheduler_score,
                next_fetch_at=next_fetch_at,
                added_at=added_at,
                physical_queue=physical_queue,
            )
            for url, host, scheduler_score, next_fetch_at, added_at in rows
        ]

    def rows_for_ledger_rows(
        self,
        rows: list[tuple[str, str, float, float, float]],
        *,
        physical_queue_by_url: dict[str, str],
        default_physical_queue: str,
    ) -> list[SchedulerQueueRow]:
        projected: list[SchedulerQueueRow] = []
        for url, host, discovery_value, next_fetch_at, added_at in rows:
            physical_queue = physical_queue_by_url.get(url, default_physical_queue)
            projected.append(
                SchedulerQueueRow(
                    url=url,
                    host=host,
                    scheduler_score=discovery_value,
                    next_fetch_at=next_fetch_at,
                    added_at=added_at,
                    physical_queue=self.normalize_physical_queue(physical_queue),
                )
            )
        return projected

    def queue_head_pairs_for_urls(self, cur, urls: list[str]) -> list[tuple[str, str]]:
        normalized_urls = sorted({normalize_url(url) for url in urls if url})
        if not normalized_urls:
            return []

        pairs: list[tuple[str, str]] = []
        for physical_queue in self.physical_queues():
            cur.execute(
                f"""SELECT DISTINCT host
                    FROM {self.queue_table_sql(physical_queue)}
                    WHERE url = ANY(%s)""",
                (normalized_urls,),
            )
            pairs.extend((physical_queue, host) for (host,) in cur.fetchall())
        cur.execute(
            f"""SELECT DISTINCT physical_queue, host
                FROM {self._host_runnable_heads_table}
                WHERE head_url = ANY(%s)""",
            (normalized_urls,),
        )
        pairs.extend(
            (self.normalize_physical_queue(physical_queue), host)
            for physical_queue, host in cur.fetchall()
        )
        return pairs

    def refresh_host_heads_for_pairs(
        self,
        cur,
        pairs: list[tuple[str, str]],
    ) -> None:
        if self._host_heads is None:
            return
        self._host_heads.refresh_hosts_in_tx(cur, pairs)

    def delete_queue_entries(self, cur, urls: list[str]) -> None:
        if not urls:
            return
        affected_pairs = self.queue_head_pairs_for_urls(cur, urls)
        for table_name in QUEUE_TABLES:
            cur.execute(f"DELETE FROM {table_name} WHERE url = ANY(%s)", (urls,))
        cur.execute(f"DELETE FROM {self._blocked_queue_table} WHERE url = ANY(%s)", (urls,))
        cur.execute(
            f"DELETE FROM {self._host_runnable_heads_table} WHERE head_url = ANY(%s)",
            (urls,),
        )
        self.refresh_host_heads_for_pairs(cur, affected_pairs)

    def insert_pending_rows(
        self,
        cur,
        rows: list[SchedulerQueueRowInput],
    ) -> None:
        row_tuples = self._row_tuples(rows)
        grouped: dict[str, list[tuple[str, str, float, float, float, str]]] = {
            physical_queue: [] for physical_queue in PHYSICAL_QUEUE_NAMES
        }
        for url, host, scheduler_score, next_fetch_at, added_at, physical_queue in row_tuples:
            normalized_url = normalize_url(url)
            grouped[self.normalize_physical_queue(physical_queue)].append(
                (
                    normalized_url,
                    host,
                    scheduler_score,
                    next_fetch_at,
                    added_at,
                    url_branch_key(normalized_url),
                )
            )

        for physical_queue, pending_rows in grouped.items():
            if not pending_rows:
                continue
            psycopg2.extras.execute_values(
                cur,
                f"""INSERT INTO {self.queue_table_sql(physical_queue)}
                        (url, host, scheduler_score, next_fetch_at, added_at, branch_key)
                    VALUES %s
                    ON CONFLICT (url) DO UPDATE
                    SET host = EXCLUDED.host,
                        scheduler_score = EXCLUDED.scheduler_score,
                        next_fetch_at = EXCLUDED.next_fetch_at,
                        added_at = EXCLUDED.added_at,
                        branch_key = EXCLUDED.branch_key""",
                pending_rows,
                page_size=200,
            )
        if self._host_heads is not None:
            self._host_heads.upsert_candidates_in_tx(cur, row_tuples)

    def replace_pending_rows(
        self,
        cur,
        rows: list[SchedulerQueueRowInput],
    ) -> None:
        row_tuples = self._row_tuples(rows)
        normalized_urls = sorted({normalize_url(url) for url, *_ in row_tuples if url})
        if not normalized_urls:
            return
        self.delete_queue_entries(cur, normalized_urls)
        self.insert_pending_rows(cur, row_tuples)
