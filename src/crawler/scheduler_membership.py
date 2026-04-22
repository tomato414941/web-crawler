"""Scheduler membership storage and surface naming."""

from __future__ import annotations

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

    def delete_queue_entries(self, cur, urls: list[str]) -> None:
        if not urls:
            return
        for table_name in QUEUE_TABLES:
            cur.execute(f"DELETE FROM {table_name} WHERE url = ANY(%s)", (urls,))
        cur.execute(f"DELETE FROM {self._blocked_queue_table} WHERE url = ANY(%s)", (urls,))
        cur.execute(
            f"DELETE FROM {self._host_runnable_heads_table} WHERE head_url = ANY(%s)",
            (urls,),
        )

    def insert_pending_rows(
        self,
        cur,
        rows: list[tuple[str, str, float, float, float, str]],
    ) -> None:
        grouped: dict[str, list[tuple[str, str, float, float, float, str]]] = {
            physical_queue: [] for physical_queue in PHYSICAL_QUEUE_NAMES
        }
        for url, host, scheduler_score, next_fetch_at, added_at, physical_queue in rows:
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

    def replace_pending_rows(
        self,
        cur,
        rows: list[tuple[str, str, float, float, float, str]],
    ) -> None:
        normalized_urls = sorted({normalize_url(url) for url, *_ in rows if url})
        if not normalized_urls:
            return
        self.delete_queue_entries(cur, normalized_urls)
        self.insert_pending_rows(cur, rows)
