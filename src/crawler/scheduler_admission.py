"""Scheduler admission service."""

from __future__ import annotations

import logging
import time
from typing import Any

from .url_identity import MAX_URL_IDENTITY_BYTES, url_identity_length
from .urls import normalize_url

logger = logging.getLogger(__name__)

ADMISSION_DIAGNOSTIC_FIELDS = (
    "admit_update_intents_ms",
    "admit_fetch_rows_ms",
    "admit_delete_membership_ms",
    "admit_insert_membership_ms",
    "admit_host_heads_ms",
    "admit_delete_leases_ms",
    "admit_commit_ms",
)


def _elapsed_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 1)


class SchedulerAdmissionService:
    """Synchronize admitted durable URL rows into scheduler membership."""

    def __init__(
        self,
        ledger: Any,
        *,
        task_cls: type,
        url_ledger_table: str,
        blocked_host_backoff_table: str,
        lease_table: str,
        host_head_update_dirty: str,
    ) -> None:
        self._ledger = ledger
        self._task_cls = task_cls
        self._url_ledger_table = url_ledger_table
        self._blocked_host_backoff_table = blocked_host_backoff_table
        self._lease_table = lease_table
        self._host_head_update_dirty = host_head_update_dirty

    def admission_physical_queue_by_url(self, tasks: list[Any]) -> dict[str, str]:
        return {
            task.url: self._ledger._physical_queue_for_model(
                runnable_surface=task.runnable_surface,
                intent=task.intent,
            )
            for task in tasks
        }

    def fetch_admission_ledger_rows_for_tasks(
        self,
        cur,
        tasks: list[Any],
    ) -> list[tuple[str, str, float, float, float]]:
        """Load known ledger rows used by scheduler admission."""
        normalized_urls = sorted({task.url for task in tasks if task.url})
        normalized_urls = [
            url for url in normalized_urls if url_identity_length(url) <= MAX_URL_IDENTITY_BYTES
        ]
        if not normalized_urls:
            return []
        cur.execute(
            f"""SELECT url, host, discovery_value, next_fetch_at, added_at
                FROM {self._url_ledger_table}
                WHERE url = ANY(%s)
                  AND terminal_reason IS NULL
                  AND last_success_at IS NULL""",
            (normalized_urls,),
        )
        return list(cur.fetchall())

    def admit_queue_membership(self, tasks: list[Any]) -> int:
        """Assign scheduler membership for known ledger URLs."""
        diagnostics = self._ledger._empty_admission_diagnostics()
        if not tasks:
            self._ledger._last_admission_diagnostics = diagnostics
            return 0

        prepared_tasks = self._ledger._prepare_tasks(tasks)
        try:
            with self._ledger._conn.cursor() as cur:
                started = time.perf_counter()
                self._ledger._update_task_intents(cur, prepared_tasks)
                diagnostics["admit_update_intents_ms"] = _elapsed_ms(started)

                started = time.perf_counter()
                ledger_rows = self.fetch_admission_ledger_rows_for_tasks(cur, prepared_tasks)
                diagnostics["admit_fetch_rows_ms"] = _elapsed_ms(started)
                pending_rows = self._ledger._membership.rows_for_ledger_rows(
                    ledger_rows,
                    physical_queue_by_url=self.admission_physical_queue_by_url(prepared_tasks),
                    default_physical_queue=self._ledger._default_scheduled_physical_queue(),
                )
                membership_timings = self._ledger._membership.replace_pending_rows(
                    cur,
                    pending_rows,
                    host_head_update=self._host_head_update_dirty,
                )
                diagnostics["admit_delete_membership_ms"] = membership_timings.get(
                    "delete_membership_ms",
                    0.0,
                )
                diagnostics["admit_insert_membership_ms"] = membership_timings.get(
                    "insert_membership_ms",
                    0.0,
                )
                diagnostics["admit_host_heads_ms"] = membership_timings.get(
                    "host_heads_ms",
                    0.0,
                )
                started = time.perf_counter()
                self._ledger._leases.delete(cur, self._ledger._membership.row_urls(pending_rows))
                diagnostics["admit_delete_leases_ms"] = _elapsed_ms(started)
                count = len(pending_rows)
        except Exception:
            self._ledger._conn.rollback()
            self._ledger._last_admission_diagnostics = diagnostics
            logger.exception("Failed to admit %d URLs", len(tasks))
            return 0

        started = time.perf_counter()
        self._ledger._conn.commit()
        diagnostics["admit_commit_ms"] = _elapsed_ms(started)
        self._ledger._last_admission_diagnostics = diagnostics
        return count

    def admit_discovered_tasks(self, tasks: list[Any]) -> int:
        """Assign scheduler membership to discovered URLs using task admission metadata."""
        return self.admit_queue_membership(tasks)

    def admit_urls(
        self,
        urls: list[str],
        *,
        runnable_surface: str | None = None,
        intent: str | None = None,
    ) -> int:
        """Assign scheduler membership to known ledger URLs using surface and intent."""
        normalized_urls = sorted({normalize_url(url) for url in urls if url})
        if not normalized_urls:
            return 0

        admission_tasks = [
            self._task_cls(
                url=url,
                runnable_surface=runnable_surface,
                intent=intent,
            )
            for url in normalized_urls
        ]
        return self.admit_queue_membership(admission_tasks)

    def select_admission_candidate_rows(
        self,
        cur,
        *,
        limit: int,
    ) -> list[tuple[str, str, float, float, float]]:
        """Return ledger rows that are known but lack current scheduler membership."""
        queue_joins, queue_absence = self._ledger._queue_membership_join_sql(
            ledger_alias="ledger"
        )
        cur.execute(
            f"""SELECT ledger.url,
                       ledger.host,
                       ledger.discovery_value,
                       ledger.next_fetch_at,
                       ledger.added_at
                FROM {self._url_ledger_table} AS ledger
                {queue_joins}
                LEFT JOIN {self._blocked_host_backoff_table} AS blocked
                    ON blocked.url = ledger.url
                LEFT JOIN {self._lease_table} AS lease
                    ON lease.url = ledger.url
                WHERE {queue_absence}
                  AND blocked.url IS NULL
                  AND lease.url IS NULL
                  AND ledger.last_success_at IS NULL
                  AND ledger.terminal_reason IS NULL
                  AND ledger.url_length <= %s
                ORDER BY ledger.discovery_value DESC,
                         ledger.next_fetch_at ASC,
                         ledger.added_at ASC,
                         ledger.url ASC
                LIMIT %s
                FOR UPDATE OF ledger SKIP LOCKED""",
            (MAX_URL_IDENTITY_BYTES, limit),
        )
        return list(cur.fetchall())

    def admit_discovered_urls(
        self,
        limit: int,
        *,
        runnable_surface: str | None = None,
        intent: str | None = None,
    ) -> int:
        """Assign scheduler membership to discovered ledger rows without task metadata."""
        if limit <= 0:
            return 0

        normalized_physical_queue = self._ledger._physical_queue_for_model(
            runnable_surface=runnable_surface,
            intent=intent,
        )
        resolved_intent = self._ledger._intent_for_model(
            runnable_surface=runnable_surface,
            intent=intent,
        )

        try:
            with self._ledger._conn.cursor() as cur:
                candidate_rows = self.select_admission_candidate_rows(cur, limit=limit)
                if resolved_intent is not None and candidate_rows:
                    cur.execute(
                        f"""UPDATE {self._url_ledger_table}
                            SET current_intent = %s
                            WHERE url = ANY(%s)""",
                        (resolved_intent, [row[0] for row in candidate_rows]),
                    )
                pending_rows = self._ledger._membership.rows_for_physical_queue(
                    candidate_rows,
                    normalized_physical_queue,
                )
                self._ledger._membership.replace_pending_rows(
                    cur,
                    pending_rows,
                    host_head_update=self._host_head_update_dirty,
                )
                count = len(pending_rows)
        except Exception:
            self._ledger._conn.rollback()
            logger.exception("Failed to admit discovered URLs (limit=%d)", limit)
            return 0

        self._ledger._conn.commit()
        return count
