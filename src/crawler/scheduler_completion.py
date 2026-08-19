"""Scheduler completion transitions."""

from __future__ import annotations

import time

import psycopg2.extras

from .host_ledger import HostLedgerStore
from .scheduler_leases import ACTIVE_LEASES_TABLE, ExecutionLeaseStore
from .scheduler_membership import SCHEDULER_SURFACE_SCHEDULED, SchedulerMembershipStore
from .scheduler_queue_policy import SchedulerQueuePolicy
from .scheduler_retry_policy import SchedulerRetryPolicy
from .scheduler_task import CrawlTask
from .url_ledger_store import URL_LEDGER_TABLE
from .urls import normalize_url


class SchedulerCompletionService:
    """Apply successful and failed crawl outcomes atomically."""

    def __init__(
        self,
        conn,
        *,
        membership: SchedulerMembershipStore,
        leases: ExecutionLeaseStore,
        host_ledger: HostLedgerStore,
        retry_policy: SchedulerRetryPolicy,
        queue_policy: SchedulerQueuePolicy,
    ) -> None:
        self._conn = conn
        self._membership = membership
        self._leases = leases
        self._host_ledger = host_ledger
        self._retry_policy = retry_policy
        self._queue_policy = queue_policy

    def mark_done(self, url: str, lease_token: str | None = None) -> bool:
        normalized = normalize_url(url)
        now = time.time()
        lease_sql, lease_params = self._leases.match_sql("ledger", lease_token)
        with self._conn.cursor() as cur:
            cur.execute(
                f"""UPDATE {URL_LEDGER_TABLE} AS ledger
                    SET next_fetch_at = %s,
                        current_intent = NULL,
                        last_success_at = %s,
                        fail_streak = 0,
                        last_error = NULL,
                        terminal_reason = NULL,
                        terminalized_at = NULL
                    WHERE url = %s{lease_sql}
                    RETURNING url, host""",
                (now, now, normalized, *lease_params),
            )
            rows = cur.fetchall()
            urls = [row[0] for row in rows]
            self._membership.delete_queue_entries(cur, urls)
            self._leases.delete(cur, urls)
            for _url, host in rows:
                self._host_ledger.record_success_in_tx(cur, host, at=now)
        self._conn.commit()
        return bool(rows)

    def mark_done_many(self, tasks: list[CrawlTask]) -> int:
        rows_by_url = {
            normalize_url(task.url): (normalize_url(task.url), task.lease_token)
            for task in tasks
            if task.url
        }
        now = time.time()
        rows = [
            (normalized, lease_token, now, now) for normalized, lease_token in rows_by_url.values()
        ]
        if not rows:
            return 0
        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"""WITH incoming(url, lease_token, next_fetch_at, last_success_at) AS (VALUES %s),
                    updated AS (
                        UPDATE {URL_LEDGER_TABLE} AS ledger
                        SET next_fetch_at = incoming.next_fetch_at,
                            current_intent = NULL,
                            last_success_at = incoming.last_success_at,
                            fail_streak = 0,
                            last_error = NULL,
                            terminal_reason = NULL,
                            terminalized_at = NULL
                        FROM incoming
                        WHERE ledger.url = incoming.url
                          AND (
                              incoming.lease_token IS NULL
                              OR EXISTS (
                                  SELECT 1 FROM {ACTIVE_LEASES_TABLE} AS active
                                  WHERE active.url = ledger.url
                                    AND active.lease_token = incoming.lease_token
                              )
                          )
                        RETURNING ledger.url, ledger.host
                    ) SELECT url, host FROM updated""",
                rows,
                template="(%s, %s, %s, %s)",
                page_size=200,
            )
            updated_rows = list(cur.fetchall())
            updated_urls = [row[0] for row in updated_rows]
            self._membership.delete_queue_entries(cur, updated_urls)
            self._leases.delete(cur, updated_urls)
            for _url, host in updated_rows:
                self._host_ledger.record_success_in_tx(cur, host, at=now)
        self._conn.commit()
        return len(updated_rows)

    def mark_failed(
        self,
        url: str,
        *,
        retryable: bool = False,
        error: str | None = None,
        backoff_seconds: float | None = None,
        lease_token: str | None = None,
    ) -> bool:
        normalized = normalize_url(url)
        now = time.time()
        lease_sql, lease_params = self._leases.match_sql("ledger", lease_token)
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT fail_streak, discovery_value, host FROM {URL_LEDGER_TABLE} "
                f"AS ledger WHERE url = %s{lease_sql} FOR UPDATE",
                (normalized, *lease_params),
            )
            row = cur.fetchone()
            if row is None:
                self._conn.rollback()
                return False
            transition = self._retry_policy.failure_transition(
                fail_streak=row[0],
                discovery_value=row[1],
                retryable=retryable,
                error=error,
                backoff_seconds=backoff_seconds,
                now=now,
            )
            if transition.retryable:
                cur.execute(
                    f"""UPDATE {URL_LEDGER_TABLE} AS ledger
                        SET next_fetch_at = %s,
                            current_intent = %s,
                            fail_streak = %s,
                            last_error = %s,
                            terminal_reason = NULL,
                            terminalized_at = NULL
                        WHERE url = %s{lease_sql}
                        RETURNING url, host, %s::real AS scheduler_score,
                                  next_fetch_at, added_at""",
                    (
                        transition.next_fetch_at,
                        transition.current_intent,
                        transition.next_fail_streak,
                        transition.last_error,
                        normalized,
                        *lease_params,
                        transition.next_scheduler_score,
                    ),
                )
                rows = cur.fetchall()
                for _url, host, *_rest in rows:
                    self._host_ledger.record_failure_in_tx(cur, host, at=now)
                physical_queue = self._queue_policy.single_physical_queue_for_surface(
                    SCHEDULER_SURFACE_SCHEDULED
                )
                pending_rows = self._membership.rows_for_physical_queue(rows, physical_queue)
                self._membership.replace_pending_rows(cur, pending_rows)
                self._leases.delete(cur, self._membership.row_urls(pending_rows))
            else:
                cur.execute(
                    f"""UPDATE {URL_LEDGER_TABLE} AS ledger
                        SET next_fetch_at = %s,
                            current_intent = NULL,
                            fail_streak = %s,
                            last_error = %s,
                            terminal_reason = %s,
                            terminalized_at = %s
                        WHERE url = %s{lease_sql}
                        RETURNING url, host""",
                    (
                        transition.next_fetch_at,
                        transition.next_fail_streak,
                        transition.last_error,
                        transition.terminal_reason,
                        transition.terminalized_at,
                        normalized,
                        *lease_params,
                    ),
                )
                rows = cur.fetchall()
                urls = [row[0] for row in rows]
                self._membership.delete_queue_entries(cur, urls)
                self._leases.delete(cur, urls)
                for _url, host in rows:
                    self._host_ledger.record_failure_in_tx(cur, host, at=now)
        self._conn.commit()
        return bool(rows)
