"""Scheduler requeue service."""

from __future__ import annotations

from collections import Counter
import time
from typing import Any
from urllib.parse import urlparse

import psycopg2.extras

from .url_identity import URL_IDENTITY_VERSION, url_identity_hash, url_identity_length
from .urls import normalize_url, url_branch_key


class SchedulerRequeueService:
    """Owns scheduler requeue and lease recovery mutations."""

    def __init__(
        self,
        ledger: Any,
        *,
        url_ledger_table: str,
        blocked_host_backoff_table: str,
        intent_explore: str,
        intent_retry: str,
        intent_refresh: str,
        scheduler_surface_runnable: str,
        scheduler_surface_refresh: str,
        scheduler_surface_scheduled: str,
    ) -> None:
        self._ledger = ledger
        self._url_ledger_table = url_ledger_table
        self._blocked_host_backoff_table = blocked_host_backoff_table
        self._intent_explore = intent_explore
        self._intent_retry = intent_retry
        self._intent_refresh = intent_refresh
        self._scheduler_surface_runnable = scheduler_surface_runnable
        self._scheduler_surface_refresh = scheduler_surface_refresh
        self._scheduler_surface_scheduled = scheduler_surface_scheduled

    def insert_blocked_host_backoff_rows(
        self,
        cur,
        rows: list[tuple[str, str, float, float, float, str]],
        *,
        quarantined_at: float | None = None,
    ) -> None:
        """Insert URLs into the blocked-host-backoff physical queue."""
        now = time.time() if quarantined_at is None else quarantined_at
        blocked_rows = [
            (
                normalize_url(url),
                host,
                self._ledger._normalize_physical_queue(physical_queue),
                scheduler_score,
                next_fetch_at,
                added_at,
                now,
                url_branch_key(normalize_url(url)),
            )
            for url, host, scheduler_score, next_fetch_at, added_at, physical_queue in rows
        ]
        if not blocked_rows:
            return
        psycopg2.extras.execute_values(
            cur,
            f"""INSERT INTO {self._blocked_host_backoff_table}
                    (url, host, physical_queue, scheduler_score, next_fetch_at, added_at, quarantined_at, branch_key)
                VALUES %s
                ON CONFLICT (url) DO UPDATE
                SET host = EXCLUDED.host,
                    physical_queue = EXCLUDED.physical_queue,
                    scheduler_score = EXCLUDED.scheduler_score,
                    next_fetch_at = EXCLUDED.next_fetch_at,
                    added_at = EXCLUDED.added_at,
                    quarantined_at = EXCLUDED.quarantined_at,
                    branch_key = EXCLUDED.branch_key""",
            blocked_rows,
            page_size=200,
        )

    def recover_leased_locked(self, now: float, expired_only: bool) -> int:
        """Reset leased URLs back to pending inside an open transaction."""
        with self._ledger._conn.cursor() as cur:
            recovered_rows = self._ledger._leases.recover_rows(
                cur,
                now=now,
                expired_only=expired_only,
            )
            if not recovered_rows:
                return 0
            psycopg2.extras.execute_values(
                cur,
                f"""SELECT ledger.url,
                           ledger.host,
                           ledger.discovery_value,
                           ledger.fail_streak,
                           ledger.next_fetch_at,
                           ledger.added_at,
                           recovered.physical_queue
                    FROM {self._url_ledger_table} AS ledger
                    JOIN (VALUES %s) AS recovered(url, host, physical_queue)
                      ON recovered.url = ledger.url""",
                recovered_rows,
            )
            rows = cur.fetchall()
            pending_rows = [
                (
                    url,
                    host,
                    self._ledger._compute_retry_scheduler_score(discovery_value, fail_streak),
                    next_fetch_at,
                    added_at,
                    physical_queue,
                )
                for (
                    url,
                    host,
                    discovery_value,
                    fail_streak,
                    next_fetch_at,
                    added_at,
                    physical_queue,
                ) in rows
            ]
            self._ledger._membership.replace_pending_rows(cur, pending_rows)
            return len(rows)

    def requeue_urls(
        self,
        urls: list[str],
        *,
        runnable_surface: str | None = None,
        intent: str | None = None,
        next_fetch_at: float | None = None,
        current_statuses: list[str] | None = None,
    ) -> int:
        """Move known URLs back into a pending physical queue and synchronize scheduler state."""
        normalized_urls = sorted({normalize_url(url) for url in urls if url})
        if not normalized_urls:
            return 0

        scheduled_at = time.time() if next_fetch_at is None else next_fetch_at
        normalized_physical_queue = self._ledger._physical_queue_for_model(
            runnable_surface=runnable_surface,
            intent=intent,
        )
        normalized_intent = self._ledger._intent_for_model(
            runnable_surface=runnable_surface,
            intent=intent,
        )

        with self._ledger._conn.cursor() as cur:
            if current_statuses is None:
                cur.execute(
                    f"""UPDATE {self._url_ledger_table} AS ledger
                        SET next_fetch_at = %s,
                            current_intent = %s,
                            terminal_reason = NULL,
                            terminalized_at = NULL
                        WHERE url = ANY(%s)
                        RETURNING url, host, discovery_value, next_fetch_at, added_at""",
                    (scheduled_at, normalized_intent, normalized_urls),
                )
            else:
                cur.execute(
                    f"""UPDATE {self._url_ledger_table} AS ledger
                        SET next_fetch_at = %s,
                            current_intent = %s,
                            terminal_reason = NULL,
                            terminalized_at = NULL
                        WHERE url = ANY(%s)
                        RETURNING url, host, discovery_value, next_fetch_at, added_at""",
                    (scheduled_at, normalized_intent, normalized_urls),
                )
            rows = cur.fetchall()
            pending_rows = [
                (url, host, discovery_value, next_fetch_at, added_at, normalized_physical_queue)
                for url, host, discovery_value, next_fetch_at, added_at in rows
            ]
            self._ledger._membership.replace_pending_rows(cur, pending_rows)
            self._ledger._leases.delete(cur, self._ledger._membership.row_urls(pending_rows))
            count = len(pending_rows)

        self._ledger._conn.commit()
        return count

    def requeue_refresh_urls(
        self,
        urls: list[str],
        *,
        next_fetch_at: float | None = None,
        current_statuses: list[str] | None = None,
    ) -> int:
        """Requeue known URLs for refresh intent on the refresh runnable surface."""
        return self.requeue_urls(
            urls,
            runnable_surface=self._scheduler_surface_refresh,
            intent=self._intent_refresh,
            next_fetch_at=next_fetch_at,
            current_statuses=current_statuses,
        )

    def requeue_failed(self) -> int:
        """Requeue failed URLs for retry."""
        now = time.time()
        with self._ledger._conn.cursor() as cur:
            cur.execute(
                f"""UPDATE {self._url_ledger_table}
                   SET next_fetch_at = %s,
                       current_intent = %s,
                       terminal_reason = NULL,
                       terminalized_at = NULL
                   WHERE terminal_reason IS NOT NULL
                   RETURNING url, host, discovery_value, next_fetch_at, added_at""",
                (now, self._intent_retry),
            )
            rows = cur.fetchall()
            pending_rows = self._ledger._pending_rows_for_physical_queue(
                rows,
                self._ledger._single_physical_queue_for_surface(
                    self._scheduler_surface_scheduled
                ),
            )
            self._ledger._membership.replace_pending_rows(cur, pending_rows)
            self._ledger._leases.delete(cur, self._ledger._membership.row_urls(pending_rows))
            count = len(pending_rows)
        self._ledger._conn.commit()
        return count

    def recover_leased(self, expired_only: bool = True) -> int:
        """Reset leased URLs back to pending."""
        count = self.recover_leased_locked(time.time(), expired_only=expired_only)
        self._ledger._conn.commit()
        return count

    def upsert_seeds(self, urls: list[str], discovery_value: float = 2.0) -> int:
        """Insert or requeue seed URLs."""
        if not urls:
            return 0

        rows = []
        now = time.time()
        for url in urls:
            normalized = normalize_url(url)
            host = urlparse(normalized).netloc
            rows.append(
                (
                    normalized,
                    url_identity_hash(normalized),
                    url_identity_length(normalized),
                    URL_IDENTITY_VERSION,
                    host,
                    discovery_value,
                    now,
                    now,
                )
            )

        with self._ledger._conn.cursor() as cur:
            normalized_urls = [row[0] for row in rows]
            cur.execute(
                f"SELECT url FROM {self._url_ledger_table} WHERE url = ANY(%s)",
                (normalized_urls,),
            )
            existing_urls = {url for (url,) in cur.fetchall()}
            new_host_counts = Counter(row[4] for row in rows if row[0] not in existing_urls)
            psycopg2.extras.execute_values(
                cur,
                f"""INSERT INTO {self._url_ledger_table} (
                       url, url_hash, url_length, url_identity_version, host,
                       discovery_value, source_url, added_at, next_fetch_at, current_intent
                   )
                   VALUES %s
                   ON CONFLICT (url) DO UPDATE SET
                       url_hash = EXCLUDED.url_hash,
                       url_length = EXCLUDED.url_length,
                       url_identity_version = EXCLUDED.url_identity_version,
                       added_at = EXCLUDED.added_at,
                       next_fetch_at = EXCLUDED.next_fetch_at,
                       current_intent = EXCLUDED.current_intent,
                       discovery_value = EXCLUDED.discovery_value,
                       fail_streak = 0,
                       last_error = NULL,
                       terminal_reason = NULL,
                       terminalized_at = NULL
                   RETURNING url, host, discovery_value, next_fetch_at, added_at""",
                rows,
                template=f"(%s, %s, %s, %s, %s, %s, NULL, %s, %s, '{self._intent_explore}')",
                page_size=200,
            )
            ledger_rows = cur.fetchall()
            seen_hosts = {row[4] for row in rows if row[4]}
            host_counts = Counter({host: 0 for host in seen_hosts})
            host_counts.update(new_host_counts)
            self._ledger._host_ledger.record_discovered_urls_in_tx(
                cur,
                host_counts,
                seen_at=now,
            )
            pending_rows = self._ledger._pending_rows_for_physical_queue(
                ledger_rows,
                self._ledger._single_physical_queue_for_surface(
                    self._scheduler_surface_runnable
                ),
            )
            self._ledger._membership.replace_pending_rows(cur, pending_rows)
            self._ledger._leases.delete(cur, self._ledger._membership.row_urls(pending_rows))
            affected = len(ledger_rows)
        self._ledger._conn.commit()
        return affected
