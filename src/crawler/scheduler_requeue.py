"""Scheduler requeue service."""

from __future__ import annotations

from collections import Counter
import time
from urllib.parse import urlparse

import psycopg2.extras

from .host_ledger import HostLedgerStore
from .scheduler_leases import ExecutionLeaseStore
from .scheduler_membership import (
    SCHEDULER_SURFACE_REFRESH,
    SCHEDULER_SURFACE_RUNNABLE,
    SCHEDULER_SURFACE_SCHEDULED,
    SchedulerMembershipStore,
)
from .scheduler_queue_policy import SchedulerQueuePolicy
from .scheduler_retry_policy import SchedulerRetryPolicy
from .scheduler_task import INTENT_EXPLORE, INTENT_REFRESH, INTENT_RETRY
from .url_identity import (
    MAX_URL_IDENTITY_BYTES,
    URL_IDENTITY_VERSION,
    url_identity_hash,
    url_identity_length,
)
from .urls import normalize_url, url_branch_key


class SchedulerRequeueService:
    """Owns scheduler requeue and lease recovery mutations."""

    def __init__(
        self,
        conn,
        *,
        membership: SchedulerMembershipStore,
        leases: ExecutionLeaseStore,
        host_ledger: HostLedgerStore,
        retry_policy: SchedulerRetryPolicy,
        queue_policy: SchedulerQueuePolicy,
        url_ledger_table: str,
        blocked_host_backoff_table: str,
    ) -> None:
        self._conn = conn
        self._membership = membership
        self._leases = leases
        self._host_ledger = host_ledger
        self._retry_policy = retry_policy
        self._queue_policy = queue_policy
        self._url_ledger_table = url_ledger_table
        self._blocked_host_backoff_table = blocked_host_backoff_table

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
                normalized_url,
                host,
                self._membership.normalize_physical_queue(physical_queue),
                scheduler_score,
                next_fetch_at,
                added_at,
                now,
                url_branch_key(normalized_url),
            )
            for url, host, scheduler_score, next_fetch_at, added_at, physical_queue in rows
            for normalized_url in [normalize_url(url)]
            if url_identity_length(normalized_url) <= MAX_URL_IDENTITY_BYTES
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
        with self._conn.cursor() as cur:
            recovered_rows = self._leases.recover_rows(
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
                    self._retry_policy.compute_scheduler_score(discovery_value, fail_streak),
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
            self._membership.replace_pending_rows(cur, pending_rows)
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
        normalized_urls = sorted(
            {
                normalized_url
                for url in urls
                if url
                for normalized_url in [normalize_url(url)]
                if url_identity_length(normalized_url) <= MAX_URL_IDENTITY_BYTES
            }
        )
        if not normalized_urls:
            return 0

        scheduled_at = time.time() if next_fetch_at is None else next_fetch_at
        normalized_physical_queue = self._queue_policy.physical_queue_for_model(
            runnable_surface=runnable_surface,
            intent=intent,
        )
        normalized_intent = self._queue_policy.intent_for_model(
            runnable_surface=runnable_surface,
            intent=intent,
        )

        with self._conn.cursor() as cur:
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
            self._membership.replace_pending_rows(cur, pending_rows)
            self._leases.delete(cur, self._membership.row_urls(pending_rows))
            count = len(pending_rows)

        self._conn.commit()
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
            runnable_surface=SCHEDULER_SURFACE_REFRESH,
            intent=INTENT_REFRESH,
            next_fetch_at=next_fetch_at,
            current_statuses=current_statuses,
        )

    def requeue_failed(self) -> int:
        """Requeue failed URLs for retry."""
        now = time.time()
        with self._conn.cursor() as cur:
            cur.execute(
                f"""UPDATE {self._url_ledger_table}
                   SET next_fetch_at = %s,
                       current_intent = %s,
                       terminal_reason = NULL,
                       terminalized_at = NULL
                   WHERE terminal_reason IS NOT NULL
                   RETURNING url, host, discovery_value, next_fetch_at, added_at""",
                (now, INTENT_RETRY),
            )
            rows = cur.fetchall()
            pending_rows = self._membership.rows_for_physical_queue(
                rows,
                self._queue_policy.single_physical_queue_for_surface(SCHEDULER_SURFACE_SCHEDULED),
            )
            self._membership.replace_pending_rows(cur, pending_rows)
            self._leases.delete(cur, self._membership.row_urls(pending_rows))
            count = len(pending_rows)
        self._conn.commit()
        return count

    def recover_leased(self, expired_only: bool = True) -> int:
        """Reset leased URLs back to pending."""
        count = self.recover_leased_locked(time.time(), expired_only=expired_only)
        self._conn.commit()
        return count

    def delay_overcrowded_scheduled_surface(
        self,
        *,
        keep_runnable_per_host: int,
        keep_runnable_per_branch: int,
        limit: int | None,
        delay_seconds: float,
    ) -> int:
        """Delay excess scheduled work from overrepresented hosts and branches."""
        if keep_runnable_per_host <= 0 or keep_runnable_per_branch <= 0:
            return 0
        if limit is not None and limit <= 0:
            return 0
        now = time.time()
        limit_sql = "" if limit is None else "\n                        LIMIT %s"
        params: tuple[object, ...] = (
            now,
            keep_runnable_per_host,
            keep_runnable_per_branch,
            *((limit,) if limit is not None else ()),
            now + delay_seconds,
        )
        scheduled_queue = self._queue_policy.single_physical_queue_for_surface(
            SCHEDULER_SURFACE_SCHEDULED
        )
        with self._conn.cursor() as cur:
            cur.execute(
                f"""WITH ranked AS (
                        SELECT queue.url,
                            ROW_NUMBER() OVER (
                                PARTITION BY queue.host
                                ORDER BY queue.scheduler_score DESC, queue.next_fetch_at ASC,
                                         queue.added_at ASC, queue.url ASC
                            ) AS host_rownum,
                            ROW_NUMBER() OVER (
                                PARTITION BY queue.host, queue.branch_key
                                ORDER BY queue.scheduler_score DESC, queue.next_fetch_at ASC,
                                         queue.added_at ASC, queue.url ASC
                            ) AS branch_rownum
                        FROM {self._membership.queue_table_sql(scheduled_queue)} AS queue
                        WHERE queue.next_fetch_at <= %s
                    ), scheduled AS (
                        SELECT ranked.url FROM ranked
                        WHERE ranked.host_rownum > %s OR ranked.branch_rownum > %s
                        ORDER BY ranked.host_rownum DESC, ranked.branch_rownum DESC,
                                 ranked.url ASC
                        {limit_sql}
                    )
                    UPDATE {self._url_ledger_table}
                    SET next_fetch_at = GREATEST(next_fetch_at, %s)
                    WHERE url IN (SELECT url FROM scheduled)
                    RETURNING url, host, discovery_value, next_fetch_at, added_at""",
                params,
            )
            pending_rows = self._membership.rows_for_physical_queue(
                cur.fetchall(),
                scheduled_queue,
            )
            self._membership.replace_pending_rows(cur, pending_rows)
        self._conn.commit()
        return len(pending_rows)

    def promote_scheduled_host_heads(
        self,
        target_pending: int,
        *,
        current_runnable: int,
        per_host: int,
        candidate_limit: int,
    ) -> int:
        """Promote one scheduled head per host into the runnable queue."""
        if target_pending <= 0 or per_host <= 0 or candidate_limit <= 0:
            return 0
        needed = target_pending - current_runnable
        if needed <= 0:
            return 0
        runnable_queue = self._queue_policy.single_physical_queue_for_surface(
            SCHEDULER_SURFACE_RUNNABLE
        )
        scheduled_queue = self._queue_policy.single_physical_queue_for_surface(
            SCHEDULER_SURFACE_SCHEDULED
        )
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT host FROM {self._membership.queue_table_sql(runnable_queue)}"
            )
            existing_hosts = {host for (host,) in cur.fetchall()}
            cur.execute(
                f"""SELECT url, host FROM {self._membership.queue_table_sql(scheduled_queue)}
                    ORDER BY scheduler_score DESC, added_at ASC, url ASC
                    LIMIT %s""",
                (max(candidate_limit, needed * 20),),
            )
            promoted_urls: list[str] = []
            host_counts: Counter[str] = Counter()
            for url, host in cur.fetchall():
                if host in existing_hosts or host_counts[host] >= per_host:
                    continue
                promoted_urls.append(normalize_url(url))
                existing_hosts.add(host)
                host_counts[host] += 1
                if len(promoted_urls) >= needed:
                    break
            if not promoted_urls:
                return 0
            cur.execute(
                f"""SELECT url, host, scheduler_score, next_fetch_at, added_at
                    FROM {self._membership.queue_table_sql(scheduled_queue)}
                    WHERE url = ANY(%s)""",
                (promoted_urls,),
            )
            rows = self._membership.rows_for_physical_queue(cur.fetchall(), runnable_queue)
            self._membership.delete_queue_entries(cur, self._membership.row_urls(rows))
            self._membership.insert_pending_rows(cur, rows)
        self._conn.commit()
        return len(rows)

    def upsert_seeds(self, urls: list[str], discovery_value: float = 2.0) -> int:
        """Insert or requeue seed URLs."""
        if not urls:
            return 0

        rows = []
        now = time.time()
        for url in urls:
            normalized = normalize_url(url)
            if url_identity_length(normalized) > MAX_URL_IDENTITY_BYTES:
                continue
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

        if not rows:
            return 0

        with self._conn.cursor() as cur:
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
                template=f"(%s, %s, %s, %s, %s, %s, NULL, %s, %s, '{INTENT_EXPLORE}')",
                page_size=200,
            )
            ledger_rows = cur.fetchall()
            seen_hosts = {row[4] for row in rows if row[4]}
            host_counts = Counter({host: 0 for host in seen_hosts})
            host_counts.update(new_host_counts)
            self._host_ledger.record_discovered_urls_in_tx(
                cur,
                host_counts,
                seen_at=now,
            )
            pending_rows = self._membership.rows_for_physical_queue(
                ledger_rows,
                self._queue_policy.single_physical_queue_for_surface(SCHEDULER_SURFACE_RUNNABLE),
            )
            self._membership.replace_pending_rows(cur, pending_rows)
            self._leases.delete(cur, self._membership.row_urls(pending_rows))
            affected = len(ledger_rows)
        self._conn.commit()
        return affected
