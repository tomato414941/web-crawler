"""URL Frontier with PostgreSQL persistence."""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import psycopg2.extras

from .discovery import DISCOVERY_SEED, discovery_rank, rank_discovered_url, rank_seed_url, seed_hosts_from_urls
from .urls import normalize_url

if TYPE_CHECKING:
    from .domain_store import DomainStore

logger = logging.getLogger(__name__)

PENDING_STATUS = "pending"
LEASED_STATUS = "leased"
DONE_STATUS = "done"
FAILED_STATUS = "failed"

DEFAULT_LEASE_SECONDS = 300.0
DEFAULT_RETRY_BACKOFF_SECONDS = 30.0
MAX_RETRY_BACKOFF_SECONDS = 1800.0

FRONTIER_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS frontier (
    url TEXT PRIMARY KEY,
    domain TEXT NOT NULL,
    depth INTEGER NOT NULL,
    priority REAL NOT NULL DEFAULT 1.0,
    discovery_kind TEXT NOT NULL DEFAULT 'seed',
    source_url TEXT,
    added_at DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    next_fetch_at DOUBLE PRECISION NOT NULL DEFAULT 0,
    last_success_at DOUBLE PRECISION,
    fail_streak INTEGER NOT NULL DEFAULT 0,
    lease_token TEXT,
    lease_expires_at DOUBLE PRECISION,
    last_error TEXT
);

ALTER TABLE frontier ADD COLUMN IF NOT EXISTS next_fetch_at DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE frontier ADD COLUMN IF NOT EXISTS last_success_at DOUBLE PRECISION;
ALTER TABLE frontier ADD COLUMN IF NOT EXISTS fail_streak INTEGER NOT NULL DEFAULT 0;
ALTER TABLE frontier ADD COLUMN IF NOT EXISTS lease_token TEXT;
ALTER TABLE frontier ADD COLUMN IF NOT EXISTS lease_expires_at DOUBLE PRECISION;
ALTER TABLE frontier ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE frontier ADD COLUMN IF NOT EXISTS discovery_kind TEXT NOT NULL DEFAULT 'seed';

CREATE INDEX IF NOT EXISTS idx_frontier_status ON frontier(status);
CREATE INDEX IF NOT EXISTS idx_frontier_domain ON frontier(domain);
CREATE INDEX IF NOT EXISTS idx_frontier_pending
    ON frontier(priority DESC, next_fetch_at ASC, added_at ASC) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_frontier_pending_domain
    ON frontier(domain) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_frontier_leased_expiry
    ON frontier(lease_expires_at) WHERE status = 'leased';
"""


@dataclass
class CrawlTask:
    """A URL to crawl with metadata."""

    url: str
    depth: int
    priority: float = 1.0
    discovery_kind: str = DISCOVERY_SEED
    source_url: str | None = None
    added_at: float = 0.0
    next_fetch_at: float = 0.0
    lease_token: str | None = None
    lease_expires_at: float | None = None

    def __post_init__(self):
        if self.added_at == 0.0:
            self.added_at = time.time()
        if self.next_fetch_at == 0.0:
            self.next_fetch_at = self.added_at


class Frontier:
    """URL frontier with PostgreSQL persistence. Dedup via ON CONFLICT."""

    def __init__(
        self,
        conn,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
        max_retry_backoff_seconds: float = MAX_RETRY_BACKOFF_SECONDS,
    ):
        self._conn = conn
        self._lease_seconds = lease_seconds
        self._retry_backoff_seconds = retry_backoff_seconds
        self._max_retry_backoff_seconds = max_retry_backoff_seconds
        self._domain_store: DomainStore | None = None
        self._init_schema()

    def attach_domain_store(self, domain_store: "DomainStore | None") -> None:
        """Attach the persistent host scheduler used for lease selection."""
        self._domain_store = domain_store

    def _compute_retry_backoff(self, fail_streak: int) -> float:
        """Compute exponential retry backoff for a failed URL."""
        base = max(self._retry_backoff_seconds, 0.0)
        if fail_streak <= 1:
            return base
        delay = base * (2 ** (fail_streak - 1))
        return min(delay, self._max_retry_backoff_seconds)

    def _lease_match_sql(self, lease_token: str | None) -> tuple[str, tuple]:
        """Build an optional lease-token predicate for completion updates."""
        if lease_token is None:
            return "", ()
        return " AND lease_token = %s", (lease_token,)

    def _build_ready_where(
        self,
        *,
        alias: str,
        now: float,
        domain: str | None = None,
    ) -> tuple[str, list[object]]:
        """Build the ready-candidate filter for lease selection."""
        conditions = [
            f"{alias}.status = '{PENDING_STATUS}'",
            f"{alias}.next_fetch_at <= %s",
        ]
        params: list[object] = [now]

        if self._domain_store is not None:
            conditions.extend(
                [
                    f"""COALESCE((
                            SELECT ds.next_request_at
                            FROM domain_state AS ds
                            WHERE ds.host_key = {alias}.domain
                        ), 0) <= %s""",
                    f"""COALESCE((
                            SELECT ds.backoff_until
                            FROM domain_state AS ds
                            WHERE ds.host_key = {alias}.domain
                        ), 0) <= %s""",
                ]
            )
            params.extend([now, now])

        if domain:
            conditions.append(f"{alias}.domain = %s")
            params.append(domain)

        return " AND ".join(conditions), params

    def _recover_leased_locked(self, now: float, expired_only: bool) -> int:
        """Reset leased URLs back to pending inside an open transaction."""
        if expired_only:
            where = (
                f"status = '{LEASED_STATUS}' AND "
                "(lease_expires_at IS NULL OR lease_expires_at <= %s)"
            )
            params = (now,)
        else:
            where = f"status = '{LEASED_STATUS}'"
            params = ()

        with self._conn.cursor() as cur:
            cur.execute(
                f"""UPDATE frontier
                    SET status = '{PENDING_STATUS}',
                        lease_token = NULL,
                        lease_expires_at = NULL
                    WHERE {where}""",
                params,
            )
            return cur.rowcount

    def _init_schema(self):
        with self._conn.cursor() as cur:
            cur.execute(FRONTIER_SCHEMA_SQL)
            cur.execute(
                f"""UPDATE frontier
                    SET status = '{LEASED_STATUS}'
                    WHERE status = 'processing'"""
            )
        self._conn.commit()
        logger.info("Frontier schema initialized")

    def _is_better_task(self, candidate: CrawlTask, current: CrawlTask) -> bool:
        """Return True when candidate should replace current task metadata."""
        if candidate.priority != current.priority:
            return candidate.priority > current.priority
        return discovery_rank(candidate.discovery_kind) > discovery_rank(current.discovery_kind)

    def _merge_task(self, current: CrawlTask, candidate: CrawlTask) -> CrawlTask:
        """Merge duplicate task metadata before bulk upsert."""
        preferred = candidate if self._is_better_task(candidate, current) else current
        return CrawlTask(
            url=preferred.url,
            depth=min(current.depth, candidate.depth),
            priority=preferred.priority,
            discovery_kind=preferred.discovery_kind,
            source_url=preferred.source_url or current.source_url or candidate.source_url,
            added_at=min(current.added_at, candidate.added_at),
            next_fetch_at=min(current.next_fetch_at, candidate.next_fetch_at),
        )

    def _prepare_tasks(self, tasks: list[CrawlTask]) -> list[CrawlTask]:
        """Normalize and deduplicate tasks before writing to Postgres."""
        merged: dict[str, CrawlTask] = {}
        for task in tasks:
            normalized_url = normalize_url(task.url)
            normalized = CrawlTask(
                url=normalized_url,
                depth=task.depth,
                priority=task.priority,
                discovery_kind=task.discovery_kind,
                source_url=task.source_url,
                added_at=task.added_at,
                next_fetch_at=task.next_fetch_at,
            )
            existing = merged.get(normalized.url)
            if existing is None:
                merged[normalized.url] = normalized
            else:
                merged[normalized.url] = self._merge_task(existing, normalized)
        return list(merged.values())

    def _discovery_rank_sql(self, column: str) -> str:
        """Return SQL that maps discovery kind to a comparable rank."""
        return (
            f"CASE {column} "
            f"WHEN 'external' THEN 1 "
            f"WHEN 'seed_host' THEN 2 "
            f"WHEN 'same_host' THEN 3 "
            f"WHEN 'seed' THEN 4 "
            f"ELSE 0 END"
        )

    def _host_pressure_sql(self, alias: str) -> str:
        """Return SQL that estimates how congested a host is in the pending queue."""
        return (
            "COALESCE(("
            "SELECT COUNT(*) "
            "FROM frontier AS pressure "
            f"WHERE pressure.status = '{PENDING_STATUS}' "
            f"AND pressure.domain = {alias}.domain"
            "), 0)"
        )

    def _upsert_tasks(self, tasks: list[CrawlTask]) -> int:
        """Insert new tasks and promote existing metadata when a better discovery wins."""
        if not tasks:
            return 0

        rows = []
        for task in self._prepare_tasks(tasks):
            domain = urlparse(task.url).netloc
            next_fetch_at = task.next_fetch_at or task.added_at or time.time()
            rows.append(
                (
                    task.url,
                    domain,
                    task.depth,
                    task.priority,
                    task.discovery_kind,
                    task.source_url,
                    task.added_at,
                    next_fetch_at,
                )
            )

        existing_rank = self._discovery_rank_sql("frontier.discovery_kind")
        new_rank = self._discovery_rank_sql("EXCLUDED.discovery_kind")
        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    f"""INSERT INTO frontier (
                           url, domain, depth, priority, discovery_kind, source_url, added_at, next_fetch_at
                       )
                       VALUES %s
                       ON CONFLICT (url) DO UPDATE SET
                           priority = GREATEST(frontier.priority, EXCLUDED.priority),
                           discovery_kind = CASE
                               WHEN {new_rank} > {existing_rank}
                                   THEN EXCLUDED.discovery_kind
                               ELSE frontier.discovery_kind
                           END,
                           source_url = COALESCE(frontier.source_url, EXCLUDED.source_url),
                           depth = LEAST(frontier.depth, EXCLUDED.depth),
                           added_at = LEAST(frontier.added_at, EXCLUDED.added_at),
                           next_fetch_at = LEAST(frontier.next_fetch_at, EXCLUDED.next_fetch_at)
                       WHERE
                           EXCLUDED.priority > frontier.priority
                           OR {new_rank} > {existing_rank}
                           OR EXCLUDED.depth < frontier.depth
                           OR frontier.source_url IS NULL
                           OR EXCLUDED.next_fetch_at < frontier.next_fetch_at""",
                    rows,
                    page_size=200,
                )
                return cur.rowcount
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to upsert batch of %d URLs", len(tasks))
            return 0

    def rerank_discovered(self, seed_urls: list[str]) -> int:
        """Recompute backlog priority and discovery metadata using current ranking rules."""
        seed_hosts = seed_hosts_from_urls(seed_urls)
        updates: list[tuple[float, str, str]] = []
        with self._conn.cursor() as cur:
            cur.execute("SELECT url, source_url, priority, discovery_kind FROM frontier")
            rows = cur.fetchall()

        for url, source_url, priority, discovery_kind in rows:
            if source_url:
                decision = rank_discovered_url(
                    parent_url=source_url,
                    url=url,
                    seed_hosts=seed_hosts,
                )
            else:
                decision = rank_seed_url(url)

            if priority != decision.priority or discovery_kind != decision.discovery_kind:
                updates.append((decision.priority, decision.discovery_kind, url))

        if not updates:
            return 0

        with self._conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """UPDATE frontier
                   SET priority = %s,
                       discovery_kind = %s
                   WHERE url = %s""",
                updates,
                page_size=200,
            )
        self._conn.commit()
        return len(updates)

    def add(self, task: CrawlTask) -> bool:
        """Add a URL to the frontier. Returns True if inserted or metadata improved."""
        return self._upsert_tasks([task]) > 0

    def add_many(self, tasks: list[CrawlTask]) -> int:
        """Add multiple URLs. Existing rows are promoted when a better discovery wins."""
        return self._upsert_tasks(tasks)

    def lease_next(
        self,
        domain: str | None = None,
        lease_seconds: float | None = None,
    ) -> CrawlTask | None:
        """Lease the next ready URL, optionally filtered by domain."""
        now = time.time()
        lease_token = uuid.uuid4().hex
        duration = self._lease_seconds if lease_seconds is None else lease_seconds
        lease_expires_at = now + duration
        where, where_params = self._build_ready_where(alias="candidate", now=now, domain=domain)
        host_pressure = self._host_pressure_sql("candidate")
        params: list[object] = [lease_token, lease_expires_at, *where_params]

        try:
            self._recover_leased_locked(now, expired_only=True)
            with self._conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE frontier
                        SET status = '{LEASED_STATUS}',
                            lease_token = %s,
                            lease_expires_at = %s
                        WHERE url = (
                            SELECT candidate.url
                            FROM frontier AS candidate
                            WHERE {where}
                            ORDER BY
                                candidate.priority DESC,
                                {host_pressure} ASC,
                                candidate.next_fetch_at ASC,
                                candidate.added_at ASC
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                        )
                        RETURNING
                            url,
                            depth,
                            priority,
                            discovery_kind,
                            source_url,
                            added_at,
                            next_fetch_at,
                            lease_token,
                            lease_expires_at""",
                    params,
                )
                row = cur.fetchone()
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to lease next URL")
            return None

        if row:
            (
                url,
                depth,
                priority,
                discovery_kind,
                source_url,
                added_at,
                next_fetch_at,
                lease_token,
                lease_expires_at,
            ) = row
            return CrawlTask(
                url=url, depth=depth, priority=priority,
                discovery_kind=discovery_kind,
                source_url=source_url, added_at=added_at,
                next_fetch_at=next_fetch_at,
                lease_token=lease_token, lease_expires_at=lease_expires_at,
            )
        return None

    def lease_batch(
        self,
        count: int = 10,
        domain: str | None = None,
        lease_seconds: float | None = None,
    ) -> list[CrawlTask]:
        """Lease a batch of ready URLs."""
        now = time.time()
        lease_token = uuid.uuid4().hex
        duration = self._lease_seconds if lease_seconds is None else lease_seconds
        lease_expires_at = now + duration
        where, where_params = self._build_ready_where(alias="candidate", now=now, domain=domain)
        host_pressure = self._host_pressure_sql("candidate")
        params: list[object] = [lease_token, lease_expires_at, *where_params, count]

        try:
            self._recover_leased_locked(now, expired_only=True)
            with self._conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE frontier
                        SET status = '{LEASED_STATUS}',
                            lease_token = %s,
                            lease_expires_at = %s
                        WHERE url IN (
                            SELECT candidate.url
                            FROM frontier AS candidate
                            WHERE {where}
                            ORDER BY
                                candidate.priority DESC,
                                {host_pressure} ASC,
                                candidate.next_fetch_at ASC,
                                candidate.added_at ASC
                            LIMIT %s
                            FOR UPDATE SKIP LOCKED
                        )
                        RETURNING
                            url,
                            depth,
                            priority,
                            discovery_kind,
                            source_url,
                            added_at,
                            next_fetch_at,
                            lease_token,
                            lease_expires_at""",
                    params,
                )
                rows = cur.fetchall()
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to lease batch of URLs")
            return []

        return [
            CrawlTask(
                url=url,
                depth=depth,
                priority=priority,
                discovery_kind=discovery_kind,
                source_url=source_url,
                added_at=added_at,
                next_fetch_at=next_fetch_at,
                lease_token=row_lease_token,
                lease_expires_at=row_lease_expires_at,
            )
            for (
                url,
                depth,
                priority,
                discovery_kind,
                source_url,
                added_at,
                next_fetch_at,
                row_lease_token,
                row_lease_expires_at,
            ) in rows
        ]

    def mark_done(self, url: str, lease_token: str | None = None) -> bool:
        """Mark a URL as successfully crawled."""
        normalized = normalize_url(url)
        now = time.time()
        lease_sql, lease_params = self._lease_match_sql(lease_token)

        with self._conn.cursor() as cur:
            cur.execute(
                f"""UPDATE frontier
                    SET status = '{DONE_STATUS}',
                        next_fetch_at = %s,
                        last_success_at = %s,
                        fail_streak = 0,
                        lease_token = NULL,
                        lease_expires_at = NULL,
                        last_error = NULL
                    WHERE url = %s{lease_sql}""",
                (now, now, normalized, *lease_params),
            )
            updated = cur.rowcount > 0
        self._conn.commit()
        return updated

    def mark_failed(
        self,
        url: str,
        retryable: bool = False,
        error: str | None = None,
        backoff_seconds: float | None = None,
        lease_token: str | None = None,
    ) -> bool:
        """Mark a URL as failed, optionally scheduling a retry."""
        normalized = normalize_url(url)
        now = time.time()
        lease_sql, lease_params = self._lease_match_sql(lease_token)

        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT fail_streak FROM frontier WHERE url = %s{lease_sql} FOR UPDATE",
                (normalized, *lease_params),
            )
            row = cur.fetchone()
            if row is None:
                self._conn.rollback()
                return False

            next_fail_streak = row[0] + 1
            retry_delay = backoff_seconds
            if retryable and retry_delay is None:
                retry_delay = self._compute_retry_backoff(next_fail_streak)

            status = PENDING_STATUS if retryable else FAILED_STATUS
            next_fetch_at = now + (retry_delay or 0.0) if retryable else now
            cur.execute(
                f"""UPDATE frontier
                    SET status = %s,
                        next_fetch_at = %s,
                        fail_streak = %s,
                        last_error = %s,
                        lease_token = NULL,
                        lease_expires_at = NULL
                    WHERE url = %s{lease_sql}""",
                (
                    status,
                    next_fetch_at,
                    next_fail_streak,
                    error,
                    normalized,
                    *lease_params,
                ),
            )
            updated = cur.rowcount > 0
        self._conn.commit()
        return updated

    def requeue_failed(self) -> int:
        """Requeue failed URLs for retry."""
        now = time.time()
        with self._conn.cursor() as cur:
            cur.execute(
                """UPDATE frontier
                   SET status = %s,
                       next_fetch_at = %s,
                       lease_token = NULL,
                       lease_expires_at = NULL
                   WHERE status = %s""",
                (PENDING_STATUS, now, FAILED_STATUS),
            )
            count = cur.rowcount
        self._conn.commit()
        return count

    def recover_leased(self, expired_only: bool = True) -> int:
        """Reset leased URLs back to pending."""
        count = self._recover_leased_locked(time.time(), expired_only=expired_only)
        self._conn.commit()
        return count

    def upsert_seeds(self, urls: list[str], priority: float = 2.0) -> int:
        """Insert or requeue seed URLs."""
        if not urls:
            return 0

        rows = []
        now = time.time()
        for url in urls:
            normalized = normalize_url(url)
            domain = urlparse(normalized).netloc
            rows.append((normalized, domain, 0, priority, DISCOVERY_SEED, now, now))

        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO frontier (
                       url, domain, depth, priority, discovery_kind, source_url, added_at, next_fetch_at, status
                   )
                   VALUES %s
                   ON CONFLICT (url) DO UPDATE SET
                       status = 'pending',
                       added_at = EXCLUDED.added_at,
                       next_fetch_at = EXCLUDED.next_fetch_at,
                       priority = EXCLUDED.priority,
                       fail_streak = 0,
                       last_error = NULL,
                       lease_token = NULL,
                       lease_expires_at = NULL""",
                rows,
                template="(%s, %s, %s, %s, %s, NULL, %s, %s, 'pending')",
                page_size=200,
            )
            affected = cur.rowcount
        self._conn.commit()
        return affected

    def stats(self) -> dict:
        """Get queue statistics."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM frontier GROUP BY status")
            stats = dict(cur.fetchall())
        stats["total"] = sum(stats.values())
        return stats

    def pending_count(self) -> int:
        """Get count of pending URLs."""
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM frontier WHERE status = %s",
                (PENDING_STATUS,),
            )
            return cur.fetchone()[0]

    def is_seen(self, url: str) -> bool:
        """Check if URL exists in frontier."""
        normalized = normalize_url(url)
        with self._conn.cursor() as cur:
            cur.execute("SELECT 1 FROM frontier WHERE url = %s LIMIT 1", (normalized,))
            return cur.fetchone() is not None
