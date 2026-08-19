"""Persistence for durable URL facts."""

from __future__ import annotations

from collections import Counter
import logging
import time
from urllib.parse import urlparse

import psycopg2.extras

from .config import settings
from .egress_guard import is_url_allowed_without_dns
from .host_ledger import HostLedgerStore
from .scheduler_queue_policy import SchedulerQueuePolicy
from .scheduler_task import CrawlTask
from .url_identity import (
    MAX_URL_IDENTITY_BYTES,
    URL_IDENTITY_VERSION,
    url_identity_hash,
    url_identity_length,
)
from .urls import normalize_url

logger = logging.getLogger(__name__)

URL_LEDGER_TABLE = "url_ledger"


class UrlLedgerStore:
    """Read and write durable URL discovery and crawl facts."""

    def __init__(
        self,
        conn,
        *,
        host_ledger: HostLedgerStore,
        queue_policy: SchedulerQueuePolicy,
    ) -> None:
        self._conn = conn
        self._host_ledger = host_ledger
        self._queue_policy = queue_policy

    def prepare_tasks(self, tasks: list[CrawlTask]) -> list[CrawlTask]:
        merged: dict[str, CrawlTask] = {}
        for task in tasks:
            normalized_url = normalize_url(task.url)
            if url_identity_length(normalized_url) > MAX_URL_IDENTITY_BYTES:
                continue
            if not is_url_allowed_without_dns(
                normalized_url,
                allow_private_network_egress=settings.allow_private_network_egress,
                allowed_ports=settings.allowed_egress_ports,
            ).allowed:
                continue
            normalized = self._queue_policy.normalize_task_metadata(
                task,
                normalized_url=normalized_url,
            )
            current = merged.get(normalized.url)
            merged[normalized.url] = (
                normalized
                if current is None
                else self._queue_policy.merge_tasks(current, normalized)
            )
        return list(merged.values())

    def update_task_intents(self, cur, tasks: list[CrawlTask]) -> None:
        rows = [
            (task.url, normalized_intent)
            for task in tasks
            if (normalized_intent := self._queue_policy.normalize_intent(task.intent)) is not None
        ]
        if not rows:
            return
        psycopg2.extras.execute_values(
            cur,
            f"""UPDATE {URL_LEDGER_TABLE} AS ledger
                SET current_intent = payload.current_intent
                FROM (VALUES %s) AS payload(url, current_intent)
                WHERE ledger.url = payload.url
                  AND ledger.terminal_reason IS NULL""",
            rows,
            template="(%s, %s)",
            page_size=200,
        )

    def upsert_tasks(self, tasks: list[CrawlTask]) -> tuple[list[CrawlTask], int]:
        if not tasks:
            return [], 0
        prepared_tasks = self.prepare_tasks(tasks)
        if not prepared_tasks:
            return [], 0
        rows = [
            (
                task.url,
                url_identity_hash(task.url),
                url_identity_length(task.url),
                URL_IDENTITY_VERSION,
                urlparse(task.url).netloc,
                task.discovery_value,
                task.source_url,
                task.added_at,
                task.next_fetch_at or task.added_at or time.time(),
                task.intent,
            )
            for task in prepared_tasks
        ]
        try:
            with self._conn.cursor() as cur:
                normalized_urls = [task.url for task in prepared_tasks]
                cur.execute(
                    f"SELECT url FROM {URL_LEDGER_TABLE} WHERE url = ANY(%s)",
                    (normalized_urls,),
                )
                existing_urls = {url for (url,) in cur.fetchall()}
                new_host_counts = Counter(
                    urlparse(task.url).netloc
                    for task in prepared_tasks
                    if task.url not in existing_urls
                )
                psycopg2.extras.execute_values(
                    cur,
                    f"""INSERT INTO {URL_LEDGER_TABLE} (
                           url, url_hash, url_length, url_identity_version, host,
                           discovery_value, source_url, added_at, next_fetch_at, current_intent
                       ) VALUES %s
                       ON CONFLICT (url) DO UPDATE SET
                           url_hash = EXCLUDED.url_hash,
                           url_length = EXCLUDED.url_length,
                           url_identity_version = EXCLUDED.url_identity_version,
                           discovery_value = GREATEST({URL_LEDGER_TABLE}.discovery_value, EXCLUDED.discovery_value),
                           source_url = COALESCE({URL_LEDGER_TABLE}.source_url, EXCLUDED.source_url),
                           added_at = LEAST({URL_LEDGER_TABLE}.added_at, EXCLUDED.added_at),
                           next_fetch_at = LEAST({URL_LEDGER_TABLE}.next_fetch_at, EXCLUDED.next_fetch_at),
                           current_intent = COALESCE(EXCLUDED.current_intent, {URL_LEDGER_TABLE}.current_intent)
                       WHERE {URL_LEDGER_TABLE}.terminal_reason IS NULL
                         AND {URL_LEDGER_TABLE}.last_success_at IS NULL
                         AND (
                           EXCLUDED.discovery_value > {URL_LEDGER_TABLE}.discovery_value
                           OR EXCLUDED.url_hash IS DISTINCT FROM {URL_LEDGER_TABLE}.url_hash
                           OR EXCLUDED.url_length IS DISTINCT FROM {URL_LEDGER_TABLE}.url_length
                           OR EXCLUDED.url_identity_version IS DISTINCT FROM {URL_LEDGER_TABLE}.url_identity_version
                           OR ({URL_LEDGER_TABLE}.source_url IS NULL AND EXCLUDED.source_url IS NOT NULL)
                           OR EXCLUDED.next_fetch_at < {URL_LEDGER_TABLE}.next_fetch_at
                           OR EXCLUDED.current_intent IS DISTINCT FROM {URL_LEDGER_TABLE}.current_intent
                         )
                       RETURNING url""",
                    rows,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    page_size=200,
                )
                changed = len(cur.fetchall())
                seen_hosts = {urlparse(task.url).netloc for task in prepared_tasks if task.url}
                host_counts = Counter({host: 0 for host in seen_hosts})
                host_counts.update(new_host_counts)
                self._host_ledger.record_discovered_urls_in_tx(cur, host_counts)
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to upsert batch of %d URLs", len(tasks))
            return [], 0
        self._conn.commit()
        return prepared_tasks, changed

    def is_seen(self, url: str) -> bool:
        normalized = normalize_url(url)
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {URL_LEDGER_TABLE} WHERE url = %s LIMIT 1", (normalized,))
            return cur.fetchone() is not None
