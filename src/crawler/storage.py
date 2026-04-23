"""Postgres storage for crawl results."""

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
import logging
import re
import time
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras

from .error_stats import categorize_crawl_error
from .host_manager import compute_host_budget
from .page_storage_policy import prepare_page_content
from .url_ledger import (
    BLOCKED_HOST_BACKOFF_TABLE,
    LEASE_TABLE,
    PHYSICAL_QUEUE_DEFAULT_SCHEDULER_SURFACE,
    PHYSICAL_QUEUE_ORDER,
    PHYSICAL_QUEUE_TABLES,
    URL_LEDGER_TABLE,
)
from .scheduler_observability import SchedulerObservability
from .config import settings
from .result import CrawlResult, result_to_dict
from .schema import assert_public_table_columns
from .telemetry import StorageTelemetry

logger = logging.getLogger(__name__)
PAGES_REQUIRED_COLUMNS = {
    "url_hash",
    "url",
    "host",
    "title",
    "content_length",
    "content_type",
    "source_url",
    "outlinks",
    "storage_tier",
    "storage_reason",
    "stored_content_bytes",
    "content_truncated",
    "outlink_count",
    "stored_outlink_count",
    "crawled_at",
    "created_at",
}
PAGE_CONTENT_REQUIRED_COLUMNS = {
    "url_hash",
    "content",
    "updated_at",
}
URL_LEDGER_STATS_REQUIRED_COLUMNS = {
    "host",
    "current_intent",
    "last_error",
}
DIAGNOSTIC_STATS_STATEMENT_TIMEOUT_MS = 15000


_TITLE_PATTERN = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)


@dataclass(slots=True)
class StorageSaveResult:
    """Result of one page persistence attempt."""

    saved: bool
    telemetry: StorageTelemetry | None = None

    def __bool__(self) -> bool:
        return self.saved


@dataclass(slots=True)
class _PreparedPageSave:
    """Prepared page payload for one storage write."""

    data: Mapping[str, object]
    url: str
    url_hash: str
    host: str
    title: str | None
    outlinks: list[str]
    outlink_count: int
    stored_outlink_count: int
    content_type: str
    crawled_at: float
    content_updated_at: float
    stored_content: object
    telemetry: StorageTelemetry


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000


def _url_hash(url: str) -> str:
    return hashlib.blake2b(url.encode(), digest_size=8).hexdigest()


def _sanitize_stored_content(content: object) -> str:
    """Drop content that cannot be represented safely in the TEXT storage column."""
    if not isinstance(content, str):
        return ""
    if "\x00" in content:
        return ""
    return content


def _prepare_page_save(result: CrawlResult | Mapping[str, object]) -> StorageSaveResult | _PreparedPageSave:
    """Prepare one page payload for storage mutation."""
    prepare_started = time.perf_counter()
    data = result_to_dict(result)
    if data.get("error"):
        return StorageSaveResult(saved=False)

    url = str(data["url"])
    url_hash = _url_hash(url)
    host = urlparse(url).netloc

    title = None
    content = _sanitize_stored_content(data.get("content", ""))
    if content:
        match = _TITLE_PATTERN.search(content)
        if match:
            title = match.group(1).strip()[:500]

    outlinks = data.get("outlinks", [])
    if not isinstance(outlinks, list):
        outlinks = []
    outlink_count = data.get("outlink_count")
    if not isinstance(outlink_count, int):
        outlink_count = len(outlinks)
    stored_outlink_count = len(outlinks)
    content_type = str(data.get("content_type") or "")
    discovery_value = float(data.get("discovery_value") or 1.0)
    stored_content = prepare_page_content(
        content=content,
        content_type=content_type,
        discovery_value=discovery_value,
        summary_bytes=settings.stored_content_summary_bytes,
        standard_bytes=settings.stored_content_standard_bytes,
        extended_bytes=settings.stored_content_extended_bytes,
        standard_min_discovery_value=settings.stored_content_standard_min_discovery_value,
        extended_min_discovery_value=settings.stored_content_extended_min_discovery_value,
    )
    crawled_at = data.get("timestamp", time.time())
    telemetry = StorageTelemetry(
        prepare_ms=_elapsed_ms(prepare_started),
        stored_content_bytes=stored_content.stored_content_bytes,
        storage_tier=stored_content.storage_tier,
        content_truncated=stored_content.content_truncated,
    )
    return _PreparedPageSave(
        data=data,
        url=url,
        url_hash=url_hash,
        host=host,
        title=title,
        outlinks=outlinks,
        outlink_count=outlink_count,
        stored_outlink_count=stored_outlink_count,
        content_type=content_type,
        crawled_at=crawled_at,
        content_updated_at=time.time(),
        stored_content=stored_content,
        telemetry=telemetry,
    )


def _pending_queue_sql() -> str:
    queue_union = "\n                        UNION ALL\n                        ".join(
        f"SELECT '{physical_queue}' AS physical_queue, url, host, next_fetch_at FROM public.{PHYSICAL_QUEUE_TABLES[physical_queue]}"
        for physical_queue in PHYSICAL_QUEUE_ORDER
    )
    return f"SELECT physical_queue, url, host, next_fetch_at FROM (\n                        {queue_union}\n                    ) AS pending_queue_rows"


def _retry_surface_sql() -> str:
    retry_union = "\n                               UNION\n                               ".join(
        f"SELECT url FROM public.{PHYSICAL_QUEUE_TABLES[physical_queue]}"
        for physical_queue in PHYSICAL_QUEUE_ORDER
    )
    return f"WITH retry_surface AS (\n                               {retry_union}\n                           )"


def _physical_queue_count_projection_sql(physical_queue_expr: str) -> str:
    return ",\n                                ".join(
        f"COUNT(*) FILTER (WHERE {physical_queue_expr} = '{physical_queue}') AS queue_count_{index}"
        for index, physical_queue in enumerate(PHYSICAL_QUEUE_ORDER)
    )


def _surface_counts_from_physical_queue_count_values(
    queue_count_values: tuple[int, ...],
) -> dict[str, int]:
    surface_counts: dict[str, int] = {}
    for physical_queue, count in zip(PHYSICAL_QUEUE_ORDER, queue_count_values, strict=True):
        surface = PHYSICAL_QUEUE_DEFAULT_SCHEDULER_SURFACE[physical_queue]
        surface_counts[surface] = surface_counts.get(surface, 0) + int(count or 0)
    return surface_counts


def _build_operator_summary(
    scheduler_status: Mapping[str, object],
    readiness: Mapping[str, object],
    effective_state_counts: Mapping[str, int],
    runtime: Mapping[str, object],
    active_error_breakdown: Mapping[str, int],
    host_budget_summary: Mapping[str, object],
) -> dict[str, object]:
    """Build a compact operator-facing metrics surface from detailed stats."""
    runtime_payload = runtime.get("payload") if isinstance(runtime.get("payload"), Mapping) else {}
    active_cycle = (
        runtime_payload.get("active_cycle")
        if isinstance(runtime_payload.get("active_cycle"), Mapping)
        else runtime_payload
    )
    last_completed_cycle = (
        runtime_payload.get("last_completed_cycle")
        if isinstance(runtime_payload.get("last_completed_cycle"), Mapping)
        else {}
    )
    scheduler_state_views = _scheduler_state_views(
        scheduler_status=scheduler_status,
        readiness=readiness,
        effective_state_counts=effective_state_counts,
    )
    scheduler_state_snapshot = scheduler_state_views["scheduler_state_snapshot"]
    state_counts = scheduler_state_views["readiness_state_counts"]
    cycle_errors = runtime_payload.get("errors")
    if not isinstance(cycle_errors, Mapping):
        cycle_errors = active_cycle.get("failure_breakdown")
    if not isinstance(cycle_errors, Mapping):
        cycle_errors = last_completed_cycle.get("errors")
    if not isinstance(cycle_errors, Mapping):
        cycle_errors = active_error_breakdown

    scheduler_readiness_states = {
        "pending": int(readiness.get("pending", 0) or 0),
        "runnable": int(state_counts.get("runnable", 0) or 0),
        "scheduled": int(state_counts.get("scheduled", 0) or 0),
        "blocked_host_next_request": int(state_counts.get("blocked_host_next_request", 0) or 0),
        "blocked_host_backoff": int(state_counts.get("blocked_host_backoff", 0) or 0),
        "retry_quarantine": int(state_counts.get("retry_quarantine", 0) or 0),
        "leased": int(scheduler_status.get("leased", 0) or 0),
    }

    return {
        "scheduler_state": dict(scheduler_readiness_states),
        "scheduler_readiness_states": dict(scheduler_readiness_states),
        "scheduler_state_snapshot": dict(scheduler_state_snapshot),
        "scheduler_durable_states": dict(scheduler_state_views["durable_state_counts"]),
        "scheduler_effective_states": dict(scheduler_state_views["effective_state_counts"]),
        "scheduler_intents": dict(scheduler_status.get("intent_counts", {})),
        "scheduler_blocked_reasons": dict(scheduler_state_views["blocked_reason_counts"]),
        "throughput": {
            "pages_per_second": last_completed_cycle.get(
                "pages_per_second",
                runtime_payload.get("pages_per_second"),
            ),
            "cycle_pages": last_completed_cycle.get("pages", runtime_payload.get("pages")),
            "active_hosts": int(active_cycle.get("active_hosts", 0) or 0),
            "errors": dict(cycle_errors),
        },
        "backpressure": {
            "parse_queue_size": int(active_cycle.get("parse_queue_size", 0) or 0),
            "finalize_queue_size": int(active_cycle.get("finalize_queue_size", 0) or 0),
            "publish_queue_size": int(active_cycle.get("publish_queue_size", 0) or 0),
            "parse_queue_wait_max_ms": active_cycle.get("parse_queue_wait_max_ms", 0.0),
            "finalize_queue_wait_max_ms": active_cycle.get("finalize_queue_wait_max_ms", 0.0),
            "publish_queue_wait_max_ms": active_cycle.get("publish_queue_wait_max_ms", 0.0),
        },
        "adaptive_budget": {
            "observed_hosts": int(host_budget_summary.get("observed_hosts", 0) or 0),
            "eligible_hosts": int(host_budget_summary.get("eligible_hosts", 0) or 0),
            "eligible_pending": int(host_budget_summary.get("eligible_pending", 0) or 0),
            "ineligible_due_to_failures": int(
                host_budget_summary.get("ineligible_due_to_failures", 0) or 0
            ),
            "ineligible_due_to_latency": int(
                host_budget_summary.get("ineligible_due_to_latency", 0) or 0
            ),
            "max_budget": int(host_budget_summary.get("max_budget", 0) or 0),
        },
    }


def _scheduler_state_views(
    *,
    scheduler_status: Mapping[str, object],
    readiness: Mapping[str, object],
    effective_state_counts: Mapping[str, int],
) -> dict[str, dict[str, int]]:
    """Resolve scheduler-facing state aliases from the canonical state snapshot first."""
    scheduler_state_snapshot = (
        scheduler_status.get("scheduler_state_snapshot")
        if isinstance(scheduler_status.get("scheduler_state_snapshot"), Mapping)
        else {}
    )
    durable_state_counts = (
        scheduler_state_snapshot.get("durable_state_counts")
        if isinstance(scheduler_state_snapshot.get("durable_state_counts"), Mapping)
        else scheduler_status.get("durable_state_counts")
        if isinstance(scheduler_status.get("durable_state_counts"), Mapping)
        else {}
    )
    readiness_state_counts = (
        scheduler_state_snapshot.get("readiness_state_counts")
        if isinstance(scheduler_state_snapshot.get("readiness_state_counts"), Mapping)
        else scheduler_status.get("readiness_state_counts")
        if isinstance(scheduler_status.get("readiness_state_counts"), Mapping)
        else readiness.get("state_counts")
        if isinstance(readiness.get("state_counts"), Mapping)
        else {}
    )
    effective_state_counts_view = (
        scheduler_state_snapshot.get("effective_state_counts")
        if isinstance(scheduler_state_snapshot.get("effective_state_counts"), Mapping)
        else effective_state_counts
    )
    blocked_reason_counts = (
        scheduler_state_snapshot.get("blocked_reason_counts")
        if isinstance(scheduler_state_snapshot.get("blocked_reason_counts"), Mapping)
        else scheduler_status.get("blocked_reason_counts")
        if isinstance(scheduler_status.get("blocked_reason_counts"), Mapping)
        else {}
    )
    return {
        "scheduler_state_snapshot": dict(scheduler_state_snapshot),
        "durable_state_counts": dict(durable_state_counts),
        "readiness_state_counts": dict(readiness_state_counts),
        "effective_state_counts": dict(effective_state_counts_view),
        "blocked_reason_counts": dict(blocked_reason_counts),
    }


def _runtime_payload_dict(runtime: Mapping[str, object]) -> dict[str, object]:
    """Return the stored runtime payload when it has the expected shape."""
    payload = runtime.get("payload")
    return dict(payload) if isinstance(payload, Mapping) else {}


def _runtime_readiness_from_payload(payload: Mapping[str, object]) -> dict[str, object]:
    """Build a readiness view from persisted daemon runtime state."""
    state_counts = payload.get("readiness_state_counts")
    if not isinstance(state_counts, Mapping):
        state_counts = payload.get("scheduler_readiness_states")
    if not isinstance(state_counts, Mapping):
        state_counts = {}

    blocked = payload.get("blocked_reason_counts")
    if not isinstance(blocked, Mapping):
        blocked = payload.get("readiness_blocked")
    if not isinstance(blocked, Mapping):
        blocked = {}

    return {
        "pending": int(payload.get("pending", 0) or 0),
        "runnable": int(payload.get("runnable", state_counts.get("runnable", 0)) or 0),
        "runnable_hosts": int(payload.get("runnable_hosts", 0) or 0),
        "next_runnable_delay": payload.get("next_runnable_delay"),
        "blocked": dict(blocked),
        "state_counts": dict(state_counts),
    }


def _runtime_scheduler_status_from_payload(payload: Mapping[str, object]) -> dict[str, object]:
    """Build a scheduler status view without live scheduler aggregation."""
    snapshot = payload.get("scheduler_state_snapshot")
    if not isinstance(snapshot, Mapping):
        snapshot = {}

    readiness = _runtime_readiness_from_payload(payload)
    state_counts = readiness["state_counts"]
    pending = int(payload.get("pending", readiness.get("pending", 0)) or 0)
    leased = int(payload.get("leased", 0) or 0)

    return {
        "leased": leased,
        "done": int(payload.get("done", 0) or 0),
        "failed": int(payload.get("failed", 0) or 0),
        "intent_counts": dict(payload.get("intent_counts", {}))
        if isinstance(payload.get("intent_counts"), Mapping)
        else {},
        "scheduler_state_snapshot": {key: dict(value) for key, value in snapshot.items()}
        if snapshot
        else {},
        "readiness_state_counts": dict(state_counts),
        "effective_state_counts": dict(payload.get("effective_scheduler_states", {}))
        if isinstance(payload.get("effective_scheduler_states"), Mapping)
        else {},
        "blocked_reason_counts": dict(readiness["blocked"]),
        "pending_surfaces": dict(payload.get("pending_surfaces", {}))
        if isinstance(payload.get("pending_surfaces"), Mapping)
        else {},
        "blocked_surfaces": dict(payload.get("blocked_surfaces", {}))
        if isinstance(payload.get("blocked_surfaces"), Mapping)
        else {},
        "pending": pending,
        "total": int(payload.get("total", pending + leased) or 0),
    }


class PgStorage:
    """Store crawl results in Postgres."""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = False
        assert_public_table_columns(self._conn, "pages", PAGES_REQUIRED_COLUMNS)
        assert_public_table_columns(self._conn, "page_content", PAGE_CONTENT_REQUIRED_COLUMNS)
        self._count = 0

    def _finish_read(self) -> None:
        """Close read-only transactions so API requests do not hold relation locks."""
        self._conn.commit()

    def save(self, result: CrawlResult | Mapping[str, object]) -> StorageSaveResult:
        """Save a single crawl result and return persistence telemetry."""
        return self.save_many([result])[0]

    def save_many(
        self,
        results: list[CrawlResult | Mapping[str, object]],
    ) -> list[StorageSaveResult]:
        """Save multiple crawl results in one transaction."""
        total_started = time.perf_counter()
        prepared_results = [_prepare_page_save(result) for result in results]
        prepared_pages = [
            prepared
            for prepared in prepared_results
            if isinstance(prepared, _PreparedPageSave)
        ]
        if not prepared_pages:
            return [
                prepared
                if isinstance(prepared, StorageSaveResult)
                else StorageSaveResult(saved=False, telemetry=prepared.telemetry)
                for prepared in prepared_results
            ]

        prepared_by_hash: dict[str, _PreparedPageSave] = {}
        for prepared in prepared_pages:
            prepared_by_hash[prepared.url_hash] = prepared
        batch_pages = list(prepared_by_hash.values())

        page_rows = [
            (
                prepared.url_hash,
                prepared.url,
                prepared.host,
                prepared.title,
                prepared.data.get("status"),
                prepared.data.get("content_length"),
                prepared.content_type,
                prepared.data.get("source_url"),
                prepared.outlinks,
                prepared.stored_content.storage_tier,
                prepared.stored_content.storage_reason,
                prepared.stored_content.stored_content_bytes,
                prepared.stored_content.content_truncated,
                prepared.outlink_count,
                prepared.stored_outlink_count,
                prepared.crawled_at,
            )
            for prepared in batch_pages
        ]
        content_rows = [
            (
                prepared.url_hash,
                prepared.stored_content.content,
                prepared.content_updated_at,
            )
            for prepared in batch_pages
            if prepared.stored_content.content
        ]
        delete_hashes = [
            prepared.url_hash
            for prepared in batch_pages
            if not prepared.stored_content.content
        ]

        try:
            with self._conn.cursor() as cur:
                pages_started = time.perf_counter()
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO pages (
                           url_hash, url, host, title, status, content_length, content_type,
                           source_url, outlinks, storage_tier, storage_reason,
                           stored_content_bytes, content_truncated, outlink_count,
                           stored_outlink_count, crawled_at
                       )
                       VALUES %s
                       ON CONFLICT (url_hash) DO UPDATE SET
                           title = EXCLUDED.title,
                           status = EXCLUDED.status,
                           content_length = EXCLUDED.content_length,
                           content_type = EXCLUDED.content_type,
                           source_url = EXCLUDED.source_url,
                           outlinks = EXCLUDED.outlinks,
                           storage_tier = EXCLUDED.storage_tier,
                           storage_reason = EXCLUDED.storage_reason,
                           stored_content_bytes = EXCLUDED.stored_content_bytes,
                           content_truncated = EXCLUDED.content_truncated,
                           outlink_count = EXCLUDED.outlink_count,
                           stored_outlink_count = EXCLUDED.stored_outlink_count,
                           crawled_at = EXCLUDED.crawled_at""",
                    page_rows,
                    page_size=200,
                )
                pages_upsert_ms = _elapsed_ms(pages_started)
                content_started = time.perf_counter()
                if content_rows:
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO page_content (url_hash, content, updated_at)
                           VALUES %s
                           ON CONFLICT (url_hash) DO UPDATE SET
                               content = EXCLUDED.content,
                               updated_at = EXCLUDED.updated_at""",
                        content_rows,
                        page_size=200,
                    )
                if delete_hashes:
                    cur.execute(
                        "DELETE FROM page_content WHERE url_hash = ANY(%s)",
                        (delete_hashes,),
                    )
                page_content_ms = _elapsed_ms(content_started)
            commit_started = time.perf_counter()
            self._conn.commit()
            commit_ms = _elapsed_ms(commit_started)
            total_ms = _elapsed_ms(total_started)
            self._count += len(prepared_pages)
            save_results: list[StorageSaveResult] = []
            prepared_iter = iter(prepared_pages)
            for prepared in prepared_results:
                if isinstance(prepared, StorageSaveResult):
                    save_results.append(prepared)
                    continue
                page = next(prepared_iter)
                page.telemetry.pages_upsert_ms = pages_upsert_ms
                page.telemetry.page_content_ms = page_content_ms
                page.telemetry.commit_ms = commit_ms
                page.telemetry.total_ms = total_ms
                save_results.append(StorageSaveResult(saved=True, telemetry=page.telemetry))
            return save_results
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to save batch of %d pages", len(prepared_pages))
            raise

    @property
    def count(self) -> int:
        return self._count

    @property
    def conn(self):
        """Expose connection for the URL ledger (which shares the same Postgres)."""
        return self._conn

    def list_pages(
        self,
        since: float = 0,
        limit: int = 100,
        offset: int = 0,
        host: str | None = None,
    ) -> list[dict]:
        """List crawled pages with optional filters."""
        conditions = ["crawled_at > %s"]
        params: list = [since]

        if host:
            conditions.append("host = %s")
            params.append(host)

        where = " AND ".join(conditions)
        params.extend([limit, offset])

        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""SELECT url_hash, url, host, title, status, content_length,
                               content_type, outlinks, storage_tier, storage_reason,
                               stored_content_bytes, content_truncated, outlink_count,
                               stored_outlink_count, crawled_at
                        FROM pages WHERE {where}
                        ORDER BY crawled_at ASC
                        LIMIT %s OFFSET %s""",
                    params,
                )
                pages = [dict(row) for row in cur.fetchall()]
            self._finish_read()
            return pages
        except Exception:
            self._conn.rollback()
            raise

    def upsert_runtime_stats(self, component: str, payload: Mapping[str, object]) -> None:
        """Store runtime crawler stats for API consumption."""
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO crawler_runtime_stats (component, payload, updated_at)
                       VALUES (%s, %s::jsonb, %s)
                       ON CONFLICT (component) DO UPDATE SET
                           payload = EXCLUDED.payload,
                           updated_at = EXCLUDED.updated_at""",
                    (component, json.dumps(dict(payload)), time.time()),
                )
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to update runtime stats for %s", component)

    def get_runtime_stats(self, component: str | None = None) -> dict[str, object]:
        """Fetch runtime crawler stats snapshots."""
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT to_regclass('public.crawler_runtime_stats') AS table_name")
                exists = cur.fetchone()["table_name"] is not None
                if not exists:
                    self._finish_read()
                    return {}

                if component is None:
                    cur.execute("SELECT component, payload, updated_at FROM crawler_runtime_stats")
                    rows = cur.fetchall()
                    self._finish_read()
                    return {
                        row["component"]: {
                            "payload": row["payload"],
                            "updated_at": row["updated_at"],
                        }
                        for row in rows
                    }

                cur.execute(
                    "SELECT payload, updated_at FROM crawler_runtime_stats WHERE component = %s",
                    (component,),
                )
                row = cur.fetchone()
            self._finish_read()
            if not row:
                return {}
            return {
                "payload": row["payload"],
                "updated_at": row["updated_at"],
            }
        except Exception:
            self._conn.rollback()
            raise

    def get_page(self, url_hash: str) -> dict | None:
        """Get a single page with full content."""
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """SELECT pages.url_hash, pages.url, pages.host, pages.title,
                              COALESCE(page_content.content, '') AS content,
                              pages.status, pages.content_length, pages.content_type,
                              pages.source_url, pages.outlinks, pages.storage_tier,
                              pages.storage_reason, pages.stored_content_bytes,
                              pages.content_truncated, pages.outlink_count,
                              pages.stored_outlink_count, pages.crawled_at
                       FROM pages
                       LEFT JOIN page_content ON page_content.url_hash = pages.url_hash
                       WHERE pages.url_hash = %s""",
                    (url_hash,),
                )
                row = cur.fetchone()
            self._finish_read()
            return dict(row) if row else None
        except Exception:
            self._conn.rollback()
            raise

    def get_runtime_stats_summary(self) -> dict:
        """Get fast operator stats from the persisted runtime snapshot."""
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """SELECT
                         count(*) as total_pages,
                         count(DISTINCT host) as hosts,
                         min(crawled_at) as oldest,
                         max(crawled_at) as newest,
                         sum(content_length) as total_bytes,
                         sum(stored_content_bytes) as total_stored_bytes
                       FROM pages"""
                )
                page_stats_row = cur.fetchone()
            self._finish_read()

            runtime = self.get_runtime_stats("crawler")
        except Exception:
            self._conn.rollback()
            raise

        runtime_payload = _runtime_payload_dict(runtime)
        scheduler_status = _runtime_scheduler_status_from_payload(runtime_payload)
        readiness = _runtime_readiness_from_payload(runtime_payload)
        effective_state_counts = dict(scheduler_status.get("effective_state_counts", {}))
        active_error_breakdown = runtime_payload.get("errors")
        if not isinstance(active_error_breakdown, Mapping):
            active_error_breakdown = runtime_payload.get("failure_breakdown")
        if not isinstance(active_error_breakdown, Mapping):
            active_error_breakdown = {}

        operator_summary = _build_operator_summary(
            scheduler_status=scheduler_status,
            readiness=readiness,
            effective_state_counts=effective_state_counts,
            runtime=runtime,
            active_error_breakdown=active_error_breakdown,
            host_budget_summary={},
        )
        scheduler_state_views = _scheduler_state_views(
            scheduler_status=scheduler_status,
            readiness=readiness,
            effective_state_counts=effective_state_counts,
        )

        return {
            "stats_source": "runtime_snapshot",
            "diagnostics_endpoint": "/stats/diagnostics",
            "total_pages": page_stats_row[0],
            "hosts": page_stats_row[1],
            "oldest_crawl": page_stats_row[2],
            "newest_crawl": page_stats_row[3],
            "total_bytes": page_stats_row[4],
            "total_stored_bytes": page_stats_row[5],
            "scheduler_status": scheduler_status,
            "scheduler_state_snapshot": dict(scheduler_state_views["scheduler_state_snapshot"]),
            "intent_counts": dict(scheduler_status.get("intent_counts", {})),
            "durable_state_counts": dict(scheduler_state_views["durable_state_counts"]),
            "readiness_state_counts": dict(scheduler_state_views["readiness_state_counts"]),
            "effective_state_counts": dict(scheduler_state_views["effective_state_counts"]),
            "blocked_reason_counts": dict(scheduler_state_views["blocked_reason_counts"]),
            "pending_surfaces": dict(scheduler_status.get("pending_surfaces", {})),
            "blocked_surfaces": dict(scheduler_status.get("blocked_surfaces", {})),
            "readiness": readiness,
            "top_page_hosts": [],
            "top_pending_hosts": [],
            "top_blocked_hosts": [],
            "top_slow_hosts": [],
            "top_budget_hosts": [],
            "active_error_breakdown": dict(active_error_breakdown),
            "top_error_hosts": [],
            "runtime": runtime,
            "operator_summary": operator_summary,
        }

    def get_stats(self) -> dict:
        """Get crawl statistics."""
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SET LOCAL statement_timeout = %s",
                    (DIAGNOSTIC_STATS_STATEMENT_TIMEOUT_MS,),
                )
                cur.execute(
                    """SELECT
                         count(*) as total_pages,
                         count(DISTINCT host) as hosts,
                         min(crawled_at) as oldest,
                         max(crawled_at) as newest,
                         sum(content_length) as total_bytes,
                         sum(stored_content_bytes) as total_stored_bytes
                       FROM pages"""
                )
                page_stats_row = cur.fetchone()

                cur.execute(f"SELECT to_regclass('public.{URL_LEDGER_TABLE}')")
                url_ledger_exists = cur.fetchone()[0] is not None

                scheduler_status: dict[str, int] = {}
                pending_surfaces: dict[str, int] = {}
                blocked_surfaces: dict[str, int] = {}
                readiness: dict[str, object] = {}
                top_pending_hosts: list[dict[str, object]] = []
                top_blocked_hosts: list[dict[str, object]] = []
                top_slow_hosts: list[dict[str, object]] = []
                top_budget_hosts: list[dict[str, object]] = []
                active_error_breakdown: dict[str, int] = {}
                top_error_hosts: list[dict[str, object]] = []
                runtime: dict[str, object] = {}
                operator_summary: dict[str, object] = {}
                effective_state_counts: dict[str, int] = {}
                host_budget_summary: dict[str, object] = {}
                pending_queue_sql = _pending_queue_sql()
                blocked_queue_sql = f"""SELECT physical_queue, url, host, next_fetch_at
                    FROM public.{BLOCKED_HOST_BACKOFF_TABLE}"""
                cur.execute("SELECT to_regclass('public.crawler_runtime_stats')")
                runtime_exists = cur.fetchone()[0] is not None
                if runtime_exists:
                    cur.execute(
                        "SELECT payload, updated_at FROM crawler_runtime_stats WHERE component = 'crawler'"
                    )
                    runtime_row = cur.fetchone()
                    if runtime_row:
                        runtime = {
                            "payload": runtime_row[0],
                            "updated_at": runtime_row[1],
                        }

                if url_ledger_exists:
                    cur.execute("SAVEPOINT scheduler_diagnostics")
                    try:
                        assert_public_table_columns(
                            self._conn,
                            URL_LEDGER_TABLE,
                            URL_LEDGER_STATS_REQUIRED_COLUMNS,
                        )

                        observability = SchedulerObservability(
                            self._conn,
                            physical_queue_tables=PHYSICAL_QUEUE_TABLES,
                            physical_queue_order=PHYSICAL_QUEUE_ORDER,
                            physical_queue_default_runnable_surface=PHYSICAL_QUEUE_DEFAULT_SCHEDULER_SURFACE,
                            blocked_queue_table=BLOCKED_HOST_BACKOFF_TABLE,
                            lease_table=LEASE_TABLE,
                        )

                        scheduler_status = observability.status_counts()
                        pending_surfaces = dict(scheduler_status.get("pending_surfaces", {}))
                        blocked_surfaces = dict(scheduler_status.get("blocked_surfaces", {}))

                        cur.execute(
                            f"""SELECT host, COUNT(*)
                           FROM ({pending_queue_sql}) AS pending_queue_rows
                           GROUP BY host
                           ORDER BY COUNT(*) DESC, host ASC
                           LIMIT 10"""
                        )
                        top_pending_hosts = [
                            {"host": host, "count": count} for host, count in cur.fetchall()
                        ]

                        now = time.time()
                        readiness_snapshot = observability.readiness(now=now)
                        readiness = {
                            "pending": readiness_snapshot.pending,
                            "runnable": readiness_snapshot.runnable,
                            "runnable_hosts": readiness_snapshot.runnable_hosts,
                            "next_runnable_delay": readiness_snapshot.next_runnable_delay,
                            "blocked": dict(readiness_snapshot.blocked),
                            "state_counts": dict(
                                scheduler_status.get(
                                    "readiness_state_counts", readiness_snapshot.state_counts
                                )
                            ),
                        }
                        effective_state_counts = dict(
                            scheduler_status.get("effective_state_counts", {})
                        )

                        cur.execute(
                            f"""WITH pending_entries AS (
                                SELECT physical_queue, url, host, next_fetch_at, FALSE AS forced_retry_quarantine
                                FROM ({pending_queue_sql}) AS pending_queue_rows
                                UNION ALL
                                SELECT physical_queue, url, host, next_fetch_at, TRUE AS forced_retry_quarantine
                                FROM ({blocked_queue_sql}) AS blocked_queue_rows
                            )
                            SELECT
                                pending_queue_rows.host,
                                COUNT(*) AS pending_count,
                                COUNT(*) FILTER (
                                    WHERE NOT pending_queue_rows.forced_retry_quarantine
                                      AND COALESCE(host_state.next_request_at, 0) > %(now)s
                                      AND COALESCE(host_state.backoff_until, 0) <= %(now)s
                                ) AS blocked_host_next_request,
                                COUNT(*) FILTER (
                                    WHERE NOT pending_queue_rows.forced_retry_quarantine
                                      AND COALESCE(host_state.backoff_until, 0) > %(now)s
                                ) AS blocked_host_backoff,
                                COUNT(*) FILTER (
                                    WHERE pending_queue_rows.forced_retry_quarantine
                                ) AS retry_quarantine,
                                GREATEST(
                                    MAX(COALESCE(host_state.next_request_at, 0)) - %(now)s,
                                    0
                                ) AS next_request_wait_seconds,
                                GREATEST(
                                    MAX(COALESCE(host_state.backoff_until, 0)) - %(now)s,
                                    0
                                ) AS backoff_wait_seconds,
                                COALESCE(MAX(host_state.consecutive_failures), 0) AS consecutive_failures
                            FROM pending_entries AS pending_queue_rows
                            LEFT JOIN public.host_state ON host_state.host_key = pending_queue_rows.host
                            WHERE pending_queue_rows.forced_retry_quarantine
                               OR COALESCE(host_state.next_request_at, 0) > %(now)s
                               OR COALESCE(host_state.backoff_until, 0) > %(now)s
                            GROUP BY pending_queue_rows.host
                            ORDER BY
                                COUNT(*) FILTER (
                                    WHERE pending_queue_rows.forced_retry_quarantine
                                ) DESC,
                                COUNT(*) FILTER (
                                    WHERE NOT pending_queue_rows.forced_retry_quarantine
                                      AND COALESCE(host_state.backoff_until, 0) > %(now)s
                                ) DESC,
                                GREATEST(
                                    MAX(COALESCE(host_state.backoff_until, 0)) - %(now)s,
                                    0
                                ) DESC,
                                COUNT(*) DESC,
                                pending_queue_rows.host ASC
                            LIMIT 10""",
                            {"now": now},
                        )
                        top_blocked_hosts = []
                        for blocked_row in cur.fetchall():
                            (
                                host,
                                pending_count,
                                blocked_next_request_count,
                                blocked_host_backoff_count,
                                retry_quarantine_count,
                                next_request_wait_seconds,
                                backoff_wait_seconds,
                                consecutive_failures,
                            ) = blocked_row
                            dominant_reason = "retry_quarantine"
                            wait_seconds = backoff_wait_seconds
                            if retry_quarantine_count == 0 and blocked_host_backoff_count > 0:
                                dominant_reason = "host_backoff"
                            elif (
                                retry_quarantine_count == 0
                                and blocked_host_backoff_count == 0
                                and blocked_next_request_count > 0
                            ):
                                dominant_reason = "host_next_request"
                                wait_seconds = next_request_wait_seconds
                            top_blocked_hosts.append(
                                {
                                    "host": host,
                                    "pending_count": pending_count,
                                    "blocked_counts": {
                                        "host_next_request": blocked_next_request_count,
                                        "host_backoff": blocked_host_backoff_count,
                                        "retry_quarantine": retry_quarantine_count,
                                    },
                                    "wait_seconds": round(wait_seconds, 3),
                                    "dominant_reason": dominant_reason,
                                    "consecutive_failures": consecutive_failures,
                                }
                            )

                        cur.execute(
                            f"""WITH pending_entries AS (
                                SELECT physical_queue, url, host
                                FROM ({pending_queue_sql}) AS pending_queue_rows
                                UNION ALL
                                SELECT physical_queue, url, host
                                FROM ({blocked_queue_sql}) AS blocked_queue_rows
                            )
                            SELECT
                                pending_entries.host,
                                COUNT(*) AS pending_count,
                                MAX(COALESCE(host_state.latency_ewma_ms, 0)) AS latency_ewma_ms,
                                MAX(COALESCE(host_state.latency_last_ms, 0)) AS latency_last_ms,
                                MAX(COALESCE(host_state.latency_observed_at, 0)) AS latency_observed_at,
                                MAX(COALESCE(host_state.latency_sample_count, 0)) AS latency_sample_count,
                                COALESCE(MAX(host_state.consecutive_failures), 0) AS consecutive_failures,
                                {_physical_queue_count_projection_sql("pending_entries.physical_queue")}
                            FROM pending_entries
                            JOIN public.host_state ON host_state.host_key = pending_entries.host
                            WHERE COALESCE(host_state.latency_ewma_ms, 0) > 0
                            GROUP BY pending_entries.host
                            ORDER BY
                                MAX(COALESCE(host_state.latency_ewma_ms, 0)) DESC,
                                COUNT(*) DESC,
                                pending_entries.host ASC
                            LIMIT 10"""
                        )
                        top_slow_hosts = []
                        for row in cur.fetchall():
                            (
                                host,
                                pending_count,
                                latency_ewma_ms,
                                latency_last_ms,
                                latency_observed_at,
                                latency_sample_count,
                                consecutive_failures,
                                *queue_count_values,
                            ) = row
                            top_slow_hosts.append(
                                {
                                    "host": host,
                                    "pending_count": pending_count,
                                    "latency_ewma_ms": round(latency_ewma_ms, 1),
                                    "latency_last_ms": round(latency_last_ms, 1),
                                    "latency_observed_at": latency_observed_at,
                                    "latency_sample_count": latency_sample_count,
                                    "consecutive_failures": consecutive_failures,
                                    "surface_counts": _surface_counts_from_physical_queue_count_values(
                                        tuple(queue_count_values)
                                    ),
                                }
                            )
                        elevated_budget_hosts = []
                        observed_hosts = 0
                        ineligible_due_to_failures = 0
                        ineligible_due_to_latency = 0
                        for host_entry in top_slow_hosts:
                            observed_hosts += 1
                            host_budget = compute_host_budget(
                                latency_ewma_ms=float(host_entry["latency_ewma_ms"]),
                                consecutive_failures=int(host_entry["consecutive_failures"]),
                                default_budget=settings.max_inflight_requests_per_host,
                            )
                            if host_budget <= settings.max_inflight_requests_per_host:
                                if int(host_entry["consecutive_failures"]) > 0:
                                    ineligible_due_to_failures += 1
                                else:
                                    ineligible_due_to_latency += 1
                                continue
                            elevated_budget_hosts.append(
                                {
                                    **host_entry,
                                    "host_budget": host_budget,
                                }
                            )
                        top_budget_hosts = elevated_budget_hosts[:10]
                        host_budget_summary = {
                            "observed_hosts": observed_hosts,
                            "eligible_hosts": len(elevated_budget_hosts),
                            "eligible_pending": sum(
                                int(host_entry["pending_count"])
                                for host_entry in elevated_budget_hosts
                            ),
                            "ineligible_due_to_failures": ineligible_due_to_failures,
                            "ineligible_due_to_latency": ineligible_due_to_latency,
                            "max_budget": max(
                                (
                                    int(host_entry["host_budget"])
                                    for host_entry in elevated_budget_hosts
                                ),
                                default=settings.max_inflight_requests_per_host,
                            ),
                        }

                        cur.execute(
                            _retry_surface_sql()
                            + """
                           SELECT url_ledger.last_error, COUNT(*)
                           FROM public.url_ledger AS url_ledger
                           LEFT JOIN retry_surface ON retry_surface.url = url_ledger.url
                           WHERE url_ledger.last_error IS NOT NULL
                             AND (url_ledger.terminal_reason IS NOT NULL OR retry_surface.url IS NOT NULL)
                           GROUP BY url_ledger.last_error"""
                        )
                        error_counts = Counter()
                        for error, count in cur.fetchall():
                            category = categorize_crawl_error(error)
                            if category:
                                error_counts[category] += count
                        active_error_breakdown = {
                            category: error_counts[category]
                            for category in (
                                "http_4xx",
                                "http_5xx",
                                "timeout",
                                "connection_error",
                                "http_other",
                                "other",
                            )
                            if error_counts.get(category)
                        }

                        cur.execute(
                            _retry_surface_sql()
                            + """
                           SELECT url_ledger.host, COUNT(*)
                           FROM public.url_ledger AS url_ledger
                           LEFT JOIN retry_surface ON retry_surface.url = url_ledger.url
                           WHERE url_ledger.last_error IS NOT NULL
                             AND (url_ledger.terminal_reason IS NOT NULL OR retry_surface.url IS NOT NULL)
                           GROUP BY url_ledger.host
                           ORDER BY COUNT(*) DESC, url_ledger.host ASC
                           LIMIT 10"""
                        )
                        top_error_hosts = [
                            {"host": host, "count": count} for host, count in cur.fetchall()
                        ]
                        cur.execute("RELEASE SAVEPOINT scheduler_diagnostics")
                    except psycopg2.Error as exc:
                        cur.execute("ROLLBACK TO SAVEPOINT scheduler_diagnostics")
                        scheduler_status = {
                            "diagnostics_unavailable": True,
                            "diagnostics_error": exc.__class__.__name__,
                        }
                        pending_surfaces = {}
                        blocked_surfaces = {}
                        logger.warning(
                            "Scheduler diagnostics unavailable: %s", exc.__class__.__name__
                        )

                cur.execute(
                    """SELECT host, COUNT(*)
                       FROM pages
                       GROUP BY host
                       ORDER BY COUNT(*) DESC, host ASC
                       LIMIT 10"""
                )
                top_page_hosts = [{"host": host, "count": count} for host, count in cur.fetchall()]
            self._finish_read()
        except Exception:
            self._conn.rollback()
            raise

        operator_summary = _build_operator_summary(
            scheduler_status=scheduler_status,
            readiness=readiness,
            effective_state_counts=effective_state_counts,
            runtime=runtime,
            active_error_breakdown=active_error_breakdown,
            host_budget_summary=host_budget_summary,
        )

        scheduler_state_views = _scheduler_state_views(
            scheduler_status=scheduler_status,
            readiness=readiness,
            effective_state_counts=effective_state_counts,
        )
        return {
            "total_pages": page_stats_row[0],
            "hosts": page_stats_row[1],
            "oldest_crawl": page_stats_row[2],
            "newest_crawl": page_stats_row[3],
            "total_bytes": page_stats_row[4],
            "total_stored_bytes": page_stats_row[5],
            "scheduler_status": scheduler_status,
            "scheduler_state_snapshot": dict(scheduler_state_views["scheduler_state_snapshot"]),
            "intent_counts": dict(scheduler_status.get("intent_counts", {})),
            "durable_state_counts": dict(scheduler_state_views["durable_state_counts"]),
            "readiness_state_counts": dict(scheduler_state_views["readiness_state_counts"]),
            "effective_state_counts": dict(scheduler_state_views["effective_state_counts"]),
            "blocked_reason_counts": dict(scheduler_state_views["blocked_reason_counts"]),
            "pending_surfaces": pending_surfaces,
            "blocked_surfaces": blocked_surfaces,
            "readiness": readiness,
            "top_page_hosts": top_page_hosts,
            "top_pending_hosts": top_pending_hosts,
            "top_blocked_hosts": top_blocked_hosts,
            "top_slow_hosts": top_slow_hosts,
            "top_budget_hosts": top_budget_hosts,
            "active_error_breakdown": active_error_breakdown,
            "top_error_hosts": top_error_hosts,
            "runtime": runtime,
            "operator_summary": operator_summary,
        }

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
