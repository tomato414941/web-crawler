"""Postgres storage for crawl results."""

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
import logging
import re
import time
from typing import cast
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras

from .r2_content import R2ContentStore
from .urls import normalize_url
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
RUNTIME_STATS_STATEMENT_TIMEOUT_MS = 15000


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
    content_bytes: bytes
    storage_tier: str
    storage_reason: str
    content_truncated: bool
    telemetry: StorageTelemetry


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000


def _url_hash(url: str) -> str:
    return hashlib.sha256(normalize_url(url).encode("utf-8")).hexdigest()


def _sanitize_stored_content(content: object) -> str:
    """Drop content that cannot be represented safely in the TEXT storage column."""
    if not isinstance(content, str):
        return ""
    if "\x00" in content:
        return ""
    return content


def _prepare_page_save(
    result: CrawlResult | Mapping[str, object],
) -> StorageSaveResult | _PreparedPageSave:
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
    content_bytes = result.content_bytes if isinstance(result, CrawlResult) else b""
    if not content_bytes and content:
        content_bytes = content.encode("utf-8")
    storage_tier = "body" if content_bytes else "metadata_only"
    storage_reason = "text_content_type" if content_bytes else "no_text_body"
    content_truncated = bool(data.get("body_truncated", False))
    crawled_at = data.get("timestamp", time.time())
    telemetry = StorageTelemetry(
        prepare_ms=_elapsed_ms(prepare_started),
        stored_content_bytes=len(content_bytes),
        storage_tier=storage_tier,
        content_truncated=content_truncated,
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
        content_bytes=content_bytes,
        storage_tier=storage_tier,
        storage_reason=storage_reason,
        content_truncated=content_truncated,
        telemetry=telemetry,
    )


def _discovery_admission_summary(
    runtime_payload: Mapping[str, object],
    active_cycle: Mapping[str, object],
    last_completed_cycle: Mapping[str, object],
) -> dict[str, object]:
    timing_summary = runtime_payload.get("timing_summary")
    if not isinstance(timing_summary, Mapping):
        timing_summary = active_cycle.get("timing_summary")
    if not isinstance(timing_summary, Mapping):
        timing_summary = last_completed_cycle.get("timing_summary")
    if not isinstance(timing_summary, Mapping):
        timing_summary = {}

    counts = timing_summary.get("counts")
    if not isinstance(counts, Mapping):
        counts = {}
    admission_counts = counts.get("discovery_admission")
    if not isinstance(admission_counts, Mapping):
        admission_counts = {}

    normalized = {
        str(key): int(value or 0) for key, value in admission_counts.items() if int(value or 0) > 0
    }
    extracted = int(normalized.get("extracted", 0))
    admitted = int(normalized.get("admitted", 0))
    non_rejection_keys = {"extracted", "admitted", "external_generic"}
    rejection_reasons = {
        key: value for key, value in sorted(normalized.items()) if key not in non_rejection_keys
    }
    rejected = sum(rejection_reasons.values())
    return {
        "extracted": extracted,
        "admitted": admitted,
        "rejected": rejected,
        "admit_ratio": round(admitted / extracted, 4) if extracted else None,
        "rejection_reasons": rejection_reasons,
        "counts": normalized,
    }


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
            "parse_queue_wait_max_ms": active_cycle.get("parse_queue_wait_max_ms", 0.0),
            "finalize_queue_wait_max_ms": active_cycle.get("finalize_queue_wait_max_ms", 0.0),
        },
        "admission_control": dict(active_cycle.get("admission_control", {})),
        "discovery_admission": _discovery_admission_summary(
            runtime_payload,
            active_cycle,
            last_completed_cycle,
        ),
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


class PageWriteStore:
    """Own page persistence mutations."""

    def __init__(self, storage: "PgStorage") -> None:
        self._storage = storage

    def save(self, result: CrawlResult | Mapping[str, object]) -> StorageSaveResult:
        return self.save_many([result])[0]

    def save_many(
        self,
        results: list[CrawlResult | Mapping[str, object]],
    ) -> list[StorageSaveResult]:
        total_started = time.perf_counter()
        prepared_results = [_prepare_page_save(result) for result in results]
        prepared_pages = [
            prepared for prepared in prepared_results if isinstance(prepared, _PreparedPageSave)
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
                prepared.storage_tier,
                prepared.storage_reason,
                len(prepared.content_bytes),
                prepared.content_truncated,
                prepared.outlink_count,
                prepared.stored_outlink_count,
                prepared.crawled_at,
            )
            for prepared in batch_pages
        ]
        try:
            content_started = time.perf_counter()
            content_store = self._storage._get_content_store()
            for prepared in batch_pages:
                if prepared.content_bytes:
                    content_store.put(
                        prepared.url_hash,
                        prepared.content_bytes,
                        prepared.content_type,
                    )
                else:
                    content_store.delete(prepared.url_hash)
            content_store_ms = _elapsed_ms(content_started)

            with self._storage._conn.cursor() as cur:
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
            commit_started = time.perf_counter()
            self._storage._conn.commit()
            commit_ms = _elapsed_ms(commit_started)
            total_ms = _elapsed_ms(total_started)
            self._storage._count += len(prepared_pages)
            save_results: list[StorageSaveResult] = []
            prepared_iter = iter(prepared_pages)
            for prepared in prepared_results:
                if isinstance(prepared, StorageSaveResult):
                    save_results.append(prepared)
                    continue
                page = next(prepared_iter)
                page.telemetry.pages_upsert_ms = pages_upsert_ms
                page.telemetry.content_store_ms = content_store_ms
                page.telemetry.commit_ms = commit_ms
                page.telemetry.total_ms = total_ms
                save_results.append(StorageSaveResult(saved=True, telemetry=page.telemetry))
            return save_results
        except Exception:
            self._storage._conn.rollback()
            logger.exception("Failed to save batch of %d pages", len(prepared_pages))
            raise

    @property
    def count(self) -> int:
        return self._storage._count


class PageQueryStore:
    """Own page read queries."""

    def __init__(self, storage: "PgStorage") -> None:
        self._storage = storage

    def list_pages(
        self,
        since: float = 0,
        limit: int = 100,
        offset: int = 0,
        host: str | None = None,
    ) -> list[dict]:
        conditions = ["crawled_at > %s"]
        params: list = [since]

        if host:
            conditions.append("host = %s")
            params.append(host)

        where = " AND ".join(conditions)
        params.extend([limit, offset])

        try:
            with self._storage._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
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
            self._storage._finish_read()
            return pages
        except Exception:
            self._storage._conn.rollback()
            raise

    def get_page(self, url_hash: str) -> dict | None:
        try:
            with self._storage._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """SELECT pages.url_hash, pages.url, pages.host, pages.title,
                              pages.status, pages.content_length, pages.content_type,
                              pages.source_url, pages.outlinks, pages.storage_tier,
                              pages.storage_reason, pages.stored_content_bytes,
                              pages.content_truncated, pages.outlink_count,
                              pages.stored_outlink_count, pages.crawled_at
                       FROM pages
                       WHERE pages.url_hash = %s""",
                    (url_hash,),
                )
                row = cur.fetchone()
            self._storage._finish_read()
            if not row:
                return None
            page = dict(row)
            if page["stored_content_bytes"]:
                body = self._storage._get_content_store().get(url_hash)
                page["content"] = body.decode("utf-8", errors="replace")
            else:
                page["content"] = ""
            return page
        except Exception:
            self._storage._conn.rollback()
            raise


class RuntimeStatsStore:
    """Own persisted runtime snapshot reads and writes."""

    def __init__(self, storage: "PgStorage") -> None:
        self._storage = storage

    def upsert(self, component: str, payload: Mapping[str, object]) -> None:
        try:
            with self._storage._conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO crawler_runtime_stats (component, payload, updated_at)
                       VALUES (%s, %s::jsonb, %s)
                       ON CONFLICT (component) DO UPDATE SET
                           payload = EXCLUDED.payload,
                           updated_at = EXCLUDED.updated_at""",
                    (component, json.dumps(dict(payload)), time.time()),
                )
            self._storage._conn.commit()
        except Exception:
            self._storage._conn.rollback()
            logger.exception("Failed to update runtime stats for %s", component)

    def get(self, component: str | None = None) -> dict[str, object]:
        try:
            with self._storage._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT to_regclass('public.crawler_runtime_stats') AS table_name")
                exists = cur.fetchone()["table_name"] is not None
                if not exists:
                    self._storage._finish_read()
                    return {}

                if component is None:
                    cur.execute("SELECT component, payload, updated_at FROM crawler_runtime_stats")
                    rows = cur.fetchall()
                    self._storage._finish_read()
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
            self._storage._finish_read()
            if not row:
                return {}
            return {
                "payload": row["payload"],
                "updated_at": row["updated_at"],
            }
        except Exception:
            self._storage._conn.rollback()
            raise

    def summary(self) -> dict:
        """Return runtime snapshot plus lightweight page totals for operators."""
        try:
            with self._storage._conn.cursor() as cur:
                cur.execute(
                    "SET LOCAL statement_timeout = %s",
                    (RUNTIME_STATS_STATEMENT_TIMEOUT_MS,),
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
            self._storage._finish_read()

            runtime = self.get("crawler")
        except Exception:
            self._storage._conn.rollback()
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


_DEFAULT_CONTENT_STORE = object()


class PgStorage:
    """Store crawl results in Postgres."""

    def __init__(self, dsn: str, content_store: object = _DEFAULT_CONTENT_STORE):
        self.dsn = dsn
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = False
        assert_public_table_columns(self._conn, "pages", PAGES_REQUIRED_COLUMNS)
        self._content_store = content_store
        self._count = 0
        self.page_writes = PageWriteStore(self)
        self.page_queries = PageQueryStore(self)
        self.runtime_stats = RuntimeStatsStore(self)

    def _get_content_store(self) -> R2ContentStore:
        if self._content_store is _DEFAULT_CONTENT_STORE:
            self._content_store = R2ContentStore.from_settings()
        return cast(R2ContentStore, self._content_store)

    def _finish_read(self) -> None:
        """Close read-only transactions so API requests do not hold relation locks."""
        self._conn.commit()

    def save(self, result: CrawlResult | Mapping[str, object]) -> StorageSaveResult:
        """Save a single crawl result and return persistence telemetry."""
        return self.page_writes.save(result)

    def save_many(
        self,
        results: list[CrawlResult | Mapping[str, object]],
    ) -> list[StorageSaveResult]:
        """Save multiple crawl results in one transaction."""
        return self.page_writes.save_many(results)

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
        return self.page_queries.list_pages(since=since, limit=limit, offset=offset, host=host)

    def upsert_runtime_stats(self, component: str, payload: Mapping[str, object]) -> None:
        """Store runtime crawler stats for API consumption."""
        self.runtime_stats.upsert(component, payload)

    def get_runtime_stats(self, component: str | None = None) -> dict[str, object]:
        """Fetch runtime crawler stats snapshots."""
        return self.runtime_stats.get(component)

    def get_page(self, url_hash: str) -> dict | None:
        """Get a single page with full content."""
        return self.page_queries.get_page(url_hash)

    def get_runtime_stats_summary(self) -> dict:
        """Get fast operator stats from the persisted runtime snapshot."""
        return self.runtime_stats.summary()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
