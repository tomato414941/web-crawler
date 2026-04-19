"""Crawler engine with async concurrency."""

from __future__ import annotations

import asyncio
import concurrent.futures
from collections import Counter
from dataclasses import dataclass
import logging
import re
import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import httpx
import typer

from .config import settings
from .content_policy import should_extract_links, should_store_text_content
from .core import HttpFetcher, Response
from .discovery import PageSignals, rank_discovered_url, rank_seed_url, seed_hosts_from_urls
from .error_stats import categorize_crawl_error
from .host_manager import HostManager
from .host_store import HostStore
from .url_ledger import (
    CrawlTask,
    INTENT_EXPLORE,
    INTENT_REFRESH,
    LEASE_STRATEGY_HOST_FIRST,
    LEASE_STRATEGY_URL_ORDER,
    SCHEDULER_SURFACE_SCHEDULED,
    SCHEDULER_SURFACE_RUNNABLE,
    SCHEDULER_SURFACE_NORMAL,
    SCHEDULER_SURFACE_REFRESH,
    UrlLedger,
)
from .output import StreamingOutputWriter
from .result import CrawlFailure, CrawlResult, CrawlStageTimings
from .urls import extract_links

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .storage import PgStorage

# Workers wait this many idle ticks (× 0.1s) before giving up
_WORKER_PATIENCE = 50
_PUBLISHER_SENTINEL = object()
_FINALIZER_SENTINEL = object()
_PARSER_SENTINEL = object()
_TITLE_PATTERN = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_META_ROBOTS_PATTERN = re.compile(
    r'<meta[^>]+name=["\']robots["\'][^>]+content=["\']([^"\']+)["\']',
    re.IGNORECASE | re.DOTALL,
)


def _split_worker_pools(concurrency: int) -> tuple[int, int]:
    """Split worker capacity into normal and refresh pools."""
    total = max(1, concurrency)
    refresh = 1 if total >= 4 else 0
    normal = total - refresh
    return normal, refresh


def _elapsed_ms(started_at: float) -> float:
    """Return elapsed wall-clock time in milliseconds."""
    return round((time.perf_counter() - started_at) * 1000, 1)


def _format_timings(timings: CrawlStageTimings | None) -> str:
    """Format per-stage timings for logs."""
    if timings is None:
        return ""
    return (
        "lease=%0.1fms precheck=%0.1fms fetch=%0.1fms request=%0.1fms body=%0.1fms parse=%0.1fms "
        "scheduler=%0.1fms persist=%0.1fms output=%0.1fms "
        "parse_q_wait=%0.1fms finalize_q_wait=%0.1fms publish_q_wait=%0.1fms process=%0.1fms slot=%0.1fms "
        "parse_q_depth=%d finalize_q_depth=%d publish_q_depth=%d"
    ) % (
        timings.lease_ms,
        timings.precheck_ms,
        timings.fetch_ms,
        timings.fetch_request_ms,
        timings.fetch_body_read_ms,
        timings.parse_ms,
        timings.scheduler_ms,
        timings.persist_ms,
        timings.output_ms,
        timings.parse_queue_wait_ms,
        timings.finalize_queue_wait_ms,
        timings.publish_queue_wait_ms,
        timings.process_ms,
        timings.slot_ms,
        timings.parse_queue_depth,
        timings.finalize_queue_depth,
        timings.publish_queue_depth,
    )


@dataclass(slots=True)
class _FetchedPage:
    """Fetched page handed from fetch workers to parse workers."""

    task: CrawlTask
    response: Response
    timings: CrawlStageTimings
    process_started: float
    enqueued_at: float = 0.0
    queue_depth: int = 0


@dataclass(frozen=True, slots=True)
class _LeaseLane:
    """Operational lease surface for one worker pool, not a first-class scheduler concept."""

    runnable_surface: str
    lease_strategy: str
    intent: str | None = None


@dataclass(slots=True)
class _PublishItem:
    """Finalized result handed from finalizer to publisher."""

    result: CrawlResult
    enqueued_at: float = 0.0
    queue_depth: int = 0


@dataclass(slots=True)
class _FinalizeItem:
    """Parsed payload handed from parser to finalizer."""

    parsed: _ParsedPage | None = None
    failed: _FailedTask | None = None
    skipped: _SkippedTask | None = None
    enqueued_at: float = 0.0
    queue_depth: int = 0


@dataclass(slots=True)
class _ParsedPage:
    """Parsed page handed from parse workers to finalizer workers."""

    task: CrawlTask
    result: CrawlResult
    new_tasks: list[CrawlTask]
    process_started: float


@dataclass(slots=True)
class _FailedTask:
    """Failed crawl handed to finalizer for scheduler mutation."""

    task: CrawlTask
    failure: CrawlFailure
    process_started: float
    mark_done: bool = False
    record_success: bool = False
    record_error: bool = False
    backoff_seconds: float | None = None


@dataclass(slots=True)
class _SkippedTask:
    """Skipped crawl handed to finalizer for non-error scheduler mutation."""

    task: CrawlTask
    reason: str
    timings: CrawlStageTimings
    process_started: float


class CrawlerEngine:
    """Async crawler engine with concurrent processing."""

    def __init__(
        self,
        start_url: str = "",
        max_pages: int = 100,
        same_host: bool = True,
        use_browser: bool = False,
        delay: float = 1.0,
        concurrency: int = 5,
        output_writer: StreamingOutputWriter | None = None,
        pg_storage: "PgStorage | None" = None,
        url_ledger: UrlLedger | None = None,
        host_manager: HostManager | None = None,
        host_store: HostStore | None = None,
        seed_urls: list[str] | None = None,
    ):
        self.start_url = start_url
        self.max_pages = max_pages
        self.same_host = same_host
        self.use_browser = use_browser
        self.concurrency = concurrency
        self.output_writer = output_writer
        self.pg_storage = pg_storage
        self.parser_workers = max(1, min(concurrency, 4))
        self.max_inflight_requests_per_host = max(1, settings.max_inflight_requests_per_host)
        self.normal_workers, self.refresh_workers = _split_worker_pools(concurrency)
        # Compatibility fields for older runtime consumers; execution uses normal_workers.
        self.runnable_workers = self.normal_workers
        self.scheduled_workers = 0
        self._publisher_executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._publisher_storage = None
        self._finalizer_executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._finalizer_storage = None
        self._finalizer_scheduler: UrlLedger | None = None
        self._finalizer_host_store: HostStore | None = None

        self.start_host = urlparse(start_url).netloc if start_url else ""
        self.seed_hosts = seed_hosts_from_urls(seed_urls or [])
        if self.start_host:
            self.seed_hosts.add(self.start_host.lower())
        if url_ledger:
            self.scheduler = url_ledger
        elif pg_storage:
            self.scheduler = UrlLedger(pg_storage.conn)
        else:
            raise ValueError("Postgres connection required for scheduler state")

        if host_store is None and pg_storage is not None:
            host_store = HostStore(pg_storage.conn, default_delay=delay)
        self.host_store = host_store
        if self.host_store is not None:
            self.scheduler.attach_host_store(self.host_store)
        self.host_ledger_store = getattr(self.scheduler, "host_ledger_store", None)

        if host_manager:
            self.host_manager = host_manager
            self._owns_host_manager = False
            if hasattr(self.host_manager, "attach_store"):
                self.host_manager.attach_store(self.host_store)
            if self.host_ledger_store is not None and hasattr(
                self.host_manager, "attach_host_ledger_store"
            ):
                self.host_manager.attach_host_ledger_store(self.host_ledger_store)
        else:
            self.host_manager = HostManager(
                user_agent=settings.user_agent,
                default_delay=delay,
                host_store=self.host_store,
                host_ledger_store=self.host_ledger_store,
            )
            self._owns_host_manager = True

        if use_browser:
            from .core import get_browser_fetcher

            self.fetcher = get_browser_fetcher()(timeout=30.0)
        else:
            self.fetcher = HttpFetcher(timeout=settings.timeout)

        self.results: list[dict] = []
        self.pages_crawled = 0
        self.failure_breakdown: dict[str, int] = {}
        self._running = False
        self._claimed_pages = 0
        self._page_lock = asyncio.Lock()
        self._lease_lock = asyncio.Lock()
        self._failure_counts: Counter[str] = Counter()
        self._active_host_counts: Counter[str] = Counter()
        self._parse_queue: asyncio.Queue[_FetchedPage | object] | None = None
        self._finalize_queue: asyncio.Queue[_FinalizeItem | object] | None = None
        self._publish_queue: asyncio.Queue[_PublishItem | object] | None = None
        self._parse_queue_wait_last_ms = 0.0
        self._finalize_queue_wait_last_ms = 0.0
        self._publish_queue_wait_last_ms = 0.0
        self._parse_queue_wait_max_ms = 0.0
        self._finalize_queue_wait_max_ms = 0.0
        self._publish_queue_wait_max_ms = 0.0
        self._parse_queue_depth_max = 0
        self._finalize_queue_depth_max = 0
        self._publish_queue_depth_max = 0

    def snapshot_runtime_stats(self) -> dict[str, object]:
        """Return live queue/backpressure stats for external observers."""
        return {
            "running": self._running,
            "pages_crawled": self.pages_crawled,
            "claimed_pages": self._claimed_pages,
            "max_pages": self.max_pages,
            "concurrency": self.concurrency,
            "parser_workers": self.parser_workers,
            "normal_workers": self.normal_workers,
            "runnable_workers": self.runnable_workers,
            "scheduled_workers": self.scheduled_workers,
            "refresh_workers": self.refresh_workers,
            "active_hosts": len(self._active_host_counts),
            "parse_queue_size": self._parse_queue.qsize() if self._parse_queue is not None else 0,
            "finalize_queue_size": self._finalize_queue.qsize()
            if self._finalize_queue is not None
            else 0,
            "publish_queue_size": self._publish_queue.qsize()
            if self._publish_queue is not None
            else 0,
            "parse_queue_wait_last_ms": self._parse_queue_wait_last_ms,
            "finalize_queue_wait_last_ms": self._finalize_queue_wait_last_ms,
            "publish_queue_wait_last_ms": self._publish_queue_wait_last_ms,
            "parse_queue_wait_max_ms": self._parse_queue_wait_max_ms,
            "finalize_queue_wait_max_ms": self._finalize_queue_wait_max_ms,
            "publish_queue_wait_max_ms": self._publish_queue_wait_max_ms,
            "parse_queue_depth_max": self._parse_queue_depth_max,
            "finalize_queue_depth_max": self._finalize_queue_depth_max,
            "publish_queue_depth_max": self._publish_queue_depth_max,
            "failure_breakdown": dict(self._failure_counts),
        }

    async def __aenter__(self) -> "CrawlerEngine":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """Close all resources."""
        if self._publisher_executor is not None:
            self._publisher_executor.shutdown(wait=True, cancel_futures=False)
            self._publisher_executor = None
        if self._publisher_storage is not None:
            self._publisher_storage.close()
            self._publisher_storage = None
        if self._finalizer_executor is not None:
            self._finalizer_executor.shutdown(wait=True, cancel_futures=False)
        self._finalizer_executor = None
        self._finalizer_scheduler = None
        self._finalizer_host_store = None
        if self._finalizer_storage is not None:
            self._finalizer_storage.close()
            self._finalizer_storage = None
        if hasattr(self.fetcher, "close"):
            await self.fetcher.close()
        if self._owns_host_manager:
            await self.host_manager.close()

    def _finalize_sync(
        self,
        task: CrawlTask,
        new_tasks: list[CrawlTask],
        request_latency_ms: float | None,
    ) -> None:
        """Apply durable scheduler mutations on the dedicated finalizer connection."""
        scheduler = self._finalizer_scheduler or self.scheduler
        if new_tasks:
            if hasattr(scheduler, "discover_many") and hasattr(scheduler, "admit_discovered_tasks"):
                scheduler.discover_many(new_tasks)
                scheduler.admit_discovered_tasks(new_tasks)
            else:
                scheduler.place_many(new_tasks)

        host_store = self._finalizer_host_store or self._host_store_for_success_tracking()
        if host_store is not None:
            host_store.record_success(
                self._host_key_for_url(task.url),
                request_latency_ms=request_latency_ms,
            )

        scheduler.mark_done(task.url, lease_token=task.lease_token)

    def _host_store_for_success_tracking(self) -> HostStore | None:
        """Return the durable store used for host success resets when available."""
        return getattr(self.host_manager, "_host_store", None)

    def _enqueue_finalize_item(self, item: _FinalizeItem) -> None:
        """Track finalize queue depth before handing work to finalizer workers."""
        self._finalize_queue_depth_max = max(self._finalize_queue_depth_max, item.queue_depth)

    def _record_error_runtime(self, url: str) -> float:
        """Advance runtime failure state without requiring durable writes on the event loop."""
        if hasattr(self.host_manager, "record_error_runtime"):
            return self.host_manager.record_error_runtime(url)
        self.host_manager.record_error(url)
        return 0.0

    def _host_inflight_budget(self, host_key: str) -> int:
        """Resolve the allowed concurrent in-flight requests for a host."""
        default_budget = self.max_inflight_requests_per_host
        if hasattr(self.host_manager, "get_host_budget"):
            budget = self.host_manager.get_host_budget(
                host_key,
                default_budget=default_budget,
            )
            return max(1, int(budget))
        return default_budget

    def _finalize_failed_sync(self, failed: _FailedTask) -> None:
        """Apply durable failure mutations on the dedicated finalizer connection."""
        scheduler = self._finalizer_scheduler or self.scheduler
        host_store = self._finalizer_host_store or self._host_store_for_success_tracking()
        if failed.record_success and host_store is not None:
            host_store.record_success(self._host_key_for_url(failed.task.url))
        if failed.record_error and host_store is not None:
            host_store.record_failure(
                self._host_key_for_url(failed.task.url),
                backoff_seconds=failed.backoff_seconds or 0.0,
            )

        if failed.mark_done:
            scheduler.mark_done(failed.task.url, lease_token=failed.task.lease_token)
            return

        scheduler.mark_failed(
            failed.task.url,
            retryable=failed.failure.retryable,
            error=failed.failure.error,
            backoff_seconds=failed.backoff_seconds,
            lease_token=failed.task.lease_token,
        )

    async def _finalize_failed_task(self, failed: _FailedTask) -> CrawlFailure:
        """Apply scheduler mutations for a failed crawl outside fetch and parse workers."""
        failure = failed.failure
        scheduler_started = time.perf_counter()
        if self._finalizer_executor is not None:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                self._finalizer_executor,
                self._finalize_failed_sync,
                failed,
            )
        else:
            self._finalize_failed_sync(failed)
        failure.timings.scheduler_ms += _elapsed_ms(scheduler_started)
        failure.timings.process_ms = _elapsed_ms(failed.process_started)
        return failure

    def _is_valid_url(self, url: str) -> bool:
        """Check if URL should be crawled."""
        if self.same_host:
            return urlparse(url).netloc == self.start_host
        return True

    def _build_seed_task(self, url: str) -> CrawlTask:
        """Build the initial scheduler task for an explicit seed URL."""
        decision = rank_seed_url(url)
        return CrawlTask(
            url=url,
            priority=decision.priority,
            runnable_surface=SCHEDULER_SURFACE_RUNNABLE,
            intent=INTENT_EXPLORE,
        )

    def _build_page_signals(self, response) -> PageSignals:
        """Extract lightweight ranking signals from a fetched page."""
        headers = {key.lower(): value for key, value in response.headers.items()}
        content_type = headers.get("content-type", "")
        title = None
        meta_robots = None

        if "html" in content_type.lower():
            if match := _TITLE_PATTERN.search(response.text):
                title = match.group(1).strip()[:300]
            if match := _META_ROBOTS_PATTERN.search(response.text):
                meta_robots = match.group(1).strip().lower()

        return PageSignals(
            content_type=content_type,
            content_length=len(response.content),
            title=title,
            meta_robots=meta_robots,
        )

    def _build_discovered_tasks(
        self,
        parent_url: str,
        links: list[str],
        parent_signals: PageSignals | None = None,
    ) -> list[CrawlTask]:
        """Assign ranking metadata to discovered outlinks before enqueueing."""
        tasks: list[CrawlTask] = []
        for link in links:
            if not self._is_valid_url(link):
                continue
            decision = rank_discovered_url(
                parent_url=parent_url,
                url=link,
                seed_hosts=self.seed_hosts,
                parent_signals=parent_signals,
            )
            tasks.append(
                CrawlTask(
                    url=link,
                    priority=decision.priority,
                    runnable_surface=SCHEDULER_SURFACE_SCHEDULED,
                    intent=INTENT_EXPLORE,
                    source_url=parent_url,
                )
            )
        if hasattr(self.scheduler, "preview_tasks"):
            return self.scheduler.preview_tasks(tasks)
        return tasks

    def _finalize_skipped_sync(self, skipped: _SkippedTask) -> None:
        """Apply durable scheduler mutation for a skipped crawl."""
        scheduler = self._finalizer_scheduler or self.scheduler
        scheduler.mark_done(skipped.task.url, lease_token=skipped.task.lease_token)

    async def _finalize_skipped_task(self, skipped: _SkippedTask) -> _SkippedTask:
        """Apply scheduler mutations for a skipped crawl outside fetch workers."""
        scheduler_started = time.perf_counter()
        if self._finalizer_executor is not None:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                self._finalizer_executor,
                self._finalize_skipped_sync,
                skipped,
            )
        else:
            self._finalize_skipped_sync(skipped)
        skipped.timings.scheduler_ms += _elapsed_ms(scheduler_started)
        skipped.timings.process_ms = _elapsed_ms(skipped.process_started)
        return skipped

    async def _process_url(
        self, task: CrawlTask
    ) -> _FetchedPage | _FailedTask | _SkippedTask | None:
        """Process a single URL."""
        url = task.url
        timings = CrawlStageTimings()
        process_started = time.perf_counter()

        precheck_started = time.perf_counter()
        if not await self.host_manager.is_allowed(url):
            timings.precheck_ms = _elapsed_ms(precheck_started)
            return _SkippedTask(
                task=task,
                reason="robots_denied",
                timings=timings,
                process_started=process_started,
            )

        await self.host_manager.wait_for_rate_limit(url)
        timings.precheck_ms = _elapsed_ms(precheck_started)

        fetch_started = time.perf_counter()
        try:
            response = await self.fetcher.fetch(url)
            timings.fetch_ms = _elapsed_ms(fetch_started)
            timings.fetch_request_ms = getattr(response, "fetch_request_ms", 0.0)
            timings.fetch_body_read_ms = getattr(response, "fetch_body_read_ms", 0.0)

            if response.status >= 400:
                if 400 <= response.status < 500:
                    if response.status in {401, 403}:
                        backoff_seconds = self._record_error_runtime(url)
                        failed = _FailedTask(
                            task=task,
                            failure=CrawlFailure(
                                url=response.url,
                                error=f"http_{response.status}",
                                retryable=False,
                                timings=timings,
                            ),
                            process_started=process_started,
                            record_error=True,
                            backoff_seconds=backoff_seconds,
                        )
                    else:
                        if hasattr(self.host_manager, "record_success_runtime"):
                            self.host_manager.record_success_runtime(url)
                        else:
                            self.host_manager.record_success(url)
                        failed = _FailedTask(
                            task=task,
                            failure=CrawlFailure(
                                url=response.url,
                                error=f"http_{response.status}",
                                retryable=False,
                                timings=timings,
                            ),
                            process_started=process_started,
                            mark_done=True,
                            record_success=True,
                        )
                else:
                    backoff_seconds = self._record_error_runtime(url)
                    retryable = True
                    failed = _FailedTask(
                        task=task,
                        failure=CrawlFailure(
                            url=response.url,
                            error=f"http_{response.status}",
                            retryable=True,
                            timings=timings,
                        ),
                        process_started=process_started,
                        record_error=True,
                        backoff_seconds=backoff_seconds,
                    )
                return failed

            return _FetchedPage(
                task=task,
                response=response,
                timings=timings,
                process_started=process_started,
            )

        except httpx.TimeoutException:
            timings.fetch_ms = _elapsed_ms(fetch_started)
            backoff_seconds = self._record_error_runtime(url)
            return _FailedTask(
                task=task,
                failure=CrawlFailure(
                    url=url,
                    error="timeout",
                    retryable=True,
                    timings=timings,
                ),
                process_started=process_started,
                record_error=True,
                backoff_seconds=backoff_seconds,
            )

        except httpx.ConnectError:
            timings.fetch_ms = _elapsed_ms(fetch_started)
            backoff_seconds = self._record_error_runtime(url)
            return _FailedTask(
                task=task,
                failure=CrawlFailure(
                    url=url,
                    error="connection_error",
                    retryable=True,
                    timings=timings,
                ),
                process_started=process_started,
                record_error=True,
                backoff_seconds=backoff_seconds,
            )

        except Exception as e:
            timings.fetch_ms = _elapsed_ms(fetch_started)
            backoff_seconds = self._record_error_runtime(url)
            retryable = self.host_manager.should_retry(url)
            return _FailedTask(
                task=task,
                failure=CrawlFailure(
                    url=url,
                    error=str(e),
                    retryable=retryable,
                    timings=timings,
                ),
                process_started=process_started,
                record_error=True,
                backoff_seconds=backoff_seconds,
            )

    def _host_key_for_url(self, url: str) -> str:
        """Return the host key used for in-flight request limiting."""
        return urlparse(url).netloc.lower()

    async def _release_active_host(self, url: str) -> None:
        """Release in-flight host reservations after fetch processing finishes."""
        host_key = self._host_key_for_url(url)
        async with self._lease_lock:
            current = self._active_host_counts.get(host_key, 0)
            if current <= 1:
                self._active_host_counts.pop(host_key, None)
            else:
                self._active_host_counts[host_key] = current - 1

    async def _claim_page_slot(self) -> bool:
        """Reserve capacity so concurrent workers do not exceed max_pages."""
        async with self._page_lock:
            if self.pages_crawled + self._claimed_pages >= self.max_pages:
                return False
            self._claimed_pages += 1
            return True

    async def _release_page_slot(self, success: bool):
        """Release a reserved page slot and commit successful crawls."""
        async with self._page_lock:
            self._claimed_pages -= 1
            if success:
                self.pages_crawled += 1

    async def _worker(
        self,
        worker_id: int,
        *,
        lease_lane: _LeaseLane,
    ):
        """Worker coroutine that processes URLs from a dedicated runnable surface."""
        idle_ticks = 0

        while self._running:
            if not await self._claim_page_slot():
                break

            slot_started = time.perf_counter()
            lease_started = time.perf_counter()
            task = await self._lease_task(lease_lane=lease_lane)
            lease_ms = _elapsed_ms(lease_started)
            if not task:
                await self._release_page_slot(success=False)
                idle_ticks += 1
                if idle_ticks >= _WORKER_PATIENCE:
                    break
                await asyncio.sleep(0.1)
                continue

            idle_ticks = 0
            try:
                result = await self._process_url(task)
            finally:
                await self._release_active_host(task.url)
            if not result:
                await self._release_page_slot(success=False)
                continue

            if isinstance(result, _SkippedTask):
                result.timings.lease_ms = lease_ms
                result.timings.slot_ms = _elapsed_ms(slot_started)
                await self._release_page_slot(success=False)
                if self._finalize_queue is not None:
                    queue_item = _FinalizeItem(
                        skipped=result,
                        enqueued_at=time.perf_counter(),
                        queue_depth=self._finalize_queue.qsize(),
                    )
                    self._enqueue_finalize_item(queue_item)
                    await self._finalize_queue.put(queue_item)
                else:
                    skipped = await self._finalize_skipped_task(result)
                    logger.info(
                        "Skipped %s: %s (%s)",
                        skipped.task.url,
                        skipped.reason,
                        _format_timings(skipped.timings),
                    )
            elif isinstance(result, _FailedTask):
                result.failure.timings.lease_ms = lease_ms
                result.failure.timings.slot_ms = _elapsed_ms(slot_started)
                await self._release_page_slot(success=False)
                category = categorize_crawl_error(result.failure.error)
                if category:
                    self._failure_counts[category] += 1
                if self._finalize_queue is not None:
                    queue_item = _FinalizeItem(
                        failed=result,
                        enqueued_at=time.perf_counter(),
                        queue_depth=self._finalize_queue.qsize(),
                    )
                    self._enqueue_finalize_item(queue_item)
                    await self._finalize_queue.put(queue_item)
                else:
                    failure = await self._finalize_failed_task(result)
                    logger.warning(
                        "Failed %s: %s (%s)",
                        failure.url,
                        failure.error,
                        _format_timings(failure.timings),
                    )
            else:
                await self._release_page_slot(success=True)
                result.timings.lease_ms = lease_ms
                result.timings.slot_ms = _elapsed_ms(slot_started)
                if self._parse_queue is not None:
                    result.queue_depth = self._parse_queue.qsize()
                    self._parse_queue_depth_max = max(
                        self._parse_queue_depth_max,
                        result.queue_depth,
                    )
                    result.enqueued_at = time.perf_counter()
                    await self._parse_queue.put(result)
                else:
                    parsed = await self._parse_fetched_page(result)
                    finalized = await self._finalize_parsed_page(parsed)
                    finalized.timings.publish_queue_depth = 0
                    finalized.timings.publish_queue_wait_ms = 0.0
                    await self._publish_result(finalized)
                    logger.info(
                        "[%d/%d] %s (%s)",
                        self.pages_crawled,
                        self.max_pages,
                        finalized.url,
                        _format_timings(finalized.timings),
                    )

    def _prepare_parsed_payload(
        self,
        task: CrawlTask,
        response: Response,
    ) -> tuple[str, list[str], list[CrawlTask]]:
        """Prepare parsed content and discovered tasks away from the event loop."""
        content = (
            response.text
            if should_store_text_content(
                response.headers.get("content-type"),
                response.content,
            )
            else ""
        )
        outlinks: list[str] = []
        new_tasks: list[CrawlTask] = []

        if should_extract_links(
            response.headers.get("content-type"),
            response.content,
        ):
            links = extract_links(response.text, response.url)
            outlinks = links
            page_signals = self._build_page_signals(response)
            new_tasks = self._build_discovered_tasks(
                task.url,
                links,
                parent_signals=page_signals,
            )

        return content, outlinks, new_tasks

    async def _parse_fetched_page(self, fetched: _FetchedPage) -> _ParsedPage:
        """Parse fetched content into a publishable payload outside fetch workers."""
        task = fetched.task
        response = fetched.response
        timings = fetched.timings

        parse_started = time.perf_counter()
        content, outlinks, new_tasks = await asyncio.to_thread(
            self._prepare_parsed_payload,
            task,
            response,
        )
        timings.parse_ms = _elapsed_ms(parse_started)

        return _ParsedPage(
            task=task,
            new_tasks=new_tasks,
            process_started=fetched.process_started,
            result=CrawlResult(
                url=response.url,
                status=response.status,
                content_length=len(response.content),
                source_url=task.source_url,
                timestamp=time.time(),
                content=content,
                outlinks=outlinks,
                timings=timings,
            ),
        )

    async def _parser(self):
        """Drain fetched pages and parse them into crawl results."""
        if self._parse_queue is None:
            return

        while True:
            item = await self._parse_queue.get()
            if item is _PARSER_SENTINEL:
                self._parse_queue.task_done()
                break

            fetched = item
            fetched.timings.parse_queue_wait_ms = (
                _elapsed_ms(fetched.enqueued_at) if fetched.enqueued_at else 0.0
            )
            fetched.timings.parse_queue_depth = fetched.queue_depth
            self._parse_queue_wait_last_ms = fetched.timings.parse_queue_wait_ms
            self._parse_queue_wait_max_ms = max(
                self._parse_queue_wait_max_ms,
                fetched.timings.parse_queue_wait_ms,
            )
            self._parse_queue_depth_max = max(
                self._parse_queue_depth_max,
                fetched.queue_depth,
            )
            try:
                parsed = await self._parse_fetched_page(fetched)
                if self._finalize_queue is not None:
                    queue_item = _FinalizeItem(
                        parsed=parsed,
                        enqueued_at=time.perf_counter(),
                        queue_depth=self._finalize_queue.qsize(),
                    )
                    self._finalize_queue_depth_max = max(
                        self._finalize_queue_depth_max,
                        queue_item.queue_depth,
                    )
                    await self._finalize_queue.put(queue_item)
                else:
                    finalized = await self._finalize_parsed_page(parsed)
                    await self._publish_result(finalized)
                    logger.info(
                        "[%d/%d] %s (%s)",
                        self.pages_crawled,
                        self.max_pages,
                        finalized.url,
                        _format_timings(finalized.timings),
                    )
            except Exception as exc:
                backoff_seconds = self._record_error_runtime(fetched.task.url)
                failure = CrawlFailure(
                    url=fetched.task.url,
                    error=str(exc),
                    retryable=self.host_manager.should_retry(fetched.task.url),
                    timings=fetched.timings,
                )
                category = categorize_crawl_error(str(exc))
                if category:
                    self._failure_counts[category] += 1
                failed = _FailedTask(
                    task=fetched.task,
                    failure=failure,
                    process_started=fetched.process_started,
                    record_error=True,
                    backoff_seconds=backoff_seconds,
                )
                if self._finalize_queue is not None:
                    queue_item = _FinalizeItem(
                        failed=failed,
                        enqueued_at=time.perf_counter(),
                        queue_depth=self._finalize_queue.qsize(),
                    )
                    self._enqueue_finalize_item(queue_item)
                    await self._finalize_queue.put(queue_item)
                else:
                    finalized = await self._finalize_failed_task(failed)
                    logger.warning(
                        "Failed %s during parse: %s (%s)",
                        fetched.task.url,
                        exc,
                        _format_timings(finalized.timings),
                    )
            finally:
                self._parse_queue.task_done()

    async def _finalize_parsed_page(self, parsed: _ParsedPage) -> CrawlResult:
        """Apply scheduler mutations after parse and before persistence."""
        result = parsed.result
        scheduler_started = time.perf_counter()
        if self._finalizer_executor is not None:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                self._finalizer_executor,
                self._finalize_sync,
                parsed.task,
                parsed.new_tasks,
                parsed.result.timings.fetch_request_ms or parsed.result.timings.fetch_ms,
            )
            if hasattr(self.host_manager, "record_success_runtime"):
                self.host_manager.record_success_runtime(parsed.task.url)
            else:
                self.host_manager.record_success(parsed.task.url)
        else:
            if parsed.new_tasks:
                if hasattr(self.scheduler, "discover_many") and hasattr(
                    self.scheduler, "admit_discovered_tasks"
                ):
                    self.scheduler.discover_many(parsed.new_tasks)
                    self.scheduler.admit_discovered_tasks(parsed.new_tasks)
                else:
                    self.scheduler.place_many(parsed.new_tasks)
            self.host_manager.record_success(parsed.task.url)
            self.scheduler.mark_done(parsed.task.url, lease_token=parsed.task.lease_token)
        result.timings.scheduler_ms += _elapsed_ms(scheduler_started)
        result.timings.process_ms = _elapsed_ms(parsed.process_started)

        return result

    async def _publish_result(self, result: CrawlResult):
        """Persist crawl output outside fetch, parse, and finalize workers."""
        loop = asyncio.get_running_loop()
        storage = self._publisher_storage or self.pg_storage
        executor = self._publisher_executor

        if storage:
            persist_started = time.perf_counter()
            if executor is not None:
                await loop.run_in_executor(executor, storage.save, result)
            else:
                await asyncio.to_thread(storage.save, result)
            result.timings.persist_ms = _elapsed_ms(persist_started)
        if self.output_writer:
            output_started = time.perf_counter()
            if executor is not None:
                await loop.run_in_executor(executor, self.output_writer.write_one, result)
            else:
                await asyncio.to_thread(self.output_writer.write_one, result)
            result.timings.output_ms = _elapsed_ms(output_started)
        elif not storage:
            self.results.append(result.to_dict())

    async def _finalizer(self):
        """Drain parsed payloads and apply scheduler mutations before persistence."""
        if self._finalize_queue is None:
            return

        while True:
            item = await self._finalize_queue.get()
            if item is _FINALIZER_SENTINEL:
                self._finalize_queue.task_done()
                break

            queue_item = item
            queue_wait_ms = _elapsed_ms(queue_item.enqueued_at) if queue_item.enqueued_at else 0.0
            self._finalize_queue_wait_last_ms = queue_wait_ms
            self._finalize_queue_wait_max_ms = max(
                self._finalize_queue_wait_max_ms,
                queue_wait_ms,
            )
            self._finalize_queue_depth_max = max(
                self._finalize_queue_depth_max,
                queue_item.queue_depth,
            )
            try:
                if queue_item.parsed is not None:
                    parsed = queue_item.parsed
                    parsed.result.timings.finalize_queue_wait_ms = queue_wait_ms
                    parsed.result.timings.finalize_queue_depth = queue_item.queue_depth
                    result = await self._finalize_parsed_page(parsed)
                    if self._publish_queue is not None:
                        publish_item = _PublishItem(
                            result=result,
                            enqueued_at=time.perf_counter(),
                            queue_depth=self._publish_queue.qsize(),
                        )
                        self._publish_queue_depth_max = max(
                            self._publish_queue_depth_max,
                            publish_item.queue_depth,
                        )
                        await self._publish_queue.put(publish_item)
                    else:
                        await self._publish_result(result)
                        logger.info(
                            "[%d/%d] %s (%s)",
                            self.pages_crawled,
                            self.max_pages,
                            result.url,
                            _format_timings(result.timings),
                        )
                elif queue_item.skipped is not None:
                    skipped = queue_item.skipped
                    skipped.timings.finalize_queue_wait_ms = queue_wait_ms
                    skipped.timings.finalize_queue_depth = queue_item.queue_depth
                    skipped = await self._finalize_skipped_task(skipped)
                    logger.info(
                        "Skipped %s: %s (%s)",
                        skipped.task.url,
                        skipped.reason,
                        _format_timings(skipped.timings),
                    )
                else:
                    failed = queue_item.failed
                    failed.failure.timings.finalize_queue_wait_ms = queue_wait_ms
                    failed.failure.timings.finalize_queue_depth = queue_item.queue_depth
                    failure = await self._finalize_failed_task(failed)
                    logger.warning(
                        "Failed %s: %s (%s)",
                        failure.url,
                        failure.error,
                        _format_timings(failure.timings),
                    )
            finally:
                self._finalize_queue.task_done()

    async def _publisher(self):
        """Drain processed crawl results and perform blocking writes."""
        if self._publish_queue is None:
            return

        while True:
            item = await self._publish_queue.get()
            if item is _PUBLISHER_SENTINEL:
                self._publish_queue.task_done()
                break

            queue_item = item
            result = queue_item.result
            result.timings.publish_queue_wait_ms = (
                _elapsed_ms(queue_item.enqueued_at) if queue_item.enqueued_at else 0.0
            )
            result.timings.publish_queue_depth = queue_item.queue_depth
            self._publish_queue_wait_last_ms = result.timings.publish_queue_wait_ms
            self._publish_queue_wait_max_ms = max(
                self._publish_queue_wait_max_ms,
                result.timings.publish_queue_wait_ms,
            )
            self._publish_queue_depth_max = max(
                self._publish_queue_depth_max,
                queue_item.queue_depth,
            )
            try:
                await self._publish_result(result)
                logger.info(
                    "[%d/%d] %s (%s)",
                    self.pages_crawled,
                    self.max_pages,
                    result.url,
                    _format_timings(result.timings),
                )
            finally:
                self._publish_queue.task_done()

    async def _lease_task(
        self,
        *,
        lease_lane: _LeaseLane,
    ) -> CrawlTask | None:
        """Lease work for a specific runnable worker pool."""
        async with self._lease_lock:
            excluded_hosts = [
                host
                for host, count in self._active_host_counts.items()
                if count >= self._host_inflight_budget(host)
            ]
            task = self.scheduler.lease_next(
                runnable_surface=lease_lane.runnable_surface,
                lease_strategy=lease_lane.lease_strategy,
                exclude_hosts=excluded_hosts or None,
            )
            if task is not None:
                host_key = self._host_key_for_url(task.url)
                self._active_host_counts[host_key] += 1
            return task

    def _refresh_host_runnable_heads_read_model(self) -> None:
        """Refresh the loose host-first read model once before workers start."""
        rebuild = getattr(self.scheduler, "rebuild_host_runnable_heads", None)
        if rebuild is None:
            return
        started_at = time.perf_counter()
        try:
            rebuilt = rebuild(runnable_surface=SCHEDULER_SURFACE_NORMAL)
        except Exception:
            logger.exception(
                "Failed to refresh host runnable-head read model; using derived host-first fallback"
            )
            return
        logger.info(
            "Refreshed host runnable-head read model rows=%d elapsed=%0.1fms",
            rebuilt,
            _elapsed_ms(started_at),
        )

    async def crawl(self) -> list[dict]:
        """Run the crawler and return results."""
        self._running = True
        self.pages_crawled = 0
        self.failure_breakdown = {}
        self._failure_counts = Counter()
        self._claimed_pages = 0
        self._parse_queue_wait_last_ms = 0.0
        self._finalize_queue_wait_last_ms = 0.0
        self._publish_queue_wait_last_ms = 0.0
        self._parse_queue_wait_max_ms = 0.0
        self._finalize_queue_wait_max_ms = 0.0
        self._publish_queue_wait_max_ms = 0.0
        self._parse_queue_depth_max = 0
        self._finalize_queue_depth_max = 0
        self._publish_queue_depth_max = 0

        if self.start_url and self.scheduler.pending_count() == 0:
            self.scheduler.place(self._build_seed_task(self.start_url))

        self._refresh_host_runnable_heads_read_model()

        self._parse_queue = asyncio.Queue()
        self._finalize_queue = asyncio.Queue()
        self._publish_queue = asyncio.Queue()
        publisher_dsn = (
            getattr(self.pg_storage, "_dsn", None) if self.pg_storage is not None else None
        )
        if publisher_dsn and self._publisher_storage is None:
            from .storage import PgStorage

            self._publisher_storage = PgStorage(publisher_dsn)
            self._finalizer_storage = PgStorage(publisher_dsn)
            self._finalizer_scheduler = UrlLedger(self._finalizer_storage.conn)
            self._finalizer_host_store = HostStore(
                self._finalizer_storage.conn,
                default_delay=self.host_manager.default_delay,
            )
        if publisher_dsn and self._publisher_executor is None:
            self._publisher_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="crawler-publisher",
            )
        if publisher_dsn and self._finalizer_executor is None:
            self._finalizer_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="crawler-finalizer",
            )
        parsers = [asyncio.create_task(self._parser()) for _ in range(self.parser_workers)]
        finalizers = [asyncio.create_task(self._finalizer()) for _ in range(self.parser_workers)]
        publisher = asyncio.create_task(self._publisher())

        workers: list[asyncio.Task] = []
        worker_id = 0
        normal_lane = _LeaseLane(
            runnable_surface=SCHEDULER_SURFACE_NORMAL,
            lease_strategy=LEASE_STRATEGY_HOST_FIRST,
            intent=INTENT_EXPLORE,
        )
        refresh_lane = _LeaseLane(
            runnable_surface=SCHEDULER_SURFACE_REFRESH,
            lease_strategy=LEASE_STRATEGY_URL_ORDER,
            intent=INTENT_REFRESH,
        )
        for _ in range(self.normal_workers):
            workers.append(
                asyncio.create_task(
                    self._worker(
                        worker_id,
                        lease_lane=normal_lane,
                    )
                )
            )
            worker_id += 1
        for _ in range(self.refresh_workers):
            workers.append(
                asyncio.create_task(
                    self._worker(
                        worker_id,
                        lease_lane=refresh_lane,
                    )
                )
            )
            worker_id += 1

        try:
            await asyncio.gather(*workers)
            await self._parse_queue.join()
            for _ in range(self.parser_workers):
                await self._parse_queue.put(_PARSER_SENTINEL)
            await asyncio.gather(*parsers)

            await self._finalize_queue.join()
            for _ in range(len(finalizers)):
                await self._finalize_queue.put(_FINALIZER_SENTINEL)
            await asyncio.gather(*finalizers)

            await self._publish_queue.join()
            await self._publish_queue.put(_PUBLISHER_SENTINEL)
            await publisher
        finally:
            self._running = False
            self.failure_breakdown = dict(self._failure_counts)

        return self.results

    def stop(self):
        """Stop the crawler."""
        self._running = False


async def run_crawl(
    start_url: str,
    max_pages: int = 100,
    same_host: bool = True,
    output_file: str | None = None,
    use_browser: bool = False,
    delay: float = 1.0,
    concurrency: int = 5,
    include_content: bool = True,
    postgres_dsn: str | None = None,
):
    """Run a crawl and save results."""
    if not postgres_dsn:
        raise ValueError("--postgres is required")

    typer.echo(f"Starting crawl from {start_url}")
    typer.echo(f"Max pages: {max_pages}, Concurrency: {concurrency}")

    start_time = time.time()
    from .storage import PgStorage

    with PgStorage(postgres_dsn) as pg_storage:
        writer = (
            StreamingOutputWriter(output_file, include_content=include_content)
            if output_file
            else None
        )
        try:
            if writer:
                writer.__enter__()
            async with CrawlerEngine(
                start_url=start_url,
                max_pages=max_pages,
                same_host=same_host,
                use_browser=use_browser,
                delay=delay,
                concurrency=concurrency,
                output_writer=writer,
                pg_storage=pg_storage,
                seed_urls=[start_url],
            ) as engine:
                await engine.crawl()
                elapsed = time.time() - start_time
                typer.echo(f"\nCrawl complete: {engine.pages_crawled} pages in {elapsed:.1f}s")
                typer.echo(f"Postgres: {pg_storage.count} pages saved")
                if output_file:
                    typer.echo(f"Results saved to {output_file}")
                typer.echo(f"Scheduler stats: {engine.scheduler.stats()}")
        finally:
            if writer:
                writer.__exit__(None, None, None)
