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
from .domain_manager import DomainManager
from .domain_store import DomainStore
from .error_stats import categorize_crawl_error
from .frontier import CrawlTask, Frontier
from .output import StreamingOutputWriter
from .result import CrawlFailure, CrawlResult, CrawlStageTimings
from .urls import extract_links

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .storage import PgStorage

# Workers wait this many idle ticks (× 0.1s) before giving up
_WORKER_PATIENCE = 50
_EXPLORATION_EVERY = 4
_PUBLISHER_SENTINEL = object()
_PARSER_SENTINEL = object()
_TITLE_PATTERN = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_META_ROBOTS_PATTERN = re.compile(
    r'<meta[^>]+name=["\']robots["\'][^>]+content=["\']([^"\']+)["\']',
    re.IGNORECASE | re.DOTALL,
)


def _elapsed_ms(started_at: float) -> float:
    """Return elapsed wall-clock time in milliseconds."""
    return round((time.perf_counter() - started_at) * 1000, 1)


def _format_timings(timings: CrawlStageTimings | None) -> str:
    """Format per-stage timings for logs."""
    if timings is None:
        return ""
    return (
        "lease=%0.1fms precheck=%0.1fms fetch=%0.1fms request=%0.1fms body=%0.1fms parse=%0.1fms "
        "frontier=%0.1fms persist=%0.1fms output=%0.1fms "
        "parse_q_wait=%0.1fms publish_q_wait=%0.1fms process=%0.1fms slot=%0.1fms "
        "parse_q_depth=%d publish_q_depth=%d"
    ) % (
        timings.lease_ms,
        timings.precheck_ms,
        timings.fetch_ms,
        timings.fetch_request_ms,
        timings.fetch_body_read_ms,
        timings.parse_ms,
        timings.frontier_ms,
        timings.persist_ms,
        timings.output_ms,
        timings.parse_queue_wait_ms,
        timings.publish_queue_wait_ms,
        timings.process_ms,
        timings.slot_ms,
        timings.parse_queue_depth,
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


@dataclass(slots=True)
class _PublishItem:
    """Parsed result handed from parser to publisher."""

    result: CrawlResult
    enqueued_at: float = 0.0
    queue_depth: int = 0


class CrawlerEngine:
    """Async crawler engine with concurrent processing."""

    def __init__(
        self,
        start_url: str = "",
        max_pages: int = 100,
        max_depth: int = 3,
        same_domain: bool = True,
        use_browser: bool = False,
        delay: float = 1.0,
        concurrency: int = 5,
        output_writer: StreamingOutputWriter | None = None,
        pg_storage: "PgStorage | None" = None,
        frontier: Frontier | None = None,
        domain_manager: DomainManager | None = None,
        domain_store: DomainStore | None = None,
        seed_urls: list[str] | None = None,
    ):
        self.start_url = start_url
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.same_domain = same_domain
        self.use_browser = use_browser
        self.concurrency = concurrency
        self.output_writer = output_writer
        self.pg_storage = pg_storage
        self.parser_workers = max(1, min(concurrency, 4))
        self.max_inflight_requests_per_host = max(1, settings.max_inflight_requests_per_host)
        self._publisher_executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._publisher_storage = None

        self.start_domain = urlparse(start_url).netloc if start_url else ""
        self.seed_hosts = seed_hosts_from_urls(seed_urls or [])
        if self.start_domain:
            self.seed_hosts.add(self.start_domain.lower())
        if frontier:
            self.frontier = frontier
        elif pg_storage:
            self.frontier = Frontier(pg_storage.conn)
        else:
            raise ValueError("Postgres connection required for frontier")

        if domain_store is None and pg_storage is not None:
            domain_store = DomainStore(pg_storage.conn, default_delay=delay)
        self.domain_store = domain_store
        if self.domain_store is not None:
            self.frontier.attach_domain_store(self.domain_store)

        if domain_manager:
            self.domain_manager = domain_manager
            self._owns_domain_manager = False
            if hasattr(self.domain_manager, "attach_store"):
                self.domain_manager.attach_store(self.domain_store)
        else:
            self.domain_manager = DomainManager(
                user_agent=settings.user_agent,
                default_delay=delay,
                domain_store=self.domain_store,
            )
            self._owns_domain_manager = True

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
        self._leases_issued = 0
        self._failure_counts: Counter[str] = Counter()
        self._active_host_counts: Counter[str] = Counter()
        self._parse_queue: asyncio.Queue[_FetchedPage | object] | None = None
        self._publish_queue: asyncio.Queue[_PublishItem | object] | None = None
        self._parse_queue_wait_last_ms = 0.0
        self._publish_queue_wait_last_ms = 0.0
        self._parse_queue_wait_max_ms = 0.0
        self._publish_queue_wait_max_ms = 0.0
        self._parse_queue_depth_max = 0
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
            "active_hosts": len(self._active_host_counts),
            "parse_queue_size": self._parse_queue.qsize() if self._parse_queue is not None else 0,
            "publish_queue_size": self._publish_queue.qsize()
            if self._publish_queue is not None
            else 0,
            "parse_queue_wait_last_ms": self._parse_queue_wait_last_ms,
            "publish_queue_wait_last_ms": self._publish_queue_wait_last_ms,
            "parse_queue_wait_max_ms": self._parse_queue_wait_max_ms,
            "publish_queue_wait_max_ms": self._publish_queue_wait_max_ms,
            "parse_queue_depth_max": self._parse_queue_depth_max,
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
        if hasattr(self.fetcher, "close"):
            await self.fetcher.close()
        if self._owns_domain_manager:
            await self.domain_manager.close()

    def _is_valid_url(self, url: str) -> bool:
        """Check if URL should be crawled."""
        if self.same_domain:
            return urlparse(url).netloc == self.start_domain
        return True

    def _build_seed_task(self, url: str) -> CrawlTask:
        """Build the initial frontier task for an explicit seed URL."""
        decision = rank_seed_url(url)
        return CrawlTask(
            url=url,
            depth=0,
            priority=decision.priority,
            discovery_kind=decision.discovery_kind,
            archetype=decision.archetype,
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
        depth: int,
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
                    depth=depth,
                    priority=decision.priority,
                    discovery_kind=decision.discovery_kind,
                    archetype=decision.archetype,
                    source_url=parent_url,
                )
            )
        return tasks

    async def _process_url(self, task: CrawlTask) -> _FetchedPage | CrawlFailure | None:
        """Process a single URL."""
        url = task.url
        timings = CrawlStageTimings()
        process_started = time.perf_counter()

        precheck_started = time.perf_counter()
        if not await self.domain_manager.is_allowed(url):
            timings.precheck_ms = _elapsed_ms(precheck_started)
            frontier_started = time.perf_counter()
            self.frontier.mark_done(url, lease_token=task.lease_token)
            timings.frontier_ms = _elapsed_ms(frontier_started)
            timings.process_ms = _elapsed_ms(process_started)
            return None

        await self.domain_manager.wait_for_rate_limit(url)
        timings.precheck_ms = _elapsed_ms(precheck_started)

        fetch_started = time.perf_counter()
        try:
            response = await self.fetcher.fetch(url)
            timings.fetch_ms = _elapsed_ms(fetch_started)
            timings.fetch_request_ms = getattr(response, "fetch_request_ms", 0.0)
            timings.fetch_body_read_ms = getattr(response, "fetch_body_read_ms", 0.0)

            if response.status >= 400:
                frontier_started = time.perf_counter()
                if 400 <= response.status < 500:
                    self.domain_manager.record_success(url)
                    self.frontier.mark_done(url, lease_token=task.lease_token)
                else:
                    self.domain_manager.record_error(url)
                    self.frontier.mark_failed(
                        url,
                        retryable=True,
                        error=f"http_{response.status}",
                        lease_token=task.lease_token,
                    )
                timings.frontier_ms = _elapsed_ms(frontier_started)
                timings.process_ms = _elapsed_ms(process_started)
                return CrawlFailure(
                    url=response.url,
                    error=f"http_{response.status}",
                    retryable=response.status >= 500,
                    depth=task.depth,
                    timings=timings,
                )

            return _FetchedPage(
                task=task,
                response=response,
                timings=timings,
                process_started=process_started,
            )

        except httpx.TimeoutException:
            timings.fetch_ms = _elapsed_ms(fetch_started)
            self.domain_manager.record_error(url)
            frontier_started = time.perf_counter()
            self.frontier.mark_failed(
                url,
                retryable=True,
                error="timeout",
                lease_token=task.lease_token,
            )
            timings.frontier_ms = _elapsed_ms(frontier_started)
            timings.process_ms = _elapsed_ms(process_started)
            return CrawlFailure(
                url=url,
                error="timeout",
                retryable=True,
                depth=task.depth,
                timings=timings,
            )

        except httpx.ConnectError:
            timings.fetch_ms = _elapsed_ms(fetch_started)
            self.domain_manager.record_error(url)
            frontier_started = time.perf_counter()
            self.frontier.mark_failed(
                url,
                retryable=True,
                error="connection_error",
                lease_token=task.lease_token,
            )
            timings.frontier_ms = _elapsed_ms(frontier_started)
            timings.process_ms = _elapsed_ms(process_started)
            return CrawlFailure(
                url=url,
                error="connection_error",
                retryable=True,
                depth=task.depth,
                timings=timings,
            )

        except Exception as e:
            timings.fetch_ms = _elapsed_ms(fetch_started)
            self.domain_manager.record_error(url)
            retryable = self.domain_manager.should_retry(url)
            frontier_started = time.perf_counter()
            self.frontier.mark_failed(
                url,
                retryable=retryable,
                error=str(e),
                lease_token=task.lease_token,
            )
            timings.frontier_ms = _elapsed_ms(frontier_started)
            timings.process_ms = _elapsed_ms(process_started)
            return CrawlFailure(
                url=url,
                error=str(e),
                retryable=False,
                depth=task.depth,
                timings=timings,
            )

    def _host_key_for_url(self, url: str) -> str:
        """Return the host key used for in-flight request limiting."""
        return urlparse(url).netloc.lower()

    async def _release_active_host(self, url: str) -> None:
        """Release an in-flight host reservation after fetch processing finishes."""
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

    async def _worker(self, worker_id: int):
        """Worker coroutine that processes URLs from the frontier."""
        idle_ticks = 0

        while self._running:
            if not await self._claim_page_slot():
                break

            slot_started = time.perf_counter()
            lease_started = time.perf_counter()
            task = await self._lease_task()
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

            if result.timings is None:
                result.timings = CrawlStageTimings()
            result.timings.lease_ms = lease_ms

            if isinstance(result, CrawlFailure):
                result.timings.slot_ms = _elapsed_ms(slot_started)
                await self._release_page_slot(success=False)
                category = categorize_crawl_error(result.error)
                if category:
                    self._failure_counts[category] += 1
                logger.warning(
                    "Failed %s: %s (%s)",
                    result.url,
                    result.error,
                    _format_timings(result.timings),
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
                    parsed.timings.publish_queue_depth = 0
                    parsed.timings.publish_queue_wait_ms = 0.0
                    await self._publish_result(parsed)
                    logger.info(
                        "[%d/%d] %s (%s)",
                        self.pages_crawled,
                        self.max_pages,
                        parsed.url,
                        _format_timings(parsed.timings),
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

        if task.depth < self.max_depth and should_extract_links(
            response.headers.get("content-type"),
            response.content,
        ):
            links = extract_links(response.text, response.url)
            outlinks = links
            page_signals = self._build_page_signals(response)
            new_tasks = self._build_discovered_tasks(
                task.url,
                links,
                task.depth + 1,
                parent_signals=page_signals,
            )

        return content, outlinks, new_tasks

    async def _parse_fetched_page(self, fetched: _FetchedPage) -> CrawlResult:
        """Parse fetched content and apply frontier updates outside fetch workers."""
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

        if new_tasks:
            frontier_started = time.perf_counter()
            self.frontier.add_many(new_tasks)
            timings.frontier_ms += _elapsed_ms(frontier_started)

        self.domain_manager.record_success(task.url)
        frontier_started = time.perf_counter()
        self.frontier.mark_done(task.url, lease_token=task.lease_token)
        timings.frontier_ms += _elapsed_ms(frontier_started)
        timings.process_ms = _elapsed_ms(fetched.process_started)

        return CrawlResult(
            url=response.url,
            status=response.status,
            content_length=len(response.content),
            depth=task.depth,
            source_url=task.source_url,
            timestamp=time.time(),
            content=content,
            outlinks=outlinks,
            timings=timings,
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
                result = await self._parse_fetched_page(fetched)
                if self._publish_queue is not None:
                    queue_item = _PublishItem(
                        result=result,
                        enqueued_at=time.perf_counter(),
                        queue_depth=self._publish_queue.qsize(),
                    )
                    self._publish_queue_depth_max = max(
                        self._publish_queue_depth_max,
                        queue_item.queue_depth,
                    )
                    await self._publish_queue.put(queue_item)
                else:
                    await self._publish_result(result)
                    logger.info(
                        "[%d/%d] %s (%s)",
                        self.pages_crawled,
                        self.max_pages,
                        result.url,
                        _format_timings(result.timings),
                    )
            except Exception as exc:
                self.domain_manager.record_error(fetched.task.url)
                self.frontier.mark_failed(
                    fetched.task.url,
                    retryable=self.domain_manager.should_retry(fetched.task.url),
                    error=str(exc),
                    lease_token=fetched.task.lease_token,
                )
                category = categorize_crawl_error(str(exc))
                if category:
                    self._failure_counts[category] += 1
                logger.warning(
                    "Failed %s during parse: %s (%s)",
                    fetched.task.url,
                    exc,
                    _format_timings(fetched.timings),
                )
            finally:
                self._parse_queue.task_done()

    async def _publish_result(self, result: CrawlResult):
        """Persist crawl output outside the fetch worker hot path."""
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

    async def _lease_task(self) -> CrawlTask | None:
        """Alternate between best-first and breadth-first leases."""
        async with self._lease_lock:
            prioritize_breadth = self._leases_issued % _EXPLORATION_EVERY == 0
            self._leases_issued += 1
            excluded_hosts = [
                host
                for host, count in self._active_host_counts.items()
                if count >= self.max_inflight_requests_per_host
            ]
            task = self.frontier.lease_next(
                prioritize_breadth=prioritize_breadth,
                exclude_domains=excluded_hosts or None,
            )
            if task is None and prioritize_breadth:
                task = self.frontier.lease_next(exclude_domains=excluded_hosts or None)
            if task is not None:
                host_key = self._host_key_for_url(task.url)
                self._active_host_counts[host_key] += 1
            return task

    async def crawl(self) -> list[dict]:
        """Run the crawler and return results."""
        self._running = True
        self.pages_crawled = 0
        self.failure_breakdown = {}
        self._failure_counts = Counter()
        self._claimed_pages = 0
        self._leases_issued = 0
        self._parse_queue_wait_last_ms = 0.0
        self._publish_queue_wait_last_ms = 0.0
        self._parse_queue_wait_max_ms = 0.0
        self._publish_queue_wait_max_ms = 0.0
        self._parse_queue_depth_max = 0
        self._publish_queue_depth_max = 0

        if self.start_url and self.frontier.pending_count() == 0:
            self.frontier.add(self._build_seed_task(self.start_url))

        self._parse_queue = asyncio.Queue()
        self._publish_queue = asyncio.Queue()
        publisher_dsn = getattr(self.pg_storage, "_dsn", None) if self.pg_storage is not None else None
        if publisher_dsn and self._publisher_storage is None:
            from .storage import PgStorage

            self._publisher_storage = PgStorage(publisher_dsn)
        if publisher_dsn and self._publisher_executor is None:
            self._publisher_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="crawler-publisher",
            )
        parsers = [asyncio.create_task(self._parser()) for _ in range(self.parser_workers)]
        publisher = asyncio.create_task(self._publisher())

        workers = [asyncio.create_task(self._worker(i)) for i in range(self.concurrency)]

        try:
            await asyncio.gather(*workers)
            for _ in range(self.parser_workers):
                await self._parse_queue.put(_PARSER_SENTINEL)
            await self._parse_queue.join()
            await asyncio.gather(*parsers)
            await self._publish_queue.put(_PUBLISHER_SENTINEL)
            await self._publish_queue.join()
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
    max_depth: int = 3,
    same_domain: bool = True,
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
    typer.echo(f"Max pages: {max_pages}, Max depth: {max_depth}, Concurrency: {concurrency}")

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
                max_depth=max_depth,
                same_domain=same_domain,
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
                typer.echo(f"Queue stats: {engine.frontier.stats()}")
        finally:
            if writer:
                writer.__exit__(None, None, None)
