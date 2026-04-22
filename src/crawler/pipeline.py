"""Crawler pipeline stages and runtime observability."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import logging
import time

from .core import Response
from .result import CrawlFailure, CrawlResult, CrawlStageTimings
from .url_ledger import CrawlTask

logger = logging.getLogger(__name__)

PUBLISHER_SENTINEL = object()
FINALIZER_SENTINEL = object()
PARSER_SENTINEL = object()


def elapsed_ms(started_at: float) -> float:
    """Return elapsed wall-clock time in milliseconds."""
    return round((time.perf_counter() - started_at) * 1000, 1)


@dataclass(slots=True)
class FetchedPage:
    """Fetched page handed from fetch workers to parse workers."""

    task: CrawlTask
    response: Response
    timings: CrawlStageTimings
    process_started: float
    enqueued_at: float = 0.0
    queue_depth: int = 0


@dataclass(slots=True)
class PublishItem:
    """Finalized result handed from finalizer to publisher."""

    result: CrawlResult
    enqueued_at: float = 0.0
    queue_depth: int = 0


@dataclass(slots=True)
class FinalizeItem:
    """Parsed payload handed from parser to finalizer."""

    parsed: ParsedPage | None = None
    failed: FailedTask | None = None
    skipped: SkippedTask | None = None
    enqueued_at: float = 0.0
    queue_depth: int = 0


@dataclass(slots=True)
class ParsedPage:
    """Parsed page handed from parse workers to finalizer workers."""

    task: CrawlTask
    result: CrawlResult
    new_tasks: list[CrawlTask]
    process_started: float


@dataclass(slots=True)
class FailedTask:
    """Failed crawl handed to finalizer for scheduler mutation."""

    task: CrawlTask
    failure: CrawlFailure
    process_started: float
    mark_done: bool = False
    record_success: bool = False
    record_error: bool = False
    backoff_seconds: float | None = None


@dataclass(slots=True)
class SkippedTask:
    """Skipped crawl handed to finalizer for non-error scheduler mutation."""

    task: CrawlTask
    reason: str
    timings: CrawlStageTimings
    process_started: float


@dataclass(slots=True)
class QueueStats:
    """Runtime queue wait and depth observations."""

    wait_last_ms: float = 0.0
    wait_max_ms: float = 0.0
    depth_max: int = 0

    def record_enqueue(self, depth: int) -> None:
        self.depth_max = max(self.depth_max, depth)

    def record_dequeue(self, enqueued_at: float, depth: int) -> float:
        wait_ms = elapsed_ms(enqueued_at) if enqueued_at else 0.0
        self.wait_last_ms = wait_ms
        self.wait_max_ms = max(self.wait_max_ms, wait_ms)
        self.depth_max = max(self.depth_max, depth)
        return wait_ms


class PipelineQueues:
    """Bounded queues and queue metrics for the crawl pipeline."""

    def __init__(self, maxsize: int) -> None:
        self.maxsize = max(0, maxsize)
        self.parse: asyncio.Queue[FetchedPage | object] = asyncio.Queue(maxsize=self.maxsize)
        self.finalize: asyncio.Queue[FinalizeItem | object] = asyncio.Queue(maxsize=self.maxsize)
        self.publish: asyncio.Queue[PublishItem | object] = asyncio.Queue(maxsize=self.maxsize)
        self.parse_stats = QueueStats()
        self.finalize_stats = QueueStats()
        self.publish_stats = QueueStats()

    def snapshot(self) -> dict[str, object]:
        return {
            "parse_queue_size": self.parse.qsize(),
            "finalize_queue_size": self.finalize.qsize(),
            "publish_queue_size": self.publish.qsize(),
            "parse_queue_wait_last_ms": self.parse_stats.wait_last_ms,
            "finalize_queue_wait_last_ms": self.finalize_stats.wait_last_ms,
            "publish_queue_wait_last_ms": self.publish_stats.wait_last_ms,
            "parse_queue_wait_max_ms": self.parse_stats.wait_max_ms,
            "finalize_queue_wait_max_ms": self.finalize_stats.wait_max_ms,
            "publish_queue_wait_max_ms": self.publish_stats.wait_max_ms,
            "parse_queue_depth_max": self.parse_stats.depth_max,
            "finalize_queue_depth_max": self.finalize_stats.depth_max,
            "publish_queue_depth_max": self.publish_stats.depth_max,
            "pipeline_queue_maxsize": self.maxsize,
        }

    @staticmethod
    def empty_snapshot(maxsize: int) -> dict[str, object]:
        return {
            "parse_queue_size": 0,
            "finalize_queue_size": 0,
            "publish_queue_size": 0,
            "parse_queue_wait_last_ms": 0.0,
            "finalize_queue_wait_last_ms": 0.0,
            "publish_queue_wait_last_ms": 0.0,
            "parse_queue_wait_max_ms": 0.0,
            "finalize_queue_wait_max_ms": 0.0,
            "publish_queue_wait_max_ms": 0.0,
            "parse_queue_depth_max": 0,
            "finalize_queue_depth_max": 0,
            "publish_queue_depth_max": 0,
            "pipeline_queue_maxsize": maxsize,
        }


class StageLiveness:
    """Per-stage progress counters exposed in runtime snapshots."""

    def __init__(self, *, include_kind: bool = False) -> None:
        self.include_kind = include_kind
        self.started = 0
        self.completed = 0
        self.failed = 0
        self.last_progress_at = 0.0
        self.current_url: str | None = None
        self.current_kind: str | None = None

    def start(self, *, url: str | None, kind: str | None = None) -> None:
        self.started += 1
        self.current_url = url
        self.current_kind = kind
        self.last_progress_at = time.time()

    def complete(self) -> None:
        self.completed += 1
        self.last_progress_at = time.time()

    def fail(self) -> None:
        self.failed += 1
        self.last_progress_at = time.time()

    def clear_current(self) -> None:
        self.current_url = None
        self.current_kind = None

    def snapshot(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "started": self.started,
            "completed": self.completed,
            "failed": self.failed,
            "last_progress_at": self.last_progress_at,
            "current_url": self.current_url,
        }
        if self.include_kind:
            payload["current_kind"] = self.current_kind
        return payload


def finalize_item_context(item: FinalizeItem) -> tuple[str, str | None]:
    if item.parsed is not None:
        return "success", item.parsed.task.url
    if item.skipped is not None:
        return "skipped", item.skipped.task.url
    if item.failed is not None:
        return "failed", item.failed.task.url
    return "unknown", None


class FinalizeStage:
    """Drain parsed payloads and apply scheduler mutations before persistence."""

    def __init__(
        self,
        *,
        finalize_queue: asyncio.Queue[FinalizeItem | object],
        publish_queue: asyncio.Queue[PublishItem | object] | None,
        finalize_stats: QueueStats,
        publish_stats: QueueStats,
        liveness: StageLiveness,
        finalize_parsed_page: Callable[[ParsedPage], Awaitable[CrawlResult]],
        finalize_skipped_task: Callable[[SkippedTask], Awaitable[SkippedTask]],
        finalize_failed_task: Callable[[FailedTask], Awaitable[CrawlFailure]],
        publish_result: Callable[[CrawlResult], Awaitable[None]],
        record_timing: Callable[[str, CrawlStageTimings | None], None],
        progress: Callable[[], tuple[int, int]],
        format_timings: Callable[[CrawlStageTimings | None], str],
    ) -> None:
        self.finalize_queue = finalize_queue
        self.publish_queue = publish_queue
        self.finalize_stats = finalize_stats
        self.publish_stats = publish_stats
        self.liveness = liveness
        self.finalize_parsed_page = finalize_parsed_page
        self.finalize_skipped_task = finalize_skipped_task
        self.finalize_failed_task = finalize_failed_task
        self.publish_result = publish_result
        self.record_timing = record_timing
        self.progress = progress
        self.format_timings = format_timings

    async def run(self) -> None:
        while True:
            item = await self.finalize_queue.get()
            if item is FINALIZER_SENTINEL:
                self.finalize_queue.task_done()
                break

            queue_item = item
            item_kind, item_url = finalize_item_context(queue_item)
            self.liveness.start(kind=item_kind, url=item_url)
            queue_wait_ms = self.finalize_stats.record_dequeue(
                queue_item.enqueued_at,
                queue_item.queue_depth,
            )
            try:
                await self._process_item(queue_item, queue_wait_ms)
                self.liveness.complete()
            except Exception:
                self.liveness.fail()
                logger.exception(
                    "Finalizer failed while processing queued crawl result: kind=%s url=%s",
                    item_kind,
                    item_url,
                )
            finally:
                self.liveness.clear_current()
                self.finalize_queue.task_done()

    async def _process_item(self, queue_item: FinalizeItem, queue_wait_ms: float) -> None:
        if queue_item.parsed is not None:
            parsed = queue_item.parsed
            parsed.result.timings.finalize_queue_wait_ms = queue_wait_ms
            parsed.result.timings.finalize_queue_depth = queue_item.queue_depth
            result = await self.finalize_parsed_page(parsed)
            if self.publish_queue is not None:
                publish_item = PublishItem(
                    result=result,
                    enqueued_at=time.perf_counter(),
                    queue_depth=self.publish_queue.qsize(),
                )
                self.publish_stats.record_enqueue(publish_item.queue_depth)
                await self.publish_queue.put(publish_item)
            else:
                await self.publish_result(result)
                self.record_timing("success", result.timings)
                pages_crawled, max_pages = self.progress()
                logger.info(
                    "[%d/%d] %s (%s)",
                    pages_crawled,
                    max_pages,
                    result.url,
                    self.format_timings(result.timings),
                )
        elif queue_item.skipped is not None:
            skipped = queue_item.skipped
            skipped.timings.finalize_queue_wait_ms = queue_wait_ms
            skipped.timings.finalize_queue_depth = queue_item.queue_depth
            skipped = await self.finalize_skipped_task(skipped)
            self.record_timing("skipped", skipped.timings)
            logger.info(
                "Skipped %s: %s (%s)",
                skipped.task.url,
                skipped.reason,
                self.format_timings(skipped.timings),
            )
        elif queue_item.failed is not None:
            failed = queue_item.failed
            failed.failure.timings.finalize_queue_wait_ms = queue_wait_ms
            failed.failure.timings.finalize_queue_depth = queue_item.queue_depth
            failure = await self.finalize_failed_task(failed)
            self.record_timing("failed", failure.timings)
            logger.warning(
                "Failed %s: %s (%s)",
                failure.url,
                failure.error,
                self.format_timings(failure.timings),
            )
        else:
            raise ValueError("finalize item has no payload")


class ParseStage:
    """Drain fetched pages and parse them into finalize-stage items."""

    def __init__(
        self,
        *,
        parse_queue: asyncio.Queue[FetchedPage | object],
        finalize_queue: asyncio.Queue[FinalizeItem | object],
        parse_stats: QueueStats,
        finalize_stats: QueueStats,
        liveness: StageLiveness,
        parse_fetched_page: Callable[[FetchedPage], Awaitable[ParsedPage]],
        build_failed_task: Callable[[FetchedPage, Exception], FailedTask],
        record_failure_category: Callable[[str], None],
    ) -> None:
        self.parse_queue = parse_queue
        self.finalize_queue = finalize_queue
        self.parse_stats = parse_stats
        self.finalize_stats = finalize_stats
        self.liveness = liveness
        self.parse_fetched_page = parse_fetched_page
        self.build_failed_task = build_failed_task
        self.record_failure_category = record_failure_category

    async def run(self) -> None:
        while True:
            item = await self.parse_queue.get()
            if item is PARSER_SENTINEL:
                self.parse_queue.task_done()
                break

            fetched = item
            self.liveness.start(kind="parse", url=fetched.task.url)
            fetched.timings.parse_queue_wait_ms = self.parse_stats.record_dequeue(
                fetched.enqueued_at,
                fetched.queue_depth,
            )
            fetched.timings.parse_queue_depth = fetched.queue_depth
            try:
                await self._process_item(fetched)
                self.liveness.complete()
            except Exception:
                self.liveness.fail()
                logger.exception(
                    "Parser failed while processing fetched page: url=%s",
                    fetched.task.url,
                )
            finally:
                self.liveness.clear_current()
                self.parse_queue.task_done()

    async def _process_item(self, fetched: FetchedPage) -> None:
        try:
            parsed = await self.parse_fetched_page(fetched)
            queue_item = FinalizeItem(
                parsed=parsed,
                enqueued_at=time.perf_counter(),
                queue_depth=self.finalize_queue.qsize(),
            )
        except Exception as exc:
            failed = self.build_failed_task(fetched, exc)
            self.record_failure_category(failed.failure.error)
            queue_item = FinalizeItem(
                failed=failed,
                enqueued_at=time.perf_counter(),
                queue_depth=self.finalize_queue.qsize(),
            )
        self.finalize_stats.record_enqueue(queue_item.queue_depth)
        await self.finalize_queue.put(queue_item)


class PublishStage:
    """Drain processed crawl results and perform blocking writes."""

    def __init__(
        self,
        *,
        publish_queue: asyncio.Queue[PublishItem | object],
        publish_stats: QueueStats,
        liveness: StageLiveness,
        publish_result: Callable[[CrawlResult], Awaitable[None]],
        record_timing: Callable[[str, CrawlStageTimings | None], None],
        progress: Callable[[], tuple[int, int]],
        format_timings: Callable[[CrawlStageTimings | None], str],
    ) -> None:
        self.publish_queue = publish_queue
        self.publish_stats = publish_stats
        self.liveness = liveness
        self.publish_result = publish_result
        self.record_timing = record_timing
        self.progress = progress
        self.format_timings = format_timings

    async def run(self) -> None:
        while True:
            item = await self.publish_queue.get()
            if item is PUBLISHER_SENTINEL:
                self.publish_queue.task_done()
                break

            queue_item = item
            result = queue_item.result
            self.liveness.start(url=result.url)
            result.timings.publish_queue_wait_ms = self.publish_stats.record_dequeue(
                queue_item.enqueued_at,
                queue_item.queue_depth,
            )
            result.timings.publish_queue_depth = queue_item.queue_depth
            try:
                await self.publish_result(result)
                self.record_timing("success", result.timings)
                pages_crawled, max_pages = self.progress()
                logger.info(
                    "[%d/%d] %s (%s)",
                    pages_crawled,
                    max_pages,
                    result.url,
                    self.format_timings(result.timings),
                )
                self.liveness.complete()
            except Exception:
                self.liveness.fail()
                logger.exception(
                    "Publisher failed while processing queued crawl result: url=%s",
                    result.url,
                )
            finally:
                self.liveness.clear_current()
                self.publish_queue.task_done()
