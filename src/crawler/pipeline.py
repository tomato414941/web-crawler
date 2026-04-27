"""Crawler pipeline stages and runtime observability."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import logging
import time
from typing import Any

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
        finalize_parsed_pages: Callable[[list[ParsedPage]], Awaitable[list[CrawlResult]]]
        | None = None,
        success_batch_size: int = 1,
        success_batch_wait_ms: float = 0.0,
    ) -> None:
        self.finalize_queue = finalize_queue
        self.publish_queue = publish_queue
        self.finalize_stats = finalize_stats
        self.publish_stats = publish_stats
        self.liveness = liveness
        self.finalize_parsed_page = finalize_parsed_page
        self.finalize_parsed_pages = finalize_parsed_pages
        self.finalize_skipped_task = finalize_skipped_task
        self.finalize_failed_task = finalize_failed_task
        self.publish_result = publish_result
        self.record_timing = record_timing
        self.progress = progress
        self.format_timings = format_timings
        self.success_batch_size = max(1, success_batch_size)
        self.success_batch_wait_seconds = max(0.0, success_batch_wait_ms / 1000.0)
        self._pending_item: FinalizeItem | object | None = None

    async def run(self) -> None:
        while True:
            item = await self._next_item()
            if item is FINALIZER_SENTINEL:
                self.finalize_queue.task_done()
                break

            queue_item = item
            if queue_item.parsed is not None:
                await self._process_success_batch(queue_item)
                continue

            item_kind, item_url = finalize_item_context(queue_item)
            queue_wait_ms = self._start_item(queue_item, item_kind, item_url)
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

    async def _next_item(self) -> FinalizeItem | object:
        if self._pending_item is not None:
            item = self._pending_item
            self._pending_item = None
            return item
        return await self.finalize_queue.get()

    def _start_item(
        self,
        queue_item: FinalizeItem,
        item_kind: str,
        item_url: str | None,
    ) -> float:
        self.liveness.start(kind=item_kind, url=item_url)
        return self.finalize_stats.record_dequeue(
            queue_item.enqueued_at,
            queue_item.queue_depth,
        )

    async def _collect_success_batch(self, first_item: FinalizeItem) -> list[FinalizeItem]:
        batch = [first_item]
        waited = False
        while len(batch) < self.success_batch_size:
            try:
                if self.finalize_queue.qsize() > 0:
                    item = self.finalize_queue.get_nowait()
                elif not waited and self.success_batch_wait_seconds > 0:
                    waited = True
                    item = await asyncio.wait_for(
                        self.finalize_queue.get(),
                        timeout=self.success_batch_wait_seconds,
                    )
                else:
                    break
            except (asyncio.QueueEmpty, TimeoutError):
                break

            if item is FINALIZER_SENTINEL or item.parsed is None:
                self._pending_item = item
                break
            batch.append(item)
        return batch

    async def _process_success_batch(self, first_item: FinalizeItem) -> None:
        batch = await self._collect_success_batch(first_item)
        parsed_pages = [queue_item.parsed for queue_item in batch if queue_item.parsed is not None]
        for queue_item in batch:
            parsed = queue_item.parsed
            if parsed is None:
                continue
            queue_wait_ms = self._start_item(queue_item, "success", parsed.task.url)
            parsed.result.timings.finalize_queue_wait_ms = queue_wait_ms
            parsed.result.timings.finalize_queue_depth = queue_item.queue_depth
        try:
            results = await self._finalize_parsed_batch(parsed_pages)
            for result in results:
                await self._handle_success_result(result)
                self.liveness.complete()
        except Exception:
            for _queue_item in batch:
                self.liveness.fail()
            logger.exception(
                "Finalizer failed while processing success batch: size=%d",
                len(parsed_pages),
            )
        finally:
            self.liveness.clear_current()
            for _queue_item in batch:
                self.finalize_queue.task_done()

    async def _finalize_parsed_batch(self, parsed_pages: list[ParsedPage]) -> list[CrawlResult]:
        if self.finalize_parsed_pages is not None:
            return await self.finalize_parsed_pages(parsed_pages)
        return [await self.finalize_parsed_page(parsed) for parsed in parsed_pages]

    async def _handle_success_result(self, result: CrawlResult) -> None:
        if self.publish_queue is not None:
            publish_item = PublishItem(
                result=result,
                enqueued_at=time.perf_counter(),
                queue_depth=self.publish_queue.qsize(),
            )
            self.publish_stats.record_enqueue(publish_item.queue_depth)
            await self.publish_queue.put(publish_item)
            return
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


class FetchStage:
    """Lease URL work and hand fetch outcomes to downstream pipeline queues."""

    def __init__(
        self,
        *,
        parse_queue: asyncio.Queue[FetchedPage | object],
        finalize_queue: asyncio.Queue[FinalizeItem | object],
        parse_stats: QueueStats,
        finalize_stats: QueueStats,
        is_running: Callable[[], bool],
        claim_page_slot: Callable[[], Awaitable[bool]],
        release_page_slot: Callable[[bool], Awaitable[None]],
        lease_task: Callable[[float], Awaitable[tuple[CrawlTask | None, Any]]],
        process_url: Callable[[CrawlTask], Awaitable[FetchedPage | FailedTask | SkippedTask | None]],
        release_active_host: Callable[[str], Awaitable[None]],
        record_failure_category: Callable[[str], None],
        worker_patience: int,
    ) -> None:
        self.parse_queue = parse_queue
        self.finalize_queue = finalize_queue
        self.parse_stats = parse_stats
        self.finalize_stats = finalize_stats
        self.is_running = is_running
        self.claim_page_slot = claim_page_slot
        self.release_page_slot = release_page_slot
        self.lease_task = lease_task
        self.process_url = process_url
        self.release_active_host = release_active_host
        self.record_failure_category = record_failure_category
        self.worker_patience = worker_patience

    async def run(self) -> None:
        idle_ticks = 0

        while self.is_running():
            if not await self.claim_page_slot():
                break

            slot_started = time.perf_counter()
            lease_started = time.perf_counter()
            try:
                task, lease_telemetry = await self.lease_task(lease_started)
            except Exception:
                await self.release_page_slot(False)
                idle_ticks += 1
                logger.exception("Fetch worker failed while leasing task")
                if idle_ticks >= self.worker_patience:
                    break
                await asyncio.sleep(0.1)
                continue
            lease_ms = getattr(lease_telemetry, "elapsed_ms", 0.0)
            if not task:
                await self.release_page_slot(False)
                idle_ticks += 1
                if idle_ticks >= self.worker_patience:
                    break
                await asyncio.sleep(0.1)
                continue

            idle_ticks = 0
            try:
                result = await self.process_url(task)
            except Exception as exc:
                result = FailedTask(
                    task=task,
                    failure=CrawlFailure(
                        url=task.url,
                        error=str(exc),
                        retryable=True,
                        timings=CrawlStageTimings(),
                    ),
                    process_started=slot_started,
                    record_error=True,
                )
                logger.exception("Fetch worker failed while processing URL: url=%s", task.url)
            finally:
                await self.release_active_host(task.url)

            if result is None:
                await self.release_page_slot(False)
                continue

            if isinstance(result, SkippedTask):
                await self._enqueue_skipped(result, lease_ms, lease_telemetry, slot_started)
            elif isinstance(result, FailedTask):
                await self._enqueue_failed(result, lease_ms, lease_telemetry, slot_started)
            else:
                await self._enqueue_fetched(result, lease_ms, lease_telemetry, slot_started)

    async def _enqueue_skipped(
        self,
        skipped: SkippedTask,
        lease_ms: float,
        lease_telemetry: Any,
        slot_started: float,
    ) -> None:
        skipped.timings.lease_ms = lease_ms
        skipped.timings.lease = lease_telemetry
        skipped.timings.slot_ms = elapsed_ms(slot_started)
        await self.release_page_slot(False)
        queue_item = FinalizeItem(
            skipped=skipped,
            enqueued_at=time.perf_counter(),
            queue_depth=self.finalize_queue.qsize(),
        )
        self.finalize_stats.record_enqueue(queue_item.queue_depth)
        await self.finalize_queue.put(queue_item)

    async def _enqueue_failed(
        self,
        failed: FailedTask,
        lease_ms: float,
        lease_telemetry: Any,
        slot_started: float,
    ) -> None:
        failed.failure.timings.lease_ms = lease_ms
        failed.failure.timings.lease = lease_telemetry
        failed.failure.timings.slot_ms = elapsed_ms(slot_started)
        await self.release_page_slot(False)
        self.record_failure_category(failed.failure.error)
        queue_item = FinalizeItem(
            failed=failed,
            enqueued_at=time.perf_counter(),
            queue_depth=self.finalize_queue.qsize(),
        )
        self.finalize_stats.record_enqueue(queue_item.queue_depth)
        await self.finalize_queue.put(queue_item)

    async def _enqueue_fetched(
        self,
        fetched: FetchedPage,
        lease_ms: float,
        lease_telemetry: Any,
        slot_started: float,
    ) -> None:
        await self.release_page_slot(True)
        fetched.timings.lease_ms = lease_ms
        fetched.timings.lease = lease_telemetry
        fetched.timings.slot_ms = elapsed_ms(slot_started)
        fetched.queue_depth = self.parse_queue.qsize()
        self.parse_stats.record_enqueue(fetched.queue_depth)
        fetched.enqueued_at = time.perf_counter()
        await self.parse_queue.put(fetched)


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
        publish_results: Callable[[list[CrawlResult]], Awaitable[None]] | None = None,
        success_batch_size: int = 1,
        success_batch_wait_ms: float = 0.0,
    ) -> None:
        self.publish_queue = publish_queue
        self.publish_stats = publish_stats
        self.liveness = liveness
        self.publish_result = publish_result
        self.publish_results = publish_results
        self.record_timing = record_timing
        self.progress = progress
        self.format_timings = format_timings
        self.success_batch_size = max(1, success_batch_size)
        self.success_batch_wait_seconds = max(0.0, success_batch_wait_ms / 1000.0)
        self._pending_item: PublishItem | object | None = None

    async def run(self) -> None:
        while True:
            item = await self._next_item()
            if item is PUBLISHER_SENTINEL:
                self.publish_queue.task_done()
                break

            await self._process_success_batch(item)

    async def _next_item(self) -> PublishItem | object:
        if self._pending_item is not None:
            item = self._pending_item
            self._pending_item = None
            return item
        return await self.publish_queue.get()

    def _start_item(self, queue_item: PublishItem) -> None:
        result = queue_item.result
        self.liveness.start(url=result.url)
        result.timings.publish_queue_wait_ms = self.publish_stats.record_dequeue(
            queue_item.enqueued_at,
            queue_item.queue_depth,
        )
        result.timings.publish_queue_depth = queue_item.queue_depth

    async def _collect_success_batch(self, first_item: PublishItem) -> list[PublishItem]:
        batch = [first_item]
        waited = False
        while len(batch) < self.success_batch_size:
            try:
                if self.publish_queue.qsize() > 0:
                    item = self.publish_queue.get_nowait()
                elif not waited and self.success_batch_wait_seconds > 0:
                    waited = True
                    item = await asyncio.wait_for(
                        self.publish_queue.get(),
                        timeout=self.success_batch_wait_seconds,
                    )
                else:
                    break
            except (asyncio.QueueEmpty, TimeoutError):
                break
            if item is PUBLISHER_SENTINEL:
                self._pending_item = item
                break
            batch.append(item)
        return batch

    async def _publish_batch(self, results: list[CrawlResult]) -> None:
        if self.publish_results is not None:
            await self.publish_results(results)
            return
        for result in results:
            await self.publish_result(result)

    async def _process_success_batch(self, first_item: PublishItem) -> None:
        batch = await self._collect_success_batch(first_item)
        results = [queue_item.result for queue_item in batch]
        for queue_item in batch:
            self._start_item(queue_item)
        try:
            await self._publish_batch(results)
            for result in results:
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
            for result in results:
                self.liveness.fail()
                logger.exception(
                    "Publisher failed while processing queued crawl result: url=%s",
                    result.url,
                )
        finally:
            self.liveness.clear_current()
            for _queue_item in batch:
                self.publish_queue.task_done()
