"""Timing instrumentation tests for crawler engine."""

import asyncio
import concurrent.futures
import time
from types import SimpleNamespace

import pytest

from crawler.crawl import (
    CrawlerEngine,
    _FINALIZER_SENTINEL,
    _PUBLISHER_SENTINEL,
    _FinalizeItem,
    _ParsedPage,
    _PublishItem,
    _SkippedTask,
)
from crawler.result import CrawlResult, CrawlStageTimings
from crawler.telemetry import TelemetryAccumulator
from crawler.url_ledger import CrawlTask


class _FakeLedger:
    def __init__(self, task):
        self._task = task
        self.done = []
        self.added = []

    def lease_next(self, lease_strategy=None, **_kwargs):
        task, self._task = self._task, None
        return task

    def mark_done(self, url, lease_token=None):
        self.done.append((url, lease_token))

    def mark_failed(self, url, retryable, error, lease_token=None):
        raise AssertionError(f"unexpected failure for {url}: {error}")

    def place(self, task):
        self.added.append(task)

    def place_many(self, tasks):
        self.added.extend(tasks)

    def discover_many(self, tasks):
        return len(tasks)

    def admit_discovered_tasks(self, tasks):
        self.place_many(tasks)
        return len(tasks)

    def pending_count(self):
        return 0


class _FakeHostManager:
    async def is_allowed(self, url):
        return True

    async def wait_for_rate_limit(self, url):
        return None

    def record_success(self, url):
        return None

    def record_error(self, url):
        return None

    def should_retry(self, url):
        return False


class _FakeFetcher:
    async def fetch(self, url):
        html = "<html><body><a href='/next'>next</a></body></html>"
        return SimpleNamespace(
            url=url,
            status=200,
            content=html.encode(),
            text=html,
            headers={"content-type": "text/html"},
        )

    async def close(self):
        return None


class _FakeStorage:
    def __init__(self):
        self.saved = []

    def save(self, result):
        self.saved.append(result)
        return True


def _crawl_result(url="https://example.com/"):
    return CrawlResult(
        url=url,
        status=200,
        content_length=13,
        source_url=None,
        timestamp=1000.0,
        content="<html></html>",
        outlinks=[],
        timings=CrawlStageTimings(),
    )


async def _stop_queue_worker(queue, sentinel, worker):
    await queue.put(sentinel)
    await asyncio.wait_for(worker, timeout=2)


def test_timing_accumulator_summarizes_stage_percentiles():
    timings = TelemetryAccumulator()
    timings.record("success", CrawlStageTimings(lease_ms=1.0, fetch_ms=10.0))
    timings.record("failed", CrawlStageTimings(lease_ms=3.0, fetch_ms=30.0))
    timings.record("skipped", CrawlStageTimings(lease_ms=2.0, fetch_ms=20.0))

    summary = timings.snapshot()

    assert summary["samples"] == 3
    assert summary["outcomes"] == {"success": 1, "skipped": 1, "failed": 1}
    assert summary["stages"]["lease_ms"] == {
        "count": 3,
        "avg": 2.0,
        "p50": 2.0,
        "p95": 3.0,
        "max": 3.0,
    }
    assert summary["stages"]["fetch_ms"]["p95"] == 30.0
    assert "counts" in summary


def test_timing_accumulator_handles_empty_samples():
    summary = TelemetryAccumulator().snapshot()

    assert summary["samples"] == 0
    assert summary["outcomes"] == {"success": 0, "skipped": 0, "failed": 0}
    assert summary["stages"]["lease_ms"] == {
        "count": 0,
        "avg": 0.0,
        "p50": 0.0,
        "p95": 0.0,
        "max": 0.0,
    }


def test_runtime_stats_include_host_first_fallback_stats():
    class FallbackLedger(_FakeLedger):
        def host_first_fallback_stats(self):
            return {"attempts": 2, "hits": 1, "misses": 1}

    engine = CrawlerEngine(
        max_pages=0,
        url_ledger=FallbackLedger(None),
        host_manager=_FakeHostManager(),
    )

    assert engine.snapshot_runtime_stats()["host_first_fallback"] == {
        "attempts": 2,
        "hits": 1,
        "misses": 1,
        "read_model_hits": 0,
        "read_model_stale": 0,
        "read_model_misses": 0,
        "read_model_errors": 0,
    }
    assert engine.snapshot_runtime_stats()["active_cycle"]["host_first_fallback"] == {
        "attempts": 2,
        "hits": 1,
        "misses": 1,
        "read_model_hits": 0,
        "read_model_stale": 0,
        "read_model_misses": 0,
        "read_model_errors": 0,
    }


@pytest.mark.asyncio
async def test_finalizer_and_publisher_drain_success_with_dedicated_executors():
    ledger = _FakeLedger(None)
    engine = CrawlerEngine(
        max_pages=1,
        url_ledger=ledger,
        host_manager=_FakeHostManager(),
    )
    storage = _FakeStorage()
    engine.pg_storage = storage
    engine._finalize_queue = asyncio.Queue()
    engine._publish_queue = asyncio.Queue()
    engine._finalizer_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    engine._publisher_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    finalizer = asyncio.create_task(engine._finalizer())
    publisher = asyncio.create_task(engine._publisher())
    result = _crawl_result()
    await engine._finalize_queue.put(
        _FinalizeItem(
            parsed=_ParsedPage(
                task=CrawlTask(url=result.url, lease_token="lease-1"),
                result=result,
                new_tasks=[],
                process_started=time.perf_counter(),
            ),
            enqueued_at=time.perf_counter(),
            queue_depth=0,
        )
    )

    await asyncio.wait_for(engine._finalize_queue.join(), timeout=2)
    await asyncio.wait_for(engine._publish_queue.join(), timeout=2)
    await _stop_queue_worker(engine._finalize_queue, _FINALIZER_SENTINEL, finalizer)
    await _stop_queue_worker(engine._publish_queue, _PUBLISHER_SENTINEL, publisher)
    await engine.close()

    assert ledger.done == [(result.url, "lease-1")]
    assert storage.saved == [result]
    assert engine.snapshot_runtime_stats()["finalizer_liveness"] == {
        "started": 1,
        "completed": 1,
        "failed": 0,
        "last_progress_at": engine._last_finalizer_progress_at,
        "current_url": None,
        "current_kind": None,
    }
    assert engine.snapshot_runtime_stats()["publisher_liveness"] == {
        "started": 1,
        "completed": 1,
        "failed": 0,
        "last_progress_at": engine._last_publisher_progress_at,
        "current_url": None,
    }


@pytest.mark.asyncio
async def test_finalizer_survives_item_error_and_drains_next_item():
    class FlakyLedger(_FakeLedger):
        def __init__(self):
            super().__init__(None)
            self.fail_once = True

        def mark_done(self, url, lease_token=None):
            if self.fail_once:
                self.fail_once = False
                raise RuntimeError("boom")
            super().mark_done(url, lease_token=lease_token)

    ledger = FlakyLedger()
    engine = CrawlerEngine(
        max_pages=1,
        url_ledger=ledger,
        host_manager=_FakeHostManager(),
    )
    engine._finalize_queue = asyncio.Queue()
    engine._finalizer_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    finalizer = asyncio.create_task(engine._finalizer())

    for suffix in ("first", "second"):
        await engine._finalize_queue.put(
            _FinalizeItem(
                skipped=_SkippedTask(
                    task=CrawlTask(url=f"https://example.com/{suffix}", lease_token=suffix),
                    reason="robots_denied",
                    timings=CrawlStageTimings(),
                    process_started=time.perf_counter(),
                ),
                enqueued_at=time.perf_counter(),
                queue_depth=0,
            )
        )

    await asyncio.wait_for(engine._finalize_queue.join(), timeout=2)
    await _stop_queue_worker(engine._finalize_queue, _FINALIZER_SENTINEL, finalizer)
    await engine.close()

    assert ledger.done == [("https://example.com/second", "second")]
    assert engine.snapshot_runtime_stats()["finalizer_liveness"]["started"] == 2
    assert engine.snapshot_runtime_stats()["finalizer_liveness"]["completed"] == 1
    assert engine.snapshot_runtime_stats()["finalizer_liveness"]["failed"] == 1


@pytest.mark.asyncio
async def test_publisher_survives_item_error_and_drains_next_item():
    class FlakyStorage(_FakeStorage):
        def __init__(self):
            super().__init__()
            self.fail_once = True

        def save(self, result):
            if self.fail_once:
                self.fail_once = False
                raise RuntimeError("boom")
            return super().save(result)

    engine = CrawlerEngine(
        max_pages=1,
        url_ledger=_FakeLedger(None),
        host_manager=_FakeHostManager(),
    )
    storage = FlakyStorage()
    engine.pg_storage = storage
    engine._publish_queue = asyncio.Queue()
    engine._publisher_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    publisher = asyncio.create_task(engine._publisher())

    first = _crawl_result("https://example.com/first")
    second = _crawl_result("https://example.com/second")
    for result in (first, second):
        await engine._publish_queue.put(
            _PublishItem(
                result=result,
                enqueued_at=time.perf_counter(),
                queue_depth=0,
            )
        )

    await asyncio.wait_for(engine._publish_queue.join(), timeout=2)
    await _stop_queue_worker(engine._publish_queue, _PUBLISHER_SENTINEL, publisher)
    await engine.close()

    assert storage.saved == [second]
    assert engine.snapshot_runtime_stats()["publisher_liveness"]["started"] == 2
    assert engine.snapshot_runtime_stats()["publisher_liveness"]["completed"] == 1
    assert engine.snapshot_runtime_stats()["publisher_liveness"]["failed"] == 1


@pytest.mark.asyncio
async def test_crawler_engine_records_stage_timings():
    ledger = _FakeLedger(CrawlTask(url="https://example.com/", lease_token="lease-1"))
    engine = CrawlerEngine(
        start_url="https://example.com/",
        max_pages=1,
        url_ledger=ledger,
        host_manager=_FakeHostManager(),
    )
    engine.fetcher = _FakeFetcher()
    storage = _FakeStorage()
    engine.pg_storage = storage

    await engine.crawl()

    assert len(storage.saved) == 1
    result = storage.saved[0]
    assert result.timings is not None
    assert result.timings.lease_ms >= 0
    assert result.timings.precheck_ms >= 0
    assert result.timings.robots_ms >= 0
    assert result.timings.rate_limit_ms >= 0
    assert result.timings.fetch_ms >= 0
    assert result.timings.fetch_request_ms >= 0
    assert result.timings.fetch_body_read_ms >= 0
    assert result.timings.parse_ms >= 0
    assert result.timings.scheduler_ms >= 0
    assert result.timings.persist_ms >= 0
    assert result.timings.parse_queue_wait_ms >= 0
    assert result.timings.finalize_queue_wait_ms >= 0
    assert result.timings.publish_queue_wait_ms >= 0
    assert result.timings.parse_queue_depth >= 0
    assert result.timings.finalize_queue_depth >= 0
    assert result.timings.publish_queue_depth >= 0
    assert result.timings.process_ms >= result.timings.fetch_ms
    assert result.timings.process_ms >= result.timings.slot_ms
    runtime_stats = engine.snapshot_runtime_stats()
    parser_liveness = runtime_stats["parser_liveness"]
    assert parser_liveness["started"] == 1
    assert parser_liveness["completed"] == 1
    assert parser_liveness["failed"] == 0
    assert parser_liveness["current_url"] is None
    assert parser_liveness["current_kind"] is None
    timing_summary = runtime_stats["timing_summary"]
    assert timing_summary["samples"] == 1
    assert timing_summary["outcomes"]["success"] == 1
    assert timing_summary["stages"]["robots_ms"]["count"] == 1
    assert timing_summary["stages"]["rate_limit_ms"]["count"] == 1
    assert timing_summary["stages"]["fetch_ms"]["count"] == 1
    assert timing_summary["counts"]["fetch_outcomes"]["ok"] == 1
    assert timing_summary["counts"]["lease_outcomes"]["leased"] == 1
    assert timing_summary["counts"]["lease_execution_tiers"]["unknown"] == 1
    assert ledger.done == [("https://example.com/", "lease-1")]
    assert ledger.added


class _SlowLedger(_FakeLedger):
    def place_many(self, tasks):
        time.sleep(0.25)
        super().place_many(tasks)


@pytest.mark.asyncio
async def test_parse_scheduler_delay_does_not_extend_fetch_slot():
    ledger = _SlowLedger(CrawlTask(url="https://example.com/", lease_token="lease-1"))
    engine = CrawlerEngine(
        start_url="https://example.com/",
        max_pages=1,
        url_ledger=ledger,
        host_manager=_FakeHostManager(),
    )
    engine.fetcher = _FakeFetcher()
    storage = _FakeStorage()
    engine.pg_storage = storage

    await engine.crawl()

    result = storage.saved[0]
    assert result.timings.scheduler_ms >= 200
    assert result.timings.slot_ms < result.timings.scheduler_ms


class _SlowStorage(_FakeStorage):
    def save(self, result):
        time.sleep(0.25)
        return super().save(result)


@pytest.mark.asyncio
async def test_queue_wait_metrics_record_backpressure():
    ledger = _FakeLedger(CrawlTask(url="https://example.com/", lease_token="lease-1"))
    engine = CrawlerEngine(
        start_url="https://example.com/",
        max_pages=1,
        url_ledger=ledger,
        host_manager=_FakeHostManager(),
    )
    engine.fetcher = _FakeFetcher()
    storage = _SlowStorage()
    engine.pg_storage = storage

    await engine.crawl()

    result = storage.saved[0]
    assert result.timings.finalize_queue_wait_ms >= 0
    assert result.timings.finalize_queue_depth >= 0
    assert result.timings.publish_queue_wait_ms >= 0
    assert result.timings.publish_queue_depth >= 0
