"""Timing instrumentation tests for crawler engine."""

import time
from types import SimpleNamespace

import pytest

from crawler.crawl import CrawlerEngine
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
