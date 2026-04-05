"""Timing instrumentation tests for crawler engine."""

from types import SimpleNamespace

import pytest

from crawler.crawl import CrawlerEngine
from crawler.frontier import CrawlTask


class _FakeFrontier:
    def __init__(self, task):
        self._task = task
        self.done = []
        self.added = []

    def lease_next(self, prioritize_breadth=False):
        task, self._task = self._task, None
        return task

    def mark_done(self, url, lease_token=None):
        self.done.append((url, lease_token))

    def mark_failed(self, url, retryable, error, lease_token=None):
        raise AssertionError(f"unexpected failure for {url}: {error}")

    def add(self, task):
        self.added.append(task)

    def add_many(self, tasks):
        self.added.extend(tasks)

    def pending_count(self):
        return 0


class _FakeDomainManager:
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
    frontier = _FakeFrontier(CrawlTask(url="https://example.com/", depth=0, lease_token="lease-1"))
    engine = CrawlerEngine(
        start_url="https://example.com/",
        max_pages=1,
        max_depth=1,
        frontier=frontier,
        domain_manager=_FakeDomainManager(),
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
    assert result.timings.parse_ms >= 0
    assert result.timings.frontier_ms >= 0
    assert result.timings.persist_ms >= 0
    assert result.timings.process_ms >= result.timings.fetch_ms
    assert result.timings.slot_ms >= result.timings.process_ms
    assert frontier.done == [("https://example.com/", "lease-1")]
    assert frontier.added
