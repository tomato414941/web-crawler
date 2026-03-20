"""Behavior tests for crawler engine edge cases."""

import asyncio

import pytest

from crawler.core import Response
from crawler.crawl import CrawlerEngine
from crawler.discovery import (
    ARCHETYPE_DOCUMENT_PAGE,
    ARCHETYPE_GENERIC_PAGE,
    ARCHETYPE_REDIRECT_HUB,
    DISCOVERY_EXTERNAL,
    DISCOVERY_SAME_HOST,
    DISCOVERY_SEED_HOST,
)
from crawler.frontier import CrawlTask


class FakeFrontier:
    def __init__(self, tasks: list[CrawlTask]):
        self.tasks = list(tasks)
        self.done: list[str] = []
        self.failed: list[str] = []
        self.failures: list[dict] = []
        self.added_batches: list[list[CrawlTask]] = []
        self.lease_calls: list[dict[str, object]] = []

    def lease_next(self, prioritize_breadth: bool = False, **_: object):
        self.lease_calls.append({"prioritize_breadth": prioritize_breadth})
        if self.tasks:
            return self.tasks.pop(0)
        return None

    def add(self, task: CrawlTask):
        self.tasks.append(task)
        return True

    def add_many(self, tasks: list[CrawlTask]):
        self.added_batches.append(tasks)
        self.tasks.extend(tasks)
        return len(tasks)

    def mark_done(self, url: str, lease_token: str | None = None):
        self.done.append(url)

    def mark_failed(
        self,
        url: str,
        retryable: bool = False,
        error: str | None = None,
        backoff_seconds: float | None = None,
        lease_token: str | None = None,
    ):
        self.failed.append(url)
        self.failures.append(
            {
                "url": url,
                "retryable": retryable,
                "error": error,
                "backoff_seconds": backoff_seconds,
                "lease_token": lease_token,
            }
        )

    def pending_count(self):
        return len(self.tasks)


class FakeDomainManager:
    def __init__(self):
        self.errors: list[str] = []
        self.successes: list[str] = []

    async def is_allowed(self, url: str) -> bool:
        return True

    async def wait_for_rate_limit(self, url: str):
        return None

    def record_error(self, url: str):
        self.errors.append(url)

    def record_success(self, url: str):
        self.successes.append(url)

    def should_retry(self, url: str) -> bool:
        return True

    async def close(self):
        return None


class FakeFetcher:
    def __init__(self, responses: list[Response], delay: float = 0.0):
        self.responses = list(responses)
        self.delay = delay
        self.calls: list[str] = []

    async def fetch(self, url: str) -> Response:
        self.calls.append(url)
        if self.delay:
            await asyncio.sleep(self.delay)
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_crawler_marks_client_errors_done_without_saving():
    frontier = FakeFrontier([CrawlTask(url="https://example.com/missing", depth=0)])
    domain_manager = FakeDomainManager()
    fetcher = FakeFetcher([
        Response(
            url="https://example.com/missing",
            status=404,
            content=b"<html>missing</html>",
            headers={},
        )
    ])

    async with CrawlerEngine(
        max_pages=10,
        frontier=frontier,
        domain_manager=domain_manager,
    ) as engine:
        engine.fetcher = fetcher
        results = await engine.crawl()

    assert results == []
    assert engine.pages_crawled == 0
    assert frontier.done == ["https://example.com/missing"]
    assert frontier.failed == []


@pytest.mark.asyncio
async def test_crawler_marks_server_errors_failed():
    frontier = FakeFrontier([CrawlTask(url="https://example.com/error", depth=0)])
    domain_manager = FakeDomainManager()
    fetcher = FakeFetcher([
        Response(
            url="https://example.com/error",
            status=503,
            content=b"<html>error</html>",
            headers={},
        )
    ])

    async with CrawlerEngine(
        max_pages=10,
        frontier=frontier,
        domain_manager=domain_manager,
    ) as engine:
        engine.fetcher = fetcher
        results = await engine.crawl()

    assert results == []
    assert engine.pages_crawled == 0
    assert frontier.done == []
    assert frontier.failed == ["https://example.com/error"]
    assert domain_manager.errors == ["https://example.com/error"]


@pytest.mark.asyncio
async def test_crawler_does_not_exceed_max_pages_with_concurrency():
    frontier = FakeFrontier(
        [
            CrawlTask(url="https://example.com/1", depth=0),
            CrawlTask(url="https://example.com/2", depth=0),
            CrawlTask(url="https://example.com/3", depth=0),
        ]
    )
    domain_manager = FakeDomainManager()
    fetcher = FakeFetcher(
        [
            Response(url="https://example.com/1", status=200, content=b"<html>1</html>", headers={}),
            Response(url="https://example.com/2", status=200, content=b"<html>2</html>", headers={}),
            Response(url="https://example.com/3", status=200, content=b"<html>3</html>", headers={}),
        ],
        delay=0.05,
    )

    async with CrawlerEngine(
        max_pages=1,
        concurrency=3,
        frontier=frontier,
        domain_manager=domain_manager,
    ) as engine:
        engine.fetcher = fetcher
        results = await engine.crawl()

    assert engine.pages_crawled == 1
    assert len(results) == 1
    assert len(fetcher.calls) == 1


@pytest.mark.asyncio
async def test_crawler_assigns_discovery_metadata_to_outlinks():
    frontier = FakeFrontier([CrawlTask(url="https://www.iana.org/", depth=0)])
    domain_manager = FakeDomainManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://www.iana.org/",
                status=200,
                content=(
                    b'<a href="https://www.iana.org/domains">same host</a>'
                    b'<a href="https://datatracker.ietf.org/wg/">seed host</a>'
                    b'<a href="https://github.com/ietf-tools/datatracker">external</a>'
                ),
                headers={},
            )
        ]
    )

    async with CrawlerEngine(
        max_pages=1,
        max_depth=1,
        same_domain=False,
        frontier=frontier,
        domain_manager=domain_manager,
        seed_urls=["https://www.iana.org/", "https://datatracker.ietf.org/"],
    ) as engine:
        engine.fetcher = fetcher
        await engine.crawl()

    added = frontier.added_batches[0]
    by_url = {task.url: task for task in added}

    assert by_url["https://www.iana.org/domains"].discovery_kind == DISCOVERY_SAME_HOST
    assert by_url["https://www.iana.org/domains"].archetype == ARCHETYPE_GENERIC_PAGE
    assert by_url["https://www.iana.org/domains"].priority > by_url[
        "https://datatracker.ietf.org/wg"
    ].priority
    assert by_url["https://datatracker.ietf.org/wg"].discovery_kind == DISCOVERY_SEED_HOST
    assert by_url["https://datatracker.ietf.org/wg"].archetype == ARCHETYPE_GENERIC_PAGE
    assert by_url["https://datatracker.ietf.org/wg"].priority > by_url[
        "https://github.com/ietf-tools/datatracker"
    ].priority
    assert by_url["https://github.com/ietf-tools/datatracker"].discovery_kind == DISCOVERY_EXTERNAL
    assert by_url["https://github.com/ietf-tools/datatracker"].archetype == ARCHETYPE_GENERIC_PAGE


@pytest.mark.asyncio
async def test_crawler_assigns_page_archetypes_to_outlinks():
    frontier = FakeFrontier([CrawlTask(url="https://www.iana.org/", depth=0)])
    domain_manager = FakeDomainManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://www.iana.org/",
                status=200,
                content=(
                    b'<a href="https://www.iana.org/go/rfc9142">redirect hub</a>'
                    b'<a href="https://datatracker.ietf.org/doc/html/rfc9142">document</a>'
                ),
                headers={"content-type": "text/html; charset=utf-8"},
            )
        ]
    )

    async with CrawlerEngine(
        max_pages=1,
        max_depth=1,
        same_domain=False,
        frontier=frontier,
        domain_manager=domain_manager,
        seed_urls=["https://www.iana.org/", "https://datatracker.ietf.org/"],
    ) as engine:
        engine.fetcher = fetcher
        await engine.crawl()

    added = frontier.added_batches[0]
    by_url = {task.url: task for task in added}

    assert by_url["https://www.iana.org/go/rfc9142"].archetype == ARCHETYPE_REDIRECT_HUB
    assert by_url["https://datatracker.ietf.org/doc/html/rfc9142"].archetype == ARCHETYPE_DOCUMENT_PAGE
    assert by_url["https://datatracker.ietf.org/doc/html/rfc9142"].priority > by_url[
        "https://www.iana.org/go/rfc9142"
    ].priority


@pytest.mark.asyncio
async def test_crawler_reserves_some_leases_for_breadth():
    frontier = FakeFrontier(
        [
            CrawlTask(url="https://example.com/1", depth=0),
            CrawlTask(url="https://example.com/2", depth=0),
        ]
    )
    domain_manager = FakeDomainManager()
    fetcher = FakeFetcher(
        [
            Response(url="https://example.com/1", status=200, content=b"<html>1</html>", headers={}),
            Response(url="https://example.com/2", status=200, content=b"<html>2</html>", headers={}),
        ]
    )

    async with CrawlerEngine(
        max_pages=2,
        concurrency=1,
        frontier=frontier,
        domain_manager=domain_manager,
    ) as engine:
        engine.fetcher = fetcher
        await engine.crawl()

    assert frontier.lease_calls[:2] == [
        {"prioritize_breadth": True},
        {"prioritize_breadth": False},
    ]
