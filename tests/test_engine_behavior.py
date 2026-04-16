"""Behavior tests for crawler engine edge cases."""

import asyncio

import pytest

from crawler.core import Response
from crawler.crawl import CrawlerEngine
from crawler.frontier import CrawlTask


class FakeFrontier:
    def __init__(self, tasks: list[CrawlTask], known_counts: dict[str, int] | None = None):
        self.tasks = list(tasks)
        self.known_counts = known_counts or {}
        self.done: list[str] = []
        self.failed: list[str] = []
        self.failures: list[dict] = []
        self.added_batches: list[list[CrawlTask]] = []
        self.lease_calls: list[dict[str, object]] = []

    def lease_next(self, prioritize_breadth: bool = False, **kwargs: object):
        exclude_domains = set(kwargs.get("exclude_domains") or [])
        exclude_branch_keys = set(kwargs.get("exclude_branch_keys") or [])
        exclude_domain_branches = set(kwargs.get("exclude_domain_branches") or [])
        queue_classes = list(kwargs.get("queue_classes") or [])
        self.lease_calls.append(
            {
                "prioritize_breadth": prioritize_breadth,
                "exclude_domains": sorted(exclude_domains),
                "exclude_branch_keys": sorted(exclude_branch_keys),
                "exclude_domain_branches": sorted(exclude_domain_branches),
                "queue_classes": queue_classes,
            }
        )
        for index, task in enumerate(self.tasks):
            domain = task.url.split('/')[2]
            if domain in exclude_domains:
                continue
            branch_key = "/" + "/".join([part for part in task.url.split("/", 3)[-1].split("/") if part][:2])
            if branch_key in exclude_branch_keys:
                continue
            if (domain, branch_key) in exclude_domain_branches:
                continue
            effective_queue = task.queue_class or "exploration"
            if queue_classes and effective_queue not in queue_classes:
                continue
            return self.tasks.pop(index)
        return None

    def get_domain_known_counts(self, domains: set[str]):
        return {domain: self.known_counts.get(domain, 0) for domain in domains}

    def preview_tasks(self, tasks: list[CrawlTask]):
        prepared = []
        domain_counts = {task.url.split('/')[2]: self.known_counts.get(task.url.split('/')[2], 0) for task in tasks}
        batch_counts = {}
        for task in tasks:
            domain = task.url.split('/')[2]
            known_count = domain_counts.get(domain, 0) + batch_counts.get(domain, 0)
            queue_class = task.queue_class
            if queue_class is None:
                if known_count >= 8:
                    queue_class = "backlog"
                else:
                    queue_class = "backlog"
            prepared.append(CrawlTask(
                url=task.url,
                depth=task.depth,
                priority=task.priority,
                queue_class=queue_class,
                source_url=task.source_url,
                added_at=task.added_at,
                next_fetch_at=task.next_fetch_at,
            ))
            batch_counts[domain] = batch_counts.get(domain, 0) + 1
        return prepared

    def place(self, task: CrawlTask):
        self.tasks.append(task)
        return True

    def place_many(self, tasks: list[CrawlTask]):
        self.added_batches.append(tasks)
        self.tasks.extend(tasks)
        return len(tasks)

    def add(self, task: CrawlTask):
        return self.place(task)

    def add_many(self, tasks: list[CrawlTask]):
        return self.place_many(tasks)

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
    def __init__(self, budgets: dict[str, int] | None = None):
        self.errors: list[str] = []
        self.successes: list[str] = []
        self.budgets = budgets or {}

    async def is_allowed(self, url: str) -> bool:
        return True

    async def wait_for_rate_limit(self, url: str):
        return None

    def record_error(self, url: str):
        self.errors.append(url)

    def record_error_runtime(self, url: str) -> float:
        self.errors.append(url)
        return 30.0

    def record_success(self, url: str):
        self.successes.append(url)

    def record_success_runtime(self, url: str):
        self.successes.append(url)

    def get_host_budget(self, host_key: str, *, default_budget: int) -> int:
        return self.budgets.get(host_key, default_budget)

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
async def test_crawler_marks_auth_walls_failed_and_records_host_error():
    frontier = FakeFrontier([CrawlTask(url="https://example.com/forbidden", depth=0)])
    domain_manager = FakeDomainManager()
    fetcher = FakeFetcher([
        Response(
            url="https://example.com/forbidden",
            status=403,
            content=b"<html>forbidden</html>",
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
    assert frontier.failed == ["https://example.com/forbidden"]
    assert frontier.failures == [
        {
            "url": "https://example.com/forbidden",
            "retryable": False,
            "error": "http_403",
            "backoff_seconds": 30.0,
            "lease_token": None,
        }
    ]
    assert domain_manager.errors == ["https://example.com/forbidden"]
    assert domain_manager.successes == []


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
    assert engine.failure_breakdown == {"http_5xx": 1}


@pytest.mark.asyncio
async def test_crawler_marks_parse_errors_failed():
    frontier = FakeFrontier([CrawlTask(url="https://example.com/parse", depth=0)])
    domain_manager = FakeDomainManager()
    fetcher = FakeFetcher([
        Response(
            url="https://example.com/parse",
            status=200,
            content=b"<html>ok</html>",
            headers={"content-type": "text/html"},
        )
    ])

    async with CrawlerEngine(
        max_pages=10,
        frontier=frontier,
        domain_manager=domain_manager,
    ) as engine:
        engine.fetcher = fetcher

        def _raise_parse_error(task, response):
            raise RuntimeError("parse boom")

        engine._prepare_parsed_payload = _raise_parse_error
        results = await engine.crawl()

    assert results == []
    assert engine.pages_crawled == 1
    assert frontier.done == []
    assert frontier.failed == ["https://example.com/parse"]
    assert domain_manager.errors == ["https://example.com/parse"]
    assert engine.failure_breakdown == {"other": 1}


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
async def test_crawler_collects_failure_breakdown():
    frontier = FakeFrontier(
        [
            CrawlTask(url="https://example.com/missing", depth=0),
            CrawlTask(url="https://example.com/error", depth=0),
        ]
    )
    domain_manager = FakeDomainManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://example.com/missing",
                status=404,
                content=b"<html>missing</html>",
                headers={},
            ),
            Response(
                url="https://example.com/error",
                status=503,
                content=b"<html>error</html>",
                headers={},
            ),
        ]
    )

    async with CrawlerEngine(
        max_pages=10,
        concurrency=1,
        frontier=frontier,
        domain_manager=domain_manager,
    ) as engine:
        engine.fetcher = fetcher
        await engine.crawl()

    assert engine.failure_breakdown == {"http_4xx": 1, "http_5xx": 1}


@pytest.mark.asyncio
async def test_crawler_assigns_discovery_metadata_to_outlinks():
    frontier = FakeFrontier([CrawlTask(url="https://example.com/", depth=0)])
    domain_manager = FakeDomainManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://example.com/",
                status=200,
                content=(
                    b'<a href="https://example.com/domains">same host</a>'
                    b'<a href="https://docs.example.com/guide/">seed host</a>'
                    b'<a href="https://external.example.net/project">external</a>'
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
        seed_urls=["https://example.com/", "https://docs.example.com/"],
    ) as engine:
        engine.fetcher = fetcher
        await engine.crawl()

    added = frontier.added_batches[0]
    by_url = {task.url: task for task in added}

    assert by_url["https://example.com/domains"].priority > by_url[
        "https://docs.example.com/guide"
    ].priority
    assert by_url["https://docs.example.com/guide"].priority > by_url[
        "https://external.example.net/project"
    ].priority
    assert by_url["https://docs.example.com/guide"].queue_class == "backlog"
    assert by_url["https://external.example.net/project"].queue_class == "backlog"


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_crawler_assigns_known_hosts_to_backlog_queue():
    frontier = FakeFrontier(
        [CrawlTask(url="https://example.com/", depth=0)],
        known_counts={"example.com": 8},
    )
    domain_manager = FakeDomainManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://example.com/",
                status=200,
                content=(b'<a href="https://example.com/guide">same host</a>'),
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
        seed_urls=["https://example.com/"],
    ) as engine:
        engine.fetcher = fetcher
        await engine.crawl()

    added = frontier.added_batches[0]
    assert added[0].queue_class == "backlog"



@pytest.mark.asyncio
async def test_crawler_treats_pdf_as_metadata_only():
    frontier = FakeFrontier([CrawlTask(url="https://example.com/spec.pdf", depth=0)])
    domain_manager = FakeDomainManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://example.com/spec.pdf",
                status=200,
                content=b"%PDF-1.7\x00binary<a href=\"https://example.com/hidden\">ignored</a>",
                headers={"content-type": "application/pdf"},
            )
        ]
    )

    async with CrawlerEngine(
        max_pages=1,
        frontier=frontier,
        domain_manager=domain_manager,
    ) as engine:
        engine.fetcher = fetcher
        results = await engine.crawl()

    assert engine.pages_crawled == 1
    assert frontier.done == ["https://example.com/spec.pdf"]
    assert frontier.added_batches == []
    assert len(results) == 1
    assert results[0]["url"] == "https://example.com/spec.pdf"
    assert results[0]["content"] == ""
    assert results[0]["outlinks"] == []


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
        {
            "prioritize_breadth": True,
            "exclude_domains": [],
            "exclude_branch_keys": [],
            "exclude_domain_branches": [],
            "queue_classes": ["exploration"],
        },
        {
            "prioritize_breadth": True,
            "exclude_domains": [],
            "exclude_branch_keys": [],
            "exclude_domain_branches": [],
            "queue_classes": ["exploration"],
        },
    ]



@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_crawler_prefers_exploration_before_backlog_and_recrawl():
    frontier = FakeFrontier([CrawlTask(url="https://example.com/page", depth=0)])
    domain_manager = FakeDomainManager()
    fetcher = FakeFetcher([
        Response(url="https://example.com/page", status=200, content=b"<html></html>", headers={}),
    ])

    async with CrawlerEngine(
        max_pages=1,
        concurrency=1,
        frontier=frontier,
        domain_manager=domain_manager,
    ) as engine:
        engine.fetcher = fetcher
        await engine.crawl()

    assert frontier.lease_calls[0]["queue_classes"] == ["exploration"]


@pytest.mark.asyncio
async def test_crawler_avoids_leasing_same_host_while_request_in_flight():
    frontier = FakeFrontier(
        [
            CrawlTask(url="https://a.com/1", depth=0),
            CrawlTask(url="https://a.com/2", depth=0),
            CrawlTask(url="https://b.com/1", depth=0),
        ]
    )
    domain_manager = FakeDomainManager()
    fetcher = FakeFetcher(
        [
            Response(url="https://a.com/1", status=200, content=b"<html>a1</html>", headers={}),
            Response(url="https://b.com/1", status=200, content=b"<html>b1</html>", headers={}),
        ],
        delay=0.05,
    )

    async with CrawlerEngine(
        max_pages=2,
        concurrency=3,
        frontier=frontier,
        domain_manager=domain_manager,
    ) as engine:
        engine.max_inflight_requests_per_host = 1
        engine.fetcher = fetcher
        await engine.crawl()

    assert fetcher.calls == ["https://a.com/1", "https://b.com/1"]
    assert frontier.lease_calls[1]["exclude_domains"] == ["a.com"]
    assert frontier.lease_calls[1]["exclude_domain_branches"] == [("a.com", "/1")]


@pytest.mark.asyncio
async def test_crawler_allows_second_inflight_for_fast_host_budget():
    frontier = FakeFrontier(
        [
            CrawlTask(url="https://a.com/1", depth=0),
            CrawlTask(url="https://a.com/2", depth=0),
            CrawlTask(url="https://b.com/1", depth=0),
        ]
    )
    domain_manager = FakeDomainManager(budgets={"a.com": 2})
    fetcher = FakeFetcher(
        [
            Response(url="https://a.com/1", status=200, content=b"<html>a1</html>", headers={}),
            Response(url="https://a.com/2", status=200, content=b"<html>a2</html>", headers={}),
            Response(url="https://b.com/1", status=200, content=b"<html>b1</html>", headers={}),
        ],
        delay=0.05,
    )

    async with CrawlerEngine(
        max_pages=3,
        concurrency=3,
        frontier=frontier,
        domain_manager=domain_manager,
    ) as engine:
        engine.max_inflight_requests_per_host = 1
        engine.fetcher = fetcher
        await engine.crawl()

    assert fetcher.calls == ["https://a.com/1", "https://a.com/2", "https://b.com/1"]
    assert frontier.lease_calls[1]["exclude_domains"] == []
    assert frontier.lease_calls[2]["exclude_domains"] == ["a.com"]


@pytest.mark.asyncio
async def test_crawler_splits_worker_pools_by_queue_class():
    frontier = FakeFrontier([CrawlTask(url="https://example.com/page", depth=0, queue_class="exploration")])
    domain_manager = FakeDomainManager()
    fetcher = FakeFetcher([
        Response(url="https://example.com/page", status=200, content=b"<html></html>", headers={}),
    ])

    async with CrawlerEngine(
        max_pages=1,
        concurrency=6,
        frontier=frontier,
        domain_manager=domain_manager,
    ) as engine:
        engine.fetcher = fetcher
        runtime = engine.snapshot_runtime_stats()
        assert runtime["exploration_workers"] == 5
        assert runtime["backlog_workers"] == 0
        assert runtime["recrawl_workers"] == 1
        await engine.crawl()

    assert frontier.lease_calls[0]["queue_classes"] == ["exploration"]
