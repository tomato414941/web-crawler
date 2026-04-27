"""Behavior tests for crawler engine edge cases."""

import asyncio

import pytest

from crawler.core import Response
from crawler.config import settings
from crawler.crawl import CrawlerEngine
from crawler.discovery import PageSignals
from crawler.url_ledger import (
    CrawlTask,
    INTENT_EXPLORE,
    INTENT_REFRESH,
    SCHEDULER_SURFACE_SCHEDULED,
    SCHEDULER_SURFACE_RUNNABLE,
    SCHEDULER_SURFACE_NORMAL,
    SCHEDULER_SURFACE_REFRESH,
)


class FakeLedger:
    def __init__(
        self,
        tasks: list[CrawlTask],
        known_counts: dict[str, int] | None = None,
        pending_count: int = 0,
    ):
        self.tasks = list(tasks)
        self.pending_count_value = pending_count
        self.done: list[str] = []
        self.failed: list[str] = []
        self.failures: list[dict] = []
        self.added_batches: list[list[CrawlTask]] = []
        self.lease_calls: list[dict[str, object]] = []
        self.rebuild_calls: list[dict[str, object]] = []

    def lease_next(
        self,
        lease_strategy: str | None = None,
        **kwargs: object,
    ):
        exclude_hosts = set(kwargs.get("exclude_hosts") or [])
        runnable_surface = kwargs.get("runnable_surface")
        self.lease_calls.append(
            {
                "lease_strategy": lease_strategy,
                "runnable_surface": runnable_surface,
                "exclude_hosts": sorted(exclude_hosts),
                "execution_tiers": kwargs.get("execution_tiers"),
            }
        )
        for index, task in enumerate(self.tasks):
            host = task.url.split("/")[2]
            if host in exclude_hosts:
                continue
            effective_surface = task.runnable_surface or SCHEDULER_SURFACE_RUNNABLE
            if (
                runnable_surface == SCHEDULER_SURFACE_NORMAL
                and effective_surface
                not in {
                    SCHEDULER_SURFACE_RUNNABLE,
                    SCHEDULER_SURFACE_SCHEDULED,
                }
            ):
                continue
            if (
                runnable_surface is not None
                and runnable_surface != SCHEDULER_SURFACE_NORMAL
                and effective_surface != runnable_surface
            ):
                continue
            return self.tasks.pop(index)
        return None

    def preview_tasks(self, tasks: list[CrawlTask]):
        prepared = []
        for task in tasks:
            runnable_surface = task.runnable_surface
            if runnable_surface is None:
                if task.intent == INTENT_REFRESH:
                    runnable_surface = SCHEDULER_SURFACE_REFRESH
                else:
                    runnable_surface = SCHEDULER_SURFACE_SCHEDULED
            intent = task.intent
            if intent is None:
                intent = (
                    INTENT_REFRESH
                    if runnable_surface == SCHEDULER_SURFACE_REFRESH
                    else INTENT_EXPLORE
                )
            prepared.append(
                CrawlTask(
                    url=task.url,
                    discovery_value=task.discovery_value,
                    scheduler_score=task.scheduler_score,
                    runnable_surface=runnable_surface,
                    intent=intent,
                    source_url=task.source_url,
                    added_at=task.added_at,
                    next_fetch_at=task.next_fetch_at,
                )
            )
        return prepared

    def place(self, task: CrawlTask):
        self.tasks.append(task)
        return True

    def place_many(self, tasks: list[CrawlTask]):
        self.added_batches.append(tasks)
        self.tasks.extend(tasks)
        return len(tasks)

    def discover_many(self, tasks: list[CrawlTask]):
        return len(tasks)

    def admit_discovered_tasks(self, tasks: list[CrawlTask]):
        return self.place_many(tasks)

    def pending_count(self):
        return self.pending_count_value

    def mark_done(self, url: str, lease_token: str | None = None):
        self.done.append(url)
        return True

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
        return True

    def rebuild_host_runnable_heads(self, **kwargs: object):
        self.rebuild_calls.append(kwargs)
        return len(self.tasks)


class FakeHostLedgerRecord:
    def __init__(self, *, failure_count=0, success_count=0, robots_status=None):
        self.failure_count = failure_count
        self.success_count = success_count
        self.robots_status = robots_status


class FakeHostLedgerStore:
    def __init__(self, records):
        self.records = records

    def get_many(self, hosts):
        return {host: self.records[host] for host in hosts if host in self.records}


class FakeHostManager:
    def __init__(self, budgets: dict[str, int] | None = None, *, allowed: bool = True):
        self.errors: list[str] = []
        self.successes: list[str] = []
        self.budgets = budgets or {}
        self.allowed = allowed

    async def is_allowed(self, url: str) -> bool:
        return self.allowed

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


def test_discovered_tasks_are_capped_by_value_total_and_target_host(monkeypatch):
    monkeypatch.setattr(settings, "admission_target_pending", 50)
    ledger = FakeLedger([], pending_count=50)
    engine = CrawlerEngine(
        start_url="https://seed.example/",
        same_host=False,
        url_ledger=ledger,
        host_manager=FakeHostManager(),
    )

    links = [f"https://a.example/docs/{i}" for i in range(7)]
    links.extend(
        f"https://b{i:03d}.example/docs/1"
        for i in range(159)
    )
    links.append("https://low.example/redirect/1")

    tasks, admission_counts = engine._build_discovered_tasks_with_admission_counts(
        "https://seed.example/",
        links,
    )

    assert len(tasks) == 160
    assert [task.url for task in tasks[:6]] == [
        f"https://a.example/docs/{i}" for i in range(6)
    ]
    assert all(task.discovery_value >= 0.8 for task in tasks)
    assert admission_counts["extracted"] == 167
    assert admission_counts["admitted"] == 160
    assert admission_counts["per_target_host_cap"] == 1
    assert admission_counts["per_page_cap"] == 5
    assert admission_counts["low_value_archetype"] == 1
    assert engine.snapshot_runtime_stats()["admission_control"]["mode"] == "balanced"


def test_discovered_tasks_explain_low_value_rejections():
    engine = CrawlerEngine(
        start_url="https://seed.example/",
        same_host=False,
        url_ledger=FakeLedger([]),
        host_manager=FakeHostManager(),
    )

    tasks, admission_counts = engine._build_discovered_tasks_with_admission_counts(
        "https://seed.example/archive/",
        [
            "https://example.net/archive/index",
            "https://docs.example.net/doc/rfc9000",
        ],
        parent_signals=PageSignals(
            content_type="text/html",
            content_length=900_000,
            title="Archive Table Index",
            meta_robots="nofollow",
        ),
    )

    assert [task.url for task in tasks] == []
    assert admission_counts["extracted"] == 2
    assert admission_counts["admitted"] == 0
    assert admission_counts["nofollow_parent"] == 2


def test_discovered_tasks_limit_external_generic_links_under_target_pressure(monkeypatch):
    monkeypatch.setattr(settings, "admission_target_pending", 500_000)
    engine = CrawlerEngine(
        start_url="https://seed.example/",
        same_host=False,
        url_ledger=FakeLedger([], pending_count=866_000),
        host_manager=FakeHostManager(),
    )

    tasks, admission_counts = engine._build_discovered_tasks_with_admission_counts(
        "https://seed.example/",
        [
            "https://external.example.net/project",
            "https://external.example.net/doc/rfc9000",
            "https://seed.example/about",
        ],
    )

    assert [task.url for task in tasks] == [
        "https://seed.example/about",
    ]
    assert admission_counts["score_below_threshold"] == 2
    assert admission_counts["admitted"] == 1
    assert engine.snapshot_runtime_stats()["admission_control"]["mode"] == "reduce"


def test_discovered_tasks_reduce_external_documents_before_new_host_growth(monkeypatch):
    monkeypatch.setattr(settings, "admission_target_pending", 500_000)
    engine = CrawlerEngine(
        start_url="https://seed.example/",
        same_host=False,
        url_ledger=FakeLedger([], pending_count=866_000),
        host_manager=FakeHostManager(),
    )

    tasks, admission_counts = engine._build_discovered_tasks_with_admission_counts(
        "https://seed.example/",
        [
            "https://a.example/doc/rfc1",
            "https://b.example/doc/rfc2",
            "https://c.example/doc/rfc3",
            "https://seed.example/local",
        ],
    )

    assert [task.url for task in tasks] == [
        "https://seed.example/local",
    ]
    assert admission_counts["score_below_threshold"] == 3
    assert admission_counts["admitted"] == 1


def test_discovered_tasks_penalize_known_bad_hosts(monkeypatch):
    monkeypatch.setattr(settings, "admission_target_pending", 500_000)
    ledger = FakeLedger([])
    engine = CrawlerEngine(
        start_url="https://seed.example/",
        same_host=False,
        url_ledger=ledger,
        host_manager=FakeHostManager(),
    )
    engine.host_ledger_store = FakeHostLedgerStore(
        {
            "bad.example": FakeHostLedgerRecord(failure_count=4, success_count=0),
        }
    )

    tasks, admission_counts = engine._build_discovered_tasks_with_admission_counts(
        "https://seed.example/",
        [
            "https://bad.example/project",
            "https://good.example/doc/rfc9000",
        ],
    )

    assert [task.url for task in tasks] == ["https://good.example/doc/rfc9000"]
    assert admission_counts["host_policy_penalty"] == 1
    assert admission_counts["admitted"] == 1


@pytest.mark.asyncio
async def test_crawler_marks_client_errors_done_without_saving():
    ledger = FakeLedger([CrawlTask(url="https://example.com/missing")])
    host_manager = FakeHostManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://example.com/missing",
                status=404,
                content=b"<html>missing</html>",
                headers={},
            )
        ]
    )

    async with CrawlerEngine(
        max_pages=10,
        url_ledger=ledger,
        host_manager=host_manager,
    ) as engine:
        engine.fetcher = fetcher
        results = await engine.crawl()

    assert results == []
    assert engine.pages_crawled == 0
    assert ledger.done == ["https://example.com/missing"]
    assert ledger.failed == []


@pytest.mark.asyncio
async def test_crawler_finalizes_robots_denied_without_counting_failure():
    ledger = FakeLedger([CrawlTask(url="https://example.com/private")])
    host_manager = FakeHostManager(allowed=False)

    async with CrawlerEngine(
        max_pages=10,
        url_ledger=ledger,
        host_manager=host_manager,
    ) as engine:
        results = await engine.crawl()

    assert results == []
    assert engine.pages_crawled == 0
    assert ledger.done == ["https://example.com/private"]
    assert ledger.failed == []
    assert engine.failure_breakdown == {}
    assert host_manager.errors == []
    assert host_manager.successes == []


@pytest.mark.asyncio
async def test_crawler_marks_auth_walls_failed_and_records_host_error():
    ledger = FakeLedger([CrawlTask(url="https://example.com/forbidden")])
    host_manager = FakeHostManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://example.com/forbidden",
                status=403,
                content=b"<html>forbidden</html>",
                headers={},
            )
        ]
    )

    async with CrawlerEngine(
        max_pages=10,
        url_ledger=ledger,
        host_manager=host_manager,
    ) as engine:
        engine.fetcher = fetcher
        results = await engine.crawl()

    assert results == []
    assert engine.pages_crawled == 0
    assert ledger.done == []
    assert ledger.failed == ["https://example.com/forbidden"]
    assert ledger.failures == [
        {
            "url": "https://example.com/forbidden",
            "retryable": False,
            "error": "http_403",
            "backoff_seconds": 30.0,
            "lease_token": None,
        }
    ]
    assert host_manager.errors == ["https://example.com/forbidden"]
    assert host_manager.successes == []


@pytest.mark.asyncio
async def test_crawler_marks_server_errors_failed():
    ledger = FakeLedger([CrawlTask(url="https://example.com/error")])
    host_manager = FakeHostManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://example.com/error",
                status=503,
                content=b"<html>error</html>",
                headers={},
            )
        ]
    )

    async with CrawlerEngine(
        max_pages=10,
        url_ledger=ledger,
        host_manager=host_manager,
    ) as engine:
        engine.fetcher = fetcher
        results = await engine.crawl()

    assert results == []
    assert engine.pages_crawled == 0
    assert ledger.done == []
    assert ledger.failed == ["https://example.com/error"]
    assert host_manager.errors == ["https://example.com/error"]
    assert engine.failure_breakdown == {"http_5xx": 1}


@pytest.mark.asyncio
async def test_crawler_marks_parse_errors_failed():
    ledger = FakeLedger([CrawlTask(url="https://example.com/parse")])
    host_manager = FakeHostManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://example.com/parse",
                status=200,
                content=b"<html>ok</html>",
                headers={"content-type": "text/html"},
            )
        ]
    )

    async with CrawlerEngine(
        max_pages=10,
        url_ledger=ledger,
        host_manager=host_manager,
    ) as engine:
        engine.fetcher = fetcher

        def _raise_parse_error(task, response):
            raise RuntimeError("parse boom")

        engine._prepare_parsed_payload = _raise_parse_error
        results = await engine.crawl()

    assert results == []
    assert engine.pages_crawled == 1
    assert ledger.done == []
    assert ledger.failed == ["https://example.com/parse"]
    assert host_manager.errors == ["https://example.com/parse"]
    assert engine.failure_breakdown == {"other": 1}


@pytest.mark.asyncio
async def test_crawler_does_not_exceed_max_pages_with_concurrency():
    ledger = FakeLedger(
        [
            CrawlTask(url="https://example.com/1"),
            CrawlTask(url="https://example.com/2"),
            CrawlTask(url="https://example.com/3"),
        ]
    )
    host_manager = FakeHostManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://example.com/1", status=200, content=b"<html>1</html>", headers={}
            ),
            Response(
                url="https://example.com/2", status=200, content=b"<html>2</html>", headers={}
            ),
            Response(
                url="https://example.com/3", status=200, content=b"<html>3</html>", headers={}
            ),
        ],
        delay=0.05,
    )

    async with CrawlerEngine(
        max_pages=1,
        concurrency=3,
        url_ledger=ledger,
        host_manager=host_manager,
    ) as engine:
        engine.fetcher = fetcher
        results = await engine.crawl()

    assert engine.pages_crawled == 1
    assert len(results) == 1
    assert len(fetcher.calls) == 1


@pytest.mark.asyncio
async def test_crawler_collects_failure_breakdown():
    ledger = FakeLedger(
        [
            CrawlTask(url="https://example.com/missing"),
            CrawlTask(url="https://example.com/error"),
        ]
    )
    host_manager = FakeHostManager()
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
        url_ledger=ledger,
        host_manager=host_manager,
    ) as engine:
        engine.fetcher = fetcher
        await engine.crawl()

    assert engine.failure_breakdown == {"http_4xx": 1, "http_5xx": 1}


@pytest.mark.asyncio
async def test_crawler_assigns_discovery_metadata_to_outlinks():
    ledger = FakeLedger([CrawlTask(url="https://example.com/")])
    host_manager = FakeHostManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://example.com/",
                status=200,
                content=(
                    b'<a href="https://example.com/hosts">same host</a>'
                    b'<a href="https://docs.example.com/guide/">seed host</a>'
                    b'<a href="https://external.example.net/project">external</a>'
                ),
                headers={},
            )
        ]
    )

    async with CrawlerEngine(
        max_pages=1,
        same_host=False,
        url_ledger=ledger,
        host_manager=host_manager,
        seed_urls=["https://example.com/", "https://docs.example.com/"],
    ) as engine:
        engine.fetcher = fetcher
        await engine.crawl()

    added = ledger.added_batches[0]
    by_url = {task.url: task for task in added}

    assert (
        by_url["https://example.com/hosts"].scheduler_score
        > by_url["https://docs.example.com/guide"].scheduler_score
    )
    assert (
        by_url["https://docs.example.com/guide"].scheduler_score
        > by_url["https://external.example.net/project"].scheduler_score
    )
    assert by_url["https://docs.example.com/guide"].runnable_surface == SCHEDULER_SURFACE_SCHEDULED
    assert by_url["https://docs.example.com/guide"].intent == INTENT_EXPLORE
    assert (
        by_url["https://external.example.net/project"].runnable_surface == SCHEDULER_SURFACE_SCHEDULED
    )
    assert by_url["https://external.example.net/project"].intent == INTENT_EXPLORE


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_crawler_assigns_known_hosts_to_scheduled_surface():
    ledger = FakeLedger(
        [CrawlTask(url="https://example.com/")],
        known_counts={"example.com": 8},
    )
    host_manager = FakeHostManager()
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
        same_host=False,
        url_ledger=ledger,
        host_manager=host_manager,
        seed_urls=["https://example.com/"],
    ) as engine:
        engine.fetcher = fetcher
        await engine.crawl()

    added = ledger.added_batches[0]
    assert added[0].runnable_surface == SCHEDULER_SURFACE_SCHEDULED
    assert added[0].intent == INTENT_EXPLORE


@pytest.mark.asyncio
async def test_crawler_treats_pdf_as_metadata_only():
    ledger = FakeLedger([CrawlTask(url="https://example.com/spec.pdf")])
    host_manager = FakeHostManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://example.com/spec.pdf",
                status=200,
                content=b'%PDF-1.7\x00binary<a href="https://example.com/hidden">ignored</a>',
                headers={"content-type": "application/pdf"},
            )
        ]
    )

    async with CrawlerEngine(
        max_pages=1,
        url_ledger=ledger,
        host_manager=host_manager,
    ) as engine:
        engine.fetcher = fetcher
        results = await engine.crawl()

    assert engine.pages_crawled == 1
    assert ledger.done == ["https://example.com/spec.pdf"]
    assert ledger.added_batches == []
    assert len(results) == 1
    assert results[0]["url"] == "https://example.com/spec.pdf"
    assert results[0]["content"] == ""
    assert results[0]["outlinks"] == []


@pytest.mark.asyncio
async def test_crawler_completes_metadata_only_audio_without_parsing():
    ledger = FakeLedger([CrawlTask(url="https://example.com/live.mp3")])
    host_manager = FakeHostManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://example.com/live.mp3",
                status=200,
                content=b"",
                headers={"content-type": "audio/mpeg"},
                content_length=12345,
                metadata_only=True,
                admission_reason="binary_content_type",
            )
        ]
    )

    async with CrawlerEngine(
        max_pages=1,
        url_ledger=ledger,
        host_manager=host_manager,
    ) as engine:
        engine.fetcher = fetcher
        results = await engine.crawl()

    assert engine.pages_crawled == 1
    assert ledger.done == ["https://example.com/live.mp3"]
    assert ledger.added_batches == []
    assert host_manager.successes == ["https://example.com/live.mp3"]
    assert len(results) == 1
    assert results[0]["content_length"] == 12345
    assert results[0]["content"] == ""
    assert results[0]["outlinks"] == []


@pytest.mark.asyncio
async def test_crawler_reserves_some_leases_for_breadth():
    ledger = FakeLedger(
        [
            CrawlTask(url="https://example.com/1"),
            CrawlTask(url="https://example.com/2"),
        ]
    )
    host_manager = FakeHostManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://example.com/1", status=200, content=b"<html>1</html>", headers={}
            ),
            Response(
                url="https://example.com/2", status=200, content=b"<html>2</html>", headers={}
            ),
        ]
    )

    async with CrawlerEngine(
        max_pages=2,
        concurrency=1,
        url_ledger=ledger,
        host_manager=host_manager,
    ) as engine:
        engine.fetcher = fetcher
        await engine.crawl()

    assert ledger.lease_calls[:2] == [
        {
            "lease_strategy": "host_first",
            "runnable_surface": "normal",
            "exclude_hosts": [],
            "execution_tiers": [0],
        },
        {
            "lease_strategy": "host_first",
            "runnable_surface": "normal",
            "exclude_hosts": [],
            "execution_tiers": [0],
        },
    ]


@pytest.mark.asyncio
async def test_crawler_uses_normal_surface_for_regular_crawls():
    ledger = FakeLedger([CrawlTask(url="https://example.com/page")])
    host_manager = FakeHostManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://example.com/page", status=200, content=b"<html></html>", headers={}
            ),
        ]
    )

    async with CrawlerEngine(
        max_pages=1,
        concurrency=1,
        url_ledger=ledger,
        host_manager=host_manager,
    ) as engine:
        engine.fetcher = fetcher
        await engine.crawl()

    assert ledger.lease_calls[0]["runnable_surface"] == "normal"


@pytest.mark.asyncio
async def test_crawler_avoids_leasing_same_host_while_request_in_flight():
    ledger = FakeLedger(
        [
            CrawlTask(url="https://a.com/1"),
            CrawlTask(url="https://a.com/2"),
            CrawlTask(url="https://b.com/1"),
        ]
    )
    host_manager = FakeHostManager()
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
        url_ledger=ledger,
        host_manager=host_manager,
    ) as engine:
        engine.max_inflight_requests_per_host = 1
        engine.fetcher = fetcher
        await engine.crawl()

    assert fetcher.calls == ["https://a.com/1", "https://b.com/1"]
    assert ledger.lease_calls[1]["exclude_hosts"] == ["a.com"]


@pytest.mark.asyncio
async def test_crawler_allows_second_inflight_for_fast_host_budget():
    ledger = FakeLedger(
        [
            CrawlTask(url="https://a.com/1"),
            CrawlTask(url="https://a.com/2"),
            CrawlTask(url="https://b.com/1"),
        ]
    )
    host_manager = FakeHostManager(budgets={"a.com": 2})
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
        url_ledger=ledger,
        host_manager=host_manager,
    ) as engine:
        engine.max_inflight_requests_per_host = 1
        engine.fetcher = fetcher
        await engine.crawl()

    assert fetcher.calls == ["https://a.com/1", "https://a.com/2", "https://b.com/1"]
    assert ledger.lease_calls[1]["exclude_hosts"] == []
    assert ledger.lease_calls[2]["exclude_hosts"] == ["a.com"]


@pytest.mark.asyncio
async def test_crawler_uses_normal_workers_for_scheduled_work():
    ledger = FakeLedger(
        [
            CrawlTask(
                url="https://example.com/page",
                runnable_surface=SCHEDULER_SURFACE_SCHEDULED,
                intent=INTENT_EXPLORE,
            )
        ]
    )
    host_manager = FakeHostManager()
    fetcher = FakeFetcher(
        [
            Response(
                url="https://example.com/page", status=200, content=b"<html></html>", headers={}
            ),
        ]
    )

    async with CrawlerEngine(
        max_pages=1,
        concurrency=6,
        url_ledger=ledger,
        host_manager=host_manager,
    ) as engine:
        engine.fetcher = fetcher
        runtime = engine.snapshot_runtime_stats()
        assert runtime["normal_workers"] == 5
        assert runtime["warm_workers"] == 4
        assert runtime["probing_workers"] == 1
        assert runtime["runnable_workers"] == 5
        assert runtime["scheduled_workers"] == 0
        assert runtime["refresh_workers"] == 1
        assert runtime["execution_workers"] == {"warm": 4, "probing": 1, "refresh": 1}
        await engine.crawl()

    assert ledger.lease_calls[0]["runnable_surface"] == "normal"
    assert engine.pages_crawled == 1


@pytest.mark.asyncio
async def test_crawler_splits_normal_workers_into_execution_tier_lanes():
    ledger = FakeLedger(
        [
            CrawlTask(url=f"https://example{i}.com/page", runnable_surface=SCHEDULER_SURFACE_RUNNABLE)
            for i in range(5)
        ]
    )
    host_manager = FakeHostManager()
    fetcher = FakeFetcher(
        [
            Response(
                url=f"https://example{i}.com/page",
                status=200,
                content=b"<html></html>",
                headers={},
            )
            for i in range(5)
        ],
        delay=0.05,
    )

    async with CrawlerEngine(
        max_pages=5,
        concurrency=6,
        url_ledger=ledger,
        host_manager=host_manager,
    ) as engine:
        engine.fetcher = fetcher
        await engine.crawl()

    tier_calls = [
        tuple(call["execution_tiers"] or [])
        for call in ledger.lease_calls
        if call["runnable_surface"] == SCHEDULER_SURFACE_NORMAL
    ]
    assert (0,) in tier_calls
    assert (1, 2, 3) in tier_calls
