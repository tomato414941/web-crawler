"""Behavior tests for crawl daemon queue management."""

import os
import time
from types import SimpleNamespace

import psycopg2
import pytest

from crawler.daemon import CrawlDaemon, _format_error_breakdown
from crawler.frontier import CrawlTask, Frontier
from crawler.migrate import apply_migrations
from crawler.storage import PgStorage

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_POSTGRES_DSN"),
    reason="TEST_POSTGRES_DSN not set",
)


def _reset_schema(dsn: str) -> None:
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS public.schema_migrations")
            cur.execute("DROP TABLE IF EXISTS public.domain_state")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_blocked_domain_backoff")
            cur.execute("DROP TABLE IF EXISTS public.frontier")
            cur.execute("DROP TABLE IF EXISTS public.crawler_runtime_stats")
            cur.execute("DROP TABLE IF EXISTS public.pages")
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def pg_resources():
    dsn = os.environ["TEST_POSTGRES_DSN"]
    _reset_schema(dsn)
    apply_migrations(dsn)

    storage = PgStorage(dsn)
    frontier = Frontier(storage.conn)

    yield dsn, storage, frontier

    storage._conn.rollback()
    storage.close()
    _reset_schema(dsn)


def _save_page(storage: PgStorage, url: str, timestamp: float) -> None:
    storage.save(
        {
            "url": url,
            "status": 200,
            "content_length": 100,
            "depth": 0,
            "timestamp": timestamp,
            "content": "<html><title>Example</title></html>",
            "outlinks": [],
        }
    )


def test_recrawl_stale_skips_when_pending_queue_is_full(pg_resources):
    _dsn, storage, frontier = pg_resources
    now = time.time()

    for idx in range(3):
        frontier.add(
            CrawlTask(url=f"https://example.com/pending-{idx}", depth=0, added_at=now + idx)
        )

    stale_url = "https://example.com/stale"
    frontier.add(CrawlTask(url=stale_url, depth=0, added_at=now - 100))
    frontier.mark_done(stale_url)
    _save_page(storage, stale_url, now - 86400)

    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
        cycle_pages=2,
        recrawl_ttl=3600,
    )

    daemon._recrawl_stale(storage, frontier)

    with storage._conn.cursor() as cur:
        cur.execute("SELECT status FROM frontier WHERE url = %s", (stale_url,))
        (status,) = cur.fetchone()

    assert frontier.pending_count() == 3
    assert status == "done"


def test_recrawl_stale_requeues_only_oldest_rows_needed(pg_resources):
    _dsn, storage, frontier = pg_resources
    now = time.time()

    frontier.add(CrawlTask(url="https://example.com/pending", depth=0, added_at=now))

    stale_urls = [
        ("https://example.com/stale-1", now - 300),
        ("https://example.com/stale-2", now - 200),
        ("https://example.com/stale-3", now - 100),
    ]
    for url, added_at in stale_urls:
        frontier.add(CrawlTask(url=url, depth=0, added_at=added_at))
        frontier.mark_done(url)
        _save_page(storage, url, added_at)

    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
        cycle_pages=3,
        recrawl_ttl=60,
    )

    daemon._recrawl_stale(storage, frontier)

    with storage._conn.cursor() as cur:
        cur.execute(
            """
            SELECT url, status
            FROM frontier
            WHERE url LIKE 'https://example.com/stale-%'
            ORDER BY url
            """
        )
        statuses = dict(cur.fetchall())

    assert frontier.pending_count() == 3
    assert statuses == {
        "https://example.com/stale-1": "pending",
        "https://example.com/stale-2": "pending",
        "https://example.com/stale-3": "done",
    }


@pytest.mark.asyncio
async def test_daemon_does_not_auto_requeue_failed_urls():
    class FakeStorage:
        def close(self):
            return None

    class FakeFrontier:
        def __init__(self):
            self.requeue_failed_calls = 0

        def pending_count(self, queue_classes=None):
            return 1

        def readiness(self):
            return SimpleNamespace(pending=1, ready=1, next_ready_delay=None)

        def defer_overcrowded_backlog(self, **_kwargs):
            return 0

        def recover_leased(self, expired_only=False):
            return 0

        def upsert_seeds(self, urls, priority=2.0):
            return len(urls)

        def requeue_failed(self):
            self.requeue_failed_calls += 1
            return 1

        def stats(self):
            return {"pending": 1, "total": 1}

    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        recrawl_ttl=3600,
    )
    frontier = FakeFrontier()
    storage = FakeStorage()

    daemon._install_signals = lambda: None
    daemon._recrawl_stale = lambda _storage, _frontier: None

    async def fake_connect():
        return storage, frontier

    async def fake_run_cycle(_storage, _frontier):
        daemon._shutdown = True
        return 0, {}

    daemon._connect = fake_connect
    daemon._run_cycle = fake_run_cycle

    await daemon.run()

    assert frontier.requeue_failed_calls == 0


def test_format_error_breakdown_orders_known_categories():
    formatted = _format_error_breakdown(
        {
            "other": 1,
            "connection_error": 2,
            "http_4xx": 3,
        }
    )

    assert formatted == "http_4xx=3, connection_error=2, other=1"


@pytest.mark.asyncio
async def test_daemon_logs_cycle_error_breakdown(caplog):
    class FakeStorage:
        def close(self):
            return None

    class FakeFrontier:
        def pending_count(self, queue_classes=None):
            return 1

        def readiness(self):
            return SimpleNamespace(pending=1, ready=1, next_ready_delay=None)

        def defer_overcrowded_backlog(self, **_kwargs):
            return 0

        def recover_leased(self, expired_only=False):
            return 0

        def upsert_seeds(self, urls, priority=2.0):
            return len(urls)

        def stats(self):
            return {"pending": 1, "total": 1}

    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        recrawl_ttl=3600,
    )
    frontier = FakeFrontier()
    storage = FakeStorage()

    daemon._install_signals = lambda: None
    daemon._recrawl_stale = lambda _storage, _frontier: None

    async def fake_connect():
        return storage, frontier

    async def fake_run_cycle(_storage, _frontier):
        daemon._shutdown = True
        return 2, {"http_4xx": 3, "timeout": 1}

    daemon._connect = fake_connect
    daemon._run_cycle = fake_run_cycle

    with caplog.at_level("INFO", logger="crawler.daemon"):
        await daemon.run()

    assert "errors=http_4xx=3, timeout=1" in caplog.text


@pytest.mark.asyncio
async def test_daemon_uses_configured_backlog_controls():
    class FakeStorage:
        def close(self):
            return None

    class FakeFrontier:
        def __init__(self):
            self.defer_args = None

        def pending_count(self, queue_classes=None):
            return 1

        def readiness(self):
            return SimpleNamespace(pending=1, ready=1, next_ready_delay=None)

        def defer_overcrowded_backlog(self, **kwargs):
            self.defer_args = kwargs
            return 0

        def recover_leased(self, expired_only=False):
            return 0

        def upsert_seeds(self, urls, priority=2.0):
            return len(urls)

        def stats(self):
            return {"pending": 1, "total": 1}

    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        recrawl_ttl=3600,
        backlog_ready_per_domain=7,
        backlog_ready_per_branch=2,
        backlog_low_priority=0.4,
        backlog_defer_seconds=12.0,
    )
    frontier = FakeFrontier()
    storage = FakeStorage()

    daemon._install_signals = lambda: None
    daemon._recrawl_stale = lambda _storage, _frontier: None

    async def fake_connect():
        return storage, frontier

    async def fake_run_cycle(_storage, _frontier):
        daemon._shutdown = True
        return 0, {}

    daemon._connect = fake_connect
    daemon._run_cycle = fake_run_cycle

    await daemon.run()

    assert frontier.defer_args == {
        "keep_ready_per_domain": 7,
        "keep_ready_per_branch": 2,
        "low_priority_threshold": 0.4,
        "defer_seconds": 12.0,
    }


@pytest.mark.asyncio
async def test_daemon_persists_readiness_breakdown_while_waiting_for_ready():
    class FakeStorage:
        def __init__(self):
            self.payloads = []

        def close(self):
            return None

        def upsert_runtime_stats(self, component, payload):
            self.payloads.append((component, dict(payload)))

    class FakeFrontier:
        def pending_count(self, queue_classes=None):
            return 1

        def readiness(self):
            return SimpleNamespace(
                pending=3,
                ready=0,
                next_ready_delay=12.0,
                blocked={
                    "next_fetch_at": 2,
                    "domain_next_request": 1,
                    "host_backoff": 0,
                    "retry_quarantine": 2,
                },
                state_counts={
                    "ready": 0,
                    "scheduled": 0,
                    "blocked_domain_next_request": 1,
                    "blocked_host_backoff": 0,
                    "retry_quarantine": 2,
                },
            )

        def defer_overcrowded_backlog(self, **_kwargs):
            return 0

        def recover_leased(self, expired_only=False):
            return 0

        def upsert_seeds(self, urls, priority=2.0):
            return len(urls)

        def stats(self):
            return {"pending": 3, "total": 3}

    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        recrawl_ttl=3600,
        idle_sleep=30.0,
        min_ready_sleep=1.0,
    )
    frontier = FakeFrontier()
    storage = FakeStorage()

    daemon._install_signals = lambda: None
    daemon._recrawl_stale = lambda _storage, _frontier: None

    async def fake_connect():
        return storage, frontier

    async def fake_sleep(seconds):
        daemon._shutdown = True

    daemon._connect = fake_connect
    daemon._interruptible_sleep = fake_sleep

    await daemon.run()

    assert storage.payloads[-1][0] == "crawler"
    assert storage.payloads[-1][1]["state"] == "idle_waiting_ready"
    assert storage.payloads[-1][1]["next_ready_delay"] == 12.0
    assert storage.payloads[-1][1]["readiness_blocked"] == {
        "next_fetch_at": 2,
        "domain_next_request": 1,
        "host_backoff": 0,
        "retry_quarantine": 2,
    }
    assert storage.payloads[-1][1]["scheduler_state"] == {
        "ready": 0,
        "scheduled": 0,
        "blocked_domain_next_request": 1,
        "blocked_host_backoff": 0,
        "retry_quarantine": 2,
    }


def test_ensure_seeds_tops_up_when_exploration_queue_is_starved():
    class FakeFrontier:
        def __init__(self):
            self.upsert_calls = []
            self.branch_promote_calls = []

        def pending_count(self, queue_classes=None):
            if queue_classes == ["exploration"]:
                return 1
            return 25

        def ready_count(self, queue_classes=None, now=None):
            assert queue_classes == ["exploration"]
            return 1

        def promote_branch_novelty_exploration(self, target_pending, per_domain=1):
            self.branch_promote_calls.append((target_pending, per_domain))
            return 2

        def upsert_seeds(self, urls, priority=2.0):
            self.upsert_calls.append((list(urls), priority))
            return len(urls)

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        recrawl_ttl=3600,
    )
    frontier = FakeFrontier()

    daemon._ensure_seeds(frontier)

    assert frontier.branch_promote_calls == [(3, 1)]
    assert frontier.upsert_calls == []


def test_ensure_seeds_falls_back_to_seed_reinsertion_when_branch_promotion_is_insufficient():
    class FakeFrontier:
        def __init__(self):
            self.upsert_calls = []
            self.branch_promote_calls = []
            self.seed_promote_calls = []

        def pending_count(self, queue_classes=None):
            if queue_classes == ["exploration"]:
                return 1
            return 25

        def ready_count(self, queue_classes=None, now=None):
            assert queue_classes == ["exploration"]
            return 1

        def promote_branch_novelty_exploration(self, target_pending, per_domain=1):
            self.branch_promote_calls.append((target_pending, per_domain))
            return 1

        def upsert_seeds(self, urls, priority=2.0):
            self.upsert_calls.append((list(urls), priority))
            return len(urls)

        def promote_seed_host_exploration(self, seed_hosts, per_host=1, max_depth=2):
            self.seed_promote_calls.append((list(seed_hosts), per_host, max_depth))
            return 1

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        recrawl_ttl=3600,
    )
    frontier = FakeFrontier()

    daemon._ensure_seeds(frontier)

    assert frontier.branch_promote_calls == [(3, 1)]
    assert frontier.upsert_calls == [
        ([
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ], 2.0)
    ]
    assert frontier.seed_promote_calls == [
        (["datatracker.ietf.org", "www.iana.org", "www.rfc-editor.org"], 1, 2)
    ]


def test_ensure_seeds_does_not_top_up_when_exploration_queue_is_healthy():
    class FakeFrontier:
        def __init__(self):
            self.upsert_calls = []

        def pending_count(self, queue_classes=None):
            if queue_classes == ["exploration"]:
                return 3
            return 25

        def ready_count(self, queue_classes=None, now=None):
            assert queue_classes == ["exploration"]
            return 3

        def upsert_seeds(self, urls, priority=2.0):
            self.upsert_calls.append((list(urls), priority))
            return len(urls)

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        recrawl_ttl=3600,
    )
    frontier = FakeFrontier()

    daemon._ensure_seeds(frontier)

    assert frontier.upsert_calls == []


def test_ensure_seeds_reinserts_when_exploration_pending_is_high_but_ready_is_zero():
    class FakeFrontier:
        def __init__(self):
            self.upsert_calls = []
            self.branch_promote_calls = []

        def pending_count(self, queue_classes=None):
            if queue_classes == ["exploration"]:
                return 25
            return 50

        def ready_count(self, queue_classes=None, now=None):
            assert queue_classes == ["exploration"]
            return 0

        def promote_branch_novelty_exploration(self, target_pending, per_domain=1):
            self.branch_promote_calls.append((target_pending, per_domain))
            return 0

        def upsert_seeds(self, urls, priority=2.0):
            self.upsert_calls.append((list(urls), priority))
            return len(urls)

    daemon = CrawlDaemon(
        seeds=[
            "https://www.wikipedia.org/",
            "https://www.wikidata.org/",
            "https://www.openstreetmap.org/",
            "https://github.com/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        recrawl_ttl=3600,
    )
    frontier = FakeFrontier()

    daemon._ensure_seeds(frontier)

    assert frontier.branch_promote_calls == [(29, 1)]
    assert frontier.upsert_calls == [
        ([
            "https://www.wikipedia.org/",
            "https://www.wikidata.org/",
            "https://www.openstreetmap.org/",
            "https://github.com/",
        ], 2.0)
    ]


def test_promote_blocked_retry_restores_small_subset_when_ready_is_thin():
    class FakeFrontier:
        def __init__(self):
            self.calls = []

        def ready_count(self, queue_classes=None, now=None):
            assert queue_classes is None
            return 3

        def promote_blocked_domain_backoff(self, limit, per_domain=1, max_consecutive_failures=None):
            self.calls.append((limit, per_domain, max_consecutive_failures))
            return 2

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        recrawl_ttl=3600,
    )
    frontier = FakeFrontier()

    promoted = daemon._promote_blocked_retry(frontier)

    assert promoted == 2
    assert frontier.calls == [(8, 1, 8)]


def test_promote_blocked_retry_skips_when_ready_is_healthy():
    class FakeFrontier:
        def __init__(self):
            self.calls = []

        def ready_count(self, queue_classes=None, now=None):
            assert queue_classes is None
            return 20

        def promote_blocked_domain_backoff(self, limit, per_domain=1, max_consecutive_failures=None):
            self.calls.append((limit, per_domain, max_consecutive_failures))
            return 1

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        recrawl_ttl=3600,
    )
    frontier = FakeFrontier()

    promoted = daemon._promote_blocked_retry(frontier)

    assert promoted == 0
    assert frontier.calls == []
