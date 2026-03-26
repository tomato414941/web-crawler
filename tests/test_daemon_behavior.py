"""Behavior tests for crawl daemon queue management."""

import os
import time

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
            cur.execute("DROP TABLE IF EXISTS public.frontier")
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
        frontier.add(CrawlTask(url=f"https://example.com/pending-{idx}", depth=0, added_at=now + idx))

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

        def pending_count(self):
            return 1

        def ready_count(self):
            return 1

        def next_ready_delay(self):
            return None

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
        def pending_count(self):
            return 1

        def ready_count(self):
            return 1

        def next_ready_delay(self):
            return None

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

        def pending_count(self):
            return 1

        def ready_count(self):
            return 1

        def next_ready_delay(self):
            return None

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
        "low_priority_threshold": 0.4,
        "defer_seconds": 12.0,
    }
