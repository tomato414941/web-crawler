"""Behavior tests for crawl daemon queue management."""

import os
import time
from types import SimpleNamespace

import psycopg2
import pytest

from crawler.daemon import CrawlDaemon, _format_error_breakdown
from crawler.url_ledger import (
    BLOCKED_HOST_BACKOFF_TABLE,
    CrawlTask,
    HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE,
    LEASE_TABLE,
    PHYSICAL_QUEUE_TABLES,
    QUEUE_REFRESH,
    QUEUE_RUNNABLE,
    QUEUE_SCHEDULED,
    URL_LEDGER_TABLE,
    UrlLedger,
)
from crawler.migrate import apply_migrations
from crawler.storage import PgStorage
from crawler.host_ledger import HOST_LEDGER_TABLE

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
            cur.execute(f"DROP TABLE IF EXISTS public.{HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE}")
            cur.execute(f"DROP TABLE IF EXISTS public.{HOST_LEDGER_TABLE}")
            cur.execute("DROP TABLE IF EXISTS public.host_state")
            cur.execute(f"DROP TABLE IF EXISTS public.{PHYSICAL_QUEUE_TABLES[QUEUE_RUNNABLE]}")
            cur.execute(f"DROP TABLE IF EXISTS public.{PHYSICAL_QUEUE_TABLES[QUEUE_SCHEDULED]}")
            cur.execute(f"DROP TABLE IF EXISTS public.{PHYSICAL_QUEUE_TABLES[QUEUE_REFRESH]}")
            cur.execute(f"DROP TABLE IF EXISTS public.{BLOCKED_HOST_BACKOFF_TABLE}")
            cur.execute(f"DROP TABLE IF EXISTS public.{LEASE_TABLE}")
            cur.execute(f"DROP TABLE IF EXISTS public.{URL_LEDGER_TABLE} CASCADE")
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
    ledger = UrlLedger(storage.conn)

    yield dsn, storage, ledger

    storage._conn.rollback()
    storage.close()
    _reset_schema(dsn)


def _save_page(storage: PgStorage, url: str, timestamp: float) -> None:
    storage.save(
        {
            "url": url,
            "status": 200,
            "content_length": 100,
            "timestamp": timestamp,
            "content": "<html><title>Example</title></html>",
            "outlinks": [],
        }
    )


def test_refresh_stale_skips_when_pending_queue_is_full(pg_resources):
    _dsn, storage, ledger = pg_resources
    now = time.time()

    for idx in range(3):
        ledger.place(CrawlTask(url=f"https://example.com/pending-{idx}", added_at=now + idx))

    stale_url = "https://example.com/stale"
    ledger.place(CrawlTask(url=stale_url, added_at=now - 100))
    ledger.mark_done(stale_url)
    _save_page(storage, stale_url, now - 86400)

    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
        cycle_pages=2,
        refresh_ttl=3600,
    )

    daemon._refresh_stale(storage, ledger)

    with storage._conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM {PHYSICAL_QUEUE_TABLES[QUEUE_REFRESH]} WHERE url = %s",
            (stale_url,),
        )
        (refresh_count,) = cur.fetchone()

    assert ledger.pending_count() == 3
    assert refresh_count == 0


def test_refresh_stale_requeues_only_oldest_rows_needed(pg_resources):
    _dsn, storage, ledger = pg_resources
    now = time.time()

    ledger.place(CrawlTask(url="https://example.com/pending", added_at=now))

    stale_urls = [
        ("https://example.com/stale-1", now - 300),
        ("https://example.com/stale-2", now - 200),
        ("https://example.com/stale-3", now - 100),
    ]
    for url, added_at in stale_urls:
        ledger.place(CrawlTask(url=url, added_at=added_at))
        ledger.mark_done(url)
        _save_page(storage, url, added_at)

    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
        cycle_pages=3,
        refresh_ttl=60,
    )

    daemon._refresh_stale(storage, ledger)

    with storage._conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT url
            FROM {PHYSICAL_QUEUE_TABLES[QUEUE_REFRESH]}
            WHERE url LIKE 'https://example.com/stale-%'
            ORDER BY url
            """
        )
        refresh_urls = [url for (url,) in cur.fetchall()]

    assert ledger.pending_count() == 3
    assert refresh_urls == [
        "https://example.com/stale-1",
        "https://example.com/stale-2",
    ]


@pytest.mark.asyncio
async def test_daemon_does_not_auto_requeue_failed_urls():
    class FakeStorage:
        def close(self):
            return None

    class FakeLedger:
        def __init__(self):
            self.requeue_failed_calls = 0

        def pending_count(self, runnable_surface=None):
            return 1

        def readiness(self):
            return SimpleNamespace(pending=1, runnable=1, next_runnable_delay=None)

        def delay_overcrowded_scheduled_surface(self, **_kwargs):
            return 0

        def recover_leased(self, expired_only=False):
            return 0

        def upsert_seeds(self, urls, discovery_value=2.0):
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
        refresh_ttl=3600,
    )
    ledger = FakeLedger()
    storage = FakeStorage()

    daemon._install_signals = lambda: None
    daemon._refresh_stale = lambda _storage, _ledger: None

    async def fake_connect():
        return storage, ledger

    async def fake_run_cycle(_storage, _ledger):
        daemon._shutdown = True
        return 0, {}

    daemon._connect = fake_connect
    daemon._run_cycle = fake_run_cycle

    await daemon.run()

    assert ledger.requeue_failed_calls == 0


def test_bootstrap_scheduler_inserts_seeds_only_when_empty():
    class FakeLedger:
        def __init__(self, pending):
            self._pending = pending
            self.upsert_calls = []

        def pending_count(self, runnable_surface=None):
            return self._pending

        def upsert_seeds(self, urls, discovery_value=2.0):
            self.upsert_calls.append((list(urls), discovery_value))
            self._pending = len(urls)
            return len(urls)

    daemon = CrawlDaemon(
        seeds=["https://example.com/", "https://example.org/"],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )

    empty_ledger = FakeLedger(pending=0)
    populated_ledger = FakeLedger(pending=3)

    inserted = daemon._bootstrap_scheduler(empty_ledger)
    skipped = daemon._bootstrap_scheduler(populated_ledger)

    assert inserted == 2
    assert empty_ledger.upsert_calls == [(["https://example.com/", "https://example.org/"], 2.0)]
    assert skipped == 0
    assert populated_ledger.upsert_calls == []


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

    class FakeLedger:
        def pending_count(self, runnable_surface=None):
            return 1

        def readiness(self):
            return SimpleNamespace(pending=1, runnable=1, next_runnable_delay=None)

        def delay_overcrowded_scheduled_surface(self, **_kwargs):
            return 0

        def recover_leased(self, expired_only=False):
            return 0

        def upsert_seeds(self, urls, discovery_value=2.0):
            return len(urls)

        def stats(self):
            return {"pending": 1, "total": 1}

    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()
    storage = FakeStorage()

    daemon._install_signals = lambda: None
    daemon._refresh_stale = lambda _storage, _ledger: None

    async def fake_connect():
        return storage, ledger

    async def fake_run_cycle(_storage, _ledger):
        daemon._shutdown = True
        return (
            2,
            {"http_4xx": 3, "timeout": 1},
            {"samples": 2, "outcomes": {"success": 2, "skipped": 0, "failed": 0}},
            "lease_p95=1.0ms fetch_p95=2.0ms",
        )

    daemon._connect = fake_connect
    daemon._run_cycle = fake_run_cycle

    with caplog.at_level("INFO", logger="crawler.daemon"):
        await daemon.run()

    assert "errors=http_4xx=3, timeout=1" in caplog.text
    assert "timings=lease_p95=1.0ms fetch_p95=2.0ms" in caplog.text


@pytest.mark.asyncio
async def test_daemon_uses_configured_scheduled_controls():
    class FakeStorage:
        def close(self):
            return None

    class FakeLedger:
        def __init__(self):
            self.delay_args = None

        def pending_count(self, runnable_surface=None):
            return 1

        def readiness(self):
            return SimpleNamespace(pending=1, runnable=1, next_runnable_delay=None)

        def delay_overcrowded_scheduled_surface(self, **kwargs):
            self.delay_args = kwargs
            return 0

        def recover_leased(self, expired_only=False):
            return 0

        def upsert_seeds(self, urls, discovery_value=2.0):
            return len(urls)

        def stats(self):
            return {"pending": 1, "total": 1}

    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
        scheduled_runnable_per_host=7,
        scheduled_runnable_per_branch=2,
        scheduled_surface_delay_seconds=12.0,
    )
    ledger = FakeLedger()
    storage = FakeStorage()

    daemon._install_signals = lambda: None
    daemon._refresh_stale = lambda _storage, _ledger: None

    async def fake_connect():
        return storage, ledger

    async def fake_run_cycle(_storage, _ledger):
        daemon._shutdown = True
        return 0, {}

    daemon._connect = fake_connect
    daemon._run_cycle = fake_run_cycle

    await daemon.run()

    assert ledger.delay_args == {
        "keep_runnable_per_host": 7,
        "keep_runnable_per_branch": 2,
        "limit": 0,
        "delay_seconds": 12.0,
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

    class FakeLedger:
        def pending_count(self, runnable_surface=None):
            return 1

        def runnable_count(self, runnable_surface=None, now=None):
            return 0

        def runnable_host_count(self, runnable_surface=None, now=None):
            return 0

        def readiness(self):
            return SimpleNamespace(
                pending=3,
                runnable=0,
                next_runnable_delay=12.0,
                blocked={
                    "next_fetch_at": 2,
                    "host_next_request": 1,
                    "host_backoff": 0,
                    "retry_quarantine": 2,
                },
                state_counts={
                    "runnable": 0,
                    "scheduled": 0,
                    "blocked_host_next_request": 1,
                    "blocked_host_backoff": 0,
                    "retry_quarantine": 2,
                },
            )

        def delay_overcrowded_scheduled_surface(self, **_kwargs):
            return 0

        def recover_leased(self, expired_only=False):
            return 0

        def upsert_seeds(self, urls, discovery_value=2.0):
            return len(urls)

        def stats(self):
            return {"pending": 3, "total": 3}

    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
        idle_sleep=30.0,
        min_runnable_sleep=1.0,
    )
    ledger = FakeLedger()
    storage = FakeStorage()

    daemon._install_signals = lambda: None
    daemon._refresh_stale = lambda _storage, _ledger: None

    async def fake_connect():
        return storage, ledger

    async def fake_sleep(seconds):
        daemon._shutdown = True

    daemon._connect = fake_connect
    daemon._interruptible_sleep = fake_sleep

    await daemon.run()

    assert storage.payloads[-1][0] == "crawler"
    assert storage.payloads[-1][1]["state"] == "idle_waiting_runnable"
    assert storage.payloads[-1][1]["next_runnable_delay"] == 12.0
    assert storage.payloads[-1][1]["readiness_blocked"] == {
        "next_fetch_at": 2,
        "host_next_request": 1,
        "host_backoff": 0,
        "retry_quarantine": 2,
    }
    assert storage.payloads[-1][1]["readiness_blocked_reasons"] == {
        "next_fetch_at": 2,
        "host_next_request": 1,
        "host_backoff": 0,
        "retry_quarantine": 2,
    }
    assert storage.payloads[-1][1]["scheduler_state"] == {
        "runnable": 0,
        "scheduled": 0,
        "blocked_host_next_request": 1,
        "blocked_host_backoff": 0,
        "retry_quarantine": 2,
    }
    assert storage.payloads[-1][1]["scheduler_readiness_states"] == {
        "runnable": 0,
        "scheduled": 0,
        "blocked_host_next_request": 1,
        "blocked_host_backoff": 0,
        "retry_quarantine": 2,
    }
    assert storage.payloads[-1][1]["readiness_state_counts"] == {
        "runnable": 0,
        "scheduled": 0,
        "blocked_host_next_request": 1,
        "blocked_host_backoff": 0,
        "retry_quarantine": 2,
    }
    assert storage.payloads[-1][1]["blocked_reason_counts"] == {
        "next_fetch_at": 2,
        "host_next_request": 1,
        "host_backoff": 0,
        "retry_quarantine": 2,
    }
    assert storage.payloads[-1][1]["scheduler_state_snapshot"] == {
        "readiness_state_counts": {
            "runnable": 0,
            "scheduled": 0,
            "blocked_host_next_request": 1,
            "blocked_host_backoff": 0,
            "retry_quarantine": 2,
        },
        "effective_state_counts": {
            "scheduled": 0,
            "runnable": 0,
            "blocked": 5,
        },
        "blocked_reason_counts": {
            "next_fetch_at": 2,
            "host_next_request": 1,
            "host_backoff": 0,
            "retry_quarantine": 2,
        },
    }
    assert storage.payloads[-1][1]["effective_scheduler_states"] == {
        "scheduled": 0,
        "runnable": 0,
        "blocked": 5,
    }


@pytest.mark.asyncio
async def test_daemon_persists_scheduler_views_after_cycle():
    class FakeStorage:
        def __init__(self):
            self.payloads = []

        def close(self):
            return None

        def upsert_runtime_stats(self, component, payload):
            self.payloads.append((component, dict(payload)))

    class FakeLedger:
        def __init__(self):
            self.readiness_calls = 0
            self.daemon_readiness_calls = 0

        def pending_count(self, runnable_surface=None):
            return 2

        def runnable_count(self, runnable_surface=None, now=None):
            return 1

        def runnable_host_count(self, runnable_surface=None, now=None):
            return 1

        def daemon_readiness(self):
            self.daemon_readiness_calls += 1
            return SimpleNamespace(
                pending=2,
                runnable=1,
                next_runnable_delay=0.0,
                blocked={
                    "next_fetch_at": 0,
                    "host_next_request": 1,
                    "host_backoff": 0,
                    "retry_quarantine": 0,
                },
                state_counts={
                    "runnable": 1,
                    "scheduled": 0,
                    "blocked_host_next_request": 1,
                    "blocked_host_backoff": 0,
                    "retry_quarantine": 0,
                },
            )

        def readiness(self):
            self.readiness_calls += 1
            raise AssertionError("daemon cycle gating should use daemon_readiness")

        def delay_overcrowded_scheduled_surface(self, **_kwargs):
            return 0

        def recover_leased(self, expired_only=False):
            return 0

        def upsert_seeds(self, urls, discovery_value=2.0):
            return len(urls)

        def stats(self):
            raise AssertionError("cycle completion should not run live scheduler stats")

        def scheduler_state_snapshot(self, now=None):
            raise AssertionError("cycle completion should not run live scheduler snapshot")

    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()
    storage = FakeStorage()

    daemon._install_signals = lambda: None
    daemon._refresh_stale = lambda _storage, _ledger: None

    async def fake_connect():
        return storage, ledger

    async def fake_run_cycle(_storage, _ledger):
        daemon._shutdown = True
        return 2, {"timeout": 1}

    async def fake_sleep(_seconds):
        daemon._shutdown = True

    daemon._connect = fake_connect
    daemon._interruptible_sleep = fake_sleep
    daemon._run_cycle = fake_run_cycle

    await daemon.run()

    assert storage.payloads[-1][1]["state"] == "cycle_complete"
    assert ledger.daemon_readiness_calls == 1
    assert ledger.readiness_calls == 0
    assert storage.payloads[-1][1]["blocked_reason_counts"] == {
        "next_fetch_at": 0,
        "host_next_request": 1,
        "host_backoff": 0,
        "retry_quarantine": 0,
    }
    assert storage.payloads[-1][1]["readiness_blocked_reasons"] == {
        "next_fetch_at": 0,
        "host_next_request": 1,
        "host_backoff": 0,
        "retry_quarantine": 0,
    }
    assert storage.payloads[-1][1]["scheduler_state"] == {
        "runnable": 1,
        "scheduled": 0,
        "blocked_host_next_request": 1,
        "blocked_host_backoff": 0,
        "retry_quarantine": 0,
    }
    assert storage.payloads[-1][1]["scheduler_readiness_states"] == {
        "runnable": 1,
        "scheduled": 0,
        "blocked_host_next_request": 1,
        "blocked_host_backoff": 0,
        "retry_quarantine": 0,
    }
    assert storage.payloads[-1][1]["scheduler_state_snapshot"] == {
        "readiness_state_counts": {
            "runnable": 1,
            "scheduled": 0,
            "blocked_host_next_request": 1,
            "blocked_host_backoff": 0,
            "retry_quarantine": 0,
        },
        "effective_state_counts": {
            "scheduled": 0,
            "runnable": 1,
            "blocked": 1,
        },
        "blocked_reason_counts": {
            "next_fetch_at": 0,
            "host_next_request": 1,
            "host_backoff": 0,
            "retry_quarantine": 0,
        },
    }
    assert storage.payloads[-1][1]["effective_scheduler_states"] == {
        "scheduled": 0,
        "runnable": 1,
        "blocked": 1,
    }


def test_ensure_runnable_supply_tops_up_when_runnable_surface_is_starved():
    class FakeLedger:
        def __init__(self):
            self.upsert_calls = []
            self.host_promote_calls = []

        def pending_count(self, runnable_surface=None):
            if runnable_surface == "runnable":
                return 1
            return 25

        def runnable_count(self, runnable_surface=None, now=None):
            assert runnable_surface == "runnable"
            return 1

        def pending_host_count(self, runnable_surface=None):
            if runnable_surface == "runnable":
                return 1
            return 10

        def runnable_host_count(self, runnable_surface=None, now=None):
            if runnable_surface == "runnable":
                return 1
            return 10

        def promote_scheduled_host_heads(self, target_pending, per_host=1):
            self.host_promote_calls.append((target_pending, per_host))
            return 2

        def upsert_seeds(self, urls, discovery_value=2.0):
            self.upsert_calls.append((list(urls), discovery_value))
            return len(urls)

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    daemon._ensure_runnable_supply(ledger)

    assert ledger.host_promote_calls == [(3, 1)]
    assert ledger.upsert_calls == []


def test_admit_discovered_backfills_pending_deficit():
    class FakeLedger:
        def __init__(self):
            self.admit_calls = []

        def pending_count(self, runnable_surface=None):
            return 3

        def admit_discovered_urls(self, limit, runnable_surface=None, intent=None):
            self.admit_calls.append((limit, runnable_surface, intent))
            return 2

    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    admitted = daemon._policy.admit_discovered(ledger)

    assert admitted == 2
    assert ledger.admit_calls == [(7, "scheduled", "explore")]


def test_admit_discovered_stays_idle_when_pending_is_healthy():
    class FakeLedger:
        def __init__(self):
            self.admit_calls = []

        def pending_count(self, runnable_surface=None):
            return 12

        def admit_discovered_urls(self, limit, runnable_surface=None, intent=None):
            self.admit_calls.append((limit, runnable_surface, intent))
            return limit

    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    admitted = daemon._policy.admit_discovered(ledger)

    assert admitted == 0
    assert ledger.admit_calls == []


def test_ensure_runnable_supply_does_not_bootstrap_empty_ledger():
    class FakeLedger:
        def __init__(self):
            self.upsert_calls = []
            self.host_promote_calls = []

        def pending_count(self, runnable_surface=None):
            return 0

        def runnable_count(self, runnable_surface=None, now=None):
            assert runnable_surface == "runnable"
            return 0

        def runnable_host_count(self, runnable_surface=None, now=None):
            assert runnable_surface == "runnable"
            return 0

        def promote_scheduled_host_heads(self, target_pending, per_host=1):
            self.host_promote_calls.append((target_pending, per_host))
            return 0

        def upsert_seeds(self, urls, discovery_value=2.0):
            self.upsert_calls.append((list(urls), discovery_value))
            return len(urls)

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    daemon._ensure_runnable_supply(ledger)

    assert ledger.host_promote_calls == [(3, 1)]
    assert ledger.upsert_calls == []


def test_ensure_runnable_supply_stays_idle_when_host_promotion_is_insufficient():
    class FakeLedger:
        def __init__(self):
            self.upsert_calls = []
            self.host_promote_calls = []

        def pending_count(self, runnable_surface=None):
            if runnable_surface == "runnable":
                return 1
            return 25

        def runnable_count(self, runnable_surface=None, now=None):
            assert runnable_surface == "runnable"
            return 1

        def pending_host_count(self, runnable_surface=None):
            if runnable_surface == "runnable":
                return 1
            return 10

        def runnable_host_count(self, runnable_surface=None, now=None):
            if runnable_surface == "runnable":
                return 1
            return 10

        def promote_scheduled_host_heads(self, target_pending, per_host=1):
            self.host_promote_calls.append((target_pending, per_host))
            return 0

        def upsert_seeds(self, urls, discovery_value=2.0):
            self.upsert_calls.append((list(urls), discovery_value))
            return len(urls)

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    daemon._ensure_runnable_supply(ledger)

    assert ledger.host_promote_calls == [(3, 1)]
    assert ledger.upsert_calls == []


def test_ensure_runnable_supply_does_not_top_up_when_runnable_surface_is_healthy():
    class FakeLedger:
        def __init__(self):
            self.upsert_calls = []

        def pending_count(self, runnable_surface=None):
            if runnable_surface == "runnable":
                return 3
            return 25

        def runnable_count(self, runnable_surface=None, now=None):
            assert runnable_surface == "runnable"
            return 3

        def pending_host_count(self, runnable_surface=None):
            if runnable_surface == "runnable":
                return 3
            return 10

        def runnable_host_count(self, runnable_surface=None, now=None):
            if runnable_surface == "runnable":
                return 3
            return 10

        def upsert_seeds(self, urls, discovery_value=2.0):
            self.upsert_calls.append((list(urls), discovery_value))
            return len(urls)

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    daemon._ensure_runnable_supply(ledger)

    assert ledger.upsert_calls == []


def test_ensure_runnable_supply_does_not_reinsert_when_runnable_pending_is_high_but_runnable_is_zero():
    class FakeLedger:
        def __init__(self):
            self.upsert_calls = []
            self.host_promote_calls = []

        def pending_count(self, runnable_surface=None):
            if runnable_surface == "runnable":
                return 25
            return 50

        def runnable_count(self, runnable_surface=None, now=None):
            assert runnable_surface == "runnable"
            return 0

        def pending_host_count(self, runnable_surface=None):
            if runnable_surface == "runnable":
                return 2
            return 12

        def runnable_host_count(self, runnable_surface=None, now=None):
            if runnable_surface == "runnable":
                return 0
            return 12

        def promote_scheduled_host_heads(self, target_pending, per_host=1):
            self.host_promote_calls.append((target_pending, per_host))
            return 0

        def upsert_seeds(self, urls, discovery_value=2.0):
            self.upsert_calls.append((list(urls), discovery_value))
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
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    daemon._ensure_runnable_supply(ledger)

    assert ledger.host_promote_calls == [(29, 1)]
    assert ledger.upsert_calls == []


def test_ensure_runnable_supply_tops_up_when_runnable_host_diversity_is_low():
    class FakeLedger:
        def __init__(self):
            self.upsert_calls = []
            self.host_promote_calls = []

        def pending_count(self, runnable_surface=None):
            if runnable_surface == "runnable":
                return 20
            return 50

        def runnable_count(self, runnable_surface=None, now=None):
            assert runnable_surface == "runnable"
            return 3

        def pending_host_count(self, runnable_surface=None):
            if runnable_surface == "runnable":
                return 1
            return 10

        def runnable_host_count(self, runnable_surface=None, now=None):
            if runnable_surface == "runnable":
                return 1
            return 10

        def promote_scheduled_host_heads(self, target_pending, per_host=1):
            self.host_promote_calls.append((target_pending, per_host))
            return 1

        def upsert_seeds(self, urls, discovery_value=2.0):
            self.upsert_calls.append((list(urls), discovery_value))
            return len(urls)

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    daemon._ensure_runnable_supply(ledger)

    assert ledger.host_promote_calls == [(22, 1)]
    assert ledger.upsert_calls == []


def test_ensure_runnable_supply_stays_idle_when_only_runnable_depth_is_low():
    class FakeLedger:
        def __init__(self):
            self.host_promote_calls = []

        def pending_count(self, runnable_surface=None):
            if runnable_surface == "runnable":
                return 6
            return 30

        def runnable_count(self, runnable_surface=None, now=None):
            assert runnable_surface == "runnable"
            return 1

        def runnable_host_count(self, runnable_surface=None, now=None):
            if runnable_surface == "runnable":
                return 3
            return 10

        def promote_scheduled_host_heads(self, target_pending, per_host=1):
            self.host_promote_calls.append((target_pending, per_host))
            return 1

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    daemon._ensure_runnable_supply(ledger)

    assert ledger.host_promote_calls == []


def test_promote_blocked_retry_restores_small_subset_when_ready_is_thin():
    class FakeLedger:
        def __init__(self):
            self.calls = []

        def runnable_count(self, runnable_surface=None, now=None):
            return 2

        def pending_host_count(self, runnable_surface=None):
            assert runnable_surface == "runnable"
            return 3

        def runnable_host_count(self, runnable_surface=None, now=None):
            assert runnable_surface == "runnable"
            return 3

        def blocked_host_backoff_count(self):
            return 12

        def blocked_reason_counts(self):
            raise AssertionError("retry promotion should use the cheap blocked queue count")

        def promote_blocked_host_backoff(self, limit, per_host=1, max_consecutive_failures=None):
            self.calls.append((limit, per_host, max_consecutive_failures))
            return 2

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    promoted = daemon._promote_blocked_retry(ledger)

    assert promoted == 2
    assert ledger.calls == [(8, 1, 8)]


def test_promote_blocked_retry_surges_when_runnable_is_zero():
    class FakeLedger:
        def __init__(self):
            self.calls = []

        def runnable_count(self, runnable_surface=None, now=None):
            return 0

        def pending_host_count(self, runnable_surface=None):
            assert runnable_surface == "runnable"
            return 3

        def runnable_host_count(self, runnable_surface=None, now=None):
            assert runnable_surface == "runnable"
            return 3

        def blocked_reason_counts(self):
            return {
                "next_fetch_at": 0,
                "host_next_request": 0,
                "host_backoff": 0,
                "retry_quarantine": 20,
            }

        def promote_blocked_host_backoff(self, limit, per_host=1, max_consecutive_failures=None):
            self.calls.append((limit, per_host, max_consecutive_failures))
            return 5

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    promoted = daemon._promote_blocked_retry(ledger)

    assert promoted == 5
    assert ledger.calls == [(8, 8, 8)]


def test_promote_blocked_retry_skips_when_runnable_is_healthy():
    class FakeLedger:
        def __init__(self):
            self.calls = []

        def runnable_count(self, runnable_surface=None, now=None):
            return 20

        def pending_host_count(self, runnable_surface=None):
            assert runnable_surface == "runnable"
            return 3

        def runnable_host_count(self, runnable_surface=None, now=None):
            assert runnable_surface == "runnable"
            return 3

        def blocked_reason_counts(self):
            return {
                "next_fetch_at": 0,
                "host_next_request": 0,
                "host_backoff": 0,
                "retry_quarantine": 5,
            }

        def promote_blocked_host_backoff(self, limit, per_host=1, max_consecutive_failures=None):
            self.calls.append((limit, per_host, max_consecutive_failures))
            return 1

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    promoted = daemon._promote_blocked_retry(ledger)

    assert promoted == 0
    assert ledger.calls == []


def test_promote_blocked_retry_runs_when_runnable_is_healthy_but_host_diversity_is_low():
    class FakeLedger:
        def __init__(self):
            self.calls = []

        def runnable_count(self, runnable_surface=None, now=None):
            return 20

        def pending_host_count(self, runnable_surface=None):
            assert runnable_surface == "runnable"
            return 1

        def runnable_host_count(self, runnable_surface=None, now=None):
            assert runnable_surface == "runnable"
            return 1

        def blocked_reason_counts(self):
            return {
                "next_fetch_at": 0,
                "host_next_request": 0,
                "host_backoff": 0,
                "retry_quarantine": 12,
            }

        def promote_blocked_host_backoff(self, limit, per_host=1, max_consecutive_failures=None):
            self.calls.append((limit, per_host, max_consecutive_failures))
            return 3

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    promoted = daemon._promote_blocked_retry(ledger)

    assert promoted == 3
    assert ledger.calls == [(8, 1, 8)]


def test_promote_blocked_retry_skips_when_no_retry_quarantine_is_present():
    class FakeLedger:
        def __init__(self):
            self.calls = []

        def runnable_count(self, runnable_surface=None, now=None):
            return 0

        def runnable_host_count(self, runnable_surface=None, now=None):
            assert runnable_surface == "runnable"
            return 1

        def blocked_reason_counts(self):
            return {
                "next_fetch_at": 0,
                "host_next_request": 2,
                "host_backoff": 0,
                "retry_quarantine": 0,
            }

        def promote_blocked_host_backoff(self, limit, per_host=1, max_consecutive_failures=None):
            self.calls.append((limit, per_host, max_consecutive_failures))
            return 99

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    promoted = daemon._promote_blocked_retry(ledger)

    assert promoted == 0
    assert ledger.calls == []


def test_promote_blocked_retry_caps_budget_to_retry_quarantine_count():
    class FakeLedger:
        def __init__(self):
            self.calls = []

        def runnable_count(self, runnable_surface=None, now=None):
            return 0

        def runnable_host_count(self, runnable_surface=None, now=None):
            assert runnable_surface == "runnable"
            return 1

        def blocked_reason_counts(self):
            return {
                "next_fetch_at": 0,
                "host_next_request": 0,
                "host_backoff": 0,
                "retry_quarantine": 3,
            }

        def promote_blocked_host_backoff(self, limit, per_host=1, max_consecutive_failures=None):
            self.calls.append((limit, per_host, max_consecutive_failures))
            return limit

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    promoted = daemon._promote_blocked_retry(ledger)

    assert promoted == 3
    assert ledger.calls == [(3, 3, 8)]


def test_retire_blocked_retry_uses_configured_thresholds():
    class FakeLedger:
        def __init__(self):
            self.calls = []

        def retire_blocked_host_backoff(self, *, min_consecutive_failures, min_quarantine_seconds):
            self.calls.append((min_consecutive_failures, min_quarantine_seconds))
            return 3

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    retired = daemon._retire_blocked_retry(ledger)

    assert retired == 3
    assert ledger.calls == [(64, 86400.0)]


def test_retire_blocked_retry_skips_when_no_retry_quarantine_is_present():
    class FakeLedger:
        def __init__(self):
            self.calls = []

        def blocked_reason_counts(self):
            return {
                "next_fetch_at": 0,
                "host_next_request": 1,
                "host_backoff": 0,
                "retry_quarantine": 0,
            }

        def retire_blocked_host_backoff(self, *, min_consecutive_failures, min_quarantine_seconds):
            self.calls.append((min_consecutive_failures, min_quarantine_seconds))
            return 9

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=10,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    retired = daemon._retire_blocked_retry(ledger)

    assert retired == 0
    assert ledger.calls == []


def test_restore_recovered_blocked_retry_uses_cycle_sized_budget():
    class FakeLedger:
        def __init__(self):
            self.calls = []

        def restore_recovered_blocked_host_backoff(self, *, limit, per_host):
            self.calls.append((limit, per_host))
            return 7

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=300,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    restored = daemon._restore_recovered_blocked_retry(ledger)

    assert restored == 7
    assert ledger.calls == [(300, 8)]


def test_restore_recovered_blocked_retry_skips_when_no_retry_quarantine_is_present():
    class FakeLedger:
        def __init__(self):
            self.calls = []

        def blocked_reason_counts(self):
            return {
                "next_fetch_at": 0,
                "host_next_request": 0,
                "host_backoff": 2,
                "retry_quarantine": 0,
            }

        def restore_recovered_blocked_host_backoff(self, *, limit, per_host):
            self.calls.append((limit, per_host))
            return 11

    daemon = CrawlDaemon(
        seeds=[
            "https://www.iana.org/",
            "https://datatracker.ietf.org/",
            "https://www.rfc-editor.org/",
        ],
        postgres_dsn="postgresql://unused",
        cycle_pages=300,
        refresh_ttl=3600,
    )
    ledger = FakeLedger()

    restored = daemon._restore_recovered_blocked_retry(ledger)

    assert restored == 0
    assert ledger.calls == []
