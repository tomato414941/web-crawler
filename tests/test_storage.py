"""Tests for Postgres storage."""

import os
import time

import pytest
import psycopg2
from psycopg2.extensions import TRANSACTION_STATUS_IDLE

from crawler.migrate import apply_migrations

# Skip all tests if no Postgres available
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
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_exploration")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_backlog")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_recrawl")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_blocked_domain_backoff")
            cur.execute("DROP TABLE IF EXISTS public.active_leases")
            cur.execute("DROP TABLE IF EXISTS public.frontier_lease_active")
            cur.execute("DROP TABLE IF EXISTS public.frontier")
            cur.execute("DROP TABLE IF EXISTS public.crawler_runtime_stats")
            cur.execute("DROP TABLE IF EXISTS public.pages")
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def pg_storage():
    from crawler.storage import PgStorage

    dsn = os.environ["TEST_POSTGRES_DSN"]
    _reset_schema(dsn)
    apply_migrations(dsn)
    storage = PgStorage(dsn)
    yield storage
    # Cleanup
    storage._conn.rollback()
    storage.close()
    _reset_schema(dsn)


def test_save_page(pg_storage):
    result = {
        "url": "https://example.com/page1",
        "status": 200,
        "content_length": 1000,
        "depth": 0,
        "source_url": None,
        "timestamp": 1710000000.0,
        "content": "<html><title>Test Page</title><body>Hello</body></html>",
        "outlinks": ["https://example.com/page2"],
    }
    assert pg_storage.save(result) is True
    assert pg_storage.count == 1


def test_skip_error_result(pg_storage):
    result = {"url": "https://example.com/fail", "error": "timeout"}
    assert pg_storage.save(result) is False
    assert pg_storage.count == 0


def test_upsert_on_conflict(pg_storage):
    result = {
        "url": "https://example.com/page1",
        "status": 200,
        "content_length": 1000,
        "depth": 0,
        "timestamp": 1710000000.0,
        "content": "<html><title>V1</title><body>First</body></html>",
        "outlinks": [],
    }
    pg_storage.save(result)

    result["content"] = "<html><title>V2</title><body>Updated</body></html>"
    result["timestamp"] = 1710001000.0
    pg_storage.save(result)

    assert pg_storage.count == 2  # save called twice

    with pg_storage._conn.cursor() as cur:
        cur.execute("SELECT title FROM pages WHERE url = %s", ("https://example.com/page1",))
        assert cur.fetchone()[0] == "V2"


def test_save_drops_nul_content_to_metadata_only(pg_storage):
    result = {
        "url": "https://example.com/file.pdf",
        "status": 200,
        "content_length": 1000,
        "depth": 0,
        "timestamp": 1710000000.0,
        "content": "prefix\x00suffix",
        "outlinks": [],
    }

    assert pg_storage.save(result) is True

    with pg_storage._conn.cursor() as cur:
        cur.execute(
            "SELECT title, content FROM pages WHERE url = %s", ("https://example.com/file.pdf",)
        )
        title, content = cur.fetchone()

    assert title is None
    assert content == ""


def test_get_stats_includes_frontier_breakdown(pg_storage):
    page_results = [
        {
            "url": "https://example.com/page1",
            "status": 200,
            "content_length": 100,
            "depth": 0,
            "timestamp": 1710000000.0,
            "content": "<html><title>Example</title></html>",
            "outlinks": [],
        },
        {
            "url": "https://other.com/page1",
            "status": 200,
            "content_length": 50,
            "depth": 0,
            "timestamp": 1710000001.0,
            "content": "<html><title>Other</title></html>",
            "outlinks": [],
        },
    ]
    for result in page_results:
        pg_storage.save(result)

    with pg_storage._conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS public.frontier")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS frontier (
                url TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                depth INTEGER NOT NULL,
                priority REAL NOT NULL DEFAULT 1.0,
                discovery_kind TEXT NOT NULL DEFAULT 'seed',
                archetype TEXT NOT NULL DEFAULT 'generic_page',
                source_url TEXT,
                added_at DOUBLE PRECISION NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                next_fetch_at DOUBLE PRECISION NOT NULL DEFAULT 0,
                last_success_at DOUBLE PRECISION,
                fail_streak INTEGER NOT NULL DEFAULT 0,
                lease_token TEXT,
                lease_expires_at DOUBLE PRECISION,
                last_error TEXT
            )
            """
        )
        cur.execute(
            """
            INSERT INTO frontier (url, domain, depth, priority, queue_class, discovery_kind, archetype, source_url, added_at, status, next_fetch_at)
            VALUES
                ('https://example.com/page1', 'example.com', 0, 2.0, 'recrawl', 'seed', 'generic_page', NULL, 1710000000.0, 'done', 1710000000.0),
                ('https://example.com/page2', 'example.com', 1, 1.25, 'exploration', 'same_host', 'document_page', 'https://example.com/page1', 1710000002.0, 'pending', 1710000002.0),
                ('https://other.com/page1', 'other.com', 1, 0.8, 'exploration', 'external', 'redirect_hub', 'https://example.com/page1', 1710000003.0, 'pending', 1710000003.0)
            """
        )
        cur.execute(
            """
            INSERT INTO frontier_queue_exploration (url, domain, priority, next_fetch_at, added_at, branch_key)
            VALUES
                ('https://example.com/page2', 'example.com', 1.25, 1710000002.0, 1710000002.0, '/page2'),
                ('https://other.com/page1', 'other.com', 0.8, 1710000003.0, 1710000003.0, '/page1')
            """
        )
    pg_storage._conn.commit()

    stats = pg_storage.get_stats()

    assert stats["total_pages"] == 2
    assert stats["domains"] == 2
    assert stats["frontier_status"] == {"done": 1, "pending": 2}
    assert stats["legacy_frontier_status"] == {"done": 1, "pending": 2}
    assert stats["queue_classes"] == {"exploration": 2, "recrawl": 1}
    assert stats["pending_queue_classes"] == {"exploration": 2}
    assert stats["blocked_queue_classes"] == {}
    assert stats["readiness"] == {
        "pending": 2,
        "ready": 2,
        "next_ready_delay": 0.0,
        "blocked": {
            "next_fetch_at": 0,
            "domain_next_request": 0,
            "host_backoff": 0,
            "retry_quarantine": 0,
        },
        "state_counts": {
            "ready": 2,
            "scheduled": 0,
            "blocked_domain_next_request": 0,
            "blocked_host_backoff": 0,
            "retry_quarantine": 0,
        },
    }
    assert stats["archetypes"] == {"document_page": 1, "generic_page": 1, "redirect_hub": 1}
    assert stats["top_page_domains"][0] == {"domain": "example.com", "count": 1}
    assert stats["top_pending_domains"] == [
        {"domain": "example.com", "count": 1},
        {"domain": "other.com", "count": 1},
    ]
    assert stats["top_slow_domains"] == []
    assert stats["top_budget_domains"] == []
    assert stats["active_error_breakdown"] == {}
    assert stats["top_error_domains"] == []


def test_get_stats_includes_runtime_snapshot(pg_storage):
    pg_storage.upsert_runtime_stats(
        "crawler",
        {
            "running": True,
            "pages_per_second": 2.5,
            "pages": 15,
            "active_hosts": 3,
            "active_branches": 4,
            "parse_queue_size": 2,
            "finalize_queue_size": 1,
            "publish_queue_size": 1,
            "parse_queue_wait_max_ms": 12.5,
            "finalize_queue_wait_max_ms": 8.0,
            "publish_queue_wait_max_ms": 4.5,
            "errors": {"timeout": 1},
        },
    )

    stats = pg_storage.get_stats()

    assert stats["runtime"]["payload"] == {
        "running": True,
        "pages_per_second": 2.5,
        "pages": 15,
        "active_hosts": 3,
        "active_branches": 4,
        "parse_queue_size": 2,
        "finalize_queue_size": 1,
        "publish_queue_size": 1,
        "parse_queue_wait_max_ms": 12.5,
        "finalize_queue_wait_max_ms": 8.0,
        "publish_queue_wait_max_ms": 4.5,
        "errors": {"timeout": 1},
    }
    assert stats["runtime"]["updated_at"] > 0
    assert stats["operator_summary"] == {
        "scheduler_state": {
            "pending": 0,
            "ready": 0,
            "scheduled": 0,
            "blocked_domain_next_request": 0,
            "blocked_host_backoff": 0,
            "retry_quarantine": 0,
            "leased": 0,
        },
        "throughput": {
            "pages_per_second": 2.5,
            "cycle_pages": 15,
            "active_hosts": 3,
            "active_branches": 4,
            "errors": {"timeout": 1},
        },
        "backpressure": {
            "parse_queue_size": 2,
            "finalize_queue_size": 1,
            "publish_queue_size": 1,
            "parse_queue_wait_max_ms": 12.5,
            "finalize_queue_wait_max_ms": 8.0,
            "publish_queue_wait_max_ms": 4.5,
        },
        "adaptive_budget": {
            "observed_hosts": 0,
            "eligible_hosts": 0,
            "eligible_pending": 0,
            "ineligible_due_to_failures": 0,
            "ineligible_due_to_latency": 0,
            "max_budget": 1,
        },
    }


def test_get_stats_includes_readiness_breakdown(pg_storage):
    now = time.time()

    with pg_storage._conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO frontier (
                url, domain, depth, priority, queue_class, discovery_kind, archetype,
                source_url, added_at, status, next_fetch_at
            )
            VALUES
                ('https://ready.example/', 'ready.example', 0, 1.0, 'exploration', 'seed', 'generic_page', NULL, %s, 'pending', %s),
                ('https://future.example/', 'future.example', 0, 1.0, 'exploration', 'seed', 'generic_page', NULL, %s, 'pending', %s),
                ('https://backoff.example/', 'backoff.example', 0, 1.0, 'exploration', 'seed', 'generic_page', NULL, %s, 'pending', %s)
            """,
            (now, now, now, now + 30.0, now, now),
        )
        cur.execute(
            """
            INSERT INTO frontier_queue_exploration (url, domain, priority, next_fetch_at, added_at, branch_key)
            VALUES
                ('https://ready.example/', 'ready.example', 1.0, %s, %s, '/'),
                ('https://future.example/', 'future.example', 1.0, %s, %s, '/'),
                ('https://backoff.example/', 'backoff.example', 1.0, %s, %s, '/')
            """,
            (now, now, now + 30.0, now, now, now),
        )
        cur.execute(
            """
            INSERT INTO domain_state (
                host_key,
                crawl_delay_seconds,
                next_request_at,
                backoff_until,
                robots_checked_at,
                updated_at
            )
            VALUES ('backoff.example', 1.0, %s, %s, %s, %s)
            ON CONFLICT (host_key) DO UPDATE
            SET next_request_at = EXCLUDED.next_request_at,
                backoff_until = EXCLUDED.backoff_until,
                updated_at = EXCLUDED.updated_at
            """,
            (now, now + 20.0, now, now),
        )
    pg_storage._conn.commit()

    stats = pg_storage.get_stats()

    assert stats["readiness"]["pending"] == 3
    assert stats["readiness"]["ready"] == 1
    assert stats["readiness"]["next_ready_delay"] == pytest.approx(20.0, abs=1e-3)
    assert stats["readiness"]["blocked"] == {
        "next_fetch_at": 1,
        "domain_next_request": 1,
        "host_backoff": 0,
        "retry_quarantine": 0,
    }
    assert stats["readiness"]["state_counts"] == {
        "ready": 1,
        "scheduled": 1,
        "blocked_domain_next_request": 1,
        "blocked_host_backoff": 0,
        "retry_quarantine": 0,
    }
    assert stats["top_blocked_domains"] == [
        {
            "domain": "backoff.example",
            "pending_count": 1,
            "blocked_counts": {
                "domain_next_request": 1,
                "host_backoff": 0,
                "retry_quarantine": 0,
            },
            "wait_seconds": pytest.approx(20.0, abs=1e-3),
            "dominant_reason": "domain_next_request",
            "consecutive_failures": 0,
        }
    ]
    assert stats["operator_summary"]["scheduler_state"] == {
        "pending": 3,
        "ready": 1,
        "scheduled": 1,
        "blocked_domain_next_request": 1,
        "blocked_host_backoff": 0,
        "retry_quarantine": 0,
        "leased": 0,
    }


def test_get_stats_prioritizes_domains_blocked_by_backoff(pg_storage):
    now = time.time()

    with pg_storage._conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO frontier (
                url, domain, depth, priority, queue_class, discovery_kind, archetype,
                source_url, added_at, status, next_fetch_at
            )
            VALUES
                ('https://backoff.example/a', 'backoff.example', 0, 1.0, 'exploration', 'seed', 'generic_page', NULL, %s, 'pending', %s),
                ('https://backoff.example/b', 'backoff.example', 0, 1.0, 'exploration', 'seed', 'generic_page', NULL, %s, 'pending', %s),
                ('https://slot.example/', 'slot.example', 0, 1.0, 'exploration', 'seed', 'generic_page', NULL, %s, 'pending', %s)
            """,
            (now, now, now, now, now, now),
        )
        cur.execute(
            """
            INSERT INTO frontier_queue_exploration (url, domain, priority, next_fetch_at, added_at, branch_key)
            VALUES
                ('https://backoff.example/a', 'backoff.example', 1.0, %s, %s, '/a'),
                ('https://backoff.example/b', 'backoff.example', 1.0, %s, %s, '/b'),
                ('https://slot.example/', 'slot.example', 1.0, %s, %s, '/')
            """,
            (now, now, now, now, now, now),
        )
        cur.execute(
            """
            INSERT INTO domain_state (
                host_key,
                crawl_delay_seconds,
                next_request_at,
                backoff_until,
                consecutive_failures,
                robots_checked_at,
                updated_at
            )
            VALUES
                ('backoff.example', 1.0, %s, %s, 3, %s, %s),
                ('slot.example', 1.0, %s, %s, 0, %s, %s)
            ON CONFLICT (host_key) DO UPDATE
            SET next_request_at = EXCLUDED.next_request_at,
                backoff_until = EXCLUDED.backoff_until,
                consecutive_failures = EXCLUDED.consecutive_failures,
                updated_at = EXCLUDED.updated_at
            """,
            (now, now + 45.0, now, now, now + 15.0, now, now),
        )
    pg_storage._conn.commit()

    stats = pg_storage.get_stats()

    assert stats["top_blocked_domains"][0]["domain"] == "backoff.example"
    assert stats["top_blocked_domains"][0]["pending_count"] == 2
    assert stats["top_blocked_domains"][0]["blocked_counts"] == {
        "domain_next_request": 0,
        "host_backoff": 2,
        "retry_quarantine": 0,
    }
    assert stats["top_blocked_domains"][0]["dominant_reason"] == "host_backoff"
    assert stats["top_blocked_domains"][0]["wait_seconds"] == pytest.approx(45.0, abs=1e-3)
    assert stats["top_blocked_domains"][0]["consecutive_failures"] == 3


def test_get_stats_counts_blocked_queue_classes(pg_storage):
    now = time.time()

    with pg_storage._conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO frontier (
                url, domain, depth, priority, queue_class, discovery_kind, archetype,
                source_url, added_at, status, next_fetch_at
            )
            VALUES
                ('https://blocked.example/explore', 'blocked.example', 0, 1.0, 'exploration', 'seed', 'generic_page', NULL, %s, 'pending', %s),
                ('https://blocked.example/backlog', 'blocked.example', 3, 1.0, 'backlog', 'same_host', 'generic_page', NULL, %s, 'pending', %s)
            """,
            (now, now, now, now),
        )
        cur.execute(
            """
            INSERT INTO frontier_queue_blocked_domain_backoff (
                url, domain, queue_class, priority, next_fetch_at, added_at, branch_key
            )
            VALUES
                ('https://blocked.example/explore', 'blocked.example', 'exploration', 1.0, %s, %s, '/explore'),
                ('https://blocked.example/backlog', 'blocked.example', 'backlog', 1.0, %s, %s, '/backlog')
            """,
            (now, now, now, now),
        )
        cur.execute(
            """
            INSERT INTO domain_state (
                host_key,
                crawl_delay_seconds,
                next_request_at,
                backoff_until,
                consecutive_failures,
                robots_checked_at,
                updated_at
            )
            VALUES ('blocked.example', 1.0, %s, %s, 4, %s, %s)
            ON CONFLICT (host_key) DO UPDATE
            SET next_request_at = EXCLUDED.next_request_at,
                backoff_until = EXCLUDED.backoff_until,
                consecutive_failures = EXCLUDED.consecutive_failures,
                updated_at = EXCLUDED.updated_at
            """,
            (now, now + 40.0, now, now),
        )
    pg_storage._conn.commit()

    stats = pg_storage.get_stats()

    assert stats["pending_queue_classes"] == {}
    assert stats["blocked_queue_classes"] == {"backlog": 1, "exploration": 1}
    assert stats["frontier_status"]["pending"] == 2
    assert stats["readiness"]["pending"] == 2
    assert stats["readiness"]["ready"] == 0
    assert stats["readiness"]["state_counts"]["blocked_host_backoff"] == 0
    assert stats["readiness"]["state_counts"]["retry_quarantine"] == 2
    assert stats["top_blocked_domains"] == [
        {
            "domain": "blocked.example",
            "pending_count": 2,
            "blocked_counts": {
                "domain_next_request": 0,
                "host_backoff": 0,
                "retry_quarantine": 2,
            },
            "wait_seconds": pytest.approx(40.0, abs=1e-3),
            "dominant_reason": "retry_quarantine",
            "consecutive_failures": 4,
        }
    ]


def test_get_stats_includes_top_slow_domains(pg_storage):
    now = time.time()

    with pg_storage._conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO frontier (
                url, domain, depth, priority, queue_class, discovery_kind, archetype,
                source_url, added_at, status, next_fetch_at
            )
            VALUES
                ('https://slow.example/a', 'slow.example', 0, 1.0, 'exploration', 'seed', 'generic_page', NULL, %s, 'pending', %s),
                ('https://slow.example/b', 'slow.example', 0, 1.0, 'backlog', 'same_host', 'generic_page', NULL, %s, 'pending', %s),
                ('https://fast.example/', 'fast.example', 0, 1.0, 'exploration', 'seed', 'generic_page', NULL, %s, 'pending', %s)
            """,
            (now, now, now, now, now, now),
        )
        cur.execute(
            """
            INSERT INTO frontier_queue_exploration (url, domain, priority, next_fetch_at, added_at, branch_key)
            VALUES
                ('https://slow.example/a', 'slow.example', 1.0, %s, %s, '/a'),
                ('https://fast.example/', 'fast.example', 1.0, %s, %s, '/')
            """,
            (now, now, now, now),
        )
        cur.execute(
            """
            INSERT INTO frontier_queue_blocked_domain_backoff (
                url, domain, queue_class, priority, next_fetch_at, added_at, branch_key
            )
            VALUES ('https://slow.example/b', 'slow.example', 'backlog', 1.0, %s, %s, '/b')
            """,
            (now, now),
        )
        cur.execute(
            """
            INSERT INTO domain_state (
                host_key,
                crawl_delay_seconds,
                next_request_at,
                backoff_until,
                consecutive_failures,
                latency_ewma_ms,
                robots_checked_at,
                updated_at
            )
            VALUES
                ('slow.example', 1.0, %s, %s, 4, 900.0, %s, %s),
                ('fast.example', 1.0, %s, %s, 0, 80.0, %s, %s)
            ON CONFLICT (host_key) DO UPDATE
            SET next_request_at = EXCLUDED.next_request_at,
                backoff_until = EXCLUDED.backoff_until,
                consecutive_failures = EXCLUDED.consecutive_failures,
                latency_ewma_ms = EXCLUDED.latency_ewma_ms,
                updated_at = EXCLUDED.updated_at
            """,
            (now, now, now, now, now, now, now, now),
        )
    pg_storage._conn.commit()

    stats = pg_storage.get_stats()

    assert stats["top_slow_domains"] == [
        {
            "domain": "slow.example",
            "pending_count": 2,
            "latency_ewma_ms": 900.0,
            "consecutive_failures": 4,
            "queue_counts": {
                "exploration": 1,
                "backlog": 1,
                "recrawl": 0,
            },
        },
        {
            "domain": "fast.example",
            "pending_count": 1,
            "latency_ewma_ms": 80.0,
            "consecutive_failures": 0,
            "queue_counts": {
                "exploration": 1,
                "backlog": 0,
                "recrawl": 0,
            },
        },
    ]
    assert stats["top_budget_domains"] == [
        {
            "domain": "fast.example",
            "pending_count": 1,
            "latency_ewma_ms": 80.0,
            "consecutive_failures": 0,
            "queue_counts": {
                "exploration": 1,
                "backlog": 0,
                "recrawl": 0,
            },
            "host_budget": 2,
        }
    ]
    assert stats["operator_summary"]["adaptive_budget"] == {
        "observed_hosts": 2,
        "eligible_hosts": 1,
        "eligible_pending": 1,
        "ineligible_due_to_failures": 0,
        "ineligible_due_to_latency": 1,
        "max_budget": 2,
    }


def test_get_stats_includes_active_error_breakdown(pg_storage):
    with pg_storage._conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO frontier (
                url, domain, depth, priority, discovery_kind, archetype, source_url,
                added_at, status, next_fetch_at, fail_streak, last_error
            )
            VALUES
                ('https://example.com/404', 'example.com', 0, 1.0, 'seed', 'generic_page', NULL, 1710000000.0, 'pending', 1710000000.0, 1, 'http_404'),
                ('https://example.com/503', 'example.com', 0, 1.0, 'seed', 'generic_page', NULL, 1710000001.0, 'pending', 1710000001.0, 1, 'http_503'),
                ('https://other.com/timeout', 'other.com', 0, 1.0, 'external', 'generic_page', NULL, 1710000002.0, 'pending', 1710000002.0, 2, 'timeout'),
                ('https://other.com/disconnect', 'other.com', 0, 1.0, 'external', 'generic_page', NULL, 1710000003.0, 'failed', 1710000003.0, 3, 'Server disconnected without sending a response.'),
                ('https://third.com/connect', 'third.com', 0, 1.0, 'external', 'generic_page', NULL, 1710000004.0, 'pending', 1710000004.0, 1, 'connection_error'),
                ('https://third.com/other', 'third.com', 0, 1.0, 'external', 'generic_page', NULL, 1710000005.0, 'pending', 1710000005.0, 1, 'weird_error')
            """
        )
    pg_storage._conn.commit()

    stats = pg_storage.get_stats()

    assert stats["active_error_breakdown"] == {
        "http_4xx": 1,
        "http_5xx": 1,
        "timeout": 1,
        "connection_error": 2,
        "other": 1,
    }
    assert stats["top_error_domains"] == [
        {"domain": "example.com", "count": 2},
        {"domain": "other.com", "count": 2},
        {"domain": "third.com", "count": 2},
    ]


def test_get_stats_rejects_legacy_frontier_schema(pg_storage):
    result = {
        "url": "https://example.com/page1",
        "status": 200,
        "content_length": 100,
        "depth": 0,
        "timestamp": 1710000000.0,
        "content": "<html><title>Example</title></html>",
        "outlinks": [],
    }
    pg_storage.save(result)

    with pg_storage._conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS public.frontier")
    pg_storage._conn.commit()

    with pg_storage._conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE public.frontier (
                url TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                depth INTEGER NOT NULL,
                priority REAL NOT NULL DEFAULT 1.0,
                source_url TEXT,
                added_at DOUBLE PRECISION NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
            )
            """
        )
        cur.execute(
            """
            INSERT INTO frontier (url, domain, depth, priority, source_url, added_at, status)
            VALUES ('https://example.com/page2', 'example.com', 1, 1.0, 'https://example.com/page1', 1710000001.0, 'pending')
            """
        )
    pg_storage._conn.commit()

    with pytest.raises(RuntimeError, match="frontier schema is outdated"):
        pg_storage.get_stats()


def test_read_methods_leave_connection_idle(pg_storage):
    result = {
        "url": "https://example.com/page1",
        "status": 200,
        "content_length": 100,
        "depth": 0,
        "timestamp": 1710000000.0,
        "content": "<html><title>Example</title></html>",
        "outlinks": [],
    }
    pg_storage.save(result)

    listed = pg_storage.list_pages()
    assert listed
    assert pg_storage._conn.info.transaction_status == TRANSACTION_STATUS_IDLE

    page = pg_storage.get_page(listed[0]["url_hash"])
    assert page is not None
    assert pg_storage._conn.info.transaction_status == TRANSACTION_STATUS_IDLE

    pg_storage.get_stats()
    assert pg_storage._conn.info.transaction_status == TRANSACTION_STATUS_IDLE
