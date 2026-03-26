"""Tests for Postgres storage."""

import os

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
            cur.execute("DROP TABLE IF EXISTS public.frontier")
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
            INSERT INTO frontier (url, domain, depth, priority, discovery_kind, archetype, source_url, added_at, status, next_fetch_at)
            VALUES
                ('https://example.com/page1', 'example.com', 0, 2.0, 'seed', 'generic_page', NULL, 1710000000.0, 'done', 1710000000.0),
                ('https://example.com/page2', 'example.com', 1, 1.25, 'same_host', 'document_page', 'https://example.com/page1', 1710000002.0, 'pending', 1710000002.0),
                ('https://other.com/page1', 'other.com', 1, 0.8, 'external', 'redirect_hub', 'https://example.com/page1', 1710000003.0, 'pending', 1710000003.0)
            """
        )
    pg_storage._conn.commit()

    stats = pg_storage.get_stats()

    assert stats["total_pages"] == 2
    assert stats["domains"] == 2
    assert stats["frontier_status"] == {"done": 1, "pending": 2}
    assert stats["discovery_kinds"] == {"external": 1, "same_host": 1, "seed": 1}
    assert stats["archetypes"] == {"document_page": 1, "generic_page": 1, "redirect_hub": 1}
    assert stats["top_page_domains"][0] == {"domain": "example.com", "count": 1}
    assert stats["top_pending_domains"] == [
        {"domain": "example.com", "count": 1},
        {"domain": "other.com", "count": 1},
    ]
    assert stats["active_error_breakdown"] == {}
    assert stats["top_error_domains"] == []


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
