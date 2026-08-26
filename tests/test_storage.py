"""Tests for Postgres storage."""

import os
import pytest
import psycopg2
from psycopg2.extensions import TRANSACTION_STATUS_IDLE

from crawler.migrate import apply_migrations
from crawler.host_ledger import HOST_LEDGER_TABLE
from crawler.result import CrawlResult
from crawler.storage import _url_hash
from crawler.host_runnable_heads import (
    HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE,
    HOST_RUNNABLE_HEADS_TABLE,
)
from crawler.scheduler_leases import ACTIVE_LEASES_TABLE as LEASE_TABLE
from crawler.scheduler_membership import (
    PHYSICAL_QUEUE_TABLES,
    QUEUE_REFRESH,
    QUEUE_RUNNABLE,
    QUEUE_SCHEDULED,
)
from crawler.scheduler_quarantine import BLOCKED_HOST_BACKOFF_TABLE
from crawler.url_ledger_store import URL_LEDGER_TABLE

# Skip all tests if no Postgres available
pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_POSTGRES_DSN"),
    reason="TEST_POSTGRES_DSN not set",
)


class FakeContentStore:
    def __init__(self):
        self.bodies: dict[str, bytes] = {}
        self.content_types: dict[str, str] = {}

    def put(self, key: str, body: bytes, content_type: str) -> None:
        self.bodies[key] = body
        self.content_types[key] = content_type

    def get(self, key: str) -> bytes:
        return self.bodies[key]

    def delete(self, key: str) -> None:
        self.bodies.pop(key, None)
        self.content_types.pop(key, None)


def _reset_schema(dsn: str) -> None:
    runnable_table = PHYSICAL_QUEUE_TABLES[QUEUE_RUNNABLE]
    scheduled_table = PHYSICAL_QUEUE_TABLES[QUEUE_SCHEDULED]
    refresh_table = PHYSICAL_QUEUE_TABLES[QUEUE_REFRESH]
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS public.schema_migrations")
            cur.execute(f"DROP TABLE IF EXISTS public.{HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE}")
            cur.execute(f"DROP TABLE IF EXISTS public.{HOST_RUNNABLE_HEADS_TABLE}")
            cur.execute(f"DROP TABLE IF EXISTS public.{HOST_LEDGER_TABLE}")
            cur.execute("DROP TABLE IF EXISTS public.host_state")
            cur.execute(f"DROP TABLE IF EXISTS public.{runnable_table}")
            cur.execute(f"DROP TABLE IF EXISTS public.{scheduled_table}")
            cur.execute(f"DROP TABLE IF EXISTS public.{refresh_table}")
            cur.execute(f"DROP TABLE IF EXISTS public.{BLOCKED_HOST_BACKOFF_TABLE}")
            cur.execute(f"DROP TABLE IF EXISTS public.{LEASE_TABLE}")
            cur.execute(f"DROP TABLE IF EXISTS public.{URL_LEDGER_TABLE} CASCADE")
            cur.execute("DROP TABLE IF EXISTS public.crawler_runtime_stats")
            cur.execute("DROP TABLE IF EXISTS public.page_content")
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
    storage = PgStorage(dsn, content_store=FakeContentStore())
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
        "source_url": None,
        "timestamp": 1710000000.0,
        "content": "<html><title>Test Page</title><body>Hello</body></html>",
        "outlinks": ["https://example.com/page2"],
    }
    save_result = pg_storage.save(result)
    assert save_result.saved is True
    assert save_result.telemetry is not None
    assert save_result.telemetry.prepare_ms >= 0
    assert save_result.telemetry.pages_upsert_ms >= 0
    assert save_result.telemetry.content_store_ms >= 0
    assert save_result.telemetry.commit_ms >= 0
    assert save_result.telemetry.total_ms >= 0
    assert save_result.telemetry.storage_tier == "body"
    assert save_result.telemetry.stored_content_bytes > 0
    assert pg_storage.count == 1


def test_skip_error_result(pg_storage):
    result = {"url": "https://example.com/fail", "error": "timeout"}
    save_result = pg_storage.save(result)
    assert save_result.saved is False
    assert save_result.telemetry is None
    assert pg_storage.count == 0


def test_upsert_on_conflict(pg_storage):
    result = {
        "url": "https://example.com/page1",
        "status": 200,
        "content_length": 1000,
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


def test_save_many_persists_multiple_pages(pg_storage):
    results = [
        {
            "url": "https://example.com/page1",
            "status": 200,
            "content_length": 1000,
            "timestamp": 1710000000.0,
            "content": "<html><title>Page 1</title><body>One</body></html>",
            "outlinks": [],
        },
        {
            "url": "https://example.com/page2",
            "status": 200,
            "content_length": 1000,
            "timestamp": 1710000001.0,
            "content": "<html><title>Page 2</title><body>Two</body></html>",
            "outlinks": [],
        },
    ]

    save_results = pg_storage.save_many(results)

    assert [save_result.saved for save_result in save_results] == [True, True]
    assert all(save_result.telemetry is not None for save_result in save_results)
    assert all(save_result.telemetry.total_ms >= 0 for save_result in save_results)
    assert pg_storage.count == 2

    with pg_storage._conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pages")
        assert cur.fetchone()[0] == 2
    assert len(pg_storage._content_store.bodies) == 2


def test_save_many_deletes_metadata_only_content_rows(pg_storage):
    original = {
        "url": "https://example.com/page1",
        "status": 200,
        "content_length": 1000,
        "timestamp": 1710000000.0,
        "content": "<html><title>Page 1</title><body>One</body></html>",
        "outlinks": [],
    }
    pg_storage.save(original)

    updated = {
        "url": "https://example.com/page1",
        "status": 200,
        "content_length": 1000,
        "timestamp": 1710000001.0,
        "content": "prefix\x00suffix",
        "outlinks": [],
    }

    save_results = pg_storage.save_many([updated])

    assert save_results[0].saved is True
    assert save_results[0].telemetry is not None
    assert save_results[0].telemetry.storage_tier == "metadata_only"

    assert _url_hash("https://example.com/page1") not in pg_storage._content_store.bodies


def test_save_many_deduplicates_same_url_with_last_write_winning(pg_storage):
    first = {
        "url": "https://example.com/dup",
        "status": 200,
        "content_length": 1000,
        "timestamp": 1710000000.0,
        "content": "<html><title>First</title><body>One</body></html>",
        "outlinks": [],
    }
    second = {
        "url": "https://example.com/dup",
        "status": 200,
        "content_length": 1000,
        "timestamp": 1710000001.0,
        "content": "<html><title>Second</title><body>Two</body></html>",
        "outlinks": [],
    }

    save_results = pg_storage.save_many([first, second])

    assert [save_result.saved for save_result in save_results] == [True, True]

    with pg_storage._conn.cursor() as cur:
        cur.execute("SELECT title FROM pages WHERE url = %s", ("https://example.com/dup",))
        assert cur.fetchone() == ("Second",)
    assert pg_storage._content_store.bodies[_url_hash("https://example.com/dup")] == (
        b"<html><title>Second</title><body>Two</body></html>"
    )


def test_save_preserves_response_bytes(pg_storage):
    result = CrawlResult(
        url="https://example.com/raw",
        status=200,
        content_length=4,
        source_url=None,
        timestamp=1710000000.0,
        content="\ufffdPNG",
        content_bytes=b"\x89PNG",
        content_type="text/plain",
        outlinks=[],
    )

    pg_storage.save(result)

    assert pg_storage._content_store.bodies[_url_hash(result.url)] == b"\x89PNG"


def test_url_hash_is_sha256_of_normalized_url():
    assert _url_hash("HTTPS://Example.COM/path/?b=2&a=1#fragment") == _url_hash(
        "https://example.com/path?a=1&b=2"
    )
    assert len(_url_hash("https://example.com/")) == 64


def test_save_drops_nul_content_to_metadata_only(pg_storage):
    result = {
        "url": "https://example.com/file.pdf",
        "status": 200,
        "content_length": 1000,
        "timestamp": 1710000000.0,
        "content": "prefix\x00suffix",
        "outlinks": [],
    }

    save_result = pg_storage.save(result)
    assert save_result.saved is True
    assert save_result.telemetry is not None
    assert save_result.telemetry.storage_tier == "metadata_only"
    assert save_result.telemetry.stored_content_bytes == 0
    assert save_result.telemetry.content_truncated is False

    listed = pg_storage.list_pages()
    page = pg_storage.get_page(listed[0]["url_hash"])

    assert page["title"] is None
    assert page["content"] == ""
    assert page["storage_tier"] == "metadata_only"
    assert page["stored_content_bytes"] == 0


def test_get_runtime_stats_summary_uses_persisted_snapshot(pg_storage):
    pg_storage.save(
        {
            "url": "https://example.com/page1",
            "status": 200,
            "content_length": 100,
            "timestamp": 1710000000.0,
            "content": "<html><title>Example</title></html>",
            "outlinks": [],
        }
    )
    pg_storage.upsert_runtime_stats(
        "crawler",
        {
            "running": True,
            "pending": 20,
            "runnable": 12,
            "active_hosts": 4,
            "pages_per_second": 3.5,
            "errors": {"timeout": 2},
            "scheduler_state_snapshot": {
                "durable_state_counts": {
                    "discovered": 1,
                    "scheduled": 20,
                    "leased": 0,
                    "blocked": 0,
                    "terminal": 10,
                },
                "readiness_state_counts": {
                    "runnable": 12,
                    "scheduled": 8,
                    "blocked_host_next_request": 0,
                    "blocked_host_backoff": 0,
                    "retry_quarantine": 0,
                },
                "effective_state_counts": {
                    "discovered": 1,
                    "scheduled": 8,
                    "runnable": 12,
                    "blocked": 0,
                    "leased": 0,
                    "terminal": 10,
                },
                "blocked_reason_counts": {
                    "next_fetch_at": 8,
                    "host_next_request": 0,
                    "host_backoff": 0,
                    "retry_quarantine": 0,
                },
            },
            "readiness_state_counts": {
                "runnable": 12,
                "scheduled": 8,
                "blocked_host_next_request": 0,
                "blocked_host_backoff": 0,
                "retry_quarantine": 0,
            },
            "blocked_reason_counts": {
                "next_fetch_at": 8,
                "host_next_request": 0,
                "host_backoff": 0,
                "retry_quarantine": 0,
            },
            "timing_summary": {
                "counts": {
                    "discovery_admission": {
                        "extracted": 20,
                        "admitted": 8,
                        "score_below_threshold": 7,
                        "per_page_cap": 5,
                    }
                }
            },
        },
    )

    stats = pg_storage.get_runtime_stats_summary()

    assert stats["stats_source"] == "runtime_snapshot"
    assert stats["total_pages"] == 1
    assert stats["readiness"]["pending"] == 20
    assert stats["readiness"]["runnable"] == 12
    assert stats["readiness_state_counts"]["scheduled"] == 8
    assert stats["durable_state_counts"]["scheduled"] == 20
    assert stats["active_error_breakdown"] == {"timeout": 2}
    assert stats["top_pending_hosts"] == []
    assert stats["operator_summary"]["throughput"]["pages_per_second"] == 3.5
    assert stats["operator_summary"]["discovery_admission"] == {
        "extracted": 20,
        "admitted": 8,
        "rejected": 12,
        "admit_ratio": 0.4,
        "rejection_reasons": {
            "per_page_cap": 5,
            "score_below_threshold": 7,
        },
        "counts": {
            "extracted": 20,
            "admitted": 8,
            "score_below_threshold": 7,
            "per_page_cap": 5,
        },
    }


def test_get_runtime_stats_summary_handles_missing_runtime_snapshot(pg_storage):
    stats = pg_storage.get_runtime_stats_summary()

    assert stats["stats_source"] == "runtime_snapshot"
    assert stats["runtime"] == {}
    assert stats["readiness"] == {
        "pending": 0,
        "runnable": 0,
        "runnable_hosts": 0,
        "next_runnable_delay": None,
        "blocked": {},
        "state_counts": {},
    }
    assert stats["operator_summary"]["scheduler_readiness_states"]["pending"] == 0


def test_read_methods_leave_connection_idle(pg_storage):
    result = {
        "url": "https://example.com/page1",
        "status": 200,
        "content_length": 100,
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

    pg_storage.get_runtime_stats_summary()
    assert pg_storage._conn.info.transaction_status == TRANSACTION_STATUS_IDLE
