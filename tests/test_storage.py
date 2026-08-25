"""Tests for Postgres storage."""

import os
import time

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


def test_get_stats_includes_scheduler_breakdown(pg_storage):
    page_results = [
        {
            "url": "https://example.com/page1",
            "status": 200,
            "content_length": 100,
            "timestamp": 1710000000.0,
            "content": "<html><title>Example</title></html>",
            "outlinks": [],
        },
        {
            "url": "https://other.com/page1",
            "status": 200,
            "content_length": 50,
            "timestamp": 1710000001.0,
            "content": "<html><title>Other</title></html>",
            "outlinks": [],
        },
    ]
    for result in page_results:
        pg_storage.save(result)

    with pg_storage._conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {URL_LEDGER_TABLE} (url, host, discovery_value, source_url, added_at, next_fetch_at, current_intent)
            VALUES
                ('https://example.com/page1', 'example.com', 2.0, NULL, 1710000000.0, 1710000000.0, 'explore'),
                ('https://example.com/page2', 'example.com', 1.25, 'https://example.com/page1', 1710000002.0, 1710000002.0, 'explore'),
                ('https://other.com/page1', 'other.com', 0.8, 'https://example.com/page1', 1710000003.0, 1710000003.0, 'explore')
            """
        )
        cur.execute(
            f"""
            INSERT INTO {PHYSICAL_QUEUE_TABLES[QUEUE_RUNNABLE]} (url, host, discovery_value, next_fetch_at, added_at, branch_key)
            VALUES
                ('https://example.com/page2', 'example.com', 1.25, 1710000002.0, 1710000002.0, '/page2'),
                ('https://other.com/page1', 'other.com', 0.8, 1710000003.0, 1710000003.0, '/page1')
            """
        )
    pg_storage._conn.commit()

    stats = pg_storage.get_stats()

    assert stats["total_pages"] == 2
    assert stats["hosts"] == 2
    assert stats["scheduler_status"] == {
        "leased": 0,
        "done": 0,
        "failed": 0,
        "intent_counts": {"explore": 2, "refresh": 0, "retry": 0},
        "durable_state_counts": {
            "discovered": 1,
            "scheduled": 2,
            "leased": 0,
            "blocked": 0,
            "terminal": 0,
        },
        "readiness_state_counts": {
            "runnable": 2,
            "scheduled": 0,
            "blocked_host_next_request": 0,
            "blocked_host_backoff": 0,
            "retry_quarantine": 0,
        },
        "effective_state_counts": {
            "discovered": 1,
            "scheduled": 0,
            "runnable": 2,
            "blocked": 0,
            "leased": 0,
            "terminal": 0,
        },
        "blocked_reason_counts": {
            "next_fetch_at": 0,
            "host_next_request": 0,
            "host_backoff": 0,
            "retry_quarantine": 0,
        },
        "scheduler_state_snapshot": {
            "durable_state_counts": {
                "discovered": 1,
                "scheduled": 2,
                "leased": 0,
                "blocked": 0,
                "terminal": 0,
            },
            "readiness_state_counts": {
                "runnable": 2,
                "scheduled": 0,
                "blocked_host_next_request": 0,
                "blocked_host_backoff": 0,
                "retry_quarantine": 0,
            },
            "effective_state_counts": {
                "discovered": 1,
                "scheduled": 0,
                "runnable": 2,
                "blocked": 0,
                "leased": 0,
                "terminal": 0,
            },
            "blocked_reason_counts": {
                "next_fetch_at": 0,
                "host_next_request": 0,
                "host_backoff": 0,
                "retry_quarantine": 0,
            },
        },
        "pending_surfaces": {"runnable": 2},
        "blocked_surfaces": {},
        "pending": 2,
        "total": 2,
    }
    assert stats["intent_counts"] == {"explore": 2, "refresh": 0, "retry": 0}
    assert stats["durable_state_counts"] == {
        "discovered": 1,
        "scheduled": 2,
        "leased": 0,
        "blocked": 0,
        "terminal": 0,
    }
    assert stats["readiness_state_counts"] == {
        "runnable": 2,
        "scheduled": 0,
        "blocked_host_next_request": 0,
        "blocked_host_backoff": 0,
        "retry_quarantine": 0,
    }
    assert stats["effective_state_counts"] == {
        "discovered": 1,
        "scheduled": 0,
        "runnable": 2,
        "blocked": 0,
        "leased": 0,
        "terminal": 0,
    }
    assert stats["blocked_reason_counts"] == {
        "next_fetch_at": 0,
        "host_next_request": 0,
        "host_backoff": 0,
        "retry_quarantine": 0,
    }
    assert stats["scheduler_state_snapshot"] == {
        "durable_state_counts": {
            "discovered": 1,
            "scheduled": 2,
            "leased": 0,
            "blocked": 0,
            "terminal": 0,
        },
        "readiness_state_counts": {
            "runnable": 2,
            "scheduled": 0,
            "blocked_host_next_request": 0,
            "blocked_host_backoff": 0,
            "retry_quarantine": 0,
        },
        "effective_state_counts": {
            "discovered": 1,
            "scheduled": 0,
            "runnable": 2,
            "blocked": 0,
            "leased": 0,
            "terminal": 0,
        },
        "blocked_reason_counts": {
            "next_fetch_at": 0,
            "host_next_request": 0,
            "host_backoff": 0,
            "retry_quarantine": 0,
        },
    }
    assert stats["pending_surfaces"] == {"runnable": 2}
    assert stats["blocked_surfaces"] == {}
    assert stats["readiness"] == {
        "pending": 2,
        "runnable": 2,
        "runnable_hosts": 2,
        "next_runnable_delay": 0.0,
        "blocked": {
            "next_fetch_at": 0,
            "host_next_request": 0,
            "host_backoff": 0,
            "retry_quarantine": 0,
        },
        "state_counts": {
            "runnable": 2,
            "scheduled": 0,
            "blocked_host_next_request": 0,
            "blocked_host_backoff": 0,
            "retry_quarantine": 0,
        },
    }
    assert stats["top_page_hosts"][0] == {"host": "example.com", "count": 1}
    assert stats["top_pending_hosts"] == [
        {"host": "example.com", "count": 1},
        {"host": "other.com", "count": 1},
    ]
    assert stats["top_slow_hosts"] == []
    assert stats["top_budget_hosts"] == []
    assert stats["active_error_breakdown"] == {}
    assert stats["top_error_hosts"] == []


def test_get_stats_degrades_when_scheduler_diagnostics_time_out(pg_storage, monkeypatch):
    result = {
        "url": "https://example.com/page1",
        "status": 200,
        "content_length": 100,
        "timestamp": 1710000000.0,
        "content": "<html><title>Example</title></html>",
        "outlinks": [],
    }
    pg_storage.save(result)

    def raise_timeout(self):
        raise psycopg2.errors.QueryCanceled("statement timeout")

    monkeypatch.setattr("crawler.storage.SchedulerObservability.status_counts", raise_timeout)

    stats = pg_storage.get_stats()

    assert stats["total_pages"] == 1
    assert stats["scheduler_status"]["diagnostics_unavailable"] is True
    assert stats["scheduler_status"]["diagnostics_error"] == "QueryCanceled"
    assert pg_storage._conn.info.transaction_status == TRANSACTION_STATUS_IDLE


def test_get_stats_includes_runtime_snapshot(pg_storage):
    pg_storage.upsert_runtime_stats(
        "crawler",
        {
            "running": True,
            "pages_per_second": 2.5,
            "pages": 15,
            "active_hosts": 3,
            "parse_queue_size": 2,
            "finalize_queue_size": 1,
            "parse_queue_wait_max_ms": 12.5,
            "finalize_queue_wait_max_ms": 8.0,
            "errors": {"timeout": 1},
        },
    )

    stats = pg_storage.get_stats()

    assert stats["runtime"]["payload"] == {
        "running": True,
        "pages_per_second": 2.5,
        "pages": 15,
        "active_hosts": 3,
        "parse_queue_size": 2,
        "finalize_queue_size": 1,
        "parse_queue_wait_max_ms": 12.5,
        "finalize_queue_wait_max_ms": 8.0,
        "errors": {"timeout": 1},
    }
    assert stats["runtime"]["updated_at"] > 0
    assert stats["operator_summary"] == {
        "scheduler_state": {
            "pending": 0,
            "runnable": 0,
            "scheduled": 0,
            "blocked_host_next_request": 0,
            "blocked_host_backoff": 0,
            "retry_quarantine": 0,
            "leased": 0,
        },
        "scheduler_readiness_states": {
            "pending": 0,
            "runnable": 0,
            "scheduled": 0,
            "blocked_host_next_request": 0,
            "blocked_host_backoff": 0,
            "retry_quarantine": 0,
            "leased": 0,
        },
        "scheduler_state_snapshot": {
            "durable_state_counts": {
                "discovered": 0,
                "scheduled": 0,
                "leased": 0,
                "blocked": 0,
                "terminal": 0,
            },
            "readiness_state_counts": {
                "runnable": 0,
                "scheduled": 0,
                "blocked_host_next_request": 0,
                "blocked_host_backoff": 0,
                "retry_quarantine": 0,
            },
            "effective_state_counts": {
                "discovered": 0,
                "scheduled": 0,
                "runnable": 0,
                "blocked": 0,
                "leased": 0,
                "terminal": 0,
            },
            "blocked_reason_counts": {
                "next_fetch_at": 0,
                "host_next_request": 0,
                "host_backoff": 0,
                "retry_quarantine": 0,
            },
        },
        "scheduler_durable_states": {
            "discovered": 0,
            "scheduled": 0,
            "leased": 0,
            "blocked": 0,
            "terminal": 0,
        },
        "scheduler_effective_states": {
            "discovered": 0,
            "scheduled": 0,
            "runnable": 0,
            "blocked": 0,
            "leased": 0,
            "terminal": 0,
        },
        "scheduler_intents": {
            "explore": 0,
            "refresh": 0,
            "retry": 0,
        },
        "scheduler_blocked_reasons": {
            "next_fetch_at": 0,
            "host_next_request": 0,
            "host_backoff": 0,
            "retry_quarantine": 0,
        },
        "throughput": {
            "pages_per_second": 2.5,
            "cycle_pages": 15,
            "active_hosts": 3,
            "errors": {"timeout": 1},
        },
        "backpressure": {
            "parse_queue_size": 2,
            "finalize_queue_size": 1,
            "parse_queue_wait_max_ms": 12.5,
            "finalize_queue_wait_max_ms": 8.0,
        },
        "admission_control": {},
        "discovery_admission": {
            "extracted": 0,
            "admitted": 0,
            "rejected": 0,
            "admit_ratio": None,
            "rejection_reasons": {},
            "counts": {},
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
    assert stats["diagnostics_endpoint"] == "/stats/diagnostics"
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


def test_get_stats_includes_readiness_breakdown(pg_storage):
    now = time.time()

    with pg_storage._conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO url_ledger (
                url, host, discovery_value,
                source_url, added_at, next_fetch_at
            )
            VALUES
                ('https://ready.example/', 'ready.example', 1.0, NULL, %s, %s),
                ('https://future.example/', 'future.example', 1.0, NULL, %s, %s),
                ('https://backoff.example/', 'backoff.example', 1.0, NULL, %s, %s)
            """,
            (now, now, now, now + 30.0, now, now),
        )
        cur.execute(
            f"""
            INSERT INTO {PHYSICAL_QUEUE_TABLES[QUEUE_RUNNABLE]} (url, host, discovery_value, next_fetch_at, added_at, branch_key)
            VALUES
                ('https://ready.example/', 'ready.example', 1.0, %s, %s, '/'),
                ('https://future.example/', 'future.example', 1.0, %s, %s, '/'),
                ('https://backoff.example/', 'backoff.example', 1.0, %s, %s, '/')
            """,
            (now, now, now + 30.0, now, now, now),
        )
        cur.execute(
            """
            INSERT INTO host_state (
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
    assert stats["readiness"]["runnable"] == 1
    assert stats["readiness"]["runnable_hosts"] == 1
    assert stats["readiness"]["next_runnable_delay"] == 0.0
    assert stats["readiness"]["blocked"] == {
        "next_fetch_at": 1,
        "host_next_request": 0,
        "host_backoff": 1,
        "retry_quarantine": 0,
    }
    assert stats["readiness"]["state_counts"] == {
        "runnable": 1,
        "scheduled": 1,
        "blocked_host_next_request": 0,
        "blocked_host_backoff": 1,
        "retry_quarantine": 0,
    }
    assert stats["top_blocked_hosts"] == [
        {
            "host": "backoff.example",
            "pending_count": 1,
            "blocked_counts": {
                "host_next_request": 0,
                "host_backoff": 1,
                "retry_quarantine": 0,
            },
            "wait_seconds": pytest.approx(20.0, abs=3.0),
            "dominant_reason": "host_backoff",
            "consecutive_failures": 0,
        }
    ]
    assert stats["operator_summary"]["scheduler_state"] == {
        "pending": 3,
        "runnable": 1,
        "scheduled": 1,
        "blocked_host_next_request": 0,
        "blocked_host_backoff": 1,
        "retry_quarantine": 0,
        "leased": 0,
    }
    assert stats["operator_summary"]["scheduler_readiness_states"] == {
        "pending": 3,
        "runnable": 1,
        "scheduled": 1,
        "blocked_host_next_request": 0,
        "blocked_host_backoff": 1,
        "retry_quarantine": 0,
        "leased": 0,
    }
    assert stats["operator_summary"]["scheduler_intents"] == {
        "explore": 0,
        "refresh": 0,
        "retry": 0,
    }
    assert stats["operator_summary"]["scheduler_blocked_reasons"] == {
        "next_fetch_at": 1,
        "host_next_request": 0,
        "host_backoff": 1,
        "retry_quarantine": 0,
    }


def test_get_stats_prioritizes_hosts_blocked_by_backoff(pg_storage):
    now = time.time()

    with pg_storage._conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO url_ledger (
                url, host, discovery_value,
                source_url, added_at, next_fetch_at
            )
            VALUES
                ('https://backoff.example/a', 'backoff.example', 1.0, NULL, %s, %s),
                ('https://backoff.example/b', 'backoff.example', 1.0, NULL, %s, %s),
                ('https://slot.example/', 'slot.example', 1.0, NULL, %s, %s)
            """,
            (now, now, now, now, now, now),
        )
        cur.execute(
            f"""
            INSERT INTO {PHYSICAL_QUEUE_TABLES[QUEUE_RUNNABLE]} (url, host, discovery_value, next_fetch_at, added_at, branch_key)
            VALUES
                ('https://backoff.example/a', 'backoff.example', 1.0, %s, %s, '/a'),
                ('https://backoff.example/b', 'backoff.example', 1.0, %s, %s, '/b'),
                ('https://slot.example/', 'slot.example', 1.0, %s, %s, '/')
            """,
            (now, now, now, now, now, now),
        )
        cur.execute(
            """
            INSERT INTO host_state (
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
            (now, now + 45.0, now, now, now + 15.0, now + 15.0, now, now),
        )
    pg_storage._conn.commit()

    stats = pg_storage.get_stats()

    assert stats["top_blocked_hosts"][0]["host"] == "backoff.example"
    assert stats["top_blocked_hosts"][0]["pending_count"] == 2
    assert stats["top_blocked_hosts"][0]["blocked_counts"] == {
        "host_next_request": 0,
        "host_backoff": 2,
        "retry_quarantine": 0,
    }
    assert stats["top_blocked_hosts"][0]["dominant_reason"] == "host_backoff"
    assert stats["top_blocked_hosts"][0]["wait_seconds"] == pytest.approx(45.0, abs=3.0)
    assert stats["top_blocked_hosts"][0]["consecutive_failures"] == 3


def test_get_stats_counts_blocked_surfaces(pg_storage):
    now = time.time()

    with pg_storage._conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO url_ledger (
                url, host, discovery_value,
                source_url, added_at, next_fetch_at, current_intent
            )
            VALUES
                ('https://blocked.example/explore', 'blocked.example', 1.0, NULL, %s, %s, 'retry'),
                ('https://blocked.example/scheduled', 'blocked.example', 1.0, NULL, %s, %s, 'retry')
            """,
            (now, now, now, now),
        )
        cur.execute(
            f"""
            INSERT INTO {BLOCKED_HOST_BACKOFF_TABLE} (
                url, host, physical_queue, scheduler_score, next_fetch_at, added_at, branch_key
            )
            VALUES
                ('https://blocked.example/explore', 'blocked.example', 'runnable', 1.0, %s, %s, '/explore'),
                ('https://blocked.example/scheduled', 'blocked.example', 'scheduled', 1.0, %s, %s, '/scheduled')
            """,
            (now, now, now, now),
        )
        cur.execute(
            """
            INSERT INTO host_state (
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

    assert stats["pending_surfaces"] == {}
    assert stats["blocked_surfaces"] == {"scheduled": 1, "runnable": 1}
    assert stats["intent_counts"] == {"explore": 0, "refresh": 0, "retry": 2}
    assert stats["durable_state_counts"] == {
        "discovered": 0,
        "scheduled": 0,
        "leased": 0,
        "blocked": 2,
        "terminal": 0,
    }
    assert stats["effective_state_counts"] == {
        "discovered": 0,
        "scheduled": 0,
        "runnable": 0,
        "blocked": 2,
        "leased": 0,
        "terminal": 0,
    }
    assert stats["blocked_reason_counts"] == {
        "next_fetch_at": 0,
        "host_next_request": 0,
        "host_backoff": 0,
        "retry_quarantine": 2,
    }
    assert stats["scheduler_status"]["pending"] == 2
    assert stats["readiness"]["pending"] == 2
    assert stats["readiness"]["runnable"] == 0
    assert stats["readiness"]["state_counts"]["blocked_host_backoff"] == 0
    assert stats["readiness"]["state_counts"]["retry_quarantine"] == 2
    assert stats["top_blocked_hosts"] == [
        {
            "host": "blocked.example",
            "pending_count": 2,
            "blocked_counts": {
                "host_next_request": 0,
                "host_backoff": 0,
                "retry_quarantine": 2,
            },
            "wait_seconds": pytest.approx(40.0, abs=3.0),
            "dominant_reason": "retry_quarantine",
            "consecutive_failures": 4,
        }
    ]


def test_get_stats_includes_top_slow_hosts(pg_storage):
    now = time.time()

    with pg_storage._conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO url_ledger (
                url, host, discovery_value,
                source_url, added_at, next_fetch_at
            )
            VALUES
                ('https://slow.example/a', 'slow.example', 1.0, NULL, %s, %s),
                ('https://slow.example/b', 'slow.example', 1.0, NULL, %s, %s),
                ('https://fast.example/', 'fast.example', 1.0, NULL, %s, %s)
            """,
            (now, now, now, now, now, now),
        )
        cur.execute(
            f"""
            INSERT INTO {PHYSICAL_QUEUE_TABLES[QUEUE_RUNNABLE]} (url, host, discovery_value, next_fetch_at, added_at, branch_key)
            VALUES
                ('https://slow.example/a', 'slow.example', 1.0, %s, %s, '/a'),
                ('https://fast.example/', 'fast.example', 1.0, %s, %s, '/')
            """,
            (now, now, now, now),
        )
        cur.execute(
            f"""
            INSERT INTO {BLOCKED_HOST_BACKOFF_TABLE} (
                url, host, physical_queue, scheduler_score, next_fetch_at, added_at, branch_key
            )
            VALUES ('https://slow.example/b', 'slow.example', 'scheduled', 1.0, %s, %s, '/b')
            """,
            (now, now),
        )
        cur.execute(
            """
            INSERT INTO host_state (
                host_key,
                crawl_delay_seconds,
                next_request_at,
                backoff_until,
                consecutive_failures,
                latency_ewma_ms,
                latency_last_ms,
                latency_observed_at,
                latency_sample_count,
                robots_checked_at,
                updated_at
            )
            VALUES
                ('slow.example', 1.0, %s, %s, 4, 900.0, 1200.0, %s, 5, %s, %s),
                ('fast.example', 1.0, %s, %s, 0, 80.0, 40.0, %s, 2, %s, %s)
            ON CONFLICT (host_key) DO UPDATE
            SET next_request_at = EXCLUDED.next_request_at,
                backoff_until = EXCLUDED.backoff_until,
                consecutive_failures = EXCLUDED.consecutive_failures,
                latency_ewma_ms = EXCLUDED.latency_ewma_ms,
                latency_last_ms = EXCLUDED.latency_last_ms,
                latency_observed_at = EXCLUDED.latency_observed_at,
                latency_sample_count = EXCLUDED.latency_sample_count,
                updated_at = EXCLUDED.updated_at
            """,
            (now, now, now, now, now, now, now, now, now, now),
        )
    pg_storage._conn.commit()

    stats = pg_storage.get_stats()

    assert stats["total_pages"] == 0
    assert stats["hosts"] == 0
    assert stats["top_slow_hosts"] == [
        {
            "host": "slow.example",
            "pending_count": 2,
            "latency_ewma_ms": 900.0,
            "latency_last_ms": 1200.0,
            "latency_observed_at": now,
            "latency_sample_count": 5,
            "consecutive_failures": 4,
            "surface_counts": {
                "runnable": 1,
                "scheduled": 1,
                "refresh": 0,
            },
        },
        {
            "host": "fast.example",
            "pending_count": 1,
            "latency_ewma_ms": 80.0,
            "latency_last_ms": 40.0,
            "latency_observed_at": now,
            "latency_sample_count": 2,
            "consecutive_failures": 0,
            "surface_counts": {
                "runnable": 1,
                "scheduled": 0,
                "refresh": 0,
            },
        },
    ]
    assert stats["top_budget_hosts"] == [
        {
            "host": "fast.example",
            "pending_count": 1,
            "latency_ewma_ms": 80.0,
            "latency_last_ms": 40.0,
            "latency_observed_at": now,
            "latency_sample_count": 2,
            "consecutive_failures": 0,
            "surface_counts": {
                "runnable": 1,
                "scheduled": 0,
                "refresh": 0,
            },
            "host_budget": 2,
        }
    ]
    assert stats["operator_summary"]["adaptive_budget"] == {
        "observed_hosts": 2,
        "eligible_hosts": 1,
        "eligible_pending": 1,
        "ineligible_due_to_failures": 1,
        "ineligible_due_to_latency": 0,
        "max_budget": 2,
    }


def test_get_stats_includes_active_error_breakdown(pg_storage):
    with pg_storage._conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO url_ledger (
                url, host, discovery_value, source_url,
                added_at, next_fetch_at, fail_streak, last_error, terminal_reason
            )
            VALUES
                ('https://example.com/404', 'example.com', 1.0, NULL, 1710000000.0, 1710000000.0, 1, 'http_404', 'http_404'),
                ('https://example.com/503', 'example.com', 1.0, NULL, 1710000001.0, 1710000001.0, 1, 'http_503', 'http_503'),
                ('https://other.com/timeout', 'other.com', 1.0, NULL, 1710000002.0, 1710000002.0, 2, 'timeout', 'timeout'),
                ('https://other.com/disconnect', 'other.com', 1.0, NULL, 1710000003.0, 1710000003.0, 3, 'Server disconnected without sending a response.', 'Server disconnected without sending a response.'),
                ('https://third.com/connect', 'third.com', 1.0, NULL, 1710000004.0, 1710000004.0, 1, 'connection_error', 'connection_error'),
                ('https://third.com/other', 'third.com', 1.0, NULL, 1710000005.0, 1710000005.0, 1, 'weird_error', 'weird_error')
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
    assert stats["top_error_hosts"] == [
        {"host": "example.com", "count": 2},
        {"host": "other.com", "count": 2},
        {"host": "third.com", "count": 2},
    ]


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

    pg_storage.get_stats()
    assert pg_storage._conn.info.transaction_status == TRANSACTION_STATUS_IDLE

    pg_storage.get_runtime_stats_summary()
    assert pg_storage._conn.info.transaction_status == TRANSACTION_STATUS_IDLE
