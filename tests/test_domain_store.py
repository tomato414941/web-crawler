"""Tests for persistent domain state storage."""

import os
import time

import psycopg2
import pytest

from crawler.domain_store import DomainStore
from crawler.migrate import apply_migrations

PG_DSN = os.environ.get("TEST_POSTGRES_DSN", "postgresql://crawler:crawler@localhost/crawldb_test")


def _pg_available():
    try:
        conn = psycopg2.connect(PG_DSN)
        conn.close()
        return True
    except Exception:
        return False


requires_pg = pytest.mark.skipif(not _pg_available(), reason="Postgres not available")


@requires_pg
class TestDomainStore:
    @pytest.fixture(autouse=True)
    def store(self):
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS schema_migrations")
            cur.execute("DROP TABLE IF EXISTS domain_state")
            cur.execute("DROP TABLE IF EXISTS frontier")
            cur.execute("DROP TABLE IF EXISTS pages")
        conn.commit()
        conn.close()
        apply_migrations(PG_DSN)
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        store = DomainStore(conn, default_delay=1.5)
        yield store
        conn.close()

    def test_get_or_create_returns_defaults(self, store):
        state = store.get_or_create("example.com")
        assert state.host_key == "example.com"
        assert state.crawl_delay_seconds == 1.5
        assert state.next_request_at == 0.0
        assert state.backoff_until == 0.0
        assert state.consecutive_failures == 0

    def test_update_robots_persists_delay(self, store):
        checked_at = time.time()
        state = store.update_robots(
            "example.com",
            crawl_delay_seconds=2.5,
            checked_at=checked_at,
        )
        assert state.crawl_delay_seconds == 2.5
        assert state.robots_checked_at == checked_at

    def test_reserve_request_slot_advances_next_request_at(self, store):
        wait_seconds, first = store.reserve_request_slot(
            "example.com",
            crawl_delay_seconds=0.2,
            now=100.0,
        )
        assert wait_seconds == 0.0
        assert first.next_request_at == 100.2

        wait_seconds, second = store.reserve_request_slot(
            "example.com",
            crawl_delay_seconds=0.2,
            now=100.05,
        )
        assert wait_seconds == pytest.approx(0.15, rel=0.0, abs=1e-6)
        assert second.next_request_at == pytest.approx(100.4, rel=0.0, abs=1e-6)

    def test_record_failure_increments_streak_and_backoff(self, store):
        state = store.record_failure("example.com", backoff_seconds=30.0, now=100.0)
        assert state.consecutive_failures == 1
        assert state.backoff_until == 130.0

    def test_record_success_resets_failure_state(self, store):
        store.record_failure("example.com", backoff_seconds=30.0, now=100.0)
        state = store.record_success("example.com", now=110.0)
        assert state.consecutive_failures == 0
        assert state.backoff_until == 0.0
