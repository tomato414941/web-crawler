"""Tests for persistent host state storage."""

import os
import time

import psycopg2
import pytest

from crawler.host_store import HostStore
from crawler.host_ledger import HOST_LEDGER_TABLE
from crawler.migrate import apply_migrations
from crawler.url_ledger_store import URL_LEDGER_TABLE

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
class TestHostStore:
    @pytest.fixture(autouse=True)
    def store(self):
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS schema_migrations")
            cur.execute(f"DROP TABLE IF EXISTS {HOST_LEDGER_TABLE}")
            cur.execute("DROP TABLE IF EXISTS host_state")
            cur.execute(f"DROP TABLE IF EXISTS {URL_LEDGER_TABLE} CASCADE")
            cur.execute("DROP TABLE IF EXISTS page_content")
            cur.execute("DROP TABLE IF EXISTS pages")
        conn.commit()
        conn.close()
        apply_migrations(PG_DSN)
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        store = HostStore(conn, default_delay=1.5)
        yield store
        conn.close()

    def test_get_or_create_returns_defaults(self, store):
        state = store.get_or_create("example.com")
        assert state.host_key == "example.com"
        assert state.crawl_delay_seconds == 1.5
        assert state.next_request_at == 0.0
        assert state.backoff_until == 0.0
        assert state.consecutive_failures == 0
        assert state.latency_ewma_ms == 0.0
        assert state.latency_last_ms == 0.0
        assert state.latency_observed_at == 0.0
        assert state.latency_sample_count == 0

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

    def test_record_success_updates_latency_ewma(self, store):
        first = store.record_success("example.com", now=100.0, request_latency_ms=100.0)
        second = store.record_success("example.com", now=110.0, request_latency_ms=300.0)

        assert first.latency_ewma_ms == pytest.approx(100.0, abs=1e-6)
        assert first.latency_last_ms == pytest.approx(100.0, abs=1e-6)
        assert first.latency_observed_at == pytest.approx(100.0, abs=1e-6)
        assert first.latency_sample_count == 1
        assert second.latency_ewma_ms == pytest.approx(140.0, abs=1e-6)
        assert second.latency_last_ms == pytest.approx(300.0, abs=1e-6)
        assert second.latency_observed_at == pytest.approx(110.0, abs=1e-6)
        assert second.latency_sample_count == 2

    def test_record_success_without_latency_preserves_observations(self, store):
        observed = store.record_success("example.com", now=100.0, request_latency_ms=100.0)
        unobserved = store.record_success("example.com", now=110.0)

        assert unobserved.latency_ewma_ms == pytest.approx(observed.latency_ewma_ms, abs=1e-6)
        assert unobserved.latency_last_ms == pytest.approx(observed.latency_last_ms, abs=1e-6)
        assert unobserved.latency_observed_at == pytest.approx(
            observed.latency_observed_at,
            abs=1e-6,
        )
        assert unobserved.latency_sample_count == observed.latency_sample_count

    def test_record_success_many_updates_hosts_in_one_call(self, store):
        store.record_failure("a.example", backoff_seconds=30.0, now=100.0)

        updated = store.record_success_many(
            [
                ("a.example", 100.0),
                ("a.example", 300.0),
                ("b.example", None),
            ],
            now=120.0,
        )

        assert updated == 2
        a_state = store.get_or_create("a.example")
        b_state = store.get_or_create("b.example")
        assert a_state.consecutive_failures == 0
        assert a_state.backoff_until == 0.0
        assert a_state.latency_last_ms == pytest.approx(300.0, abs=1e-6)
        assert a_state.latency_sample_count == 2
        assert b_state.consecutive_failures == 0
        assert b_state.latency_sample_count == 0
