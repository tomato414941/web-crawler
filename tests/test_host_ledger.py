"""Tests for persistent host ledger storage."""

import os

import psycopg2
import pytest

from crawler.host_ledger import HOST_LEDGER_TABLE, HostLedgerStore, registrable_domain_for_host
from crawler.migrate import apply_migrations
from crawler.url_ledger import URL_LEDGER_TABLE

PG_DSN = os.environ.get("TEST_POSTGRES_DSN", "postgresql://crawler:crawler@localhost/crawldb_test")


def _pg_available():
    try:
        conn = psycopg2.connect(PG_DSN)
        conn.close()
        return True
    except Exception:
        return False


requires_pg = pytest.mark.skipif(not _pg_available(), reason="Postgres not available")


def test_registrable_domain_for_host_uses_best_effort_suffix():
    assert registrable_domain_for_host("www.example.com") == "example.com"
    assert registrable_domain_for_host("example.com:8443") == "example.com"
    assert registrable_domain_for_host("127.0.0.1:8080") == "127.0.0.1"
    assert registrable_domain_for_host("localhost") == "localhost"


@requires_pg
class TestHostLedgerStore:
    @pytest.fixture(autouse=True)
    def store(self):
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS schema_migrations")
            cur.execute(f"DROP TABLE IF EXISTS {HOST_LEDGER_TABLE}")
            cur.execute(f"DROP TABLE IF EXISTS {URL_LEDGER_TABLE} CASCADE")
            cur.execute("DROP TABLE IF EXISTS page_content")
            cur.execute("DROP TABLE IF EXISTS pages")
        conn.commit()
        conn.close()
        apply_migrations(PG_DSN)
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        store = HostLedgerStore(conn)
        yield store
        conn.close()

    def test_record_discovered_urls_is_idempotent_for_zero_new_urls(self, store):
        store.record_discovered_urls({"www.example.com": 1}, seen_at=100.0)
        store.record_discovered_urls({"www.example.com": 0}, seen_at=120.0)

        record = store.get("www.example.com")

        assert record is not None
        assert record.registrable_domain == "example.com"
        assert record.first_seen_at == 100.0
        assert record.last_seen_at == 120.0
        assert record.known_url_count == 1

    def test_record_success_and_failure_update_history(self, store):
        store.record_discovered_urls({"example.com": 2}, seen_at=100.0)
        store.record_success("example.com", at=130.0)
        store.record_failure("example.com", at=150.0)

        record = store.get("example.com")

        assert record is not None
        assert record.last_success_at == 130.0
        assert record.last_failure_at == 150.0
        assert record.success_count == 1
        assert record.failure_count == 1

    def test_record_robots_check_updates_summary(self, store):
        store.record_robots_check("example.com", status="ok", checked_at=200.0)

        record = store.get("example.com")

        assert record is not None
        assert record.robots_last_checked_at == 200.0
        assert record.robots_status == "ok"
