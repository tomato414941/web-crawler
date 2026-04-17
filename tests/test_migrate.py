"""Tests for database migrations."""

import os

import psycopg2
import pytest

from crawler.migrate import apply_migrations
from crawler.url_ledger import (
    BLOCKED_DOMAIN_BACKOFF_TABLE,
    LEASE_TABLE,
    QUEUE_BACKLOG,
    QUEUE_EXPLORATION,
    QUEUE_RECRAWL,
    QUEUE_TABLE_BY_CLASS,
    URL_LEDGER_TABLE,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_POSTGRES_DSN"),
    reason="TEST_POSTGRES_DSN not set",
)


def _reset_schema(dsn: str) -> None:
    frontline_table = QUEUE_TABLE_BY_CLASS[QUEUE_EXPLORATION]
    deferred_table = QUEUE_TABLE_BY_CLASS[QUEUE_BACKLOG]
    refresh_table = QUEUE_TABLE_BY_CLASS[QUEUE_RECRAWL]
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS public.schema_migrations")
            cur.execute("DROP TABLE IF EXISTS public.domain_state")
            cur.execute(f"DROP TABLE IF EXISTS public.{frontline_table}")
            cur.execute(f"DROP TABLE IF EXISTS public.{deferred_table}")
            cur.execute(f"DROP TABLE IF EXISTS public.{refresh_table}")
            cur.execute(f"DROP TABLE IF EXISTS public.{BLOCKED_DOMAIN_BACKOFF_TABLE}")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_exploration")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_backlog")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_recrawl")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_blocked_domain_backoff")
            cur.execute(f"DROP TABLE IF EXISTS public.{LEASE_TABLE}")
            cur.execute("DROP TABLE IF EXISTS public.frontier_lease_active")
            cur.execute(f"DROP TABLE IF EXISTS public.{URL_LEDGER_TABLE}")
            cur.execute("DROP TABLE IF EXISTS public.crawler_runtime_stats")
            cur.execute("DROP TABLE IF EXISTS public.pages")
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def migrated_dsn():
    dsn = os.environ["TEST_POSTGRES_DSN"]
    _reset_schema(dsn)
    yield dsn
    _reset_schema(dsn)


def test_apply_migrations_creates_expected_tables(migrated_dsn):
    applied = apply_migrations(migrated_dsn)

    assert applied == [
        "001_initial_schema.sql",
        "002_runtime_stats.sql",
        "003_frontier_queue_classes.sql",
        "004_reclassify_frontier_queue_classes.sql",
        "005_rebalance_exploration_queue_classes.sql",
        "006_reclassify_queue_by_domain_novelty.sql",
        "007_frontier_pending_queue_tables.sql",
        "008_expand_frontier_pending_queue_tables.sql",
        "009_frontier_active_lease_table.sql",
        "010_frontier_queue_branch_keys.sql",
        "011_frontier_blocked_domain_backoff_queue.sql",
        "012_blocked_domain_backoff_quarantined_at.sql",
        "013_domain_state_latency_ewma.sql",
        "014_rename_frontier_lease_active.sql",
        "015_frontier_terminal_decision.sql",
        "016_drop_frontier_queue_class.sql",
        "017_drop_frontier_status.sql",
        "018_drop_frontier_discovery_kind.sql",
        "019_drop_frontier_archetype.sql",
        "020_rename_frontier_to_url_ledger.sql",
        "021_drop_depth_columns.sql",
        "022_drop_url_ledger_lease_columns.sql",
        "023_rename_frontier_queue_tables.sql",
    ]

    conn = psycopg2.connect(migrated_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT to_regclass('public.pages'),
                       to_regclass('public.url_ledger'),
                       to_regclass('public.domain_state'),
                       to_regclass('public.schema_migrations'),
                       to_regclass('public.crawler_runtime_stats'),
                       to_regclass('public.{QUEUE_TABLE_BY_CLASS[QUEUE_EXPLORATION]}'),
                       to_regclass('public.{QUEUE_TABLE_BY_CLASS[QUEUE_BACKLOG]}'),
                       to_regclass('public.{QUEUE_TABLE_BY_CLASS[QUEUE_RECRAWL]}'),
                       to_regclass('public.{BLOCKED_DOMAIN_BACKOFF_TABLE}'),
                       to_regclass('public.{LEASE_TABLE}')
                """
            )
            assert cur.fetchone() == (
                "pages",
                "url_ledger",
                "domain_state",
                "schema_migrations",
                "crawler_runtime_stats",
                QUEUE_TABLE_BY_CLASS[QUEUE_EXPLORATION],
                QUEUE_TABLE_BY_CLASS[QUEUE_BACKLOG],
                QUEUE_TABLE_BY_CLASS[QUEUE_RECRAWL],
                BLOCKED_DOMAIN_BACKOFF_TABLE,
                LEASE_TABLE,
            )
    finally:
        conn.close()


def test_apply_migrations_drops_legacy_url_ledger_columns(migrated_dsn):
    apply_migrations(migrated_dsn)

    conn = psycopg2.connect(migrated_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'url_ledger'
                ORDER BY ordinal_position
                """
            )
            columns = [column_name for (column_name,) in cur.fetchall()]
    finally:
        conn.close()

    assert "discovery_kind" not in columns
    assert "archetype" not in columns
    assert "depth" not in columns
    assert "lease_token" not in columns
    assert "lease_expires_at" not in columns

    conn = psycopg2.connect(migrated_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'pages'
                ORDER BY ordinal_position
                """
            )
            page_columns = [column_name for (column_name,) in cur.fetchall()]
    finally:
        conn.close()

    assert "depth" not in page_columns


def test_apply_migrations_is_idempotent(migrated_dsn):
    apply_migrations(migrated_dsn)

    applied = apply_migrations(migrated_dsn)

    assert applied == []
