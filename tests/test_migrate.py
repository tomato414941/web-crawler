"""Tests for database migrations."""

import os

import psycopg2
import pytest

from importlib import resources

from crawler.migrate import (
    BASELINE_VERSION,
    MIGRATIONS_PACKAGE,
    SCHEMA_MIGRATIONS_SQL,
    apply_migrations,
)
from crawler.url_ledger import (
    BLOCKED_HOST_BACKOFF_TABLE,
    LEASE_TABLE,
    PHYSICAL_QUEUE_TABLES,
    QUEUE_BACKLOG,
    QUEUE_EXPLORATION,
    QUEUE_RECRAWL,
    URL_LEDGER_TABLE,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_POSTGRES_DSN"),
    reason="TEST_POSTGRES_DSN not set",
)


def _reset_schema(dsn: str) -> None:
    frontline_table = PHYSICAL_QUEUE_TABLES[QUEUE_EXPLORATION]
    deferred_table = PHYSICAL_QUEUE_TABLES[QUEUE_BACKLOG]
    refresh_table = PHYSICAL_QUEUE_TABLES[QUEUE_RECRAWL]
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS public.schema_migrations")
            cur.execute("DROP TABLE IF EXISTS public.host_state")
            cur.execute(f"DROP TABLE IF EXISTS public.{frontline_table}")
            cur.execute(f"DROP TABLE IF EXISTS public.{deferred_table}")
            cur.execute(f"DROP TABLE IF EXISTS public.{refresh_table}")
            cur.execute(f"DROP TABLE IF EXISTS public.{BLOCKED_HOST_BACKOFF_TABLE}")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_exploration")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_backlog")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_recrawl")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_blocked_domain_backoff")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_blocked_host_backoff")
            cur.execute(f"DROP TABLE IF EXISTS public.{LEASE_TABLE}")
            cur.execute("DROP TABLE IF EXISTS public.frontier_lease_active")
            cur.execute(f"DROP TABLE IF EXISTS public.{URL_LEDGER_TABLE} CASCADE")
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
        "001_current_schema.sql",
        "024_normalize_constraint_names.sql",
        "025_rename_physical_queue_columns.sql",
        "026_add_current_intent.sql",
        "027_rename_domain_to_host.sql",
    ]

    conn = psycopg2.connect(migrated_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT to_regclass('public.pages'),
                       to_regclass('public.url_ledger'),
                       to_regclass('public.host_state'),
                       to_regclass('public.schema_migrations'),
                       to_regclass('public.crawler_runtime_stats'),
                       to_regclass('public.{PHYSICAL_QUEUE_TABLES[QUEUE_EXPLORATION]}'),
                       to_regclass('public.{PHYSICAL_QUEUE_TABLES[QUEUE_BACKLOG]}'),
                       to_regclass('public.{PHYSICAL_QUEUE_TABLES[QUEUE_RECRAWL]}'),
                       to_regclass('public.{BLOCKED_HOST_BACKOFF_TABLE}'),
                       to_regclass('public.{LEASE_TABLE}')
                """
            )
            assert cur.fetchone() == (
                "pages",
                "url_ledger",
                "host_state",
                "schema_migrations",
                "crawler_runtime_stats",
                PHYSICAL_QUEUE_TABLES[QUEUE_EXPLORATION],
                PHYSICAL_QUEUE_TABLES[QUEUE_BACKLOG],
                PHYSICAL_QUEUE_TABLES[QUEUE_RECRAWL],
                BLOCKED_HOST_BACKOFF_TABLE,
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
    assert "current_intent" in columns

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


def test_apply_migrations_skips_baseline_when_legacy_history_exists(migrated_dsn):
    root = resources.files(MIGRATIONS_PACKAGE)
    baseline_sql = root.joinpath(BASELINE_VERSION).read_text(encoding="utf-8")

    conn = psycopg2.connect(migrated_dsn)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_MIGRATIONS_SQL)
            cur.execute(baseline_sql)
            cur.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, EXTRACT(epoch FROM now()))",
                ("023_rename_frontier_queue_tables.sql",),
            )
        conn.commit()
    finally:
        conn.close()

    applied = apply_migrations(migrated_dsn)

    assert applied == [
        "024_normalize_constraint_names.sql",
        "025_rename_physical_queue_columns.sql",
        "026_add_current_intent.sql",
        "027_rename_domain_to_host.sql",
    ]

    conn = psycopg2.connect(migrated_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version FROM schema_migrations ORDER BY version")
            versions = [version for (version,) in cur.fetchall()]
    finally:
        conn.close()

    assert "023_rename_frontier_queue_tables.sql" in versions
    assert "024_normalize_constraint_names.sql" in versions
    assert "025_rename_physical_queue_columns.sql" in versions
    assert "026_add_current_intent.sql" in versions
    assert "027_rename_domain_to_host.sql" in versions
    assert BASELINE_VERSION not in versions
