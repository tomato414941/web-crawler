"""Tests for database migrations."""

import os

import psycopg2
import pytest

from crawler.migrate import (
    apply_migrations,
)
from crawler.url_ledger import (
    BLOCKED_HOST_BACKOFF_TABLE,
    HOST_RUNNABLE_HEADS_TABLE,
    LEASE_TABLE,
    PHYSICAL_QUEUE_TABLES,
    QUEUE_RUNNABLE,
    QUEUE_SCHEDULED,
    QUEUE_REFRESH,
    URL_LEDGER_TABLE,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_POSTGRES_DSN"),
    reason="TEST_POSTGRES_DSN not set",
)


def _reset_schema(dsn: str) -> None:
    runnable_table = PHYSICAL_QUEUE_TABLES[QUEUE_RUNNABLE]
    scheduled_table = PHYSICAL_QUEUE_TABLES[QUEUE_SCHEDULED]
    refresh_table = PHYSICAL_QUEUE_TABLES[QUEUE_REFRESH]
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS public.schema_migrations")
            cur.execute(f"DROP TABLE IF EXISTS public.{HOST_RUNNABLE_HEADS_TABLE}")
            cur.execute("DROP TABLE IF EXISTS public.host_ledger")
            cur.execute("DROP TABLE IF EXISTS public.host_state")
            cur.execute(f"DROP TABLE IF EXISTS public.{runnable_table}")
            cur.execute(f"DROP TABLE IF EXISTS public.{scheduled_table}")
            cur.execute(f"DROP TABLE IF EXISTS public.{refresh_table}")
            cur.execute(f"DROP TABLE IF EXISTS public.{BLOCKED_HOST_BACKOFF_TABLE}")
            cur.execute(f"DROP TABLE IF EXISTS public.{LEASE_TABLE}")
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

    assert applied == ["001_schema.sql"]

    conn = psycopg2.connect(migrated_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT to_regclass('public.pages'),
                       to_regclass('public.url_ledger'),
                       to_regclass('public.host_ledger'),
                       to_regclass('public.host_state'),
                       to_regclass('public.schema_migrations'),
                       to_regclass('public.crawler_runtime_stats'),
                       to_regclass('public.{PHYSICAL_QUEUE_TABLES[QUEUE_RUNNABLE]}'),
                       to_regclass('public.{PHYSICAL_QUEUE_TABLES[QUEUE_SCHEDULED]}'),
                       to_regclass('public.{PHYSICAL_QUEUE_TABLES[QUEUE_REFRESH]}'),
                       to_regclass('public.{BLOCKED_HOST_BACKOFF_TABLE}'),
                       to_regclass('public.{LEASE_TABLE}'),
                       to_regclass('public.{HOST_RUNNABLE_HEADS_TABLE}'),
                       to_regclass('public.idx_host_runnable_heads_ready'),
                       to_regclass('public.idx_host_runnable_heads_head_url'),
                       to_regclass('public.idx_scheduler_queue_runnable_host_head'),
                       to_regclass('public.idx_scheduler_queue_scheduled_host_head'),
                       to_regclass('public.idx_scheduler_queue_refresh_host_head')
                """
            )
            assert cur.fetchone() == (
                "pages",
                "url_ledger",
                "host_ledger",
                "host_state",
                "schema_migrations",
                "crawler_runtime_stats",
                PHYSICAL_QUEUE_TABLES[QUEUE_RUNNABLE],
                PHYSICAL_QUEUE_TABLES[QUEUE_SCHEDULED],
                PHYSICAL_QUEUE_TABLES[QUEUE_REFRESH],
                BLOCKED_HOST_BACKOFF_TABLE,
                LEASE_TABLE,
                HOST_RUNNABLE_HEADS_TABLE,
                "idx_host_runnable_heads_ready",
                "idx_host_runnable_heads_head_url",
                "idx_scheduler_queue_runnable_host_head",
                "idx_scheduler_queue_scheduled_host_head",
                "idx_scheduler_queue_refresh_host_head",
            )
    finally:
        conn.close()


def test_apply_migrations_creates_current_url_ledger_columns(migrated_dsn):
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

    conn = psycopg2.connect(migrated_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'host_runnable_heads'
                ORDER BY ordinal_position
                """
            )
            host_head_columns = [column_name for (column_name,) in cur.fetchall()]
    finally:
        conn.close()

    assert "execution_tier" in host_head_columns


def test_apply_migrations_is_idempotent(migrated_dsn):
    apply_migrations(migrated_dsn)

    applied = apply_migrations(migrated_dsn)

    assert applied == []
