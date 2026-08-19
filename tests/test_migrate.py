"""Tests for database migrations."""

import os

import psycopg2
import pytest

from crawler.migrate import (
    apply_migrations,
)
from crawler.url_identity import URL_IDENTITY_VERSION, url_identity_hash, url_identity_length
from crawler.host_runnable_heads import (
    HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE,
    HOST_RUNNABLE_HEADS_TABLE,
)
from crawler.scheduler_leases import ACTIVE_LEASES_TABLE as LEASE_TABLE
from crawler.scheduler_membership import (
    PHYSICAL_QUEUE_TABLES,
    QUEUE_RUNNABLE,
    QUEUE_SCHEDULED,
    QUEUE_REFRESH,
)
from crawler.scheduler_quarantine import BLOCKED_HOST_BACKOFF_TABLE
from crawler.url_ledger import URL_LEDGER_TABLE

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
            cur.execute(f"DROP TABLE IF EXISTS public.{HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE}")
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
            cur.execute("DROP TABLE IF EXISTS public.page_content")
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
        "001_schema.sql",
        "002_host_runnable_head_dirty_hosts.sql",
        "003_host_runnable_head_dirty_hosts_index.sql",
        "004_page_content_storage.sql",
        "005_url_ledger_identity.sql",
        "006_drop_page_content.sql",
    ]

    conn = psycopg2.connect(migrated_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT to_regclass('public.pages'),
                       to_regclass('public.page_content'),
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
                       to_regclass('public.{HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE}'),
                       to_regclass('public.idx_host_runnable_heads_ready'),
                       to_regclass('public.idx_host_runnable_heads_head_url'),
                       to_regclass('public.idx_host_runnable_head_dirty_hosts_queue_marked_at_host'),
                       to_regclass('public.idx_scheduler_queue_runnable_host_head'),
                       to_regclass('public.idx_scheduler_queue_scheduled_host_head'),
                       to_regclass('public.idx_scheduler_queue_refresh_host_head'),
                       to_regclass('public.idx_url_ledger_url_hash'),
                       to_regclass('public.idx_url_ledger_url_length')
                """
            )
            assert cur.fetchone() == (
                "pages",
                None,
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
                HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE,
                "idx_host_runnable_heads_ready",
                "idx_host_runnable_heads_head_url",
                "idx_host_runnable_head_dirty_hosts_queue_marked_at_host",
                "idx_scheduler_queue_runnable_host_head",
                "idx_scheduler_queue_scheduled_host_head",
                "idx_scheduler_queue_refresh_host_head",
                "idx_url_ledger_url_hash",
                "idx_url_ledger_url_length",
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
    assert "priority" not in columns
    assert "url_hash" in columns
    assert "url_length" in columns
    assert "url_identity_version" in columns
    assert "discovery_value" in columns
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
    assert "content" not in page_columns
    assert "content_type" in page_columns
    assert "storage_tier" in page_columns
    assert "storage_reason" in page_columns
    assert "stored_content_bytes" in page_columns
    assert "content_truncated" in page_columns
    assert "outlink_count" in page_columns
    assert "stored_outlink_count" in page_columns

    conn = psycopg2.connect(migrated_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'page_content'
                ORDER BY ordinal_position
                """
            )
            page_content_columns = [column_name for (column_name,) in cur.fetchall()]
    finally:
        conn.close()

    assert page_content_columns == []

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
    assert "head_priority" not in host_head_columns
    assert "head_scheduler_score" in host_head_columns

    conn = psycopg2.connect(migrated_dsn)
    try:
        with conn.cursor() as cur:
            for queue_table in (
                PHYSICAL_QUEUE_TABLES[QUEUE_RUNNABLE],
                PHYSICAL_QUEUE_TABLES[QUEUE_SCHEDULED],
                PHYSICAL_QUEUE_TABLES[QUEUE_REFRESH],
                BLOCKED_HOST_BACKOFF_TABLE,
            ):
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (queue_table,),
                )
                queue_columns = [column_name for (column_name,) in cur.fetchall()]
                assert "priority" not in queue_columns
                assert "scheduler_score" in queue_columns
    finally:
        conn.close()


def test_apply_migrations_is_idempotent(migrated_dsn):
    apply_migrations(migrated_dsn)

    applied = apply_migrations(migrated_dsn)

    assert applied == []


def test_url_ledger_identity_migration_backfills_existing_rows(migrated_dsn):
    conn = psycopg2.connect(migrated_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE public.schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at DOUBLE PRECISION NOT NULL
                )"""
            )
            cur.executemany(
                "INSERT INTO public.schema_migrations (version, applied_at) VALUES (%s, 1.0)",
                [
                    ("001_schema.sql",),
                    ("002_host_runnable_head_dirty_hosts.sql",),
                    ("003_host_runnable_head_dirty_hosts_index.sql",),
                    ("004_page_content_storage.sql",),
                ],
            )
            cur.execute(
                """
                CREATE TABLE public.url_ledger (
                    url text PRIMARY KEY,
                    host text NOT NULL,
                    discovery_value double precision NOT NULL DEFAULT 0,
                    source_url text,
                    added_at double precision NOT NULL,
                    next_fetch_at double precision NOT NULL,
                    fetch_count integer NOT NULL DEFAULT 0,
                    fail_streak integer NOT NULL DEFAULT 0,
                    last_status integer,
                    last_error text,
                    last_fetch_at double precision,
                    terminal_reason text,
                    terminalized_at double precision,
                    current_intent text
                )
                """
            )
            cur.execute(
                """INSERT INTO public.url_ledger (
                       url, host, discovery_value, added_at, next_fetch_at, current_intent
                   ) VALUES (%s, 'example.com', 1.0, 1.0, 1.0, 'explore')""",
                ("https://example.com/old",),
            )
        conn.commit()
    finally:
        conn.close()

    applied = apply_migrations(migrated_dsn)

    assert applied == ["005_url_ledger_identity.sql", "006_drop_page_content.sql"]
    conn = psycopg2.connect(migrated_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT url_hash, url_length, url_identity_version
                    FROM public.{URL_LEDGER_TABLE}
                    WHERE url = %s""",
                ("https://example.com/old",),
            )
            assert cur.fetchone() == (
                url_identity_hash("https://example.com/old"),
                url_identity_length("https://example.com/old"),
                URL_IDENTITY_VERSION,
            )
    finally:
        conn.close()
