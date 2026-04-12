"""Tests for database migrations."""

import os

import psycopg2
import pytest

from crawler.migrate import apply_migrations

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
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_exploration")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_backlog")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_recrawl")
            cur.execute("DROP TABLE IF EXISTS public.frontier_queue_blocked_domain_backoff")
            cur.execute("DROP TABLE IF EXISTS public.frontier_lease_active")
            cur.execute("DROP TABLE IF EXISTS public.frontier")
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
    ]

    conn = psycopg2.connect(migrated_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT to_regclass('public.pages'),
                       to_regclass('public.frontier'),
                       to_regclass('public.domain_state'),
                       to_regclass('public.schema_migrations'),
                       to_regclass('public.crawler_runtime_stats'),
                       to_regclass('public.frontier_queue_exploration'),
                       to_regclass('public.frontier_queue_backlog'),
                       to_regclass('public.frontier_queue_recrawl'),
                       to_regclass('public.frontier_queue_blocked_domain_backoff'),
                       to_regclass('public.frontier_lease_active')
                """
            )
            assert cur.fetchone() == (
                "pages",
                "frontier",
                "domain_state",
                "schema_migrations",
                "crawler_runtime_stats",
                "frontier_queue_exploration",
                "frontier_queue_backlog",
                "frontier_queue_recrawl",
                "frontier_queue_blocked_domain_backoff",
                "frontier_lease_active",
            )
    finally:
        conn.close()


def test_apply_migrations_is_idempotent(migrated_dsn):
    apply_migrations(migrated_dsn)

    applied = apply_migrations(migrated_dsn)

    assert applied == []
