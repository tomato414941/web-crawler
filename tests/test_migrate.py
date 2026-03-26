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
            cur.execute("DROP TABLE IF EXISTS public.frontier")
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

    assert applied == ["001_initial_schema.sql"]

    conn = psycopg2.connect(migrated_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT to_regclass('public.pages'),
                       to_regclass('public.frontier'),
                       to_regclass('public.domain_state'),
                       to_regclass('public.schema_migrations')
                """
            )
            assert cur.fetchone() == (
                "pages",
                "frontier",
                "domain_state",
                "schema_migrations",
            )
    finally:
        conn.close()


def test_apply_migrations_is_idempotent(migrated_dsn):
    apply_migrations(migrated_dsn)

    applied = apply_migrations(migrated_dsn)

    assert applied == []
