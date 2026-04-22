"""Database migration runner."""

from __future__ import annotations

from importlib import resources
from time import time

import psycopg2

MIGRATIONS_PACKAGE = "crawler.sql_migrations"
BASELINE_VERSION = "001_schema.sql"
SCHEMA_MIGRATIONS_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at DOUBLE PRECISION NOT NULL
);
"""
CURRENT_SCHEMA_SQL = """
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'url_ledger'
          AND column_name = 'priority'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'url_ledger'
          AND column_name = 'discovery_value'
    ) THEN
        ALTER TABLE public.url_ledger RENAME COLUMN priority TO discovery_value;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'scheduler_queue_runnable'
          AND column_name = 'priority'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'scheduler_queue_runnable'
          AND column_name = 'scheduler_score'
    ) THEN
        ALTER TABLE public.scheduler_queue_runnable RENAME COLUMN priority TO scheduler_score;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'scheduler_queue_scheduled'
          AND column_name = 'priority'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'scheduler_queue_scheduled'
          AND column_name = 'scheduler_score'
    ) THEN
        ALTER TABLE public.scheduler_queue_scheduled RENAME COLUMN priority TO scheduler_score;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'scheduler_queue_refresh'
          AND column_name = 'priority'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'scheduler_queue_refresh'
          AND column_name = 'scheduler_score'
    ) THEN
        ALTER TABLE public.scheduler_queue_refresh RENAME COLUMN priority TO scheduler_score;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'scheduler_queue_retry_quarantine'
          AND column_name = 'priority'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'scheduler_queue_retry_quarantine'
          AND column_name = 'scheduler_score'
    ) THEN
        ALTER TABLE public.scheduler_queue_retry_quarantine RENAME COLUMN priority TO scheduler_score;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'host_runnable_heads'
          AND column_name = 'head_priority'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'host_runnable_heads'
          AND column_name = 'head_scheduler_score'
    ) THEN
        ALTER TABLE public.host_runnable_heads RENAME COLUMN head_priority TO head_scheduler_score;
    END IF;
END $$;
"""


def _migration_names() -> list[str]:
    root = resources.files(MIGRATIONS_PACKAGE)
    return sorted(
        entry.name for entry in root.iterdir() if entry.is_file() and entry.name.endswith(".sql")
    )


def apply_migrations(dsn: str) -> list[str]:
    """Apply pending SQL migrations and return the versions applied."""
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_MIGRATIONS_SQL)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT version FROM schema_migrations")
            applied = {version for (version,) in cur.fetchall()}

        applied_now: list[str] = []
        root = resources.files(MIGRATIONS_PACKAGE)
        for version in _migration_names():
            if version in applied:
                continue

            sql = root.joinpath(version).read_text(encoding="utf-8")
            with conn.cursor() as cur:
                cur.execute(sql)
                cur.execute(
                    "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                    (version, time()),
                )
            conn.commit()
            applied_now.append(version)

        with conn.cursor() as cur:
            cur.execute(CURRENT_SCHEMA_SQL)
        conn.commit()

        return applied_now
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
