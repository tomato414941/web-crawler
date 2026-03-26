"""Database migration runner."""

from __future__ import annotations

from importlib import resources
from time import time

import psycopg2

MIGRATIONS_PACKAGE = "crawler.sql_migrations"
SCHEMA_MIGRATIONS_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at DOUBLE PRECISION NOT NULL
);
"""


def _migration_names() -> list[str]:
    root = resources.files(MIGRATIONS_PACKAGE)
    return sorted(
        entry.name
        for entry in root.iterdir()
        if entry.is_file() and entry.name.endswith(".sql")
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

        return applied_now
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
