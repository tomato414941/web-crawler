"""Scheduler invariant checker tests."""

import os

import psycopg2
import pytest

from crawler.migrate import apply_migrations
from crawler.scheduler_invariants import SchedulerInvariantChecker
from crawler.url_ledger import (
    BLOCKED_HOST_BACKOFF_TABLE,
    HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE,
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
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS public.schema_migrations")
            cur.execute(f"DROP TABLE IF EXISTS public.{HOST_RUNNABLE_HEAD_DIRTY_HOSTS_TABLE}")
            cur.execute(f"DROP TABLE IF EXISTS public.{HOST_RUNNABLE_HEADS_TABLE}")
            cur.execute("DROP TABLE IF EXISTS public.host_ledger")
            cur.execute("DROP TABLE IF EXISTS public.host_state")
            cur.execute(f"DROP TABLE IF EXISTS public.{PHYSICAL_QUEUE_TABLES[QUEUE_RUNNABLE]}")
            cur.execute(f"DROP TABLE IF EXISTS public.{PHYSICAL_QUEUE_TABLES[QUEUE_SCHEDULED]}")
            cur.execute(f"DROP TABLE IF EXISTS public.{PHYSICAL_QUEUE_TABLES[QUEUE_REFRESH]}")
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
def conn():
    dsn = os.environ["TEST_POSTGRES_DSN"]
    _reset_schema(dsn)
    apply_migrations(dsn)
    connection = psycopg2.connect(dsn)
    connection.autocommit = False
    yield connection
    connection.rollback()
    connection.close()
    _reset_schema(dsn)


def _insert_ledger(cur, url: str, *, terminal_reason: str | None = None) -> None:
    cur.execute(
        f"""INSERT INTO {URL_LEDGER_TABLE} (
                url, host, discovery_value, source_url, added_at, next_fetch_at,
                current_intent, terminal_reason, terminalized_at
            )
            VALUES (%s, %s, 1.0, NULL, 100.0, 100.0, 'explore', %s, %s)""",
        (
            url,
            "example.com",
            terminal_reason,
            101.0 if terminal_reason else None,
        ),
    )


def _insert_queue(cur, queue: str, url: str) -> None:
    cur.execute(
        f"""INSERT INTO {PHYSICAL_QUEUE_TABLES[queue]} (
                url, host, scheduler_score, next_fetch_at, added_at, branch_key
            )
            VALUES (%s, 'example.com', 1.0, 100.0, 100.0, '/')""",
        (url,),
    )


def test_scheduler_invariant_checker_reports_clean_state(conn):
    report = SchedulerInvariantChecker(conn).check(now=200.0)

    assert report.ok is True
    assert report.violations_total == 0
    assert report.to_dict()["samples"]["duplicate_memberships"] == []


def test_scheduler_invariant_checker_detects_duplicate_membership(conn):
    with conn.cursor() as cur:
        _insert_ledger(cur, "https://example.com/a")
        _insert_queue(cur, QUEUE_RUNNABLE, "https://example.com/a")
        _insert_queue(cur, QUEUE_SCHEDULED, "https://example.com/a")
    conn.commit()

    report = SchedulerInvariantChecker(conn).check(now=200.0)

    assert report.ok is False
    assert report.duplicate_memberships == 1
    assert report.samples["duplicate_memberships"] == [
        {
            "url": "https://example.com/a",
            "memberships": [QUEUE_RUNNABLE, QUEUE_SCHEDULED],
        }
    ]


def test_scheduler_invariant_checker_detects_terminal_url_in_live_queue(conn):
    with conn.cursor() as cur:
        _insert_ledger(cur, "https://example.com/terminal", terminal_reason="unsafe_egress")
        _insert_queue(cur, QUEUE_RUNNABLE, "https://example.com/terminal")
    conn.commit()

    report = SchedulerInvariantChecker(conn).check(now=200.0)

    assert report.terminal_in_live_queue == 1
    assert report.samples["terminal_in_live_queue"] == [
        {
            "url": "https://example.com/terminal",
            "membership": QUEUE_RUNNABLE,
            "terminal_reason": "unsafe_egress",
        }
    ]


def test_scheduler_invariant_checker_detects_expired_lease(conn):
    with conn.cursor() as cur:
        _insert_ledger(cur, "https://example.com/leased")
        cur.execute(
            f"""INSERT INTO {LEASE_TABLE} (
                    url, host, physical_queue, lease_token, lease_expires_at
                )
                VALUES (%s, 'example.com', %s, 'lease-1', 100.0)""",
            ("https://example.com/leased", QUEUE_RUNNABLE),
        )
    conn.commit()

    report = SchedulerInvariantChecker(conn).check(now=200.0)

    assert report.expired_leases == 1
    assert report.samples["expired_leases"] == [
        {
            "url": "https://example.com/leased",
            "physical_queue": QUEUE_RUNNABLE,
            "lease_expires_at": 100.0,
        }
    ]


def test_scheduler_invariant_checker_detects_orphan_host_head(conn):
    with conn.cursor() as cur:
        _insert_ledger(cur, "https://example.com/head")
        cur.execute(
            f"""INSERT INTO {HOST_RUNNABLE_HEADS_TABLE} (
                    physical_queue, host, head_url, head_next_fetch_at, head_added_at,
                    head_scheduler_score, runnable_url_count, execution_tier,
                    latency_penalty, runnable_at, refreshed_at
                )
                VALUES (
                    %s, 'example.com', %s, 100.0, 100.0, 1.0, 1, 1, 0, 100.0, 100.0
                )""",
            (QUEUE_RUNNABLE, "https://example.com/head"),
        )
    conn.commit()

    report = SchedulerInvariantChecker(conn).check(now=200.0)

    assert report.orphan_host_heads == 1
    assert report.samples["orphan_host_heads"] == [
        {
            "physical_queue": QUEUE_RUNNABLE,
            "host": "example.com",
            "head_url": "https://example.com/head",
        }
    ]
