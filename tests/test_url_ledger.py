"""Tests for URL ledger module."""

import os
import time

import psycopg2
import pytest

from crawler.domain_store import DomainStore
from crawler.url_ledger import (
    BLOCKED_DOMAIN_BACKOFF_TABLE,
    CrawlTask,
    INTENT_EXPLORE,
    INTENT_REFRESH,
    LEASE_TABLE,
    LEASE_STRATEGY_HOST_FIRST,
    QUEUE_BACKLOG,
    QUEUE_EXPLORATION,
    QUEUE_RECRAWL,
    QUEUE_TABLE_BY_CLASS,
    RUNNABLE_SURFACE_FRONTLINE,
    RUNNABLE_SURFACE_DEFERRED,
    RUNNABLE_SURFACE_REFRESH,
    RUNNABLE_SURFACE_NORMAL,
    URL_LEDGER_TABLE,
    UrlLedger,
)
from crawler.migrate import apply_migrations
from crawler.urls import normalize_url

PG_DSN = os.environ.get("TEST_POSTGRES_DSN", "postgresql://crawler:crawler@localhost/crawldb_test")


def _pg_available():
    try:
        conn = psycopg2.connect(PG_DSN)
        conn.close()
        return True
    except Exception:
        return False


requires_pg = pytest.mark.skipif(not _pg_available(), reason="Postgres not available")


class TestNormalizeUrl:
    def test_removes_fragment(self):
        result = normalize_url("http://example.com/page#section")
        assert result == "http://example.com/page"

    def test_sorts_query_params(self):
        result = normalize_url("http://example.com/page?b=2&a=1")
        assert result == "http://example.com/page?a=1&b=2"

    def test_removes_trailing_slash(self):
        result = normalize_url("http://example.com/path/")
        assert result == "http://example.com/path"

    def test_keeps_root_slash(self):
        result = normalize_url("http://example.com/")
        assert result == "http://example.com/"

    def test_lowercases_scheme_and_host(self):
        result = normalize_url("HTTP://EXAMPLE.COM/Path")
        assert result == "http://example.com/Path"

    def test_empty_query_params(self):
        result = normalize_url("http://example.com/page")
        assert result == "http://example.com/page"

    def test_complex_url(self):
        result = normalize_url("HTTPS://Example.COM/path/?z=3&a=1&m=2#anchor")
        assert result == "https://example.com/path?a=1&m=2&z=3"


class TestCrawlTask:
    def test_default_values(self):
        task = CrawlTask(url="http://example.com")
        assert task.url == "http://example.com"
        assert task.priority == 1.0
        assert task.source_url is None
        assert task.added_at > 0

    def test_custom_values(self):
        task = CrawlTask(
            url="http://example.com/page",
            priority=0.5,
            source_url="http://example.com",
            added_at=1000.0,
            next_fetch_at=1200.0,
        )
        assert task.priority == 0.5
        assert task.added_at == 1000.0
        assert task.next_fetch_at == 1200.0

    def test_added_at_auto_set(self):
        before = time.time()
        task = CrawlTask(url="http://example.com")
        after = time.time()
        assert before <= task.added_at <= after


@requires_pg
class TestUrlLedger:
    @pytest.fixture(autouse=True)
    def ledger(self):
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS schema_migrations")
            cur.execute(f"DROP TABLE IF EXISTS {LEASE_TABLE}")
            cur.execute("DROP TABLE IF EXISTS frontier_lease_active")
            cur.execute(f"DROP TABLE IF EXISTS {QUEUE_TABLE_BY_CLASS[QUEUE_EXPLORATION]}")
            cur.execute(f"DROP TABLE IF EXISTS {QUEUE_TABLE_BY_CLASS[QUEUE_BACKLOG]}")
            cur.execute(f"DROP TABLE IF EXISTS {QUEUE_TABLE_BY_CLASS[QUEUE_RECRAWL]}")
            cur.execute(f"DROP TABLE IF EXISTS {BLOCKED_DOMAIN_BACKOFF_TABLE}")
            cur.execute("DROP TABLE IF EXISTS frontier_queue_exploration")
            cur.execute("DROP TABLE IF EXISTS frontier_queue_backlog")
            cur.execute("DROP TABLE IF EXISTS frontier_queue_recrawl")
            cur.execute("DROP TABLE IF EXISTS frontier_queue_blocked_domain_backoff")
            cur.execute(f"DROP TABLE IF EXISTS {URL_LEDGER_TABLE}")
            cur.execute("DROP TABLE IF EXISTS domain_state")
            cur.execute("DROP TABLE IF EXISTS crawler_runtime_stats")
            cur.execute("DROP TABLE IF EXISTS pages")
        conn.commit()
        conn.close()
        apply_migrations(PG_DSN)
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        f = UrlLedger(conn)
        self.domain_store = DomainStore(conn)
        f.attach_domain_store(self.domain_store)
        yield f
        conn.close()

    def _queue_counts(self, ledger, url: str) -> tuple[int, int, int]:
        with ledger._conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {QUEUE_TABLE_BY_CLASS[QUEUE_EXPLORATION]} WHERE url = %s", (url,))
            (exploration_count,) = cur.fetchone()
            cur.execute(f"SELECT COUNT(*) FROM {QUEUE_TABLE_BY_CLASS[QUEUE_BACKLOG]} WHERE url = %s", (url,))
            (backlog_count,) = cur.fetchone()
            cur.execute(f"SELECT COUNT(*) FROM {QUEUE_TABLE_BY_CLASS[QUEUE_RECRAWL]} WHERE url = %s", (url,))
            (recrawl_count,) = cur.fetchone()
        return exploration_count, backlog_count, recrawl_count

    def test_add_new_url_returns_true(self, ledger):
        task = CrawlTask(url="http://example.com")
        assert ledger.place(task) is True

    def test_add_duplicate_url_returns_false(self, ledger):
        task1 = CrawlTask(url="http://example.com")
        task2 = CrawlTask(url="http://example.com")
        ledger.place(task1)
        assert ledger.place(task2) is False

    def test_add_normalizes_url(self, ledger):
        task1 = CrawlTask(url="http://example.com/page#section")
        task2 = CrawlTask(url="http://example.com/page")
        ledger.place(task1)
        assert ledger.place(task2) is False

    def test_add_many_returns_count(self, ledger):
        tasks = [
            CrawlTask(url="http://example.com/1"),
            CrawlTask(url="http://example.com/2"),
            CrawlTask(url="http://example.com/1"),
        ]
        assert ledger.place_many(tasks) == 2

    def test_discover_keeps_url_out_of_queue_tables(self, ledger):
        task = CrawlTask(url="http://example.com/discovered")

        assert ledger.discover(task) is True

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {URL_LEDGER_TABLE} WHERE url = %s",
                ("http://example.com/discovered",),
            )
            (ledger_count,) = cur.fetchone()
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            ledger, "http://example.com/discovered"
        )
        assert ledger_count == 1
        assert exploration_count == 0
        assert backlog_count == 0
        assert recrawl_count == 0

    def test_admit_urls_assigns_scheduler_membership(self, ledger):
        ledger.discover(CrawlTask(url="http://example.com/discovered"))

        scheduled = ledger.admit_urls(
            ["http://example.com/discovered"],
            queue_class=QUEUE_EXPLORATION,
        )

        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            ledger, "http://example.com/discovered"
        )
        assert scheduled == 1
        assert exploration_count == 1
        assert backlog_count == 0
        assert recrawl_count == 0

    def test_admit_urls_can_schedule_unchanged_ledger_row(self, ledger):
        ledger.discover(CrawlTask(url="http://example.com/discovered"))
        assert ledger.place(CrawlTask(url="http://example.com/discovered")) is False

        scheduled = ledger.admit_urls(["http://example.com/discovered"])

        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            ledger, "http://example.com/discovered"
        )
        assert scheduled == 1
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_admit_urls_can_target_refresh_intent_surface(self, ledger):
        ledger.discover(CrawlTask(url="http://example.com/discovered"))

        scheduled = ledger.admit_urls(
            ["http://example.com/discovered"],
            runnable_surface=RUNNABLE_SURFACE_REFRESH,
            intent=INTENT_REFRESH,
        )

        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            ledger, "http://example.com/discovered"
        )
        assert scheduled == 1
        assert exploration_count == 0
        assert backlog_count == 0
        assert recrawl_count == 1

    def test_admit_discovered_tasks_uses_task_queue_classes(self, ledger):
        ledger.discover_many(
            [
                CrawlTask(url="http://example.com/explore", queue_class=QUEUE_EXPLORATION),
                CrawlTask(url="http://example.com/again", queue_class=QUEUE_RECRAWL),
            ]
        )

        admitted = ledger.admit_discovered_tasks(
            [
                CrawlTask(url="http://example.com/explore", queue_class=QUEUE_EXPLORATION),
                CrawlTask(url="http://example.com/again", queue_class=QUEUE_RECRAWL),
            ]
        )

        explore_counts = self._queue_counts(ledger, "http://example.com/explore")
        recrawl_counts = self._queue_counts(ledger, "http://example.com/again")
        assert admitted == 2
        assert explore_counts == (1, 0, 0)
        assert recrawl_counts == (0, 0, 1)

    def test_admit_discovered_tasks_can_use_intent_and_surface(self, ledger):
        ledger.discover_many(
            [
                CrawlTask(
                    url="http://example.com/explore",
                    runnable_surface=RUNNABLE_SURFACE_DEFERRED,
                    intent=INTENT_EXPLORE,
                ),
                CrawlTask(
                    url="http://example.com/again",
                    runnable_surface=RUNNABLE_SURFACE_REFRESH,
                    intent=INTENT_REFRESH,
                ),
            ]
        )

        admitted = ledger.admit_discovered_tasks(
            [
                CrawlTask(
                    url="http://example.com/explore",
                    runnable_surface=RUNNABLE_SURFACE_DEFERRED,
                    intent=INTENT_EXPLORE,
                ),
                CrawlTask(
                    url="http://example.com/again",
                    runnable_surface=RUNNABLE_SURFACE_REFRESH,
                    intent=INTENT_REFRESH,
                ),
            ]
        )

        explore_counts = self._queue_counts(ledger, "http://example.com/explore")
        recrawl_counts = self._queue_counts(ledger, "http://example.com/again")
        assert admitted == 2
        assert explore_counts == (0, 1, 0)
        assert recrawl_counts == (0, 0, 1)

    def test_prepare_tasks_restores_surface_and_intent_from_queue_class(self, ledger):
        prepared = ledger._prepare_tasks(
            [
                CrawlTask(url="http://example.com/seed", queue_class=QUEUE_EXPLORATION),
                CrawlTask(url="http://example.com/seed", queue_class=QUEUE_BACKLOG),
                CrawlTask(url="http://example.com/refresh", queue_class=QUEUE_RECRAWL),
                CrawlTask(url="http://example.com/refresh", queue_class=QUEUE_RECRAWL),
            ]
        )

        by_url = {task.url: task for task in prepared}
        assert by_url["http://example.com/seed"].queue_class is None
        assert by_url["http://example.com/seed"].runnable_surface == RUNNABLE_SURFACE_FRONTLINE
        assert by_url["http://example.com/seed"].intent == INTENT_EXPLORE
        assert by_url["http://example.com/refresh"].queue_class is None
        assert by_url["http://example.com/refresh"].runnable_surface == RUNNABLE_SURFACE_REFRESH
        assert by_url["http://example.com/refresh"].intent == INTENT_REFRESH

    def test_prepare_tasks_prefers_more_urgent_surface_over_queue_class(self, ledger):
        prepared = ledger._prepare_tasks(
            [
                CrawlTask(
                    url="http://example.com/page",
                    runnable_surface=RUNNABLE_SURFACE_DEFERRED,
                    intent=INTENT_EXPLORE,
                ),
                CrawlTask(
                    url="http://example.com/page",
                    runnable_surface=RUNNABLE_SURFACE_FRONTLINE,
                    intent=INTENT_EXPLORE,
                ),
            ]
        )

        assert len(prepared) == 1
        task = prepared[0]
        assert task.queue_class is None
        assert task.runnable_surface == RUNNABLE_SURFACE_FRONTLINE
        assert task.intent == INTENT_EXPLORE

    def test_admit_discovered_urls_assigns_backlog_membership(self, ledger):
        ledger.discover_many(
            [
                CrawlTask(url="http://example.com/discovered-1"),
                CrawlTask(url="http://example.com/discovered-2"),
            ]
        )

        admitted = ledger.admit_discovered_urls(1)

        counts_1 = self._queue_counts(ledger, "http://example.com/discovered-1")
        counts_2 = self._queue_counts(ledger, "http://example.com/discovered-2")
        assert admitted == 1
        assert counts_1 in ((0, 1, 0), (0, 0, 0))
        assert counts_2 in ((0, 1, 0), (0, 0, 0))
        assert counts_1 != counts_2

    def test_admit_discovered_urls_skips_done_rows(self, ledger):
        ledger.discover(CrawlTask(url="http://example.com/done"))
        ledger.admit_urls(["http://example.com/done"])
        ledger.mark_done("http://example.com/done")

        admitted = ledger.admit_discovered_urls(10)

        assert admitted == 0
        assert self._queue_counts(ledger, "http://example.com/done") == (0, 0, 0)

    def test_add_preserves_first_seen_source_url_when_priority_improves(self, ledger):
        assert ledger.place(
            CrawlTask(
                url="http://example.com/page",
                priority=0.8,
                source_url="http://other.com",
            )
        )

        assert ledger.place(
            CrawlTask(
                url="http://example.com/page",
                priority=1.25,
                source_url="http://example.com/",
            )
        )

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"SELECT priority, source_url FROM {URL_LEDGER_TABLE} WHERE url = %s",
                ("http://example.com/page",),
            )
            priority, source_url = cur.fetchone()

        assert priority == 1.25
        assert source_url == "http://other.com"

    def test_add_classifies_shallow_urls_as_backlog(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))

        exploration_count, backlog_count, recrawl_count = self._queue_counts(ledger, "http://example.com/")
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_add_classifies_same_host_urls_as_backlog_through_depth_three(self, ledger):
        ledger.place(CrawlTask(url="http://example.com/guide"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            ledger, "http://example.com/guide"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_add_classifies_same_host_urls_as_backlog_from_depth_four(self, ledger):
        ledger.place(CrawlTask(url="http://example.com/guide"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            ledger, "http://example.com/guide"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_add_classifies_seed_host_urls_as_backlog_through_depth_two(self, ledger):
        ledger.place(CrawlTask(url="http://docs.example.com/guide"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            ledger, "http://docs.example.com/guide"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_add_classifies_seed_host_urls_as_backlog_through_depth_three(self, ledger):
        ledger.place(CrawlTask(url="http://docs.example.com/guide"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            ledger, "http://docs.example.com/guide"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_add_defaults_implicit_urls_to_backlog(self, ledger):
        ledger.place(CrawlTask(url="http://example.com/seed"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            ledger, "http://example.com/seed"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_upsert_seeds_keeps_seed_urls_in_exploration(self, ledger):
        ledger.upsert_seeds(["http://example.com/seed"])

        with ledger._conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {URL_LEDGER_TABLE} WHERE url = %s", ("http://example.com/seed",))
            (ledger_count,) = cur.fetchone()
            cur.execute(
                f"SELECT COUNT(*) FROM {QUEUE_TABLE_BY_CLASS[QUEUE_EXPLORATION]} WHERE url = %s",
                ("http://example.com/seed",),
            )
            (queue_count,) = cur.fetchone()

        assert ledger_count == 1
        assert queue_count == 1


    def test_add_classifies_known_domains_as_backlog_even_when_shallow(self, ledger):
        for i in range(8):
            ledger.place(CrawlTask(url=f"http://example.com/known-{i}"))

        ledger.place(CrawlTask(url="http://example.com/new-branch"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            ledger, "http://example.com/new-branch"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0


    def test_add_classifies_external_deep_urls_as_backlog(self, ledger):
        ledger.place(CrawlTask(url="http://external.example.com/deep"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            ledger, "http://external.example.com/deep"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_add_classifies_deep_urls_as_backlog(self, ledger):
        ledger.place(CrawlTask(url="http://example.com/deep"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            ledger, "http://example.com/deep"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_lease_next_returns_task(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))
        result = ledger.lease_next()
        assert result is not None
        assert "example.com" in result.url
        assert result.lease_token is not None
        assert result.lease_expires_at is not None
        assert result.next_fetch_at > 0

    def test_lease_next_returns_none_when_empty(self, ledger):
        assert ledger.lease_next() is None

    def test_lease_next_priority_order(self, ledger):
        ledger.place(CrawlTask(url="http://example.com/low", priority=0.5))
        ledger.place(CrawlTask(url="http://example.com/high", priority=1.5))
        result = ledger.lease_next()
        assert "high" in result.url

    def test_lease_next_fifo_same_priority(self, ledger):
        ledger.place(CrawlTask(url="http://example.com/first", added_at=1000))
        ledger.place(CrawlTask(url="http://example.com/second", added_at=2000))
        result = ledger.lease_next()
        assert "first" in result.url

    def test_lease_next_prefers_less_congested_host_when_priority_matches(self, ledger):
        ledger.place(CrawlTask(url="http://a.com/1", priority=1.0, added_at=1000))
        ledger.place(CrawlTask(url="http://a.com/2", priority=1.0, added_at=1001))
        ledger.place(CrawlTask(url="http://a.com/3", priority=1.0, added_at=1002))
        ledger.place(CrawlTask(url="http://b.com/1", priority=1.0, added_at=2000))

        result = ledger.lease_next()

        assert result is not None
        assert result.url == "http://b.com/1"

    def test_lease_next_prefers_lower_latency_host_when_priority_matches(self, ledger):
        ledger.place(CrawlTask(url="http://slow.com/1", priority=1.0, added_at=1000))
        ledger.place(CrawlTask(url="http://fast.com/1", priority=1.0, added_at=900))

        self.domain_store.record_success("slow.com", now=time.time(), request_latency_ms=900.0)
        self.domain_store.record_success("fast.com", now=time.time(), request_latency_ms=80.0)

        result = ledger.lease_next()

        assert result is not None
        assert result.url == "http://fast.com/1"

    def test_lease_next_can_prefer_breadth_over_depth(self, ledger):
        for i in range(5):
            ledger.place(CrawlTask(url=f"http://a.com/{i}", priority=1.0, added_at=1000 + i))
        ledger.place(CrawlTask(url="http://b.com/1", priority=0.8, added_at=2000))

        result = ledger.lease_next(lease_strategy=LEASE_STRATEGY_HOST_FIRST)

        assert result is not None
        assert result.url == "http://b.com/1"

    def test_runnable_host_heads_returns_one_head_per_host(self, ledger):
        ledger.place(CrawlTask(url="http://a.com/1", priority=1.0, added_at=1000))
        ledger.place(CrawlTask(url="http://a.com/2", priority=1.0, added_at=1001))
        ledger.place(CrawlTask(url="http://b.com/1", priority=0.8, added_at=2000))

        heads = ledger.runnable_host_heads(runnable_surface=RUNNABLE_SURFACE_FRONTLINE)

        assert [(head.host_key, head.url) for head in heads] == [
            ("a.com", "http://a.com/1"),
            ("b.com", "http://b.com/1"),
        ]

    def test_runnable_host_heads_can_read_normal_runnable_surface(self, ledger):
        ledger.place(CrawlTask(url="http://a.com/1", priority=1.0, added_at=1000, queue_class=QUEUE_EXPLORATION))
        ledger.place(CrawlTask(url="http://b.com/1", priority=0.8, added_at=2000, queue_class=QUEUE_BACKLOG))

        heads = ledger.runnable_host_heads(runnable_surface=RUNNABLE_SURFACE_NORMAL)

        assert [(head.host_key, head.url) for head in heads] == [
            ("a.com", "http://a.com/1"),
            ("b.com", "http://b.com/1"),
        ]

    def test_runnable_host_heads_orders_hosts_by_host_first_priority(self, ledger):
        ledger.place(CrawlTask(url="http://slow.com/1", priority=1.0, added_at=1000))
        ledger.place(CrawlTask(url="http://fast.com/1", priority=1.0, added_at=1200))

        self.domain_store.record_success("slow.com", now=time.time(), request_latency_ms=900.0)
        self.domain_store.record_success("fast.com", now=time.time(), request_latency_ms=80.0)

        heads = ledger.runnable_host_heads(runnable_surface=RUNNABLE_SURFACE_FRONTLINE)

        assert [head.host_key for head in heads] == ["fast.com", "slow.com"]

    def test_select_runnable_host_head_uses_same_host_first_order(self, ledger):
        ledger.place(CrawlTask(url="http://slow.com/1", priority=1.0, added_at=1000))
        ledger.place(CrawlTask(url="http://fast.com/1", priority=1.0, added_at=1200))

        self.domain_store.record_success("slow.com", now=time.time(), request_latency_ms=900.0)
        self.domain_store.record_success("fast.com", now=time.time(), request_latency_ms=80.0)

        head = ledger.select_runnable_host_head(runnable_surface=RUNNABLE_SURFACE_FRONTLINE)

        assert head is not None
        assert head.host_key == "fast.com"
        assert head.url == "http://fast.com/1"

    def test_select_runnable_host_head_can_use_normal_runnable_surface(self, ledger):
        ledger.place(CrawlTask(url="http://a.com/1", priority=1.0, added_at=1000, queue_class=QUEUE_EXPLORATION))
        ledger.place(CrawlTask(url="http://b.com/1", priority=0.8, added_at=2000, queue_class=QUEUE_BACKLOG))

        head = ledger.select_runnable_host_head(runnable_surface=RUNNABLE_SURFACE_NORMAL)

        assert head is not None
        assert head.host_key == "a.com"
        assert head.url == "http://a.com/1"

    def test_lease_next_marks_leased(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))
        ledger.lease_next()
        assert ledger.lease_next() is None

        with ledger._conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {LEASE_TABLE} WHERE url = %s", ("http://example.com/",))
            (active_count,) = cur.fetchone()

        assert active_count == 1

    def test_lease_batch(self, ledger):
        for i in range(5):
            ledger.place(CrawlTask(url=f"http://example.com/{i}"))
        batch = ledger.lease_batch(count=3)
        assert len(batch) == 3

    def test_lease_batch_uses_host_first_breadth_order_for_queue_tables(self, ledger):
        ledger.place(CrawlTask(url="http://a.com/docs/python/1", priority=1.0, added_at=1000))
        ledger.place(CrawlTask(url="http://a.com/docs/python/2", priority=1.0, added_at=1001))
        ledger.place(CrawlTask(url="http://a.com/docs/rust/1", priority=1.0, added_at=2000))

        batch = ledger.lease_batch(
            count=2,
            lease_strategy=LEASE_STRATEGY_HOST_FIRST,
            runnable_surface=RUNNABLE_SURFACE_FRONTLINE,
        )

        assert {task.url for task in batch} == {
            "http://a.com/docs/python/1",
            "http://a.com/docs/python/2",
        }

    def test_lease_next_uses_host_first_breadth_without_branch_rotation(self, ledger):
        ledger.place(CrawlTask(url="http://a.com/docs/python/1", priority=1.0, added_at=1000))
        ledger.place(CrawlTask(url="http://a.com/docs/python/2", priority=1.0, added_at=1001))
        ledger.place(CrawlTask(url="http://a.com/docs/rust/1", priority=1.0, added_at=2000))
        ledger.place(CrawlTask(url="http://b.com/1", priority=1.5, added_at=5000))

        first = ledger.lease_next(
            lease_strategy=LEASE_STRATEGY_HOST_FIRST,
            runnable_surface=RUNNABLE_SURFACE_FRONTLINE,
        )
        assert first is not None
        assert first.url == "http://a.com/docs/python/1"
        ledger.mark_done(first.url, lease_token=first.lease_token)

        second = ledger.lease_next(
            lease_strategy=LEASE_STRATEGY_HOST_FIRST,
            runnable_surface=RUNNABLE_SURFACE_FRONTLINE,
        )
        assert second is not None
        assert second.url == "http://b.com/1"

    def test_mark_done(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))
        result = ledger.lease_next()
        ledger.mark_done(result.url, lease_token=result.lease_token)
        assert ledger.stats().get("done", 0) == 1
        with ledger._conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {LEASE_TABLE} WHERE url = %s", (result.url,))
            (active_count,) = cur.fetchone()
            cur.execute(
                f"SELECT last_success_at, fail_streak, last_error, terminal_reason, terminalized_at FROM {URL_LEDGER_TABLE} WHERE url = %s",
                (result.url,),
            )
            last_success_at, fail_streak, last_error, terminal_reason, terminalized_at = cur.fetchone()
        assert active_count == 0
        assert last_success_at is not None
        assert fail_streak == 0
        assert last_error is None
        assert terminal_reason is None
        assert terminalized_at is None

    def test_lease_state_lives_only_in_active_lease_table(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))

        result = ledger.lease_next()

        with ledger._conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'url_ledger'
                  AND column_name IN ('lease_token', 'lease_expires_at')
                ORDER BY column_name
                """
            )
            lease_columns = [column_name for (column_name,) in cur.fetchall()]
            cur.execute(
                f"SELECT lease_token, lease_expires_at FROM {LEASE_TABLE} WHERE url = %s",
                (result.url,),
            )
            active_lease_token, active_lease_expires_at = cur.fetchone()

        assert lease_columns == []
        assert active_lease_token == result.lease_token
        assert active_lease_expires_at == result.lease_expires_at

    def test_mark_done_uses_active_lease_table_for_token_validation(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))
        result = ledger.lease_next()

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"UPDATE {LEASE_TABLE} SET lease_token = %s WHERE url = %s",
                ("tampered", result.url),
            )
        ledger._conn.commit()

        assert ledger.mark_done(result.url, lease_token=result.lease_token) is False

    def test_recover_leased_uses_active_lease_table(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))
        result = ledger.lease_next()

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"UPDATE {LEASE_TABLE} SET lease_expires_at = %s WHERE url = %s",
                (time.time() + 3600, result.url),
            )
        ledger._conn.commit()

        assert ledger.recover_leased(expired_only=False) == 1
        assert ledger.pending_count() == 1

    def test_mark_failed(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))
        result = ledger.lease_next()
        ledger.mark_failed(result.url, lease_token=result.lease_token)
        assert ledger.stats().get("failed", 0) == 1

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"SELECT terminal_reason, terminalized_at FROM {URL_LEDGER_TABLE} WHERE url = %s",
                (result.url,),
            )
            terminal_reason, terminalized_at = cur.fetchone()

        assert terminal_reason == "failed"
        assert terminalized_at is not None

    def test_requeue_failed(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))
        result = ledger.lease_next()
        ledger.mark_failed(result.url, lease_token=result.lease_token)
        assert ledger.requeue_failed() == 1
        assert ledger.pending_count() == 1

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"SELECT terminal_reason, terminalized_at FROM {URL_LEDGER_TABLE} WHERE url = %s",
                (result.url,),
            )
            terminal_reason, terminalized_at = cur.fetchone()

        assert terminal_reason is None
        assert terminalized_at is None

    def test_recover_leased(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))
        ledger.lease_next()
        assert ledger.recover_leased(expired_only=False) == 1
        assert ledger.pending_count() == 1

    def test_upsert_seeds_requeues_done_url(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))
        result = ledger.lease_next()
        ledger.mark_done(result.url, lease_token=result.lease_token)

        ledger.upsert_seeds(["http://example.com"])

        assert ledger.pending_count() == 1

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {QUEUE_TABLE_BY_CLASS[QUEUE_EXPLORATION]} WHERE url = %s",
                ("http://example.com/",),
            )
            (queue_count,) = cur.fetchone()

        assert queue_count == 1

    def test_stats(self, ledger):
        ledger.place(CrawlTask(url="http://example.com/1"))
        ledger.place(CrawlTask(url="http://example.com/2"))
        stats = ledger.stats()
        assert stats["total"] == 2
        assert stats.get("pending", 0) == 2

    def test_is_seen(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))
        assert ledger.is_seen("http://example.com") is True
        assert ledger.is_seen("http://example.com#section") is True
        assert ledger.is_seen("http://other.com") is False

    def test_pending_count(self, ledger):
        ledger.place(CrawlTask(url="http://example.com/1"))
        ledger.place(CrawlTask(url="http://example.com/2"))
        assert ledger.pending_count() == 2
        ledger.lease_next()
        assert ledger.pending_count() == 1

    def test_pending_membership_comes_from_queue_tables(self, ledger):
        ledger.place(CrawlTask(url="http://example.com/1"))

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"""UPDATE {URL_LEDGER_TABLE}
                    SET terminal_reason = %s,
                        terminalized_at = %s
                    WHERE url = %s""",
                ("done", time.time(), "http://example.com/1"),
            )
        ledger._conn.commit()

        assert ledger.pending_count() == 1

        leased = ledger.lease_next()
        assert leased is not None
        assert leased.url == "http://example.com/1"

    def test_pending_domain_count(self, ledger):
        ledger.place(CrawlTask(url="http://example.com/1"))
        ledger.place(CrawlTask(url="http://example.com/2"))
        ledger.place(CrawlTask(url="http://other.com/1"))

        assert ledger.pending_domain_count() == 2
        assert ledger.pending_domain_count(runnable_surface=RUNNABLE_SURFACE_FRONTLINE) == 2

    def test_ready_domain_count_ignores_scheduled_and_blocked_hosts(self, ledger):
        now = time.time()
        ledger.place(CrawlTask(url="http://a.com/docs/1", next_fetch_at=now))
        ledger.place(CrawlTask(url="http://a.com/blog/1", next_fetch_at=now))
        ledger.place(CrawlTask(url="http://b.com/future", next_fetch_at=now + 30.0))
        ledger.place(CrawlTask(url="http://c.com/backoff", next_fetch_at=now))
        self.domain_store.record_failure("c.com", backoff_seconds=20.0, now=now)

        assert ledger.runnable_domain_count(now=now) == 1

    def test_ready_count_ignores_future_next_fetch(self, ledger):
        now = time.time()
        ledger.place(
            CrawlTask(
                url="http://example.com/future",
                next_fetch_at=now + 60,
            )
        )
        ledger.place(CrawlTask(url="http://example.com/ready", next_fetch_at=now))

        assert ledger.pending_count() == 2
        assert ledger.runnable_count(now=now) == 1
        assert ledger.runnable_count(now=now) == 1
        assert ledger.scheduled_count(now=now) == 1
        assert ledger.next_runnable_delay(now=now) == pytest.approx(0.0, abs=1e-6)

    def test_ready_count_respects_domain_backoff(self, ledger):
        now = time.time()
        ledger.place(CrawlTask(url="http://a.com/1", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)

        assert ledger.pending_count() == 1
        assert ledger.runnable_count(now=now) == 0
        assert ledger.runnable_count(now=now) == 0
        assert ledger.scheduled_count(now=now) == 0
        assert ledger.next_runnable_delay(now=now) == pytest.approx(30.0, abs=1e-3)

    def test_ready_count_can_filter_queue_classes(self, ledger):
        now = time.time()
        ledger.place(CrawlTask(url="http://a.com/explore", next_fetch_at=now))
        ledger.place(CrawlTask(url="http://a.com/backlog", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=20.0, now=now)
        ledger.place(CrawlTask(url="http://b.com/explore", next_fetch_at=now))

        assert ledger.runnable_count(now=now, runnable_surface=RUNNABLE_SURFACE_FRONTLINE) == 1
        assert ledger.runnable_count(now=now, runnable_surface=RUNNABLE_SURFACE_DEFERRED) == 0

    def test_readiness_summarizes_pending_and_ready(self, ledger):
        now = time.time()
        ledger.place(CrawlTask(url="http://a.com/1", next_fetch_at=now))
        ledger.place(CrawlTask(url="http://b.com/1", next_fetch_at=now + 30))
        self.domain_store.record_failure("a.com", backoff_seconds=20.0, now=now)

        readiness = ledger.readiness(now=now)

        assert readiness.pending == 2
        assert readiness.runnable == 0
        assert readiness.runnable == 0
        assert readiness.scheduled == 1
        assert readiness.runnable_domains == 0
        assert readiness.next_runnable_delay == pytest.approx(20.0, abs=1e-3)
        assert readiness.blocked == {
            "next_fetch_at": 1,
            "domain_next_request": 0,
            "host_backoff": 1,
            "retry_quarantine": 0,
        }
        assert readiness.state_counts == {
            "runnable": 0,
            "scheduled": 1,
            "blocked_domain_next_request": 0,
            "blocked_host_backoff": 1,
            "retry_quarantine": 0,
        }

    def test_rebalance_blocked_domain_backoff_quarantines_urls(self, ledger):
        now = time.time()
        ledger.place(CrawlTask(url="http://a.com/blocked", next_fetch_at=now))
        ledger.place(CrawlTask(url="http://b.com/ready", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)

        quarantined, restored = ledger.rebalance_blocked_domain_backoff(now=now)

        assert quarantined == 1
        assert restored == 0
        assert ledger.pending_count() == 1
        assert ledger.blocked_domain_backoff_count() == 1

        readiness = ledger.readiness(now=now)
        assert readiness.pending == 2
        assert readiness.runnable == 1
        assert readiness.runnable_domains == 1
        assert readiness.state_counts == {
            "runnable": 1,
            "scheduled": 0,
            "blocked_domain_next_request": 0,
            "blocked_host_backoff": 0,
            "retry_quarantine": 1,
        }

    def test_promote_blocked_domain_backoff_restores_small_cooldown_subset(self, ledger):
        now = time.time()
        ledger.place(CrawlTask(url="http://a.com/blocked-1", next_fetch_at=now))
        ledger.place(CrawlTask(url="http://a.com/blocked-2", next_fetch_at=now))
        ledger.place(CrawlTask(url="http://b.com/blocked", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)
        self.domain_store.record_failure("b.com", backoff_seconds=30.0, now=now)
        ledger.rebalance_blocked_domain_backoff(now=now)

        self.domain_store.record_success("a.com", now=now + 31.0)
        self.domain_store.record_success("b.com", now=now + 31.0)

        promoted = ledger.promote_blocked_domain_backoff(2, per_domain=1, now=now + 31.0)

        assert promoted == 2
        assert ledger.pending_count() == 2
        assert ledger.pending_count(runnable_surface=RUNNABLE_SURFACE_DEFERRED) == 2
        assert ledger.blocked_domain_backoff_count() == 1

        readiness = ledger.readiness(now=now + 31.0)
        assert readiness.runnable == 2
        assert readiness.runnable_domains == 2
        assert readiness.state_counts == {
            "runnable": 2,
            "scheduled": 0,
            "blocked_domain_next_request": 0,
            "blocked_host_backoff": 0,
            "retry_quarantine": 1,
        }

    def test_promote_blocked_domain_backoff_skips_high_failure_domains(self, ledger):
        now = time.time()
        ledger.place(CrawlTask(url="http://a.com/blocked", next_fetch_at=now))
        ledger.place(CrawlTask(url="http://b.com/blocked", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now + 1.0)
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now + 2.0)
        self.domain_store.record_failure("b.com", backoff_seconds=30.0, now=now)
        ledger.rebalance_blocked_domain_backoff(now=now + 2.0)

        promoted = ledger.promote_blocked_domain_backoff(
            2,
            per_domain=1,
            max_consecutive_failures=1,
            now=now + 40.0,
        )

        assert promoted == 1
        assert ledger.pending_count() == 1
        assert ledger.blocked_domain_backoff_count() == 1

        leased = ledger.lease_next(now=now + 40.0)
        assert leased is not None
        assert leased.url == "http://b.com/blocked"

    def test_promote_blocked_domain_backoff_returns_ready_urls_to_deferred_surface_without_domain_bias(self, ledger):
        now = time.time()
        ledger.place(CrawlTask(url="http://a.com/blocked", next_fetch_at=now))
        ledger.place(CrawlTask(url="http://b.com/blocked", next_fetch_at=now + 5.0))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)
        self.domain_store.record_failure("b.com", backoff_seconds=30.0, now=now)
        ledger.rebalance_blocked_domain_backoff(now=now)

        ledger.place(CrawlTask(url="http://a.com/ready-2", next_fetch_at=now + 31.0))
        self.domain_store.record_success("a.com", now=now + 31.0)
        self.domain_store.record_success("b.com", now=now + 31.0)

        promoted = ledger.promote_blocked_domain_backoff(1, per_domain=1, now=now + 31.0)

        assert promoted == 1
        assert ledger.pending_count(runnable_surface=RUNNABLE_SURFACE_DEFERRED) == 2
        leased = ledger.lease_next(now=now + 31.0)
        assert leased is not None
        assert leased.url == "http://a.com/ready-2"
        assert ledger.blocked_domain_backoff_count() == 1

        leased = ledger.lease_next(now=now + 31.0, runnable_surface=RUNNABLE_SURFACE_DEFERRED)
        assert leased is not None
        assert leased.url == "http://a.com/blocked"

    def test_retire_blocked_domain_backoff_marks_old_high_failure_urls_failed(self, ledger):
        now = time.time()
        ledger.place(CrawlTask(url="http://a.com/blocked", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now + 1.0)
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now + 2.0)
        ledger.rebalance_blocked_domain_backoff(now=now + 2.0)

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"UPDATE {BLOCKED_DOMAIN_BACKOFF_TABLE} SET quarantined_at = %s WHERE url = %s",
                (now - 100.0, "http://a.com/blocked"),
            )
        ledger._conn.commit()

        retired = ledger.retire_blocked_domain_backoff(
            min_consecutive_failures=3,
            min_quarantine_seconds=60.0,
            now=now,
        )

        assert retired == 1
        assert ledger.pending_count() == 0
        assert ledger.blocked_domain_backoff_count() == 0

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"SELECT last_error, terminal_reason, terminalized_at FROM {URL_LEDGER_TABLE} WHERE url = %s",
                ("http://a.com/blocked",),
            )
            last_error, terminal_reason, terminalized_at = cur.fetchone()

        assert last_error == "retry_quarantine_retired"
        assert terminal_reason == "retry_quarantine_retired"
        assert terminalized_at is not None

    def test_restore_recovered_blocked_domain_backoff_restores_healthy_domains(self, ledger):
        now = time.time()
        ledger.place(CrawlTask(url="http://a.com/blocked-1", next_fetch_at=now))
        ledger.place(CrawlTask(url="http://a.com/blocked-2", next_fetch_at=now))
        ledger.place(CrawlTask(url="http://b.com/blocked", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)
        self.domain_store.record_failure("b.com", backoff_seconds=30.0, now=now)
        ledger.rebalance_blocked_domain_backoff(now=now)

        self.domain_store.record_success("a.com", now=now + 31.0)
        restored = ledger.restore_recovered_blocked_domain_backoff(
            limit=10,
            per_domain=10,
            now=now + 31.0,
        )

        assert restored == 2
        assert ledger.pending_count() == 2
        assert ledger.pending_count(runnable_surface=RUNNABLE_SURFACE_DEFERRED) == 2
        assert ledger.blocked_domain_backoff_count() == 1

        leased = ledger.lease_batch(count=2, runnable_surface=RUNNABLE_SURFACE_DEFERRED, now=now + 31.0)
        assert {task.url for task in leased} == {"http://a.com/blocked-1", "http://a.com/blocked-2"}

    def test_readiness_filters_blocked_queue_by_queue_class(self, ledger):
        now = time.time()
        ledger.place(CrawlTask(url="http://a.com/explore", next_fetch_at=now))
        ledger.place(CrawlTask(url="http://a.com/backlog", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)
        ledger.rebalance_blocked_domain_backoff(now=now)

        exploration = ledger.readiness(now=now, runnable_surface=RUNNABLE_SURFACE_FRONTLINE)
        backlog = ledger.readiness(now=now, runnable_surface=RUNNABLE_SURFACE_DEFERRED)

        assert exploration.pending == 1
        assert exploration.state_counts["retry_quarantine"] == 1
        assert backlog.pending == 1
        assert backlog.state_counts["retry_quarantine"] == 1

    def test_domain_filter(self, ledger):
        ledger.place(CrawlTask(url="http://a.com/page"))
        ledger.place(CrawlTask(url="http://b.com/page"))
        result = ledger.lease_next(domain="a.com")
        assert result is not None
        assert "a.com" in result.url

    def test_lease_next_excludes_active_domains(self, ledger):
        ledger.place(CrawlTask(url="http://a.com/1", priority=3.0))
        ledger.place(CrawlTask(url="http://a.com/2", priority=2.0))
        ledger.place(CrawlTask(url="http://b.com/1", priority=1.0))

        result = ledger.lease_next(exclude_domains=["a.com"])

        assert result is not None
        assert "b.com" in result.url

    def test_lease_next_filters_queue_classes(self, ledger):
        ledger.place(CrawlTask(url="http://a.com/explore"))
        ledger.place(CrawlTask(url="http://a.com/backlog"))

        result = ledger.lease_next(runnable_surface=RUNNABLE_SURFACE_DEFERRED)

        assert result is not None
        assert result.url == "http://a.com/backlog"

    def test_lease_next_skips_host_under_backoff(self, ledger):
        self.domain_store.record_failure("a.com", backoff_seconds=60.0, now=time.time())
        ledger.place(CrawlTask(url="http://a.com/page", priority=2.0))
        ledger.place(CrawlTask(url="http://b.com/page", priority=1.0))

        result = ledger.lease_next()

        assert result is not None
        assert "b.com" in result.url

    def test_lease_next_recovers_expired_lease(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))
        first = ledger.lease_next(lease_seconds=0.01)
        assert first is not None

        time.sleep(0.02)

        second = ledger.lease_next()
        assert second is not None
        assert second.url == first.url
        assert second.lease_token != first.lease_token

    def test_retryable_failure_delays_next_fetch(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))
        result = ledger.lease_next()
        assert result is not None

        ledger.mark_failed(
            result.url,
            retryable=True,
            error="timeout",
            backoff_seconds=60,
            lease_token=result.lease_token,
        )

        assert ledger.lease_next() is None

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"SELECT fail_streak, last_error, next_fetch_at, terminal_reason, terminalized_at FROM {URL_LEDGER_TABLE} WHERE url = %s",
                (result.url,),
            )
            fail_streak, last_error, next_fetch_at, terminal_reason, terminalized_at = cur.fetchone()

        assert fail_streak == 1
        assert last_error == "timeout"
        assert next_fetch_at > time.time()
        assert terminal_reason is None
        assert terminalized_at is None

    def test_retryable_failure_demotes_priority(self, ledger):
        ledger.place(CrawlTask(url="http://example.com/retry", priority=1.25))
        result = ledger.lease_next()
        assert result is not None

        ledger.mark_failed(
            result.url,
            retryable=True,
            error="timeout",
            backoff_seconds=0,
            lease_token=result.lease_token,
        )

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"SELECT fail_streak, priority FROM {URL_LEDGER_TABLE} WHERE url = %s",
                (result.url,),
            )
            fail_streak, priority = cur.fetchone()

        assert fail_streak == 1
        assert priority == 0.75

    def test_compute_retry_backoff_uses_configured_values(self, ledger):
        configured = UrlLedger(ledger._conn, retry_backoff_seconds=5.0, max_retry_backoff_seconds=12.0)

        assert configured._compute_retry_backoff(1) == 5.0
        assert configured._compute_retry_backoff(2) == 10.0
        assert configured._compute_retry_backoff(3) == 12.0

    def test_lease_next_prefers_fresh_url_over_retried_url(self, ledger):
        ledger.place(CrawlTask(url="http://retry.com/page", priority=1.25))
        first = ledger.lease_next(domain="retry.com")
        assert first is not None

        ledger.mark_failed(
            first.url,
            retryable=True,
            error="timeout",
            backoff_seconds=0,
            lease_token=first.lease_token,
        )

        ledger.place(CrawlTask(url="http://fresh.com/page", priority=1.0))

        next_task = ledger.lease_next()

        assert next_task is not None
        assert next_task.url == "http://fresh.com/page"

    def test_upsert_seeds_marks_seeds_as_exploration(self, ledger):
        ledger.upsert_seeds(["http://example.com"])

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {QUEUE_TABLE_BY_CLASS[QUEUE_EXPLORATION]} WHERE url = %s",
                ("http://example.com/",),
            )
            (queue_count,) = cur.fetchone()

        assert queue_count == 1

    def test_promote_deferred_host_heads_promotes_distinct_backlog_domains(self, ledger):
        ledger.place(CrawlTask(url="http://example.com/docs/a", queue_class=QUEUE_BACKLOG))
        ledger.place(CrawlTask(url="http://example.com/docs/b", queue_class=QUEUE_BACKLOG))
        ledger.place(CrawlTask(url="http://other.com/news/a", queue_class=QUEUE_BACKLOG))
        ledger.place(CrawlTask(url="http://third.com/start", queue_class=QUEUE_BACKLOG))

        promoted = ledger.promote_deferred_host_heads(target_pending=2, per_domain=1, candidate_limit=10)

        assert promoted == 2
        with ledger._conn.cursor() as cur:
            cur.execute(f"SELECT url FROM {QUEUE_TABLE_BY_CLASS[QUEUE_EXPLORATION]} ORDER BY url")
            promoted_urls = [url for (url,) in cur.fetchall()]

        assert promoted_urls == [
            "http://example.com/docs/a",
            "http://other.com/news/a",
        ]

    def test_promote_deferred_host_heads_uses_queue_membership_not_task_queue_class(self, ledger):
        ledger.place(CrawlTask(url="http://example.com/docs/a", queue_class=QUEUE_BACKLOG))
        ledger.place(CrawlTask(url="http://other.com/news/a", queue_class=QUEUE_BACKLOG))

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"""UPDATE {URL_LEDGER_TABLE}
                    SET terminal_reason = %s,
                        terminalized_at = %s
                    WHERE url IN (%s, %s)""",
                ("reclassified", time.time(), "http://example.com/docs/a", "http://other.com/news/a"),
            )
        ledger._conn.commit()

        promoted = ledger.promote_deferred_host_heads(target_pending=2, per_domain=1, candidate_limit=10)

        assert promoted == 2
        with ledger._conn.cursor() as cur:
            cur.execute(f"SELECT url FROM {QUEUE_TABLE_BY_CLASS[QUEUE_EXPLORATION]} ORDER BY url")
            promoted_urls = [url for (url,) in cur.fetchall()]

        assert promoted_urls == [
            "http://example.com/docs/a",
            "http://other.com/news/a",
        ]

    def test_recrawl_queue_class_can_be_leased_separately(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))
        leased = ledger.lease_next()
        assert leased is not None
        ledger.mark_done(leased.url, lease_token=leased.lease_token)

        requeued = ledger.requeue_refresh_urls([leased.url])
        assert requeued == 1

        recrawl = ledger.lease_next(runnable_surface=RUNNABLE_SURFACE_REFRESH)
        assert recrawl is not None
        assert recrawl.url == leased.url

    def test_lease_next_uses_queue_membership_not_task_queue_class(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))
        leased = ledger.lease_next()
        assert leased is not None
        ledger.mark_done(leased.url, lease_token=leased.lease_token)

        requeued = ledger.requeue_refresh_urls([leased.url])
        assert requeued == 1

        recrawl = ledger.lease_next(runnable_surface=RUNNABLE_SURFACE_REFRESH)
        assert recrawl is not None
        assert recrawl.url == leased.url

    def test_mark_done_resets_fail_streak(self, ledger):
        ledger.place(CrawlTask(url="http://example.com"))
        first = ledger.lease_next()
        assert first is not None

        ledger.mark_failed(
            first.url,
            retryable=True,
            error="timeout",
            backoff_seconds=0,
            lease_token=first.lease_token,
        )

        second = ledger.lease_next()
        assert second is not None

        ledger.mark_done(second.url, lease_token=second.lease_token)

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"SELECT fail_streak, last_success_at, last_error, terminal_reason, terminalized_at FROM {URL_LEDGER_TABLE} WHERE url = %s",
                (second.url,),
            )
            fail_streak, last_success_at, last_error, terminal_reason, terminalized_at = cur.fetchone()

        assert fail_streak == 0
        assert last_success_at is not None
        assert last_error is None
        assert terminal_reason is None
        assert terminalized_at is None

    def test_defer_overcrowded_deferred_surface_delays_excess_ready_urls(self, ledger):
        ledger.place(CrawlTask(url="http://a.com/1", priority=0.55, added_at=1000))
        ledger.place(CrawlTask(url="http://a.com/2", priority=0.55, added_at=1001))
        ledger.place(CrawlTask(url="http://a.com/3", priority=0.55, added_at=1002))

        delayed = ledger.defer_overcrowded_deferred_surface(
            keep_runnable_per_domain=1,
            defer_seconds=60.0,
        )

        assert delayed == 2

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"SELECT url, next_fetch_at FROM {URL_LEDGER_TABLE} WHERE domain = 'a.com' ORDER BY url ASC"
            )
            rows = cur.fetchall()

        ready = [url for url, next_fetch_at in rows if next_fetch_at <= time.time()]
        deferred = [url for url, next_fetch_at in rows if next_fetch_at > time.time()]

        assert ready == ["http://a.com/1"]
        assert deferred == ["http://a.com/2", "http://a.com/3"]

    def test_defer_overcrowded_deferred_surface_delays_excess_branch_urls(self, ledger):
        ledger.place(CrawlTask(url="http://a.com/docs/python/1", priority=0.55, added_at=1000))
        ledger.place(CrawlTask(url="http://a.com/docs/python/2", priority=0.55, added_at=1001))
        ledger.place(CrawlTask(url="http://a.com/docs/rust/1", priority=0.55, added_at=1002))

        delayed = ledger.defer_overcrowded_deferred_surface(
            keep_runnable_per_domain=10,
            keep_runnable_per_branch=1,
            defer_seconds=60.0,
        )

        assert delayed == 1

        with ledger._conn.cursor() as cur:
            cur.execute(
                f"SELECT url, next_fetch_at FROM {URL_LEDGER_TABLE} WHERE domain = 'a.com' ORDER BY url ASC"
            )
            rows = cur.fetchall()

        ready = [url for url, next_fetch_at in rows if next_fetch_at <= time.time()]
        deferred = [url for url, next_fetch_at in rows if next_fetch_at > time.time()]

        assert ready == ["http://a.com/docs/python/1", "http://a.com/docs/rust/1"]
        assert deferred == ["http://a.com/docs/python/2"]
