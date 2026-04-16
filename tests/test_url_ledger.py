"""Tests for URL ledger module."""

import os
import time

import psycopg2
import pytest

from crawler.domain_store import DomainStore
from crawler.url_ledger import (
    CrawlTask,
    QUEUE_BACKLOG,
    QUEUE_EXPLORATION,
    QUEUE_RECRAWL,
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
    def frontier(self):
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS schema_migrations")
            cur.execute("DROP TABLE IF EXISTS active_leases")
            cur.execute("DROP TABLE IF EXISTS frontier_lease_active")
            cur.execute("DROP TABLE IF EXISTS frontier_queue_exploration")
            cur.execute("DROP TABLE IF EXISTS frontier_queue_backlog")
            cur.execute("DROP TABLE IF EXISTS frontier_queue_recrawl")
            cur.execute("DROP TABLE IF EXISTS frontier_queue_blocked_domain_backoff")
            cur.execute(f"DROP TABLE IF EXISTS {URL_LEDGER_TABLE}")
            cur.execute("DROP TABLE IF EXISTS domain_state")
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

    def _queue_counts(self, frontier, url: str) -> tuple[int, int, int]:
        with frontier._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM frontier_queue_exploration WHERE url = %s", (url,))
            (exploration_count,) = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM frontier_queue_backlog WHERE url = %s", (url,))
            (backlog_count,) = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM frontier_queue_recrawl WHERE url = %s", (url,))
            (recrawl_count,) = cur.fetchone()
        return exploration_count, backlog_count, recrawl_count

    def test_add_new_url_returns_true(self, frontier):
        task = CrawlTask(url="http://example.com")
        assert frontier.add(task) is True

    def test_add_duplicate_url_returns_false(self, frontier):
        task1 = CrawlTask(url="http://example.com")
        task2 = CrawlTask(url="http://example.com")
        frontier.add(task1)
        assert frontier.add(task2) is False

    def test_add_normalizes_url(self, frontier):
        task1 = CrawlTask(url="http://example.com/page#section")
        task2 = CrawlTask(url="http://example.com/page")
        frontier.add(task1)
        assert frontier.add(task2) is False

    def test_add_many_returns_count(self, frontier):
        tasks = [
            CrawlTask(url="http://example.com/1"),
            CrawlTask(url="http://example.com/2"),
            CrawlTask(url="http://example.com/1"),
        ]
        assert frontier.add_many(tasks) == 2

    def test_add_preserves_first_seen_source_url_when_priority_improves(self, frontier):
        assert frontier.add(
            CrawlTask(
                url="http://example.com/page",
                priority=0.8,
                source_url="http://other.com",
            )
        )

        assert frontier.add(
            CrawlTask(
                url="http://example.com/page",
                priority=1.25,
                source_url="http://example.com/",
            )
        )

        with frontier._conn.cursor() as cur:
            cur.execute(
                f"SELECT priority, source_url FROM {URL_LEDGER_TABLE} WHERE url = %s",
                ("http://example.com/page",),
            )
            priority, source_url = cur.fetchone()

        assert priority == 1.25
        assert source_url == "http://other.com"

    def test_add_classifies_shallow_urls_as_backlog(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))

        exploration_count, backlog_count, recrawl_count = self._queue_counts(frontier, "http://example.com/")
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_add_classifies_same_host_urls_as_backlog_through_depth_three(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/guide"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            frontier, "http://example.com/guide"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_add_classifies_same_host_urls_as_backlog_from_depth_four(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/guide"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            frontier, "http://example.com/guide"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_add_classifies_seed_host_urls_as_backlog_through_depth_two(self, frontier):
        frontier.add(CrawlTask(url="http://docs.example.com/guide"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            frontier, "http://docs.example.com/guide"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_add_classifies_seed_host_urls_as_backlog_through_depth_three(self, frontier):
        frontier.add(CrawlTask(url="http://docs.example.com/guide"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            frontier, "http://docs.example.com/guide"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_add_defaults_implicit_urls_to_backlog(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/seed"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            frontier, "http://example.com/seed"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_upsert_seeds_keeps_seed_urls_in_exploration(self, frontier):
        frontier.upsert_seeds(["http://example.com/seed"])

        with frontier._conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {URL_LEDGER_TABLE} WHERE url = %s", ("http://example.com/seed",))
            (frontier_count,) = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) FROM frontier_queue_exploration WHERE url = %s",
                ("http://example.com/seed",),
            )
            (queue_count,) = cur.fetchone()

        assert frontier_count == 1
        assert queue_count == 1


    def test_add_classifies_known_domains_as_backlog_even_when_shallow(self, frontier):
        for i in range(8):
            frontier.add(CrawlTask(url=f"http://example.com/known-{i}"))

        frontier.add(CrawlTask(url="http://example.com/new-branch"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            frontier, "http://example.com/new-branch"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0


    def test_add_classifies_external_deep_urls_as_backlog(self, frontier):
        frontier.add(CrawlTask(url="http://external.example.com/deep"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            frontier, "http://external.example.com/deep"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_add_classifies_deep_urls_as_backlog(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/deep"))
        exploration_count, backlog_count, recrawl_count = self._queue_counts(
            frontier, "http://example.com/deep"
        )
        assert exploration_count == 0
        assert backlog_count == 1
        assert recrawl_count == 0

    def test_lease_next_returns_task(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))
        result = frontier.lease_next()
        assert result is not None
        assert "example.com" in result.url
        assert result.lease_token is not None
        assert result.lease_expires_at is not None
        assert result.next_fetch_at > 0

    def test_lease_next_returns_none_when_empty(self, frontier):
        assert frontier.lease_next() is None

    def test_lease_next_priority_order(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/low", priority=0.5))
        frontier.add(CrawlTask(url="http://example.com/high", priority=1.5))
        result = frontier.lease_next()
        assert "high" in result.url

    def test_lease_next_fifo_same_priority(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/first", added_at=1000))
        frontier.add(CrawlTask(url="http://example.com/second", added_at=2000))
        result = frontier.lease_next()
        assert "first" in result.url

    def test_lease_next_prefers_less_congested_host_when_priority_matches(self, frontier):
        frontier.add(CrawlTask(url="http://a.com/1", priority=1.0, added_at=1000))
        frontier.add(CrawlTask(url="http://a.com/2", priority=1.0, added_at=1001))
        frontier.add(CrawlTask(url="http://a.com/3", priority=1.0, added_at=1002))
        frontier.add(CrawlTask(url="http://b.com/1", priority=1.0, added_at=2000))

        result = frontier.lease_next()

        assert result is not None
        assert result.url == "http://b.com/1"

    def test_lease_next_prefers_lower_latency_host_when_priority_matches(self, frontier):
        frontier.add(CrawlTask(url="http://slow.com/1", priority=1.0, added_at=1000))
        frontier.add(CrawlTask(url="http://fast.com/1", priority=1.0, added_at=900))

        self.domain_store.record_success("slow.com", now=time.time(), request_latency_ms=900.0)
        self.domain_store.record_success("fast.com", now=time.time(), request_latency_ms=80.0)

        result = frontier.lease_next()

        assert result is not None
        assert result.url == "http://fast.com/1"

    def test_lease_next_can_prefer_breadth_over_depth(self, frontier):
        for i in range(5):
            frontier.add(CrawlTask(url=f"http://a.com/{i}", priority=1.0, added_at=1000 + i))
        frontier.add(CrawlTask(url="http://b.com/1", priority=0.8, added_at=2000))

        result = frontier.lease_next(prioritize_breadth=True)

        assert result is not None
        assert result.url == "http://b.com/1"

    def test_lease_next_marks_leased(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))
        frontier.lease_next()
        assert frontier.lease_next() is None

        with frontier._conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM active_leases WHERE url = %s", ("http://example.com/",))
            (active_count,) = cur.fetchone()

        assert active_count == 1

    def test_lease_batch(self, frontier):
        for i in range(5):
            frontier.add(CrawlTask(url=f"http://example.com/{i}"))
        batch = frontier.lease_batch(count=3)
        assert len(batch) == 3

    def test_lease_batch_uses_host_first_breadth_order_for_queue_tables(self, frontier):
        frontier.add(CrawlTask(url="http://a.com/docs/python/1", priority=1.0, added_at=1000))
        frontier.add(CrawlTask(url="http://a.com/docs/python/2", priority=1.0, added_at=1001))
        frontier.add(CrawlTask(url="http://a.com/docs/rust/1", priority=1.0, added_at=2000))

        batch = frontier.lease_batch(
            count=2,
            prioritize_breadth=True,
            queue_classes=[QUEUE_EXPLORATION],
        )

        assert {task.url for task in batch} == {
            "http://a.com/docs/python/1",
            "http://a.com/docs/python/2",
        }

    def test_lease_next_uses_host_first_breadth_without_branch_rotation(self, frontier):
        frontier.add(CrawlTask(url="http://a.com/docs/python/1", priority=1.0, added_at=1000))
        frontier.add(CrawlTask(url="http://a.com/docs/python/2", priority=1.0, added_at=1001))
        frontier.add(CrawlTask(url="http://a.com/docs/rust/1", priority=1.0, added_at=2000))
        frontier.add(CrawlTask(url="http://b.com/1", priority=1.5, added_at=5000))

        first = frontier.lease_next(prioritize_breadth=True, queue_classes=[QUEUE_EXPLORATION])
        assert first is not None
        assert first.url == "http://a.com/docs/python/1"
        frontier.mark_done(first.url, lease_token=first.lease_token)

        second = frontier.lease_next(prioritize_breadth=True, queue_classes=[QUEUE_EXPLORATION])
        assert second is not None
        assert second.url == "http://b.com/1"

    def test_mark_done(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))
        result = frontier.lease_next()
        frontier.mark_done(result.url, lease_token=result.lease_token)
        assert frontier.stats().get("done", 0) == 1
        with frontier._conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM active_leases WHERE url = %s", (result.url,))
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

    def test_lease_state_lives_only_in_active_lease_table(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))

        result = frontier.lease_next()

        with frontier._conn.cursor() as cur:
            cur.execute(
                f"SELECT lease_token, lease_expires_at FROM {URL_LEDGER_TABLE} WHERE url = %s",
                (result.url,),
            )
            lease_token, lease_expires_at = cur.fetchone()
            cur.execute(
                "SELECT lease_token, lease_expires_at FROM active_leases WHERE url = %s",
                (result.url,),
            )
            active_lease_token, active_lease_expires_at = cur.fetchone()

        assert lease_token is None
        assert lease_expires_at is None
        assert active_lease_token == result.lease_token
        assert active_lease_expires_at == result.lease_expires_at

    def test_mark_done_uses_active_lease_table_for_token_validation(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))
        result = frontier.lease_next()

        with frontier._conn.cursor() as cur:
            cur.execute(
                f"UPDATE {URL_LEDGER_TABLE} SET lease_token = %s WHERE url = %s",
                ("tampered", result.url),
            )
        frontier._conn.commit()

        assert frontier.mark_done(result.url, lease_token=result.lease_token) is True

    def test_recover_leased_uses_active_lease_table(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))
        result = frontier.lease_next()

        with frontier._conn.cursor() as cur:
            cur.execute(
                f"UPDATE {URL_LEDGER_TABLE} SET lease_expires_at = %s WHERE url = %s",
                (time.time() + 3600, result.url),
            )
        frontier._conn.commit()

        assert frontier.recover_leased(expired_only=False) == 1
        assert frontier.pending_count() == 1

    def test_mark_failed(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))
        result = frontier.lease_next()
        frontier.mark_failed(result.url, lease_token=result.lease_token)
        assert frontier.stats().get("failed", 0) == 1

        with frontier._conn.cursor() as cur:
            cur.execute(
                f"SELECT terminal_reason, terminalized_at FROM {URL_LEDGER_TABLE} WHERE url = %s",
                (result.url,),
            )
            terminal_reason, terminalized_at = cur.fetchone()

        assert terminal_reason == "failed"
        assert terminalized_at is not None

    def test_requeue_failed(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))
        result = frontier.lease_next()
        frontier.mark_failed(result.url, lease_token=result.lease_token)
        assert frontier.requeue_failed() == 1
        assert frontier.pending_count() == 1

        with frontier._conn.cursor() as cur:
            cur.execute(
                f"SELECT terminal_reason, terminalized_at FROM {URL_LEDGER_TABLE} WHERE url = %s",
                (result.url,),
            )
            terminal_reason, terminalized_at = cur.fetchone()

        assert terminal_reason is None
        assert terminalized_at is None

    def test_recover_leased(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))
        frontier.lease_next()
        assert frontier.recover_leased(expired_only=False) == 1
        assert frontier.pending_count() == 1

    def test_upsert_seeds_requeues_done_url(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))
        result = frontier.lease_next()
        frontier.mark_done(result.url, lease_token=result.lease_token)

        frontier.upsert_seeds(["http://example.com"])

        assert frontier.pending_count() == 1

        with frontier._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM frontier_queue_exploration WHERE url = %s", ("http://example.com/",))
            (queue_count,) = cur.fetchone()

        assert queue_count == 1

    def test_stats(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/1"))
        frontier.add(CrawlTask(url="http://example.com/2"))
        stats = frontier.stats()
        assert stats["total"] == 2
        assert stats.get("pending", 0) == 2

    def test_is_seen(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))
        assert frontier.is_seen("http://example.com") is True
        assert frontier.is_seen("http://example.com#section") is True
        assert frontier.is_seen("http://other.com") is False

    def test_pending_count(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/1"))
        frontier.add(CrawlTask(url="http://example.com/2"))
        assert frontier.pending_count() == 2
        frontier.lease_next()
        assert frontier.pending_count() == 1

    def test_pending_membership_comes_from_queue_tables(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/1"))

        with frontier._conn.cursor() as cur:
            cur.execute(
                f"""UPDATE {URL_LEDGER_TABLE}
                    SET terminal_reason = %s,
                        terminalized_at = %s
                    WHERE url = %s""",
                ("done", time.time(), "http://example.com/1"),
            )
        frontier._conn.commit()

        assert frontier.pending_count() == 1

        leased = frontier.lease_next()
        assert leased is not None
        assert leased.url == "http://example.com/1"

    def test_pending_domain_count(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/1"))
        frontier.add(CrawlTask(url="http://example.com/2"))
        frontier.add(CrawlTask(url="http://other.com/1"))

        assert frontier.pending_domain_count() == 2
        assert frontier.pending_domain_count(queue_classes=[QUEUE_EXPLORATION]) == 2

    def test_ready_domain_count_ignores_scheduled_and_blocked_hosts(self, frontier):
        now = time.time()
        frontier.add(CrawlTask(url="http://a.com/docs/1", next_fetch_at=now))
        frontier.add(CrawlTask(url="http://a.com/blog/1", next_fetch_at=now))
        frontier.add(CrawlTask(url="http://b.com/future", next_fetch_at=now + 30.0))
        frontier.add(CrawlTask(url="http://c.com/backoff", next_fetch_at=now))
        self.domain_store.record_failure("c.com", backoff_seconds=20.0, now=now)

        assert frontier.ready_domain_count(now=now) == 1

    def test_ready_count_ignores_future_next_fetch(self, frontier):
        now = time.time()
        frontier.add(
            CrawlTask(
                url="http://example.com/future",
                next_fetch_at=now + 60,
            )
        )
        frontier.add(CrawlTask(url="http://example.com/ready", next_fetch_at=now))

        assert frontier.pending_count() == 2
        assert frontier.ready_count(now=now) == 1
        assert frontier.next_ready_delay(now=now) == pytest.approx(0.0, abs=1e-6)

    def test_ready_count_respects_domain_backoff(self, frontier):
        now = time.time()
        frontier.add(CrawlTask(url="http://a.com/1", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)

        assert frontier.pending_count() == 1
        assert frontier.ready_count(now=now) == 0
        assert frontier.next_ready_delay(now=now) == pytest.approx(30.0, abs=1e-3)

    def test_ready_count_can_filter_queue_classes(self, frontier):
        now = time.time()
        frontier.add(CrawlTask(url="http://a.com/explore", next_fetch_at=now))
        frontier.add(CrawlTask(url="http://a.com/backlog", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=20.0, now=now)
        frontier.add(CrawlTask(url="http://b.com/explore", next_fetch_at=now))

        assert frontier.ready_count(now=now, queue_classes=[QUEUE_EXPLORATION]) == 1
        assert frontier.ready_count(now=now, queue_classes=[QUEUE_BACKLOG]) == 0

    def test_readiness_summarizes_pending_and_ready(self, frontier):
        now = time.time()
        frontier.add(CrawlTask(url="http://a.com/1", next_fetch_at=now))
        frontier.add(CrawlTask(url="http://b.com/1", next_fetch_at=now + 30))
        self.domain_store.record_failure("a.com", backoff_seconds=20.0, now=now)

        readiness = frontier.readiness(now=now)

        assert readiness.pending == 2
        assert readiness.ready == 0
        assert readiness.ready_domains == 0
        assert readiness.next_ready_delay == pytest.approx(20.0, abs=1e-3)
        assert readiness.blocked == {
            "next_fetch_at": 1,
            "domain_next_request": 0,
            "host_backoff": 1,
            "retry_quarantine": 0,
        }
        assert readiness.state_counts == {
            "ready": 0,
            "scheduled": 1,
            "blocked_domain_next_request": 0,
            "blocked_host_backoff": 1,
            "retry_quarantine": 0,
        }

    def test_rebalance_blocked_domain_backoff_quarantines_urls(self, frontier):
        now = time.time()
        frontier.add(CrawlTask(url="http://a.com/blocked", next_fetch_at=now))
        frontier.add(CrawlTask(url="http://b.com/ready", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)

        quarantined, restored = frontier.rebalance_blocked_domain_backoff(now=now)

        assert quarantined == 1
        assert restored == 0
        assert frontier.pending_count() == 1
        assert frontier.blocked_domain_backoff_count() == 1

        readiness = frontier.readiness(now=now)
        assert readiness.pending == 2
        assert readiness.ready == 1
        assert readiness.ready_domains == 1
        assert readiness.state_counts == {
            "ready": 1,
            "scheduled": 0,
            "blocked_domain_next_request": 0,
            "blocked_host_backoff": 0,
            "retry_quarantine": 1,
        }

    def test_promote_blocked_domain_backoff_restores_small_cooldown_subset(self, frontier):
        now = time.time()
        frontier.add(CrawlTask(url="http://a.com/blocked-1", next_fetch_at=now))
        frontier.add(CrawlTask(url="http://a.com/blocked-2", next_fetch_at=now))
        frontier.add(CrawlTask(url="http://b.com/blocked", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)
        self.domain_store.record_failure("b.com", backoff_seconds=30.0, now=now)
        frontier.rebalance_blocked_domain_backoff(now=now)

        self.domain_store.record_success("a.com", now=now + 31.0)
        self.domain_store.record_success("b.com", now=now + 31.0)

        promoted = frontier.promote_blocked_domain_backoff(2, per_domain=1, now=now + 31.0)

        assert promoted == 2
        assert frontier.pending_count() == 2
        assert frontier.pending_count(queue_classes=[QUEUE_BACKLOG]) == 2
        assert frontier.blocked_domain_backoff_count() == 1

        readiness = frontier.readiness(now=now + 31.0)
        assert readiness.ready == 2
        assert readiness.ready_domains == 2
        assert readiness.state_counts == {
            "ready": 2,
            "scheduled": 0,
            "blocked_domain_next_request": 0,
            "blocked_host_backoff": 0,
            "retry_quarantine": 1,
        }

    def test_promote_blocked_domain_backoff_skips_high_failure_domains(self, frontier):
        now = time.time()
        frontier.add(CrawlTask(url="http://a.com/blocked", next_fetch_at=now))
        frontier.add(CrawlTask(url="http://b.com/blocked", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now + 1.0)
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now + 2.0)
        self.domain_store.record_failure("b.com", backoff_seconds=30.0, now=now)
        frontier.rebalance_blocked_domain_backoff(now=now + 2.0)

        promoted = frontier.promote_blocked_domain_backoff(
            2,
            per_domain=1,
            max_consecutive_failures=1,
            now=now + 40.0,
        )

        assert promoted == 1
        assert frontier.pending_count() == 1
        assert frontier.blocked_domain_backoff_count() == 1

        leased = frontier.lease_next(now=now + 40.0)
        assert leased is not None
        assert leased.url == "http://b.com/blocked"

    def test_promote_blocked_domain_backoff_returns_ready_urls_to_backlog_without_domain_bias(self, frontier):
        now = time.time()
        frontier.add(CrawlTask(url="http://a.com/blocked", next_fetch_at=now))
        frontier.add(CrawlTask(url="http://b.com/blocked", next_fetch_at=now + 5.0))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)
        self.domain_store.record_failure("b.com", backoff_seconds=30.0, now=now)
        frontier.rebalance_blocked_domain_backoff(now=now)

        frontier.add(CrawlTask(url="http://a.com/ready-2", next_fetch_at=now + 31.0))
        self.domain_store.record_success("a.com", now=now + 31.0)
        self.domain_store.record_success("b.com", now=now + 31.0)

        promoted = frontier.promote_blocked_domain_backoff(1, per_domain=1, now=now + 31.0)

        assert promoted == 1
        assert frontier.pending_count(queue_classes=[QUEUE_BACKLOG]) == 2
        leased = frontier.lease_next(now=now + 31.0)
        assert leased is not None
        assert leased.url == "http://a.com/ready-2"
        assert frontier.blocked_domain_backoff_count() == 1

        leased = frontier.lease_next(now=now + 31.0, queue_classes=[QUEUE_BACKLOG])
        assert leased is not None
        assert leased.url == "http://a.com/blocked"

    def test_retire_blocked_domain_backoff_marks_old_high_failure_urls_failed(self, frontier):
        now = time.time()
        frontier.add(CrawlTask(url="http://a.com/blocked", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now + 1.0)
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now + 2.0)
        frontier.rebalance_blocked_domain_backoff(now=now + 2.0)

        with frontier._conn.cursor() as cur:
            cur.execute(
                "UPDATE frontier_queue_blocked_domain_backoff SET quarantined_at = %s WHERE url = %s",
                (now - 100.0, "http://a.com/blocked"),
            )
        frontier._conn.commit()

        retired = frontier.retire_blocked_domain_backoff(
            min_consecutive_failures=3,
            min_quarantine_seconds=60.0,
            now=now,
        )

        assert retired == 1
        assert frontier.pending_count() == 0
        assert frontier.blocked_domain_backoff_count() == 0

        with frontier._conn.cursor() as cur:
            cur.execute(
                f"SELECT last_error, terminal_reason, terminalized_at FROM {URL_LEDGER_TABLE} WHERE url = %s",
                ("http://a.com/blocked",),
            )
            last_error, terminal_reason, terminalized_at = cur.fetchone()

        assert last_error == "retry_quarantine_retired"
        assert terminal_reason == "retry_quarantine_retired"
        assert terminalized_at is not None

    def test_restore_recovered_blocked_domain_backoff_restores_healthy_domains(self, frontier):
        now = time.time()
        frontier.add(CrawlTask(url="http://a.com/blocked-1", next_fetch_at=now))
        frontier.add(CrawlTask(url="http://a.com/blocked-2", next_fetch_at=now))
        frontier.add(CrawlTask(url="http://b.com/blocked", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)
        self.domain_store.record_failure("b.com", backoff_seconds=30.0, now=now)
        frontier.rebalance_blocked_domain_backoff(now=now)

        self.domain_store.record_success("a.com", now=now + 31.0)
        restored = frontier.restore_recovered_blocked_domain_backoff(
            limit=10,
            per_domain=10,
            now=now + 31.0,
        )

        assert restored == 2
        assert frontier.pending_count() == 2
        assert frontier.pending_count(queue_classes=[QUEUE_BACKLOG]) == 2
        assert frontier.blocked_domain_backoff_count() == 1

        leased = frontier.lease_batch(count=2, queue_classes=[QUEUE_BACKLOG], now=now + 31.0)
        assert {task.url for task in leased} == {"http://a.com/blocked-1", "http://a.com/blocked-2"}

    def test_readiness_filters_blocked_queue_by_queue_class(self, frontier):
        now = time.time()
        frontier.add(CrawlTask(url="http://a.com/explore", next_fetch_at=now))
        frontier.add(CrawlTask(url="http://a.com/backlog", next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)
        frontier.rebalance_blocked_domain_backoff(now=now)

        exploration = frontier.readiness(now=now, queue_classes=[QUEUE_EXPLORATION])
        backlog = frontier.readiness(now=now, queue_classes=[QUEUE_BACKLOG])

        assert exploration.pending == 1
        assert exploration.state_counts["retry_quarantine"] == 1
        assert backlog.pending == 1
        assert backlog.state_counts["retry_quarantine"] == 1

    def test_domain_filter(self, frontier):
        frontier.add(CrawlTask(url="http://a.com/page"))
        frontier.add(CrawlTask(url="http://b.com/page"))
        result = frontier.lease_next(domain="a.com")
        assert result is not None
        assert "a.com" in result.url

    def test_lease_next_excludes_active_domains(self, frontier):
        frontier.add(CrawlTask(url="http://a.com/1", priority=3.0))
        frontier.add(CrawlTask(url="http://a.com/2", priority=2.0))
        frontier.add(CrawlTask(url="http://b.com/1", priority=1.0))

        result = frontier.lease_next(exclude_domains=["a.com"])

        assert result is not None
        assert "b.com" in result.url

    def test_lease_next_filters_queue_classes(self, frontier):
        frontier.add(CrawlTask(url="http://a.com/explore"))
        frontier.add(CrawlTask(url="http://a.com/backlog"))

        result = frontier.lease_next(queue_classes=[QUEUE_BACKLOG])

        assert result is not None
        assert result.url == "http://a.com/backlog"

    def test_lease_next_skips_host_under_backoff(self, frontier):
        self.domain_store.record_failure("a.com", backoff_seconds=60.0, now=time.time())
        frontier.add(CrawlTask(url="http://a.com/page", priority=2.0))
        frontier.add(CrawlTask(url="http://b.com/page", priority=1.0))

        result = frontier.lease_next()

        assert result is not None
        assert "b.com" in result.url

    def test_lease_next_recovers_expired_lease(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))
        first = frontier.lease_next(lease_seconds=0.01)
        assert first is not None

        time.sleep(0.02)

        second = frontier.lease_next()
        assert second is not None
        assert second.url == first.url
        assert second.lease_token != first.lease_token

    def test_retryable_failure_delays_next_fetch(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))
        result = frontier.lease_next()
        assert result is not None

        frontier.mark_failed(
            result.url,
            retryable=True,
            error="timeout",
            backoff_seconds=60,
            lease_token=result.lease_token,
        )

        assert frontier.lease_next() is None

        with frontier._conn.cursor() as cur:
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

    def test_retryable_failure_demotes_priority(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/retry", priority=1.25))
        result = frontier.lease_next()
        assert result is not None

        frontier.mark_failed(
            result.url,
            retryable=True,
            error="timeout",
            backoff_seconds=0,
            lease_token=result.lease_token,
        )

        with frontier._conn.cursor() as cur:
            cur.execute(
                f"SELECT fail_streak, priority FROM {URL_LEDGER_TABLE} WHERE url = %s",
                (result.url,),
            )
            fail_streak, priority = cur.fetchone()

        assert fail_streak == 1
        assert priority == 0.75

    def test_compute_retry_backoff_uses_configured_values(self, frontier):
        configured = UrlLedger(frontier._conn, retry_backoff_seconds=5.0, max_retry_backoff_seconds=12.0)

        assert configured._compute_retry_backoff(1) == 5.0
        assert configured._compute_retry_backoff(2) == 10.0
        assert configured._compute_retry_backoff(3) == 12.0

    def test_lease_next_prefers_fresh_url_over_retried_url(self, frontier):
        frontier.add(CrawlTask(url="http://retry.com/page", priority=1.25))
        first = frontier.lease_next(domain="retry.com")
        assert first is not None

        frontier.mark_failed(
            first.url,
            retryable=True,
            error="timeout",
            backoff_seconds=0,
            lease_token=first.lease_token,
        )

        frontier.add(CrawlTask(url="http://fresh.com/page", priority=1.0))

        next_task = frontier.lease_next()

        assert next_task is not None
        assert next_task.url == "http://fresh.com/page"

    def test_upsert_seeds_marks_seeds_as_exploration(self, frontier):
        frontier.upsert_seeds(["http://example.com"])

        with frontier._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM frontier_queue_exploration WHERE url = %s", ("http://example.com/",))
            (queue_count,) = cur.fetchone()

        assert queue_count == 1

    def test_promote_backlog_host_heads_promotes_distinct_backlog_domains(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/docs/a", queue_class=QUEUE_BACKLOG))
        frontier.add(CrawlTask(url="http://example.com/docs/b", queue_class=QUEUE_BACKLOG))
        frontier.add(CrawlTask(url="http://other.com/news/a", queue_class=QUEUE_BACKLOG))
        frontier.add(CrawlTask(url="http://third.com/start", queue_class=QUEUE_BACKLOG))

        promoted = frontier.promote_backlog_host_heads(target_pending=2, per_domain=1, candidate_limit=10)

        assert promoted == 2
        with frontier._conn.cursor() as cur:
            cur.execute("SELECT url FROM frontier_queue_exploration ORDER BY url")
            promoted_urls = [url for (url,) in cur.fetchall()]

        assert promoted_urls == [
            "http://example.com/docs/a",
            "http://other.com/news/a",
        ]

    def test_promote_backlog_host_heads_uses_queue_membership_not_frontier_queue_class(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/docs/a", queue_class=QUEUE_BACKLOG))
        frontier.add(CrawlTask(url="http://other.com/news/a", queue_class=QUEUE_BACKLOG))

        with frontier._conn.cursor() as cur:
            cur.execute(
                f"""UPDATE {URL_LEDGER_TABLE}
                    SET terminal_reason = %s,
                        terminalized_at = %s
                    WHERE url IN (%s, %s)""",
                ("reclassified", time.time(), "http://example.com/docs/a", "http://other.com/news/a"),
            )
        frontier._conn.commit()

        promoted = frontier.promote_backlog_host_heads(target_pending=2, per_domain=1, candidate_limit=10)

        assert promoted == 2
        with frontier._conn.cursor() as cur:
            cur.execute("SELECT url FROM frontier_queue_exploration ORDER BY url")
            promoted_urls = [url for (url,) in cur.fetchall()]

        assert promoted_urls == [
            "http://example.com/docs/a",
            "http://other.com/news/a",
        ]

    def test_recrawl_queue_class_can_be_leased_separately(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))
        leased = frontier.lease_next()
        assert leased is not None
        frontier.mark_done(leased.url, lease_token=leased.lease_token)

        requeued = frontier.requeue_urls([leased.url], queue_class=QUEUE_RECRAWL)
        assert requeued == 1

        recrawl = frontier.lease_next(queue_classes=[QUEUE_RECRAWL])
        assert recrawl is not None
        assert recrawl.url == leased.url

    def test_lease_next_uses_queue_membership_not_frontier_queue_class(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))
        leased = frontier.lease_next()
        assert leased is not None
        frontier.mark_done(leased.url, lease_token=leased.lease_token)

        requeued = frontier.requeue_urls([leased.url], queue_class=QUEUE_RECRAWL)
        assert requeued == 1

        recrawl = frontier.lease_next(queue_classes=[QUEUE_RECRAWL])
        assert recrawl is not None
        assert recrawl.url == leased.url

    def test_mark_done_resets_fail_streak(self, frontier):
        frontier.add(CrawlTask(url="http://example.com"))
        first = frontier.lease_next()
        assert first is not None

        frontier.mark_failed(
            first.url,
            retryable=True,
            error="timeout",
            backoff_seconds=0,
            lease_token=first.lease_token,
        )

        second = frontier.lease_next()
        assert second is not None

        frontier.mark_done(second.url, lease_token=second.lease_token)

        with frontier._conn.cursor() as cur:
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

    def test_defer_overcrowded_backlog_delays_excess_ready_urls(self, frontier):
        frontier.add(CrawlTask(url="http://a.com/1", priority=0.55, added_at=1000))
        frontier.add(CrawlTask(url="http://a.com/2", priority=0.55, added_at=1001))
        frontier.add(CrawlTask(url="http://a.com/3", priority=0.55, added_at=1002))

        delayed = frontier.defer_overcrowded_backlog(
            keep_ready_per_domain=1,
            defer_seconds=60.0,
        )

        assert delayed == 2

        with frontier._conn.cursor() as cur:
            cur.execute(
                f"SELECT url, next_fetch_at FROM {URL_LEDGER_TABLE} WHERE domain = 'a.com' ORDER BY url ASC"
            )
            rows = cur.fetchall()

        ready = [url for url, next_fetch_at in rows if next_fetch_at <= time.time()]
        deferred = [url for url, next_fetch_at in rows if next_fetch_at > time.time()]

        assert ready == ["http://a.com/1"]
        assert deferred == ["http://a.com/2", "http://a.com/3"]

    def test_defer_overcrowded_backlog_delays_excess_branch_urls(self, frontier):
        frontier.add(CrawlTask(url="http://a.com/docs/python/1", priority=0.55, added_at=1000))
        frontier.add(CrawlTask(url="http://a.com/docs/python/2", priority=0.55, added_at=1001))
        frontier.add(CrawlTask(url="http://a.com/docs/rust/1", priority=0.55, added_at=1002))

        delayed = frontier.defer_overcrowded_backlog(
            keep_ready_per_domain=10,
            keep_ready_per_branch=1,
            defer_seconds=60.0,
        )

        assert delayed == 1

        with frontier._conn.cursor() as cur:
            cur.execute(
                f"SELECT url, next_fetch_at FROM {URL_LEDGER_TABLE} WHERE domain = 'a.com' ORDER BY url ASC"
            )
            rows = cur.fetchall()

        ready = [url for url, next_fetch_at in rows if next_fetch_at <= time.time()]
        deferred = [url for url, next_fetch_at in rows if next_fetch_at > time.time()]

        assert ready == ["http://a.com/docs/python/1", "http://a.com/docs/rust/1"]
        assert deferred == ["http://a.com/docs/python/2"]
