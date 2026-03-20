"""Tests for URL Frontier module."""

import os
import time

import psycopg2
import pytest

from crawler.domain_store import DomainStore
from crawler.discovery import DISCOVERY_EXTERNAL, DISCOVERY_SAME_HOST, DISCOVERY_SEED_HOST
from crawler.frontier import CrawlTask, Frontier, LEASED_STATUS
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
        task = CrawlTask(url="http://example.com", depth=0)
        assert task.url == "http://example.com"
        assert task.depth == 0
        assert task.priority == 1.0
        assert task.source_url is None
        assert task.added_at > 0

    def test_custom_values(self):
        task = CrawlTask(
            url="http://example.com/page",
            depth=2,
            priority=0.5,
            source_url="http://example.com",
            added_at=1000.0,
            next_fetch_at=1200.0,
        )
        assert task.depth == 2
        assert task.priority == 0.5
        assert task.added_at == 1000.0
        assert task.next_fetch_at == 1200.0

    def test_added_at_auto_set(self):
        before = time.time()
        task = CrawlTask(url="http://example.com", depth=0)
        after = time.time()
        assert before <= task.added_at <= after


@requires_pg
class TestFrontier:
    @pytest.fixture(autouse=True)
    def frontier(self):
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS frontier")
            cur.execute("DROP TABLE IF EXISTS domain_state")
        conn.commit()
        f = Frontier(conn)
        self.domain_store = DomainStore(conn)
        f.attach_domain_store(self.domain_store)
        yield f
        conn.close()

    def test_add_new_url_returns_true(self, frontier):
        task = CrawlTask(url="http://example.com", depth=0)
        assert frontier.add(task) is True

    def test_add_duplicate_url_returns_false(self, frontier):
        task1 = CrawlTask(url="http://example.com", depth=0)
        task2 = CrawlTask(url="http://example.com", depth=0)
        frontier.add(task1)
        assert frontier.add(task2) is False

    def test_add_normalizes_url(self, frontier):
        task1 = CrawlTask(url="http://example.com/page#section", depth=0)
        task2 = CrawlTask(url="http://example.com/page", depth=0)
        frontier.add(task1)
        assert frontier.add(task2) is False

    def test_add_many_returns_count(self, frontier):
        tasks = [
            CrawlTask(url="http://example.com/1", depth=0),
            CrawlTask(url="http://example.com/2", depth=0),
            CrawlTask(url="http://example.com/1", depth=0),
        ]
        assert frontier.add_many(tasks) == 2

    def test_add_upgrades_existing_metadata_when_better_discovery_arrives(self, frontier):
        assert frontier.add(
            CrawlTask(
                url="http://example.com/page",
                depth=1,
                priority=0.8,
                discovery_kind=DISCOVERY_EXTERNAL,
                source_url="http://other.com",
            )
        )

        assert frontier.add(
            CrawlTask(
                url="http://example.com/page",
                depth=1,
                priority=1.25,
                discovery_kind=DISCOVERY_SAME_HOST,
                source_url="http://example.com/",
            )
        )

        with frontier._conn.cursor() as cur:
            cur.execute(
                "SELECT priority, discovery_kind, source_url FROM frontier WHERE url = %s",
                ("http://example.com/page",),
            )
            priority, discovery_kind, source_url = cur.fetchone()

        assert priority == 1.25
        assert discovery_kind == DISCOVERY_SAME_HOST
        assert source_url == "http://other.com"

    def test_add_persists_discovery_kind(self, frontier):
        frontier.add(
            CrawlTask(
                url="http://example.com",
                depth=0,
                discovery_kind=DISCOVERY_SEED_HOST,
            )
        )

        with frontier._conn.cursor() as cur:
            cur.execute(
                "SELECT discovery_kind FROM frontier WHERE url = %s",
                ("http://example.com/",),
            )
            (discovery_kind,) = cur.fetchone()

        assert discovery_kind == DISCOVERY_SEED_HOST

    def test_lease_next_returns_task(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        result = frontier.lease_next()
        assert result is not None
        assert "example.com" in result.url
        assert result.lease_token is not None
        assert result.lease_expires_at is not None
        assert result.next_fetch_at > 0

    def test_lease_next_returns_none_when_empty(self, frontier):
        assert frontier.lease_next() is None

    def test_lease_next_priority_order(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/low", depth=0, priority=0.5))
        frontier.add(CrawlTask(url="http://example.com/high", depth=0, priority=1.5))
        result = frontier.lease_next()
        assert "high" in result.url

    def test_lease_next_fifo_same_priority(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/first", depth=0, added_at=1000))
        frontier.add(CrawlTask(url="http://example.com/second", depth=0, added_at=2000))
        result = frontier.lease_next()
        assert "first" in result.url

    def test_lease_next_prefers_less_congested_host_when_priority_matches(self, frontier):
        frontier.add(CrawlTask(url="http://a.com/1", depth=0, priority=1.0, added_at=1000))
        frontier.add(CrawlTask(url="http://a.com/2", depth=0, priority=1.0, added_at=1001))
        frontier.add(CrawlTask(url="http://a.com/3", depth=0, priority=1.0, added_at=1002))
        frontier.add(CrawlTask(url="http://b.com/1", depth=0, priority=1.0, added_at=2000))

        result = frontier.lease_next()

        assert result is not None
        assert result.url == "http://b.com/1"

    def test_lease_next_can_prefer_breadth_over_depth(self, frontier):
        for i in range(5):
            frontier.add(CrawlTask(url=f"http://a.com/{i}", depth=0, priority=1.0, added_at=1000 + i))
        frontier.add(CrawlTask(url="http://b.com/1", depth=0, priority=0.8, added_at=2000))

        result = frontier.lease_next(prioritize_breadth=True)

        assert result is not None
        assert result.url == "http://b.com/1"

    def test_lease_next_marks_leased(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        frontier.lease_next()
        assert frontier.lease_next() is None

        with frontier._conn.cursor() as cur:
            cur.execute("SELECT status FROM frontier WHERE url = %s", ("http://example.com/",))
            (status,) = cur.fetchone()

        assert status == LEASED_STATUS

    def test_lease_batch(self, frontier):
        for i in range(5):
            frontier.add(CrawlTask(url=f"http://example.com/{i}", depth=0))
        batch = frontier.lease_batch(count=3)
        assert len(batch) == 3

    def test_mark_done(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        result = frontier.lease_next()
        frontier.mark_done(result.url, lease_token=result.lease_token)
        assert frontier.stats().get("done", 0) == 1

    def test_mark_failed(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        result = frontier.lease_next()
        frontier.mark_failed(result.url, lease_token=result.lease_token)
        assert frontier.stats().get("failed", 0) == 1

    def test_requeue_failed(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        result = frontier.lease_next()
        frontier.mark_failed(result.url, lease_token=result.lease_token)
        assert frontier.requeue_failed() == 1
        assert frontier.pending_count() == 1

    def test_recover_leased(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        frontier.lease_next()
        assert frontier.recover_leased(expired_only=False) == 1
        assert frontier.pending_count() == 1

    def test_upsert_seeds_requeues_done_url(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        result = frontier.lease_next()
        frontier.mark_done(result.url, lease_token=result.lease_token)

        frontier.upsert_seeds(["http://example.com"])

        assert frontier.pending_count() == 1

        with frontier._conn.cursor() as cur:
            cur.execute(
                "SELECT discovery_kind FROM frontier WHERE url = %s",
                ("http://example.com/",),
            )
            (discovery_kind,) = cur.fetchone()

        assert discovery_kind == "seed"

    def test_stats(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/1", depth=0))
        frontier.add(CrawlTask(url="http://example.com/2", depth=0))
        stats = frontier.stats()
        assert stats["total"] == 2
        assert stats.get("pending", 0) == 2

    def test_is_seen(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        assert frontier.is_seen("http://example.com") is True
        assert frontier.is_seen("http://example.com#section") is True
        assert frontier.is_seen("http://other.com") is False

    def test_pending_count(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/1", depth=0))
        frontier.add(CrawlTask(url="http://example.com/2", depth=0))
        assert frontier.pending_count() == 2
        frontier.lease_next()
        assert frontier.pending_count() == 1

    def test_ready_count_ignores_future_next_fetch(self, frontier):
        now = time.time()
        frontier.add(
            CrawlTask(
                url="http://example.com/future",
                depth=0,
                next_fetch_at=now + 60,
            )
        )
        frontier.add(CrawlTask(url="http://example.com/ready", depth=0, next_fetch_at=now))

        assert frontier.pending_count() == 2
        assert frontier.ready_count(now=now) == 1
        assert frontier.next_ready_delay(now=now) == pytest.approx(0.0, abs=1e-6)

    def test_ready_count_respects_domain_backoff(self, frontier):
        now = time.time()
        frontier.add(CrawlTask(url="http://a.com/1", depth=0, next_fetch_at=now))
        self.domain_store.record_failure("a.com", backoff_seconds=30.0, now=now)

        assert frontier.pending_count() == 1
        assert frontier.ready_count(now=now) == 0
        assert frontier.next_ready_delay(now=now) == pytest.approx(30.0, abs=1e-3)

    def test_domain_filter(self, frontier):
        frontier.add(CrawlTask(url="http://a.com/page", depth=0))
        frontier.add(CrawlTask(url="http://b.com/page", depth=0))
        result = frontier.lease_next(domain="a.com")
        assert result is not None
        assert "a.com" in result.url

    def test_lease_next_skips_host_under_backoff(self, frontier):
        self.domain_store.record_failure("a.com", backoff_seconds=60.0, now=time.time())
        frontier.add(CrawlTask(url="http://a.com/page", depth=0, priority=2.0))
        frontier.add(CrawlTask(url="http://b.com/page", depth=0, priority=1.0))

        result = frontier.lease_next()

        assert result is not None
        assert "b.com" in result.url

    def test_lease_next_recovers_expired_lease(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        first = frontier.lease_next(lease_seconds=0.01)
        assert first is not None

        time.sleep(0.02)

        second = frontier.lease_next()
        assert second is not None
        assert second.url == first.url
        assert second.lease_token != first.lease_token

    def test_retryable_failure_delays_next_fetch(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
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
                "SELECT status, fail_streak, last_error, next_fetch_at FROM frontier WHERE url = %s",
                (result.url,),
            )
            status, fail_streak, last_error, next_fetch_at = cur.fetchone()

        assert status == "pending"
        assert fail_streak == 1
        assert last_error == "timeout"
        assert next_fetch_at > time.time()

    def test_mark_done_resets_fail_streak(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
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
                "SELECT status, fail_streak, last_success_at, last_error FROM frontier WHERE url = %s",
                (second.url,),
            )
            status, fail_streak, last_success_at, last_error = cur.fetchone()

        assert status == "done"
        assert fail_streak == 0
        assert last_success_at is not None
        assert last_error is None

    def test_defer_overcrowded_backlog_delays_excess_low_priority_urls(self, frontier):
        frontier.add(CrawlTask(url="http://a.com/1", depth=1, priority=0.55, added_at=1000))
        frontier.add(CrawlTask(url="http://a.com/2", depth=1, priority=0.55, added_at=1001))
        frontier.add(CrawlTask(url="http://a.com/3", depth=1, priority=0.55, added_at=1002))

        delayed = frontier.defer_overcrowded_backlog(
            keep_ready_per_domain=1,
            low_priority_threshold=0.75,
            defer_seconds=60.0,
        )

        assert delayed == 2

        with frontier._conn.cursor() as cur:
            cur.execute(
                "SELECT url, next_fetch_at FROM frontier WHERE domain = 'a.com' ORDER BY url ASC"
            )
            rows = cur.fetchall()

        ready = [url for url, next_fetch_at in rows if next_fetch_at <= time.time()]
        deferred = [url for url, next_fetch_at in rows if next_fetch_at > time.time()]

        assert ready == ["http://a.com/1"]
        assert deferred == ["http://a.com/2", "http://a.com/3"]

    def test_rerank_discovered_updates_legacy_backlog(self, frontier):
        frontier.add(
            CrawlTask(
                url="http://example.com/page",
                depth=1,
                priority=1.0,
                discovery_kind="seed",
                source_url="http://example.com/",
            )
        )
        frontier.add(
            CrawlTask(
                url="http://external.com/page",
                depth=1,
                priority=1.0,
                discovery_kind="seed",
                source_url="http://example.com/",
            )
        )

        updated = frontier.rerank_discovered(["http://example.com/"])

        assert updated == 2

        with frontier._conn.cursor() as cur:
            cur.execute(
                "SELECT url, priority, discovery_kind FROM frontier ORDER BY url ASC"
            )
            rows = cur.fetchall()

        assert rows == [
            ("http://example.com/page", 1.25, DISCOVERY_SAME_HOST),
            ("http://external.com/page", 0.8, DISCOVERY_EXTERNAL),
        ]

    def test_rerank_discovered_lowers_bulk_backlog_urls(self, frontier):
        frontier.add(
            CrawlTask(
                url="https://www.iana.org/domains/idn-tables/tables/zara_uk_1.txt",
                depth=1,
                priority=1.25,
                discovery_kind=DISCOVERY_SAME_HOST,
                source_url="https://www.iana.org/domains/idn-tables",
            )
        )

        updated = frontier.rerank_discovered(["https://www.iana.org/"])

        assert updated == 1

        with frontier._conn.cursor() as cur:
            cur.execute(
                "SELECT priority, discovery_kind FROM frontier WHERE url = %s",
                ("https://www.iana.org/domains/idn-tables/tables/zara_uk_1.txt",),
            )
            priority, discovery_kind = cur.fetchone()

        assert discovery_kind == DISCOVERY_SAME_HOST
        assert priority < 0.75
