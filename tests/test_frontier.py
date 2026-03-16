"""Tests for URL Frontier module."""

import os
import time

import psycopg2
import pytest

from crawler.frontier import CrawlTask, Frontier
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
        )
        assert task.depth == 2
        assert task.priority == 0.5
        assert task.added_at == 1000.0

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
        conn.commit()
        f = Frontier(conn)
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

    def test_get_next_returns_task(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        result = frontier.get_next()
        assert result is not None
        assert "example.com" in result.url

    def test_get_next_returns_none_when_empty(self, frontier):
        assert frontier.get_next() is None

    def test_get_next_priority_order(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/low", depth=0, priority=0.5))
        frontier.add(CrawlTask(url="http://example.com/high", depth=0, priority=1.5))
        result = frontier.get_next()
        assert "high" in result.url

    def test_get_next_fifo_same_priority(self, frontier):
        frontier.add(CrawlTask(url="http://example.com/first", depth=0, added_at=1000))
        frontier.add(CrawlTask(url="http://example.com/second", depth=0, added_at=2000))
        result = frontier.get_next()
        assert "first" in result.url

    def test_get_next_marks_processing(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        frontier.get_next()
        assert frontier.get_next() is None

    def test_get_batch(self, frontier):
        for i in range(5):
            frontier.add(CrawlTask(url=f"http://example.com/{i}", depth=0))
        batch = frontier.get_batch(count=3)
        assert len(batch) == 3

    def test_mark_done(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        result = frontier.get_next()
        frontier.mark_done(result.url)
        assert frontier.stats().get("done", 0) == 1

    def test_mark_failed(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        result = frontier.get_next()
        frontier.mark_failed(result.url)
        assert frontier.stats().get("failed", 0) == 1

    def test_requeue_failed(self, frontier):
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        result = frontier.get_next()
        frontier.mark_failed(result.url)
        assert frontier.requeue_failed() == 1
        assert frontier.pending_count() == 1

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
        frontier.get_next()
        assert frontier.pending_count() == 1

    def test_domain_filter(self, frontier):
        frontier.add(CrawlTask(url="http://a.com/page", depth=0))
        frontier.add(CrawlTask(url="http://b.com/page", depth=0))
        result = frontier.get_next(domain="a.com")
        assert result is not None
        assert "a.com" in result.url
