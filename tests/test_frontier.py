"""Tests for URL Frontier module."""

import time

import pytest

from crawler.frontier import CrawlTask, Frontier, normalize_url


class TestNormalizeUrl:
    def test_removes_fragment(self):
        """Fragment should be removed."""
        result = normalize_url("http://example.com/page#section")
        assert result == "http://example.com/page"

    def test_sorts_query_params(self):
        """Query parameters should be sorted alphabetically."""
        result = normalize_url("http://example.com/page?b=2&a=1")
        assert result == "http://example.com/page?a=1&b=2"

    def test_removes_trailing_slash(self):
        """Trailing slash should be removed from paths."""
        result = normalize_url("http://example.com/path/")
        assert result == "http://example.com/path"

    def test_keeps_root_slash(self):
        """Root path should remain as single slash."""
        result = normalize_url("http://example.com/")
        assert result == "http://example.com/"

    def test_lowercases_scheme_and_host(self):
        """Scheme and host should be lowercased."""
        result = normalize_url("HTTP://EXAMPLE.COM/Path")
        assert result == "http://example.com/Path"

    def test_empty_query_params(self):
        """URL with no query params should work."""
        result = normalize_url("http://example.com/page")
        assert result == "http://example.com/page"

    def test_complex_url(self):
        """Complex URL with multiple normalizations."""
        result = normalize_url("HTTPS://Example.COM/path/?z=3&a=1&m=2#anchor")
        assert result == "https://example.com/path?a=1&m=2&z=3"


class TestCrawlTask:
    def test_default_values(self):
        """CrawlTask should have sensible defaults."""
        task = CrawlTask(url="http://example.com", depth=0)
        assert task.url == "http://example.com"
        assert task.depth == 0
        assert task.priority == 1.0
        assert task.source_url is None
        assert task.added_at > 0

    def test_custom_values(self):
        """CrawlTask should accept custom values."""
        task = CrawlTask(
            url="http://example.com/page",
            depth=2,
            priority=0.5,
            source_url="http://example.com",
            added_at=1000.0,
        )
        assert task.url == "http://example.com/page"
        assert task.depth == 2
        assert task.priority == 0.5
        assert task.source_url == "http://example.com"
        assert task.added_at == 1000.0

    def test_added_at_auto_set(self):
        """added_at should be auto-set if not provided."""
        before = time.time()
        task = CrawlTask(url="http://example.com", depth=0)
        after = time.time()
        assert before <= task.added_at <= after


class TestFrontier:
    @pytest.fixture
    def frontier(self):
        """Create an in-memory frontier for testing."""
        return Frontier(db_path=":memory:")

    def test_add_new_url_returns_true(self, frontier):
        """Adding a new URL should return True."""
        task = CrawlTask(url="http://example.com", depth=0)
        result = frontier.add(task)
        assert result is True

    def test_add_duplicate_url_returns_false(self, frontier):
        """Adding a duplicate URL should return False."""
        task1 = CrawlTask(url="http://example.com", depth=0)
        task2 = CrawlTask(url="http://example.com", depth=0)
        frontier.add(task1)
        result = frontier.add(task2)
        assert result is False

    def test_add_normalizes_url(self, frontier):
        """URLs should be normalized before deduplication."""
        task1 = CrawlTask(url="http://example.com/page#section", depth=0)
        task2 = CrawlTask(url="http://example.com/page", depth=0)
        frontier.add(task1)
        result = frontier.add(task2)
        assert result is False

    def test_add_many_returns_count(self, frontier):
        """add_many should return count of new URLs added."""
        tasks = [
            CrawlTask(url="http://example.com/1", depth=0),
            CrawlTask(url="http://example.com/2", depth=0),
            CrawlTask(url="http://example.com/1", depth=0),  # duplicate
        ]
        count = frontier.add_many(tasks)
        assert count == 2

    def test_get_next_returns_task(self, frontier):
        """get_next should return a task."""
        task = CrawlTask(url="http://example.com", depth=0)
        frontier.add(task)
        result = frontier.get_next()
        assert result is not None
        assert "example.com" in result.url

    def test_get_next_returns_none_when_empty(self, frontier):
        """get_next should return None when queue is empty."""
        result = frontier.get_next()
        assert result is None

    def test_get_next_priority_order(self, frontier):
        """get_next should return highest priority first."""
        frontier.add(CrawlTask(url="http://example.com/low", depth=0, priority=0.5))
        frontier.add(CrawlTask(url="http://example.com/high", depth=0, priority=1.5))
        result = frontier.get_next()
        assert "high" in result.url

    def test_get_next_fifo_same_priority(self, frontier):
        """get_next should return FIFO for same priority."""
        frontier.add(CrawlTask(url="http://example.com/first", depth=0, added_at=1000))
        frontier.add(CrawlTask(url="http://example.com/second", depth=0, added_at=2000))
        result = frontier.get_next()
        assert "first" in result.url

    def test_get_next_marks_processing(self, frontier):
        """get_next should mark task as processing."""
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        frontier.get_next()
        # Second call should return None (no pending tasks)
        result = frontier.get_next()
        assert result is None

    def test_get_batch(self, frontier):
        """get_batch should return multiple tasks."""
        for i in range(5):
            frontier.add(CrawlTask(url=f"http://example.com/{i}", depth=0))
        batch = frontier.get_batch(count=3)
        assert len(batch) == 3

    def test_mark_done(self, frontier):
        """mark_done should update task status."""
        task = CrawlTask(url="http://example.com", depth=0)
        frontier.add(task)
        result = frontier.get_next()
        frontier.mark_done(result.url)
        stats = frontier.stats()
        assert stats.get("done", 0) == 1

    def test_mark_failed(self, frontier):
        """mark_failed should update task status."""
        task = CrawlTask(url="http://example.com", depth=0)
        frontier.add(task)
        result = frontier.get_next()
        frontier.mark_failed(result.url)
        stats = frontier.stats()
        assert stats.get("failed", 0) == 1

    def test_requeue_failed(self, frontier):
        """requeue_failed should reset failed tasks to pending."""
        task = CrawlTask(url="http://example.com", depth=0)
        frontier.add(task)
        result = frontier.get_next()
        frontier.mark_failed(result.url)
        requeued = frontier.requeue_failed()
        assert requeued == 1
        assert frontier.pending_count() == 1

    def test_stats(self, frontier):
        """stats should return correct counts."""
        frontier.add(CrawlTask(url="http://example.com/1", depth=0))
        frontier.add(CrawlTask(url="http://example.com/2", depth=0))
        stats = frontier.stats()
        assert stats["total"] == 2
        assert stats.get("pending", 0) == 2

    def test_is_seen(self, frontier):
        """is_seen should return True for added URLs."""
        frontier.add(CrawlTask(url="http://example.com", depth=0))
        assert frontier.is_seen("http://example.com") is True
        assert frontier.is_seen("http://example.com#section") is True  # normalized
        assert frontier.is_seen("http://other.com") is False

    def test_pending_count(self, frontier):
        """pending_count should return correct count."""
        frontier.add(CrawlTask(url="http://example.com/1", depth=0))
        frontier.add(CrawlTask(url="http://example.com/2", depth=0))
        assert frontier.pending_count() == 2
        frontier.get_next()
        assert frontier.pending_count() == 1

    def test_domain_filter(self, frontier):
        """get_next with domain filter should only return matching tasks."""
        frontier.add(CrawlTask(url="http://a.com/page", depth=0))
        frontier.add(CrawlTask(url="http://b.com/page", depth=0))
        result = frontier.get_next(domain="a.com")
        assert result is not None
        assert "a.com" in result.url
