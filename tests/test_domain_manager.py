"""Tests for domain manager module."""

import time

import httpx

from crawler.domain_manager import DomainManager, DomainState


class TestDomainState:
    def test_default_values(self):
        """DomainState should have sensible defaults."""
        state = DomainState(domain="example.com")
        assert state.domain == "example.com"
        assert state.robots_parser is None
        assert state.robots_fetched is False
        assert state.error_count == 0
        assert state.request_count == 0
        assert state.crawl_delay == 1.0


class TestDomainManagerIsAllowed:
    async def test_is_allowed_without_robots(self, httpx_mock):
        """Should allow all URLs when robots.txt is not available."""
        httpx_mock.add_response(url="http://example.com/robots.txt", status_code=404)

        manager = DomainManager()
        try:
            allowed = await manager.is_allowed("http://example.com/page")
            assert allowed is True
        finally:
            await manager.close()

    async def test_is_allowed_with_allow_all_robots(self, httpx_mock):
        """Should allow URLs when robots.txt allows all."""
        robots_txt = """
User-agent: *
Allow: /
        """
        httpx_mock.add_response(
            url="http://example.com/robots.txt",
            status_code=200,
            text=robots_txt,
        )

        manager = DomainManager()
        try:
            allowed = await manager.is_allowed("http://example.com/page")
            assert allowed is True
        finally:
            await manager.close()

    async def test_is_allowed_with_disallow_robots(self, httpx_mock):
        """Should disallow URLs when robots.txt disallows."""
        robots_txt = """
User-agent: *
Disallow: /private/
        """
        httpx_mock.add_response(
            url="http://example.com/robots.txt",
            status_code=200,
            text=robots_txt,
        )

        manager = DomainManager()
        try:
            allowed_public = await manager.is_allowed("http://example.com/public")
            allowed_private = await manager.is_allowed("http://example.com/private/secret")
            assert allowed_public is True
            assert allowed_private is False
        finally:
            await manager.close()

    async def test_is_allowed_respects_user_agent(self, httpx_mock):
        """Should respect user agent specific rules."""
        robots_txt = """
User-agent: TestBot
Disallow: /

User-agent: *
Allow: /
        """
        httpx_mock.add_response(
            url="http://example.com/robots.txt",
            status_code=200,
            text=robots_txt,
        )

        manager = DomainManager(user_agent="TestBot")
        try:
            allowed = await manager.is_allowed("http://example.com/page")
            assert allowed is False
        finally:
            await manager.close()

    async def test_is_allowed_when_respect_robots_false(self, httpx_mock):
        """Should allow all URLs when respect_robots is False."""
        manager = DomainManager(respect_robots=False)
        try:
            # No HTTP mock needed since robots.txt won't be fetched
            allowed = await manager.is_allowed("http://example.com/private")
            assert allowed is True
        finally:
            await manager.close()

    async def test_is_allowed_handles_robots_fetch_error(self, httpx_mock):
        """Should allow URLs when robots.txt fetch fails."""
        httpx_mock.add_exception(
            httpx.ConnectError("Connection refused"),
            url="http://example.com/robots.txt",
        )

        manager = DomainManager()
        try:
            allowed = await manager.is_allowed("http://example.com/page")
            assert allowed is True  # Default to allow
        finally:
            await manager.close()


class TestDomainManagerRateLimit:
    async def test_first_request_no_wait(self):
        """First request should not wait (excluding robots.txt fetch time)."""
        # Use respect_robots=False to skip robots.txt fetch
        manager = DomainManager(default_delay=1.0, respect_robots=False)
        try:
            start = time.time()
            await manager.wait_for_rate_limit("http://example.com/page")
            elapsed = time.time() - start
            assert elapsed < 0.1  # Should be nearly instant
        finally:
            await manager.close()

    async def test_consecutive_requests_wait(self):
        """Consecutive requests should wait for delay."""
        manager = DomainManager(default_delay=0.2, respect_robots=False)
        try:
            await manager.wait_for_rate_limit("http://example.com/page1")
            start = time.time()
            await manager.wait_for_rate_limit("http://example.com/page2")
            elapsed = time.time() - start
            assert elapsed >= 0.15  # Should wait at least most of the delay
        finally:
            await manager.close()

    async def test_default_delay_is_used(self):
        """Default delay should be used when no Crawl-delay in robots.txt."""
        manager = DomainManager(default_delay=0.5, respect_robots=False)
        try:
            await manager.get_state("http://example.com/page")
            state = manager._domains["example.com"]
            assert state.crawl_delay == 0.5
        finally:
            await manager.close()


class TestDomainManagerErrorHandling:
    async def test_record_error_increments_count(self, httpx_mock):
        """record_error should increment error count."""
        httpx_mock.add_response(url="http://example.com/robots.txt", status_code=404)

        manager = DomainManager()
        try:
            await manager.get_state("http://example.com/page")
            manager.record_error("http://example.com/page")
            state = manager._domains["example.com"]
            assert state.error_count == 1
        finally:
            await manager.close()

    async def test_should_retry_under_max_retries(self, httpx_mock):
        """should_retry should return True under max_retries."""
        httpx_mock.add_response(url="http://example.com/robots.txt", status_code=404)

        manager = DomainManager(max_retries=3)
        try:
            await manager.get_state("http://example.com/page")
            manager.record_error("http://example.com/page")
            manager.record_error("http://example.com/page")
            assert manager.should_retry("http://example.com/page") is True
        finally:
            await manager.close()

    async def test_should_retry_at_max_retries(self, httpx_mock):
        """should_retry should return False at max_retries."""
        httpx_mock.add_response(url="http://example.com/robots.txt", status_code=404)

        manager = DomainManager(max_retries=3)
        try:
            await manager.get_state("http://example.com/page")
            manager.record_error("http://example.com/page")
            manager.record_error("http://example.com/page")
            manager.record_error("http://example.com/page")
            assert manager.should_retry("http://example.com/page") is False
        finally:
            await manager.close()

    def test_should_retry_unknown_domain(self):
        """should_retry should return True for unknown domain."""
        manager = DomainManager()
        assert manager.should_retry("http://unknown.com/page") is True


class TestDomainManagerStats:
    async def test_get_stats(self, httpx_mock):
        """get_stats should return domain statistics."""
        httpx_mock.add_response(url="http://example.com/robots.txt", status_code=404)

        manager = DomainManager()
        try:
            await manager.get_state("http://example.com/page")
            await manager.wait_for_rate_limit("http://example.com/page")
            manager.record_error("http://example.com/page")

            stats = manager.get_stats()
            assert "example.com" in stats
            assert stats["example.com"]["request_count"] == 1
            assert stats["example.com"]["error_count"] == 1
        finally:
            await manager.close()


class TestDomainManagerCaching:
    async def test_robots_cache(self, httpx_mock):
        """Should cache robots.txt and not refetch."""
        httpx_mock.add_response(
            url="http://example.com/robots.txt",
            status_code=200,
            text="User-agent: *\nAllow: /",
        )

        manager = DomainManager()
        try:
            # First request - should fetch
            await manager.is_allowed("http://example.com/page1")

            # Second request - should use cache
            await manager.is_allowed("http://example.com/page2")

            # Should only have made one request
            assert len(httpx_mock.get_requests()) == 1
        finally:
            await manager.close()

    async def test_different_domains_separate_state(self, httpx_mock):
        """Different domains should have separate state."""
        httpx_mock.add_response(url="http://a.com/robots.txt", status_code=404)
        httpx_mock.add_response(url="http://b.com/robots.txt", status_code=404)

        manager = DomainManager()
        try:
            await manager.get_state("http://a.com/page")
            await manager.get_state("http://b.com/page")

            assert "a.com" in manager._domains
            assert "b.com" in manager._domains
        finally:
            await manager.close()
