"""Tests for link checking module."""

import pytest
import httpx

from crawler.links import extract_links_from_html, check_url


class TestExtractLinksFromHtml:
    def test_extracts_absolute_urls(self):
        """Should extract absolute URLs."""
        html = '<a href="http://example.com/page">Link</a>'
        links = extract_links_from_html(html, "http://base.com")
        assert len(links) == 1
        assert links[0]["url"] == "http://example.com/page"

    def test_converts_relative_urls(self):
        """Should convert relative URLs to absolute."""
        html = '<a href="/page">Link</a>'
        links = extract_links_from_html(html, "http://example.com/current/")
        assert len(links) == 1
        assert links[0]["url"] == "http://example.com/page"

    def test_extracts_link_text(self):
        """Should extract link text."""
        html = '<a href="/page">Click Here</a>'
        links = extract_links_from_html(html, "http://example.com")
        assert links[0]["text"] == "Click Here"

    def test_strips_html_from_text(self):
        """Should strip HTML tags from link text."""
        html = '<a href="/page"><strong>Bold</strong> Text</a>'
        links = extract_links_from_html(html, "http://example.com")
        assert links[0]["text"] == "Bold Text"

    def test_truncates_long_text(self):
        """Should truncate text longer than 100 characters."""
        long_text = "A" * 150
        html = f'<a href="/page">{long_text}</a>'
        links = extract_links_from_html(html, "http://example.com")
        assert len(links[0]["text"]) == 100

    def test_includes_source_url(self):
        """Should include source URL in result."""
        html = '<a href="/page">Link</a>'
        links = extract_links_from_html(html, "http://example.com/base")
        assert links[0]["source"] == "http://example.com/base"

    def test_skips_javascript_urls(self):
        """Should skip javascript: URLs."""
        html = '<a href="javascript:alert(1)">Link</a>'
        links = extract_links_from_html(html, "http://example.com")
        assert len(links) == 0

    def test_skips_mailto_urls(self):
        """Should skip mailto: URLs."""
        html = '<a href="mailto:test@example.com">Email</a>'
        links = extract_links_from_html(html, "http://example.com")
        assert len(links) == 0

    def test_skips_tel_urls(self):
        """Should skip tel: URLs."""
        html = '<a href="tel:+1234567890">Call</a>'
        links = extract_links_from_html(html, "http://example.com")
        assert len(links) == 0

    def test_skips_data_urls(self):
        """Should skip data: URLs."""
        html = '<a href="data:text/html,<h1>Hi</h1>">Data</a>'
        links = extract_links_from_html(html, "http://example.com")
        assert len(links) == 0

    def test_skips_fragment_only_links(self):
        """Should skip fragment-only links."""
        html = '<a href="#section">Anchor</a>'
        links = extract_links_from_html(html, "http://example.com")
        assert len(links) == 0

    def test_extracts_multiple_links(self):
        """Should extract multiple links."""
        html = '''
        <a href="/page1">Link 1</a>
        <a href="/page2">Link 2</a>
        '''
        links = extract_links_from_html(html, "http://example.com")
        assert len(links) == 2

    def test_handles_nested_tags(self):
        """Should handle nested tags in link."""
        html = '<a href="/page"><span><img src="icon.png">Text</span></a>'
        links = extract_links_from_html(html, "http://example.com")
        assert len(links) == 1
        assert "Text" in links[0]["text"]

    def test_only_http_https_urls(self):
        """Should only return http/https URLs."""
        html = '''
        <a href="http://example.com">HTTP</a>
        <a href="https://example.com">HTTPS</a>
        <a href="ftp://example.com">FTP</a>
        '''
        links = extract_links_from_html(html, "http://example.com")
        urls = [link["url"] for link in links]
        assert "http://example.com" in urls
        assert "https://example.com" in urls
        assert "ftp://example.com" not in urls


class TestCheckUrl:
    @pytest.fixture
    def mock_client(self):
        """Create a mock HTTP client."""
        return httpx.AsyncClient()

    async def test_check_url_ok(self, httpx_mock):
        """Should return ok=True for 200 response."""
        httpx_mock.add_response(url="http://example.com/page", method="HEAD", status_code=200)

        async with httpx.AsyncClient() as client:
            result = await check_url(client, "http://example.com/page")

        assert result["ok"] is True
        assert result["status"] == 200
        assert result["redirect"] is False

    async def test_check_url_404(self, httpx_mock):
        """Should return ok=False for 404 response."""
        httpx_mock.add_response(url="http://example.com/notfound", method="HEAD", status_code=404)

        async with httpx.AsyncClient() as client:
            result = await check_url(client, "http://example.com/notfound")

        assert result["ok"] is False
        assert result["status"] == 404

    async def test_check_url_redirect_detected(self, httpx_mock):
        """Should detect redirects when final_url differs from original."""
        # Simulate a redirect by returning a different URL in the response
        httpx_mock.add_response(
            url="http://example.com/old",
            method="HEAD",
            status_code=301,
            headers={"Location": "http://example.com/new"},
        )
        httpx_mock.add_response(
            url="http://example.com/new",
            method="HEAD",
            status_code=200,
        )

        async with httpx.AsyncClient(follow_redirects=True) as client:
            result = await check_url(client, "http://example.com/old")

        assert result["ok"] is True
        assert result["redirect"] is True
        assert result["final_url"] == "http://example.com/new"

    async def test_check_url_timeout(self, httpx_mock):
        """Should handle timeout."""
        httpx_mock.add_exception(
            httpx.TimeoutException("Connection timed out"),
            url="http://example.com/slow",
            method="HEAD",
        )

        async with httpx.AsyncClient() as client:
            result = await check_url(client, "http://example.com/slow")

        assert result["ok"] is False
        assert result["status"] == 0
        assert result["error"] == "timeout"

    async def test_check_url_connection_error(self, httpx_mock):
        """Should handle connection error."""
        httpx_mock.add_exception(
            httpx.ConnectError("Connection refused"),
            url="http://example.com/down",
            method="HEAD",
        )

        async with httpx.AsyncClient() as client:
            result = await check_url(client, "http://example.com/down")

        assert result["ok"] is False
        assert result["status"] == 0
        assert "error" in result

    async def test_check_url_3xx_is_ok(self, httpx_mock):
        """3xx responses should be ok after redirect."""
        httpx_mock.add_response(url="http://example.com/redirect", method="HEAD", status_code=301)

        async with httpx.AsyncClient() as client:
            result = await check_url(client, "http://example.com/redirect")

        # 301 < 400, so should be ok
        assert result["ok"] is True

    async def test_check_url_includes_final_url(self, httpx_mock):
        """Should include final URL after redirects."""
        httpx_mock.add_response(url="http://example.com/page", method="HEAD", status_code=200)

        async with httpx.AsyncClient() as client:
            result = await check_url(client, "http://example.com/page")

        assert "final_url" in result
