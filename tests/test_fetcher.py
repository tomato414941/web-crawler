"""Tests for HttpFetcher."""

import pytest

from crawler.core import HttpFetcher, Response


@pytest.fixture
def fetcher():
    return HttpFetcher(timeout=10.0)


class TestHttpFetcher:
    async def test_fetch_example_com(self, fetcher):
        """Fetch example.com and verify response."""
        response = await fetcher.fetch("https://example.com")

        assert isinstance(response, Response)
        assert response.status == 200
        assert "example.com" in response.url
        assert "Example Domain" in response.text
        assert "text/html" in response.headers.get("content-type", "")

    async def test_fetch_returns_response_fields(self, fetcher):
        """Verify all response fields are populated."""
        response = await fetcher.fetch("https://example.com")

        assert response.url is not None
        assert response.status > 0
        assert response.content is not None
        assert isinstance(response.headers, dict)

    async def test_fetch_follows_redirects(self, fetcher):
        """Verify redirects are followed."""
        response = await fetcher.fetch("http://httpbin.org/redirect-to?url=https://example.com")

        assert "example.com" in response.url
        assert response.status == 200


class TestResponse:
    def test_text_property(self):
        """Verify text property decodes content."""
        response = Response(
            url="https://example.com",
            status=200,
            content=b"Hello, World!",
            headers={},
        )
        assert response.text == "Hello, World!"

    def test_text_handles_invalid_utf8(self):
        """Verify text property handles invalid UTF-8."""
        response = Response(
            url="https://example.com",
            status=200,
            content=b"\xff\xfe",
            headers={},
        )
        # Should not raise, uses replacement character
        assert isinstance(response.text, str)
