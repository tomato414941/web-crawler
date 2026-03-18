"""Tests for HttpFetcher."""

import pytest

from crawler.core import HttpFetcher, Response


@pytest.fixture
def fetcher():
    return HttpFetcher(timeout=10.0)


class TestHttpFetcher:
    async def test_fetch_returns_response(self, fetcher, httpx_mock):
        """Fetch returns a Response with correct fields."""
        httpx_mock.add_response(
            url="https://example.com",
            status_code=200,
            html='<html><head><title>Example Domain</title></head><body></body></html>',
            headers={"content-type": "text/html; charset=utf-8"},
        )
        response = await fetcher.fetch("https://example.com")

        assert isinstance(response, Response)
        assert response.status == 200
        assert "example.com" in response.url
        assert "Example Domain" in response.text
        assert "text/html" in response.headers.get("content-type", "")

    async def test_fetch_returns_response_fields(self, fetcher, httpx_mock):
        """Verify all response fields are populated."""
        httpx_mock.add_response(
            url="https://example.com",
            status_code=200,
            html="<html></html>",
            headers={"content-type": "text/html"},
        )
        response = await fetcher.fetch("https://example.com")

        assert response.url is not None
        assert response.status > 0
        assert response.content is not None
        assert isinstance(response.headers, dict)

    async def test_fetch_follows_redirects(self, fetcher, httpx_mock):
        """Verify redirects are followed."""
        httpx_mock.add_response(
            url="http://example.com/old",
            status_code=301,
            headers={"location": "https://example.com/new"},
        )
        httpx_mock.add_response(
            url="https://example.com/new",
            status_code=200,
            html="<html>redirected</html>",
        )
        response = await fetcher.fetch("http://example.com/old")

        assert "example.com/new" in response.url
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
        assert isinstance(response.text, str)
