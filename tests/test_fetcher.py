"""Tests for HttpFetcher."""

import ssl

import pytest

from crawler.core import HttpFetcher, Response
from crawler.tls import build_ssl_context


@pytest.fixture
def fetcher():
    return HttpFetcher(timeout=10.0)


class TestHttpFetcher:
    async def test_fetcher_uses_certifi_bundle(self, monkeypatch):
        """Fetcher should pin the CA bundle for stable TLS verification."""
        captured: dict = {}

        class DummyClient:
            async def get(self, url):
                raise AssertionError("network call not expected")

            async def aclose(self):
                return None

        def fake_async_client(*args, **kwargs):
            captured.update(kwargs)
            return DummyClient()

        monkeypatch.setattr("crawler.core.fetcher.httpx.AsyncClient", fake_async_client)

        fetcher = HttpFetcher(timeout=10.0)
        await fetcher._get_client()

        context = captured["verify"]
        assert isinstance(context, ssl.SSLContext)
        assert context.cert_store_stats() == build_ssl_context().cert_store_stats()

    async def test_fetch_returns_response(self, fetcher, httpx_mock):
        """Fetch returns a Response with correct fields."""
        httpx_mock.add_response(
            url="https://example.com",
            status_code=200,
            html="<html><head><title>Example Host</title></head><body></body></html>",
            headers={"content-type": "text/html; charset=utf-8"},
        )
        response = await fetcher.fetch("https://example.com")

        assert isinstance(response, Response)
        assert response.status == 200
        assert "example.com" in response.url
        assert "Example Host" in response.text
        assert "text/html" in response.headers.get("content-type", "")
        assert response.fetch_request_ms >= 0
        assert response.fetch_body_read_ms >= 0
        assert response.telemetry is not None
        assert response.telemetry.outcome == "ok"
        assert response.telemetry.status == 200
        assert response.telemetry.response_headers_ms >= 0
        assert response.telemetry.body_read_ms >= 0

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

    async def test_fetch_skips_binary_body_after_headers(self, monkeypatch):
        """Binary response bodies should not be read."""
        body_read = False

        class DummyResponse:
            url = "https://example.com/live.mp3"
            status_code = 200
            headers = {"content-type": "audio/mpeg"}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def aiter_bytes(self):
                nonlocal body_read
                body_read = True
                yield b"should-not-be-read"

        class DummyClient:
            def stream(self, method, url):
                assert method == "GET"
                assert url == "https://example.com/live.mp3"
                return DummyResponse()

            async def aclose(self):
                return None

        monkeypatch.setattr(
            "crawler.core.fetcher.httpx.AsyncClient",
            lambda *args, **kwargs: DummyClient(),
        )

        fetcher = HttpFetcher(timeout=10.0)
        response = await fetcher.fetch("https://example.com/live.mp3")

        assert response.metadata_only is True
        assert response.content == b""
        assert response.admission_reason == "binary_content_type"
        assert response.telemetry is not None
        assert response.telemetry.metadata_only is True
        assert response.telemetry.admission_reason == "binary_content_type"
        assert body_read is False

    async def test_fetch_truncates_unbounded_body(self, monkeypatch):
        """Unknown-size bodies should be bounded by max_body_bytes."""

        class DummyResponse:
            url = "https://example.com/page"
            status_code = 200
            headers = {"content-type": "text/html"}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def aiter_bytes(self):
                yield b"abc"
                yield b"defghij"

        class DummyClient:
            def stream(self, method, url):
                return DummyResponse()

            async def aclose(self):
                return None

        monkeypatch.setattr(
            "crawler.core.fetcher.httpx.AsyncClient",
            lambda *args, **kwargs: DummyClient(),
        )

        fetcher = HttpFetcher(timeout=10.0, max_body_bytes=5, body_timeout=1.0)
        response = await fetcher.fetch("https://example.com/page")

        assert response.content == b"abcde"
        assert response.body_truncated is True
        assert response.admission_reason == "body_truncated"
        assert response.content_length == 5
        assert response.telemetry is not None
        assert response.telemetry.body_truncated is True
        assert response.telemetry.bytes_read == 5


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
