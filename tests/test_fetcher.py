"""Tests for HttpFetcher."""

import ssl

import pytest

from crawler.config import CrawlerSettings
from crawler.egress_guard import EgressBlockedError
from crawler.core import HttpFetcher, Response
from crawler.tls import build_ssl_context


@pytest.fixture
def fetcher():
    return HttpFetcher(timeout=10.0)


class TestHttpFetcher:
    def test_settings_reject_require_proxy_without_proxy(self):
        with pytest.raises(ValueError, match="CRAWLER_REQUIRE_EGRESS_PROXY"):
            CrawlerSettings(require_egress_proxy=True, egress_proxy=None)

    def test_settings_reject_direct_disallowed_without_proxy(self):
        with pytest.raises(ValueError, match="CRAWLER_DIRECT_EGRESS_ALLOWED"):
            CrawlerSettings(direct_egress_allowed=False, egress_proxy=None)

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

    async def test_fetcher_uses_configured_proxy_policy(self, monkeypatch):
        """Fetcher should pass the shared explicit proxy policy to httpx."""
        captured: dict = {}

        class DummyClient:
            async def aclose(self):
                return None

        def fake_async_client(*args, **kwargs):
            captured.update(kwargs)
            return DummyClient()

        monkeypatch.setattr("crawler.core.fetcher.httpx.AsyncClient", fake_async_client)
        monkeypatch.setattr("crawler.core.fetcher.settings.egress_proxy", "http://proxy.local:8080")
        monkeypatch.setattr("crawler.core.fetcher.settings.require_egress_proxy", True)
        monkeypatch.setattr("crawler.core.fetcher.settings.direct_egress_allowed", False)

        fetcher = HttpFetcher(timeout=10.0)
        await fetcher._get_client()

        assert captured["proxy"] == "http://proxy.local:8080"
        assert captured["trust_env"] is False

    def test_fetcher_fails_fast_when_direct_disallowed_without_proxy(self, monkeypatch):
        monkeypatch.setattr("crawler.core.fetcher.settings.egress_proxy", None)
        monkeypatch.setattr("crawler.core.fetcher.settings.require_egress_proxy", False)
        monkeypatch.setattr("crawler.core.fetcher.settings.direct_egress_allowed", False)

        with pytest.raises(ValueError, match="CRAWLER_DIRECT_EGRESS_ALLOWED"):
            HttpFetcher(timeout=10.0)

    async def test_fetcher_passes_configured_proxy_to_httpx(self, monkeypatch):
        """Fetcher should route HTTP through the configured egress proxy."""
        captured: dict = {}

        class DummyClient:
            async def aclose(self):
                return None

        def fake_async_client(*args, **kwargs):
            captured.update(kwargs)
            return DummyClient()

        monkeypatch.setattr("crawler.core.fetcher.settings.egress_proxy", "http://proxy:3128")
        monkeypatch.setattr("crawler.core.fetcher.httpx.AsyncClient", fake_async_client)

        fetcher = HttpFetcher(timeout=10.0)
        await fetcher._get_client()

        assert captured["proxy"] == "http://proxy:3128"

    def test_fetcher_fails_fast_when_proxy_required_without_proxy(self, monkeypatch):
        """Proxy-required runtime settings should fail before fetching starts."""
        monkeypatch.setattr("crawler.core.fetcher.settings.egress_proxy", None)
        monkeypatch.setattr("crawler.core.fetcher.settings.require_egress_proxy", True)

        with pytest.raises(ValueError, match="CRAWLER_REQUIRE_EGRESS_PROXY"):
            HttpFetcher(timeout=10.0)

    def test_fetcher_fails_fast_when_direct_disabled_without_proxy(self, monkeypatch):
        """Disabling direct egress requires a configured proxy."""
        monkeypatch.setattr("crawler.core.fetcher.settings.egress_proxy", None)
        monkeypatch.setattr("crawler.core.fetcher.settings.direct_egress_allowed", False)

        with pytest.raises(ValueError, match="CRAWLER_DIRECT_EGRESS_ALLOWED"):
            HttpFetcher(timeout=10.0)

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

    async def test_fetch_blocks_private_ip_before_request(self, monkeypatch):
        """Private targets should be rejected before any outbound HTTP request is started."""

        class DummyClient:
            def stream(self, method, url):
                raise AssertionError("network request should not be attempted for blocked egress")

            async def aclose(self):
                return None

        monkeypatch.setattr(
            "crawler.core.fetcher.httpx.AsyncClient",
            lambda *args, **kwargs: DummyClient(),
        )

        fetcher = HttpFetcher(timeout=10.0)
        with pytest.raises(EgressBlockedError) as exc_info:
            await fetcher.fetch("http://127.0.0.1:8080/admin")

        assert exc_info.value.decision.reason == "blocked_ip_literal"

    async def test_fetch_blocks_metadata_endpoint_before_request(self, monkeypatch):
        """Metadata endpoint targets should be rejected before starting a request."""

        class DummyClient:
            def stream(self, method, url):
                raise AssertionError("network request should not be attempted for blocked egress")

            async def aclose(self):
                return None

        monkeypatch.setattr(
            "crawler.core.fetcher.httpx.AsyncClient",
            lambda *args, **kwargs: DummyClient(),
        )

        fetcher = HttpFetcher(timeout=10.0)
        with pytest.raises(EgressBlockedError) as exc_info:
            await fetcher.fetch("http://169.254.169.254/latest/meta-data/")

        assert exc_info.value.decision.reason == "blocked_ip_literal"

    async def test_fetch_blocks_private_redirect_before_following(self, httpx_mock):
        """Redirects to private targets should be rejected before following them."""
        httpx_mock.add_response(
            url="http://example.com/old",
            status_code=302,
            headers={"location": "http://127.0.0.1/admin"},
        )

        async def resolver(_hostname: str, _port: int | None) -> list[str]:
            return ["93.184.216.34"]

        fetcher = HttpFetcher(timeout=10.0, egress_resolver=resolver)
        with pytest.raises(EgressBlockedError) as exc_info:
            await fetcher.fetch("http://example.com/old")

        assert exc_info.value.decision.reason == "blocked_ip_literal"
        assert len(httpx_mock.get_requests()) == 1

    async def test_fetch_raises_on_redirect_limit(self, httpx_mock):
        """Redirect loops should fail with an explicit redirect error."""
        for _ in range(21):
            httpx_mock.add_response(
                url="http://example.com/loop",
                status_code=302,
                headers={"location": "http://example.com/loop"},
            )

        async def resolver(_hostname: str, _port: int | None) -> list[str]:
            return ["93.184.216.34"]

        fetcher = HttpFetcher(timeout=10.0, egress_resolver=resolver)
        with pytest.raises(Exception) as exc_info:
            await fetcher.fetch("http://example.com/loop")

        assert "redirect" in type(exc_info.value).__name__.lower()


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
