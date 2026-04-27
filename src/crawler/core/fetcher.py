"""HTTP fetcher implementation using httpx."""

import asyncio
import time
from urllib.parse import urljoin

import httpx

from ..config import settings
from ..content_policy import should_fetch_body
from ..egress_guard import AddressResolver, check_url, raise_if_blocked
from ..telemetry import FetchTelemetry
from ..tls import build_ssl_context
from .protocols import Response

DEFAULT_USER_AGENT = "WebCrawler/0.1 (+https://github.com/web-crawler)"
MAX_REDIRECTS = 20


class HttpFetcher:
    """Async HTTP fetcher using httpx with connection reuse."""

    def __init__(
        self,
        timeout: float = 10.0,
        user_agent: str = DEFAULT_USER_AGENT,
        max_connections: int = 100,
        max_keepalive_connections: int = 20,
        max_body_bytes: int | None = None,
        body_timeout: float | None = None,
        egress_resolver: AddressResolver | None = None,
    ):
        self.timeout = httpx.Timeout(timeout)
        self.user_agent = user_agent
        self.max_body_bytes = (
            settings.max_response_body_bytes if max_body_bytes is None else max_body_bytes
        )
        self.body_timeout = settings.fetch_body_timeout if body_timeout is None else body_timeout
        self.egress_resolver = egress_resolver
        self.limits = httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
        )
        self._client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client with double-checked locking."""
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    self._client = httpx.AsyncClient(
                        timeout=self.timeout,
                        limits=self.limits,
                        headers={"User-Agent": self.user_agent},
                        follow_redirects=False,
                        verify=build_ssl_context(),
                    )
        return self._client

    @staticmethod
    def _header_value(headers: dict[str, str], name: str) -> str | None:
        lower_name = name.lower()
        for key, value in headers.items():
            if key.lower() == lower_name:
                return value
        return None

    @classmethod
    def _content_length(cls, headers: dict[str, str]) -> int | None:
        value = cls._header_value(headers, "content-length")
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    async def _read_bounded_body(self, resp: httpx.Response) -> tuple[bytes, bool]:
        chunks: list[bytes] = []
        total = 0
        async for chunk in resp.aiter_bytes():
            if not chunk:
                continue
            remaining = self.max_body_bytes - total
            if remaining <= 0:
                return b"".join(chunks), True
            if len(chunk) > remaining:
                chunks.append(chunk[:remaining])
                return b"".join(chunks), True
            chunks.append(chunk)
            total += len(chunk)
        return b"".join(chunks), False

    async def fetch(self, url: str) -> Response:
        """Fetch a URL and return the response."""
        client = await self._get_client()
        total_started = time.perf_counter()
        current_url = url
        redirect_count = 0
        guard = await check_url(
            current_url,
            resolver=self.egress_resolver,
            allow_private_network_egress=settings.allow_private_network_egress,
        )
        raise_if_blocked(guard)

        while True:
            request_started = time.perf_counter()
            async with client.stream("GET", current_url) as resp:
                if 300 <= resp.status_code < 400 and "location" in resp.headers:
                    if redirect_count >= MAX_REDIRECTS:
                        raise httpx.TooManyRedirects(
                            f"Exceeded maximum redirects ({MAX_REDIRECTS})",
                            request=resp.request,
                        )
                    next_url = urljoin(str(resp.url), resp.headers["location"])
                    guard = await check_url(
                        next_url,
                        resolver=self.egress_resolver,
                        allow_private_network_egress=settings.allow_private_network_egress,
                    )
                    raise_if_blocked(guard)
                    current_url = next_url
                    redirect_count += 1
                    continue
                return await self._build_response(
                    resp=resp,
                    total_started=total_started,
                    request_started=request_started,
                    redirect_count=redirect_count,
                )

    async def _build_response(
        self,
        *,
        resp: httpx.Response,
        total_started: float,
        request_started: float,
        redirect_count: int,
    ) -> Response:
        fetch_request_ms = round((time.perf_counter() - request_started) * 1000, 1)
        headers = dict(resp.headers)
        content_length = self._content_length(headers)
        decision = should_fetch_body(
            self._header_value(headers, "content-type"),
            content_length,
            str(resp.url),
            max_body_bytes=self.max_body_bytes,
        )
        if not decision.should_read:
            outcome = (
                "too_large" if decision.reason == "content_length_too_large" else "metadata_only"
            )
            telemetry = FetchTelemetry(
                outcome=outcome,
                status=resp.status_code,
                final_url=str(resp.url),
                redirect_count=redirect_count,
                content_length=content_length,
                bytes_read=0,
                metadata_only=decision.metadata_only,
                admission_reason=decision.reason,
                total_ms=round((time.perf_counter() - total_started) * 1000, 1),
                response_headers_ms=fetch_request_ms,
                body_read_ms=0.0,
            )
            return Response(
                url=str(resp.url),
                status=resp.status_code,
                content=b"",
                headers=headers,
                fetch_request_ms=fetch_request_ms,
                fetch_body_read_ms=0.0,
                content_length=content_length,
                metadata_only=decision.metadata_only,
                admission_reason=decision.reason,
                telemetry=telemetry,
            )

        body_started = time.perf_counter()
        content, body_truncated = await asyncio.wait_for(
            self._read_bounded_body(resp),
            timeout=self.body_timeout,
        )
        fetch_body_read_ms = round((time.perf_counter() - body_started) * 1000, 1)
        telemetry = FetchTelemetry(
            outcome="http_error" if resp.status_code >= 400 else "ok",
            status=resp.status_code,
            final_url=str(resp.url),
            redirect_count=redirect_count,
            content_length=content_length if content_length is not None else len(content),
            bytes_read=len(content),
            body_truncated=body_truncated,
            admission_reason="body_truncated" if body_truncated else None,
            total_ms=round((time.perf_counter() - total_started) * 1000, 1),
            response_headers_ms=fetch_request_ms,
            body_read_ms=fetch_body_read_ms,
        )
        return Response(
            url=str(resp.url),
            status=resp.status_code,
            content=content,
            headers=headers,
            fetch_request_ms=fetch_request_ms,
            fetch_body_read_ms=fetch_body_read_ms,
            content_length=content_length if content_length is not None else len(content),
            body_truncated=body_truncated,
            admission_reason="body_truncated" if body_truncated else None,
            telemetry=telemetry,
        )

    async def close(self):
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
