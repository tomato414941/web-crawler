"""HTTP fetcher implementation using httpx."""

import asyncio
import time

import httpx

from ..config import settings
from ..content_policy import should_fetch_body
from ..tls import build_ssl_context
from .protocols import Response

DEFAULT_USER_AGENT = "WebCrawler/0.1 (+https://github.com/web-crawler)"


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
    ):
        self.timeout = httpx.Timeout(timeout)
        self.user_agent = user_agent
        self.max_body_bytes = (
            settings.max_response_body_bytes if max_body_bytes is None else max_body_bytes
        )
        self.body_timeout = settings.fetch_body_timeout if body_timeout is None else body_timeout
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
                        follow_redirects=True,
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
        request_started = time.perf_counter()
        async with client.stream("GET", url) as resp:
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
                )

            body_started = time.perf_counter()
            content, body_truncated = await asyncio.wait_for(
                self._read_bounded_body(resp),
                timeout=self.body_timeout,
            )
            fetch_body_read_ms = round((time.perf_counter() - body_started) * 1000, 1)
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
            )

    async def close(self):
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
