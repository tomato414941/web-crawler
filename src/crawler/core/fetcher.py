"""HTTP fetcher implementation using httpx."""

import asyncio

import httpx

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
    ):
        self.timeout = httpx.Timeout(timeout)
        self.user_agent = user_agent
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
                    )
        return self._client

    async def fetch(self, url: str) -> Response:
        """Fetch a URL and return the response."""
        client = await self._get_client()
        resp = await client.get(url)
        return Response(
            url=str(resp.url),
            status=resp.status_code,
            content=resp.content,
            headers=dict(resp.headers),
        )

    async def close(self):
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
