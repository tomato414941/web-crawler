"""Browser-based fetcher using Playwright."""

import asyncio

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

from .protocols import Response


class BrowserPool:
    """Manages a pool of browser pages for reuse."""

    def __init__(
        self,
        pool_size: int = 3,
        headless: bool = True,
        user_agent: str | None = None,
    ):
        self.pool_size = pool_size
        self.headless = headless
        self.user_agent = user_agent
        self._playwright = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._pages: asyncio.Queue[Page] = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._initialized = False

    async def _initialize(self):
        """Initialize browser and page pool."""
        if self._initialized:
            return

        async with self._lock:
            if self._initialized:
                return

            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=self.headless)

            context_opts = {}
            if self.user_agent:
                context_opts["user_agent"] = self.user_agent
            self._context = await self._browser.new_context(**context_opts)

            for _ in range(self.pool_size):
                page = await self._context.new_page()
                await self._pages.put(page)

            self._initialized = True

    async def acquire(self) -> Page:
        """Acquire a page from the pool."""
        await self._initialize()
        return await self._pages.get()

    async def release(self, page: Page):
        """Release a page back to the pool."""
        await self._pages.put(page)

    async def close(self):
        """Close all browser resources."""
        if self._browser is not None:
            await self._browser.close()
            self._browser = None
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None
        self._initialized = False


class BrowserFetcher:
    """Async browser fetcher using Playwright for JavaScript rendering."""

    def __init__(
        self,
        timeout: float = 30.0,
        user_agent: str | None = None,
        headless: bool = True,
        pool_size: int = 3,
    ):
        self.timeout = timeout * 1000  # Playwright uses milliseconds
        self.user_agent = user_agent
        self.headless = headless
        self._pool = BrowserPool(
            pool_size=pool_size,
            headless=headless,
            user_agent=user_agent,
        )

    async def fetch(self, url: str) -> Response:
        """Fetch a URL using a pooled browser page."""
        page = await self._pool.acquire()
        try:
            response = await page.goto(url, timeout=self.timeout, wait_until="networkidle")

            content = await page.content()
            final_url = page.url

            headers = {}
            status = 200
            if response:
                headers = await response.all_headers()
                status = response.status

            return Response(
                url=final_url,
                status=status,
                content=content.encode("utf-8"),
                headers=headers,
            )
        finally:
            await self._pool.release(page)

    async def fetch_with_snapshot(self, url: str) -> tuple[Response, str]:
        """Fetch URL and return accessibility tree snapshot for AI agents."""
        page = await self._pool.acquire()
        try:
            response = await page.goto(url, timeout=self.timeout, wait_until="networkidle")

            content = await page.content()
            final_url = page.url

            # Get accessibility tree snapshot
            snapshot = await page.accessibility.snapshot()

            headers = {}
            status = 200
            if response:
                headers = await response.all_headers()
                status = response.status

            resp = Response(
                url=final_url,
                status=status,
                content=content.encode("utf-8"),
                headers=headers,
            )

            return resp, _format_a11y_tree(snapshot) if snapshot else ""
        finally:
            await self._pool.release(page)

    async def close(self):
        """Close the browser pool."""
        await self._pool.close()


def _format_a11y_tree(node: dict, prefix: str = "", counter: list | None = None) -> str:
    """Format accessibility tree with semantic references (@e1, @e2, etc.)."""
    if counter is None:
        counter = [0]

    lines = []
    role = node.get("role", "")
    name = node.get("name", "")

    if role and role != "none":
        counter[0] += 1
        ref = f"@e{counter[0]}"
        label = f"{role}"
        if name:
            label += f' "{name}"'
        lines.append(f"{prefix}{ref} {label}")

    children = node.get("children", [])
    for child in children:
        lines.append(_format_a11y_tree(child, prefix + "  ", counter))

    return "\n".join(filter(None, lines))
