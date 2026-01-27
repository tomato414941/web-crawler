"""Adaptive fetcher that chooses between HTTP and browser based on content."""

import re

from .browser_fetcher import BrowserFetcher
from .fetcher import HttpFetcher
from .protocols import Response

JS_INDICATORS = [
    re.compile(r"<noscript>", re.IGNORECASE),
    re.compile(r'data-react|data-vue|data-ng|ng-app', re.IGNORECASE),
    re.compile(r'__NEXT_DATA__|__NUXT__|window\.__INITIAL_STATE__', re.IGNORECASE),
    re.compile(r'<div id="(root|app|__next)">\s*</div>', re.IGNORECASE),
]

MINIMAL_CONTENT_THRESHOLD = 500  # bytes


class AdaptiveFetcher:
    """Fetcher that automatically switches between HTTP and browser."""

    def __init__(
        self,
        timeout: float = 10.0,
        browser_timeout: float = 30.0,
        user_agent: str | None = None,
        force_browser: bool = False,
    ):
        self.http_fetcher = HttpFetcher(timeout=timeout, user_agent=user_agent or "")
        self.browser_fetcher = BrowserFetcher(timeout=browser_timeout, user_agent=user_agent)
        self.force_browser = force_browser

    async def fetch(self, url: str) -> tuple[Response, bool]:
        """
        Fetch URL, using browser if JS rendering is needed.

        Returns:
            Tuple of (Response, used_browser: bool)
        """
        if self.force_browser:
            return await self.browser_fetcher.fetch(url), True

        # Try HTTP first
        response = await self.http_fetcher.fetch(url)

        # Check if we need browser rendering
        if self._needs_browser(response):
            return await self.browser_fetcher.fetch(url), True

        return response, False

    def _needs_browser(self, response: Response) -> bool:
        """Determine if the response likely needs JavaScript rendering."""
        text = response.text

        # Check for JS framework indicators
        for pattern in JS_INDICATORS:
            if pattern.search(text):
                return True

        # Check for minimal content (might be JS-rendered)
        # Strip scripts and styles first
        stripped = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', text, flags=re.DOTALL | re.IGNORECASE)
        stripped = re.sub(r'<[^>]+>', '', stripped)
        stripped = stripped.strip()

        if len(stripped) < MINIMAL_CONTENT_THRESHOLD:
            return True

        return False
