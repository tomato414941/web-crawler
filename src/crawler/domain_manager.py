"""Domain manager for robots.txt handling and rate limiting."""

import asyncio
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx
from robotexclusionrulesparser import RobotExclusionRulesParser


@dataclass
class DomainState:
    """State tracking for a single domain."""
    domain: str
    robots_parser: RobotExclusionRulesParser | None = None
    robots_fetched: bool = False
    last_request_time: float = 0.0
    request_count: int = 0
    error_count: int = 0
    crawl_delay: float = 1.0


class DomainManager:
    """Manages per-domain state including robots.txt and rate limiting."""

    def __init__(
        self,
        user_agent: str = "WebCrawler/0.1",
        default_delay: float = 1.0,
        respect_robots: bool = True,
        max_retries: int = 3,
    ):
        self.user_agent = user_agent
        self.default_delay = default_delay
        self.respect_robots = respect_robots
        self.max_retries = max_retries
        self._domains: dict[str, DomainState] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        return urlparse(url).netloc

    async def _get_lock(self, domain: str) -> asyncio.Lock:
        """Get or create lock for domain."""
        if domain not in self._locks:
            self._locks[domain] = asyncio.Lock()
        return self._locks[domain]

    async def get_state(self, url: str) -> DomainState:
        """Get or create state for a domain."""
        domain = self._get_domain(url)

        if domain not in self._domains:
            self._domains[domain] = DomainState(
                domain=domain,
                crawl_delay=self.default_delay,
            )

        state = self._domains[domain]

        if self.respect_robots and not state.robots_fetched:
            await self._fetch_robots(state, url)

        return state

    async def _fetch_robots(self, state: DomainState, url: str):
        """Fetch and parse robots.txt for a domain."""
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(robots_url)
                if resp.status_code == 200:
                    parser = RobotExclusionRulesParser()
                    parser.parse(resp.text)
                    state.robots_parser = parser

                    # Get crawl delay if specified
                    delay = parser.get_crawl_delay(self.user_agent)
                    if delay:
                        state.crawl_delay = max(delay, self.default_delay)
        except Exception:
            pass  # robots.txt not available or error

        state.robots_fetched = True

    async def is_allowed(self, url: str) -> bool:
        """Check if URL is allowed by robots.txt (async version)."""
        if not self.respect_robots:
            return True

        # Ensure robots.txt is fetched before checking
        state = await self.get_state(url)

        if not state.robots_parser:
            return True  # No robots.txt means everything is allowed

        return state.robots_parser.is_allowed(self.user_agent, url)

    async def wait_for_rate_limit(self, url: str):
        """Wait if needed to respect rate limit."""
        domain = self._get_domain(url)
        state = await self.get_state(url)
        lock = await self._get_lock(domain)

        async with lock:
            now = time.time()
            elapsed = now - state.last_request_time
            wait_time = state.crawl_delay - elapsed

            if wait_time > 0:
                await asyncio.sleep(wait_time)

            state.last_request_time = time.time()
            state.request_count += 1

    def record_error(self, url: str):
        """Record an error for a domain."""
        domain = self._get_domain(url)
        if domain in self._domains:
            self._domains[domain].error_count += 1

    def should_retry(self, url: str) -> bool:
        """Check if we should retry requests to this domain."""
        domain = self._get_domain(url)
        state = self._domains.get(domain)
        if not state:
            return True
        return state.error_count < self.max_retries

    def get_stats(self) -> dict:
        """Get statistics for all domains."""
        return {
            domain: {
                "request_count": state.request_count,
                "error_count": state.error_count,
                "crawl_delay": state.crawl_delay,
            }
            for domain, state in self._domains.items()
        }
