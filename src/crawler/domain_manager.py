"""Domain manager for robots.txt handling and rate limiting."""

import asyncio
import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import httpx
from robotexclusionrulesparser import RobotExclusionRulesParser

from .domain_state import PersistedDomainState, RuntimeDomainState

if TYPE_CHECKING:
    from .domain_store import DomainStore

# Default TTL for robots.txt cache (1 hour)
ROBOTS_CACHE_TTL = 3600.0
DEFAULT_HOST_BACKOFF_SECONDS = 30.0
MAX_HOST_BACKOFF_SECONDS = 600.0

DomainState = RuntimeDomainState

__all__ = [
    "DomainManager",
    "DomainState",
    "RuntimeDomainState",
    "PersistedDomainState",
    "ROBOTS_CACHE_TTL",
]


class DomainManager:
    """Manages per-host runtime state including robots.txt and rate limiting."""

    def __init__(
        self,
        user_agent: str = "WebCrawler/0.1",
        default_delay: float = 1.0,
        respect_robots: bool = True,
        max_retries: int = 3,
        robots_cache_ttl: float = ROBOTS_CACHE_TTL,
        domain_store: "DomainStore | None" = None,
        host_backoff_seconds: float = DEFAULT_HOST_BACKOFF_SECONDS,
        max_host_backoff_seconds: float = MAX_HOST_BACKOFF_SECONDS,
    ):
        self.user_agent = user_agent
        self.default_delay = default_delay
        self.respect_robots = respect_robots
        self.max_retries = max_retries
        self.robots_cache_ttl = robots_cache_ttl
        self._domain_store = domain_store
        self._host_backoff_seconds = host_backoff_seconds
        self._max_host_backoff_seconds = max_host_backoff_seconds
        self._runtime_states: dict[str, RuntimeDomainState] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._client: httpx.AsyncClient | None = None
        self._client_lock = asyncio.Lock()
        self._domains = self._runtime_states

    def _get_host_key(self, url: str) -> str:
        """Extract the host key used for per-host scheduling."""
        return urlparse(url).netloc

    async def _get_lock(self, host_key: str) -> asyncio.Lock:
        """Get or create a lock for a host key."""
        if host_key not in self._locks:
            self._locks[host_key] = asyncio.Lock()
        return self._locks[host_key]

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create shared HTTP client for robots.txt fetching."""
        if self._client is None:
            async with self._client_lock:
                if self._client is None:
                    self._client = httpx.AsyncClient(
                        timeout=10.0,
                        headers={"User-Agent": self.user_agent},
                    )
        return self._client

    def _is_robots_cache_valid(self, state: RuntimeDomainState) -> bool:
        """Check if robots.txt cache is still valid."""
        if not state.has_checked_robots:
            return False
        elapsed = time.time() - state.robots_checked_at
        return elapsed < self.robots_cache_ttl

    def _build_runtime_state(self, host_key: str) -> RuntimeDomainState:
        """Create a new in-memory runtime state."""
        return RuntimeDomainState(
            host_key=host_key,
            crawl_delay_seconds=self.default_delay,
        )

    def _apply_persisted_state(
        self,
        runtime_state: RuntimeDomainState,
        persisted_state: PersistedDomainState,
    ) -> RuntimeDomainState:
        """Copy durable scheduling fields into runtime state."""
        runtime_state.crawl_delay_seconds = persisted_state.crawl_delay_seconds
        runtime_state.consecutive_failures = persisted_state.consecutive_failures
        return runtime_state

    def _compute_host_backoff(self, consecutive_failures: int) -> float:
        """Compute exponential host cooldown after failures."""
        base = max(self._host_backoff_seconds, 0.0)
        if consecutive_failures <= 1:
            return base
        delay = base * (2 ** (consecutive_failures - 1))
        return min(delay, self._max_host_backoff_seconds)

    def attach_store(self, domain_store: "DomainStore | None") -> None:
        """Attach or replace the durable domain store."""
        self._domain_store = domain_store

    def build_persisted_state(self, host_key: str) -> PersistedDomainState:
        """Create the durable state shape that P2 will persist."""
        return PersistedDomainState(
            host_key=host_key,
            crawl_delay_seconds=self.default_delay,
        )

    async def get_state(self, url: str) -> RuntimeDomainState:
        """Get or create runtime state for a host key."""
        host_key = self._get_host_key(url)

        if host_key not in self._runtime_states:
            runtime_state = self._build_runtime_state(host_key)
            if self._domain_store is not None:
                persisted_state = self._domain_store.get_or_create(host_key)
                runtime_state = self._apply_persisted_state(runtime_state, persisted_state)
            self._runtime_states[host_key] = runtime_state

        state = self._runtime_states[host_key]

        if self.respect_robots and not self._is_robots_cache_valid(state):
            await self._fetch_robots(state, url)

        return state

    async def _fetch_robots(self, state: RuntimeDomainState, url: str):
        """Fetch and parse robots.txt for a host key using the shared client."""
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

        try:
            client = await self._get_client()
            resp = await client.get(robots_url)
            if resp.status_code == 200:
                parser = RobotExclusionRulesParser()
                parser.parse(resp.text)
                state.robots_parser = parser

                # Get crawl delay if specified
                delay = parser.get_crawl_delay(self.user_agent)
                if delay:
                    state.crawl_delay_seconds = max(delay, self.default_delay)
        except Exception:
            pass  # robots.txt not available or error

        checked_at = time.time()
        state.has_checked_robots = True
        state.robots_checked_at = checked_at
        if self._domain_store is not None:
            persisted_state = self._domain_store.update_robots(
                state.host_key,
                crawl_delay_seconds=state.crawl_delay_seconds,
                checked_at=checked_at,
            )
            self._apply_persisted_state(state, persisted_state)

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
        host_key = self._get_host_key(url)
        state = await self.get_state(url)
        lock = await self._get_lock(host_key)

        async with lock:
            if self._domain_store is not None:
                wait_time, persisted_state = self._domain_store.reserve_request_slot(
                    host_key,
                    crawl_delay_seconds=state.crawl_delay_seconds,
                )
                self._apply_persisted_state(state, persisted_state)
            else:
                now = time.time()
                elapsed = now - state.last_request_started_at
                wait_time = state.crawl_delay_seconds - elapsed

            if wait_time > 0:
                await asyncio.sleep(wait_time)

            state.last_request_started_at = time.time()
            state.request_count += 1

    def record_error(self, url: str):
        """Record a failed attempt for a host key."""
        host_key = self._get_host_key(url)
        if host_key in self._runtime_states:
            state = self._runtime_states[host_key]
            state.consecutive_failures += 1
            if self._domain_store is not None:
                persisted_state = self._domain_store.record_failure(
                    host_key,
                    backoff_seconds=self._compute_host_backoff(state.consecutive_failures),
                )
                self._apply_persisted_state(state, persisted_state)

    def record_success(self, url: str):
        """Record a successful request for a host key."""
        host_key = self._get_host_key(url)
        if host_key in self._runtime_states:
            state = self._runtime_states[host_key]
            state.consecutive_failures = 0
            if self._domain_store is not None:
                persisted_state = self._domain_store.record_success(host_key)
                self._apply_persisted_state(state, persisted_state)

    def should_retry(self, url: str) -> bool:
        """Check if we should retry requests to this host key."""
        host_key = self._get_host_key(url)
        state = self._runtime_states.get(host_key)
        if not state:
            return True
        return state.consecutive_failures < self.max_retries

    def get_stats(self) -> dict:
        """Get statistics for all host keys."""
        return {
            host_key: {
                "request_count": state.request_count,
                "consecutive_failures": state.consecutive_failures,
                "crawl_delay_seconds": state.crawl_delay_seconds,
                "robots_checked_at": state.robots_checked_at,
            }
            for host_key, state in self._runtime_states.items()
        }

    async def close(self):
        """Close the shared HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
