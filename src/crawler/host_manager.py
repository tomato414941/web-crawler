"""Host manager for robots.txt handling and rate limiting."""

import asyncio
import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import httpx
from robotexclusionrulesparser import RobotExclusionRulesParser

from .config import settings
from .host_state import PersistedHostState, RuntimeHostState
from .tls import build_ssl_context

if TYPE_CHECKING:
    from .host_ledger import HostLedgerStore
    from .host_store import HostStore

# Default TTL for robots.txt cache (1 hour)
ROBOTS_CACHE_TTL = 3600.0
DEFAULT_HOST_BACKOFF_SECONDS = 30.0
MAX_HOST_BACKOFF_SECONDS = 600.0

HostState = RuntimeHostState

__all__ = [
    "compute_host_budget",
    "HostManager",
    "HostState",
    "RuntimeHostState",
    "PersistedHostState",
    "ROBOTS_CACHE_TTL",
]


def compute_host_budget(
    *,
    latency_ewma_ms: float,
    consecutive_failures: int,
    default_budget: int,
    fast_latency_threshold_ms: float | None = None,
    fast_host_budget: int | None = None,
) -> int:
    """Return the scheduler inflight budget for a host from compact host signals."""
    budget = max(1, default_budget)
    if consecutive_failures > 0:
        return 1
    threshold_ms = (
        settings.fast_host_latency_threshold_ms
        if fast_latency_threshold_ms is None
        else fast_latency_threshold_ms
    )
    elevated_budget = (
        settings.fast_host_max_inflight_requests_per_host
        if fast_host_budget is None
        else fast_host_budget
    )
    if latency_ewma_ms > 0 and latency_ewma_ms <= threshold_ms:
        return max(budget, elevated_budget)
    return budget


class HostManager:
    """Manages per-host runtime state including robots.txt and rate limiting."""

    def __init__(
        self,
        user_agent: str = "WebCrawler/0.1",
        default_delay: float = 1.0,
        respect_robots: bool = True,
        max_retries: int = 3,
        robots_fetch_timeout: float | None = None,
        robots_cache_ttl: float | None = None,
        host_store: "HostStore | None" = None,
        host_ledger_store: "HostLedgerStore | None" = None,
        host_backoff_seconds: float | None = None,
        max_host_backoff_seconds: float | None = None,
    ):
        self.user_agent = user_agent
        self.default_delay = default_delay
        self.respect_robots = respect_robots
        self.max_retries = max_retries
        self.robots_cache_ttl = (
            settings.robots_cache_ttl if robots_cache_ttl is None else robots_cache_ttl
        )
        self.robots_fetch_timeout = (
            settings.robots_fetch_timeout
            if robots_fetch_timeout is None
            else robots_fetch_timeout
        )
        self._host_store = host_store
        self._host_ledger_store = host_ledger_store
        self._host_backoff_seconds = (
            settings.host_backoff_seconds if host_backoff_seconds is None else host_backoff_seconds
        )
        self._max_host_backoff_seconds = (
            settings.max_host_backoff_seconds
            if max_host_backoff_seconds is None
            else max_host_backoff_seconds
        )
        self._runtime_states: dict[str, RuntimeHostState] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._robots_locks: dict[str, asyncio.Lock] = {}
        self._client: httpx.AsyncClient | None = None
        self._client_lock = asyncio.Lock()
        self._hosts = self._runtime_states

    def _get_host_key(self, url: str) -> str:
        """Extract the host key used for per-host scheduling."""
        return urlparse(url).netloc

    async def _get_lock(self, host_key: str) -> asyncio.Lock:
        """Get or create a lock for a host key."""
        if host_key not in self._locks:
            self._locks[host_key] = asyncio.Lock()
        return self._locks[host_key]

    async def _get_robots_lock(self, host_key: str) -> asyncio.Lock:
        """Get or create the robots fetch lock for a host key."""
        if host_key not in self._robots_locks:
            self._robots_locks[host_key] = asyncio.Lock()
        return self._robots_locks[host_key]

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create shared HTTP client for robots.txt fetching."""
        if self._client is None:
            async with self._client_lock:
                if self._client is None:
                    self._client = httpx.AsyncClient(
                        timeout=self.robots_fetch_timeout,
                        headers={"User-Agent": self.user_agent},
                        verify=build_ssl_context(),
                    )
        return self._client

    def _is_robots_cache_valid(self, state: RuntimeHostState) -> bool:
        """Check if robots.txt cache is still valid."""
        if not state.has_checked_robots:
            return False
        elapsed = time.time() - state.robots_checked_at
        return elapsed < self.robots_cache_ttl

    def _build_runtime_state(self, host_key: str) -> RuntimeHostState:
        """Create a new in-memory runtime state."""
        return RuntimeHostState(
            host_key=host_key,
            crawl_delay_seconds=self.default_delay,
        )

    def _apply_persisted_state(
        self,
        runtime_state: RuntimeHostState,
        persisted_state: PersistedHostState,
    ) -> RuntimeHostState:
        """Copy durable scheduling fields into runtime state."""
        runtime_state.crawl_delay_seconds = persisted_state.crawl_delay_seconds
        runtime_state.consecutive_failures = persisted_state.consecutive_failures
        runtime_state.latency_ewma_ms = persisted_state.latency_ewma_ms
        runtime_state.latency_last_ms = persisted_state.latency_last_ms
        runtime_state.latency_observed_at = persisted_state.latency_observed_at
        runtime_state.latency_sample_count = persisted_state.latency_sample_count
        return runtime_state

    def _compute_host_backoff(self, consecutive_failures: int) -> float:
        """Compute exponential host cooldown after failures."""
        base = max(self._host_backoff_seconds, 0.0)
        if consecutive_failures <= 1:
            return base
        delay = base * (2 ** (consecutive_failures - 1))
        return min(delay, self._max_host_backoff_seconds)

    def attach_store(self, host_store: "HostStore | None") -> None:
        """Attach or replace the durable host store."""
        self._host_store = host_store

    def attach_host_ledger_store(self, host_ledger_store: "HostLedgerStore | None") -> None:
        """Attach or replace the durable host ledger store."""
        self._host_ledger_store = host_ledger_store

    def build_persisted_state(self, host_key: str) -> PersistedHostState:
        """Create the durable state shape that P2 will persist."""
        return PersistedHostState(
            host_key=host_key,
            crawl_delay_seconds=self.default_delay,
        )

    async def get_state(self, url: str) -> RuntimeHostState:
        """Get or create runtime state for a host key."""
        host_key = self._get_host_key(url)

        if host_key not in self._runtime_states:
            runtime_state = self._build_runtime_state(host_key)
            if self._host_store is not None:
                persisted_state = self._host_store.get_or_create(host_key)
                runtime_state = self._apply_persisted_state(runtime_state, persisted_state)
            self._runtime_states[host_key] = runtime_state

        state = self._runtime_states[host_key]

        if self.respect_robots and not self._is_robots_cache_valid(state):
            robots_lock = await self._get_robots_lock(host_key)
            async with robots_lock:
                if not self._is_robots_cache_valid(state):
                    await self._fetch_robots(state, url)

        return state

    async def _fetch_robots(self, state: RuntimeHostState, url: str):
        """Fetch and parse robots.txt for a host key using the shared client."""
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

        robots_status = "unavailable"
        try:
            client = await self._get_client()
            resp = await client.get(robots_url)
            if resp.status_code == 200:
                robots_status = "ok"
                parser = RobotExclusionRulesParser()
                parser.parse(resp.text)
                state.robots_parser = parser

                # Get crawl delay if specified
                delay = parser.get_crawl_delay(self.user_agent)
                if delay:
                    state.crawl_delay_seconds = max(delay, self.default_delay)
            else:
                robots_status = f"http_{resp.status_code}"
        except Exception:
            robots_status = "error"
            pass  # robots.txt not available or error

        checked_at = time.time()
        state.has_checked_robots = True
        state.robots_checked_at = checked_at
        if self._host_store is not None:
            persisted_state = self._host_store.update_robots(
                state.host_key,
                crawl_delay_seconds=state.crawl_delay_seconds,
                checked_at=checked_at,
            )
            self._apply_persisted_state(state, persisted_state)
        if self._host_ledger_store is not None:
            self._host_ledger_store.record_robots_check(
                state.host_key,
                status=robots_status,
                checked_at=checked_at,
            )

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
            if self._host_store is not None:
                wait_time, persisted_state = self._host_store.reserve_request_slot(
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
            if self._host_store is not None:
                persisted_state = self._host_store.record_failure(
                    host_key,
                    backoff_seconds=self._compute_host_backoff(state.consecutive_failures),
                )
                self._apply_persisted_state(state, persisted_state)

    def record_error_runtime(self, url: str) -> float:
        """Advance in-memory failure state without touching durable storage."""
        host_key = self._get_host_key(url)
        if host_key in self._runtime_states:
            state = self._runtime_states[host_key]
            state.consecutive_failures += 1
            return self._compute_host_backoff(state.consecutive_failures)
        return self._compute_host_backoff(1)

    def record_success(self, url: str):
        """Record a successful request for a host key."""
        host_key = self._get_host_key(url)
        if host_key in self._runtime_states:
            state = self._runtime_states[host_key]
            state.consecutive_failures = 0
            if self._host_store is not None:
                persisted_state = self._host_store.record_success(host_key)
                self._apply_persisted_state(state, persisted_state)

    def record_success_runtime(self, url: str) -> None:
        """Reset in-memory failure state without touching durable storage."""
        host_key = self._get_host_key(url)
        if host_key in self._runtime_states:
            self._runtime_states[host_key].consecutive_failures = 0

    def get_host_budget(self, host_key: str, *, default_budget: int) -> int:
        """Return the allowed in-flight request count for a host."""
        state = self._runtime_states.get(host_key)
        if state is None:
            return max(1, default_budget)
        return compute_host_budget(
            latency_ewma_ms=state.latency_ewma_ms,
            consecutive_failures=state.consecutive_failures,
            default_budget=default_budget,
        )

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
                "latency_ewma_ms": state.latency_ewma_ms,
                "latency_last_ms": state.latency_last_ms,
                "latency_observed_at": state.latency_observed_at,
                "latency_sample_count": state.latency_sample_count,
            }
            for host_key, state in self._runtime_states.items()
        }

    async def close(self):
        """Close the shared HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
