"""Discovery ranking for newly found URLs."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

DISCOVERY_SEED = "seed"
DISCOVERY_SAME_HOST = "same_host"
DISCOVERY_SEED_HOST = "seed_host"
DISCOVERY_EXTERNAL = "external"

SEED_PRIORITY = 2.0
SAME_HOST_PRIORITY = 1.25
SEED_HOST_PRIORITY = 1.1
EXTERNAL_PRIORITY = 0.8


@dataclass(frozen=True)
class EnqueueDecision:
    """Priority and provenance assigned when enqueueing a URL."""

    priority: float
    discovery_kind: str


def host_key(url: str) -> str:
    """Return the normalized host:port key used for discovery decisions."""
    return urlparse(url).netloc.lower()


def seed_hosts_from_urls(urls: list[str]) -> set[str]:
    """Extract normalized host keys from seed URLs."""
    return {host for host in (host_key(url) for url in urls) if host}


def rank_seed_url(url: str) -> EnqueueDecision:
    """Assign the highest priority to explicit seed URLs."""
    return EnqueueDecision(priority=SEED_PRIORITY, discovery_kind=DISCOVERY_SEED)


def rank_discovered_url(
    *,
    parent_url: str,
    url: str,
    seed_hosts: set[str] | None = None,
) -> EnqueueDecision:
    """Assign queue priority to a discovered outlink."""
    child_host = host_key(url)
    parent_host = host_key(parent_url)
    known_seed_hosts = seed_hosts or set()

    if child_host and child_host == parent_host:
        return EnqueueDecision(priority=SAME_HOST_PRIORITY, discovery_kind=DISCOVERY_SAME_HOST)

    if child_host and child_host in known_seed_hosts:
        return EnqueueDecision(priority=SEED_HOST_PRIORITY, discovery_kind=DISCOVERY_SEED_HOST)

    return EnqueueDecision(priority=EXTERNAL_PRIORITY, discovery_kind=DISCOVERY_EXTERNAL)
