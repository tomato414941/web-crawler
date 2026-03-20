"""Discovery ranking for newly found URLs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from urllib.parse import urlparse

DISCOVERY_SEED = "seed"
DISCOVERY_SAME_HOST = "same_host"
DISCOVERY_SEED_HOST = "seed_host"
DISCOVERY_EXTERNAL = "external"

SEED_PRIORITY = 2.0
SAME_HOST_PRIORITY = 1.25
SEED_HOST_PRIORITY = 1.1
EXTERNAL_PRIORITY = 0.8

_DISCOVERY_RANKS = {
    DISCOVERY_EXTERNAL: 1,
    DISCOVERY_SEED_HOST: 2,
    DISCOVERY_SAME_HOST: 3,
    DISCOVERY_SEED: 4,
}

_MIN_PRIORITY = 0.25
_BULK_FILE_SUFFIXES = {
    ".txt",
    ".csv",
    ".tsv",
    ".json",
    ".xml",
}
_BULK_PATH_HINTS = (
    "/table/",
    "/tables/",
    "/archive/",
    "/archives/",
    "/download/",
    "/downloads/",
    "/registry/",
    "/registries/",
    "/mirror/",
    "/mirrors/",
    "/data/",
    "/datasets/",
)
_BULK_TITLE_HINTS = (
    "index of",
    "directory listing",
    "archive",
    "archives",
    "table",
    "tables",
    "registry",
    "registries",
    "repository",
    "repositories",
    "catalog",
    "catalogue",
    "dataset",
    "datasets",
)


@dataclass(frozen=True)
class PageSignals:
    """Lightweight signals extracted from a fetched parent page."""

    content_type: str = ""
    content_length: int = 0
    title: str | None = None
    meta_robots: str | None = None


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


def discovery_rank(discovery_kind: str) -> int:
    """Return an ordering score for discovery provenance."""
    return _DISCOVERY_RANKS.get(discovery_kind, 0)


def _normalized_path(url: str) -> str:
    """Return a lowercase URL path for path-based ranking heuristics."""
    return urlparse(url).path.lower()


def _bulk_path_penalty(url: str) -> float:
    """Reduce priority for URLs that look like bulk data or generated inventories."""
    path = _normalized_path(url)
    suffix = PurePosixPath(path).suffix.lower()

    if "/domains/idn-tables/tables/" in path:
        return 0.7

    if suffix in _BULK_FILE_SUFFIXES:
        if any(hint in path for hint in _BULK_PATH_HINTS):
            return 0.45
        return 0.2

    return 0.0


def _parent_page_penalty(parent_url: str, parent_signals: PageSignals | None) -> float:
    """Reduce child priority when the parent page looks like a bulk listing."""
    if parent_signals is None:
        return 0.0

    penalty = 0.0
    parent_path = _normalized_path(parent_url)
    content_type = parent_signals.content_type.lower()
    title = (parent_signals.title or "").lower()
    meta_robots = (parent_signals.meta_robots or "").lower()

    if "/domains/idn-tables/" in parent_path:
        penalty += 0.3

    if any(hint in title for hint in _BULK_TITLE_HINTS):
        penalty += 0.2

    if parent_signals.content_length >= 512 * 1024:
        penalty += 0.1

    if content_type and "html" not in content_type:
        penalty += 0.1

    if "nofollow" in meta_robots:
        penalty += 0.15

    return min(penalty, 0.45)


def _adjust_priority(
    base_priority: float,
    *,
    url: str,
    parent_url: str,
    parent_signals: PageSignals | None,
) -> float:
    """Apply lightweight quality heuristics while keeping discovery open."""
    priority = base_priority
    priority -= _bulk_path_penalty(url)
    priority -= _parent_page_penalty(parent_url, parent_signals)
    return max(_MIN_PRIORITY, round(priority, 2))


def rank_seed_url(url: str) -> EnqueueDecision:
    """Assign the highest priority to explicit seed URLs."""
    return EnqueueDecision(priority=SEED_PRIORITY, discovery_kind=DISCOVERY_SEED)


def rank_discovered_url(
    *,
    parent_url: str,
    url: str,
    seed_hosts: set[str] | None = None,
    parent_signals: PageSignals | None = None,
) -> EnqueueDecision:
    """Assign queue priority to a discovered outlink."""
    child_host = host_key(url)
    parent_host = host_key(parent_url)
    known_seed_hosts = seed_hosts or set()

    if child_host and child_host == parent_host:
        return EnqueueDecision(
            priority=_adjust_priority(
                SAME_HOST_PRIORITY,
                url=url,
                parent_url=parent_url,
                parent_signals=parent_signals,
            ),
            discovery_kind=DISCOVERY_SAME_HOST,
        )

    if child_host and child_host in known_seed_hosts:
        return EnqueueDecision(
            priority=_adjust_priority(
                SEED_HOST_PRIORITY,
                url=url,
                parent_url=parent_url,
                parent_signals=parent_signals,
            ),
            discovery_kind=DISCOVERY_SEED_HOST,
        )

    return EnqueueDecision(
        priority=_adjust_priority(
            EXTERNAL_PRIORITY,
            url=url,
            parent_url=parent_url,
            parent_signals=parent_signals,
        ),
        discovery_kind=DISCOVERY_EXTERNAL,
    )
