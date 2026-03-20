"""Discovery ranking for newly found URLs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from urllib.parse import urlparse

DISCOVERY_SEED = "seed"
DISCOVERY_SAME_HOST = "same_host"
DISCOVERY_SEED_HOST = "seed_host"
DISCOVERY_EXTERNAL = "external"

ARCHETYPE_GENERIC_PAGE = "generic_page"
ARCHETYPE_DOCUMENT_PAGE = "document_page"
ARCHETYPE_REDIRECT_HUB = "redirect_hub"
ARCHETYPE_REGISTRY_LISTING = "registry_listing"

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
_ARCHETYPE_ADJUSTMENTS = {
    ARCHETYPE_GENERIC_PAGE: 0.0,
    ARCHETYPE_DOCUMENT_PAGE: 0.15,
    ARCHETYPE_REDIRECT_HUB: -0.3,
    ARCHETYPE_REGISTRY_LISTING: -0.35,
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
    archetype: str


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


def classify_url_archetype(url: str) -> str:
    """Classify a discovered URL into a coarse page archetype."""
    path = _normalized_path(url)
    host = host_key(url)
    suffix = PurePosixPath(path).suffix.lower()

    if host == "www.iana.org" and path.startswith("/go/"):
        return ARCHETYPE_REDIRECT_HUB

    if host == "datatracker.ietf.org" and (
        path.startswith("/doc/html/")
        or path.startswith("/doc/draft-")
    ):
        return ARCHETYPE_DOCUMENT_PAGE

    if host == "www.rfc-editor.org" and (
        path.startswith("/rfc/")
        or path.startswith("/in-notes/")
    ):
        return ARCHETYPE_DOCUMENT_PAGE

    if "/domains/idn-tables/tables/" in path:
        return ARCHETYPE_REGISTRY_LISTING

    if path.startswith("/assignments/"):
        return ARCHETYPE_REGISTRY_LISTING

    if suffix in _BULK_FILE_SUFFIXES:
        if any(hint in path for hint in _BULK_PATH_HINTS):
            return ARCHETYPE_REGISTRY_LISTING
        return ARCHETYPE_GENERIC_PAGE

    return ARCHETYPE_GENERIC_PAGE


def classify_parent_archetype(parent_url: str, parent_signals: PageSignals | None) -> str:
    """Classify the fetched parent page so child ranking can react to context."""
    parent_path = _normalized_path(parent_url)
    if "/domains/idn-tables" in parent_path:
        return ARCHETYPE_REGISTRY_LISTING

    if parent_signals is None:
        return classify_url_archetype(parent_url)

    content_type = parent_signals.content_type.lower()
    title = (parent_signals.title or "").lower()
    if any(hint in title for hint in _BULK_TITLE_HINTS):
        return ARCHETYPE_REGISTRY_LISTING

    if parent_signals.content_length >= 512 * 1024:
        return ARCHETYPE_REGISTRY_LISTING

    if content_type and "html" not in content_type:
        return ARCHETYPE_REGISTRY_LISTING

    return classify_url_archetype(parent_url)


def _context_penalty(parent_archetype: str, parent_signals: PageSignals | None) -> float:
    """Reduce child priority when discovered from low-signal parent pages."""
    penalty = 0.0
    meta_robots = (parent_signals.meta_robots or "").lower() if parent_signals else ""

    if parent_archetype == ARCHETYPE_REGISTRY_LISTING:
        penalty += 0.2
    elif parent_archetype == ARCHETYPE_REDIRECT_HUB:
        penalty += 0.1

    if "nofollow" in meta_robots:
        penalty += 0.15

    return min(penalty, 0.35)


def _adjust_priority(
    base_priority: float,
    *,
    url: str,
    parent_url: str,
    parent_signals: PageSignals | None,
) -> tuple[float, str]:
    """Apply lightweight quality heuristics while keeping discovery open."""
    archetype = classify_url_archetype(url)
    parent_archetype = classify_parent_archetype(parent_url, parent_signals)
    priority = base_priority
    priority += _ARCHETYPE_ADJUSTMENTS[archetype]
    priority -= _context_penalty(parent_archetype, parent_signals)
    return max(_MIN_PRIORITY, round(priority, 2)), archetype


def rank_seed_url(url: str) -> EnqueueDecision:
    """Assign the highest priority to explicit seed URLs."""
    return EnqueueDecision(
        priority=SEED_PRIORITY,
        discovery_kind=DISCOVERY_SEED,
        archetype=classify_url_archetype(url),
    )


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
        priority, archetype = _adjust_priority(
            SAME_HOST_PRIORITY,
            url=url,
            parent_url=parent_url,
            parent_signals=parent_signals,
        )
        return EnqueueDecision(
            priority=priority,
            discovery_kind=DISCOVERY_SAME_HOST,
            archetype=archetype,
        )

    if child_host and child_host in known_seed_hosts:
        priority, archetype = _adjust_priority(
            SEED_HOST_PRIORITY,
            url=url,
            parent_url=parent_url,
            parent_signals=parent_signals,
        )
        return EnqueueDecision(
            priority=priority,
            discovery_kind=DISCOVERY_SEED_HOST,
            archetype=archetype,
        )

    priority, archetype = _adjust_priority(
        EXTERNAL_PRIORITY,
        url=url,
        parent_url=parent_url,
        parent_signals=parent_signals,
    )
    return EnqueueDecision(
        priority=priority,
        discovery_kind=DISCOVERY_EXTERNAL,
        archetype=archetype,
    )
