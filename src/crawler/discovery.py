"""Discovery ranking for newly found URLs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from urllib.parse import urlparse

ARCHETYPE_GENERIC_PAGE = "generic_page"
ARCHETYPE_DOCUMENT_PAGE = "document_page"
ARCHETYPE_REDIRECT_HUB = "redirect_hub"
ARCHETYPE_REGISTRY_LISTING = "registry_listing"

PARENT_CONTEXT_GENERIC = "generic_parent"
PARENT_CONTEXT_NOFOLLOW = "nofollow_parent"
PARENT_CONTEXT_LOW_SIGNAL = "low_signal_parent"

ADMISSION_REASON_CANDIDATE = "candidate"
ADMISSION_REASON_BELOW_MIN_VALUE = "below_min_value"
ADMISSION_REASON_LOW_VALUE_ARCHETYPE = "low_value_archetype"
ADMISSION_REASON_NOFOLLOW_PARENT = "nofollow_parent"
ADMISSION_REASON_EXTERNAL_PRESSURE = "external_pressure"
ADMISSION_REASON_HOST_POLICY_PENALTY = "host_policy_penalty"

SEED_DISCOVERY_VALUE = 2.0
SAME_HOST_DISCOVERY_VALUE = 1.25
SEED_HOST_DISCOVERY_VALUE = 1.1
EXTERNAL_DISCOVERY_VALUE = 0.8

_ARCHETYPE_ADJUSTMENTS = {
    ARCHETYPE_GENERIC_PAGE: 0.0,
    ARCHETYPE_DOCUMENT_PAGE: 0.15,
    ARCHETYPE_REDIRECT_HUB: -0.3,
    ARCHETYPE_REGISTRY_LISTING: -0.35,
}
_LOW_VALUE_ARCHETYPES = {ARCHETYPE_REDIRECT_HUB, ARCHETYPE_REGISTRY_LISTING}

_MIN_DISCOVERY_VALUE = 0.25
_REDIRECT_SEGMENTS = {"go", "goto", "redirect", "r", "out", "jump"}
_DOCUMENT_HINTS = {"doc", "docs", "document", "documents", "draft", "drafts", "spec", "specs"}
_DOCUMENT_FILENAME_PREFIXES = ("draft-", "rfc")
_LISTING_HINTS = (
    "assignment",
    "assignments",
    "archive",
    "archives",
    "catalog",
    "catalogue",
    "dataset",
    "datasets",
    "download",
    "downloads",
    "index",
    "indexes",
    "mirror",
    "mirrors",
    "registry",
    "registries",
    "repository",
    "repositories",
    "table",
    "tables",
)
_LISTING_TITLE_HINTS = (
    "index of",
    "directory listing",
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
    """Discovery value assigned when enqueueing a URL."""

    discovery_value: float
    archetype: str = ARCHETYPE_GENERIC_PAGE
    parent_archetype: str = ARCHETYPE_GENERIC_PAGE
    parent_context: str = PARENT_CONTEXT_GENERIC


@dataclass(frozen=True)
class DiscoveryAdmissionDecision:
    """Scheduler admission decision for a discovered URL."""

    url: str
    discovery_value: float
    archetype: str
    parent_context: str
    admitted: bool
    reason: str


@dataclass(frozen=True)
class FrontierPressure:
    """Current frontier pressure used to keep discovery growth bounded."""

    pending: int = 0
    pending_threshold: int = 0
    external_min_value: float = 1.0

    @property
    def active(self) -> bool:
        return self.pending_threshold > 0 and self.pending >= self.pending_threshold


@dataclass(frozen=True)
class HostAdmissionContext:
    """Known host quality signals for admission scoring."""

    known: bool = False
    robots_status: str | None = None
    failure_count: int = 0
    success_count: int = 0
    consecutive_failures: int = 0
    penalty: float = 0.0

    @property
    def is_low_value(self) -> bool:
        if self.consecutive_failures >= 4:
            return True
        if self.robots_status in {"denied", "disallowed"}:
            return True
        return self.failure_count >= 3 and self.success_count == 0


def host_key(url: str) -> str:
    """Return the normalized host:port key used for discovery decisions."""
    return urlparse(url).netloc.lower()


def seed_hosts_from_urls(urls: list[str]) -> set[str]:
    """Extract normalized host keys from seed URLs."""
    return {host for host in (host_key(url) for url in urls) if host}


def _normalized_path(url: str) -> str:
    """Return a lowercase URL path for path-based ranking heuristics."""
    return urlparse(url).path.lower()


def _path_segments(path: str) -> tuple[str, ...]:
    """Return normalized path segments for host-agnostic heuristics."""
    return tuple(
        segment.lower() for segment in PurePosixPath(path).parts if segment not in {"", "/"}
    )


def _path_has_hint(path: str, hints: tuple[str, ...] | set[str]) -> bool:
    """Return True when a normalized path contains one of the hint segments."""
    for hint in hints:
        if f"/{hint}/" in path or path.endswith(f"/{hint}"):
            return True
    return False


def _contains_hint(text: str, hints: tuple[str, ...] | set[str]) -> bool:
    """Return True when free text contains any hint token."""
    return any(hint in text for hint in hints)


def _is_redirect_hub(segments: tuple[str, ...]) -> bool:
    """Identify short redirect-style paths without relying on host-specific rules."""
    return bool(segments) and segments[0] in _REDIRECT_SEGMENTS and len(segments) <= 2


def _is_document_path(segments: tuple[str, ...], filename: str) -> bool:
    """Identify document-like URLs from generic path structure."""
    if filename.startswith(_DOCUMENT_FILENAME_PREFIXES):
        return True
    return any(segment in _DOCUMENT_HINTS for segment in segments[:-1])


def _is_listing_path(path: str) -> bool:
    """Identify bulk/listing pages from generic path hints."""
    return _path_has_hint(path, _LISTING_HINTS)


def classify_url_archetype(url: str) -> str:
    """Classify a discovered URL into a coarse page archetype."""
    path = _normalized_path(url)
    segments = _path_segments(path)
    filename = PurePosixPath(path).name.lower()

    if _is_redirect_hub(segments):
        return ARCHETYPE_REDIRECT_HUB

    if _is_document_path(segments, filename):
        return ARCHETYPE_DOCUMENT_PAGE

    if _is_listing_path(path):
        return ARCHETYPE_REGISTRY_LISTING

    return ARCHETYPE_GENERIC_PAGE


def classify_parent_archetype(parent_url: str, parent_signals: PageSignals | None) -> str:
    """Classify the fetched parent page so child ranking can react to context."""
    parent_path = _normalized_path(parent_url)
    if _is_listing_path(parent_path):
        return ARCHETYPE_REGISTRY_LISTING

    if parent_signals is None:
        return classify_url_archetype(parent_url)

    content_type = parent_signals.content_type.lower()
    title = (parent_signals.title or "").lower()
    if _contains_hint(title, _LISTING_TITLE_HINTS) or _contains_hint(title, _LISTING_HINTS):
        return ARCHETYPE_REGISTRY_LISTING

    if parent_signals.content_length >= 512 * 1024:
        return ARCHETYPE_REGISTRY_LISTING

    if content_type and "html" not in content_type:
        return ARCHETYPE_REGISTRY_LISTING

    return classify_url_archetype(parent_url)


def classify_parent_context(
    parent_archetype: str,
    parent_signals: PageSignals | None,
) -> str:
    """Classify why a parent page weakens child discovery confidence."""
    meta_robots = (parent_signals.meta_robots or "").lower() if parent_signals else ""
    if "nofollow" in meta_robots:
        return PARENT_CONTEXT_NOFOLLOW
    if parent_archetype in {ARCHETYPE_REGISTRY_LISTING, ARCHETYPE_REDIRECT_HUB}:
        return PARENT_CONTEXT_LOW_SIGNAL
    return PARENT_CONTEXT_GENERIC


def _context_penalty(parent_archetype: str, parent_signals: PageSignals | None) -> float:
    """Reduce child value when discovered from low-signal parent pages."""
    penalty = 0.0
    meta_robots = (parent_signals.meta_robots or "").lower() if parent_signals else ""

    if parent_archetype == ARCHETYPE_REGISTRY_LISTING:
        penalty += 0.2
    elif parent_archetype == ARCHETYPE_REDIRECT_HUB:
        penalty += 0.1

    if "nofollow" in meta_robots:
        penalty += 0.15

    return min(penalty, 0.35)


def _adjust_discovery_value(
    base_discovery_value: float,
    *,
    url: str,
    parent_url: str,
    parent_signals: PageSignals | None,
) -> tuple[float, str]:
    """Apply lightweight quality heuristics while keeping discovery open."""
    archetype = classify_url_archetype(url)
    parent_archetype = classify_parent_archetype(parent_url, parent_signals)
    discovery_value = base_discovery_value
    discovery_value += _ARCHETYPE_ADJUSTMENTS[archetype]
    discovery_value -= _context_penalty(parent_archetype, parent_signals)
    return max(_MIN_DISCOVERY_VALUE, round(discovery_value, 2)), archetype


def _is_external_url(parent_url: str, url: str, seed_hosts: set[str] | None = None) -> bool:
    child_host = host_key(url)
    if not child_host:
        return False
    if child_host == host_key(parent_url):
        return False
    return child_host not in (seed_hosts or set())


def _apply_host_policy(
    discovery_value: float,
    host_context: HostAdmissionContext | None,
) -> tuple[float, bool]:
    if host_context is None or not host_context.is_low_value:
        return discovery_value, False
    penalty = max(0.0, host_context.penalty)
    return max(_MIN_DISCOVERY_VALUE, round(discovery_value - penalty, 2)), True


def rank_seed_url(url: str) -> EnqueueDecision:
    """Assign the highest discovery value to explicit seed URLs."""
    return EnqueueDecision(discovery_value=SEED_DISCOVERY_VALUE)


def rank_discovered_url(
    *,
    parent_url: str,
    url: str,
    seed_hosts: set[str] | None = None,
    parent_signals: PageSignals | None = None,
) -> EnqueueDecision:
    """Assign discovery value to a discovered outlink."""
    child_host = host_key(url)
    parent_host = host_key(parent_url)
    known_seed_hosts = seed_hosts or set()

    if child_host and child_host == parent_host:
        discovery_value, archetype = _adjust_discovery_value(
            SAME_HOST_DISCOVERY_VALUE,
            url=url,
            parent_url=parent_url,
            parent_signals=parent_signals,
        )
        parent_archetype = classify_parent_archetype(parent_url, parent_signals)
        return EnqueueDecision(
            discovery_value=discovery_value,
            archetype=archetype,
            parent_archetype=parent_archetype,
            parent_context=classify_parent_context(parent_archetype, parent_signals),
        )

    if child_host and child_host in known_seed_hosts:
        discovery_value, archetype = _adjust_discovery_value(
            SEED_HOST_DISCOVERY_VALUE,
            url=url,
            parent_url=parent_url,
            parent_signals=parent_signals,
        )
        parent_archetype = classify_parent_archetype(parent_url, parent_signals)
        return EnqueueDecision(
            discovery_value=discovery_value,
            archetype=archetype,
            parent_archetype=parent_archetype,
            parent_context=classify_parent_context(parent_archetype, parent_signals),
        )

    discovery_value, archetype = _adjust_discovery_value(
        EXTERNAL_DISCOVERY_VALUE,
        url=url,
        parent_url=parent_url,
        parent_signals=parent_signals,
    )
    parent_archetype = classify_parent_archetype(parent_url, parent_signals)
    return EnqueueDecision(
        discovery_value=discovery_value,
        archetype=archetype,
        parent_archetype=parent_archetype,
        parent_context=classify_parent_context(parent_archetype, parent_signals),
    )


def _rejection_reason(decision: EnqueueDecision) -> str:
    """Return the operator-facing reason a ranked URL should not be admitted."""
    if decision.parent_context == PARENT_CONTEXT_NOFOLLOW:
        return ADMISSION_REASON_NOFOLLOW_PARENT
    if decision.archetype in _LOW_VALUE_ARCHETYPES:
        return ADMISSION_REASON_LOW_VALUE_ARCHETYPE
    return ADMISSION_REASON_BELOW_MIN_VALUE


def decide_discovered_url_admission(
    *,
    parent_url: str,
    url: str,
    seed_hosts: set[str] | None = None,
    parent_signals: PageSignals | None = None,
    min_discovery_value: float,
    low_value_archetype_min_discovery_value: float,
    frontier_pressure: FrontierPressure | None = None,
    host_context: HostAdmissionContext | None = None,
) -> DiscoveryAdmissionDecision:
    """Decide whether a discovered URL is valuable enough for scheduler admission."""
    decision = rank_discovered_url(
        parent_url=parent_url,
        url=url,
        seed_hosts=seed_hosts,
        parent_signals=parent_signals,
    )
    discovery_value, host_penalized = _apply_host_policy(
        decision.discovery_value,
        host_context,
    )
    decision = EnqueueDecision(
        discovery_value=discovery_value,
        archetype=decision.archetype,
        parent_archetype=decision.parent_archetype,
        parent_context=decision.parent_context,
    )
    admitted = True
    reason = ADMISSION_REASON_CANDIDATE
    if (
        decision.archetype in _LOW_VALUE_ARCHETYPES
        and decision.discovery_value < low_value_archetype_min_discovery_value
    ) or decision.discovery_value < min_discovery_value:
        admitted = False
        reason = (
            ADMISSION_REASON_HOST_POLICY_PENALTY
            if host_penalized
            else _rejection_reason(decision)
        )
    elif host_penalized and decision.discovery_value < low_value_archetype_min_discovery_value:
        admitted = False
        reason = ADMISSION_REASON_HOST_POLICY_PENALTY
    elif (
        frontier_pressure is not None
        and frontier_pressure.active
        and _is_external_url(parent_url, url, seed_hosts)
        and decision.archetype == ARCHETYPE_GENERIC_PAGE
        and decision.discovery_value < frontier_pressure.external_min_value
    ):
        admitted = False
        reason = ADMISSION_REASON_EXTERNAL_PRESSURE
    return DiscoveryAdmissionDecision(
        url=url,
        discovery_value=decision.discovery_value,
        archetype=decision.archetype,
        parent_context=decision.parent_context,
        admitted=admitted,
        reason=reason,
    )
