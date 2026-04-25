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
ADMISSION_REASON_SCORE_BELOW_THRESHOLD = "score_below_threshold"
ADMISSION_REASON_LOW_VALUE_ARCHETYPE = "low_value_archetype"
ADMISSION_REASON_NOFOLLOW_PARENT = "nofollow_parent"
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
_HOST_POLICY_PENALTY = 0.35
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
class AdmissionControl:
    """Frontier-targeted controls derived from the current pending count."""

    mode: str
    target_pending: int
    pending: int = 0
    min_discovery_value: float = 0.5
    per_page_cap: int = 200
    per_target_host_cap: int = 8
    new_external_host_cap: int = 8

    @property
    def active(self) -> bool:
        return self.mode in {"reduce", "drain"}

    def snapshot(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "target_pending": self.target_pending,
            "pending": self.pending,
            "min_score": self.min_discovery_value,
            "per_page_cap": self.per_page_cap,
            "per_target_host_cap": self.per_target_host_cap,
            "new_external_host_cap": self.new_external_host_cap,
        }


FrontierPressure = AdmissionControl


def build_admission_control(*, pending: int, target_pending: int) -> AdmissionControl:
    """Derive admission limits from the distance to the pending target."""
    if target_pending <= 0:
        raise ValueError("target_pending must be positive")
    pending = max(0, int(pending))
    target_pending = int(target_pending)
    ratio = pending / target_pending
    if ratio < 0.6:
        return AdmissionControl(
            mode="expand",
            target_pending=target_pending,
            pending=pending,
            min_discovery_value=0.5,
            per_page_cap=200,
            per_target_host_cap=8,
            new_external_host_cap=8,
        )
    if ratio < 1.2:
        return AdmissionControl(
            mode="balanced",
            target_pending=target_pending,
            pending=pending,
            min_discovery_value=0.8,
            per_page_cap=160,
            per_target_host_cap=6,
            new_external_host_cap=5,
        )
    if ratio < 1.8:
        return AdmissionControl(
            mode="reduce",
            target_pending=target_pending,
            pending=pending,
            min_discovery_value=1.0,
            per_page_cap=80,
            per_target_host_cap=4,
            new_external_host_cap=2,
        )
    return AdmissionControl(
        mode="drain",
        target_pending=target_pending,
        pending=pending,
        min_discovery_value=1.15,
        per_page_cap=40,
        per_target_host_cap=2,
        new_external_host_cap=1,
    )


@dataclass(frozen=True)
class HostAdmissionContext:
    """Known host quality signals for admission scoring."""

    known: bool = False
    robots_status: str | None = None
    failure_count: int = 0
    success_count: int = 0
    consecutive_failures: int = 0
    penalty: float = _HOST_POLICY_PENALTY

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


def decide_discovered_url_admission(
    *,
    parent_url: str,
    url: str,
    seed_hosts: set[str] | None = None,
    parent_signals: PageSignals | None = None,
    admission_control: AdmissionControl,
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
    if decision.parent_context == PARENT_CONTEXT_NOFOLLOW:
        admitted = False
        reason = ADMISSION_REASON_NOFOLLOW_PARENT
    elif decision.archetype in _LOW_VALUE_ARCHETYPES:
        admitted = False
        reason = ADMISSION_REASON_LOW_VALUE_ARCHETYPE
    elif host_penalized and decision.discovery_value < admission_control.min_discovery_value:
        admitted = False
        reason = ADMISSION_REASON_HOST_POLICY_PENALTY
    elif decision.discovery_value < admission_control.min_discovery_value:
        admitted = False
        reason = ADMISSION_REASON_SCORE_BELOW_THRESHOLD
    return DiscoveryAdmissionDecision(
        url=url,
        discovery_value=decision.discovery_value,
        archetype=decision.archetype,
        parent_context=decision.parent_context,
        admitted=admitted,
        reason=reason,
    )
