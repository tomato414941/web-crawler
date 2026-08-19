"""Admission policy for discovered URLs."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from .discovery import (
    ARCHETYPE_GENERIC_PAGE,
    ARCHETYPE_REDIRECT_HUB,
    ARCHETYPE_REGISTRY_LISTING,
    PARENT_CONTEXT_NOFOLLOW,
    PageSignals,
    classify_url_archetype,
    host_key,
    rank_discovered_url,
)
from .egress_guard import is_url_allowed_without_dns
from .scheduler_membership import SCHEDULER_SURFACE_SCHEDULED
from .scheduler_task import CrawlTask, INTENT_EXPLORE

ADMISSION_REASON_CANDIDATE = "candidate"
ADMISSION_REASON_SCORE_BELOW_THRESHOLD = "score_below_threshold"
ADMISSION_REASON_LOW_VALUE_ARCHETYPE = "low_value_archetype"
ADMISSION_REASON_NOFOLLOW_PARENT = "nofollow_parent"
ADMISSION_REASON_HOST_POLICY_PENALTY = "host_policy_penalty"
ADMISSION_REASON_EGRESS_BLOCKED = "egress_blocked"

_HOST_POLICY_PENALTY = 0.35
_LOW_VALUE_ARCHETYPES = {ARCHETYPE_REDIRECT_HUB, ARCHETYPE_REGISTRY_LISTING}
_MIN_DISCOVERY_VALUE = 0.25


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


@dataclass(frozen=True)
class DiscoveryAdmissionResult:
    """Admitted crawl tasks plus operator-facing admission counts."""

    tasks: list[CrawlTask]
    counts: dict[str, int]
    control: AdmissionControl


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


def _apply_host_policy(
    discovery_value: float,
    host_context: HostAdmissionContext | None,
) -> tuple[float, bool]:
    if host_context is None or not host_context.is_low_value:
        return discovery_value, False
    penalty = max(0.0, host_context.penalty)
    return max(_MIN_DISCOVERY_VALUE, round(discovery_value - penalty, 2)), True


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
    admitted = True
    reason = ADMISSION_REASON_CANDIDATE
    if decision.parent_context == PARENT_CONTEXT_NOFOLLOW:
        admitted = False
        reason = ADMISSION_REASON_NOFOLLOW_PARENT
    elif decision.archetype in _LOW_VALUE_ARCHETYPES:
        admitted = False
        reason = ADMISSION_REASON_LOW_VALUE_ARCHETYPE
    elif host_penalized and discovery_value < admission_control.min_discovery_value:
        admitted = False
        reason = ADMISSION_REASON_HOST_POLICY_PENALTY
    elif discovery_value < admission_control.min_discovery_value:
        admitted = False
        reason = ADMISSION_REASON_SCORE_BELOW_THRESHOLD
    return DiscoveryAdmissionDecision(
        url=url,
        discovery_value=discovery_value,
        archetype=decision.archetype,
        parent_context=decision.parent_context,
        admitted=admitted,
        reason=reason,
    )


class DiscoveryAdmissionPolicy:
    """Build scheduler tasks from discovered outlinks under admission controls."""

    def __init__(
        self,
        *,
        seed_hosts: set[str],
        is_valid_url: Callable[[str], bool],
        is_egress_allowed_url: Callable[[str], bool] | None = None,
    ) -> None:
        self.seed_hosts = seed_hosts
        self.is_valid_url = is_valid_url
        self.is_egress_allowed_url = (
            is_egress_allowed_url
            if is_egress_allowed_url is not None
            else lambda url: is_url_allowed_without_dns(url).allowed
        )

    def build_tasks(
        self,
        *,
        parent_url: str,
        links: list[str],
        parent_signals: PageSignals | None,
        admission_control: AdmissionControl,
        host_contexts: Mapping[str, HostAdmissionContext],
    ) -> DiscoveryAdmissionResult:
        counts: Counter[str] = Counter()
        counts["extracted"] = len(links)
        candidates = self._candidate_tasks(
            parent_url=parent_url,
            links=links,
            parent_signals=parent_signals,
            admission_control=admission_control,
            host_contexts=host_contexts,
            counts=counts,
        )
        selected = self._apply_caps(
            parent_url=parent_url,
            candidates=candidates,
            admission_control=admission_control,
            host_contexts=host_contexts,
            counts=counts,
        )
        counts["admitted"] = len(selected)
        return DiscoveryAdmissionResult(
            tasks=selected,
            counts=dict(counts),
            control=admission_control,
        )

    def _candidate_tasks(
        self,
        *,
        parent_url: str,
        links: list[str],
        parent_signals: PageSignals | None,
        admission_control: AdmissionControl,
        host_contexts: Mapping[str, HostAdmissionContext],
        counts: Counter[str],
    ) -> list[tuple[CrawlTask, DiscoveryAdmissionDecision]]:
        candidates: list[tuple[CrawlTask, DiscoveryAdmissionDecision]] = []
        for link in links:
            if not self.is_valid_url(link):
                counts["scope_filtered"] += 1
                continue
            if not self.is_egress_allowed_url(link):
                counts[ADMISSION_REASON_EGRESS_BLOCKED] += 1
                continue
            target_host = host_key(link)
            decision = decide_discovered_url_admission(
                parent_url=parent_url,
                url=link,
                seed_hosts=self.seed_hosts,
                parent_signals=parent_signals,
                admission_control=admission_control,
                host_context=host_contexts.get(target_host),
            )
            if not decision.admitted:
                counts[decision.reason] += 1
                continue
            candidates.append(
                (
                    CrawlTask(
                        url=link,
                        discovery_value=decision.discovery_value,
                        runnable_surface=SCHEDULER_SURFACE_SCHEDULED,
                        intent=INTENT_EXPLORE,
                        source_url=parent_url,
                    ),
                    decision,
                )
            )
        candidates.sort(key=lambda item: (-item[0].discovery_value, item[0].url))
        return candidates

    def _apply_caps(
        self,
        *,
        parent_url: str,
        candidates: list[tuple[CrawlTask, DiscoveryAdmissionDecision]],
        admission_control: AdmissionControl,
        host_contexts: Mapping[str, HostAdmissionContext],
        counts: Counter[str],
    ) -> list[CrawlTask]:
        selected: list[CrawlTask] = []
        target_host_counts: Counter[str] = Counter()
        pressure_new_hosts: set[str] = set()
        for task, _decision in candidates:
            if admission_control.per_page_cap > 0 and len(selected) >= (
                admission_control.per_page_cap
            ):
                counts["per_page_cap"] += 1
                continue
            target_host = host_key(task.url)
            if self._is_pressure_limited_new_host(
                parent_url=parent_url,
                target_host=target_host,
                admission_control=admission_control,
                host_context=host_contexts.get(target_host),
                selected_new_hosts=pressure_new_hosts,
            ):
                counts["new_host_pressure"] += 1
                continue
            if (
                admission_control.per_target_host_cap > 0
                and target_host_counts[target_host]
                >= admission_control.per_target_host_cap
            ):
                counts["per_target_host_cap"] += 1
                continue
            selected.append(task)
            target_host_counts[target_host] += 1
            if self._is_external_generic(parent_url, task.url):
                counts["external_generic"] += 1
            if (
                admission_control.active
                and self._is_external_host(parent_url, target_host)
                and not host_contexts.get(target_host, HostAdmissionContext()).known
            ):
                pressure_new_hosts.add(target_host)
        return selected

    def _is_external_generic(self, parent_url: str, url: str) -> bool:
        target_host = host_key(url)
        if not self._is_external_host(parent_url, target_host):
            return False
        return classify_url_archetype(url) == ARCHETYPE_GENERIC_PAGE

    def _is_external_host(self, parent_url: str, target_host: str) -> bool:
        return bool(
            target_host
            and target_host != host_key(parent_url)
            and target_host not in self.seed_hosts
        )

    def _is_pressure_limited_new_host(
        self,
        *,
        parent_url: str,
        target_host: str,
        admission_control: AdmissionControl,
        host_context: HostAdmissionContext | None,
        selected_new_hosts: set[str],
    ) -> bool:
        if not admission_control.active:
            return False
        limit = admission_control.new_external_host_cap
        if limit <= 0 or len(selected_new_hosts) < limit:
            return False
        if host_context is not None and host_context.known:
            return False
        if target_host == host_key(parent_url) or target_host in self.seed_hosts:
            return False
        return target_host not in selected_new_hosts
