"""Runtime telemetry shapes for crawler execution."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
import math
from typing import Any, Mapping


TIMING_STAGE_FIELDS = (
    "lease_ms",
    "precheck_ms",
    "robots_ms",
    "rate_limit_ms",
    "fetch_ms",
    "fetch_request_ms",
    "fetch_body_read_ms",
    "parse_ms",
    "scheduler_ms",
    "persist_ms",
    "output_ms",
    "parse_queue_wait_ms",
    "finalize_queue_wait_ms",
    "publish_queue_wait_ms",
    "process_ms",
    "slot_ms",
)

FINALIZER_TIMING_FIELDS = (
    "discover_ms",
    "admit_ms",
    "admit_update_intents_ms",
    "admit_fetch_rows_ms",
    "admit_delete_membership_ms",
    "admit_insert_membership_ms",
    "admit_host_heads_ms",
    "admit_delete_leases_ms",
    "admit_commit_ms",
    "host_success_ms",
    "host_failure_ms",
    "mark_done_ms",
    "mark_failed_ms",
    "total_ms",
)


STORAGE_TIMING_FIELDS = (
    "prepare_ms",
    "pages_upsert_ms",
    "page_content_ms",
    "commit_ms",
    "total_ms",
)


@dataclass(slots=True)
class FetchTelemetry:
    """Cause-oriented telemetry for one HTTP fetch."""

    outcome: str = "unknown"
    status: int | None = None
    final_url: str | None = None
    redirect_count: int = 0
    content_length: int | None = None
    bytes_read: int = 0
    metadata_only: bool = False
    body_truncated: bool = False
    admission_reason: str | None = None
    total_ms: float = 0.0
    response_headers_ms: float = 0.0
    body_read_ms: float = 0.0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RobotsDecision:
    """Robots decision plus the reason it took time."""

    allowed: bool
    reason: str
    cache_status: str
    robots_status: str
    elapsed_ms: float
    host: str

    def __bool__(self) -> bool:
        return self.allowed

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class LeaseTelemetry:
    """Telemetry for one scheduler lease attempt."""

    outcome: str
    runnable_surface: str
    lease_strategy: str
    elapsed_ms: float
    excluded_hosts_count: int = 0
    read_model: str = "unknown"
    fallback: str = "none"
    execution_tier: str = "unknown"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PipelineTelemetry:
    """Queue and pipeline execution measurements for one crawl attempt."""

    parse_queue_wait_ms: float = 0.0
    finalize_queue_wait_ms: float = 0.0
    publish_queue_wait_ms: float = 0.0
    parse_queue_depth: int = 0
    finalize_queue_depth: int = 0
    publish_queue_depth: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class FinalizerTelemetry:
    """Detailed scheduler mutation timings for one finalized crawl attempt."""

    kind: str
    new_tasks_count: int = 0
    discover_ms: float = 0.0
    admit_ms: float = 0.0
    admit_update_intents_ms: float = 0.0
    admit_fetch_rows_ms: float = 0.0
    admit_delete_membership_ms: float = 0.0
    admit_insert_membership_ms: float = 0.0
    admit_host_heads_ms: float = 0.0
    admit_delete_leases_ms: float = 0.0
    admit_commit_ms: float = 0.0
    host_success_ms: float = 0.0
    host_failure_ms: float = 0.0
    mark_done_ms: float = 0.0
    mark_failed_ms: float = 0.0
    total_ms: float = 0.0
    batch_size: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class StorageTelemetry:
    """Detailed page persistence timings for one saved crawl result."""

    prepare_ms: float = 0.0
    pages_upsert_ms: float = 0.0
    page_content_ms: float = 0.0
    commit_ms: float = 0.0
    total_ms: float = 0.0
    stored_content_bytes: int = 0
    storage_tier: str = "unknown"
    content_truncated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def percentile(values: list[float], percentile_value: int) -> float:
    """Return a nearest-rank percentile for small cycle-local samples."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(
        0,
        min(len(ordered) - 1, math.ceil(percentile_value / 100 * len(ordered)) - 1),
    )
    return round(ordered[index], 1)


def _summarize_values(values: list[float]) -> dict[str, float | int]:
    if not values:
        return {
            "count": 0,
            "avg": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "max": 0.0,
        }
    return {
        "count": len(values),
        "avg": round(sum(values) / len(values), 1),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "max": round(max(values), 1),
    }


class TelemetryAccumulator:
    """Cycle-local aggregation of timings and cause labels."""

    def __init__(self) -> None:
        self._outcomes: Counter[str] = Counter()
        self._stages: dict[str, list[float]] = {field: [] for field in TIMING_STAGE_FIELDS}
        self._fetch_outcomes: Counter[str] = Counter()
        self._fetch_statuses: Counter[str] = Counter()
        self._robots_statuses: Counter[str] = Counter()
        self._robots_cache_statuses: Counter[str] = Counter()
        self._robots_reasons: Counter[str] = Counter()
        self._lease_outcomes: Counter[str] = Counter()
        self._lease_read_models: Counter[str] = Counter()
        self._lease_fallbacks: Counter[str] = Counter()
        self._lease_execution_tiers: Counter[str] = Counter()
        self._finalizer_stages: dict[str, list[float]] = {
            field: [] for field in FINALIZER_TIMING_FIELDS
        }
        self._finalizer_kinds: Counter[str] = Counter()
        self._finalizer_new_tasks_total = 0
        self._finalizer_new_tasks_nonzero_items = 0
        self._finalizer_batch_sizes: list[float] = []
        self._storage_stages: dict[str, list[float]] = {
            field: [] for field in STORAGE_TIMING_FIELDS
        }
        self._storage_tiers: Counter[str] = Counter()
        self._storage_truncated: Counter[str] = Counter()
        self._stored_content_bytes: list[float] = []
        self._discovery_admission: Counter[str] = Counter()

    def record_discovery_admission(self, counts: Mapping[str, int]) -> None:
        """Record discovered-link admission reasons for one parsed page."""
        self._discovery_admission.update(
            {str(key): int(value) for key, value in counts.items() if int(value) > 0}
        )

    def record(self, outcome: str, timings: object | None) -> None:
        """Record one finalized crawl attempt."""
        self._outcomes[outcome] += 1
        if timings is None:
            return

        for field in TIMING_STAGE_FIELDS:
            self._stages[field].append(float(getattr(timings, field)))

        fetch = getattr(timings, "fetch", None)
        if fetch is not None:
            self._fetch_outcomes[str(getattr(fetch, "outcome", "unknown"))] += 1
            status = getattr(fetch, "status", None)
            if status is not None:
                self._fetch_statuses[str(status)] += 1

        robots = getattr(timings, "robots", None)
        if robots is not None:
            self._robots_statuses[str(getattr(robots, "robots_status", "unknown"))] += 1
            self._robots_cache_statuses[str(getattr(robots, "cache_status", "unknown"))] += 1
            self._robots_reasons[str(getattr(robots, "reason", "unknown"))] += 1

        lease = getattr(timings, "lease", None)
        if lease is not None:
            self._lease_outcomes[str(getattr(lease, "outcome", "unknown"))] += 1
            self._lease_read_models[str(getattr(lease, "read_model", "unknown"))] += 1
            self._lease_fallbacks[str(getattr(lease, "fallback", "unknown"))] += 1
            self._lease_execution_tiers[str(getattr(lease, "execution_tier", "unknown"))] += 1

        finalizer = getattr(timings, "finalizer", None)
        if finalizer is not None:
            self._finalizer_kinds[str(getattr(finalizer, "kind", "unknown"))] += 1
            new_tasks_count = int(getattr(finalizer, "new_tasks_count", 0))
            self._finalizer_new_tasks_total += new_tasks_count
            if new_tasks_count > 0:
                self._finalizer_new_tasks_nonzero_items += 1
            self._finalizer_batch_sizes.append(float(getattr(finalizer, "batch_size", 1)))
            for field in FINALIZER_TIMING_FIELDS:
                self._finalizer_stages[field].append(float(getattr(finalizer, field)))

        storage = getattr(timings, "storage", None)
        if storage is not None:
            for field in STORAGE_TIMING_FIELDS:
                self._storage_stages[field].append(float(getattr(storage, field)))
            self._storage_tiers[str(getattr(storage, "storage_tier", "unknown"))] += 1
            truncated = bool(getattr(storage, "content_truncated", False))
            self._storage_truncated[str(truncated).lower()] += 1
            self._stored_content_bytes.append(float(getattr(storage, "stored_content_bytes", 0)))

    def snapshot(self) -> dict[str, object]:
        """Return a runtime-safe summary of observed cycle telemetry."""
        stages = {
            field: _summarize_values(values) for field, values in self._stages.items()
        }
        finalizer = {
            field: _summarize_values(values)
            for field, values in self._finalizer_stages.items()
        }
        storage = {
            field: _summarize_values(values)
            for field, values in self._storage_stages.items()
        }

        outcomes = {
            "success": int(self._outcomes.get("success", 0)),
            "skipped": int(self._outcomes.get("skipped", 0)),
            "failed": int(self._outcomes.get("failed", 0)),
        }
        return {
            "samples": sum(outcomes.values()),
            "outcomes": outcomes,
            "stages": stages,
            "counts": {
                "fetch_outcomes": dict(self._fetch_outcomes),
                "fetch_statuses": dict(self._fetch_statuses),
                "robots_statuses": dict(self._robots_statuses),
                "robots_cache_statuses": dict(self._robots_cache_statuses),
                "robots_reasons": dict(self._robots_reasons),
                "lease_outcomes": dict(self._lease_outcomes),
                "lease_read_models": dict(self._lease_read_models),
                "lease_fallbacks": dict(self._lease_fallbacks),
                "lease_execution_tiers": dict(self._lease_execution_tiers),
                "finalizer_kinds": dict(self._finalizer_kinds),
                "finalizer_new_tasks": {
                    "total": self._finalizer_new_tasks_total,
                    "nonzero_items": self._finalizer_new_tasks_nonzero_items,
                },
                "finalizer_batch_size": _summarize_values(self._finalizer_batch_sizes),
                "storage_tiers": dict(self._storage_tiers),
                "storage_truncated": dict(self._storage_truncated),
                "stored_content_bytes": _summarize_values(self._stored_content_bytes),
                "discovery_admission": dict(self._discovery_admission),
            },
            "finalizer": finalizer,
            "storage": storage,
        }
