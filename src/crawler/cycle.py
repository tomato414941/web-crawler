"""State and operator snapshots for one crawl cycle."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass

from .pipeline import PipelineQueues, StageLiveness
from .result import CrawlStageTimings
from .telemetry import PipelineTelemetry, TelemetryAccumulator


class CrawlCycle:
    """Own the mutable state and operational queues for one crawl cycle."""

    def __init__(self, queue_maxsize: int) -> None:
        self.running = False
        self.pages_crawled = 0
        self.claimed_pages = 0
        self.results: list[dict] = []
        self.failure_counts: Counter[str] = Counter()
        self.active_host_counts: Counter[str] = Counter()
        self.pipeline_queues = PipelineQueues(maxsize=queue_maxsize)
        self.parse_queue = self.pipeline_queues.parse
        self.finalize_queue = self.pipeline_queues.finalize
        self.parser_liveness = StageLiveness(include_kind=True)
        self.finalizer_liveness = StageLiveness(include_kind=True)
        self.timing_summary = TelemetryAccumulator()
        self.admission_control: dict[str, object] = {}

    def queue_payload(self) -> dict[str, object]:
        """Return queue-depth and wait snapshots for the current runtime state."""
        return self.pipeline_queues.snapshot()

    def record_timing(self, outcome: str, timings: CrawlStageTimings | None) -> None:
        """Record one finalized crawl attempt in cycle-local timing stats."""
        if timings is not None and timings.pipeline is None:
            timings.pipeline = PipelineTelemetry(
                parse_queue_wait_ms=timings.parse_queue_wait_ms,
                finalize_queue_wait_ms=timings.finalize_queue_wait_ms,
                parse_queue_depth=timings.parse_queue_depth,
                finalize_queue_depth=timings.finalize_queue_depth,
            )
        self.timing_summary.record(outcome, timings)


@dataclass(frozen=True, slots=True)
class CycleSnapshotBuilder:
    """Build operator-facing runtime payloads from cycle-local state."""

    max_pages: int
    concurrency: int
    parser_workers: int
    normal_workers: int
    warm_workers: int
    probing_workers: int
    refresh_workers: int
    host_first_fallback_stats: Callable[[], dict[str, int]]

    def active_cycle_payload(self, cycle: CrawlCycle) -> dict[str, object]:
        """Return active-cycle stats without completed-cycle fields."""
        return {
            "running": cycle.running,
            "state": "active" if cycle.running else "idle",
            "pages_crawled": cycle.pages_crawled,
            "claimed_pages": cycle.claimed_pages,
            "max_pages": self.max_pages,
            "concurrency": self.concurrency,
            "parser_workers": self.parser_workers,
            "normal_workers": self.normal_workers,
            "warm_workers": self.warm_workers,
            "probing_workers": self.probing_workers,
            "refresh_workers": self.refresh_workers,
            "execution_workers": {
                "warm": self.warm_workers,
                "probing": self.probing_workers,
                "refresh": self.refresh_workers,
            },
            "active_hosts": len(cycle.active_host_counts),
            **cycle.queue_payload(),
            "parser_liveness": cycle.parser_liveness.snapshot(),
            "finalizer_liveness": cycle.finalizer_liveness.snapshot(),
            "failure_breakdown": dict(cycle.failure_counts),
            "timing_summary": cycle.timing_summary.snapshot(),
            "admission_control": dict(cycle.admission_control),
            "host_first_fallback": self.host_first_fallback_stats(),
        }

    def runtime_stats(self, cycle: CrawlCycle) -> dict[str, object]:
        """Return the live runtime stats payload for external observers."""
        active_cycle = self.active_cycle_payload(cycle)
        return {
            **active_cycle,
            "pages": None,
            "elapsed_seconds": None,
            "pages_per_second": None,
            "errors": dict(cycle.failure_counts),
            "active_cycle": active_cycle,
        }
