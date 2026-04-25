"""Runtime state containers for crawler orchestration."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass

from .pipeline import PipelineQueues, StageLiveness
from .result import CrawlStageTimings
from .telemetry import PipelineTelemetry, TelemetryAccumulator


class CrawlerRuntime:
    """Own cycle-local runtime state and operational queues."""

    def __init__(self, queue_maxsize: int) -> None:
        self.queue_maxsize = queue_maxsize
        self.reset_cycle()

    def reset_cycle(self) -> None:
        """Reset all cycle-local runtime state."""
        self.running = False
        self.pages_crawled = 0
        self.claimed_pages = 0
        self.failure_counts: Counter[str] = Counter()
        self.active_host_counts: Counter[str] = Counter()
        self.pipeline_queues: PipelineQueues | None = None
        self.parse_queue: asyncio.Queue[object] | None = None
        self.finalize_queue: asyncio.Queue[object] | None = None
        self.publish_queue: asyncio.Queue[object] | None = None
        self.parser_liveness = StageLiveness(include_kind=True)
        self.finalizer_liveness = StageLiveness(include_kind=True)
        self.publisher_liveness = StageLiveness()
        self.timing_summary = TelemetryAccumulator()
        self.admission_control: dict[str, object] = {}

    def queue_payload(self) -> dict[str, object]:
        """Return queue-depth and wait snapshots for the current runtime state."""
        queue_payload = (
            self.pipeline_queues.snapshot()
            if self.pipeline_queues is not None
            else PipelineQueues.empty_snapshot(self.queue_maxsize)
        )
        if self.pipeline_queues is None:
            queue_payload["parse_queue_size"] = (
                self.parse_queue.qsize() if self.parse_queue is not None else 0
            )
            queue_payload["finalize_queue_size"] = (
                self.finalize_queue.qsize() if self.finalize_queue is not None else 0
            )
            queue_payload["publish_queue_size"] = (
                self.publish_queue.qsize() if self.publish_queue is not None else 0
            )
        return queue_payload

    def record_timing(self, outcome: str, timings: CrawlStageTimings | None) -> None:
        """Record one finalized crawl attempt in cycle-local timing stats."""
        if timings is not None and timings.pipeline is None:
            timings.pipeline = PipelineTelemetry(
                parse_queue_wait_ms=timings.parse_queue_wait_ms,
                finalize_queue_wait_ms=timings.finalize_queue_wait_ms,
                publish_queue_wait_ms=timings.publish_queue_wait_ms,
                parse_queue_depth=timings.parse_queue_depth,
                finalize_queue_depth=timings.finalize_queue_depth,
                publish_queue_depth=timings.publish_queue_depth,
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
    runnable_workers: int
    scheduled_workers: int
    refresh_workers: int
    host_first_fallback_stats: Callable[[], dict[str, int]]

    def active_cycle_payload(self, runtime: CrawlerRuntime) -> dict[str, object]:
        """Return active-cycle stats without completed-cycle fields."""
        return {
            "running": runtime.running,
            "state": "active" if runtime.running else "idle",
            "pages_crawled": runtime.pages_crawled,
            "claimed_pages": runtime.claimed_pages,
            "max_pages": self.max_pages,
            "concurrency": self.concurrency,
            "parser_workers": self.parser_workers,
            "normal_workers": self.normal_workers,
            "warm_workers": self.warm_workers,
            "probing_workers": self.probing_workers,
            "runnable_workers": self.runnable_workers,
            "scheduled_workers": self.scheduled_workers,
            "refresh_workers": self.refresh_workers,
            "execution_workers": {
                "warm": self.warm_workers,
                "probing": self.probing_workers,
                "refresh": self.refresh_workers,
            },
            "active_hosts": len(runtime.active_host_counts),
            **runtime.queue_payload(),
            "parser_liveness": runtime.parser_liveness.snapshot(),
            "finalizer_liveness": runtime.finalizer_liveness.snapshot(),
            "publisher_liveness": runtime.publisher_liveness.snapshot(),
            "failure_breakdown": dict(runtime.failure_counts),
            "timing_summary": runtime.timing_summary.snapshot(),
            "admission_control": dict(runtime.admission_control),
            "host_first_fallback": self.host_first_fallback_stats(),
        }

    def runtime_stats(self, runtime: CrawlerRuntime) -> dict[str, object]:
        """Return the live runtime stats payload for external observers."""
        active_cycle = self.active_cycle_payload(runtime)
        return {
            **active_cycle,
            "pages": None,
            "elapsed_seconds": None,
            "pages_per_second": None,
            "errors": dict(runtime.failure_counts),
            "active_cycle": active_cycle,
        }
