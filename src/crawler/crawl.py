"""Crawler engine with async concurrency."""

from __future__ import annotations

import asyncio
import concurrent.futures
from dataclasses import dataclass
import logging
import re
import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import httpx
import typer

from .config import settings
from .content_policy import should_extract_links, should_store_text_content
from .core import HttpFetcher, Response
from .egress_guard import EgressBlockedError, is_url_allowed_without_dns
from .discovery_admission import (
    AdmissionControl,
    DiscoveryAdmissionPolicy,
    HostAdmissionContext,
    build_admission_control,
)
from .discovery import (
    PageSignals,
    host_key,
    rank_seed_url,
    seed_hosts_from_urls,
)
from .error_stats import categorize_crawl_error
from .host_manager import HostManager
from .host_runnable_heads import (
    HOST_EXECUTION_TIER_DEFERRED,
    HOST_EXECUTION_TIER_PROBING,
    HOST_EXECUTION_TIER_SLOW,
    HOST_EXECUTION_TIER_WARM,
)
from .host_store import HostStore
from .pipeline import (
    FINALIZER_SENTINEL as _FINALIZER_SENTINEL,
    PARSER_SENTINEL as _PARSER_SENTINEL,
    FailedTask as _FailedTask,
    FetchStage,
    FetchedPage as _FetchedPage,
    FinalizeStage,
    ParseStage,
    ParsedPage as _ParsedPage,
    SkippedTask as _SkippedTask,
)
from .scheduler_membership import (
    SCHEDULER_SURFACE_RUNNABLE,
    SCHEDULER_SURFACE_NORMAL,
    SCHEDULER_SURFACE_REFRESH,
)
from .scheduler_queue_policy import LEASE_STRATEGY_HOST_FIRST, LEASE_STRATEGY_URL_ORDER
from .scheduler_task import CrawlTask, INTENT_EXPLORE, INTENT_REFRESH
from .scheduler import Scheduler
from .output import StreamingOutputWriter
from .result import CrawlFailure, CrawlResult, CrawlStageTimings
from .cycle import CrawlCycle, CycleSnapshotBuilder
from .services import FinalizerService
from .telemetry import FetchTelemetry, LeaseTelemetry
from .urls import extract_links

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .storage import PgStorage

# Workers wait this many idle ticks (× 0.1s) before giving up
_WORKER_PATIENCE = 50
_TITLE_PATTERN = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_META_ROBOTS_PATTERN = re.compile(
    r'<meta[^>]+name=["\']robots["\'][^>]+content=["\']([^"\']+)["\']',
    re.IGNORECASE | re.DOTALL,
)


def _response_content_length(response: Response) -> int:
    """Return declared resource length when known, otherwise downloaded bytes."""
    content_length = response.content_length
    return content_length if content_length is not None else len(response.content)


def _response_header(response: Response, name: str) -> str:
    """Return a response header value using case-insensitive lookup."""
    lower_name = name.lower()
    for key, value in response.headers.items():
        if key.lower() == lower_name:
            return value
    return ""


def _split_worker_pools(concurrency: int) -> tuple[int, int]:
    """Split worker capacity into normal and refresh pools."""
    total = max(1, concurrency)
    refresh = 1 if total >= 4 else 0
    normal = total - refresh
    return normal, refresh


def _split_execution_worker_pools(normal_workers: int, probing_ratio: float) -> tuple[int, int]:
    """Split normal worker capacity into warm and probing host lanes."""
    total = max(0, normal_workers)
    ratio = max(0.0, min(1.0, probing_ratio))
    if total <= 1 or ratio <= 0:
        return total, 0
    probing = max(1, round(total * ratio))
    probing = min(total - 1, probing)
    return total - probing, probing


def _elapsed_ms(started_at: float) -> float:
    """Return elapsed wall-clock time in milliseconds."""
    return round((time.perf_counter() - started_at) * 1000, 1)


def _elapsed_ms_between(started_at: float, finished_at: float) -> float:
    """Return elapsed wall-clock time between two captured timestamps."""
    return round((finished_at - started_at) * 1000, 1)


def _format_timings(timings: CrawlStageTimings | None) -> str:
    """Format per-stage timings for logs."""
    if timings is None:
        return ""
    return (
        "lease=%0.1fms precheck=%0.1fms robots=%0.1fms rate_limit=%0.1fms "
        "fetch=%0.1fms request=%0.1fms body=%0.1fms parse=%0.1fms "
        "scheduler=%0.1fms persist=%0.1fms output=%0.1fms "
        "parse_q_wait=%0.1fms finalize_q_wait=%0.1fms process=%0.1fms slot=%0.1fms "
        "parse_q_depth=%d finalize_q_depth=%d"
    ) % (
        timings.lease_ms,
        timings.precheck_ms,
        timings.robots_ms,
        timings.rate_limit_ms,
        timings.fetch_ms,
        timings.fetch_request_ms,
        timings.fetch_body_read_ms,
        timings.parse_ms,
        timings.scheduler_ms,
        timings.persist_ms,
        timings.output_ms,
        timings.parse_queue_wait_ms,
        timings.finalize_queue_wait_ms,
        timings.process_ms,
        timings.slot_ms,
        timings.parse_queue_depth,
        timings.finalize_queue_depth,
    )


def _format_timing_summary(summary: dict[str, object]) -> str:
    """Render the highest-signal timing summary fields for cycle logs."""
    stages = summary.get("stages")
    if not isinstance(stages, dict):
        return "timings=unavailable"
    fields = (
        "lease_ms",
        "precheck_ms",
        "robots_ms",
        "rate_limit_ms",
        "fetch_ms",
        "scheduler_ms",
        "persist_ms",
        "finalize_queue_wait_ms",
    )
    parts = []
    for field in fields:
        stage = stages.get(field)
        if isinstance(stage, dict):
            parts.append(f"{field.removesuffix('_ms')}_p95={stage.get('p95', 0.0)}ms")
    return " ".join(parts) if parts else "timings=unavailable"


@dataclass(frozen=True, slots=True)
class _LeaseLane:
    """Operational lease surface for one worker pool, not a first-class scheduler concept."""

    runnable_surface: str
    lease_strategy: str
    intent: str | None = None
    execution_tiers: tuple[int, ...] | None = None


class CrawlerEngine:
    """Async crawler engine with concurrent processing."""

    def __init__(
        self,
        start_url: str = "",
        max_pages: int = 100,
        same_host: bool = True,
        use_browser: bool = False,
        delay: float = 1.0,
        concurrency: int = 5,
        output_writer: StreamingOutputWriter | None = None,
        pg_storage: "PgStorage | None" = None,
        scheduler: Scheduler | None = None,
        host_manager: HostManager | None = None,
        host_store: HostStore | None = None,
        seed_urls: list[str] | None = None,
    ):
        self.start_url = start_url
        self.max_pages = max_pages
        self.same_host = same_host
        self.use_browser = use_browser
        self.concurrency = concurrency
        self.output_writer = output_writer
        self.pg_storage = pg_storage
        self.parser_workers = max(1, min(concurrency, 4))
        self.max_inflight_requests_per_host = max(1, settings.max_inflight_requests_per_host)
        self.normal_workers, self.refresh_workers = _split_worker_pools(concurrency)
        self.warm_workers, self.probing_workers = _split_execution_worker_pools(
            self.normal_workers,
            settings.execution_probing_worker_ratio,
        )
        self._finalizer_executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._finalizer_storage = None
        self._finalizer_scheduler: Scheduler | None = None
        self._finalizer_host_store: HostStore | None = None

        self.start_host = urlparse(start_url).netloc if start_url else ""
        self.seed_hosts = seed_hosts_from_urls(seed_urls or [])
        if self.start_host:
            self.seed_hosts.add(self.start_host.lower())
        if scheduler:
            self.scheduler = scheduler
        elif pg_storage:
            self.scheduler = Scheduler(pg_storage.conn)
        else:
            raise ValueError("Postgres connection required for scheduler state")

        if host_store is None and pg_storage is not None:
            host_store = HostStore(pg_storage.conn, default_delay=delay)
        self.host_store = host_store
        if self.host_store is not None:
            self.scheduler.attach_host_store(self.host_store)
        self.host_ledger_store = self.scheduler.host_ledger_store

        if host_manager is not None:
            self.host_manager = host_manager
            self._owns_host_manager = False
            self.host_manager.attach_store(self.host_store)
            self.host_manager.attach_host_ledger_store(self.host_ledger_store)
        else:
            self.host_manager = HostManager(
                user_agent=settings.user_agent,
                default_delay=delay,
                host_store=self.host_store,
                host_ledger_store=self.host_ledger_store,
            )
            self._owns_host_manager = True

        if use_browser:
            from .core import get_browser_fetcher

            self.fetcher = get_browser_fetcher()(timeout=30.0)
        else:
            self.fetcher = HttpFetcher(timeout=settings.timeout)

        self._page_lock = asyncio.Lock()
        self._lease_lock = asyncio.Lock()
        self._frontier_pending_cache: tuple[float, int] = (0.0, 0)
        self._pipeline_queue_maxsize = max(16, concurrency * 4)
        self._cycle = CrawlCycle(queue_maxsize=self._pipeline_queue_maxsize)
        self._snapshot_builder = CycleSnapshotBuilder(
            max_pages=self.max_pages,
            concurrency=self.concurrency,
            parser_workers=self.parser_workers,
            normal_workers=self.normal_workers,
            warm_workers=self.warm_workers,
            probing_workers=self.probing_workers,
            refresh_workers=self.refresh_workers,
            host_first_fallback_stats=self._host_first_fallback_stats,
        )

    @property
    def pages_crawled(self) -> int:
        return self._cycle.pages_crawled

    @property
    def results(self) -> list[dict]:
        return self._cycle.results

    @property
    def failure_breakdown(self) -> dict[str, int]:
        return dict(self._cycle.failure_counts)

    def _host_first_fallback_stats(self) -> dict[str, int]:
        raw = self.scheduler.host_first_fallback_stats()
        return {
            "attempts": int(raw.get("attempts", 0)),
            "hits": int(raw.get("hits", 0)),
            "misses": int(raw.get("misses", 0)),
            "read_model_hits": int(raw.get("read_model_hits", 0)),
            "read_model_stale": int(raw.get("read_model_stale", 0)),
            "read_model_misses": int(raw.get("read_model_misses", 0)),
            "read_model_errors": int(raw.get("read_model_errors", 0)),
        }

    def snapshot_runtime_stats(self) -> dict[str, object]:
        """Return live queue/backpressure stats for external observers."""
        return self._snapshot_builder.runtime_stats(self._cycle)

    def timing_summary(self) -> dict[str, object]:
        """Return the current cycle timing summary."""
        return self._cycle.timing_summary.snapshot()

    def timing_summary_log(self) -> str:
        """Return a compact log representation of the current cycle timings."""
        return _format_timing_summary(self.timing_summary())

    async def __aenter__(self) -> "CrawlerEngine":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """Close all resources."""
        if self._finalizer_executor is not None:
            self._finalizer_executor.shutdown(wait=True, cancel_futures=False)
        self._finalizer_executor = None
        self._finalizer_scheduler = None
        self._finalizer_host_store = None
        if self._finalizer_storage is not None:
            self._finalizer_storage.close()
            self._finalizer_storage = None
        await self.fetcher.close()
        if self._owns_host_manager:
            await self.host_manager.close()

    def _finalizer_service(self) -> FinalizerService:
        """Build the finalizer service for the current runtime wiring."""
        return FinalizerService(
            scheduler=self._finalizer_scheduler or self.scheduler,
            host_store=self._finalizer_host_store or self.host_store,
            storage=self._finalizer_storage or self.pg_storage,
            executor=self._finalizer_executor,
            output_writer=self.output_writer,
            results_sink=self._cycle.results,
            host_key_for_url=self._host_key_for_url,
        )

    def _progress(self) -> tuple[int, int]:
        return self.pages_crawled, self.max_pages

    def _record_failure_category(self, error: str) -> None:
        category = categorize_crawl_error(error)
        if category:
            self._cycle.failure_counts[category] += 1

    def _record_error_runtime(self, url: str) -> float:
        """Advance runtime failure state without requiring durable writes on the event loop."""
        return self.host_manager.record_error_runtime(url)

    def _build_retryable_failed_task(
        self,
        *,
        task: CrawlTask,
        error: str,
        timings: CrawlStageTimings,
        process_started: float,
    ) -> _FailedTask:
        """Build a retryable failure without letting host runtime hooks escape."""
        try:
            backoff_seconds = self._record_error_runtime(task.url)
        except Exception:
            logger.debug("Failed to record host runtime error", exc_info=True)
            backoff_seconds = None
        try:
            retryable = self.host_manager.should_retry(task.url)
        except Exception:
            logger.debug("Failed to read host retry policy", exc_info=True)
            retryable = True
        return _FailedTask(
            task=task,
            failure=CrawlFailure(
                url=task.url,
                error=error,
                retryable=retryable,
                timings=timings,
            ),
            process_started=process_started,
            record_error=True,
            backoff_seconds=backoff_seconds,
        )

    def _host_inflight_budget(self, host_key: str) -> int:
        """Resolve the allowed concurrent in-flight requests for a host."""
        default_budget = self.max_inflight_requests_per_host
        budget = self.host_manager.get_host_budget(
            host_key,
            default_budget=default_budget,
        )
        return max(1, int(budget))

    async def _finalize_failed_task(self, failed: _FailedTask) -> CrawlFailure:
        """Apply scheduler mutations for a failed crawl outside fetch and parse workers."""
        return await self._finalizer_service().finalize_failed_task(failed)

    def _is_valid_url(self, url: str) -> bool:
        """Check if URL should be crawled."""
        if self.same_host:
            return urlparse(url).netloc == self.start_host
        return True

    def _is_egress_allowed_url(self, url: str) -> bool:
        """Check fast egress rules that do not require DNS resolution."""
        return is_url_allowed_without_dns(
            url,
            allow_private_network_egress=settings.allow_private_network_egress,
            allowed_ports=settings.allowed_egress_ports,
        ).allowed

    def _build_seed_task(self, url: str) -> CrawlTask:
        """Build the initial scheduler task for an explicit seed URL."""
        decision = rank_seed_url(url)
        return CrawlTask(
            url=url,
            discovery_value=decision.discovery_value,
            runnable_surface=SCHEDULER_SURFACE_RUNNABLE,
            intent=INTENT_EXPLORE,
        )

    def _build_page_signals(self, response) -> PageSignals:
        """Extract lightweight ranking signals from a fetched page."""
        content_type = _response_header(response, "content-type")
        title = None
        meta_robots = None

        if "html" in content_type.lower():
            if match := _TITLE_PATTERN.search(response.text):
                title = match.group(1).strip()[:300]
            if match := _META_ROBOTS_PATTERN.search(response.text):
                meta_robots = match.group(1).strip().lower()

        return PageSignals(
            content_type=content_type,
            content_length=_response_content_length(response),
            title=title,
            meta_robots=meta_robots,
        )

    def _build_discovered_tasks(
        self,
        parent_url: str,
        links: list[str],
        parent_signals: PageSignals | None = None,
    ) -> list[CrawlTask]:
        """Assign ranking metadata to discovered outlinks before enqueueing."""
        tasks, _counts = self._build_discovered_tasks_with_admission_counts(
            parent_url,
            links,
            parent_signals=parent_signals,
        )
        return tasks

    def _build_discovered_tasks_with_admission_counts(
        self,
        parent_url: str,
        links: list[str],
        parent_signals: PageSignals | None = None,
    ) -> tuple[list[CrawlTask], dict[str, int]]:
        """Build admitted tasks and explain why discovered links were kept or rejected."""
        admission_control = self._admission_control()
        self._cycle.admission_control = admission_control.snapshot()
        host_contexts = self._host_admission_contexts(links)
        result = DiscoveryAdmissionPolicy(
            seed_hosts=self.seed_hosts,
            is_valid_url=self._is_valid_url,
            is_egress_allowed_url=self._is_egress_allowed_url,
        ).build_tasks(
            parent_url=parent_url,
            links=links,
            parent_signals=parent_signals,
            admission_control=admission_control,
            host_contexts=host_contexts,
        )
        selected = result.tasks
        admission_counts = result.counts
        selected = self.scheduler.preview_tasks(selected)
        admission_counts["admitted"] = len(selected)
        return selected, dict(admission_counts)

    def _admission_control(self) -> AdmissionControl:
        pending = self._cached_pending_count()
        return build_admission_control(
            pending=pending,
            target_pending=settings.admission_target_pending,
        )

    def _cached_pending_count(self) -> int:
        now = time.monotonic()
        cached_at, pending = self._frontier_pending_cache
        if now - cached_at < 30.0:
            return pending
        try:
            pending = int(self.scheduler.pending_count() or 0)
        except Exception:
            logger.debug("Failed to read pending count for admission pressure", exc_info=True)
            pending = 0
        self._frontier_pending_cache = (now, pending)
        return pending

    def _host_admission_contexts(self, links: list[str]) -> dict[str, HostAdmissionContext]:
        hosts = sorted({host_key(link) for link in links if self._is_valid_url(link)})
        if not hosts or self.host_ledger_store is None:
            return {}
        try:
            records = self.host_ledger_store.get_many(hosts)
        except Exception:
            logger.debug("Failed to read host admission context", exc_info=True)
            return {}
        return {
            host: HostAdmissionContext(
                known=True,
                robots_status=record.robots_status,
                failure_count=record.failure_count,
                success_count=record.success_count,
            )
            for host, record in records.items()
        }

    async def _finalize_skipped_task(self, skipped: _SkippedTask) -> _SkippedTask:
        """Apply scheduler mutations for a skipped crawl outside fetch workers."""
        return await self._finalizer_service().finalize_skipped_task(skipped)

    async def _process_url(
        self, task: CrawlTask
    ) -> _FetchedPage | _FailedTask | _SkippedTask | None:
        """Process a single URL."""
        url = task.url
        timings = CrawlStageTimings()
        process_started = time.perf_counter()

        precheck_started = time.perf_counter()
        if not self._is_egress_allowed_url(url):
            timings.precheck_ms = _elapsed_ms(precheck_started)
            return _SkippedTask(
                task=task,
                reason="egress_blocked",
                timings=timings,
                process_started=process_started,
            )
        robots_started = time.perf_counter()
        try:
            robots_decision = await self.host_manager.is_allowed(url)
        except Exception as e:
            timings.robots_ms = _elapsed_ms(robots_started)
            timings.precheck_ms = _elapsed_ms(precheck_started)
            timings.fetch = FetchTelemetry(
                outcome="precheck_error",
                final_url=url,
                total_ms=timings.precheck_ms,
                error=str(e),
            )
            return self._build_retryable_failed_task(
                task=task,
                error=str(e),
                timings=timings,
                process_started=process_started,
            )
        timings.robots = robots_decision
        timings.robots_ms = robots_decision.elapsed_ms
        if not robots_decision.allowed:
            timings.precheck_ms = _elapsed_ms(precheck_started)
            return _SkippedTask(
                task=task,
                reason="robots_denied",
                timings=timings,
                process_started=process_started,
            )

        rate_limit_started = time.perf_counter()
        try:
            await self.host_manager.wait_for_rate_limit(url)
        except Exception as e:
            timings.rate_limit_ms = _elapsed_ms(rate_limit_started)
            timings.precheck_ms = _elapsed_ms(precheck_started)
            timings.fetch = FetchTelemetry(
                outcome="precheck_error",
                final_url=url,
                total_ms=timings.precheck_ms,
                error=str(e),
            )
            return self._build_retryable_failed_task(
                task=task,
                error=str(e),
                timings=timings,
                process_started=process_started,
            )
        timings.rate_limit_ms = _elapsed_ms(rate_limit_started)
        timings.precheck_ms = _elapsed_ms(precheck_started)

        fetch_started = time.perf_counter()
        try:
            response = await asyncio.wait_for(
                self.fetcher.fetch(url),
                timeout=settings.fetch_total_timeout,
            )
            timings.fetch_ms = _elapsed_ms(fetch_started)
            timings.fetch_request_ms = response.fetch_request_ms
            timings.fetch_body_read_ms = response.fetch_body_read_ms
            timings.fetch = response.telemetry
            if timings.fetch is None:
                timings.fetch = FetchTelemetry(
                    outcome="http_error" if response.status >= 400 else "ok",
                    status=response.status,
                    final_url=response.url,
                    content_length=_response_content_length(response),
                    bytes_read=len(response.content),
                    metadata_only=response.metadata_only,
                    body_truncated=response.body_truncated,
                    admission_reason=response.admission_reason,
                    total_ms=timings.fetch_ms,
                    response_headers_ms=timings.fetch_request_ms,
                    body_read_ms=timings.fetch_body_read_ms,
                )

            if response.status >= 400:
                if 400 <= response.status < 500:
                    if response.status in {401, 403}:
                        backoff_seconds = self._record_error_runtime(url)
                        failed = _FailedTask(
                            task=task,
                            failure=CrawlFailure(
                                url=response.url,
                                error=f"http_{response.status}",
                                retryable=False,
                                timings=timings,
                            ),
                            process_started=process_started,
                            record_error=True,
                            backoff_seconds=backoff_seconds,
                        )
                    else:
                        self.host_manager.record_success_runtime(url)
                        failed = _FailedTask(
                            task=task,
                            failure=CrawlFailure(
                                url=response.url,
                                error=f"http_{response.status}",
                                retryable=False,
                                timings=timings,
                            ),
                            process_started=process_started,
                            mark_done=True,
                            record_success=True,
                        )
                else:
                    backoff_seconds = self._record_error_runtime(url)
                    failed = _FailedTask(
                        task=task,
                        failure=CrawlFailure(
                            url=response.url,
                            error=f"http_{response.status}",
                            retryable=True,
                            timings=timings,
                        ),
                        process_started=process_started,
                        record_error=True,
                        backoff_seconds=backoff_seconds,
                    )
                return failed

            return _FetchedPage(
                task=task,
                response=response,
                timings=timings,
                process_started=process_started,
            )

        except (httpx.TimeoutException, asyncio.TimeoutError):
            timings.fetch_ms = _elapsed_ms(fetch_started)
            timings.fetch = FetchTelemetry(
                outcome="timeout",
                final_url=url,
                total_ms=timings.fetch_ms,
                error="timeout",
            )
            backoff_seconds = self._record_error_runtime(url)
            return _FailedTask(
                task=task,
                failure=CrawlFailure(
                    url=url,
                    error="timeout",
                    retryable=True,
                    timings=timings,
                ),
                process_started=process_started,
                record_error=True,
                backoff_seconds=backoff_seconds,
            )

        except httpx.ConnectError:
            timings.fetch_ms = _elapsed_ms(fetch_started)
            timings.fetch = FetchTelemetry(
                outcome="connect_error",
                final_url=url,
                total_ms=timings.fetch_ms,
                error="connection_error",
            )
            backoff_seconds = self._record_error_runtime(url)
            return _FailedTask(
                task=task,
                failure=CrawlFailure(
                    url=url,
                    error="connection_error",
                    retryable=True,
                    timings=timings,
                ),
                process_started=process_started,
                record_error=True,
                backoff_seconds=backoff_seconds,
            )

        except EgressBlockedError as e:
            timings.fetch_ms = _elapsed_ms(fetch_started)
            timings.fetch = FetchTelemetry(
                outcome="egress_blocked",
                final_url=e.decision.url,
                total_ms=timings.fetch_ms,
                error=e.decision.reason,
            )
            return _SkippedTask(
                task=task,
                reason="egress_blocked",
                timings=timings,
                process_started=process_started,
            )

        except Exception as e:
            timings.fetch_ms = _elapsed_ms(fetch_started)
            timings.fetch = FetchTelemetry(
                outcome="unexpected_error",
                final_url=url,
                total_ms=timings.fetch_ms,
                error=str(e),
            )
            return self._build_retryable_failed_task(
                task=task,
                error=str(e),
                timings=timings,
                process_started=process_started,
            )

    def _host_key_for_url(self, url: str) -> str:
        """Return the host key used for in-flight request limiting."""
        return urlparse(url).netloc.lower()

    async def _release_active_host(self, url: str) -> None:
        """Release in-flight host reservations after fetch processing finishes."""
        host_key = self._host_key_for_url(url)
        async with self._lease_lock:
            current = self._cycle.active_host_counts.get(host_key, 0)
            if current <= 1:
                self._cycle.active_host_counts.pop(host_key, None)
            else:
                self._cycle.active_host_counts[host_key] = current - 1

    async def _claim_page_slot(self) -> bool:
        """Reserve capacity so concurrent workers do not exceed max_pages."""
        async with self._page_lock:
            if self._cycle.pages_crawled + self._cycle.claimed_pages >= self.max_pages:
                return False
            self._cycle.claimed_pages += 1
            return True

    async def _release_page_slot(self, success: bool):
        """Release a reserved page slot and commit successful crawls."""
        async with self._page_lock:
            self._cycle.claimed_pages -= 1
            if success:
                self._cycle.pages_crawled += 1

    async def _worker(
        self,
        worker_id: int,
        *,
        lease_lane: _LeaseLane,
    ):
        """Worker coroutine that processes URLs from a dedicated runnable surface."""
        stage = FetchStage(
            parse_queue=self._cycle.parse_queue,
            finalize_queue=self._cycle.finalize_queue,
            parse_stats=self._cycle.pipeline_queues.parse_stats,
            finalize_stats=self._cycle.pipeline_queues.finalize_stats,
            is_running=lambda: self._cycle.running,
            claim_page_slot=self._claim_page_slot,
            release_page_slot=self._release_page_slot,
            lease_task=lambda lease_started: self._lease_task(
                lease_lane=lease_lane,
                lease_started=lease_started,
            ),
            process_url=self._process_url,
            release_active_host=self._release_active_host,
            record_failure_category=self._record_failure_category,
            worker_patience=_WORKER_PATIENCE,
        )
        await stage.run()

    def _prepare_parsed_payload(
        self,
        task: CrawlTask,
        response: Response,
    ) -> tuple[str, bytes, list[str], list[CrawlTask], int, dict[str, int]]:
        """Prepare parsed content and discovered tasks away from the event loop."""
        if response.metadata_only:
            return "", b"", [], [], 0, {}

        stores_text = should_store_text_content(
            _response_header(response, "content-type"),
            response.content,
        )
        content = response.text if stores_text else ""
        content_bytes = response.content if stores_text else b""
        outlinks: list[str] = []
        new_tasks: list[CrawlTask] = []
        admission_counts: dict[str, int] = {}

        if should_extract_links(
            _response_header(response, "content-type"),
            response.content,
        ):
            links = extract_links(response.text, response.url)
            page_signals = self._build_page_signals(response)
            new_tasks, admission_counts = self._build_discovered_tasks_with_admission_counts(
                task.url,
                links,
                parent_signals=page_signals,
            )
            outlinks = [new_task.url for new_task in new_tasks]
            outlink_count = len(links)
        else:
            outlink_count = 0

        return content, content_bytes, outlinks, new_tasks, outlink_count, admission_counts

    async def _parse_fetched_page(self, fetched: _FetchedPage) -> _ParsedPage:
        """Parse fetched content into a publishable payload outside fetch workers."""
        task = fetched.task
        response = fetched.response
        timings = fetched.timings

        parse_started = time.perf_counter()
        (
            content,
            content_bytes,
            outlinks,
            new_tasks,
            outlink_count,
            admission_counts,
        ) = await asyncio.to_thread(self._prepare_parsed_payload, task, response)
        if admission_counts:
            self._cycle.timing_summary.record_discovery_admission(admission_counts)
        timings.parse_ms = _elapsed_ms(parse_started)

        return _ParsedPage(
            task=task,
            new_tasks=new_tasks,
            process_started=fetched.process_started,
            result=CrawlResult(
                url=response.url,
                status=response.status,
                content_length=_response_content_length(response),
                source_url=task.source_url,
                timestamp=time.time(),
                content=content,
                outlinks=outlinks,
                timings=timings,
                content_type=_response_header(response, "content-type"),
                discovery_value=task.discovery_value,
                outlink_count=outlink_count,
                content_bytes=content_bytes,
                body_truncated=response.body_truncated,
            ),
        )

    def _build_parse_failed_task(self, fetched: _FetchedPage, exc: Exception) -> _FailedTask:
        """Convert parse-stage exceptions into scheduler-finalizable failures."""
        backoff_seconds = self._record_error_runtime(fetched.task.url)
        failure = CrawlFailure(
            url=fetched.task.url,
            error=str(exc),
            retryable=self.host_manager.should_retry(fetched.task.url),
            timings=fetched.timings,
        )
        return _FailedTask(
            task=fetched.task,
            failure=failure,
            process_started=fetched.process_started,
            record_error=True,
            backoff_seconds=backoff_seconds,
        )

    async def _parser(self):
        """Drain fetched pages and parse them into crawl results."""
        stage = ParseStage(
            parse_queue=self._cycle.parse_queue,
            finalize_queue=self._cycle.finalize_queue,
            parse_stats=self._cycle.pipeline_queues.parse_stats,
            finalize_stats=self._cycle.pipeline_queues.finalize_stats,
            liveness=self._cycle.parser_liveness,
            parse_fetched_page=self._parse_fetched_page,
            build_failed_task=self._build_parse_failed_task,
            record_failure_category=self._record_failure_category,
        )
        await stage.run()

    async def _finalize_parsed_page(self, parsed: _ParsedPage) -> CrawlResult:
        """Persist one parsed page and apply its scheduler outcome."""
        return (await self._finalize_parsed_pages([parsed]))[0]

    async def _finalize_parsed_pages(self, parsed_pages: list[_ParsedPage]) -> list[CrawlResult]:
        """Persist parsed pages before applying scheduler completion mutations."""
        return await self._finalizer_service().finalize_parsed_pages(
            parsed_pages,
            record_success_runtime=self.host_manager.record_success_runtime,
        )

    async def _finalizer(self):
        """Persist parsed payloads and apply their scheduler outcome."""
        stage = FinalizeStage(
            finalize_queue=self._cycle.finalize_queue,
            finalize_stats=self._cycle.pipeline_queues.finalize_stats,
            liveness=self._cycle.finalizer_liveness,
            finalize_parsed_page=self._finalize_parsed_page,
            finalize_parsed_pages=self._finalize_parsed_pages,
            finalize_skipped_task=self._finalize_skipped_task,
            finalize_failed_task=self._finalize_failed_task,
            record_timing=self._cycle.record_timing,
            record_failure_category=self._record_failure_category,
            progress=self._progress,
            format_timings=_format_timings,
            success_batch_size=settings.finalizer_batch_size,
            success_batch_wait_ms=settings.finalizer_batch_wait_ms,
        )
        await stage.run()

    async def _lease_task(
        self,
        *,
        lease_lane: _LeaseLane,
        lease_started: float,
    ) -> tuple[CrawlTask | None, LeaseTelemetry]:
        """Lease work for a specific runnable worker pool."""
        async with self._lease_lock:
            excluded_hosts = [
                host
                for host, count in self._cycle.active_host_counts.items()
                if count >= self._host_inflight_budget(host)
            ]
            task = self.scheduler.lease_next(
                runnable_surface=lease_lane.runnable_surface,
                lease_strategy=lease_lane.lease_strategy,
                exclude_hosts=excluded_hosts or None,
                execution_tiers=list(lease_lane.execution_tiers)
                if lease_lane.execution_tiers is not None
                else None,
            )
            if task is not None:
                host_key = self._host_key_for_url(task.url)
                self._cycle.active_host_counts[host_key] += 1
            read_model = "unknown"
            fallback = "none"
            if lease_lane.lease_strategy == LEASE_STRATEGY_HOST_FIRST:
                diagnostics = self.scheduler.last_lease_diagnostics()
                read_model = str(diagnostics.get("read_model", "unknown"))
                fallback = str(diagnostics.get("fallback", "none"))
                execution_tier = str(diagnostics.get("execution_tier", "unknown"))
            else:
                execution_tier = "unknown"
            lease = LeaseTelemetry(
                outcome="leased" if task is not None else "empty",
                runnable_surface=lease_lane.runnable_surface,
                lease_strategy=lease_lane.lease_strategy,
                elapsed_ms=_elapsed_ms(lease_started),
                excluded_hosts_count=len(excluded_hosts),
                read_model=read_model,
                fallback=fallback,
                execution_tier=execution_tier,
            )
            return task, lease

    async def crawl(self) -> list[dict]:
        """Run the crawler and return results."""
        self._cycle = CrawlCycle(queue_maxsize=self._pipeline_queue_maxsize)
        self._cycle.running = True
        self.scheduler.reset_host_first_fallback_stats()

        if (
            self.start_url
            and self._is_egress_allowed_url(self.start_url)
            and self.scheduler.pending_count() == 0
        ):
            self.scheduler.place(self._build_seed_task(self.start_url))

        finalizer_dsn = self.pg_storage.dsn if self.pg_storage is not None else None
        if finalizer_dsn and self._finalizer_storage is None:
            from .storage import PgStorage

            self._finalizer_storage = PgStorage(finalizer_dsn)
            self._finalizer_scheduler = Scheduler(self._finalizer_storage.conn)
            self._finalizer_host_store = HostStore(
                self._finalizer_storage.conn,
                default_delay=self.host_manager.default_delay,
            )
        if finalizer_dsn and self._finalizer_executor is None:
            self._finalizer_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="crawler-finalizer",
            )
        parsers = [asyncio.create_task(self._parser()) for _ in range(self.parser_workers)]
        finalizers = [asyncio.create_task(self._finalizer()) for _ in range(self.parser_workers)]

        workers: list[asyncio.Task] = []
        worker_id = 0
        warm_lane = _LeaseLane(
            runnable_surface=SCHEDULER_SURFACE_NORMAL,
            lease_strategy=LEASE_STRATEGY_HOST_FIRST,
            intent=INTENT_EXPLORE,
            execution_tiers=(HOST_EXECUTION_TIER_WARM,),
        )
        probing_lane = _LeaseLane(
            runnable_surface=SCHEDULER_SURFACE_NORMAL,
            lease_strategy=LEASE_STRATEGY_HOST_FIRST,
            intent=INTENT_EXPLORE,
            execution_tiers=(
                HOST_EXECUTION_TIER_PROBING,
                HOST_EXECUTION_TIER_SLOW,
                HOST_EXECUTION_TIER_DEFERRED,
            ),
        )
        refresh_lane = _LeaseLane(
            runnable_surface=SCHEDULER_SURFACE_REFRESH,
            lease_strategy=LEASE_STRATEGY_URL_ORDER,
            intent=INTENT_REFRESH,
        )
        for _ in range(self.warm_workers):
            workers.append(
                asyncio.create_task(
                    self._worker(
                        worker_id,
                        lease_lane=warm_lane,
                    )
                )
            )
            worker_id += 1
        for _ in range(self.probing_workers):
            workers.append(
                asyncio.create_task(
                    self._worker(
                        worker_id,
                        lease_lane=probing_lane,
                    )
                )
            )
            worker_id += 1
        for _ in range(self.refresh_workers):
            workers.append(
                asyncio.create_task(
                    self._worker(
                        worker_id,
                        lease_lane=refresh_lane,
                    )
                )
            )
            worker_id += 1

        try:
            await asyncio.gather(*workers)
            await self._cycle.parse_queue.join()
            for _ in range(self.parser_workers):
                await self._cycle.parse_queue.put(_PARSER_SENTINEL)
            await asyncio.gather(*parsers)

            await self._cycle.finalize_queue.join()
            for _ in range(len(finalizers)):
                await self._cycle.finalize_queue.put(_FINALIZER_SENTINEL)
            await asyncio.gather(*finalizers)
        finally:
            self._cycle.running = False

        return self._cycle.results

    def stop(self):
        """Stop the crawler."""
        self._cycle.running = False


async def run_crawl(
    start_url: str,
    max_pages: int = 100,
    same_host: bool = True,
    output_file: str | None = None,
    use_browser: bool = False,
    delay: float = 1.0,
    concurrency: int = 5,
    include_content: bool = True,
    postgres_dsn: str | None = None,
):
    """Run a crawl and save results."""
    if not postgres_dsn:
        raise ValueError("--postgres is required")

    typer.echo(f"Starting crawl from {start_url}")
    typer.echo(f"Max pages: {max_pages}, Concurrency: {concurrency}")

    start_time = time.time()
    from .storage import PgStorage

    with PgStorage(postgres_dsn) as pg_storage:
        writer = (
            StreamingOutputWriter(output_file, include_content=include_content)
            if output_file
            else None
        )
        try:
            if writer:
                writer.__enter__()
            async with CrawlerEngine(
                start_url=start_url,
                max_pages=max_pages,
                same_host=same_host,
                use_browser=use_browser,
                delay=delay,
                concurrency=concurrency,
                output_writer=writer,
                pg_storage=pg_storage,
                seed_urls=[start_url],
            ) as engine:
                await engine.crawl()
                elapsed = time.time() - start_time
                typer.echo(f"\nCrawl complete: {engine.pages_crawled} pages in {elapsed:.1f}s")
                typer.echo(f"Postgres: {pg_storage.count} pages saved")
                if output_file:
                    typer.echo(f"Results saved to {output_file}")
                typer.echo(f"Scheduler stats: {engine.scheduler.stats()}")
        finally:
            if writer:
                writer.__exit__(None, None, None)
