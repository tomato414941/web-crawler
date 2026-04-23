"""Execution services for finalization and publishing."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import concurrent.futures
from dataclasses import dataclass, replace
import time
from typing import TYPE_CHECKING

from .result import CrawlFailure, CrawlResult
from .telemetry import FINALIZER_TIMING_FIELDS, FinalizerTelemetry, PublisherTelemetry

if TYPE_CHECKING:
    from .host_store import HostStore
    from .pipeline import FailedTask, ParsedPage, SkippedTask
    from .storage import PgStorage
    from .url_ledger import UrlLedger


def _elapsed_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 1)


def _elapsed_ms_between(started_at: float, finished_at: float) -> float:
    return round((finished_at - started_at) * 1000, 1)


@dataclass(frozen=True, slots=True)
class _TimedStorageSaveMany:
    started_at: float
    finished_at: float
    save_results: list[object] | None = None
    error: Exception | None = None


@dataclass(frozen=True, slots=True)
class _TimedOutputWrite:
    started_at: float
    finished_at: float
    error: Exception | None = None


def _timed_storage_save_many(
    storage: object,
    results: list[CrawlResult],
) -> _TimedStorageSaveMany:
    started_at = time.perf_counter()
    try:
        save_results = storage.save_many(results)
    except Exception as exc:  # pragma: no cover - exercised via caller path
        finished_at = time.perf_counter()
        return _TimedStorageSaveMany(
            started_at=started_at,
            finished_at=finished_at,
            error=exc,
        )
    finished_at = time.perf_counter()
    return _TimedStorageSaveMany(
        started_at=started_at,
        finished_at=finished_at,
        save_results=save_results,
    )


def _timed_output_write(output_writer: object, result: CrawlResult) -> _TimedOutputWrite:
    started_at = time.perf_counter()
    try:
        output_writer.write_one(result)
    except Exception as exc:  # pragma: no cover - exercised via caller path
        finished_at = time.perf_counter()
        return _TimedOutputWrite(
            started_at=started_at,
            finished_at=finished_at,
            error=exc,
        )
    finished_at = time.perf_counter()
    return _TimedOutputWrite(
        started_at=started_at,
        finished_at=finished_at,
    )


class FinalizerService:
    """Own blocking finalizer work and telemetry shaping."""

    def __init__(
        self,
        *,
        scheduler: "UrlLedger",
        host_store: "HostStore | None",
        executor: concurrent.futures.ThreadPoolExecutor | None,
        host_key_for_url: Callable[[str], str],
    ) -> None:
        self.scheduler = scheduler
        self.host_store = host_store
        self.executor = executor
        self.host_key_for_url = host_key_for_url

    def finalize_success_batch_sync(self, parsed_pages: list["ParsedPage"]) -> FinalizerTelemetry:
        """Apply success scheduler mutations for multiple parsed pages in one batch."""
        all_new_tasks = [new_task for parsed in parsed_pages for new_task in parsed.new_tasks]
        telemetry = FinalizerTelemetry(
            kind="success",
            new_tasks_count=len(all_new_tasks),
            batch_size=len(parsed_pages),
        )
        total_started = time.perf_counter()
        if all_new_tasks:
            if hasattr(self.scheduler, "discover_many") and hasattr(
                self.scheduler, "admit_discovered_tasks"
            ):
                discover_started = time.perf_counter()
                self.scheduler.discover_many(all_new_tasks)
                telemetry.discover_ms = _elapsed_ms(discover_started)
                admit_started = time.perf_counter()
                self.scheduler.admit_discovered_tasks(all_new_tasks)
                telemetry.admit_ms = _elapsed_ms(admit_started)
                diagnostics_fn = getattr(self.scheduler, "last_admission_diagnostics", None)
                if callable(diagnostics_fn):
                    diagnostics = diagnostics_fn()
                    for field in FINALIZER_TIMING_FIELDS:
                        if field.startswith("admit_") and field in diagnostics:
                            setattr(telemetry, field, float(diagnostics[field]))
            else:
                admit_started = time.perf_counter()
                self.scheduler.place_many(all_new_tasks)
                telemetry.admit_ms = _elapsed_ms(admit_started)

        if self.host_store is not None:
            host_started = time.perf_counter()
            success_records = [
                (
                    self.host_key_for_url(parsed.task.url),
                    parsed.result.timings.fetch_request_ms or parsed.result.timings.fetch_ms,
                )
                for parsed in parsed_pages
            ]
            if hasattr(self.host_store, "record_success_many"):
                self.host_store.record_success_many(success_records)
            else:
                for host_key, request_latency_ms in success_records:
                    self.host_store.record_success(
                        host_key,
                        request_latency_ms=request_latency_ms,
                    )
            telemetry.host_success_ms = _elapsed_ms(host_started)

        mark_started = time.perf_counter()
        if hasattr(self.scheduler, "mark_done_many"):
            self.scheduler.mark_done_many([parsed.task for parsed in parsed_pages])
        else:
            for parsed in parsed_pages:
                self.scheduler.mark_done(parsed.task.url, lease_token=parsed.task.lease_token)
        telemetry.mark_done_ms = _elapsed_ms(mark_started)
        telemetry.total_ms = _elapsed_ms(total_started)
        return telemetry

    async def finalize_parsed_pages(
        self,
        parsed_pages: list["ParsedPage"],
        *,
        record_success_runtime: Callable[[str], None],
    ) -> list[CrawlResult]:
        """Finalize successful parsed pages outside parse workers."""
        if not parsed_pages:
            return []
        scheduler_started = time.perf_counter()
        if self.executor is not None:
            loop = asyncio.get_running_loop()
            telemetry = await loop.run_in_executor(
                self.executor,
                self.finalize_success_batch_sync,
                parsed_pages,
            )
        else:
            telemetry = self.finalize_success_batch_sync(parsed_pages)
        scheduler_ms = _elapsed_ms(scheduler_started)
        results: list[CrawlResult] = []
        for parsed in parsed_pages:
            record_success_runtime(parsed.task.url)
            result = parsed.result
            result.timings.scheduler_ms += scheduler_ms
            result.timings.finalizer = replace(
                telemetry,
                new_tasks_count=len(parsed.new_tasks),
            )
            result.timings.process_ms = _elapsed_ms(parsed.process_started)
            results.append(result)
        return results

    def finalize_failed_sync(self, failed: "FailedTask") -> FinalizerTelemetry:
        """Apply durable failure mutations on the dedicated finalizer connection."""
        telemetry = FinalizerTelemetry(kind="failed")
        total_started = time.perf_counter()
        if failed.record_success and self.host_store is not None:
            host_started = time.perf_counter()
            self.host_store.record_success(self.host_key_for_url(failed.task.url))
            telemetry.host_success_ms = _elapsed_ms(host_started)
        if failed.record_error and self.host_store is not None:
            host_started = time.perf_counter()
            self.host_store.record_failure(
                self.host_key_for_url(failed.task.url),
                backoff_seconds=failed.backoff_seconds or 0.0,
            )
            telemetry.host_failure_ms = _elapsed_ms(host_started)

        if failed.mark_done:
            mark_started = time.perf_counter()
            self.scheduler.mark_done(failed.task.url, lease_token=failed.task.lease_token)
            telemetry.mark_done_ms = _elapsed_ms(mark_started)
            telemetry.total_ms = _elapsed_ms(total_started)
            return telemetry

        mark_started = time.perf_counter()
        self.scheduler.mark_failed(
            failed.task.url,
            retryable=failed.failure.retryable,
            error=failed.failure.error,
            backoff_seconds=failed.backoff_seconds,
            lease_token=failed.task.lease_token,
        )
        telemetry.mark_failed_ms = _elapsed_ms(mark_started)
        telemetry.total_ms = _elapsed_ms(total_started)
        return telemetry

    async def finalize_failed_task(self, failed: "FailedTask") -> CrawlFailure:
        """Apply scheduler mutations for a failed crawl outside fetch and parse workers."""
        failure = failed.failure
        scheduler_started = time.perf_counter()
        if self.executor is not None:
            loop = asyncio.get_running_loop()
            telemetry = await loop.run_in_executor(
                self.executor,
                self.finalize_failed_sync,
                failed,
            )
        else:
            telemetry = self.finalize_failed_sync(failed)
        failure.timings.scheduler_ms += _elapsed_ms(scheduler_started)
        failure.timings.finalizer = telemetry
        failure.timings.process_ms = _elapsed_ms(failed.process_started)
        return failure

    def finalize_skipped_sync(self, skipped: "SkippedTask") -> FinalizerTelemetry:
        """Apply durable scheduler mutation for a skipped crawl."""
        telemetry = FinalizerTelemetry(kind="skipped")
        total_started = time.perf_counter()
        mark_started = time.perf_counter()
        self.scheduler.mark_done(skipped.task.url, lease_token=skipped.task.lease_token)
        telemetry.mark_done_ms = _elapsed_ms(mark_started)
        telemetry.total_ms = _elapsed_ms(total_started)
        return telemetry

    async def finalize_skipped_task(self, skipped: "SkippedTask") -> "SkippedTask":
        """Apply scheduler mutations for a skipped crawl outside fetch workers."""
        scheduler_started = time.perf_counter()
        if self.executor is not None:
            loop = asyncio.get_running_loop()
            telemetry = await loop.run_in_executor(
                self.executor,
                self.finalize_skipped_sync,
                skipped,
            )
        else:
            telemetry = self.finalize_skipped_sync(skipped)
        skipped.timings.scheduler_ms += _elapsed_ms(scheduler_started)
        skipped.timings.finalizer = telemetry
        skipped.timings.process_ms = _elapsed_ms(skipped.process_started)
        return skipped


class PublisherService:
    """Own persistence and output publishing."""

    def __init__(
        self,
        *,
        storage: "PgStorage | None",
        executor: concurrent.futures.ThreadPoolExecutor | None,
        output_writer: object | None,
        results_sink: list[dict],
    ) -> None:
        self.storage = storage
        self.executor = executor
        self.output_writer = output_writer
        self.results_sink = results_sink

    async def publish_results(self, results: list[CrawlResult]) -> None:
        """Persist crawl output for one or more finalized crawl results."""
        if not results:
            return
        loop = asyncio.get_running_loop()
        publisher_started = time.perf_counter()
        for result in results:
            result.timings.publisher = result.timings.publisher or PublisherTelemetry()

        if self.storage is not None:
            persist_started = time.perf_counter()
            if self.executor is not None:
                timed_save_many = await loop.run_in_executor(
                    self.executor,
                    _timed_storage_save_many,
                    self.storage,
                    results,
                )
            else:
                timed_save_many = await asyncio.to_thread(
                    _timed_storage_save_many,
                    self.storage,
                    results,
                )
            persist_ms = _elapsed_ms(persist_started)
            save_dispatch_wait_ms = _elapsed_ms_between(
                persist_started,
                timed_save_many.started_at,
            )
            save_run_ms = _elapsed_ms_between(
                timed_save_many.started_at,
                timed_save_many.finished_at,
            )
            for result in results:
                result.timings.persist_ms = persist_ms
                result.timings.publisher.save_dispatch_wait_ms = save_dispatch_wait_ms
                result.timings.publisher.save_run_ms = save_run_ms
            if timed_save_many.error is not None:
                total_ms = _elapsed_ms(publisher_started)
                for result in results:
                    result.timings.publisher.total_ms = total_ms
                raise timed_save_many.error
            for result, save_result in zip(results, timed_save_many.save_results or [], strict=False):
                storage_telemetry = getattr(save_result, "telemetry", None)
                if storage_telemetry is not None:
                    result.timings.storage = storage_telemetry
        if self.output_writer is not None:
            for result in results:
                output_started = time.perf_counter()
                if self.executor is not None:
                    timed_output = await loop.run_in_executor(
                        self.executor,
                        _timed_output_write,
                        self.output_writer,
                        result,
                    )
                else:
                    timed_output = await asyncio.to_thread(
                        _timed_output_write,
                        self.output_writer,
                        result,
                    )
                result.timings.output_ms = _elapsed_ms(output_started)
                result.timings.publisher.output_dispatch_wait_ms = _elapsed_ms_between(
                    output_started,
                    timed_output.started_at,
                )
                result.timings.publisher.output_run_ms = _elapsed_ms_between(
                    timed_output.started_at,
                    timed_output.finished_at,
                )
                if timed_output.error is not None:
                    total_ms = _elapsed_ms(publisher_started)
                    result.timings.publisher.total_ms = total_ms
                    raise timed_output.error
        elif self.storage is None:
            for result in results:
                self.results_sink.append(result.to_dict())
        total_ms = _elapsed_ms(publisher_started)
        for result in results:
            result.timings.publisher.total_ms = total_ms

    async def publish_result(self, result: CrawlResult) -> None:
        """Persist a single crawl result."""
        await self.publish_results([result])
