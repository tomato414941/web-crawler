"""Execution services for crawl finalization."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import concurrent.futures
from dataclasses import replace
import logging
import time
from typing import TYPE_CHECKING

from .result import CrawlFailure, CrawlResult
from .telemetry import FINALIZER_TIMING_FIELDS, FinalizerTelemetry

if TYPE_CHECKING:
    from .host_store import HostStore
    from .pipeline import FailedTask, ParsedPage, SkippedTask
    from .storage import PgStorage
    from .scheduler import Scheduler


logger = logging.getLogger(__name__)


def _elapsed_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 1)


class FinalizerService:
    """Persist successful results, then finalize their scheduler state."""

    def __init__(
        self,
        *,
        scheduler: "Scheduler",
        host_store: "HostStore | None",
        storage: "PgStorage | None",
        executor: concurrent.futures.ThreadPoolExecutor | None,
        output_writer: object | None,
        results_sink: list[dict],
        host_key_for_url: Callable[[str], str],
    ) -> None:
        self.scheduler = scheduler
        self.host_store = host_store
        self.storage = storage
        self.executor = executor
        self.output_writer = output_writer
        self.results_sink = results_sink
        self.host_key_for_url = host_key_for_url

    def _persist_results(self, results: list[CrawlResult]) -> None:
        if self.storage is not None:
            persist_started = time.perf_counter()
            save_results = self.storage.save_many(results)
            persist_ms = _elapsed_ms(persist_started)
            for result, save_result in zip(results, save_results, strict=False):
                result.timings.persist_ms = persist_ms
                storage_telemetry = getattr(save_result, "telemetry", None)
                if storage_telemetry is not None:
                    result.timings.storage = storage_telemetry

        if self.output_writer is not None:
            for result in results:
                output_started = time.perf_counter()
                self.output_writer.write_one(result)
                result.timings.output_ms = _elapsed_ms(output_started)

    def _retry_completion_failures(self, parsed_pages: list["ParsedPage"]) -> None:
        if self.storage is not None:
            self.storage.conn.rollback()
        for parsed in parsed_pages:
            updated = self.scheduler.mark_failed(
                parsed.task.url,
                retryable=True,
                error="completion_error",
                lease_token=parsed.task.lease_token,
            )
            if not updated:
                self.scheduler.mark_failed(
                    parsed.task.url,
                    retryable=True,
                    error="completion_error",
                    lease_token=None,
                )

    def finalize_success_batch_sync(self, parsed_pages: list["ParsedPage"]) -> FinalizerTelemetry:
        """Persist successful pages before applying scheduler completion mutations."""
        try:
            self._persist_results([parsed.result for parsed in parsed_pages])
            return self._finalize_persisted_batch(parsed_pages)
        except Exception:
            self._retry_completion_failures(parsed_pages)
            raise

    def _finalize_persisted_batch(
        self,
        parsed_pages: list["ParsedPage"],
    ) -> FinalizerTelemetry:
        """Apply scheduler mutations for an already-persisted success batch."""
        all_new_tasks = [new_task for parsed in parsed_pages for new_task in parsed.new_tasks]
        telemetry = FinalizerTelemetry(
            kind="success",
            new_tasks_count=len(all_new_tasks),
            batch_size=len(parsed_pages),
        )
        total_started = time.perf_counter()
        if all_new_tasks:
            discover_started = time.perf_counter()
            self.scheduler.discover_many(all_new_tasks)
            telemetry.discover_ms = _elapsed_ms(discover_started)
            admit_started = time.perf_counter()
            self.scheduler.admit_discovered_tasks(all_new_tasks)
            telemetry.admit_ms = _elapsed_ms(admit_started)
            diagnostics = self.scheduler.last_admission_diagnostics()
            for field in FINALIZER_TIMING_FIELDS:
                if field.startswith("admit_") and field in diagnostics:
                    setattr(telemetry, field, float(diagnostics[field]))

        mark_started = time.perf_counter()
        updated_count = self.scheduler.mark_done_many([parsed.task for parsed in parsed_pages])
        if updated_count != len(parsed_pages):
            fallback_count = 0
            for parsed in parsed_pages:
                if self.scheduler.mark_done(parsed.task.url, lease_token=None):
                    fallback_count += 1
            updated_count += fallback_count
            if updated_count < len(parsed_pages):
                logger.warning(
                    "Scheduler mark_done updated fewer success rows than expected: updated=%s expected=%s",
                    updated_count,
                    len(parsed_pages),
                )
            updated_count = min(updated_count, len(parsed_pages))
        telemetry.mark_done_ms = _elapsed_ms(mark_started)

        if self.host_store is not None and updated_count:
            host_started = time.perf_counter()
            success_records = [
                (
                    self.host_key_for_url(parsed.task.url),
                    parsed.result.timings.fetch_request_ms or parsed.result.timings.fetch_ms,
                )
                for parsed in parsed_pages
            ]
            self.host_store.record_success_many(success_records)
            telemetry.host_success_ms = _elapsed_ms(host_started)

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
        if self.executor is not None:
            loop = asyncio.get_running_loop()
            telemetry = await loop.run_in_executor(
                self.executor,
                self.finalize_success_batch_sync,
                parsed_pages,
            )
        else:
            telemetry = self.finalize_success_batch_sync(parsed_pages)
        results: list[CrawlResult] = []
        for parsed in parsed_pages:
            record_success_runtime(parsed.task.url)
            result = parsed.result
            result.timings.scheduler_ms += telemetry.total_ms
            result.timings.finalizer = replace(
                telemetry,
                new_tasks_count=len(parsed.new_tasks),
            )
            result.timings.process_ms = _elapsed_ms(parsed.process_started)
            results.append(result)
        if self.storage is None and self.output_writer is None:
            self.results_sink.extend(result.to_dict() for result in results)
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
            updated = self.scheduler.mark_done(
                failed.task.url,
                lease_token=failed.task.lease_token,
            )
            if not updated:
                updated = self.scheduler.mark_done(failed.task.url, lease_token=None)
            if not updated:
                logger.warning("Scheduler mark_done no-op for failed task: url=%s", failed.task.url)
            telemetry.mark_done_ms = _elapsed_ms(mark_started)
            telemetry.total_ms = _elapsed_ms(total_started)
            return telemetry

        mark_started = time.perf_counter()
        updated = self.scheduler.mark_failed(
            failed.task.url,
            retryable=failed.failure.retryable,
            error=failed.failure.error,
            backoff_seconds=failed.backoff_seconds,
            lease_token=failed.task.lease_token,
        )
        if not updated:
            updated = self.scheduler.mark_failed(
                failed.task.url,
                retryable=failed.failure.retryable,
                error=failed.failure.error,
                backoff_seconds=failed.backoff_seconds,
                lease_token=None,
            )
        if not updated:
            logger.warning("Scheduler mark_failed no-op for failed task: url=%s", failed.task.url)
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
        updated = self.scheduler.mark_done(skipped.task.url, lease_token=skipped.task.lease_token)
        if not updated:
            updated = self.scheduler.mark_done(skipped.task.url, lease_token=None)
        if not updated:
            logger.warning("Scheduler mark_done no-op for skipped task: url=%s", skipped.task.url)
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
