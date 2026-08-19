"""Pipeline stage contract tests."""

import asyncio
from types import SimpleNamespace
import time

import pytest

from crawler.pipeline import (
    FINALIZER_SENTINEL,
    PARSER_SENTINEL,
    PUBLISHER_SENTINEL,
    FailedTask,
    FetchStage,
    FetchedPage,
    FinalizeItem,
    FinalizeStage,
    ParseStage,
    ParsedPage,
    PipelineQueues,
    PublishItem,
    PublishStage,
    QueueStats,
    SkippedTask,
    StageLiveness,
)
from crawler.result import CrawlFailure, CrawlResult, CrawlStageTimings
from crawler.scheduler_task import CrawlTask


def _crawl_result(url="https://example.com/"):
    return CrawlResult(
        url=url,
        status=200,
        content_length=13,
        source_url=None,
        timestamp=1000.0,
        content="<html></html>",
        outlinks=[],
        timings=CrawlStageTimings(),
    )


def _fetched_page(url="https://example.com/"):
    return FetchedPage(
        task=CrawlTask(url=url, lease_token="lease-1"),
        response=object(),
        timings=CrawlStageTimings(),
        process_started=time.perf_counter(),
        enqueued_at=time.perf_counter(),
        queue_depth=0,
    )


def _format_timings(_timings):
    return "timings"


def _progress():
    return 1, 1


async def _no_publish(_result):
    return None


async def _no_finalize_failed(_failed):
    return _failed.failure


async def _no_finalize_skipped(skipped):
    return skipped


class _LeaseTelemetry:
    elapsed_ms = 3.0


def _record(records):
    def record(outcome, timings):
        records.append((outcome, timings))

    return record


def _parse_failure(fetched, exc):
    return FailedTask(
        task=fetched.task,
        failure=CrawlFailure(
            url=fetched.task.url,
            error=str(exc),
            retryable=True,
            timings=fetched.timings,
        ),
        process_started=fetched.process_started,
        record_error=True,
        backoff_seconds=1.0,
    )


@pytest.mark.asyncio
async def test_fetch_stage_success_enqueues_fetched_page():
    queues = PipelineQueues(maxsize=4)
    task = CrawlTask(url="https://example.com/", lease_token="lease-1")
    claimed = 0
    releases = []
    released_hosts = []

    async def claim_page_slot():
        nonlocal claimed
        claimed += 1
        return claimed == 1

    async def release_page_slot(success):
        releases.append(success)

    async def lease_task(_lease_started):
        return task, _LeaseTelemetry()

    async def process_url(leased_task):
        return FetchedPage(
            task=leased_task,
            response=SimpleNamespace(url=leased_task.url),
            timings=CrawlStageTimings(),
            process_started=time.perf_counter(),
        )

    async def release_active_host(url):
        released_hosts.append(url)

    stage = FetchStage(
        parse_queue=queues.parse,
        finalize_queue=queues.finalize,
        parse_stats=queues.parse_stats,
        finalize_stats=queues.finalize_stats,
        is_running=lambda: True,
        claim_page_slot=claim_page_slot,
        release_page_slot=release_page_slot,
        lease_task=lease_task,
        process_url=process_url,
        release_active_host=release_active_host,
        record_failure_category=lambda _error: None,
        worker_patience=1,
    )

    await stage.run()

    fetched = queues.parse.get_nowait()
    assert fetched.task is task
    assert fetched.timings.lease is not None
    assert fetched.timings.lease_ms == 3.0
    assert fetched.timings.slot_ms >= 0
    assert releases == [True]
    assert released_hosts == [task.url]
    assert queues.parse_stats.depth_max == 0
    assert queues.finalize.empty()


@pytest.mark.asyncio
async def test_fetch_stage_routes_skipped_and_failed_to_finalizer():
    queues = PipelineQueues(maxsize=4)
    tasks = [
        CrawlTask(url="https://example.com/skip", lease_token="skip"),
        CrawlTask(url="https://example.com/fail", lease_token="fail"),
    ]
    claimed = 0
    releases = []
    failures = []
    released_hosts = []

    async def claim_page_slot():
        nonlocal claimed
        claimed += 1
        return claimed <= 2

    async def release_page_slot(success):
        releases.append(success)

    async def lease_task(_lease_started):
        return tasks[claimed - 1], _LeaseTelemetry()

    async def process_url(task):
        if task.url.endswith("/skip"):
            return SkippedTask(
                task=task,
                reason="robots_denied",
                timings=CrawlStageTimings(),
                process_started=time.perf_counter(),
            )
        return FailedTask(
            task=task,
            failure=CrawlFailure(
                url=task.url,
                error="timeout",
                retryable=True,
                timings=CrawlStageTimings(),
            ),
            process_started=time.perf_counter(),
            record_error=True,
            backoff_seconds=1.0,
        )

    async def release_active_host(url):
        released_hosts.append(url)

    stage = FetchStage(
        parse_queue=queues.parse,
        finalize_queue=queues.finalize,
        parse_stats=queues.parse_stats,
        finalize_stats=queues.finalize_stats,
        is_running=lambda: True,
        claim_page_slot=claim_page_slot,
        release_page_slot=release_page_slot,
        lease_task=lease_task,
        process_url=process_url,
        release_active_host=release_active_host,
        record_failure_category=failures.append,
        worker_patience=1,
    )

    await stage.run()

    first = queues.finalize.get_nowait()
    second = queues.finalize.get_nowait()
    assert first.skipped is not None
    assert first.skipped.timings.lease_ms == 3.0
    assert second.failed is not None
    assert second.failed.failure.timings.lease_ms == 3.0
    assert failures == ["timeout"]
    assert releases == [False, False]
    assert released_hosts == [task.url for task in tasks]
    assert queues.parse.empty()


@pytest.mark.asyncio
async def test_fetch_stage_releases_slot_when_lease_task_fails():
    queues = PipelineQueues(maxsize=4)
    releases = []

    async def claim_page_slot():
        return True

    async def release_page_slot(success):
        releases.append(success)

    async def lease_task(_lease_started):
        raise RuntimeError("lease unavailable")

    stage = FetchStage(
        parse_queue=queues.parse,
        finalize_queue=queues.finalize,
        parse_stats=queues.parse_stats,
        finalize_stats=queues.finalize_stats,
        is_running=lambda: True,
        claim_page_slot=claim_page_slot,
        release_page_slot=release_page_slot,
        lease_task=lease_task,
        process_url=lambda _task: None,
        release_active_host=lambda _url: None,
        record_failure_category=lambda _error: None,
        worker_patience=1,
    )

    await stage.run()

    assert releases == [False]
    assert queues.parse.empty()
    assert queues.finalize.empty()


@pytest.mark.asyncio
async def test_fetch_stage_converts_process_url_exception_to_failed_item():
    queues = PipelineQueues(maxsize=4)
    task = CrawlTask(url="https://example.com/error", lease_token="lease-1")
    claimed = 0
    releases = []
    released_hosts = []
    failures = []

    async def claim_page_slot():
        nonlocal claimed
        claimed += 1
        return claimed == 1

    async def release_page_slot(success):
        releases.append(success)

    async def lease_task(_lease_started):
        return task, _LeaseTelemetry()

    async def process_url(_task):
        raise RuntimeError("precheck exploded")

    async def release_active_host(url):
        released_hosts.append(url)

    stage = FetchStage(
        parse_queue=queues.parse,
        finalize_queue=queues.finalize,
        parse_stats=queues.parse_stats,
        finalize_stats=queues.finalize_stats,
        is_running=lambda: True,
        claim_page_slot=claim_page_slot,
        release_page_slot=release_page_slot,
        lease_task=lease_task,
        process_url=process_url,
        release_active_host=release_active_host,
        record_failure_category=failures.append,
        worker_patience=1,
    )

    await stage.run()

    item = queues.finalize.get_nowait()
    assert item.failed is not None
    assert item.failed.task is task
    assert item.failed.failure.retryable is True
    assert item.failed.failure.error == "precheck exploded"
    assert releases == [False]
    assert released_hosts == [task.url]
    assert failures == ["precheck exploded"]
    assert queues.parse.empty()


def test_pipeline_queues_snapshot_records_depth_and_wait():
    queues = PipelineQueues(maxsize=4)

    queues.parse_stats.record_enqueue(2)
    wait_ms = queues.parse_stats.record_dequeue(time.perf_counter() - 0.01, 2)
    snapshot = queues.snapshot()

    assert queues.parse.maxsize == 4
    assert wait_ms >= 0
    assert snapshot["parse_queue_wait_last_ms"] == wait_ms
    assert snapshot["parse_queue_wait_max_ms"] == wait_ms
    assert snapshot["parse_queue_depth_max"] == 2
    assert snapshot["pipeline_queue_maxsize"] == 4


def test_stage_liveness_tracks_started_completed_failed_and_current_item():
    liveness = StageLiveness(include_kind=True)

    liveness.start(kind="success", url="https://example.com/")
    snapshot = liveness.snapshot()
    assert snapshot["started"] == 1
    assert snapshot["current_url"] == "https://example.com/"
    assert snapshot["current_kind"] == "success"

    liveness.complete()
    liveness.clear_current()
    liveness.fail()

    assert liveness.snapshot() == {
        "started": 1,
        "completed": 1,
        "failed": 1,
        "last_progress_at": liveness.last_progress_at,
        "current_url": None,
        "current_kind": None,
    }


@pytest.mark.asyncio
async def test_parse_stage_success_enqueues_parsed_finalize_item():
    queues = PipelineQueues(maxsize=4)
    liveness = StageLiveness(include_kind=True)
    failures = []

    async def parse_fetched(fetched):
        return ParsedPage(
            task=fetched.task,
            result=_crawl_result(fetched.task.url),
            new_tasks=[],
            process_started=fetched.process_started,
        )

    stage = ParseStage(
        parse_queue=queues.parse,
        finalize_queue=queues.finalize,
        parse_stats=queues.parse_stats,
        finalize_stats=queues.finalize_stats,
        liveness=liveness,
        parse_fetched_page=parse_fetched,
        build_failed_task=_parse_failure,
        record_failure_category=failures.append,
    )
    worker = asyncio.create_task(stage.run())

    await queues.parse.put(_fetched_page())
    await asyncio.wait_for(queues.parse.join(), timeout=2)
    await queues.parse.put(PARSER_SENTINEL)
    await asyncio.wait_for(worker, timeout=2)

    finalize_item = queues.finalize.get_nowait()
    assert finalize_item.parsed is not None
    assert finalize_item.failed is None
    assert finalize_item.parsed.task.url == "https://example.com/"
    assert failures == []
    assert liveness.snapshot()["started"] == 1
    assert liveness.snapshot()["completed"] == 1
    assert liveness.snapshot()["failed"] == 0


@pytest.mark.asyncio
async def test_parse_stage_converts_parse_error_to_failed_finalize_item():
    queues = PipelineQueues(maxsize=4)
    failures = []

    async def parse_fetched(_fetched):
        raise RuntimeError("parse boom")

    stage = ParseStage(
        parse_queue=queues.parse,
        finalize_queue=queues.finalize,
        parse_stats=queues.parse_stats,
        finalize_stats=queues.finalize_stats,
        liveness=StageLiveness(include_kind=True),
        parse_fetched_page=parse_fetched,
        build_failed_task=_parse_failure,
        record_failure_category=failures.append,
    )
    worker = asyncio.create_task(stage.run())

    await queues.parse.put(_fetched_page())
    await asyncio.wait_for(queues.parse.join(), timeout=2)
    await queues.parse.put(PARSER_SENTINEL)
    await asyncio.wait_for(worker, timeout=2)

    finalize_item = queues.finalize.get_nowait()
    assert finalize_item.failed is not None
    assert finalize_item.parsed is None
    assert finalize_item.failed.failure.error == "parse boom"
    assert failures == ["parse boom"]


@pytest.mark.asyncio
async def test_parse_stage_survives_parse_error_and_drains_next_item():
    queues = PipelineQueues(maxsize=4)
    calls = []

    async def parse_fetched(fetched):
        calls.append(fetched.task.url)
        if len(calls) == 1:
            raise RuntimeError("parse boom")
        return ParsedPage(
            task=fetched.task,
            result=_crawl_result(fetched.task.url),
            new_tasks=[],
            process_started=fetched.process_started,
        )

    stage = ParseStage(
        parse_queue=queues.parse,
        finalize_queue=queues.finalize,
        parse_stats=queues.parse_stats,
        finalize_stats=queues.finalize_stats,
        liveness=StageLiveness(include_kind=True),
        parse_fetched_page=parse_fetched,
        build_failed_task=_parse_failure,
        record_failure_category=lambda _error: None,
    )
    worker = asyncio.create_task(stage.run())

    await queues.parse.put(_fetched_page("https://example.com/first"))
    await queues.parse.put(_fetched_page("https://example.com/second"))
    await asyncio.wait_for(queues.parse.join(), timeout=2)
    await queues.parse.put(PARSER_SENTINEL)
    await asyncio.wait_for(worker, timeout=2)

    first = queues.finalize.get_nowait()
    second = queues.finalize.get_nowait()
    assert first.failed is not None
    assert second.parsed is not None
    assert calls == ["https://example.com/first", "https://example.com/second"]


@pytest.mark.asyncio
async def test_finalize_stage_success_enqueues_publish_item_and_records_liveness():
    queues = PipelineQueues(maxsize=4)
    liveness = StageLiveness(include_kind=True)
    records = []

    async def finalize_parsed(parsed):
        return parsed.result

    stage = FinalizeStage(
        finalize_queue=queues.finalize,
        publish_queue=queues.publish,
        finalize_stats=queues.finalize_stats,
        publish_stats=queues.publish_stats,
        liveness=liveness,
        finalize_parsed_page=finalize_parsed,
        finalize_skipped_task=_no_finalize_skipped,
        finalize_failed_task=_no_finalize_failed,
        publish_result=_no_publish,
        record_timing=_record(records),
        progress=_progress,
        format_timings=_format_timings,
    )
    worker = asyncio.create_task(stage.run())
    result = _crawl_result()

    await queues.finalize.put(
        FinalizeItem(
            parsed=ParsedPage(
                task=CrawlTask(url=result.url, lease_token="lease-1"),
                result=result,
                new_tasks=[],
                process_started=time.perf_counter(),
            ),
            enqueued_at=time.perf_counter(),
            queue_depth=0,
        )
    )

    await asyncio.wait_for(queues.finalize.join(), timeout=2)
    await queues.finalize.put(FINALIZER_SENTINEL)
    await asyncio.wait_for(worker, timeout=2)

    publish_item = queues.publish.get_nowait()
    assert publish_item.result is result
    assert liveness.snapshot()["started"] == 1
    assert liveness.snapshot()["completed"] == 1
    assert liveness.snapshot()["failed"] == 0
    assert records == []


@pytest.mark.asyncio
async def test_finalize_stage_batches_success_items():
    queues = PipelineQueues(maxsize=8)
    liveness = StageLiveness(include_kind=True)
    batches = []

    async def finalize_batch(parsed_pages):
        batches.append([parsed.task.url for parsed in parsed_pages])
        return [parsed.result for parsed in parsed_pages]

    stage = FinalizeStage(
        finalize_queue=queues.finalize,
        publish_queue=queues.publish,
        finalize_stats=queues.finalize_stats,
        publish_stats=queues.publish_stats,
        liveness=liveness,
        finalize_parsed_page=lambda parsed: parsed.result,
        finalize_parsed_pages=finalize_batch,
        finalize_skipped_task=_no_finalize_skipped,
        finalize_failed_task=_no_finalize_failed,
        publish_result=_no_publish,
        record_timing=_record([]),
        progress=_progress,
        format_timings=_format_timings,
        success_batch_size=3,
    )
    worker = asyncio.create_task(stage.run())

    results = [_crawl_result(f"https://example.com/{index}") for index in range(3)]
    for result in results:
        await queues.finalize.put(
            FinalizeItem(
                parsed=ParsedPage(
                    task=CrawlTask(url=result.url, lease_token="lease-1"),
                    result=result,
                    new_tasks=[],
                    process_started=time.perf_counter(),
                ),
                enqueued_at=time.perf_counter(),
                queue_depth=queues.finalize.qsize(),
            )
        )

    await asyncio.wait_for(queues.finalize.join(), timeout=2)
    await queues.finalize.put(FINALIZER_SENTINEL)
    await asyncio.wait_for(worker, timeout=2)

    assert batches == [[result.url for result in results]]
    assert [queues.publish.get_nowait().result for _ in results] == results
    assert liveness.snapshot()["started"] == 3
    assert liveness.snapshot()["completed"] == 3


@pytest.mark.asyncio
async def test_finalize_stage_survives_item_error_and_drains_next_item():
    queues = PipelineQueues(maxsize=4)
    liveness = StageLiveness(include_kind=True)
    calls = []
    records = []

    async def finalize_skipped(skipped):
        calls.append(skipped.task.url)
        if len(calls) == 1:
            raise RuntimeError("boom")
        return skipped

    stage = FinalizeStage(
        finalize_queue=queues.finalize,
        publish_queue=None,
        finalize_stats=queues.finalize_stats,
        publish_stats=QueueStats(),
        liveness=liveness,
        finalize_parsed_page=lambda parsed: parsed.result,
        finalize_skipped_task=finalize_skipped,
        finalize_failed_task=_no_finalize_failed,
        publish_result=_no_publish,
        record_timing=_record(records),
        progress=_progress,
        format_timings=_format_timings,
    )
    worker = asyncio.create_task(stage.run())

    for suffix in ("first", "second"):
        await queues.finalize.put(
            FinalizeItem(
                skipped=SkippedTask(
                    task=CrawlTask(url=f"https://example.com/{suffix}", lease_token=suffix),
                    reason="robots_denied",
                    timings=CrawlStageTimings(),
                    process_started=time.perf_counter(),
                ),
                enqueued_at=time.perf_counter(),
                queue_depth=0,
            )
        )

    await asyncio.wait_for(queues.finalize.join(), timeout=2)
    await queues.finalize.put(FINALIZER_SENTINEL)
    await asyncio.wait_for(worker, timeout=2)

    assert calls == ["https://example.com/first", "https://example.com/second"]
    assert len(records) == 1
    assert records[0][0] == "skipped"
    assert liveness.snapshot()["started"] == 2
    assert liveness.snapshot()["completed"] == 1
    assert liveness.snapshot()["failed"] == 1


@pytest.mark.asyncio
async def test_publish_stage_survives_item_error_and_drains_next_item():
    queues = PipelineQueues(maxsize=4)
    liveness = StageLiveness()
    records = []
    published = []

    async def publish_result(result):
        if not published:
            published.append("failed-once")
            raise RuntimeError("boom")
        published.append(result.url)

    stage = PublishStage(
        publish_queue=queues.publish,
        publish_stats=queues.publish_stats,
        liveness=liveness,
        publish_result=publish_result,
        record_timing=_record(records),
        progress=_progress,
        format_timings=_format_timings,
    )
    worker = asyncio.create_task(stage.run())

    first = _crawl_result("https://example.com/first")
    second = _crawl_result("https://example.com/second")
    for result in (first, second):
        await queues.publish.put(
            PublishItem(
                result=result,
                enqueued_at=time.perf_counter(),
                queue_depth=0,
            )
        )

    await asyncio.wait_for(queues.publish.join(), timeout=2)
    await queues.publish.put(PUBLISHER_SENTINEL)
    await asyncio.wait_for(worker, timeout=2)

    assert published == ["failed-once", "https://example.com/second"]
    assert records == [("success", second.timings)]
    assert liveness.snapshot()["started"] == 2
    assert liveness.snapshot()["completed"] == 1
    assert liveness.snapshot()["failed"] == 1


@pytest.mark.asyncio
async def test_publish_stage_batches_results():
    queues = PipelineQueues(maxsize=8)
    liveness = StageLiveness()
    records = []
    batches = []

    async def publish_results(results):
        batches.append([result.url for result in results])

    stage = PublishStage(
        publish_queue=queues.publish,
        publish_stats=queues.publish_stats,
        liveness=liveness,
        publish_result=_no_publish,
        publish_results=publish_results,
        record_timing=_record(records),
        progress=_progress,
        format_timings=_format_timings,
        success_batch_size=3,
    )
    worker = asyncio.create_task(stage.run())

    results = [_crawl_result(f"https://example.com/{index}") for index in range(3)]
    for result in results:
        await queues.publish.put(
            PublishItem(
                result=result,
                enqueued_at=time.perf_counter(),
                queue_depth=queues.publish.qsize(),
            )
        )

    await asyncio.wait_for(queues.publish.join(), timeout=2)
    await queues.publish.put(PUBLISHER_SENTINEL)
    await asyncio.wait_for(worker, timeout=2)

    assert batches == [[result.url for result in results]]
    assert records == [("success", result.timings) for result in results]
    assert liveness.snapshot()["started"] == 3
    assert liveness.snapshot()["completed"] == 3
    assert liveness.snapshot()["failed"] == 0
