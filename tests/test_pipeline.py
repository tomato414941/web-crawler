"""Pipeline stage contract tests."""

import asyncio
import time

import pytest

from crawler.pipeline import (
    FINALIZER_SENTINEL,
    PUBLISHER_SENTINEL,
    FinalizeItem,
    FinalizeStage,
    ParsedPage,
    PipelineQueues,
    PublishItem,
    PublishStage,
    QueueStats,
    SkippedTask,
    StageLiveness,
)
from crawler.result import CrawlResult, CrawlStageTimings
from crawler.url_ledger import CrawlTask


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


def _record(records):
    def record(outcome, timings):
        records.append((outcome, timings))

    return record


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
