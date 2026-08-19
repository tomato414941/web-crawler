"""Tests for scheduler work items."""

import time

import crawler.scheduler as scheduler
from crawler.scheduler_task import CrawlTask


def test_crawl_task_defaults_scheduler_score_to_discovery_value():
    task = CrawlTask(url="http://example.com")

    assert task.url == "http://example.com"
    assert task.discovery_value == 1.0
    assert task.scheduler_score == 1.0
    assert task.source_url is None
    assert task.added_at > 0


def test_crawl_task_preserves_explicit_metadata():
    task = CrawlTask(
        url="http://example.com/page",
        discovery_value=0.5,
        source_url="http://example.com",
        added_at=1000.0,
        next_fetch_at=1200.0,
    )

    assert task.discovery_value == 0.5
    assert task.scheduler_score == 0.5
    assert task.added_at == 1000.0
    assert task.next_fetch_at == 1200.0


def test_crawl_task_sets_current_time_when_added_at_is_missing():
    before = time.time()
    task = CrawlTask(url="http://example.com")
    after = time.time()

    assert before <= task.added_at <= after


def test_scheduler_does_not_reexport_owned_definitions():
    old_exports = {
        "CrawlTask",
        "INTENT_EXPLORE",
        "SCHEDULER_SURFACE_NORMAL",
        "LEASE_STRATEGY_HOST_FIRST",
        "UrlLedger",
        "SchedulerKernel",
        "SchedulerTopology",
    }

    assert old_exports.isdisjoint(vars(scheduler))
