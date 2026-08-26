"""Runtime reporting tests for crawl daemon."""

import threading
import time

from crawler.daemon import CrawlDaemon
from crawler.host_runnable_heads import HostRunnableHeadDirtyRefreshSummary


class _FakeStorage:
    payloads: list[tuple[str, dict]] = []
    conn = object()

    def __init__(self, dsn):
        self.dsn = dsn

    def upsert_runtime_stats(self, component, payload):
        self.payloads.append((component, dict(payload)))

    def close(self):
        return None


class _FakeScheduler:
    def __init__(self, conn):
        self.conn = conn


class _FakeEngine:
    def __init__(self):
        self.running = False
        self.calls = 0

    def snapshot_runtime_stats(self):
        self.calls += 1
        return {"running": self.running, "tick": self.calls}


def test_report_runtime_stats_waits_for_engine_to_start(monkeypatch):
    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
    )
    _FakeStorage.payloads = []
    monkeypatch.setattr("crawler.daemon.PgStorage", _FakeStorage)
    monkeypatch.setattr("crawler.daemon.Scheduler", _FakeScheduler)
    engine = _FakeEngine()
    stop_event = threading.Event()

    reporter = threading.Thread(
        target=daemon._report_runtime_stats,
        args=(stop_event, engine),
        daemon=True,
    )
    reporter.start()
    try:
        time.sleep(0.05)
        assert _FakeStorage.payloads == []

        engine.running = True
        time.sleep(1.1)

        assert _FakeStorage.payloads
        component, payload = _FakeStorage.payloads[-1]
        assert component == "crawler"
        assert payload["running"] is True
        assert payload["tick"] >= 1
    finally:
        stop_event.set()
        reporter.join(timeout=2.0)


def test_idle_runtime_payload_preserves_pipeline_liveness():
    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
    )
    daemon._last_runtime_snapshot.update(
        {
            "parser_liveness": {"started": 2, "completed": 2, "failed": 0},
            "finalizer_liveness": {"started": 2, "completed": 2, "failed": 0},
        }
    )

    payload = daemon._idle_runtime_payload(
        state="cycle_complete",
        pending=10,
        runnable=8,
        cycle=3,
    )

    assert payload["parser_liveness"] == {"started": 2, "completed": 2, "failed": 0}
    assert payload["finalizer_liveness"] == {"started": 2, "completed": 2, "failed": 0}
    assert payload["active_cycle"]["parser_liveness"] == payload["parser_liveness"]
    assert payload["active_cycle"]["finalizer_liveness"] == payload["finalizer_liveness"]


def test_runtime_payload_includes_host_head_dirty_refresh():
    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
    )
    daemon._last_host_head_dirty_refresh = {
        "selected_hosts": 3,
        "refreshed_hosts": 2,
        "remaining_hosts": 1,
        "elapsed_ms": 12.3,
    }

    payload = daemon._idle_runtime_payload(
        state="cycle_complete",
        pending=10,
        runnable=8,
        cycle=3,
    )

    assert payload["host_head_dirty_refresh"] == {
        "selected_hosts": 3,
        "refreshed_hosts": 2,
        "remaining_hosts": 1,
        "elapsed_ms": 12.3,
    }
    assert payload["active_cycle"]["host_head_dirty_refresh"] == {
        "selected_hosts": 3,
        "refreshed_hosts": 2,
        "remaining_hosts": 1,
        "elapsed_ms": 12.3,
    }


def test_refresh_dirty_host_runnable_heads_records_summary():
    class FakeScheduler:
        def refresh_dirty_host_runnable_heads(self, limit):
            assert limit == 11
            return HostRunnableHeadDirtyRefreshSummary(
                selected_hosts=4,
                refreshed_hosts=3,
                remaining_hosts=2,
                elapsed_ms=9.8,
            )

    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
    )
    daemon._host_head_dirty_refresh_limit = 11

    daemon._refresh_dirty_host_runnable_heads(FakeScheduler())

    assert daemon._last_host_head_dirty_refresh == {
        "selected_hosts": 4,
        "refreshed_hosts": 3,
        "remaining_hosts": 2,
        "elapsed_ms": 9.8,
    }
