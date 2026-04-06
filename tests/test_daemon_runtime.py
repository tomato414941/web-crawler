"""Runtime reporting tests for crawl daemon."""

import asyncio

import pytest

from crawler.daemon import CrawlDaemon


class _FakeStorage:
    def __init__(self):
        self.payloads = []

    def upsert_runtime_stats(self, component, payload):
        self.payloads.append((component, dict(payload)))


class _FakeEngine:
    def __init__(self):
        self._running = False
        self.calls = 0

    def snapshot_runtime_stats(self):
        self.calls += 1
        return {"running": self._running, "tick": self.calls}


@pytest.mark.asyncio
async def test_report_runtime_stats_waits_for_engine_to_start():
    daemon = CrawlDaemon(
        seeds=["https://example.com/"],
        postgres_dsn="postgresql://unused",
    )
    storage = _FakeStorage()
    engine = _FakeEngine()

    task = asyncio.create_task(daemon._report_runtime_stats(storage, engine))
    try:
        await asyncio.sleep(0.05)
        assert storage.payloads == []

        engine._running = True
        await asyncio.sleep(1.1)

        assert storage.payloads
        component, payload = storage.payloads[-1]
        assert component == "crawler"
        assert payload["running"] is True
        assert payload["tick"] >= 1
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
