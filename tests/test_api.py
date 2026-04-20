"""Tests for API route wiring."""

import pytest

fastapi_testclient = pytest.importorskip("fastapi.testclient")
TestClient = fastapi_testclient.TestClient
api = pytest.importorskip("crawler.api")


class FakeStorage:
    def get_runtime_stats_summary(self):
        return {"stats_source": "runtime_snapshot"}

    def get_stats(self):
        raise AssertionError("live diagnostics should not be called")


@pytest.fixture(autouse=True)
def reset_storage():
    old_storage = api._storage
    api._storage = FakeStorage()
    yield
    api._storage = old_storage


def test_stats_uses_fast_runtime_summary():
    client = TestClient(api.app)

    response = client.get("/stats")

    assert response.status_code == 200
    assert response.json() == {"stats_source": "runtime_snapshot"}


def test_stats_diagnostics_uses_runtime_snapshot_only():
    client = TestClient(api.app)

    response = client.get("/stats/diagnostics")

    assert response.status_code == 200
    assert response.json() == {
        "stats_source": "runtime_snapshot",
        "diagnostics_unavailable": True,
        "diagnostics_error": "live_scheduler_diagnostics_disabled",
        "diagnostics_mode": "runtime_snapshot_only",
    }
