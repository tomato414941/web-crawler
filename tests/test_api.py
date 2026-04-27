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
def reset_storage(monkeypatch):
    old_storage = api._storage
    api._storage = FakeStorage()
    monkeypatch.delenv("CRAWLER_API_TOKEN", raising=False)
    monkeypatch.delenv("CRAWLER_ALLOW_UNAUTHENTICATED_API", raising=False)
    yield
    api._storage = old_storage


def test_stats_requires_configured_token_by_default():
    client = TestClient(api.app)

    response = client.get("/stats")

    assert response.status_code == 503
    assert response.json() == {"detail": "api_token_not_configured"}


def test_stats_allows_explicit_unauthenticated_mode(monkeypatch):
    monkeypatch.setenv("CRAWLER_ALLOW_UNAUTHENTICATED_API", "true")
    client = TestClient(api.app)

    response = client.get("/stats")

    assert response.status_code == 200
    assert response.json() == {"stats_source": "runtime_snapshot"}


def test_stats_diagnostics_uses_runtime_snapshot_only(monkeypatch):
    monkeypatch.setenv("CRAWLER_ALLOW_UNAUTHENTICATED_API", "true")
    client = TestClient(api.app)

    response = client.get("/stats/diagnostics")

    assert response.status_code == 200
    assert response.json() == {
        "stats_source": "runtime_snapshot",
        "diagnostics_unavailable": True,
        "diagnostics_error": "live_scheduler_diagnostics_disabled",
        "diagnostics_mode": "runtime_snapshot_only",
    }


def test_stats_requires_token_when_configured(monkeypatch):
    monkeypatch.setenv("CRAWLER_API_TOKEN", "secret-token")
    client = TestClient(api.app)

    response = client.get("/stats")

    assert response.status_code == 401


def test_stats_accepts_bearer_token_when_configured(monkeypatch):
    monkeypatch.setenv("CRAWLER_API_TOKEN", "secret-token")
    client = TestClient(api.app)

    response = client.get("/stats", headers={"Authorization": "Bearer secret-token"})

    assert response.status_code == 200
    assert response.json() == {"stats_source": "runtime_snapshot"}


def test_health_remains_public_when_token_configured(monkeypatch):
    monkeypatch.setenv("CRAWLER_API_TOKEN", "secret-token")
    client = TestClient(api.app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
