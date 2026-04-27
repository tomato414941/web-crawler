"""Static checks for deployment defaults."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_compose_requires_api_token():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert "CRAWLER_API_TOKEN: ${CRAWLER_API_TOKEN:?required}" in compose


def test_compose_binds_public_services_to_loopback():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert '"127.0.0.1:8080:8080"' in compose
    assert '"127.0.0.1:5433:5432"' in compose
    assert '"8080:8080"' not in compose
    assert '"5433:5432"' not in compose
