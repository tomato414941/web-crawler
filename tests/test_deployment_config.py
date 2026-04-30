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


def test_hardened_compose_routes_crawler_through_proxy():
    compose = (ROOT / "docker-compose.hardened.yml").read_text(encoding="utf-8")

    assert "egress-proxy:" in compose
    assert "image: ubuntu/squid:" in compose
    assert "CRAWLER_EGRESS_PROXY: http://egress-proxy:3128" in compose
    assert "CRAWLER_REQUIRE_EGRESS_PROXY: \"true\"" in compose
    assert "CRAWLER_DIRECT_EGRESS_ALLOWED: \"false\"" in compose
    assert "CRAWLER_ALLOW_PRIVATE_NETWORK_EGRESS: \"false\"" in compose
    assert "crawler_internal:" in compose
    assert "internal: true" in compose


def test_hardened_compose_has_private_service_smoke():
    compose = (ROOT / "docker-compose.hardened.yml").read_text(encoding="utf-8")

    assert "private-test:" in compose
    assert "image: nginx:" in compose
    assert "egress-smoke:" in compose
    assert "HTTP_PROXY: http://egress-proxy:3128" in compose
    assert "EGRESS_SMOKE_PRIVATE_URL: http://private-test/" in compose
    assert "profiles:" in compose
    assert "- egress-smoke" in compose


def test_hardened_proxy_denies_private_destinations():
    squid = (ROOT / "config/egress-proxy/squid.conf").read_text(encoding="utf-8")

    for cidr in (
        "10.0.0.0/8",
        "100.64.0.0/10",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "198.18.0.0/15",
        "fc00::/7",
        "fe80::/10",
    ):
        assert f"acl to_private dst {cidr}" in squid

    assert "http_access deny to_private" in squid
