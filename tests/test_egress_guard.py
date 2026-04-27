"""Tests for outbound egress safety checks."""

import pytest

from crawler.egress_guard import (
    EgressBlockedError,
    GuardDecision,
    check_url,
    is_url_allowed_without_dns,
    raise_if_blocked,
)


@pytest.mark.parametrize(
    ("url", "reason"),
    [
        ("ftp://example.com/file", "unsupported_scheme"),
        ("https:///missing-host", "missing_host"),
        ("http://example.com:99999/path", "invalid_url"),
        ("http://[::1/path", "invalid_url"),
        ("http://localhost/admin", "blocked_hostname"),
        ("http://service.localhost/status", "blocked_hostname"),
        ("http://127.0.0.1:8080/admin", "blocked_ip_literal"),
        ("http://10.0.0.5/admin", "blocked_ip_literal"),
        ("http://172.16.0.5/admin", "blocked_ip_literal"),
        ("http://192.168.1.10/admin", "blocked_ip_literal"),
        ("http://169.254.169.254/latest/meta-data/", "blocked_ip_literal"),
        ("http://[::1]/admin", "blocked_ip_literal"),
        ("http://[fc00::1]/admin", "blocked_ip_literal"),
        ("http://[fe80::1]/admin", "blocked_ip_literal"),
    ],
)
def test_url_shape_guard_blocks_unsupported_and_private_targets(url, reason):
    decision = is_url_allowed_without_dns(url)

    assert decision.allowed is False
    assert decision.reason == reason
    assert decision.url == url


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/",
        "https://example.com/path",
        "http://93.184.216.34/",
        "https://[2606:4700:4700::1111]/dns-query",
    ],
)
def test_url_shape_guard_allows_public_http_targets(url):
    decision = is_url_allowed_without_dns(url)

    assert decision == GuardDecision(True, "allowed", url, decision.hostname)


async def test_dns_guard_blocks_hostname_that_resolves_to_private_address():
    async def resolver(hostname: str, port: int | None) -> list[str]:
        assert hostname == "public-name.example"
        assert port == 443
        return ["93.184.216.34", "10.0.0.8"]

    decision = await check_url("https://public-name.example/path", resolver=resolver)

    assert decision.allowed is False
    assert decision.reason == "blocked_resolved_ip"
    assert decision.hostname == "public-name.example"


async def test_dns_guard_allows_hostname_when_all_answers_are_public():
    async def resolver(hostname: str, port: int | None) -> list[str]:
        assert hostname == "example.com"
        assert port == 80
        return ["93.184.216.34", "2606:4700:4700::1111"]

    decision = await check_url("http://example.com/page", resolver=resolver)

    assert decision.allowed is True
    assert decision.reason == "allowed"


async def test_dns_guard_blocks_unresolvable_hostnames():
    async def resolver(_hostname: str, _port: int | None) -> list[str]:
        raise OSError("no answer")

    decision = await check_url("https://unresolvable.example/", resolver=resolver)

    assert decision.allowed is False
    assert decision.reason == "dns_error"


async def test_allow_private_network_egress_bypasses_private_ip_and_dns_checks():
    resolver_called = False

    async def resolver(_hostname: str, _port: int | None) -> list[str]:
        nonlocal resolver_called
        resolver_called = True
        return ["10.0.0.8"]

    literal_decision = await check_url(
        "http://127.0.0.1/admin",
        resolver=resolver,
        allow_private_network_egress=True,
    )
    hostname_decision = await check_url(
        "http://private-name.example/admin",
        resolver=resolver,
        allow_private_network_egress=True,
    )

    assert literal_decision.allowed is True
    assert hostname_decision.allowed is True
    assert resolver_called is False


def test_raise_if_blocked_preserves_decision():
    decision = GuardDecision(False, "blocked_ip_literal", "http://127.0.0.1/", "127.0.0.1")

    with pytest.raises(EgressBlockedError) as exc_info:
        raise_if_blocked(decision)

    assert exc_info.value.decision is decision
