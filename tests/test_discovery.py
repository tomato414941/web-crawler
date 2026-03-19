"""Tests for discovery ranking."""

from crawler.discovery import (
    DISCOVERY_EXTERNAL,
    DISCOVERY_SAME_HOST,
    DISCOVERY_SEED,
    DISCOVERY_SEED_HOST,
    EXTERNAL_PRIORITY,
    SAME_HOST_PRIORITY,
    SEED_HOST_PRIORITY,
    SEED_PRIORITY,
    rank_discovered_url,
    rank_seed_url,
    seed_hosts_from_urls,
)


def test_seed_hosts_from_urls_normalizes_hosts():
    result = seed_hosts_from_urls(
        [
            "HTTPS://WWW.IANA.ORG/",
            "https://datatracker.ietf.org/wg",
        ]
    )

    assert result == {"www.iana.org", "datatracker.ietf.org"}


def test_rank_seed_url_returns_seed_priority():
    result = rank_seed_url("https://www.iana.org/")

    assert result.discovery_kind == DISCOVERY_SEED
    assert result.priority == SEED_PRIORITY


def test_rank_discovered_url_prefers_same_host():
    result = rank_discovered_url(
        parent_url="https://www.iana.org/domains",
        url="https://www.iana.org/protocols",
        seed_hosts={"www.iana.org"},
    )

    assert result.discovery_kind == DISCOVERY_SAME_HOST
    assert result.priority == SAME_HOST_PRIORITY


def test_rank_discovered_url_prefers_seed_host_over_external():
    result = rank_discovered_url(
        parent_url="https://www.iana.org/domains",
        url="https://datatracker.ietf.org/wg/",
        seed_hosts={"www.iana.org", "datatracker.ietf.org"},
    )

    assert result.discovery_kind == DISCOVERY_SEED_HOST
    assert result.priority == SEED_HOST_PRIORITY


def test_rank_discovered_url_marks_other_hosts_external():
    result = rank_discovered_url(
        parent_url="https://www.iana.org/domains",
        url="https://github.com/ietf-tools/datatracker",
        seed_hosts={"www.iana.org", "datatracker.ietf.org"},
    )

    assert result.discovery_kind == DISCOVERY_EXTERNAL
    assert result.priority == EXTERNAL_PRIORITY
