"""Tests for discovery ranking."""

from crawler.discovery import (
    ARCHETYPE_DOCUMENT_PAGE,
    ARCHETYPE_GENERIC_PAGE,
    ARCHETYPE_REDIRECT_HUB,
    ARCHETYPE_REGISTRY_LISTING,
    DISCOVERY_EXTERNAL,
    DISCOVERY_SAME_HOST,
    DISCOVERY_SEED,
    DISCOVERY_SEED_HOST,
    EXTERNAL_PRIORITY,
    PageSignals,
    SAME_HOST_PRIORITY,
    SEED_HOST_PRIORITY,
    SEED_PRIORITY,
    classify_parent_archetype,
    classify_url_archetype,
    discovery_rank,
    rank_discovered_url,
    rank_seed_url,
    seed_hosts_from_urls,
)


def test_seed_hosts_from_urls_normalizes_hosts():
    result = seed_hosts_from_urls(
        [
            "HTTPS://EXAMPLE.COM/",
            "https://docs.example.com/guide",
        ]
    )

    assert result == {"example.com", "docs.example.com"}


def test_rank_seed_url_returns_seed_priority():
    result = rank_seed_url("https://example.com/")

    assert result.discovery_kind == DISCOVERY_SEED
    assert result.priority == SEED_PRIORITY
    assert result.archetype == ARCHETYPE_GENERIC_PAGE


def test_rank_discovered_url_prefers_same_host():
    result = rank_discovered_url(
        parent_url="https://example.com/domains",
        url="https://example.com/protocols",
        seed_hosts={"example.com"},
    )

    assert result.discovery_kind == DISCOVERY_SAME_HOST
    assert result.priority == SAME_HOST_PRIORITY


def test_rank_discovered_url_prefers_seed_host_over_external():
    result = rank_discovered_url(
        parent_url="https://example.com/domains",
        url="https://docs.example.com/guide/",
        seed_hosts={"example.com", "docs.example.com"},
    )

    assert result.discovery_kind == DISCOVERY_SEED_HOST
    assert result.priority == SEED_HOST_PRIORITY


def test_rank_discovered_url_marks_other_hosts_external():
    result = rank_discovered_url(
        parent_url="https://example.com/domains",
        url="https://external.example.net/project",
        seed_hosts={"example.com", "docs.example.com"},
    )

    assert result.discovery_kind == DISCOVERY_EXTERNAL
    assert result.priority == EXTERNAL_PRIORITY
    assert result.archetype == ARCHETYPE_GENERIC_PAGE


def test_classify_url_archetype_detects_redirect_hubs():
    assert classify_url_archetype("https://example.com/go/rfc9000") == ARCHETYPE_REDIRECT_HUB


def test_classify_url_archetype_detects_registry_listings():
    assert (
        classify_url_archetype("https://example.com/assignments/tls-extensiontype-values/tls-extensiontype-values.xhtml")
        == ARCHETYPE_REGISTRY_LISTING
    )


def test_classify_url_archetype_detects_document_pages():
    assert classify_url_archetype("https://docs.example.com/doc/rfc9000") == ARCHETYPE_DOCUMENT_PAGE


def test_rank_discovered_url_downgrades_bulk_data_paths():
    result = rank_discovered_url(
        parent_url="https://example.com/archives/index",
        url="https://example.com/tables/zara_uk_1.txt",
        seed_hosts={"example.com"},
    )

    assert result.discovery_kind == DISCOVERY_SAME_HOST
    assert result.priority < 0.75
    assert result.archetype == ARCHETYPE_REGISTRY_LISTING


def test_rank_discovered_url_downgrades_redirect_hubs():
    result = rank_discovered_url(
        parent_url="https://example.com/assignments/",
        url="https://example.com/go/rfc9142",
        seed_hosts={"example.com", "docs.example.com"},
    )

    assert result.discovery_kind == DISCOVERY_SAME_HOST
    assert result.archetype == ARCHETYPE_REDIRECT_HUB
    assert result.priority < SAME_HOST_PRIORITY


def test_rank_discovered_url_promotes_document_pages():
    result = rank_discovered_url(
        parent_url="https://example.com/go/rfc9142",
        url="https://docs.example.com/doc/rfc9142",
        seed_hosts={"example.com", "docs.example.com"},
    )

    assert result.discovery_kind == DISCOVERY_SEED_HOST
    assert result.archetype == ARCHETYPE_DOCUMENT_PAGE
    assert result.priority > SEED_HOST_PRIORITY


def test_rank_discovered_url_uses_parent_page_signals():
    result = rank_discovered_url(
        parent_url="https://example.com/archive/",
        url="https://docs.example.com/specification",
        seed_hosts={"example.com"},
        parent_signals=PageSignals(
            content_type="text/html; charset=utf-8",
            content_length=900_000,
            title="Archive Table Index",
            meta_robots="nofollow",
        ),
    )

    assert result.discovery_kind == DISCOVERY_EXTERNAL
    assert result.priority < EXTERNAL_PRIORITY
    assert result.priority >= 0.25
    assert classify_parent_archetype(
        "https://example.com/archive/",
        PageSignals(
            content_type="text/html; charset=utf-8",
            content_length=900_000,
            title="Archive Table Index",
            meta_robots="nofollow",
        ),
    ) == ARCHETYPE_REGISTRY_LISTING


def test_discovery_rank_orders_best_to_worst():
    assert discovery_rank(DISCOVERY_SEED) > discovery_rank(DISCOVERY_SAME_HOST)
    assert discovery_rank(DISCOVERY_SAME_HOST) > discovery_rank(DISCOVERY_SEED_HOST)
    assert discovery_rank(DISCOVERY_SEED_HOST) > discovery_rank(DISCOVERY_EXTERNAL)
