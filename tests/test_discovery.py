"""Tests for discovery ranking."""

from crawler.discovery import (
    ARCHETYPE_DOCUMENT_PAGE,
    ARCHETYPE_REDIRECT_HUB,
    ARCHETYPE_REGISTRY_LISTING,
    ADMISSION_REASON_CANDIDATE,
    ADMISSION_REASON_EXTERNAL_PRESSURE,
    ADMISSION_REASON_HOST_POLICY_PENALTY,
    ADMISSION_REASON_NOFOLLOW_PARENT,
    EXTERNAL_DISCOVERY_VALUE,
    FrontierPressure,
    HostAdmissionContext,
    PARENT_CONTEXT_LOW_SIGNAL,
    PARENT_CONTEXT_NOFOLLOW,
    PageSignals,
    SAME_HOST_DISCOVERY_VALUE,
    SEED_HOST_DISCOVERY_VALUE,
    SEED_DISCOVERY_VALUE,
    classify_parent_archetype,
    classify_url_archetype,
    decide_discovered_url_admission,
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


def test_rank_seed_url_returns_seed_discovery_value():
    result = rank_seed_url("https://example.com/")

    assert result.discovery_value == SEED_DISCOVERY_VALUE


def test_rank_discovered_url_prefers_same_host():
    result = rank_discovered_url(
        parent_url="https://example.com/hosts",
        url="https://example.com/protocols",
        seed_hosts={"example.com"},
    )

    assert result.discovery_value == SAME_HOST_DISCOVERY_VALUE


def test_rank_discovered_url_prefers_seed_host_over_external():
    result = rank_discovered_url(
        parent_url="https://example.com/hosts",
        url="https://docs.example.com/guide/",
        seed_hosts={"example.com", "docs.example.com"},
    )

    assert result.discovery_value == SEED_HOST_DISCOVERY_VALUE


def test_rank_discovered_url_marks_other_hosts_external():
    result = rank_discovered_url(
        parent_url="https://example.com/hosts",
        url="https://external.example.net/project",
        seed_hosts={"example.com", "docs.example.com"},
    )

    assert result.discovery_value == EXTERNAL_DISCOVERY_VALUE


def test_classify_url_archetype_detects_redirect_hubs():
    assert classify_url_archetype("https://example.com/go/rfc9000") == ARCHETYPE_REDIRECT_HUB


def test_classify_url_archetype_detects_registry_listings():
    assert (
        classify_url_archetype(
            "https://example.com/assignments/tls-extensiontype-values/tls-extensiontype-values.xhtml"
        )
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

    assert result.discovery_value < 0.75
    assert result.archetype == ARCHETYPE_REGISTRY_LISTING
    assert result.parent_context == PARENT_CONTEXT_LOW_SIGNAL


def test_rank_discovered_url_downgrades_redirect_hubs():
    result = rank_discovered_url(
        parent_url="https://example.com/assignments/",
        url="https://example.com/go/rfc9142",
        seed_hosts={"example.com", "docs.example.com"},
    )

    assert result.discovery_value < SAME_HOST_DISCOVERY_VALUE


def test_rank_discovered_url_promotes_document_pages():
    result = rank_discovered_url(
        parent_url="https://example.com/go/rfc9142",
        url="https://docs.example.com/doc/rfc9142",
        seed_hosts={"example.com", "docs.example.com"},
    )

    assert result.discovery_value > SEED_HOST_DISCOVERY_VALUE


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

    assert result.discovery_value < EXTERNAL_DISCOVERY_VALUE
    assert result.discovery_value >= 0.25
    assert result.parent_context == PARENT_CONTEXT_NOFOLLOW
    assert (
        classify_parent_archetype(
            "https://example.com/archive/",
            PageSignals(
                content_type="text/html; charset=utf-8",
                content_length=900_000,
                title="Archive Table Index",
                meta_robots="nofollow",
            ),
        )
        == ARCHETYPE_REGISTRY_LISTING
    )


def test_decide_discovered_url_admission_admits_candidate():
    result = decide_discovered_url_admission(
        parent_url="https://example.com/",
        url="https://example.com/doc/rfc9000",
        seed_hosts={"example.com"},
        min_discovery_value=0.5,
        low_value_archetype_min_discovery_value=1.0,
    )

    assert result.admitted is True
    assert result.reason == ADMISSION_REASON_CANDIDATE
    assert result.archetype == ARCHETYPE_DOCUMENT_PAGE


def test_decide_discovered_url_admission_rejects_explained_low_value():
    result = decide_discovered_url_admission(
        parent_url="https://example.com/archive/",
        url="https://example.net/archive/index",
        seed_hosts={"example.com"},
        parent_signals=PageSignals(
            content_type="text/html",
            content_length=900_000,
            title="Archive Table Index",
            meta_robots="nofollow",
        ),
        min_discovery_value=0.5,
        low_value_archetype_min_discovery_value=1.0,
    )

    assert result.admitted is False
    assert result.reason == ADMISSION_REASON_NOFOLLOW_PARENT
    assert result.parent_context == PARENT_CONTEXT_NOFOLLOW


def test_decide_discovered_url_admission_rejects_external_generic_under_pressure():
    result = decide_discovered_url_admission(
        parent_url="https://example.com/",
        url="https://external.example.net/project",
        seed_hosts={"example.com"},
        min_discovery_value=0.5,
        low_value_archetype_min_discovery_value=1.0,
        frontier_pressure=FrontierPressure(
            pending=100_000,
            pending_threshold=100_000,
            external_min_value=1.0,
        ),
    )

    assert result.admitted is False
    assert result.reason == ADMISSION_REASON_EXTERNAL_PRESSURE


def test_decide_discovered_url_admission_keeps_document_external_under_pressure():
    result = decide_discovered_url_admission(
        parent_url="https://example.com/",
        url="https://external.example.net/doc/rfc9000",
        seed_hosts={"example.com"},
        min_discovery_value=0.5,
        low_value_archetype_min_discovery_value=1.0,
        frontier_pressure=FrontierPressure(
            pending=100_000,
            pending_threshold=100_000,
            external_min_value=1.0,
        ),
    )

    assert result.admitted is True
    assert result.reason == ADMISSION_REASON_CANDIDATE


def test_decide_discovered_url_admission_rejects_known_bad_host_after_penalty():
    result = decide_discovered_url_admission(
        parent_url="https://example.com/",
        url="https://bad.example.net/project",
        seed_hosts={"example.com"},
        min_discovery_value=0.5,
        low_value_archetype_min_discovery_value=1.0,
        host_context=HostAdmissionContext(
            known=True,
            failure_count=4,
            success_count=0,
            penalty=0.35,
        ),
    )

    assert result.admitted is False
    assert result.reason == ADMISSION_REASON_HOST_POLICY_PENALTY
