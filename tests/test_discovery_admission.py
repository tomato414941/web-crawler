"""Tests for discovered URL admission policy."""

from crawler.discovery import ARCHETYPE_DOCUMENT_PAGE, PARENT_CONTEXT_NOFOLLOW, PageSignals
from crawler.discovery_admission import (
    ADMISSION_REASON_CANDIDATE,
    ADMISSION_REASON_HOST_POLICY_PENALTY,
    ADMISSION_REASON_NOFOLLOW_PARENT,
    ADMISSION_REASON_SCORE_BELOW_THRESHOLD,
    AdmissionControl,
    DiscoveryAdmissionPolicy,
    HostAdmissionContext,
    build_admission_control,
    decide_discovered_url_admission,
)


BALANCED_CONTROL = AdmissionControl(
    mode="balanced",
    target_pending=500_000,
    pending=500_000,
    min_discovery_value=0.5,
)


def test_decide_discovered_url_admission_admits_candidate():
    result = decide_discovered_url_admission(
        parent_url="https://example.com/",
        url="https://example.com/doc/rfc9000",
        seed_hosts={"example.com"},
        admission_control=BALANCED_CONTROL,
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
        admission_control=BALANCED_CONTROL,
    )

    assert result.admitted is False
    assert result.reason == ADMISSION_REASON_NOFOLLOW_PARENT
    assert result.parent_context == PARENT_CONTEXT_NOFOLLOW


def test_build_admission_control_uses_target_pending_bands():
    assert build_admission_control(pending=299_999, target_pending=500_000).mode == "expand"
    assert build_admission_control(pending=300_000, target_pending=500_000).mode == "balanced"
    assert build_admission_control(pending=600_000, target_pending=500_000).mode == "reduce"
    assert build_admission_control(pending=900_000, target_pending=500_000).mode == "drain"


def test_decide_discovered_url_admission_rejects_external_generic_under_reduce():
    result = decide_discovered_url_admission(
        parent_url="https://example.com/",
        url="https://external.example.net/project",
        seed_hosts={"example.com"},
        admission_control=build_admission_control(
            pending=866_000,
            target_pending=500_000,
        ),
    )

    assert result.admitted is False
    assert result.reason == ADMISSION_REASON_SCORE_BELOW_THRESHOLD


def test_decide_discovered_url_admission_rejects_document_external_under_reduce():
    result = decide_discovered_url_admission(
        parent_url="https://example.com/",
        url="https://external.example.net/doc/rfc9000",
        seed_hosts={"example.com"},
        admission_control=build_admission_control(
            pending=866_000,
            target_pending=500_000,
        ),
    )

    assert result.admitted is False
    assert result.reason == ADMISSION_REASON_SCORE_BELOW_THRESHOLD


def test_decide_discovered_url_admission_keeps_same_host_document_under_drain():
    result = decide_discovered_url_admission(
        parent_url="https://example.com/",
        url="https://example.com/doc/rfc9000",
        seed_hosts={"example.com"},
        admission_control=build_admission_control(
            pending=950_000,
            target_pending=500_000,
        ),
    )

    assert result.admitted is True
    assert result.reason == ADMISSION_REASON_CANDIDATE


def test_decide_discovered_url_admission_rejects_known_bad_host_after_penalty():
    result = decide_discovered_url_admission(
        parent_url="https://example.com/",
        url="https://bad.example.net/project",
        seed_hosts={"example.com"},
        admission_control=BALANCED_CONTROL,
        host_context=HostAdmissionContext(
            known=True,
            failure_count=4,
            success_count=0,
            penalty=0.35,
        ),
    )

    assert result.admitted is False
    assert result.reason == ADMISSION_REASON_HOST_POLICY_PENALTY


def test_policy_applies_value_order_and_caps():
    links = [f"https://a.example/docs/{i}" for i in range(7)]
    links.extend(f"https://b{i:03d}.example/docs/1" for i in range(159))
    links.append("https://low.example/redirect/1")
    result = DiscoveryAdmissionPolicy(
        seed_hosts={"seed.example"},
        is_valid_url=lambda url: url.startswith("https://"),
    ).build_tasks(
        parent_url="https://seed.example/",
        links=links,
        parent_signals=None,
        admission_control=build_admission_control(pending=50, target_pending=50),
        host_contexts={},
    )

    assert len(result.tasks) == 160
    assert [task.url for task in result.tasks[:6]] == [
        f"https://a.example/docs/{i}" for i in range(6)
    ]
    assert all(task.discovery_value >= 0.8 for task in result.tasks)
    assert result.counts["extracted"] == 167
    assert result.counts["admitted"] == 160
    assert result.counts["per_target_host_cap"] == 1
    assert result.counts["per_page_cap"] == 5
    assert result.counts["low_value_archetype"] == 1
