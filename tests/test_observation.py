import json

from crawler.observation import (
    build_operator_observation,
    format_operator_observation,
    serialize_operator_observation,
)


def test_build_operator_observation_compacts_runtime_and_storage_shape():
    observation = build_operator_observation(
        {
            "stats_source": "runtime_snapshot",
            "total_pages": 10,
            "hosts": 3,
            "total_bytes": 2048,
            "total_stored_bytes": 1024,
            "operator_summary": {
                "scheduler_readiness_states": {
                    "pending": 50,
                    "runnable": 30,
                    "scheduled": 10,
                    "retry_quarantine": 2,
                    "leased": 1,
                },
                "throughput": {
                    "pages_per_second": 4.5,
                    "cycle_pages": 100,
                    "active_hosts": 8,
                    "errors": {"timeout": 1},
                },
                "backpressure": {
                    "parse_queue_size": 1,
                    "finalize_queue_size": 2,
                    "parse_queue_wait_max_ms": 10.0,
                    "finalize_queue_wait_max_ms": 20.0,
                },
                "admission_control": {
                    "mode": "reduce",
                    "target_pending": 500_000,
                    "pending": 700_000,
                    "min_score": 1.0,
                    "per_page_cap": 80,
                    "per_target_host_cap": 4,
                    "new_external_host_cap": 2,
                },
                "discovery_admission": {
                    "extracted": 20,
                    "admitted": 5,
                    "rejected": 15,
                    "admit_ratio": 0.25,
                    "rejection_reasons": {
                        "score_below_threshold": 10,
                        "per_page_cap": 5,
                    },
                },
            },
            "runtime": {"updated_at": 1710000000.0},
        },
        {
            "tiers": [
                {
                    "storage_tier": "standard",
                    "pages": 10,
                    "stored_content_bytes": 1024,
                    "content_length": 2048,
                }
            ],
            "totals": {"outlink_count": 20, "stored_outlink_count": 5},
            "url_ledger": {"urls": 50, "hosts": 10},
            "relations": [{"relation": "pages", "total_bytes": 4096}],
        },
        {
            "ok": False,
            "violations_total": 2,
            "duplicate_memberships": 1,
            "terminal_in_live_queue": 1,
            "expired_leases": 0,
            "orphan_host_heads": 0,
            "host_head_mismatches": 0,
            "url_hash_missing": 0,
            "url_hash_mismatches": 1,
            "url_length_mismatches": 0,
            "url_hash_duplicates": 0,
            "url_too_long": 0,
            "checked_at": 1710000001.0,
        },
    )

    assert observation["crawl"]["total_pages"] == 10
    assert observation["scheduler"]["runnable"] == 30
    assert observation["scheduler"]["invariants"] == {
        "checked": True,
        "ok": False,
        "violations_total": 2,
        "duplicate_memberships": 1,
        "terminal_in_live_queue": 1,
        "expired_leases": 0,
        "orphan_host_heads": 0,
        "host_head_mismatches": 0,
        "url_hash_missing": 0,
        "url_hash_mismatches": 1,
        "url_length_mismatches": 0,
        "url_hash_duplicates": 0,
        "url_too_long": 0,
        "checked_at": 1710000001.0,
    }
    assert observation["throughput"]["errors"] == {"timeout": 1}
    assert observation["backpressure"]["finalize_queue_size"] == 2
    assert observation["admission_control"]["mode"] == "reduce"
    assert observation["admission_control"]["per_target_host_cap"] == 4
    assert observation["discovery_admission"]["admit_ratio"] == 0.25
    assert observation["discovery_admission"]["rejection_reasons"] == {
        "score_below_threshold": 10,
        "per_page_cap": 5,
    }
    assert observation["storage"]["outlinks"]["stored_ratio"] == 0.25


def test_format_operator_observation_is_stable_and_readable():
    text = format_operator_observation(
        {
            "crawl": {
                "total_pages": 10,
                "hosts": 3,
                "total_bytes": 2048,
                "total_stored_bytes": 1024,
            },
            "scheduler": {
                "pending": 50,
                "runnable": 30,
                "scheduled": 10,
                "retry_quarantine": 2,
                "leased": 1,
                "invariants": {
                    "checked": True,
                    "ok": False,
                    "violations_total": 3,
                    "duplicate_memberships": 1,
                    "terminal_in_live_queue": 0,
                    "expired_leases": 1,
                    "orphan_host_heads": 1,
                    "host_head_mismatches": 0,
                    "url_hash_missing": 0,
                    "url_hash_mismatches": 1,
                    "url_length_mismatches": 3,
                    "url_hash_duplicates": 0,
                    "url_too_long": 2,
                },
            },
            "throughput": {
                "pages_per_second": 4.5,
                "cycle_pages": 100,
                "active_hosts": 8,
                "errors": {"timeout": 1},
            },
            "backpressure": {
                "parse_queue_size": 1,
                "finalize_queue_size": 2,
                "parse_queue_wait_max_ms": 10.0,
                "finalize_queue_wait_max_ms": 20.0,
            },
            "discovery_admission": {
                "extracted": 20,
                "admitted": 5,
                "rejected": 15,
                "admit_ratio": 0.25,
                "rejection_reasons": {"score_below_threshold": 10},
            },
            "storage": {
                "tiers": [
                    {
                        "storage_tier": "standard",
                        "pages": 10,
                        "stored_content_bytes": 1024,
                        "content_length": 2048,
                    }
                ],
                "outlinks": {"extracted": 20, "stored": 5, "stored_ratio": 0.25},
                "relations": [{"relation": "pages", "total_bytes": 4096}],
            },
            "runtime": {"stats_source": "runtime_snapshot", "updated_at": 1710000000.0},
        }
    )

    assert "Crawler Observation" in text
    assert "pages=10 hosts=3" in text
    assert "pending=50 runnable=30 scheduled=10 retry=2 leased=1" in text
    assert "invariants ok=false violations=3 duplicates=1 terminal=0" in text
    assert "url_hash_mismatches=1 url_length_mismatches=3 url_too_long=2" in text
    assert "discovery extracted=20 admitted=5 rejected=15 admit_ratio=25.0%" in text
    assert "rejection_reasons={'score_below_threshold': 10}" in text
    assert "stored_ratio=25.0%" in text
    assert "pages: 4.0 KiB" in text


def test_format_operator_observation_marks_unchecked_scheduler_invariants():
    text = format_operator_observation(
        {
            "crawl": {},
            "scheduler": {"invariants": {"checked": False}},
            "throughput": {},
            "backpressure": {},
            "admission_control": {},
            "discovery_admission": {},
            "storage": {},
            "runtime": {},
        }
    )

    assert "invariants not_checked" in text


def test_serialize_operator_observation_outputs_structured_json():
    text = serialize_operator_observation({"crawl": {"total_pages": 10}})

    assert json.loads(text) == {"crawl": {"total_pages": 10}}
