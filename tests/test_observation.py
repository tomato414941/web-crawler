import json
import logging

from crawler.observation import (
    ObservationWatchConfig,
    ObservationWatchFailed,
    ObservationWatcher,
    append_observation_record,
    build_observation_error_record,
    build_observation_record,
    build_operator_observation,
    format_operator_observation,
    sanitize_observation_error,
    serialize_operator_observation,
)


class FakeStorage:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None


def test_build_operator_observation_compacts_runtime_and_storage_shape():
    observation = build_operator_observation(
        {
            "stats_source": "runtime_snapshot",
            "diagnostics_endpoint": "/stats/diagnostics",
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
                    "publish_queue_size": 3,
                    "parse_queue_wait_max_ms": 10.0,
                    "finalize_queue_wait_max_ms": 20.0,
                    "publish_queue_wait_max_ms": 30.0,
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
    assert observation["backpressure"]["publish_queue_size"] == 3
    assert observation["admission_control"]["mode"] == "reduce"
    assert observation["admission_control"]["per_target_host_cap"] == 4
    assert observation["discovery_admission"]["admit_ratio"] == 0.25
    assert observation["discovery_admission"]["rejection_reasons"] == {
        "score_below_threshold": 10,
        "per_page_cap": 5,
    }
    assert observation["storage"]["outlinks"]["stored_ratio"] == 0.25
    assert observation["runtime"]["diagnostics_endpoint"] == "/stats/diagnostics"


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
                "publish_queue_size": 3,
                "parse_queue_wait_max_ms": 10.0,
                "finalize_queue_wait_max_ms": 20.0,
                "publish_queue_wait_max_ms": 30.0,
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


def test_build_observation_record_wraps_snapshot():
    record = build_observation_record(
        {"crawl": {"total_pages": 10}},
        observed_at=1710000000.0,
    )

    assert record == {
        "ok": True,
        "observed_at": 1710000000.0,
        "observation": {"crawl": {"total_pages": 10}},
    }


def test_build_observation_error_record_avoids_secret_details():
    error = RuntimeError("could not connect to postgresql://user:secret@example/db")

    record = build_observation_error_record(error, observed_at=1710000000.0)

    assert record["ok"] is False
    assert record["error_type"] == "RuntimeError"
    assert "secret" not in str(record["error"])
    assert "postgresql://" not in str(record["error"])


def test_sanitize_observation_error_preserves_safe_context():
    error = RuntimeError("statement timeout after 15000ms")

    assert sanitize_observation_error(error) == "statement timeout after 15000ms"


def test_sanitize_observation_error_redacts_url_credentials():
    error = RuntimeError("could not connect to postgresql://user:secret@example/db")

    message = sanitize_observation_error(error)

    assert "secret" not in message
    assert "postgresql://" not in message
    assert "<redacted-url>" in message


def test_sanitize_observation_error_falls_back_for_empty_message():
    assert sanitize_observation_error(RuntimeError()) == (
        "Observation failed; check service logs for details."
    )


def test_append_observation_record_writes_jsonl(tmp_path):
    output = tmp_path / "nested" / "observations.jsonl"

    append_observation_record(output, {"ok": True, "observed_at": 1710000000.0})

    assert output.read_text(encoding="utf-8") == '{"observed_at": 1710000000.0, "ok": true}\n'


def test_append_observation_record_rotates_when_max_bytes_is_reached(tmp_path):
    output = tmp_path / "observations.jsonl"
    output.write_text("old\n", encoding="utf-8")

    append_observation_record(output, {"ok": True}, max_bytes=1, max_files=2)

    assert output.with_name("observations.jsonl.1").read_text(encoding="utf-8") == "old\n"
    assert output.read_text(encoding="utf-8") == '{"ok": true}\n'


def test_append_observation_record_limits_rotated_files(tmp_path):
    output = tmp_path / "observations.jsonl"
    output.write_text("current\n", encoding="utf-8")
    output.with_name("observations.jsonl.1").write_text("one\n", encoding="utf-8")
    output.with_name("observations.jsonl.2").write_text("two\n", encoding="utf-8")

    append_observation_record(output, {"ok": True}, max_bytes=1, max_files=2)

    assert output.with_name("observations.jsonl.1").read_text(encoding="utf-8") == "current\n"
    assert output.with_name("observations.jsonl.2").read_text(encoding="utf-8") == "one\n"
    assert not output.with_name("observations.jsonl.3").exists()


def test_append_observation_record_skips_rotation_when_disabled(tmp_path):
    output = tmp_path / "observations.jsonl"
    output.write_text("old\n", encoding="utf-8")

    append_observation_record(output, {"ok": True}, max_bytes=0, max_files=2)

    assert not output.with_name("observations.jsonl.1").exists()
    assert output.read_text(encoding="utf-8") == 'old\n{"ok": true}\n'


def test_observation_watcher_writes_success_record(monkeypatch, tmp_path, caplog):
    output = tmp_path / "observations.jsonl"
    caplog.set_level(logging.INFO, logger="crawler.observation")

    monkeypatch.setattr(
        "crawler.observation.read_operator_observation",
        lambda storage: {
            "crawl": {"total_pages": 1},
            "scheduler": {"pending": 2},
            "throughput": {"pages_per_second": 3.0},
        },
    )

    watcher = ObservationWatcher(
        storage_factory=FakeStorage,
        config=ObservationWatchConfig(output=output, limit=1),
        clock=lambda: 1710000000.0,
    )

    assert watcher.run() == 1
    assert "observed ok pages=1 pending=2 pps=3.0" in caplog.text
    assert json.loads(output.read_text(encoding="utf-8")) == {
        "observation": {
            "crawl": {"total_pages": 1},
            "scheduler": {"pending": 2},
            "throughput": {"pages_per_second": 3.0},
        },
        "observed_at": 1710000000.0,
        "ok": True,
    }


def test_observation_watcher_exits_after_max_failures(monkeypatch, tmp_path, caplog):
    output = tmp_path / "observations.jsonl"
    caplog.set_level(logging.WARNING, logger="crawler.observation")

    def fail(_storage):
        raise RuntimeError("statement timeout")

    monkeypatch.setattr("crawler.observation.read_operator_observation", fail)

    watcher = ObservationWatcher(
        storage_factory=FakeStorage,
        config=ObservationWatchConfig(output=output, max_failures=1),
        clock=lambda: 1710000000.0,
    )

    try:
        watcher.run()
    except ObservationWatchFailed:
        pass
    else:
        raise AssertionError("watcher should fail after one observation failure")

    assert "failed observation attempt=1" in caplog.text
    assert "exiting after 1 consecutive observation failures" in caplog.text
    assert json.loads(output.read_text(encoding="utf-8"))["error"] == "statement timeout"


def test_observation_watcher_resets_failure_count_after_success(monkeypatch, tmp_path, caplog):
    output = tmp_path / "observations.jsonl"
    caplog.set_level(logging.WARNING, logger="crawler.observation")
    calls = {"count": 0}

    def read(_storage):
        calls["count"] += 1
        if calls["count"] in {1, 3}:
            raise RuntimeError("temporary failure")
        return {"crawl": {"total_pages": calls["count"]}}

    monkeypatch.setattr("crawler.observation.read_operator_observation", read)

    watcher = ObservationWatcher(
        storage_factory=FakeStorage,
        config=ObservationWatchConfig(output=output, limit=3, interval=1, max_failures=2),
        sleep=lambda interval: None,
        clock=lambda: 1710000000.0,
    )

    assert watcher.run() == 3
    assert "exiting after" not in caplog.text
    records = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert [record["ok"] for record in records] == [False, True, False]
