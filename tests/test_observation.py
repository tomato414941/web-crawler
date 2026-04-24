from crawler.observation import build_operator_observation, format_operator_observation


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
    )

    assert observation["crawl"]["total_pages"] == 10
    assert observation["scheduler"]["runnable"] == 30
    assert observation["throughput"]["errors"] == {"timeout": 1}
    assert observation["backpressure"]["publish_queue_size"] == 3
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
    assert "stored_ratio=25.0%" in text
    assert "pages: 4.0 KiB" in text
