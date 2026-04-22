from crawler.scheduler_membership import (
    QUEUE_RUNNABLE,
    QUEUE_SCHEDULED,
    SchedulerMembershipStore,
    SchedulerQueueRow,
)


def _store():
    return SchedulerMembershipStore(
        None,
        blocked_queue_table="scheduler_queue_retry_quarantine",
        host_runnable_heads_table="host_runnable_heads",
        host_runnable_head_dirty_hosts_table="host_runnable_head_dirty_hosts",
    )


def test_rows_for_physical_queue_projects_scheduler_rows():
    rows = _store().rows_for_physical_queue(
        [
            ("http://example.com/a", "example.com", 1.25, 100.0, 90.0),
            ("http://example.com/b", "example.com", 0.8, 110.0, 91.0),
        ],
        QUEUE_RUNNABLE,
    )

    assert rows == [
        SchedulerQueueRow("http://example.com/a", "example.com", 1.25, 100.0, 90.0, "runnable"),
        SchedulerQueueRow("http://example.com/b", "example.com", 0.8, 110.0, 91.0, "runnable"),
    ]


def test_rows_for_ledger_rows_uses_discovery_value_as_initial_scheduler_score():
    rows = _store().rows_for_ledger_rows(
        [
            ("http://example.com/a", "example.com", 1.25, 100.0, 90.0),
            ("http://example.com/b", "example.com", 0.8, 110.0, 91.0),
        ],
        physical_queue_by_url={"http://example.com/a": QUEUE_RUNNABLE},
        default_physical_queue=QUEUE_SCHEDULED,
    )

    assert rows == [
        SchedulerQueueRow("http://example.com/a", "example.com", 1.25, 100.0, 90.0, "runnable"),
        SchedulerQueueRow("http://example.com/b", "example.com", 0.8, 110.0, 91.0, "scheduled"),
    ]


def test_row_urls_accepts_dataclass_and_legacy_tuple_rows():
    assert _store().row_urls(
        [
            SchedulerQueueRow("http://example.com/a", "example.com", 1.25, 100.0, 90.0, "runnable"),
            ("http://example.com/b", "example.com", 0.8, 110.0, 91.0, "scheduled"),
        ]
    ) == ["http://example.com/a", "http://example.com/b"]
