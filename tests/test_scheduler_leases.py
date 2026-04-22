from crawler.scheduler_leases import (
    ACTIVE_LEASES_TABLE,
    ExecutionLeaseRow,
    ExecutionLeaseStore,
    LEASE_REQUIRED_COLUMNS,
)


class _FakeCursor:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.queries = []

    def execute(self, sql, params=()):
        self.queries.append((sql, params))

    def fetchall(self):
        return self.rows


def test_lease_schema_contract_names_active_lease_table():
    assert ACTIVE_LEASES_TABLE == "active_leases"
    assert LEASE_REQUIRED_COLUMNS == {
        "url",
        "host",
        "physical_queue",
        "lease_token",
        "lease_expires_at",
    }


def test_execution_lease_row_preserves_storage_tuple_shape():
    row = ExecutionLeaseRow(
        url="http://example.com/",
        host="example.com",
        physical_queue="runnable",
        lease_token="lease-1",
        lease_expires_at=123.0,
    )

    assert row.as_tuple() == (
        "http://example.com/",
        "example.com",
        "runnable",
        "lease-1",
        123.0,
    )


def test_match_sql_is_empty_without_token():
    assert ExecutionLeaseStore(None).match_sql("ledger", None) == ("", ())


def test_match_sql_validates_against_active_lease_table():
    sql, params = ExecutionLeaseStore(None).match_sql("ledger", "lease-1")

    assert "FROM active_leases AS active" in sql
    assert "active.url = ledger.url" in sql
    assert "active.lease_token = %s" in sql
    assert params == ("lease-1",)


def test_delete_normalizes_and_deduplicates_urls():
    cur = _FakeCursor()
    ExecutionLeaseStore(None).delete(
        cur,
        [
            "HTTP://EXAMPLE.COM/a#fragment",
            "http://example.com/a",
            "",
        ],
    )

    assert len(cur.queries) == 1
    sql, params = cur.queries[0]
    assert sql == "DELETE FROM active_leases WHERE url = ANY(%s)"
    assert params == (["http://example.com/a"],)


def test_recover_rows_deletes_expired_or_all_active_leases():
    expired_cur = _FakeCursor(rows=[("http://example.com/", "example.com", "runnable")])
    all_cur = _FakeCursor(rows=[("http://example.org/", "example.org", "scheduled")])
    store = ExecutionLeaseStore(None)

    assert store.recover_rows(expired_cur, now=100.0, expired_only=True) == [
        ("http://example.com/", "example.com", "runnable")
    ]
    assert "WHERE lease_expires_at <= %s" in expired_cur.queries[0][0]
    assert expired_cur.queries[0][1] == (100.0,)

    assert store.recover_rows(all_cur, now=100.0, expired_only=False) == [
        ("http://example.org/", "example.org", "scheduled")
    ]
    assert "WHERE TRUE" in all_cur.queries[0][0]
    assert all_cur.queries[0][1] == ()


def test_new_token_returns_distinct_non_empty_tokens():
    store = ExecutionLeaseStore(None)

    first = store.new_token()
    second = store.new_token()

    assert first
    assert second
    assert first != second
