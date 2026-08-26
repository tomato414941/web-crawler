import json
from crawler.cli import _fetch
from crawler.cli import app
from crawler.core import Response
from typer.testing import CliRunner


def _response(url: str = "https://example.com") -> Response:
    return Response(
        url=url,
        status=200,
        content=b"<html>ok</html>",
        headers={"content-type": "text/html"},
    )


class RecordingFetcher:
    calls: list[str] = []
    close_calls = 0

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    async def fetch(self, url: str):
        self.calls.append(url)
        return _response(url)

    async def close(self):
        type(self).close_calls += 1


class RecordingAdaptiveFetcher(RecordingFetcher):
    async def fetch(self, url: str):
        self.calls.append(url)
        return _response(url), True


def _reset(fetcher_type):
    fetcher_type.calls = []
    fetcher_type.close_calls = 0


class FakeStorage:
    dsn: str | None = None
    conn = object()

    def __init__(self, dsn: str):
        type(self).dsn = dsn

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None


async def test_fetch_uses_http_fetcher_once(monkeypatch):
    _reset(RecordingFetcher)
    monkeypatch.setattr("crawler.cli.HttpFetcher", RecordingFetcher)

    result = await _fetch("https://example.com")

    assert result.url == "https://example.com"
    assert result.used_browser is False
    assert RecordingFetcher.calls == ["https://example.com"]
    assert RecordingFetcher.close_calls == 1


async def test_fetch_uses_browser_fetcher_once(monkeypatch):
    _reset(RecordingFetcher)
    monkeypatch.setattr("crawler.core.get_browser_fetcher", lambda: RecordingFetcher)

    result = await _fetch("https://example.com", use_browser=True)

    assert result.used_browser is True
    assert RecordingFetcher.calls == ["https://example.com"]
    assert RecordingFetcher.close_calls == 1


async def test_fetch_uses_adaptive_fetcher_once(monkeypatch):
    _reset(RecordingAdaptiveFetcher)
    monkeypatch.setattr("crawler.core.get_adaptive_fetcher", lambda: RecordingAdaptiveFetcher)

    result = await _fetch("https://example.com", auto=True)

    assert result.used_browser is True
    assert RecordingAdaptiveFetcher.calls == ["https://example.com"]
    assert RecordingAdaptiveFetcher.close_calls == 1


def test_observe_command_prints_formatted_observation(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr("crawler.storage.PgStorage", FakeStorage)
    monkeypatch.setattr(
        "crawler.observation.read_operator_observation",
        lambda storage, **_kwargs: {"crawl": {"total_pages": 1}},
    )
    monkeypatch.setattr(
        "crawler.observation.format_operator_observation",
        lambda observation: f"pages={observation['crawl']['total_pages']}",
    )

    result = runner.invoke(app, ["observe", "--postgres", "postgresql://example"])

    assert result.exit_code == 0
    assert result.stdout == "pages=1\n"
    assert FakeStorage.dsn == "postgresql://example"


def test_scheduler_check_command_prints_summary(monkeypatch):
    runner = CliRunner()

    class FakeChecker:
        repaired = False

        def __init__(self, conn):
            self.conn = conn

        def repair_terminal_memberships(self):
            type(self).repaired = True
            return type(
                "RepairReport",
                (),
                {
                    "to_dict": lambda self: {
                        "deleted_total": 3,
                        "deleted_queue_rows": {"runnable": 2},
                        "deleted_blocked_rows": 0,
                        "deleted_leases": 1,
                        "deleted_host_heads": 0,
                        "deleted_dirty_hosts": 0,
                    }
                },
            )()

        def check(self, sample_limit=5):
            assert sample_limit == 2
            return type(
                "Report",
                (),
                {
                    "to_dict": lambda self: {
                        "ok": False,
                        "violations_total": 1,
                        "duplicate_memberships": 1,
                        "terminal_in_live_queue": 0,
                        "expired_leases": 0,
                        "orphan_host_heads": 0,
                        "host_head_mismatches": 0,
                        "url_hash_missing": 0,
                        "url_hash_mismatches": 0,
                        "url_length_mismatches": 0,
                        "url_hash_duplicates": 0,
                        "url_too_long": 0,
                        "samples": {},
                    }
                },
            )()

    monkeypatch.setattr("crawler.storage.PgStorage", FakeStorage)
    monkeypatch.setattr("crawler.scheduler_invariants.SchedulerInvariantChecker", FakeChecker)

    result = runner.invoke(
        app,
        ["scheduler-check", "--postgres", "postgresql://example", "--sample-limit", "2"],
    )

    assert result.exit_code == 0
    assert "Scheduler Invariants" in result.stdout
    assert "ok=false violations=1" in result.stdout
    assert "duplicates=1 terminal=0 expired_leases=0" in result.stdout
    assert FakeChecker.repaired is False


def test_scheduler_check_repair_terminal_prints_repair_summary(monkeypatch):
    runner = CliRunner()

    class FakeChecker:
        def __init__(self, conn):
            self.conn = conn

        def repair_terminal_memberships(self):
            return type(
                "RepairReport",
                (),
                {
                    "to_dict": lambda self: {
                        "deleted_total": 3,
                        "deleted_queue_rows": {"runnable": 2},
                        "deleted_blocked_rows": 0,
                        "deleted_leases": 1,
                        "deleted_host_heads": 0,
                        "deleted_dirty_hosts": 0,
                    }
                },
            )()

        def check(self, sample_limit=5):
            return type(
                "Report",
                (),
                {
                    "to_dict": lambda self: {
                        "ok": True,
                        "violations_total": 0,
                        "duplicate_memberships": 0,
                        "terminal_in_live_queue": 0,
                        "expired_leases": 0,
                        "orphan_host_heads": 0,
                        "host_head_mismatches": 0,
                        "url_hash_missing": 0,
                        "url_hash_mismatches": 0,
                        "url_length_mismatches": 0,
                        "url_hash_duplicates": 0,
                        "url_too_long": 0,
                        "samples": {},
                    }
                },
            )()

    monkeypatch.setattr("crawler.storage.PgStorage", FakeStorage)
    monkeypatch.setattr("crawler.scheduler_invariants.SchedulerInvariantChecker", FakeChecker)

    result = runner.invoke(
        app,
        ["scheduler-check", "--postgres", "postgresql://example", "--repair-terminal"],
    )

    assert result.exit_code == 0
    assert "Scheduler Terminal Repair" in result.stdout
    assert "deleted_total=3" in result.stdout
    assert "ok=true violations=0" in result.stdout


def test_scheduler_check_repair_host_heads_prints_repair_summary(monkeypatch):
    runner = CliRunner()

    class FakeChecker:
        def __init__(self, conn):
            self.conn = conn

        def check(self, sample_limit=5):
            return type(
                "Report",
                (),
                {
                    "to_dict": lambda self: {
                        "ok": True,
                        "violations_total": 0,
                        "duplicate_memberships": 0,
                        "terminal_in_live_queue": 0,
                        "expired_leases": 0,
                        "orphan_host_heads": 0,
                        "host_head_mismatches": 0,
                        "url_hash_missing": 0,
                        "url_hash_mismatches": 0,
                        "url_length_mismatches": 0,
                        "url_hash_duplicates": 0,
                        "url_too_long": 0,
                        "samples": {},
                    }
                },
            )()

    class FakeScheduler:
        def __init__(self, conn):
            self.conn = conn

        def repair_host_runnable_heads(self, *, limit):
            assert limit == 7
            return type(
                "RepairReport",
                (),
                {
                    "as_dict": lambda self: {
                        "checked_heads": 3,
                        "orphan_heads": 1,
                        "stale_heads": 0,
                        "missing_heads": 0,
                        "repaired_hosts": 1,
                    }
                },
            )()

    monkeypatch.setattr("crawler.storage.PgStorage", FakeStorage)
    monkeypatch.setattr("crawler.scheduler_invariants.SchedulerInvariantChecker", FakeChecker)
    monkeypatch.setattr("crawler.scheduler.Scheduler", FakeScheduler)

    result = runner.invoke(
        app,
        ["scheduler-check", "--postgres", "postgresql://example", "--repair-host-heads", "7"],
    )

    assert result.exit_code == 0
    assert "Scheduler Host-Head Repair" in result.stdout
    assert "checked=3 orphan=1 stale=0 missing=0 repaired=1" in result.stdout
    assert "ok=true violations=0" in result.stdout


def test_scheduler_check_requires_postgres():
    runner = CliRunner()

    result = runner.invoke(app, ["scheduler-check"])

    assert result.exit_code == 1
    assert "CRAWLER_POSTGRES_DSN is required" in result.stderr


def test_observe_command_prints_json_observation(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr("crawler.storage.PgStorage", FakeStorage)
    monkeypatch.setattr(
        "crawler.observation.read_operator_observation",
        lambda storage, **_kwargs: {"crawl": {"total_pages": 1}},
    )

    result = runner.invoke(app, ["observe", "--postgres", "postgresql://example", "--json"])

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {"crawl": {"total_pages": 1}}


def test_observe_command_requires_postgres():
    runner = CliRunner()

    result = runner.invoke(app, ["observe"])

    assert result.exit_code == 1
    assert "CRAWLER_POSTGRES_DSN is required" in result.stderr
