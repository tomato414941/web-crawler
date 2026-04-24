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
        lambda storage: {"crawl": {"total_pages": 1}},
    )
    monkeypatch.setattr(
        "crawler.observation.format_operator_observation",
        lambda observation: f"pages={observation['crawl']['total_pages']}",
    )

    result = runner.invoke(app, ["observe", "--postgres", "postgresql://example"])

    assert result.exit_code == 0
    assert result.stdout == "pages=1\n"
    assert FakeStorage.dsn == "postgresql://example"


def test_observe_command_prints_json_observation(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr("crawler.storage.PgStorage", FakeStorage)
    monkeypatch.setattr(
        "crawler.observation.read_operator_observation",
        lambda storage: {"crawl": {"total_pages": 1}},
    )

    result = runner.invoke(app, ["observe", "--postgres", "postgresql://example", "--json"])

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {"crawl": {"total_pages": 1}}


def test_observe_command_requires_postgres():
    runner = CliRunner()

    result = runner.invoke(app, ["observe"])

    assert result.exit_code == 1
    assert "CRAWLER_POSTGRES_DSN is required" in result.stderr


def test_observe_watch_writes_one_jsonl_record(monkeypatch, tmp_path):
    runner = CliRunner()
    output = tmp_path / "observations.jsonl"

    monkeypatch.setattr("crawler.storage.PgStorage", FakeStorage)
    monkeypatch.setattr(
        "crawler.observation.read_operator_observation",
        lambda storage: {"crawl": {"total_pages": 1}},
    )

    result = runner.invoke(
        app,
        [
            "observe-watch",
            "--postgres",
            "postgresql://example",
            "--output",
            str(output),
            "--interval",
            "1",
            "--limit",
            "1",
        ],
    )

    assert result.exit_code == 0
    records = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert records == [
        {
            "ok": True,
            "observed_at": records[0]["observed_at"],
            "observation": {"crawl": {"total_pages": 1}},
        }
    ]


def test_observe_watch_writes_error_record_without_secret(monkeypatch, tmp_path):
    runner = CliRunner()
    output = tmp_path / "observations.jsonl"

    monkeypatch.setattr("crawler.storage.PgStorage", FakeStorage)

    def fail(_storage):
        raise RuntimeError("could not connect to postgresql://user:secret@example/db")

    monkeypatch.setattr("crawler.observation.read_operator_observation", fail)

    result = runner.invoke(
        app,
        [
            "observe-watch",
            "--postgres",
            "postgresql://example",
            "--output",
            str(output),
            "--interval",
            "1",
            "--limit",
            "1",
        ],
    )

    assert result.exit_code == 0
    record = json.loads(output.read_text(encoding="utf-8"))
    assert record["ok"] is False
    assert record["error_type"] == "RuntimeError"
    assert "secret" not in record["error"]
    assert "postgresql://" not in record["error"]


def test_observe_watch_requires_postgres(tmp_path):
    runner = CliRunner()

    result = runner.invoke(app, ["observe-watch", "--output", str(tmp_path / "out.jsonl")])

    assert result.exit_code == 1
    assert "CRAWLER_POSTGRES_DSN is required" in result.stderr
