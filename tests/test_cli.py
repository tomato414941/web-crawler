from crawler.cli import _fetch
from crawler.core import Response


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
