"""Structured result types."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


@dataclass(slots=True)
class CrawlResult:
    """Successful crawl result."""

    url: str
    status: int
    content_length: int
    depth: int
    source_url: str | None
    timestamp: float
    content: str
    outlinks: list[str]

    def to_dict(self, include_content: bool = True) -> dict[str, Any]:
        """Convert result to a plain dict."""
        data = asdict(self)
        if not include_content:
            data.pop("content", None)
        return data


@dataclass(slots=True)
class CrawlFailure:
    """Failed crawl attempt."""

    url: str
    error: str
    depth: int
    retryable: bool

    def to_dict(self) -> dict[str, Any]:
        """Convert failure to a plain dict."""
        return asdict(self)


@dataclass(slots=True)
class FetchResult:
    """Single-page fetch result."""

    url: str
    status: int
    content_length: int
    headers: dict[str, str]
    content: str
    used_browser: bool = False

    def to_dict(self, include_content: bool = True) -> dict[str, Any]:
        """Convert result to a plain dict."""
        data = asdict(self)
        if not include_content:
            data.pop("content", None)
        return data


@dataclass(slots=True)
class ExtractResult:
    """Data extraction result."""

    url: str
    selector: str | None
    items: list[str]

    def to_dict(self) -> dict[str, Any]:
        """Convert result to a plain dict."""
        return {
            "url": self.url,
            "selector": self.selector,
            "items": list(self.items),
            "count": len(self.items),
        }


@dataclass(slots=True)
class LinkReference:
    """Link found in a page."""

    url: str
    text: str
    source: str

    def to_dict(self) -> dict[str, Any]:
        """Convert result to a plain dict."""
        return asdict(self)


@dataclass(slots=True)
class CheckedLink:
    """Checked link status."""

    url: str
    status: int
    ok: bool
    final_url: str | None = None
    redirect: bool = False
    error: str | None = None
    source: str | None = None
    text: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert result to a plain dict."""
        data = {
            "url": self.url,
            "status": self.status,
            "ok": self.ok,
            "redirect": self.redirect,
        }
        if self.final_url is not None:
            data["final_url"] = self.final_url
        if self.error is not None:
            data["error"] = self.error
        if self.source is not None:
            data["source"] = self.source
        if self.text is not None:
            data["text"] = self.text
        return data


@dataclass(slots=True)
class LinkCheckResult:
    """Summary of a page link check."""

    start_url: str
    total_links: int
    ok: int
    broken: int
    redirects: int
    broken_links: list[CheckedLink]
    redirect_links: list[CheckedLink]

    def to_dict(self) -> dict[str, Any]:
        """Convert result to a plain dict."""
        return {
            "start_url": self.start_url,
            "total_links": self.total_links,
            "ok": self.ok,
            "broken": self.broken,
            "redirects": self.redirects,
            "broken_links": [link.to_dict() for link in self.broken_links],
            "redirect_links": [link.to_dict() for link in self.redirect_links],
        }


def result_to_dict(result: object | Mapping[str, Any], include_content: bool = True) -> dict[str, Any]:
    """Normalize crawl results to a plain dict."""
    if hasattr(result, "to_dict"):
        try:
            return result.to_dict(include_content=include_content)
        except TypeError:
            return result.to_dict()

    data = dict(result)
    if not include_content:
        data.pop("content", None)
    return data
