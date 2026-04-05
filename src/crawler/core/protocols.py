"""Protocol definitions for crawler components."""

from dataclasses import dataclass
from typing import Protocol


@dataclass
class Response:
    """HTTP response container."""

    url: str
    status: int
    content: bytes
    headers: dict[str, str]
    fetch_request_ms: float = 0.0
    fetch_body_read_ms: float = 0.0

    @property
    def text(self) -> str:
        """Decode content as UTF-8."""
        return self.content.decode("utf-8", errors="replace")


class Fetcher(Protocol):
    """Protocol for URL fetchers."""

    async def fetch(self, url: str) -> Response:
        """Fetch a URL and return the response."""
        ...
