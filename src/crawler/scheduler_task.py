"""Scheduler work item and intent definitions."""

from __future__ import annotations

from dataclasses import dataclass
import time


INTENT_EXPLORE = "explore"
INTENT_REFRESH = "refresh"
INTENT_RETRY = "retry"


@dataclass(init=False)
class CrawlTask:
    """A URL to crawl with scheduler metadata."""

    url: str
    discovery_value: float = 1.0
    scheduler_score: float = 1.0
    runnable_surface: str | None = None
    intent: str | None = None
    source_url: str | None = None
    added_at: float = 0.0
    next_fetch_at: float = 0.0
    lease_token: str | None = None
    lease_expires_at: float | None = None

    def __init__(
        self,
        url: str,
        discovery_value: float = 1.0,
        *,
        scheduler_score: float | None = None,
        runnable_surface: str | None = None,
        intent: str | None = None,
        source_url: str | None = None,
        added_at: float = 0.0,
        next_fetch_at: float = 0.0,
        lease_token: str | None = None,
        lease_expires_at: float | None = None,
    ):
        self.url = url
        self.discovery_value = discovery_value
        self.scheduler_score = discovery_value if scheduler_score is None else scheduler_score
        self.runnable_surface = runnable_surface
        self.intent = intent
        self.source_url = source_url
        self.added_at = added_at
        self.next_fetch_at = next_fetch_at
        self.lease_token = lease_token
        self.lease_expires_at = lease_expires_at
        self.__post_init__()

    def __post_init__(self) -> None:
        if self.added_at == 0.0:
            self.added_at = time.time()
        if self.next_fetch_at == 0.0:
            self.next_fetch_at = self.added_at
