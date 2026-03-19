"""Structured domain state models."""

from __future__ import annotations

from dataclasses import dataclass

from robotexclusionrulesparser import RobotExclusionRulesParser


@dataclass(slots=True)
class PersistedDomainState:
    """Durable scheduling state for a host key."""

    host_key: str
    crawl_delay_seconds: float = 1.0
    next_request_at: float = 0.0
    backoff_until: float = 0.0
    consecutive_failures: int = 0
    robots_checked_at: float = 0.0
    updated_at: float = 0.0


@dataclass(slots=True)
class RuntimeDomainState:
    """In-memory crawler state for a host key."""

    host_key: str
    robots_parser: RobotExclusionRulesParser | None = None
    has_checked_robots: bool = False
    robots_checked_at: float = 0.0
    last_request_started_at: float = 0.0
    request_count: int = 0
    consecutive_failures: int = 0
    crawl_delay_seconds: float = 1.0


DomainState = RuntimeDomainState
