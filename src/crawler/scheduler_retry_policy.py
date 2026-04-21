"""Retry transition policy for scheduler failures."""

from __future__ import annotations

from dataclasses import dataclass


RETRY_PRIORITY_DECAY = 0.6
MIN_RETRY_PRIORITY = 0.25


@dataclass(frozen=True)
class SchedulerFailureTransition:
    """Computed URL failure transition values."""

    retryable: bool
    next_fail_streak: int
    next_priority: float
    next_fetch_at: float
    current_intent: str | None
    last_error: str | None
    terminal_reason: str | None
    terminalized_at: float | None


class SchedulerRetryPolicy:
    """Compute retry and terminal failure transitions."""

    def __init__(
        self,
        *,
        retry_backoff_seconds: float,
        max_retry_backoff_seconds: float,
        retry_intent: str,
    ):
        self._retry_backoff_seconds = retry_backoff_seconds
        self._max_retry_backoff_seconds = max_retry_backoff_seconds
        self._retry_intent = retry_intent

    def compute_backoff(self, fail_streak: int) -> float:
        base = max(self._retry_backoff_seconds, 0.0)
        if fail_streak <= 1:
            return base
        delay = base * (2 ** (fail_streak - 1))
        return min(delay, self._max_retry_backoff_seconds)

    def compute_priority(self, priority: float, fail_streak: int) -> float:
        if fail_streak <= 0:
            return priority
        return max(MIN_RETRY_PRIORITY, round(priority * (RETRY_PRIORITY_DECAY**fail_streak), 2))

    def failure_transition(
        self,
        *,
        fail_streak: int,
        priority: float,
        retryable: bool,
        error: str | None,
        backoff_seconds: float | None,
        now: float,
    ) -> SchedulerFailureTransition:
        next_fail_streak = fail_streak + 1
        next_priority = self.compute_priority(priority, next_fail_streak)

        if retryable:
            retry_delay = (
                backoff_seconds
                if backoff_seconds is not None
                else self.compute_backoff(next_fail_streak)
            )
            return SchedulerFailureTransition(
                retryable=True,
                next_fail_streak=next_fail_streak,
                next_priority=next_priority,
                next_fetch_at=now + (retry_delay or 0.0),
                current_intent=self._retry_intent,
                last_error=error,
                terminal_reason=None,
                terminalized_at=None,
            )

        return SchedulerFailureTransition(
            retryable=False,
            next_fail_streak=next_fail_streak,
            next_priority=next_priority,
            next_fetch_at=now,
            current_intent=None,
            last_error=error,
            terminal_reason=error or "failed",
            terminalized_at=now,
        )
