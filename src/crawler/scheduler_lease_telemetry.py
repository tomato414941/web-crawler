"""Cycle-local scheduler lease telemetry."""

from __future__ import annotations

from .host_runnable_heads import host_execution_tier_label


class HostFirstLeaseTelemetry:
    """Track host-first lease diagnostics without owning scheduler state."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self._fallback_attempts = 0
        self._fallback_hits = 0
        self._fallback_misses = 0
        self._read_model_hits = 0
        self._read_model_stale = 0
        self._read_model_misses = 0
        self._read_model_errors = 0
        self._last_lease_diagnostics = self._unknown_diagnostics()

    def fallback_stats(self) -> dict[str, int]:
        return {
            "attempts": self._fallback_attempts,
            "hits": self._fallback_hits,
            "misses": self._fallback_misses,
            "read_model_hits": self._read_model_hits,
            "read_model_stale": self._read_model_stale,
            "read_model_misses": self._read_model_misses,
            "read_model_errors": self._read_model_errors,
        }

    def record_fallback(self, *, hit: bool) -> None:
        self._fallback_attempts += 1
        if hit:
            self._fallback_hits += 1
        else:
            self._fallback_misses += 1

    def record_read_model(self, status: str) -> None:
        if status == "hit":
            self._read_model_hits += 1
        elif status == "stale":
            self._read_model_stale += 1
        elif status == "error":
            self._read_model_errors += 1
        else:
            self._read_model_misses += 1

    def set_last_lease_diagnostics(
        self,
        *,
        read_model: str,
        fallback: str,
        read_model_candidates: int = 0,
        stale_candidates: int = 0,
        execution_tier: int | None = None,
    ) -> None:
        self._last_lease_diagnostics = {
            "read_model": read_model,
            "fallback": fallback,
            "read_model_candidates": int(read_model_candidates),
            "stale_candidates": int(stale_candidates),
            "execution_tier": host_execution_tier_label(execution_tier),
        }

    def last_lease_diagnostics(self) -> dict[str, object]:
        return dict(self._last_lease_diagnostics)

    def _unknown_diagnostics(self) -> dict[str, object]:
        return {
            "read_model": "unknown",
            "fallback": "none",
            "read_model_candidates": 0,
            "stale_candidates": 0,
            "execution_tier": "unknown",
        }
