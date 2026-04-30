"""Deployment smoke test for runtime network-layer egress containment."""

from __future__ import annotations

import socket
import sys
from dataclasses import dataclass


TIMEOUT_SECONDS = 1.0
PUBLIC_TARGET = ("example.com", 80)
BLOCKED_TARGETS = (
    ("127.0.0.1", 80),
    ("169.254.169.254", 80),
    ("10.0.0.1", 80),
    ("172.16.0.1", 80),
    ("192.168.0.1", 80),
    ("100.64.0.1", 80),
    ("198.18.0.1", 80),
)


@dataclass(frozen=True)
class ProbeResult:
    """Result for one TCP connection probe."""

    host: str
    port: int
    connected: bool
    error: str | None = None


def _probe(host: str, port: int, *, timeout: float = TIMEOUT_SECONDS) -> ProbeResult:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return ProbeResult(host=host, port=port, connected=True)
    except OSError as exc:
        return ProbeResult(
            host=host,
            port=port,
            connected=False,
            error=type(exc).__name__,
        )


def main() -> int:
    failures: list[str] = []
    public = _probe(*PUBLIC_TARGET, timeout=2.0)
    if public.connected:
        print(f"ok public {public.host}:{public.port} connected")
    else:
        failures.append(
            f"public {public.host}:{public.port} did not connect ({public.error})"
        )

    for host, port in BLOCKED_TARGETS:
        result = _probe(host, port)
        if result.connected:
            failures.append(f"blocked {host}:{port} unexpectedly connected")
        else:
            print(f"ok blocked {host}:{port} not connected ({result.error})")

    if failures:
        for failure in failures:
            print(f"fail {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
