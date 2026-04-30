"""Deployment smoke test for runtime network-layer egress containment."""

from __future__ import annotations

import os
import socket
import sys
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener


TIMEOUT_SECONDS = 1.0
PUBLIC_TARGET = ("example.com", 80)
PUBLIC_URL = os.getenv("EGRESS_SMOKE_PUBLIC_URL", "http://example.com/")
PRIVATE_URL = os.getenv("EGRESS_SMOKE_PRIVATE_URL")
PROXY_HOST = os.getenv("EGRESS_SMOKE_PROXY_HOST")
PROXY_PORT = int(os.getenv("EGRESS_SMOKE_PROXY_PORT", "3128"))
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


def _fetch(url: str, *, use_proxy_env: bool) -> tuple[int | None, str | None]:
    handler = ProxyHandler() if use_proxy_env else ProxyHandler({})
    opener = build_opener(handler)
    request = Request(url, headers={"User-Agent": "web-crawler-egress-smoke/1.0"})
    try:
        with opener.open(request, timeout=5.0) as response:
            response.read(1)
            return response.status, None
    except HTTPError as exc:
        return exc.code, None
    except (OSError, URLError) as exc:
        return None, type(exc).__name__


def main() -> int:
    failures: list[str] = []

    if PROXY_HOST:
        proxy = _probe(PROXY_HOST, PROXY_PORT, timeout=2.0)
        if proxy.connected:
            print(f"ok proxy {proxy.host}:{proxy.port} connected")
        else:
            failures.append(
                f"proxy {proxy.host}:{proxy.port} did not connect ({proxy.error})"
            )

    public_status, public_error = _fetch(PUBLIC_URL, use_proxy_env=True)
    if public_status and 200 <= public_status < 400:
        print(f"ok public {PUBLIC_URL} fetched via proxy ({public_status})")
    else:
        failures.append(
            f"public {PUBLIC_URL} did not fetch via proxy ({public_status or public_error})"
        )

    direct_status, direct_error = _fetch(PUBLIC_URL, use_proxy_env=False)
    if direct_status and 200 <= direct_status < 400:
        print(f"ok public {PUBLIC_URL} fetched without proxy ({direct_status})")
    else:
        print(
            f"ok public {PUBLIC_URL} not fetched without proxy "
            f"({direct_status or direct_error})"
        )

    if PRIVATE_URL:
        private_status, private_error = _fetch(PRIVATE_URL, use_proxy_env=True)
        if private_status and 200 <= private_status < 400:
            failures.append(
                f"private test {PRIVATE_URL} unexpectedly fetched via proxy "
                f"({private_status})"
            )
        else:
            print(
                f"ok private test {PRIVATE_URL} not fetched via proxy "
                f"({private_status or private_error})"
            )

    public = _probe(*PUBLIC_TARGET, timeout=2.0)
    if public.connected:
        print(f"ok public {public.host}:{public.port} direct tcp connected")
    elif not PROXY_HOST:
        failures.append(
            f"public {public.host}:{public.port} did not connect ({public.error})"
        )
    else:
        print(
            f"ok public {public.host}:{public.port} direct tcp not connected "
            f"({public.error})"
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
