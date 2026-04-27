"""Outbound network safety checks for crawler URLs."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from urllib.parse import ParseResult, urlparse


SUPPORTED_SCHEMES = {"http", "https"}


@dataclass(frozen=True)
class GuardDecision:
    """Decision returned by the outbound egress guard."""

    allowed: bool
    reason: str
    url: str
    hostname: str | None = None


class EgressBlockedError(Exception):
    """Raised when a URL is blocked by the outbound egress guard."""

    def __init__(self, decision: GuardDecision):
        super().__init__(f"egress blocked: {decision.reason}: {decision.url}")
        self.decision = decision


AddressResolver = Callable[[str, int | None], Awaitable[Sequence[str]]]


def _parse_url(url: str) -> tuple[ParseResult | None, str | None]:
    try:
        return urlparse(url), None
    except ValueError:
        return None, "invalid_url"


def _normalize_hostname(hostname: str | None) -> str | None:
    if hostname is None:
        return None
    normalized = hostname.strip().lower().rstrip(".")
    return normalized or None


def _is_blocked_hostname(hostname: str) -> bool:
    return hostname == "localhost" or hostname.endswith(".localhost")


def _ip_address(hostname: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    candidate = hostname.strip("[]")
    try:
        return ipaddress.ip_address(candidate)
    except ValueError:
        return None


def _is_blocked_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return any(
        (
            ip.is_private,
            ip.is_loopback,
            ip.is_link_local,
            ip.is_multicast,
            ip.is_reserved,
            ip.is_unspecified,
        )
    )


def is_url_allowed_without_dns(
    url: str,
    *,
    allow_private_network_egress: bool = False,
) -> GuardDecision:
    """Validate URL shape and IP literals without resolving DNS."""
    parsed, parse_error = _parse_url(url)
    if parsed is None:
        return GuardDecision(False, parse_error or "invalid_url", url)
    try:
        hostname = _normalize_hostname(parsed.hostname)
        parsed.port
    except ValueError:
        return GuardDecision(False, "invalid_url", url)
    if parsed.scheme.lower() not in SUPPORTED_SCHEMES:
        return GuardDecision(False, "unsupported_scheme", url, hostname)
    if hostname is None:
        return GuardDecision(False, "missing_host", url, hostname)
    if allow_private_network_egress:
        return GuardDecision(True, "allowed", url, hostname)
    if _is_blocked_hostname(hostname):
        return GuardDecision(False, "blocked_hostname", url, hostname)
    ip = _ip_address(hostname)
    if ip is not None and _is_blocked_ip(ip):
        return GuardDecision(False, "blocked_ip_literal", url, hostname)
    return GuardDecision(True, "allowed", url, hostname)


async def resolve_host_addresses(hostname: str, port: int | None) -> Sequence[str]:
    """Resolve all A/AAAA addresses for a hostname."""
    addrinfo = await asyncio.to_thread(
        socket.getaddrinfo,
        hostname,
        port,
        type=socket.SOCK_STREAM,
    )
    return sorted({item[4][0] for item in addrinfo})


async def check_url(
    url: str,
    *,
    resolver: AddressResolver | None = None,
    allow_private_network_egress: bool = False,
) -> GuardDecision:
    """Validate a URL including DNS answers for hostnames."""
    decision = is_url_allowed_without_dns(
        url,
        allow_private_network_egress=allow_private_network_egress,
    )
    if not decision.allowed or allow_private_network_egress:
        return decision

    hostname = decision.hostname
    if hostname is None:
        return decision
    if _ip_address(hostname) is not None:
        return decision

    parsed, parse_error = _parse_url(url)
    if parsed is None:
        return GuardDecision(False, parse_error or "invalid_url", url, hostname)
    try:
        port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
    except ValueError:
        return GuardDecision(False, "invalid_url", url, hostname)
    resolve = resolve_host_addresses if resolver is None else resolver
    try:
        addresses = await resolve(hostname, port)
    except Exception:
        return GuardDecision(False, "dns_error", url, hostname)
    for address in addresses:
        try:
            ip = ipaddress.ip_address(address)
        except ValueError:
            return GuardDecision(False, "dns_error", url, hostname)
        if _is_blocked_ip(ip):
            return GuardDecision(False, "blocked_resolved_ip", url, hostname)
    return decision


def raise_if_blocked(decision: GuardDecision) -> None:
    """Raise EgressBlockedError when the decision rejects the URL."""
    if not decision.allowed:
        raise EgressBlockedError(decision)
