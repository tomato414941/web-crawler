"""Outbound network safety checks for crawler URLs."""

from __future__ import annotations

import asyncio
import ipaddress
import re
import socket
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from urllib.parse import ParseResult, urlparse


SUPPORTED_SCHEMES = {"http", "https"}
DEFAULT_ALLOWED_PORTS = (80, 443)
EXPLICIT_BLOCKED_NETWORKS = tuple(
    ipaddress.ip_network(cidr)
    for cidr in (
        "100.64.0.0/10",
        "198.18.0.0/15",
        "fc00::/7",
        "fe80::/10",
    )
)
_LEGACY_IPV4_TOKEN_RE = re.compile(r"^(?:0x[0-9a-f]+|\d+)$", re.IGNORECASE)


@dataclass(frozen=True)
class GuardDecision:
    """Decision returned by the outbound egress guard."""

    allowed: bool
    reason: str
    url: str
    hostname: str | None = None
    port: int | None = None
    resolved_addresses: tuple[str, ...] = ()


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


def _looks_like_legacy_ipv4(hostname: str) -> bool:
    """Return True for IPv4 forms some stacks may coerce to dotted decimal."""
    if ":" in hostname:
        return False
    parts = hostname.split(".")
    if not 1 <= len(parts) <= 4:
        return False
    if not all(_LEGACY_IPV4_TOKEN_RE.match(part) for part in parts):
        return False
    if len(parts) != 4:
        return True
    for part in parts:
        if part.lower().startswith("0x"):
            return True
        if len(part) > 1 and part.startswith("0"):
            return True
    return False


def _is_blocked_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        return True
    if any(ip in network for network in EXPLICIT_BLOCKED_NETWORKS):
        return True
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
    allowed_ports: Sequence[int] = DEFAULT_ALLOWED_PORTS,
) -> GuardDecision:
    """Validate URL shape and IP literals without resolving DNS."""
    parsed, parse_error = _parse_url(url)
    if parsed is None:
        return GuardDecision(False, parse_error or "invalid_url", url)
    try:
        hostname = _normalize_hostname(parsed.hostname)
        parsed_port = parsed.port
    except ValueError:
        return GuardDecision(False, "invalid_url", url)
    if parsed.scheme.lower() not in SUPPORTED_SCHEMES:
        return GuardDecision(False, "unsupported_scheme", url, hostname)
    if hostname is None:
        return GuardDecision(False, "missing_host", url, hostname)
    if parsed.username is not None or parsed.password is not None:
        return GuardDecision(False, "userinfo_not_allowed", url, hostname)
    port = parsed_port or (443 if parsed.scheme.lower() == "https" else 80)
    if _looks_like_legacy_ipv4(hostname):
        return GuardDecision(False, "blocked_legacy_ipv4_literal", url, hostname, port)
    if not allow_private_network_egress:
        if _is_blocked_hostname(hostname):
            return GuardDecision(False, "blocked_hostname", url, hostname, port)
        ip = _ip_address(hostname)
        if ip is not None and _is_blocked_ip(ip):
            return GuardDecision(False, "blocked_ip_literal", url, hostname, port)
    if port not in set(allowed_ports):
        return GuardDecision(False, "blocked_port", url, hostname, port)
    if allow_private_network_egress:
        return GuardDecision(True, "allowed", url, hostname, port)
    return GuardDecision(True, "allowed", url, hostname, port)


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
    allowed_ports: Sequence[int] = DEFAULT_ALLOWED_PORTS,
) -> GuardDecision:
    """Validate a URL including DNS answers for hostnames."""
    decision = is_url_allowed_without_dns(
        url,
        allow_private_network_egress=allow_private_network_egress,
        allowed_ports=allowed_ports,
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
    port = decision.port
    resolve = resolve_host_addresses if resolver is None else resolver
    try:
        addresses = await resolve(hostname, port)
    except Exception:
        return GuardDecision(False, "dns_error", url, hostname, port)
    resolved_addresses = tuple(sorted(addresses))
    for address in addresses:
        try:
            ip = ipaddress.ip_address(address)
        except ValueError:
            return GuardDecision(False, "dns_error", url, hostname, port, resolved_addresses)
        if _is_blocked_ip(ip):
            return GuardDecision(False, "blocked_resolved_ip", url, hostname, port, resolved_addresses)
    return GuardDecision(
        decision.allowed,
        decision.reason,
        decision.url,
        decision.hostname,
        decision.port,
        resolved_addresses,
    )


def raise_if_blocked(decision: GuardDecision) -> None:
    """Raise EgressBlockedError when the decision rejects the URL."""
    if not decision.allowed:
        raise EgressBlockedError(decision)
