# Egress Policy

The crawler treats the public web as untrusted input. Application-layer URL checks are a
defense-in-depth layer, not the only containment boundary.

## Default Policy

By default, crawler egress allows only:

- `http` and `https` URLs
- TCP ports `80` and `443`
- hostnames or IP literals that are not local, private, link-local, reserved, multicast,
  unspecified, CGNAT, benchmarking, or cloud metadata targets

The policy rejects:

- `localhost` and `*.localhost`
- loopback ranges such as `127.0.0.0/8` and `::1`
- RFC1918 private ranges
- link-local ranges, including `169.254.0.0/16` and `fe80::/10`
- IPv6 ULA `fc00::/7`
- CGNAT `100.64.0.0/10`
- benchmarking `198.18.0.0/15`
- multicast, reserved, and unspecified addresses
- IPv4-mapped IPv6 addresses
- legacy IPv4 forms such as `2130706433`, `0177.0.0.1`, and `0x7f000001`
- URLs with userinfo such as `http://user:pass@example.com/`
- non-allowed ports such as `22`, `2375`, `5432`, and `8000`

If any DNS answer for a hostname is blocked, the URL is rejected.

## Settings

`CRAWLER_ALLOW_PRIVATE_NETWORK_EGRESS=true` is a dangerous development-only override. It bypasses
private-network hostname and address checks, but it does not make unsupported schemes, userinfo,
legacy IPv4 forms, or blocked ports acceptable. Do not use it in production compose deployments.

`CRAWLER_ALLOWED_EGRESS_PORTS=80,443` controls the default allowed TCP ports. Keep production
deployments on `80,443` unless there is a narrow operational reason to expand the set.

## Remaining Risk

Application-layer checks happen before the underlying HTTP or browser stack opens a socket. They
reduce accidental unsafe fetches, but they do not fully eliminate DNS rebinding or time-of-check /
time-of-use gaps by themselves.

Production deployments should also enforce network-layer containment with host firewall, container
network policy, cloud security groups, or a controlled egress proxy. The fast HTTP path may remain
direct for throughput, but private, local, link-local, and metadata destinations should still be
blocked by the runtime environment.

## Network-Layer Smoke Test

Run the smoke test from the crawler container after applying host firewall, cloud security group,
or container network policy rules:

```bash
docker compose build crawler
docker compose run --rm --no-deps crawler python scripts/egress_smoke.py
```

The smoke test keeps normal HTTP egress direct. It verifies that `example.com:80` remains
reachable while representative local, private, link-local, metadata, CGNAT, and benchmarking
targets do not accept TCP connections from the crawler runtime.

This is a deployment smoke test, not an application unit test. If it fails, fix the runtime
network policy rather than weakening the application egress guard.
