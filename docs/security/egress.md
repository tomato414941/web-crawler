# Egress Policy

The crawler treats the public web as untrusted input. Application-layer URL checks are a
defense-in-depth layer, not the only containment boundary.

## Threat Model

Production broad-web crawling assumes fetched pages, redirects, DNS answers, and rendered
subresources can be hostile. A public page can link or redirect toward private infrastructure,
cloud metadata, service networks, unsupported schemes, or ports that are not part of the intended
crawl surface. JavaScript execution can also create browser subresource requests that are harder
to reason about than the HTTP fast path.

The production requirement is therefore a hardened runtime: application egress checks plus a
network-layer boundary such as host firewall rules, container network policy, cloud security
groups, or a controlled egress proxy. The application egress guard is required, but it is
defense-in-depth rather than the sole production control.

The standard acquisition path is direct HTTP fetching through `HttpFetcher`. It is the fast path
for normal broad-web crawl. Browser rendering through `BrowserFetcher` is an auxiliary path for
pages that need JavaScript, and the AI browser agent is outside the crawler core. Browser and agent
execution should be isolated from normal broad-web crawl capacity and should not weaken runtime
egress containment.

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

Direct egress means the HTTP fast path connects from the crawler runtime to public `http` and
`https` targets after the application guard allows the URL. This is the default throughput path
and still requires network-layer blocks for private, local, link-local, metadata, and other
non-public destinations.

Proxy egress means the runtime sends outbound HTTP/HTTPS through a controlled proxy. The proxy is
part of the hardened runtime and must enforce the same deny surface as the application guard.
Using a proxy can centralize audit and policy, but it does not make `CRAWLER_ALLOW_PRIVATE_NETWORK_EGRESS`
safe for production and does not remove the need for application URL checks.

`CRAWLER_EGRESS_PROXY=http://egress-proxy:3128` routes crawler-owned HTTP clients through a
proxy. `CRAWLER_REQUIRE_EGRESS_PROXY=true` fails startup when that proxy is not configured.
`CRAWLER_DIRECT_EGRESS_ALLOWED=false` also fails startup unless a proxy is configured. Hardened
profiles should set all three so HTTP page fetches and robots fetches share one transport policy.

## Remaining Risk

Application-layer checks happen before the underlying HTTP or browser stack opens a socket. They
reduce accidental unsafe fetches, but they do not fully eliminate DNS rebinding or time-of-check /
time-of-use gaps by themselves.

Production deployments should also enforce network-layer containment with host firewall, container
network policy, cloud security groups, or a controlled egress proxy. `docker-compose.yml` remains
the development compose file. `docker-compose.hardened.yml` is an override that puts crawler
workers on an internal Docker network and routes HTTP/HTTPS through an existing Squid proxy image.
The proxy is the only service attached to the external egress network.

Use the hardened override for deployments that should force proxy egress:

```bash
docker compose -f docker-compose.yml -f docker-compose.hardened.yml up -d
```

The hardened override sets `CRAWLER_EGRESS_PROXY`, `CRAWLER_REQUIRE_EGRESS_PROXY=true`, and
`CRAWLER_DIRECT_EGRESS_ALLOWED=false` for the crawler. The crawler HTTP client uses that explicit
proxy policy instead of inheriting ambient proxy environment variables. The proxy configuration
denies local, private, link-local, metadata, CGNAT, benchmarking, multicast, reserved, and unsafe
port destinations.

The current private Docker Compose deployment keeps the HTTP fast path direct and uses a host
firewall rule in `DOCKER-USER` to block link-local / metadata egress:

```bash
iptables -C DOCKER-USER -d 169.254.0.0/16 -j REJECT 2>/dev/null \
  || iptables -I DOCKER-USER 1 -d 169.254.0.0/16 -j REJECT
```

That rule is intentionally narrow. The application egress guard still rejects private networks
such as RFC1918, CGNAT, and benchmarking ranges before fetch. A broad network-layer RFC1918 block
would need service-network exceptions because Docker itself uses private address space for
PostgreSQL and service-to-service traffic.

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

For the hardened proxy path, run the smoke profile:

```bash
docker compose -f docker-compose.yml -f docker-compose.hardened.yml \
  --profile egress-smoke run --rm egress-smoke
```

The hardened smoke starts a private nginx test service on an internal network that only the proxy
can see. It verifies that public HTTP succeeds through the proxy, direct public egress from the
smoke container is unavailable, and the private test service is not reachable through the proxy.

This is a deployment smoke test, not an application unit test. If it fails, fix the runtime
network policy rather than weakening the application egress guard.
