# Operations

This document covers the current private deployment model. It is intentionally narrower than a
general production guide.

## Current Deployment Shape

- Server: Hetzner `cx23`
- Path: `/home/dev/projects/web-crawler`
- Network: Tailscale preferred
- Runtime: Docker Compose
- Exposed API: loopback port `8080`; use Tailscale or a reverse proxy for remote access

## Deploy

Production deploys from GitHub `main`. The production `origin` remote should be:

```bash
https://github.com/tomato414941/web-crawler.git
```

Run from the production server:

```bash
cd /home/dev/projects/web-crawler

git status --short --branch
git pull --ff-only origin main

docker compose build migrate api crawler observer
docker compose run --rm migrate
docker compose up -d api crawler observer

docker compose ps
curl -sS http://127.0.0.1:8080/health
docker compose run --rm api crawler observe
docker compose run --rm api crawler scheduler-check --sample-limit 0
docker compose run --rm --no-deps crawler python scripts/egress_smoke.py
docker compose logs --tail 20 observer
```

## Rules

- Stop if `git pull --ff-only` fails; do not deploy from a divergent tree.
- Do not use `git reset` or bundle transfer for normal deploys.
- Run `migrate` every deploy as an idempotent schema check.
- Do not touch the PostgreSQL volume during a normal deploy.
- Keep `CRAWLER_API_TOKEN` set.
- Unauthenticated API access requires an explicit local-only override and should not be used in
  production.
- The application rejects private, loopback, link-local, multicast, reserved, unresolved, and
  unsupported egress targets before fetch. This is defense in depth. Production broad-web crawl
  requires a hardened runtime with network-layer egress controls.
- Keep the standard HTTP fast path direct unless an operator intentionally routes it through a
  controlled proxy. Browser rendering is an auxiliary path; do not use it for normal broad-web
  crawl without separate isolation.

## Recommended Environment

```bash
CRAWL_SEED_URLS="https://www.iana.org/ https://datatracker.ietf.org/ https://www.rfc-editor.org/"
CRAWL_CYCLE_PAGES=300
CRAWL_RECRAWL_TTL=2592000
CRAWL_CONCURRENCY=6
CRAWL_DELAY=0.5
CRAWLER_OBSERVE_INTERVAL=300
CRAWLER_OBSERVE_MAX_BYTES=10485760
CRAWLER_OBSERVE_MAX_FILES=7
CRAWLER_OBSERVE_MAX_FAILURES=5
CRAWLER_API_TOKEN=<random-long-token>
CRAWLER_R2_ENDPOINT_URL=https://<account-id>.r2.cloudflarestorage.com
CRAWLER_R2_BUCKET=web-crawler
CRAWLER_R2_ACCESS_KEY_ID=<access-key-id>
CRAWLER_R2_SECRET_ACCESS_KEY=<secret-access-key>
```

Store runtime-specific values in a local `.env` on the server. Do not commit them.

The R2 bucket is private. Each text-like response body is stored under the SHA-256 hash of its
normalized URL. PostgreSQL stores the same hash and page metadata; it does not store the body.

## Scheduler Tuning

`docker-compose.yml` consumes `CRAWL_*` variables as CLI flags for `crawler daemon`. Lower-level
`CRAWLER_*` settings tune scheduler behavior without changing those daemon CLI arguments:

```bash
CRAWLER_SCHEDULER_LEASE_SECONDS=300
CRAWLER_SCHEDULER_RETRY_BACKOFF_SECONDS=30
CRAWLER_SCHEDULER_MAX_RETRY_BACKOFF_SECONDS=1800
CRAWLER_ROBOTS_CACHE_TTL=3600
CRAWLER_HOST_BACKOFF_SECONDS=30
CRAWLER_MAX_HOST_BACKOFF_SECONDS=600
CRAWLER_DAEMON_KEEP_RUNNABLE_PER_HOST=128
CRAWLER_DAEMON_KEEP_RUNNABLE_PER_BRANCH=16
CRAWLER_DAEMON_SCHEDULED_SURFACE_DELAY_SECONDS=1800
CRAWLER_DAEMON_MIN_RUNNABLE_SLEEP=0.5
CRAWLER_DAEMON_MIN_RUNNABLE_SUPPLY_COUNT=20
CRAWLER_DAEMON_MIN_RUNNABLE_SUPPLY_HOSTS=8
CRAWLER_DAEMON_BLOCKED_RETRY_BUDGET=8
CRAWLER_DAEMON_BLOCKED_RETRY_PER_HOST=1
CRAWLER_DAEMON_BLOCKED_RETRY_MAX_CONSECUTIVE_FAILURES=8
CRAWLER_DAEMON_QUARANTINE_RETIRE_MIN_CONSECUTIVE_FAILURES=64
CRAWLER_DAEMON_QUARANTINE_RETIRE_AFTER_SECONDS=86400
CRAWLER_ADMISSION_TARGET_PENDING=500000
```

`CRAWLER_ADMISSION_TARGET_PENDING` is the primary discovery admission knob. The crawler derives
its admission mode, score threshold, and per-page/per-host caps from the current pending count
relative to that target.

## Observation

Use read-only observation after deploys and during production checks:

```bash
docker compose run --rm api crawler observe
docker compose run --rm api crawler observe --json
docker compose run --rm api crawler scheduler-check --sample-limit 0
```

## Production Egress Smoke

Run the network-layer smoke after deploying or changing host firewall, cloud security group, or
container network policy rules:

```bash
docker compose run --rm --no-deps crawler python scripts/egress_smoke.py
```

The expected result is:

- public `example.com:80` connects
- representative local, private, link-local, metadata, CGNAT, and benchmarking targets do not
  connect

This confirms the runtime containment boundary without forcing the crawler HTTP fast path through
a proxy.

To force crawler HTTP/HTTPS through a controlled proxy, layer the hardened compose override on top
of the development compose file:

```bash
docker compose -f docker-compose.yml -f docker-compose.hardened.yml up -d
```

Run the hardened egress smoke with its private test service:

```bash
docker compose -f docker-compose.yml -f docker-compose.hardened.yml \
  --profile egress-smoke run --rm egress-smoke
```

Expected hardened smoke results:

- public HTTP fetch succeeds through `egress-proxy`
- direct public HTTP from the smoke container fails because the container is on an internal network
- `http://private-test/` does not fetch through the proxy
- representative local, private, link-local, metadata, CGNAT, and benchmarking TCP probes do not
  connect

Direct egress means the crawler container opens outbound HTTP/HTTPS connections itself after the
application egress guard allows the URL. Proxy egress means outbound HTTP/HTTPS is routed through a
controlled proxy that must enforce at least the same private, local, link-local, metadata, and
port restrictions. Either mode still requires the application egress guard; proxy mode is a
runtime containment choice, not a replacement for URL admission checks.

For the hardened compose profile, validate the merged configuration before deployment:

```bash
CRAWLER_API_TOKEN=replace-me \
  CRAWLER_R2_ENDPOINT_URL=https://example.r2.cloudflarestorage.com \
  CRAWLER_R2_BUCKET=web-crawler \
  CRAWLER_R2_ACCESS_KEY_ID=replace-me \
  CRAWLER_R2_SECRET_ACCESS_KEY=replace-me \
  docker compose -f docker-compose.yml -f docker-compose.hardened.yml config >/dev/null
```

The hardened profile sets `CRAWLER_EGRESS_PROXY`, `CRAWLER_REQUIRE_EGRESS_PROXY=true`, and
`CRAWLER_DIRECT_EGRESS_ALLOWED=false` for the crawler service.

Current private deployment status as of 2026-04-30:

- `web-crawler-egress-firewall.service` is enabled and active.
- The service restores a `DOCKER-USER` reject rule for `169.254.0.0/16`.
- `scripts/egress_smoke.py` passes from the crawler container.
- Public `example.com:80` remains reachable directly.
- `169.254.169.254:80` no longer accepts TCP connections from the crawler container.
- Other representative private / CGNAT / benchmarking probe targets did not connect in the smoke
  test.

The current network-layer block is deliberately narrow so Docker's private service network keeps
working. The application egress policy remains the primary fast-path guard for RFC1918, CGNAT,
benchmarking, legacy IPv4, blocked ports, and unsafe DNS answers.

For periodic production observation, use `observe-watch`:

```bash
crawler observe-watch \
  --postgres postgresql://user:pass@host/db \
  --interval 300 \
  --output /var/log/web-crawler/observations.jsonl
```

In Docker Compose, the `observer` service writes records to the `observer_logs` volume at
`/observations/observations.jsonl`. Treat each JSONL file as single-writer output.
