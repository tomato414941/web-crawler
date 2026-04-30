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
  unsupported egress targets before fetch. This is defense in depth; keep network-layer egress
  controls in place for stronger containment.

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
```

Store runtime-specific values in a local `.env` on the server. Do not commit them.

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

For periodic production observation, use `observe-watch`:

```bash
crawler observe-watch \
  --postgres postgresql://user:pass@host/db \
  --interval 300 \
  --output /var/log/web-crawler/observations.jsonl
```

In Docker Compose, the `observer` service writes records to the `observer_logs` volume at
`/observations/observations.jsonl`. Treat each JSONL file as single-writer output.
