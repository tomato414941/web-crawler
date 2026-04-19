# web-crawler

Async web crawler with adaptive rendering, AI agent, and REST API.

This project targets the broad public web as a whole. It is not a site-specific crawler.
Current crawl coverage may be biased by implementation limits or temporary seed choices; that
bias is an artifact to correct, not the intended scope of the project. Seed URLs are bootstrap
entry points for discovery, not an allowlist and not a statement of the crawler's target scope.

## Features

- **Adaptive Fetching** — HTTP first, auto-switches to browser rendering for JS-heavy sites
- **AI Agent** — Claude-powered autonomous browsing for complex tasks
- **Web-scale Discovery** — Seed URLs start the crawl, but discovered external hosts are valid crawl targets
- **Postgres-backed Scheduler** — Persistent crawl scheduler with URL leasing and retry backoff
- **Physical Scheduler Queues** — Exploration / backlog / recrawl queues plus retry quarantine
- **Host Scheduling State** — Durable per-host crawl delay and cooldown tracking in PostgreSQL
- **REST API** — Serve crawled pages via `/pages`, `/stats` endpoints
- **JSONL Export** — Optional streaming output alongside Postgres storage
- **robots.txt** — Per-host rate limiting and access control
- **Link Checker** — Detect broken links on any page
- **Data Extraction** — CSS selectors and XPath
- **Daemon Mode** — Continuous crawl loop with stale-page requeueing

## Install

```bash
pip install -e .

# Development / tests
pip install -e ".[dev]"

# Browser support (optional)
pip install -e ".[browser]"

# API support (optional)
pip install -e ".[api]"

# Postgres storage support (required for crawl / serve / daemon)
pip install -e ".[postgres]"

# AI agent (optional)
pip install -e ".[agent]"

# Everything
pip install -e ".[all]"
```

## Quick Start

```bash
# Fetch a single page
crawler fetch https://example.com

# Start PostgreSQL locally with Docker
docker compose up -d postgres

# Apply schema migrations
crawler migrate --postgres postgresql://crawler:crawler@localhost:5433/crawldb

# Crawl a site (Postgres is required)
crawler crawl https://example.com -n 100 \
  --postgres postgresql://crawler:crawler@localhost:5433/crawldb

# Also stream results to JSONL
crawler crawl https://example.com -o results.jsonl \
  --postgres postgresql://crawler:crawler@localhost:5433/crawldb

# Serve crawled pages over REST API
crawler serve --port 8080 \
  --postgres postgresql://crawler:crawler@localhost:5433/crawldb
```

## CLI Commands

| Command | Description |
|---|---|
| `fetch` | Fetch a single page (`--js` for browser, `--auto` for adaptive) |
| `crawl` | Crawl a site with persistent scheduler management |
| `check-links` | Find broken links (`-r` for recursive) |
| `extract` | Extract data with CSS/XPath selectors |
| `agent` | AI-powered autonomous browsing |
| `serve` | Start REST API server |
| `migrate` | Apply pending database migrations |
| `daemon` | Run the continuous crawler loop |

### crawl

```bash
crawler crawl <url> [options]

Options:
  -n, --max-pages     Max pages to crawl (default: 100)
  -c, --concurrency   Concurrent workers (default: 5)
  --delay             Per-host delay in seconds (default: 1.0)
  --same-host         Stay on the same host (default)
  --any-host          Follow links to other hosts
  --js                Use browser rendering for all pages
  -o, --output        Stream results to JSONL file
  --postgres DSN      Required: store scheduler state and pages in PostgreSQL
  --no-content        Exclude page content from output
```

### agent

```bash
crawler agent <url> -t "task description"

Options:
  -t, --task          Task to perform (required)
  --max-steps         Step limit (default: 10)
  -m, --model         Claude model (default: claude-sonnet-4-20250514)
  --headless          Run browser headless (default)
  --headed            Show browser window
```

### extract

```bash
crawler extract <url> -s "CSS selector"

Options:
  -s, --selector      CSS selector
  -x, --xpath         XPath expression
  -a, --attr          Extract attribute instead of text
  --js                Use browser rendering
```

## REST API

```bash
crawler serve --port 8080 --postgres postgresql://user:pass@localhost/db
```

| Endpoint | Description |
|---|---|
| `GET /health` | Health check |
| `GET /pages` | List pages (`?since=`, `?limit=`, `?host=`) |
| `GET /pages/{url_hash}` | Get page details with content |
| `GET /stats` | Crawl statistics, including scheduler error breakdown and top error hosts |

Daemon logs also emit a per-cycle `errors=...` summary using the same categories as `/stats`.

## Docker

```bash
# Start the full stack
docker compose up -d

# The compose stack runs migrations before api / crawler
docker compose ps -a

# Run a one-shot crawl manually
docker compose run --rm crawler crawler crawl https://example.com -n 100
```

Default compose services:
- `postgres` — persistent crawl data, scheduler state, and host scheduling state
- `migrate` — one-shot schema migration runner
- `api` — FastAPI server on port `8080`
- `crawler` — continuous daemon worker

## Architecture

```
crawler/
├── cli.py              # Typer CLI
├── api.py              # FastAPI REST server
├── crawl.py            # Crawler engine (worker pool)
├── url_ledger.py       # Scheduler facade and URL ledger state
├── scheduler_observability.py # Read-only scheduler snapshots
├── scheduler_quarantine.py    # Retry quarantine state transitions
├── daemon_policy.py    # Pre-cycle scheduler policy
├── host_manager.py   # robots.txt, runtime host state
├── host_store.py     # Persistent host scheduling state
├── host_state.py     # Runtime / persisted host state models
├── storage.py          # PostgreSQL storage
├── output.py           # JSONL streaming output
├── result.py           # Typed crawl success/failure results
├── extract.py          # CSS/XPath extraction
├── links.py            # Link checker
├── agent.py            # Claude AI agent
├── config.py           # Pydantic settings
└── core/
    ├── fetcher.py          # HTTP fetcher (httpx)
    ├── browser_fetcher.py  # Playwright fetcher
    ├── adaptive_fetcher.py # Auto HTTP→Browser switch
    └── protocols.py        # Response dataclass
```

### Fetcher Pipeline

```
URL → AdaptiveFetcher
      ├─ HTTP (fast path)
      │   └─ JS detected? → Browser fallback
      └─ Response
```

Current runtime note:

- The success path is now split as `lease -> fetch -> parse -> finalize -> persist`.
- `parse` builds parsed payloads only, `finalize` applies scheduler mutations, and `persist`
  writes pages/output.
- `finalize` uses a dedicated connection / executor so scheduler mutations no longer run on the
  main event loop.
- The remaining hot-path coupling is mostly on the failure side: runtime error bookkeeping still
  begins in workers, and latency is still a secondary scheduler signal rather than a primary one.

### Deduplication

Two layers:
1. **URL normalization** — scheme/host lowering, query sort, fragment removal
2. **PostgreSQL scheduler state** — `url_ledger` plus queue tables persist scheduler state

### Scheduling

Two persistent schedulers work together:
1. **URL scheduler** — controls retry timing, leasing, and recrawl eligibility
2. **Host state** — controls per-host crawl delay and cooldown via `host_state`

Current scheduler state is split across explicit physical tables:

- `url_ledger` — URL ledger and crawl result metadata
- `scheduler_queue_frontline` — frontline runnable or scheduled discovery work
- `scheduler_queue_deferred` — deferred discovery work
- `scheduler_queue_refresh` — stale-page revisit work
- `scheduler_queue_retry_quarantine` — retry quarantine for host-cooled URLs
- `active_leases` — active leases only

Current module boundaries:

- `url_ledger.py` — scheduler-facing facade used by the crawler
- `scheduler_observability.py` — queue and readiness snapshots
- `scheduler_quarantine.py` — host-backoff quarantine policy and state transitions
- `daemon_policy.py` — pre-cycle scheduler maintenance policy

Current runtime queue stages exposed in daemon stats:

- `parse_queue_*` — fetch to parse handoff
- `finalize_queue_*` — parse to scheduler-mutation handoff
- `publish_queue_*` — finalize to storage/output handoff

`pending` in `/stats` should be read as "not done yet", not as "immediately runnable".
For actual scheduler state, prefer `readiness`:

- `runnable` — runnable now
- `scheduled` — waiting on `next_fetch_at`
- `blocked_host_next_request` — waiting on per-host request slot timing
- `blocked_host_backoff` — still in host cooldown while in normal queues
- `retry_quarantine` — already isolated from normal queues and only restored through retry budget

### Scheduler Design Principles

The scheduler should be judged by a small set of explicit principles:

1. Separate runtime scheduler truth from operator read models.
2. Keep the crawl hot path as small as possible.
3. Prefer requeue over in-slot stubborn retry.
4. Keep host pacing state small and explicit.
5. Keep planner logic thin; do not let it become the home for product policy,
   observability, and safety all at once.

These principles are intentionally narrower than any one implementation. They
are the rules `web-crawler` should preserve even if tables, workers, or queue
names change later.

In daemon mode, seeds are starting points for graph expansion. The crawler is expected to
discover and follow links onto other hosts unless a specific crawl run is configured to stay
on the same host.

Discovery priority is now based on generic URL structure rather than site-specific rules.
Redirect-like paths, document-like paths, and bulk/listing paths are classified from reusable
path heuristics so the scheduler does not depend on hard-coded `IANA` / `IETF` / `RFC Editor`
special cases.

### Content Scope

Content handling policy is documented in [docs/CONTENT_POLICY.md](/home/dev/projects/web-crawler/docs/CONTENT_POLICY.md).
Use that document as the source of truth for what is stored as page content, what is treated as
metadata-only, and which content types remain deferred.

## Deployment

Current deployment shape:
- Server: Hetzner `cx23`
- Path: `~/projects/web-crawler`
- Network: Tailscale preferred
- Runtime: Docker Compose
- Exposed API: port `8080`

Recommended production `.env`:

```bash
CRAWL_SEED_URLS="https://www.iana.org/ https://datatracker.ietf.org/ https://www.rfc-editor.org/"
CRAWL_CYCLE_PAGES=300
CRAWL_RECRAWL_TTL=2592000
CRAWL_CONCURRENCY=6
CRAWL_DELAY=0.5
```

These defaults avoid `www.icann.org`, which is currently hostile to the crawler, and reduce
stale-page churn so the daemon does not spend cycles requeueing dead backlog too aggressively.
Store them in a local `.env` on the server; do not commit runtime-specific values.

These production seeds are only bootstrap points. They do not define the full crawl scope.

The committed seed catalog lives in `config/seeds.json`. Treat it as the operator-facing source
of truth for which URLs are seeds and why they exist. Runtime `.env` files should only contain
the rendered `CRAWL_SEED_URLS` string for the currently enabled subset.

Render the current catalog into an env assignment with:

```bash
PYTHONPATH=src python scripts/render_seed_env.py
```

Each catalog entry stores:
- `url` — the seed URL itself
- `enabled` — whether it should appear in rendered runtime seed lists
- `tags` — operator metadata such as `tech`, `media`, `culture`, `public-sector`
- `notes` — short rationale for why the seed exists

Tags are for operator understanding and seed-set maintenance. They are not currently used by
runtime scheduling policy.

`docker-compose.yml` consumes these `CRAWL_*` variables as CLI flags for `crawler daemon`.
The application also exposes lower-level `CRAWLER_*` settings for scheduler tuning:

```bash
CRAWLER_FRONTIER_LEASE_SECONDS=300
CRAWLER_FRONTIER_RETRY_BACKOFF_SECONDS=30
CRAWLER_FRONTIER_MAX_RETRY_BACKOFF_SECONDS=1800
CRAWLER_ROBOTS_CACHE_TTL=3600
CRAWLER_HOST_BACKOFF_SECONDS=30
CRAWLER_MAX_HOST_BACKOFF_SECONDS=600
CRAWLER_DAEMON_KEEP_READY_PER_HOST=128
CRAWLER_DAEMON_BACKLOG_LOW_PRIORITY=0.75
CRAWLER_DAEMON_BACKLOG_DEFER_SECONDS=1800
CRAWLER_DAEMON_MIN_READY_SLEEP=0.5
CRAWLER_DAEMON_MIN_EXPLORATION_READY=20
CRAWLER_DAEMON_BLOCKED_RETRY_BUDGET=8
CRAWLER_DAEMON_BLOCKED_RETRY_PER_HOST=1
CRAWLER_DAEMON_BLOCKED_RETRY_MAX_CONSECUTIVE_FAILURES=8
CRAWLER_DAEMON_QUARANTINE_RETIRE_MIN_CONSECUTIVE_FAILURES=64
CRAWLER_DAEMON_QUARANTINE_RETIRE_AFTER_SECONDS=86400
```

Use `CRAWLER_*` only when you need to tune scheduler behavior without changing the daemon CLI
arguments wired through Compose.

Before pushing:
- Run `pytest -q`
- Run `ruff check src tests`
- Review `docker-compose.yml` env defaults for seeds and crawl pacing

## License

MIT
