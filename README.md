# web-crawler

Async web crawler with adaptive rendering, AI agent, and REST API.

This project targets the public web as a whole. Seed URLs are entry points for discovery,
not an allowlist of the only domains the crawler may visit.

## Features

- **Adaptive Fetching** — HTTP first, auto-switches to browser rendering for JS-heavy sites
- **AI Agent** — Claude-powered autonomous browsing for complex tasks
- **Web-scale Discovery** — Seed URLs start the crawl, but discovered external domains are valid crawl targets
- **Postgres-backed Frontier** — Persistent crawl scheduler with URL leasing and retry backoff
- **Host Scheduling State** — Durable per-host crawl delay and cooldown tracking in PostgreSQL
- **REST API** — Serve crawled pages via `/pages`, `/stats` endpoints
- **JSONL Export** — Optional streaming output alongside Postgres storage
- **robots.txt** — Per-domain rate limiting and access control
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
| `crawl` | Crawl a site with frontier management |
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
  -d, --max-depth     Link depth limit (default: 3)
  -c, --concurrency   Concurrent workers (default: 5)
  --delay             Per-domain delay in seconds (default: 1.0)
  --same-domain       Stay on the same domain (default)
  --any-domain        Follow links to other domains
  --js                Use browser rendering for all pages
  -o, --output        Stream results to JSONL file
  --postgres DSN      Required: store frontier and pages in PostgreSQL
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
| `GET /pages` | List pages (`?since=`, `?limit=`, `?domain=`) |
| `GET /pages/{url_hash}` | Get page details with content |
| `GET /stats` | Crawl statistics, including frontier error breakdown and top error domains |

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
- `postgres` — persistent crawl data, frontier state, and host scheduling state
- `migrate` — one-shot schema migration runner
- `api` — FastAPI server on port `8080`
- `crawler` — continuous daemon worker

## Architecture

```
crawler/
├── cli.py              # Typer CLI
├── api.py              # FastAPI REST server
├── crawl.py            # Crawler engine (worker pool)
├── frontier.py         # URL scheduler + PostgreSQL leasing
├── domain_manager.py   # robots.txt, runtime host state
├── domain_store.py     # Persistent host scheduling state
├── domain_state.py     # Runtime / persisted host state models
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

### Deduplication

Two layers:
1. **URL normalization** — scheme/host lowering, query sort, fragment removal
2. **PostgreSQL frontier** — unique URL primary key with `pending` / `leased` / `done` / `failed` state

### Scheduling

Two persistent schedulers work together:
1. **URL frontier** — controls retry timing, leasing, and recrawl eligibility
2. **Host state** — controls per-host crawl delay and cooldown via `domain_state`

In daemon mode, seeds are starting points for graph expansion. The crawler is expected to
discover and follow links onto other domains unless a specific crawl run is configured to stay
on the same domain.

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
CRAWL_MAX_DEPTH=2
CRAWL_CONCURRENCY=6
CRAWL_DELAY=0.5
```

These defaults avoid `www.icann.org`, which is currently hostile to the crawler, and reduce
stale-page churn so the daemon does not spend cycles requeueing dead backlog too aggressively.
Store them in a local `.env` on the server; do not commit runtime-specific values.

These production seeds are only bootstrap points. They do not define the full crawl scope.

`docker-compose.yml` consumes these `CRAWL_*` variables as CLI flags for `crawler daemon`.
The application also exposes lower-level `CRAWLER_*` settings for scheduler tuning:

```bash
CRAWLER_FRONTIER_LEASE_SECONDS=300
CRAWLER_FRONTIER_RETRY_BACKOFF_SECONDS=30
CRAWLER_FRONTIER_MAX_RETRY_BACKOFF_SECONDS=1800
CRAWLER_ROBOTS_CACHE_TTL=3600
CRAWLER_HOST_BACKOFF_SECONDS=30
CRAWLER_MAX_HOST_BACKOFF_SECONDS=600
CRAWLER_DAEMON_KEEP_READY_PER_DOMAIN=128
CRAWLER_DAEMON_BACKLOG_LOW_PRIORITY=0.75
CRAWLER_DAEMON_BACKLOG_DEFER_SECONDS=1800
CRAWLER_DAEMON_MIN_READY_SLEEP=0.5
```

Use `CRAWLER_*` only when you need to tune scheduler behavior without changing the daemon CLI
arguments wired through Compose.

Before pushing:
- Run `pytest -q`
- Run `ruff check src tests`
- Review `docker-compose.yml` env defaults for seeds and crawl pacing

## License

MIT
