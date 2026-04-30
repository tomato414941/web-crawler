# web-crawler

Async broad-web crawler with adaptive fetching, PostgreSQL scheduler state, operator
observation, and a REST API.

`web-crawler` is designed to discover, fetch, classify, and record structured observations from
the broad public web. Seed URLs are bootstrap inputs, not the crawler's target scope. Detailed
design principles live in [docs/DESIGN_PRINCIPLES.md](docs/DESIGN_PRINCIPLES.md).

## Current Status

This project is under active development. It has safety guards, persistent scheduling, host
pacing, migrations, tests, and deployment scripts, but it should still be treated as an
experimental crawler rather than a production-ready web-scale system.

Production broad-web crawling requires a hardened runtime with network-layer egress containment.
The standard acquisition path is direct HTTP fetching. Browser rendering and the AI browser agent
are auxiliary paths and should run only under explicit isolation. See
[docs/security/egress.md](docs/security/egress.md).

The AI browser agent is experimental and outside the crawler core. See
[docs/AGENT_BOUNDARY.md](docs/AGENT_BOUNDARY.md).

## Features

- Adaptive HTTP-first fetching with optional browser rendering
- PostgreSQL-backed URL ledger, physical queues, leases, retries, and refresh scheduling
- Host-level robots, crawl delay, cooldown, and durable host state
- Discovery admission controls for broad-web expansion
- REST API for pages and runtime stats
- Read-only operator observation and scheduler invariant checks
- JSONL output for one-shot crawl runs
- Docker Compose stack for local or private deployment

## Install

```bash
pip install -e .

# Development / tests
pip install -e ".[dev]"

# Optional extras
pip install -e ".[browser]"
pip install -e ".[api]"
pip install -e ".[postgres]"
pip install -e ".[agent]"

# Everything
pip install -e ".[all]"
```

PostgreSQL support is required for `crawl`, `serve`, and `daemon`.

## Quick Start

```bash
# Fetch a single page
crawler fetch https://example.com

# Start PostgreSQL locally with Docker
docker compose up -d postgres

# Apply schema migrations
crawler migrate --postgres postgresql://crawler:crawler@localhost:5433/crawldb

# Run a bounded one-shot crawl. By default this stays on the start host.
crawler crawl https://example.com -n 100 \
  --postgres postgresql://crawler:crawler@localhost:5433/crawldb

# Allow the one-shot crawl to follow links to other hosts.
crawler crawl https://example.com -n 100 --any-host \
  --postgres postgresql://crawler:crawler@localhost:5433/crawldb

# Serve crawled pages over the REST API
crawler serve --port 8080 \
  --postgres postgresql://crawler:crawler@localhost:5433/crawldb

# Inspect runtime/storage state without mutating the database
crawler observe --postgres postgresql://crawler:crawler@localhost:5433/crawldb
```

## Common Commands

| Command | Description |
|---|---|
| `fetch` | Fetch one page (`--js` for browser, `--auto` for adaptive fetching) |
| `crawl` | Run a bounded one-shot crawl with PostgreSQL scheduler state |
| `daemon` | Run the continuous broad-web crawler loop |
| `serve` | Start the REST API server |
| `migrate` | Apply pending database migrations |
| `observe` | Print a read-only operator snapshot |
| `observe-watch` | Append periodic operator snapshots as JSON Lines |
| `scheduler-check` | Run read-only scheduler invariant checks and optional repairs |
| `check-links` | Find broken links from a page |
| `extract` | Extract content with CSS selectors or XPath |
| `agent` | Run the experimental AI browser agent for a bounded task |

### `crawl`

```bash
crawler crawl <url> [options]

Options:
  -n, --max-pages     Max pages to crawl (default: 100)
  -c, --concurrency   Concurrent workers (default: 5)
  --delay             Per-host delay in seconds (default: 1.0)
  --same-host         Keep the one-shot crawl on the start host (default)
  --any-host          Allow the one-shot crawl to follow links to other hosts
  --js                Use browser rendering for all pages
  -o, --output        Stream results to JSONL file
  --postgres DSN      Required: store scheduler state and pages in PostgreSQL
  --no-content        Exclude page content from output
```

### `observe`

```bash
crawler observe --postgres postgresql://user:pass@host/db
crawler observe --postgres postgresql://user:pass@host/db --json
```

Use `--scheduler-invariants` when you need live invariant checks. It may be expensive on large
databases.

### `scheduler-check`

```bash
crawler scheduler-check --postgres postgresql://user:pass@host/db
crawler scheduler-check --postgres postgresql://user:pass@host/db --json
```

The checker reports duplicate scheduler memberships, terminal URLs in live queues, expired
leases, host-head read-model drift, and URL identity issues.

## Docker

```bash
# Start the full stack. CRAWLER_API_TOKEN is required by docker-compose.yml.
docker compose up -d

# Check services and migration status
docker compose ps -a

# Run a manual one-shot crawl
docker compose run --rm crawler crawler crawl https://example.com -n 100

# Print an operator snapshot using the compose-provided DSN
docker compose run --rm api crawler observe
```

Default compose services:

- `postgres` — persistent crawl data, scheduler state, and host scheduling state
- `migrate` — one-shot schema migration runner
- `api` — FastAPI server on loopback port `8080`
- `crawler` — continuous daemon worker
- `observer` — periodic JSONL operator snapshots

For proxy-contained deployments, keep `docker-compose.yml` as the development baseline and layer
the hardened override:

```bash
docker compose -f docker-compose.yml -f docker-compose.hardened.yml up -d
docker compose -f docker-compose.yml -f docker-compose.hardened.yml \
  --profile egress-smoke run --rm egress-smoke
```

The hardened override routes crawler HTTP/HTTPS through `egress-proxy` and includes a private
test service for the egress smoke.

## Documentation

- [docs/README.md](docs/README.md) — documentation index
- [docs/DESIGN_PRINCIPLES.md](docs/DESIGN_PRINCIPLES.md) — product positioning and core principles
- [docs/system-architecture.md](docs/system-architecture.md) — subsystem boundaries and current gaps
- [docs/scheduler-state-model.md](docs/scheduler-state-model.md) — scheduler state, source of truth, and invariants
- [docs/scheduler-execution.md](docs/scheduler-execution.md) — lease path, hot-path constraints, and execution strategy
- [docs/discovered-representation.md](docs/discovered-representation.md) — discovered URL representation
- [docs/CONTENT_POLICY.md](docs/CONTENT_POLICY.md) — content handling and metadata-only resources
- [docs/AGENT_BOUNDARY.md](docs/AGENT_BOUNDARY.md) — experimental AI agent boundary
- [docs/security/egress.md](docs/security/egress.md) — threat model, outbound policy, and containment expectations
- [docs/api.md](docs/api.md) — REST API usage and authentication
- [docs/operations.md](docs/operations.md) — deployment and production operations
- [docs/seed-catalog.md](docs/seed-catalog.md) — seed catalog maintenance

## Development

```bash
pytest -q
ruff check src tests
```

Before pushing, also review Docker/daemon defaults when changing crawl pacing, seeds, scheduler
behavior, migrations, or API authentication.

## License

MIT
