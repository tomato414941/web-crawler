# web-crawler

Async web crawler with adaptive rendering, AI agent, and REST API.

## Features

- **Adaptive Fetching** — HTTP first, auto-switches to browser rendering for JS-heavy sites
- **AI Agent** — Claude-powered autonomous browsing for complex tasks
- **Bloom Filter** — Memory-efficient URL deduplication
- **REST API** — Serve crawled pages via `/pages`, `/stats` endpoints
- **Multiple Output** — JSONL, SQLite, WARC, PostgreSQL
- **robots.txt** — Per-domain rate limiting and access control
- **Link Checker** — Detect broken links on any page
- **Data Extraction** — CSS selectors and XPath

## Install

```bash
pip install -e .

# Browser support (optional)
pip install -e ".[browser]"

# AI agent (optional)
pip install -e ".[all]"
```

## Quick Start

```bash
# Fetch a single page
crawler fetch https://example.com

# Crawl a site (100 pages max)
crawler crawl https://example.com -n 100

# Output to JSONL
crawler crawl https://example.com -o results.jsonl

# Store in PostgreSQL
crawler crawl https://example.com --postgres postgresql://user:pass@localhost/db
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
  --format            Output format: jsonl, sqlite, warc
  --postgres DSN      Store results in PostgreSQL
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
| `GET /stats` | Crawl statistics |

## Docker

```bash
# Start Postgres + API
docker compose up -d

# Run a crawl
docker compose run --rm crawler crawler crawl https://example.com -n 100
```

## Architecture

```
crawler/
├── cli.py              # Typer CLI
├── api.py              # FastAPI REST server
├── crawl.py            # Crawler engine (worker pool)
├── frontier.py         # URL queue + Bloom filter
├── domain_manager.py   # robots.txt, rate limiting
├── storage.py          # PostgreSQL storage
├── output.py           # JSONL streaming output
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

Three layers:
1. **Bloom filter** — fast in-memory check (0.1% false positive)
2. **SQLite frontier** — persistent unique constraint
3. **URL normalization** — scheme/host lowering, query sort, fragment removal

## License

MIT
