# Repository Guidelines

## What This Project Is
A broad-WWW async web crawler service. Crawls autonomously, stores results in PostgreSQL, and serves them via REST API. Designed to be consumed by other services (e.g., search engines) but has no knowledge of its consumers.

The target is the public web as a whole, not any specific topic, standards body, or ecosystem. If existing seeds, pending queues, or stored pages are biased toward one domain community, treat that as an implementation artifact to correct rather than as the intended product scope.

## Project Structure
Single Python package with CLI entry point:
- `src/crawler/` — all source code
  - `cli.py` — Typer CLI (fetch, crawl, serve, agent, extract, check-links)
  - `api.py` — FastAPI REST server
  - `crawl.py` — crawler engine (worker pool, link extraction)
  - `url_ledger.py` — scheduler facade and durable URL state
  - `domain_manager.py` — robots.txt and runtime host scheduling state
  - `domain_store.py` — durable host scheduling state in Postgres
  - `domain_state.py` — runtime / persisted host state models
  - `storage.py` — PostgreSQL storage
  - `output.py` — JSONL streaming output
  - `result.py` — typed result models
  - `extract.py` — CSS/XPath data extraction
  - `links.py` — broken link checker
  - `agent.py` — Claude AI web agent
  - `config.py` — Pydantic settings
  - `core/` — fetcher layer (HTTP, browser, adaptive)
- `tests/` — pytest test suite

## Build, Test, and Development Commands
```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Lint
ruff check src/ && ruff format src/

# Run crawler
crawler crawl https://example.com -n 100

# Run API server
crawler serve --port 8080 --postgres postgresql://crawler:crawler@localhost/crawldb

# Docker
docker compose up -d
```

## Coding Style
- Python 3.11+, async/await throughout
- 4-space indentation, `ruff` formatting, line length 100
- Modules and functions: `snake_case`, Classes: `PascalCase`
- Protocol-based fetcher design (HTTP/Browser are swappable)

## Testing
- `pytest` with `asyncio_mode = "auto"`
- Test files: `test_*.py`, test functions: `test_*`
- Add tests for new behavior

## Commit Messages
`type: description` format (e.g., `feat:`, `fix:`, `refactor:`, `docs:`, `test:`)

## Scope Guardrails
- Treat broad WWW crawling as the highest-level product constraint
- Do not infer product scope from the current corpus, pending queue, or existing seed list
- Prefer host diversity over same-host branch coverage when choosing bootstrap seeds
- If a design choice improves one domain ecosystem at the expense of broad-web coverage, reject it unless explicitly requested

## Key Design Decisions
- **Frontier uses Postgres** — Postgres stores URL state, lease timing, and retry metadata
- **Host scheduler uses Postgres** — per-host crawl delay and cooldown survive restarts
- **Adaptive fetcher** — HTTP first, falls back to Playwright if JS rendering detected
- **Two crawl modes** — `crawler crawl` is one-shot, `crawler daemon` is continuous
- **PostgreSQL for results** — crawled pages stored in `pages` table, served via API
- **No consumer knowledge** — this service does not know about web-search or any consumer

## Deployment
- Server: `web-crawler` (Hetzner cx23, Nuremberg, 46.225.221.84)
- Tailscale: 100.92.121.94
- Docker Compose: `postgres` + `api` + `crawler`
- Repo is deployed to `~/projects/web-crawler` on the server
- SSH: `ssh dev@100.92.121.94`

## Release Notes
- This repo currently deploys from `main`
- Check `git status --short --branch` before commit / push
- Prefer small commits and push only after `pytest -q` and `ruff check src tests`

## Security
- Never commit credentials
- PostgreSQL DSN is passed via environment variable `CRAWLER_POSTGRES_DSN`
- API key for Claude agent via `ANTHROPIC_API_KEY` env var
