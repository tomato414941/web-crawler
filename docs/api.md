# REST API

The API is intended as an operator and integration surface for crawled pages and runtime
statistics. It is not currently designed as a public internet-facing API.

## Start

```bash
crawler serve --port 8080 --postgres postgresql://user:pass@localhost/db
```

The API also requires the four `CRAWLER_R2_*` settings documented in
[operations.md](operations.md) to load page content.

## Authentication

Set `CRAWLER_API_TOKEN` to require either:

- `Authorization: Bearer <token>`
- `X-API-Token: <token>`

All endpoints except `/health` require authentication when the token is configured. If the token
is missing, non-health endpoints fail closed.

For local-only experiments, set `CRAWLER_ALLOW_UNAUTHENTICATED_API=true` to explicitly allow
unauthenticated access.

## Endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | Health check |
| `GET /pages` | List pages with `?since=`, `?limit=`, `?offset=`, and `?host=` filters |
| `GET /pages/{url_hash}` | Get page details with content loaded from R2 |
| `GET /stats` | Fast runtime crawl statistics from the persisted daemon snapshot |
| `GET /stats/diagnostics` | Runtime diagnostics surface; live full-queue diagnostics are disabled in production |

Daemon logs also emit a per-cycle `errors=...` summary using the same categories as `/stats`.

## Current Limitations

- Authentication is a single static bearer token.
- `/pages` still uses offset-style pagination.
- Content-bearing responses should be treated as internal data.
- TLS, rate limiting, audit logging, and token rotation should be handled by the deployment
  boundary until the API grows first-class support for them.
