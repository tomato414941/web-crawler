"""REST API for serving crawl results."""

import hmac
import os

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse

from .storage import PgStorage

app = FastAPI(title="Web Crawler API", version="0.1.0")

_storage: PgStorage | None = None


def get_storage() -> PgStorage:
    if _storage is not None:
        return _storage
    dsn = os.environ.get("CRAWLER_POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("CRAWLER_POSTGRES_DSN is required")
    return PgStorage(dsn)


def close_storage(storage: PgStorage) -> None:
    if storage is not _storage:
        storage.close()


def require_api_token(
    authorization: str | None = Header(None),
    x_api_token: str | None = Header(None),
) -> None:
    token = os.environ.get("CRAWLER_API_TOKEN", "").strip()
    if not token:
        allow_unauthenticated = (
            os.environ.get("CRAWLER_ALLOW_UNAUTHENTICATED_API", "").strip().lower()
            in {"1", "true", "yes"}
        )
        if allow_unauthenticated:
            return
        raise HTTPException(status_code=503, detail="api_token_not_configured")

    supplied = x_api_token or ""
    if authorization:
        scheme, _, value = authorization.partition(" ")
        if scheme.lower() == "bearer":
            supplied = value

    if not hmac.compare_digest(supplied, token):
        raise HTTPException(status_code=401, detail="unauthorized")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/pages")
def list_pages(
    since: float = Query(0, description="Unix timestamp, return pages crawled after this time"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    host: str | None = Query(None),
    _auth: None = Depends(require_api_token),
):
    """List crawled pages."""
    storage = get_storage()
    try:
        pages = storage.list_pages(since=since, limit=limit, offset=offset, host=host)
    finally:
        close_storage(storage)
    for page in pages:
        if page.get("outlinks") is None:
            page["outlinks"] = []
    return {"pages": pages, "count": len(pages)}


@app.get("/pages/{url_hash}")
def get_page(url_hash: str, _auth: None = Depends(require_api_token)):
    """Get a single page with full content."""
    storage = get_storage()
    try:
        page = storage.get_page(url_hash)
    finally:
        close_storage(storage)
    if not page:
        return JSONResponse(status_code=404, content={"error": "not found"})
    if page.get("outlinks") is None:
        page["outlinks"] = []
    return page


@app.get("/stats")
def stats(_auth: None = Depends(require_api_token)):
    """Fast runtime crawl statistics."""
    storage = get_storage()
    try:
        return storage.get_runtime_stats_summary()
    finally:
        close_storage(storage)


@app.get("/stats/diagnostics")
def diagnostic_stats(_auth: None = Depends(require_api_token)):
    """Runtime-only diagnostics; live full-queue diagnostics are disabled in production."""
    storage = get_storage()
    try:
        stats = storage.get_runtime_stats_summary()
        stats["diagnostics_unavailable"] = True
        stats["diagnostics_error"] = "live_scheduler_diagnostics_disabled"
        stats["diagnostics_mode"] = "runtime_snapshot_only"
        return stats
    finally:
        close_storage(storage)
