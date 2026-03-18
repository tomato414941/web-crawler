"""REST API for serving crawl results."""

import os

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from .storage import PgStorage

app = FastAPI(title="Web Crawler API", version="0.1.0")

_storage: PgStorage | None = None


def get_storage() -> PgStorage:
    global _storage
    if _storage is None:
        dsn = os.environ.get("CRAWLER_POSTGRES_DSN")
        if not dsn:
            raise RuntimeError("CRAWLER_POSTGRES_DSN is required")
        _storage = PgStorage(dsn)
    return _storage


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/pages")
def list_pages(
    since: float = Query(0, description="Unix timestamp, return pages crawled after this time"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    domain: str | None = Query(None),
):
    """List crawled pages."""
    storage = get_storage()
    pages = storage.list_pages(since=since, limit=limit, offset=offset, domain=domain)
    for page in pages:
        if page.get("outlinks") is None:
            page["outlinks"] = []
    return {"pages": pages, "count": len(pages)}


@app.get("/pages/{url_hash}")
def get_page(url_hash: str):
    """Get a single page with full content."""
    storage = get_storage()
    page = storage.get_page(url_hash)
    if not page:
        return JSONResponse(status_code=404, content={"error": "not found"})
    if page.get("outlinks") is None:
        page["outlinks"] = []
    return page


@app.get("/stats")
def stats():
    """Crawl statistics."""
    storage = get_storage()
    return storage.get_stats()
