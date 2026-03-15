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
    conditions = ["crawled_at > %s"]
    params: list = [since]

    if domain:
        conditions.append("domain = %s")
        params.append(domain)

    where = " AND ".join(conditions)
    params.extend([limit, offset])

    with storage._conn.cursor() as cur:
        cur.execute(
            f"""SELECT url_hash, url, domain, title, status, content_length,
                       outlinks, crawled_at
                FROM pages WHERE {where}
                ORDER BY crawled_at ASC
                LIMIT %s OFFSET %s""",
            params,
        )
        rows = cur.fetchall()

    pages = [
        {
            "url_hash": r[0],
            "url": r[1],
            "domain": r[2],
            "title": r[3],
            "status": r[4],
            "content_length": r[5],
            "outlinks": r[6] or [],
            "crawled_at": r[7],
        }
        for r in rows
    ]
    return {"pages": pages, "count": len(pages)}


@app.get("/pages/{url_hash}")
def get_page(url_hash: str):
    """Get a single page with full content."""
    storage = get_storage()
    with storage._conn.cursor() as cur:
        cur.execute(
            """SELECT url_hash, url, domain, title, content, status,
                      content_length, depth, source_url, outlinks, crawled_at
               FROM pages WHERE url_hash = %s""",
            (url_hash,),
        )
        row = cur.fetchone()

    if not row:
        return JSONResponse(status_code=404, content={"error": "not found"})

    return {
        "url_hash": row[0],
        "url": row[1],
        "domain": row[2],
        "title": row[3],
        "content": row[4],
        "status": row[5],
        "content_length": row[6],
        "depth": row[7],
        "source_url": row[8],
        "outlinks": row[9] or [],
        "crawled_at": row[10],
    }


@app.get("/stats")
def stats():
    """Crawl statistics."""
    storage = get_storage()
    with storage._conn.cursor() as cur:
        cur.execute(
            """SELECT
                 count(*) as total_pages,
                 count(DISTINCT domain) as domains,
                 min(crawled_at) as oldest,
                 max(crawled_at) as newest,
                 sum(content_length) as total_bytes
               FROM pages"""
        )
        row = cur.fetchone()

    return {
        "total_pages": row[0],
        "domains": row[1],
        "oldest_crawl": row[2],
        "newest_crawl": row[3],
        "total_bytes": row[4],
    }
