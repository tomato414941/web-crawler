"""Crawler engine with async concurrency."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import httpx
import typer

from .config import settings
from .core import HttpFetcher
from .domain_manager import DomainManager
from .frontier import CrawlTask, Frontier
from .output import StreamingOutputWriter
from .urls import extract_links

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .storage import PgStorage

# Workers wait this many idle ticks (× 0.1s) before giving up
_WORKER_PATIENCE = 50


class CrawlerEngine:
    """Async crawler engine with concurrent processing."""

    def __init__(
        self,
        start_url: str = "",
        max_pages: int = 100,
        max_depth: int = 3,
        same_domain: bool = True,
        use_browser: bool = False,
        delay: float = 1.0,
        concurrency: int = 5,
        output_writer: StreamingOutputWriter | None = None,
        pg_storage: "PgStorage | None" = None,
        frontier: Frontier | None = None,
        domain_manager: DomainManager | None = None,
    ):
        self.start_url = start_url
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.same_domain = same_domain
        self.use_browser = use_browser
        self.concurrency = concurrency
        self.output_writer = output_writer
        self.pg_storage = pg_storage

        self.start_domain = urlparse(start_url).netloc if start_url else ""
        if frontier:
            self.frontier = frontier
        elif pg_storage:
            self.frontier = Frontier(pg_storage.conn)
        else:
            raise ValueError("Postgres connection required for frontier")

        if domain_manager:
            self.domain_manager = domain_manager
            self._owns_domain_manager = False
        else:
            self.domain_manager = DomainManager(
                user_agent=settings.user_agent,
                default_delay=delay,
            )
            self._owns_domain_manager = True

        if use_browser:
            from .core import get_browser_fetcher
            self.fetcher = get_browser_fetcher()(timeout=30.0)
        else:
            self.fetcher = HttpFetcher(timeout=settings.timeout)

        self.results: list[dict] = []
        self.pages_crawled = 0
        self._running = False

    async def __aenter__(self) -> "CrawlerEngine":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """Close all resources."""
        if hasattr(self.fetcher, 'close'):
            await self.fetcher.close()
        if self._owns_domain_manager:
            await self.domain_manager.close()

    def _is_valid_url(self, url: str) -> bool:
        """Check if URL should be crawled."""
        if self.same_domain:
            return urlparse(url).netloc == self.start_domain
        return True

    async def _process_url(self, task: CrawlTask) -> dict | None:
        """Process a single URL."""
        url = task.url

        if not await self.domain_manager.is_allowed(url):
            self.frontier.mark_done(url)
            return None

        await self.domain_manager.wait_for_rate_limit(url)

        try:
            response = await self.fetcher.fetch(url)

            result = {
                "url": response.url,
                "status": response.status,
                "content_length": len(response.content),
                "depth": task.depth,
                "source_url": task.source_url,
                "timestamp": time.time(),
                "content": response.text,
            }

            # Extract and queue new links (dedup handled by frontier ON CONFLICT)
            outlinks = []
            if task.depth < self.max_depth:
                links = extract_links(response.text, response.url)
                outlinks = links
                new_tasks = [
                    CrawlTask(url=link, depth=task.depth + 1, source_url=url)
                    for link in links
                    if self._is_valid_url(link)
                ]
                self.frontier.add_many(new_tasks)

            result["outlinks"] = outlinks
            self.frontier.mark_done(url)
            return result

        except httpx.TimeoutException:
            self.domain_manager.record_error(url)
            self.frontier.mark_failed(url)
            return {"url": url, "error": "timeout", "retryable": True, "depth": task.depth}

        except httpx.ConnectError:
            self.domain_manager.record_error(url)
            self.frontier.mark_failed(url)
            return {"url": url, "error": "connection_error", "retryable": True, "depth": task.depth}

        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            if 400 <= status_code < 500:
                self.frontier.mark_done(url)
            else:
                self.domain_manager.record_error(url)
                self.frontier.mark_failed(url)
            return {"url": url, "error": f"http_{status_code}", "retryable": status_code >= 500, "depth": task.depth}

        except Exception as e:
            self.domain_manager.record_error(url)
            if self.domain_manager.should_retry(url):
                self.frontier.mark_failed(url)
            else:
                self.frontier.mark_done(url)
            return {"url": url, "error": str(e), "retryable": False, "depth": task.depth}

    async def _worker(self, worker_id: int):
        """Worker coroutine that processes URLs from the frontier."""
        idle_ticks = 0

        while self._running:
            if self.pages_crawled >= self.max_pages:
                break

            task = self.frontier.get_next()
            if not task:
                idle_ticks += 1
                if idle_ticks >= _WORKER_PATIENCE:
                    break
                await asyncio.sleep(0.1)
                continue

            idle_ticks = 0
            result = await self._process_url(task)
            if not result:
                continue

            if result.get("error"):
                logger.warning("Failed %s: %s", result["url"], result["error"])
            else:
                if self.pg_storage:
                    self.pg_storage.save(result)
                if self.output_writer:
                    self.output_writer.write_one(result)
                elif not self.pg_storage:
                    self.results.append(result)
                self.pages_crawled += 1
                logger.info("[%d/%d] %s", self.pages_crawled, self.max_pages, result["url"])

    async def crawl(self) -> list[dict]:
        """Run the crawler and return results."""
        self._running = True
        self.pages_crawled = 0

        if self.start_url and self.frontier.pending_count() == 0:
            self.frontier.add(CrawlTask(url=self.start_url, depth=0))

        # Start workers
        workers = [
            asyncio.create_task(self._worker(i))
            for i in range(self.concurrency)
        ]

        await asyncio.gather(*workers)
        self._running = False

        return self.results

    def stop(self):
        """Stop the crawler."""
        self._running = False


async def run_crawl(
    start_url: str,
    max_pages: int = 100,
    max_depth: int = 3,
    same_domain: bool = True,
    output_file: str | None = None,
    use_browser: bool = False,
    delay: float = 1.0,
    concurrency: int = 5,
    include_content: bool = True,
    postgres_dsn: str | None = None,
):
    """Run a crawl and save results."""
    if not postgres_dsn:
        raise ValueError("--postgres is required")

    typer.echo(f"Starting crawl from {start_url}")
    typer.echo(f"Max pages: {max_pages}, Max depth: {max_depth}, Concurrency: {concurrency}")

    start_time = time.time()
    from .storage import PgStorage

    with PgStorage(postgres_dsn) as pg_storage:
        writer = StreamingOutputWriter(output_file, include_content=include_content) if output_file else None
        try:
            if writer:
                writer.__enter__()
            async with CrawlerEngine(
                start_url=start_url,
                max_pages=max_pages,
                max_depth=max_depth,
                same_domain=same_domain,
                use_browser=use_browser,
                delay=delay,
                concurrency=concurrency,
                output_writer=writer,
                pg_storage=pg_storage,
            ) as engine:
                await engine.crawl()
                elapsed = time.time() - start_time
                typer.echo(f"\nCrawl complete: {engine.pages_crawled} pages in {elapsed:.1f}s")
                typer.echo(f"Postgres: {pg_storage.count} pages saved")
                if output_file:
                    typer.echo(f"Results saved to {output_file}")
                typer.echo(f"Queue stats: {engine.frontier.stats()}")
        finally:
            if writer:
                writer.__exit__(None, None, None)
