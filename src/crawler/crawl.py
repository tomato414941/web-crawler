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
from .discovery import rank_discovered_url, rank_seed_url, seed_hosts_from_urls
from .domain_manager import DomainManager
from .domain_store import DomainStore
from .frontier import CrawlTask, Frontier
from .output import StreamingOutputWriter
from .result import CrawlFailure, CrawlResult
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
        domain_store: DomainStore | None = None,
        seed_urls: list[str] | None = None,
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
        self.seed_hosts = seed_hosts_from_urls(seed_urls or [])
        if self.start_domain:
            self.seed_hosts.add(self.start_domain.lower())
        if frontier:
            self.frontier = frontier
        elif pg_storage:
            self.frontier = Frontier(pg_storage.conn)
        else:
            raise ValueError("Postgres connection required for frontier")

        if domain_store is None and pg_storage is not None:
            domain_store = DomainStore(pg_storage.conn, default_delay=delay)
        self.domain_store = domain_store
        if self.domain_store is not None:
            self.frontier.attach_domain_store(self.domain_store)

        if domain_manager:
            self.domain_manager = domain_manager
            self._owns_domain_manager = False
            if hasattr(self.domain_manager, "attach_store"):
                self.domain_manager.attach_store(self.domain_store)
        else:
            self.domain_manager = DomainManager(
                user_agent=settings.user_agent,
                default_delay=delay,
                domain_store=self.domain_store,
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
        self._claimed_pages = 0
        self._page_lock = asyncio.Lock()

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

    def _build_seed_task(self, url: str) -> CrawlTask:
        """Build the initial frontier task for an explicit seed URL."""
        decision = rank_seed_url(url)
        return CrawlTask(
            url=url,
            depth=0,
            priority=decision.priority,
            discovery_kind=decision.discovery_kind,
        )

    def _build_discovered_tasks(self, parent_url: str, links: list[str], depth: int) -> list[CrawlTask]:
        """Assign ranking metadata to discovered outlinks before enqueueing."""
        tasks: list[CrawlTask] = []
        for link in links:
            if not self._is_valid_url(link):
                continue
            decision = rank_discovered_url(
                parent_url=parent_url,
                url=link,
                seed_hosts=self.seed_hosts,
            )
            tasks.append(
                CrawlTask(
                    url=link,
                    depth=depth,
                    priority=decision.priority,
                    discovery_kind=decision.discovery_kind,
                    source_url=parent_url,
                )
            )
        return tasks

    async def _process_url(self, task: CrawlTask) -> CrawlResult | CrawlFailure | None:
        """Process a single URL."""
        url = task.url

        if not await self.domain_manager.is_allowed(url):
            self.frontier.mark_done(url, lease_token=task.lease_token)
            return None

        await self.domain_manager.wait_for_rate_limit(url)

        try:
            response = await self.fetcher.fetch(url)

            if response.status >= 400:
                if 400 <= response.status < 500:
                    self.domain_manager.record_success(url)
                    self.frontier.mark_done(url, lease_token=task.lease_token)
                else:
                    self.domain_manager.record_error(url)
                    self.frontier.mark_failed(
                        url,
                        retryable=True,
                        error=f"http_{response.status}",
                        lease_token=task.lease_token,
                    )
                return CrawlFailure(
                    url=response.url,
                    error=f"http_{response.status}",
                    retryable=response.status >= 500,
                    depth=task.depth,
                )

            result = CrawlResult(
                url=response.url,
                status=response.status,
                content_length=len(response.content),
                depth=task.depth,
                source_url=task.source_url,
                timestamp=time.time(),
                content=response.text,
                outlinks=[],
            )

            # Extract and queue new links (dedup handled by frontier ON CONFLICT)
            if task.depth < self.max_depth:
                links = extract_links(response.text, response.url)
                result.outlinks = links
                new_tasks = self._build_discovered_tasks(url, links, task.depth + 1)
                self.frontier.add_many(new_tasks)

            self.domain_manager.record_success(url)
            self.frontier.mark_done(url, lease_token=task.lease_token)
            return result

        except httpx.TimeoutException:
            self.domain_manager.record_error(url)
            self.frontier.mark_failed(
                url,
                retryable=True,
                error="timeout",
                lease_token=task.lease_token,
            )
            return CrawlFailure(url=url, error="timeout", retryable=True, depth=task.depth)

        except httpx.ConnectError:
            self.domain_manager.record_error(url)
            self.frontier.mark_failed(
                url,
                retryable=True,
                error="connection_error",
                lease_token=task.lease_token,
            )
            return CrawlFailure(url=url, error="connection_error", retryable=True, depth=task.depth)

        except Exception as e:
            self.domain_manager.record_error(url)
            if self.domain_manager.should_retry(url):
                self.frontier.mark_failed(
                    url,
                    retryable=True,
                    error=str(e),
                    lease_token=task.lease_token,
                )
            else:
                self.frontier.mark_failed(
                    url,
                    retryable=False,
                    error=str(e),
                    lease_token=task.lease_token,
                )
            return CrawlFailure(url=url, error=str(e), retryable=False, depth=task.depth)

    async def _claim_page_slot(self) -> bool:
        """Reserve capacity so concurrent workers do not exceed max_pages."""
        async with self._page_lock:
            if self.pages_crawled + self._claimed_pages >= self.max_pages:
                return False
            self._claimed_pages += 1
            return True

    async def _release_page_slot(self, success: bool):
        """Release a reserved page slot and commit successful crawls."""
        async with self._page_lock:
            self._claimed_pages -= 1
            if success:
                self.pages_crawled += 1

    async def _worker(self, worker_id: int):
        """Worker coroutine that processes URLs from the frontier."""
        idle_ticks = 0

        while self._running:
            if not await self._claim_page_slot():
                break

            task = self.frontier.lease_next()
            if not task:
                await self._release_page_slot(success=False)
                idle_ticks += 1
                if idle_ticks >= _WORKER_PATIENCE:
                    break
                await asyncio.sleep(0.1)
                continue

            idle_ticks = 0
            result = await self._process_url(task)
            if not result:
                await self._release_page_slot(success=False)
                continue

            if isinstance(result, CrawlFailure):
                await self._release_page_slot(success=False)
                logger.warning("Failed %s: %s", result.url, result.error)
            else:
                await self._release_page_slot(success=True)
                if self.pg_storage:
                    self.pg_storage.save(result)
                if self.output_writer:
                    self.output_writer.write_one(result)
                elif not self.pg_storage:
                    self.results.append(result.to_dict())
                logger.info("[%d/%d] %s", self.pages_crawled, self.max_pages, result.url)

    async def crawl(self) -> list[dict]:
        """Run the crawler and return results."""
        self._running = True
        self.pages_crawled = 0
        self._claimed_pages = 0

        if self.start_url and self.frontier.pending_count() == 0:
            self.frontier.add(self._build_seed_task(self.start_url))

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
                seed_urls=[start_url],
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
