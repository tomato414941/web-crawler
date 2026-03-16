"""Crawler engine with async concurrency."""

import asyncio
import re
import time
from urllib.parse import urljoin, urlparse

import httpx
import typer

from .config import settings
from .core import HttpFetcher
from .domain_manager import DomainManager
from .frontier import CrawlTask, Frontier
from .output import StreamingOutputWriter
from .urls import normalize_url


def extract_links(html: str, base_url: str) -> list[str]:
    """Extract links from HTML content."""
    links = []

    for match in _HREF_PATTERN.finditer(html):
        href = match.group(1).strip()

        # Skip non-HTTP schemes and fragments
        if href.startswith(('#', 'javascript:', 'mailto:', 'tel:', 'data:')):
            continue

        # Handle protocol-relative URLs
        if href.startswith('//'):
            href = 'https:' + href

        absolute_url = urljoin(base_url, href)

        # Only keep http(s) URLs
        if absolute_url.startswith(('http://', 'https://')):
            normalized = normalize_url(absolute_url)
            links.append(normalized)

    return list(set(links))


class CrawlerEngine:
    """Async crawler engine with concurrent processing."""

    def __init__(
        self,
        start_url: str,
        max_pages: int = 100,
        max_depth: int = 3,
        same_domain: bool = True,
        use_browser: bool = False,
        delay: float = 1.0,
        concurrency: int = 5,
        output_writer: StreamingOutputWriter | None = None,
        pg_storage: "PgStorage | None" = None,
    ):
        self.start_url = start_url
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.same_domain = same_domain
        self.use_browser = use_browser
        self.concurrency = concurrency
        self.output_writer = output_writer
        self.pg_storage = pg_storage

        self.start_domain = urlparse(start_url).netloc
        if pg_storage:
            self.frontier = Frontier(pg_storage._conn)
        else:
            raise ValueError("Postgres connection required for frontier")
        self.domain_manager = DomainManager(
            user_agent=settings.user_agent,
            default_delay=delay,
        )

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

            # Extract and queue new links
            outlinks = []
            if task.depth < self.max_depth:
                links = extract_links(response.text, response.url)
                outlinks = links
                new_tasks = []
                for link in links:
                    if self._is_valid_url(link) and not self.frontier.is_seen(link):
                        new_tasks.append(CrawlTask(
                            url=link,
                            depth=task.depth + 1,
                            source_url=url,
                        ))
                self.frontier.add_many(new_tasks)

            result["outlinks"] = outlinks
            self.frontier.mark_done(url)
            return result

        except httpx.TimeoutException:
            # Timeout: retryable
            self.domain_manager.record_error(url)
            self.frontier.mark_failed(url)
            return {"url": url, "error": "timeout", "retryable": True, "depth": task.depth}

        except httpx.ConnectError:
            # Connection error: retryable
            self.domain_manager.record_error(url)
            self.frontier.mark_failed(url)
            return {"url": url, "error": "connection_error", "retryable": True, "depth": task.depth}

        except httpx.HTTPStatusError as e:
            # HTTP error: 4xx is permanent, 5xx is retryable
            status_code = e.response.status_code
            if 400 <= status_code < 500:
                self.frontier.mark_done(url)  # Permanent error
            else:
                self.domain_manager.record_error(url)
                self.frontier.mark_failed(url)  # Server error, retryable
            return {"url": url, "error": f"http_{status_code}", "retryable": status_code >= 500, "depth": task.depth}

        except Exception as e:
            # Unknown error: use retry logic
            self.domain_manager.record_error(url)
            if self.domain_manager.should_retry(url):
                self.frontier.mark_failed(url)
            else:
                self.frontier.mark_done(url)
            return {"url": url, "error": str(e), "retryable": False, "depth": task.depth}

    async def _worker(self, worker_id: int):
        """Worker coroutine that processes URLs from the frontier."""
        while self._running:
            if self.pages_crawled >= self.max_pages:
                break

            task = self.frontier.get_next()
            if not task:
                await asyncio.sleep(0.1)
                if self.frontier.pending_count() == 0:
                    break
                continue

            result = await self._process_url(task)
            if result:
                if self.pg_storage:
                    self.pg_storage.save(result)
                if self.output_writer:
                    self.output_writer.write_one(result)
                elif not self.pg_storage:
                    self.results.append(result)
                self.pages_crawled += 1

                if not result.get("error"):
                    typer.echo(f"[{self.pages_crawled}/{self.max_pages}] {result['url']}")

    async def crawl(self) -> list[dict]:
        """Run the crawler and return results."""
        self._running = True

        # Seed the frontier
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
    output_dir: str | None = "crawl_results",
    output_file: str | None = None,
    output_format: str = "jsonl",
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

    pg_storage = PgStorage(postgres_dsn)

    try:
        writer = None
        if output_file:
            writer = StreamingOutputWriter(output_file, include_content=include_content)
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
            stats = engine.frontier.stats()
            typer.echo(f"Queue stats: {stats}")
    finally:
        if writer:
            writer.__exit__(None, None, None)
        pg_storage.close()
