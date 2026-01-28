"""Crawler engine with async concurrency."""

import asyncio
import json
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

import httpx
import typer

from .config import settings
from .core import HttpFetcher
from .domain_manager import DomainManager
from .frontier import CrawlTask, Frontier


def normalize_url(url: str) -> str:
    """Normalize URL for deduplication (remove fragment, sort query params)."""
    parsed = urlparse(url)

    # Sort query parameters
    query_params = parse_qsl(parsed.query)
    sorted_query = urlencode(sorted(query_params))

    # Normalize path (remove trailing slash except for root)
    path = parsed.path.rstrip('/') or '/'

    normalized = urlunparse((
        parsed.scheme.lower(),
        parsed.netloc.lower(),
        path,
        parsed.params,
        sorted_query,
        ''  # Remove fragment
    ))
    return normalized


def extract_links(html: str, base_url: str) -> list[str]:
    """Extract links from HTML content."""
    links = []
    # Improved regex: handles whitespace around =
    pattern = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)

    for match in pattern.finditer(html):
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
    ):
        self.start_url = start_url
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.same_domain = same_domain
        self.use_browser = use_browser
        self.concurrency = concurrency

        self.start_domain = urlparse(start_url).netloc
        self.frontier = Frontier()
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
            if task.depth < self.max_depth:
                links = extract_links(response.text, response.url)
                new_tasks = []
                for link in links:
                    if self._is_valid_url(link) and not self.frontier.is_seen(link):
                        new_tasks.append(CrawlTask(
                            url=link,
                            depth=task.depth + 1,
                            source_url=url,
                        ))
                self.frontier.add_many(new_tasks)

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


class OutputWriter:
    """Writes crawl results to various formats."""

    def __init__(self, output_dir: str, output_format: str = "jsonl"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.format = output_format

    def write(self, results: list[dict]):
        """Write results to output."""
        if self.format == "jsonl":
            self._write_jsonl(results)
        elif self.format == "sqlite":
            self._write_sqlite(results)
        elif self.format == "warc":
            self._write_warc(results)
        else:
            self._write_jsonl(results)

    def _write_jsonl(self, results: list[dict]):
        """Write results as JSON Lines."""
        output_file = self.output_dir / "results.jsonl"
        with open(output_file, "w") as f:
            for result in results:
                f.write(json.dumps(result, ensure_ascii=False) + "\n")
        typer.echo(f"Results saved to {output_file}")

    def _write_sqlite(self, results: list[dict]):
        """Write results to SQLite database."""
        import sqlite3
        output_file = self.output_dir / "results.db"
        conn = sqlite3.connect(output_file)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY,
                url TEXT NOT NULL,
                status INTEGER,
                content_length INTEGER,
                depth INTEGER,
                source_url TEXT,
                timestamp REAL,
                content TEXT,
                error TEXT
            )
        """)

        for result in results:
            conn.execute(
                """INSERT INTO pages (url, status, content_length, depth, source_url, timestamp, content, error)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    result.get("url"),
                    result.get("status"),
                    result.get("content_length"),
                    result.get("depth"),
                    result.get("source_url"),
                    result.get("timestamp"),
                    result.get("content"),
                    result.get("error"),
                )
            )
        conn.commit()
        conn.close()
        typer.echo(f"Results saved to {output_file}")

    def _write_warc(self, results: list[dict]):
        """Write results in simplified WARC-like format."""
        output_file = self.output_dir / "results.warc"
        with open(output_file, "w") as f:
            for result in results:
                f.write("WARC/1.0\n")
                f.write("WARC-Type: response\n")
                f.write(f"WARC-Target-URI: {result.get('url', '')}\n")
                f.write(f"WARC-Date: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(result.get('timestamp', 0)))}\n")
                f.write(f"Content-Length: {result.get('content_length', 0)}\n")
                f.write("\n")
                f.write(result.get("content", ""))
                f.write("\n\n")
        typer.echo(f"Results saved to {output_file}")


async def run_crawl(
    start_url: str,
    max_pages: int = 100,
    max_depth: int = 3,
    same_domain: bool = True,
    output_dir: str = "crawl_results",
    output_format: str = "jsonl",
    use_browser: bool = False,
    delay: float = 1.0,
    concurrency: int = 5,
):
    """Run a crawl and save results."""
    typer.echo(f"Starting crawl from {start_url}")
    typer.echo(f"Max pages: {max_pages}, Max depth: {max_depth}, Concurrency: {concurrency}")

    engine = CrawlerEngine(
        start_url=start_url,
        max_pages=max_pages,
        max_depth=max_depth,
        same_domain=same_domain,
        use_browser=use_browser,
        delay=delay,
        concurrency=concurrency,
    )

    start_time = time.time()
    results = await engine.crawl()
    elapsed = time.time() - start_time

    typer.echo(f"\nCrawl complete: {len(results)} pages in {elapsed:.1f}s")

    writer = OutputWriter(output_dir, output_format)
    writer.write(results)

    # Show stats
    stats = engine.frontier.stats()
    typer.echo(f"Queue stats: {stats}")
