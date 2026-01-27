"""CLI interface using typer."""

import asyncio
import json
import sys

import typer

from .config import settings
from .core import HttpFetcher

app = typer.Typer(
    name="crawler",
    help="Async web crawler with browser support",
    no_args_is_help=True,
)


async def _fetch(url: str, use_browser: bool = False, auto: bool = False) -> dict:
    """Fetch a URL and return result as dict."""
    used_browser = False

    if auto:
        from .core import get_adaptive_fetcher
        AdaptiveFetcher = get_adaptive_fetcher()
        fetcher = AdaptiveFetcher(
            timeout=settings.timeout,
            browser_timeout=30.0,
            user_agent=settings.user_agent,
        )
        response, used_browser = await fetcher.fetch(url)
    elif use_browser:
        from .core import get_browser_fetcher
        BrowserFetcher = get_browser_fetcher()
        fetcher = BrowserFetcher(timeout=30.0, user_agent=settings.user_agent)
        response = await fetcher.fetch(url)
        used_browser = True
    else:
        fetcher = HttpFetcher(
            timeout=settings.timeout,
            user_agent=settings.user_agent,
            max_connections=settings.max_connections,
            max_keepalive_connections=settings.max_keepalive_connections,
        )
        response = await fetcher.fetch(url)

    return {
        "url": response.url,
        "status": response.status,
        "content_length": len(response.content),
        "headers": response.headers,
        "content": response.text,
        "used_browser": used_browser,
    }


@app.command()
def fetch(
    url: str = typer.Argument(..., help="URL to fetch"),
    output: str = typer.Option(None, "-o", "--output", help="Output file (JSON)"),
    js: bool = typer.Option(False, "--js", help="Use browser for JavaScript rendering"),
    auto: bool = typer.Option(False, "--auto", help="Auto-detect if browser is needed"),
    quiet: bool = typer.Option(False, "-q", "--quiet", help="Only output content"),
):
    """Fetch a single URL."""
    result = asyncio.run(_fetch(url, use_browser=js, auto=auto))

    if output:
        with open(output, "w") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        typer.echo(f"Saved to {output}")
    elif quiet:
        sys.stdout.write(result["content"])
    else:
        typer.echo(f"URL: {result['url']}")
        typer.echo(f"Status: {result['status']}")
        typer.echo(f"Content-Length: {result['content_length']}")
        if result.get("used_browser"):
            typer.echo("Renderer: Browser (Playwright)")
        typer.echo("---")
        typer.echo(result["content"][:2000])
        if len(result["content"]) > 2000:
            typer.echo(f"\n... (truncated, {len(result['content'])} chars total)")


@app.command()
def crawl(
    start_url: str = typer.Argument(..., help="Starting URL for crawl"),
    max_pages: int = typer.Option(100, "--max-pages", "-n", help="Maximum pages to crawl"),
    max_depth: int = typer.Option(3, "--max-depth", "-d", help="Maximum link depth"),
    same_domain: bool = typer.Option(True, "--same-domain/--any-domain", help="Stay on same domain"),
    output: str = typer.Option("crawl_results", "-o", "--output", help="Output directory"),
    output_format: str = typer.Option("jsonl", "--format", "-f", help="Output format: jsonl, sqlite, warc"),
    js: bool = typer.Option(False, "--js", help="Use browser for all pages"),
    delay: float = typer.Option(1.0, "--delay", help="Delay between requests (seconds)"),
    concurrency: int = typer.Option(5, "--concurrency", "-c", help="Concurrent requests"),
):
    """Crawl a website starting from a URL."""
    from .crawl import run_crawl

    asyncio.run(run_crawl(
        start_url=start_url,
        max_pages=max_pages,
        max_depth=max_depth,
        same_domain=same_domain,
        output_dir=output,
        output_format=output_format,
        use_browser=js,
        delay=delay,
        concurrency=concurrency,
    ))


@app.command("check-links")
def check_links(
    url: str = typer.Argument(..., help="URL to check links for"),
    recursive: bool = typer.Option(False, "-r", "--recursive", help="Check links recursively"),
    max_depth: int = typer.Option(1, "--max-depth", "-d", help="Maximum depth for recursive check"),
    external: bool = typer.Option(True, "--external/--internal-only", help="Check external links"),
    output: str = typer.Option(None, "-o", "--output", help="Output file (JSON)"),
):
    """Check for broken links on a page."""
    from .links import check_page_links

    result = asyncio.run(check_page_links(
        url=url,
        recursive=recursive,
        max_depth=max_depth,
        check_external=external,
    ))

    if output:
        with open(output, "w") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        typer.echo(f"Saved to {output}")
    else:
        typer.echo(f"Checked {result['total_links']} links")
        typer.echo(f"  OK: {result['ok']}")
        typer.echo(f"  Broken: {result['broken']}")
        typer.echo(f"  Redirects: {result['redirects']}")
        if result['broken_links']:
            typer.echo("\nBroken links:")
            for link in result['broken_links']:
                typer.echo(f"  {link['status']} {link['url']}")
                typer.echo(f"    Found on: {link['source']}")


@app.command()
def extract(
    url: str = typer.Argument(..., help="URL to extract data from"),
    selector: str = typer.Option(None, "-s", "--selector", help="CSS selector"),
    xpath: str = typer.Option(None, "-x", "--xpath", help="XPath expression"),
    attr: str = typer.Option(None, "-a", "--attr", help="Extract attribute instead of text"),
    js: bool = typer.Option(False, "--js", help="Use browser for JavaScript rendering"),
    output: str = typer.Option(None, "-o", "--output", help="Output file"),
):
    """Extract data from a page using CSS selectors or XPath."""
    from .extract import extract_data

    result = asyncio.run(extract_data(
        url=url,
        css_selector=selector,
        xpath=xpath,
        attribute=attr,
        use_browser=js,
    ))

    if output:
        with open(output, "w") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        typer.echo(f"Saved to {output}")
    else:
        for i, item in enumerate(result['items'], 1):
            typer.echo(f"{i}. {item}")


@app.command()
def agent(
    url: str = typer.Argument(..., help="Starting URL"),
    task: str = typer.Option(..., "-t", "--task", help="Task description for the AI agent"),
    max_steps: int = typer.Option(10, "--max-steps", help="Maximum steps before stopping"),
    model: str = typer.Option("claude-sonnet-4-20250514", "--model", "-m", help="Claude model to use"),
    headless: bool = typer.Option(True, "--headless/--headed", help="Run browser headlessly"),
    verbose: bool = typer.Option(False, "-v", "--verbose", help="Show detailed output"),
):
    """Run an AI agent to perform tasks on web pages."""
    from .agent import run_agent

    result = asyncio.run(run_agent(
        start_url=url,
        task=task,
        max_steps=max_steps,
        model=model,
        headless=headless,
        verbose=verbose,
    ))

    typer.echo(f"\nAgent completed in {result['steps']} steps")
    typer.echo(f"Status: {result['status']}")
    if result.get('result'):
        typer.echo(f"Result: {result['result']}")


@app.command()
def version():
    """Show version."""
    from . import __version__

    typer.echo(f"web-crawler {__version__}")


if __name__ == "__main__":
    app()
