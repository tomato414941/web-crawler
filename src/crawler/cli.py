"""CLI interface using typer."""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Mapping

import typer

from .config import settings
from .core import HttpFetcher
from .result import ExtractResult, FetchResult, LinkCheckResult, result_to_dict

app = typer.Typer(
    name="crawler",
    help="Async web crawler with browser support",
    no_args_is_help=True,
)


async def _fetch(url: str, use_browser: bool = False, auto: bool = False) -> FetchResult:
    """Fetch a URL and return structured result."""
    used_browser = False

    if auto:
        from .core import get_adaptive_fetcher

        AdaptiveFetcher = get_adaptive_fetcher()
        fetcher = AdaptiveFetcher(
            timeout=settings.timeout,
            browser_timeout=30.0,
            user_agent=settings.user_agent,
        )
    elif use_browser:
        from .core import get_browser_fetcher

        BrowserFetcher = get_browser_fetcher()
        fetcher = BrowserFetcher(timeout=30.0, user_agent=settings.user_agent)
        used_browser = True
    else:
        fetcher = HttpFetcher(
            timeout=settings.timeout,
            user_agent=settings.user_agent,
            max_connections=settings.max_connections,
            max_keepalive_connections=settings.max_keepalive_connections,
        )
    try:
        if auto:
            response, used_browser = await fetcher.fetch(url)
        else:
            response = await fetcher.fetch(url)
        return FetchResult(
            url=response.url,
            status=response.status,
            content_length=response.content_length
            if response.content_length is not None
            else len(response.content),
            headers=response.headers,
            content=response.text,
            used_browser=used_browser,
        )
    finally:
        await fetcher.close()


def _write_json_output(output_path: str, result: object | Mapping[str, Any]):
    """Write structured output as JSON."""
    path = Path(output_path)
    with path.open("w", encoding="utf-8") as f:
        json.dump(result_to_dict(result), f, indent=2, ensure_ascii=False)
    typer.echo(f"Saved to {output_path}")


def _echo_fetch_result(result: FetchResult):
    """Render fetch results for terminal output."""
    typer.echo(f"URL: {result.url}")
    typer.echo(f"Status: {result.status}")
    typer.echo(f"Content-Length: {result.content_length}")
    if result.used_browser:
        typer.echo("Renderer: Browser (Playwright)")
    typer.echo("---")
    typer.echo(result.content[:2000])
    if len(result.content) > 2000:
        typer.echo(f"\n... (truncated, {len(result.content)} chars total)")


def _echo_link_check_result(result: LinkCheckResult):
    """Render link-check summary for terminal output."""
    typer.echo(f"Checked {result.total_links} links")
    typer.echo(f"  OK: {result.ok}")
    typer.echo(f"  Broken: {result.broken}")
    typer.echo(f"  Redirects: {result.redirects}")
    if result.broken_links:
        typer.echo("\nBroken links:")
        for link in result.broken_links:
            typer.echo(f"  {link.status} {link.url}")
            if link.source:
                typer.echo(f"    Found on: {link.source}")


def _echo_extract_result(result: ExtractResult):
    """Render extraction results for terminal output."""
    for i, item in enumerate(result.items, 1):
        typer.echo(f"{i}. {item}")


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
        _write_json_output(output, result)
    elif quiet:
        sys.stdout.write(result.content)
    else:
        _echo_fetch_result(result)


@app.command()
def crawl(
    start_url: str = typer.Argument(..., help="Starting URL for crawl"),
    max_pages: int = typer.Option(100, "--max-pages", "-n", help="Maximum pages to crawl"),
    same_host: bool = typer.Option(
        True,
        "--same-host/--any-host",
        help="Keep this one-shot crawl on the start host",
    ),
    output: str = typer.Option(None, "-o", "--output", help="Output file path (JSONL)"),
    js: bool = typer.Option(False, "--js", help="Use browser for all pages"),
    delay: float = typer.Option(1.0, "--delay", help="Delay between requests (seconds)"),
    concurrency: int = typer.Option(5, "--concurrency", "-c", help="Concurrent requests"),
    no_content: bool = typer.Option(
        False, "--no-content", help="Save metadata only, exclude page content"
    ),
    postgres: str = typer.Option(
        None, "--postgres", envvar="CRAWLER_POSTGRES_DSN", help="Postgres DSN for storing results"
    ),
):
    """Run a bounded one-shot crawl starting from a URL."""
    from .crawl import run_crawl

    asyncio.run(
        run_crawl(
            start_url=start_url,
            max_pages=max_pages,
            same_host=same_host,
            output_file=output,
            use_browser=js,
            delay=delay,
            concurrency=concurrency,
            include_content=not no_content,
            postgres_dsn=postgres,
        )
    )


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

    result = asyncio.run(
        check_page_links(
            url=url,
            recursive=recursive,
            max_depth=max_depth,
            check_external=external,
            progress=typer.echo,
        )
    )

    if output:
        _write_json_output(output, result)
    else:
        _echo_link_check_result(result)


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

    result = asyncio.run(
        extract_data(
            url=url,
            css_selector=selector,
            xpath=xpath,
            attribute=attr,
            use_browser=js,
        )
    )

    if output:
        _write_json_output(output, result)
    else:
        _echo_extract_result(result)


@app.command()
def serve(
    host: str = typer.Option("0.0.0.0", "--host", help="Bind host"),
    port: int = typer.Option(8080, "--port", "-p", help="Bind port"),
    postgres: str = typer.Option(
        None, "--postgres", envvar="CRAWLER_POSTGRES_DSN", help="Postgres DSN"
    ),
):
    """Start the API server to serve crawl results."""
    import uvicorn

    if postgres:
        os.environ["CRAWLER_POSTGRES_DSN"] = postgres

    if not os.environ.get("CRAWLER_POSTGRES_DSN"):
        typer.echo("Error: --postgres or CRAWLER_POSTGRES_DSN is required", err=True)
        raise typer.Exit(1)

    typer.echo(f"Starting API server on {host}:{port}")
    uvicorn.run("crawler.api:app", host=host, port=port)


@app.command()
def migrate(
    postgres: str = typer.Option(
        None, "--postgres", envvar="CRAWLER_POSTGRES_DSN", help="Postgres DSN"
    ),
):
    """Apply pending database migrations."""
    if not postgres:
        typer.echo("Error: --postgres or CRAWLER_POSTGRES_DSN is required", err=True)
        raise typer.Exit(1)

    from .migrate import apply_migrations

    applied = apply_migrations(postgres)
    if applied:
        typer.echo("Applied migrations:")
        for version in applied:
            typer.echo(f"  {version}")
    else:
        typer.echo("No pending migrations")


@app.command()
def observe(
    postgres: str = typer.Option(
        None, "--postgres", envvar="CRAWLER_POSTGRES_DSN", help="Postgres DSN"
    ),
    json_output: bool = typer.Option(False, "--json", help="Output raw observation JSON"),
    scheduler_invariants: bool = typer.Option(
        False,
        "--scheduler-invariants",
        help="Include live scheduler invariant checks; may be expensive",
    ),
):
    """Print a read-only production observation snapshot."""
    if not postgres:
        typer.echo("Error: --postgres or CRAWLER_POSTGRES_DSN is required", err=True)
        raise typer.Exit(1)

    from .observation import (
        format_operator_observation,
        read_operator_observation,
        serialize_operator_observation,
    )
    from .storage import PgStorage

    with PgStorage(postgres) as storage:
        observation = read_operator_observation(
            storage,
            include_scheduler_invariants=scheduler_invariants,
        )

    if json_output:
        typer.echo(serialize_operator_observation(observation))
    else:
        typer.echo(format_operator_observation(observation))


@app.command("scheduler-check")
def scheduler_check(
    postgres: str = typer.Option(
        None, "--postgres", envvar="CRAWLER_POSTGRES_DSN", help="Postgres DSN"
    ),
    json_output: bool = typer.Option(False, "--json", help="Output raw invariant JSON"),
    sample_limit: int = typer.Option(5, "--sample-limit", help="Sample URLs per violation type"),
    repair_terminal: bool = typer.Option(
        False,
        "--repair-terminal",
        help="Remove terminal URLs from live scheduler membership tables",
    ),
    repair_host_heads: int = typer.Option(
        0,
        "--repair-host-heads",
        help="Repair a bounded number of stale or orphan host-head read-model rows",
    ),
):
    """Run read-only scheduler invariant checks."""
    if not postgres:
        typer.echo("Error: --postgres or CRAWLER_POSTGRES_DSN is required", err=True)
        raise typer.Exit(1)
    if sample_limit < 0:
        typer.echo("Error: --sample-limit must be 0 or greater", err=True)
        raise typer.Exit(1)
    if repair_host_heads < 0:
        typer.echo("Error: --repair-host-heads must be 0 or greater", err=True)
        raise typer.Exit(1)

    from .scheduler_invariants import SchedulerInvariantChecker
    from .storage import PgStorage
    from .scheduler import Scheduler

    with PgStorage(postgres) as storage:
        checker = SchedulerInvariantChecker(storage.conn)
        repair_report = None
        host_head_repair_report = None
        if repair_terminal:
            repair_report = checker.repair_terminal_memberships().to_dict()
        if repair_host_heads:
            host_head_repair_report = (
                Scheduler(storage.conn)
                .repair_host_runnable_heads(limit=repair_host_heads)
                .as_dict()
            )
        report = checker.check(sample_limit=sample_limit).to_dict()

    if json_output:
        output = {"invariants": report}
        if repair_report is not None:
            output["repair_terminal"] = repair_report
        if host_head_repair_report is not None:
            output["repair_host_heads"] = host_head_repair_report
        typer.echo(
            json.dumps(
                output
                if repair_report is not None or host_head_repair_report is not None
                else report,
                indent=2,
                ensure_ascii=False,
            )
        )
        return

    if repair_report is not None:
        typer.echo("Scheduler Terminal Repair")
        typer.echo(
            f"  deleted_total={repair_report['deleted_total']} "
            f"queues={repair_report['deleted_queue_rows']} "
            f"blocked={repair_report['deleted_blocked_rows']} "
            f"leases={repair_report['deleted_leases']} "
            f"heads={repair_report['deleted_host_heads']} "
            f"dirty={repair_report['deleted_dirty_hosts']}"
        )
    if host_head_repair_report is not None:
        typer.echo("Scheduler Host-Head Repair")
        typer.echo(
            f"  checked={host_head_repair_report['checked_heads']} "
            f"orphan={host_head_repair_report['orphan_heads']} "
            f"stale={host_head_repair_report['stale_heads']} "
            f"missing={host_head_repair_report['missing_heads']} "
            f"repaired={host_head_repair_report['repaired_hosts']}"
        )

    typer.echo("Scheduler Invariants")
    typer.echo(f"  ok={str(bool(report['ok'])).lower()} violations={report['violations_total']}")
    typer.echo(
        "  "
        f"duplicates={report['duplicate_memberships']} "
        f"terminal={report['terminal_in_live_queue']} "
        f"expired_leases={report['expired_leases']} "
        f"orphan_heads={report['orphan_host_heads']} "
        f"head_mismatches={report['host_head_mismatches']}"
    )
    typer.echo(
        "  "
        f"url_hash_missing={report['url_hash_missing']} "
        f"url_hash_mismatches={report['url_hash_mismatches']} "
        f"url_length_mismatches={report['url_length_mismatches']} "
        f"url_hash_duplicates={report['url_hash_duplicates']} "
        f"url_too_long={report['url_too_long']}"
    )


@app.command("observe-watch")
def observe_watch(
    postgres: str = typer.Option(
        None, "--postgres", envvar="CRAWLER_POSTGRES_DSN", help="Postgres DSN"
    ),
    output: Path = typer.Option(..., "--output", "-o", help="JSONL output file"),
    interval: float = typer.Option(300.0, "--interval", help="Seconds between observations"),
    limit: int | None = typer.Option(None, "--limit", help="Stop after N observations"),
    max_bytes: int = typer.Option(
        10_485_760,
        "--max-bytes",
        help="Rotate output when it reaches this size; 0 disables rotation",
    ),
    max_files: int = typer.Option(7, "--max-files", help="Number of rotated files to keep"),
    max_failures: int = typer.Option(
        5,
        "--max-failures",
        help="Exit after N consecutive observation failures; 0 disables failure exit",
    ),
):
    """Append read-only production observations to a JSON Lines file."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if not postgres:
        typer.echo("Error: --postgres or CRAWLER_POSTGRES_DSN is required", err=True)
        raise typer.Exit(1)
    if interval <= 0:
        typer.echo("Error: --interval must be greater than 0", err=True)
        raise typer.Exit(1)
    if limit is not None and limit <= 0:
        typer.echo("Error: --limit must be greater than 0", err=True)
        raise typer.Exit(1)
    if max_bytes < 0:
        typer.echo("Error: --max-bytes must be 0 or greater", err=True)
        raise typer.Exit(1)
    if max_files <= 0:
        typer.echo("Error: --max-files must be greater than 0", err=True)
        raise typer.Exit(1)
    if max_failures < 0:
        typer.echo("Error: --max-failures must be 0 or greater", err=True)
        raise typer.Exit(1)

    from .observation import (
        ObservationWatchConfig,
        ObservationWatchFailed,
        ObservationWatcher,
    )
    from .storage import PgStorage

    watcher = ObservationWatcher(
        storage_factory=lambda: PgStorage(postgres),
        config=ObservationWatchConfig(
            output=output,
            interval=interval,
            limit=limit,
            max_bytes=max_bytes,
            max_files=max_files,
            max_failures=max_failures,
        ),
    )
    try:
        watcher.run()
    except ObservationWatchFailed:
        raise typer.Exit(1) from None


@app.command()
def daemon(
    seeds: list[str] = typer.Argument(..., help="Seed URLs to crawl"),
    cycle_pages: int = typer.Option(500, "--cycle-pages", help="Pages per cycle"),
    refresh_ttl: int = typer.Option(
        86400, "--refresh-ttl", help="Refresh pages older than N seconds"
    ),
    concurrency: int = typer.Option(5, "--concurrency", "-c", help="Concurrent requests"),
    delay: float = typer.Option(1.0, "--delay", help="Delay between requests (seconds)"),
    cycle_pause: float = typer.Option(5.0, "--cycle-pause", help="Pause between cycles (seconds)"),
    idle_sleep: float = typer.Option(
        60.0, "--idle-sleep", help="Sleep when no URLs to crawl (seconds)"
    ),
    postgres: str = typer.Option(
        None, "--postgres", envvar="CRAWLER_POSTGRES_DSN", help="Postgres DSN"
    ),
):
    """Run crawler as a continuous daemon."""
    if not postgres:
        typer.echo("Error: --postgres or CRAWLER_POSTGRES_DSN is required", err=True)
        raise typer.Exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    from .daemon import CrawlDaemon

    d = CrawlDaemon(
        seeds=seeds,
        postgres_dsn=postgres,
        cycle_pages=cycle_pages,
        refresh_ttl=refresh_ttl,
        concurrency=concurrency,
        delay=delay,
        cycle_pause=cycle_pause,
        idle_sleep=idle_sleep,
    )
    asyncio.run(d.run())


@app.command()
def version():
    """Show version."""
    from . import __version__

    typer.echo(f"web-crawler {__version__}")


if __name__ == "__main__":
    app()
