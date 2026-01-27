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


async def _fetch(url: str) -> dict:
    """Fetch a URL and return result as dict."""
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
    }


@app.command()
def fetch(
    url: str = typer.Argument(..., help="URL to fetch"),
    output: str = typer.Option(None, "-o", "--output", help="Output file (JSON)"),
    js: bool = typer.Option(False, "--js", help="Use browser for JavaScript rendering (Phase 2)"),
    quiet: bool = typer.Option(False, "-q", "--quiet", help="Only output content"),
):
    """Fetch a single URL."""
    if js:
        typer.echo("Error: --js option requires browser support (Phase 2)", err=True)
        raise typer.Exit(1)

    result = asyncio.run(_fetch(url))

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
        typer.echo("---")
        typer.echo(result["content"][:2000])
        if len(result["content"]) > 2000:
            typer.echo(f"\n... (truncated, {len(result['content'])} chars total)")


@app.command()
def version():
    """Show version."""
    from . import __version__

    typer.echo(f"web-crawler {__version__}")


if __name__ == "__main__":
    app()
