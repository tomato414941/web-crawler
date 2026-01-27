"""Link checking functionality."""

import asyncio
import re
from urllib.parse import urljoin, urlparse

import httpx
import typer

from .config import settings
from .core import HttpFetcher


def extract_links_from_html(html: str, base_url: str) -> list[dict]:
    """Extract all links from HTML with context."""
    links = []
    pattern = re.compile(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)

    for match in pattern.finditer(html):
        href = match.group(1)
        text = re.sub(r'<[^>]+>', '', match.group(2)).strip()

        if href.startswith(('#', 'javascript:', 'mailto:', 'tel:', 'data:')):
            continue

        absolute_url = urljoin(base_url, href)
        if absolute_url.startswith(('http://', 'https://')):
            links.append({
                "url": absolute_url,
                "text": text[:100],  # Truncate long text
                "source": base_url,
            })

    return links


async def check_url(client: httpx.AsyncClient, url: str) -> dict:
    """Check a single URL and return its status."""
    try:
        resp = await client.head(url, follow_redirects=True, timeout=10.0)
        return {
            "url": url,
            "status": resp.status_code,
            "final_url": str(resp.url),
            "ok": 200 <= resp.status_code < 400,
            "redirect": str(resp.url) != url,
        }
    except httpx.TimeoutException:
        return {"url": url, "status": 0, "error": "timeout", "ok": False}
    except httpx.RequestError as e:
        return {"url": url, "status": 0, "error": str(e), "ok": False}


async def check_page_links(
    url: str,
    recursive: bool = False,
    max_depth: int = 1,
    check_external: bool = True,
) -> dict:
    """Check all links on a page for broken links."""
    fetcher = HttpFetcher(timeout=settings.timeout)
    base_domain = urlparse(url).netloc

    checked_urls: set[str] = set()
    pages_to_check: list[tuple[str, int]] = [(url, 0)]  # (url, depth)

    all_links: list[dict] = []
    ok_links: list[dict] = []
    broken_links: list[dict] = []
    redirect_links: list[dict] = []

    async with httpx.AsyncClient(
        headers={"User-Agent": settings.user_agent},
        follow_redirects=True,
    ) as client:
        while pages_to_check:
            current_url, depth = pages_to_check.pop(0)

            if current_url in checked_urls:
                continue
            checked_urls.add(current_url)

            typer.echo(f"Checking page: {current_url}")

            try:
                response = await fetcher.fetch(current_url)
            except Exception as e:
                typer.echo(f"  Error fetching page: {e}")
                continue

            links = extract_links_from_html(response.text, response.url)

            # Filter links
            filtered_links = []
            for link in links:
                link_domain = urlparse(link["url"]).netloc
                is_external = link_domain != base_domain

                if is_external and not check_external:
                    continue

                if link["url"] not in checked_urls:
                    filtered_links.append(link)

            typer.echo(f"  Found {len(filtered_links)} links to check")

            # Check links in batches
            batch_size = 10
            for i in range(0, len(filtered_links), batch_size):
                batch = filtered_links[i:i + batch_size]
                results = await asyncio.gather(*[
                    check_url(client, link["url"]) for link in batch
                ])

                for link, result in zip(batch, results):
                    result["source"] = link["source"]
                    result["text"] = link["text"]
                    all_links.append(result)

                    if result["ok"]:
                        if result.get("redirect"):
                            redirect_links.append(result)
                        else:
                            ok_links.append(result)
                    else:
                        broken_links.append(result)
                        typer.echo(f"  BROKEN: {result['status']} {result['url']}")

                    checked_urls.add(link["url"])

            # Add internal pages for recursive crawl
            if recursive and depth < max_depth:
                for link in links:
                    link_domain = urlparse(link["url"]).netloc
                    if link_domain == base_domain and link["url"] not in checked_urls:
                        # Check if it's HTML (simple heuristic)
                        parsed = urlparse(link["url"])
                        path = parsed.path.lower()
                        if not path or path.endswith(('/', '.html', '.htm', '.php', '.asp', '.aspx')):
                            pages_to_check.append((link["url"], depth + 1))

    return {
        "start_url": url,
        "total_links": len(all_links),
        "ok": len(ok_links),
        "broken": len(broken_links),
        "redirects": len(redirect_links),
        "broken_links": broken_links,
        "redirect_links": redirect_links,
    }
