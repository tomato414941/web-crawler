"""URL normalization and extraction utilities."""

from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from selectolax.parser import HTMLParser

_SKIP_SCHEMES = ('#', 'javascript:', 'mailto:', 'tel:', 'data:')


def normalize_url(url: str) -> str:
    """Normalize URL for deduplication (remove fragment, sort query params)."""
    parsed = urlparse(url)

    query_params = parse_qsl(parsed.query)
    sorted_query = urlencode(sorted(query_params))

    path = parsed.path.rstrip("/") or "/"

    normalized = urlunparse((
        parsed.scheme.lower(),
        parsed.netloc.lower(),
        path,
        parsed.params,
        sorted_query,
        "",
    ))
    return normalized


def extract_links(html: str, base_url: str) -> list[str]:
    """Extract unique normalized URLs from <a> tags in HTML."""
    seen: set[str] = set()
    results: list[str] = []
    for url, _text in extract_anchors(html, base_url):
        if url in seen:
            continue
        seen.add(url)
        results.append(url)
    return results


def extract_anchors(html: str, base_url: str) -> list[tuple[str, str]]:
    """Extract (absolute_url, link_text) pairs from <a> tags in HTML.

    Resolves relative URLs, skips non-HTTP schemes, normalizes URLs.
    """
    results = []
    tree = HTMLParser(html)
    for node in tree.css("a[href]"):
        href = node.attributes.get("href", "").strip()

        if href.startswith(_SKIP_SCHEMES):
            continue

        if href.startswith('//'):
            href = 'https:' + href

        absolute_url = urljoin(base_url, href)

        if absolute_url.startswith(('http://', 'https://')):
            text = node.text(separator=" ", strip=True)
            results.append((normalize_url(absolute_url), text))

    return results
