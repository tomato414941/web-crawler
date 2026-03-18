"""URL normalization and extraction utilities."""

import re
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

_ANCHOR_PATTERN = re.compile(
    r'<a\s[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL
)
_TAG_PATTERN = re.compile(r'<[^>]+>')

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
    return list({url for url, _text in extract_anchors(html, base_url)})


def extract_anchors(html: str, base_url: str) -> list[tuple[str, str]]:
    """Extract (absolute_url, link_text) pairs from <a> tags in HTML.

    Resolves relative URLs, skips non-HTTP schemes, normalizes URLs.
    """
    results = []
    for match in _ANCHOR_PATTERN.finditer(html):
        href = match.group(1).strip()

        if href.startswith(_SKIP_SCHEMES):
            continue

        if href.startswith('//'):
            href = 'https:' + href

        absolute_url = urljoin(base_url, href)

        if absolute_url.startswith(('http://', 'https://')):
            text = _TAG_PATTERN.sub('', match.group(2)).strip()
            results.append((normalize_url(absolute_url), text))

    return results
