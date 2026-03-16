"""URL normalization utilities."""

from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


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
