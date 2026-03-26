"""Shared error categorization for crawl observability."""


def categorize_crawl_error(error: str | None) -> str | None:
    """Collapse raw crawl errors into stable operator-facing buckets."""
    if not error:
        return None

    if error.startswith("http_"):
        try:
            status = int(error.split("_", 1)[1])
        except ValueError:
            return "http_other"
        if 400 <= status < 500:
            return "http_4xx"
        if 500 <= status < 600:
            return "http_5xx"
        return "http_other"

    if error == "timeout":
        return "timeout"

    if error == "connection_error" or "Server disconnected without sending a response." in error:
        return "connection_error"

    return "other"
