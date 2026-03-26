"""Content-type based crawl handling rules."""

from __future__ import annotations


_TEXTUAL_APPLICATION_TYPES = {
    "application/javascript",
    "application/json",
    "application/ld+json",
    "application/xhtml+xml",
    "application/xml",
    "application/x-www-form-urlencoded",
}
_BINARY_APPLICATION_TYPES = {
    "application/octet-stream",
    "application/pdf",
    "application/gzip",
    "application/zip",
    "application/x-bzip",
    "application/x-bzip2",
    "application/x-gzip",
    "application/x-rar-compressed",
    "application/x-tar",
    "application/x-zip-compressed",
}
_BINARY_APPLICATION_PREFIXES = (
    "application/msword",
    "application/vnd.",
)
_BINARY_TOP_LEVEL_TYPES = (
    "audio/",
    "font/",
    "image/",
    "video/",
)


def normalize_content_type(content_type: str | None) -> str:
    """Return a normalized mime type without parameters."""
    if not content_type:
        return ""
    return content_type.split(";", 1)[0].strip().lower()


def is_html_content_type(content_type: str | None) -> bool:
    """Return True when the payload should be parsed as HTML."""
    normalized = normalize_content_type(content_type)
    return normalized in {"text/html", "application/xhtml+xml"}


def is_text_content_type(content_type: str | None) -> bool:
    """Return True when the payload is safe to store as text."""
    normalized = normalize_content_type(content_type)
    if not normalized:
        return False
    if normalized.startswith("text/"):
        return True
    if normalized in _TEXTUAL_APPLICATION_TYPES:
        return True
    return normalized.endswith("+json") or normalized.endswith("+xml")


def is_binary_content_type(content_type: str | None) -> bool:
    """Return True when the payload should be treated as binary metadata-only content."""
    normalized = normalize_content_type(content_type)
    if not normalized:
        return False
    if is_text_content_type(normalized):
        return False
    if normalized in _BINARY_APPLICATION_TYPES:
        return True
    if normalized.startswith(_BINARY_TOP_LEVEL_TYPES):
        return True
    return normalized.startswith(_BINARY_APPLICATION_PREFIXES)


def should_store_text_content(content_type: str | None, content: bytes) -> bool:
    """Return True when the payload should be persisted as page text."""
    normalized = normalize_content_type(content_type)
    if normalized:
        return is_text_content_type(normalized)
    return b"\x00" not in content


def should_extract_links(content_type: str | None, content: bytes) -> bool:
    """Return True when the payload should be parsed for HTML outlinks."""
    normalized = normalize_content_type(content_type)
    if normalized:
        return is_html_content_type(normalized)
    snippet = content[:2048].lstrip().lower()
    return snippet.startswith(b"<!doctype html") or snippet.startswith(b"<html") or b"<a " in snippet
