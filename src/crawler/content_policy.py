"""Content-type based crawl handling rules."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath


@dataclass(frozen=True, slots=True)
class FetchBodyDecision:
    """Decision for whether the fetcher should read a response body."""

    should_read: bool
    metadata_only: bool = False
    reason: str | None = None


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
_BINARY_URL_SUFFIXES = {
    ".7z",
    ".aac",
    ".avi",
    ".bmp",
    ".bz2",
    ".dmg",
    ".doc",
    ".docx",
    ".flac",
    ".gif",
    ".gz",
    ".ico",
    ".iso",
    ".jpeg",
    ".jpg",
    ".m4a",
    ".m4v",
    ".mov",
    ".mp3",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".ogg",
    ".ogv",
    ".pdf",
    ".png",
    ".ppt",
    ".pptx",
    ".rar",
    ".tar",
    ".tgz",
    ".webm",
    ".webp",
    ".woff",
    ".woff2",
    ".xls",
    ".xlsx",
    ".zip",
}


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


def has_binary_url_suffix(url: str | None) -> bool:
    """Return True when the URL path has a known binary resource suffix."""
    if not url:
        return False
    suffix = PurePosixPath(url.split("?", 1)[0]).suffix.lower()
    return suffix in _BINARY_URL_SUFFIXES


def should_fetch_body(
    content_type: str | None,
    content_length: int | None,
    url: str | None = None,
    *,
    max_body_bytes: int,
) -> FetchBodyDecision:
    """Decide whether a response body is useful enough to read."""
    if is_binary_content_type(content_type):
        return FetchBodyDecision(False, metadata_only=True, reason="binary_content_type")
    if content_length is not None and content_length > max_body_bytes:
        return FetchBodyDecision(False, metadata_only=True, reason="content_length_too_large")
    if not normalize_content_type(content_type) and has_binary_url_suffix(url):
        return FetchBodyDecision(False, metadata_only=True, reason="binary_url_suffix")
    return FetchBodyDecision(True)


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
    return (
        snippet.startswith(b"<!doctype html") or snippet.startswith(b"<html") or b"<a " in snippet
    )
