"""Storage tiering policy for crawled page content."""

from __future__ import annotations

from dataclasses import dataclass

from .content_policy import is_text_content_type, normalize_content_type

STORAGE_TIER_METADATA_ONLY = "metadata_only"
STORAGE_TIER_SUMMARY = "summary"
STORAGE_TIER_STANDARD = "standard"
STORAGE_TIER_EXTENDED = "extended"


@dataclass(frozen=True, slots=True)
class StoredPageContent:
    """Prepared content payload for durable page storage."""

    content: str
    storage_tier: str
    storage_reason: str
    stored_content_bytes: int
    content_truncated: bool


def _utf8_len(value: str) -> int:
    return len(value.encode("utf-8"))


def _truncate_utf8(value: str, max_bytes: int) -> tuple[str, bool]:
    if max_bytes <= 0:
        return "", bool(value)

    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value, False
    return encoded[:max_bytes].decode("utf-8", errors="ignore"), True


def _is_storable_text(content_type: str | None, content: str) -> bool:
    normalized = normalize_content_type(content_type)
    if normalized:
        return is_text_content_type(normalized)
    return bool(content)


def prepare_page_content(
    *,
    content: str,
    content_type: str | None,
    discovery_value: float,
    summary_bytes: int,
    standard_bytes: int,
    extended_bytes: int,
    standard_min_discovery_value: float,
    extended_min_discovery_value: float,
) -> StoredPageContent:
    """Choose a storage tier and truncate content to that tier's byte budget."""
    if not content:
        return StoredPageContent(
            content="",
            storage_tier=STORAGE_TIER_METADATA_ONLY,
            storage_reason="empty_content",
            stored_content_bytes=0,
            content_truncated=False,
        )

    if not _is_storable_text(content_type, content):
        return StoredPageContent(
            content="",
            storage_tier=STORAGE_TIER_METADATA_ONLY,
            storage_reason="non_text_content_type",
            stored_content_bytes=0,
            content_truncated=True,
        )

    original_bytes = _utf8_len(content)
    if discovery_value >= extended_min_discovery_value:
        tier = STORAGE_TIER_EXTENDED
        max_bytes = extended_bytes
        reason = "high_discovery_value"
    elif original_bytes > extended_bytes:
        tier = STORAGE_TIER_SUMMARY
        max_bytes = summary_bytes
        reason = "oversized_low_value"
    elif discovery_value >= standard_min_discovery_value:
        tier = STORAGE_TIER_STANDARD
        max_bytes = standard_bytes
        reason = "standard_discovery_value"
    else:
        tier = STORAGE_TIER_SUMMARY
        max_bytes = summary_bytes
        reason = "low_discovery_value"

    stored_content, truncated = _truncate_utf8(content, max_bytes)
    return StoredPageContent(
        content=stored_content,
        storage_tier=tier,
        storage_reason=reason,
        stored_content_bytes=_utf8_len(stored_content),
        content_truncated=truncated,
    )
