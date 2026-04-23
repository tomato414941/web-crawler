"""Tests for page content storage tiering."""

from crawler.page_storage_policy import (
    STORAGE_TIER_EXTENDED,
    STORAGE_TIER_METADATA_ONLY,
    STORAGE_TIER_STANDARD,
    STORAGE_TIER_SUMMARY,
    prepare_page_content,
)


def _prepare(content: str, *, discovery_value: float = 1.0, content_type: str = "text/html"):
    return prepare_page_content(
        content=content,
        content_type=content_type,
        discovery_value=discovery_value,
        summary_bytes=32,
        standard_bytes=128,
        extended_bytes=512,
        standard_min_discovery_value=1.0,
        extended_min_discovery_value=1.4,
    )


def test_metadata_only_for_empty_content():
    stored = _prepare("")

    assert stored.storage_tier == STORAGE_TIER_METADATA_ONLY
    assert stored.content == ""
    assert stored.stored_content_bytes == 0


def test_summary_for_low_value_content():
    stored = _prepare("x" * 80, discovery_value=0.7)

    assert stored.storage_tier == STORAGE_TIER_SUMMARY
    assert stored.stored_content_bytes == 32
    assert stored.content_truncated is True


def test_standard_for_normal_value_content():
    stored = _prepare("x" * 80, discovery_value=1.0)

    assert stored.storage_tier == STORAGE_TIER_STANDARD
    assert stored.content == "x" * 80
    assert stored.content_truncated is False


def test_extended_for_high_value_content():
    stored = _prepare("x" * 300, discovery_value=1.5)

    assert stored.storage_tier == STORAGE_TIER_EXTENDED
    assert stored.stored_content_bytes == 300
    assert stored.content_truncated is False


def test_oversized_low_value_content_is_summary():
    stored = _prepare("x" * 600, discovery_value=1.0)

    assert stored.storage_tier == STORAGE_TIER_SUMMARY
    assert stored.storage_reason == "oversized_low_value"
    assert stored.stored_content_bytes == 32


def test_non_text_content_type_is_metadata_only():
    stored = _prepare("not really text", content_type="image/png")

    assert stored.storage_tier == STORAGE_TIER_METADATA_ONLY
    assert stored.content == ""
    assert stored.content_truncated is True
