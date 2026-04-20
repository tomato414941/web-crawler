"""Tests for content handling rules."""

from crawler.content_policy import (
    is_binary_content_type,
    is_html_content_type,
    should_fetch_body,
    should_extract_links,
    should_store_text_content,
)


def test_html_is_parsed_and_stored_as_text():
    assert is_html_content_type("text/html; charset=utf-8") is True
    assert should_store_text_content("text/html; charset=utf-8", b"<html>Hello</html>") is True
    assert (
        should_extract_links("text/html; charset=utf-8", b"<a href='https://example.com'>x</a>")
        is True
    )


def test_pdf_is_binary_metadata_only():
    assert is_binary_content_type("application/pdf") is True
    decision = should_fetch_body("application/pdf", 1000, max_body_bytes=100)
    assert decision.should_read is False
    assert decision.metadata_only is True
    assert decision.reason == "binary_content_type"
    assert should_store_text_content("application/pdf", b"%PDF-1.7\x00binary") is False
    assert should_extract_links("application/pdf", b"<a href='https://example.com'>x</a>") is False


def test_streaming_audio_is_not_read_as_body():
    decision = should_fetch_body("audio/mpeg", None, max_body_bytes=100)

    assert decision.should_read is False
    assert decision.metadata_only is True
    assert decision.reason == "binary_content_type"


def test_oversized_response_is_metadata_only():
    decision = should_fetch_body("text/html", 101, max_body_bytes=100)

    assert decision.should_read is False
    assert decision.metadata_only is True
    assert decision.reason == "content_length_too_large"


def test_binary_suffix_is_fallback_when_content_type_is_missing():
    decision = should_fetch_body("", None, "https://example.com/live.mp3", max_body_bytes=100)

    assert decision.should_read is False
    assert decision.metadata_only is True
    assert decision.reason == "binary_url_suffix"


def test_unknown_text_without_nul_can_still_be_stored():
    decision = should_fetch_body("", None, "https://example.com/page", max_body_bytes=100)

    assert decision.should_read is True
    assert should_store_text_content("", b"<html>Hello</html>") is True
    assert should_extract_links("", b"<html><a href='https://example.com'>x</a></html>") is True


def test_unknown_binary_with_nul_is_not_stored():
    assert should_store_text_content("", b"prefix\x00suffix") is False
