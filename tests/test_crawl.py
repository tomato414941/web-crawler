"""Tests for crawler engine module."""

from crawler.crawl import extract_links, normalize_url


class TestNormalizeUrl:
    def test_removes_fragment(self):
        """Fragment should be removed."""
        result = normalize_url("http://example.com/page#section")
        assert result == "http://example.com/page"

    def test_sorts_query_params(self):
        """Query parameters should be sorted alphabetically."""
        result = normalize_url("http://example.com/page?b=2&a=1")
        assert result == "http://example.com/page?a=1&b=2"

    def test_removes_trailing_slash(self):
        """Trailing slash should be removed from paths."""
        result = normalize_url("http://example.com/path/")
        assert result == "http://example.com/path"

    def test_keeps_root_slash(self):
        """Root path should remain as single slash."""
        result = normalize_url("http://example.com/")
        assert result == "http://example.com/"

    def test_lowercases_scheme_and_host(self):
        """Scheme and host should be lowercased."""
        result = normalize_url("HTTP://EXAMPLE.COM/Path")
        assert result == "http://example.com/Path"


class TestExtractLinks:
    def test_extracts_absolute_urls(self):
        """Should extract absolute URLs."""
        html = '<a href="http://example.com/page1">Link</a>'
        links = extract_links(html, "http://example.com")
        assert "http://example.com/page1" in links

    def test_converts_relative_urls(self):
        """Should convert relative URLs to absolute."""
        html = '<a href="/page1">Link</a>'
        links = extract_links(html, "http://example.com/current/")
        assert "http://example.com/page1" in links

    def test_handles_protocol_relative_urls(self):
        """Should handle protocol-relative URLs."""
        html = '<a href="//other.com/page">Link</a>'
        links = extract_links(html, "http://example.com")
        assert "https://other.com/page" in links

    def test_skips_javascript_urls(self):
        """Should skip javascript: URLs."""
        html = '<a href="javascript:void(0)">Link</a>'
        links = extract_links(html, "http://example.com")
        assert len(links) == 0

    def test_skips_mailto_urls(self):
        """Should skip mailto: URLs."""
        html = '<a href="mailto:test@example.com">Link</a>'
        links = extract_links(html, "http://example.com")
        assert len(links) == 0

    def test_skips_tel_urls(self):
        """Should skip tel: URLs."""
        html = '<a href="tel:+1234567890">Link</a>'
        links = extract_links(html, "http://example.com")
        assert len(links) == 0

    def test_skips_data_urls(self):
        """Should skip data: URLs."""
        html = '<a href="data:text/plain;base64,SGVsbG8=">Link</a>'
        links = extract_links(html, "http://example.com")
        assert len(links) == 0

    def test_skips_fragment_only_links(self):
        """Should skip fragment-only links."""
        html = '<a href="#section">Link</a>'
        links = extract_links(html, "http://example.com")
        assert len(links) == 0

    def test_removes_duplicates(self):
        """Should remove duplicate links."""
        html = '''
        <a href="/page1">Link 1</a>
        <a href="/page1">Link 1 Again</a>
        <a href="/page1#section">Link 1 With Fragment</a>
        '''
        links = extract_links(html, "http://example.com")
        # All three should normalize to the same URL
        assert len(links) == 1

    def test_extracts_multiple_links(self):
        """Should extract multiple distinct links."""
        html = '''
        <a href="/page1">Link 1</a>
        <a href="/page2">Link 2</a>
        <a href="/page3">Link 3</a>
        '''
        links = extract_links(html, "http://example.com")
        assert len(links) == 3

    def test_handles_single_quotes(self):
        """Should handle single-quoted href attributes."""
        html = "<a href='/page1'>Link</a>"
        links = extract_links(html, "http://example.com")
        assert "http://example.com/page1" in links

    def test_handles_mixed_quotes(self):
        """Should handle mixed quote styles."""
        html = '''
        <a href="/page1">Link 1</a>
        <a href='/page2'>Link 2</a>
        '''
        links = extract_links(html, "http://example.com")
        assert len(links) == 2

    def test_normalizes_extracted_links(self):
        """Extracted links should be normalized."""
        html = '<a href="/PAGE?b=2&a=1#section">Link</a>'
        links = extract_links(html, "http://EXAMPLE.COM")
        # Should be lowercased host, sorted params, no fragment
        assert links[0] == "http://example.com/PAGE?a=1&b=2"

    def test_resolves_relative_paths(self):
        """Should resolve relative paths correctly."""
        html = '<a href="../other/page">Link</a>'
        links = extract_links(html, "http://example.com/dir/current/")
        assert "http://example.com/dir/other/page" in links

    def test_empty_html(self):
        """Should handle empty HTML."""
        links = extract_links("", "http://example.com")
        assert links == []

    def test_no_links_in_html(self):
        """Should handle HTML without links."""
        html = "<p>No links here</p>"
        links = extract_links(html, "http://example.com")
        assert links == []
