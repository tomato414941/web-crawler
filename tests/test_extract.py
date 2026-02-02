"""Tests for data extraction module."""

import pytest

from crawler.extract import Extractor


class TestExtractorCss:
    @pytest.fixture
    def html(self):
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Test Page</title>
            <meta name="description" content="A test page">
            <meta property="og:title" content="OG Title">
        </head>
        <body>
            <h1>Main Title</h1>
            <h2 class="subtitle">Subtitle One</h2>
            <h2 class="subtitle">Subtitle Two</h2>
            <a href="http://example.com/page1">Link One</a>
            <a href="/page2">Link Two</a>
            <a href="#">Empty Link</a>
            <img src="/image1.jpg" alt="Image One">
            <img src="/image2.png" alt="Image Two">
            <div class="content">
                <p>Paragraph one</p>
                <p>Paragraph two</p>
            </div>
        </body>
        </html>
        """

    def test_css_extracts_text(self, html):
        """CSS selector should extract text content."""
        extractor = Extractor(html)
        result = extractor.css("h1")
        assert result == ["Main Title"]

    def test_css_extracts_multiple(self, html):
        """CSS selector should extract multiple matches."""
        extractor = Extractor(html)
        result = extractor.css("h2")
        assert result == ["Subtitle One", "Subtitle Two"]

    def test_css_extracts_attribute(self, html):
        """CSS selector with attribute should extract attribute value."""
        extractor = Extractor(html)
        result = extractor.css("a", attribute="href")
        assert "http://example.com/page1" in result
        assert "/page2" in result

    def test_css_extracts_class_selector(self, html):
        """CSS selector with class should work."""
        extractor = Extractor(html)
        result = extractor.css(".subtitle")
        assert len(result) == 2

    def test_css_extracts_nested(self, html):
        """Nested CSS selector should work."""
        extractor = Extractor(html)
        result = extractor.css(".content p")
        assert result == ["Paragraph one", "Paragraph two"]

    def test_css_returns_empty_for_no_match(self, html):
        """CSS selector should return empty list for no matches."""
        extractor = Extractor(html)
        result = extractor.css(".nonexistent")
        assert result == []

    def test_css_skips_empty_text(self):
        """CSS selector should skip elements with empty text."""
        html = "<div><p>  </p><p>Text</p></div>"
        extractor = Extractor(html)
        result = extractor.css("p")
        assert result == ["Text"]


class TestExtractorCssFirst:
    def test_css_first_returns_first_match(self):
        """css_first should return only the first match."""
        html = "<h1>First</h1><h1>Second</h1>"
        extractor = Extractor(html)
        result = extractor.css_first("h1")
        assert result == "First"

    def test_css_first_returns_none_for_no_match(self):
        """css_first should return None for no matches."""
        html = "<h1>Title</h1>"
        extractor = Extractor(html)
        result = extractor.css_first(".nonexistent")
        assert result is None


class TestExtractorXpathToCss:
    def test_xpath_to_css_class_attribute(self):
        """XPath class selector should convert to CSS."""
        extractor = Extractor("<html></html>")
        result = extractor._xpath_to_css('//div[@class="foo"]')
        assert 'div[class="foo"]' in result

    def test_xpath_to_css_attribute_presence(self):
        """XPath attribute presence should convert to CSS."""
        extractor = Extractor("<html></html>")
        result = extractor._xpath_to_css("//a[@href]")
        assert "a[href]" in result

    def test_xpath_to_css_descendant(self):
        """XPath descendant should convert to CSS."""
        extractor = Extractor("<html></html>")
        result = extractor._xpath_to_css("//div/p")
        assert "div" in result and "p" in result


class TestExtractorXpath:
    def test_xpath_extracts_text(self):
        """XPath should extract text via CSS conversion."""
        html = '<div class="test">Content</div>'
        extractor = Extractor(html)
        result = extractor.xpath('//div[@class="test"]')
        assert "Content" in result


class TestExtractorExtractAll:
    def test_extract_all_multiple_selectors(self):
        """extract_all should extract multiple selectors."""
        html = "<h1>Title</h1><p>Para 1</p><p>Para 2</p>"
        extractor = Extractor(html)
        result = extractor.extract_all({
            "title": "h1",
            "paragraphs": "p",
        })
        assert result["title"] == ["Title"]
        assert result["paragraphs"] == ["Para 1", "Para 2"]


class TestExtractorGetText:
    def test_get_text_returns_all_visible_text(self):
        """get_text should return all visible text."""
        html = "<body><h1>Title</h1><p>Paragraph</p></body>"
        extractor = Extractor(html)
        result = extractor.get_text()
        assert "Title" in result
        assert "Paragraph" in result


class TestExtractorGetLinks:
    def test_get_links_returns_href_and_text(self):
        """get_links should return href and text."""
        html = '<a href="http://example.com">Link Text</a>'
        extractor = Extractor(html)
        links = extractor.get_links()
        assert len(links) == 1
        assert links[0]["href"] == "http://example.com"
        assert links[0]["text"] == "Link Text"

    def test_get_links_skips_no_href(self):
        """get_links should skip links without href."""
        html = '<a name="anchor">Anchor</a><a href="/page">Link</a>'
        extractor = Extractor(html)
        links = extractor.get_links()
        assert len(links) == 1
        assert links[0]["href"] == "/page"


class TestExtractorGetImages:
    def test_get_images_returns_src_and_alt(self):
        """get_images should return src and alt."""
        html = '<img src="/image.jpg" alt="Description">'
        extractor = Extractor(html)
        images = extractor.get_images()
        assert len(images) == 1
        assert images[0]["src"] == "/image.jpg"
        assert images[0]["alt"] == "Description"

    def test_get_images_handles_missing_alt(self):
        """get_images should handle missing alt attribute."""
        html = '<img src="/image.jpg">'
        extractor = Extractor(html)
        images = extractor.get_images()
        assert len(images) == 1
        assert images[0]["alt"] == ""


class TestExtractorGetMeta:
    def test_get_meta_extracts_name_content(self):
        """get_meta should extract name/content pairs."""
        html = '<head><meta name="description" content="Page description"></head>'
        extractor = Extractor(html)
        meta = extractor.get_meta()
        assert meta["description"] == "Page description"

    def test_get_meta_extracts_og_tags(self):
        """get_meta should extract Open Graph tags."""
        html = '<head><meta property="og:title" content="OG Title"></head>'
        extractor = Extractor(html)
        meta = extractor.get_meta()
        assert meta["og:title"] == "OG Title"

    def test_get_meta_combines_name_and_property(self):
        """get_meta should combine name and property tags."""
        html = """
        <head>
            <meta name="description" content="Description">
            <meta property="og:title" content="OG Title">
        </head>
        """
        extractor = Extractor(html)
        meta = extractor.get_meta()
        assert "description" in meta
        assert "og:title" in meta
