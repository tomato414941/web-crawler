"""Data extraction using CSS selectors and XPath."""

from selectolax.parser import HTMLParser

from .config import settings
from .core import HttpFetcher


class Extractor:
    """Extract data from HTML using CSS selectors or XPath."""

    def __init__(self, html: str):
        self.tree = HTMLParser(html)

    def css(self, selector: str, attribute: str | None = None) -> list[str]:
        """Extract data using CSS selector."""
        nodes = self.tree.css(selector)
        results = []

        for node in nodes:
            if attribute:
                attr_val = node.attributes.get(attribute)
                if attr_val:
                    results.append(attr_val)
            else:
                text = node.text(strip=True)
                if text:
                    results.append(text)

        return results

    def css_first(self, selector: str, attribute: str | None = None) -> str | None:
        """Extract first match using CSS selector."""
        results = self.css(selector, attribute)
        return results[0] if results else None

    def xpath(self, expression: str, attribute: str | None = None) -> list[str]:
        """
        Extract data using XPath-like expression.
        Note: selectolax has limited XPath support, converts to CSS where possible.
        """
        # Convert simple XPath to CSS
        css_selector = self._xpath_to_css(expression)
        if css_selector:
            return self.css(css_selector, attribute)

        # Fallback: try direct matching
        return []

    def _xpath_to_css(self, xpath: str) -> str | None:
        """Convert simple XPath expressions to CSS selectors."""
        # Handle common patterns
        if xpath.startswith("//"):
            xpath = xpath[2:]

        # //div[@class="foo"] -> div.foo
        # //a[@href] -> a[href]
        # //div/p -> div > p

        import re

        # Replace // with descendant selector
        result = xpath.replace("//", " ")

        # Convert [@attr="value"] to [attr="value"]
        result = re.sub(r'\[@(\w+)="([^"]+)"\]', r'[\1="\2"]', result)

        # Convert [@attr] to [attr]
        result = re.sub(r'\[@(\w+)\]', r'[\1]', result)

        # Convert / to >
        result = result.replace("/", " > ")

        # Clean up
        result = result.strip()
        result = re.sub(r'\s+', ' ', result)

        return result if result else None

    def extract_all(self, selectors: dict[str, str]) -> dict[str, list[str]]:
        """Extract multiple fields using a mapping of names to selectors."""
        results = {}
        for name, selector in selectors.items():
            results[name] = self.css(selector)
        return results

    def get_text(self) -> str:
        """Get all visible text from the page."""
        return self.tree.body.text(strip=True) if self.tree.body else ""

    def get_links(self) -> list[dict]:
        """Get all links with text and href."""
        links = []
        for node in self.tree.css("a[href]"):
            href = node.attributes.get("href", "")
            text = node.text(strip=True)
            links.append({"href": href, "text": text})
        return links

    def get_images(self) -> list[dict]:
        """Get all images with src and alt."""
        images = []
        for node in self.tree.css("img[src]"):
            src = node.attributes.get("src", "")
            alt = node.attributes.get("alt", "")
            images.append({"src": src, "alt": alt})
        return images

    def get_meta(self) -> dict[str, str]:
        """Get meta tags as dictionary."""
        meta = {}
        for node in self.tree.css("meta[name]"):
            name = node.attributes.get("name", "")
            content = node.attributes.get("content", "")
            if name:
                meta[name] = content

        # Also get og: tags
        for node in self.tree.css("meta[property]"):
            prop = node.attributes.get("property", "")
            content = node.attributes.get("content", "")
            if prop:
                meta[prop] = content

        return meta


async def extract_data(
    url: str,
    css_selector: str | None = None,
    xpath: str | None = None,
    attribute: str | None = None,
    use_browser: bool = False,
) -> dict:
    """Fetch URL and extract data."""
    if use_browser:
        from .core import get_browser_fetcher
        fetcher = get_browser_fetcher()(timeout=30.0)
    else:
        fetcher = HttpFetcher(timeout=settings.timeout)

    response = await fetcher.fetch(url)
    extractor = Extractor(response.text)

    items = []
    if css_selector:
        items = extractor.css(css_selector, attribute)
    elif xpath:
        items = extractor.xpath(xpath, attribute)
    else:
        # Default: extract links
        links = extractor.get_links()
        items = [f"{link['text']} -> {link['href']}" for link in links if link['href']]

    return {
        "url": response.url,
        "selector": css_selector or xpath,
        "items": items,
        "count": len(items),
    }
