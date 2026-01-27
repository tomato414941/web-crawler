"""Browser-based fetcher using Playwright."""

from playwright.async_api import async_playwright

from .protocols import Response


class BrowserFetcher:
    """Async browser fetcher using Playwright for JavaScript rendering."""

    def __init__(
        self,
        timeout: float = 30.0,
        user_agent: str | None = None,
        headless: bool = True,
    ):
        self.timeout = timeout * 1000  # Playwright uses milliseconds
        self.user_agent = user_agent
        self.headless = headless

    async def fetch(self, url: str) -> Response:
        """Fetch a URL using a headless browser."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context_opts = {}
            if self.user_agent:
                context_opts["user_agent"] = self.user_agent

            context = await browser.new_context(**context_opts)
            page = await context.new_page()

            response = await page.goto(url, timeout=self.timeout, wait_until="networkidle")

            content = await page.content()
            final_url = page.url

            headers = {}
            status = 200
            if response:
                headers = await response.all_headers()
                status = response.status

            await browser.close()

            return Response(
                url=final_url,
                status=status,
                content=content.encode("utf-8"),
                headers=headers,
            )

    async def fetch_with_snapshot(self, url: str) -> tuple[Response, str]:
        """Fetch URL and return accessibility tree snapshot for AI agents."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context_opts = {}
            if self.user_agent:
                context_opts["user_agent"] = self.user_agent

            context = await browser.new_context(**context_opts)
            page = await context.new_page()

            response = await page.goto(url, timeout=self.timeout, wait_until="networkidle")

            content = await page.content()
            final_url = page.url

            # Get accessibility tree snapshot
            snapshot = await page.accessibility.snapshot()

            headers = {}
            status = 200
            if response:
                headers = await response.all_headers()
                status = response.status

            await browser.close()

            resp = Response(
                url=final_url,
                status=status,
                content=content.encode("utf-8"),
                headers=headers,
            )

            return resp, _format_a11y_tree(snapshot) if snapshot else ""


def _format_a11y_tree(node: dict, prefix: str = "", counter: list | None = None) -> str:
    """Format accessibility tree with semantic references (@e1, @e2, etc.)."""
    if counter is None:
        counter = [0]

    lines = []
    role = node.get("role", "")
    name = node.get("name", "")

    if role and role != "none":
        counter[0] += 1
        ref = f"@e{counter[0]}"
        label = f"{role}"
        if name:
            label += f' "{name}"'
        lines.append(f"{prefix}{ref} {label}")

    children = node.get("children", [])
    for child in children:
        lines.append(_format_a11y_tree(child, prefix + "  ", counter))

    return "\n".join(filter(None, lines))
