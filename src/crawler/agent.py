"""AI agent for autonomous web browsing using Claude."""

import json
import re
from dataclasses import dataclass, field

import anthropic
import typer
from playwright.async_api import Page, async_playwright

SYSTEM_PROMPT = """You are a web browsing agent. You interact with web pages to complete tasks.

## Available Actions

You can perform these actions by responding with a JSON object:

1. **click** - Click an element
   {"action": "click", "ref": "@e1"}

2. **type** - Type text into an input field
   {"action": "type", "ref": "@e1", "text": "search query"}

3. **scroll** - Scroll the page
   {"action": "scroll", "direction": "down"}  // or "up"

4. **goto** - Navigate to a URL
   {"action": "goto", "url": "https://example.com"}

5. **wait** - Wait for page to load
   {"action": "wait", "seconds": 2}

6. **done** - Task completed
   {"action": "done", "result": "Description of what was accomplished"}

7. **fail** - Task cannot be completed
   {"action": "fail", "reason": "Why the task failed"}

## Element References

Elements are referenced using semantic IDs like @e1, @e2, etc.
These correspond to interactive elements in the accessibility tree.

## Guidelines

- Use the accessibility tree to understand the page structure
- Click buttons and links to navigate
- Type in text fields for search or forms
- Be efficient - complete tasks in as few steps as possible
- If stuck, try a different approach
- Report completion or failure clearly

Respond with ONLY a JSON object for your action. No other text."""


def parse_action(response_text: str) -> dict:
    """Extract action JSON from Claude's response."""
    text = response_text.strip()

    # Method 1: Entire response is JSON
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Method 2: JSON inside code block
    code_block = re.search(r'```(?:json)?\s*(\{[\s\S]*?\})\s*```', text)
    if code_block:
        try:
            return json.loads(code_block.group(1))
        except json.JSONDecodeError:
            pass

    # Method 3: Extract nested JSON by bracket matching
    depth = 0
    start = -1
    for i, char in enumerate(text):
        if char == '{':
            if depth == 0:
                start = i
            depth += 1
        elif char == '}':
            depth -= 1
            if depth == 0 and start != -1:
                try:
                    return json.loads(text[start:i+1])
                except json.JSONDecodeError:
                    start = -1

    return {"action": "fail", "reason": "Could not parse action"}


@dataclass
class AgentState:
    """State of the AI agent."""
    url: str
    task: str
    steps: int = 0
    history: list[dict] = field(default_factory=list)
    element_map: dict[str, dict] = field(default_factory=dict)
    status: str = "running"
    result: str | None = None


class WebAgent:
    """AI-powered web browsing agent."""

    def __init__(
        self,
        task: str,
        model: str = "claude-sonnet-4-20250514",
        max_steps: int = 10,
        headless: bool = True,
        verbose: bool = False,
    ):
        self.task = task
        self.model = model
        self.max_steps = max_steps
        self.headless = headless
        self.verbose = verbose

        self.client = anthropic.Anthropic()
        self.state: AgentState | None = None

    async def _get_accessibility_tree(self, page: Page) -> tuple[str, dict[str, dict]]:
        """Get accessibility tree with semantic references."""
        snapshot = await page.accessibility.snapshot()
        if not snapshot:
            return "No accessibility tree available", {}

        lines = []
        element_map = {}
        counter = [0]

        def process_node(node: dict, indent: int = 0):
            role = node.get("role", "")
            name = node.get("name", "")

            if role and role not in ("none", "generic", "group"):
                counter[0] += 1
                ref = f"@e{counter[0]}"

                # Build element info
                elem_info = {
                    "role": role,
                    "name": name,
                }

                # Add value if present
                if node.get("value"):
                    elem_info["value"] = node.get("value")

                element_map[ref] = elem_info

                # Format line
                prefix = "  " * indent
                line = f"{prefix}{ref} [{role}]"
                if name:
                    line += f' "{name[:50]}"'
                if node.get("value"):
                    line += f' value="{node.get("value")[:30]}"'

                lines.append(line)

            for child in node.get("children", []):
                process_node(child, indent + 1)

        process_node(snapshot)
        return "\n".join(lines), element_map

    async def _find_element(self, page: Page, ref: str) -> str | None:
        """Find element selector from reference."""
        info = self.state.element_map.get(ref)
        if not info:
            return None

        role = info.get("role", "")
        name = info.get("name", "")

        # Try to locate by role and name
        if name:
            # Escape special characters in name
            escaped_name = name.replace('"', '\\"')
            selectors = [
                f'role={role}[name="{escaped_name}"]',
                f'text="{escaped_name}"',
                f'[aria-label="{escaped_name}"]',
            ]

            for selector in selectors:
                try:
                    locator = page.locator(selector).first
                    if await locator.count() > 0:
                        return selector
                except Exception:
                    continue

        return None

    async def _execute_action(self, page: Page, action: dict) -> str:
        """Execute an action on the page."""
        action_type = action.get("action", "")

        try:
            if action_type == "click":
                ref = action.get("ref", "")
                selector = await self._find_element(page, ref)
                if selector:
                    await page.locator(selector).first.click()
                    await page.wait_for_load_state("networkidle", timeout=5000)
                    return f"Clicked {ref}"
                return f"Could not find element {ref}"

            elif action_type == "type":
                ref = action.get("ref", "")
                text = action.get("text", "")
                selector = await self._find_element(page, ref)
                if selector:
                    await page.locator(selector).first.fill(text)
                    return f"Typed '{text}' into {ref}"
                return f"Could not find element {ref}"

            elif action_type == "scroll":
                direction = action.get("direction", "down")
                delta = 500 if direction == "down" else -500
                await page.mouse.wheel(0, delta)
                return f"Scrolled {direction}"

            elif action_type == "goto":
                url = action.get("url", "")
                await page.goto(url, wait_until="networkidle", timeout=30000)
                return f"Navigated to {url}"

            elif action_type == "wait":
                seconds = action.get("seconds", 2)
                import asyncio
                await asyncio.sleep(seconds)
                return f"Waited {seconds} seconds"

            elif action_type == "done":
                self.state.status = "completed"
                self.state.result = action.get("result", "Task completed")
                return f"Task completed: {self.state.result}"

            elif action_type == "fail":
                self.state.status = "failed"
                self.state.result = action.get("reason", "Task failed")
                return f"Task failed: {self.state.result}"

            else:
                return f"Unknown action: {action_type}"

        except Exception as e:
            return f"Error executing {action_type}: {str(e)}"

    def _build_messages(self, a11y_tree: str, last_result: str | None = None) -> list[dict]:
        """Build messages for Claude API."""
        messages = []

        # Add history
        for entry in self.state.history:
            messages.append({"role": "user", "content": entry["observation"]})
            messages.append({"role": "assistant", "content": json.dumps(entry["action"])})

        # Current state
        current_content = f"""## Current Page
URL: {self.state.url}

## Accessibility Tree
{a11y_tree}

## Task
{self.task}

Step {self.state.steps + 1}/{self.max_steps}"""

        if last_result:
            current_content += f"\n\n## Last Action Result\n{last_result}"

        messages.append({"role": "user", "content": current_content})

        return messages

    async def run(self, start_url: str) -> dict:
        """Run the agent starting from a URL."""
        self.state = AgentState(url=start_url, task=self.task)

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context()
            page = await context.new_page()

            await page.goto(start_url, wait_until="networkidle", timeout=30000)
            self.state.url = page.url

            last_result = None

            while self.state.status == "running" and self.state.steps < self.max_steps:
                self.state.steps += 1

                # Get current page state
                a11y_tree, self.state.element_map = await self._get_accessibility_tree(page)

                if self.verbose:
                    typer.echo(f"\n--- Step {self.state.steps} ---")
                    typer.echo(f"URL: {self.state.url}")
                    typer.echo(f"Elements: {len(self.state.element_map)}")

                # Get action from Claude
                messages = self._build_messages(a11y_tree, last_result)

                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=1024,
                    system=SYSTEM_PROMPT,
                    messages=messages,
                )

                action_text = response.content[0].text.strip()

                if self.verbose:
                    typer.echo(f"Action: {action_text}")

                # Parse action using improved parser
                action = parse_action(action_text)

                # Execute action
                last_result = await self._execute_action(page, action)
                self.state.url = page.url

                if self.verbose:
                    typer.echo(f"Result: {last_result}")

                # Record history
                self.state.history.append({
                    "observation": f"URL: {self.state.url}, Elements: {len(self.state.element_map)}",
                    "action": action,
                    "result": last_result,
                })

            await browser.close()

        if self.state.status == "running":
            self.state.status = "max_steps_reached"

        return {
            "status": self.state.status,
            "result": self.state.result,
            "steps": self.state.steps,
            "history": self.state.history,
        }


async def run_agent(
    start_url: str,
    task: str,
    max_steps: int = 10,
    model: str = "claude-sonnet-4-20250514",
    headless: bool = True,
    verbose: bool = False,
) -> dict:
    """Run an AI agent to perform a task on web pages."""
    agent = WebAgent(
        task=task,
        model=model,
        max_steps=max_steps,
        headless=headless,
        verbose=verbose,
    )

    typer.echo(f"Starting agent with task: {task}")
    typer.echo(f"Model: {model}, Max steps: {max_steps}")

    result = await agent.run(start_url)
    return result
