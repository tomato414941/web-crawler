import pytest

from crawler.agent import WebAgent
from crawler.egress_guard import EgressBlockedError


@pytest.mark.asyncio
async def test_agent_blocks_private_start_url():
    with pytest.raises(EgressBlockedError):
        await WebAgent._guard_url("http://127.0.0.1:8080")


@pytest.mark.asyncio
async def test_agent_blocks_private_goto_action():
    with pytest.raises(EgressBlockedError):
        await WebAgent._guard_url("http://169.254.169.254/latest/meta-data")


def test_agent_blocks_external_navigation_by_default():
    agent = object.__new__(WebAgent)
    agent.allow_external_navigation = False
    agent._start_host = "example.com"

    with pytest.raises(ValueError):
        agent._guard_navigation_scope("https://other.example/path")


def test_agent_allows_external_navigation_when_explicitly_enabled():
    agent = object.__new__(WebAgent)
    agent.allow_external_navigation = True
    agent._start_host = "example.com"

    agent._guard_navigation_scope("https://other.example/path")


@pytest.mark.asyncio
async def test_agent_blocks_form_input_by_default():
    agent = object.__new__(WebAgent)
    agent.allow_form_input = False
    agent.state = None

    result = await agent._execute_action(object(), {"action": "type", "ref": "@e1", "text": "q"})

    assert result == "Form input is disabled for this agent run"
