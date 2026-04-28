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
