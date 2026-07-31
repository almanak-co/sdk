"""Direct serialization tests for agent-tool framework adapters."""

import json
import sys
from decimal import Decimal
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.agent_tools.adapters.langchain_adapter import get_langchain_tools
from almanak.framework.agent_tools.adapters.mcp_adapter import AlmanakMCPServer
from almanak.framework.agent_tools.schemas import ToolResponse, ToolResponseStatus


def _json_mode_response() -> ToolResponse:
    return ToolResponse(
        status=ToolResponseStatus.SIMULATED,
        data={"amount": Decimal("1.25")},
    )


class _StructuredToolStub:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


@pytest.fixture
def langchain_tools_module(monkeypatch):
    package = ModuleType("langchain_core")
    tools_module = ModuleType("langchain_core.tools")
    tools_module.StructuredTool = _StructuredToolStub
    package.tools = tools_module
    monkeypatch.setitem(sys.modules, "langchain_core", package)
    monkeypatch.setitem(sys.modules, "langchain_core.tools", tools_module)


def _single_tool_catalog():
    catalog = MagicMock()
    catalog.list_tools.return_value = [
        SimpleNamespace(
            name="test_json_serialization",
            description="Test JSON serialization",
            request_schema=None,
        )
    ]
    return catalog


def _assert_json_mode_response(text: str) -> None:
    payload = json.loads(text)
    assert payload == {
        "status": "simulated",
        "data": {"amount": "1.25"},
    }


@pytest.mark.asyncio
async def test_langchain_adapter_serializes_sync_and_async_responses(langchain_tools_module):
    executor = MagicMock()
    executor.execute = AsyncMock(return_value=_json_mode_response())
    tool = get_langchain_tools(_single_tool_catalog(), executor)[0]

    _assert_json_mode_response(tool.func())
    _assert_json_mode_response(await tool.coroutine())
    assert executor.execute.await_count == 2


@pytest.mark.asyncio
async def test_mcp_adapter_serializes_response_in_json_mode():
    executor = MagicMock()
    executor.execute = AsyncMock(return_value=_json_mode_response())
    server = AlmanakMCPServer(executor=executor, catalog=MagicMock())

    result = await server.tools_call("test_json_serialization", {"value": "1.25"})

    assert result["content"][0]["type"] == "text"
    _assert_json_mode_response(result["content"][0]["text"])
    executor.execute.assert_awaited_once_with("test_json_serialization", {"value": "1.25"})
