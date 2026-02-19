"""Tests for the shared agent loop used by agentic trading examples."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

# Add examples to path so we can import shared code
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "examples" / "agentic"))

from shared.agent_loop import run_agent_loop  # noqa: E402
from shared.llm_client import MockLLMClient  # noqa: E402

from almanak.framework.agent_tools.schemas import ToolResponse  # noqa: E402


def _text_response(content: str) -> dict:
    """Helper: build an LLM response with just text (no tool calls)."""
    return {"choices": [{"message": {"role": "assistant", "content": content}}]}


def _tool_call_response(tool_name: str, args: dict, call_id: str = "call_1") -> dict:
    """Helper: build an LLM response with a single tool call."""
    return {
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": call_id,
                            "type": "function",
                            "function": {
                                "name": tool_name,
                                "arguments": json.dumps(args),
                            },
                        }
                    ],
                }
            }
        ]
    }


def _multi_tool_call_response(calls: list[tuple[str, dict, str]]) -> dict:
    """Helper: build an LLM response with multiple tool calls."""
    tool_calls = [
        {
            "id": call_id,
            "type": "function",
            "function": {"name": name, "arguments": json.dumps(args)},
        }
        for name, args, call_id in calls
    ]
    return {"choices": [{"message": {"role": "assistant", "content": None, "tool_calls": tool_calls}}]}


def _mock_executor() -> AsyncMock:
    """Create a mock ToolExecutor."""
    executor = AsyncMock()
    executor.execute.return_value = ToolResponse(
        status="success",
        data={"token": "ETH", "price_usd": 2500.0, "source": "coingecko"},
    )
    return executor


class TestRunAgentLoop:
    """Tests for the agent loop mechanics."""

    @pytest.mark.asyncio
    async def test_immediate_text_response(self):
        """LLM returns text immediately (no tool calls) -> single LLM call."""
        mock_llm = MockLLMClient([_text_response("I'll hold. No action needed.")])
        executor = _mock_executor()

        result = await run_agent_loop(
            llm_client=mock_llm,
            executor=executor,
            tools_openai=[],
            system_prompt="You are a test agent.",
            user_prompt="What should we do?",
        )

        assert result == "I'll hold. No action needed."
        assert len(mock_llm.call_log) == 1
        executor.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_one_tool_call_then_text(self):
        """LLM calls one tool, gets result, then returns text."""
        mock_llm = MockLLMClient([
            _tool_call_response("get_price", {"token": "ETH", "chain": "arbitrum"}),
            _text_response("ETH is at $2500. Holding."),
        ])
        executor = _mock_executor()

        result = await run_agent_loop(
            llm_client=mock_llm,
            executor=executor,
            tools_openai=[],
            system_prompt="You are a test agent.",
            user_prompt="Check the market.",
        )

        assert "Holding" in result
        assert len(mock_llm.call_log) == 2
        executor.execute.assert_called_once_with("get_price", {"token": "ETH", "chain": "arbitrum"})

    @pytest.mark.asyncio
    async def test_multiple_sequential_tool_calls(self):
        """LLM calls tools across multiple rounds."""
        mock_llm = MockLLMClient([
            _tool_call_response("get_price", {"token": "ETH"}, "call_1"),
            _tool_call_response("get_balance", {"token": "ETH", "chain": "arbitrum"}, "call_2"),
            _text_response("Done checking. All good."),
        ])
        executor = _mock_executor()

        result = await run_agent_loop(
            llm_client=mock_llm,
            executor=executor,
            tools_openai=[],
            system_prompt="Agent",
            user_prompt="Check everything",
        )

        assert "All good" in result
        assert len(mock_llm.call_log) == 3
        assert executor.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_parallel_tool_calls_in_one_round(self):
        """LLM calls multiple tools in a single response."""
        mock_llm = MockLLMClient([
            _multi_tool_call_response([
                ("get_price", {"token": "ETH"}, "call_1"),
                ("get_price", {"token": "BTC"}, "call_2"),
            ]),
            _text_response("ETH and BTC prices checked."),
        ])
        executor = _mock_executor()

        result = await run_agent_loop(
            llm_client=mock_llm,
            executor=executor,
            tools_openai=[],
            system_prompt="Agent",
            user_prompt="Check prices",
        )

        assert "checked" in result
        assert executor.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_max_rounds_exceeded(self):
        """Agent hits max_rounds and gets forced hold."""
        perpetual_tool_call = _tool_call_response("get_price", {"token": "ETH"})
        mock_llm = MockLLMClient([perpetual_tool_call] * 5)
        executor = _mock_executor()

        result = await run_agent_loop(
            llm_client=mock_llm,
            executor=executor,
            tools_openai=[],
            system_prompt="Agent",
            user_prompt="Keep going",
            max_rounds=3,
        )

        assert "max tool rounds exceeded" in result
        assert executor.execute.call_count == 3

    @pytest.mark.asyncio
    async def test_tool_error_propagated_to_llm(self):
        """Tool returns error status -> error fed back to LLM context."""
        mock_llm = MockLLMClient([
            _tool_call_response("get_price", {"token": "INVALID"}),
            _text_response("The token lookup failed. I'll hold."),
        ])
        executor = AsyncMock()
        executor.execute.return_value = ToolResponse(
            status="error",
            error={
                "error_code": "validation_error",
                "message": "Unknown token",
                "recoverable": True,
            },
        )

        result = await run_agent_loop(
            llm_client=mock_llm,
            executor=executor,
            tools_openai=[],
            system_prompt="Agent",
            user_prompt="Check INVALID token",
        )

        # The LLM got the error and decided to hold
        assert "hold" in result.lower() or "failed" in result.lower()
        # Verify the error was included in the tool message sent back to LLM
        second_call_messages = mock_llm.call_log[1]["messages"]
        tool_msg = [m for m in second_call_messages if m.get("role") == "tool"][0]
        assert "error" in tool_msg["content"]

    @pytest.mark.asyncio
    async def test_messages_contain_system_and_user(self):
        """Verify the initial messages include system and user prompts."""
        mock_llm = MockLLMClient([_text_response("ok")])
        executor = _mock_executor()

        await run_agent_loop(
            llm_client=mock_llm,
            executor=executor,
            tools_openai=[],
            system_prompt="You are an LP agent.",
            user_prompt="Manage the position.",
        )

        messages = mock_llm.call_log[0]["messages"]
        assert messages[0] == {"role": "system", "content": "You are an LP agent."}
        assert messages[1] == {"role": "user", "content": "Manage the position."}

    @pytest.mark.asyncio
    async def test_tool_results_fed_back_correctly(self):
        """Verify tool call results are properly formatted as tool messages."""
        mock_llm = MockLLMClient([
            _tool_call_response("get_price", {"token": "ETH"}, "call_abc"),
            _text_response("done"),
        ])
        executor = _mock_executor()

        await run_agent_loop(
            llm_client=mock_llm,
            executor=executor,
            tools_openai=[],
            system_prompt="Agent",
            user_prompt="Go",
        )

        # Check the second LLM call has the tool result message
        second_call_messages = mock_llm.call_log[1]["messages"]
        tool_msgs = [m for m in second_call_messages if m.get("role") == "tool"]
        assert len(tool_msgs) == 1
        assert tool_msgs[0]["tool_call_id"] == "call_abc"
        parsed = json.loads(tool_msgs[0]["content"])
        assert parsed["status"] == "success"
        assert parsed["data"]["token"] == "ETH"
