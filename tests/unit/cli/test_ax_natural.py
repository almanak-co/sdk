"""Tests for the natural language mode of ``almanak ax --natural``."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from almanak.framework.agent_tools.llm_client import LLMConfig, LLMConfigError
from almanak.framework.cli.ax_natural import (
    InterpretedAction,
    NaturalLanguageError,
    _parse_llm_response,
    interpret_natural_language,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_tool_call_response(tool_name: str, arguments: dict, content: str = "") -> dict:
    """Build a mock OpenAI chat completion response with a tool call."""
    return {
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": content,
                    "tool_calls": [
                        {
                            "id": "call_abc123",
                            "type": "function",
                            "function": {
                                "name": tool_name,
                                "arguments": json.dumps(arguments),
                            },
                        }
                    ],
                }
            }
        ]
    }


def _make_text_response(content: str) -> dict:
    """Build a mock response with text only (no tool calls)."""
    return {
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": content,
                }
            }
        ]
    }


CATALOG_NAMES = {
    "get_price",
    "get_balance",
    "swap_tokens",
    "open_lp_position",
    "close_lp_position",
    "compile_intent",
    "batch_get_balances",
}


# ---------------------------------------------------------------------------
# _parse_llm_response tests
# ---------------------------------------------------------------------------


class TestParseLLMResponse:
    def test_parse_valid_tool_call(self):
        response = _make_tool_call_response(
            "get_price", {"token": "ETH", "chain": "arbitrum"}
        )
        result = _parse_llm_response(response, catalog_names=CATALOG_NAMES)
        assert result.tool_name == "get_price"
        assert result.arguments == {"token": "ETH", "chain": "arbitrum"}

    def test_parse_swap_tool_call(self):
        response = _make_tool_call_response(
            "swap_tokens",
            {"token_in": "USDC", "token_out": "ETH", "amount": "5", "chain": "base"},
        )
        result = _parse_llm_response(response, catalog_names=CATALOG_NAMES)
        assert result.tool_name == "swap_tokens"
        assert result.arguments["amount"] == "5"
        assert result.arguments["chain"] == "base"

    def test_text_response_raises_error(self):
        response = _make_text_response("I can help you with that!")
        with pytest.raises(NaturalLanguageError, match="Could not interpret"):
            _parse_llm_response(response, catalog_names=CATALOG_NAMES)

    def test_empty_choices_raises_error(self):
        with pytest.raises(NaturalLanguageError, match="empty response"):
            _parse_llm_response({"choices": []}, catalog_names=CATALOG_NAMES)

    def test_unknown_tool_raises_error(self):
        response = _make_tool_call_response(
            "nonexistent_tool", {"foo": "bar"}
        )
        with pytest.raises(NaturalLanguageError, match="unknown tool"):
            _parse_llm_response(response, catalog_names=CATALOG_NAMES)

    def test_multiple_tool_calls_uses_first(self):
        """When LLM returns multiple tool calls, take the first."""
        response = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "call_1",
                                "type": "function",
                                "function": {
                                    "name": "get_price",
                                    "arguments": json.dumps({"token": "ETH"}),
                                },
                            },
                            {
                                "id": "call_2",
                                "type": "function",
                                "function": {
                                    "name": "get_balance",
                                    "arguments": json.dumps({"token": "USDC"}),
                                },
                            },
                        ],
                    }
                }
            ]
        }
        result = _parse_llm_response(response, catalog_names=CATALOG_NAMES)
        assert result.tool_name == "get_price"

    def test_malformed_arguments_raises_error(self):
        response = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "call_1",
                                "type": "function",
                                "function": {
                                    "name": "get_price",
                                    "arguments": "not valid json{{{",
                                },
                            }
                        ],
                    }
                }
            ]
        }
        with pytest.raises(NaturalLanguageError, match="malformed"):
            _parse_llm_response(response, catalog_names=CATALOG_NAMES)

    def test_explanation_extracted_from_content(self):
        response = _make_tool_call_response(
            "get_price",
            {"token": "ETH"},
            content="I'll check the ETH price for you.",
        )
        result = _parse_llm_response(response, catalog_names=CATALOG_NAMES)
        assert "ETH price" in result.explanation


# ---------------------------------------------------------------------------
# interpret_natural_language tests (with mocked LLM)
# ---------------------------------------------------------------------------


class TestInterpretNaturalLanguage:
    @pytest.mark.asyncio
    async def test_successful_interpretation(self):
        mock_response = _make_tool_call_response(
            "get_price", {"token": "ETH", "chain": "arbitrum"}
        )
        config = LLMConfig(api_key="test-key", base_url="http://test:8080/v1")

        with patch(
            "almanak.framework.cli.ax_natural.LLMClient"
        ) as MockClient:
            instance = MockClient.return_value
            instance.chat = AsyncMock(return_value=mock_response)
            instance.close = AsyncMock()

            result = await interpret_natural_language(
                "what's the price of ETH?", "arbitrum", config
            )

        assert result.tool_name == "get_price"
        assert result.arguments["token"] == "ETH"

    @pytest.mark.asyncio
    async def test_llm_connection_error(self):
        config = LLMConfig(api_key="test-key", base_url="http://unreachable:8080/v1")

        with patch(
            "almanak.framework.cli.ax_natural.LLMClient"
        ) as MockClient:
            instance = MockClient.return_value
            instance.chat = AsyncMock(side_effect=ConnectionError("refused"))
            instance.close = AsyncMock()

            with pytest.raises(LLMConfigError, match="LLM request failed"):
                await interpret_natural_language("swap stuff", "arbitrum", config)

    @pytest.mark.asyncio
    async def test_no_tool_call_returns_error(self):
        mock_response = _make_text_response("I don't understand what you want")
        config = LLMConfig(api_key="test-key", base_url="http://test:8080/v1")

        with patch(
            "almanak.framework.cli.ax_natural.LLMClient"
        ) as MockClient:
            instance = MockClient.return_value
            instance.chat = AsyncMock(return_value=mock_response)
            instance.close = AsyncMock()

            with pytest.raises(NaturalLanguageError, match="Could not interpret"):
                await interpret_natural_language("do something weird", "arbitrum", config)


# ---------------------------------------------------------------------------
# InterpretedAction dataclass
# ---------------------------------------------------------------------------


class TestInterpretedAction:
    def test_defaults(self):
        action = InterpretedAction(tool_name="get_price", arguments={"token": "ETH"})
        assert action.explanation == ""

    def test_with_explanation(self):
        action = InterpretedAction(
            tool_name="swap_tokens",
            arguments={"token_in": "USDC", "token_out": "ETH", "amount": "5"},
            explanation="Swapping USDC to ETH",
        )
        assert action.explanation == "Swapping USDC to ETH"
