"""Tests for the shared LLM client used by agentic trading examples."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Add examples to path so we can import shared code
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "examples" / "agentic"))

from shared.llm_client import LLMClient, LLMClientProtocol, LLMConfig, MockLLMClient  # noqa: E402


class TestLLMConfig:
    """Tests for LLMConfig defaults and from_env."""

    def test_defaults(self):
        config = LLMConfig()
        assert config.api_key == ""
        assert config.base_url == "https://api.openai.com/v1"
        assert config.model == "gpt-4o"
        assert config.temperature == 0.1
        assert config.max_tokens == 4096
        assert config.timeout == 60.0

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("AGENT_LLM_API_KEY", "sk-test-key")
        monkeypatch.setenv("AGENT_LLM_BASE_URL", "https://custom.api.com/v1")
        monkeypatch.setenv("AGENT_LLM_MODEL", "gpt-4o-mini")

        config = LLMConfig.from_env()
        assert config.api_key == "sk-test-key"
        assert config.base_url == "https://custom.api.com/v1"
        assert config.model == "gpt-4o-mini"

    def test_from_env_defaults(self, monkeypatch):
        monkeypatch.delenv("AGENT_LLM_API_KEY", raising=False)
        monkeypatch.delenv("AGENT_LLM_BASE_URL", raising=False)
        monkeypatch.delenv("AGENT_LLM_MODEL", raising=False)

        config = LLMConfig.from_env()
        assert config.api_key == ""
        assert config.base_url == "https://api.openai.com/v1"
        assert config.model == "gpt-4o"


class TestMockLLMClient:
    """Tests for MockLLMClient scripted responses."""

    @pytest.mark.asyncio
    async def test_returns_scripted_responses(self):
        responses = [
            {"choices": [{"message": {"content": "hello"}}]},
            {"choices": [{"message": {"content": "world"}}]},
        ]
        mock = MockLLMClient(responses)

        r1 = await mock.chat([{"role": "user", "content": "hi"}])
        assert r1["choices"][0]["message"]["content"] == "hello"

        r2 = await mock.chat([{"role": "user", "content": "bye"}])
        assert r2["choices"][0]["message"]["content"] == "world"

    @pytest.mark.asyncio
    async def test_records_call_log(self):
        mock = MockLLMClient([{"choices": [{"message": {"content": "ok"}}]}])
        messages = [{"role": "user", "content": "test"}]
        tools = [{"type": "function", "function": {"name": "test"}}]

        await mock.chat(messages, tools=tools)

        assert len(mock.call_log) == 1
        assert mock.call_log[0]["messages"] == messages
        assert mock.call_log[0]["tools"] == tools

    @pytest.mark.asyncio
    async def test_exhaustion_raises(self):
        mock = MockLLMClient([{"choices": [{"message": {"content": "only one"}}]}])
        await mock.chat([])  # consume the one response

        with pytest.raises(RuntimeError, match="exhausted"):
            await mock.chat([])

    @pytest.mark.asyncio
    async def test_empty_responses(self):
        mock = MockLLMClient([])
        with pytest.raises(RuntimeError, match="exhausted"):
            await mock.chat([])


class TestLLMClientProtocol:
    """Tests for the LLMClientProtocol type checking."""

    def test_mock_implements_protocol(self):
        mock = MockLLMClient([])
        assert isinstance(mock, LLMClientProtocol)

    def test_real_client_implements_protocol(self):
        client = LLMClient(LLMConfig(api_key="test"))
        assert isinstance(client, LLMClientProtocol)


class TestLLMClient:
    """Tests for the real LLMClient (mocked HTTP)."""

    @pytest.mark.asyncio
    async def test_chat_sends_correct_payload(self):
        config = LLMConfig(api_key="sk-test", model="gpt-4o")
        client = LLMClient(config)

        expected_response = {"choices": [{"message": {"content": "response"}}]}

        with patch.object(client._http, "post", new_callable=AsyncMock) as mock_post:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = expected_response
            mock_resp.raise_for_status.return_value = None
            mock_post.return_value = mock_resp

            result = await client.chat(
                [{"role": "user", "content": "hello"}],
                tools=[{"type": "function", "function": {"name": "test"}}],
            )

            assert result == expected_response
            mock_post.assert_called_once()
            call_kwargs = mock_post.call_args
            payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
            assert payload["model"] == "gpt-4o"
            assert payload["temperature"] == 0.1
            assert len(payload["messages"]) == 1
            assert payload["tools"] is not None
            assert payload["tool_choice"] == "auto"

        await client.close()
