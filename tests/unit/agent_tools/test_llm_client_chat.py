"""Branch coverage for ``LLMClient.chat`` and friends in ``llm_client.py``.

No network calls: every test swaps the client's httpx transport for an
``httpx.MockTransport`` driven by a scripted handler. Swapping only the
transport (not the whole ``AsyncClient``) keeps the production wiring --
base_url join, Authorization header, timeout -- intact and assertable.

Retry sleeps are captured by monkeypatching ``asyncio.sleep`` (``chat`` does
``import asyncio as _asyncio`` per call, so the module attribute is the seam).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest

import almanak.framework.agent_tools.llm_client as llm_client_mod
from almanak.framework.agent_tools.llm_client import (
    LLMClient,
    LLMConfig,
    LLMConfigError,
    validate_llm_config,
)

BASE_URL = "https://llm.test/v1"


class _Script:
    """Scripted transport handler: each step is a Response or an Exception."""

    def __init__(self, steps: list[Any]) -> None:
        self.steps = list(steps)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        assert self.steps, "handler called more times than scripted"
        step = self.steps.pop(0)
        if isinstance(step, Exception):
            raise step
        return step


def _scripted_client(steps: list[Any], config: LLMConfig | None = None) -> tuple[LLMClient, _Script]:
    config = config or LLMConfig(api_key="test-key", base_url=BASE_URL)
    client = LLMClient(config)
    script = _Script(steps)
    client._http._transport = httpx.MockTransport(script)
    return client, script


async def _chat(
    client: LLMClient,
    messages: list[dict[str, Any]] | None = None,
    tools: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    try:
        return await client.chat(
            messages if messages is not None else [{"role": "user", "content": "hi"}],
            tools=tools,
        )
    finally:
        await client.close()


async def _preflight(client: LLMClient) -> None:
    try:
        await client.preflight_check()
    finally:
        await client.close()


@pytest.fixture
def sleep_delays(monkeypatch) -> list[float]:
    """Capture retry sleeps without actually waiting."""
    delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    return delays


# ---------------------------------------------------------------------------
# chat: happy paths
# ---------------------------------------------------------------------------


class TestChatHappyPath:
    def test_payload_assembly_and_response_passthrough(self):
        body = {"choices": [{"message": {"role": "assistant", "content": "hello"}}]}
        config = LLMConfig(
            api_key="sk-unit",
            base_url=BASE_URL,
            model="test-model",
            temperature=0.42,
            max_tokens=123,
        )
        client, script = _scripted_client([httpx.Response(200, json=body)], config=config)
        messages = [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": "hi"},
        ]

        result = asyncio.run(_chat(client, messages=messages))

        assert result == body
        assert len(script.requests) == 1
        request = script.requests[0]
        assert request.url == f"{BASE_URL}/chat/completions"
        assert request.headers["Authorization"] == "Bearer sk-unit"
        payload = json.loads(request.content)
        assert payload == {
            "model": "test-model",
            "messages": messages,
            "temperature": 0.42,
            "max_tokens": 123,
        }

    def test_tools_propagated_with_auto_tool_choice(self):
        tools = [{"type": "function", "function": {"name": "get_price", "parameters": {}}}]
        body = {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "call_1",
                                "type": "function",
                                "function": {"name": "get_price", "arguments": "{}"},
                            }
                        ],
                    }
                }
            ]
        }
        client, script = _scripted_client([httpx.Response(200, json=body)])

        result = asyncio.run(_chat(client, tools=tools))

        assert result == body
        payload = json.loads(script.requests[0].content)
        assert payload["tools"] == tools
        assert payload["tool_choice"] == "auto"

    def test_empty_tools_list_omitted_from_payload(self):
        client, script = _scripted_client([httpx.Response(200, json={"choices": []})])

        asyncio.run(_chat(client, tools=[]))

        payload = json.loads(script.requests[0].content)
        assert "tools" not in payload
        assert "tool_choice" not in payload


# ---------------------------------------------------------------------------
# chat: retry logic (429 / 5xx)
# ---------------------------------------------------------------------------


class TestChatRetries:
    @pytest.mark.parametrize(
        ("headers", "expected_delay"),
        [
            ({"Retry-After": "2.5"}, 2.5),
            ({"Retry-After": "not-a-number"}, 5.0),
            ({}, 5.0),  # header absent -> default "5"
            ({"Retry-After": "100"}, 30.0),  # capped at 30s
        ],
    )
    def test_429_retries_honouring_retry_after(self, sleep_delays, headers, expected_delay):
        client, script = _scripted_client(
            [
                httpx.Response(429, headers=headers, json={"error": "rate limited"}),
                httpx.Response(200, json={"choices": []}),
            ]
        )

        result = asyncio.run(_chat(client))

        assert result == {"choices": []}
        assert len(script.requests) == 2
        assert sleep_delays == [expected_delay]

    def test_429_exhausts_retries(self, sleep_delays):
        client, script = _scripted_client([httpx.Response(429, json={}) for _ in range(4)])

        with pytest.raises(httpx.HTTPStatusError, match=r"Rate limited \(429\)"):
            asyncio.run(_chat(client))

        assert len(script.requests) == 4
        assert sleep_delays == [5.0, 5.0, 5.0]

    def test_5xx_retries_then_success(self, sleep_delays):
        client, script = _scripted_client(
            [
                httpx.Response(500, text="boom"),
                httpx.Response(200, json={"choices": []}),
            ]
        )

        result = asyncio.run(_chat(client))

        assert result == {"choices": []}
        assert len(script.requests) == 2
        assert sleep_delays == [1.0]

    def test_5xx_exponential_backoff_then_exhaustion(self, sleep_delays):
        client, script = _scripted_client([httpx.Response(503, text="down") for _ in range(4)])

        with pytest.raises(httpx.HTTPStatusError, match=r"Server error \(503\)"):
            asyncio.run(_chat(client))

        assert len(script.requests) == 4
        assert sleep_delays == [1.0, 2.0, 4.0]

    def test_retry_after_override_resets_between_attempts(self, sleep_delays):
        """A 429 Retry-After must not leak into the following 5xx backoff."""
        client, script = _scripted_client(
            [
                httpx.Response(429, headers={"Retry-After": "9"}, json={}),
                httpx.Response(500, text="boom"),
                httpx.Response(200, json={"choices": []}),
            ]
        )

        result = asyncio.run(_chat(client))

        assert result == {"choices": []}
        assert len(script.requests) == 3
        # attempt 0: Retry-After 9; attempt 1: exponential 1.0 * 2**1 = 2.0
        assert sleep_delays == [9.0, 2.0]

    @pytest.mark.parametrize("status", [400, 404])
    def test_4xx_raises_immediately_without_retry(self, sleep_delays, status):
        client, script = _scripted_client([httpx.Response(status, json={"error": "bad request"})])

        with pytest.raises(httpx.HTTPStatusError):
            asyncio.run(_chat(client))

        assert len(script.requests) == 1
        assert sleep_delays == []


# ---------------------------------------------------------------------------
# chat: network errors and malformed bodies
# ---------------------------------------------------------------------------


class TestChatNetworkErrors:
    @pytest.mark.parametrize("exc_type", [httpx.ReadTimeout, httpx.ConnectError])
    def test_transient_network_error_retries_then_success(self, sleep_delays, exc_type):
        client, script = _scripted_client(
            [
                exc_type("transient"),
                httpx.Response(200, json={"choices": []}),
            ]
        )

        result = asyncio.run(_chat(client))

        assert result == {"choices": []}
        assert len(script.requests) == 2
        assert sleep_delays == [1.0]

    def test_connect_error_exhausts_retries(self, sleep_delays):
        client, script = _scripted_client([httpx.ConnectError("refused") for _ in range(4)])

        with pytest.raises(httpx.ConnectError, match="refused"):
            asyncio.run(_chat(client))

        assert len(script.requests) == 4
        assert sleep_delays == [1.0, 2.0, 4.0]

    def test_timeout_exhausts_retries(self, sleep_delays):
        client, script = _scripted_client([httpx.ReadTimeout("slow") for _ in range(4)])

        with pytest.raises(httpx.ReadTimeout, match="slow"):
            asyncio.run(_chat(client))

        assert len(script.requests) == 4
        assert sleep_delays == [1.0, 2.0, 4.0]

    def test_malformed_json_body_raises_value_error(self, sleep_delays):
        client, script = _scripted_client(
            [httpx.Response(200, content=b"not json", headers={"Content-Type": "application/json"})]
        )

        with pytest.raises(ValueError):
            asyncio.run(_chat(client))

        assert len(script.requests) == 1
        assert sleep_delays == []


# ---------------------------------------------------------------------------
# preflight_check
# ---------------------------------------------------------------------------


class TestPreflightCheck:
    def test_success_is_silent(self):
        client, script = _scripted_client([httpx.Response(200, json={"choices": []})])

        asyncio.run(_preflight(client))

        payload = json.loads(script.requests[0].content)
        assert payload["max_tokens"] == 1
        assert payload["messages"] == [{"role": "system", "content": "ping"}]

    @pytest.mark.parametrize("status", [401, 403])
    def test_auth_failure_gives_actionable_error(self, status):
        client, _ = _scripted_client([httpx.Response(status, json={})])

        with pytest.raises(LLMConfigError, match="Invalid AGENT_LLM_API_KEY"):
            asyncio.run(_preflight(client))

    def test_model_not_found(self):
        client, _ = _scripted_client([httpx.Response(404, json={})])

        with pytest.raises(LLMConfigError, match="not found"):
            asyncio.run(_preflight(client))

    def test_other_4xx_5xx_generic_error(self):
        client, _ = _scripted_client([httpx.Response(500, text="oops")])

        with pytest.raises(LLMConfigError, match="returned HTTP 500"):
            asyncio.run(_preflight(client))

    def test_connect_error_maps_to_config_error(self):
        client, _ = _scripted_client([httpx.ConnectError("refused")])

        with pytest.raises(LLMConfigError, match="Cannot reach LLM"):
            asyncio.run(_preflight(client))

    def test_timeout_maps_to_config_error(self):
        client, _ = _scripted_client([httpx.ReadTimeout("slow")])

        with pytest.raises(LLMConfigError, match="timed out"):
            asyncio.run(_preflight(client))


# ---------------------------------------------------------------------------
# LLMConfig.from_env
# ---------------------------------------------------------------------------


@pytest.fixture
def _no_dotenv(monkeypatch):
    """Keep from_env deterministic: never merge a developer .env into os.environ."""
    monkeypatch.setattr("almanak.config.agent_tools._load_dotenv_once", lambda *a, **k: None)


@pytest.mark.usefixtures("_no_dotenv")
class TestLLMConfigFromEnv:
    def test_reads_env_vars(self, monkeypatch):
        monkeypatch.setenv("AGENT_LLM_API_KEY", "sk-env")
        monkeypatch.setenv("AGENT_LLM_BASE_URL", "https://ollama.test/v1")
        monkeypatch.setenv("AGENT_LLM_MODEL", "llama3:70b")

        config = LLMConfig.from_env()

        assert config.api_key == "sk-env"
        assert config.base_url == "https://ollama.test/v1"
        assert config.model == "llama3:70b"
        # Fields not sourced from env keep dataclass defaults.
        assert config.temperature == 0.1
        assert config.max_tokens == 4096
        assert config.timeout == 60.0

    def test_defaults_when_unset(self, monkeypatch):
        monkeypatch.delenv("AGENT_LLM_API_KEY", raising=False)
        monkeypatch.delenv("AGENT_LLM_BASE_URL", raising=False)
        monkeypatch.delenv("AGENT_LLM_MODEL", raising=False)

        config = LLMConfig.from_env()

        assert config.api_key == ""
        assert config.base_url == "https://api.openai.com/v1"
        assert config.model == "gpt-4o"

    def test_empty_api_key_treated_as_missing(self, monkeypatch):
        monkeypatch.setenv("AGENT_LLM_API_KEY", "")
        monkeypatch.delenv("AGENT_LLM_BASE_URL", raising=False)
        monkeypatch.delenv("AGENT_LLM_MODEL", raising=False)

        config = LLMConfig.from_env()

        assert config.api_key == ""


# ---------------------------------------------------------------------------
# validate_llm_config
# ---------------------------------------------------------------------------


class _StubLLMClient:
    """Records preflight/close calls; optionally raises from preflight."""

    last_instance: _StubLLMClient | None = None
    preflight_error: Exception | None = None

    def __init__(self, config: LLMConfig) -> None:
        self.config = config
        self.preflight_calls = 0
        self.closed = False
        type(self).last_instance = self

    async def preflight_check(self) -> None:
        self.preflight_calls += 1
        if type(self).preflight_error is not None:
            raise type(self).preflight_error

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def stub_llm_client(monkeypatch):
    monkeypatch.setattr(llm_client_mod, "LLMClient", _StubLLMClient)
    _StubLLMClient.last_instance = None
    _StubLLMClient.preflight_error = None
    yield _StubLLMClient
    _StubLLMClient.last_instance = None
    _StubLLMClient.preflight_error = None


class TestValidateLLMConfig:
    def test_missing_api_key_fails_fast(self):
        with pytest.raises(LLMConfigError, match="No LLM API key configured"):
            asyncio.run(validate_llm_config(LLMConfig(api_key="")))

    def test_valid_key_runs_preflight_and_closes(self, stub_llm_client):
        asyncio.run(validate_llm_config(LLMConfig(api_key="sk-ok")))

        instance = stub_llm_client.last_instance
        assert instance is not None
        assert instance.preflight_calls == 1
        assert instance.closed is True

    def test_preflight_failure_propagates_but_still_closes(self, stub_llm_client):
        stub_llm_client.preflight_error = LLMConfigError("endpoint unreachable")

        with pytest.raises(LLMConfigError, match="endpoint unreachable"):
            asyncio.run(validate_llm_config(LLMConfig(api_key="sk-bad")))

        instance = stub_llm_client.last_instance
        assert instance is not None
        assert instance.closed is True
