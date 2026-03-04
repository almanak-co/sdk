"""Thin LLM client for agentic trading examples.

Uses httpx to call OpenAI-compatible chat completions endpoints.
No new dependencies -- httpx is already in the almanak-sdk dependency tree.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

import httpx

logger = logging.getLogger(__name__)


class LLMConfigError(Exception):
    """Raised when LLM configuration is missing or invalid.

    Provides actionable error messages for common misconfigurations.
    """


@dataclass
class LLMConfig:
    """Configuration for the LLM client."""

    api_key: str = ""
    base_url: str = "https://api.openai.com/v1"
    model: str = "gpt-4o"
    temperature: float = 0.1
    max_tokens: int = 4096
    timeout: float = 60.0

    @classmethod
    def from_env(cls) -> LLMConfig:
        """Load config from environment variables."""
        return cls(
            api_key=os.environ.get("AGENT_LLM_API_KEY", ""),
            base_url=os.environ.get("AGENT_LLM_BASE_URL", "https://api.openai.com/v1"),
            model=os.environ.get("AGENT_LLM_MODEL", "gpt-4o"),
        )


@runtime_checkable
class LLMClientProtocol(Protocol):
    """Protocol for LLM clients -- real or mock."""

    async def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]: ...


class LLMClient:
    """Async LLM client using httpx against OpenAI-compatible endpoints."""

    def __init__(self, config: LLMConfig) -> None:
        self._config = config
        self._http = httpx.AsyncClient(
            base_url=config.base_url,
            headers={
                "Authorization": f"Bearer {config.api_key}",
                "Content-Type": "application/json",
            },
            timeout=config.timeout,
        )

    async def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Send a chat completion request with retry for transient errors.

        Retries on:
        - 429 (rate limited) -- respects Retry-After header
        - 5xx (server errors)
        - Network timeouts and connection errors

        Does NOT retry on 4xx (except 429) -- those indicate bad requests.
        """
        import asyncio as _asyncio

        # Newer OpenAI models (gpt-5+, o-series) require max_completion_tokens
        # instead of max_tokens. Use max_completion_tokens for all models since
        # the API accepts it universally for newer endpoints.
        token_param = "max_completion_tokens" if "gpt-5" in self._config.model or "o1" in self._config.model or "o3" in self._config.model else "max_tokens"
        payload: dict[str, Any] = {
            "model": self._config.model,
            "messages": messages,
            "temperature": self._config.temperature,
            token_param: self._config.max_tokens,
        }
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = "auto"

        max_retries = 3
        base_delay = 1.0
        last_error: Exception | None = None
        retry_after_override: float | None = None

        for attempt in range(max_retries + 1):
            try:
                logger.debug("LLM request: model=%s, messages=%d, tools=%d (attempt %d)",
                              self._config.model, len(messages), len(tools or []), attempt + 1)
                resp = await self._http.post("/chat/completions", json=payload)

                if resp.status_code == 429:
                    try:
                        retry_after_override = float(resp.headers.get("Retry-After", "5"))
                    except (ValueError, TypeError):
                        retry_after_override = 5.0  # HTTP-date or unparseable
                    raise httpx.TimeoutException(f"Rate limited, retry after {retry_after_override}s")
                if resp.status_code >= 500:
                    logger.error("LLM API server error %d: %s", resp.status_code, resp.text)
                    raise httpx.TimeoutException(f"Server error {resp.status_code}")
                if resp.status_code >= 400:
                    logger.error("LLM API error %d: %s", resp.status_code, resp.text)
                resp.raise_for_status()
                return resp.json()
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                last_error = e
                if attempt >= max_retries:
                    logger.error("LLM API failed after %d attempts: %s", max_retries + 1, e)
                    raise
                if retry_after_override is not None:
                    delay = min(retry_after_override, 30.0)
                    retry_after_override = None
                else:
                    delay = min(base_delay * (2 ** attempt), 30.0)
                logger.warning("LLM API error (attempt %d/%d): %s -- retrying in %.1fs",
                               attempt + 1, max_retries + 1, e, delay)
                await _asyncio.sleep(delay)

        # Unreachable, but satisfies type checker
        raise last_error or RuntimeError("LLM retry exhausted")

    async def preflight_check(self) -> None:
        """Send a minimal request to verify the LLM endpoint is reachable and the API key is valid.

        Raises LLMConfigError with actionable message on failure.
        """
        try:
            resp = await self._http.post(
                "/chat/completions",
                json={
                    "model": self._config.model,
                    "messages": [{"role": "system", "content": "ping"}],
                    "max_tokens": 1,
                },
                timeout=15.0,
            )
            if resp.status_code in (401, 403):
                raise LLMConfigError(
                    f"Invalid AGENT_LLM_API_KEY (HTTP {resp.status_code}).\n"
                    "Check your key and ensure it has access to the configured model.\n"
                    f"Endpoint: {self._config.base_url}"
                )
            if resp.status_code == 404:
                raise LLMConfigError(
                    f"Model '{self._config.model}' not found at {self._config.base_url} (HTTP 404).\n"
                    "Check AGENT_LLM_MODEL is correct for your provider."
                )
            if resp.status_code >= 400:
                raise LLMConfigError(
                    f"LLM endpoint returned HTTP {resp.status_code}.\n"
                    f"Endpoint: {self._config.base_url}\n"
                    f"Response: {resp.text[:200]}"
                )
        except httpx.ConnectError:
            raise LLMConfigError(
                f"Cannot reach LLM at {self._config.base_url}.\n"
                "Is the endpoint running? Check AGENT_LLM_BASE_URL."
            )
        except httpx.TimeoutException:
            raise LLMConfigError(
                f"LLM endpoint at {self._config.base_url} timed out.\n"
                "Check network connectivity and AGENT_LLM_BASE_URL."
            )

    async def close(self) -> None:
        await self._http.aclose()


async def validate_llm_config(config: LLMConfig) -> None:
    """Fail-fast validation: check API key is set, then ping the endpoint.

    Call this at the top of run_once() (before gateway connection) to give
    users an immediate, actionable error instead of failing mid-execution.
    """
    if not config.api_key:
        raise LLMConfigError(
            "No LLM API key configured.\n\n"
            "Agentic strategies require your own LLM API key.\n"
            "Set it via environment variable:\n\n"
            "  export AGENT_LLM_API_KEY=sk-...\n\n"
            "Any OpenAI-compatible provider works (OpenAI, Anthropic, Ollama, etc.).\n"
            "See: https://docs.almanak.co/agentic/"
        )
    client = LLMClient(config)
    try:
        await client.preflight_check()
    finally:
        await client.close()


class MockLLMClient:
    """Scripted mock for testing -- returns pre-configured responses in order."""

    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self._responses = list(responses)
        self._index = 0
        self.call_log: list[dict[str, Any]] = []

    async def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Return the next scripted response."""
        self.call_log.append({"messages": messages, "tools": tools})
        if self._index >= len(self._responses):
            raise RuntimeError(
                f"MockLLMClient exhausted: {len(self._responses)} responses "
                f"configured but call #{self._index + 1} requested"
            )
        response = self._responses[self._index]
        self._index += 1
        return response


RoundFn = Any  # Callable[[dict], dict] -- avoids import issues


class DynamicMockLLMClient:
    """Mock LLM that chains tool outputs into subsequent calls.

    Each round is a function that receives accumulated context from previous
    tool results and returns the next mock response. This allows later rounds
    to reference real values (e.g., the vault address deployed in round 2).

    Usage:
        def round_1(ctx):
            return mock_response(mock_tool_call("get_price", {"token": "ETH"}))

        def round_2(ctx):
            # ctx["vault_address"] was extracted from round 1 results
            return mock_response(mock_tool_call("get_vault_state", {
                "vault_address": ctx["vault_address"],
            }))

        llm = DynamicMockLLMClient([round_1, round_2])
    """

    def __init__(self, rounds: list[RoundFn]) -> None:
        self._rounds = rounds
        self._round_idx = 0
        self._context: dict[str, Any] = {}
        self.call_log: list[dict[str, Any]] = []

    async def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Extract tool results from messages, then call the next round function."""
        self.call_log.append({"messages": messages, "tools": tools})

        # Extract tool results from assistant/tool messages into context
        self._extract_context(messages)

        if self._round_idx >= len(self._rounds):
            raise RuntimeError(
                f"DynamicMockLLMClient exhausted: {len(self._rounds)} rounds "
                f"configured but round #{self._round_idx + 1} requested"
            )

        round_fn = self._rounds[self._round_idx]
        self._round_idx += 1
        return round_fn(self._context)

    def _extract_context(self, messages: list[dict[str, Any]]) -> None:
        """Parse tool result messages and accumulate key values in context."""
        import json as _json

        for msg in messages:
            if msg.get("role") != "tool":
                continue
            content = msg.get("content", "")
            try:
                data = _json.loads(content) if isinstance(content, str) else content
            except (_json.JSONDecodeError, TypeError):
                continue

            if not isinstance(data, dict):
                continue

            result_data = data.get("data", data)
            if not isinstance(result_data, dict):
                continue

            # Extract well-known fields
            if "vault_address" in result_data and result_data["vault_address"]:
                self._context["vault_address"] = result_data["vault_address"]
            if "tx_hash" in result_data and result_data["tx_hash"]:
                self._context.setdefault("tx_hashes", []).append(result_data["tx_hash"])
            if "balance" in result_data:
                token = result_data.get("token", "unknown")
                self._context[f"balance_{token}"] = result_data["balance"]
            if "price_usd" in result_data:
                token = result_data.get("token", "unknown")
                self._context[f"price_{token}"] = result_data["price_usd"]
            if "position_id" in result_data and result_data.get("position_id"):
                self._context["position_id"] = result_data["position_id"]
            if "current_price" in result_data and result_data["current_price"]:
                try:
                    price = float(result_data["current_price"])
                    if price > 0:
                        self._context["current_price"] = price
                except (ValueError, TypeError):
                    pass
            if "state" in result_data:
                self._context["loaded_state"] = result_data["state"]
