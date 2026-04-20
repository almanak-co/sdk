"""Thin LLM client for agent tool execution.

Uses httpx to call OpenAI-compatible chat completions endpoints.
No new dependencies -- httpx is already in the almanak-sdk dependency tree.

Promoted from ``examples/agentic/shared/llm_client.py`` to make it a
first-class framework dependency (used by ``almanak ax --natural``).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
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

        # Use max_tokens which is universally supported across OpenAI-compatible
        # APIs. Providers that require max_completion_tokens also accept max_tokens.
        payload: dict[str, Any] = {
            "model": self._config.model,
            "messages": messages,
            "temperature": self._config.temperature,
            "max_tokens": self._config.max_tokens,
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
                logger.debug(
                    "LLM request: model=%s, messages=%d, tools=%d (attempt %d)",
                    self._config.model,
                    len(messages),
                    len(tools or []),
                    attempt + 1,
                )
                resp = await self._http.post("/chat/completions", json=payload)

                # Retriable status codes: rate limit (429) and server errors (5xx)
                if resp.status_code == 429:
                    try:
                        retry_after_override = float(resp.headers.get("Retry-After", "5"))
                    except (ValueError, TypeError):
                        retry_after_override = 5.0
                    last_error = httpx.HTTPStatusError("Rate limited (429)", request=resp.request, response=resp)
                elif resp.status_code >= 500:
                    logger.error("LLM API server error %d: %s", resp.status_code, resp.text)
                    last_error = httpx.HTTPStatusError(
                        f"Server error ({resp.status_code})", request=resp.request, response=resp
                    )
                else:
                    if resp.status_code >= 400:
                        logger.error("LLM API error %d: %s", resp.status_code, resp.text)
                    resp.raise_for_status()
                    return resp.json()

                # Fall through to retry logic for 429/5xx
                if attempt >= max_retries:
                    logger.error("LLM API failed after %d attempts: %s", max_retries + 1, last_error)
                    raise last_error
                delay = min(retry_after_override or base_delay * (2**attempt), 30.0)
                retry_after_override = None
                logger.warning(
                    "LLM API error (attempt %d/%d): %s -- retrying in %.1fs",
                    attempt + 1,
                    max_retries + 1,
                    last_error,
                    delay,
                )
                await _asyncio.sleep(delay)
                continue
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                last_error = e
                if attempt >= max_retries:
                    logger.exception("LLM API failed after %d attempts", max_retries + 1)
                    raise
                delay = min(base_delay * (2**attempt), 30.0)
                logger.warning(
                    "LLM API error (attempt %d/%d): %s -- retrying in %.1fs", attempt + 1, max_retries + 1, e, delay
                )
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
        except httpx.ConnectError as exc:
            raise LLMConfigError(
                f"Cannot reach LLM at {self._config.base_url}.\nIs the endpoint running? Check AGENT_LLM_BASE_URL."
            ) from exc
        except httpx.TimeoutException as exc:
            raise LLMConfigError(
                f"LLM endpoint at {self._config.base_url} timed out.\n"
                "Check network connectivity and AGENT_LLM_BASE_URL."
            ) from exc

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
            "Natural language mode requires an LLM API key.\n"
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
