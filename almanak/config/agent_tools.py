"""Typed agent-tools configuration submodel.

Phase 6 of the config-service migration (see
``docs/internal/config-service-plan.md``). Owns every env read for the
agent-tools cluster: the LLM client wired into ``almanak ax --natural``
and the on-disk bundle cache used by the same one-shot CLI flow.

Two surfaces consolidated here:

* **LLM client config** (``llm_api_key``, ``llm_base_url``,
  ``llm_model``). Read by ``framework/agent_tools/llm_client.py`` —
  the OpenAI-compatible HTTP client used by ``almanak ax --natural``
  and the (future) MCP server. ``llm_api_key`` is wrapped in
  ``SecretStr`` so a stray ``logger.info(repr(cfg))`` cannot leak
  the credential.

* **Bundle-cache directory hint** (``XDG_CACHE_HOME`` env override).
  Read by ``framework/agent_tools/bundle_cache.py``. The persistent
  bundle cache that backs ``compile_intent`` →
  ``execute_compiled_bundle`` lives under
  ``${XDG_CACHE_HOME:-~/.cache}/almanak/bundles/``. The XDG override
  itself is a *path resolution* concern, not a config field — the
  helper :func:`cache_dir` is the single allowlisted env reader for
  that lookup.

Import direction
----------------
Strict (mirrors :mod:`almanak.config.connectors` and
:mod:`almanak.config.backtest`): this module MUST NOT import from
``almanak.framework.agent_tools.*``. The agent-tools code imports
:class:`AgentToolsConfig` from here at construction time; reverse
imports would create a cycle and make the typed-config service depend
on the agent-tools layer it is meant to feed.
"""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from almanak.config.env import _load_dotenv_once

# =============================================================================
# Defaults — bit-for-bit mirrors of the legacy module-level constants
# =============================================================================

# Default OpenAI-compatible base URL. Mirrors the legacy
# ``os.environ.get("AGENT_LLM_BASE_URL", "https://api.openai.com/v1")``
# fallback in ``llm_client.py``.
DEFAULT_LLM_BASE_URL: str = "https://api.openai.com/v1"

# Default chat-completion model. Mirrors the legacy
# ``os.environ.get("AGENT_LLM_MODEL", "gpt-4o")`` fallback.
DEFAULT_LLM_MODEL: str = "gpt-4o"


# =============================================================================
# AgentToolsConfig — typed, validated, secret-safe
# =============================================================================


class AgentToolsConfig(BaseModel):
    """Typed configuration for the agent-tools cluster.

    Every field is optional from the consumer's standpoint — when
    ``llm_api_key`` is ``None``, ``validate_llm_config`` raises a typed
    :class:`LLMConfigError` with an actionable message; when the base
    URL or model is unset, the documented OpenAI defaults are used.
    Field-by-field policy is preserved bit-for-bit on cutover; this
    model is the *single env reader*, not a behavioural rewrite.

    The ``llm_api_key`` field is :class:`pydantic.SecretStr` so the
    model's ``__repr__`` and ``model_dump()`` (without
    ``mode="python", context={"reveal_secrets": True}``) emit the
    sentinel ``SecretStr('**********')`` instead of the raw key. The
    consumer reads the value via ``api_key.get_secret_value()``.
    """

    llm_api_key: SecretStr | None = Field(default=None, repr=False)
    """OpenAI-compatible API key (``AGENT_LLM_API_KEY``).

    Required when ``almanak ax --natural`` is invoked.
    ``validate_llm_config`` raises :class:`LLMConfigError` with an
    actionable error message when missing; otherwise the consumer
    treats ``None`` as "no LLM available".
    """

    llm_base_url: str = DEFAULT_LLM_BASE_URL
    """OpenAI-compatible chat-completions base URL (``AGENT_LLM_BASE_URL``).

    Defaults to OpenAI's public endpoint. Operators pointing at
    Anthropic / Ollama / a self-hosted gateway override this.
    """

    llm_model: str = DEFAULT_LLM_MODEL
    """Chat-completion model name (``AGENT_LLM_MODEL``).

    Defaults to ``gpt-4o``. Provider-specific names go here verbatim
    (``claude-3-5-sonnet-20240620``, ``llama3:70b``, etc.).
    """

    model_config = ConfigDict(
        # Reject typos at the service boundary — a misspelt kwarg here
        # would silently flow into the config without populating any
        # consumer field.
        extra="forbid",
    )


# =============================================================================
# Boundary helper — XDG_CACHE_HOME lookup for the bundle cache directory
# =============================================================================


def cache_dir(home: Path) -> Path:
    """Resolve the agent-tools cache directory, honouring ``XDG_CACHE_HOME``.

    Mirrors the legacy ``framework/agent_tools/bundle_cache.py:default_cache_dir``
    helper bit-for-bit:

    1. ``XDG_CACHE_HOME`` env var, when set and non-empty, overrides
       the home-relative default.
    2. Otherwise the cache lives under ``home / ".cache"``.

    The ``almanak/bundles`` suffix is appended so the returned path is
    the per-process bundle-cache directory the consumer actually
    writes to. The env read itself stays in ``almanak.config`` so the
    boundary lint sees a single allowlisted reader for
    ``XDG_CACHE_HOME``.

    Args:
        home: The user's home directory (typically ``Path.home()``).
            Threaded in by the caller so test code can patch home
            without monkey-patching ``Path.home``.
    """
    base = os.environ.get("XDG_CACHE_HOME") or str(home / ".cache")
    return Path(base) / "almanak" / "bundles"


# =============================================================================
# Public factory — single env-reading entry point for agent-tools config
# =============================================================================


def agent_tools_config_from_env(
    *,
    dotenv_path: str | None = None,
) -> AgentToolsConfig:
    """Construct an :class:`AgentToolsConfig` from environment variables.

    Single env-reading entry point for every consumer under
    ``framework/agent_tools/*``. Mirrors the legacy
    ``LLMConfig.from_env`` classmethod bit-for-bit:

    * ``AGENT_LLM_API_KEY`` → ``llm_api_key`` (wrapped in
      :class:`SecretStr`; ``None`` when unset or empty).
    * ``AGENT_LLM_BASE_URL`` → ``llm_base_url`` (default
      :data:`DEFAULT_LLM_BASE_URL`).
    * ``AGENT_LLM_MODEL`` → ``llm_model`` (default
      :data:`DEFAULT_LLM_MODEL`).

    Args:
        dotenv_path: Optional ``.env`` path; routed through the shared
            single-shot loader.
    """
    _load_dotenv_once(dotenv_path)

    raw_api_key = os.environ.get("AGENT_LLM_API_KEY") or None
    return AgentToolsConfig(
        llm_api_key=SecretStr(raw_api_key) if raw_api_key else None,
        llm_base_url=os.environ.get("AGENT_LLM_BASE_URL") or DEFAULT_LLM_BASE_URL,
        llm_model=os.environ.get("AGENT_LLM_MODEL") or DEFAULT_LLM_MODEL,
    )


__all__ = [
    "DEFAULT_LLM_BASE_URL",
    "DEFAULT_LLM_MODEL",
    "AgentToolsConfig",
    "agent_tools_config_from_env",
    "cache_dir",
]
