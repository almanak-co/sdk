"""Tests for ``almanak.config.agent_tools``.

Phase 6 of the config-service migration. These tests pin the contract
that :func:`agent_tools_config_from_env` is the single env reader for
the agent-tools cluster — the LLM client and bundle-cache helpers under
``almanak/framework/agent_tools/*``:

* Empty env → ``llm_api_key`` is ``None`` and ``llm_base_url`` /
  ``llm_model`` fall back to the documented OpenAI defaults.
* Each documented env var is honoured.
* :class:`SecretStr` wrapping suppresses the API key in ``repr()``
  output; ``model_dump()`` still surfaces the raw value for the
  consumer.
* :func:`cache_dir` honours ``XDG_CACHE_HOME`` and falls back to
  ``home / ".cache"`` otherwise.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from almanak.config.agent_tools import (
    DEFAULT_LLM_BASE_URL,
    DEFAULT_LLM_MODEL,
    AgentToolsConfig,
    agent_tools_config_from_env,
    cache_dir,
)

_AGENT_TOOLS_ENV_VARS: tuple[str, ...] = (
    "AGENT_LLM_API_KEY",
    "AGENT_LLM_BASE_URL",
    "AGENT_LLM_MODEL",
    "XDG_CACHE_HOME",
)


@pytest.fixture(autouse=True)
def _scrub_agent_tools_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Scrub every env var the factory or :func:`cache_dir` reads, plus isolate
    from any repo-local ``.env``.

    Without the scrub these tests are non-deterministic — a developer's
    ``.env`` (or a prior test that called ``setenv``) would silently
    populate a "default" assertion. ``agent_tools_config_from_env`` calls
    ``_load_dotenv_once`` which walks the cwd looking for a ``.env``;
    chdir to an empty ``tmp_path`` so the dotenv loader finds nothing
    (CodeRabbit review on PR 2156).
    """
    monkeypatch.chdir(tmp_path)
    for name in _AGENT_TOOLS_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


# =============================================================================
# Defaults
# =============================================================================


class TestDefaults:
    def test_api_key_defaults_to_none(self):
        cfg = agent_tools_config_from_env()
        assert cfg.llm_api_key is None

    def test_base_url_default(self):
        cfg = agent_tools_config_from_env()
        assert cfg.llm_base_url == DEFAULT_LLM_BASE_URL
        assert cfg.llm_base_url == "https://api.openai.com/v1"

    def test_model_default(self):
        cfg = agent_tools_config_from_env()
        assert cfg.llm_model == DEFAULT_LLM_MODEL
        assert cfg.llm_model == "gpt-4o"


# =============================================================================
# Env overrides — one test per field for a paper trail.
# =============================================================================


class TestEnvOverrides:
    def test_llm_api_key(self, monkeypatch):
        monkeypatch.setenv("AGENT_LLM_API_KEY", "sk-test-secret")
        cfg = agent_tools_config_from_env()
        assert cfg.llm_api_key is not None
        assert cfg.llm_api_key.get_secret_value() == "sk-test-secret"

    def test_llm_api_key_empty_treated_as_unset(self, monkeypatch):
        # Mirrors the legacy ``os.environ.get(..., "")`` → ``not key``
        # branch in the LLM client; an empty string must not produce an
        # empty SecretStr on the typed config.
        monkeypatch.setenv("AGENT_LLM_API_KEY", "")
        assert agent_tools_config_from_env().llm_api_key is None

    def test_llm_base_url(self, monkeypatch):
        monkeypatch.setenv("AGENT_LLM_BASE_URL", "https://api.anthropic.com/v1")
        assert agent_tools_config_from_env().llm_base_url == "https://api.anthropic.com/v1"

    def test_llm_model(self, monkeypatch):
        monkeypatch.setenv("AGENT_LLM_MODEL", "claude-3-5-sonnet-20240620")
        assert agent_tools_config_from_env().llm_model == "claude-3-5-sonnet-20240620"


# =============================================================================
# Secret repr suppression — the API key must never reach a log line.
# =============================================================================


class TestSecretReprSuppression:
    """``llm_api_key`` is :class:`SecretStr` — ``repr`` masks it.

    A stray ``logger.info(f"config={cfg!r}")`` is the most likely
    accident; the model has to make sure that line never leaks the
    key string.
    """

    @pytest.fixture
    def populated(self) -> AgentToolsConfig:
        from pydantic import SecretStr

        return AgentToolsConfig(
            llm_api_key=SecretStr("sk-VERY-SECRET-VALUE"),
            llm_base_url="https://api.test/v1",
            llm_model="test-model",
        )

    def test_repr_suppresses_api_key(self, populated: AgentToolsConfig):
        text = repr(populated)
        assert "sk-VERY-SECRET-VALUE" not in text, f"Secret leaked into repr: {text}"

    def test_get_secret_value_returns_raw(self, populated: AgentToolsConfig):
        # Consumer reads the value via ``.get_secret_value()`` — must
        # surface the original string for the LLM client to use.
        assert populated.llm_api_key is not None
        assert populated.llm_api_key.get_secret_value() == "sk-VERY-SECRET-VALUE"


# =============================================================================
# Forbid extra — typo at the service boundary fails loud.
# =============================================================================


class TestForbidExtra:
    def test_unknown_field_rejected(self):
        with pytest.raises(ValueError):
            AgentToolsConfig(unknown_secret="oops")  # type: ignore[call-arg]


# =============================================================================
# cache_dir boundary helper
# =============================================================================


class TestCacheDir:
    """``cache_dir`` honours ``XDG_CACHE_HOME`` and falls back to
    ``home / ".cache"`` otherwise.

    The helper is the single allowlisted env reader for ``XDG_CACHE_HOME``
    in the SDK; the bundle-cache module imports it instead of reading
    env directly.
    """

    def test_falls_back_to_home_cache_when_xdg_unset(self, monkeypatch, tmp_path):
        monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
        result = cache_dir(tmp_path)
        assert result == tmp_path / ".cache" / "almanak" / "bundles"

    def test_xdg_override_when_set(self, monkeypatch, tmp_path):
        xdg_root = tmp_path / "custom_xdg"
        monkeypatch.setenv("XDG_CACHE_HOME", str(xdg_root))
        result = cache_dir(tmp_path)
        assert result == xdg_root / "almanak" / "bundles"

    def test_xdg_empty_string_treated_as_unset(self, monkeypatch, tmp_path):
        # Mirrors the legacy ``os.environ.get("XDG_CACHE_HOME") or
        # str(Path.home() / ".cache")`` pattern: an empty string falls
        # back to the home-relative default.
        monkeypatch.setenv("XDG_CACHE_HOME", "")
        result = cache_dir(tmp_path)
        assert result == tmp_path / ".cache" / "almanak" / "bundles"

    def test_returns_path_object(self, monkeypatch, tmp_path):
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "x"))
        result = cache_dir(tmp_path)
        assert isinstance(result, Path)


# =============================================================================
# Dotenv ingestion — factory must route through _load_dotenv_once.
# =============================================================================


class TestDotenvIngestion:
    def test_factory_invocation_does_not_raise_with_no_dotenv(self, monkeypatch):
        # Smoke test: the factory must not blow up when the cwd has no
        # ``.env`` file (CI runners and most dev shells).
        cfg = agent_tools_config_from_env()
        assert cfg is not None

    def test_factory_accepts_dotenv_path(self, monkeypatch, tmp_path):
        # The ``dotenv_path`` kwarg threads through to
        # ``_load_dotenv_once``; an unreadable file is silently
        # ignored at the dotenv layer.
        bogus = tmp_path / "missing.env"
        cfg = agent_tools_config_from_env(dotenv_path=str(bogus))
        assert cfg is not None
        # Restore env state for following tests.
        os.environ.pop("AGENT_LLM_API_KEY", None)
