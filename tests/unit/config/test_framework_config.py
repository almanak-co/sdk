"""Tests for ``almanak.config.framework``.

Phase 6 of the config-service migration. These tests pin the contract
that :func:`framework_config_from_env` is the single env reader for the
framework-toggle cluster: log emojis, strategy / accounting paths,
API-key validator list, dashboard auth, Anvil fork timeouts, and the
token-resolver negative-cache knobs.

* Empty env → boolean defaults to ``True``, paths default to ``None``,
  ``api_keys`` is empty, ``dashboard_api_key`` is ``None``, fork
  timeouts use the documented defaults.
* Each documented env var is honoured.
* Truthy / falsy parsing for ``ALMANAK_LOG_EMOJIS`` matches the legacy
  ladder bit-for-bit.
* :class:`SecretStr` wrapping suppresses ``dashboard_api_key`` and
  ``api_keys`` is ``repr=False``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from almanak.config.framework import (
    DEFAULT_ANVIL_FORK_HEALTH_TIMEOUT_S,
    DEFAULT_ANVIL_FORK_RPC_TIMEOUT_S,
    FrameworkConfig,
    framework_config_from_env,
)

_FRAMEWORK_ENV_VARS: tuple[str, ...] = (
    "ALMANAK_LOG_EMOJIS",
    "ALMANAK_STRATEGIES_DIR",
    "ALMANAK_ACCOUNTING_DIR",
    "ALMANAK_API_KEYS",
    "ALMANAK_DASHBOARD_API_KEY",
    "ANVIL_FORK_CACHE_PATH",
    "ALMANAK_FORK_RPC_TIMEOUT",
    "ALMANAK_FORK_HEALTH_TIMEOUT",
    "ALMANAK_TOKEN_NEGATIVE_CACHE_TTL_S",
    "ALMANAK_TOKEN_NEGATIVE_CACHE_MAX",
)


@pytest.fixture(autouse=True)
def _scrub_framework_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Scrub every env var the factory reads, plus isolate from any repo-local ``.env``.

    Without the scrub these tests are non-deterministic — a developer's
    ``.env`` (or a prior test that called ``setenv``) would silently
    populate a "default" assertion. ``framework_config_from_env`` calls
    ``_load_dotenv_once`` which walks the cwd looking for a ``.env``;
    chdir to an empty ``tmp_path`` so the dotenv loader finds nothing
    and the just-scrubbed env stays scrubbed (CodeRabbit review on
    PR 2156).
    """
    monkeypatch.chdir(tmp_path)
    for name in _FRAMEWORK_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


# =============================================================================
# Defaults
# =============================================================================


class TestDefaults:
    def test_log_emojis_default_true(self):
        cfg = framework_config_from_env()
        assert cfg.log_emojis is True

    def test_paths_default_to_none(self):
        cfg = framework_config_from_env()
        assert cfg.strategies_dir is None
        assert cfg.accounting_dir is None
        assert cfg.anvil_fork_cache_path is None

    def test_api_keys_default_empty(self):
        cfg = framework_config_from_env()
        assert cfg.api_keys == ()

    def test_dashboard_api_key_default_none(self):
        cfg = framework_config_from_env()
        assert cfg.dashboard_api_key is None

    def test_anvil_fork_timeouts_defaults(self):
        cfg = framework_config_from_env()
        assert cfg.anvil_fork_rpc_timeout_seconds == DEFAULT_ANVIL_FORK_RPC_TIMEOUT_S
        assert cfg.anvil_fork_rpc_timeout_seconds == 8.0
        assert cfg.anvil_fork_health_timeout_seconds == DEFAULT_ANVIL_FORK_HEALTH_TIMEOUT_S
        assert cfg.anvil_fork_health_timeout_seconds == 5.0

    def test_token_cache_knobs_default_none(self):
        cfg = framework_config_from_env()
        assert cfg.token_negative_cache_ttl_s is None
        assert cfg.token_negative_cache_max is None


# =============================================================================
# ALMANAK_LOG_EMOJIS — falsy ladder.
# =============================================================================


class TestLogEmojis:
    """Mirror of the legacy
    ``not in ("false", "0", "no")`` ladder in
    ``framework/utils/log_formatters.py``.
    """

    @pytest.mark.parametrize("falsy", ["false", "FALSE", "False", "0", "no", "NO", "  false  "])
    def test_falsy_values_disable(self, monkeypatch, falsy):
        monkeypatch.setenv("ALMANAK_LOG_EMOJIS", falsy)
        assert framework_config_from_env().log_emojis is False

    @pytest.mark.parametrize("truthy", ["true", "1", "yes", "anything-else", "  on  "])
    def test_truthy_or_unrecognised_keeps_default(self, monkeypatch, truthy):
        monkeypatch.setenv("ALMANAK_LOG_EMOJIS", truthy)
        assert framework_config_from_env().log_emojis is True

    def test_unset_keeps_default(self, monkeypatch):
        monkeypatch.delenv("ALMANAK_LOG_EMOJIS", raising=False)
        assert framework_config_from_env().log_emojis is True


# =============================================================================
# Path overrides — relative paths NOT pre-resolved.
# =============================================================================


class TestPathOverrides:
    def test_strategies_dir_relative_preserved(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_STRATEGIES_DIR", "custom/strategies")
        cfg = framework_config_from_env()
        assert cfg.strategies_dir == Path("custom/strategies")
        # The legacy code resolves relative paths against ``Path.cwd()``
        # at consumer time — the typed config preserves the original
        # path so the consumer can apply that resolution itself.
        assert not cfg.strategies_dir.is_absolute()

    def test_strategies_dir_absolute_preserved(self, monkeypatch, tmp_path):
        monkeypatch.setenv("ALMANAK_STRATEGIES_DIR", str(tmp_path))
        cfg = framework_config_from_env()
        assert cfg.strategies_dir == tmp_path

    def test_accounting_dir(self, monkeypatch, tmp_path):
        monkeypatch.setenv("ALMANAK_ACCOUNTING_DIR", str(tmp_path))
        assert framework_config_from_env().accounting_dir == tmp_path

    def test_anvil_fork_cache_path(self, monkeypatch, tmp_path):
        monkeypatch.setenv("ANVIL_FORK_CACHE_PATH", str(tmp_path))
        assert framework_config_from_env().anvil_fork_cache_path == tmp_path

    def test_empty_string_treated_as_unset(self, monkeypatch):
        # Mirrors the legacy ``os.environ.get(..., None) or None`` guard
        # in the consumer — empty string must not produce a
        # ``Path("")`` which silently behaves like cwd.
        monkeypatch.setenv("ALMANAK_STRATEGIES_DIR", "")
        assert framework_config_from_env().strategies_dir is None


# =============================================================================
# ALMANAK_API_KEYS — CSV split.
# =============================================================================


class TestApiKeys:
    def test_single_key(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_API_KEYS", "key-a")
        assert framework_config_from_env().api_keys == ("key-a",)

    def test_multiple_keys(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_API_KEYS", "key-a,key-b,key-c")
        assert framework_config_from_env().api_keys == ("key-a", "key-b", "key-c")

    def test_whitespace_stripped(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_API_KEYS", "  key-a , key-b  ,  key-c  ")
        assert framework_config_from_env().api_keys == ("key-a", "key-b", "key-c")

    def test_empty_entries_dropped(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_API_KEYS", "key-a,,key-b,")
        assert framework_config_from_env().api_keys == ("key-a", "key-b")

    def test_empty_string_yields_empty_tuple(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_API_KEYS", "")
        assert framework_config_from_env().api_keys == ()


# =============================================================================
# ALMANAK_DASHBOARD_API_KEY — SecretStr.
# =============================================================================


class TestDashboardApiKey:
    def test_set_value_wrapped_in_secretstr(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_DASHBOARD_API_KEY", "dash-secret")
        cfg = framework_config_from_env()
        assert cfg.dashboard_api_key is not None
        assert cfg.dashboard_api_key.get_secret_value() == "dash-secret"

    def test_empty_string_treated_as_unset(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_DASHBOARD_API_KEY", "")
        assert framework_config_from_env().dashboard_api_key is None


# =============================================================================
# Anvil fork timeouts — float parsing with malformed-input fallback.
# =============================================================================


class TestAnvilForkTimeouts:
    def test_rpc_timeout_set(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_FORK_RPC_TIMEOUT", "30.0")
        assert framework_config_from_env().anvil_fork_rpc_timeout_seconds == 30.0

    def test_health_timeout_set(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_FORK_HEALTH_TIMEOUT", "12.5")
        assert framework_config_from_env().anvil_fork_health_timeout_seconds == 12.5

    def test_malformed_rpc_timeout_raises(self, monkeypatch):
        # CodeRabbit review on PR 2156: the legacy
        # ``float(os.environ.get("ALMANAK_FORK_RPC_TIMEOUT", "8.0"))`` path
        # would raise ``ValueError`` on garbage input — the typed factory
        # must preserve that bit-for-bit rather than silently rewriting
        # the user's intent to the default.
        monkeypatch.setenv("ALMANAK_FORK_RPC_TIMEOUT", "garbage")
        with pytest.raises(ValueError, match="ALMANAK_FORK_RPC_TIMEOUT"):
            framework_config_from_env()

    def test_malformed_health_timeout_raises(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_FORK_HEALTH_TIMEOUT", "garbage")
        with pytest.raises(ValueError, match="ALMANAK_FORK_HEALTH_TIMEOUT"):
            framework_config_from_env()

    def test_zero_preserved_to_let_post_init_validate(self, monkeypatch):
        # Legacy behaviour: ``float("0")`` returns ``0.0`` and flows
        # through to ``RollingForkConfig.__post_init__`` which raises
        # "must be positive". The typed factory must not pre-empt that
        # validator with a silent default-rewrite.
        monkeypatch.setenv("ALMANAK_FORK_RPC_TIMEOUT", "0")
        assert framework_config_from_env().anvil_fork_rpc_timeout_seconds == 0.0

    def test_negative_preserved_to_let_post_init_validate(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_FORK_HEALTH_TIMEOUT", "-5")
        assert framework_config_from_env().anvil_fork_health_timeout_seconds == -5.0


# =============================================================================
# Token-resolver knobs — positive int / float; non-positive falls back.
# =============================================================================


class TestTokenNegativeCacheKnobs:
    def test_ttl_set(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_TOKEN_NEGATIVE_CACHE_TTL_S", "60.5")
        assert framework_config_from_env().token_negative_cache_ttl_s == 60.5

    def test_max_set(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_TOKEN_NEGATIVE_CACHE_MAX", "5000")
        assert framework_config_from_env().token_negative_cache_max == 5000

    def test_zero_or_negative_returns_none(self, monkeypatch):
        # The legacy resolver only honours positive values; the typed
        # factory produces ``None`` for non-positive input so the
        # consumer's "default" branch fires.
        monkeypatch.setenv("ALMANAK_TOKEN_NEGATIVE_CACHE_TTL_S", "0")
        assert framework_config_from_env().token_negative_cache_ttl_s is None
        monkeypatch.setenv("ALMANAK_TOKEN_NEGATIVE_CACHE_TTL_S", "-1")
        assert framework_config_from_env().token_negative_cache_ttl_s is None
        monkeypatch.setenv("ALMANAK_TOKEN_NEGATIVE_CACHE_MAX", "0")
        assert framework_config_from_env().token_negative_cache_max is None

    def test_malformed_returns_none(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_TOKEN_NEGATIVE_CACHE_TTL_S", "abc")
        assert framework_config_from_env().token_negative_cache_ttl_s is None


# =============================================================================
# Secret repr suppression — credentials must never reach a log line.
# =============================================================================


class TestSecretReprSuppression:
    @pytest.fixture
    def populated(self) -> FrameworkConfig:
        from pydantic import SecretStr

        return FrameworkConfig(
            log_emojis=False,
            strategies_dir=Path("/test/strategies"),
            accounting_dir=Path("/test/acct"),
            api_keys=("VALIDATOR-KEY-VALUE-1", "VALIDATOR-KEY-VALUE-2"),
            dashboard_api_key=SecretStr("DASH-VERY-SECRET-VALUE"),
            anvil_fork_cache_path=Path("/test/cache"),
            anvil_fork_rpc_timeout_seconds=10.0,
            anvil_fork_health_timeout_seconds=6.0,
            token_negative_cache_ttl_s=60.0,
            token_negative_cache_max=5000,
        )

    def test_repr_suppresses_dashboard_key_and_api_keys(self, populated: FrameworkConfig):
        text = repr(populated)
        for s in ("DASH-VERY-SECRET-VALUE", "VALIDATOR-KEY-VALUE-1", "VALIDATOR-KEY-VALUE-2"):
            assert s not in text, f"Secret {s!r} leaked into repr: {text}"

    def test_model_dump_still_returns_values(self, populated: FrameworkConfig):
        # ``repr=False`` only affects ``__repr__`` — explicit
        # ``model_dump()`` still surfaces the original values for the
        # consumer.
        dumped = populated.model_dump()
        assert dumped["api_keys"] == ("VALIDATOR-KEY-VALUE-1", "VALIDATOR-KEY-VALUE-2")
        # SecretStr stays wrapped on dump unless the consumer explicitly
        # asks for the secret value.
        assert populated.dashboard_api_key is not None
        assert populated.dashboard_api_key.get_secret_value() == "DASH-VERY-SECRET-VALUE"


# =============================================================================
# Forbid extra — typo at the service boundary fails loud.
# =============================================================================


class TestForbidExtra:
    def test_unknown_field_rejected(self):
        with pytest.raises(ValueError):
            FrameworkConfig(unknown_secret="oops")  # type: ignore[call-arg]
