"""Typed framework-toggle configuration submodel.

Phase 6 of the config-service migration (see
``docs/internal/config-service-plan.md``). Owns every env read for the
framework-tier toggles and paths that don't fit into any of the
existing typed submodels: log emoji rendering, strategy / accounting
directory overrides, dashboard authentication, the API-key validator
list, the Anvil fork-manager timeouts, and the token-resolver negative
cache knobs.

Five surfaces consolidated here:

* **Logging** — ``log_emojis`` (``ALMANAK_LOG_EMOJIS``). Read by
  :func:`format_intent_type_emoji` in ``framework/utils/log_formatters.py``
  on every log line; centralising the truthy-string ladder here lets
  the formatter consume a parsed boolean.

* **Strategy and accounting paths** — ``strategies_dir``
  (``ALMANAK_STRATEGIES_DIR``) and ``accounting_dir``
  (``ALMANAK_ACCOUNTING_DIR``). The strategy-discovery path used by
  ``framework/strategies/__init__.py`` and the accounting sidecar
  directory used by ``framework/accounting/sidecar.py``. Both fields
  are typed as ``Path | None`` so the consumer can fall back to its
  documented default (cwd-relative ``./strategies`` for the first,
  ``~/.almanak/accounting`` for the second).

* **API authentication** — ``api_keys`` (``ALMANAK_API_KEYS``,
  comma-separated) and ``dashboard_api_key``
  (``ALMANAK_DASHBOARD_API_KEY``). The first is read by
  :class:`EnvironmentApiKeyValidator` in ``framework/api/actions.py``
  and split into a tuple at the boundary so the validator can hash
  each key once at construction; the second is the dashboard's
  outbound REST credential and is wrapped in :class:`SecretStr` so a
  stray ``logger.info(repr(cfg))`` cannot leak it.

* **Anvil fork manager** — ``anvil_fork_cache_path``
  (``ANVIL_FORK_CACHE_PATH``), ``anvil_fork_rpc_timeout_seconds``
  (``ALMANAK_FORK_RPC_TIMEOUT``), and
  ``anvil_fork_health_timeout_seconds`` (``ALMANAK_FORK_HEALTH_TIMEOUT``).
  Read by both :class:`AnvilForkConfig` and :class:`RollingForkManager`.
  The legacy code used per-dataclass ``field(default_factory=lambda: ...)``
  closures; the typed shape moves the lookup to construction time of
  the framework config and lets the dataclasses pull their defaults
  from a single source.

* **Token resolver negative cache** —
  ``token_negative_cache_ttl_s`` (``ALMANAK_TOKEN_NEGATIVE_CACHE_TTL_S``)
  and ``token_negative_cache_max``
  (``ALMANAK_TOKEN_NEGATIVE_CACHE_MAX``). VIB-2715 negative-cache
  knobs the resolver reads at construction time. ``None`` preserves
  the resolver's hard-coded defaults (300s TTL, 10000-entry cap).

Import direction
----------------
Strict (mirrors the other Phase 5/6 submodels): this module MUST NOT
import from ``almanak.framework.*``. The framework code imports
:class:`FrameworkConfig` from here at construction time; reverse
imports would create a cycle and make the typed-config service
depend on the framework layer it is meant to feed.
"""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from almanak.config.env import _load_dotenv_once

# =============================================================================
# Defaults — bit-for-bit mirrors of the legacy module-level constants
# =============================================================================

# Default Anvil fork RPC timeout. Mirrors the legacy
# ``float(os.environ.get("ALMANAK_FORK_RPC_TIMEOUT", "8.0"))`` fallback
# in ``framework/anvil/fork_manager.py``.
DEFAULT_ANVIL_FORK_RPC_TIMEOUT_S: float = 8.0

# Default Anvil fork health-check timeout. Mirrors the legacy
# ``float(os.environ.get("ALMANAK_FORK_HEALTH_TIMEOUT", "5.0"))`` fallback.
DEFAULT_ANVIL_FORK_HEALTH_TIMEOUT_S: float = 5.0

# Truthy strings the legacy ``ALMANAK_LOG_EMOJIS`` ladder *negates*.
# Bit-for-bit mirror of the
# ``not in ("false", "0", "no")`` test in ``log_formatters.py``.
_FALSY_LOG_EMOJI_VALUES: frozenset[str] = frozenset({"false", "0", "no"})


def _parse_log_emojis(value: str | None) -> bool:
    """Parse the ``ALMANAK_LOG_EMOJIS`` env value to a boolean.

    Mirrors the legacy
    ``os.environ.get("ALMANAK_LOG_EMOJIS", "true").strip().lower() not in ("false", "0", "no")``
    expression: ``None`` / unrecognised values are emoji-on, only the
    documented falsy strings disable rendering.
    """
    if value is None:
        return True
    return value.strip().lower() not in _FALSY_LOG_EMOJI_VALUES


def _require_float_env(env_var: str, raw: str) -> float:
    """Coerce a non-empty env-var string to ``float`` or raise a typed error.

    Used for fork-timeout reads where the legacy ``float(os.environ.get(...))``
    semantic must be preserved (CodeRabbit review, PR 2156): missing →
    typed default, present-but-malformed → ``ValueError`` (fail loud at
    boot rather than silently rewriting to a default that masks the typo).
    """
    try:
        return float(raw.strip())
    except ValueError as exc:
        raise ValueError(f"Invalid float for env var {env_var}={raw!r}: must be a number") from exc


def _parse_positive_float(value: str | None) -> float | None:
    """Parse a positive float; return ``None`` on missing / malformed / non-positive.

    Distinct from :func:`almanak.config.cli_runtime._parse_optional_float`,
    which raises on malformed and accepts any sign — this helper folds
    every non-positive value back to ``None`` so the model default takes
    over (used for fork timeouts and cache TTLs where ``0`` or negatives
    are nonsensical).
    """
    if value is None or not value.strip():
        return None
    try:
        parsed = float(value.strip())
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _parse_positive_int(value: str | None) -> int | None:
    """Parse a positive int; return ``None`` on missing / malformed / non-positive.

    Same fall-through semantics as :func:`_parse_positive_float`.
    """
    if value is None or not value.strip():
        return None
    try:
        parsed = int(value.strip())
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _parse_api_keys(value: str | None) -> tuple[str, ...]:
    """Split the ``ALMANAK_API_KEYS`` CSV into a tuple of non-empty entries.

    Mirrors the legacy ``[k.strip() for k in keys_str.split(",") if k.strip()]``
    list comprehension in ``framework/api/actions.py``. An unset env
    var yields the empty tuple, matching the legacy "no keys
    configured" branch.
    """
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


# =============================================================================
# FrameworkConfig — typed, validated, secret-safe
# =============================================================================


class FrameworkConfig(BaseModel):
    """Typed configuration for the framework-toggle cluster.

    Every field is optional from the consumer's standpoint — when a
    field is ``None`` (or the dict-shaped ``api_keys`` field is empty),
    the consumer falls back to its existing missing-env behaviour
    (default strategy directory, default accounting directory, no
    dashboard auth, no API-key validator list, default fork timeouts,
    default negative-cache TTL / cap). Field-by-field policy is
    preserved bit-for-bit on cutover; this model is the *single env
    reader*, not a behavioural rewrite.

    Secret fields (``dashboard_api_key``) are wrapped in
    :class:`SecretStr` so the model's ``__repr__`` does not leak the
    credential. ``api_keys`` is a tuple of plaintext strings — each is
    a validator key that is hashed on the consumer side; the tuple
    itself is stored ``repr=False`` so a populated list of operator
    keys does not appear in default ``__repr__`` output.
    """

    log_emojis: bool = True
    """Whether emoji prefixes are rendered in framework log output
    (``ALMANAK_LOG_EMOJIS``).

    Default ``True``. Falsy values (``"false"`` / ``"0"`` / ``"no"``,
    case-insensitive, whitespace-tolerant) disable rendering and
    switch the formatter to ``[INTENT_TYPE]`` text tags.
    """

    strategies_dir: Path | None = None
    """Override for the strategy-discovery directory (``ALMANAK_STRATEGIES_DIR``).

    ``None`` means "use ``./strategies`` relative to cwd". Relative
    paths are resolved against cwd at consumer time (``framework/strategies/__init__.py``)
    so the typed config does not pre-resolve them — preserving the
    legacy "compute path each call" semantics.
    """

    accounting_dir: Path | None = None
    """Override for the per-strategy accounting-sidecar directory
    (``ALMANAK_ACCOUNTING_DIR``).

    ``None`` means "use ``~/.almanak/accounting`` (or
    ``<tempdir>/.almanak/accounting`` when HOME is unset)". The
    consumer in ``framework/accounting/sidecar.py`` applies the
    fallback ladder.
    """

    api_keys: tuple[str, ...] = Field(default_factory=tuple, repr=False)
    """Comma-separated valid API keys for the framework REST API
    (``ALMANAK_API_KEYS``).

    Read by :class:`EnvironmentApiKeyValidator`. Each entry is hashed
    once at validator construction and compared with constant-time
    comparison thereafter. An empty tuple disables authentication
    (the validator rejects all requests with a warning).
    """

    dashboard_api_key: SecretStr | None = Field(default=None, repr=False)
    """Dashboard outbound API key (``ALMANAK_DASHBOARD_API_KEY``).

    Required when the dashboard wants to invoke a strategy action via
    REST. The dashboard refuses to call the action API without it.
    """

    anvil_fork_cache_path: Path | None = None
    """Anvil fork RPC-response cache path (``ANVIL_FORK_CACHE_PATH``).

    Reduces upstream RPC calls during fork startup. ``None`` disables
    the cache (Anvil hits the upstream provider for every request).
    """

    anvil_fork_rpc_timeout_seconds: float = DEFAULT_ANVIL_FORK_RPC_TIMEOUT_S
    """Anvil fork RPC request timeout (``ALMANAK_FORK_RPC_TIMEOUT``).

    Default ``8.0``. Tuned via env for slow remote forks.
    """

    anvil_fork_health_timeout_seconds: float = DEFAULT_ANVIL_FORK_HEALTH_TIMEOUT_S
    """Anvil fork health-check timeout (``ALMANAK_FORK_HEALTH_TIMEOUT``).

    Default ``5.0``. Tuned via env for slow remote forks.
    """

    token_negative_cache_ttl_s: float | None = None
    """Token-resolver negative-cache TTL in seconds
    (``ALMANAK_TOKEN_NEGATIVE_CACHE_TTL_S``).

    VIB-2715. ``None`` preserves the resolver's hard-coded default
    (300s). The consumer in ``framework/data/tokens/resolver.py``
    only honours positive values; non-positive / malformed inputs
    fall back to the default.
    """

    token_negative_cache_max: int | None = None
    """Token-resolver negative-cache size cap
    (``ALMANAK_TOKEN_NEGATIVE_CACHE_MAX``).

    VIB-2715. ``None`` preserves the resolver's hard-coded default
    (10000). The consumer only honours positive values; non-positive
    / malformed inputs fall back to the default.
    """

    model_config = ConfigDict(
        # Reject typos at the service boundary — a misspelt kwarg here
        # would silently flow into the config without populating any
        # consumer field.
        extra="forbid",
    )


# =============================================================================
# Public factory — single env-reading entry point for framework config
# =============================================================================


def framework_config_from_env(
    *,
    dotenv_path: str | None = None,
) -> FrameworkConfig:
    """Construct a :class:`FrameworkConfig` from environment variables.

    Single env-reading entry point for the framework-toggle cluster.
    Mirrors the legacy per-callsite lookups bit-for-bit:

    * ``ALMANAK_LOG_EMOJIS`` → ``log_emojis`` (truthy ladder; ``"false"``
      / ``"0"`` / ``"no"`` are falsy, everything else is truthy).
    * ``ALMANAK_STRATEGIES_DIR`` → ``strategies_dir`` (``Path`` or
      ``None``; relative paths are *not* pre-resolved).
    * ``ALMANAK_ACCOUNTING_DIR`` → ``accounting_dir``.
    * ``ALMANAK_API_KEYS`` → ``api_keys`` (CSV split, whitespace
      stripped, empty entries dropped).
    * ``ALMANAK_DASHBOARD_API_KEY`` → ``dashboard_api_key``
      (wrapped in :class:`SecretStr`).
    * ``ANVIL_FORK_CACHE_PATH`` → ``anvil_fork_cache_path``.
    * ``ALMANAK_FORK_RPC_TIMEOUT`` → ``anvil_fork_rpc_timeout_seconds``
      (default :data:`DEFAULT_ANVIL_FORK_RPC_TIMEOUT_S`).
    * ``ALMANAK_FORK_HEALTH_TIMEOUT`` → ``anvil_fork_health_timeout_seconds``
      (default :data:`DEFAULT_ANVIL_FORK_HEALTH_TIMEOUT_S`).
    * ``ALMANAK_TOKEN_NEGATIVE_CACHE_TTL_S`` →
      ``token_negative_cache_ttl_s`` (positive float or ``None``).
    * ``ALMANAK_TOKEN_NEGATIVE_CACHE_MAX`` → ``token_negative_cache_max``
      (positive int or ``None``).

    Args:
        dotenv_path: Optional ``.env`` path; routed through the shared
            single-shot loader.
    """
    _load_dotenv_once(dotenv_path)

    strategies_dir_raw = os.environ.get("ALMANAK_STRATEGIES_DIR") or None
    accounting_dir_raw = os.environ.get("ALMANAK_ACCOUNTING_DIR") or None
    fork_cache_raw = os.environ.get("ANVIL_FORK_CACHE_PATH") or None
    dashboard_key_raw = os.environ.get("ALMANAK_DASHBOARD_API_KEY") or None

    rpc_timeout_raw = os.environ.get("ALMANAK_FORK_RPC_TIMEOUT")
    health_timeout_raw = os.environ.get("ALMANAK_FORK_HEALTH_TIMEOUT")

    return FrameworkConfig(
        log_emojis=_parse_log_emojis(os.environ.get("ALMANAK_LOG_EMOJIS")),
        strategies_dir=Path(strategies_dir_raw) if strategies_dir_raw else None,
        accounting_dir=Path(accounting_dir_raw) if accounting_dir_raw else None,
        api_keys=_parse_api_keys(os.environ.get("ALMANAK_API_KEYS")),
        dashboard_api_key=SecretStr(dashboard_key_raw) if dashboard_key_raw else None,
        anvil_fork_cache_path=Path(fork_cache_raw) if fork_cache_raw else None,
        # Bit-for-bit legacy semantics for fork timeouts (CodeRabbit review,
        # PR 2156): missing → typed default, present → ``float()`` directly
        # so malformed input fails loud at boot AND ``0``/negative values
        # flow through to the dataclass __post_init__ in fork_manager.py
        # where the existing "must be positive" validation owns the error.
        anvil_fork_rpc_timeout_seconds=(
            DEFAULT_ANVIL_FORK_RPC_TIMEOUT_S
            if rpc_timeout_raw is None
            else _require_float_env("ALMANAK_FORK_RPC_TIMEOUT", rpc_timeout_raw)
        ),
        anvil_fork_health_timeout_seconds=(
            DEFAULT_ANVIL_FORK_HEALTH_TIMEOUT_S
            if health_timeout_raw is None
            else _require_float_env("ALMANAK_FORK_HEALTH_TIMEOUT", health_timeout_raw)
        ),
        token_negative_cache_ttl_s=_parse_positive_float(os.environ.get("ALMANAK_TOKEN_NEGATIVE_CACHE_TTL_S")),
        token_negative_cache_max=_parse_positive_int(os.environ.get("ALMANAK_TOKEN_NEGATIVE_CACHE_MAX")),
    )


__all__ = [
    "DEFAULT_ANVIL_FORK_HEALTH_TIMEOUT_S",
    "DEFAULT_ANVIL_FORK_RPC_TIMEOUT_S",
    "FrameworkConfig",
    "framework_config_from_env",
]
