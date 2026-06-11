"""Typed CLI-runtime configuration submodel.

Phase 5e of the config-service migration (see
``docs/internal/config-service-plan.md``). Owns the CLI-specific env
reads that don't fit any existing submodel: knobs the click handlers
under ``almanak/cli/cli.py`` and ``almanak/framework/cli/*`` consume to
shape startup behaviour.

Five surfaces consolidated here:

* **Gateway client auth** — ``GATEWAY_AUTH_TOKEN`` (the legacy
  unprefixed form, alongside the typed
  :attr:`GatewayConfig.auth_token` which already covers
  ``ALMANAK_GATEWAY_AUTH_TOKEN``). The CLI's gRPC-client paths
  (``ax`` subcommands, ``run`` --no-gateway, dashboard subprocess)
  prefer the typed gateway token but fall back to the unprefixed
  bare-name shape that some operator scripts still set.

* **Gateway-wallets discriminator** — ``ALMANAK_GATEWAY_WALLETS``.
  When set (any truthy value), the gateway's ``WalletRegistry`` owns
  per-chain wallet identity and the framework treats local
  private-key resolution as optional. Read by run.py /
  run_helpers.py / sidecar code paths.

* **Safe-mode preflight** — ``ALMANAK_GATEWAY_SAFE_MODE``,
  ``ALMANAK_GATEWAY_SAFE_ADDRESS``, ``ALMANAK_SAFE_ADDRESS``,
  ``ALMANAK_EOA_ADDRESS``, ``ALMANAK_EXECUTION_MODE``. Read by
  ``_validate_safe_mode_preflight`` to verify the framework and
  gateway agree on Safe-mode configuration before any signed
  transaction lands. The gateway-side mirror is
  :attr:`GatewayConfig.safe_address`; this submodel exposes the
  CLI's read-only view of the same env state.

* **Solana fork (Anvil-equivalent)** — ``SOLANA_RPC_URL``,
  ``SOLANA_VALIDATOR_PORT``. The Solana fork-bring-up path that
  pre-clones Orca pool accounts uses these. The connector-side
  ``SOLANA_RPC_URL`` read in :class:`ConnectorsConfig` covers the
  Drift / Jupiter direct paths; the CLI fork-manager path is
  separate (CLI runs before any connector is constructed).

* **Reconciliation enforcement / hardcoded prices toggles** —
  ``ALMANAK_RECONCILIATION_ENFORCEMENT`` (VIB-3348) and
  ``ALMANAK_ALLOW_HARDCODED_PRICES`` (VIB-2562). Boolean knobs the
  CLI parses identically every time; centralising the parsing here
  removes per-callsite truthy-string ladders.

* **Bare-name RPC URL fallbacks** — ``ALMANAK_<CHAIN>_RPC_URL`` /
  ``<CHAIN>_RPC_URL`` / ``ALMANAK_RPC_URL`` / ``RPC_URL``. The
  paper-trading subcommand walks this ladder when no ``--rpc-url``
  flag is given. The runtime-config layer covers the ALMANAK-prefixed
  forms; the CLI also accepts the legacy unprefixed shape, which is
  what this submodel surfaces.

Environment toggles (``CI``) and miscellaneous one-shots (``ANVIL_<CHAIN>_PORT``)
are also captured so consumers can stop reading env directly.

Import direction
----------------
Strict (mirrors the other Phase 5 submodels): this module MUST NOT
import from ``almanak.framework.cli.*`` or ``almanak.cli.*``. The CLI
imports :class:`CliRuntimeConfig` from here at construction time;
reverse imports would create a cycle and make the typed-config
service depend on the CLI surface it is meant to feed.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from almanak.config.env import _load_dotenv_once
from almanak.core.chains._helpers import evm_chain_names

logger = logging.getLogger(__name__)

# =============================================================================
# Chain-RPC ladder shared with the paper-trading CLI. The legacy callsite
# walks this exact ordering before falling back to ``--rpc-url``; preserve
# bit-for-bit so the cutover doesn't change which env var wins.
# =============================================================================

_CHAIN_RPC_VAR_LADDER: tuple[str, ...] = (
    "ALMANAK_{CHAIN}_RPC_URL",
    "{CHAIN}_RPC_URL",
    "ALMANAK_RPC_URL",
    "RPC_URL",
)


def chain_rpc_url_from_env(chain: str) -> tuple[str | None, list[str]]:
    """Walk the legacy chain-RPC env ladder; return ``(url, env_var_names)``.

    Mirrors the paper-trading CLI's resolution exactly:

    1. ``ALMANAK_<CHAIN>_RPC_URL`` (prefixed, chain-specific)
    2. ``<CHAIN>_RPC_URL`` (bare-name, chain-specific)
    3. ``ALMANAK_RPC_URL`` (prefixed, generic)
    4. ``RPC_URL`` (bare-name, generic)

    The walk stops at the first non-empty value. The full ordered list of
    env-var names is also returned so the caller can echo a useful "set
    one of: ..." error message.

    Args:
        chain: Chain name (uppercased internally; case-insensitive).

    Returns:
        ``(url, env_var_names)``. ``url`` is ``None`` when no candidate
        is set; ``env_var_names`` is the full ladder for diagnostics.
    """
    chain_upper = chain.upper()
    env_var_names = [tmpl.format(CHAIN=chain_upper) for tmpl in _CHAIN_RPC_VAR_LADDER]
    for env_var in env_var_names:
        value = os.environ.get(env_var)
        if value:
            return value, env_var_names
    return None, env_var_names


# =============================================================================
# Constants — bit-for-bit mirrors of the legacy CLI hard-coded fallbacks.
# =============================================================================

# Solana mainnet public RPC. Mirrors the legacy
# ``os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")``
# fallback in run_helpers.py / teardown_helpers.py / _solana_setup.py.
DEFAULT_SOLANA_RPC_URL: str = "https://api.mainnet-beta.solana.com"

# Default ``solana-test-validator`` JSON-RPC port. Mirrors the legacy
# ``int(os.environ.get("SOLANA_VALIDATOR_PORT", "8899"))`` fallback.
DEFAULT_SOLANA_VALIDATOR_PORT: int = 8899

# Default Anvil JSON-RPC port. Mirrors the legacy
# ``os.environ.get(f"ANVIL_{CHAIN}_PORT", "8545")`` fallback.
DEFAULT_ANVIL_PORT: int = 8545

# Truthy strings the legacy reconciliation / hardcoded-prices ladders accept.
# Whitespace-tolerant, case-insensitive — matches the legacy
# ``.strip().lower() in ("1", "true", "yes")`` pattern in run_helpers.py.
_TRUTHY_VALUES: frozenset[str] = frozenset({"1", "true", "yes"})


def _parse_truthy(value: str | None) -> bool:
    """Return True iff ``value`` is one of the documented truthy strings.

    Mirrors the legacy ``_reconciliation_enforcement_from_env`` ladder
    bit-for-bit: ``None`` / empty / unrecognised → False. Whitespace
    around the value is tolerated.
    """
    if value is None:
        return False
    return value.strip().lower() in _TRUTHY_VALUES


# =============================================================================
# CliRuntimeConfig — typed, validated, secret-safe
# =============================================================================


class CliRuntimeConfig(BaseModel):
    """Typed configuration for CLI-runtime-only env reads.

    Every field is optional from the CLI's standpoint — when a value is
    missing the CLI's existing default-or-degrade behaviour fires
    (``GatewayConfig.auth_token`` for the typed gateway-auth path,
    ``DEFAULT_SOLANA_RPC_URL`` for the Solana fork path, etc.). Field-by-field
    behaviour is preserved bit-for-bit on cutover; this model is the *single
    env reader* for the CLI-specific cluster, not a behavioural rewrite.
    """

    # -------------------------------------------------------------------------
    # Gateway client auth (CLI side) — repr suppressed.
    # -------------------------------------------------------------------------

    legacy_gateway_auth_token: str | None = Field(default=None, repr=False)
    """Unprefixed gateway auth token (``GATEWAY_AUTH_TOKEN``).

    Legacy bare-name fallback the CLI honours alongside the typed
    :attr:`GatewayConfig.auth_token` (which reads
    ``ALMANAK_GATEWAY_AUTH_TOKEN`` via pydantic-settings). The CLI
    callsite chain is: CLI flag > ctx.obj > typed gateway > legacy
    bare-name. Operators running scripts predating the prefix
    convention rely on this fallback.
    """

    legacy_gateway_host: str | None = None
    """Unprefixed gateway host (``GATEWAY_HOST``).

    Legacy bare-name fallback the gRPC client honours alongside the
    typed :attr:`GatewayConfig.host` (which reads
    ``ALMANAK_GATEWAY_HOST`` via pydantic-settings). Same precedence
    ladder as :attr:`legacy_gateway_auth_token`: typed gateway >
    legacy bare-name. ``None`` means "no legacy override; use the
    typed default".
    """

    legacy_gateway_port: int | None = None
    """Unprefixed gateway port (``GATEWAY_PORT``).

    Legacy bare-name fallback the gRPC client honours alongside the
    typed :attr:`GatewayConfig.grpc_port`. Missing/whitespace values
    resolve to ``None`` (the typed default takes over). Present-but-
    invalid values — non-integer or out of the ``1..65535`` port range
    — raise ``ValueError`` so env typos fail loud at boot rather than
    silently dialing the wrong gateway (Codex / CodeRabbit review on
    PR 2156).
    """

    legacy_gateway_timeout: float | None = None
    """Unprefixed gateway client RPC timeout (``GATEWAY_TIMEOUT``).

    Legacy bare-name fallback alongside the typed
    :attr:`GatewayConfig.timeout`. Missing/whitespace values resolve
    to ``None``. Present-but-invalid values — non-numeric or
    non-positive (``<= 0``) — raise ``ValueError`` so a typo or a
    nonsensical setting can't collapse every client call into an
    immediate deadline failure.
    """

    # -------------------------------------------------------------------------
    # Gateway-client resolved values — prefixed-then-unprefixed precedence
    # already applied. The gRPC client reads these instead of constructing
    # its own ladder, so the env-presence check stays at the service boundary.
    # -------------------------------------------------------------------------

    gateway_client_host_resolved: str = "localhost"
    """Resolved gRPC client host — ``ALMANAK_GATEWAY_HOST`` →
    ``GATEWAY_HOST`` → ``"localhost"`` (the gRPC-client legacy
    default, distinct from the gateway-server default
    ``"127.0.0.1"``).

    Default ``"localhost"`` matches the legacy
    :meth:`GatewayClientConfig.from_env` behaviour: the gateway server
    binds to ``127.0.0.1`` but the strategy-side gRPC client dials
    ``localhost`` because that resolves identically on every platform
    SDK developers run. Callers that need the typed gateway-server
    host should read :attr:`GatewayConfig.host` directly.
    """

    gateway_client_port_resolved: int = 50051
    """Resolved gRPC client port — ``ALMANAK_GATEWAY_PORT`` →
    ``GATEWAY_PORT`` → ``50051``.
    """

    gateway_client_timeout_resolved: float = 30.0
    """Resolved gRPC client timeout in seconds — ``ALMANAK_GATEWAY_TIMEOUT``
    → ``GATEWAY_TIMEOUT`` → ``30.0``.
    """

    gateway_client_auth_token_resolved: str | None = Field(default=None, repr=False)
    """Resolved gRPC client auth token — ``ALMANAK_GATEWAY_AUTH_TOKEN``
    → ``GATEWAY_AUTH_TOKEN`` → ``None``. Repr-suppressed.
    """

    # -------------------------------------------------------------------------
    # Gateway-wallets discriminator + Safe-mode preflight (non-secret addresses).
    # -------------------------------------------------------------------------

    gateway_wallets_configured: bool = False
    """Whether ``ALMANAK_GATEWAY_WALLETS`` is set (any truthy/present value).

    Mirrors the legacy ``bool(os.environ.get("ALMANAK_GATEWAY_WALLETS"))``
    discriminator. When True, the gateway's ``WalletRegistry`` owns
    per-chain wallet identity; the framework's local private-key
    resolution becomes optional and ``register_chains()`` runs after
    gateway setup to pin the resolved wallet on the runtime config.
    """

    gateway_safe_mode: str | None = None
    """Gateway-side Safe execution mode (``ALMANAK_GATEWAY_SAFE_MODE``).

    ``"direct"`` or ``"zodiac"`` when configured; ``None`` (or any other
    value) fails the preflight when the framework is also in Safe mode.
    Read only by ``_validate_safe_mode_preflight``.
    """

    gateway_safe_address: str | None = None
    """Gateway-side Safe address (``ALMANAK_GATEWAY_SAFE_ADDRESS``)."""

    safe_address: str | None = None
    """Framework-side Safe address (``ALMANAK_SAFE_ADDRESS``).

    Used as a fallback for ``gateway_safe_address`` and read by the
    sidecar runtime config builder.
    """

    eoa_address: str | None = None
    """Framework-side EOA address (``ALMANAK_EOA_ADDRESS``).

    Read by the sidecar runtime config builder when ``safe_address`` is
    unset.
    """

    execution_mode: str | None = None
    """Framework-side execution mode (``ALMANAK_EXECUTION_MODE``).

    Lowercased on read. ``"safe_zodiac"`` selects the Zodiac path in
    the Safe-mode preflight; any other value is treated as the
    "direct" path.
    """

    # -------------------------------------------------------------------------
    # Solana fork — Anvil-equivalent for solana-test-validator.
    # -------------------------------------------------------------------------

    solana_rpc_url: str = DEFAULT_SOLANA_RPC_URL
    """Solana mainnet RPC for the fork-manager (``SOLANA_RPC_URL``).

    Default ``https://api.mainnet-beta.solana.com``. The Solana fork
    bring-up path (Orca pool pre-cloning) and the
    :class:`SolanaForkManager` constructor read this. Disjoint from
    :attr:`ConnectorsConfig.solana_rpc_url` — the connector field is
    consumed by Drift / Jupiter direct paths after gateway setup, the
    CLI field is consumed before gateway setup.
    """

    solana_validator_port: int = DEFAULT_SOLANA_VALIDATOR_PORT
    """JSON-RPC port for ``solana-test-validator`` (``SOLANA_VALIDATOR_PORT``).

    Default ``8899`` (the upstream solana-test-validator default).
    """

    # -------------------------------------------------------------------------
    # Anvil ports — single CHAIN-keyed dict.
    # -------------------------------------------------------------------------

    anvil_ports: dict[str, int] = Field(default_factory=dict)
    """Per-chain Anvil JSON-RPC ports (``ANVIL_<CHAIN>_PORT``).

    Keys are lowercase chain names; values are the resolved port
    integers. Missing chains are absent — consumers fall back to
    :data:`DEFAULT_ANVIL_PORT` (``8545``). The factory does *not*
    pre-populate every supported chain; it only stores entries the
    operator has explicitly configured, matching the legacy
    ``os.environ.get(f"ANVIL_{CHAIN}_PORT", "8545")`` pattern.
    """

    # -------------------------------------------------------------------------
    # Boolean toggles — parsed once so consumers don't repeat the truthy ladder.
    # -------------------------------------------------------------------------

    reconciliation_enforcement: bool = False
    """Whether to opt back into fail-closed reconciliation (``ALMANAK_RECONCILIATION_ENFORCEMENT``).

    Default observation mode (False) until VIB-3348 block-anchored
    balance reads close the false-positive race. Truthy values:
    ``1``, ``true``, ``yes`` (case-insensitive, whitespace-tolerant).
    """

    allow_hardcoded_prices: bool = False
    """Whether paper-trading accepts hardcoded prices for tokens
    without price feeds (``ALMANAK_ALLOW_HARDCODED_PRICES``).

    VIB-2562. Useful for Pendle PT tokens etc. Truthy values: ``"1"``
    only (the legacy callsite tested ``== "1"`` exactly, not the full
    truthy ladder; preserved verbatim).
    """

    # -------------------------------------------------------------------------
    # Environment hints.
    # -------------------------------------------------------------------------

    is_ci: bool = False
    """Whether the process is running under CI (``CI`` env var truthy).

    Truthy = any non-empty value (matches the legacy ``not os.environ.get("CI")``
    test in ``new_strategy.py``). Consumers use this as a UX hint —
    e.g. ``new`` skips auto-detection of ``strategies/incubating/`` when CI
    is set.
    """

    model_config = ConfigDict(
        # Reject typos at the service boundary — a misspelt kwarg here
        # would silently flow into the config without populating any
        # consumer field.
        extra="forbid",
    )


# =============================================================================
# Public factory — single env-reading entry point for CLI-runtime config
# =============================================================================


def cli_runtime_config_from_env(
    *,
    dotenv_path: str | None = None,
    anvil_chains: tuple[str, ...] | None = None,
) -> CliRuntimeConfig:
    """Construct a :class:`CliRuntimeConfig` from environment variables.

    Single env-reading entry point for the CLI-specific cluster.
    Mirrors the legacy per-callsite lookups bit-for-bit:

    * ``GATEWAY_AUTH_TOKEN`` → ``legacy_gateway_auth_token`` (legacy
      bare-name fallback the CLI accepts alongside the typed
      :attr:`GatewayConfig.auth_token`).
    * ``ALMANAK_GATEWAY_WALLETS`` (truthy) → ``gateway_wallets_configured``.
    * ``ALMANAK_GATEWAY_SAFE_MODE`` → ``gateway_safe_mode`` (lowercased).
    * ``ALMANAK_GATEWAY_SAFE_ADDRESS`` → ``gateway_safe_address``.
    * ``ALMANAK_SAFE_ADDRESS`` → ``safe_address``.
    * ``ALMANAK_EOA_ADDRESS`` → ``eoa_address``.
    * ``ALMANAK_EXECUTION_MODE`` → ``execution_mode`` (lowercased).
    * ``SOLANA_RPC_URL`` → ``solana_rpc_url``
      (default :data:`DEFAULT_SOLANA_RPC_URL`).
    * ``SOLANA_VALIDATOR_PORT`` → ``solana_validator_port``
      (default :data:`DEFAULT_SOLANA_VALIDATOR_PORT`).
    * ``ANVIL_<CHAIN>_PORT`` (per ``anvil_chains``) → ``anvil_ports[chain]``.
      Only set when the env var is present; absent entries fall back to
      :data:`DEFAULT_ANVIL_PORT` at the consumer. A non-empty value that
      fails ``int()`` raises ``ValueError`` at boot rather than silently
      falling back — a misconfigured ``ANVIL_ETHEREUM_PORT=abc`` should
      surface immediately (PR #2152 review).
    * ``ALMANAK_RECONCILIATION_ENFORCEMENT`` → ``reconciliation_enforcement``
      (truthy ladder).
    * ``ALMANAK_ALLOW_HARDCODED_PRICES`` → ``allow_hardcoded_prices``
      (``"1"`` only, mirrors the legacy callsite).
    * ``CI`` → ``is_ci`` (any non-empty value).

    Args:
        dotenv_path: Optional ``.env`` path; routed through the shared
            single-shot loader.
        anvil_chains: Override the chain list for the
            ``ANVIL_<CHAIN>_PORT`` lookup. ``None`` (the default)
            resolves lazily to every registered EVM chain
            (``evm_chain_names()`` — registration order, non-semantic;
            a new chain file under almanak/core/chains/ joins
            automatically, VIB-4851 CS-3); tests pass a custom tuple
            to exercise specific chains.
    """
    _load_dotenv_once(dotenv_path)

    if anvil_chains is None:
        anvil_chains = evm_chain_names()

    safe_mode_raw = os.environ.get("ALMANAK_GATEWAY_SAFE_MODE")
    execution_mode_raw = os.environ.get("ALMANAK_EXECUTION_MODE")

    anvil_ports: dict[str, int] = {}
    for chain in anvil_chains:
        env_var = f"ANVIL_{chain.upper()}_PORT"
        raw = os.environ.get(env_var)
        if raw is None or not raw.strip():
            continue
        anvil_ports[chain.lower()] = _require_int_env(env_var, raw)

    # Gateway-client resolved values — preserve the legacy
    # prefixed-then-unprefixed precedence ladder at the service boundary
    # so ``GatewayClientConfig.from_env`` doesn't have to read env itself.
    legacy_host = os.environ.get("GATEWAY_HOST")
    legacy_port = _parse_optional_port_or_none("GATEWAY_PORT", os.environ.get("GATEWAY_PORT"))
    legacy_timeout = _parse_optional_positive_float_or_none("GATEWAY_TIMEOUT", os.environ.get("GATEWAY_TIMEOUT"))
    legacy_auth = os.environ.get("GATEWAY_AUTH_TOKEN")
    almanak_host = os.environ.get("ALMANAK_GATEWAY_HOST")
    almanak_port = _parse_optional_port_or_none("ALMANAK_GATEWAY_PORT", os.environ.get("ALMANAK_GATEWAY_PORT"))
    almanak_timeout = _parse_optional_positive_float_or_none(
        "ALMANAK_GATEWAY_TIMEOUT", os.environ.get("ALMANAK_GATEWAY_TIMEOUT")
    )
    almanak_auth = os.environ.get("ALMANAK_GATEWAY_AUTH_TOKEN")

    kwargs: dict[str, Any] = {
        "legacy_gateway_auth_token": legacy_auth or None,
        "legacy_gateway_host": legacy_host or None,
        "legacy_gateway_port": legacy_port,
        "legacy_gateway_timeout": legacy_timeout,
        "gateway_client_host_resolved": almanak_host or legacy_host or "localhost",
        "gateway_client_port_resolved": (
            almanak_port if almanak_port is not None else (legacy_port if legacy_port is not None else 50051)
        ),
        "gateway_client_timeout_resolved": (
            almanak_timeout if almanak_timeout is not None else (legacy_timeout if legacy_timeout is not None else 30.0)
        ),
        "gateway_client_auth_token_resolved": almanak_auth or legacy_auth or None,
        "gateway_wallets_configured": bool(os.environ.get("ALMANAK_GATEWAY_WALLETS")),
        "gateway_safe_mode": (safe_mode_raw.lower() if safe_mode_raw else None) or None,
        "gateway_safe_address": os.environ.get("ALMANAK_GATEWAY_SAFE_ADDRESS") or None,
        "safe_address": os.environ.get("ALMANAK_SAFE_ADDRESS") or None,
        "eoa_address": os.environ.get("ALMANAK_EOA_ADDRESS") or None,
        "execution_mode": (execution_mode_raw.lower() if execution_mode_raw else None) or None,
        "solana_rpc_url": os.environ.get("SOLANA_RPC_URL") or DEFAULT_SOLANA_RPC_URL,
        "solana_validator_port": _parse_optional_int(
            "SOLANA_VALIDATOR_PORT",
            os.environ.get("SOLANA_VALIDATOR_PORT"),
            DEFAULT_SOLANA_VALIDATOR_PORT,
        ),
        "anvil_ports": anvil_ports,
        "reconciliation_enforcement": _parse_truthy(os.environ.get("ALMANAK_RECONCILIATION_ENFORCEMENT")),
        # Legacy callsite tested ``== "1"`` exactly — preserve that strictness.
        "allow_hardcoded_prices": (os.environ.get("ALMANAK_ALLOW_HARDCODED_PRICES") or "").strip() == "1",
        "is_ci": bool(os.environ.get("CI")),
    }
    return CliRuntimeConfig(**kwargs)


def _parse_optional_int(env_var: str, value: str | None, default: int) -> int:
    """Parse an optional int env value, falling back to ``default`` when unset.

    Returns ``default`` when ``value`` is ``None`` or whitespace-only. A
    non-empty value that fails ``int()`` raises ``ValueError`` with the
    env var name and the offending raw value — silent fallback would
    point the process at the wrong local node and is far harder to
    diagnose than a loud config error at boot (PR #2152 review).
    """
    if value is None:
        return default
    stripped = value.strip()
    if not stripped:
        return default
    return _require_int_env(env_var, stripped)


def _require_int_env(env_var: str, raw: str) -> int:
    """Coerce a non-empty env-var string to ``int`` or raise a typed error."""
    try:
        return int(raw.strip())
    except ValueError as exc:
        raise ValueError(f"Invalid integer for env var {env_var}={raw!r}: must be an integer port number") from exc


def _require_float_env(env_var: str, raw: str) -> float:
    """Coerce a non-empty env-var string to ``float`` or raise a typed error."""
    try:
        return float(raw.strip())
    except ValueError as exc:
        raise ValueError(f"Invalid float for env var {env_var}={raw!r}: must be a number") from exc


def _parse_optional_int_or_none(env_var: str, value: str | None) -> int | None:
    """Parse an optional int env value.

    Returns ``None`` when ``value`` is ``None`` or whitespace-only —
    legacy-only field semantics: "not configured, use the typed default".

    Raises ``ValueError`` on a present-but-malformed value. Silent
    fallback is unsafe for ports because a typo in ``GATEWAY_PORT``
    could make a strategy dial the default localhost gateway already
    serving another strategy (Codex review on PR 2156).
    """
    if value is None or not value.strip():
        return None
    return _require_int_env(env_var, value)


def _parse_optional_port_or_none(env_var: str, value: str | None) -> int | None:
    """Parse an optional gateway-port env value with range validation.

    Same fall-through as :func:`_parse_optional_int_or_none`: ``None``
    for missing/whitespace, raise on malformed. In addition, raises
    ``ValueError`` when the integer is outside the valid TCP port
    range ``1..65535`` — a typo like ``GATEWAY_PORT=70000`` becomes a
    later socket failure that's far harder to diagnose than a clean
    boot-time error (CodeRabbit review on PR 2156).
    """
    parsed = _parse_optional_int_or_none(env_var, value)
    if parsed is not None and not (1 <= parsed <= 65535):
        raise ValueError(f"Invalid port for env var {env_var}={parsed}: must be in 1..65535")
    return parsed


def _parse_optional_float(env_var: str, value: str | None) -> float | None:
    """Parse an optional float env value.

    Same strict fall-through as :func:`_parse_optional_int_or_none`:
    ``None`` for missing/whitespace, raise on malformed.
    """
    if value is None or not value.strip():
        return None
    return _require_float_env(env_var, value)


def _parse_optional_positive_float_or_none(env_var: str, value: str | None) -> float | None:
    """Parse an optional positive float env value.

    Same fall-through as :func:`_parse_optional_float`: ``None`` for
    missing/whitespace, raise on malformed. In addition, raises
    ``ValueError`` when the value is non-positive — a non-positive
    timeout would collapse every gRPC client call into an immediate
    deadline failure, which is harder to diagnose than a boot-time
    error (CodeRabbit review on PR 2156).
    """
    parsed = _parse_optional_float(env_var, value)
    if parsed is not None and parsed <= 0:
        raise ValueError(f"Invalid value for env var {env_var}={parsed}: must be > 0")
    return parsed


def anvil_port_for_chain(chain: str) -> int | None:
    """Read ``ANVIL_<CHAIN>_PORT`` directly without rebuilding the full config.

    Minimal-cost variant of
    ``cli_runtime_config_from_env().anvil_ports.get(chain.lower())``,
    intended for hot paths in the intent compiler where rebuilding the
    full :class:`CliRuntimeConfig` model on every chain RPC resolution
    is wasteful.

    The dynamic env read is mandatory: ``managed.py`` mutates this env
    var at runtime when starting a fork, and the compiler must observe
    the post-mutation value, so caching is not an option.

    Returns ``None`` when the env var is unset or whitespace; raises
    ``ValueError`` on a present-but-malformed integer (same strictness
    as the typed config path).
    """
    env_var = f"ANVIL_{chain.upper()}_PORT"
    raw = os.environ.get(env_var)
    if raw is None or not raw.strip():
        return None
    return _require_int_env(env_var, raw)


def subprocess_env_with_overrides(overrides: dict[str, str]) -> dict[str, str]:
    """Return ``os.environ.copy()`` with ``overrides`` merged in.

    The CLI spawns subprocesses (the Streamlit dashboard, paper-trading
    background process) that need the full parent environment plus a few
    targeted overrides (``GATEWAY_HOST``, ``GATEWAY_PORT``,
    ``ALMANAK_GATEWAY_AUTH_TOKEN``). This helper centralises the
    ``os.environ.copy() + dict.update`` pattern so the boundary helper
    is the single thing flagged by the config-boundary lint, not every
    callsite.

    Args:
        overrides: Keys/values to set or overwrite in the returned dict.

    Returns:
        A fresh dict — caller mutations don't leak back into ``os.environ``.
    """
    env = os.environ.copy()
    env.update(overrides)
    return env


# =============================================================================
# Gas/risk override env-presence map — read by run.py's
# create_execution_orchestrator. The legacy code branched on
# ``os.environ.get(...) or os.environ.get(legacy)`` to decide whether
# the user had explicitly set a value (vs. the chain default). The
# typed shape exposes the same presence semantics as a dict of bools
# plus the optional explicit string for ``MAX_VALUE_USD``.
# =============================================================================


_GAS_RISK_OVERRIDE_VARS: dict[str, tuple[str, str]] = {
    # field_name → (prefixed_env, legacy_unprefixed_env)
    "max_gas_price_gwei": ("ALMANAK_MAX_GAS_PRICE_GWEI", "MAX_GAS_PRICE_GWEI"),
    "max_gas_cost_native": ("ALMANAK_MAX_GAS_COST_NATIVE", "MAX_GAS_COST_NATIVE"),
    "max_gas_cost_usd": ("ALMANAK_MAX_GAS_COST_USD", "MAX_GAS_COST_USD"),
    "max_slippage_bps": ("ALMANAK_MAX_SLIPPAGE_BPS", "MAX_SLIPPAGE_BPS"),
}


def gas_risk_override_presence() -> dict[str, bool]:
    """Return a presence map for the gas/risk override env vars.

    The CLI's :func:`create_execution_orchestrator` flow needs to know
    whether the operator explicitly set ``ALMANAK_MAX_GAS_PRICE_GWEI``
    (or the legacy unprefixed shape) so that the explicit value can
    override the chain-specific default. Returns ``{field_name: True}``
    when EITHER the prefixed OR the legacy unprefixed env var is set
    (any non-None value, including empty string).
    """
    presence: dict[str, bool] = {}
    for field, (prefixed, legacy) in _GAS_RISK_OVERRIDE_VARS.items():
        presence[field] = bool(os.environ.get(prefixed)) or bool(os.environ.get(legacy))
    return presence


def max_value_usd_override() -> str | None:
    """Return the raw ``ALMANAK_MAX_VALUE_USD`` / ``MAX_VALUE_USD`` value, or ``None``.

    The CLI parses this into a ``Decimal`` at the consumer side; the
    typed reader keeps the raw string semantics so the consumer can
    decide whether to raise on a malformed value (it does — the legacy
    code raised :class:`InvalidOperation`).
    """
    return os.environ.get("ALMANAK_MAX_VALUE_USD") or os.environ.get("MAX_VALUE_USD") or None


def _almanak_chain_env() -> str | None:
    """Return the lowercased ``ALMANAK_CHAIN`` env value, or ``None``.

    Mirrors the legacy ``(os.environ.get("ALMANAK_CHAIN") or "").strip().lower() or None``
    pattern used by the CLI's chain-override echo and the ``_apply_strategy_config_chain``
    helper. The runtime-config layer also reads this var via ``runtime_config_from_env``
    when no explicit ``chain=`` kwarg is passed; this helper exposes the same
    raw value to the CLI surfaces that need to compare it against the
    strategy's config-file chain.
    """
    raw = os.environ.get("ALMANAK_CHAIN") or ""
    raw = raw.strip().lower()
    return raw or None


def almanak_chain_from_env() -> str | None:
    """Public helper for the lowercased ``ALMANAK_CHAIN`` env override."""
    return _almanak_chain_env()


def chain_scoped_gwei_override(*, chain: str, prefix: str = "ALMANAK_") -> int | None:
    """Return the chain-scoped ``MAX_GAS_PRICE_GWEI_<CHAIN>`` env override.

    VIB-4879 introduces the chain-scoped escape hatch for operators who want
    explicit per-chain gwei caps. The env var name is
    ``<prefix>MAX_GAS_PRICE_GWEI_<CHAIN_UPPER>`` (e.g.
    ``ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON``). The legacy unprefixed form
    ``MAX_GAS_PRICE_GWEI_<CHAIN_UPPER>`` is also accepted for symmetry with
    the other risk env vars.

    Returns the parsed positive integer, clamped to
    :data:`almanak.framework.execution.gas.constants.SANE_GWEI_CEILING` (a
    WARNING is logged on clamp). Returns ``None`` when the env is unset.
    Raises ``ValueError`` on malformed (non-int / non-positive) values so
    typo'd ``.env`` lines fail loudly at boot rather than silently fall
    back to the chain default.
    """
    from almanak.framework.execution.gas.constants import SANE_GWEI_CEILING

    chain_upper = chain.upper()
    primary = f"{prefix}MAX_GAS_PRICE_GWEI_{chain_upper}"
    legacy = f"MAX_GAS_PRICE_GWEI_{chain_upper}"

    # gemini (VIB-4879 PR #2488 review): track which env was actually resolved
    # so error messages + clamp WARNING reference the operator's actual env
    # var name, not the prefixed form they may not have set. Also handles the
    # empty-string case correctly — an explicit empty prefixed env should NOT
    # silently fall through to the legacy form (per CodeRabbit's adjacent
    # finding); raise on the offending name.
    raw = os.environ.get(primary)
    env_name = primary
    if raw is None:
        raw = os.environ.get(legacy)
        env_name = legacy

    if raw is not None and raw.strip() == "":
        raise ValueError(f"{env_name} is set to empty/whitespace; expected a positive integer (gwei).")

    if not raw:
        return None

    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{env_name} must be a positive integer (gwei). Got: {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{env_name} must be a positive integer (gwei). Got: {value}")

    if value > SANE_GWEI_CEILING:
        logger.warning(
            "%s=%d exceeds SANE_GWEI_CEILING (%d); clamping. "
            "A gwei cap above %d gwei almost certainly indicates a misconfigured "
            "env value — verify the chain's typical gas price before raising it.",
            env_name,
            value,
            SANE_GWEI_CEILING,
            SANE_GWEI_CEILING,
        )
        return SANE_GWEI_CEILING

    return value


__all__ = [
    "DEFAULT_ANVIL_PORT",
    "DEFAULT_SOLANA_RPC_URL",
    "DEFAULT_SOLANA_VALIDATOR_PORT",
    "CliRuntimeConfig",
    "almanak_chain_from_env",
    "chain_rpc_url_from_env",
    "chain_scoped_gwei_override",
    "cli_runtime_config_from_env",
    "gas_risk_override_presence",
    "max_value_usd_override",
    "subprocess_env_with_overrides",
]
