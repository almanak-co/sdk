"""Typed backtest configuration submodel.

Phase 5c of the config-service migration (see
``docs/internal/config-service-plan.md``). Owns every env read for the
backtest cluster: paper-trading and the historical PnL providers under
``almanak/framework/backtesting/*``, plus the standalone service
runtime config consumed by ``almanak/services/backtest/*``.

Three families of config consolidated here:

* **API keys / secrets** — ``coingecko_api_key`` (CoinGecko Pro tier),
  ``thegraph_api_key`` (The Graph decentralized network), and the
  per-chain Etherscan-family keys used by the gas provider
  (``ETHERSCAN_API_KEY``, ``ARBISCAN_API_KEY`` etc.). Stored
  ``repr=False`` so a stray ``logger.info(repr(cfg))`` cannot leak
  credentials.

* **Archive RPC URLs** — the ``ARCHIVE_RPC_URL_<CHAIN>`` cluster used
  by Chainlink, TWAP, gas, and the aggregated provider. The legacy
  shape was N independent env reads keyed off the chain name; the
  typed model collapses them into a single ``dict[str, str]`` keyed
  by lowercase chain name.

* **SSL cert path** — paper-trading's spawned subprocess on macOS
  needs ``SSL_CERT_FILE`` set to a usable bundle (multiprocessing on
  macOS spawns a fresh interpreter that does not pick up the system
  trust store automatically). Stored as a typed field so the writer
  helper has one source of truth instead of inlining the legacy
  cert-search ladder twice.

Import direction
----------------
Strict (mirrors :mod:`almanak.config.runtime` and
:mod:`almanak.config.connectors`): this module MUST NOT import from
``almanak.framework.backtesting.*`` or ``almanak.services.backtest.*``.
The backtesting code imports :class:`BacktestConfig` from here at
construction time, and the standalone service imports
:class:`BacktestServiceConfig` from here at boot. Reverse imports would
create a cycle and make the typed-config service depend on the runtime
layers it is meant to feed.
"""

from __future__ import annotations

import os
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from almanak.config.env import _load_dotenv_once

# =============================================================================
# Constants — chains and env-var name maps used by the env factory.
# These are bit-for-bit mirrors of the legacy module-level constants in
# almanak/framework/backtesting/pnl/providers/{chainlink,gas,twap}.py;
# the consumer modules keep their own re-exports for back-compat (tests
# import them by name) and feed them into this factory.
# =============================================================================

# Chains the historical providers (Chainlink, TWAP, gas) read archive
# RPC URLs for. Lowercase to match the field-key convention; the env
# var itself is uppercased (``ARCHIVE_RPC_URL_ETHEREUM`` etc).
DEFAULT_ARCHIVE_RPC_CHAINS: tuple[str, ...] = (
    "ethereum",
    "arbitrum",
    "base",
    "optimism",
    "polygon",
    "avalanche",
)

# Etherscan-family API key env vars by chain. Mirrors
# ``ETHERSCAN_API_KEY_ENV_VARS`` in
# ``almanak/framework/backtesting/pnl/providers/gas.py``; the gas
# provider re-exports its copy for back-compat.
DEFAULT_GAS_API_KEY_ENV_VARS: dict[str, str] = {
    "ethereum": "ETHERSCAN_API_KEY",
    "arbitrum": "ARBISCAN_API_KEY",
    "optimism": "OPTIMISTIC_ETHERSCAN_API_KEY",
    "base": "BASESCAN_API_KEY",
    "polygon": "POLYGONSCAN_API_KEY",
    "bsc": "BSCSCAN_API_KEY",
    "avalanche": "SNOWTRACE_API_KEY",
}

DEFAULT_BACKTEST_SERVICE_HOST: str = "0.0.0.0"
DEFAULT_BACKTEST_SERVICE_PORT: int = 8000
DEFAULT_BACKTEST_SERVICE_WORKERS: int = 1
DEFAULT_BACKTEST_MAX_JOBS: int = 4
DEFAULT_BACKTEST_MAX_PAPER_SESSIONS: int = 2
DEFAULT_BACKTEST_LOG_LEVEL: str = "info"


# =============================================================================
# GasApiConfig — per-chain Etherscan-family credentials
# =============================================================================


class GasApiConfig(BaseModel):
    """Per-chain Etherscan-family API keys for the gas price provider.

    Keys are lowercase chain names; values are the resolved API key
    strings (``""`` when nothing is configured for that chain). The
    field is stored ``repr=False`` so the populated dict (which carries
    every block-explorer API key the deployment has) does not appear
    in default ``__repr__`` output.

    The legacy code lazily read these env vars in the
    ``EtherscanGasPriceProvider.__init__`` body; the typed shape moves
    the lookup to construction time, exposes the resolved values for
    diagnostics (``logger.info(extra={...})`` already flagged "chains
    with keys"), and centralises the env-name mapping with
    :data:`DEFAULT_GAS_API_KEY_ENV_VARS`.
    """

    api_keys: dict[str, str] = Field(default_factory=dict, repr=False)
    """Map of lowercase chain name → API key (e.g. ``{"ethereum": "..."}``)."""

    model_config = ConfigDict(extra="forbid")


# =============================================================================
# BacktestConfig — typed, validated, secret-safe
# =============================================================================


class BacktestConfig(BaseModel):
    """Typed configuration for the backtesting cluster.

    Every field is optional from the consumer's standpoint — when a
    secret is ``None`` (or an entry is missing from a dict-shaped
    field), the consumer falls back to its existing missing-env
    behaviour (CoinGecko free tier, anonymous TheGraph access, RPC-only
    gas estimation, etc). Field-by-field policy is preserved bit-for-bit
    on cutover; this model is the *single env reader*, not a behavioural
    rewrite.

    Secret fields (``coingecko_api_key``, ``thegraph_api_key``,
    ``gas_api.api_keys``) carry ``Field(repr=False)`` so the model's
    ``__repr__`` never leaks credentials into logs. Plaintext is
    intentional — providers today consume the raw string (no
    ``SecretStr`` round-trip) and changing the on-the-wire type on
    cutover would be a behavioural change.
    """

    # -------------------------------------------------------------------------
    # API keys — repr suppressed.
    # -------------------------------------------------------------------------

    coingecko_api_key: str | None = Field(default=None, repr=False)
    """CoinGecko Pro API key (``COINGECKO_API_KEY``).

    Optional — when set, ``CoinGeckoDataProvider`` picks the Pro
    endpoint (``https://pro-api.coingecko.com/api/v3``) and the higher
    rate-limit tier; the benchmark helpers read the same value to
    decide which endpoint to call. When unset, every consumer falls
    back to the public free endpoint (``https://api.coingecko.com``).
    """

    thegraph_api_key: str | None = Field(default=None, repr=False)
    """The Graph decentralized-network API key (``THEGRAPH_API_KEY``).

    Optional — when set, ``SubgraphClientConfig`` adds the key to the
    Authorization header for every subgraph query. When unset, the
    client targets the anonymous public tier (lower rate limits).
    """

    alchemy_api_key: str | None = Field(default=None, repr=False)
    """Alchemy RPC API key (``ALCHEMY_API_KEY``).

    Optional — used by the perps-position-reader legacy / fallback
    path (``framework/valuation/perps_position_reader.py``) to build
    an Alchemy RPC URL when no gateway client is supplied. Hosted-mode
    callers always pass ``gateway_client``; this field is read only by
    paper-trading + backtest harnesses that bypass the gateway.

    The gateway-tier copy lives at ``GatewayConfig.alchemy_api_key``
    (read by the gateway server's RPC plumbing). Both fields read the
    same env var; they exist on different sides of the gateway boundary.
    """

    # -------------------------------------------------------------------------
    # Archive RPC URLs — borderline secret (some providers carry a key
    # in the path). ``repr=False`` preserves the legacy redaction
    # semantics; consumers read individual entries by chain.
    # -------------------------------------------------------------------------

    archive_rpc_urls: dict[str, str] = Field(default_factory=dict, repr=False)
    """Per-chain archive RPC URL map keyed by lowercase chain name.

    Replaces the legacy ``ARCHIVE_RPC_URL_<CHAIN>`` env-var cluster
    with a single typed dict. The Chainlink, TWAP, gas, and aggregated
    providers all consume from this map; missing entries surface as
    "no archive access for chain X", which is the same fallback the
    legacy code took when the env var was unset.
    """

    # -------------------------------------------------------------------------
    # Gas API — per-chain Etherscan-family credentials.
    # -------------------------------------------------------------------------

    gas_api: GasApiConfig = Field(default_factory=GasApiConfig)
    """Per-chain Etherscan-family API keys for ``EtherscanGasPriceProvider``."""

    # -------------------------------------------------------------------------
    # SSL cert file — paper-trading's spawned subprocess hint.
    # -------------------------------------------------------------------------

    ssl_cert_file: str | None = None
    """Path to an SSL CA bundle for paper-trading's spawned subprocess.

    macOS multiprocessing spawns a fresh interpreter that does not pick
    up the system trust store automatically; ``ssl.SSLContext`` then
    fails to verify HTTPS connections. The legacy code searched two
    OS-specific paths (``/private/etc/ssl/cert.pem``,
    ``/etc/ssl/cert.pem``) and assigned the first hit to
    ``os.environ["SSL_CERT_FILE"]``.

    The factory ``backtest_config_from_env`` runs the same search at
    config-construction time (preferring an existing
    ``SSL_CERT_FILE`` env value, then falling back to certifi's bundle
    when available, then the OS paths). The helper
    :func:`apply_ssl_cert_file` writes the resolved value back to
    ``os.environ`` so the spawned subprocess inherits it. Setting this
    field to ``None`` disables the helper (no env mutation).
    """

    service: BacktestServiceConfig = Field(default_factory=lambda: BacktestServiceConfig())
    """Standalone backtest-service runtime config.

    Part of the same backtest cluster and built as part of the one-shot
    :func:`almanak.config.load_config` flow. Service boot paths should prefer
    ``load_config().backtest.service`` over a second env-reading factory.
    """

    model_config = ConfigDict(
        # Reject typos at the service boundary — a misspelt kwarg here
        # would silently flow into the config without populating any
        # consumer field.
        extra="forbid",
    )


class BacktestServiceConfig(BaseModel):
    """Typed runtime config for the standalone backtest service."""

    host: str = DEFAULT_BACKTEST_SERVICE_HOST
    port: int = DEFAULT_BACKTEST_SERVICE_PORT
    workers: int = DEFAULT_BACKTEST_SERVICE_WORKERS
    max_concurrent_backtest_jobs: int = DEFAULT_BACKTEST_MAX_JOBS
    max_concurrent_paper_sessions: int = DEFAULT_BACKTEST_MAX_PAPER_SESSIONS
    log_level: str = DEFAULT_BACKTEST_LOG_LEVEL

    model_config = ConfigDict(extra="forbid")


def _backtest_service_config_from_env_values() -> BacktestServiceConfig:
    """Build the standalone backtest-service config from current env values."""
    return BacktestServiceConfig(
        host=os.environ.get("BACKTEST_SERVICE_HOST", DEFAULT_BACKTEST_SERVICE_HOST),
        port=int(os.environ.get("BACKTEST_SERVICE_PORT", str(DEFAULT_BACKTEST_SERVICE_PORT))),
        workers=int(os.environ.get("BACKTEST_SERVICE_WORKERS", str(DEFAULT_BACKTEST_SERVICE_WORKERS))),
        max_concurrent_backtest_jobs=int(os.environ.get("BACKTEST_MAX_JOBS", str(DEFAULT_BACKTEST_MAX_JOBS))),
        max_concurrent_paper_sessions=int(
            os.environ.get("BACKTEST_MAX_PAPER_SESSIONS", str(DEFAULT_BACKTEST_MAX_PAPER_SESSIONS))
        ),
        log_level=os.environ.get("BACKTEST_LOG_LEVEL", DEFAULT_BACKTEST_LOG_LEVEL),
    )


# =============================================================================
# SSL cert resolution — boundary-side logic preserved verbatim
# =============================================================================


# Legacy paths checked by paper/background.py. macOS ships
# ``/private/etc/ssl/cert.pem``; some BSD/Linux installs use
# ``/etc/ssl/cert.pem``. The list is ordered by likelihood on the
# affected platform (macOS, where multiprocessing's spawn mode lost
# the trust store).
_SSL_CERT_FALLBACK_PATHS: tuple[str, ...] = (
    "/private/etc/ssl/cert.pem",
    "/etc/ssl/cert.pem",
)


def _resolve_ssl_cert_file() -> str | None:
    """Resolve a usable SSL CA bundle path; ``None`` if no candidate.

    Lookup order:

    1. ``SSL_CERT_FILE`` env var, if set and the file exists. Honours
       an explicit operator override.
    2. ``certifi.where()``, if certifi is installed. Standard answer
       on macOS — certifi ships an up-to-date Mozilla root CA bundle.
    3. The OS paths checked by the legacy code
       (``/private/etc/ssl/cert.pem`` then ``/etc/ssl/cert.pem``).

    Returns ``None`` when no candidate exists; the boundary helper
    treats that as "no override" and leaves ``os.environ`` alone.
    """
    explicit = os.environ.get("SSL_CERT_FILE")
    if explicit and os.path.exists(explicit):
        return explicit
    try:
        import certifi  # noqa: PLC0415 — soft optional dep, lazy import is correct here

        path = certifi.where()
        if path and os.path.exists(path):
            return path
    except ImportError:
        # certifi is a transitive dep of aiohttp / requests in this
        # codebase, so it should always be present — but the import
        # guard keeps the helper safe in stripped-down test envs.
        pass
    for path in _SSL_CERT_FALLBACK_PATHS:
        if os.path.exists(path):
            return path
    return None


# =============================================================================
# Public factory — single env-reading entry point for backtest config
# =============================================================================


def backtest_config_from_env(
    *,
    dotenv_path: str | None = None,
    archive_rpc_chains: tuple[str, ...] = DEFAULT_ARCHIVE_RPC_CHAINS,
    gas_api_key_env_vars: dict[str, str] | None = None,
) -> BacktestConfig:
    """Construct a :class:`BacktestConfig` from environment variables.

    Single env-reading entry point for every consumer under
    ``framework/backtesting/*``. Mirrors the legacy per-provider
    lookups bit-for-bit:

    * ``COINGECKO_API_KEY`` → ``coingecko_api_key`` (consumed by
      ``CoinGeckoDataProvider``, the benchmark helpers, and the
      crisis-runner date-range guard).
    * ``THEGRAPH_API_KEY`` → ``thegraph_api_key`` (consumed by
      ``SubgraphClientConfig``).
    * ``ARCHIVE_RPC_URL_<CHAIN>`` → ``archive_rpc_urls[chain]``
      (consumed by Chainlink, TWAP, gas, and aggregated providers).
      Empty values are *not* stored — the dict carries only chains
      that actually have an URL configured, matching the legacy
      ``if url:`` guard.
    * ``ETHERSCAN_API_KEY`` / ``ARBISCAN_API_KEY`` /
      ``OPTIMISTIC_ETHERSCAN_API_KEY`` / ``BASESCAN_API_KEY`` /
      ``POLYGONSCAN_API_KEY`` / ``BSCSCAN_API_KEY`` /
      ``SNOWTRACE_API_KEY`` → ``gas_api.api_keys[chain]`` (consumed
      by ``EtherscanGasPriceProvider``).
    * ``SSL_CERT_FILE`` (or certifi / OS fallback) → ``ssl_cert_file``
      (consumed by paper-trading's :func:`apply_ssl_cert_file`).

    Args:
        dotenv_path: Optional ``.env`` path; routed through the shared
            single-shot loader.
        archive_rpc_chains: Override the chain list for the
            ``ARCHIVE_RPC_URL_<CHAIN>`` lookup. Defaults to the union
            of chains the legacy provider modules covered. Tests pass
            a custom tuple to exercise specific chains; production
            should use the default.
        gas_api_key_env_vars: Override the chain → env-var name map
            for the gas provider. Defaults to the legacy
            ``ETHERSCAN_API_KEY_ENV_VARS`` shape.
    """
    _load_dotenv_once(dotenv_path)

    archive_rpc_urls: dict[str, str] = {}
    for chain in archive_rpc_chains:
        env_var = f"ARCHIVE_RPC_URL_{chain.upper()}"
        url = os.environ.get(env_var, "")
        if url:
            archive_rpc_urls[chain.lower()] = url

    gas_api_keys: dict[str, str] = {}
    chain_to_env = gas_api_key_env_vars if gas_api_key_env_vars is not None else DEFAULT_GAS_API_KEY_ENV_VARS
    for chain, env_var in chain_to_env.items():
        key = os.environ.get(env_var, "")
        if key:
            gas_api_keys[chain.lower()] = key

    kwargs: dict[str, Any] = {
        "coingecko_api_key": os.environ.get("COINGECKO_API_KEY") or None,
        "thegraph_api_key": os.environ.get("THEGRAPH_API_KEY") or None,
        "alchemy_api_key": os.environ.get("ALCHEMY_API_KEY") or None,
        "archive_rpc_urls": archive_rpc_urls,
        "gas_api": GasApiConfig(api_keys=gas_api_keys),
        "ssl_cert_file": _resolve_ssl_cert_file(),
        "service": _backtest_service_config_from_env_values(),
    }
    return BacktestConfig(**kwargs)


def backtest_service_config_from_env() -> BacktestServiceConfig:
    """Construct the standalone backtest-service config from environment.

    Use this narrow loader when only ``BACKTEST_*`` env vars are needed
    (standalone service boot, tests, and the legacy
    :class:`almanak.services.backtest.config.BacktestServiceConfig`
    adapter). It keeps the failure surface scoped to backtest inputs
    instead of validating the full config tree. Boot paths inside the
    integrated SDK that already consume the full tree should use
    ``load_config().backtest.service`` directly.
    """
    _load_dotenv_once()
    return _backtest_service_config_from_env_values()


# =============================================================================
# Boundary helper — controlled SSL_CERT_FILE env mutation
# =============================================================================


def apply_ssl_cert_file(cfg: BacktestConfig) -> None:
    """Apply ``cfg.ssl_cert_file`` to ``os.environ["SSL_CERT_FILE"]``.

    Paper-trading spawns a separate Python interpreter via
    ``multiprocessing.Process``; on macOS the spawned interpreter loses
    the system trust store and HTTPS connections fail at SSL
    verification. The legacy fix wrote ``os.environ["SSL_CERT_FILE"]``
    so the child process would inherit a usable bundle path at fork
    time.

    The mutation is intentional — there is no portable way to thread
    the path explicitly through ``multiprocessing.Process`` on every
    platform, and the env-mutation form has been the supported
    workaround in CPython documentation for years. The helper is
    idempotent: if the env var already points at a valid path, it is
    left alone (so an operator override survives).

    Calling code is paper-trading's :class:`BackgroundPaperTrader`
    (the parent side, before ``Process.start()``). The helper is on
    the permanent allowlist for the config-boundary lint.

    Args:
        cfg: the active backtest config. ``cfg.ssl_cert_file is None``
            disables the helper (no env mutation).
    """
    if cfg.ssl_cert_file is None:
        return
    existing = os.environ.get("SSL_CERT_FILE")
    if existing and os.path.exists(existing):
        # Operator explicitly set a usable value already; preserve it.
        return
    if not os.path.exists(cfg.ssl_cert_file):
        # Resolved path went stale between config build and helper call
        # (e.g. user uninstalled certifi). Fail silently like the legacy
        # `for path in [...]: if os.path.exists(path): break` ladder.
        return
    os.environ["SSL_CERT_FILE"] = cfg.ssl_cert_file


__all__ = [
    "DEFAULT_ARCHIVE_RPC_CHAINS",
    "DEFAULT_BACKTEST_LOG_LEVEL",
    "DEFAULT_BACKTEST_MAX_JOBS",
    "DEFAULT_BACKTEST_MAX_PAPER_SESSIONS",
    "DEFAULT_BACKTEST_SERVICE_HOST",
    "DEFAULT_BACKTEST_SERVICE_PORT",
    "DEFAULT_BACKTEST_SERVICE_WORKERS",
    "DEFAULT_GAS_API_KEY_ENV_VARS",
    "BacktestConfig",
    "BacktestServiceConfig",
    "GasApiConfig",
    "apply_ssl_cert_file",
    "backtest_config_from_env",
    "backtest_service_config_from_env",
]
