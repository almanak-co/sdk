"""Shared fixtures for ``almanak.config`` unit tests.

The Phase 0 surface is intentionally small — only the gateway submodel is
populated — so ``config_factory`` accepts a ``mode`` discriminator and a
``gateway=`` kwarg that is unpacked into a fresh :class:`GatewayConfig`. The
factory's API is forward-compatible: future submodels (runtime, simulation,
backtest) drop into the same kwargs map without breaking call sites.

Tests using this fixture must NOT rely on env state leaking through. The
factory scrubs every ``ALMANAK_GATEWAY_*`` and unprefixed ALMANAK / Polymarket
fallback so its returns are deterministic regardless of the developer's
``.env`` and the order pytest happens to collect tests.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from almanak.config import GatewayConfig, HostedConfig, LocalConfig

# Env vars the GatewaySettings model + its fallback ladders read. Listed
# explicitly so the scrub is auditable; if a future field is added to the
# model and a test forgets it, the failing test points at the missing entry.
_GATEWAY_ENV_VARS: tuple[str, ...] = (
    # ALMANAK_GATEWAY_* prefix (pydantic auto-binds these to fields).
    "ALMANAK_GATEWAY_HOST",
    "ALMANAK_GATEWAY_PORT",
    "ALMANAK_GATEWAY_DEBUG",
    "ALMANAK_GATEWAY_LOG_LEVEL",
    "ALMANAK_GATEWAY_GRPC_HOST",
    "ALMANAK_GATEWAY_GRPC_PORT",
    "ALMANAK_GATEWAY_GRPC_MAX_WORKERS",
    "ALMANAK_GATEWAY_NETWORK",
    "ALMANAK_GATEWAY_CHAINS",
    "ALMANAK_GATEWAY_ENABLE_MANUAL_PRICE_OVERRIDES",
    "ALMANAK_GATEWAY_METRICS_ENABLED",
    "ALMANAK_GATEWAY_METRICS_PORT",
    "ALMANAK_GATEWAY_AUDIT_ENABLED",
    "ALMANAK_GATEWAY_AUDIT_LOG_LEVEL",
    "ALMANAK_GATEWAY_ALCHEMY_API_KEY",
    "ALMANAK_GATEWAY_COINGECKO_API_KEY",
    "ALMANAK_GATEWAY_ENSO_API_KEY",
    "ALMANAK_GATEWAY_PENDLE_API_KEY",
    "ALMANAK_GATEWAY_THEGRAPH_API_KEY",
    "ALMANAK_GATEWAY_PORTFOLIO_API_KEY",
    "ALMANAK_GATEWAY_PORTFOLIO_API_PROVIDER",
    "ALMANAK_GATEWAY_PORTFOLIO_API_CACHE_TTL",
    "ALMANAK_GATEWAY_PORTFOLIO_PROVIDERS",
    "ALMANAK_GATEWAY_TENDERLY_ACCOUNT_SLUG",
    "ALMANAK_GATEWAY_TENDERLY_PROJECT_SLUG",
    "ALMANAK_GATEWAY_TENDERLY_ACCESS_KEY",
    "ALMANAK_GATEWAY_DEXSCREENER_MIN_LIQUIDITY_USD",
    "ALMANAK_GATEWAY_DEXSCREENER_MIN_VOLUME_USD",
    "ALMANAK_GATEWAY_DEXSCREENER_MIN_TURNOVER_RATIO",
    "ALMANAK_GATEWAY_DEXSCREENER_DOMINANCE_MULTIPLE",
    "ALMANAK_GATEWAY_POLYMARKET_NETWORK",
    "ALMANAK_GATEWAY_POLYMARKET_MARKET_CACHE_TTL_SECONDS",
    "ALMANAK_GATEWAY_ANVIL_WATCHDOG_INTERVAL",
    "ALMANAK_GATEWAY_PRIVATE_KEY",
    "ALMANAK_GATEWAY_SOLANA_PRIVATE_KEY",
    "ALMANAK_GATEWAY_EOA_ADDRESS",
    "ALMANAK_GATEWAY_SAFE_ADDRESS",
    "ALMANAK_GATEWAY_ZODIAC_ROLES_ADDRESS",
    "ALMANAK_GATEWAY_SIGNER_SERVICE_URL",
    "ALMANAK_GATEWAY_SIGNER_SERVICE_JWT",
    "ALMANAK_GATEWAY_POLYMARKET_WALLET_ADDRESS",
    "ALMANAK_GATEWAY_POLYMARKET_PRIVATE_KEY",
    "ALMANAK_GATEWAY_POLYMARKET_API_KEY",
    "ALMANAK_GATEWAY_POLYMARKET_SECRET",
    "ALMANAK_GATEWAY_POLYMARKET_PASSPHRASE",
    "ALMANAK_GATEWAY_DATABASE_URL",
    "ALMANAK_GATEWAY_AUTH_TOKEN",
    "ALMANAK_GATEWAY_TIMEOUT",
    "ALMANAK_GATEWAY_ALLOW_INSECURE",
    # Unprefixed ALMANAK_* fallbacks consumed by ``_fallback_env_vars`` and
    # ``_resolve_polymarket_credentials``.
    "ALMANAK_PRIVATE_KEY",
    "SOLANA_PRIVATE_KEY",
    "ALMANAK_EOA_ADDRESS",
    "ALMANAK_SAFE_ADDRESS",
    "ALMANAK_ZODIAC_ADDRESS",
    "ALMANAK_SIGNER_SERVICE_URL",
    "ALMANAK_SIGNER_SERVICE_JWT",
    "ALCHEMY_API_KEY",
    "COINGECKO_API_KEY",
    "ENSO_API_KEY",
    "ALMANAK_PORTFOLIO_API_KEY",
    "ZERION_API_KEY",
    "PORTFOLIO_PROVIDERS",
    "TENDERLY_ACCOUNT_SLUG",
    "TENDERLY_PROJECT_SLUG",
    "TENDERLY_ACCESS_KEY",
    "ALMANAK_DEXSCREENER_MIN_LIQUIDITY_USD",
    "ALMANAK_DEXSCREENER_MIN_VOLUME_USD",
    "ALMANAK_DEXSCREENER_MIN_TURNOVER_RATIO",
    "ALMANAK_DEXSCREENER_DOMINANCE_MULTIPLE",
    "ALMANAK_POLYMARKET_NETWORK",
    "ALMANAK_POLYMARKET_MARKET_CACHE_TTL_SECONDS",
    "ALMANAK_ANVIL_WATCHDOG_INTERVAL",
    # Polymarket bare-name + ALMANAK_* alias ladder.
    "POLYMARKET_WALLET_ADDRESS",
    "ALMANAK_POLYMARKET_WALLET_ADDRESS",
    "POLYMARKET_PRIVATE_KEY",
    "ALMANAK_POLYMARKET_PRIVATE_KEY",
    "POLYMARKET_API_KEY",
    "ALMANAK_POLYMARKET_API_KEY",
    "POLYMARKET_SECRET",
    "ALMANAK_POLYMARKET_SECRET",
    "POLYMARKET_PASSPHRASE",
    "ALMANAK_POLYMARKET_PASSPHRASE",
    # Deployment-mode discriminator.
    "ALMANAK_IS_HOSTED",
    "ALMANAK_DEPLOYMENT_ID",
    # Backtest env vars (Phase 5c). The factory ``backtest_config_from_env``
    # reads these eagerly via the LocalConfig / HostedConfig
    # ``default_factory`` path; if a developer's ``.env`` carries any of
    # these, the bare ``LocalConfig()`` construction in tests would
    # silently pick them up.
    "THEGRAPH_API_KEY",
    "ARCHIVE_RPC_URL_ETHEREUM",
    "ARCHIVE_RPC_URL_ARBITRUM",
    "ARCHIVE_RPC_URL_BASE",
    "ARCHIVE_RPC_URL_OPTIMISM",
    "ARCHIVE_RPC_URL_POLYGON",
    "ARCHIVE_RPC_URL_AVALANCHE",
    "ETHERSCAN_API_KEY",
    "ARBISCAN_API_KEY",
    "OPTIMISTIC_ETHERSCAN_API_KEY",
    "BASESCAN_API_KEY",
    "POLYGONSCAN_API_KEY",
    "BSCSCAN_API_KEY",
    "SNOWTRACE_API_KEY",
    # SSL cert hint — the typed config falls back to certifi when no
    # explicit path is set, so test runs on hosts with a usable certifi
    # bundle still get a populated ``ssl_cert_file`` field. That's the
    # production-aligned behaviour and tests asserting ``ssl_cert_file
    # is not None`` should accept it.
    "SSL_CERT_FILE",
    # CLI-runtime env vars (Phase 5e). The factory ``cli_runtime_config_from_env``
    # reads these eagerly via the LocalConfig / HostedConfig
    # ``default_factory`` path; if a developer's ``.env`` carries any of
    # these, the bare ``LocalConfig()`` construction in tests would
    # silently pick them up.
    "GATEWAY_AUTH_TOKEN",
    "ALMANAK_GATEWAY_WALLETS",
    "ALMANAK_GATEWAY_SAFE_MODE",
    "ALMANAK_GATEWAY_SAFE_ADDRESS",
    "ALMANAK_EXECUTION_MODE",
    "SOLANA_RPC_URL",
    "SOLANA_VALIDATOR_PORT",
    "ALMANAK_RECONCILIATION_ENFORCEMENT",
    "ALMANAK_ALLOW_HARDCODED_PRICES",
    "CI",
    # ``ANVIL_<CHAIN>_PORT`` cluster — read for every chain in the default
    # ``anvil_chains`` tuple of ``cli_runtime_config_from_env``. Listed
    # explicitly so a stray ``ANVIL_ARBITRUM_PORT=8546`` in the developer's
    # shell can't leak into the bare ``LocalConfig()`` construction.
    "ANVIL_ETHEREUM_PORT",
    "ANVIL_ARBITRUM_PORT",
    "ANVIL_OPTIMISM_PORT",
    "ANVIL_POLYGON_PORT",
    "ANVIL_BASE_PORT",
    "ANVIL_AVALANCHE_PORT",
    "ANVIL_BSC_PORT",
    "ANVIL_LINEA_PORT",
    "ANVIL_BLAST_PORT",
    "ANVIL_MANTLE_PORT",
    "ANVIL_BERACHAIN_PORT",
    "ANVIL_SONIC_PORT",
    "ANVIL_MONAD_PORT",
    "ANVIL_XLAYER_PORT",
    "ANVIL_ZEROG_PORT",
    "ANVIL_PLASMA_PORT",
    # ``RuntimeConfig`` env reads (PR #2152 review). These are not on the
    # default_factory path today (``LocalConfig.runtime`` defaults to
    # ``None`` and is wired explicitly after strategy load), but tests that
    # call ``runtime_config_from_env()`` directly need the same isolation
    # so a stray developer ``.env`` cannot bleed into the parity tests.
    "ALMANAK_CHAIN",
    "ALMANAK_CHAINS",
    "ALMANAK_RPC_URL",
    "RPC_URL",
    "ALMANAK_NETWORK",
    "ALMANAK_TX_TIMEOUT_SECONDS",
    "ALMANAK_MAX_GAS_PRICE_GWEI",
    "ALMANAK_MAX_GAS_COST_NATIVE",
    "ALMANAK_MAX_GAS_COST_USD",
    "ALMANAK_MAX_SLIPPAGE_BPS",
    "ALMANAK_MAX_TX_VALUE_ETH",
    "ALMANAK_BASE_RETRY_DELAY",
    "ALMANAK_MAX_RETRY_DELAY",
    "ALMANAK_MAX_RETRIES",
    "ALMANAK_SIMULATION_ENABLED",
    "ALMANAK_DATA_FRESHNESS_POLICY",
    "ALMANAK_STALE_DATA_THRESHOLD_SECONDS",
    # Phase 6 — agent_tools (LLM client) + framework (toggles + paths)
    # env vars. Same eager-factory rationale as Phase 5: bare
    # ``LocalConfig()`` construction in tests would silently pick up
    # whatever the developer's ``.env`` carries.
    "AGENT_LLM_API_KEY",
    "AGENT_LLM_BASE_URL",
    "AGENT_LLM_MODEL",
    "XDG_CACHE_HOME",
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


def _scrub_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _GATEWAY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def gateway_env_scrub(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Scrub every gateway-relevant env var; return the same monkeypatch."""
    _scrub_env(monkeypatch)
    return monkeypatch


@pytest.fixture
def config_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[..., LocalConfig | HostedConfig]:
    """Build a typed ``LocalConfig`` / ``HostedConfig`` without env coupling.

    Usage:

        def test_something(config_factory):
            config = config_factory(mode="local", gateway={"grpc_port": 50071})

    The factory unpacks ``gateway=`` into a fresh :class:`GatewayConfig` so the
    submodel can be customised without touching env vars. Future submodels
    (runtime, simulation, backtest) will accept the same kwargs shape.
    """

    def _build(*, mode: str = "local", **overrides: object) -> LocalConfig | HostedConfig:
        _scrub_env(monkeypatch)
        gateway_overrides = overrides.pop("gateway", {}) or {}
        if not isinstance(gateway_overrides, dict):
            raise TypeError("config_factory(gateway=...) expects a dict of GatewayConfig kwargs.")
        gateway = GatewayConfig(**gateway_overrides)
        if mode == "hosted":
            return HostedConfig(gateway=gateway, **overrides)
        if mode == "local":
            return LocalConfig(gateway=gateway, **overrides)
        raise ValueError(f"Unknown mode: {mode!r}. Expected 'local' or 'hosted'.")

    return _build
