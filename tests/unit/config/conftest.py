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
    "ALMANAK_GATEWAY_PORTFOLIO_API_KEY",
    "ALMANAK_GATEWAY_PORTFOLIO_PROVIDERS",
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
    "AGENT_ID",
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

    def _build(
        *, mode: str = "local", **overrides: object
    ) -> LocalConfig | HostedConfig:
        _scrub_env(monkeypatch)
        gateway_overrides = overrides.pop("gateway", {}) or {}
        if not isinstance(gateway_overrides, dict):
            raise TypeError(
                "config_factory(gateway=...) expects a dict of GatewayConfig kwargs."
            )
        gateway = GatewayConfig(**gateway_overrides)
        if mode == "hosted":
            return HostedConfig(gateway=gateway, **overrides)
        if mode == "local":
            return LocalConfig(gateway=gateway, **overrides)
        raise ValueError(f"Unknown mode: {mode!r}. Expected 'local' or 'hosted'.")

    return _build
