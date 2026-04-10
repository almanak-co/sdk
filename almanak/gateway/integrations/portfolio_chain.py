"""Multi-provider portfolio chain with circuit-breaker failover.

PortfolioProviderChain tries configured providers in priority order.
Each provider has its own CircuitBreaker — when a provider fails
repeatedly, it is skipped for a cooldown period.

The chain returns None when ALL providers fail or are circuit-broken.
Callers must handle the None case explicitly.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

from almanak.gateway.integrations.base import BaseIntegration
from almanak.gateway.integrations.circuit_breaker import CircuitBreaker
from almanak.gateway.integrations.models import WalletPortfolioSnapshot
from almanak.gateway.utils.rpc_provider import _get_gateway_api_key

logger = logging.getLogger(__name__)


@dataclass
class PortfolioProviderConfig:
    """Configuration for a single portfolio provider."""

    name: str
    api_key: str | None = None
    priority: int = 0
    cache_ttl: int = 60
    chain_filter: list[str] = field(default_factory=list)  # Phase 2: per-chain routing


class PortfolioProviderChain:
    """Tries portfolio providers in priority order with per-provider circuit breaking."""

    def __init__(self, providers: list[BaseIntegration]):
        self._providers = [p for p in providers if p.supports_portfolio()]
        self._circuits: dict[str, CircuitBreaker] = {
            p.name: CircuitBreaker(
                failure_threshold=3,
                failure_window_seconds=60,
                recovery_seconds=300,
            )
            for p in self._providers
        }

    @property
    def providers(self) -> list[BaseIntegration]:
        """Return the list of providers (for introspection/testing)."""
        return list(self._providers)

    def get_provider(self, name: str) -> BaseIntegration | None:
        """Get a specific provider by name. Returns None if not found."""
        for p in self._providers:
            if p.name == name:
                return p
        return None

    async def get_wallet_portfolio(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot | None:
        """Try providers in order, returning the first successful result."""
        return await self._try_providers("get_wallet_portfolio", wallet_address, chain)

    async def get_wallet_positions(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot | None:
        """Try providers in order, returning the first successful result."""
        return await self._try_providers("get_wallet_positions", wallet_address, chain)

    async def _try_providers(self, method_name: str, wallet_address: str, chain: str) -> WalletPortfolioSnapshot | None:
        for provider in self._providers:
            circuit = self._circuits[provider.name]
            if circuit.is_open:
                logger.debug("Skipping provider %s (circuit open)", provider.name)
                continue
            try:
                method = getattr(provider, method_name)
                snapshot = await method(wallet_address=wallet_address, chain=chain)
                circuit.record_success()
                return snapshot
            except Exception as e:
                circuit.record_failure()
                logger.warning(
                    "Provider %s failed for %s on %s: %s",
                    provider.name,
                    wallet_address,
                    chain,
                    e,
                )
                continue
        return None

    async def close(self) -> None:
        """Close all provider connections."""
        for provider in self._providers:
            await provider.close()


def get_portfolio_provider_configs(
    portfolio_providers_csv: str | None,
    portfolio_api_key: str | None,
    portfolio_api_provider: str = "zerion",
    portfolio_api_cache_ttl: int = 60,
) -> list[PortfolioProviderConfig]:
    """Parse provider configuration from settings/env vars.

    Supports two modes:
    - Multi-provider: PORTFOLIO_PROVIDERS=zerion,moralis with per-provider env vars
    - Legacy single-provider: PORTFOLIO_API_KEY with portfolio_api_provider
    """
    if portfolio_providers_csv:
        names = [n.strip() for n in portfolio_providers_csv.split(",") if n.strip()]
    elif portfolio_api_key:
        names = [portfolio_api_provider]
    else:
        return []

    configs = []
    for priority, name in enumerate(names):
        key_env = f"{name.upper()}_API_KEY"
        api_key = _get_gateway_api_key(key_env)
        # Legacy fallback: if this is the zerion provider and no ZERION_API_KEY,
        # use the portfolio_api_key
        if not api_key and name == "zerion" and portfolio_api_key:
            api_key = portfolio_api_key

        chain_filter_env = os.environ.get(f"{name.upper()}_CHAIN_FILTER", "")
        chain_filter = [c.strip() for c in chain_filter_env.split(",") if c.strip()] if chain_filter_env else []

        configs.append(
            PortfolioProviderConfig(
                name=name,
                api_key=api_key,
                priority=priority,
                cache_ttl=int(os.environ.get(f"{name.upper()}_CACHE_TTL", str(portfolio_api_cache_ttl))),
                chain_filter=chain_filter,
            )
        )
    return configs


def build_portfolio_chain(
    portfolio_providers_csv: str | None,
    portfolio_api_key: str | None,
    portfolio_api_provider: str = "zerion",
    portfolio_api_cache_ttl: int = 60,
) -> PortfolioProviderChain | None:
    """Construct the provider chain from settings. Returns None if no providers configured."""
    from almanak.gateway.integrations.zerion import ZerionIntegration

    configs = get_portfolio_provider_configs(
        portfolio_providers_csv=portfolio_providers_csv,
        portfolio_api_key=portfolio_api_key,
        portfolio_api_provider=portfolio_api_provider,
        portfolio_api_cache_ttl=portfolio_api_cache_ttl,
    )
    if not configs:
        return None

    providers: list[BaseIntegration] = []
    for cfg in configs:
        if cfg.name == "zerion" and cfg.api_key:
            providers.append(ZerionIntegration(api_key=cfg.api_key, cache_ttl=cfg.cache_ttl))
        elif cfg.name == "moralis" and cfg.api_key:
            from almanak.gateway.integrations.moralis import MoralisIntegration

            providers.append(MoralisIntegration(api_key=cfg.api_key, cache_ttl=cfg.cache_ttl))
        elif cfg.name == "okx" and cfg.api_key:
            from almanak.gateway.integrations.okx import OkxIntegration

            providers.append(OkxIntegration(api_key=cfg.api_key, cache_ttl=cfg.cache_ttl))
        else:
            logger.info("Skipping unconfigured portfolio provider: %s", cfg.name)

    return PortfolioProviderChain(providers) if providers else None
