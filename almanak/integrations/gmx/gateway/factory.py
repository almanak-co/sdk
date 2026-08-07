from __future__ import annotations

from typing import Any

from almanak.integrations._base import PriceSourceScope


class GmxTickerPriceSourceFactory:
    name = "gmx"
    scope = PriceSourceScope.CHAIN
    order = 10

    def supports(self, chain: str | None) -> bool:
        # Qualified dispatch: match the provider's declared integration
        # identity (``ticker_price_integration()`` — connector-owned, typed)
        # AND the chain. Comparing against our own manifest name keeps
        # selection deterministic without a protocol-name literal
        # (chain/protocol coupling ratchet, blueprint 22).
        if not chain:
            return False
        from almanak.connectors._base.gateway_capabilities import GatewayVenueTickerPriceCapability
        from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

        return any(
            provider.ticker_price_integration() == self.name and chain in provider.ticker_price_chains()
            for provider in GATEWAY_REGISTRY.capability_providers(GatewayVenueTickerPriceCapability)  # type: ignore[type-abstract]
        )

    def build(self, *, chain: str | None, settings: Any) -> Any:
        if not chain:
            raise ValueError("GmxTickerPriceSourceFactory requires a chain")
        from .price_source import GmxTickerPriceSource

        return GmxTickerPriceSource(chain=chain)
