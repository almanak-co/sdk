from __future__ import annotations

from typing import Any

from almanak.integrations._base import PriceSourceScope


class HypercorePriceSourceFactory:
    name = "hypercore"
    scope = PriceSourceScope.CHAIN
    order = 10

    def supports(self, chain: str | None) -> bool:
        if not chain:
            return False
        from almanak.connectors._base.gateway_capabilities import GatewayOraclePriceCapability
        from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

        return any(
            provider.oracle_price_chain() == chain
            for provider in GATEWAY_REGISTRY.capability_providers(GatewayOraclePriceCapability)  # type: ignore[type-abstract]
        )

    def build(self, *, chain: str | None, settings: Any) -> Any:
        from .price_source import HypercoreOraclePriceSource

        return HypercoreOraclePriceSource(network=settings.network)
