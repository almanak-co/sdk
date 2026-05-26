"""Gateway-side connector binding for Aave v3.

Declares the ``GatewayMarketLookupCapability`` so the gateway boot loop
can discover the Aave v3 receipt-token (aToken / vToken) lookup without
hand-wiring an import in :mod:`almanak.gateway.services.token_service`.

Phase 1+2 (VIB-4810) — the capability is declared but ``token_service``
continues to call ``get_aave_lookup`` directly. Phase 4 collapses the
explicit per-protocol accessor methods on ``TokenService`` into a loop
over ``GATEWAY_REGISTRY.capability_providers(GatewayMarketLookupCapability)``.

Phase 3 (VIB-4811) adds:

* ``GatewayDefillamaSlugCapability`` — DefiLlama project slug
  (``"aave-v3"``).
* ``GatewaySubgraphCapability`` — TheGraph subgraph URLs (Ethereum,
  Arbitrum, Optimism, Polygon). Moved verbatim from
  ``almanak.gateway.integrations.thegraph.DEFAULT_ALLOWED_SUBGRAPHS``.
* ``GatewayPriceIdCapability`` — Aave governance token CoinGecko slug
  (``AAVE`` → ``aave``). Moved verbatim from
  ``almanak.gateway.data.price.coingecko``'s per-chain token-id tables.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayDefillamaSlugCapability,
    GatewayMarketLookupCapability,
    GatewayPriceIdCapability,
    GatewaySubgraphCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from .market_lookup import get_aave_lookup

# Aave v3 subgraph URLs. Moved verbatim from
# ``thegraph.DEFAULT_ALLOWED_SUBGRAPHS``.
_AAVE_V3_SUBGRAPHS: dict[str, str] = {
    "aave-v3-ethereum": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3",
    "aave-v3-arbitrum": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-arbitrum",
    "aave-v3-optimism": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-optimism",
    "aave-v3-polygon": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-polygon",
}


class AaveV3GatewayConnector(
    GatewayConnector,
    GatewayMarketLookupCapability,
    GatewayDefillamaSlugCapability,
    GatewaySubgraphCapability,
    GatewayPriceIdCapability,
):
    """Gateway-side connector for Aave v3."""

    protocol: ClassVar[ProtocolName] = ProtocolName("aave_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def market_lookup(self):
        """Return the awaitable Aave market-lookup singleton factory.

        The underlying ``get_aave_lookup`` is a coroutine factory that
        returns a lazily-loaded singleton with disk-cache + retry
        plumbing (see ``ProtocolTokenLookup``). Phase 4 will swap this
        for an ``async`` capability contract; for Phase 1+2 the provider
        method just returns the callable so the capability registration
        is visible without coupling to the lookup's async lifecycle.
        """
        return get_aave_lookup

    def defillama_slug(self) -> str | None:
        """DefiLlama project slug for Aave v3."""
        return "aave-v3"

    def defillama_slug_aliases(self) -> dict[str, str]:
        return {}

    def subgraph_endpoints(self) -> dict[str, str]:
        """TheGraph subgraph URLs for Aave v3 (one per supported chain)."""
        return dict(_AAVE_V3_SUBGRAPHS)

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the Aave governance token."""
        return {"AAVE": "aave"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """Aave token addresses are resolved via ``TokenResolver`` on EVM chains."""
        return {}


__all__ = ["AaveV3GatewayConnector"]
