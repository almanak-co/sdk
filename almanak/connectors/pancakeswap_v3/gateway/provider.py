"""Gateway-side connector binding for PancakeSwap V3 (VIB-4811 / VIB-4817).

PancakeSwap V3 has strategy-side intent code under
``almanak/connectors/pancakeswap_v3/`` (pre-migration layout)
and a gateway-side connector class that publishes its DefiLlama slug,
CoinGecko slug, and DexScreener identifiers.

Contributes:

* ``GatewayPriceIdCapability`` — ``CAKE`` (governance token, BSC).
  Moved verbatim from ``BSC_TOKEN_IDS`` in
  ``almanak.gateway.data.price.coingecko``.
* ``GatewayDefillamaSlugCapability`` — DefiLlama project slug
  (``"pancakeswap-amm-v3"``). VIB-4817 moves this off the
  ``_PROTOCOL_TO_LLAMA_TODO_FALLBACK`` dict in
  ``almanak.gateway.services.pool_analytics_service`` onto the connector.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayDefillamaSlugCapability,
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class PancakeSwapV3GatewayConnector(
    GatewayConnector,
    GatewayPriceIdCapability,
    GatewayDefillamaSlugCapability,
):
    """Gateway-side connector for PancakeSwap V3."""

    protocol: ClassVar[ProtocolName] = ProtocolName("pancakeswap_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the PancakeSwap governance token."""
        return {"CAKE": "pancakeswap-token"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """CAKE is an EVM-only token resolved via ``TokenResolver``."""
        return {}

    def defillama_slug(self) -> str | None:
        """DefiLlama project slug for PancakeSwap V3."""
        return "pancakeswap-amm-v3"

    def defillama_slug_aliases(self) -> dict[str, str]:
        return {}


__all__ = ["PancakeSwapV3GatewayConnector"]
