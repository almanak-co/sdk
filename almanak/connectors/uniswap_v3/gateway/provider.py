"""Gateway-side connector binding for Uniswap V3.

Phase 3 (VIB-4811) introduces capability-keyed dispatch at the gateway
boundary. Uniswap V3 contributes:

* ``GatewayPoolHistoryCapability`` — pool history is supported on
  Ethereum, Arbitrum, Base, Optimism, and Polygon (the chains with a
  registered Uniswap V3 subgraph). Previously this set lived in
  ``almanak.gateway.services.pool_history_service.SUPPORTED_POOL_PAIRS``.
* ``GatewayDefillamaSlugCapability`` — DefiLlama project slug
  (``"uniswap-v3"``).
* ``GatewaySubgraphCapability`` — TheGraph subgraph URLs for the chains
  where Uniswap V3 pool history is available. Moved verbatim from
  ``almanak.gateway.integrations.thegraph.DEFAULT_ALLOWED_SUBGRAPHS``.
* ``GatewayPriceIdCapability`` — Uniswap governance token CoinGecko
  slug (``UNI`` → ``uniswap``). Moved verbatim from
  ``almanak.gateway.data.price.coingecko``'s per-chain token-id tables.
* ``GatewayDexQuoteCapability`` — DEX quote function for the multi-DEX
  price service. The simulation logic stays on
  ``MultiDexPriceService`` (where it shares state with siblings);
  this connector only delegates dispatch.
"""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayDefillamaSlugCapability,
    GatewayDexQuoteCapability,
    GatewayPoolHistoryCapability,
    GatewayPriceIdCapability,
    GatewaySubgraphCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

# Subgraph URLs for Uniswap V3. Keyed by the public alias the strategy
# caller passes (``"uniswap-v3-<chain>"``). Moved verbatim from
# ``thegraph.DEFAULT_ALLOWED_SUBGRAPHS``.
_UNISWAP_V3_SUBGRAPHS: dict[str, str] = {
    "uniswap-v3-ethereum": "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
    "uniswap-v3-arbitrum": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-arbitrum-one",
    "uniswap-v3-optimism": "https://api.thegraph.com/subgraphs/name/ianlapham/optimism-post-regenesis",
    "uniswap-v3-polygon": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-polygon",
    "uniswap-v3-base": "https://api.studio.thegraph.com/query/48211/uniswap-v3-base/version/latest",
}


class UniswapV3GatewayConnector(
    GatewayConnector,
    GatewayPoolHistoryCapability,
    GatewayDefillamaSlugCapability,
    GatewaySubgraphCapability,
    GatewayPriceIdCapability,
    GatewayDexQuoteCapability,
):
    """Gateway-side connector for Uniswap V3."""

    protocol: ClassVar[ProtocolName] = ProtocolName("uniswap_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def pool_history_supported_chains(self) -> frozenset[str]:
        """Chains where Uniswap V3 pool history is available.

        Matches the historical
        ``SUPPORTED_POOL_PAIRS`` Uniswap V3 entries in
        ``pool_history_service.py`` (Ethereum, Arbitrum, Base, Optimism,
        Polygon). The set is closed: a new chain requires a new
        subgraph URL contribution AND adding it here.
        """
        return frozenset(
            {
                "ethereum",
                "arbitrum",
                "base",
                "optimism",
                "polygon",
            }
        )

    def defillama_slug(self) -> str | None:
        """DefiLlama project slug for Uniswap V3."""
        return "uniswap-v3"

    def defillama_slug_aliases(self) -> dict[str, str]:
        """No alias variants ride this connector."""
        return {}

    def subgraph_endpoints(self) -> dict[str, str]:
        """TheGraph subgraph URLs for Uniswap V3 (one per supported chain)."""
        return dict(_UNISWAP_V3_SUBGRAPHS)

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the Uniswap governance token."""
        return {"UNI": "uniswap"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """UNI is an EVM-only token resolved via ``TokenResolver``."""
        return {}

    def dex_name(self) -> str:
        """DEX identifier — matches the legacy ``Dex.UNISWAP_V3`` string."""
        return "uniswap_v3"

    def supported_chains(self) -> frozenset[str]:
        """Chains where Uniswap V3 quotes are available via the multi-DEX service.

        Matches the historical ``DEX_CHAINS`` entries that listed
        ``"uniswap_v3"`` (Ethereum, Arbitrum, Optimism, Polygon, Base).
        """
        return frozenset(
            {
                "ethereum",
                "arbitrum",
                "optimism",
                "polygon",
                "base",
            }
        )

    async def quote(
        self,
        service: Any,
        token_in: str,
        token_out: str,
        amount_in: Any,
    ) -> Any:
        """Delegate to ``MultiDexPriceService._get_uniswap_v3_quote``.

        The simulation helpers (default-price lookup, price-impact +
        slippage curves, mock-quote hooks) stay on the service so they
        keep their shared state. This capability layer only owns
        dispatch.
        """
        return await service._get_uniswap_v3_quote(token_in, token_out, amount_in)


__all__ = ["UniswapV3GatewayConnector"]
