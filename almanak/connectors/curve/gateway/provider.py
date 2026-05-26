"""Gateway-side connector binding for Curve (VIB-4811 / VIB-4817).

Phase 3 scaffolding — Curve does not yet have a full strategy-side
connector under ``almanak/connectors/curve/``. This scaffold publishes
the protocol's DEX-quote function and TheGraph subgraph endpoints.

Contributes:

* ``GatewayDexQuoteCapability`` — DEX quote function for the
  multi-DEX price service (Ethereum, Arbitrum). The simulation logic
  stays on ``MultiDexPriceService`` (where it shares state with
  siblings); this connector only delegates dispatch.
* ``GatewaySubgraphCapability`` (VIB-4817) — TheGraph subgraph URLs
  for Curve, moved verbatim out of the ``_PENDING_SUBGRAPHS`` dict in
  ``almanak.gateway.integrations.thegraph``.
"""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayDexQuoteCapability,
    GatewaySubgraphCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

# Curve subgraph URLs — moved verbatim from the ``_PENDING_SUBGRAPHS``
# entries previously held in ``thegraph.py``. Endpoints are sourced from
# the Convex community's curve-volume subgraphs, which historically
# back the analytics surface in the gateway.
_CURVE_SUBGRAPHS: dict[str, str] = {
    "curve-ethereum": "https://api.thegraph.com/subgraphs/name/convex-community/volume-mainnet",
    "curve-arbitrum": "https://api.thegraph.com/subgraphs/name/convex-community/volume-arbitrum",
}


class CurveGatewayConnector(
    GatewayConnector,
    GatewayDexQuoteCapability,
    GatewaySubgraphCapability,
):
    """Gateway-side connector for Curve."""

    protocol: ClassVar[ProtocolName] = ProtocolName("curve")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP

    def dex_name(self) -> str:
        """DEX identifier — matches the legacy ``Dex.CURVE`` string."""
        return "curve"

    def supported_chains(self) -> frozenset[str]:
        """Chains where Curve quotes are available via the multi-DEX service.

        Matches the historical ``DEX_CHAINS`` entries that listed
        ``"curve"`` (Ethereum, Arbitrum).
        """
        return frozenset({"ethereum", "arbitrum"})

    async def quote(
        self,
        service: Any,
        token_in: str,
        token_out: str,
        amount_in: Any,
    ) -> Any:
        """Delegate to ``MultiDexPriceService._get_curve_quote``."""
        return await service._get_curve_quote(token_in, token_out, amount_in)

    def subgraph_endpoints(self) -> dict[str, str]:
        """TheGraph subgraph URLs for Curve (Ethereum, Arbitrum)."""
        return dict(_CURVE_SUBGRAPHS)


__all__ = ["CurveGatewayConnector"]
