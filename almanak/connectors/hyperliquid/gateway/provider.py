"""Gateway-side connector binding for Hyperliquid.

Phase 3 (VIB-4811) introduces capability-keyed dispatch at the gateway
boundary. Hyperliquid contributes:

* ``GatewayFundingRateCapability`` — venue identifier, per-market
  default funding rates, and the live REST fetch. Previously these
  lived as a venue branch in
  ``almanak.gateway.services.funding_rate_service``.

The live fetch delegates to the gateway servicer's existing
``_fetch_hyperliquid_rate(market)`` method so the venue-specific REST
client + Pydantic parser plumbing stays alongside the
``HyperliquidAssetContext`` / ``HyperliquidUniverseItem`` models, and
the existing unit tests for that method continue to pass.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayFundingRateCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

# Default per-market hourly funding rates — fallback when the REST
# fetch fails / times out. Moved verbatim from
# ``funding_rate_service.DEFAULT_RATES["hyperliquid"]``.
_HYPERLIQUID_DEFAULT_RATES: dict[str, Decimal] = {
    "ETH-USD": Decimal("0.000015"),
    "BTC-USD": Decimal("0.000011"),
    "ARB-USD": Decimal("0.000018"),
    "LINK-USD": Decimal("0.000009"),
    "SOL-USD": Decimal("0.000022"),
}

# Historical fallback for unknown markets (matches the previous
# ``_get_default_rate`` second arg to ``.get``).
_UNKNOWN_MARKET_DEFAULT = Decimal("0.00001")


class HyperliquidGatewayConnector(GatewayConnector, GatewayFundingRateCapability):
    """Gateway-side connector for Hyperliquid perp venue."""

    protocol: ClassVar[ProtocolName] = ProtocolName("hyperliquid")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def venue(self) -> str:
        return "hyperliquid"

    def default_funding_rate(self, market: str) -> Decimal:
        return _HYPERLIQUID_DEFAULT_RATES.get(market, _UNKNOWN_MARKET_DEFAULT)

    async def fetch_funding_rate(
        self,
        servicer: Any,
        market: str,
        chain: str,
    ) -> Any:
        """Delegate to the servicer's existing REST fetch helper.

        ``chain`` is unused for Hyperliquid (the API is chain-agnostic)
        but the capability contract takes it for parity with on-chain
        venues like GMX V2.
        """
        return await servicer._fetch_hyperliquid_rate(market)


__all__ = ["HyperliquidGatewayConnector"]
