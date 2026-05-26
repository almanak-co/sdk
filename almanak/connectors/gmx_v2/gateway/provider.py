"""Gateway-side connector binding for GMX V2.

Phase 3 (VIB-4811) introduces capability-keyed dispatch at the gateway
boundary. GMX V2 contributes:

* ``GatewayFundingRateCapability`` — venue identifier, per-market
  default funding rates, and the live on-chain fetch. Previously these
  lived as a venue branch in
  ``almanak.gateway.services.funding_rate_service``.

The live fetch delegates to the gateway servicer's existing
``_fetch_gmx_v2_rate(market, chain)`` method so the venue-specific
web3 + ABI plumbing stays in one place (alongside the GMX V2 ABI
constants and reader addresses) and the existing unit tests for that
method (``tests/unit/gateway/test_funding_rate_service.py``) continue
to pass.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayFundingRateCapability,
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

# Default per-market hourly funding rates — fallback when the on-chain
# fetch fails / times out. Moved verbatim from
# ``funding_rate_service.DEFAULT_RATES["gmx_v2"]``.
_GMX_V2_DEFAULT_RATES: dict[str, Decimal] = {
    "ETH-USD": Decimal("0.000012"),
    "BTC-USD": Decimal("0.000010"),
    "ARB-USD": Decimal("0.000015"),
    "LINK-USD": Decimal("0.000008"),
    "SOL-USD": Decimal("0.000018"),
}

# Historical fallback for unknown markets (matches the previous
# ``_get_default_rate`` second arg to ``.get``).
_UNKNOWN_MARKET_DEFAULT = Decimal("0.00001")


class GmxV2GatewayConnector(
    GatewayConnector,
    GatewayFundingRateCapability,
    GatewayPriceIdCapability,
):
    """Gateway-side connector for GMX V2 perp venue."""

    protocol: ClassVar[ProtocolName] = ProtocolName("gmx_v2")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def venue(self) -> str:
        return "gmx_v2"

    def default_funding_rate(self, market: str) -> Decimal:
        return _GMX_V2_DEFAULT_RATES.get(market, _UNKNOWN_MARKET_DEFAULT)

    async def fetch_funding_rate(
        self,
        servicer: Any,
        market: str,
        chain: str,
    ) -> Any:
        """Delegate to the servicer's existing on-chain fetch helper.

        The venue-specific web3 ABI + reader address plumbing stays on
        the servicer where it shares the gateway's web3 cache and SSL
        context. The capability layer only owns dispatch.
        """
        return await servicer._fetch_gmx_v2_rate(market, chain)

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the GMX governance token (Arbitrum)."""
        return {"GMX": "gmx"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """GMX is an EVM-only token resolved via ``TokenResolver``."""
        return {}


__all__ = ["GmxV2GatewayConnector"]
