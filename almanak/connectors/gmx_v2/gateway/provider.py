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

W1 (VIB-4853) adds:

* ``GatewayAddressCapability`` — per-chain ExchangeRouter / Router /
  DataStore / OrderVault / Reader + per-pair market addresses, moved
  verbatim from ``almanak.core.contracts``. Non-connector callers
  (teardown discovery, ContractRegistry, CLI support matrix) resolve
  GMX addresses through this capability instead of importing the dict
  by name.

W7 (VIB-4859) adds:

* ``GatewayFundingHistoryCapability`` — GMX V2 has no native historical
  funding-rate endpoint. The pre-W7 framework code in
  ``framework/data/rates/history.py`` routed ``venue="gmx_v2"`` requests
  through the Hyperliquid fallback (both venues quote the same
  ETH-USD / BTC-USD markets, with Hyperliquid serving the public
  reference rate). The capability is declared so the registry dispatcher
  routes GMX history requests through this connector; the body delegates
  to the Hyperliquid connector via ``GATEWAY_REGISTRY`` so the
  cross-venue fallback survives the migration. Tracked separately under
  VIB-4870 if a native GMX historical endpoint ever ships.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayFundingHistoryCapability,
    GatewayFundingRateCapability,
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import GMX_V2

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

# W7: markets the cross-venue Hyperliquid fallback can serve for GMX.
# Equal to the intersection of the pre-W7 funding fallback chain
# (history.py:L878–L894) and Hyperliquid's coverage.
_GMX_HISTORICAL_MARKETS = frozenset({"ETH-USD", "BTC-USD", "ARB-USD", "LINK-USD", "SOL-USD"})


class GmxV2GatewayConnector(
    GatewayConnector,
    GatewayAddressCapability,
    GatewayFundingRateCapability,
    GatewayFundingHistoryCapability,
    GatewayPriceIdCapability,
):
    """Gateway-side connector for GMX V2 perp venue."""

    protocol: ClassVar[ProtocolName] = ProtocolName("gmx_v2")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the GMX V2 contract addresses for ``chain`` (or empty)."""
        return GMX_V2.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which GMX V2 addresses are registered."""
        return frozenset(GMX_V2.keys())

    def venue(self) -> str:
        return "gmx_v2"

    def default_funding_rate(self, market: str) -> Decimal:
        from almanak.core.perp_markets import perp_market_funding_key

        return _GMX_V2_DEFAULT_RATES.get(perp_market_funding_key(market) or market, _UNKNOWN_MARKET_DEFAULT)

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

    # ---------------------------------------------------------------------
    # GatewayFundingHistoryCapability (VIB-4859 / W7)
    # ---------------------------------------------------------------------

    def funding_venue(self) -> str:
        """Venue identifier matching :meth:`venue` for the live capability."""
        return "gmx_v2"

    def funding_supported_markets(self) -> frozenset[str]:
        """Markets the Hyperliquid cross-venue fallback can serve for GMX."""
        return _GMX_HISTORICAL_MARKETS

    async def fetch_funding_history(
        self,
        servicer: Any,
        *,
        market: str,
        chain: str,
        start_ts: int,
        end_ts: int,
    ) -> Any:
        """Cross-venue funding-history fallback through Hyperliquid.

        GMX V2 has no native historical funding endpoint. The pre-W7
        ``framework/data/rates/history.py:_fetch_funding_with_fallback``
        routed both ``venue="hyperliquid"`` and ``venue="gmx_v2"`` to the
        Hyperliquid Info API because the two venues quote the same
        reference markets (ETH-USD, BTC-USD, etc.). This capability
        preserves that behaviour by delegating to the Hyperliquid
        connector via ``GATEWAY_REGISTRY``.
        """
        from almanak.connectors._base.gateway_capabilities import (
            GatewayFundingHistoryCapability,
        )
        from almanak.connectors._gateway_registry import GATEWAY_REGISTRY
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        hyperliquid_provider: GatewayFundingHistoryCapability | None = None
        for provider in GATEWAY_REGISTRY.capability_providers(GatewayFundingHistoryCapability):  # type: ignore[type-abstract]
            if provider.funding_venue().lower() == "hyperliquid":
                hyperliquid_provider = provider
                break

        if hyperliquid_provider is None:
            raise RateHistoryUnavailable(
                "gmx_v2",
                "GMX V2 historical funding requires the Hyperliquid connector to be registered (cross-venue fallback)",
            )

        return await hyperliquid_provider.fetch_funding_history(
            servicer,
            market=market,
            chain=chain,
            start_ts=start_ts,
            end_ts=end_ts,
        )


__all__ = ["GmxV2GatewayConnector"]
