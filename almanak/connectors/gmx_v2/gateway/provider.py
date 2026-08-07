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

import aiohttp

from almanak.connectors._base.gateway_capabilities import (
    FundingHistorySource,
    GatewayAddressCapability,
    GatewayFundingHistoryCapability,
    GatewayFundingRateCapability,
    GatewayPerpMarketDiscoveryCapability,
    GatewayPerpPriceHistoryCapability,
    GatewayPriceIdCapability,
    GatewayVenueTickerPriceCapability,
    PerpMarketCatalogueUnavailable,
    PerpMarketRecord,
    PerpPriceCandle,
    PerpPriceCandlePage,
    VenueTickerPrice,
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
    GatewayPerpMarketDiscoveryCapability,
    GatewayPerpPriceHistoryCapability,
    GatewayVenueTickerPriceCapability,
):
    """Gateway-side connector for GMX V2 perp venue."""

    protocol: ClassVar[ProtocolName] = ProtocolName("gmx_v2")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def __init__(self) -> None:
        from .market_registry import GmxV2MarketRegistry
        from .ticker_prices import GmxV2TickerPriceReader

        self._market_registry = GmxV2MarketRegistry()
        self._ticker_price_reader = GmxV2TickerPriceReader()

    def perp_market_discovery_chains(self) -> frozenset[str]:
        """Chains backed by GMX's official metadata API and Reader contracts."""
        return frozenset(GMX_V2)

    async def resolve_perp_market(
        self,
        *,
        chain: str,
        market: str,
        eth_call: Any,
    ) -> PerpMarketRecord | None:
        """Resolve API metadata and require an exact on-chain identity match."""
        return await self._market_registry.resolve(chain=chain, market=market, eth_call=eth_call)

    # ---------------------------------------------------------------------
    # GatewayPerpPriceHistoryCapability (ALM-3149)
    # ---------------------------------------------------------------------

    def price_history_venue(self) -> str:
        return "gmx_v2"

    def price_history_chains(self) -> frozenset[str]:
        from .market_registry import GMX_API_BASE_URLS

        return frozenset(GMX_API_BASE_URLS)

    def price_history_timeframes(self) -> tuple[str, ...]:
        # Finest to coarsest.  The backtester probes actual coverage in this
        # order; the order is not a duration threshold table.
        from almanak.framework.data.timeframes import CANONICAL_OHLCV_TIMEFRAME_VALUES

        return CANONICAL_OHLCV_TIMEFRAME_VALUES

    async def fetch_price_candles(
        self,
        servicer: Any,
        *,
        market: str,
        chain: str,
        timeframe: str,
        before_ts: int,
        limit: int,
    ) -> PerpPriceCandlePage:
        """Resolve a listed GMX market, verify it on-chain, then read candles."""
        from almanak.gateway.services.pt_rpc_adapter import build_gateway_eth_call

        from .market_registry import GMX_API_BASE_URLS

        record = await self._market_registry.resolve(
            chain=chain,
            market=market,
            eth_call=build_gateway_eth_call(chain=chain, network=servicer.settings.network),
            # Historical backtests must not silently keep trading a market the
            # venue has disabled.  Exact-address delisted resolution remains
            # available to the compiler's risk-reducing close path.
            allow_delisted_address=False,
            # A short label may name multiple collateral-specific markets.
            # Candles are index-scoped, so the registry may accept that label
            # only after every matching market is verified on-chain and proven
            # to carry one identical index-price identity. No SDK market table
            # participates in discovery or selection.
            allow_index_equivalent=True,
        )
        if record is None:
            raise ValueError(f"listed GMX market {market!r} does not exist on {chain}")

        session = await servicer._get_http_session()
        url = f"{GMX_API_BASE_URLS[chain]}/prices/candles"
        try:
            async with session.get(
                url,
                params={
                    "tokenSymbol": record.index_symbol,
                    "period": timeframe,
                    "before": str(before_ts),
                    "limit": str(limit),
                },
                headers={"Accept": "application/json"},
            ) as response:
                response.raise_for_status()
                payload = await response.json()
        except (TimeoutError, aiohttp.ClientError) as exc:
            raise PerpMarketCatalogueUnavailable("GMX price-candle catalogue is temporarily unavailable") from exc

        if not isinstance(payload, dict) or payload.get("period") != timeframe:
            raise ValueError("GMX candle response did not preserve the requested timeframe")
        rows = payload.get("candles")
        if not isinstance(rows, list):
            raise ValueError("GMX candle response does not contain a candles list")

        candles: list[PerpPriceCandle] = []
        for row in rows:
            if not isinstance(row, list | tuple) or len(row) < 5:
                raise ValueError("GMX candle response contains a malformed row")
            try:
                candle = PerpPriceCandle(
                    timestamp=int(row[0]),
                    open=Decimal(str(row[1])),
                    high=Decimal(str(row[2])),
                    low=Decimal(str(row[3])),
                    close=Decimal(str(row[4])),
                )
            except (ArithmeticError, TypeError, ValueError) as exc:
                raise ValueError("GMX candle response contains a malformed numeric value") from exc
            values = (candle.open, candle.high, candle.low, candle.close)
            if candle.timestamp <= 0 or any(not value.is_finite() or value <= 0 for value in values):
                raise ValueError("GMX candle response contains an invalid observation")
            if candle.low > min(candle.open, candle.close) or candle.high < max(candle.open, candle.close):
                raise ValueError("GMX candle response violates OHLC bounds")
            candles.append(candle)

        candles.sort(key=lambda item: item.timestamp, reverse=True)
        return PerpPriceCandlePage(
            market=record.label,
            market_token=record.market_token,
            index_token=record.index_token,
            index_symbol=record.index_symbol,
            timeframe=timeframe,
            candles=tuple(candles),
        )

    # ---------------------------------------------------------------------
    # GatewayVenueTickerPriceCapability (ALM-3177)
    # ---------------------------------------------------------------------

    def ticker_price_venue(self) -> str:
        return "gmx_v2"

    def ticker_price_integration(self) -> str:
        """The ``almanak.integrations`` package fronting this feed (dispatch identity)."""
        return "gmx"

    def ticker_price_chains(self) -> frozenset[str]:
        return self._ticker_price_reader.chains()

    async def fetch_ticker_prices(self, *, chain: str) -> Mapping[str, VenueTickerPrice]:
        """Synthetic-index USD mids from the venue's signed oracle ticker feed.

        Serves the symbols no address-based source can price (synthetic index
        tokens have no deployed contract anywhere), so `PERP_OPEN` acceptable
        price derivation works for every market the venue lists — not just the
        curated ones with hand-declared CoinGecko slugs in
        :meth:`coingecko_ids`.
        """
        return await self._ticker_price_reader.fetch(chain=chain)

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
        """CoinGecko slugs for the GMX token AND every perp index symbol (VIB-6219).

        The index symbols are here, not in a per-chain token table, because GMX's
        markets are **synthetic**: ``LTC/USD`` trades on Arbitrum with no LTC token
        deployed there, so no chain table legitimately owns the slug. This connector
        defines the markets, so it declares how to price them.

        Why this is load-bearing rather than cosmetic: since VIB-6219 the compiler
        must read the index price to derive ``acceptablePrice`` instead of shipping an
        accept-anything sentinel, and it **fails closed** when the price is
        unavailable. A missing slug therefore does not degrade protection — it makes
        the market **uncompilable**. Auditing all 20 ``(chain, market)`` pairs found
        four index symbols priceable by neither CoinGecko nor Binance:

            LTC  (arbitrum + avalanche)   XRP, ATOM, NEAR  (arbitrum)

        which is five of twenty markets. ``DOGE`` had no CoinGecko slug either but
        does resolve via Binance ``DOGEUSDT``; it is declared here anyway so the
        primary source covers it rather than relying on the fallback.

        Registry contract: ``_build_registry_price_ids`` raises ``RuntimeError`` if
        two connectors give the same symbol different slugs, so these must agree with
        any other declaration of the same asset.
        """
        return {
            "GMX": "gmx",
            "HYPE": "hyperliquid",
            # Perp index symbols — see docstring. Ordered as in GMX_V2_MARKETS.
            "AAVE": "aave",
            "ARB": "arbitrum",
            "ATOM": "cosmos",
            "AVAX": "avalanche-2",
            "BTC": "bitcoin",
            "DOGE": "dogecoin",
            "ETH": "ethereum",
            "LINK": "chainlink",
            "LTC": "litecoin",
            "NEAR": "near",
            "OP": "optimism",
            "SOL": "solana",
            "UNI": "uniswap",
            "XRP": "ripple",
        }

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

    def funding_history_source(self, chain: str) -> FundingHistorySource:
        """Identify the same upstream budget Hyperliquid history consumes."""
        return FundingHistorySource(
            key="hyperliquid_info",
            scope="",
            requests_per_minute=30,
            burst_size=6,
        )

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
