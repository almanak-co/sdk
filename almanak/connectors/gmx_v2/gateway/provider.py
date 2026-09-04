"""Gateway-side connector binding for GMX V2.

Phase 3 (VIB-4811) introduces capability-keyed dispatch at the gateway
boundary. GMX V2 contributes:

* ``GatewayFundingRateCapability`` — venue identifier and the latest completed
  funding observation from GMX's own Synthetics indexer.

The current-rate fetch uses the same official indexer as the historical lane
and truthfully reports the completed observation as historical rather than
pretending it is realtime. The gateway servicer contributes its shared HTTP
session and premium RPC-backed market verification; the connector owns GMX
identity and rate semantics.
The exported ``getMarketInfo`` ABI remains available for direct on-chain calls;
``tests/audit/test_gmx_v2_funding_reader_abi.py`` checks selected field
positions against live readers.

W1 (VIB-4853) adds:

* ``GatewayAddressCapability`` — per-chain ExchangeRouter / Router /
  DataStore / OrderVault / Reader + per-pair market addresses, moved
  verbatim from ``almanak.core.contracts``. Non-connector callers
  (teardown discovery, ContractRegistry, CLI support matrix) resolve
  GMX addresses through this capability instead of importing the dict
  by name.

Historical funding is address-first and GMX-native. The connector verifies the
exact market token through :class:`GmxV2MarketRegistry`, then reads GMX's
official chain-specific Synthetics indexer. No static market allowlist,
invented default, or Hyperliquid proxy participates in this lane.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import UTC, datetime
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

logger = logging.getLogger(__name__)

# Public module ABI for direct ``Reader.getMarketInfo`` calls, with the complete
# currently documented output. The live-reader audit verifies selected fields
# still decode at their expected positions on each supported deployment.
_GMX_COLLATERAL_TYPE = [
    {"name": "longToken", "type": "uint256"},
    {"name": "shortToken", "type": "uint256"},
]
_GMX_POSITION_TYPE = [
    {"name": "long", "type": "tuple", "components": _GMX_COLLATERAL_TYPE},
    {"name": "short", "type": "tuple", "components": _GMX_COLLATERAL_TYPE},
]
_GMX_PRICE_PROPS = [
    {"name": "min", "type": "uint256"},
    {"name": "max", "type": "uint256"},
]
GMX_V2_READER_GET_MARKET_INFO_ABI = [
    {
        "inputs": [
            {"name": "dataStore", "type": "address"},
            {
                "name": "prices",
                "type": "tuple",
                "components": [
                    {"name": "indexTokenPrice", "type": "tuple", "components": _GMX_PRICE_PROPS},
                    {"name": "longTokenPrice", "type": "tuple", "components": _GMX_PRICE_PROPS},
                    {"name": "shortTokenPrice", "type": "tuple", "components": _GMX_PRICE_PROPS},
                ],
            },
            {"name": "marketKey", "type": "address"},
        ],
        "name": "getMarketInfo",
        "outputs": [
            {
                "name": "",
                "type": "tuple",
                "components": [
                    {
                        "name": "market",
                        "type": "tuple",
                        "components": [
                            {"name": "marketToken", "type": "address"},
                            {"name": "indexToken", "type": "address"},
                            {"name": "longToken", "type": "address"},
                            {"name": "shortToken", "type": "address"},
                        ],
                    },
                    {"name": "borrowingFactorPerSecondForLongs", "type": "uint256"},
                    {"name": "borrowingFactorPerSecondForShorts", "type": "uint256"},
                    {
                        "name": "baseFunding",
                        "type": "tuple",
                        "components": [
                            {"name": "fundingFeeAmountPerSize", "type": "tuple", "components": _GMX_POSITION_TYPE},
                            {
                                "name": "claimableFundingAmountPerSize",
                                "type": "tuple",
                                "components": _GMX_POSITION_TYPE,
                            },
                        ],
                    },
                    {
                        "name": "nextFunding",
                        "type": "tuple",
                        "components": [
                            {"name": "longsPayShorts", "type": "bool"},
                            {"name": "fundingFactorPerSecond", "type": "uint256"},
                            {"name": "nextSavedFundingFactorPerSecond", "type": "int256"},
                            {
                                "name": "fundingFeeAmountPerSizeDelta",
                                "type": "tuple",
                                "components": _GMX_POSITION_TYPE,
                            },
                            {
                                "name": "claimableFundingAmountPerSizeDelta",
                                "type": "tuple",
                                "components": _GMX_POSITION_TYPE,
                            },
                        ],
                    },
                    {
                        "name": "virtualInventory",
                        "type": "tuple",
                        "components": [
                            {"name": "virtualPoolAmountForLongToken", "type": "uint256"},
                            {"name": "virtualPoolAmountForShortToken", "type": "uint256"},
                            {"name": "virtualInventoryForPositions", "type": "int256"},
                        ],
                    },
                    {"name": "isDisabled", "type": "bool"},
                ],
            },
        ],
        "stateMutability": "view",
        "type": "function",
    },
]


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
        require_listed: bool = False,
    ) -> PerpMarketRecord | None:
        """Resolve API metadata and require an exact on-chain identity match.

        ``require_listed=True`` (risk-increasing callers) excludes delisted
        rows AND the registry's serve-stale grace — identity is immutable but
        listing status is not, and an increase on a disabled market burns its
        keeper fee.
        """
        return await self._market_registry.resolve(
            chain=chain,
            market=market,
            eth_call=eth_call,
            allow_delisted_address=not require_listed,
        )

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

    async def _resolve_funding_market(
        self,
        servicer: Any,
        *,
        market: str,
        market_address: str,
        chain: str,
    ) -> PerpMarketRecord:
        """Resolve and verify one exact GMX market identity for funding."""
        from almanak.core.perp_markets import perp_market_base
        from almanak.gateway.services.pt_rpc_adapter import build_gateway_eth_call
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        query = market_address.strip() or market
        try:
            record = await self._market_registry.resolve(
                chain=chain,
                market=query,
                eth_call=build_gateway_eth_call(chain=chain, network=servicer.settings.network),
                allow_delisted_address=False,
            )
        except Exception as exc:
            raise RateHistoryUnavailable(
                "gmx_market_registry",
                f"GMX market identity could not be verified for {market!r} on {chain}: {exc}",
            ) from exc
        if record is None:
            raise RateHistoryUnavailable(
                "gmx_market_registry",
                f"GMX market {query!r} does not exist or is not listed on {chain}",
            )
        requested_base = perp_market_base(market)
        if requested_base is not None and requested_base.upper() != record.index_symbol.upper():
            raise RateHistoryUnavailable(
                "gmx_market_registry",
                f"GMX market address {record.market_token} is {record.index_symbol}, not requested {market}",
            )
        return record

    async def fetch_funding_rate(
        self,
        servicer: Any,
        market: str,
        chain: str,
        market_address: str = "",
    ) -> Any:
        """Fetch the latest completed GMX-native hourly funding snapshot.

        The indexer observation is not a realtime quote. Its actual timestamp
        is returned and ``is_live_data`` remains false.
        """
        from almanak.core.perp_markets import perp_market_funding_key
        from almanak.gateway.services.funding_rate_service import FundingRateData

        from .funding_history import fetch_latest_gmx_funding_snapshot

        market = perp_market_funding_key(market) or market
        record = await self._resolve_funding_market(
            servicer,
            market=market,
            market_address=market_address,
            chain=chain,
        )
        completed_hour = int(datetime.now(UTC).timestamp() // 3600) * 3600 - 3600
        point = await fetch_latest_gmx_funding_snapshot(
            await servicer._get_http_session(),
            chain=chain,
            market_address=record.market_token,
            end_ts=completed_hour,
        )
        assert point.rate_hourly is not None
        observed_at = datetime.fromtimestamp(point.timestamp, tz=UTC)

        return FundingRateData(
            venue="gmx_v2",
            market=market,
            rate_hourly=point.rate_hourly,
            long_rate_hourly=point.long_rate_hourly,
            short_rate_hourly=point.short_rate_hourly,
            market_address=record.market_token,
            open_interest_long=None,
            open_interest_short=None,
            mark_price=None,
            index_price=None,
            next_funding_time=None,
            is_live_data=False,
            observed_at=observed_at,
        )

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
            # Perp index symbols — see docstring. Alphabetical.
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

    def funding_supported_markets(self) -> frozenset[str] | None:
        """GMX markets are discovered and verified dynamically by address."""
        return None

    def funding_history_source(self, chain: str) -> FundingHistorySource:
        """Chain-scoped budget for GMX's official Synthetics indexer."""
        return FundingHistorySource(
            key="gmx_synthetics_subsquid",
            scope=chain,
            requests_per_minute=60,
            burst_size=10,
        )

    async def fetch_funding_history(
        self,
        servicer: Any,
        *,
        market: str,
        market_address: str,
        chain: str,
        start_ts: int,
        end_ts: int,
    ) -> Any:
        """Verified exact-market funding history from GMX's own indexer."""
        from .funding_history import fetch_gmx_funding_history

        record = await self._resolve_funding_market(
            servicer,
            market=market,
            market_address=market_address,
            chain=chain,
        )
        return await fetch_gmx_funding_history(
            await servicer._get_http_session(),
            chain=chain,
            market_address=record.market_token,
            start_ts=start_ts,
            end_ts=end_ts,
        )


__all__ = ["GMX_V2_READER_GET_MARKET_INFO_ABI", "GmxV2GatewayConnector"]
