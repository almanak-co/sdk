"""Gateway-side connector binding for GMX V2.

Phase 3 (VIB-4811) introduces capability-keyed dispatch at the gateway
boundary. GMX V2 contributes:

* ``GatewayFundingRateCapability`` — venue identifier, per-market
  default funding rates, and the live on-chain fetch. Previously these
  lived as a venue branch in
  ``almanak.gateway.services.funding_rate_service``.

The live fetch is implemented here, on the connector's audited address
catalogue (``..addresses``): the gateway servicer contributes only the
venue-agnostic pieces (its shared web3 cache and default mark prices).
It originally delegated back to a servicer-side ``_fetch_gmx_v2_rate``
whose hand-copied reader/market dicts sat outside the
``tests/audit/test_gmx_v2_market_identity.py`` audit — and whose
``getMarketInfo`` ABI declared a 9-word ``MarketInfo`` no deployed
reader returns (both readers answer the 29-word struct), so every live
fetch failed decode and silently served the default rates. Moving the
fetch behind the connector boundary removes the copies; the ABI here is
pinned on-chain by ``tests/audit/test_gmx_v2_funding_reader_abi.py``.

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

import asyncio
import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
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

# ``Reader.getMarketInfo`` ABI. The output struct must be declared in FULL:
# eth-abi decodes strictly, so a partial ``MarketInfo`` (the pre-consolidation
# gateway copy declared 9 words; every deployed reader returns 29) raises
# ``BadFunctionCallOutput`` on each call — which the fetch's broad exception
# handler converts into a permanent, silent default-rate fallback. Layout is
# pinned against the live readers by
# ``tests/audit/test_gmx_v2_funding_reader_abi.py``.
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

# ``MarketInfo`` field positions consumed by the funding fetch.
_MARKET_INFO_NEXT_FUNDING = 4
_NEXT_FUNDING_LONGS_PAY_SHORTS = 0
_NEXT_FUNDING_FACTOR_PER_SECOND = 1

# GMX fixed-point precision for funding factors.
_GMX_FUNDING_PRECISION = Decimal(10) ** 30


def _signed_funding_factor_per_second(market_info: Sequence[Any]) -> Decimal:
    """Signed per-second funding rate from a decoded ``MarketInfo`` tuple.

    ``nextFunding.fundingFactorPerSecond`` is an unsigned magnitude;
    ``nextFunding.longsPayShorts`` carries the direction. The service-wide
    sign convention is positive = longs pay shorts.
    """
    next_funding = market_info[_MARKET_INFO_NEXT_FUNDING]
    magnitude = Decimal(int(next_funding[_NEXT_FUNDING_FACTOR_PER_SECOND])) / _GMX_FUNDING_PRECISION
    return magnitude if next_funding[_NEXT_FUNDING_LONGS_PAY_SHORTS] else -magnitude


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

    def default_funding_rate(self, market: str) -> Decimal:
        from almanak.core.perp_markets import perp_market_funding_key

        return _GMX_V2_DEFAULT_RATES.get(perp_market_funding_key(market) or market, _UNKNOWN_MARKET_DEFAULT)

    async def fetch_funding_rate(
        self,
        servicer: Any,
        market: str,
        chain: str,
    ) -> Any:
        """Fetch the live GMX V2 funding rate via ``Reader.getMarketInfo``.

        Reader / DataStore come from ``GMX_V2`` and the market token from
        the dynamic market registry — venue-catalogue discovery whose address
        tuple is verified on-chain via ``Reader.getMarket`` before use
        (``GmxV2MarketRegistry``); the servicer contributes its shared web3
        cache (``_get_web3``) and default mark prices. Any chain/market the
        registry cannot resolve, and any RPC or decode failure, falls back to
        the connector's default rate with ``is_live_data=False``.

        Live funding requires an unambiguous market — an exact market
        address, a full market name, or a pair label listing exactly one
        market. GMX funding factors are per market token, so an ambiguous
        pair label is never guessed at: it gets the default rate.
        """
        from almanak.core.perp_markets import perp_market_funding_key, perp_market_pair_key
        from almanak.gateway.services.funding_rate_service import FundingRateData

        # Canonicalize defensively (the RPC ingress already does): the
        # default-rate table is keyed by the dash form, the market
        # catalogue by the slash form.
        market = perp_market_funding_key(market) or market
        rate_hourly = self.default_funding_rate(market)
        open_interest_long = Decimal("125000000")
        open_interest_short = Decimal("118000000")
        mark_price = servicer._get_default_mark_price(market)
        is_live_data = False

        contracts = GMX_V2.get(chain, {})
        reader_address = contracts.get("reader")
        data_store_address = contracts.get("data_store")

        web3 = await servicer._get_web3(chain) if reader_address and data_store_address else None
        market_address: str | None = None
        if web3 is not None:
            try:
                from almanak.gateway.services.pt_rpc_adapter import build_gateway_eth_call

                record = await self._market_registry.resolve(
                    chain=chain,
                    market=perp_market_pair_key(market) or market,
                    eth_call=build_gateway_eth_call(chain=chain, network=servicer.settings.network),
                    # No index-equivalence: GMX funding factors are scoped to
                    # the individual market token, so two collateral variants
                    # of one pair label carry different rates. An ambiguous
                    # label raises inside this guarded block and falls through
                    # to the default rate — refuse to guess rather than serve
                    # an arbitrary variant's live rate under the pair's name.
                    # Exact addresses and full market names resolve precisely.
                )
                market_address = record.market_token if record is not None else None
                if market_address is None:
                    logger.debug("GMX V2 market %s is not in the venue catalogue for %s", market, chain)
            except Exception as e:
                # Fail open to the default rate: resolution errors (catalogue
                # outage, ambiguous/unknown market, verification failure) must
                # degrade exactly like an RPC failure, never propagate.
                logger.warning("Failed to resolve GMX V2 market %s on %s: %s", market, chain, e)
                market_address = None

        if web3 is not None and market_address is not None:
            try:
                reader = web3.eth.contract(
                    address=web3.to_checksum_address(reader_address),
                    abi=GMX_V2_READER_GET_MARKET_INFO_ABI,
                )

                # Approximate 30-decimal prices: getMarketInfo needs a price
                # triple to value the pool for its borrowing-factor fields,
                # but the funding factor this fetch consumes derives from USD
                # open interest, so coarse prices do not distort it.
                eth_price = 3000 * 10**30
                btc_price = 60000 * 10**30
                price = btc_price if "BTC" in market else eth_price
                market_prices = (
                    (price, price),  # indexTokenPrice (min, max)
                    (price, price),  # longTokenPrice
                    (1 * 10**30, 1 * 10**30),  # shortTokenPrice (USDC = $1)
                )

                market_info = await asyncio.wait_for(
                    reader.functions.getMarketInfo(
                        web3.to_checksum_address(data_store_address),
                        market_prices,
                        web3.to_checksum_address(market_address),
                    ).call(),
                    timeout=10.0,
                )

                funding_per_second = _signed_funding_factor_per_second(market_info)
                rate_hourly = funding_per_second * Decimal("3600")
                is_live_data = True
                logger.debug("Fetched GMX V2 rate for %s: %s/hour (live)", market, rate_hourly)

            except TimeoutError:
                logger.warning("Timeout fetching GMX V2 rate for %s", market)
            except Exception as e:
                logger.warning("Failed to fetch GMX V2 rate for %s: %s", market, e)

        # GMX V2 settles funding hourly.
        now = datetime.now(UTC)
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

        return FundingRateData(
            venue="gmx_v2",
            market=market,
            rate_hourly=rate_hourly,
            open_interest_long=open_interest_long,
            open_interest_short=open_interest_short,
            mark_price=mark_price,
            index_price=mark_price,
            next_funding_time=next_hour,
            is_live_data=is_live_data,
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


__all__ = ["GMX_V2_READER_GET_MARKET_INFO_ABI", "GmxV2GatewayConnector"]
