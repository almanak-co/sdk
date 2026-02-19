"""Gateway-backed CoinGecko Historical Data Provider for PnL backtesting.

This module provides a gateway-backed implementation of the HistoricalDataProvider
protocol using the CoinGecko API through the gateway sidecar.

Key Features:
    - All API calls go through the gateway (no direct external access)
    - Implements rate limiting via gateway
    - Caches fetched data to minimize API calls
    - Supports the iterate() method for backtesting engine integration

Example:
    from almanak.framework.backtesting.pnl.providers.gateway_coingecko import (
        GatewayCoinGeckoDataProvider,
    )
    from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig
    from almanak.framework.gateway_client import GatewayClient
    from datetime import datetime

    with GatewayClient() as gateway:
        provider = GatewayCoinGeckoDataProvider(gateway_client=gateway)
        config = HistoricalDataConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            interval_seconds=3600,
            tokens=["WETH", "USDC", "ARB"],
        )

        async for timestamp, market_state in provider.iterate(config):
            eth_price = market_state.get_price("WETH")
            # ... process market state
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, ClassVar

from ..data_provider import OHLCV, HistoricalDataConfig, MarketState

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


# Token ID mappings for common tokens
# CoinGecko uses specific IDs for each token
TOKEN_IDS: dict[str, str] = {
    "ETH": "ethereum",
    "WETH": "weth",
    "USDC": "usd-coin",
    "USDC.E": "usd-coin",
    "ARB": "arbitrum",
    "WBTC": "wrapped-bitcoin",
    "USDT": "tether",
    "DAI": "dai",
    "LINK": "chainlink",
    "UNI": "uniswap",
    "GMX": "gmx",
    "PENDLE": "pendle",
    "RDNT": "radiant-capital",
    "SOL": "solana",
    "JOE": "trader-joe",
    "LDO": "lido-dao",
    "BTC": "bitcoin",
    "STETH": "lido-dao-wrapped-staked-eth",
    "CBETH": "coinbase-wrapped-staked-eth",
    "OP": "optimism",
    "AVAX": "avalanche-2",
    "BNB": "binancecoin",
    "MATIC": "matic-network",
    "AAVE": "aave",
    "CRV": "curve-dao-token",
}


@dataclass
class OHLCVCache:
    """Cache for OHLCV data to minimize API calls."""

    data: dict[str, list[OHLCV]]  # token -> list of OHLCV
    fetched_at: datetime

    def get_price_at(self, token: str, timestamp: datetime) -> Decimal | None:
        """Get interpolated price at a specific timestamp."""
        token_upper = token.upper()
        if token_upper not in self.data:
            return None

        ohlcv_list = self.data[token_upper]
        if not ohlcv_list:
            return None

        # Find the closest OHLCV candle
        for i, candle in enumerate(ohlcv_list):
            if candle.timestamp >= timestamp:
                # Use the close price of the previous candle if available
                if i > 0:
                    return ohlcv_list[i - 1].close
                return candle.open
            if i == len(ohlcv_list) - 1:
                # Past the last candle, use its close
                return candle.close

        return None

    def get_ohlcv_at(self, token: str, timestamp: datetime) -> OHLCV | None:
        """Get OHLCV data at or just before a specific timestamp."""
        token_upper = token.upper()
        if token_upper not in self.data:
            return None

        ohlcv_list = self.data[token_upper]
        if not ohlcv_list:
            return None

        # Find the closest OHLCV candle at or before the timestamp
        result: OHLCV | None = None
        for candle in ohlcv_list:
            if candle.timestamp <= timestamp:
                result = candle
            else:
                break

        return result


class GatewayCoinGeckoDataProvider:
    """Gateway-backed CoinGecko historical data provider implementation.

    Implements the HistoricalDataProvider protocol to provide historical
    price and OHLCV data from CoinGecko through the gateway sidecar.

    All API requests are routed through the gateway, which handles:
    - API key management
    - Rate limiting
    - Caching
    - Security isolation

    Attributes:
        gateway_client: Connected GatewayClient instance

    Example:
        with GatewayClient() as gateway:
            provider = GatewayCoinGeckoDataProvider(gateway_client=gateway)

            # Get a single historical price
            price = await provider.get_price("WETH", datetime(2024, 1, 15))

            # Get OHLCV data for a range
            ohlcv = await provider.get_ohlcv(
                "WETH",
                datetime(2024, 1, 1),
                datetime(2024, 1, 31),
                interval_seconds=3600,
            )

            # Iterate for backtesting
            async for ts, market_state in provider.iterate(config):
                price = market_state.get_price("WETH")
    """

    # Supported tokens
    _SUPPORTED_TOKENS: ClassVar[list[str]] = list(TOKEN_IDS.keys())

    # Supported chains
    _SUPPORTED_CHAINS: ClassVar[list[str]] = ["arbitrum", "ethereum", "base", "optimism", "avalanche", "bnb"]

    def __init__(self, gateway_client: "GatewayClient") -> None:
        """Initialize the gateway-backed CoinGecko data provider.

        Args:
            gateway_client: Connected GatewayClient instance
        """
        self._gateway_client = gateway_client
        self._cache: OHLCVCache | None = None

        logger.info("Initialized GatewayCoinGeckoDataProvider")

    def _resolve_token_id(self, token: str) -> str | None:
        """Resolve token symbol to CoinGecko ID."""
        return TOKEN_IDS.get(token.upper())

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        """Get the price of a token at a specific timestamp.

        Uses the gateway's CoinGecko historical price endpoint.

        Args:
            token: Token symbol (e.g., "WETH", "USDC", "ARB")
            timestamp: The historical point in time

        Returns:
            Price in USD at the specified timestamp

        Raises:
            ValueError: If price data is not available for the token/timestamp
        """
        token_id = self._resolve_token_id(token)
        if token_id is None:
            raise ValueError(f"Unknown token: {token}")

        # Check cache first
        if self._cache is not None:
            cached_price = self._cache.get_price_at(token, timestamp)
            if cached_price is not None:
                return cached_price

        # Format date for CoinGecko API (dd-mm-yyyy)
        date_str = timestamp.strftime("%d-%m-%Y")

        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.CoinGeckoHistoricalPriceRequest(
            token_id=token_id,
            date=date_str,
        )

        response = await asyncio.to_thread(
            self._gateway_client.integration.CoinGeckoGetHistoricalPrice,
            request,
            self._gateway_client.config.timeout,
        )

        if not response.success:
            raise ValueError(f"Failed to get historical price: {response.error}")

        price_str = response.price_usd
        if not price_str or price_str == "0":
            raise ValueError(f"No price data available for {token} on {date_str}")

        return Decimal(price_str)

    async def get_ohlcv(
        self,
        token: str,
        start: datetime,
        end: datetime,
        interval_seconds: int = 3600,
    ) -> list[OHLCV]:
        """Get OHLCV data for a token over a time range.

        Uses the gateway's CoinGecko market chart range endpoint.

        Note: CoinGecko's API has granularity limits:
        - 1-2 days: 5-minute intervals
        - 3-90 days: hourly intervals
        - >90 days: daily intervals

        Args:
            token: Token symbol (e.g., "WETH", "USDC", "ARB")
            start: Start of the time range (inclusive)
            end: End of the time range (inclusive)
            interval_seconds: Candle interval in seconds (default: 3600 = 1 hour)
                              Note: CoinGecko may return different intervals based
                              on the date range.

        Returns:
            List of OHLCV data points, sorted by timestamp ascending

        Raises:
            ValueError: If data is not available for the token/range
        """
        # interval_seconds is required by HistoricalDataProvider protocol but unused here
        # CoinGecko API determines granularity automatically based on date range
        _ = interval_seconds

        token_id = self._resolve_token_id(token)
        if token_id is None:
            raise ValueError(f"Unknown token: {token}")

        # Convert to Unix timestamps
        start_ts = int(start.timestamp())
        end_ts = int(end.timestamp())

        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.CoinGeckoMarketChartRangeRequest(
            token_id=token_id,
            from_timestamp=start_ts,
            to_timestamp=end_ts,
            vs_currency="usd",
        )

        response = await asyncio.to_thread(
            self._gateway_client.integration.CoinGeckoGetMarketChartRange,
            request,
            self._gateway_client.config.timeout,
        )

        if not response.success:
            raise ValueError(f"Failed to get market chart: {response.error}")

        if not response.prices:
            raise ValueError(f"No price data available for {token} in range")

        # CoinGecko returns [timestamp_ms, price] pairs via our proto
        # Convert to OHLCV (using same price for O/H/L/C since we only get close prices)
        ohlcv_list: list[OHLCV] = []

        for data_point in response.prices:
            ts = datetime.fromtimestamp(data_point.timestamp / 1000, tz=UTC)
            price_dec = Decimal(data_point.price)

            ohlcv = OHLCV(
                timestamp=ts,
                open=price_dec,
                high=price_dec,
                low=price_dec,
                close=price_dec,
                volume=None,  # Volume data is separate in CoinGecko API
            )
            ohlcv_list.append(ohlcv)

        # Sort by timestamp ascending
        ohlcv_list.sort(key=lambda x: x.timestamp)

        return ohlcv_list

    async def _prefetch_ohlcv_data(self, config: HistoricalDataConfig) -> OHLCVCache:
        """Prefetch all OHLCV data needed for the backtest.

        This method fetches all historical data upfront to minimize API calls
        during iteration and avoid rate limiting issues.

        Args:
            config: Historical data configuration

        Returns:
            OHLCVCache with all prefetched data
        """
        data: dict[str, list[OHLCV]] = {}

        for token in config.tokens:
            try:
                ohlcv = await self.get_ohlcv(
                    token,
                    config.start_time,
                    config.end_time,
                    config.interval_seconds,
                )
                data[token.upper()] = ohlcv
                logger.info("Prefetched %d data points for %s via gateway", len(ohlcv), token)
            except ValueError as e:
                logger.warning("Failed to prefetch data for %s: %s", token, e)
                data[token.upper()] = []

        return OHLCVCache(data=data, fetched_at=datetime.now(UTC))

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Iterate through historical market states.

        This method prefetches all OHLCV data upfront, then yields market
        state snapshots at regular intervals throughout the configured time range.

        Args:
            config: Configuration specifying time range, interval, and tokens

        Yields:
            Tuples of (timestamp, MarketState) for each time point

        Example:
            async for timestamp, market_state in provider.iterate(config):
                eth_price = market_state.get_price("WETH")
                # Process market state
        """
        logger.info(
            "Starting iteration from %s to %s with %ds interval for tokens: %s",
            config.start_time,
            config.end_time,
            config.interval_seconds,
            config.tokens,
        )

        # Prefetch all OHLCV data to minimize API calls
        self._cache = await self._prefetch_ohlcv_data(config)

        # Generate timestamps at the specified interval
        current_time = config.start_time
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=UTC)

        end_time = config.end_time
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=UTC)

        interval = timedelta(seconds=config.interval_seconds)

        while current_time <= end_time:
            # Build prices dict from cache
            prices: dict[str, Decimal] = {}
            ohlcv_data: dict[str, OHLCV] = {}

            for token in config.tokens:
                token_upper = token.upper()

                # Get OHLCV data if requested
                if config.include_ohlcv:
                    candle = self._cache.get_ohlcv_at(token_upper, current_time)
                    if candle is not None:
                        ohlcv_data[token_upper] = candle
                        prices[token_upper] = candle.close

                # If no OHLCV, try to get price directly from cache
                if token_upper not in prices:
                    price = self._cache.get_price_at(token_upper, current_time)
                    if price is not None:
                        prices[token_upper] = price

            # Create MarketState for this timestamp
            market_state = MarketState(
                timestamp=current_time,
                prices=prices,
                ohlcv=ohlcv_data if config.include_ohlcv else {},
                chain=config.chains[0] if config.chains else "arbitrum",
                block_number=None,  # Not available from CoinGecko
                gas_price_gwei=None,  # Not available from CoinGecko
            )

            yield (current_time, market_state)

            current_time += interval

        logger.info("Completed iteration with %d data points", config.estimated_data_points)

    @property
    def provider_name(self) -> str:
        """Return the unique name of this data provider."""
        return "gateway_coingecko"

    @property
    def supported_tokens(self) -> list[str]:
        """Return list of supported token symbols."""
        return self._SUPPORTED_TOKENS.copy()

    @property
    def supported_chains(self) -> list[str]:
        """Return list of supported chain identifiers."""
        return self._SUPPORTED_CHAINS.copy()

    @property
    def min_timestamp(self) -> datetime | None:
        """Return the earliest timestamp with available data.

        CoinGecko has data going back to each token's launch date.
        For most major tokens, this is several years back.
        """
        # CoinGecko has data going back many years for major tokens
        # Return a reasonable minimum (January 2017)
        return datetime(2017, 1, 1, tzinfo=UTC)

    @property
    def max_timestamp(self) -> datetime | None:
        """Return the latest timestamp with available data.

        For CoinGecko, this is approximately "now" minus a small delay.
        """
        return datetime.now(UTC) - timedelta(minutes=5)


__all__ = [
    "GatewayCoinGeckoDataProvider",
    "TOKEN_IDS",
]
