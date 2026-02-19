"""Historical data provider interface for PnL backtesting.

This module defines the protocol and data models for historical data providers
that feed price and market data to the PnL backtesting engine.

Key Components:
    - HistoricalDataProvider: Protocol defining the data provider interface
    - MarketState: Point-in-time snapshot of market data
    - HistoricalDataConfig: Configuration for data retrieval
    - OHLCV: OHLCV (Open-High-Low-Close-Volume) data structure

Example:
    from almanak.framework.backtesting.pnl.data_provider import (
        HistoricalDataProvider,
        MarketState,
        HistoricalDataConfig,
    )

    config = HistoricalDataConfig(
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 6, 1),
        interval_seconds=3600,  # 1 hour
        tokens=["WETH", "USDC", "ARB"],
        chains=["arbitrum"],
    )

    async for timestamp, market_state in data_provider.iterate(config):
        price = market_state.get_price("WETH")
        # ... process market state
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable


class HistoricalDataCapability(StrEnum):
    """Declares the historical data capability of a data provider.

    This enum helps users understand what to expect from different data providers
    when running backtests. Each provider should declare its capability so the
    backtesting engine can warn about potential data limitations.

    Values:
        FULL: Provider can fetch historical data for any past timestamp on-demand.
            This is the most flexible capability. The provider has access to a
            complete historical database and can return accurate prices for any
            point in time within its supported range.
            Example: CoinGecko API - can query historical prices for any date.

        CURRENT_ONLY: Provider can only fetch current/live data.
            The provider does not have access to historical data and can only
            return the current market price. When used in backtesting, this means
            the "historical" prices will actually be the price at backtest runtime,
            which is incorrect for historical analysis.
            Example: TWAP from on-chain DEX pools - only reflects current pool state.

        PRE_CACHE: Provider requires data to be pre-fetched/cached before backtest.
            The provider can access historical data, but requires it to be loaded
            into a cache before the backtest starts. This is typically due to
            rate limits or slow data access that would make on-demand fetching
            impractical during simulation.
            Example: Chainlink oracles - historical round data must be pre-fetched.
    """

    FULL = "full"
    CURRENT_ONLY = "current_only"
    PRE_CACHE = "pre_cache"


@dataclass
class OHLCV:
    """OHLCV (Open-High-Low-Close-Volume) price data.

    Represents a single candlestick/bar of price data for a token.

    Attributes:
        timestamp: Start time of this candle
        open: Opening price in USD
        high: Highest price in USD during the period
        low: Lowest price in USD during the period
        close: Closing price in USD
        volume: Trading volume in USD (optional)
    """

    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None = None

    @property
    def mid_price(self) -> Decimal:
        """Get the mid-point price (average of high and low)."""
        return (self.high + self.low) / Decimal("2")

    @property
    def typical_price(self) -> Decimal:
        """Get the typical price (average of high, low, close)."""
        return (self.high + self.low + self.close) / Decimal("3")

    @property
    def range(self) -> Decimal:
        """Get the price range (high - low)."""
        return self.high - self.low

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume) if self.volume is not None else None,
        }


@dataclass
class MarketState:
    """Point-in-time snapshot of market data.

    Holds all relevant market data at a specific timestamp for use
    in backtesting simulations. This includes token prices, OHLCV data,
    and optional additional market context.

    Attributes:
        timestamp: The point in time this state represents
        prices: Dictionary mapping token symbols to USD prices
        ohlcv: Dictionary mapping token symbols to OHLCV data (optional)
        chain: The blockchain this data is from
        block_number: Approximate block number at this timestamp (optional)
        gas_price_gwei: Gas price in gwei at this timestamp (optional)
        metadata: Additional market data (e.g., funding rates, liquidity)
    """

    timestamp: datetime
    prices: dict[str, Decimal] = field(default_factory=dict)
    ohlcv: dict[str, OHLCV] = field(default_factory=dict)
    chain: str = "arbitrum"
    block_number: int | None = None
    gas_price_gwei: Decimal | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def get_price(self, token: str) -> Decimal:
        """Get the price of a token at this market state.

        Args:
            token: Token symbol (e.g., "WETH", "USDC")

        Returns:
            Price in USD

        Raises:
            KeyError: If token price is not available
        """
        token_upper = token.upper()
        if token_upper in self.prices:
            return self.prices[token_upper]

        # Try to get price from OHLCV close
        if token_upper in self.ohlcv:
            return self.ohlcv[token_upper].close

        raise KeyError(f"Price not available for token: {token}")

    def get_ohlcv(self, token: str) -> OHLCV | None:
        """Get OHLCV data for a token at this market state.

        Args:
            token: Token symbol (e.g., "WETH", "USDC")

        Returns:
            OHLCV data if available, None otherwise
        """
        return self.ohlcv.get(token.upper())

    def has_token(self, token: str) -> bool:
        """Check if price data is available for a token.

        Args:
            token: Token symbol

        Returns:
            True if price data exists
        """
        token_upper = token.upper()
        return token_upper in self.prices or token_upper in self.ohlcv

    @property
    def available_tokens(self) -> list[str]:
        """Get list of tokens with available price data."""
        tokens = set(self.prices.keys())
        tokens.update(self.ohlcv.keys())
        return sorted(tokens)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "prices": {k: str(v) for k, v in self.prices.items()},
            "ohlcv": {k: v.to_dict() for k, v in self.ohlcv.items()},
            "chain": self.chain,
            "block_number": self.block_number,
            "gas_price_gwei": str(self.gas_price_gwei) if self.gas_price_gwei is not None else None,
            "metadata": self.metadata,
        }


@dataclass
class HistoricalDataConfig:
    """Configuration for historical data retrieval.

    Specifies the time range, interval, and tokens to fetch for
    a backtest simulation.

    Attributes:
        start_time: Start of the historical period (inclusive)
        end_time: End of the historical period (inclusive)
        interval_seconds: Time between data points in seconds (default: 3600 = 1 hour)
        tokens: List of token symbols to fetch prices for
        chains: List of chain identifiers to fetch data for (default: ["arbitrum"])
        include_ohlcv: Whether to fetch OHLCV data (default: True)
        include_gas_prices: Whether to fetch historical gas prices (default: False)
    """

    start_time: datetime
    end_time: datetime
    interval_seconds: int = 3600  # 1 hour default
    tokens: list[str] = field(default_factory=lambda: ["WETH", "USDC"])
    chains: list[str] = field(default_factory=lambda: ["arbitrum"])
    include_ohlcv: bool = True
    include_gas_prices: bool = False

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if not self.tokens:
            raise ValueError("tokens list cannot be empty")
        if not self.chains:
            raise ValueError("chains list cannot be empty")

    @property
    def duration_seconds(self) -> int:
        """Get the total duration in seconds."""
        delta = self.end_time - self.start_time
        return int(delta.total_seconds())

    @property
    def duration_days(self) -> float:
        """Get the total duration in days."""
        return self.duration_seconds / (24 * 3600)

    @property
    def estimated_data_points(self) -> int:
        """Get the estimated number of data points."""
        return self.duration_seconds // self.interval_seconds

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "interval_seconds": self.interval_seconds,
            "tokens": self.tokens,
            "chains": self.chains,
            "include_ohlcv": self.include_ohlcv,
            "include_gas_prices": self.include_gas_prices,
            "duration_days": self.duration_days,
            "estimated_data_points": self.estimated_data_points,
        }


@runtime_checkable
class HistoricalDataProvider(Protocol):
    """Protocol defining the interface for historical data providers.

    Historical data providers are responsible for fetching price and
    market data for past time periods. They are used by the PnL
    backtesting engine to simulate strategy execution.

    Implementations should handle:
    - Fetching historical prices for specified tokens
    - Providing OHLCV data when available
    - Rate limiting and caching as needed
    - Graceful handling of missing data

    Example implementation:
        class MyDataProvider:
            async def get_price(
                self, token: str, timestamp: datetime
            ) -> Decimal:
                # Fetch price from data source
                ...

            async def get_ohlcv(
                self, token: str, start: datetime, end: datetime, interval: int
            ) -> list[OHLCV]:
                # Fetch OHLCV data
                ...

            async def iterate(
                self, config: HistoricalDataConfig
            ) -> AsyncIterator[tuple[datetime, MarketState]]:
                # Yield market states for each time point
                ...
    """

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        """Get the price of a token at a specific timestamp.

        Args:
            token: Token symbol (e.g., "WETH", "USDC", "ARB")
            timestamp: The historical point in time

        Returns:
            Price in USD at the specified timestamp

        Raises:
            ValueError: If price data is not available for the token/timestamp
            DataSourceUnavailable: If the data source is unavailable
        """
        ...

    async def get_ohlcv(
        self,
        token: str,
        start: datetime,
        end: datetime,
        interval_seconds: int = 3600,
    ) -> list[OHLCV]:
        """Get OHLCV data for a token over a time range.

        Args:
            token: Token symbol (e.g., "WETH", "USDC", "ARB")
            start: Start of the time range (inclusive)
            end: End of the time range (inclusive)
            interval_seconds: Candle interval in seconds (default: 3600 = 1 hour)

        Returns:
            List of OHLCV data points, sorted by timestamp ascending

        Raises:
            ValueError: If data is not available for the token/range
            DataSourceUnavailable: If the data source is unavailable
        """
        ...

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Iterate through historical market states.

        This is the primary method used by the backtesting engine.
        It yields market state snapshots at regular intervals throughout
        the configured time range.

        Args:
            config: Configuration specifying time range, interval, and tokens

        Yields:
            Tuples of (timestamp, MarketState) for each time point

        Raises:
            DataSourceUnavailable: If the data source is unavailable

        Example:
            async for timestamp, market_state in provider.iterate(config):
                eth_price = market_state.get_price("WETH")
                # Process market state
        """
        ...
        # This is needed to make the method signature a generator
        yield  # type: ignore[misc]

    @property
    def provider_name(self) -> str:
        """Return the unique name of this data provider."""
        ...

    @property
    def supported_tokens(self) -> list[str]:
        """Return list of supported token symbols."""
        ...

    @property
    def supported_chains(self) -> list[str]:
        """Return list of supported chain identifiers."""
        ...

    @property
    def min_timestamp(self) -> datetime | None:
        """Return the earliest timestamp with available data, or None if unknown."""
        ...

    @property
    def max_timestamp(self) -> datetime | None:
        """Return the latest timestamp with available data, or None if unknown."""
        ...


__all__ = [
    "HistoricalDataCapability",
    "OHLCV",
    "MarketState",
    "HistoricalDataConfig",
    "HistoricalDataProvider",
]
