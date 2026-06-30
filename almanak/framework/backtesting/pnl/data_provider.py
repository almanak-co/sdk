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
        chains=[DEFAULT_CHAIN],
    )

    async for timestamp, market_state in data_provider.iterate(config):
        price = market_state.get_price("WETH")
        # ... process market state
"""

import re
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol, TypeGuard, runtime_checkable

from almanak.core.chains import DEFAULT_CHAIN

TokenKey = tuple[str, str]
TokenRef = str | TokenKey

_EVM_ADDRESS_RE = re.compile(r"^0[xX][a-fA-F0-9]{40}$")
_MISSING = object()


def normalize_token_key(chain: str, address: str) -> TokenKey:
    """Return the canonical market-data key for an address on a chain."""
    normalized_address = address.lower() if is_address_like(address) else address
    return chain.lower(), normalized_address


def is_token_key(token: object) -> TypeGuard[TokenKey]:
    """Return True for the ``(chain, address)`` token identity shape."""
    return isinstance(token, tuple) and len(token) == 2 and isinstance(token[0], str) and isinstance(token[1], str)


def is_address_like(token: str) -> bool:
    """Return True for EVM contract-address-shaped strings."""
    return bool(_EVM_ADDRESS_RE.fullmatch(token.strip()))


def token_ref_display(token: TokenRef) -> str:
    """Return a stable display/cache string for token refs."""
    if is_token_key(token):
        chain, address = normalize_token_key(token[0], token[1])
        return f"{chain}:{address}"
    assert isinstance(token, str)
    return token.lower() if is_address_like(token) else token


def token_ref_provider_symbol(
    token: TokenRef,
    chain: str | None = None,
    *,
    unwrap_wrapped_native: bool = False,
) -> str:
    """Resolve a TokenRef to the symbol expected by historical data providers."""
    normalized = normalize_token_ref(token, chain)

    if is_token_key(normalized):
        token_chain, address = normalize_token_key(normalized[0], normalized[1])
        registry_symbol = _token_ref_registry_symbol(
            token_chain,
            address,
            unwrap_wrapped_native=unwrap_wrapped_native,
        )
        if registry_symbol is not None:
            return registry_symbol
        return token_ref_display((token_chain, address))

    assert isinstance(normalized, str)
    if is_address_like(normalized):
        registry_symbol = _token_ref_registry_symbol(
            chain,
            normalized,
            unwrap_wrapped_native=unwrap_wrapped_native,
        )
        if registry_symbol is not None:
            return registry_symbol
    return normalized


def _token_ref_registry_symbol(
    chain: str | None,
    address: str,
    *,
    unwrap_wrapped_native: bool,
) -> str | None:
    """Resolve an address TokenRef to a provider symbol without gateway I/O."""
    if not chain:
        return None

    try:
        from almanak.framework.data.tokens import get_token_resolver
        from almanak.framework.data.tokens.exceptions import TokenResolutionError

        resolved = get_token_resolver().resolve(address, chain, log_errors=False, skip_gateway=True)
    except TokenResolutionError:
        return None

    symbol = resolved.symbol.upper()
    if not unwrap_wrapped_native:
        return symbol

    from almanak.framework.data.models import OHLCV_PROXY_MAP

    return OHLCV_PROXY_MAP.get(symbol, symbol).upper()


def _parse_token_ref_display(token: str) -> TokenKey | None:
    """Parse ``token_ref_display((chain, address))`` back to a token key."""
    chain, separator, address = token.strip().partition(":")
    if separator and chain and is_address_like(address):
        return normalize_token_key(chain, address)
    return None


def normalize_token_ref(token: TokenRef, default_chain: str | None = None) -> TokenRef:
    """Canonicalize a token ref for in-memory backtest lookups."""
    if is_token_key(token):
        return normalize_token_key(token[0], token[1])
    assert isinstance(token, str)
    parsed = _parse_token_ref_display(token)
    if parsed is not None:
        return parsed
    if is_address_like(token):
        return normalize_token_key(default_chain, token) if default_chain else token.lower()
    return token.upper()


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
        prices: Dictionary mapping token identity to USD prices. Address-native
            providers use ``(chain, address)`` keys; custom test providers may
            still use symbols for minimal fixtures.
        ohlcv: Dictionary mapping token identity to OHLCV data (optional)
        chain: The blockchain this data is from
        block_number: Approximate block number at this timestamp (optional)
        gas_price_gwei: Gas price in gwei at this timestamp (optional)
        metadata: Additional market data (e.g., funding rates, liquidity)
    """

    timestamp: datetime
    prices: dict[TokenRef, Decimal] = field(default_factory=dict)
    ohlcv: dict[TokenRef, OHLCV] = field(default_factory=dict)
    chain: str = DEFAULT_CHAIN
    block_number: int | None = None
    gas_price_gwei: Decimal | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def _lookup_keys(self, token: TokenRef) -> list[TokenRef]:
        """Return direct and address-native lookup candidates for ``token``."""
        keys: list[TokenRef] = []

        def add(key: TokenRef) -> None:
            if key not in keys:
                keys.append(key)

        if is_token_key(token):
            chain, address = normalize_token_key(token[0], token[1])
            add((chain, address))
            if chain != str(self.chain).lower():
                return keys
            add(address)
            return keys

        assert isinstance(token, str)
        add(token)
        normalized = normalize_token_ref(token, self.chain)
        if is_token_key(normalized):
            chain, address = normalize_token_key(normalized[0], normalized[1])
            add((chain, address))
            if chain != str(self.chain).lower():
                return keys
            add(address)
            return keys

        if isinstance(normalized, str):
            add(normalized)
            add(normalized.upper())

        return keys

    @staticmethod
    def _lookup(mapping: dict[TokenRef, Any], key: TokenRef) -> Any:
        if key in mapping:
            return mapping[key]
        if isinstance(key, str):
            key_upper = key.upper()
            for stored_key, value in mapping.items():
                if isinstance(stored_key, str) and stored_key.upper() == key_upper:
                    return value
        return _MISSING

    def get_price(self, token: TokenRef) -> Decimal:
        """Get the price of a token at this market state.

        Args:
            token: Token identity as a ``(chain, address)`` key, a contract
                address on ``self.chain``, or a legacy symbol.

        Returns:
            Price in USD

        Raises:
            KeyError: If token price is not available
        """
        for key in self._lookup_keys(token):
            price = self._lookup(self.prices, key)
            if price is not _MISSING:
                return price

            # Try to get price from OHLCV close
            candle = self._lookup(self.ohlcv, key)
            if candle is not _MISSING:
                return candle.close

        raise KeyError(f"Price not available for token: {token}")

    def get_ohlcv(self, token: TokenRef) -> OHLCV | None:
        """Get OHLCV data for a token at this market state.

        Args:
            token: Token identity as a ``(chain, address)`` key, a contract
                address on ``self.chain``, or a legacy symbol.

        Returns:
            OHLCV data if available, None otherwise
        """
        for key in self._lookup_keys(token):
            candle = self._lookup(self.ohlcv, key)
            if candle is not _MISSING:
                return candle
        return None

    def has_token(self, token: TokenRef) -> bool:
        """Check if price data is available for a token.

        Args:
            token: Token identity as a ``(chain, address)`` key, a contract
                address on ``self.chain``, or a legacy symbol.

        Returns:
            True if price data exists
        """
        for key in self._lookup_keys(token):
            if self._lookup(self.prices, key) is not _MISSING or self._lookup(self.ohlcv, key) is not _MISSING:
                return True
        return False

    @property
    def available_tokens(self) -> list[str]:
        """Get list of tokens with available price data."""
        tokens = {token_ref_display(key) for key in self.prices}
        tokens.update(token_ref_display(key) for key in self.ohlcv)
        return sorted(tokens)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "prices": {token_ref_display(k): str(v) for k, v in self.prices.items()},
            "ohlcv": {token_ref_display(k): v.to_dict() for k, v in self.ohlcv.items()},
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
        tokens: List of resolved ``(chain, address)`` token identities or
            legacy token symbols to fetch prices for.
        chains: List of chain identifiers to fetch data for (default: [DEFAULT_CHAIN])
        include_ohlcv: Whether to fetch OHLCV data (default: True)
        include_gas_prices: Whether to fetch historical gas prices (default: False)
    """

    start_time: datetime
    end_time: datetime
    interval_seconds: int = 3600  # 1 hour default
    tokens: list[TokenRef] = field(default_factory=lambda: ["WETH", "USDC"])
    chains: list[str] = field(default_factory=lambda: [DEFAULT_CHAIN])
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
            "tokens": [token_ref_display(token) for token in self.tokens],
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

    async def get_price(self, token: TokenRef, timestamp: datetime) -> Decimal:
        """Get the price of a token at a specific timestamp.

        Args:
            token: Token identity as a ``(chain, address)`` key, or legacy symbol.
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
        token: TokenRef,
        start: datetime,
        end: datetime,
        interval_seconds: int = 3600,
    ) -> list[OHLCV]:
        """Get OHLCV data for a token over a time range.

        Args:
            token: Token identity as a ``(chain, address)`` key, or legacy symbol.
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
    "TokenKey",
    "TokenRef",
    "MarketState",
    "HistoricalDataConfig",
    "HistoricalDataProvider",
    "is_address_like",
    "is_token_key",
    "normalize_token_key",
    "token_ref_display",
]
