"""Data Module Interfaces for Price Sources, Oracles, and Balance Providers.

This module defines the core interfaces (ABCs and Protocols) for the data layer,
enabling multiple data provider implementations with consistent behavior.

Key Components:
    - BasePriceSource: Abstract base class for individual price data sources
    - PriceResult: Dataclass representing a price fetch result with metadata
    - PriceOracle: Protocol for price aggregation logic
    - BalanceProvider: Protocol for on-chain balance queries

Design Philosophy:
    - Each price source (CoinGecko, Chainlink, etc.) implements BasePriceSource
    - PriceOracle aggregates multiple sources and handles failures gracefully
    - BalanceProvider abstracts on-chain balance queries with caching support
    - All results include metadata (staleness, confidence) for informed decisions

Example:
    from almanak.framework.data.interfaces import BasePriceSource, PriceResult, PriceOracle

    class CoinGeckoPriceSource(BasePriceSource):
        async def get_price(self, token: str, quote: str = "USD") -> PriceResult:
            # Fetch from CoinGecko API
            ...

        @property
        def source_name(self) -> str:
            return "coingecko"

    # Use in aggregator
    aggregator = PriceAggregator(sources=[CoinGeckoPriceSource()])
    result = await aggregator.get_aggregated_price("ETH", "USD")
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

# =============================================================================
# Exceptions
# =============================================================================


class DataSourceError(Exception):
    """Base exception for data source errors."""

    pass


class DataSourceUnavailable(DataSourceError):
    """Raised when a data source is unavailable and no cached data exists.

    This exception should be raised when:
    - Network request times out with no cached fallback
    - Rate limit exceeded with no cached fallback
    - Service is down with no cached fallback

    Attributes:
        source: Name of the data source
        reason: Human-readable reason for unavailability
        retry_after: Suggested seconds to wait before retrying (optional)
    """

    def __init__(
        self,
        source: str,
        reason: str,
        retry_after: float | None = None,
    ) -> None:
        self.source = source
        self.reason = reason
        self.retry_after = retry_after
        super().__init__(f"Data source '{source}' unavailable: {reason}")


class DataSourceTimeout(DataSourceError):
    """Raised when a data source request times out.

    This is distinct from DataSourceUnavailable as it may indicate
    a transient issue that could resolve with a retry.
    """

    def __init__(self, source: str, timeout_seconds: float) -> None:
        self.source = source
        self.timeout_seconds = timeout_seconds
        super().__init__(f"Data source '{source}' timed out after {timeout_seconds}s")


class DataSourceRateLimited(DataSourceError):
    """Raised when a data source rate limit is exceeded.

    Attributes:
        source: Name of the data source
        retry_after: Seconds to wait before retrying
    """

    def __init__(self, source: str, retry_after: float) -> None:
        self.source = source
        self.retry_after = retry_after
        super().__init__(f"Data source '{source}' rate limited. Retry after {retry_after}s")


class AllDataSourcesFailed(DataSourceError):
    """Raised when all data sources fail to provide data.

    This exception aggregates errors from multiple sources for debugging.

    Attributes:
        errors: Dictionary mapping source names to their error messages
    """

    def __init__(self, errors: dict[str, str]) -> None:
        self.errors = errors
        error_summary = "; ".join(f"{k}: {v}" for k, v in errors.items())
        super().__init__(f"All data sources failed: {error_summary}")


class InsufficientDataError(DataSourceError):
    """Raised when there isn't enough historical data for calculations.

    Used by indicators like RSI that require a minimum amount of history.

    Attributes:
        required: Number of data points required
        available: Number of data points available
        indicator: Name of the indicator (e.g., "RSI")
    """

    def __init__(self, required: int, available: int, indicator: str = "") -> None:
        self.required = required
        self.available = available
        self.indicator = indicator
        msg = f"Insufficient data: need {required} points, have {available}"
        if indicator:
            msg = f"{indicator}: {msg}"
        super().__init__(msg)


class StaleData(DataSourceError):
    """Raised when data is stale beyond acceptable thresholds.

    This exception indicates that the data is too old to be reliable
    for trading decisions. The caller should either retry with a
    fresh source or handle the staleness appropriately.

    Attributes:
        source: Name of the data source
        age_seconds: How old the data is in seconds
        threshold_seconds: The threshold that was exceeded
    """

    def __init__(
        self,
        source: str,
        age_seconds: float,
        threshold_seconds: float,
    ) -> None:
        self.source = source
        self.age_seconds = age_seconds
        self.threshold_seconds = threshold_seconds
        super().__init__(f"Data from '{source}' is stale: {age_seconds:.1f}s old (threshold: {threshold_seconds:.1f}s)")


class StaleDataWarning(Warning):
    """Warning raised when data is approaching staleness threshold.

    This is a warning, not an error - it indicates that data is getting
    stale but is not yet critical. Strategies can choose to log this
    warning or take precautionary measures.

    Use this for soft freshness thresholds (e.g., price_warn_sec=30).
    For critical staleness that should stop execution, use StaleDataError.

    Attributes:
        source: Name of the data source
        age_seconds: How old the data is in seconds
        threshold_seconds: The warning threshold that was exceeded
        data_type: Type of data (e.g., "price", "gas", "pool")
    """

    def __init__(
        self,
        source: str,
        age_seconds: float,
        threshold_seconds: float,
        data_type: str = "data",
    ) -> None:
        self.source = source
        self.age_seconds = age_seconds
        self.threshold_seconds = threshold_seconds
        self.data_type = data_type
        super().__init__(
            f"{data_type.capitalize()} from '{source}' is stale: {age_seconds:.1f}s old "
            f"(warn threshold: {threshold_seconds:.1f}s)"
        )


class StaleDataError(DataSourceError):
    """Raised when data staleness is critical and execution should stop.

    This exception is raised when data exceeds the critical staleness
    threshold (e.g., price_error_sec=300). Unlike StaleDataWarning,
    this indicates a serious freshness issue that should prevent
    trading decisions from being made.

    Attributes:
        source: Name of the data source
        age_seconds: How old the data is in seconds
        threshold_seconds: The error threshold that was exceeded
        data_type: Type of data (e.g., "price", "gas", "pool")
    """

    def __init__(
        self,
        source: str,
        age_seconds: float,
        threshold_seconds: float,
        data_type: str = "data",
    ) -> None:
        self.source = source
        self.age_seconds = age_seconds
        self.threshold_seconds = threshold_seconds
        self.data_type = data_type
        super().__init__(
            f"{data_type.capitalize()} from '{source}' is critically stale: {age_seconds:.1f}s old "
            f"(error threshold: {threshold_seconds:.1f}s)"
        )


class Divergence(DataSourceError):
    """Raised when data sources diverge beyond acceptable thresholds.

    This exception indicates that multiple data sources are reporting
    significantly different values, which may indicate a data quality
    issue or market anomaly.

    Attributes:
        sources: Dictionary mapping source names to their reported values
        divergence_pct: The percentage divergence between sources
        threshold_pct: The threshold that was exceeded
    """

    def __init__(
        self,
        sources: dict[str, Decimal],
        divergence_pct: float,
        threshold_pct: float,
    ) -> None:
        self.sources = sources
        self.divergence_pct = divergence_pct
        self.threshold_pct = threshold_pct
        source_summary = ", ".join(f"{k}: {v}" for k, v in sources.items())
        super().__init__(
            f"Data sources diverge by {divergence_pct:.2f}% (threshold: {threshold_pct:.2f}%): {source_summary}"
        )


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class PriceResult:
    """Result of a price fetch from a data source.

    This dataclass encapsulates a price result with metadata indicating
    the quality and freshness of the data, allowing consumers to make
    informed decisions about data reliability.

    Attributes:
        price: The fetched price as a Decimal for precision
        source: Name of the data source (e.g., "coingecko", "chainlink")
        timestamp: When the price was fetched/updated
        confidence: Confidence score from 0.0 (unreliable) to 1.0 (fully confident)
            - 1.0: Fresh data, directly from source
            - 0.8-0.99: Fresh data with minor degradation
            - 0.5-0.79: Stale data from cache
            - < 0.5: Highly degraded, use with caution
        stale: Whether the data is from cache due to source unavailability

    Example:
        # Fresh price from live source
        result = PriceResult(
            price=Decimal("2500.50"),
            source="coingecko",
            timestamp=datetime.now(timezone.utc),
            confidence=1.0,
            stale=False,
        )

        # Stale cached price after timeout
        result = PriceResult(
            price=Decimal("2500.50"),
            source="coingecko",
            timestamp=datetime.now(timezone.utc) - timedelta(minutes=5),
            confidence=0.7,
            stale=True,
        )
    """

    price: Decimal
    source: str
    timestamp: datetime
    confidence: float
    stale: bool = False
    source_details: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        """Validate confidence is within bounds."""
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Confidence must be between 0 and 1, got {self.confidence}")
        if not isinstance(self.price, Decimal):
            # Allow conversion from float/int for convenience
            object.__setattr__(self, "price", Decimal(str(self.price)))

    @property
    def age_seconds(self) -> float:
        """Calculate age of the price data in seconds."""
        return (datetime.now(UTC) - self.timestamp).total_seconds()

    @property
    def is_fresh(self) -> bool:
        """Check if data is fresh (not stale and recent)."""
        return not self.stale and self.age_seconds < 60  # 1 minute threshold

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "price": str(self.price),
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "confidence": self.confidence,
            "stale": self.stale,
            "age_seconds": self.age_seconds,
            "source_details": self.source_details,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PriceResult":
        """Create PriceResult from dictionary."""
        return cls(
            price=Decimal(data["price"]),
            source=data["source"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            confidence=data["confidence"],
            stale=data.get("stale", False),
            source_details=data.get("source_details"),
        )


@dataclass
class QualityFlags:
    """Flags indicating data quality issues.

    These flags provide granular information about potential data quality
    concerns, allowing consumers to make informed decisions about how to
    handle or weight the data.

    Attributes:
        is_stale: Data is older than expected freshness threshold
        is_partial: Data is incomplete (e.g., missing some sources)
        is_inferred: Data was inferred/estimated rather than directly observed
        fallback_used: Primary source failed, using backup source
        outlier_detected: Value deviates significantly from expected range
        low_liquidity: Market has low liquidity, prices may be unreliable
        source_degraded: Data source is experiencing issues but still functional
    """

    is_stale: bool = False
    is_partial: bool = False
    is_inferred: bool = False
    fallback_used: bool = False
    outlier_detected: bool = False
    low_liquidity: bool = False
    source_degraded: bool = False

    @property
    def has_issues(self) -> bool:
        """Check if any quality flags are set."""
        return any(
            [
                self.is_stale,
                self.is_partial,
                self.is_inferred,
                self.fallback_used,
                self.outlier_detected,
                self.low_liquidity,
                self.source_degraded,
            ]
        )

    def to_dict(self) -> dict[str, bool]:
        """Convert to dictionary for serialization."""
        return {
            "is_stale": self.is_stale,
            "is_partial": self.is_partial,
            "is_inferred": self.is_inferred,
            "fallback_used": self.fallback_used,
            "outlier_detected": self.outlier_detected,
            "low_liquidity": self.low_liquidity,
            "source_degraded": self.source_degraded,
        }


@dataclass
class DataPoint:
    """Generic data point with comprehensive metadata.

    This is the foundational data structure for the data module V2,
    representing a single observed value with full provenance and
    quality information.

    Attributes:
        value: The observed value (can be any type: Decimal, float, dict, etc.)
        observed_at: When the value was observed at the source
        fetched_at: When the value was fetched by our system
        source_time: Original timestamp from the data source (if available)
        confidence: Confidence score from 0.0 to 1.0
        source: Name of the data source
        chain_id: Blockchain chain ID (e.g., 1 for Ethereum mainnet)
        asset_id: Asset identifier (e.g., token address or symbol)
        venue_id: Trading venue identifier (e.g., "uniswap_v3", "coingecko")
        quality_flags: Detailed quality indicators
        metadata: Additional source-specific metadata
    """

    value: Any
    observed_at: datetime
    fetched_at: datetime
    source: str
    confidence: float = 1.0
    source_time: datetime | None = None
    chain_id: int | None = None
    asset_id: str | None = None
    venue_id: str | None = None
    quality_flags: QualityFlags = field(default_factory=QualityFlags)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate confidence is within bounds."""
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Confidence must be between 0 and 1, got {self.confidence}")

    @property
    def age_seconds(self) -> float:
        """Calculate age of the data in seconds from observed_at."""
        return (datetime.now(UTC) - self.observed_at).total_seconds()

    @property
    def fetch_latency_seconds(self) -> float:
        """Calculate latency between observation and fetch."""
        return (self.fetched_at - self.observed_at).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "value": str(self.value) if isinstance(self.value, Decimal) else self.value,
            "observed_at": self.observed_at.isoformat(),
            "fetched_at": self.fetched_at.isoformat(),
            "source_time": self.source_time.isoformat() if self.source_time else None,
            "confidence": self.confidence,
            "source": self.source,
            "chain_id": self.chain_id,
            "asset_id": self.asset_id,
            "venue_id": self.venue_id,
            "quality_flags": self.quality_flags.to_dict(),
            "metadata": self.metadata,
        }


@dataclass
class OHLCVCandle:
    """OHLCV (Open, High, Low, Close, Volume) candlestick data.

    Represents a single candlestick with all values as Decimal for
    precision in financial calculations.

    Attributes:
        timestamp: Start time of the candle
        open: Opening price
        high: Highest price during the period
        low: Lowest price during the period
        close: Closing price
        volume: Trading volume (can be None if unavailable)
    """

    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None = None

    def __post_init__(self) -> None:
        """Convert numeric types to Decimal if needed."""
        for field_name in ("open", "high", "low", "close"):
            val = getattr(self, field_name)
            if not isinstance(val, Decimal):
                object.__setattr__(self, field_name, Decimal(str(val)))
        if self.volume is not None and not isinstance(self.volume, Decimal):
            object.__setattr__(self, "volume", Decimal(str(self.volume)))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume) if self.volume is not None else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OHLCVCandle":
        """Create OHLCVCandle from dictionary."""
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            open=Decimal(data["open"]),
            high=Decimal(data["high"]),
            low=Decimal(data["low"]),
            close=Decimal(data["close"]),
            volume=Decimal(data["volume"]) if data.get("volume") else None,
        )


@dataclass
class BalanceResult:
    """Result of a balance query.

    Attributes:
        balance: Token balance in human-readable units (e.g., 1.5 ETH, not wei)
        token: Token symbol (e.g., "WETH", "USDC")
        address: Token contract address (or native token placeholder)
        decimals: Token decimal places for conversion
        raw_balance: Raw balance in smallest units (wei)
        timestamp: When the balance was fetched
        stale: Whether from cache due to RPC unavailability
    """

    balance: Decimal
    token: str
    address: str
    decimals: int
    raw_balance: int
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    stale: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "balance": str(self.balance),
            "token": self.token,
            "address": self.address,
            "decimals": self.decimals,
            "raw_balance": str(self.raw_balance),
            "timestamp": self.timestamp.isoformat(),
            "stale": self.stale,
        }


# =============================================================================
# Abstract Base Classes
# =============================================================================


class BasePriceSource(ABC):
    """Abstract base class for price data sources.

    Each price data source (CoinGecko, Chainlink, Binance, etc.) should
    implement this interface to provide a consistent way to fetch prices.

    Implementations must handle:
    - Caching with configurable TTL
    - Rate limiting with backoff
    - Timeout handling with graceful degradation
    - Error recovery with stale data fallback

    The contract for implementations:
    1. On success: Return fresh PriceResult with confidence=1.0
    2. On timeout with cache: Return stale PriceResult with reduced confidence
    3. On timeout without cache: Raise DataSourceUnavailable
    4. On rate limit: Raise DataSourceRateLimited with retry_after

    Example implementation:
        class CoinGeckoPriceSource(BasePriceSource):
            def __init__(self, cache_ttl: int = 30):
                self._cache_ttl = cache_ttl
                self._cache: dict[str, tuple[PriceResult, datetime]] = {}

            async def get_price(self, token: str, quote: str = "USD") -> PriceResult:
                cache_key = f"{token}/{quote}"

                # Check cache first
                if cache_key in self._cache:
                    result, cached_at = self._cache[cache_key]
                    if (datetime.now(timezone.utc) - cached_at).seconds < self._cache_ttl:
                        return result

                # Fetch fresh data
                try:
                    price = await self._fetch_from_api(token, quote)
                    result = PriceResult(
                        price=price,
                        source=self.source_name,
                        timestamp=datetime.now(timezone.utc),
                        confidence=1.0,
                    )
                    self._cache[cache_key] = (result, datetime.now(timezone.utc))
                    return result
                except TimeoutError:
                    # Return stale if available
                    if cache_key in self._cache:
                        stale_result, _ = self._cache[cache_key]
                        return PriceResult(
                            price=stale_result.price,
                            source=self.source_name,
                            timestamp=stale_result.timestamp,
                            confidence=0.7,  # Reduced due to staleness
                            stale=True,
                        )
                    raise DataSourceUnavailable(self.source_name, "Timeout with no cache")

            @property
            def source_name(self) -> str:
                return "coingecko"

            @property
            def supported_tokens(self) -> list[str]:
                return ["ETH", "BTC", "USDC", "ARB", "WETH", ...]
    """

    @abstractmethod
    async def get_price(self, token: str, quote: str = "USD") -> PriceResult:
        """Fetch the current price for a token.

        Args:
            token: Token symbol (e.g., "ETH", "WETH", "ARB")
            quote: Quote currency (default "USD")

        Returns:
            PriceResult with price and metadata

        Raises:
            DataSourceUnavailable: If source is unavailable and no cache exists
            DataSourceTimeout: If request times out (before checking cache)
            DataSourceRateLimited: If rate limit is exceeded
        """
        pass

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return the unique name of this data source.

        This name is used in logging, metrics, and aggregation logic.
        Should be lowercase with underscores (e.g., "coingecko", "chainlink_eth").

        Returns:
            Unique source identifier string
        """
        pass

    @property
    def supported_tokens(self) -> list[str]:
        """Return list of tokens this source supports.

        Override to provide the list of token symbols this source can fetch.
        Default implementation returns empty list (meaning unknown/dynamic support).

        Returns:
            List of supported token symbols
        """
        return []

    @property
    def cache_ttl_seconds(self) -> int:
        """Return the default cache TTL for this source.

        Override to customize caching behavior per source.
        Default is 30 seconds.

        Returns:
            Cache TTL in seconds
        """
        return 30

    async def health_check(self) -> bool:
        """Check if the data source is healthy and responding.

        Override to implement source-specific health checks.
        Default implementation tries to fetch ETH price as a ping.

        Returns:
            True if source is healthy, False otherwise
        """
        try:
            await self.get_price("ETH")
            return True
        except Exception:
            return False


# =============================================================================
# Protocols
# =============================================================================


@runtime_checkable
class PriceOracle(Protocol):
    """Protocol for price aggregation and oracle logic.

    A PriceOracle wraps one or more BasePriceSource implementations and
    provides aggregation logic (median, weighted average, etc.) along with
    outlier detection and graceful degradation.

    The oracle is the primary interface for strategies to get price data,
    abstracting away the complexity of managing multiple sources.

    Key responsibilities:
    - Aggregate prices from multiple sources
    - Detect and filter outliers (>2% deviation from median)
    - Handle partial failures (some sources down)
    - Track source health metrics for routing decisions

    Example implementation:
        class PriceAggregator:
            def __init__(self, sources: list[BasePriceSource]):
                self._sources = sources
                self._health_metrics: dict[str, SourceHealthMetrics] = {}

            async def get_aggregated_price(
                self, token: str, quote: str = "USD"
            ) -> PriceResult:
                results = []
                errors = {}

                for source in self._sources:
                    try:
                        result = await source.get_price(token, quote)
                        results.append(result)
                    except Exception as e:
                        errors[source.source_name] = str(e)

                if not results:
                    raise AllDataSourcesFailed(errors)

                # Return median price with aggregated confidence
                median_price = self._calculate_median(results)
                confidence = self._calculate_confidence(results)
                return PriceResult(
                    price=median_price,
                    source="aggregated",
                    timestamp=datetime.now(timezone.utc),
                    confidence=confidence,
                )
    """

    async def get_aggregated_price(self, token: str, quote: str = "USD") -> PriceResult:
        """Get aggregated price from multiple sources.

        Args:
            token: Token symbol to get price for
            quote: Quote currency (default "USD")

        Returns:
            PriceResult with aggregated price and confidence

        Raises:
            AllDataSourcesFailed: If all sources fail to provide data
        """
        ...

    def get_source_health(self, source_name: str) -> dict[str, Any] | None:
        """Get health metrics for a specific source.

        Returns metrics like success rate, average latency, last error time.

        Args:
            source_name: Name of the source to query

        Returns:
            Dictionary with health metrics, or None if source unknown
        """
        ...


@runtime_checkable
class BalanceProvider(Protocol):
    """Protocol for on-chain balance queries.

    A BalanceProvider abstracts the complexity of querying ERC-20 and
    native token balances from the blockchain, handling decimal conversion,
    caching, and RPC error recovery.

    Key responsibilities:
    - Query ERC-20 balances via balanceOf
    - Query native ETH balance via eth_getBalance
    - Handle token decimal conversion correctly
    - Cache balances to reduce RPC load
    - Invalidate cache after transaction execution

    Example implementation:
        class Web3BalanceProvider:
            def __init__(self, web3: Web3, wallet_address: str):
                self._web3 = web3
                self._wallet = wallet_address
                self._cache: dict[str, BalanceResult] = {}
                self._cache_ttl = 5  # 5 second cache

            async def get_balance(self, token: str) -> BalanceResult:
                if token == "ETH":
                    return await self._get_native_balance()

                token_info = self._get_token_info(token)
                contract = self._web3.eth.contract(
                    address=token_info.address,
                    abi=ERC20_ABI,
                )
                raw_balance = contract.functions.balanceOf(self._wallet).call()
                balance = Decimal(raw_balance) / Decimal(10 ** token_info.decimals)

                return BalanceResult(
                    balance=balance,
                    token=token,
                    address=token_info.address,
                    decimals=token_info.decimals,
                    raw_balance=raw_balance,
                )

            def invalidate_cache(self, token: Optional[str] = None) -> None:
                if token:
                    self._cache.pop(token, None)
                else:
                    self._cache.clear()
    """

    async def get_balance(self, token: str) -> BalanceResult:
        """Get the balance of a token for the configured wallet.

        Args:
            token: Token symbol (e.g., "WETH", "USDC") or "ETH" for native

        Returns:
            BalanceResult with balance in human-readable units

        Raises:
            DataSourceError: If balance cannot be fetched
        """
        ...

    async def get_native_balance(self) -> BalanceResult:
        """Get the native token balance (ETH, MATIC, etc.).

        Convenience method for getting the chain's native token balance.

        Returns:
            BalanceResult for native token
        """
        ...

    def invalidate_cache(self, token: str | None = None) -> None:
        """Invalidate cached balances.

        Should be called after transaction execution to ensure fresh data.

        Args:
            token: Specific token to invalidate, or None to clear all
        """
        ...


# Valid OHLCV timeframes
VALID_TIMEFRAMES: list[str] = ["1m", "5m", "15m", "1h", "4h", "1d"]


def validate_timeframe(timeframe: str) -> None:
    """Validate that a timeframe is one of the supported values.

    Args:
        timeframe: The timeframe string to validate

    Raises:
        ValueError: If the timeframe is not valid
    """
    if timeframe not in VALID_TIMEFRAMES:
        raise ValueError(f"Invalid timeframe '{timeframe}'. Must be one of: {', '.join(VALID_TIMEFRAMES)}")


@runtime_checkable
class OHLCVProvider(Protocol):
    """Protocol for OHLCV (candlestick) data providers.

    Used by indicators like RSI that need historical price data.

    Implementations must:
    - Support multiple timeframes (1m, 5m, 15m, 1h, 4h, 1d)
    - Return properly typed OHLCVCandle objects
    - Handle caching to avoid repeated API calls
    - Gracefully degrade if full history unavailable

    The supported_timeframes property should return the subset of
    VALID_TIMEFRAMES that this provider can supply data for.
    """

    @property
    def supported_timeframes(self) -> list[str]:
        """Return the list of timeframes this provider supports.

        Returns:
            List of supported timeframe strings (e.g., ["1h", "4h", "1d"])
        """
        ...

    async def get_ohlcv(
        self,
        token: str,
        quote: str = "USD",
        timeframe: str = "1h",
        limit: int = 100,
    ) -> list[OHLCVCandle]:
        """Get OHLCV data for a token.

        Args:
            token: Token symbol
            quote: Quote currency
            timeframe: Candle timeframe (must be in supported_timeframes)
            limit: Number of candles to fetch

        Returns:
            List of OHLCVCandle objects sorted by timestamp ascending

        Raises:
            DataSourceError: If data cannot be fetched
            InsufficientDataError: If requested limit exceeds available data
            ValueError: If timeframe is not supported
        """
        ...


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    # Constants
    "VALID_TIMEFRAMES",
    # Utility functions
    "validate_timeframe",
    # Data classes
    "PriceResult",
    "BalanceResult",
    "QualityFlags",
    "DataPoint",
    "OHLCVCandle",
    # Abstract base classes
    "BasePriceSource",
    # Protocols
    "PriceOracle",
    "BalanceProvider",
    "OHLCVProvider",
    # Exceptions
    "DataSourceError",
    "DataSourceUnavailable",
    "DataSourceTimeout",
    "DataSourceRateLimited",
    "AllDataSourcesFailed",
    "InsufficientDataError",
    "StaleData",
    "StaleDataWarning",
    "StaleDataError",
    "Divergence",
]
