"""Aggregated Data Provider with automatic fallback support.

This module provides an AggregatedDataProvider that wraps multiple data providers
and automatically falls back to alternative providers when the primary one fails.

Key Features:
    - Priority-ordered provider list
    - Automatic fallback when providers fail
    - Data source tracking for each price fetch
    - Support for all HistoricalDataProvider protocol methods
    - BacktestDataConfig integration for configuring price provider mode
    - DataConfidence tracking (HIGH for Chainlink/TWAP, MEDIUM for CoinGecko)

Example:
    from almanak.framework.backtesting.pnl.providers import (
        AggregatedDataProvider,
        ChainlinkDataProvider,
        CoinGeckoDataProvider,
    )

    # Create individual providers
    chainlink = ChainlinkDataProvider(chain="arbitrum", rpc_url="...")
    coingecko = CoinGeckoDataProvider(api_key="...")

    # Create aggregated provider with priority order
    aggregated = AggregatedDataProvider(
        providers=[chainlink, coingecko],  # Chainlink tried first, then CoinGecko
        provider_names=["chainlink", "coingecko"],
    )

    # Get price - automatically falls back if Chainlink fails
    price, source = await aggregated.get_price_with_source("ETH", datetime.now())
    print(f"ETH price: ${price} (from {source})")

    # Or create from BacktestDataConfig for automatic fallback chain
    from almanak.framework.backtesting.config import BacktestDataConfig
    config = BacktestDataConfig(price_provider="auto")  # Chainlink -> TWAP -> CoinGecko
    provider = await AggregatedDataProvider.create_with_data_config(config, chain="arbitrum")
"""

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from ..data_provider import OHLCV, HistoricalDataConfig, MarketState
from ..types import DataConfidence, DataSourceInfo

if TYPE_CHECKING:
    from almanak.framework.backtesting.config import BacktestDataConfig

logger = logging.getLogger(__name__)

# Provider name to DataConfidence mapping
# Chainlink and TWAP are on-chain sources with HIGH confidence
# CoinGecko is an API source with MEDIUM confidence
PROVIDER_CONFIDENCE_MAP: dict[str, DataConfidence] = {
    "chainlink": DataConfidence.HIGH,
    "twap": DataConfidence.HIGH,
    "coingecko": DataConfidence.MEDIUM,
}

# Default confidence for unknown providers
DEFAULT_PROVIDER_CONFIDENCE = DataConfidence.LOW


@dataclass
class ProviderConfig:
    """Configuration for a single data provider.

    This dataclass allows creating providers from a configuration dict,
    supporting provider-specific settings like API keys and RPC URLs.

    Attributes:
        provider_type: Provider class name ("chainlink", "coingecko", "twap")
        chain: Blockchain network identifier (default: "arbitrum")
        rpc_url: RPC endpoint URL (for on-chain providers like Chainlink)
        api_key: API key (for API providers like CoinGecko)
        cache_ttl_seconds: Cache TTL in seconds (default: 60)
        priority: Provider priority (lower = higher priority)
        extra: Additional provider-specific configuration

    Example:
        # Chainlink provider config
        chainlink_config = ProviderConfig(
            provider_type="chainlink",
            chain="arbitrum",
            rpc_url="https://arb-mainnet.g.alchemy.com/v2/...",
            cache_ttl_seconds=120,
        )

        # CoinGecko provider config
        coingecko_config = ProviderConfig(
            provider_type="coingecko",
            api_key="CG-xxx...",
        )
    """

    provider_type: str
    chain: str = "arbitrum"
    rpc_url: str = ""
    api_key: str = ""
    cache_ttl_seconds: int = 60
    priority: int | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "provider_type": self.provider_type,
            "chain": self.chain,
            "rpc_url": self.rpc_url if self.rpc_url else None,
            "api_key": "***" if self.api_key else None,  # Mask API key
            "cache_ttl_seconds": self.cache_ttl_seconds,
            "priority": self.priority,
            "extra": self.extra,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProviderConfig":
        """Create from dictionary."""
        return cls(
            provider_type=data.get("provider_type", ""),
            chain=data.get("chain", "arbitrum"),
            rpc_url=data.get("rpc_url", ""),
            api_key=data.get("api_key", ""),
            cache_ttl_seconds=data.get("cache_ttl_seconds", 60),
            priority=data.get("priority"),
            extra=data.get("extra", {}),
        )


@dataclass
class PriceData:
    """Price result with data source tracking.

    This is a simplified structure for returning price data with source
    information, useful for logging and auditing which provider served
    each price request.

    Attributes:
        price: The fetched price in USD
        data_source: Name of the provider that returned this price
        timestamp: When the price was fetched/valid for
        is_stale: Whether the data source marked this as stale
    """

    price: Decimal
    data_source: str
    timestamp: datetime
    is_stale: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "price": str(self.price),
            "data_source": self.data_source,
            "timestamp": self.timestamp.isoformat(),
            "is_stale": self.is_stale,
        }


@dataclass
class PriceWithSource:
    """A price result with source attribution.

    Attributes:
        price: The fetched price in USD
        source: Name of the provider that returned this price
        timestamp: When the price was fetched/valid for
        is_stale: Whether the data source marked this as stale
        confidence: DataConfidence level for this price
        source_info: Full DataSourceInfo for detailed tracking
        metadata: Additional data from the provider
    """

    price: Decimal
    source: str
    timestamp: datetime
    is_stale: bool = False
    confidence: DataConfidence = DataConfidence.MEDIUM
    source_info: DataSourceInfo | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "price": str(self.price),
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "is_stale": self.is_stale,
            "confidence": self.confidence.value,
            "metadata": self.metadata,
        }


@dataclass
class FallbackStats:
    """Statistics about provider fallback usage.

    Attributes:
        total_requests: Total number of price requests
        provider_hits: Count of successful fetches per provider
        provider_failures: Count of failures per provider
        fallback_count: Number of times fallback was triggered
    """

    total_requests: int = 0
    provider_hits: dict[str, int] = field(default_factory=dict)
    provider_failures: dict[str, int] = field(default_factory=dict)
    fallback_count: int = 0

    def record_success(self, provider_name: str) -> None:
        """Record a successful fetch from a provider."""
        self.total_requests += 1
        self.provider_hits[provider_name] = self.provider_hits.get(provider_name, 0) + 1

    def record_failure(self, provider_name: str) -> None:
        """Record a failed fetch from a provider."""
        self.provider_failures[provider_name] = self.provider_failures.get(provider_name, 0) + 1

    def record_fallback(self) -> None:
        """Record that a fallback was triggered."""
        self.fallback_count += 1

    def get_hit_rate(self, provider_name: str) -> float:
        """Get the hit rate for a specific provider."""
        hits = self.provider_hits.get(provider_name, 0)
        failures = self.provider_failures.get(provider_name, 0)
        total = hits + failures
        return hits / total if total > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "total_requests": self.total_requests,
            "provider_hits": self.provider_hits.copy(),
            "provider_failures": self.provider_failures.copy(),
            "fallback_count": self.fallback_count,
        }


class AggregatedDataProvider:
    """Aggregated data provider with automatic fallback support.

    This provider wraps multiple HistoricalDataProvider implementations and
    automatically falls back to lower-priority providers when the primary
    provider fails or returns no data.

    The provider list is ordered by priority - the first provider in the list
    is tried first, and if it fails, the next one is tried, and so on.

    Attributes:
        providers: List of data provider instances in priority order
        provider_names: Names for each provider (for logging and data_source)
        stats: Statistics about provider usage and fallbacks

    Example:
        # Create with explicit providers
        aggregated = AggregatedDataProvider(
            providers=[chainlink_provider, twap_provider, coingecko_provider],
            provider_names=["chainlink", "twap", "coingecko"],
        )

        # Fetch price - tries providers in order
        price = await aggregated.get_price("ETH", datetime.now())

        # Fetch with source tracking
        result = await aggregated.get_price_with_source("ETH", datetime.now())
        print(f"Got ${result.price} from {result.source}")
    """

    # Default priority for this provider (when used in registry)
    DEFAULT_PRIORITY = 5  # Highest priority since it wraps others

    def __init__(
        self,
        providers: list[Any],
        provider_names: list[str] | None = None,
        chain: str = "arbitrum",
    ) -> None:
        """Initialize the aggregated data provider.

        Args:
            providers: List of HistoricalDataProvider instances in priority order.
                       The first provider is tried first.
            provider_names: Optional list of names for each provider. If not provided,
                           uses provider_name property or index.
            chain: Default chain identifier (used for properties).

        Raises:
            ValueError: If providers list is empty or names don't match providers.
        """
        if not providers:
            raise ValueError("At least one provider is required")

        self._providers = providers
        self._chain = chain.lower()

        # Resolve provider names
        if provider_names is not None:
            if len(provider_names) != len(providers):
                raise ValueError(
                    f"Number of names ({len(provider_names)}) must match number of providers ({len(providers)})"
                )
            self._provider_names = provider_names
        else:
            # Try to get names from provider_name property, fall back to index
            self._provider_names = []
            for i, provider in enumerate(providers):
                if hasattr(provider, "provider_name"):
                    self._provider_names.append(provider.provider_name)
                else:
                    self._provider_names.append(f"provider_{i}")

        # Track statistics
        self._stats = FallbackStats()

        logger.info(
            "Initialized AggregatedDataProvider with %d providers: %s",
            len(self._providers),
            self._provider_names,
        )

    @classmethod
    def create_from_config(
        cls,
        configs: list[ProviderConfig],
        chain: str = "arbitrum",
    ) -> "AggregatedDataProvider":
        """Create an AggregatedDataProvider from configuration objects.

        This factory method creates and configures multiple data providers
        from a list of ProviderConfig objects, handling provider-specific
        settings like API keys and RPC URLs.

        Args:
            configs: List of ProviderConfig objects in priority order.
                    The first config creates the highest-priority provider.
            chain: Default chain identifier (used if not specified in configs).

        Returns:
            Configured AggregatedDataProvider instance.

        Raises:
            ValueError: If configs is empty or a provider type is unknown.

        Example:
            configs = [
                ProviderConfig(
                    provider_type="chainlink",
                    chain="arbitrum",
                    rpc_url="https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY",
                ),
                ProviderConfig(
                    provider_type="coingecko",
                    api_key="CG-xxx...",
                ),
            ]
            provider = AggregatedDataProvider.create_from_config(configs)
        """
        if not configs:
            raise ValueError("At least one provider config is required")

        providers: list[Any] = []
        provider_names: list[str] = []

        for config in configs:
            provider_type = config.provider_type.lower()
            provider_chain = config.chain or chain

            if provider_type == "chainlink":
                # Lazy import to avoid circular dependencies
                from .chainlink import ChainlinkDataProvider

                provider: Any = ChainlinkDataProvider(
                    chain=provider_chain,
                    rpc_url=config.rpc_url,
                    cache_ttl_seconds=config.cache_ttl_seconds,
                    priority=config.priority,
                )
                providers.append(provider)
                provider_names.append("chainlink")

            elif provider_type == "coingecko":
                from .coingecko import CoinGeckoDataProvider

                provider = CoinGeckoDataProvider(
                    api_key=config.api_key,
                    **config.extra,
                )
                providers.append(provider)
                provider_names.append("coingecko")

            elif provider_type == "twap" or provider_type == "dex_twap":
                # Lazy import DEX TWAP provider
                from almanak.framework.data.price.dex_twap import DEXTWAPDataProvider

                provider = DEXTWAPDataProvider(
                    chain=provider_chain,
                    rpc_url=config.rpc_url,
                    cache_ttl_seconds=config.cache_ttl_seconds,
                    **config.extra,
                )
                providers.append(provider)
                provider_names.append("twap")

            else:
                raise ValueError(
                    f"Unknown provider type: {config.provider_type}. Supported types: chainlink, coingecko, twap"
                )

            logger.debug(
                "Created %s provider for chain %s (config: %s)",
                provider_type,
                provider_chain,
                config.to_dict(),
            )

        return cls(providers=providers, provider_names=provider_names, chain=chain)

    @classmethod
    async def create_with_data_config(
        cls,
        data_config: "BacktestDataConfig",
        chain: str = "arbitrum",
        rpc_url: str | None = None,
    ) -> "AggregatedDataProvider":
        """Create an AggregatedDataProvider from BacktestDataConfig.

        This factory method creates a provider configured based on the
        BacktestDataConfig.price_provider setting:
        - 'auto': Uses fallback chain Chainlink -> TWAP -> CoinGecko
        - 'chainlink': Uses Chainlink only
        - 'twap': Uses TWAP only
        - 'coingecko': Uses CoinGecko only

        Args:
            data_config: BacktestDataConfig with price_provider setting.
            chain: Blockchain network identifier (default: "arbitrum").
            rpc_url: Optional RPC URL for on-chain providers. If not provided,
                    will attempt to use ARCHIVE_RPC_URL_{CHAIN} env var.

        Returns:
            Configured AggregatedDataProvider instance.

        Raises:
            ValueError: If price_provider mode is invalid.

        Example:
            from almanak.framework.backtesting.config import BacktestDataConfig

            # Auto mode - tries Chainlink first, then TWAP, then CoinGecko
            config = BacktestDataConfig(price_provider="auto")
            provider = await AggregatedDataProvider.create_with_data_config(config)

            # Single provider mode
            config = BacktestDataConfig(price_provider="chainlink")
            provider = await AggregatedDataProvider.create_with_data_config(config)
        """
        import os

        providers: list[Any] = []
        provider_names: list[str] = []

        mode = data_config.price_provider
        chain_upper = chain.upper()

        # Resolve RPC URL from environment if not provided
        if rpc_url is None:
            rpc_url = os.environ.get(f"ARCHIVE_RPC_URL_{chain_upper}", "")

        logger.info(
            "Creating AggregatedDataProvider with mode=%s, chain=%s",
            mode,
            chain,
        )

        if mode == "auto":
            # Fallback chain: Chainlink -> TWAP -> CoinGecko
            providers, provider_names = await cls._create_fallback_chain(
                chain=chain,
                rpc_url=rpc_url,
                data_config=data_config,
            )
        elif mode == "chainlink":
            provider = await cls._create_chainlink_provider(chain, rpc_url)
            if provider:
                providers.append(provider)
                provider_names.append("chainlink")
        elif mode == "twap":
            provider = await cls._create_twap_provider(chain, rpc_url)
            if provider:
                providers.append(provider)
                provider_names.append("twap")
        elif mode == "coingecko":
            provider = await cls._create_coingecko_provider(data_config)
            if provider:
                providers.append(provider)
                provider_names.append("coingecko")
        else:
            raise ValueError(f"Invalid price_provider mode: {mode}. Supported modes: auto, chainlink, twap, coingecko")

        if not providers:
            raise ValueError(
                f"Failed to create any providers for mode={mode}, chain={chain}. "
                "Check that required environment variables are set."
            )

        return cls(providers=providers, provider_names=provider_names, chain=chain)

    @classmethod
    async def _create_fallback_chain(
        cls,
        chain: str,
        rpc_url: str,
        data_config: "BacktestDataConfig",
    ) -> tuple[list[Any], list[str]]:
        """Create the deterministic fallback chain: Chainlink -> TWAP -> CoinGecko.

        Returns:
            Tuple of (providers list, provider_names list).
        """
        providers: list[Any] = []
        provider_names: list[str] = []

        # 1. Try to create Chainlink provider (highest priority)
        chainlink = await cls._create_chainlink_provider(chain, rpc_url)
        if chainlink:
            providers.append(chainlink)
            provider_names.append("chainlink")
            logger.info("Added Chainlink provider to fallback chain (priority 1)")
        else:
            logger.warning(
                "Chainlink provider unavailable for chain=%s, skipping in fallback chain",
                chain,
            )

        # 2. Try to create TWAP provider (second priority)
        twap = await cls._create_twap_provider(chain, rpc_url)
        if twap:
            providers.append(twap)
            provider_names.append("twap")
            logger.info("Added TWAP provider to fallback chain (priority 2)")
        else:
            logger.warning(
                "TWAP provider unavailable for chain=%s, skipping in fallback chain",
                chain,
            )

        # 3. Create CoinGecko provider (lowest priority, always available)
        coingecko = await cls._create_coingecko_provider(data_config)
        if coingecko:
            providers.append(coingecko)
            provider_names.append("coingecko")
            logger.info("Added CoinGecko provider to fallback chain (priority 3)")

        return providers, provider_names

    @classmethod
    async def _create_chainlink_provider(
        cls,
        chain: str,
        rpc_url: str,
    ) -> Any | None:
        """Create a ChainlinkDataProvider if RPC URL is available.

        Returns:
            ChainlinkDataProvider instance or None if unavailable.
        """
        if not rpc_url:
            logger.debug("No RPC URL available for Chainlink provider")
            return None

        try:
            from .chainlink import ChainlinkDataProvider

            provider = ChainlinkDataProvider(
                chain=chain,
                rpc_url=rpc_url,
                cache_ttl_seconds=120,  # Longer cache for historical data
            )
            logger.debug("Created Chainlink provider for chain=%s", chain)
            return provider
        except Exception as e:
            logger.warning("Failed to create Chainlink provider: %s", e)
            return None

    @classmethod
    async def _create_twap_provider(
        cls,
        chain: str,
        rpc_url: str,
    ) -> Any | None:
        """Create a TWAPDataProvider if RPC URL is available.

        Returns:
            TWAPDataProvider instance or None if unavailable.
        """
        if not rpc_url:
            logger.debug("No RPC URL available for TWAP provider")
            return None

        try:
            from .twap import TWAPDataProvider

            provider = TWAPDataProvider(
                chain=chain,
                rpc_url=rpc_url,
                cache_ttl_seconds=120,  # Longer cache for historical data
            )
            logger.debug("Created TWAP provider for chain=%s", chain)
            return provider
        except Exception as e:
            logger.warning("Failed to create TWAP provider: %s", e)
            return None

    @classmethod
    async def _create_coingecko_provider(
        cls,
        data_config: "BacktestDataConfig",
    ) -> Any | None:
        """Create a CoinGeckoDataProvider with rate limiting from config.

        Returns:
            CoinGeckoDataProvider instance or None if creation fails.
        """
        try:
            from .coingecko import CoinGeckoDataProvider

            provider = CoinGeckoDataProvider(
                data_config=data_config,  # Passes rate limit settings
            )
            logger.debug(
                "Created CoinGecko provider with rate_limit=%d/min",
                data_config.coingecko_rate_limit_per_minute,
            )
            return provider
        except Exception as e:
            logger.warning("Failed to create CoinGecko provider: %s", e)
            return None

    def get_provider_confidence(self, provider_name: str) -> DataConfidence:
        """Get the confidence level for a provider.

        Args:
            provider_name: Name of the provider (e.g., "chainlink", "twap", "coingecko")

        Returns:
            DataConfidence level for the provider.
        """
        return PROVIDER_CONFIDENCE_MAP.get(
            provider_name.lower(),
            DEFAULT_PROVIDER_CONFIDENCE,
        )

    @property
    def providers(self) -> list[Any]:
        """Get the list of providers in priority order."""
        return self._providers.copy()

    @property
    def provider_names(self) -> list[str]:
        """Get the list of provider names in priority order."""
        return self._provider_names.copy()

    @property
    def stats(self) -> FallbackStats:
        """Get fallback statistics."""
        return self._stats

    def reset_stats(self) -> None:
        """Reset fallback statistics."""
        self._stats = FallbackStats()

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        """Get the price of a token at a specific timestamp.

        Tries each provider in priority order until one succeeds.

        Args:
            token: Token symbol (e.g., "ETH", "WETH", "ARB")
            timestamp: The historical point in time

        Returns:
            Price in USD at the specified timestamp

        Raises:
            ValueError: If all providers fail to return a price
        """
        result = await self.get_price_with_source(token, timestamp)
        return result.price

    async def get_price_with_source(self, token: str, timestamp: datetime) -> PriceWithSource:
        """Get the price of a token with source attribution and confidence tracking.

        Tries each provider in priority order until one succeeds.
        Returns the price along with which provider returned it and confidence level.

        The confidence level is determined by the provider type:
        - Chainlink: HIGH (on-chain oracle)
        - TWAP: HIGH (on-chain DEX oracle)
        - CoinGecko: MEDIUM (off-chain API)
        - Unknown: LOW

        Args:
            token: Token symbol (e.g., "ETH", "WETH", "ARB")
            timestamp: The historical point in time

        Returns:
            PriceWithSource containing the price, source provider name,
            confidence level, and DataSourceInfo

        Raises:
            ValueError: If all providers fail to return a price
        """
        errors: list[str] = []
        is_first_provider = True

        for i, provider in enumerate(self._providers):
            provider_name = self._provider_names[i]

            try:
                price = await provider.get_price(token, timestamp)

                # Check if price is valid
                if price is None or price <= 0:
                    raise ValueError(f"Invalid price returned: {price}")

                # Record success
                self._stats.record_success(provider_name)

                if not is_first_provider:
                    self._stats.record_fallback()

                # Check if provider marked data as stale (if it has such capability)
                is_stale = False
                if hasattr(provider, "is_data_stale"):
                    is_stale = provider.is_data_stale(token)

                # Get confidence level based on provider
                confidence = self.get_provider_confidence(provider_name)

                # Create DataSourceInfo for detailed tracking
                source_info = DataSourceInfo(
                    source=provider_name,
                    confidence=confidence,
                    timestamp=timestamp,
                )

                # Log which provider was used with confidence level
                if not is_first_provider:
                    logger.info(
                        "Price for %s at %s: $%s (fallback to %s, confidence=%s%s)",
                        token,
                        timestamp,
                        price,
                        provider_name,
                        confidence.value,
                        ", stale" if is_stale else "",
                    )
                else:
                    logger.debug(
                        "Price for %s at %s: $%s (from %s, confidence=%s%s)",
                        token,
                        timestamp,
                        price,
                        provider_name,
                        confidence.value,
                        ", stale" if is_stale else "",
                    )

                return PriceWithSource(
                    price=price,
                    source=provider_name,
                    timestamp=timestamp,
                    is_stale=is_stale,
                    confidence=confidence,
                    source_info=source_info,
                )

            except Exception as e:
                self._stats.record_failure(provider_name)
                errors.append(f"{provider_name}: {e!s}")
                logger.debug(
                    "Provider %s failed for %s at %s: %s",
                    provider_name,
                    token,
                    timestamp,
                    e,
                )
                is_first_provider = False
                continue

        # All providers failed
        error_details = "; ".join(errors)
        raise ValueError(f"All providers failed to get price for {token} at {timestamp}: {error_details}")

    async def get_price_data(self, token: str, timestamp: datetime) -> PriceData:
        """Get price data with data_source field for tracking.

        This method is similar to get_price_with_source() but returns a
        PriceData object with a `data_source` field instead of `source`,
        which is commonly expected in backtesting result structures.

        Args:
            token: Token symbol (e.g., "ETH", "WETH", "ARB")
            timestamp: The historical point in time

        Returns:
            PriceData containing price and data_source

        Raises:
            ValueError: If all providers fail to return a price

        Example:
            result = await aggregated.get_price_data("ETH", datetime.now())
            print(f"Price: ${result.price}, Source: {result.data_source}")
        """
        result = await self.get_price_with_source(token, timestamp)
        return PriceData(
            price=result.price,
            data_source=result.source,
            timestamp=result.timestamp,
            is_stale=result.is_stale,
        )

    async def get_ohlcv(
        self,
        token: str,
        start: datetime,
        end: datetime,
        interval_seconds: int = 3600,
    ) -> list[OHLCV]:
        """Get OHLCV data for a token over a time range.

        Tries each provider in priority order until one succeeds.

        Args:
            token: Token symbol (e.g., "ETH", "WETH", "ARB")
            start: Start of the time range (inclusive)
            end: End of the time range (inclusive)
            interval_seconds: Candle interval in seconds (default: 3600 = 1 hour)

        Returns:
            List of OHLCV data points, sorted by timestamp ascending

        Raises:
            ValueError: If all providers fail to return data
        """
        errors: list[str] = []

        for i, provider in enumerate(self._providers):
            provider_name = self._provider_names[i]

            try:
                if not hasattr(provider, "get_ohlcv"):
                    errors.append(f"{provider_name}: no get_ohlcv method")
                    continue

                ohlcv = await provider.get_ohlcv(token, start, end, interval_seconds)

                if not ohlcv:
                    raise ValueError("Empty OHLCV data returned")

                logger.debug(
                    "OHLCV for %s (%s to %s): %d candles (from %s)",
                    token,
                    start,
                    end,
                    len(ohlcv),
                    provider_name,
                )

                return ohlcv

            except Exception as e:
                errors.append(f"{provider_name}: {e!s}")
                logger.debug(
                    "Provider %s failed for OHLCV %s: %s",
                    provider_name,
                    token,
                    e,
                )
                continue

        # All providers failed
        error_details = "; ".join(errors)
        raise ValueError(f"All providers failed to get OHLCV for {token}: {error_details}")

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Iterate through historical market states.

        Uses the first provider that successfully provides data for iteration.
        Falls back to manual iteration with get_price() if no provider
        supports full iteration.

        Args:
            config: Configuration specifying time range, interval, and tokens

        Yields:
            Tuples of (timestamp, MarketState) for each time point
        """
        # Try to find a provider that supports iterate()
        for i, provider in enumerate(self._providers):
            provider_name = self._provider_names[i]

            if not hasattr(provider, "iterate"):
                continue

            try:
                logger.info(
                    "Starting iteration with %s from %s to %s",
                    provider_name,
                    config.start_time,
                    config.end_time,
                )

                async for timestamp, market_state in provider.iterate(config):
                    yield (timestamp, market_state)

                return

            except Exception as e:
                logger.warning(
                    "Provider %s iterate() failed: %s, trying next provider",
                    provider_name,
                    e,
                )
                continue

        # Fall back to manual iteration using get_price()
        logger.info("No provider supports iterate(), falling back to manual iteration")

        current_time = config.start_time
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=UTC)

        end_time = config.end_time
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=UTC)

        interval = timedelta(seconds=config.interval_seconds)

        while current_time <= end_time:
            prices: dict[str, Decimal] = {}

            for token in config.tokens:
                try:
                    price = await self.get_price(token, current_time)
                    prices[token.upper()] = price
                except Exception as e:
                    logger.warning(
                        "Failed to get price for %s at %s: %s",
                        token,
                        current_time,
                        e,
                    )

            market_state = MarketState(
                timestamp=current_time,
                prices=prices,
                ohlcv={},
                chain=config.chains[0] if config.chains else "arbitrum",
            )

            yield (current_time, market_state)

            current_time += interval

    async def close(self) -> None:
        """Close all underlying providers."""
        for i, provider in enumerate(self._providers):
            if hasattr(provider, "close"):
                try:
                    await provider.close()
                except Exception as e:
                    logger.warning(
                        "Error closing provider %s: %s",
                        self._provider_names[i],
                        e,
                    )

    @property
    def provider_name(self) -> str:
        """Return the unique name of this data provider."""
        return "aggregated"

    @property
    def supported_tokens(self) -> list[str]:
        """Return list of supported token symbols.

        Combines supported tokens from all providers (union).
        """
        tokens: set[str] = set()
        for provider in self._providers:
            if hasattr(provider, "supported_tokens"):
                tokens.update(provider.supported_tokens)
        return sorted(tokens)

    @property
    def supported_chains(self) -> list[str]:
        """Return list of supported chain identifiers.

        Combines supported chains from all providers (union).
        """
        chains: set[str] = set()
        for provider in self._providers:
            if hasattr(provider, "supported_chains"):
                chains.update(provider.supported_chains)
        return sorted(chains)

    @property
    def min_timestamp(self) -> datetime | None:
        """Return the earliest timestamp with available data.

        Returns the earliest min_timestamp from all providers.
        """
        min_ts: datetime | None = None
        for provider in self._providers:
            if hasattr(provider, "min_timestamp"):
                provider_min = provider.min_timestamp
                if provider_min is not None:
                    if min_ts is None or provider_min < min_ts:
                        min_ts = provider_min
        return min_ts

    @property
    def max_timestamp(self) -> datetime | None:
        """Return the latest timestamp with available data.

        Returns the latest max_timestamp from all providers.
        """
        max_ts: datetime | None = None
        for provider in self._providers:
            if hasattr(provider, "max_timestamp"):
                provider_max = provider.max_timestamp
                if provider_max is not None:
                    if max_ts is None or provider_max > max_ts:
                        max_ts = provider_max
        return max_ts

    async def __aenter__(self) -> "AggregatedDataProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


__all__ = [
    "AggregatedDataProvider",
    "PriceWithSource",
    "PriceData",
    "ProviderConfig",
    "FallbackStats",
    "PROVIDER_CONFIDENCE_MAP",
    "DEFAULT_PROVIDER_CONFIDENCE",
]
