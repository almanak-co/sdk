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

from almanak.config.backtest import backtest_config_from_env
from almanak.core.chains import DEFAULT_CHAIN, LEGACY_SERIALIZED_CHAIN
from almanak.framework.data.interfaces import DataSourceUnavailable

from ..data_provider import (
    OHLCV,
    HistoricalDataConfig,
    MarketState,
    TokenRef,
    is_address_like,
    is_token_key,
    normalize_token_key,
    token_ref_display,
)
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


@dataclass(frozen=True)
class _ConfiguredProvider:
    """Provider created from a ProviderConfig entry."""

    provider: Any
    name: str
    provider_type: str
    chain: str


def _single_provider_lists(provider: Any | None, name: str) -> tuple[list[Any], list[str]]:
    """Return AggregatedDataProvider constructor lists for an optional provider."""
    if provider is None:
        return [], []
    return [provider], [name]


def _provider_timestamps(providers: list[Any], attribute: str) -> list[datetime]:
    """Read timestamp attributes once and keep measured non-empty values."""
    timestamps: list[datetime] = []
    for provider in providers:
        timestamp = getattr(provider, attribute, None)
        if timestamp is not None:
            timestamps.append(timestamp)
    return timestamps


def _masked_provider_config(config: "ProviderConfig") -> dict[str, Any]:
    """Return a log-safe provider config dictionary."""
    data = config.to_dict()
    if data.get("rpc_url"):
        data["rpc_url"] = "***"
    return data


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
    chain: str = DEFAULT_CHAIN
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
            chain=data.get("chain", LEGACY_SERIALIZED_CHAIN),
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
        chain: str = DEFAULT_CHAIN,
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
        chain: str = DEFAULT_CHAIN,
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

        configured_providers = [cls._create_provider_from_config(config, chain) for config in configs]
        providers = [configured.provider for configured in configured_providers]
        provider_names = [configured.name for configured in configured_providers]

        for config, configured in zip(configs, configured_providers, strict=True):
            logger.debug(
                "Created %s provider for chain %s (config: %s)",
                configured.provider_type,
                configured.chain,
                _masked_provider_config(config),
            )

        return cls(providers=providers, provider_names=provider_names, chain=chain)

    @classmethod
    def _create_provider_from_config(
        cls,
        config: ProviderConfig,
        default_chain: str,
    ) -> _ConfiguredProvider:
        """Create one provider from a ProviderConfig."""
        provider_type = config.provider_type.lower()
        provider_chain = config.chain or default_chain

        if provider_type == "chainlink":
            return _ConfiguredProvider(
                provider=cls._create_configured_chainlink_provider(config, provider_chain),
                name="chainlink",
                provider_type=provider_type,
                chain=provider_chain,
            )
        if provider_type == "coingecko":
            return _ConfiguredProvider(
                provider=cls._create_configured_coingecko_provider(config),
                name="coingecko",
                provider_type=provider_type,
                chain=provider_chain,
            )
        if provider_type in {"twap", "dex_twap"}:
            return _ConfiguredProvider(
                provider=cls._create_configured_twap_provider(config, provider_chain),
                name="twap",
                provider_type=provider_type,
                chain=provider_chain,
            )

        raise ValueError(f"Unknown provider type: {config.provider_type}. Supported types: chainlink, coingecko, twap")

    @staticmethod
    def _create_configured_chainlink_provider(config: ProviderConfig, chain: str) -> Any:
        """Create a Chainlink provider from ProviderConfig."""
        from .chainlink import ChainlinkDataProvider

        return ChainlinkDataProvider(
            chain=chain,
            rpc_url=config.rpc_url,
            cache_ttl_seconds=config.cache_ttl_seconds,
            priority=config.priority,
        )

    @staticmethod
    def _create_configured_coingecko_provider(config: ProviderConfig) -> Any:
        """Create a CoinGecko provider from ProviderConfig."""
        from .coingecko import CoinGeckoDataProvider

        return CoinGeckoDataProvider(
            api_key=config.api_key,
            **config.extra,
        )

    @staticmethod
    def _create_configured_twap_provider(config: ProviderConfig, chain: str) -> Any:
        """Create the gateway-backed TWAP provider from ProviderConfig."""
        from .twap import TWAPDataProvider

        extra = dict(config.extra)
        observation_window_seconds = extra.pop("observation_window_seconds", extra.pop("twap_window_seconds", None))
        if extra:
            logger.debug("Ignoring unsupported TWAP provider config keys: %s", sorted(extra))
        return TWAPDataProvider(
            chain=chain,
            rpc_url=config.rpc_url,
            observation_window_seconds=observation_window_seconds,
            cache_ttl_seconds=config.cache_ttl_seconds,
            priority=config.priority,
        )

    @classmethod
    async def create_with_data_config(
        cls,
        data_config: "BacktestDataConfig",
        chain: str = DEFAULT_CHAIN,
        rpc_url: str | None = None,
        token_addresses: dict[str, tuple[str, str]] | None = None,
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
            token_addresses: Optional SYMBOL_UPPER -> (chain, address) map threaded
                    into the CoinGecko provider so non-native ERC20s (LINK, UNI,
                    ...) keep their dynamic contract-address resolution route on
                    the 'auto' / 'coingecko' paths. Without it the CoinGecko leg
                    can only price natives, and the preflight guard blocks the
                    rest.

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
        mode = data_config.price_provider

        # Resolve RPC URL from typed backtest config if not provided.
        # Phase 5c: env reads centralised in
        # ``almanak.config.backtest.backtest_config_from_env``. We pass the
        # requested chain explicitly so non-default chains (e.g. ``bsc``,
        # not in ``DEFAULT_ARCHIVE_RPC_CHAINS``) still get their
        # ``ARCHIVE_RPC_URL_<CHAIN>`` env var read (PR #2152 review).
        rpc_url = cls._resolve_archive_rpc_url(chain=chain, rpc_url=rpc_url)

        logger.info(
            "Creating AggregatedDataProvider with mode=%s, chain=%s",
            mode,
            chain,
        )

        providers, provider_names = await cls._create_providers_for_data_mode(
            mode=mode,
            chain=chain,
            rpc_url=rpc_url,
            data_config=data_config,
            token_addresses=token_addresses,
        )

        if not providers:
            raise ValueError(
                f"Failed to create any providers for mode={mode}, chain={chain}. "
                "Check that required environment variables are set."
            )

        return cls(providers=providers, provider_names=provider_names, chain=chain)

    @staticmethod
    def _resolve_archive_rpc_url(chain: str, rpc_url: str | None) -> str:
        """Resolve the configured archive RPC URL for a chain."""
        if rpc_url is not None:
            return rpc_url
        chain_key = chain.lower()
        return backtest_config_from_env(archive_rpc_chains=(chain_key,)).archive_rpc_urls.get(chain_key, "")

    @classmethod
    async def _create_providers_for_data_mode(
        cls,
        *,
        mode: str,
        chain: str,
        rpc_url: str,
        data_config: "BacktestDataConfig",
        token_addresses: dict[str, tuple[str, str]] | None = None,
    ) -> tuple[list[Any], list[str]]:
        """Create providers for one BacktestDataConfig price-provider mode."""
        if mode == "auto":
            return await cls._create_fallback_chain(
                chain=chain,
                rpc_url=rpc_url,
                data_config=data_config,
                token_addresses=token_addresses,
            )
        if mode == "chainlink":
            provider = await cls._create_chainlink_provider(chain, rpc_url)
            return _single_provider_lists(provider, "chainlink")
        if mode == "twap":
            provider = await cls._create_twap_provider(chain, rpc_url)
            return _single_provider_lists(provider, "twap")
        if mode == "coingecko":
            provider = await cls._create_coingecko_provider(data_config, token_addresses=token_addresses)
            return _single_provider_lists(provider, "coingecko")

        raise ValueError(f"Invalid price_provider mode: {mode}. Supported modes: auto, chainlink, twap, coingecko")

    @classmethod
    async def _create_fallback_chain(
        cls,
        chain: str,
        rpc_url: str,
        data_config: "BacktestDataConfig",
        token_addresses: dict[str, tuple[str, str]] | None = None,
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
        coingecko = await cls._create_coingecko_provider(data_config, token_addresses=token_addresses)
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
        token_addresses: dict[str, tuple[str, str]] | None = None,
    ) -> Any | None:
        """Create a CoinGeckoDataProvider with rate limiting from config.

        Args:
            data_config: Backtest data config (rate-limit settings).
            token_addresses: Optional SYMBOL_UPPER -> (chain, address) map so the
                provider can resolve non-native ERC20 coin ids dynamically via
                the contract endpoint (else it can only price natives).

        Returns:
            CoinGeckoDataProvider instance or None if creation fails.
        """
        try:
            from .coingecko import CoinGeckoDataProvider

            provider = CoinGeckoDataProvider(
                data_config=data_config,  # Passes rate limit settings
                token_addresses=token_addresses,
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

    def register_token_addresses(self, token_addresses: dict[str, tuple[str, str]]) -> None:
        """Forward address registrations to every wrapped provider that accepts them.

        Only the CoinGecko leg consumes ``token_addresses``; the on-chain legs
        (Chainlink / TWAP) have no such map and are skipped. Lets the PnL engine
        register the numeraire's contract address on the ``auto`` / ``coingecko``
        fallback chains so a numeraire the strategy never trades is still priced
        via CoinGecko's contract endpoint (VIB-5127).
        """
        for provider in self._providers:
            register = getattr(provider, "register_token_addresses", None)
            if callable(register):
                register(token_addresses)

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

    def _market_state_key(self, token: TokenRef, default_chain: str | None = None) -> TokenRef:
        """Return the MarketState key used by manual iteration."""
        if is_token_key(token):
            return normalize_token_key(token[0], token[1])
        assert isinstance(token, str)
        if is_address_like(token):
            return normalize_token_key(default_chain or self._chain, token)
        return token.upper()

    async def get_price(self, token: TokenRef, timestamp: datetime) -> Decimal:
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

    async def get_price_with_source(self, token: TokenRef, timestamp: datetime) -> PriceWithSource:
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

    async def get_price_data(self, token: TokenRef, timestamp: datetime) -> PriceData:
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
        default_chain = config.chains[0] if config.chains else self._chain

        while current_time <= end_time:
            prices: dict[TokenRef, Decimal] = {}
            missing: list[str] = []

            for token in config.tokens:
                try:
                    price = await self.get_price(token, current_time)
                    prices[self._market_state_key(token, default_chain)] = price
                except Exception as e:
                    missing.append(f"{token_ref_display(token)} ({e})")
                    logger.warning(
                        "Failed to get price for %s at %s: %s",
                        token_ref_display(token),
                        current_time,
                        e,
                    )

            if missing:
                raise DataSourceUnavailable(
                    source="aggregated",
                    reason=(
                        f"Manual iteration could not price all requested tokens at {current_time.isoformat()}: "
                        + "; ".join(missing)
                    ),
                )

            market_state = MarketState(
                timestamp=current_time,
                prices=prices,
                ohlcv={},
                chain=config.chains[0] if config.chains else DEFAULT_CHAIN,
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
        return min(_provider_timestamps(self._providers, "min_timestamp"), default=None)

    @property
    def max_timestamp(self) -> datetime | None:
        """Return the latest timestamp with available data.

        Returns the latest max_timestamp from all providers.
        """
        return max(_provider_timestamps(self._providers, "max_timestamp"), default=None)

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
