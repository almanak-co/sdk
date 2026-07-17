"""Historical data providers for PnL backtesting.

This package contains concrete implementations of the HistoricalDataProvider
protocol for different data sources, plus a registry for provider discovery.

Available Providers:
    - CoinGeckoDataProvider: Historical price data from CoinGecko API
    - ChainlinkDataProvider: On-chain price data from Chainlink oracles
    - TWAPDataProvider: DEX TWAP prices from Uniswap V3 pool oracles
    - AggregatedDataProvider: Multi-provider with automatic fallback support
    - EtherscanGasPriceProvider: Historical gas prices from Etherscan API

Provider Registry:
    - ProviderRegistry: Centralized registry for provider discovery and selection
    - ProviderMetadata: Metadata about registered providers

Example:
    from almanak.framework.backtesting.pnl.providers import (
        CoinGeckoDataProvider,
        ChainlinkDataProvider,
        AggregatedDataProvider,
        ProviderRegistry,
    )
    from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig
    from datetime import datetime

    # Use registry to get best provider for a chain
    best = ProviderRegistry.get_best_provider(chain="arbitrum")
    if best:
        provider = best.provider_class(chain="arbitrum")

    # Or directly instantiate a specific provider
    chainlink_provider = ChainlinkDataProvider(
        chain="arbitrum",
        cache_ttl_seconds=120,  # 2 minute cache
    )

    # Or use AggregatedDataProvider for automatic fallback
    coingecko_provider = CoinGeckoDataProvider()
    aggregated = AggregatedDataProvider(
        providers=[chainlink_provider, coingecko_provider],
    )

    config = HistoricalDataConfig(
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 6, 1),
        interval_seconds=3600,
        tokens=["WETH", "USDC"],
    )

    async for timestamp, market_state in provider.iterate(config):
        eth_price = market_state.get_price("WETH")
        # ... process market state
"""

from .aggregated import (
    AggregatedDataProvider,
    FallbackStats,
    PriceData,
    PriceWithSource,
    ProviderConfig,
)
from .base import (
    BacktestProviderConfig,
    HistoricalAPYProvider,
    HistoricalFundingProvider,
    HistoricalLiquidityProvider,
    HistoricalVolumeProvider,
)
from .benchmark import (
    DEFAULT_BENCHMARK,
    DEFI_INDEX_WEIGHTS,
    Benchmark,
    BenchmarkPricePoint,
    BenchmarkReturn,
    get_benchmark_price_series,
    get_benchmark_returns,
    get_benchmark_total_return,
)
from .chainlink import (
    ARCHIVE_RPC_CHAINS,
    ARCHIVE_RPC_URL_ENV_PATTERN,
    DATA_STALENESS_THRESHOLD_SECONDS,
    MAX_BINARY_SEARCH_ITERATIONS,
    CachedPrice,
    ChainlinkDataProvider,
    ChainlinkPriceResult,
    ChainlinkStaleDataError,
    PersistentCacheConfig,
    PriceCache,
)
from .coingecko import CoinGeckoDataProvider
from .data_validation import (
    DataQualityIssue,
    DataQualityIssueType,
    DataQualityResult,
    DataQualitySeverity,
    validate_ohlcv_data,
    validate_price_data,
)
from .funding_rates import (
    DEFAULT_FUNDING_RATE,
    CachedFundingRate,
    FundingRateData,
    FundingRateError,
    FundingRateNotFoundError,
    FundingRateProvider,
    FundingRateRateLimitError,
    UnsupportedProtocolError,
)
from .funding_rates import (
    RateLimitState as FundingRateLimitState,
)
from .gas import (
    DEFAULT_GAS_PRICES,
    ETHERSCAN_API_KEY_ENV_VARS,
    ETHERSCAN_API_URLS,
    EtherscanGasPriceProvider,
    GasPrice,
    GasPriceCache,
    GasPriceProvider,
)
from .lending_apy import (
    CachedLendingAPY,
    LendingAPYData,
    LendingAPYError,
    LendingAPYNotFoundError,
    LendingAPYProvider,
    LendingAPYRateLimitError,
)
from .lending_apy import (
    RateLimitState as LendingAPYRateLimitState,
)
from .lending_apy import (
    UnsupportedProtocolError as LendingUnsupportedProtocolError,
)
from .liquidity_depth import (
    DATA_SOURCE_FALLBACK as LIQUIDITY_DATA_SOURCE_FALLBACK,
)
from .liquidity_depth import (
    DEFAULT_TWAP_WINDOW_HOURS,
    LiquidityDepthProvider,
)
from .multi_dex_volume import (
    MultiDEXVolumeProvider,
)
from .rate_limiter import (
    RateLimiterStats,
    TokenBucketRateLimiter,
    create_coingecko_rate_limiter,
)
from .registry import ProviderMetadata, ProviderRegistry
from .subgraph_client import (
    SubgraphQueryError,
    SubgraphRateLimitError,
)
from .twap import (
    CachedTWAP,
    TWAPDataProvider,
    TWAPInsufficientHistoryError,
    TWAPObservation,
    TWAPPoolNotFoundError,
    TWAPResult,
)

__all__ = [
    # Base Provider Interfaces
    "HistoricalVolumeProvider",
    "HistoricalFundingProvider",
    "HistoricalAPYProvider",
    "HistoricalLiquidityProvider",
    "BacktestProviderConfig",
    # Price Providers
    "CoinGeckoDataProvider",
    "ChainlinkDataProvider",
    "ChainlinkStaleDataError",
    "ChainlinkPriceResult",
    "PersistentCacheConfig",
    "ARCHIVE_RPC_URL_ENV_PATTERN",
    "ARCHIVE_RPC_CHAINS",
    "DATA_STALENESS_THRESHOLD_SECONDS",
    "MAX_BINARY_SEARCH_ITERATIONS",
    "TWAPDataProvider",
    "TWAPObservation",
    "TWAPResult",
    "CachedTWAP",
    "TWAPInsufficientHistoryError",
    "TWAPPoolNotFoundError",
    "AggregatedDataProvider",
    # Gas Price Providers
    "GasPriceProvider",
    "EtherscanGasPriceProvider",
    "GasPrice",
    "GasPriceCache",
    "ETHERSCAN_API_URLS",
    "ETHERSCAN_API_KEY_ENV_VARS",
    "DEFAULT_GAS_PRICES",
    # Caching
    "CachedPrice",
    "PriceCache",
    # Fallback support
    "PriceWithSource",
    "PriceData",
    "ProviderConfig",
    "FallbackStats",
    # Registry
    "ProviderRegistry",
    "ProviderMetadata",
    # Benchmark
    "Benchmark",
    "BenchmarkPricePoint",
    "BenchmarkReturn",
    "DEFAULT_BENCHMARK",
    "DEFI_INDEX_WEIGHTS",
    "get_benchmark_price_series",
    "get_benchmark_returns",
    "get_benchmark_total_return",
    # Subgraph exception taxonomy (canonical: subgraph_client)
    "SubgraphRateLimitError",
    "SubgraphQueryError",
    # Multi-DEX Volume Provider
    "MultiDEXVolumeProvider",
    # Funding Rate Provider
    "FundingRateProvider",
    "FundingRateData",
    "CachedFundingRate",
    "FundingRateLimitState",
    "FundingRateError",
    "FundingRateNotFoundError",
    "FundingRateRateLimitError",
    "UnsupportedProtocolError",
    "DEFAULT_FUNDING_RATE",
    # Lending APY Provider
    "LendingAPYProvider",
    "LendingAPYData",
    "CachedLendingAPY",
    "LendingAPYRateLimitState",
    "LendingAPYError",
    "LendingAPYNotFoundError",
    "LendingAPYRateLimitError",
    "LendingUnsupportedProtocolError",
    # Rate Limiter
    "TokenBucketRateLimiter",
    "RateLimiterStats",
    "create_coingecko_rate_limiter",
    # Data Validation
    "validate_price_data",
    "validate_ohlcv_data",
    "DataQualityResult",
    "DataQualityIssue",
    "DataQualityIssueType",
    "DataQualitySeverity",
    # Liquidity Depth Provider
    "LiquidityDepthProvider",
    "LIQUIDITY_DATA_SOURCE_FALLBACK",
    "DEFAULT_TWAP_WINDOW_HOURS",
]
