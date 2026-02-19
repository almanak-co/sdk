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
from .circuit_breaker import (
    CHAINLINK_CONFIG,
    COINGECKO_CONFIG,
    ETHERSCAN_CONFIG,
    GMX_API_CONFIG,
    HYPERLIQUID_CONFIG,
    RPC_CONFIG,
    SUBGRAPH_CONFIG,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerError,
    CircuitBreakerMetrics,
    CircuitBreakerOpenError,
    CircuitBreakerRegistry,
    CircuitBreakerState,
    create_chainlink_circuit_breaker,
    create_coingecko_circuit_breaker,
    create_etherscan_circuit_breaker,
    create_gmx_circuit_breaker,
    create_hyperliquid_circuit_breaker,
    create_rpc_circuit_breaker,
    create_subgraph_circuit_breaker,
    get_all_circuit_breaker_metrics,
    get_circuit_breaker,
    get_open_circuits,
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
    DEFAULT_FUNDING_RATES,
    GMX_MARKETS,
    HYPERLIQUID_MARKETS,
    CachedFundingRate,
    FundingRateData,
    FundingRateError,
    FundingRateNotFoundError,
    FundingRateProvider,
    FundingRateRateLimitError,
    UnsupportedProtocolError,
)
from .funding_rates import (
    SUPPORTED_PROTOCOLS as FUNDING_SUPPORTED_PROTOCOLS,
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
from .lending import (
    AAVE_V3_DATA_SOURCE,
    AAVE_V3_SUBGRAPH_IDS,
    AAVE_V3_SUPPORTED_CHAINS,
    COMPOUND_V3_DATA_SOURCE,
    COMPOUND_V3_SUBGRAPH_IDS,
    COMPOUND_V3_SUPPORTED_CHAINS,
    MORPHO_BLUE_DATA_SOURCE,
    MORPHO_BLUE_SUBGRAPH_IDS,
    MORPHO_BLUE_SUPPORTED_CHAINS,
    SPARK_DATA_SOURCE,
    SPARK_SUBGRAPH_IDS,
    SPARK_SUPPORTED_CHAINS,
    AaveV3APYProvider,
    CompoundV3APYProvider,
    MorphoBlueAPYProvider,
    SparkAPYProvider,
)
from .lending_apy import (
    AAVE_V3_MARKETS,
    COMPOUND_V3_MARKETS,
    DEFAULT_BORROW_APYS,
    DEFAULT_SUPPLY_APYS,
    CachedLendingAPY,
    LendingAPYData,
    LendingAPYError,
    LendingAPYNotFoundError,
    LendingAPYProvider,
    LendingAPYRateLimitError,
)
from .lending_apy import (
    SUPPORTED_PROTOCOLS as LENDING_SUPPORTED_PROTOCOLS,
)
from .lending_apy import (
    RateLimitState as LendingAPYRateLimitState,
)
from .lending_apy import (
    UnsupportedProtocolError as LendingUnsupportedProtocolError,
)
from .liquidity_depth import (
    DATA_SOURCE_AERODROME as LIQUIDITY_DATA_SOURCE_AERODROME,
)
from .liquidity_depth import (
    DATA_SOURCE_BALANCER as LIQUIDITY_DATA_SOURCE_BALANCER,
)
from .liquidity_depth import (
    DATA_SOURCE_CURVE as LIQUIDITY_DATA_SOURCE_CURVE,
)
from .liquidity_depth import (
    DATA_SOURCE_FALLBACK as LIQUIDITY_DATA_SOURCE_FALLBACK,
)
from .liquidity_depth import (
    DATA_SOURCE_PANCAKESWAP_V3 as LIQUIDITY_DATA_SOURCE_PANCAKESWAP_V3,
)
from .liquidity_depth import (
    DATA_SOURCE_SUSHISWAP_V3 as LIQUIDITY_DATA_SOURCE_SUSHISWAP_V3,
)
from .liquidity_depth import (
    DATA_SOURCE_TRADERJOE_V2 as LIQUIDITY_DATA_SOURCE_TRADERJOE_V2,
)
from .liquidity_depth import (
    DATA_SOURCE_UNISWAP_V3 as LIQUIDITY_DATA_SOURCE_UNISWAP_V3,
)
from .liquidity_depth import (
    DEFAULT_TWAP_WINDOW_HOURS,
    LIQUIDITY_BOOK_PROTOCOLS,
    STABLESWAP_PROTOCOLS,
    V2_PROTOCOLS,
    V3_PROTOCOLS,
    WEIGHTED_POOL_PROTOCOLS,
    LiquidityDepthProvider,
)
from .liquidity_depth import (
    SUPPORTED_CHAINS as LIQUIDITY_SUPPORTED_CHAINS,
)
from .multi_dex_volume import (
    PROTOCOL_CHAIN_SUPPORT,
    PROTOCOL_PROVIDER_MAP,
    STRING_PROTOCOL_MAP,
    MultiDEXVolumeProvider,
)
from .perp import (
    GMX_API_URLS,
    GMX_DATA_SOURCE,
    GMX_MARKET_TOKENS,
    GMX_SUPPORTED_CHAINS,
    HYPERLIQUID_API_URL,
    HYPERLIQUID_DATA_SOURCE,
    HYPERLIQUID_MAX_HOURS_PER_REQUEST,
    GMXFundingProvider,
    HyperliquidFundingProvider,
)
from .rate_limiter import (
    RateLimiterStats,
    TokenBucketRateLimiter,
    create_coingecko_rate_limiter,
)
from .registry import ProviderMetadata, ProviderRegistry
from .subgraph import (
    CachedVolume,
    PoolNotFoundError,
    PoolVolumeData,
    SubgraphError,
    SubgraphQueryError,
    SubgraphRateLimitError,
    SubgraphVolumeProvider,
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
    # Subgraph Volume Provider
    "SubgraphVolumeProvider",
    "PoolVolumeData",
    "CachedVolume",
    "SubgraphError",
    "SubgraphRateLimitError",
    "SubgraphQueryError",
    "PoolNotFoundError",
    # Multi-DEX Volume Provider
    "MultiDEXVolumeProvider",
    "PROTOCOL_PROVIDER_MAP",
    "STRING_PROTOCOL_MAP",
    "PROTOCOL_CHAIN_SUPPORT",
    # Funding Rate Provider
    "FundingRateProvider",
    "FundingRateData",
    "CachedFundingRate",
    "FundingRateLimitState",
    "FundingRateError",
    "FundingRateNotFoundError",
    "FundingRateRateLimitError",
    "UnsupportedProtocolError",
    "FUNDING_SUPPORTED_PROTOCOLS",
    "DEFAULT_FUNDING_RATES",
    "GMX_MARKETS",
    "HYPERLIQUID_MARKETS",
    # Lending APY Provider
    "LendingAPYProvider",
    "LendingAPYData",
    "CachedLendingAPY",
    "LendingAPYRateLimitState",
    "LendingAPYError",
    "LendingAPYNotFoundError",
    "LendingAPYRateLimitError",
    "LendingUnsupportedProtocolError",
    "LENDING_SUPPORTED_PROTOCOLS",
    "DEFAULT_SUPPLY_APYS",
    "DEFAULT_BORROW_APYS",
    "AAVE_V3_MARKETS",
    "COMPOUND_V3_MARKETS",
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
    # Circuit Breaker
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerState",
    "CircuitBreakerMetrics",
    "CircuitBreakerRegistry",
    "CircuitBreakerError",
    "CircuitBreakerOpenError",
    "get_circuit_breaker",
    "get_all_circuit_breaker_metrics",
    "get_open_circuits",
    "create_coingecko_circuit_breaker",
    "create_chainlink_circuit_breaker",
    "create_subgraph_circuit_breaker",
    "create_etherscan_circuit_breaker",
    "create_rpc_circuit_breaker",
    "create_gmx_circuit_breaker",
    "create_hyperliquid_circuit_breaker",
    "COINGECKO_CONFIG",
    "CHAINLINK_CONFIG",
    "SUBGRAPH_CONFIG",
    "ETHERSCAN_CONFIG",
    "RPC_CONFIG",
    "GMX_API_CONFIG",
    "HYPERLIQUID_CONFIG",
    # GMX V2 Historical Funding Provider
    "GMXFundingProvider",
    "GMX_API_URLS",
    "GMX_MARKET_TOKENS",
    "GMX_SUPPORTED_CHAINS",
    "GMX_DATA_SOURCE",
    # Hyperliquid Historical Funding Provider
    "HyperliquidFundingProvider",
    "HYPERLIQUID_API_URL",
    "HYPERLIQUID_DATA_SOURCE",
    "HYPERLIQUID_MAX_HOURS_PER_REQUEST",
    # Aave V3 Historical APY Provider
    "AaveV3APYProvider",
    "AAVE_V3_SUBGRAPH_IDS",
    "AAVE_V3_SUPPORTED_CHAINS",
    "AAVE_V3_DATA_SOURCE",
    # Compound V3 Historical APY Provider
    "CompoundV3APYProvider",
    "COMPOUND_V3_SUBGRAPH_IDS",
    "COMPOUND_V3_SUPPORTED_CHAINS",
    "COMPOUND_V3_DATA_SOURCE",
    # Morpho Blue Historical APY Provider
    "MorphoBlueAPYProvider",
    "MORPHO_BLUE_SUBGRAPH_IDS",
    "MORPHO_BLUE_SUPPORTED_CHAINS",
    "MORPHO_BLUE_DATA_SOURCE",
    # Spark Historical APY Provider
    "SparkAPYProvider",
    "SPARK_SUBGRAPH_IDS",
    "SPARK_SUPPORTED_CHAINS",
    "SPARK_DATA_SOURCE",
    # Liquidity Depth Provider
    "LiquidityDepthProvider",
    "LIQUIDITY_SUPPORTED_CHAINS",
    "LIQUIDITY_DATA_SOURCE_UNISWAP_V3",
    "LIQUIDITY_DATA_SOURCE_SUSHISWAP_V3",
    "LIQUIDITY_DATA_SOURCE_PANCAKESWAP_V3",
    "LIQUIDITY_DATA_SOURCE_AERODROME",
    "LIQUIDITY_DATA_SOURCE_TRADERJOE_V2",
    "LIQUIDITY_DATA_SOURCE_CURVE",
    "LIQUIDITY_DATA_SOURCE_BALANCER",
    "LIQUIDITY_DATA_SOURCE_FALLBACK",
    "V3_PROTOCOLS",
    "V2_PROTOCOLS",
    "LIQUIDITY_BOOK_PROTOCOLS",
    "WEIGHTED_POOL_PROTOCOLS",
    "STABLESWAP_PROTOCOLS",
    "DEFAULT_TWAP_WINDOW_HOURS",
]
