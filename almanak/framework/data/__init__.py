"""Almanak Data Module.

This package provides data infrastructure for price feeds, balance queries,
market indicators, and quant-grade analytics with proper caching, error
handling, and graceful degradation.

Key Components:
    - interfaces: Core ABCs and Protocols for data providers
    - models: DataEnvelope, DataMeta, Instrument provenance models
    - exceptions: Quant-layer exception hierarchy
    - routing/: DataRouter, CircuitBreaker, provider configuration
    - pools/: Pool price readers, aggregation, analytics, liquidity
    - volatility/: Realized vol estimators, vol cone
    - risk/: Portfolio risk metrics (Sharpe, VaR, drawdown)
    - yields/: Cross-protocol yield comparison
    - rates/: Lending rate monitor, historical rates
    - ohlcv/: Multi-provider OHLCV with CEX/DEX routing
    - cache/: Versioned data cache for deterministic replay
    - price/: Price source implementations (CoinGecko, Chainlink, etc.)
    - balance/: On-chain balance providers
    - indicators/: Technical indicators (RSI, etc.)
    - market_snapshot: Unified market data interface for strategies

Example:
    from almanak.framework.data import (
        DataEnvelope, DataMeta, DataClassification, Instrument,
        PoolPrice, AggregatedPrice, VolatilityResult, PortfolioRisk,
        DataRouter, CircuitBreaker, DataProvider,
        DataUnavailableError, StaleDataError, LowConfidenceError,
    )
"""

# Web3BalanceProvider moved to gateway
# Import from almanak.gateway.data.balance for Web3BalanceProvider

# --- Quant Data Layer: Core provenance models ---
from .exceptions import (
    DataUnavailableError,
    LowConfidenceError,
)
from .exceptions import (
    StaleDataError as QuantStaleDataError,
)
from .funding import (
    SUPPORTED_MARKETS as SUPPORTED_FUNDING_MARKETS,
)
from .funding import (
    SUPPORTED_VENUES,
    VENUE_CHAINS,
    FundingRate,
    FundingRateError,
    FundingRateProvider,
    FundingRateSpread,
    HistoricalFundingData,
    HistoricalFundingRate,
    MarketNotSupportedError,
    Venue,
    VenueNotSupportedError,
)
from .funding import (
    FundingRateUnavailableError as ProviderFundingRateUnavailableError,
)
from .health import (
    CacheStats,
    HealthReport,
    SourceHealth,
)
from .indicators import (
    # New Indicators
    ATRCalculator,
    # Base and Registry
    BaseIndicator,
    BollingerBandsCalculator,
    BollingerBandsResult,
    # RSI and OHLCV
    CoinGeckoOHLCVProvider,
    IndicatorRegistry,
    MACDCalculator,
    MACDResult,
    MovingAverageCalculator,
    OHLCVData,
    RSICalculator,
    StochasticCalculator,
    StochasticResult,
)
from .interfaces import (
    AllDataSourcesFailed,
    BalanceProvider,
    BalanceResult,
    # Abstract base classes
    BasePriceSource,
    # Exceptions
    DataSourceError,
    DataSourceRateLimited,
    DataSourceTimeout,
    DataSourceUnavailable,
    InsufficientDataError,
    OHLCVProvider,
    # Protocols
    PriceOracle,
    # Data classes
    PriceResult,
)
from .lp import (
    COMMON_PRICE_CHANGES,
    ILCalculator,
    ILCalculatorError,
    ILExposure,
    ILResult,
    InvalidPriceError,
    InvalidWeightError,
    LPPosition,
    PoolType,
    PositionNotFoundError,
    ProjectedILResult,
    calculate_il_simple,
    project_il_table,
)
from .lp import (
    ILExposureUnavailableError as CalculatorILExposureError,
)
from .market_snapshot import (
    BalanceUnavailableError,
    DexQuoteUnavailableError,
    FundingRateHistoryUnavailableError,
    FundingRateUnavailableError,
    ILExposureUnavailableError,
    LendingRateHistoryUnavailableError,
    LendingRateUnavailableError,
    LiquidityDepthUnavailableError,
    MarketSnapshot,
    MarketSnapshotError,
    PoolAnalyticsUnavailableError,
    PoolHistoryUnavailableError,
    PoolPriceUnavailableError,
    PortfolioRiskUnavailableError,
    PredictionUnavailableError,
    PriceUnavailableError,
    RollingSharpeUnavailableError,
    RSIUnavailableError,
    SlippageEstimateUnavailableError,
    VolatilityUnavailableError,
    VolConeUnavailableError,
    YieldOpportunitiesUnavailableError,
)
from .market_snapshot import (
    RSICalculator as RSICalculatorProtocol,
)
from .models import (
    CEX_SYMBOL_MAP,
    DataClassification,
    DataEnvelope,
    DataMeta,
    Instrument,
    resolve_instrument,
)
from .ohlcv import (
    BINANCE_SYMBOL_MAP,
    BinanceOHLCVProvider,
)

# --- Quant Data Layer: Pool price readers, aggregation, analytics ---
from .pools import (
    AggregatedPrice,
    LiquidityDepth,
    LiquidityDepthReader,
    PoolAnalytics,
    PoolAnalyticsReader,
    PoolAnalyticsResult,
    PoolContribution,
    PoolHistoryReader,
    PoolPrice,
    PoolReaderRegistry,
    PoolSnapshot,
    PriceAggregator,
    SlippageEstimate,
    SlippageEstimator,
    TickData,
)
from .prediction_provider import (
    HistoricalPrice,
    HistoricalTrade,
    PredictionMarket,
    PredictionMarketDataProvider,
    PredictionOrder,
    PredictionPosition,
    PriceHistory,
)

# DEX price services moved to gateway
# Import from almanak.gateway.data.price for MultiDexPriceService, DexQuote, etc.
from .rates import (
    PROTOCOL_CHAINS,
    SUPPORTED_PROTOCOLS,
    SUPPORTED_TOKENS,
    BestRateResult,
    FundingRateSnapshot,
    LendingRate,
    LendingRateResult,
    LendingRateSnapshot,
    Protocol,
    ProtocolNotSupportedError,
    ProtocolRates,
    RateHistoryReader,
    RateMonitor,
    RateMonitorError,
    RateSide,
    RateUnavailableError,
    TokenNotSupportedError,
)

# --- Quant Data Layer: Risk metrics ---
from .risk import (
    PortfolioRisk,
    PortfolioRiskCalculator,
    RiskConventions,
    RollingSharpeEntry,
    RollingSharpeResult,
    VaRMethod,
)

# --- Quant Data Layer: Data routing ---
from .routing import (
    CircuitBreaker,
    CircuitState,
    DataProvider,
    DataRouter,
    DataRoutingConfig,
    ProviderConfig,
    QuotaConfig,
)

# --- Quant Data Layer: Volatility ---
from .volatility import (
    RealizedVolatilityCalculator,
    VolatilityResult,
    VolConeEntry,
    VolConeResult,
)

# --- Quant Data Layer: Yield comparison ---
from .yields import (
    YieldAggregator,
    YieldOpportunity,
)

__all__ = [
    # Data classes
    "PriceResult",
    "BalanceResult",
    "OHLCVData",
    # Health data classes
    "SourceHealth",
    "CacheStats",
    "HealthReport",
    # Abstract base classes
    "BasePriceSource",
    # Protocols
    "PriceOracle",
    "BalanceProvider",
    "OHLCVProvider",
    "RSICalculatorProtocol",
    # Exceptions (legacy)
    "DataSourceError",
    "DataSourceUnavailable",
    "DataSourceTimeout",
    "DataSourceRateLimited",
    "AllDataSourcesFailed",
    "InsufficientDataError",
    # Exceptions (quant data layer)
    "DataUnavailableError",
    "QuantStaleDataError",
    "LowConfidenceError",
    # MarketSnapshot
    "MarketSnapshot",
    "MarketSnapshotError",
    "PriceUnavailableError",
    "BalanceUnavailableError",
    "RSIUnavailableError",
    "PoolHistoryUnavailableError",
    "PoolPriceUnavailableError",
    "LendingRateUnavailableError",
    "LendingRateHistoryUnavailableError",
    "FundingRateUnavailableError",
    "FundingRateHistoryUnavailableError",
    "DexQuoteUnavailableError",
    "ILExposureUnavailableError",
    "PredictionUnavailableError",
    "LiquidityDepthUnavailableError",
    "SlippageEstimateUnavailableError",
    "VolatilityUnavailableError",
    "VolConeUnavailableError",
    "PoolAnalyticsUnavailableError",
    "PortfolioRiskUnavailableError",
    "RollingSharpeUnavailableError",
    "YieldOpportunitiesUnavailableError",
    # Quant Data Layer - Core models
    "DataEnvelope",
    "DataMeta",
    "DataClassification",
    "Instrument",
    "CEX_SYMBOL_MAP",
    "resolve_instrument",
    # Quant Data Layer - Pool price readers & aggregation
    "PoolPrice",
    "PoolReaderRegistry",
    "AggregatedPrice",
    "PoolContribution",
    "PriceAggregator",
    # Quant Data Layer - Pool analytics & history
    "PoolAnalytics",
    "PoolAnalyticsReader",
    "PoolAnalyticsResult",
    "PoolSnapshot",
    "PoolHistoryReader",
    # Quant Data Layer - Liquidity & slippage
    "LiquidityDepth",
    "LiquidityDepthReader",
    "SlippageEstimate",
    "SlippageEstimator",
    "TickData",
    # Quant Data Layer - Volatility
    "VolatilityResult",
    "VolConeEntry",
    "VolConeResult",
    "RealizedVolatilityCalculator",
    # Quant Data Layer - Risk metrics
    "PortfolioRisk",
    "PortfolioRiskCalculator",
    "RiskConventions",
    "RollingSharpeEntry",
    "RollingSharpeResult",
    "VaRMethod",
    # Quant Data Layer - Yield comparison
    "YieldOpportunity",
    "YieldAggregator",
    # Quant Data Layer - Historical rates
    "LendingRateSnapshot",
    "FundingRateSnapshot",
    "RateHistoryReader",
    # Quant Data Layer - Data routing
    "DataRouter",
    "CircuitBreaker",
    "CircuitState",
    "DataProvider",
    "DataRoutingConfig",
    "ProviderConfig",
    "QuotaConfig",
    # Balance providers
    # "Web3BalanceProvider",  # Moved to gateway
    # Indicators - Base and Registry
    "BaseIndicator",
    "IndicatorRegistry",
    "BollingerBandsResult",
    "MACDResult",
    "StochasticResult",
    # Indicators - RSI and OHLCV Providers
    "RSICalculator",
    "CoinGeckoOHLCVProvider",
    "BinanceOHLCVProvider",
    "BINANCE_SYMBOL_MAP",
    # Indicators - New Calculators
    "MovingAverageCalculator",
    "BollingerBandsCalculator",
    "MACDCalculator",
    "StochasticCalculator",
    "ATRCalculator",
    # Rate Monitor
    "RateMonitor",
    "LendingRate",
    "LendingRateResult",
    "BestRateResult",
    "ProtocolRates",
    "RateSide",
    "Protocol",
    "RateMonitorError",
    "RateUnavailableError",
    "ProtocolNotSupportedError",
    "TokenNotSupportedError",
    "SUPPORTED_PROTOCOLS",
    "PROTOCOL_CHAINS",
    "SUPPORTED_TOKENS",
    # Funding Rate Provider
    "FundingRateProvider",
    "FundingRate",
    "HistoricalFundingRate",
    "FundingRateSpread",
    "HistoricalFundingData",
    "Venue",
    "FundingRateError",
    "ProviderFundingRateUnavailableError",
    "VenueNotSupportedError",
    "MarketNotSupportedError",
    "SUPPORTED_VENUES",
    "VENUE_CHAINS",
    "SUPPORTED_FUNDING_MARKETS",
    # Multi-DEX Price Service - moved to gateway
    # Import from almanak.gateway.data.price for DEX services
    # IL Calculator
    "ILCalculator",
    "PoolType",
    "ILResult",
    "ProjectedILResult",
    "LPPosition",
    "ILExposure",
    "ILCalculatorError",
    "InvalidPriceError",
    "InvalidWeightError",
    "PositionNotFoundError",
    "CalculatorILExposureError",
    "calculate_il_simple",
    "project_il_table",
    "COMMON_PRICE_CHANGES",
    # Prediction Market Data Provider
    "PredictionMarketDataProvider",
    "PredictionMarket",
    "PredictionPosition",
    "PredictionOrder",
    "HistoricalPrice",
    "PriceHistory",
    "HistoricalTrade",
]
