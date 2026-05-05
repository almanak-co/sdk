"""Almanak Data Module.

This package provides data infrastructure for price feeds, balance queries,
market indicators, and quant-grade analytics with proper caching, error
handling, and graceful degradation.

Public names are resolved lazily via :pep:`562` ``__getattr__`` so that
gateway-side imports such as ``from almanak.framework.data.tokens.exceptions
import AmbiguousTokenError`` do not transitively load ``market_snapshot``
(pandas / pyarrow / numpy), ``prediction_provider`` (the full connector
graph), ``indicators`` (RSI / OHLCV / TA), and the other heavy submodules.

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

from typing import TYPE_CHECKING

from almanak._lazy import LazySpec, build_lazy_module_dispatch

if TYPE_CHECKING:
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
        FundingRateSpread,
        GatewayFundingRateProvider,
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
        ATRCalculator,
        BaseIndicator,
        BollingerBandsCalculator,
        BollingerBandsResult,
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
        BasePriceSource,
        DataSourceError,
        DataSourceRateLimited,
        DataSourceTimeout,
        DataSourceUnavailable,
        InsufficientDataError,
        OHLCVProvider,
        PriceOracle,
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
        LSTDataUnavailableError,
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
    from .risk import (
        PortfolioRisk,
        PortfolioRiskCalculator,
        RiskConventions,
        RollingSharpeEntry,
        RollingSharpeResult,
        VaRMethod,
    )
    from .routing import (
        CircuitBreaker,
        CircuitState,
        DataProvider,
        DataRouter,
        DataRoutingConfig,
        ProviderConfig,
        QuotaConfig,
    )
    from .staking import (
        LSTExchangeRate,
        LSTProtocol,
        SolanaLSTProvider,
    )
    from .volatility import (
        RealizedVolatilityCalculator,
        VolatilityResult,
        VolConeEntry,
        VolConeResult,
    )
    from .yields import (
        YieldAggregator,
        YieldOpportunity,
    )


# Maps each public name to (relative module path, attribute name on that
# module). Uses a tuple so we can express renames (``QuantStaleDataError`` is
# ``StaleDataError`` from .exceptions, etc.) without parallel maps.
_LAZY_IMPORTS: dict[str, LazySpec] = {
    # .exceptions
    "DataUnavailableError": (".exceptions", "DataUnavailableError"),
    "LowConfidenceError": (".exceptions", "LowConfidenceError"),
    "QuantStaleDataError": (".exceptions", "StaleDataError"),
    # .funding
    "SUPPORTED_FUNDING_MARKETS": (".funding", "SUPPORTED_MARKETS"),
    "SUPPORTED_VENUES": (".funding", "SUPPORTED_VENUES"),
    "VENUE_CHAINS": (".funding", "VENUE_CHAINS"),
    "FundingRate": (".funding", "FundingRate"),
    "FundingRateError": (".funding", "FundingRateError"),
    "FundingRateSpread": (".funding", "FundingRateSpread"),
    "GatewayFundingRateProvider": (".funding", "GatewayFundingRateProvider"),
    "MarketNotSupportedError": (".funding", "MarketNotSupportedError"),
    "Venue": (".funding", "Venue"),
    "VenueNotSupportedError": (".funding", "VenueNotSupportedError"),
    "ProviderFundingRateUnavailableError": (".funding", "FundingRateUnavailableError"),
    # .health
    "CacheStats": (".health", "CacheStats"),
    "HealthReport": (".health", "HealthReport"),
    "SourceHealth": (".health", "SourceHealth"),
    # .indicators
    "ATRCalculator": (".indicators", "ATRCalculator"),
    "BaseIndicator": (".indicators", "BaseIndicator"),
    "BollingerBandsCalculator": (".indicators", "BollingerBandsCalculator"),
    "BollingerBandsResult": (".indicators", "BollingerBandsResult"),
    "CoinGeckoOHLCVProvider": (".indicators", "CoinGeckoOHLCVProvider"),
    "IndicatorRegistry": (".indicators", "IndicatorRegistry"),
    "MACDCalculator": (".indicators", "MACDCalculator"),
    "MACDResult": (".indicators", "MACDResult"),
    "MovingAverageCalculator": (".indicators", "MovingAverageCalculator"),
    "OHLCVData": (".indicators", "OHLCVData"),
    "RSICalculator": (".indicators", "RSICalculator"),
    "StochasticCalculator": (".indicators", "StochasticCalculator"),
    "StochasticResult": (".indicators", "StochasticResult"),
    # .interfaces
    "AllDataSourcesFailed": (".interfaces", "AllDataSourcesFailed"),
    "BalanceProvider": (".interfaces", "BalanceProvider"),
    "BalanceResult": (".interfaces", "BalanceResult"),
    "BasePriceSource": (".interfaces", "BasePriceSource"),
    "DataSourceError": (".interfaces", "DataSourceError"),
    "DataSourceRateLimited": (".interfaces", "DataSourceRateLimited"),
    "DataSourceTimeout": (".interfaces", "DataSourceTimeout"),
    "DataSourceUnavailable": (".interfaces", "DataSourceUnavailable"),
    "InsufficientDataError": (".interfaces", "InsufficientDataError"),
    "OHLCVProvider": (".interfaces", "OHLCVProvider"),
    "PriceOracle": (".interfaces", "PriceOracle"),
    "PriceResult": (".interfaces", "PriceResult"),
    # .lp
    "COMMON_PRICE_CHANGES": (".lp", "COMMON_PRICE_CHANGES"),
    "ILCalculator": (".lp", "ILCalculator"),
    "ILCalculatorError": (".lp", "ILCalculatorError"),
    "ILExposure": (".lp", "ILExposure"),
    "ILResult": (".lp", "ILResult"),
    "InvalidPriceError": (".lp", "InvalidPriceError"),
    "InvalidWeightError": (".lp", "InvalidWeightError"),
    "LPPosition": (".lp", "LPPosition"),
    "PoolType": (".lp", "PoolType"),
    "PositionNotFoundError": (".lp", "PositionNotFoundError"),
    "ProjectedILResult": (".lp", "ProjectedILResult"),
    "calculate_il_simple": (".lp", "calculate_il_simple"),
    "project_il_table": (".lp", "project_il_table"),
    "CalculatorILExposureError": (".lp", "ILExposureUnavailableError"),
    # .market_snapshot
    "BalanceUnavailableError": (".market_snapshot", "BalanceUnavailableError"),
    "DexQuoteUnavailableError": (".market_snapshot", "DexQuoteUnavailableError"),
    "FundingRateHistoryUnavailableError": (".market_snapshot", "FundingRateHistoryUnavailableError"),
    "FundingRateUnavailableError": (".market_snapshot", "FundingRateUnavailableError"),
    "ILExposureUnavailableError": (".market_snapshot", "ILExposureUnavailableError"),
    "LendingRateHistoryUnavailableError": (".market_snapshot", "LendingRateHistoryUnavailableError"),
    "LendingRateUnavailableError": (".market_snapshot", "LendingRateUnavailableError"),
    "LiquidityDepthUnavailableError": (".market_snapshot", "LiquidityDepthUnavailableError"),
    "LSTDataUnavailableError": (".market_snapshot", "LSTDataUnavailableError"),
    "MarketSnapshot": (".market_snapshot", "MarketSnapshot"),
    "MarketSnapshotError": (".market_snapshot", "MarketSnapshotError"),
    "PoolAnalyticsUnavailableError": (".market_snapshot", "PoolAnalyticsUnavailableError"),
    "PoolHistoryUnavailableError": (".market_snapshot", "PoolHistoryUnavailableError"),
    "PoolPriceUnavailableError": (".market_snapshot", "PoolPriceUnavailableError"),
    "PortfolioRiskUnavailableError": (".market_snapshot", "PortfolioRiskUnavailableError"),
    "PredictionUnavailableError": (".market_snapshot", "PredictionUnavailableError"),
    "PriceUnavailableError": (".market_snapshot", "PriceUnavailableError"),
    "RollingSharpeUnavailableError": (".market_snapshot", "RollingSharpeUnavailableError"),
    "RSIUnavailableError": (".market_snapshot", "RSIUnavailableError"),
    "SlippageEstimateUnavailableError": (".market_snapshot", "SlippageEstimateUnavailableError"),
    "VolatilityUnavailableError": (".market_snapshot", "VolatilityUnavailableError"),
    "VolConeUnavailableError": (".market_snapshot", "VolConeUnavailableError"),
    "YieldOpportunitiesUnavailableError": (".market_snapshot", "YieldOpportunitiesUnavailableError"),
    "RSICalculatorProtocol": (".market_snapshot", "RSICalculator"),
    # .models
    "CEX_SYMBOL_MAP": (".models", "CEX_SYMBOL_MAP"),
    "DataClassification": (".models", "DataClassification"),
    "DataEnvelope": (".models", "DataEnvelope"),
    "DataMeta": (".models", "DataMeta"),
    "Instrument": (".models", "Instrument"),
    "resolve_instrument": (".models", "resolve_instrument"),
    # .pools
    "AggregatedPrice": (".pools", "AggregatedPrice"),
    "LiquidityDepth": (".pools", "LiquidityDepth"),
    "LiquidityDepthReader": (".pools", "LiquidityDepthReader"),
    "PoolAnalytics": (".pools", "PoolAnalytics"),
    "PoolAnalyticsReader": (".pools", "PoolAnalyticsReader"),
    "PoolAnalyticsResult": (".pools", "PoolAnalyticsResult"),
    "PoolContribution": (".pools", "PoolContribution"),
    "PoolHistoryReader": (".pools", "PoolHistoryReader"),
    "PoolPrice": (".pools", "PoolPrice"),
    "PoolReaderRegistry": (".pools", "PoolReaderRegistry"),
    "PoolSnapshot": (".pools", "PoolSnapshot"),
    "PriceAggregator": (".pools", "PriceAggregator"),
    "SlippageEstimate": (".pools", "SlippageEstimate"),
    "SlippageEstimator": (".pools", "SlippageEstimator"),
    "TickData": (".pools", "TickData"),
    # .prediction_provider
    "HistoricalPrice": (".prediction_provider", "HistoricalPrice"),
    "HistoricalTrade": (".prediction_provider", "HistoricalTrade"),
    "PredictionMarket": (".prediction_provider", "PredictionMarket"),
    "PredictionMarketDataProvider": (".prediction_provider", "PredictionMarketDataProvider"),
    "PredictionOrder": (".prediction_provider", "PredictionOrder"),
    "PredictionPosition": (".prediction_provider", "PredictionPosition"),
    "PriceHistory": (".prediction_provider", "PriceHistory"),
    # .rates
    "PROTOCOL_CHAINS": (".rates", "PROTOCOL_CHAINS"),
    "SUPPORTED_PROTOCOLS": (".rates", "SUPPORTED_PROTOCOLS"),
    "SUPPORTED_TOKENS": (".rates", "SUPPORTED_TOKENS"),
    "BestRateResult": (".rates", "BestRateResult"),
    "FundingRateSnapshot": (".rates", "FundingRateSnapshot"),
    "LendingRate": (".rates", "LendingRate"),
    "LendingRateResult": (".rates", "LendingRateResult"),
    "LendingRateSnapshot": (".rates", "LendingRateSnapshot"),
    "Protocol": (".rates", "Protocol"),
    "ProtocolNotSupportedError": (".rates", "ProtocolNotSupportedError"),
    "ProtocolRates": (".rates", "ProtocolRates"),
    "RateHistoryReader": (".rates", "RateHistoryReader"),
    "RateMonitor": (".rates", "RateMonitor"),
    "RateMonitorError": (".rates", "RateMonitorError"),
    "RateSide": (".rates", "RateSide"),
    "RateUnavailableError": (".rates", "RateUnavailableError"),
    "TokenNotSupportedError": (".rates", "TokenNotSupportedError"),
    # .risk
    "PortfolioRisk": (".risk", "PortfolioRisk"),
    "PortfolioRiskCalculator": (".risk", "PortfolioRiskCalculator"),
    "RiskConventions": (".risk", "RiskConventions"),
    "RollingSharpeEntry": (".risk", "RollingSharpeEntry"),
    "RollingSharpeResult": (".risk", "RollingSharpeResult"),
    "VaRMethod": (".risk", "VaRMethod"),
    # .routing
    "CircuitBreaker": (".routing", "CircuitBreaker"),
    "CircuitState": (".routing", "CircuitState"),
    "DataProvider": (".routing", "DataProvider"),
    "DataRouter": (".routing", "DataRouter"),
    "DataRoutingConfig": (".routing", "DataRoutingConfig"),
    "ProviderConfig": (".routing", "ProviderConfig"),
    "QuotaConfig": (".routing", "QuotaConfig"),
    # .staking
    "LSTExchangeRate": (".staking", "LSTExchangeRate"),
    "LSTProtocol": (".staking", "LSTProtocol"),
    "SolanaLSTProvider": (".staking", "SolanaLSTProvider"),
    # .volatility
    "RealizedVolatilityCalculator": (".volatility", "RealizedVolatilityCalculator"),
    "VolatilityResult": (".volatility", "VolatilityResult"),
    "VolConeEntry": (".volatility", "VolConeEntry"),
    "VolConeResult": (".volatility", "VolConeResult"),
    # .yields
    "YieldAggregator": (".yields", "YieldAggregator"),
    "YieldOpportunity": (".yields", "YieldOpportunity"),
}

__all__ = [*sorted(_LAZY_IMPORTS)]

__getattr__, __dir__ = build_lazy_module_dispatch(_LAZY_IMPORTS, package=__name__, namespace=globals())
