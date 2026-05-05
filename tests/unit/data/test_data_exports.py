"""Tests verifying all quant data layer types are properly exported.

Ensures strategy authors can import all public types from the expected paths:
- almanak.framework.data (primary data module)
- almanak.framework (framework convenience re-exports)
- almanak (top-level convenience re-exports)
"""

from __future__ import annotations


class TestDataModuleExports:
    """Verify all new quant data types are importable from almanak.framework.data."""

    def test_core_provenance_models(self) -> None:
        from almanak.framework.data import (
            CEX_SYMBOL_MAP,
            DataClassification,
            DataEnvelope,
            DataMeta,
            Instrument,
            resolve_instrument,
        )

        assert DataClassification.EXECUTION_GRADE is not None
        assert DataClassification.INFORMATIONAL is not None
        assert DataEnvelope is not None
        assert DataMeta is not None
        assert Instrument is not None
        assert callable(resolve_instrument)
        assert isinstance(CEX_SYMBOL_MAP, dict)

    def test_quant_exceptions(self) -> None:
        from almanak.framework.data import (
            DataUnavailableError,
            LowConfidenceError,
            QuantStaleDataError,
        )

        assert issubclass(DataUnavailableError, Exception)
        assert issubclass(QuantStaleDataError, Exception)
        assert issubclass(LowConfidenceError, Exception)

    def test_pool_price_types(self) -> None:
        from almanak.framework.data import (
            AggregatedPrice,
            PoolContribution,
            PoolPrice,
            PoolReaderRegistry,
            PriceAggregator,
        )

        assert PoolPrice is not None
        assert PoolReaderRegistry is not None
        assert AggregatedPrice is not None
        assert PoolContribution is not None
        assert PriceAggregator is not None

    def test_pool_analytics_types(self) -> None:
        from almanak.framework.data import (
            PoolAnalytics,
            PoolAnalyticsReader,
            PoolAnalyticsResult,
            PoolHistoryReader,
            PoolSnapshot,
        )

        assert PoolAnalytics is not None
        assert PoolAnalyticsReader is not None
        assert PoolAnalyticsResult is not None
        assert PoolSnapshot is not None
        assert PoolHistoryReader is not None

    def test_liquidity_and_slippage_types(self) -> None:
        from almanak.framework.data import (
            LiquidityDepth,
            LiquidityDepthReader,
            SlippageEstimate,
            SlippageEstimator,
            TickData,
        )

        assert LiquidityDepth is not None
        assert LiquidityDepthReader is not None
        assert SlippageEstimate is not None
        assert SlippageEstimator is not None
        assert TickData is not None

    def test_volatility_types(self) -> None:
        from almanak.framework.data import (
            RealizedVolatilityCalculator,
            VolatilityResult,
            VolConeEntry,
            VolConeResult,
        )

        assert VolatilityResult is not None
        assert VolConeEntry is not None
        assert VolConeResult is not None
        assert RealizedVolatilityCalculator is not None

    def test_risk_types(self) -> None:
        from almanak.framework.data import (
            PortfolioRisk,
            PortfolioRiskCalculator,
            RiskConventions,
            RollingSharpeEntry,
            RollingSharpeResult,
            VaRMethod,
        )

        assert PortfolioRisk is not None
        assert PortfolioRiskCalculator is not None
        assert RiskConventions is not None
        assert RollingSharpeEntry is not None
        assert RollingSharpeResult is not None
        assert VaRMethod.PARAMETRIC is not None

    def test_yield_types(self) -> None:
        from almanak.framework.data import (
            YieldAggregator,
            YieldOpportunity,
        )

        assert YieldOpportunity is not None
        assert YieldAggregator is not None

    def test_historical_rate_types(self) -> None:
        from almanak.framework.data import (
            FundingRateSnapshot,
            LendingRateSnapshot,
            RateHistoryReader,
        )

        assert LendingRateSnapshot is not None
        assert FundingRateSnapshot is not None
        assert RateHistoryReader is not None

    def test_routing_types(self) -> None:
        from almanak.framework.data import (
            CircuitBreaker,
            CircuitState,
            DataProvider,
            DataRouter,
            DataRoutingConfig,
            ProviderConfig,
            QuotaConfig,
        )

        assert DataRouter is not None
        assert CircuitBreaker is not None
        assert CircuitState is not None
        assert DataProvider is not None
        assert DataRoutingConfig is not None
        assert ProviderConfig is not None
        assert QuotaConfig is not None

    def test_market_snapshot_error_exports(self) -> None:
        from almanak.framework.data import (
            LiquidityDepthUnavailableError,
            PoolAnalyticsUnavailableError,
            PoolHistoryUnavailableError,
            PoolPriceUnavailableError,
            PortfolioRiskUnavailableError,
            RollingSharpeUnavailableError,
            SlippageEstimateUnavailableError,
            VolatilityUnavailableError,
            VolConeUnavailableError,
            YieldOpportunitiesUnavailableError,
        )

        assert PoolPriceUnavailableError is not None
        assert PoolHistoryUnavailableError is not None
        assert LiquidityDepthUnavailableError is not None
        assert SlippageEstimateUnavailableError is not None
        assert VolatilityUnavailableError is not None
        assert VolConeUnavailableError is not None
        assert PoolAnalyticsUnavailableError is not None
        assert PortfolioRiskUnavailableError is not None
        assert RollingSharpeUnavailableError is not None
        assert YieldOpportunitiesUnavailableError is not None

    def test_all_names_in_dunder_all(self) -> None:
        """Verify all new quant data type names are listed in __all__."""
        import almanak.framework.data as data_module

        expected_names = [
            # Core models
            "DataEnvelope",
            "DataMeta",
            "DataClassification",
            "Instrument",
            "CEX_SYMBOL_MAP",
            "resolve_instrument",
            # Quant exceptions
            "DataUnavailableError",
            "QuantStaleDataError",
            "LowConfidenceError",
            # Pool types
            "PoolPrice",
            "PoolReaderRegistry",
            "AggregatedPrice",
            "PoolContribution",
            "PriceAggregator",
            "PoolAnalytics",
            "PoolAnalyticsReader",
            "PoolAnalyticsResult",
            "PoolSnapshot",
            "PoolHistoryReader",
            # Liquidity & slippage
            "LiquidityDepth",
            "LiquidityDepthReader",
            "SlippageEstimate",
            "SlippageEstimator",
            "TickData",
            # Volatility
            "VolatilityResult",
            "VolConeEntry",
            "VolConeResult",
            "RealizedVolatilityCalculator",
            # Risk
            "PortfolioRisk",
            "PortfolioRiskCalculator",
            "RiskConventions",
            "RollingSharpeEntry",
            "RollingSharpeResult",
            "VaRMethod",
            # Yield
            "YieldOpportunity",
            "YieldAggregator",
            # Historical rates
            "LendingRateSnapshot",
            "FundingRateSnapshot",
            "RateHistoryReader",
            # Routing
            "DataRouter",
            "CircuitBreaker",
            "CircuitState",
            "DataProvider",
            "DataRoutingConfig",
            "ProviderConfig",
            "QuotaConfig",
        ]

        all_names = set(data_module.__all__)
        for name in expected_names:
            assert name in all_names, f"'{name}' missing from almanak.framework.data.__all__"


class TestFrameworkExports:
    """Verify key quant data types are importable from almanak.framework."""

    def test_core_models_from_framework(self) -> None:
        from almanak.framework import (
            DataClassification,
            DataEnvelope,
            DataMeta,
            Instrument,
            resolve_instrument,
        )

        assert DataEnvelope is not None
        assert DataMeta is not None
        assert DataClassification is not None
        assert Instrument is not None
        assert callable(resolve_instrument)

    def test_data_types_from_framework(self) -> None:
        from almanak.framework import (
            AggregatedPrice,
            LiquidityDepth,
            PoolAnalytics,
            PoolPrice,
            PoolSnapshot,
            SlippageEstimate,
        )

        assert PoolPrice is not None
        assert AggregatedPrice is not None
        assert PoolAnalytics is not None
        assert PoolSnapshot is not None
        assert LiquidityDepth is not None
        assert SlippageEstimate is not None

    def test_risk_and_vol_from_framework(self) -> None:
        from almanak.framework import (
            PortfolioRisk,
            RiskConventions,
            VaRMethod,
            VolatilityResult,
        )

        assert PortfolioRisk is not None
        assert RiskConventions is not None
        assert VaRMethod is not None
        assert VolatilityResult is not None

    def test_routing_from_framework(self) -> None:
        from almanak.framework import (
            CircuitBreaker,
            DataProvider,
            DataRouter,
        )

        assert DataRouter is not None
        assert CircuitBreaker is not None
        assert DataProvider is not None

    def test_yield_and_exceptions_from_framework(self) -> None:
        from almanak.framework import (
            DataUnavailableError,
            LowConfidenceError,
            YieldOpportunity,
        )

        assert YieldOpportunity is not None
        assert DataUnavailableError is not None
        assert LowConfidenceError is not None


class TestTopLevelExports:
    """Verify key quant data types are importable from almanak (top-level)."""

    def test_core_models_from_almanak(self) -> None:
        from almanak import (
            DataClassification,
            DataEnvelope,
            DataMeta,
            Instrument,
            resolve_instrument,
        )

        assert DataEnvelope is not None
        assert DataMeta is not None
        assert DataClassification is not None
        assert Instrument is not None
        assert callable(resolve_instrument)

    def test_data_types_from_almanak(self) -> None:
        from almanak import (
            AggregatedPrice,
            LiquidityDepth,
            PoolAnalytics,
            PoolPrice,
            PoolSnapshot,
            SlippageEstimate,
        )

        assert PoolPrice is not None
        assert AggregatedPrice is not None
        assert PoolAnalytics is not None
        assert PoolSnapshot is not None
        assert LiquidityDepth is not None
        assert SlippageEstimate is not None

    def test_risk_and_vol_from_almanak(self) -> None:
        from almanak import (
            PortfolioRisk,
            RiskConventions,
            VaRMethod,
            VolatilityResult,
        )

        assert PortfolioRisk is not None
        assert RiskConventions is not None
        assert VaRMethod is not None
        assert VolatilityResult is not None

    def test_routing_from_almanak(self) -> None:
        from almanak import (
            CircuitBreaker,
            DataProvider,
            DataRouter,
        )

        assert DataRouter is not None
        assert CircuitBreaker is not None
        assert DataProvider is not None

    def test_exceptions_from_almanak(self) -> None:
        from almanak import (
            DataUnavailableError,
            LowConfidenceError,
        )

        assert DataUnavailableError is not None
        assert LowConfidenceError is not None

    def test_yield_from_almanak(self) -> None:
        from almanak import YieldOpportunity

        assert YieldOpportunity is not None

    def test_all_quant_names_in_top_level_dunder_all(self) -> None:
        """Verify key quant data type names are in almanak.__all__."""
        import almanak

        expected_names = [
            "DataEnvelope",
            "DataMeta",
            "DataClassification",
            "Instrument",
            "resolve_instrument",
            "DataUnavailableError",
            "LowConfidenceError",
            "PoolPrice",
            "AggregatedPrice",
            "PoolAnalytics",
            "PoolSnapshot",
            "LiquidityDepth",
            "SlippageEstimate",
            "VolatilityResult",
            "PortfolioRisk",
            "RiskConventions",
            "VaRMethod",
            "YieldOpportunity",
            "DataRouter",
            "CircuitBreaker",
            "DataProvider",
        ]

        all_names = set(almanak.__all__)
        for name in expected_names:
            assert name in all_names, f"'{name}' missing from almanak.__all__"
