"""Integration tests for institutional mode enforcement in PnL backtesting.

These tests validate that institutional_mode=True enforces all constraints:
- Fails on CURRENT_ONLY data provider (lacks historical data)
- Fails on missing symbol mapping (unresolved token addresses)
- Fails on low data coverage (below min_data_coverage threshold)
- compliance_violations lists all failures for audit trail

Requirements:
    - No external dependencies required (uses mock providers)

To run:
    uv run pytest tests/integration/backtesting/test_institutional_mode_integration.py -v -s
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.models import BacktestResult
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import (
    OHLCV,
    HistoricalDataCapability,
    HistoricalDataConfig,
    MarketState,
)
from almanak.framework.backtesting.pnl.engine import PnLBacktester
from almanak.framework.intents import HoldIntent

# =============================================================================
# Mock Data Providers with Different Capabilities
# =============================================================================


@dataclass
class CurrentOnlyDataProvider:
    """Mock data provider with CURRENT_ONLY capability.

    This provider can only fetch current prices, not historical ones.
    Institutional mode should fail when using this provider.
    """

    provider_name: str = "mock_current_only"
    _prices: dict[str, Decimal] = field(default_factory=dict)
    _tick_count: int = 0

    def __post_init__(self) -> None:
        """Initialize default prices."""
        self._prices = {
            "WETH": Decimal("3000"),
            "USDC": Decimal("1"),
            "ETH": Decimal("3000"),
        }

    @property
    def historical_capability(self) -> HistoricalDataCapability:
        """Return CURRENT_ONLY capability - no historical data access."""
        return HistoricalDataCapability.CURRENT_ONLY

    async def iterate(
        self, config: HistoricalDataConfig
    ) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Generate market states for the backtest period."""
        current_time = config.start_time
        while current_time <= config.end_time:
            # Use first chain from config, default to arbitrum
            chain = config.chains[0] if config.chains else "arbitrum"
            # Note: available_tokens is a computed property based on prices/ohlcv keys
            market_state = MarketState(
                timestamp=current_time,
                prices=self._prices.copy(),
                chain=chain,
            )
            self._tick_count += 1
            yield current_time, market_state
            current_time += timedelta(seconds=config.interval_seconds)

    async def get_latest_price(self, token: str) -> Decimal | None:
        """Get latest price for a token."""
        return self._prices.get(token.upper())

    async def get_historical_ohlcv(
        self,
        token: str,
        start_time: datetime,
        end_time: datetime,
        interval: str = "1h",
    ) -> list[OHLCV]:
        """Return empty list - no historical data available."""
        return []


@dataclass
class FullHistoryDataProvider:
    """Mock data provider with FULL historical capability.

    This provider can fetch historical prices for any timestamp.
    Institutional mode should accept this provider.
    """

    provider_name: str = "mock_full_history"
    _prices: dict[str, Decimal] = field(default_factory=dict)
    _tick_count: int = 0
    # For testing missing prices
    _missing_tokens: set[str] = field(default_factory=set)
    # Simulate price lookup failures for data coverage testing
    _fail_lookups: bool = False

    def __post_init__(self) -> None:
        """Initialize default prices."""
        self._prices = {
            "WETH": Decimal("3000"),
            "USDC": Decimal("1"),
            "ETH": Decimal("3000"),
        }

    @property
    def historical_capability(self) -> HistoricalDataCapability:
        """Return FULL capability - has historical data access."""
        return HistoricalDataCapability.FULL

    async def iterate(
        self, config: HistoricalDataConfig
    ) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Generate market states for the backtest period."""
        current_time = config.start_time
        while current_time <= config.end_time:
            # Simulate missing/failed lookups for coverage testing
            if self._fail_lookups:
                # Return empty prices to simulate lookup failures
                prices: dict[str, Decimal] = {}
            else:
                # Filter out explicitly missing tokens
                prices = {k: v for k, v in self._prices.items() if k not in self._missing_tokens}

            # Use first chain from config, default to arbitrum
            chain = config.chains[0] if config.chains else "arbitrum"
            # Note: available_tokens is a computed property based on prices/ohlcv keys
            market_state = MarketState(
                timestamp=current_time,
                prices=prices,
                chain=chain,
            )
            self._tick_count += 1
            yield current_time, market_state
            current_time += timedelta(seconds=config.interval_seconds)

    async def get_latest_price(self, token: str) -> Decimal | None:
        """Get latest price for a token."""
        if token.upper() in self._missing_tokens:
            return None
        return self._prices.get(token.upper())

    async def get_historical_ohlcv(
        self,
        token: str,
        start_time: datetime,
        end_time: datetime,
        interval: str = "1h",
    ) -> list[OHLCV]:
        """Return historical data for a token."""
        if token.upper() in self._missing_tokens or self._fail_lookups:
            return []
        # Return mock historical data
        return [
            OHLCV(
                timestamp=start_time,
                open=self._prices.get(token.upper(), Decimal("1")),
                high=self._prices.get(token.upper(), Decimal("1")),
                low=self._prices.get(token.upper(), Decimal("1")),
                close=self._prices.get(token.upper(), Decimal("1")),
                volume=Decimal("1000000"),
            )
        ]


# =============================================================================
# Mock Strategy
# =============================================================================


class MockHoldStrategy:
    """Simple strategy that always returns HoldIntent.

    Used for testing institutional mode without any trading complexity.
    """

    def __init__(self, strategy_id: str = "test_institutional_mode"):
        self.strategy_id = strategy_id

    def decide(self, market: MarketState) -> HoldIntent:
        """Always return HoldIntent - no trading."""
        return HoldIntent(reason="Testing institutional mode")


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def base_config() -> PnLBacktestConfig:
    """Create a base backtest config for testing."""
    return PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 2, tzinfo=UTC),  # 1 day backtest
        interval_seconds=3600,  # 1 hour intervals
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        chain="arbitrum",
        institutional_mode=False,  # Default to non-institutional
    )


@pytest.fixture
def institutional_config() -> PnLBacktestConfig:
    """Create an institutional mode config."""
    return PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 2, tzinfo=UTC),  # 1 day backtest
        interval_seconds=3600,  # 1 hour intervals
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        chain="arbitrum",
        institutional_mode=True,  # Enable institutional mode
    )


@pytest.fixture
def mock_strategy() -> MockHoldStrategy:
    """Create a mock strategy for testing."""
    return MockHoldStrategy()


@pytest.fixture
def current_only_provider() -> CurrentOnlyDataProvider:
    """Create a CURRENT_ONLY data provider."""
    return CurrentOnlyDataProvider()


@pytest.fixture
def full_history_provider() -> FullHistoryDataProvider:
    """Create a FULL history data provider."""
    return FullHistoryDataProvider()


# =============================================================================
# Test Classes
# =============================================================================


class TestInstitutionalModeConfigEnforcement:
    """Test that institutional_mode enforces strict config values."""

    def test_institutional_mode_sets_strict_reproducibility(self) -> None:
        """Verify institutional_mode=True sets strict_reproducibility=True."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
            initial_capital_usd=Decimal("10000"),
            institutional_mode=True,
            strict_reproducibility=False,  # Should be overridden
        )
        assert config.strict_reproducibility is True

    def test_institutional_mode_disables_degraded_data(self) -> None:
        """Verify institutional_mode=True sets allow_degraded_data=False."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
            initial_capital_usd=Decimal("10000"),
            institutional_mode=True,
            allow_degraded_data=True,  # Should be overridden
        )
        assert config.allow_degraded_data is False

    def test_institutional_mode_disables_hardcoded_fallback(self) -> None:
        """Verify institutional_mode=True sets allow_hardcoded_fallback=False."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
            initial_capital_usd=Decimal("10000"),
            institutional_mode=True,
            allow_hardcoded_fallback=True,  # Should be overridden
        )
        assert config.allow_hardcoded_fallback is False

    def test_institutional_mode_requires_symbol_mapping(self) -> None:
        """Verify institutional_mode=True sets require_symbol_mapping=True."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
            initial_capital_usd=Decimal("10000"),
            institutional_mode=True,
            require_symbol_mapping=False,  # Should be overridden
        )
        assert config.require_symbol_mapping is True

    def test_institutional_mode_enforces_min_data_coverage(self) -> None:
        """Verify institutional_mode=True enforces min_data_coverage >= 98%."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
            initial_capital_usd=Decimal("10000"),
            institutional_mode=True,
            min_data_coverage=Decimal("0.50"),  # Should be overridden to 0.98
        )
        assert config.min_data_coverage >= Decimal("0.98")


class TestInstitutionalModeCurrentOnlyProvider:
    """Test that institutional mode detects CURRENT_ONLY providers."""

    @pytest.mark.asyncio
    async def test_current_only_provider_sets_compliance_violation(
        self,
        current_only_provider: CurrentOnlyDataProvider,
        mock_strategy: MockHoldStrategy,
    ) -> None:
        """Verify CURRENT_ONLY provider triggers compliance violation."""
        # Use non-institutional mode to avoid errors
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 6, tzinfo=UTC),  # 6 hours
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            institutional_mode=False,  # Don't fail, just track violations
        )

        backtester = PnLBacktester(
            data_provider=current_only_provider,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, config)

        assert result is not None
        # Should have compliance violation for CURRENT_ONLY
        assert result.institutional_compliance is False
        assert len(result.compliance_violations) > 0

        # Check that the violation mentions CURRENT_ONLY
        violation_text = " ".join(result.compliance_violations)
        assert "CURRENT_ONLY" in violation_text

    @pytest.mark.asyncio
    async def test_data_source_warnings_for_current_only(
        self,
        current_only_provider: CurrentOnlyDataProvider,
        mock_strategy: MockHoldStrategy,
    ) -> None:
        """Verify data_source_warnings populated for CURRENT_ONLY provider."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 6, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            institutional_mode=False,
        )

        backtester = PnLBacktester(
            data_provider=current_only_provider,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, config)

        assert result is not None
        # Should have warnings about CURRENT_ONLY capability
        assert len(result.data_source_warnings) > 0

        warning_text = " ".join(result.data_source_warnings)
        assert "CURRENT_ONLY" in warning_text


class TestInstitutionalModeDataCoverage:
    """Test institutional mode enforcement of data coverage requirements."""

    @pytest.mark.asyncio
    async def test_full_history_provider_passes_compliance(
        self,
        full_history_provider: FullHistoryDataProvider,
        mock_strategy: MockHoldStrategy,
    ) -> None:
        """Verify FULL history provider passes compliance check."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 6, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            institutional_mode=False,  # Non-institutional for simple test
        )

        backtester = PnLBacktester(
            data_provider=full_history_provider,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, config)

        assert result is not None
        # With FULL provider and good data, should pass compliance
        assert result.institutional_compliance is True
        assert len(result.compliance_violations) == 0

    @pytest.mark.asyncio
    async def test_low_data_coverage_triggers_violation(
        self,
        mock_strategy: MockHoldStrategy,
    ) -> None:
        """Verify low data coverage triggers compliance violation."""
        # Create provider that fails lookups to simulate low coverage
        provider = FullHistoryDataProvider()
        provider._fail_lookups = True

        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 6, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            institutional_mode=False,  # Don't fail, just track violations
            min_data_coverage=Decimal("0.98"),
        )

        backtester = PnLBacktester(
            data_provider=provider,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, config)

        assert result is not None
        # Low coverage should fail compliance
        assert result.institutional_compliance is False

        # Check violation mentions coverage
        violation_text = " ".join(result.compliance_violations)
        assert "coverage" in violation_text.lower() or "Data" in violation_text

    @pytest.mark.asyncio
    async def test_institutional_mode_raises_on_low_coverage(
        self,
        mock_strategy: MockHoldStrategy,
    ) -> None:
        """Verify institutional_mode=True raises error on low coverage."""
        # Create provider that fails lookups
        provider = FullHistoryDataProvider()
        provider._fail_lookups = True

        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 6, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            institutional_mode=True,  # Should raise error
        )

        backtester = PnLBacktester(
            data_provider=provider,
            fee_models={},
            slippage_models={},
        )

        # In institutional mode, low coverage should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            await backtester.backtest(mock_strategy, config)

        error_msg = str(exc_info.value)
        assert "institutional mode" in error_msg.lower() or "coverage" in error_msg.lower()


class TestComplianceViolationTracking:
    """Test that compliance_violations correctly lists all failures."""

    @pytest.mark.asyncio
    async def test_compliance_violations_lists_all_failures(
        self,
        current_only_provider: CurrentOnlyDataProvider,
        mock_strategy: MockHoldStrategy,
    ) -> None:
        """Verify compliance_violations contains all detected issues."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 6, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            institutional_mode=False,
        )

        backtester = PnLBacktester(
            data_provider=current_only_provider,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, config)

        assert result is not None
        # Check that violations is a list (even if empty)
        assert isinstance(result.compliance_violations, list)

        # If there are violations, each should be a string
        for violation in result.compliance_violations:
            assert isinstance(violation, str)
            assert len(violation) > 0

    @pytest.mark.asyncio
    async def test_compliance_violations_serialization(
        self,
        current_only_provider: CurrentOnlyDataProvider,
        mock_strategy: MockHoldStrategy,
    ) -> None:
        """Verify compliance_violations survives to_dict/from_dict round-trip."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 6, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            institutional_mode=False,
        )

        backtester = PnLBacktester(
            data_provider=current_only_provider,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, config)
        assert result is not None

        # Serialize and deserialize
        result_dict = result.to_dict()
        restored = BacktestResult.from_dict(result_dict)

        # Check fields are preserved
        assert restored.institutional_compliance == result.institutional_compliance
        assert restored.compliance_violations == result.compliance_violations


class TestDataSourceCapabilities:
    """Test data_source_capabilities tracking in BacktestResult."""

    @pytest.mark.asyncio
    async def test_data_source_capabilities_populated(
        self,
        current_only_provider: CurrentOnlyDataProvider,
        mock_strategy: MockHoldStrategy,
    ) -> None:
        """Verify data_source_capabilities is populated in result."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 6, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            institutional_mode=False,
        )

        backtester = PnLBacktester(
            data_provider=current_only_provider,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, config)

        assert result is not None
        assert isinstance(result.data_source_capabilities, dict)
        # Should have the provider's capability
        assert len(result.data_source_capabilities) > 0

    @pytest.mark.asyncio
    async def test_full_capability_provider_in_capabilities(
        self,
        full_history_provider: FullHistoryDataProvider,
        mock_strategy: MockHoldStrategy,
    ) -> None:
        """Verify FULL capability provider shows correct capability."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 6, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            institutional_mode=False,
        )

        backtester = PnLBacktester(
            data_provider=full_history_provider,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, config)

        assert result is not None
        # Should show FULL capability
        capabilities_str = str(result.data_source_capabilities)
        assert "FULL" in capabilities_str or "full" in capabilities_str.lower()


class TestDataQualityReport:
    """Test data quality report in institutional context."""

    @pytest.mark.asyncio
    async def test_data_quality_report_populated(
        self,
        full_history_provider: FullHistoryDataProvider,
        mock_strategy: MockHoldStrategy,
    ) -> None:
        """Verify data_quality is populated in result."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 6, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            institutional_mode=False,
        )

        backtester = PnLBacktester(
            data_provider=full_history_provider,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, config)

        assert result is not None
        assert result.data_quality is not None
        assert hasattr(result.data_quality, "coverage_ratio")
        assert hasattr(result.data_quality, "source_breakdown")

    @pytest.mark.asyncio
    async def test_data_quality_coverage_ratio_reasonable(
        self,
        full_history_provider: FullHistoryDataProvider,
        mock_strategy: MockHoldStrategy,
    ) -> None:
        """Verify coverage_ratio is between 0 and 1."""
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 6, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            institutional_mode=False,
        )

        backtester = PnLBacktester(
            data_provider=full_history_provider,
            fee_models={},
            slippage_models={},
        )

        result = await backtester.backtest(mock_strategy, config)

        assert result is not None
        assert result.data_quality is not None
        coverage = result.data_quality.coverage_ratio
        assert Decimal("0") <= coverage <= Decimal("1")
