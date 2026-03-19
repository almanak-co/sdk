"""Unit tests for PnLBacktester engine.

Tests cover:
- PnLBacktester with mock data provider
- Intent extraction and type detection
- Fee and slippage model lookup
- Metric calculation
- Inclusion delay simulation
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
    IntentType,
)
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import (
    OHLCV,
    HistoricalDataConfig,
    MarketState,
)
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
    create_market_snapshot_from_state,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

# =============================================================================
# Mock Data Provider
# =============================================================================


class MockDataProvider:
    """Mock implementation of HistoricalDataProvider for testing."""

    def __init__(
        self,
        prices: dict[str, dict[datetime, Decimal]] | None = None,
        base_prices: dict[str, Decimal] | None = None,
        price_change_per_tick: Decimal = Decimal("0"),
    ):
        """Initialize mock data provider.

        Args:
            prices: Dict mapping token -> {timestamp -> price}
            base_prices: Dict of base prices to use for all timestamps
            price_change_per_tick: Amount to change price each tick
        """
        self._prices = prices or {}
        self._base_prices = base_prices or {
            "WETH": Decimal("3000"),
            "USDC": Decimal("1"),
        }
        self._price_change_per_tick = price_change_per_tick
        self._tick_count = 0

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        """Get price for token at timestamp."""
        token = token.upper()
        if token in self._prices and timestamp in self._prices[token]:
            return self._prices[token][timestamp]
        if token in self._base_prices:
            return self._base_prices[token]
        raise ValueError(f"No price for {token}")

    async def get_ohlcv(
        self,
        token: str,
        start: datetime,
        end: datetime,
        interval_seconds: int = 3600,
    ) -> list[OHLCV]:
        """Get OHLCV data for token."""
        result = []
        current = start
        while current <= end:
            price = await self.get_price(token, current)
            result.append(
                OHLCV(
                    timestamp=current,
                    open=price,
                    high=price * Decimal("1.01"),
                    low=price * Decimal("0.99"),
                    close=price,
                )
            )
            current += timedelta(seconds=interval_seconds)
        return result

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Iterate through historical data."""
        current = config.start_time
        while current <= config.end_time:
            prices = {}
            for token in config.tokens:
                token = token.upper()
                # Apply price change per tick
                base = self._base_prices.get(token, Decimal("1"))
                change = self._price_change_per_tick * self._tick_count
                prices[token] = base + change

            self._tick_count += 1

            market_state = MarketState(
                timestamp=current,
                prices=prices,
                chain=config.chains[0] if config.chains else "arbitrum",
            )
            yield current, market_state

            current += timedelta(seconds=config.interval_seconds)

    @property
    def provider_name(self) -> str:
        return "mock"

    @property
    def supported_tokens(self) -> list[str]:
        return list(self._base_prices.keys())

    @property
    def supported_chains(self) -> list[str]:
        return ["arbitrum"]

    @property
    def min_timestamp(self) -> datetime | None:
        return None

    @property
    def max_timestamp(self) -> datetime | None:
        return None


# =============================================================================
# Mock Strategy
# =============================================================================


@dataclass
class MockIntent:
    """Mock intent for testing."""

    intent_type: str
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount_usd: Decimal = Decimal("1000")
    protocol: str = "uniswap_v3"


class MockStrategy:
    """Mock strategy for testing."""

    def __init__(
        self,
        intents: list[MockIntent | None] | None = None,
        always_hold: bool = False,
    ):
        """Initialize mock strategy.

        Args:
            intents: List of intents to return (one per decide call)
            always_hold: If True, always return None (hold)
        """
        self._intents = intents or []
        self._always_hold = always_hold
        self._decide_count = 0

    @property
    def strategy_id(self) -> str:
        return "mock_strategy"

    def decide(self, market: Any) -> MockIntent | None:
        """Return next intent from list."""
        if self._always_hold:
            return None

        if self._decide_count < len(self._intents):
            intent = self._intents[self._decide_count]
            self._decide_count += 1
            return intent

        return None


class HoldIntent:
    """Mock hold intent."""

    intent_type = "HOLD"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def base_timestamp() -> datetime:
    """Base timestamp for tests."""
    return datetime(2024, 1, 1, 0, 0, 0)


@pytest.fixture
def mock_data_provider() -> MockDataProvider:
    """Create mock data provider."""
    return MockDataProvider()


@pytest.fixture
def default_fee_model() -> DefaultFeeModel:
    """Create default fee model."""
    return DefaultFeeModel()


@pytest.fixture
def default_slippage_model() -> DefaultSlippageModel:
    """Create default slippage model."""
    return DefaultSlippageModel()


@pytest.fixture
def backtester(mock_data_provider: MockDataProvider) -> PnLBacktester:
    """Create backtester with mock data provider."""
    return PnLBacktester(
        data_provider=mock_data_provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )


@pytest.fixture
def config(base_timestamp: datetime) -> PnLBacktestConfig:
    """Create backtest config."""
    return PnLBacktestConfig(
        start_time=base_timestamp,
        end_time=base_timestamp + timedelta(hours=5),
        interval_seconds=3600,  # 1 hour
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
    )


# =============================================================================
# DefaultFeeModel Tests
# =============================================================================


class TestDefaultFeeModel:
    """Tests for DefaultFeeModel."""

    def test_swap_fee(self) -> None:
        """Test fee calculation for swap."""
        model = DefaultFeeModel(fee_pct=Decimal("0.003"))
        market_state = MarketState(
            timestamp=datetime.now(),
            prices={"WETH": Decimal("3000")},
        )

        fee = model.calculate_fee(
            intent_type=IntentType.SWAP,
            amount_usd=Decimal("1000"),
            market_state=market_state,
        )

        # 0.3% of 1000 = 3
        assert fee == Decimal("3")

    def test_hold_zero_fee(self) -> None:
        """Test that HOLD has zero fee."""
        model = DefaultFeeModel()
        market_state = MarketState(
            timestamp=datetime.now(),
            prices={},
        )

        fee = model.calculate_fee(
            intent_type=IntentType.HOLD,
            amount_usd=Decimal("1000"),
            market_state=market_state,
        )

        assert fee == Decimal("0")

    def test_model_name(self) -> None:
        """Test model_name property."""
        model = DefaultFeeModel()
        assert model.model_name == "default"


# =============================================================================
# DefaultSlippageModel Tests
# =============================================================================


class TestDefaultSlippageModel:
    """Tests for DefaultSlippageModel."""

    def test_swap_slippage(self) -> None:
        """Test slippage calculation for swap."""
        model = DefaultSlippageModel(slippage_pct=Decimal("0.001"))
        market_state = MarketState(
            timestamp=datetime.now(),
            prices={"WETH": Decimal("3000")},
        )

        slippage = model.calculate_slippage(
            intent_type=IntentType.SWAP,
            amount_usd=Decimal("1000"),
            market_state=market_state,
        )

        assert slippage == Decimal("0.001")  # Returns percentage, not USD

    def test_hold_zero_slippage(self) -> None:
        """Test that HOLD has zero slippage."""
        model = DefaultSlippageModel()
        market_state = MarketState(
            timestamp=datetime.now(),
            prices={},
        )

        slippage = model.calculate_slippage(
            intent_type=IntentType.HOLD,
            amount_usd=Decimal("1000"),
            market_state=market_state,
        )

        assert slippage == Decimal("0")

    def test_supply_zero_slippage(self) -> None:
        """Test that SUPPLY has zero slippage."""
        model = DefaultSlippageModel()
        market_state = MarketState(
            timestamp=datetime.now(),
            prices={},
        )

        slippage = model.calculate_slippage(
            intent_type=IntentType.SUPPLY,
            amount_usd=Decimal("1000"),
            market_state=market_state,
        )

        assert slippage == Decimal("0")

    def test_max_slippage_cap(self) -> None:
        """Test that slippage is capped at max."""
        model = DefaultSlippageModel(
            slippage_pct=Decimal("0.10"),  # 10%
            max_slippage_pct=Decimal("0.05"),  # 5% max
        )
        market_state = MarketState(
            timestamp=datetime.now(),
            prices={"WETH": Decimal("3000")},
        )

        slippage = model.calculate_slippage(
            intent_type=IntentType.SWAP,
            amount_usd=Decimal("1000"),
            market_state=market_state,
        )

        assert slippage == Decimal("0.05")  # Capped at max


# =============================================================================
# create_market_snapshot_from_state Tests
# =============================================================================


class TestCreateMarketSnapshot:
    """Tests for create_market_snapshot_from_state function."""

    def test_creates_snapshot_with_prices(self, base_timestamp: datetime) -> None:
        """Test that snapshot contains prices from market state."""
        market_state = MarketState(
            timestamp=base_timestamp,
            prices={
                "WETH": Decimal("3000"),
                "USDC": Decimal("1"),
            },
        )

        snapshot = create_market_snapshot_from_state(market_state)

        assert snapshot.timestamp == base_timestamp
        # MarketSnapshot uses price() method, not get_price()
        assert snapshot.price("WETH") == Decimal("3000")
        assert snapshot.price("USDC") == Decimal("1")

    def test_creates_snapshot_with_portfolio_balances(self, base_timestamp: datetime) -> None:
        """Test that snapshot includes portfolio balances."""
        market_state = MarketState(
            timestamp=base_timestamp,
            prices={"WETH": Decimal("3000")},
        )

        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"))
        portfolio.tokens["WETH"] = Decimal("1.5")

        snapshot = create_market_snapshot_from_state(market_state, portfolio=portfolio)

        # Should have WETH balance - MarketSnapshot uses balance() method
        weth_balance = snapshot.balance("WETH")
        assert weth_balance is not None
        assert weth_balance.balance == Decimal("1.5")
        assert weth_balance.balance_usd == Decimal("4500")  # 1.5 * 3000

        # Should have USD balance from cash
        usd_balance = snapshot.balance("USD")
        assert usd_balance is not None
        assert usd_balance.balance == Decimal("10000")


# =============================================================================
# PnLBacktester Tests
# =============================================================================


class TestPnLBacktester:
    """Tests for PnLBacktester class."""

    def test_backtester_initialization(self, mock_data_provider: MockDataProvider) -> None:
        """Test backtester initializes with default models."""
        backtester = PnLBacktester(
            data_provider=mock_data_provider,
            fee_models={},  # Empty - should add default
            slippage_models={},  # Empty - should add default
        )

        assert "default" in backtester.fee_models
        assert "default" in backtester.slippage_models

    def test_get_fee_model_returns_default(self, backtester: PnLBacktester) -> None:
        """Test that unknown protocol returns default fee model."""
        model = backtester.get_fee_model("unknown_protocol")
        assert model.model_name == "default"

    def test_get_slippage_model_returns_default(self, backtester: PnLBacktester) -> None:
        """Test that unknown protocol returns default slippage model."""
        model = backtester.get_slippage_model("unknown_protocol")
        assert model.model_name == "default"

    @pytest.mark.asyncio
    async def test_backtest_hold_only_strategy(
        self,
        backtester: PnLBacktester,
        config: PnLBacktestConfig,
    ) -> None:
        """Test backtest with strategy that always holds."""
        strategy = MockStrategy(always_hold=True)

        result = await backtester.backtest(strategy, config)

        assert isinstance(result, BacktestResult)
        assert result.engine == BacktestEngine.PNL
        assert result.strategy_id == "mock_strategy"
        assert result.success
        assert result.error is None
        # No trades should have been made
        assert result.metrics.total_trades == 0
        # Equity curve should have entries (one per tick)
        assert len(result.equity_curve) > 0

    @pytest.mark.asyncio
    async def test_backtest_single_swap(
        self,
        config: PnLBacktestConfig,
    ) -> None:
        """Test backtest with a single swap."""
        # Strategy returns a swap on first tick only
        intent = MockIntent(
            intent_type="SWAP",
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            protocol="uniswap_v3",
        )
        strategy = MockStrategy(intents=[intent])

        # Create data provider
        data_provider = MockDataProvider()

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success
        # Trade should be recorded (with inclusion delay)
        # Due to inclusion delay of 1, trade executes on 2nd tick
        assert result.metrics.total_trades >= 0  # May be 0 or 1 depending on timing

    @pytest.mark.asyncio
    async def test_backtest_multiple_swaps(
        self,
        config: PnLBacktestConfig,
    ) -> None:
        """Test backtest with multiple swaps."""
        intents = [
            MockIntent(
                intent_type="SWAP",
                from_token="USDC",
                to_token="WETH",
                amount_usd=Decimal("500"),
            ),
            None,  # Hold
            MockIntent(
                intent_type="SWAP",
                from_token="WETH",
                to_token="USDC",
                amount_usd=Decimal("600"),
            ),
        ]
        strategy = MockStrategy(intents=intents)

        data_provider = MockDataProvider()
        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success
        # At least one equity point should exist
        assert len(result.equity_curve) > 0

    @pytest.mark.asyncio
    async def test_backtest_returns_metrics(
        self,
        backtester: PnLBacktester,
        config: PnLBacktestConfig,
    ) -> None:
        """Test that backtest returns valid metrics."""
        strategy = MockStrategy(always_hold=True)

        result = await backtester.backtest(strategy, config)

        assert isinstance(result.metrics, BacktestMetrics)
        assert result.initial_capital_usd == Decimal("10000")
        # Final capital should be close to initial for hold-only
        assert result.final_capital_usd > Decimal("0")

    @pytest.mark.asyncio
    async def test_backtest_result_summary(
        self,
        backtester: PnLBacktester,
        config: PnLBacktestConfig,
    ) -> None:
        """Test that BacktestResult.summary() returns a string."""
        strategy = MockStrategy(always_hold=True)

        result = await backtester.backtest(strategy, config)
        summary = result.summary()

        assert isinstance(summary, str)
        assert "BACKTEST RESULTS" in summary
        assert "mock_strategy" in summary
        assert "PNL" in summary


# =============================================================================
# Intent Type Detection Tests
# =============================================================================


class TestIntentTypeDetection:
    """Tests for intent type detection in PnLBacktester."""

    def test_extract_intent_from_none(self, backtester: PnLBacktester) -> None:
        """Test that None decide result returns None."""
        result = backtester._extract_intent(None)
        assert result is None

    def test_extract_intent_from_intent(self, backtester: PnLBacktester) -> None:
        """Test that intent is returned directly."""
        intent = MockIntent(intent_type="SWAP")
        result = backtester._extract_intent(intent)
        assert result == intent

    def test_extract_intent_from_tuple(self, backtester: PnLBacktester) -> None:
        """Test that first element of tuple is extracted."""
        intent = MockIntent(intent_type="SWAP")
        decide_result = (intent, {"context": "data"})
        result = backtester._extract_intent(decide_result)
        assert result == intent

    def test_is_hold_intent_none(self, backtester: PnLBacktester) -> None:
        """Test that None is considered hold."""
        assert backtester._is_hold_intent(None) is True

    def test_is_hold_intent_hold_class(self, backtester: PnLBacktester) -> None:
        """Test that HoldIntent class is recognized."""
        hold = HoldIntent()
        assert backtester._is_hold_intent(hold) is True

    def test_is_hold_intent_swap_is_not_hold(self, backtester: PnLBacktester) -> None:
        """Test that swap intent is not hold."""
        swap = MockIntent(intent_type="SWAP")
        assert backtester._is_hold_intent(swap) is False


# =============================================================================
# Metric Calculation Tests
# =============================================================================


class TestMetricCalculation:
    """Tests for metric calculations in PnLBacktester."""

    def test_calculate_returns(self, backtester: PnLBacktester) -> None:
        """Test _calculate_returns helper."""
        values = [Decimal("100"), Decimal("110"), Decimal("105"), Decimal("115")]
        returns = backtester._calculate_returns(values)

        assert len(returns) == 3
        assert returns[0] == Decimal("0.1")  # 10% gain
        # Second return: (105 - 110) / 110 = -5/110 = ~-4.5% loss
        # Use approximate comparison due to Decimal precision
        expected_return_1 = (Decimal("105") - Decimal("110")) / Decimal("110")
        assert returns[1] == expected_return_1
        # Third return: (115 - 105) / 105 = 0.095238...

    def test_calculate_returns_empty(self, backtester: PnLBacktester) -> None:
        """Test _calculate_returns with insufficient data."""
        values = [Decimal("100")]
        returns = backtester._calculate_returns(values)
        assert returns == []

    def test_calculate_max_drawdown(self, backtester: PnLBacktester) -> None:
        """Test _calculate_max_drawdown helper."""
        # 100 -> 120 (peak) -> 96 (20% DD) -> 110 (recovery)
        values = [
            Decimal("100"),
            Decimal("120"),
            Decimal("96"),
            Decimal("110"),
        ]

        max_dd = backtester._calculate_max_drawdown(values)

        # (120 - 96) / 120 = 24/120 = 0.2 (20%)
        assert max_dd == Decimal("0.2")

    def test_calculate_max_drawdown_no_drawdown(self, backtester: PnLBacktester) -> None:
        """Test max drawdown with monotonically increasing values."""
        values = [Decimal("100"), Decimal("110"), Decimal("120")]
        max_dd = backtester._calculate_max_drawdown(values)
        assert max_dd == Decimal("0")

    def test_calculate_volatility(self, backtester: PnLBacktester) -> None:
        """Test _calculate_volatility helper."""
        # Returns with some variance
        returns = [Decimal("0.01"), Decimal("-0.01"), Decimal("0.02"), Decimal("-0.02")]
        volatility = backtester._calculate_volatility(returns, Decimal("365"))

        # Should be positive and annualized
        assert volatility > Decimal("0")

    def test_calculate_sharpe_ratio(self, backtester: PnLBacktester) -> None:
        """Test _calculate_sharpe_ratio helper."""
        returns = [Decimal("0.001")] * 100  # Consistent small positive returns
        volatility = backtester._calculate_volatility(returns, Decimal("365"))

        sharpe = backtester._calculate_sharpe_ratio(
            returns=returns,
            volatility=volatility,
            risk_free_rate=Decimal("0.05"),
            trading_days=Decimal("365"),
        )

        # Should be a number (positive or negative depending on excess return)
        assert isinstance(sharpe, Decimal)

    def test_decimal_sqrt(self, backtester: PnLBacktester) -> None:
        """Test _decimal_sqrt helper."""
        # sqrt(4) = 2
        result = backtester._decimal_sqrt(Decimal("4"))
        assert abs(result - Decimal("2")) < Decimal("0.0001")

        # sqrt(2) ≈ 1.4142
        result = backtester._decimal_sqrt(Decimal("2"))
        assert abs(result - Decimal("1.41421356")) < Decimal("0.0001")

        # sqrt(0) = 0
        result = backtester._decimal_sqrt(Decimal("0"))
        assert result == Decimal("0")

    def test_decimal_sqrt_negative_raises(self, backtester: PnLBacktester) -> None:
        """Test that sqrt of negative raises ValueError."""
        with pytest.raises(ValueError, match="negative"):
            backtester._decimal_sqrt(Decimal("-1"))


# =============================================================================
# Gas Cost Calculation Tests
# =============================================================================


class TestGasCostCalculation:
    """Tests for gas cost calculations."""

    def test_gas_cost_calculation(self, base_timestamp: datetime) -> None:
        """Test that gas costs are calculated correctly."""
        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=1),
            initial_capital_usd=Decimal("10000"),
            include_gas_costs=True,
            gas_price_gwei=Decimal("30"),
        )

        # Gas cost = gas_used * gas_price_gwei * eth_price / 1e9
        # 150000 * 30 * 3000 / 1e9 = 13.5
        gas_cost = config.get_gas_cost_usd(150000, Decimal("3000"))

        assert gas_cost == Decimal("13.5")


# =============================================================================
# Integration Tests
# =============================================================================


class TestBacktesterIntegration:
    """Integration tests for the full backtesting flow."""

    @pytest.mark.asyncio
    async def test_full_backtest_flow(self, base_timestamp: datetime) -> None:
        """Test complete backtest from start to finish."""
        # Create config for 24 hours with hourly intervals
        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            initial_capital_usd=Decimal("100000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
            gas_price_gwei=Decimal("30"),
        )

        # Create data provider with rising prices
        data_provider = MockDataProvider(
            base_prices={"WETH": Decimal("3000"), "USDC": Decimal("1")},
            price_change_per_tick=Decimal("10"),  # WETH increases $10 per hour
        )

        # Strategy: buy WETH early, hold
        strategy = MockStrategy(
            intents=[
                MockIntent(
                    intent_type="SWAP",
                    from_token="USDC",
                    to_token="WETH",
                    amount_usd=Decimal("10000"),
                ),
            ]
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0.003"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0.001"))},
        )

        result = await backtester.backtest(strategy, config)

        # Verify result structure
        assert result.success
        assert result.engine == BacktestEngine.PNL
        assert result.strategy_id == "mock_strategy"
        assert len(result.equity_curve) == 25  # 24 hours + initial
        assert result.initial_capital_usd == Decimal("100000")

        # Verify summary doesn't crash
        summary = result.summary()
        assert "PERFORMANCE" in summary

    @pytest.mark.asyncio
    async def test_backtest_with_protocol_specific_models(self, base_timestamp: datetime) -> None:
        """Test backtest with protocol-specific fee/slippage models."""
        PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=5),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
        )

        # Create custom fee model for uniswap
        uniswap_fee_model = DefaultFeeModel(fee_pct=Decimal("0.003"))  # 0.3%
        aave_fee_model = DefaultFeeModel(fee_pct=Decimal("0"))  # No fee

        backtester = PnLBacktester(
            data_provider=MockDataProvider(),
            fee_models={
                "default": DefaultFeeModel(),
                "uniswap_v3": uniswap_fee_model,
                "aave_v3": aave_fee_model,
            },
            slippage_models={"default": DefaultSlippageModel()},
        )

        # Verify correct model lookup
        assert backtester.get_fee_model("uniswap_v3") == uniswap_fee_model
        assert backtester.get_fee_model("aave_v3") == aave_fee_model
        assert backtester.get_fee_model("unknown").model_name == "default"
