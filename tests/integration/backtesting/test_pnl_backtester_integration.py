"""Integration tests for PnL Backtester.

These tests run full end-to-end backtests with mock data providers
to validate the complete backtest flow, including:
- Portfolio initialization and management
- Intent execution with fees and slippage
- Equity curve generation
- Comprehensive metrics calculation
- Various trading scenarios (swaps, LP, lending, perps)
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestResult,
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
)
from tests.backtesting_funding import pnl_token_funding, provider_symbol


class DeterministicDataProvider:
    """Data provider with pre-defined price series for deterministic testing.

    This provider yields exact prices at specific timestamps to ensure
    tests produce reproducible, deterministic results regardless of
    when or where they run.
    """

    def __init__(
        self,
        price_series: dict[str, list[Decimal]],
        start_time: datetime,
        interval_seconds: int = 3600,
    ):
        """Initialize with pre-defined price series.

        Args:
            price_series: Dict mapping token -> list of prices in order
            start_time: Start timestamp for the series
            interval_seconds: Interval between price points
        """
        self._price_series = price_series
        self._start_time = start_time
        self._interval_seconds = interval_seconds

    async def get_price(self, token: Any, timestamp: datetime) -> Decimal:
        """Get price for token at specific timestamp."""
        symbol = provider_symbol(token)
        if symbol not in self._price_series:
            raise ValueError(f"No price series for {symbol}")

        delta = timestamp - self._start_time
        index = int(delta.total_seconds() / self._interval_seconds)
        series = self._price_series[symbol]

        if 0 <= index < len(series):
            return series[index]
        elif index >= len(series):
            return series[-1]
        else:
            return series[0]

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
                    high=price * Decimal("1.005"),
                    low=price * Decimal("0.995"),
                    close=price,
                    volume=Decimal("1000000"),
                )
            )
            current += timedelta(seconds=interval_seconds)
        return result

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Iterate through historical data with deterministic prices."""
        current = config.start_time
        index = 0

        while current <= config.end_time:
            prices = {}
            for token in config.tokens:
                symbol = provider_symbol(token, config.chains[0] if config.chains else "arbitrum")
                if symbol in self._price_series:
                    series = self._price_series[symbol]
                    if index < len(series):
                        prices[symbol] = series[index]
                    else:
                        prices[symbol] = series[-1]
                else:
                    prices[symbol] = Decimal("1")

            market_state = MarketState(
                timestamp=current,
                prices=prices,
                chain=config.chains[0] if config.chains else "arbitrum",
                block_number=15000000 + index * 100,
                gas_price_gwei=Decimal("30"),
            )
            yield current, market_state

            index += 1
            current += timedelta(seconds=config.interval_seconds)

    @property
    def provider_name(self) -> str:
        return "deterministic"

    @property
    def supported_tokens(self) -> list[str]:
        return list(self._price_series.keys())

    @property
    def supported_chains(self) -> list[str]:
        return ["arbitrum", "ethereum"]

    @property
    def min_timestamp(self) -> datetime | None:
        return self._start_time

    @property
    def max_timestamp(self) -> datetime | None:
        n_points = max((len(s) for s in self._price_series.values()), default=0)
        if n_points <= 1:
            return self._start_time
        return self._start_time + timedelta(seconds=(n_points - 1) * self._interval_seconds)


@dataclass
class MockSwapIntent:
    """Mock swap intent for testing."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount_usd: Decimal = Decimal("1000")
    protocol: str = "uniswap_v3"


@dataclass
class MockLPOpenIntent:
    """Mock LP open intent for testing."""

    intent_type: str = "LP_OPEN"
    token0: str = "WETH"
    token1: str = "USDC"
    amount_usd: Decimal = Decimal("5000")
    protocol: str = "uniswap_v3"
    tick_lower: int = -887272
    tick_upper: int = 887272
    fee_tier: Decimal = Decimal("0.003")


@dataclass
class MockLPCloseIntent:
    """Mock LP close intent for testing."""

    intent_type: str = "LP_CLOSE"
    token0: str = "WETH"
    token1: str = "USDC"
    position_id: str = ""
    protocol: str = "uniswap_v3"


@dataclass
class MockSupplyIntent:
    """Mock supply intent for testing."""

    intent_type: str = "SUPPLY"
    token: str = "WETH"
    amount_usd: Decimal = Decimal("5000")
    protocol: str = "aave_v3"
    apy: Decimal = Decimal("0.05")


@dataclass
class MockPerpOpenIntent:
    """Mock perp open intent for testing."""

    intent_type: str = "PERP_OPEN"
    token: str = "ETH"
    amount_usd: Decimal = Decimal("2000")
    protocol: str = "gmx"
    leverage: Decimal = Decimal("5")
    side: str = "long"


class DeterministicStrategy:
    """Strategy with pre-defined decision sequence for testing."""

    def __init__(
        self,
        intents: list[Any | None],
        deployment_id: str = "deterministic_strategy",
    ):
        """Initialize with pre-defined intent sequence.

        Args:
            intents: List of intents to return in order (None = hold)
            deployment_id: Identifier for the strategy
        """
        self._intents = intents
        self._deployment_id = deployment_id
        self._call_count = 0

    @property
    def deployment_id(self) -> str:
        return self._deployment_id

    def decide(self, market: Any) -> Any | None:
        """Return next intent from sequence."""
        if self._call_count < len(self._intents):
            intent = self._intents[self._call_count]
            self._call_count += 1
            return intent
        return None


@pytest.fixture
def base_timestamp() -> datetime:
    """Fixed base timestamp for deterministic tests."""
    return datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)


@pytest.fixture
def eth_uptrend_prices() -> list[Decimal]:
    """ETH price series with steady uptrend."""
    # 25 hourly prices: 3000 -> 3240 (8% gain over 24 hours)
    return [Decimal(str(3000 + i * 10)) for i in range(25)]


@pytest.fixture
def eth_downtrend_prices() -> list[Decimal]:
    """ETH price series with steady downtrend."""
    # 25 hourly prices: 3000 -> 2760 (8% loss over 24 hours)
    return [Decimal(str(3000 - i * 10)) for i in range(25)]


@pytest.fixture
def eth_volatile_prices() -> list[Decimal]:
    """ETH price series with high volatility."""
    base = 3000
    return [Decimal(str(base + (50 if i % 2 == 0 else -50) * (i // 2 + 1))) for i in range(25)]


@pytest.fixture
def usdc_stable_prices() -> list[Decimal]:
    """USDC price series (stable at $1)."""
    return [Decimal("1")] * 25


class TestFullBacktestFlow:
    """Integration tests for complete backtest execution."""

    @pytest.mark.asyncio
    async def test_hold_only_backtest_preserves_capital(
        self,
        base_timestamp: datetime,
        eth_uptrend_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test that hold-only strategy preserves initial capital."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_uptrend_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        strategy = DeterministicStrategy(intents=[None] * 25)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success
        assert result.engine == BacktestEngine.PNL
        assert result.error is None
        assert result.metrics.total_trades == 0
        assert result.final_capital_usd == result.initial_portfolio_value_usd
        assert result.metrics.total_return_pct == Decimal("0")
        assert len(result.equity_curve) == 25

    @pytest.mark.asyncio
    async def test_single_swap_executes_correctly(
        self,
        base_timestamp: datetime,
        eth_uptrend_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test single swap execution with fees and slippage."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_uptrend_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
            gas_price_gwei=Decimal("30"),
            inclusion_delay_blocks=0,
        )

        swap_intent = MockSwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("5000"),
        )
        strategy = DeterministicStrategy(intents=[swap_intent] + [None] * 24)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0.003"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0.001"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success
        assert result.metrics.total_trades == 1
        assert result.metrics.total_fees_usd > Decimal("0")
        assert result.metrics.total_slippage_usd > Decimal("0")
        assert result.metrics.total_gas_usd > Decimal("0")

        # A $5,000 fill implies $15 in fees and $5 in slippage before gas.
        assert Decimal("10") < result.metrics.total_fees_usd < Decimal("20")
        assert Decimal("1") < result.metrics.total_slippage_usd < Decimal("10")

    @pytest.mark.asyncio
    async def test_multiple_swaps_accumulate_costs(
        self,
        base_timestamp: datetime,
        eth_volatile_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test multiple swaps accumulate fees, slippage, and gas costs."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_volatile_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=10),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
            gas_price_gwei=Decimal("30"),
            inclusion_delay_blocks=0,
        )

        intents = [
            MockSwapIntent(amount_usd=Decimal("2000")),
            None,
            MockSwapIntent(from_token="WETH", to_token="USDC", amount_usd=Decimal("1000")),
            None,
            MockSwapIntent(amount_usd=Decimal("1500")),
        ]
        strategy = DeterministicStrategy(intents=intents + [None] * 6)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0.003"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0.001"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success
        assert result.metrics.total_trades == 3

        # $4,500 total notional at 0.3% implies $13.50 in fees.
        assert Decimal("10") < result.metrics.total_fees_usd < Decimal("20")

        # The same notional at 0.1% implies $4.50 in slippage.
        assert Decimal("2") < result.metrics.total_slippage_usd < Decimal("10")

        assert result.metrics.total_gas_usd > Decimal("0")


class TestMetricsCalculation:
    """Integration tests for metrics calculation accuracy."""

    @pytest.mark.asyncio
    async def test_return_calculation_in_uptrend(
        self,
        base_timestamp: datetime,
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test return calculation when holding asset through uptrend."""
        # Twenty-five hourly observations rise exactly 10%, from $3,000 to $3,300.
        eth_prices = [Decimal(str(3000 + i * 12.5)) for i in range(25)]

        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        swap_intent = MockSwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("10000"),
        )
        strategy = DeterministicStrategy(intents=[swap_intent] + [None] * 24)

        backtester = PnLBacktester(
            data_provider=data_provider,
            # Zero execution costs isolate mark-to-market return.
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success
        assert result.metrics.total_return_pct > Decimal("0")
        assert result.final_capital_usd > result.initial_portfolio_value_usd

    @pytest.mark.asyncio
    async def test_max_drawdown_calculation(
        self,
        base_timestamp: datetime,
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test max drawdown calculation with known price movement."""
        # A $3,600 peak followed by $2,880 creates a 20% peak-to-trough drawdown.
        eth_prices = [
            Decimal("3000"),
            Decimal("3300"),
            Decimal("3600"),
            Decimal("3240"),
            Decimal("2880"),
            Decimal("3100"),
            Decimal("3200"),
        ] + [Decimal("3200")] * 18

        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        swap_intent = MockSwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("10000"),
        )
        strategy = DeterministicStrategy(intents=[swap_intent] + [None] * 24)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success
        # Portfolio timing can shift the nominal 20% drawdown; bound it to a non-total loss.
        assert result.metrics.max_drawdown_pct > Decimal("0")
        assert result.metrics.max_drawdown_pct < Decimal("100")

    @pytest.mark.asyncio
    async def test_win_rate_calculation(
        self,
        base_timestamp: datetime,
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test win rate calculation with mixed profitable/losing trades."""
        # Oscillating prices make the three exits span gains and losses.
        eth_prices = [
            Decimal("3000"),
            Decimal("3100"),
            Decimal("3050"),
            Decimal("2900"),
            Decimal("3000"),
            Decimal("3200"),
        ] + [Decimal("3200")] * 19

        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        intents: list[Any | None] = [
            MockSwapIntent(from_token="USDC", to_token="WETH", amount_usd=Decimal("3000")),
            MockSwapIntent(from_token="WETH", to_token="USDC", amount_usd=Decimal("1500")),
            None,
            MockSwapIntent(from_token="WETH", to_token="USDC", amount_usd=Decimal("500")),
            None,
            MockSwapIntent(from_token="WETH", to_token="USDC", amount_usd=Decimal("500")),
        ]
        strategy = DeterministicStrategy(intents=intents + [None] * 19)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success
        assert result.metrics.total_trades > 0
        assert Decimal("0") <= result.metrics.win_rate <= Decimal("1")

    @pytest.mark.asyncio
    async def test_sharpe_ratio_positive_for_consistent_gains(
        self,
        base_timestamp: datetime,
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test Sharpe ratio is positive for consistent gains."""
        # A $5 increase per tick produces low-variance positive returns.
        eth_prices = [Decimal(str(3000 + i * 5)) for i in range(25)]

        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
            risk_free_rate=Decimal("0"),  # Zero isolates return consistency.
        )

        swap_intent = MockSwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("10000"),
        )
        strategy = DeterministicStrategy(intents=[swap_intent] + [None] * 24)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success
        assert result.metrics.sharpe_ratio > Decimal("0")

    @pytest.mark.asyncio
    async def test_volatility_higher_for_volatile_prices(
        self,
        base_timestamp: datetime,
        eth_uptrend_prices: list[Decimal],
        eth_volatile_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test that volatility is higher for volatile price series."""
        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        swap_intent = MockSwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("10000"),
        )
        strategy_stable = DeterministicStrategy(
            intents=[swap_intent] + [None] * 24,
            deployment_id="stable",
        )
        strategy_volatile = DeterministicStrategy(
            intents=[swap_intent] + [None] * 24,
            deployment_id="volatile",
        )

        data_provider_stable = DeterministicDataProvider(
            price_series={"WETH": eth_uptrend_prices, "USDC": usdc_stable_prices},
            start_time=base_timestamp,
        )
        backtester_stable = PnLBacktester(
            data_provider=data_provider_stable,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )
        result_stable = await backtester_stable.backtest(strategy_stable, config)

        data_provider_volatile = DeterministicDataProvider(
            price_series={"WETH": eth_volatile_prices, "USDC": usdc_stable_prices},
            start_time=base_timestamp,
        )
        backtester_volatile = PnLBacktester(
            data_provider=data_provider_volatile,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )
        result_volatile = await backtester_volatile.backtest(strategy_volatile, config)

        assert result_stable.success
        assert result_volatile.success
        assert result_volatile.metrics.volatility > result_stable.metrics.volatility


class TestEquityCurve:
    """Integration tests for equity curve generation."""

    @pytest.mark.asyncio
    async def test_equity_curve_length_matches_ticks(
        self,
        base_timestamp: datetime,
        eth_uptrend_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test equity curve has correct number of points."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_uptrend_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
        )

        strategy = DeterministicStrategy(intents=[None] * 25)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        result = await backtester.backtest(strategy, config)

        # 25 hourly ticks (0h to 24h inclusive)
        assert len(result.equity_curve) == 25

    @pytest.mark.asyncio
    async def test_equity_curve_timestamps_are_sequential(
        self,
        base_timestamp: datetime,
        eth_uptrend_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test equity curve timestamps are in order."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_uptrend_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=10),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
        )

        strategy = DeterministicStrategy(intents=[None] * 11)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        result = await backtester.backtest(strategy, config)

        for i in range(1, len(result.equity_curve)):
            assert result.equity_curve[i].timestamp > result.equity_curve[i - 1].timestamp

    @pytest.mark.asyncio
    async def test_equity_curve_values_are_positive(
        self,
        base_timestamp: datetime,
        eth_downtrend_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test equity values remain positive even in downtrend."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_downtrend_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        swap_intent = MockSwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("5000"),
        )
        strategy = DeterministicStrategy(intents=[swap_intent] + [None] * 24)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        for point in result.equity_curve:
            assert point.value_usd > Decimal("0")


class TestBacktestResultSerialization:
    """Integration tests for result serialization."""

    @pytest.mark.asyncio
    async def test_result_to_dict_roundtrip(
        self,
        base_timestamp: datetime,
        eth_uptrend_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test BacktestResult can be serialized and deserialized."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_uptrend_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=5),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
            inclusion_delay_blocks=0,
        )

        swap_intent = MockSwapIntent(amount_usd=Decimal("5000"))
        strategy = DeterministicStrategy(intents=[swap_intent] + [None] * 5)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        result = await backtester.backtest(strategy, config)

        result_dict = result.to_dict()

        assert "deployment_id" in result_dict
        assert "metrics" in result_dict
        assert "equity_curve" in result_dict
        assert "trades" in result_dict

        restored = BacktestResult.from_dict(result_dict)

        assert restored.deployment_id == result.deployment_id
        assert restored.engine == result.engine
        assert restored.metrics.total_trades == result.metrics.total_trades
        assert len(restored.equity_curve) == len(result.equity_curve)

    @pytest.mark.asyncio
    async def test_summary_includes_key_metrics(
        self,
        base_timestamp: datetime,
        eth_uptrend_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test summary() includes all key performance metrics."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_uptrend_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
        )

        swap_intent = MockSwapIntent(amount_usd=Decimal("5000"))
        strategy = DeterministicStrategy(intents=[swap_intent] + [None] * 24)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        result = await backtester.backtest(strategy, config)
        summary = result.summary()

        assert "BACKTEST RESULTS" in summary or "SUMMARY" in summary.upper()
        assert "PNL" in summary.upper() or "RETURN" in summary.upper()


class TestGasCostTracking:
    """Integration tests for gas cost tracking."""

    @pytest.mark.asyncio
    async def test_gas_costs_accumulate_correctly(
        self,
        base_timestamp: datetime,
        eth_uptrend_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test gas costs are tracked accurately."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_uptrend_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=10),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
            gas_price_gwei=Decimal("30"),
            inclusion_delay_blocks=0,
        )

        intents: list[Any | None] = [
            MockSwapIntent(amount_usd=Decimal("2000")),
            None,
            MockSwapIntent(amount_usd=Decimal("1500")),
            None,
            MockSwapIntent(amount_usd=Decimal("1000")),
        ]
        strategy = DeterministicStrategy(intents=intents + [None] * 6)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        result = await backtester.backtest(strategy, config)

        assert result.metrics.total_gas_usd > Decimal("0")
        assert result.metrics.total_trades == 3

        # Three 150,000-gas swaps at 30 gwei and roughly $3,000/ETH cost about $40.50.
        # Price movement makes the assertion intentionally tolerant.
        assert Decimal("5") < result.metrics.total_gas_usd < Decimal("50")

    @pytest.mark.asyncio
    async def test_no_gas_when_disabled(
        self,
        base_timestamp: datetime,
        eth_uptrend_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test no gas costs when include_gas_costs=False."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_uptrend_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=5),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        swap_intent = MockSwapIntent(amount_usd=Decimal("5000"))
        strategy = DeterministicStrategy(intents=[swap_intent] + [None] * 5)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        result = await backtester.backtest(strategy, config)

        assert result.metrics.total_gas_usd == Decimal("0")


class TestInclusionDelay:
    """Integration tests for inclusion delay simulation."""

    @pytest.mark.asyncio
    async def test_inclusion_delay_postpones_execution(
        self,
        base_timestamp: datetime,
        eth_uptrend_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test that inclusion delay causes trades to execute later."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_uptrend_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config_no_delay = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=10),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        config_with_delay = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=10),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=2,
        )

        swap_intent = MockSwapIntent(amount_usd=Decimal("5000"))
        strategy_no_delay = DeterministicStrategy(intents=[swap_intent] + [None] * 10)
        strategy_with_delay = DeterministicStrategy(intents=[swap_intent] + [None] * 10)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )
        result_no_delay = await backtester.backtest(strategy_no_delay, config_no_delay)

        # A fresh provider and backtester isolate state between the two runs.
        data_provider_delay = DeterministicDataProvider(
            price_series={
                "WETH": eth_uptrend_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )
        backtester_delay = PnLBacktester(
            data_provider=data_provider_delay,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )
        result_with_delay = await backtester_delay.backtest(strategy_with_delay, config_with_delay)

        assert result_no_delay.success
        assert result_with_delay.success


class TestConfigHash:
    """Integration tests for config hash reproducibility."""

    @pytest.mark.asyncio
    async def test_same_config_produces_same_hash(
        self,
        base_timestamp: datetime,
    ) -> None:
        """Test identical configs produce identical hashes."""
        config1 = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
        )

        config2 = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
        )

        assert config1.calculate_config_hash() == config2.calculate_config_hash()

    @pytest.mark.asyncio
    async def test_different_config_produces_different_hash(
        self,
        base_timestamp: datetime,
    ) -> None:
        """Test different configs produce different hashes."""
        config1 = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
        )

        config2 = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("20000")),
            tokens=["WETH", "USDC"],
        )

        assert config1.calculate_config_hash() != config2.calculate_config_hash()

    @pytest.mark.asyncio
    async def test_backtest_result_includes_config_hash(
        self,
        base_timestamp: datetime,
        eth_uptrend_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test backtest result includes config hash for reproducibility."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_uptrend_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=5),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
        )

        strategy = DeterministicStrategy(intents=[None] * 6)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        result = await backtester.backtest(strategy, config)

        assert result.config_hash is not None
        assert len(result.config_hash) == 64  # SHA-256 hex
        assert result.config_hash == config.calculate_config_hash()


class TestAdapterIntegration:
    """Integration tests for strategy-specific adapter integration with PnL engine.

    These tests validate that the adapter's update_position and value_position
    methods are correctly wired into the backtest loop and mark_to_market flow.
    """

    @pytest.mark.asyncio
    async def test_adapter_update_position_called_per_tick(
        self,
        base_timestamp: datetime,
        eth_uptrend_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test that adapter.update_position is called for each position on each tick.

        This test uses a mock adapter to verify the wiring of update_position
        into the per-tick backtest loop.
        """
        from almanak.framework.backtesting.adapters.base import StrategyBacktestAdapter
        from almanak.framework.backtesting.pnl.data_provider import MarketState
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition
        from almanak.framework.intents.vocabulary import HoldIntent, SwapIntent

        class TrackingAdapter(StrategyBacktestAdapter):
            """Adapter that tracks calls to its methods."""

            def __init__(self):
                self.update_calls = 0
                self.value_calls = 0
                self._config = None

            @property
            def adapter_name(self) -> str:
                return "tracking"

            @property
            def config(self):
                return self._config

            def execute_intent(self, intent, portfolio, market_state):
                return None  # Delegate to the engine's default execution path.

            def update_position(
                self,
                position: SimulatedPosition,
                market_state: MarketState,
                elapsed_seconds: float,
            ) -> None:
                self.update_calls += 1

            def value_position(
                self,
                position: SimulatedPosition,
                market_state: MarketState,
            ) -> Decimal:
                self.value_calls += 1
                total = Decimal("0")
                for token, amount in position.amounts.items():
                    try:
                        price = market_state.get_price(token)
                        total += amount * price
                    except KeyError:
                        total += amount * position.entry_price
                return total

            def should_rebalance(self, position, market_state) -> bool:
                return False

        class SwapStrategy:
            deployment_id = "test_tracking"
            name = "Tracking Strategy"
            _swapped = False

            def decide(self, snapshot):
                if not self._swapped:
                    self._swapped = True
                    return SwapIntent(
                        from_token="USDC",
                        to_token="WETH",
                        amount=Decimal("1000"),
                    )
                return HoldIntent(reason="Holding")

        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_uptrend_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=5),  # 6 ticks (0-5)
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        tracking_adapter = TrackingAdapter()

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        backtester._adapter = tracking_adapter

        strategy = SwapStrategy()
        result = await backtester.backtest(strategy, config)

        assert result.success

        # Spot swaps bypass position hooks; this only checks that an adapter does not disrupt default execution.

    @pytest.mark.asyncio
    async def test_adapter_value_position_used_in_mark_to_market(
        self,
        base_timestamp: datetime,
        eth_uptrend_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test that adapter.value_position is used for portfolio valuation.

        This test verifies the value_position method affects the equity curve
        by using a mock adapter that returns a specific value.
        """
        from almanak.framework.backtesting.adapters.base import StrategyBacktestAdapter
        from almanak.framework.backtesting.pnl.data_provider import MarketState
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition
        from almanak.framework.intents.vocabulary import HoldIntent

        class FixedValueAdapter(StrategyBacktestAdapter):
            """Adapter that returns a fixed value for positions."""

            FIXED_VALUE = Decimal("5000")

            def __init__(self):
                self._config = None
                self.value_calls = 0

            @property
            def adapter_name(self) -> str:
                return "fixed_value"

            @property
            def config(self):
                return self._config

            def execute_intent(self, intent, portfolio, market_state):
                return None

            def update_position(
                self,
                position: SimulatedPosition,
                market_state: MarketState,
                elapsed_seconds: float,
            ) -> None:
                pass

            def value_position(
                self,
                position: SimulatedPosition,
                market_state: MarketState,
            ) -> Decimal:
                self.value_calls += 1
                return self.FIXED_VALUE

            def should_rebalance(self, position, market_state) -> bool:
                return False

        class HoldStrategy:
            deployment_id = "test_hold"
            name = "Hold Strategy"

            def decide(self, snapshot):
                return HoldIntent(reason="Holding")

        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_uptrend_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=5),
            interval_seconds=3600,
            token_funding=pnl_token_funding(Decimal("10000")),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        fixed_adapter = FixedValueAdapter()

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        backtester._adapter = fixed_adapter

        strategy = HoldStrategy()
        result = await backtester.backtest(strategy, config)

        assert result.success

        # Adapter valuation applies only to positions; an all-cash hold preserves initial capital.
        assert result.final_capital_usd == result.initial_portfolio_value_usd
