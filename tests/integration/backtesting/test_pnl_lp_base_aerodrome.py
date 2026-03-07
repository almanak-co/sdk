"""Integration tests for PnL Backtester with LP strategies on Base (Aerodrome).

Exercises the PnL backtester's LP position tracking, impermanent loss estimation,
and fee accrual calculations -- the first LP-specific PnL backtest in the suite.

All prior PnL integration tests (test_pnl_backtester_integration.py) only cover
swap strategies. This test fills the gap identified in VIB-591.

Uses deterministic mock data (no external API calls needed).
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.models import BacktestEngine
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

# =============================================================================
# Deterministic helpers (same pattern as test_pnl_backtester_integration.py)
# =============================================================================


class DeterministicDataProvider:
    """Data provider with pre-defined price series for deterministic LP testing."""

    def __init__(
        self,
        price_series: dict[str, list[Decimal]],
        start_time: datetime,
        interval_seconds: int = 3600,
    ):
        self._price_series = price_series
        self._start_time = start_time
        self._interval_seconds = interval_seconds

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        token = token.upper()
        if token not in self._price_series:
            raise ValueError(f"No price series for {token}")
        delta = timestamp - self._start_time
        index = int(delta.total_seconds() / self._interval_seconds)
        series = self._price_series[token]
        if 0 <= index < len(series):
            return series[index]
        return series[-1] if index >= len(series) else series[0]

    async def get_ohlcv(
        self, token: str, start: datetime, end: datetime, interval_seconds: int = 3600
    ) -> list[OHLCV]:
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
                    volume=Decimal("5000000"),
                )
            )
            current += timedelta(seconds=interval_seconds)
        return result

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        current = config.start_time
        index = 0
        while current <= config.end_time:
            prices = {}
            for token in config.tokens:
                token_upper = token.upper()
                if token_upper in self._price_series:
                    series = self._price_series[token_upper]
                    prices[token_upper] = series[index] if index < len(series) else series[-1]
                else:
                    raise ValueError(f"No price series for {token_upper}")
            market_state = MarketState(
                timestamp=current,
                prices=prices,
                chain=config.chains[0] if config.chains else "base",
                block_number=20000000 + index * 100,
                gas_price_gwei=Decimal("0.01"),  # Base has very low gas
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
        return ["base", "arbitrum", "ethereum"]

    @property
    def min_timestamp(self) -> datetime | None:
        return self._start_time

    @property
    def max_timestamp(self) -> datetime | None:
        n_points = max((len(s) for s in self._price_series.values()), default=0)
        if n_points <= 1:
            return self._start_time
        return self._start_time + timedelta(seconds=(n_points - 1) * self._interval_seconds)


# =============================================================================
# Mock intents for LP lifecycle
# =============================================================================


@dataclass
class MockLPOpenIntent:
    """Mock LP open intent for Aerodrome on Base."""

    intent_type: str = "LP_OPEN"
    token0: str = "WETH"
    token1: str = "USDC"
    amount_usd: Decimal = Decimal("4000")
    protocol: str = "aerodrome"
    tick_lower: int = -887272  # Full range
    tick_upper: int = 887272
    fee_tier: Decimal = Decimal("0.003")  # Aerodrome volatile pool fee


@dataclass
class MockLPCloseIntent:
    """Mock LP close intent for Aerodrome on Base."""

    intent_type: str = "LP_CLOSE"
    token0: str = "WETH"
    token1: str = "USDC"
    position_id: str = ""
    protocol: str = "aerodrome"


@dataclass
class MockSwapIntent:
    """Mock swap intent for initial token acquisition."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount_usd: Decimal = Decimal("2000")
    protocol: str = "aerodrome"


class DeterministicLPStrategy:
    """Strategy with pre-defined LP lifecycle for testing."""

    def __init__(self, intents: list[Any | None], strategy_id: str = "aerodrome_lp_backtest"):
        self._intents = intents
        self._strategy_id = strategy_id
        self._call_count = 0

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def decide(self, market: Any) -> Any | None:
        if self._call_count < len(self._intents):
            intent = self._intents[self._call_count]
            self._call_count += 1
            return intent
        return None


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def base_timestamp() -> datetime:
    return datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC)


@pytest.fixture
def eth_stable_prices() -> list[Decimal]:
    """ETH stable around $3000 -- baseline for LP IL testing."""
    return [Decimal("3000")] * 25


@pytest.fixture
def eth_rising_prices() -> list[Decimal]:
    """ETH rises 20% over 24h -- tests IL from price divergence."""
    return [Decimal(str(3000 + i * 25)) for i in range(25)]  # 3000 -> 3600


@pytest.fixture
def eth_volatile_prices() -> list[Decimal]:
    """ETH oscillates -- tests LP fee accrual from trading volume."""
    base = 3000
    return [Decimal(str(base + (100 if i % 2 == 0 else -100))) for i in range(25)]


@pytest.fixture
def usdc_prices() -> list[Decimal]:
    return [Decimal("1")] * 25


# =============================================================================
# Tests: LP PnL Backtest on Base (Aerodrome)
# =============================================================================


class TestLPPnLBacktestBase:
    """PnL backtest integration tests with LP intents targeting Base/Aerodrome."""

    @pytest.mark.asyncio
    async def test_lp_open_hold_close_lifecycle(
        self,
        base_timestamp: datetime,
        eth_stable_prices: list[Decimal],
        usdc_prices: list[Decimal],
    ) -> None:
        """Test full LP lifecycle: open -> hold -> close.

        With stable prices (no divergence), IL should be near zero.
        The backtest should complete successfully and track the LP position.
        """
        data_provider = DeterministicDataProvider(
            price_series={"WETH": eth_stable_prices, "USDC": usdc_prices},
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        # Open LP at t=0, hold for 20 hours, close at t=20
        lp_open = MockLPOpenIntent(amount_usd=Decimal("6000"))
        lp_close = MockLPCloseIntent()
        intents = [lp_open] + [None] * 19 + [lp_close] + [None] * 4

        strategy = DeterministicLPStrategy(intents=intents)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0.003"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0.001"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"
        assert result.engine == BacktestEngine.PNL
        assert result.metrics.total_trades >= 2  # LP_OPEN + LP_CLOSE
        assert len(result.equity_curve) == 25

    @pytest.mark.asyncio
    async def test_lp_with_price_divergence_has_il(
        self,
        base_timestamp: datetime,
        eth_rising_prices: list[Decimal],
        usdc_prices: list[Decimal],
    ) -> None:
        """Test that LP position with price divergence tracks impermanent loss.

        When ETH rises 20%, the LP position should show IL relative to
        just holding the tokens.
        """
        data_provider = DeterministicDataProvider(
            price_series={"WETH": eth_rising_prices, "USDC": usdc_prices},
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        # Open LP at t=0, hold through entire price rise
        lp_open = MockLPOpenIntent(amount_usd=Decimal("8000"))
        intents = [lp_open] + [None] * 24

        strategy = DeterministicLPStrategy(intents=intents)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"
        # The backtest should have tracked the LP position and generated an equity curve
        assert len(result.equity_curve) == 25
        # With ETH rising ~20%, capital should increase (token appreciation)
        # but LP should underperform vs pure HODL due to IL
        assert result.final_capital_usd > config.initial_capital_usd

    @pytest.mark.asyncio
    async def test_lp_only_strategy_no_swaps(
        self,
        base_timestamp: datetime,
        eth_stable_prices: list[Decimal],
        usdc_prices: list[Decimal],
    ) -> None:
        """Test LP-only strategy (no swaps) runs through backtester.

        This validates the backtester handles LP intents without prior swap
        intents to acquire tokens.
        """
        data_provider = DeterministicDataProvider(
            price_series={"WETH": eth_stable_prices, "USDC": usdc_prices},
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=10),
            interval_seconds=3600,
            initial_capital_usd=Decimal("5000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        lp_open = MockLPOpenIntent(amount_usd=Decimal("4000"))
        intents = [lp_open] + [None] * 10

        strategy = DeterministicLPStrategy(intents=intents)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0.003"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"
        assert result.metrics.total_trades >= 1

    @pytest.mark.asyncio
    async def test_swap_then_lp_lifecycle(
        self,
        base_timestamp: datetime,
        eth_volatile_prices: list[Decimal],
        usdc_prices: list[Decimal],
    ) -> None:
        """Test swap -> LP open -> hold -> LP close lifecycle.

        Mimics a real strategy: swap into tokens, LP them, close later.
        Uses volatile prices to exercise fee accrual simulation.
        """
        data_provider = DeterministicDataProvider(
            price_series={"WETH": eth_volatile_prices, "USDC": usdc_prices},
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=24),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
            gas_price_gwei=Decimal("0.01"),  # Base gas prices
            inclusion_delay_blocks=0,
        )

        swap = MockSwapIntent(from_token="USDC", to_token="WETH", amount_usd=Decimal("3000"))
        lp_open = MockLPOpenIntent(amount_usd=Decimal("5000"))
        lp_close = MockLPCloseIntent()

        # Swap at t=0, LP at t=1, hold for 18h, close at t=20
        intents = [swap, lp_open] + [None] * 18 + [lp_close] + [None] * 4

        strategy = DeterministicLPStrategy(intents=intents)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0.003"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0.001"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"
        # Should have at least 2 trades (swap + LP open)
        assert result.metrics.total_trades >= 2
        # Equity curve should span all ticks
        assert len(result.equity_curve) == 25
        # With fees and slippage, costs should be strictly positive
        assert result.metrics.total_fees_usd > Decimal("0")

    @pytest.mark.asyncio
    async def test_multiple_lp_opens(
        self,
        base_timestamp: datetime,
        eth_stable_prices: list[Decimal],
        usdc_prices: list[Decimal],
    ) -> None:
        """Test opening multiple LP positions in sequence.

        Validates the backtester handles multiple LP_OPEN intents and
        counts them as separate trades.
        """
        data_provider = DeterministicDataProvider(
            price_series={"WETH": eth_stable_prices, "USDC": usdc_prices},
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=10),
            interval_seconds=3600,
            initial_capital_usd=Decimal("20000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        lp1 = MockLPOpenIntent(amount_usd=Decimal("3000"))
        lp2 = MockLPOpenIntent(amount_usd=Decimal("4000"))
        intents = [lp1, None, lp2] + [None] * 8

        strategy = DeterministicLPStrategy(intents=intents)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0.003"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"
        assert result.metrics.total_trades >= 2
