"""Unit tests for crisis scenario backtesting with TraderJoe V2 LP on Avalanche.

First LP strategy used with crisis scenario backtesting -- all 3 prior crisis
backtests were swap/lending strategies (VIB-590, VIB-1819, VIB-1937).

Tests validate that the crisis backtest infrastructure works with LP intents
(LP_OPEN, LP_CLOSE) on Avalanche chain using deterministic mock data.

VIB-2033: Backtesting: Crisis scenario backtest TraderJoe V2 LP on Avalanche
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

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
from almanak.framework.backtesting.scenarios.crisis import (
    BLACK_THURSDAY,
    FTX_COLLAPSE,
    TERRA_COLLAPSE,
    CrisisScenario,
)
from almanak.framework.backtesting.scenarios.crisis_runner import (
    CrisisBacktestConfig,
    CrisisBacktestResult,
    build_crisis_metrics,
    run_crisis_backtest,
)


# =============================================================================
# Deterministic data provider for Avalanche crisis periods
# =============================================================================


class AvalancheCrisisDataProvider:
    """Data provider generating crash-style WAVAX price patterns for Avalanche.

    Simulates a crash pattern: decline -> trough -> partial recovery.
    Reports chain as 'avalanche' and supports WAVAX/USDC pair (TraderJoe context).
    """

    def __init__(
        self,
        crash_severity: Decimal = Decimal("0.4"),
        recovery_pct: Decimal = Decimal("0.5"),
        start_time: datetime | None = None,
        duration_hours: int = 168,
    ):
        self._crash_severity = crash_severity
        self._recovery_pct = recovery_pct
        self._start_time = start_time or datetime(2020, 3, 12, tzinfo=UTC)
        self._duration_hours = duration_hours
        self._interval_seconds = 3600

        self._wavax_prices = self._generate_crash_prices(
            initial=Decimal("30"), severity=crash_severity, recovery=recovery_pct
        )

    def _generate_crash_prices(
        self,
        initial: Decimal,
        severity: Decimal,
        recovery: Decimal,
    ) -> list[Decimal]:
        """Generate a crash -> trough -> recovery price series."""
        n = self._duration_hours + 1
        crash_end = int(n * 0.3)
        trough_end = int(n * 0.5)

        trough_price = initial * (Decimal("1") - severity)
        recovery_price = trough_price + (initial - trough_price) * recovery

        prices = []
        for i in range(n):
            if i <= crash_end:
                progress = Decimal(i) / Decimal(max(crash_end, 1))
                price = initial - (initial - trough_price) * progress
            elif i <= trough_end:
                offset = Decimal("0.5") if i % 2 == 0 else Decimal("-0.5")
                price = trough_price + offset
            else:
                progress = Decimal(i - trough_end) / Decimal(max(n - trough_end - 1, 1))
                price = trough_price + (recovery_price - trough_price) * progress
            prices.append(max(price, Decimal("0.10")))

        return prices

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        token = token.upper()
        if token in ("USDC", "USDT", "DAI"):
            return Decimal("1")
        delta = timestamp - self._start_time
        index = int(delta.total_seconds() / self._interval_seconds)
        if token in ("WAVAX", "AVAX"):
            series = self._wavax_prices
        else:
            raise ValueError(f"No price for {token}")
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
                    high=price * Decimal("1.01"),
                    low=price * Decimal("0.99"),
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
                try:
                    prices[token.upper()] = await self.get_price(token, current)
                except ValueError:
                    prices[token.upper()] = Decimal("1")
            market_state = MarketState(
                timestamp=current,
                prices=prices,
                chain="avalanche",
                block_number=40000000 + index * 100,
                gas_price_gwei=Decimal("25"),  # Avalanche C-chain gas
            )
            yield current, market_state
            index += 1
            current += timedelta(seconds=config.interval_seconds)

    @property
    def provider_name(self) -> str:
        return "avalanche_crisis_deterministic"

    @property
    def supported_tokens(self) -> list[str]:
        return ["WAVAX", "USDC"]

    @property
    def supported_chains(self) -> list[str]:
        return ["avalanche"]

    @property
    def min_timestamp(self) -> datetime | None:
        return self._start_time

    @property
    def max_timestamp(self) -> datetime | None:
        return self._start_time + timedelta(hours=self._duration_hours)

    async def verify_archive_access(self) -> bool:
        return True

    async def close(self) -> None:
        return None


# =============================================================================
# Mock LP strategy for crisis testing
# =============================================================================


@dataclass
class MockLPOpenIntent:
    """Mock TraderJoe LP open intent."""

    intent_type: str = "LP_OPEN"
    pool: str = "WAVAX/USDC/20"
    amount0: Decimal = Decimal("0.5")
    amount1: Decimal = Decimal("10")
    range_lower: Decimal = Decimal("25")
    range_upper: Decimal = Decimal("35")
    protocol: str = "traderjoe_v2"


@dataclass
class MockLPCloseIntent:
    """Mock TraderJoe LP close intent."""

    intent_type: str = "LP_CLOSE"
    position_id: str = "traderjoe_crisis_lp_0"
    pool: str = "WAVAX/USDC/20"
    protocol: str = "traderjoe_v2"


class TraderJoeLPCrisisTestStrategy:
    """LP range rebalancing strategy for crisis scenario testing.

    Opens LP positions around current price, rebalances when price moves
    beyond threshold. Mimics the real TraderJoeCrisisLPStrategy but without
    IntentStrategy base class dependencies (for unit test isolation).
    """

    def __init__(
        self,
        range_width_pct: Decimal = Decimal("0.15"),
        rebalance_threshold_pct: Decimal = Decimal("0.06"),
        amount_x: Decimal = Decimal("0.5"),
        amount_y: Decimal = Decimal("10"),
        strategy_id: str = "traderjoe_crisis_lp_test",
    ):
        self._range_width_pct = range_width_pct
        self._rebalance_threshold_pct = rebalance_threshold_pct
        self._amount_x = amount_x
        self._amount_y = amount_y
        self._strategy_id = strategy_id
        self._state = "idle"
        self._entry_price: Decimal | None = None
        self._rebalance_count = 0

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def decide(self, market: Any) -> Any | None:
        try:
            price = market.price("WAVAX")
        except (ValueError, KeyError):
            return None

        # idle -> open LP
        if self._state == "idle":
            self._state = "opening"
            self._entry_price = price
            half_width = price * self._range_width_pct / Decimal("2")
            return MockLPOpenIntent(
                amount0=self._amount_x,
                amount1=self._amount_y,
                range_lower=price - half_width,
                range_upper=price + half_width,
            )

        # active -> check rebalance
        if self._state == "active" and self._entry_price is not None:
            change_pct = abs(price - self._entry_price) / self._entry_price
            if change_pct >= self._rebalance_threshold_pct:
                self._state = "closing"
                return MockLPCloseIntent()
            return None  # HOLD

        # Auto-advance stuck states (PnL backtester doesn't call on_intent_executed)
        if self._state == "opening":
            self._state = "active"
        elif self._state == "closing":
            self._state = "idle"
            self._entry_price = None
            self._rebalance_count += 1

        return None


class HoldOnlyStrategy:
    """Strategy that never trades -- baseline for comparison."""

    def __init__(self, strategy_id: str = "hold_baseline_avalanche"):
        self._strategy_id = strategy_id

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def decide(self, market: Any) -> None:
        return None


# =============================================================================
# Helper
# =============================================================================


def _make_backtester(
    scenario: CrisisScenario,
    crash_severity: Decimal = Decimal("0.4"),
    recovery_pct: Decimal = Decimal("0.5"),
    zero_costs: bool = True,
) -> PnLBacktester:
    """Create a backtester with deterministic Avalanche data for a scenario."""
    data_provider = AvalancheCrisisDataProvider(
        start_time=scenario.start_date,
        duration_hours=scenario.duration_days * 24,
        crash_severity=crash_severity,
        recovery_pct=recovery_pct,
    )
    fee_kwargs = {"fee_pct": Decimal("0")} if zero_costs else {}
    slip_kwargs = {"slippage_pct": Decimal("0")} if zero_costs else {}
    return PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel(**fee_kwargs)},
        slippage_models={"default": DefaultSlippageModel(**slip_kwargs)},
    )


# =============================================================================
# Tests: Hold baseline on Avalanche
# =============================================================================


class TestCrisisTraderJoeHoldBaseline:
    """Hold-only baseline -- verify backtester works with Avalanche chain."""

    @pytest.mark.asyncio
    async def test_hold_during_black_thursday(self) -> None:
        """Hold-only strategy preserves capital during Black Thursday on Avalanche."""
        scenario = BLACK_THURSDAY
        backtester = _make_backtester(scenario, crash_severity=Decimal("0.5"))
        strategy = HoldOnlyStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="avalanche",
            tokens=["WAVAX", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert isinstance(result, CrisisBacktestResult)
        assert result.result.success, f"Backtest failed: {result.result.error}"
        assert result.scenario_name == "black_thursday"
        assert result.result.final_capital_usd == Decimal("10000")
        assert result.result.metrics.total_trades == 0

    @pytest.mark.asyncio
    async def test_hold_during_ftx_collapse(self) -> None:
        """Hold-only through FTX collapse on Avalanche."""
        scenario = FTX_COLLAPSE
        backtester = _make_backtester(scenario, crash_severity=Decimal("0.25"))
        strategy = HoldOnlyStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="avalanche",
            tokens=["WAVAX", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.scenario_name == "ftx_collapse"
        assert result.result.final_capital_usd == Decimal("10000")


# =============================================================================
# Tests: TraderJoe LP rebalancing during crises on Avalanche
# =============================================================================


class TestCrisisTraderJoeLPTrading:
    """Tests with active TraderJoe V2 LP trading during crisis scenarios."""

    @pytest.mark.asyncio
    async def test_lp_strategy_during_black_thursday(self) -> None:
        """LP rebalancing strategy trades during Black Thursday on Avalanche."""
        scenario = BLACK_THURSDAY
        backtester = _make_backtester(
            scenario, crash_severity=Decimal("0.5"), recovery_pct=Decimal("0.3")
        )
        strategy = TraderJoeLPCrisisTestStrategy(
            range_width_pct=Decimal("0.15"),
            rebalance_threshold_pct=Decimal("0.06"),
        )

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="avalanche",
            tokens=["WAVAX", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success, f"Backtest failed: {result.result.error}"
        assert result.scenario_name == "black_thursday"
        # LP strategy should produce trades (rebalances) during severe crash
        assert result.result.metrics.total_trades >= 1

    @pytest.mark.asyncio
    async def test_lp_strategy_during_terra_collapse(self) -> None:
        """LP rebalancing during Terra/Luna collapse on Avalanche."""
        scenario = TERRA_COLLAPSE
        backtester = _make_backtester(
            scenario, crash_severity=Decimal("0.35"), recovery_pct=Decimal("0.2")
        )
        strategy = TraderJoeLPCrisisTestStrategy(
            range_width_pct=Decimal("0.15"),
            rebalance_threshold_pct=Decimal("0.06"),
        )

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="avalanche",
            tokens=["WAVAX", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.scenario_name == "terra_collapse"
        assert result.result.metrics.total_trades >= 1

    @pytest.mark.asyncio
    async def test_lp_strategy_during_ftx_collapse(self) -> None:
        """LP rebalancing during FTX collapse on Avalanche."""
        scenario = FTX_COLLAPSE
        backtester = _make_backtester(
            scenario, crash_severity=Decimal("0.25"), recovery_pct=Decimal("0.4")
        )
        strategy = TraderJoeLPCrisisTestStrategy(
            range_width_pct=Decimal("0.15"),
            rebalance_threshold_pct=Decimal("0.06"),
        )

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="avalanche",
            tokens=["WAVAX", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.scenario_name == "ftx_collapse"
        assert result.scenario_duration_days == 8
        assert result.result.metrics.total_trades >= 1

    @pytest.mark.asyncio
    async def test_rebalance_count_increases_with_severity(self) -> None:
        """Severe crashes should trigger more rebalances than mild ones."""
        trade_counts = []

        for severity, recovery in [
            (Decimal("0.5"), Decimal("0.3")),  # Severe
            (Decimal("0.1"), Decimal("0.9")),  # Mild
        ]:
            backtester = _make_backtester(
                BLACK_THURSDAY, crash_severity=severity, recovery_pct=recovery
            )
            strategy = TraderJoeLPCrisisTestStrategy(
                rebalance_threshold_pct=Decimal("0.06"),
            )

            result = await run_crisis_backtest(
                strategy=strategy,
                scenario=BLACK_THURSDAY,
                backtester=backtester,
                initial_capital_usd=Decimal("10000"),
                chain="avalanche",
                tokens=["WAVAX", "USDC"],
                include_gas_costs=False,
                inclusion_delay_blocks=0,
            )

            assert result.result.success
            trade_counts.append(result.result.metrics.total_trades)

        # Severe crash should trigger at least as many trades as mild
        assert trade_counts[0] >= trade_counts[1], (
            f"Severe crash ({trade_counts[0]} trades) should trigger >= mild ({trade_counts[1]} trades)"
        )


# =============================================================================
# Tests: Crisis metrics validation
# =============================================================================


class TestCrisisMetricsTraderJoeLP:
    """Validate crisis metrics are computed correctly for LP strategies."""

    @pytest.mark.asyncio
    async def test_crisis_metrics_contain_expected_fields(self) -> None:
        """CrisisBacktestResult should include all expected metric fields."""
        scenario = BLACK_THURSDAY
        backtester = _make_backtester(scenario, crash_severity=Decimal("0.4"))
        strategy = TraderJoeLPCrisisTestStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="avalanche",
            tokens=["WAVAX", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        # Check crisis metrics exist
        assert result.crisis_metrics is not None
        assert "max_drawdown_pct" in result.crisis_metrics or hasattr(result, "max_drawdown_during_crisis")
        assert result.scenario_duration_days == 7

    @pytest.mark.asyncio
    async def test_strategy_import_from_package(self) -> None:
        """Strategy should be importable from the demo package."""
        from almanak.demo_strategies.traderjoe_crisis_lp import TraderJoeCrisisLPStrategy

        assert TraderJoeCrisisLPStrategy is not None
