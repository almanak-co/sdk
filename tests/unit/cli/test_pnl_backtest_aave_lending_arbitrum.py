"""Tests for PnL backtest with Aave V3 lending strategy on Arbitrum.

Validates that the PnL backtest CLI works with lending intents:
1. Config resolves correctly for Aave V3 lending on Arbitrum
2. PnL backtest pipeline handles SUPPLY/BORROW/REPAY intent lifecycle
3. Backtest results contain lending-specific metrics
4. Dry-run mode shows lending strategy configuration
5. Strategy state machine transitions are correct through backtest

First PnL backtest CLI test for a lending strategy.
Kitchen Loop iteration 118, VIB-1694.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from almanak.framework.backtesting import PnLBacktestConfig
from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
)
from almanak.framework.cli.backtest import (
    SweepResult,
    backtest,
    run_sweep_backtest,
)


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def mock_pnl_config_lending_arbitrum() -> PnLBacktestConfig:
    """PnL config for Aave V3 lending on Arbitrum."""
    return PnLBacktestConfig(
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2025, 2, 1, tzinfo=UTC),
        interval_seconds=3600,
        initial_capital_usd=Decimal("10000"),
        chain="arbitrum",
        tokens=["WETH", "USDC"],
        gas_price_gwei=Decimal("0.1"),
    )


def _make_lending_backtest_result(
    sharpe: str = "0.8",
    total_return: str = "2.5",
    drawdown: str = "1.5",
    win_rate: str = "0.60",
    trades: int = 6,
) -> BacktestResult:
    """Create a BacktestResult representative of a lending strategy.

    Lending strategies typically have lower Sharpe/return than swap strategies
    but more consistent returns and lower drawdown.
    """
    return BacktestResult(
        engine=BacktestEngine.PNL,
        strategy_id="demo_aave_pnl_lending",
        start_time=datetime(2025, 1, 1, tzinfo=UTC),
        end_time=datetime(2025, 2, 1, tzinfo=UTC),
        trades=[],
        metrics=BacktestMetrics(
            total_trades=trades,
            win_rate=Decimal(win_rate),
            total_return_pct=Decimal(total_return),
            max_drawdown_pct=Decimal(drawdown),
            sharpe_ratio=Decimal(sharpe),
            sortino_ratio=Decimal("1.2"),
            calmar_ratio=Decimal("1.5"),
            profit_factor=Decimal("1.3"),
            annualized_return_pct=Decimal("30.0"),
            net_pnl_usd=Decimal("250"),
        ),
    )


# =============================================================================
# Config validation tests
# =============================================================================


class TestPnLBacktestLendingConfig:
    """Test that PnL backtest config resolves correctly for lending on Arbitrum."""

    def test_pnl_config_chain_is_arbitrum(
        self, mock_pnl_config_lending_arbitrum: PnLBacktestConfig
    ) -> None:
        assert mock_pnl_config_lending_arbitrum.chain == "arbitrum"

    def test_pnl_config_tokens_include_weth_usdc(
        self, mock_pnl_config_lending_arbitrum: PnLBacktestConfig
    ) -> None:
        """Lending strategy needs both supply token (WETH) and borrow token (USDC)."""
        assert "WETH" in mock_pnl_config_lending_arbitrum.tokens
        assert "USDC" in mock_pnl_config_lending_arbitrum.tokens

    def test_arbitrum_gas_price_low(
        self, mock_pnl_config_lending_arbitrum: PnLBacktestConfig
    ) -> None:
        """Arbitrum L2 gas should be much lower than mainnet."""
        assert mock_pnl_config_lending_arbitrum.gas_price_gwei < Decimal("1")

    def test_backtest_window_31_days(
        self, mock_pnl_config_lending_arbitrum: PnLBacktestConfig
    ) -> None:
        """31-day window gives enough ticks for supply -> borrow -> repay cycle."""
        duration = (
            mock_pnl_config_lending_arbitrum.end_time
            - mock_pnl_config_lending_arbitrum.start_time
        )
        assert duration.days == 31


# =============================================================================
# Lending strategy state machine tests
# =============================================================================


class TestAavePnLLendingStrategyLifecycle:
    """Test that the aave_pnl_lending strategy produces correct intent sequence."""

    def _make_strategy(self, config: dict[str, Any] | None = None) -> Any:
        """Import and instantiate the aave_pnl_lending strategy."""
        import importlib.util
        import os

        strategy_path = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "..",
            "strategies", "demo", "aave_pnl_lending", "strategy.py",
        )
        strategy_path = os.path.normpath(strategy_path)

        spec = importlib.util.spec_from_file_location("aave_pnl_lending", strategy_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load strategy from {strategy_path}")
        module = importlib.util.module_from_spec(spec)

        # Mock the decorator and base class
        with patch("almanak.framework.strategies.almanak_strategy", lambda **kw: lambda cls: cls):
            spec.loader.exec_module(module)

        strategy_cls = module.AavePnLLendingStrategy

        # Create with mock gateway client
        mock_config = config or {
            "supply_token": "WETH",
            "borrow_token": "USDC",
            "supply_amount": "0.01",
            "ltv_target": 0.4,
            "price_drop_threshold": 0.03,
            "price_rise_threshold": 0.05,
        }
        instance = object.__new__(strategy_cls)
        instance._config = mock_config
        instance._chain = "arbitrum"
        instance._strategy_id = "demo_aave_pnl_lending"
        instance.STRATEGY_NAME = "demo_aave_pnl_lending"

        # Manually call init logic
        instance.supply_token = mock_config.get("supply_token", "WETH")
        instance.borrow_token = mock_config.get("borrow_token", "USDC")
        instance.supply_amount = Decimal(str(mock_config.get("supply_amount", "0.01")))
        instance.ltv_target = Decimal(str(mock_config.get("ltv_target", "0.4")))
        instance.price_drop_threshold = Decimal(str(mock_config.get("price_drop_threshold", "0.03")))
        instance.price_rise_threshold = Decimal(str(mock_config.get("price_rise_threshold", "0.05")))
        instance._state = "idle"
        instance._previous_stable_state = "idle"
        instance._supplied_amount = Decimal("0")
        instance._borrowed_amount = Decimal("0")
        instance._reference_price = None

        # Mock get_config to return values from mock_config
        instance.get_config = lambda key, default=None: mock_config.get(key, default)

        return instance

    def _make_market(self, weth_price: float = 2000.0, usdc_price: float = 1.0) -> MagicMock:
        """Create a mock MarketSnapshot with configurable prices."""
        market = MagicMock()
        prices = {"WETH": Decimal(str(weth_price)), "USDC": Decimal(str(usdc_price))}
        market.price = MagicMock(side_effect=lambda token: prices[token])
        return market

    def test_first_tick_supplies_collateral(self) -> None:
        """Strategy should supply WETH on the first tick (idle state)."""
        strategy = self._make_strategy()
        market = self._make_market(weth_price=2000.0)

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"

    def test_hold_while_supplying(self) -> None:
        """Strategy should hold while waiting for supply to confirm."""
        strategy = self._make_strategy()
        strategy._state = "supplying"
        market = self._make_market(weth_price=2000.0)

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_borrow_on_price_drop(self) -> None:
        """Strategy should borrow when price drops below threshold."""
        strategy = self._make_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.01")
        strategy._reference_price = Decimal("2000")

        # Price drops 5% (> 3% threshold)
        market = self._make_market(weth_price=1900.0)
        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "BORROW"
        assert strategy._state == "borrowing"

    def test_hold_when_price_stable(self) -> None:
        """Strategy should hold when price hasn't moved enough."""
        strategy = self._make_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.01")
        strategy._reference_price = Decimal("2000")

        # Price moves only 1% (< 3% threshold)
        market = self._make_market(weth_price=1980.0)
        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_repay_on_price_rise(self) -> None:
        """Strategy should repay when price rises above threshold."""
        strategy = self._make_strategy()
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("100")
        strategy._reference_price = Decimal("1900")

        # Price rises 6% (> 5% threshold)
        market = self._make_market(weth_price=2014.0)
        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "REPAY"
        assert strategy._state == "repaying"

    def test_on_intent_executed_supply_success(self) -> None:
        """Successful supply should transition to 'supplied' state."""
        strategy = self._make_strategy()
        strategy._state = "supplying"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("0.01")

    def test_on_intent_executed_borrow_success(self) -> None:
        """Successful borrow should transition to 'borrowed' state."""
        strategy = self._make_strategy()
        strategy._state = "borrowing"
        strategy._previous_stable_state = "supplied"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "BORROW"
        mock_intent.borrow_amount = Decimal("50")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == "borrowed"
        assert strategy._borrowed_amount == Decimal("50")

    def test_on_intent_executed_repay_success(self) -> None:
        """Successful repay should return to 'supplied' state."""
        strategy = self._make_strategy()
        strategy._state = "repaying"
        strategy._previous_stable_state = "borrowed"
        strategy._borrowed_amount = Decimal("50")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "REPAY"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == "supplied"
        assert strategy._borrowed_amount == Decimal("0")

    def test_on_intent_executed_failure_reverts(self) -> None:
        """Failed intent should revert to previous stable state."""
        strategy = self._make_strategy()
        strategy._state = "borrowing"
        strategy._previous_stable_state = "supplied"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "BORROW"

        strategy.on_intent_executed(mock_intent, success=False, result=None)

        assert strategy._state == "supplied"

    def test_full_lifecycle_supply_borrow_repay(self) -> None:
        """Complete lifecycle: idle -> supply -> borrow -> repay -> supplied."""
        strategy = self._make_strategy()

        # Tick 1: Supply (idle -> supplying)
        market = self._make_market(weth_price=2000.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"

        # Confirm supply
        mock_supply = MagicMock()
        mock_supply.intent_type.value = "SUPPLY"
        strategy.on_intent_executed(mock_supply, success=True, result=None)
        assert strategy._state == "supplied"

        # Tick 2: Hold (price stable)
        market = self._make_market(weth_price=1990.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

        # Tick 3: Borrow (price drops 5%)
        market = self._make_market(weth_price=1900.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "BORROW"

        # Confirm borrow
        mock_borrow = MagicMock()
        mock_borrow.intent_type.value = "BORROW"
        mock_borrow.borrow_amount = Decimal("3.80")
        strategy.on_intent_executed(mock_borrow, success=True, result=None)
        assert strategy._state == "borrowed"

        # Tick 4: Hold (price stable after borrow)
        market = self._make_market(weth_price=1920.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

        # Tick 5: Repay (price rises 6% from borrow reference)
        market = self._make_market(weth_price=2014.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "REPAY"

        # Confirm repay
        mock_repay = MagicMock()
        mock_repay.intent_type.value = "REPAY"
        strategy.on_intent_executed(mock_repay, success=True, result=None)
        assert strategy._state == "supplied"
        assert strategy._borrowed_amount == Decimal("0")


# =============================================================================
# CLI-level PnL backtest tests
# =============================================================================


class TestPnLBacktestLendingExecution:
    """Test PnL backtest execution with lending strategy on Arbitrum."""

    @pytest.mark.asyncio
    async def test_sweep_with_lending_params(
        self, mock_pnl_config_lending_arbitrum: PnLBacktestConfig
    ) -> None:
        """Verify sweep passes lending config params through to backtester."""
        captured_configs: list[dict] = []

        class TrackingLendingStrategy:
            strategy_id = "demo_aave_pnl_lending"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config
                captured_configs.append(config.copy())

            def decide(self, market: Any) -> None:
                return None

        mock_result = _make_lending_backtest_result(trades=4)

        with patch("almanak.framework.cli.backtest.PnLBacktester") as mock_bt:
            mock_bt.return_value.backtest = AsyncMock(return_value=mock_result)

            result = await run_sweep_backtest(
                strategy_class=TrackingLendingStrategy,
                base_config={
                    "supply_token": "WETH",
                    "borrow_token": "USDC",
                    "supply_amount": "0.01",
                    "ltv_target": 0.4,
                    "price_drop_threshold": 0.03,
                    "price_rise_threshold": 0.05,
                },
                pnl_config=mock_pnl_config_lending_arbitrum,
                data_provider=MagicMock(),
                params={"ltv_target": "0.5"},
            )

        assert isinstance(result, SweepResult)
        assert result.total_trades == 4
        assert result.params == {"ltv_target": "0.5"}
        assert len(captured_configs) == 1
        assert captured_configs[0]["ltv_target"] == 0.5

    @pytest.mark.asyncio
    async def test_sweep_lending_returns_metrics(
        self, mock_pnl_config_lending_arbitrum: PnLBacktestConfig
    ) -> None:
        """Verify metrics extraction works for lending backtest."""

        class SimpleLendingStrategy:
            strategy_id = "demo_aave_pnl_lending"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

            def decide(self, market: Any) -> None:
                return None

        mock_result = _make_lending_backtest_result(
            sharpe="0.9", total_return="3.1", drawdown="1.2", trades=5
        )

        with patch("almanak.framework.cli.backtest.PnLBacktester") as mock_bt:
            mock_bt.return_value.backtest = AsyncMock(return_value=mock_result)

            result = await run_sweep_backtest(
                strategy_class=SimpleLendingStrategy,
                base_config={"supply_token": "WETH", "ltv_target": 0.4},
                pnl_config=mock_pnl_config_lending_arbitrum,
                data_provider=MagicMock(),
                params={"price_drop_threshold": "0.05"},
            )

        assert result.sharpe_ratio == Decimal("0.9")
        assert result.total_return_pct == Decimal("3.1")
        assert result.max_drawdown_pct == Decimal("1.2")
        assert result.total_trades == 5

    @pytest.mark.asyncio
    async def test_sweep_multiple_ltv_targets(
        self, mock_pnl_config_lending_arbitrum: PnLBacktestConfig
    ) -> None:
        """Verify sweep handles lending-specific parameter (LTV target)."""

        class LTVStrategy:
            strategy_id = "demo_aave_pnl_lending"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config
                self.ltv = config.get("ltv_target", 0.4)

            def decide(self, market: Any) -> None:
                return None

        results = []
        for ltv_val in ["0.3", "0.4", "0.5"]:
            mock_result = _make_lending_backtest_result(
                sharpe=str(float(ltv_val) * 2),
                total_return=str(float(ltv_val) * 5),
                trades=4,
            )

            with patch("almanak.framework.cli.backtest.PnLBacktester") as mock_bt:
                mock_bt.return_value.backtest = AsyncMock(return_value=mock_result)

                result = await run_sweep_backtest(
                    strategy_class=LTVStrategy,
                    base_config={
                        "supply_token": "WETH",
                        "borrow_token": "USDC",
                        "ltv_target": 0.4,
                    },
                    pnl_config=mock_pnl_config_lending_arbitrum,
                    data_provider=MagicMock(),
                    params={"ltv_target": ltv_val},
                )
                results.append(result)

        # Each should have different sharpe based on LTV
        sharpes = [r.sharpe_ratio for r in results]
        assert sharpes == [Decimal("0.6"), Decimal("0.8"), Decimal("1.0")]


# =============================================================================
# CLI dry-run tests
# =============================================================================


class TestPnLBacktestLendingDryRun:
    """Test PnL backtest dry-run with Aave lending strategy."""

    def test_dry_run_lending_strategy(self, cli_runner: CliRunner) -> None:
        """Dry run with lending strategy shows correct config."""
        result = cli_runner.invoke(
            backtest,
            [
                "pnl",
                "-s", "demo_aave_pnl_lending",
                "--start", "2025-01-01",
                "--end", "2025-02-01",
                "--chain", "arbitrum",
                "--tokens", "WETH,USDC",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "demo_aave_pnl_lending" in result.output
        assert "arbitrum" in result.output

    def test_dry_run_sweep_lending_params(self, cli_runner: CliRunner) -> None:
        """Dry run sweep with lending-specific params shows combinations."""
        result = cli_runner.invoke(
            backtest,
            [
                "sweep",
                "-s", "demo_aave_pnl_lending",
                "--start", "2025-01-01",
                "--end", "2025-02-01",
                "--chain", "arbitrum",
                "--tokens", "WETH,USDC",
                "--param", "ltv_target:0.3,0.4,0.5",
                "--param", "price_drop_threshold:0.03,0.05",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "demo_aave_pnl_lending" in result.output
        assert "Total combinations: 6" in result.output
        assert "Dry run - no backtests executed" in result.output


# =============================================================================
# State persistence tests
# =============================================================================


class TestAavePnLLendingPersistence:
    """Test that strategy state persists correctly through backtest."""

    def test_get_persistent_state_idle(self) -> None:
        """Idle strategy should persist default state."""
        helper = TestAavePnLLendingStrategyLifecycle()
        strategy = helper._make_strategy()

        state = strategy.get_persistent_state()
        assert state["state"] == "idle"
        assert state["supplied_amount"] == "0"
        assert state["borrowed_amount"] == "0"
        assert state["reference_price"] is None

    def test_get_persistent_state_borrowed(self) -> None:
        """Borrowed state should persist amounts and reference price."""
        helper = TestAavePnLLendingStrategyLifecycle()
        strategy = helper._make_strategy()
        strategy._state = "borrowed"
        strategy._supplied_amount = Decimal("0.01")
        strategy._borrowed_amount = Decimal("100")
        strategy._reference_price = Decimal("1900")

        state = strategy.get_persistent_state()
        assert state["state"] == "borrowed"
        assert state["supplied_amount"] == "0.01"
        assert state["borrowed_amount"] == "100"
        assert state["reference_price"] == "1900"

    def test_load_persistent_state_roundtrip(self) -> None:
        """State should survive save/load roundtrip."""
        helper = TestAavePnLLendingStrategyLifecycle()
        strategy = helper._make_strategy()

        # Set complex state
        strategy._state = "supplied"
        strategy._previous_stable_state = "idle"
        strategy._supplied_amount = Decimal("0.01")
        strategy._reference_price = Decimal("2100")

        saved_state = strategy.get_persistent_state()

        # Create fresh strategy and load state
        strategy2 = helper._make_strategy()
        strategy2.load_persistent_state(saved_state)

        assert strategy2._state == "supplied"
        assert strategy2._previous_stable_state == "idle"
        assert strategy2._supplied_amount == Decimal("0.01")
        assert strategy2._reference_price == Decimal("2100")


# =============================================================================
# Teardown tests
# =============================================================================


class TestAavePnLLendingTeardown:
    """Test teardown produces correct intent sequence for lending."""

    def test_teardown_with_supply_and_borrow(self) -> None:
        """Teardown should repay borrow first, then withdraw supply."""
        helper = TestAavePnLLendingStrategyLifecycle()
        strategy = helper._make_strategy()
        strategy._state = "borrowed"
        strategy._supplied_amount = Decimal("0.01")
        strategy._borrowed_amount = Decimal("50")

        mock_mode = MagicMock()
        intents = strategy.generate_teardown_intents(mock_mode)

        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"

    def test_teardown_supply_only(self) -> None:
        """Teardown with only supply should just withdraw."""
        helper = TestAavePnLLendingStrategyLifecycle()
        strategy = helper._make_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.01")
        strategy._borrowed_amount = Decimal("0")

        mock_mode = MagicMock()
        intents = strategy.generate_teardown_intents(mock_mode)

        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"

    def test_teardown_idle_produces_no_intents(self) -> None:
        """Teardown from idle should produce no intents."""
        helper = TestAavePnLLendingStrategyLifecycle()
        strategy = helper._make_strategy()

        mock_mode = MagicMock()
        intents = strategy.generate_teardown_intents(mock_mode)

        assert len(intents) == 0
