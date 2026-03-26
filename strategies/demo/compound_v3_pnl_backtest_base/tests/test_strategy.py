"""Unit tests for Compound V3 PnL Backtest Strategy on Base.

Tests verify the strategy's decision logic for supply rate tracking
and the supply/withdraw lifecycle on Base chain.

To run:
    uv run pytest strategies/demo/compound_v3_pnl_backtest_base/tests/ -v
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from strategies.demo.compound_v3_pnl_backtest_base import CompoundV3PnLBacktestBaseStrategy


# =============================================================================
# Fixtures
# =============================================================================


def create_strategy(config: dict | None = None) -> CompoundV3PnLBacktestBaseStrategy:
    """Create a CompoundV3PnLBacktestBaseStrategy with test configuration."""
    with patch.object(CompoundV3PnLBacktestBaseStrategy, "__init__", lambda self, *args, **kwargs: None):
        strategy = CompoundV3PnLBacktestBaseStrategy.__new__(CompoundV3PnLBacktestBaseStrategy)

    default_config = {
        "supply_token": "USDC",
        "supply_amount": "10000",
        "market": "usdc",
        "entry_rate_threshold": "0.03",
        "exit_rate_threshold": "0.01",
        "max_hold_ticks": 5,
    }
    if config:
        default_config.update(config)

    strategy.config = default_config
    strategy._chain = "base"
    strategy._wallet_address = "0x1234567890123456789012345678901234567890"
    strategy._strategy_id = "test-compound-pnl-base"

    strategy.supply_token = default_config["supply_token"]
    strategy.supply_amount = Decimal(str(default_config["supply_amount"]))
    strategy.market = default_config["market"]
    strategy.entry_rate_threshold = Decimal(str(default_config["entry_rate_threshold"]))
    strategy.exit_rate_threshold = Decimal(str(default_config["exit_rate_threshold"]))
    strategy.max_hold_ticks = int(default_config["max_hold_ticks"])
    strategy.force_entry_if_no_rate = bool(default_config.get("force_entry_if_no_rate", False))

    strategy._state = "idle"
    strategy._previous_stable_state = "idle"
    strategy._supplied_amount = Decimal("0")
    strategy._ticks_held = 0

    return strategy


def make_market_with_rate(rate: Decimal | None) -> MagicMock:
    """Create a mock market that returns a specific lending rate.

    Args:
        rate: Fractional rate (e.g. Decimal("0.05") = 5%). Converted to
              a mock LendingRate with apy_percent in percentage form.
    """
    market = MagicMock()

    if rate is None:
        market.lending_rate = MagicMock(side_effect=ValueError("No rate monitor configured for MarketSnapshot."))
    else:
        # Mock LendingRate dataclass — apy_percent is in percentage form (e.g. 5.0 for 5%)
        mock_lending_rate = MagicMock()
        mock_lending_rate.apy_percent = rate * Decimal("100")
        market.lending_rate = MagicMock(return_value=mock_lending_rate)

    return market


def make_market_no_rate() -> MagicMock:
    """Create a mock market where lending_rate() raises (backtester context)."""
    return make_market_with_rate(None)


@pytest.fixture
def strategy() -> CompoundV3PnLBacktestBaseStrategy:
    """Create a strategy instance."""
    return create_strategy()


# =============================================================================
# Initialization Tests
# =============================================================================


class TestStrategyInit:
    """Tests for strategy initialization."""

    def test_defaults(self) -> None:
        """Test default configuration values."""
        s = create_strategy()
        assert s.supply_token == "USDC"
        assert s.supply_amount == Decimal("10000")
        assert s.market == "usdc"
        assert s.entry_rate_threshold == Decimal("0.03")
        assert s.exit_rate_threshold == Decimal("0.01")
        assert s._state == "idle"
        assert s._supplied_amount == Decimal("0")
        assert s._ticks_held == 0

    def test_chain_is_base(self) -> None:
        """Test strategy is configured for Base chain."""
        s = create_strategy()
        assert s._chain == "base"


# =============================================================================
# Rate-Based Decision Tests
# =============================================================================


class TestDecideWithRate:
    """Tests when lending_rate() is available."""

    def test_idle_high_rate_supplies(self, strategy: CompoundV3PnLBacktestBaseStrategy) -> None:
        """Supplies when rate > entry threshold."""
        market = make_market_with_rate(Decimal("0.05"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"

    def test_idle_low_rate_holds(self, strategy: CompoundV3PnLBacktestBaseStrategy) -> None:
        """Holds when rate < entry threshold."""
        market = make_market_with_rate(Decimal("0.01"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._state == "idle"

    def test_supplied_high_rate_holds(self, strategy: CompoundV3PnLBacktestBaseStrategy) -> None:
        """Holds supplied position when rate > exit threshold."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        market = make_market_with_rate(Decimal("0.04"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_supplied_low_rate_withdraws(self, strategy: CompoundV3PnLBacktestBaseStrategy) -> None:
        """Withdraws when rate < exit threshold."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        market = make_market_with_rate(Decimal("0.005"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "WITHDRAW"
        assert strategy._state == "withdrawing"


# =============================================================================
# Fallback Behavior Tests (PnL Backtester Context)
# =============================================================================


class TestDecideWithoutRate:
    """Tests when lending_rate() is unavailable (backtester context)."""

    def test_idle_no_rate_holds_when_force_disabled(self) -> None:
        """Holds when rate unavailable and force_entry_if_no_rate=False."""
        strategy = create_strategy({"force_entry_if_no_rate": False})
        market = make_market_no_rate()
        assert strategy.force_entry_if_no_rate is False
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_idle_no_rate_supplies_when_forced(self) -> None:
        """Supplies on first tick when force_entry_if_no_rate=True (default for PnL demo)."""
        strategy = create_strategy({"force_entry_if_no_rate": True})
        market = make_market_no_rate()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"

    def test_supplied_withdraws_at_max_ticks(self, strategy: CompoundV3PnLBacktestBaseStrategy) -> None:
        """Withdraws when max_hold_ticks reached."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        strategy._ticks_held = 4  # max_hold_ticks=5
        market = make_market_no_rate()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "WITHDRAW"


# =============================================================================
# State Machine & Callbacks
# =============================================================================


class TestStateMachine:
    """Tests for state transitions and callbacks."""

    def test_complete_state_holds(self, strategy: CompoundV3PnLBacktestBaseStrategy) -> None:
        """Complete state always holds."""
        strategy._state = "complete"
        market = make_market_with_rate(Decimal("0.10"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "complete" in intent.reason.lower()

    def test_supply_callback_advances(self, strategy: CompoundV3PnLBacktestBaseStrategy) -> None:
        """Successful supply advances to supplied."""
        strategy._state = "supplying"
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "supplied"

    def test_withdraw_callback_completes(self, strategy: CompoundV3PnLBacktestBaseStrategy) -> None:
        """Successful withdraw advances to complete."""
        strategy._state = "withdrawing"
        strategy._supplied_amount = Decimal("10000")
        intent = MagicMock()
        intent.intent_type.value = "WITHDRAW"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._state == "complete"
        assert strategy._supplied_amount == Decimal("0")

    def test_failed_intent_reverts(self, strategy: CompoundV3PnLBacktestBaseStrategy) -> None:
        """Failed intent reverts to previous stable state."""
        strategy._state = "supplying"
        strategy._previous_stable_state = "idle"
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "idle"

    def test_supply_intent_uses_chain(self, strategy: CompoundV3PnLBacktestBaseStrategy) -> None:
        """Supply intent references Base chain."""
        market = make_market_with_rate(Decimal("0.05"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"
        assert intent.chain == "base"
        assert intent.protocol == "compound_v3"


# =============================================================================
# Teardown Tests
# =============================================================================


class TestTeardown:
    """Tests for teardown interface."""

    def test_teardown_with_position(self, strategy: CompoundV3PnLBacktestBaseStrategy) -> None:
        """Teardown generates withdraw intent when position exists."""
        from almanak.framework.teardown import TeardownMode

        strategy._supplied_amount = Decimal("10000")
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"

    def test_teardown_no_position(self, strategy: CompoundV3PnLBacktestBaseStrategy) -> None:
        """Teardown returns empty when no position."""
        from almanak.framework.teardown import TeardownMode

        strategy._supplied_amount = Decimal("0")
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0

    def test_open_positions_with_supply(self, strategy: CompoundV3PnLBacktestBaseStrategy) -> None:
        """Reports supply position."""
        strategy._supplied_amount = Decimal("10000")
        strategy._chain = "base"
        strategy.STRATEGY_NAME = "demo_compound_v3_pnl_backtest_base"
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].position_type.value == "SUPPLY"
        assert summary.positions[0].chain == "base"
