"""Unit tests for Compound V3 PnL Backtest Strategy on Ethereum.

Tests verify the strategy's decision logic for supply rate tracking
and the supply/withdraw lifecycle.

To run:
    uv run pytest strategies/demo/compound_v3_pnl_backtest_ethereum/tests/ -v
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from strategies.demo.compound_v3_pnl_backtest_ethereum import CompoundV3PnLBacktestStrategy


# =============================================================================
# Fixtures
# =============================================================================


def create_strategy(config: dict | None = None) -> CompoundV3PnLBacktestStrategy:
    """Create a CompoundV3PnLBacktestStrategy with test configuration."""
    with patch.object(CompoundV3PnLBacktestStrategy, "__init__", lambda self, *args, **kwargs: None):
        strategy = CompoundV3PnLBacktestStrategy.__new__(CompoundV3PnLBacktestStrategy)

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
    strategy._chain = "ethereum"
    strategy._wallet_address = "0x1234567890123456789012345678901234567890"
    strategy._strategy_id = "test-compound-pnl-eth"

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
    """Create a mock market that returns a specific lending rate."""
    market = MagicMock()

    if rate is None:
        # AttributeError is the typical backtester case: lending_rate() not wired up.
        market.lending_rate = MagicMock(side_effect=AttributeError("lending_rate not available"))
    else:
        market.lending_rate = MagicMock(return_value=float(rate))

    return market


def make_market_no_rate() -> MagicMock:
    """Create a mock market where lending_rate() raises (backtester context)."""
    return make_market_with_rate(None)


@pytest.fixture
def strategy() -> CompoundV3PnLBacktestStrategy:
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


# =============================================================================
# Rate-Based Decision Tests
# =============================================================================


class TestDecideWithRate:
    """Tests when lending_rate() is available (live/gateway context)."""

    def test_idle_high_rate_supplies(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test idle state supplies when rate > entry threshold."""
        market = make_market_with_rate(Decimal("0.05"))  # 5% > 3% threshold

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"

    def test_idle_low_rate_holds(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test idle state holds when rate < entry threshold."""
        market = make_market_with_rate(Decimal("0.01"))  # 1% < 3% threshold

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert strategy._state == "idle"

    def test_supplied_high_rate_holds(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test supplied state holds when rate > exit threshold."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        market = make_market_with_rate(Decimal("0.04"))  # 4% > 1% exit threshold

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_supplied_low_rate_withdraws(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test supplied state withdraws when rate < exit threshold."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        market = make_market_with_rate(Decimal("0.005"))  # 0.5% < 1% exit threshold

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "WITHDRAW"
        assert strategy._state == "withdrawing"


# =============================================================================
# Fallback Behavior Tests (PnL Backtester Context)
# =============================================================================


class TestDecideWithoutRate:
    """Tests when lending_rate() is unavailable (backtester context)."""

    def test_idle_no_rate_holds_by_default(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test safe default: holds when rate is unavailable (force_entry_if_no_rate=False)."""
        market = make_market_no_rate()
        assert strategy.force_entry_if_no_rate is False  # default must be safe

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert strategy._state == "idle"

    def test_idle_no_rate_supplies_when_force_entry_enabled(self) -> None:
        """Test explicit opt-in: supplies on first tick when force_entry_if_no_rate=True."""
        strategy = create_strategy({"force_entry_if_no_rate": True})
        market = make_market_no_rate()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"

    def test_supplied_no_rate_holds_until_max_ticks(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test fallback: holds until max_hold_ticks when rate is unavailable."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        strategy._ticks_held = 3
        market = make_market_no_rate()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert strategy._ticks_held == 4

    def test_supplied_no_rate_withdraws_at_max_ticks(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test fallback: withdraws when max_hold_ticks reached."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        strategy._ticks_held = 4  # max_hold_ticks=5, will reach 5 in decide()
        market = make_market_no_rate()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "WITHDRAW"
        assert strategy._state == "withdrawing"


# =============================================================================
# State Machine Tests
# =============================================================================


class TestStateMachine:
    """Tests for state machine transitions."""

    def test_complete_state_holds(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test complete state always holds."""
        strategy._state = "complete"
        market = make_market_with_rate(Decimal("0.10"))  # High rate, but still hold

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "complete" in intent.reason.lower()

    def test_transitional_states_hold(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test transitional states return hold (pending confirmation)."""
        for state in ("supplying", "withdrawing"):
            strategy._state = state
            market = make_market_with_rate(Decimal("0.05"))
            intent = strategy.decide(market)
            assert intent.intent_type.value == "HOLD"

    def test_supply_intent_uses_configured_amount(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test supply intent uses configured amount and market."""
        market = make_market_with_rate(Decimal("0.05"))

        intent = strategy.decide(market)

        assert intent.intent_type.value == "SUPPLY"
        assert intent.amount == Decimal("10000")
        assert intent.market_id == "usdc"
        assert intent.protocol == "compound_v3"


# =============================================================================
# on_intent_executed Tests
# =============================================================================


class TestOnIntentExecuted:
    """Tests for execution callbacks."""

    def test_successful_supply_advances_to_supplied(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test successful SUPPLY advances state."""
        strategy._state = "supplying"
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"

        strategy.on_intent_executed(intent, success=True, result=None)

        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("10000")
        assert strategy._ticks_held == 0

    def test_successful_withdraw_advances_to_complete(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test successful WITHDRAW advances state to complete."""
        strategy._state = "withdrawing"
        strategy._supplied_amount = Decimal("10000")
        intent = MagicMock()
        intent.intent_type.value = "WITHDRAW"

        strategy.on_intent_executed(intent, success=True, result=None)

        assert strategy._state == "complete"
        assert strategy._supplied_amount == Decimal("0")

    def test_failed_intent_reverts_state(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test failed intent reverts to previous stable state."""
        strategy._state = "supplying"
        strategy._previous_stable_state = "idle"
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"

        strategy.on_intent_executed(intent, success=False, result=None)

        assert strategy._state == "idle"


# =============================================================================
# Persistent State Tests
# =============================================================================


class TestPersistentState:
    """Tests for state serialization and loading."""

    def test_get_persistent_state(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test persistent state serialization."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        strategy._ticks_held = 3

        state = strategy.get_persistent_state()

        assert state["state"] == "supplied"
        assert state["supplied_amount"] == "10000"
        assert state["ticks_held"] == 3

    def test_load_persistent_state(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test persistent state loading."""
        state = {
            "state": "complete",
            "previous_stable_state": "supplied",
            "supplied_amount": "0",
            "ticks_held": 5,
        }

        strategy.load_persistent_state(state)

        assert strategy._state == "complete"
        assert strategy._previous_stable_state == "supplied"
        assert strategy._supplied_amount == Decimal("0")
        assert strategy._ticks_held == 5


# =============================================================================
# Teardown Tests
# =============================================================================


class TestTeardown:
    """Tests for teardown interface."""

    def test_generate_teardown_intents_with_position(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test teardown generates withdraw intent when position exists."""
        from almanak.framework.teardown import TeardownMode

        strategy._supplied_amount = Decimal("10000")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"
        assert intents[0].protocol == "compound_v3"

    def test_generate_teardown_intents_no_position(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test teardown returns empty list when no position."""
        from almanak.framework.teardown import TeardownMode

        strategy._supplied_amount = Decimal("0")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 0

    def test_get_open_positions_with_supply(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test get_open_positions returns supply position."""
        strategy._supplied_amount = Decimal("10000")
        strategy._chain = "ethereum"
        strategy.STRATEGY_NAME = "demo_compound_v3_pnl_backtest_ethereum"

        summary = strategy.get_open_positions()

        assert len(summary.positions) == 1
        assert summary.positions[0].position_type.value == "SUPPLY"
        assert summary.positions[0].protocol == "compound_v3"
        assert summary.positions[0].value_usd == Decimal("10000")

    def test_get_open_positions_empty(self, strategy: CompoundV3PnLBacktestStrategy) -> None:
        """Test get_open_positions returns empty when no supply."""
        strategy._supplied_amount = Decimal("0")
        strategy._chain = "ethereum"
        strategy.STRATEGY_NAME = "demo_compound_v3_pnl_backtest_ethereum"

        summary = strategy.get_open_positions()

        assert len(summary.positions) == 0
