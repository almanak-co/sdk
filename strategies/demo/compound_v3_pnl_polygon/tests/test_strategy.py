"""Unit tests for Compound V3 PnL Backtest Strategy on Polygon.

Tests verify the strategy's state machine for supply rate tracking,
the supply/withdraw lifecycle, teardown compliance, and Polygon-specific
configuration (USDC.e, usdc_e market).

To run:
    uv run pytest strategies/demo/compound_v3_pnl_polygon/tests/ -v
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.teardown import TeardownMode
from strategies.demo.compound_v3_pnl_polygon.strategy import CompoundV3PnLPolygonStrategy


# =============================================================================
# Helpers
# =============================================================================


def _create_strategy(config_overrides=None):
    """Create strategy with default Polygon config."""
    config = {
        "supply_token": "USDC.e",
        "supply_amount": "10000",
        "market": "usdc_e",
        "entry_rate_threshold": "0.03",
        "exit_rate_threshold": "0.01",
        "max_hold_ticks": 5,
        "force_entry_if_no_rate": False,
    }
    if config_overrides:
        config.update(config_overrides)
    return CompoundV3PnLPolygonStrategy(
        config=config,
        chain="polygon",
        wallet_address="0x" + "a" * 40,
    )


@pytest.fixture
def strategy():
    return _create_strategy()


def _make_market_with_rate(rate_pct: Decimal | None) -> MagicMock:
    """Create a mock MarketSnapshot with a lending rate.

    Args:
        rate_pct: Annual rate as percentage (e.g. Decimal("3.5") = 3.5%).
            None means lending_rate() is unavailable.
    """
    market = MagicMock()
    market.timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

    if rate_pct is not None:
        rate_obj = MagicMock()
        rate_obj.apy_percent = rate_pct
        market.lending_rate.return_value = rate_obj
    else:
        market.lending_rate.side_effect = AttributeError("lending_rate() not available")

    return market


def _supply_intent_mock():
    """Create a mock supply intent."""
    intent = MagicMock()
    intent.intent_type.value = "SUPPLY"
    return intent


def _withdraw_intent_mock():
    """Create a mock withdraw intent."""
    intent = MagicMock()
    intent.intent_type.value = "WITHDRAW"
    return intent


# =============================================================================
# Initialization
# =============================================================================


class TestInitialization:
    def test_default_config(self, strategy):
        assert strategy.supply_token == "USDC.e"
        assert strategy.supply_amount == Decimal("10000")
        assert strategy.market == "usdc_e"
        assert strategy.entry_rate_threshold == Decimal("0.03")
        assert strategy.exit_rate_threshold == Decimal("0.01")
        assert strategy.max_hold_ticks == 5
        assert strategy.force_entry_if_no_rate is False

    def test_custom_config(self):
        s = _create_strategy({
            "supply_amount": "5000",
            "entry_rate_threshold": "0.05",
            "max_hold_ticks": 10,
        })
        assert s.supply_amount == Decimal("5000")
        assert s.entry_rate_threshold == Decimal("0.05")
        assert s.max_hold_ticks == 10

    def test_force_entry_string_true(self):
        s = _create_strategy({"force_entry_if_no_rate": "true"})
        assert s.force_entry_if_no_rate is True

    def test_force_entry_string_false(self):
        s = _create_strategy({"force_entry_if_no_rate": "false"})
        assert s.force_entry_if_no_rate is False

    def test_initial_state_is_idle(self, strategy):
        assert strategy._state == "idle"
        assert strategy._supplied_amount == Decimal("0")
        assert strategy._ticks_held == 0


# =============================================================================
# State Machine: idle -> supplying -> supplied -> withdrawing -> complete
# =============================================================================


class TestIdleState:
    def test_supply_when_rate_above_threshold(self, strategy):
        """Rate > entry_rate_threshold triggers supply."""
        market = _make_market_with_rate(Decimal("5.0"))  # 5% > 3%
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"

    def test_hold_when_rate_below_threshold(self, strategy):
        """Rate < entry_rate_threshold holds."""
        market = _make_market_with_rate(Decimal("1.0"))  # 1% < 3%
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._state == "idle"

    def test_hold_when_rate_unavailable_and_no_force(self, strategy):
        """No rate data + force_entry_if_no_rate=False -> hold."""
        market = _make_market_with_rate(None)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_supply_when_rate_unavailable_and_force(self):
        """No rate data + force_entry_if_no_rate=True -> supply (backtest mode)."""
        s = _create_strategy({"force_entry_if_no_rate": True})
        market = _make_market_with_rate(None)
        intent = s.decide(market)
        assert intent.intent_type.value == "SUPPLY"
        assert s._state == "supplying"

    def test_supply_uses_compound_v3_protocol(self, strategy):
        market = _make_market_with_rate(Decimal("5.0"))
        intent = strategy.decide(market)
        assert intent.protocol == "compound_v3"


class TestSupplyingState:
    def test_hold_while_supplying(self, strategy):
        """Supplying state holds while waiting for confirmation."""
        strategy._state = "supplying"
        market = _make_market_with_rate(Decimal("5.0"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "supplying" in intent.reason.lower()


class TestSuppliedState:
    def test_hold_when_rate_above_exit(self, strategy):
        """Supplied + rate > exit threshold -> hold and accrue."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        market = _make_market_with_rate(Decimal("3.0"))  # 3% > 1%
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._ticks_held == 1

    def test_withdraw_when_rate_below_exit(self, strategy):
        """Supplied + rate < exit threshold -> withdraw."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        market = _make_market_with_rate(Decimal("0.5"))  # 0.5% < 1%
        intent = strategy.decide(market)
        assert intent.intent_type.value == "WITHDRAW"
        assert strategy._state == "withdrawing"

    def test_withdraw_at_max_hold_ticks(self, strategy):
        """Supplied + max_hold_ticks reached -> forced withdrawal."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        strategy._ticks_held = strategy.max_hold_ticks  # Already at max
        market = _make_market_with_rate(Decimal("5.0"))  # Rate is good but max reached
        intent = strategy.decide(market)
        assert intent.intent_type.value == "WITHDRAW"

    def test_ticks_held_increments(self, strategy):
        """Ticks held counter increments each decide()."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        for i in range(3):
            market = _make_market_with_rate(Decimal("5.0"))
            strategy.decide(market)
        assert strategy._ticks_held == 3


class TestCompleteState:
    def test_hold_when_complete(self, strategy):
        """Complete state always holds."""
        strategy._state = "complete"
        market = _make_market_with_rate(Decimal("5.0"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "complete" in intent.reason.lower()


# =============================================================================
# on_intent_executed
# =============================================================================


class TestOnIntentExecuted:
    def test_supply_success_transitions_to_supplied(self, strategy):
        strategy._state = "supplying"
        strategy.on_intent_executed(_supply_intent_mock(), success=True, result=None)
        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("10000")
        assert strategy._ticks_held == 0

    def test_withdraw_success_transitions_to_complete(self, strategy):
        strategy._state = "withdrawing"
        strategy._supplied_amount = Decimal("10000")
        strategy.on_intent_executed(_withdraw_intent_mock(), success=True, result=None)
        assert strategy._state == "complete"
        assert strategy._supplied_amount == Decimal("0")

    def test_supply_failure_reverts_to_idle(self, strategy):
        strategy._state = "supplying"
        strategy._previous_stable_state = "idle"
        strategy.on_intent_executed(_supply_intent_mock(), success=False, result=None)
        assert strategy._state == "idle"

    def test_withdraw_failure_reverts_to_supplied(self, strategy):
        strategy._state = "withdrawing"
        strategy._previous_stable_state = "supplied"
        strategy.on_intent_executed(_withdraw_intent_mock(), success=False, result=None)
        assert strategy._state == "supplied"


# =============================================================================
# Full lifecycle
# =============================================================================


class TestFullLifecycle:
    def test_idle_supply_hold_withdraw_complete(self):
        """Full lifecycle: idle -> supply -> hold -> withdraw -> complete."""
        s = _create_strategy({"max_hold_ticks": 3, "force_entry_if_no_rate": True})

        # Step 1: idle -> supply (rate unavailable, force=True)
        market = _make_market_with_rate(None)
        intent = s.decide(market)
        assert intent.intent_type.value == "SUPPLY"
        assert s._state == "supplying"

        # Confirm supply
        s.on_intent_executed(_supply_intent_mock(), success=True, result=None)
        assert s._state == "supplied"

        # Step 2: hold for 3 ticks
        for _ in range(2):
            market = _make_market_with_rate(Decimal("5.0"))
            intent = s.decide(market)
            assert intent.intent_type.value == "HOLD"

        # Step 3: max_hold_ticks reached -> withdraw
        market = _make_market_with_rate(Decimal("5.0"))
        intent = s.decide(market)
        assert intent.intent_type.value == "WITHDRAW"
        assert s._state == "withdrawing"

        # Confirm withdraw
        s.on_intent_executed(_withdraw_intent_mock(), success=True, result=None)
        assert s._state == "complete"

        # Step 4: hold in complete
        intent = s.decide(market)
        assert intent.intent_type.value == "HOLD"


# =============================================================================
# Persistent state
# =============================================================================


class TestPersistentState:
    def test_get_and_load_state(self, strategy):
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("5000")
        strategy._ticks_held = 7

        state = strategy.get_persistent_state()
        assert state["state"] == "supplied"
        assert state["supplied_amount"] == "5000"
        assert state["ticks_held"] == 7

        # Load into fresh strategy
        s2 = _create_strategy()
        s2.load_persistent_state(state)
        assert s2._state == "supplied"
        assert s2._supplied_amount == Decimal("5000")
        assert s2._ticks_held == 7


# =============================================================================
# Teardown
# =============================================================================


class TestTeardown:
    def test_open_positions_when_supplied(self, strategy):
        strategy._supplied_amount = Decimal("10000")
        strategy._state = "supplied"
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        pos = summary.positions[0]
        assert pos.position_type.value == "SUPPLY"
        assert pos.chain == "polygon"
        assert pos.protocol == "compound_v3"
        assert "USDC.e" in pos.details["asset"]

    def test_open_positions_when_idle(self, strategy):
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_teardown_generates_withdraw(self, strategy):
        strategy._supplied_amount = Decimal("10000")
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"
        assert intents[0].protocol == "compound_v3"

    def test_teardown_empty_when_no_position(self, strategy):
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0


# =============================================================================
# Status
# =============================================================================


class TestStatus:
    def test_status_contents(self, strategy):
        status = strategy.get_status()
        assert status["strategy"] == "demo_compound_v3_pnl_polygon"
        assert status["chain"] == "polygon"
        assert status["state"] == "idle"
        assert status["supplied_amount"] == "0"
        assert status["ticks_held"] == 0
