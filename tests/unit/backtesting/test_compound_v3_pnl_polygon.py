"""Unit tests for Compound V3 PnL backtest strategy on Polygon (VIB-2034).

Tests the state machine, rate-based entry/exit, and fallback behavior
without requiring a gateway or Anvil fork.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.vocabulary import IntentType


def _create_strategy(config_overrides: dict | None = None):
    """Create a CompoundV3PnLPolygonStrategy with mocked framework dependencies."""
    from almanak.demo_strategies.compound_v3_pnl_polygon.strategy import CompoundV3PnLPolygonStrategy

    with patch.object(CompoundV3PnLPolygonStrategy, "__init__", lambda self, *a, **kw: None):
        strategy = CompoundV3PnLPolygonStrategy.__new__(CompoundV3PnLPolygonStrategy)

    config = {
        "supply_token": "USDC.e",
        "supply_amount": "10000",
        "market": "usdc_e",
        "entry_rate_threshold": "0.03",
        "exit_rate_threshold": "0.01",
        "max_hold_ticks": 30,
        "force_entry_if_no_rate": True,
    }
    if config_overrides:
        config.update(config_overrides)

    strategy._strategy_id = "test-compound-v3-pnl-polygon"
    strategy._chain = "polygon"
    strategy.supply_token = config["supply_token"]
    strategy.supply_amount = Decimal(str(config["supply_amount"]))
    strategy.market = config["market"]
    strategy.entry_rate_threshold = Decimal(str(config["entry_rate_threshold"]))
    strategy.exit_rate_threshold = Decimal(str(config["exit_rate_threshold"]))
    strategy.max_hold_ticks = int(config["max_hold_ticks"])
    strategy.force_entry_if_no_rate = config["force_entry_if_no_rate"]
    strategy._state = "idle"
    strategy._previous_stable_state = "idle"
    strategy._supplied_amount = Decimal("0")
    strategy._ticks_held = 0
    strategy._last_market_timestamp = None

    return strategy


def _mock_market(supply_rate: Decimal | None = None):
    """Create a mock MarketSnapshot with optional lending rate."""
    market = MagicMock()
    market.timestamp = None  # Explicit None — avoids auto-created MagicMock leaking into TimelineEvent

    if supply_rate is not None:
        rate = MagicMock()
        rate.apy_percent = supply_rate * Decimal("100")  # Convert to percentage form
        market.lending_rate = MagicMock(return_value=rate)
    else:
        market.lending_rate = MagicMock(side_effect=ValueError("No rate monitor configured"))

    return market


class TestIdleState:
    """Test behavior in idle state."""

    def test_idle_supplies_when_rate_above_threshold(self):
        strategy = _create_strategy()
        market = _mock_market(supply_rate=Decimal("0.05"))  # 5% > 3% threshold

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.SUPPLY
        assert strategy._state == "supplying"
        market.lending_rate.assert_called_once_with("compound_v3", "USDC.e", "supply")

    def test_idle_holds_when_rate_below_threshold(self):
        strategy = _create_strategy({"force_entry_if_no_rate": False})
        market = _mock_market(supply_rate=Decimal("0.02"))  # 2% < 3% threshold

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.HOLD

    def test_idle_supplies_on_fallback_when_forced(self):
        strategy = _create_strategy({"force_entry_if_no_rate": True})
        market = _mock_market(supply_rate=None)  # No rate available

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.SUPPLY
        assert strategy._state == "supplying"

    def test_idle_holds_when_no_rate_and_not_forced(self):
        strategy = _create_strategy({"force_entry_if_no_rate": False})
        market = _mock_market(supply_rate=None)

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.HOLD


class TestSupplyExecution:
    """Test supply intent execution callbacks."""

    def test_supply_success_transitions_to_supplied(self):
        strategy = _create_strategy()
        strategy._state = "supplying"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("10000")
        assert strategy._ticks_held == 0

    def test_supply_failure_reverts_to_idle(self):
        strategy = _create_strategy()
        strategy._state = "supplying"
        strategy._previous_stable_state = "idle"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"

        strategy.on_intent_executed(mock_intent, success=False, result=None)

        assert strategy._state == "idle"


class TestSuppliedState:
    """Test behavior while holding a supply position."""

    def test_supplied_holds_while_rate_above_exit_threshold(self):
        strategy = _create_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        market = _mock_market(supply_rate=Decimal("0.05"))

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.HOLD
        assert strategy._ticks_held == 1

    def test_supplied_withdraws_when_rate_below_exit_threshold(self):
        strategy = _create_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        market = _mock_market(supply_rate=Decimal("0.005"))  # 0.5% < 1% threshold

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.WITHDRAW
        assert strategy._state == "withdrawing"

    def test_supplied_withdraws_after_max_hold_ticks(self):
        strategy = _create_strategy({"max_hold_ticks": 5})
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        strategy._ticks_held = 4  # Will become 5 on this tick

        market = _mock_market(supply_rate=Decimal("0.05"))  # Rate still good

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.WITHDRAW

    def test_ticks_counter_increments(self):
        strategy = _create_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        market = _mock_market(supply_rate=Decimal("0.05"))

        strategy.decide(market)
        assert strategy._ticks_held == 1

        strategy.decide(market)
        assert strategy._ticks_held == 2


class TestWithdrawExecution:
    """Test withdraw intent execution callbacks."""

    def test_withdraw_success_transitions_to_complete(self):
        strategy = _create_strategy()
        strategy._state = "withdrawing"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "WITHDRAW"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == "complete"
        assert strategy._supplied_amount == Decimal("0")

    def test_withdraw_failure_reverts_to_supplied(self):
        strategy = _create_strategy()
        strategy._state = "withdrawing"
        strategy._previous_stable_state = "supplied"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "WITHDRAW"

        strategy.on_intent_executed(mock_intent, success=False, result=None)

        assert strategy._state == "supplied"


class TestCompleteState:
    """Test behavior after lifecycle completes."""

    def test_complete_holds(self):
        strategy = _create_strategy()
        strategy._state = "complete"
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.HOLD


class TestStatePersistence:
    """Test state round-trip serialization."""

    def test_state_round_trip(self):
        strategy = _create_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("10000")
        strategy._ticks_held = 15

        state = strategy.get_persistent_state()

        strategy2 = _create_strategy()
        strategy2.load_persistent_state(state)

        assert strategy2._state == "supplied"
        assert strategy2._supplied_amount == Decimal("10000")
        assert strategy2._ticks_held == 15


class TestTeardown:
    """Test teardown intent generation."""

    def test_teardown_with_supply(self):
        strategy = _create_strategy()
        strategy._supplied_amount = Decimal("10000")

        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 1
        assert intents[0].intent_type == IntentType.WITHDRAW

    def test_teardown_without_supply(self):
        strategy = _create_strategy()
        strategy._supplied_amount = Decimal("0")

        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 0


class TestChainSpecific:
    """Test Polygon-specific configuration."""

    def test_strategy_targets_polygon(self):
        strategy = _create_strategy()
        assert strategy.chain == "polygon"

    def test_supply_intent_targets_polygon(self):
        strategy = _create_strategy()
        market = _mock_market(supply_rate=None)

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.SUPPLY
        assert intent.chain == "polygon"
