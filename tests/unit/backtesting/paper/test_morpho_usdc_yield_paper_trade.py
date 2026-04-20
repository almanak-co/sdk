"""Unit tests for Morpho Blue USDC supply yield paper trading strategy.

Tests validate strategy logic (decide state machine, teardown, lifecycle)
without requiring Anvil or gateway connections.

VIB-2032: Backtesting: Paper trade Morpho Blue lending on Ethereum (Anvil fork)
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.intents.vocabulary import IntentType


# =============================================================================
# Helpers
# =============================================================================


def _make_strategy(
    supply_amount: str = "1000",
    withdraw_after_ticks: int = 8,
    resupply: bool = True,
):
    """Create a MorphoUSDCYieldPaperStrategy with mocked dependencies."""
    from almanak.demo_strategies.morpho_paper_usdc_yield.strategy import MorphoUSDCYieldPaperStrategy

    strategy = MorphoUSDCYieldPaperStrategy.__new__(MorphoUSDCYieldPaperStrategy)
    strategy._chain = "ethereum"
    strategy.market_id = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
    strategy.supply_token = "USDC"
    strategy.supply_amount = Decimal(supply_amount)
    strategy.withdraw_after_ticks = withdraw_after_ticks
    strategy.resupply_after_withdraw = resupply
    strategy._state = "idle"
    strategy._supplied_amount = Decimal("0")
    strategy._tick_count = 0
    strategy._ticks_since_supply = 0
    strategy._cycle_count = 0
    return strategy


def _make_market(usdc_price: Decimal = Decimal("1")):
    """Create a mock MarketSnapshot."""
    market = MagicMock()
    market.price = lambda token: usdc_price if token.upper() == "USDC" else Decimal("3000")
    bal = MagicMock()
    bal.balance = Decimal("50000")
    bal.balance_usd = Decimal("50000")
    market.balance = lambda token: bal
    return market


# =============================================================================
# Tests: Decide logic
# =============================================================================


class TestMorphoUSDCYieldDecide:
    """Test the strategy decide() state machine."""

    def test_first_tick_supplies(self):
        """First tick should emit SUPPLY intent."""
        strategy = _make_strategy()
        market = _make_market()
        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.SUPPLY
        assert intent.protocol == "morpho_blue"
        assert intent.token == "USDC"
        assert intent.amount == Decimal("1000")

    def test_second_tick_holds(self):
        """After supply, should hold to earn yield."""
        strategy = _make_strategy()
        market = _make_market()

        # Tick 1: supply
        strategy.decide(market)
        # Tick 2: auto-advance + hold
        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.HOLD

    def test_holds_until_withdraw_threshold(self):
        """Should hold for withdraw_after_ticks before withdrawing."""
        strategy = _make_strategy(withdraw_after_ticks=3)
        market = _make_market()

        # Tick 1: supply (state: idle -> supplying)
        intent = strategy.decide(market)
        assert intent.intent_type == IntentType.SUPPLY

        # Tick 2: auto-advance supplying -> supplied, ticks_since_supply=0 -> 1
        intent = strategy.decide(market)
        assert intent.intent_type == IntentType.HOLD

        # Tick 3: ticks_since_supply=2
        intent = strategy.decide(market)
        assert intent.intent_type == IntentType.HOLD

        # Tick 4: ticks_since_supply=3 == threshold -> WITHDRAW
        intent = strategy.decide(market)
        assert intent.intent_type == IntentType.WITHDRAW

    def test_resupply_after_withdraw(self):
        """With resupply=True, should supply again after withdraw."""
        strategy = _make_strategy(withdraw_after_ticks=2, resupply=True)
        market = _make_market()

        # Tick 1: supply (idle -> supplying)
        strategy.decide(market)
        # Tick 2: auto-advance supplying -> supplied, ticks_since_supply=1
        strategy.decide(market)
        # Tick 3: ticks_since_supply=2 == threshold -> WITHDRAW
        intent = strategy.decide(market)
        assert intent.intent_type == IntentType.WITHDRAW

        # Tick 4: auto-advance withdrawing -> idle
        strategy.decide(market)
        # Tick 5: supply again (cycle #2)
        intent = strategy.decide(market)
        assert intent.intent_type == IntentType.SUPPLY
        assert strategy._cycle_count == 2

    def test_no_resupply_stays_done(self):
        """With resupply=False, should stay done after withdraw."""
        strategy = _make_strategy(withdraw_after_ticks=2, resupply=False)
        market = _make_market()

        # Run through supply -> hold -> withdraw
        strategy.decide(market)  # supply
        strategy.decide(market)  # auto-advance
        strategy.decide(market)  # hold
        strategy.decide(market)  # withdraw

        # Auto-advance to done
        strategy.decide(market)
        # Should hold forever
        intent = strategy.decide(market)
        assert intent.intent_type == IntentType.HOLD
        assert "complete" in intent.reason.lower()


# =============================================================================
# Tests: Lifecycle events
# =============================================================================


class TestMorphoUSDCYieldLifecycle:
    """Test on_intent_executed callbacks and state transitions."""

    def test_supply_success_advances_state(self):
        """Successful supply should advance to 'supplied' state."""
        strategy = _make_strategy()
        market = _make_market()

        intent = strategy.decide(market)
        strategy.on_intent_executed(intent, success=True)

        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("1000")

    def test_withdraw_success_resets_amount(self):
        """Successful withdraw should reset supplied amount to 0."""
        strategy = _make_strategy(resupply=True)
        strategy._state = "withdrawing"
        strategy._supplied_amount = Decimal("1000")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "WITHDRAW"
        strategy.on_intent_executed(mock_intent, success=True)

        assert strategy._supplied_amount == Decimal("0")
        assert strategy._state == "idle"

    def test_supply_failure_resets_to_idle(self):
        """Failed supply should reset to idle for retry."""
        strategy = _make_strategy()
        strategy._state = "supplying"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"
        strategy.on_intent_executed(mock_intent, success=False)

        assert strategy._state == "idle"

    def test_supply_failure_then_decide_retries(self):
        """After failed supply, next decide() should re-supply (not auto-advance)."""
        strategy = _make_strategy()
        market = _make_market()

        # Tick 1: supply (idle -> supplying)
        intent = strategy.decide(market)
        assert intent.intent_type == IntentType.SUPPLY

        # Simulate failure callback
        strategy.on_intent_executed(intent, success=False)
        assert strategy._state == "idle"

        # Tick 2: should re-supply, not auto-advance to 'supplied'
        intent = strategy.decide(market)
        assert intent.intent_type == IntentType.SUPPLY

    def test_withdraw_failure_resets_to_supplied(self):
        """Failed withdraw should reset to supplied for retry."""
        strategy = _make_strategy()
        strategy._state = "withdrawing"
        strategy._supplied_amount = Decimal("1000")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "WITHDRAW"
        strategy.on_intent_executed(mock_intent, success=False)

        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("1000")

    def test_withdraw_failure_then_decide_retries(self):
        """After failed withdraw, next decide() should hold then retry withdraw."""
        strategy = _make_strategy(withdraw_after_ticks=1)
        market = _make_market()
        strategy._state = "withdrawing"
        strategy._supplied_amount = Decimal("1000")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "WITHDRAW"
        strategy.on_intent_executed(mock_intent, success=False)
        assert strategy._state == "supplied"

        # Next decide: ticks_since_supply increments to 1 >= threshold 1, so WITHDRAW again
        intent = strategy.decide(market)
        assert intent.intent_type == IntentType.WITHDRAW


# =============================================================================
# Tests: Teardown
# =============================================================================


class TestMorphoUSDCYieldTeardown:
    """Test teardown support."""

    def test_teardown_generates_withdraw_when_supplied(self):
        """With active supply, teardown should generate WITHDRAW intent."""
        from almanak.framework.teardown import TeardownMode

        strategy = _make_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("1000")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type == IntentType.WITHDRAW
        assert intents[0].protocol == "morpho_blue"

    def test_teardown_empty_when_idle(self):
        """When idle (no supply), teardown should generate no intents."""
        from almanak.framework.teardown import TeardownMode

        strategy = _make_strategy()
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0

    def test_open_positions_reports_supply(self):
        """get_open_positions should report active supply position."""
        strategy = _make_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("1000")

        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].protocol == "morpho_blue"
        assert summary.positions[0].value_usd == Decimal("1000")


# =============================================================================
# Tests: Import and persistence
# =============================================================================


class TestMorphoUSDCYieldMisc:
    """Test import, persistence, and config."""

    def test_strategy_import(self):
        """Strategy should be importable from the package."""
        from almanak.demo_strategies.morpho_paper_usdc_yield import MorphoUSDCYieldPaperStrategy

        assert MorphoUSDCYieldPaperStrategy is not None

    def test_persistent_state_roundtrip(self):
        """State should survive save/load cycle."""
        strategy = _make_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("500")
        strategy._tick_count = 5
        strategy._ticks_since_supply = 3
        strategy._cycle_count = 2

        state = strategy.get_persistent_state()

        strategy2 = _make_strategy()
        strategy2.load_persistent_state(state)

        assert strategy2._state == "supplied"
        assert strategy2._supplied_amount == Decimal("500")
        assert strategy2._tick_count == 5
        assert strategy2._ticks_since_supply == 3
        assert strategy2._cycle_count == 2
