"""Tests for VIB-141: State machine recovery from failed intermediate intent.

Validates that multi-step strategies using the state machine pattern
(idle -> supplying -> supplied -> borrowing -> ...) correctly recover
when an intent fails during a transitional state, instead of getting
permanently stuck.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

# Patch timeline before importing strategies
_timeline_patch = patch("almanak.framework.api.timeline.add_event")
_timeline_patch.start()

from strategies.demo.aave_borrow.strategy import AaveBorrowStrategy
from strategies.demo.morpho_looping.strategy import MorphoLoopingStrategy

_timeline_patch.stop()


def _make_market() -> MagicMock:
    """Create a mock MarketSnapshot."""
    market = MagicMock()
    market.price.return_value = Decimal("3400")
    balance_mock = MagicMock()
    balance_mock.balance = Decimal("10.0")
    market.balance.return_value = balance_mock
    return market


def _make_intent(intent_type: str) -> MagicMock:
    """Create a mock Intent with the given type."""
    intent = MagicMock()
    intent.intent_type.value = intent_type
    intent.amount = Decimal("1.0")
    intent.borrow_amount = Decimal("1000")
    return intent


class TestMorphoLoopingRecovery:
    """Test state machine recovery for MorphoLoopingStrategy."""

    def _make_strategy(self) -> MorphoLoopingStrategy:
        """Create a strategy instance via proper constructor."""
        with patch("almanak.framework.api.timeline.add_event"):
            strategy = MorphoLoopingStrategy(
                config={
                    "market_id": "0x" + "b" * 64,
                    "collateral_token": "wstETH",
                    "borrow_token": "USDC",
                    "initial_collateral": "1.0",
                    "target_loops": 3,
                    "target_ltv": 0.75,
                    "min_health_factor": 1.5,
                    "swap_slippage": 0.005,
                },
                chain="ethereum",
                wallet_address="0x" + "1" * 40,
            )
        return strategy

    @patch("almanak.framework.api.timeline.add_event")
    def test_supply_failure_reverts_to_idle(self, mock_event):
        """When supply fails from idle state, strategy reverts to idle."""
        strategy = self._make_strategy()
        market = _make_market()

        # Step 1: decide() in idle state -> emits supply intent, transitions to "supplying"
        intent = strategy.decide(market)
        assert intent is not None
        assert strategy._loop_state == "supplying"
        assert strategy._previous_stable_state == "idle"

        # Step 2: supply execution FAILS
        supply_intent = _make_intent("SUPPLY_COLLATERAL")
        strategy.on_intent_executed(supply_intent, success=False, result=None)

        # Should revert to idle, NOT stay stuck in "supplying"
        assert strategy._loop_state == "idle"

    @patch("almanak.framework.api.timeline.add_event")
    def test_borrow_failure_reverts_to_supplied(self, mock_event):
        """When borrow fails, strategy reverts to supplied (not stuck in borrowing)."""
        strategy = self._make_strategy()
        market = _make_market()

        # Progress to "supplied" state
        strategy._loop_state = "supplied"
        strategy._previous_stable_state = "idle"
        strategy._total_collateral = Decimal("1.0")

        # decide() in supplied state -> emits borrow, transitions to "borrowing"
        intent = strategy.decide(market)
        assert intent is not None
        assert strategy._loop_state == "borrowing"
        assert strategy._previous_stable_state == "supplied"

        # Borrow execution FAILS
        borrow_intent = _make_intent("BORROW")
        strategy.on_intent_executed(borrow_intent, success=False, result=None)

        # Should revert to "supplied", NOT stay stuck in "borrowing"
        assert strategy._loop_state == "supplied"

    @patch("almanak.framework.api.timeline.add_event")
    def test_swap_failure_reverts_to_borrowed(self, mock_event):
        """When swap fails, strategy reverts to borrowed (not stuck in swapping)."""
        strategy = self._make_strategy()
        market = _make_market()

        # Progress to "borrowed" state
        strategy._loop_state = "borrowed"
        strategy._previous_stable_state = "supplied"
        strategy._total_collateral = Decimal("1.0")
        strategy._total_borrowed = Decimal("1000")
        strategy._pending_swap_amount = Decimal("1000")

        # decide() in borrowed state -> emits swap, transitions to "swapping"
        intent = strategy.decide(market)
        assert intent is not None
        assert strategy._loop_state == "swapping"
        assert strategy._previous_stable_state == "borrowed"

        # Swap execution FAILS
        swap_intent = _make_intent("SWAP")
        strategy.on_intent_executed(swap_intent, success=False, result=None)

        # Should revert to "borrowed", NOT stay stuck in "swapping"
        assert strategy._loop_state == "borrowed"

    @patch("almanak.framework.api.timeline.add_event")
    def test_recovery_allows_retry(self, mock_event):
        """After reverting, the next decide() call should re-attempt the same step."""
        strategy = self._make_strategy()
        market = _make_market()

        # Progress to supplied
        strategy._loop_state = "supplied"
        strategy._previous_stable_state = "idle"
        strategy._total_collateral = Decimal("1.0")

        # First attempt: borrow
        intent1 = strategy.decide(market)
        assert strategy._loop_state == "borrowing"

        # Borrow fails -> reverts to supplied
        strategy.on_intent_executed(_make_intent("BORROW"), success=False, result=None)
        assert strategy._loop_state == "supplied"

        # Second attempt: should re-emit borrow (not get stuck)
        intent2 = strategy.decide(market)
        assert intent2 is not None
        assert strategy._loop_state == "borrowing"

    @patch("almanak.framework.api.timeline.add_event")
    def test_swap_success_increments_loop_counter(self, mock_event):
        """Swap success should increment loop counters (moved from _handle_swapped_state)."""
        strategy = self._make_strategy()

        assert strategy._current_loop == 0
        assert strategy._loops_completed == 0

        # Simulate successful swap
        swap_intent = _make_intent("SWAP")
        strategy.on_intent_executed(swap_intent, success=True, result=None)

        assert strategy._loop_state == "swapped"
        assert strategy._current_loop == 1
        assert strategy._loops_completed == 1

    @patch("almanak.framework.api.timeline.add_event")
    def test_no_double_increment_on_supply_failure_after_swap(self, mock_event):
        """If supply fails after swap, reverting to swapped should NOT double-increment counters."""
        strategy = self._make_strategy()
        market = _make_market()

        # Simulate: swap succeeded (loop 1 complete)
        strategy._loop_state = "swapped"
        strategy._previous_stable_state = "borrowed"
        strategy._current_loop = 1
        strategy._loops_completed = 1
        strategy._pending_swap_amount = Decimal("0.85")
        strategy._total_collateral = Decimal("1.0")

        # decide() in swapped -> supply for next loop -> "supplying"
        intent = strategy.decide(market)
        assert strategy._loop_state == "supplying"
        assert strategy._previous_stable_state == "swapped"

        # Supply FAILS -> reverts to "swapped"
        strategy.on_intent_executed(_make_intent("SUPPLY_COLLATERAL"), success=False, result=None)
        assert strategy._loop_state == "swapped"

        # Counters should NOT have changed
        assert strategy._current_loop == 1
        assert strategy._loops_completed == 1

    @patch("almanak.framework.api.timeline.add_event")
    def test_decide_safety_net_for_unknown_transitional_state(self, mock_event):
        """If somehow in a transitional state during decide(), safety net reverts."""
        strategy = self._make_strategy()
        market = _make_market()

        # Manually set a transitional state (simulates crash recovery)
        strategy._loop_state = "borrowing"
        strategy._previous_stable_state = "supplied"

        # decide() should hit the safety net, revert, and HOLD
        intent = strategy.decide(market)
        assert intent is not None
        assert strategy._loop_state == "supplied"

    @patch("almanak.framework.api.timeline.add_event")
    def test_persistence_includes_previous_stable_state(self, mock_event):
        """Persistent state should include _previous_stable_state for crash recovery."""
        strategy = self._make_strategy()
        strategy._loop_state = "borrowing"
        strategy._previous_stable_state = "supplied"

        state = strategy.get_persistent_state()
        assert state["previous_stable_state"] == "supplied"

        # Test loading
        strategy2 = self._make_strategy()
        strategy2.load_persistent_state(state)
        assert strategy2._previous_stable_state == "supplied"


class TestAaveBorrowRecovery:
    """Test state machine recovery for AaveBorrowStrategy."""

    def _make_strategy(self) -> AaveBorrowStrategy:
        """Create a strategy instance via proper constructor."""
        with patch("almanak.framework.api.timeline.add_event"):
            strategy = AaveBorrowStrategy(
                config={
                    "collateral_token": "WETH",
                    "collateral_amount": "0.1",
                    "borrow_token": "USDC",
                    "ltv_target": 0.5,
                    "min_health_factor": 2.0,
                    "interest_rate_mode": "variable",
                },
                chain="arbitrum",
                wallet_address="0x" + "1" * 40,
            )
        return strategy

    @patch("almanak.framework.api.timeline.add_event")
    def test_supply_failure_reverts_to_idle(self, mock_event):
        """When supply fails, strategy reverts to idle."""
        strategy = self._make_strategy()
        market = _make_market()

        # decide() in idle -> supply -> "supplying"
        intent = strategy.decide(market)
        assert strategy._loop_state == "supplying"

        # Supply fails -> reverts to idle
        strategy.on_intent_executed(_make_intent("SUPPLY"), success=False, result=None)
        assert strategy._loop_state == "idle"

    @patch("almanak.framework.api.timeline.add_event")
    def test_borrow_failure_reverts_to_supplied(self, mock_event):
        """When borrow fails, strategy reverts to supplied (not all the way to idle)."""
        strategy = self._make_strategy()
        market = _make_market()

        # Progress to supplied
        strategy._loop_state = "supplied"
        strategy._previous_stable_state = "idle"
        strategy._supplied_amount = Decimal("0.1")

        # decide() in supplied -> borrow -> "borrowing"
        intent = strategy.decide(market)
        assert strategy._loop_state == "borrowing"
        assert strategy._previous_stable_state == "supplied"

        # Borrow fails -> reverts to supplied (not idle!)
        strategy.on_intent_executed(_make_intent("BORROW"), success=False, result=None)
        assert strategy._loop_state == "supplied"

    @patch("almanak.framework.api.timeline.add_event")
    def test_decide_safety_net(self, mock_event):
        """Safety net in decide() reverts transitional states."""
        strategy = self._make_strategy()
        market = _make_market()

        strategy._loop_state = "supplying"
        strategy._previous_stable_state = "idle"

        intent = strategy.decide(market)
        # Should revert to idle
        assert strategy._loop_state == "idle"

    @patch("almanak.framework.api.timeline.add_event")
    def test_persistence_includes_previous_stable_state(self, mock_event):
        """Persistent state should include _previous_stable_state."""
        strategy = self._make_strategy()
        strategy._previous_stable_state = "supplied"

        state = strategy.get_persistent_state()
        assert state["previous_stable_state"] == "supplied"

        strategy2 = self._make_strategy()
        strategy2.load_persistent_state(state)
        assert strategy2._previous_stable_state == "supplied"
