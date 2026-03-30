"""Tests for the Aave V3 Sweep Lending Polygon demo strategy.

Validates supply/borrow/repay state machine, threshold-based decisions,
sweep parameter overrides, and teardown with Aave V3 on Polygon.

Kitchen Loop iteration 134, VIB-2053.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from strategies.demo.aave_sweep_lending_polygon.strategy import AaveSweepLendingPolygonStrategy

    strat = AaveSweepLendingPolygonStrategy.__new__(AaveSweepLendingPolygonStrategy)
    strat.config = {}
    strat._chain = "polygon"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-aave-sweep-polygon"
    strat.supply_token = "WETH"
    strat.borrow_token = "USDC"
    strat.supply_amount = Decimal("0.01")
    strat.supply_rate_threshold = Decimal("4.0")
    strat.borrow_rate_threshold = Decimal("6.0")
    strat.ltv_target = Decimal("0.4")
    strat.max_borrow_cycles = 5
    strat._VALID_STATES = frozenset(
        {"idle", "supplying", "supplied", "borrowing", "borrowed", "repaying"}
    )
    strat._state = "idle"
    strat._previous_stable_state = "idle"
    strat._supplied_amount = Decimal("0")
    strat._borrowed_amount = Decimal("0")
    strat._borrow_cycles = 0
    strat._tick_count = 0
    strat._reference_price = None
    strat._previous_reference_price = None
    return strat


def _mock_market(weth_price: float = 3000.0, usdc_price: float = 1.0) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        if token == "WETH":
            return Decimal(str(weth_price))
        if token == "USDC":
            return Decimal(str(usdc_price))
        raise ValueError(f"Unexpected token: {token}")

    market.price = MagicMock(side_effect=price_fn)
    return market


class TestIdleState:
    def test_first_tick_supplies(self, strategy):
        """First tick should always supply (no reference price yet)."""
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"
        assert intent.token == "WETH"

    def test_supply_uses_aave_v3(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "aave_v3"

    def test_hold_when_price_unavailable(self, strategy):
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("no price"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_re_supply_gated_by_threshold(self, strategy):
        """After a cycle, re-supply only if price moved enough."""
        strategy._reference_price = Decimal("3000")
        market = _mock_market(weth_price=3010.0)  # ~0.3% change < 4% threshold
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "volatility" in intent.reason.lower()

    def test_re_supply_triggers_above_threshold(self, strategy):
        """Re-supply triggers when price moved beyond threshold."""
        strategy._reference_price = Decimal("3000")
        market = _mock_market(weth_price=3200.0)  # ~6.7% change > 4% threshold
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"


class TestSuppliedState:
    def test_borrow_when_stable_market(self, strategy):
        """Borrow when price is stable (volatility < borrow threshold)."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.01")
        strategy._reference_price = Decimal("3000")
        market = _mock_market(weth_price=3010.0)  # ~0.3% < 6%
        intent = strategy.decide(market)
        assert intent.intent_type.value == "BORROW"

    def test_borrow_uses_variable_rate(self, strategy):
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.01")
        strategy._reference_price = Decimal("3000")
        market = _mock_market(weth_price=3010.0)
        intent = strategy.decide(market)
        assert intent.interest_rate_mode == "variable"

    def test_hold_when_volatile_market(self, strategy):
        """Hold when price volatility exceeds borrow threshold."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.01")
        strategy._reference_price = Decimal("3000")
        market = _mock_market(weth_price=3300.0)  # 10% > 6%
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_when_max_cycles_reached(self, strategy):
        strategy._state = "supplied"
        strategy._borrow_cycles = 5  # max
        strategy._reference_price = Decimal("3000")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Max borrow cycles" in intent.reason

    def test_post_repay_establishes_baseline_before_borrow(self, strategy):
        """After repay clears reference, first tick establishes baseline, second borrows."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.01")
        strategy._reference_price = None  # cleared by repay
        market = _mock_market(weth_price=3300.0)
        # First tick: establish baseline
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "baseline" in intent.reason.lower()
        assert strategy._reference_price == Decimal("3300.0")
        # Second tick: stable market -> borrow
        market2 = _mock_market(weth_price=3310.0)  # 0.3% < 6%
        intent2 = strategy.decide(market2)
        assert intent2.intent_type.value == "BORROW"


class TestBorrowedState:
    def test_repay_on_large_price_move(self, strategy):
        """Repay when price moves beyond 1.5x borrow threshold."""
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("10")
        strategy._reference_price = Decimal("3000")
        # 1.5 * 6% = 9%, need > 9% move
        market = _mock_market(weth_price=3300.0)  # 10% > 9%
        intent = strategy.decide(market)
        assert intent.intent_type.value == "REPAY"

    def test_hold_when_price_stable(self, strategy):
        """Hold borrowed position when price is stable."""
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("10")
        strategy._reference_price = Decimal("3000")
        market = _mock_market(weth_price=3050.0)  # ~1.7% < 9%
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"


class TestTransientStates:
    def test_hold_during_supplying(self, strategy):
        strategy._state = "supplying"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "supplying" in intent.reason

    def test_hold_during_borrowing(self, strategy):
        strategy._state = "borrowing"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_during_repaying(self, strategy):
        strategy._state = "repaying"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"


class TestOnIntentExecuted:
    def test_supply_success_transitions_to_supplied(self, strategy):
        strategy._state = "supplying"
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        strategy.on_intent_executed(intent, True, MagicMock())
        assert strategy._state == "supplied"
        assert strategy._previous_stable_state == "supplied"
        assert strategy._supplied_amount == Decimal("0.01")

    def test_borrow_success_transitions_to_borrowed(self, strategy):
        strategy._state = "borrowing"
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.borrow_amount = Decimal("10")
        strategy.on_intent_executed(intent, True, MagicMock())
        assert strategy._state == "borrowed"
        assert strategy._previous_stable_state == "borrowed"
        assert strategy._borrow_cycles == 1

    def test_repay_success_transitions_to_supplied(self, strategy):
        strategy._state = "repaying"
        strategy._borrowed_amount = Decimal("10")
        strategy._reference_price = Decimal("3000")
        intent = MagicMock()
        intent.intent_type.value = "REPAY"
        strategy.on_intent_executed(intent, True, MagicMock())
        assert strategy._state == "supplied"
        assert strategy._previous_stable_state == "supplied"
        assert strategy._borrowed_amount == Decimal("0")
        assert strategy._reference_price is None  # reset so next borrow uses fresh baseline

    def test_failure_reverts_to_previous_state(self, strategy):
        strategy._state = "supplying"
        strategy._previous_stable_state = "idle"
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        strategy.on_intent_executed(intent, False, MagicMock())
        assert strategy._state == "idle"

    def test_first_supply_failure_clears_reference_price(self, strategy):
        """After a failed first supply, reference price is cleared so retry is immediate."""
        strategy._state = "supplying"
        strategy._previous_stable_state = "idle"
        strategy._reference_price = Decimal("3000")  # set during decide()
        strategy._previous_reference_price = None  # first supply, no previous
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        strategy.on_intent_executed(intent, False, MagicMock())
        assert strategy._state == "idle"
        assert strategy._reference_price is None  # cleared so next tick retries immediately


class TestSweepParameters:
    def test_different_supply_threshold_changes_behavior(self, strategy):
        """Verify that changing supply_rate_threshold affects decisions."""
        strategy._reference_price = Decimal("3000")
        market = _mock_market(weth_price=3100.0)  # ~3.3% change

        # With default threshold (4.0%), should hold (3.3% < 4%)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

        # With lower threshold (2.0%), should supply (3.3% > 2%)
        strategy._tick_count = 0
        strategy.supply_rate_threshold = Decimal("2.0")
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"

    def test_different_borrow_threshold_changes_behavior(self, strategy):
        """Verify that changing borrow_rate_threshold affects decisions."""
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.01")
        strategy._reference_price = Decimal("3000")
        market = _mock_market(weth_price=3150.0)  # 5% change

        # With default threshold (6.0%), should borrow (5% < 6%)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "BORROW"

        # With lower threshold (3.0%), should hold (5% > 3%)
        # Reset state fully (decide() changed state to "borrowing" and updated reference_price)
        strategy._tick_count = 0
        strategy._state = "supplied"
        strategy._reference_price = Decimal("3000")
        strategy.borrow_rate_threshold = Decimal("3.0")
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"


class TestTeardown:
    def test_teardown_empty_when_idle(self, strategy):
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 0

    def test_teardown_has_supply_position(self, strategy):
        strategy._supplied_amount = Decimal("0.01")
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 1
        assert positions.positions[0].protocol == "aave_v3"

    def test_teardown_has_both_positions(self, strategy):
        strategy._supplied_amount = Decimal("0.01")
        strategy._borrowed_amount = Decimal("10")
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 2

    def test_teardown_intents_repay_then_withdraw(self, strategy):
        strategy._supplied_amount = Decimal("0.01")
        strategy._borrowed_amount = Decimal("10")
        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"


class TestPersistence:
    def test_save_and_restore_state(self, strategy):
        strategy._state = "borrowed"
        strategy._supplied_amount = Decimal("0.01")
        strategy._borrowed_amount = Decimal("10")
        strategy._borrow_cycles = 3
        strategy._tick_count = 42
        strategy._reference_price = Decimal("3000")

        saved = strategy.get_persistent_state()

        # Reset all persisted fields to prevent false positives
        strategy._state = "idle"
        strategy._supplied_amount = Decimal("0")
        strategy._borrowed_amount = Decimal("0")
        strategy._borrow_cycles = 0
        strategy._tick_count = 0
        strategy._reference_price = None

        strategy.load_persistent_state(saved)
        assert strategy._state == "borrowed"
        assert strategy._supplied_amount == Decimal("0.01")
        assert strategy._borrowed_amount == Decimal("10")
        assert strategy._borrow_cycles == 3
        assert strategy._tick_count == 42
        assert strategy._reference_price == Decimal("3000")


class TestMetadata:
    def test_strategy_name(self):
        from strategies.demo.aave_sweep_lending_polygon.strategy import AaveSweepLendingPolygonStrategy

        assert AaveSweepLendingPolygonStrategy.STRATEGY_NAME == "demo_aave_sweep_lending_polygon"

    def test_supported_chains(self):
        from strategies.demo.aave_sweep_lending_polygon.strategy import AaveSweepLendingPolygonStrategy

        assert "polygon" in AaveSweepLendingPolygonStrategy.STRATEGY_METADATA.supported_chains

    def test_supported_protocols(self):
        from strategies.demo.aave_sweep_lending_polygon.strategy import AaveSweepLendingPolygonStrategy

        assert "aave_v3" in AaveSweepLendingPolygonStrategy.STRATEGY_METADATA.supported_protocols

    def test_default_chain(self):
        from strategies.demo.aave_sweep_lending_polygon.strategy import AaveSweepLendingPolygonStrategy

        assert AaveSweepLendingPolygonStrategy.STRATEGY_METADATA.default_chain == "polygon"

    def test_intent_types_include_lending(self):
        from strategies.demo.aave_sweep_lending_polygon.strategy import AaveSweepLendingPolygonStrategy

        types = AaveSweepLendingPolygonStrategy.STRATEGY_METADATA.intent_types
        assert "SUPPLY" in types
        assert "BORROW" in types
        assert "REPAY" in types
        assert "WITHDRAW" in types
