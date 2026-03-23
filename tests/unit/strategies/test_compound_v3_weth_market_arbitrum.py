"""Tests for Compound V3 WETH market lifecycle strategy on Arbitrum.

Validates:
1. State machine transitions (idle -> borrow -> repay -> withdraw -> complete)
2. Intent generation with correct market/collateral params
3. Failure recovery (revert to previous stable state)
4. State persistence and restoration
5. Teardown support

Kitchen Loop iteration 123.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    """Instantiate the strategy with mock internals."""
    from strategies.incubating.compound_v3_weth_market_arbitrum.strategy import (
        CompoundV3WETHMarketArbitrumStrategy,
    )

    strat = CompoundV3WETHMarketArbitrumStrategy.__new__(CompoundV3WETHMarketArbitrumStrategy)
    strat._chain = "arbitrum"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-compound-weth-arb"
    strat.STRATEGY_NAME = "compound_v3_weth_market_arbitrum"

    # Config
    strat.collateral_token = "wstETH"
    strat.collateral_amount = Decimal("0.05")
    strat.borrow_token = "WETH"
    strat.ltv_target = Decimal("0.3")
    strat.market = "weth"

    # State
    strat._loop_state = "idle"
    strat._previous_stable_state = "idle"
    strat._collateral_supplied = Decimal("0")
    strat._borrowed_amount = Decimal("0")

    return strat


def _mock_market(wsteth_price: str = "3500", weth_price: str = "3000") -> MagicMock:
    """Mock MarketSnapshot with wstETH/WETH prices."""
    market = MagicMock()

    def price_fn(symbol):
        prices = {"wstETH": Decimal(wsteth_price), "WETH": Decimal(weth_price)}
        if symbol in prices:
            return prices[symbol]
        raise ValueError(f"Unknown token: {symbol}")

    market.price = MagicMock(side_effect=price_fn)
    return market


# -------------------------------------------------------------------------
# State machine transitions
# -------------------------------------------------------------------------


class TestCompoundV3WETHStateMachine:
    """Test the state machine lifecycle."""

    def test_idle_emits_borrow_intent(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "BORROW"
        assert strategy._loop_state == "borrowing"

    def test_borrowed_emits_repay_intent(self, strategy):
        strategy._loop_state = "borrowed"
        strategy._borrowed_amount = Decimal("0.015")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "REPAY"
        assert strategy._loop_state == "repaying"

    def test_repaid_emits_withdraw_intent(self, strategy):
        strategy._loop_state = "repaid"
        strategy._collateral_supplied = Decimal("0.05")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "WITHDRAW"
        assert strategy._loop_state == "withdrawing"

    def test_complete_emits_hold(self, strategy):
        strategy._loop_state = "complete"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "complete" in intent.reason.lower()

    def test_stuck_in_borrowing_reverts(self, strategy):
        strategy._loop_state = "borrowing"
        strategy._previous_stable_state = "idle"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._loop_state == "idle"

    def test_price_unavailable_returns_hold(self, strategy):
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("no price"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()


# -------------------------------------------------------------------------
# Intent content
# -------------------------------------------------------------------------


class TestCompoundV3WETHIntents:
    """Verify intent parameters."""

    def test_borrow_intent_uses_weth_market(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "compound_v3"
        assert intent.market_id == "weth"
        assert intent.collateral_token == "wstETH"
        assert intent.borrow_token == "WETH"

    def test_borrow_amount_respects_ltv(self, strategy):
        # wstETH=$3500, WETH=$3000, collateral=0.05
        # value = 0.05 * 3500 = 175
        # borrow = 175 * 0.3 / 3000 = 0.0175
        market = _mock_market("3500", "3000")
        intent = strategy.decide(market)
        assert intent.borrow_amount == Decimal("0.017500")

    def test_repay_intent_uses_repay_full(self, strategy):
        strategy._loop_state = "borrowed"
        strategy._borrowed_amount = Decimal("0.015")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.repay_full is True
        assert intent.token == "WETH"
        assert intent.market_id == "weth"

    def test_withdraw_intent_uses_withdraw_all(self, strategy):
        strategy._loop_state = "repaid"
        strategy._collateral_supplied = Decimal("0.05")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.withdraw_all is True
        assert intent.token == "wstETH"
        assert intent.market_id == "weth"


# -------------------------------------------------------------------------
# Lifecycle callbacks
# -------------------------------------------------------------------------


class TestCompoundV3WETHLifecycle:
    """Test on_intent_executed callbacks."""

    def test_borrow_success_advances_state(self, strategy):
        strategy._loop_state = "borrowing"
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.borrow_amount = Decimal("0.015")
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "borrowed"
        assert strategy._collateral_supplied == Decimal("0.05")
        assert strategy._borrowed_amount == Decimal("0.015")

    def test_repay_success_advances_state(self, strategy):
        strategy._loop_state = "repaying"
        strategy._borrowed_amount = Decimal("0.015")
        intent = MagicMock()
        intent.intent_type.value = "REPAY"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "repaid"
        assert strategy._borrowed_amount == Decimal("0")

    def test_withdraw_success_completes(self, strategy):
        strategy._loop_state = "withdrawing"
        strategy._collateral_supplied = Decimal("0.05")
        intent = MagicMock()
        intent.intent_type.value = "WITHDRAW"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "complete"
        assert strategy._collateral_supplied == Decimal("0")

    def test_failure_reverts_state(self, strategy):
        strategy._loop_state = "borrowing"
        strategy._previous_stable_state = "idle"
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._loop_state == "idle"


# -------------------------------------------------------------------------
# State persistence
# -------------------------------------------------------------------------


class TestCompoundV3WETHPersistence:
    """State persistence and restoration."""

    def test_get_persistent_state(self, strategy):
        strategy._loop_state = "borrowed"
        strategy._previous_stable_state = "idle"
        strategy._collateral_supplied = Decimal("0.05")
        strategy._borrowed_amount = Decimal("0.015")

        state = strategy.get_persistent_state()
        assert state["loop_state"] == "borrowed"
        assert state["collateral_supplied"] == "0.05"
        assert state["borrowed_amount"] == "0.015"

    def test_load_persistent_state(self, strategy):
        state = {
            "loop_state": "repaid",
            "previous_stable_state": "borrowed",
            "collateral_supplied": "0.05",
            "borrowed_amount": "0",
        }
        strategy.load_persistent_state(state)
        assert strategy._loop_state == "repaid"
        assert strategy._previous_stable_state == "borrowed"
        assert strategy._collateral_supplied == Decimal("0.05")
        assert strategy._borrowed_amount == Decimal("0")

    def test_round_trip_persistence(self, strategy):
        strategy._loop_state = "borrowed"
        strategy._collateral_supplied = Decimal("0.05")
        strategy._borrowed_amount = Decimal("0.017")

        state = strategy.get_persistent_state()
        strategy._loop_state = "idle"
        strategy._collateral_supplied = Decimal("0")
        strategy.load_persistent_state(state)

        assert strategy._loop_state == "borrowed"
        assert strategy._collateral_supplied == Decimal("0.05")
        assert strategy._borrowed_amount == Decimal("0.017")


# -------------------------------------------------------------------------
# Teardown
# -------------------------------------------------------------------------


class TestCompoundV3WETHTeardown:
    """Teardown support."""

    def test_no_positions_when_idle(self, strategy):
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_positions_when_borrowed(self, strategy):
        strategy._collateral_supplied = Decimal("0.05")
        strategy._borrowed_amount = Decimal("0.015")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 2
        types = {p.position_type.value for p in summary.positions}
        assert "SUPPLY" in types
        assert "BORROW" in types

    def test_teardown_intents_repay_then_withdraw(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._collateral_supplied = Decimal("0.05")
        strategy._borrowed_amount = Decimal("0.015")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"

    def test_teardown_empty_when_no_positions(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0

    def test_teardown_only_withdraw_when_no_debt(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._collateral_supplied = Decimal("0.05")
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"
