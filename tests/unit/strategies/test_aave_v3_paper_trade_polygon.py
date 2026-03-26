"""Tests for Aave V3 Paper Trade strategy on Polygon.

Validates lending lifecycle decisions (supply/borrow/repay/withdraw), state
persistence, teardown, and intent generation for paper trading.

Kitchen Loop iteration 129, VIB-1918.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from strategies.incubating.aave_v3_paper_trade_polygon.strategy import (
        AaveV3PaperTradePolygonStrategy,
    )

    strat = AaveV3PaperTradePolygonStrategy.__new__(AaveV3PaperTradePolygonStrategy)
    strat.config = {}
    strat._chain = "polygon"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-aave-v3-paper-trade-polygon"
    strat.collateral_token = "USDC"
    strat.collateral_amount = Decimal("500")
    strat.borrow_token = "WETH"
    strat.ltv_target = Decimal("0.25")
    strat.supply_threshold_pct = Decimal("3.0")
    strat.max_ticks_in_position = 4
    strat._state = "idle"
    strat._ticks_in_state = 0
    strat._entry_price = Decimal("0")
    strat._supplied_amount = Decimal("0")
    strat._borrowed_amount = Decimal("0")
    strat._total_cycles = 0
    strat._total_supplies = 0
    strat._total_borrows = 0
    strat._total_repays = 0
    strat._total_withdraws = 0
    strat.create_market_snapshot = MagicMock(return_value=_mock_market())
    return strat


def _mock_market(
    usdc_price: float = 1.0,
    weth_price: float = 3000.0,
    usdc_balance: float = 10000.0,
    weth_balance: float = 5.0,
) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        if token == "USDC":
            return Decimal(str(usdc_price))
        elif token == "WETH":
            return Decimal(str(weth_price))
        raise ValueError(f"Unknown token: {token}")

    market.price = MagicMock(side_effect=price_fn)

    def balance_fn(token):
        if token == "USDC":
            return Decimal(str(usdc_balance))
        elif token == "WETH":
            return Decimal(str(weth_balance))
        return Decimal("0")

    market.balance = MagicMock(side_effect=balance_fn)
    return market


class TestDecision:
    def test_supply_from_idle_with_funds(self, strategy):
        """Supplies collateral when idle and has sufficient balance."""
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"
        assert intent.token == "USDC"

    def test_hold_when_idle_insufficient_funds(self, strategy):
        """Holds when idle but insufficient collateral."""
        market = _mock_market(usdc_balance=100.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason.lower()

    def test_hold_when_supplied_within_threshold(self, strategy):
        """Holds while supplied and price hasn't moved enough."""
        strategy._state = "supplied"
        strategy._entry_price = Decimal("3000")
        market = _mock_market(weth_price=3010.0)  # 0.33%, below 3%
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "waiting" in intent.reason.lower()

    def test_borrow_on_price_movement(self, strategy):
        """Borrows when price moves beyond threshold."""
        strategy._state = "supplied"
        strategy._entry_price = Decimal("3000")
        strategy._supplied_amount = Decimal("500")
        market = _mock_market(weth_price=3100.0)  # 3.3% move > 3% threshold
        intent = strategy.decide(market)
        assert intent.intent_type.value == "BORROW"

    def test_borrow_on_max_ticks(self, strategy):
        """Borrows when max ticks reached in supplied state."""
        strategy._state = "supplied"
        strategy._entry_price = Decimal("3000")
        strategy._supplied_amount = Decimal("500")
        strategy._ticks_in_state = 3  # Will be incremented to 4 in decide()
        market = _mock_market(weth_price=3000.0)  # No price change
        intent = strategy.decide(market)
        assert intent.intent_type.value == "BORROW"

    def test_repay_from_borrowed(self, strategy):
        """Repays when in borrowed state."""
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("0.04")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "REPAY"

    def test_withdraw_from_repaid(self, strategy):
        """Withdraws collateral when in repaid state."""
        strategy._state = "repaid"
        strategy._supplied_amount = Decimal("500")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "WITHDRAW"

    def test_cycle_restarts_from_withdrawn(self, strategy):
        """Returns to idle and holds after withdrawal."""
        strategy._state = "withdrawn"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._state == "idle"
        assert strategy._total_cycles == 1

    def test_hold_on_price_error(self, strategy):
        """Holds when price data is unavailable."""
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("price unavailable"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_on_zero_price(self, strategy):
        """Holds when price is zero."""
        market = _mock_market(weth_price=0.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"


class TestIntentCreation:
    def test_supply_intent_fields(self, strategy):
        """Supply intent has correct protocol and chain."""
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "aave_v3"
        assert intent.token == "USDC"

    def test_borrow_intent_calculates_amount(self, strategy):
        """Borrow amount is calculated from collateral value and LTV."""
        strategy._state = "supplied"
        strategy._entry_price = Decimal("3000")
        strategy._supplied_amount = Decimal("500")
        strategy._ticks_in_state = 4
        market = _mock_market(weth_price=3000.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "BORROW"
        # 500 USDC * $1 * 0.25 LTV / $3000 = ~0.041667
        assert intent.borrow_amount > Decimal("0")
        assert intent.borrow_amount < Decimal("1")

    def test_repay_intent_full(self, strategy):
        """Repay intent uses repay_full=True."""
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("0.04")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.repay_full is True

    def test_withdraw_intent_full(self, strategy):
        """Withdraw intent uses withdraw_all=True."""
        strategy._state = "repaid"
        strategy._supplied_amount = Decimal("500")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.withdraw_all is True


class TestOnIntentExecuted:
    def test_supply_transitions_to_supplied(self, strategy):
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"
        strategy.on_intent_executed(mock_intent, True, MagicMock())
        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("500")
        assert strategy._total_supplies == 1

    def test_borrow_transitions_to_borrowed(self, strategy):
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "BORROW"
        mock_intent.borrow_amount = Decimal("0.04")
        strategy.on_intent_executed(mock_intent, True, MagicMock())
        assert strategy._state == "borrowed"
        assert strategy._borrowed_amount == Decimal("0.04")
        assert strategy._total_borrows == 1

    def test_repay_clears_debt(self, strategy):
        strategy._borrowed_amount = Decimal("0.04")
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "REPAY"
        strategy.on_intent_executed(mock_intent, True, MagicMock())
        assert strategy._state == "repaid"
        assert strategy._borrowed_amount == Decimal("0")
        assert strategy._total_repays == 1

    def test_withdraw_clears_supply(self, strategy):
        strategy._supplied_amount = Decimal("500")
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "WITHDRAW"
        strategy.on_intent_executed(mock_intent, True, MagicMock())
        assert strategy._state == "withdrawn"
        assert strategy._supplied_amount == Decimal("0")
        assert strategy._total_withdraws == 1

    def test_no_update_on_failure(self, strategy):
        strategy._state = "idle"
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"
        strategy.on_intent_executed(mock_intent, False, MagicMock())
        assert strategy._state == "idle"
        assert strategy._total_supplies == 0


class TestStatePersistence:
    def test_get_persistent_state(self, strategy):
        strategy._state = "supplied"
        strategy._ticks_in_state = 3
        strategy._entry_price = Decimal("3000")
        strategy._supplied_amount = Decimal("500")
        strategy._borrowed_amount = Decimal("0.04")
        strategy._total_cycles = 2
        strategy._total_supplies = 5

        state = strategy.get_persistent_state()
        assert state["state"] == "supplied"
        assert state["ticks_in_state"] == 3
        assert state["entry_price"] == "3000"
        assert state["supplied_amount"] == "500"
        assert state["borrowed_amount"] == "0.04"
        assert state["total_cycles"] == 2
        assert state["total_supplies"] == 5

    def test_load_persistent_state(self, strategy):
        strategy.load_persistent_state({
            "state": "borrowed",
            "ticks_in_state": 2,
            "entry_price": "2800",
            "supplied_amount": "500",
            "borrowed_amount": "0.05",
            "total_cycles": 3,
            "total_supplies": 4,
            "total_borrows": 4,
            "total_repays": 3,
            "total_withdraws": 3,
        })
        assert strategy._state == "borrowed"
        assert strategy._ticks_in_state == 2
        assert strategy._entry_price == Decimal("2800")
        assert strategy._supplied_amount == Decimal("500")
        assert strategy._borrowed_amount == Decimal("0.05")
        assert strategy._total_cycles == 3

    def test_load_empty_state(self, strategy):
        strategy.load_persistent_state({})
        assert strategy._state == "idle"
        assert strategy._total_cycles == 0


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_no_positions_when_idle(self, strategy):
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_supply_position_tracked(self, strategy):
        strategy._supplied_amount = Decimal("500")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].protocol == "aave_v3"

    def test_both_positions_tracked(self, strategy):
        strategy._supplied_amount = Decimal("500")
        strategy._borrowed_amount = Decimal("0.04")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 2

    def test_teardown_intents_repay_before_withdraw(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._supplied_amount = Decimal("500")
        strategy._borrowed_amount = Decimal("0.04")
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"

    def test_teardown_no_intents_when_idle(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        assert intents == []


class TestStatus:
    def test_get_status(self, strategy):
        status = strategy.get_status()
        assert status["strategy"] == "aave_v3_paper_trade_polygon"
        assert status["chain"] == "polygon"
        assert status["state"]["current"] == "idle"


class TestLifecycle:
    def test_full_supply_borrow_repay_withdraw_cycle(self, strategy):
        """Simulates a full lending cycle: supply -> borrow -> repay -> withdraw."""
        market = _mock_market()

        # Step 1: Supply from idle
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"
        strategy.on_intent_executed(mock_intent, True, MagicMock())
        assert strategy._state == "supplied"

        # Step 2: Hold while in supplied (wait for max ticks)
        for _ in range(3):
            intent = strategy.decide(market)
            assert intent.intent_type.value == "HOLD"

        # Step 3: Borrow on max ticks
        intent = strategy.decide(market)
        assert intent.intent_type.value == "BORROW"
        mock_intent.intent_type.value = "BORROW"
        mock_intent.borrow_amount = Decimal("0.04")
        strategy.on_intent_executed(mock_intent, True, MagicMock())
        assert strategy._state == "borrowed"

        # Step 4: Repay
        intent = strategy.decide(market)
        assert intent.intent_type.value == "REPAY"
        mock_intent.intent_type.value = "REPAY"
        strategy.on_intent_executed(mock_intent, True, MagicMock())
        assert strategy._state == "repaid"

        # Step 5: Withdraw
        intent = strategy.decide(market)
        assert intent.intent_type.value == "WITHDRAW"
        mock_intent.intent_type.value = "WITHDRAW"
        strategy.on_intent_executed(mock_intent, True, MagicMock())
        assert strategy._state == "withdrawn"

        # Step 6: Cycle restarts
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._state == "idle"
        assert strategy._total_cycles == 1
        assert strategy._total_supplies == 1
        assert strategy._total_borrows == 1
        assert strategy._total_repays == 1
        assert strategy._total_withdraws == 1
