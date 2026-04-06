"""Tests for Aave V3 Paper Trade Lending Lifecycle on Ethereum (VIB-2310).

Validates the state machine:
idle -> supply -> supplied -> borrow -> borrowed -> repay -> repaid -> withdraw -> withdrawn -> idle

Also tests teardown, state persistence, and error handling.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from almanak.demo_strategies.aave_v3_paper_trade_ethereum.strategy import (
        AaveV3PaperTradeEthereumStrategy,
    )

    strat = AaveV3PaperTradeEthereumStrategy.__new__(AaveV3PaperTradeEthereumStrategy)
    strat.config = {}
    strat._chain = "ethereum"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-aave-paper-ethereum"
    strat.STRATEGY_NAME = "aave_v3_paper_trade_ethereum"

    # Config
    strat.collateral_token = "USDC"
    strat.collateral_amount = Decimal("500")
    strat.borrow_token = "WETH"
    strat.ltv_target = Decimal("0.25")
    strat.supply_threshold_pct = Decimal("3.0")
    strat.max_ticks_in_position = 4

    # State
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

    return strat


def _mock_market(
    usdc_price: float = 1.0,
    weth_price: float = 3000.0,
) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        prices = {"USDC": usdc_price, "WETH": weth_price, "ETH": weth_price}
        if token in prices:
            return Decimal(str(prices[token]))
        raise ValueError(f"Unknown token: {token}")

    def balance_fn(token):
        balances = {"USDC": Decimal("10000"), "WETH": Decimal("1"), "ETH": Decimal("10")}
        return balances.get(token, Decimal("0"))

    market.price = MagicMock(side_effect=price_fn)
    market.balance = MagicMock(side_effect=balance_fn)
    return market


# ===========================================================================
# IDLE -> SUPPLY
# ===========================================================================


class TestIdleToSupply:
    """Phase 1: idle state produces SUPPLY intent."""

    def test_idle_produces_supply(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"
        assert intent.protocol == "aave_v3"
        assert intent.token == "USDC"
        assert intent.amount == Decimal("500")

    def test_idle_records_entry_price(self, strategy):
        market = _mock_market(weth_price=3500.0)
        strategy.decide(market)
        assert strategy._entry_price == Decimal("3500.0")

    def test_idle_insufficient_balance_holds(self, strategy):
        market = _mock_market()
        market.balance = MagicMock(return_value=Decimal("100"))  # < 500
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Insufficient" in intent.reason


# ===========================================================================
# SUPPLIED -> BORROW
# ===========================================================================


class TestSuppliedToBorrow:
    """Phase 2: supplied state borrows on price move or max ticks."""

    def test_supplied_holds_within_threshold(self, strategy):
        strategy._state = "supplied"
        strategy._entry_price = Decimal("3000")
        market = _mock_market(weth_price=3010.0)  # < 3% move
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "waiting" in intent.reason.lower()

    def test_supplied_borrows_on_price_move(self, strategy):
        strategy._state = "supplied"
        strategy._entry_price = Decimal("3000")
        market = _mock_market(weth_price=3100.0)  # 3.33% > 3%
        intent = strategy.decide(market)
        assert intent.intent_type.value == "BORROW"
        assert intent.protocol == "aave_v3"
        assert intent.borrow_token == "WETH"

    def test_supplied_borrows_on_max_ticks(self, strategy):
        strategy._state = "supplied"
        strategy._entry_price = Decimal("3000")
        strategy._ticks_in_state = 3  # Will be 4 after decide increments
        market = _mock_market(weth_price=3000.0)  # No price move
        intent = strategy.decide(market)
        assert intent.intent_type.value == "BORROW"

    def test_borrow_amount_respects_ltv(self, strategy):
        strategy._state = "supplied"
        strategy._entry_price = Decimal("3000")
        strategy._ticks_in_state = 3  # Force borrow via max ticks
        market = _mock_market(usdc_price=1.0, weth_price=3000.0)
        intent = strategy.decide(market)
        # 500 USDC * $1 * 0.25 LTV / $3000 = 0.041666
        expected = Decimal("0.041666")
        assert intent.borrow_amount == expected


# ===========================================================================
# BORROWED -> REPAY -> WITHDRAW -> IDLE
# ===========================================================================


class TestUnwindPhases:
    """Phases 3-5: automatic unwind regardless of market."""

    def test_borrowed_produces_repay(self, strategy):
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("0.05")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "REPAY"
        assert intent.token == "WETH"

    def test_repaid_produces_withdraw(self, strategy):
        strategy._state = "repaid"
        strategy._supplied_amount = Decimal("500")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "WITHDRAW"
        assert intent.token == "USDC"

    def test_withdrawn_resets_to_idle(self, strategy):
        strategy._state = "withdrawn"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._state == "idle"
        assert strategy._total_cycles == 1


# ===========================================================================
# on_intent_executed CALLBACKS
# ===========================================================================


class TestOnIntentExecuted:
    """Test state transitions via on_intent_executed."""

    def _make_intent(self, intent_type: str):
        intent = MagicMock()
        intent.intent_type.value = intent_type
        return intent

    def test_supply_transitions_to_supplied(self, strategy):
        strategy.on_intent_executed(self._make_intent("SUPPLY"), True, None)
        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("500")
        assert strategy._total_supplies == 1

    def test_borrow_transitions_to_borrowed(self, strategy):
        intent = self._make_intent("BORROW")
        intent.borrow_amount = Decimal("0.05")
        strategy.on_intent_executed(intent, True, None)
        assert strategy._state == "borrowed"
        assert strategy._borrowed_amount == Decimal("0.05")
        assert strategy._total_borrows == 1

    def test_repay_transitions_to_repaid(self, strategy):
        strategy._borrowed_amount = Decimal("0.05")
        strategy.on_intent_executed(self._make_intent("REPAY"), True, None)
        assert strategy._state == "repaid"
        assert strategy._borrowed_amount == Decimal("0")
        assert strategy._total_repays == 1

    def test_withdraw_transitions_to_withdrawn(self, strategy):
        strategy._supplied_amount = Decimal("500")
        strategy.on_intent_executed(self._make_intent("WITHDRAW"), True, None)
        assert strategy._state == "withdrawn"
        assert strategy._supplied_amount == Decimal("0")
        assert strategy._total_withdraws == 1

    def test_failure_does_not_transition(self, strategy):
        strategy._state = "idle"
        strategy.on_intent_executed(self._make_intent("SUPPLY"), False, None)
        assert strategy._state == "idle"


# ===========================================================================
# STATE PERSISTENCE
# ===========================================================================


class TestStatePersistence:
    """Test get/load persistent state round-trip."""

    def test_roundtrip(self, strategy):
        strategy._state = "supplied"
        strategy._ticks_in_state = 3
        strategy._entry_price = Decimal("3000")
        strategy._supplied_amount = Decimal("500")
        strategy._borrowed_amount = Decimal("0.05")
        strategy._total_cycles = 2

        state = strategy.get_persistent_state()

        # Create fresh strategy and load state
        from almanak.demo_strategies.aave_v3_paper_trade_ethereum.strategy import (
            AaveV3PaperTradeEthereumStrategy,
        )

        strat2 = AaveV3PaperTradeEthereumStrategy.__new__(AaveV3PaperTradeEthereumStrategy)
        strat2._state = "idle"
        strat2._ticks_in_state = 0
        strat2._entry_price = Decimal("0")
        strat2._supplied_amount = Decimal("0")
        strat2._borrowed_amount = Decimal("0")
        strat2._total_cycles = 0
        strat2._total_supplies = 0
        strat2._total_borrows = 0
        strat2._total_repays = 0
        strat2._total_withdraws = 0

        strat2.load_persistent_state(state)

        assert strat2._state == "supplied"
        assert strat2._ticks_in_state == 3
        assert strat2._entry_price == Decimal("3000")
        assert strat2._supplied_amount == Decimal("500")
        assert strat2._borrowed_amount == Decimal("0.05")
        assert strat2._total_cycles == 2


# ===========================================================================
# TEARDOWN
# ===========================================================================


class TestTeardown:
    """Test teardown support methods."""

    def test_teardown_with_supply_and_borrow(self, strategy):
        strategy._supplied_amount = Decimal("500")
        strategy._borrowed_amount = Decimal("0.05")
        intents = strategy.generate_teardown_intents(mode="hard")
        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"

    def test_teardown_supply_only(self, strategy):
        strategy._supplied_amount = Decimal("500")
        strategy._borrowed_amount = Decimal("0")
        intents = strategy.generate_teardown_intents(mode="hard")
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"

    def test_teardown_empty(self, strategy):
        intents = strategy.generate_teardown_intents(mode="hard")
        assert len(intents) == 0

    def test_get_open_positions_count(self, strategy):
        strategy._supplied_amount = Decimal("500")
        strategy._borrowed_amount = Decimal("0.05")
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 2

    def test_get_open_positions_empty(self, strategy):
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 0


# ===========================================================================
# PRICE EDGE CASES
# ===========================================================================


class TestPriceEdgeCases:
    """Test behavior with price failures."""

    def test_price_unavailable_holds(self, strategy):
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("No price"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_zero_price_holds(self, strategy):
        market = _mock_market(usdc_price=0.0, weth_price=3000.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Invalid price" in intent.reason
