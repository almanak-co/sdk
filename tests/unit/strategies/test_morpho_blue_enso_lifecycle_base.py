"""Unit tests for Morpho Blue + Enso Lifecycle Base strategy (VIB-2284).

Tests the state machine logic, intent generation, and teardown sequence.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.vocabulary import (
    BorrowIntent,
    HoldIntent,
    IntentType,
    RepayIntent,
    SwapIntent,
    WithdrawIntent,
)


@pytest.fixture()
def strategy():
    """Create strategy with mock gateway."""
    from strategies.incubating.morpho_blue_enso_lifecycle_base.strategy import (
        MorphoBlueEnsoLifecycleBaseStrategy,
    )

    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        s = MorphoBlueEnsoLifecycleBaseStrategy.__new__(MorphoBlueEnsoLifecycleBaseStrategy)
        s._strategy_id = "test_morpho_blue_enso_lifecycle_base"
        s.name = "morpho_blue_enso_lifecycle_base"
        s._chain = "base"
        s._config = {
            "collateral_token": "wstETH",
            "collateral_amount": "0.1",
            "borrow_token": "USDC",
            "swap_to_token": "WETH",
            "swap_amount_usd": "20",
            "ltv_target": "0.3",
            "market_id": "0x13c42741a359ac4a8aa8287d2be109dcf28344484f91185f9a79bd5a805a55ae",
        }
        s.get_config = lambda key, default=None: s._config.get(key, default)

        # Manually call the attribute initialization (mirrors __init__)
        s.collateral_token = s.get_config("collateral_token", "wstETH")
        s.collateral_amount = Decimal(str(s.get_config("collateral_amount", "0.1")))
        s.borrow_token = s.get_config("borrow_token", "USDC")
        s.swap_to_token = s.get_config("swap_to_token", "WETH")
        s.swap_amount_usd = Decimal(str(s.get_config("swap_amount_usd", "20")))
        s.ltv_target = Decimal(str(s.get_config("ltv_target", "0.3")))
        s.market_id = s._config["market_id"]
        s._chain = "base"

        s._loop_state = "idle"
        s._previous_stable_state = "idle"
        s._collateral_supplied = Decimal("0")
        s._borrowed_amount = Decimal("0")

        return s


def _make_market(collateral_price=2000, borrow_price=1):
    """Create a mock MarketSnapshot."""
    market = MagicMock()
    market.price.side_effect = lambda token: {
        "wstETH": Decimal(str(collateral_price)),
        "USDC": Decimal(str(borrow_price)),
        "WETH": Decimal(str(collateral_price)),
    }.get(token, Decimal("1"))
    return market


class TestStateMachineTransitions:
    def test_idle_produces_borrow_intent(self, strategy):
        market = _make_market()
        intent = strategy.decide(market)

        assert isinstance(intent, BorrowIntent)
        assert intent.intent_type == IntentType.BORROW
        assert strategy._loop_state == "borrowing"

    def test_borrowed_produces_swap_intent(self, strategy):
        strategy._loop_state = "borrowed"
        market = _make_market()
        intent = strategy.decide(market)

        assert isinstance(intent, SwapIntent)
        assert intent.intent_type == IntentType.SWAP
        assert strategy._loop_state == "swapping"

    def test_swapped_produces_repay_intent(self, strategy):
        strategy._loop_state = "swapped"
        market = _make_market()
        intent = strategy.decide(market)

        assert isinstance(intent, RepayIntent)
        assert intent.intent_type == IntentType.REPAY
        assert strategy._loop_state == "repaying"

    def test_repaid_produces_withdraw_intent(self, strategy):
        strategy._loop_state = "repaid"
        market = _make_market()
        intent = strategy.decide(market)

        assert isinstance(intent, WithdrawIntent)
        assert intent.intent_type == IntentType.WITHDRAW
        assert strategy._loop_state == "withdrawing"

    def test_complete_produces_hold_intent(self, strategy):
        strategy._loop_state = "complete"
        market = _make_market()
        intent = strategy.decide(market)

        assert isinstance(intent, HoldIntent)
        assert "complete" in intent.reason.lower()

    def test_transitional_state_reverts_to_previous_stable(self, strategy):
        strategy._loop_state = "borrowing"
        strategy._previous_stable_state = "idle"
        market = _make_market()
        intent = strategy.decide(market)

        assert strategy._loop_state == "idle"
        assert isinstance(intent, HoldIntent)


class TestIntentParameters:
    def test_borrow_intent_uses_correct_market_id(self, strategy):
        market = _make_market()
        intent = strategy.decide(market)

        assert isinstance(intent, BorrowIntent)
        assert intent.market_id == "0x13c42741a359ac4a8aa8287d2be109dcf28344484f91185f9a79bd5a805a55ae"
        assert intent.collateral_token == "wstETH"
        assert intent.borrow_token == "USDC"
        assert intent.protocol == "morpho_blue"

    def test_borrow_amount_respects_ltv_target(self, strategy):
        """With 0.1 wstETH at $2000, LTV 30% => borrow 0.1 * 2000 * 0.3 / 1.0 = $60 USDC."""
        market = _make_market(collateral_price=2000.0, borrow_price=1.0)
        intent = strategy.decide(market)

        assert isinstance(intent, BorrowIntent)
        assert intent.borrow_amount == Decimal("60.00")

    def test_swap_intent_uses_enso_protocol(self, strategy):
        strategy._loop_state = "borrowed"
        market = _make_market()
        intent = strategy.decide(market)

        assert isinstance(intent, SwapIntent)
        assert intent.protocol == "enso"
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"

    def test_repay_intent_uses_repay_full(self, strategy):
        strategy._loop_state = "swapped"
        strategy._borrowed_amount = Decimal("60")
        market = _make_market()
        intent = strategy.decide(market)

        assert isinstance(intent, RepayIntent)
        assert intent.repay_full is True
        assert intent.market_id == "0x13c42741a359ac4a8aa8287d2be109dcf28344484f91185f9a79bd5a805a55ae"

    def test_withdraw_intent_uses_withdraw_all(self, strategy):
        strategy._loop_state = "repaid"
        strategy._collateral_supplied = Decimal("0.1")
        market = _make_market()
        intent = strategy.decide(market)

        assert isinstance(intent, WithdrawIntent)
        assert intent.withdraw_all is True
        assert intent.token == "wstETH"
        assert intent.protocol == "morpho_blue"


class TestOnIntentExecuted:
    def test_successful_borrow_advances_to_borrowed(self, strategy):
        strategy._loop_state = "borrowing"
        borrow_intent = MagicMock()
        borrow_intent.intent_type.value = "BORROW"
        borrow_intent.borrow_amount = Decimal("60")

        strategy.on_intent_executed(borrow_intent, success=True, result=MagicMock())

        assert strategy._loop_state == "borrowed"
        assert strategy._collateral_supplied == Decimal("0.1")
        assert strategy._borrowed_amount == Decimal("60")

    def test_successful_swap_advances_to_swapped(self, strategy):
        strategy._loop_state = "swapping"
        swap_intent = MagicMock()
        swap_intent.intent_type.value = "SWAP"

        strategy.on_intent_executed(swap_intent, success=True, result=MagicMock())

        assert strategy._loop_state == "swapped"

    def test_successful_repay_advances_to_repaid(self, strategy):
        strategy._loop_state = "repaying"
        strategy._borrowed_amount = Decimal("60")
        repay_intent = MagicMock()
        repay_intent.intent_type.value = "REPAY"

        strategy.on_intent_executed(repay_intent, success=True, result=MagicMock())

        assert strategy._loop_state == "repaid"
        assert strategy._borrowed_amount == Decimal("0")

    def test_successful_withdraw_advances_to_complete(self, strategy):
        strategy._loop_state = "withdrawing"
        strategy._collateral_supplied = Decimal("0.1")
        withdraw_intent = MagicMock()
        withdraw_intent.intent_type.value = "WITHDRAW"

        strategy.on_intent_executed(withdraw_intent, success=True, result=MagicMock())

        assert strategy._loop_state == "complete"
        assert strategy._collateral_supplied == Decimal("0")

    def test_failed_intent_reverts_state(self, strategy):
        strategy._loop_state = "borrowing"
        strategy._previous_stable_state = "idle"
        borrow_intent = MagicMock()
        borrow_intent.intent_type.value = "BORROW"

        strategy.on_intent_executed(borrow_intent, success=False, result=MagicMock())

        assert strategy._loop_state == "idle"


class TestTeardown:
    def test_teardown_intents_with_open_positions(self, strategy):
        strategy._collateral_supplied = Decimal("0.1")
        strategy._borrowed_amount = Decimal("60")

        intents = strategy.generate_teardown_intents(mode=MagicMock())

        assert len(intents) == 2
        repay_intent, withdraw_intent = intents
        assert isinstance(repay_intent, RepayIntent)
        assert repay_intent.repay_full is True
        assert isinstance(withdraw_intent, WithdrawIntent)
        assert withdraw_intent.withdraw_all is True

    def test_teardown_intents_with_no_positions(self, strategy):
        strategy._collateral_supplied = Decimal("0")
        strategy._borrowed_amount = Decimal("0")

        intents = strategy.generate_teardown_intents(mode=MagicMock())

        assert len(intents) == 0

    def test_teardown_intents_debt_only(self, strategy):
        strategy._collateral_supplied = Decimal("0")
        strategy._borrowed_amount = Decimal("60")

        intents = strategy.generate_teardown_intents(mode=MagicMock())

        assert len(intents) == 1
        assert isinstance(intents[0], RepayIntent)


class TestPersistentState:
    def test_get_persistent_state(self, strategy):
        strategy._loop_state = "borrowed"
        strategy._previous_stable_state = "idle"
        strategy._collateral_supplied = Decimal("0.1")
        strategy._borrowed_amount = Decimal("60")

        state = strategy.get_persistent_state()

        assert state["loop_state"] == "borrowed"
        assert state["previous_stable_state"] == "idle"
        assert state["collateral_supplied"] == "0.1"
        assert state["borrowed_amount"] == "60"

    def test_load_persistent_state(self, strategy):
        state = {
            "loop_state": "swapped",
            "previous_stable_state": "borrowed",
            "collateral_supplied": "0.1",
            "borrowed_amount": "60",
        }

        strategy.load_persistent_state(state)

        assert strategy._loop_state == "swapped"
        assert strategy._previous_stable_state == "borrowed"
        assert strategy._collateral_supplied == Decimal("0.1")
        assert strategy._borrowed_amount == Decimal("60")
