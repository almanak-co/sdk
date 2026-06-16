"""Tests for Aave V3 + PancakeSwap V3 teardown lifecycle on BSC.

Kitchen Loop iteration 142. Validates the full state machine:
IDLE -> SUPPLY -> BORROW -> SWAP -> SWAP_BACK -> REPAY -> WITHDRAW -> COMPLETE

Also tests teardown, state persistence, error recovery, and intent fields.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from strategies.incubating.aave_v3_pancakeswap_teardown_bsc.strategy import (
        AaveV3PancakeswapTeardownBscStrategy,
    )

    strat = AaveV3PancakeswapTeardownBscStrategy.__new__(AaveV3PancakeswapTeardownBscStrategy)
    strat.config = {}
    strat._chain = "bsc"
    strat._wallet_address = "0x" + "0" * 40
    strat._deployment_id = "test-aave-pancake-bsc"
    strat.STRATEGY_NAME = "aave_v3_pancakeswap_teardown_bsc"

    # Config values
    strat.collateral_token = "WBNB"
    strat.collateral_amount = Decimal("0.5")
    strat.borrow_token = "USDC"
    strat.swap_to_token = "USDT"
    strat.ltv_target = Decimal("0.3")
    strat.market = "usdc"

    # State
    strat._state = "idle"
    strat._previous_stable = "idle"
    strat._supplied_amount = Decimal("0")
    strat._borrowed_amount = Decimal("0")
    strat._swapped_amount = Decimal("0")

    return strat


def _mock_market(
    wbnb_price: float = 300.0,
    usdc_price: float = 1.0,
    usdt_price: float = 1.0,
) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        prices = {"WBNB": wbnb_price, "USDC": usdc_price, "USDT": usdt_price}
        if token in prices:
            return Decimal(str(prices[token]))
        raise ValueError(f"Unknown token: {token}")

    market.price = MagicMock(side_effect=price_fn)
    return market


# ===========================================================================
# ENTRY PHASE
# ===========================================================================


class TestEntrySupply:
    """Phase 1: IDLE -> SUPPLY."""

    def test_idle_produces_supply_intent(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"
        assert intent.protocol == "aave_v3"
        assert intent.token == "WBNB"
        assert intent.amount == Decimal("0.5")
        assert intent.use_as_collateral is True

    def test_supply_transitions_to_supplying(self, strategy):
        market = _mock_market()
        strategy.decide(market)
        assert strategy._state == "supplying"


class TestEntryBorrow:
    """Phase 2: SUPPLIED -> BORROW (collateral already supplied)."""

    def test_supplied_produces_borrow_intent(self, strategy):
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.5")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "BORROW"
        assert intent.protocol == "aave_v3"
        assert intent.collateral_token == "WBNB"
        assert intent.borrow_token == "USDC"
        # Collateral was supplied by the standalone SUPPLY intent (VIB-3586)
        assert intent.collateral_amount == Decimal("0")

    def test_borrow_amount_respects_ltv(self, strategy):
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.5")
        market = _mock_market(wbnb_price=300.0)
        intent = strategy.decide(market)
        # 0.5 WBNB * $300 = $150, 30% LTV = $45 USDC / $1 = 45.00
        assert intent.borrow_amount == Decimal("45.00")

    def test_borrow_with_zero_price_holds(self, strategy):
        strategy._state = "supplied"
        market = _mock_market(wbnb_price=0.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_borrow_transitions_to_borrowing(self, strategy):
        strategy._state = "supplied"
        market = _mock_market()
        strategy.decide(market)
        assert strategy._state == "borrowing"

    def test_borrow_price_unavailable_holds(self, strategy):
        strategy._state = "supplied"
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("no price"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()


class TestEntrySwap:
    """Phase 3: BORROWED -> SWAP."""

    def test_borrowed_produces_swap_intent(self, strategy):
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("45.00")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.protocol == "pancakeswap_v3"
        assert intent.from_token == "USDC"
        assert intent.to_token == "USDT"
        assert intent.amount == Decimal("45.00")

    def test_swap_max_slippage(self, strategy):
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("45.00")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.max_slippage == Decimal("0.005")


# ===========================================================================
# TEARDOWN PHASE
# ===========================================================================


class TestTeardownSwapBack:
    """Phase 3: SWAPPED -> SWAP_BACK."""

    def test_swapped_produces_swap_back(self, strategy):
        strategy._state = "swapped"
        strategy._swapped_amount = Decimal("44.90")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDT"
        assert intent.to_token == "USDC"
        assert intent.amount == Decimal("44.90")


class TestTeardownRepay:
    """Phase 4: SWAP_BACK -> REPAY."""

    def test_swap_back_produces_repay(self, strategy):
        strategy._state = "swap_back"
        strategy._borrowed_amount = Decimal("45.00")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "REPAY"
        assert intent.protocol == "aave_v3"
        assert intent.token == "USDC"
        assert intent.repay_full is True


class TestTeardownWithdraw:
    """Phase 5: REPAID -> WITHDRAW."""

    def test_repaid_produces_withdraw(self, strategy):
        strategy._state = "repaid"
        strategy._supplied_amount = Decimal("0.5")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "WITHDRAW"
        assert intent.protocol == "aave_v3"
        assert intent.token == "WBNB"
        assert intent.withdraw_all is True


class TestComplete:
    """Phase 6: COMPLETE -> HOLD."""

    def test_complete_holds(self, strategy):
        strategy._state = "complete"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "complete" in intent.reason.lower()


# ===========================================================================
# STATE MACHINE: on_intent_executed
# ===========================================================================


class TestOnIntentExecuted:
    def test_supply_success(self, strategy):
        strategy._state = "supplying"
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        intent.amount = Decimal("0.5")
        strategy.on_intent_executed(intent, success=True, result=MagicMock())
        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("0.5")

    def test_borrow_success(self, strategy):
        strategy._state = "borrowing"
        strategy._supplied_amount = Decimal("0.5")
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.borrow_amount = Decimal("45.00")
        strategy.on_intent_executed(intent, success=True, result=MagicMock())
        assert strategy._state == "borrowed"
        assert strategy._supplied_amount == Decimal("0.5")
        assert strategy._borrowed_amount == Decimal("45.00")

    def test_swap_forward_success(self, strategy):
        strategy._state = "swapping"
        strategy._borrowed_amount = Decimal("45.00")
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        result = MagicMock()
        result.swap_amounts = None
        strategy.on_intent_executed(intent, success=True, result=result)
        assert strategy._state == "swapped"
        assert strategy._swapped_amount == Decimal("45.00")

    def test_swap_forward_with_result_amounts(self, strategy):
        strategy._state = "swapping"
        strategy._borrowed_amount = Decimal("45.00")
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        result = MagicMock()
        result.swap_amounts.amount_out_decimal = Decimal("44.85")
        strategy.on_intent_executed(intent, success=True, result=result)
        assert strategy._state == "swapped"
        assert strategy._swapped_amount == Decimal("44.85")

    def test_swap_amount_preserves_18_decimal_precision(self, strategy):
        """BSC USDC/USDT have 18 decimals -- verify full precision is preserved."""
        strategy._state = "swapping"
        strategy._borrowed_amount = Decimal("45.000000000000000000")
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        result = MagicMock()
        result.swap_amounts.amount_out_decimal = Decimal("44.850000000000000123")
        strategy.on_intent_executed(intent, success=True, result=result)
        assert strategy._swapped_amount == Decimal("44.850000000000000123")
        state = strategy.get_persistent_state()
        assert state["swapped_amount"] == "44.850000000000000123"

    def test_swap_back_success(self, strategy):
        strategy._state = "swapping_back"
        strategy._swapped_amount = Decimal("44.90")
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        strategy.on_intent_executed(intent, success=True, result=MagicMock())
        assert strategy._state == "swap_back"
        assert strategy._swapped_amount == Decimal("0")

    def test_repay_success(self, strategy):
        strategy._state = "repaying"
        strategy._borrowed_amount = Decimal("45.00")
        intent = MagicMock()
        intent.intent_type.value = "REPAY"
        strategy.on_intent_executed(intent, success=True, result=MagicMock())
        assert strategy._state == "repaid"
        assert strategy._borrowed_amount == Decimal("0")

    def test_withdraw_success(self, strategy):
        strategy._state = "withdrawing"
        strategy._supplied_amount = Decimal("0.5")
        intent = MagicMock()
        intent.intent_type.value = "WITHDRAW"
        strategy.on_intent_executed(intent, success=True, result=MagicMock())
        assert strategy._state == "complete"
        assert strategy._supplied_amount == Decimal("0")

    def test_failure_reverts_to_stable(self, strategy):
        strategy._state = "borrowing"
        strategy._previous_stable = "idle"
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "idle"


# ===========================================================================
# TRANSITIONAL STATE RECOVERY
# ===========================================================================


class TestTransitionalRecovery:
    def test_stuck_supplying_reverts(self, strategy):
        strategy._state = "supplying"
        strategy._previous_stable = "idle"
        market = _mock_market()
        intent = strategy.decide(market)
        # After revert to idle, should produce SUPPLY
        assert intent.intent_type.value == "SUPPLY"

    def test_stuck_borrowing_reverts(self, strategy):
        strategy._state = "borrowing"
        strategy._previous_stable = "supplied"
        strategy._supplied_amount = Decimal("0.5")
        market = _mock_market()
        intent = strategy.decide(market)
        # After revert to supplied, should produce BORROW
        assert intent.intent_type.value == "BORROW"

    def test_stuck_swapping_reverts(self, strategy):
        strategy._state = "swapping"
        strategy._previous_stable = "borrowed"
        strategy._borrowed_amount = Decimal("45.00")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"

    def test_stuck_repaying_reverts(self, strategy):
        strategy._state = "repaying"
        strategy._previous_stable = "swap_back"
        strategy._borrowed_amount = Decimal("45.00")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "REPAY"


# ===========================================================================
# STATE PERSISTENCE
# ===========================================================================


class TestStatePersistence:
    def test_get_persistent_state(self, strategy):
        strategy._state = "borrowed"
        strategy._previous_stable = "idle"
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("45.00")
        strategy._swapped_amount = Decimal("0")

        state = strategy.get_persistent_state()
        assert state["state"] == "borrowed"
        assert state["previous_stable"] == "idle"
        assert state["supplied_amount"] == "0.5"
        assert state["borrowed_amount"] == "45.00"
        assert state["swapped_amount"] == "0"

    def test_load_persistent_state(self, strategy):
        strategy.load_persistent_state({
            "state": "swapped",
            "previous_stable": "borrowed",
            "supplied_amount": "0.5",
            "borrowed_amount": "45.00",
            "swapped_amount": "44.90",
        })
        assert strategy._state == "swapped"
        assert strategy._previous_stable == "borrowed"
        assert strategy._supplied_amount == Decimal("0.5")
        assert strategy._borrowed_amount == Decimal("45.00")
        assert strategy._swapped_amount == Decimal("44.90")

    def test_load_empty_state_uses_defaults(self, strategy):
        strategy.load_persistent_state({})
        assert strategy._state == "idle"
        assert strategy._supplied_amount == Decimal("0")


# ===========================================================================
# TEARDOWN INTERFACE
# ===========================================================================


class TestTeardownInterface:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_get_open_positions_empty(self, strategy):
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_get_open_positions_with_supply_and_borrow(self, strategy):
        strategy.create_market_snapshot = MagicMock(return_value=_mock_market())
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("45.00")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 2
        values = {p.position_type.value: p.value_usd for p in summary.positions}
        assert values["SUPPLY"] == Decimal("150.00")
        assert values["BORROW"] == Decimal("45.00")

    def test_get_open_positions_with_swap(self, strategy):
        strategy.create_market_snapshot = MagicMock(return_value=_mock_market())
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("45.00")
        strategy._swapped_amount = Decimal("44.90")
        strategy._state = "swapped"
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 3
        values = {p.position_type.value: p.value_usd for p in summary.positions}
        assert values["SUPPLY"] == Decimal("150.00")
        assert values["TOKEN"] == Decimal("44.90")

    def test_teardown_soft_from_swapped(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._state = "swapped"
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("45.00")
        strategy._swapped_amount = Decimal("44.90")

        intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT)
        assert len(intents) == 3
        assert intents[0].intent_type.value == "SWAP"  # USDT -> USDC
        assert intents[1].intent_type.value == "REPAY"
        assert intents[2].intent_type.value == "WITHDRAW"

    def test_teardown_hard_uses_wider_slippage(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._state = "swapped"
        strategy._swapped_amount = Decimal("44.90")
        strategy._borrowed_amount = Decimal("45.00")
        strategy._supplied_amount = Decimal("0.5")

        intents = strategy.generate_teardown_intents(mode=TeardownMode.HARD)
        swap_intent = intents[0]
        assert swap_intent.max_slippage == Decimal("0.03")

    def test_teardown_from_borrowed_no_swap(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._state = "borrowed"
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("45.00")
        strategy._swapped_amount = Decimal("0")

        intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT)
        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"

    def test_teardown_complete_no_intents(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._state = "complete"
        strategy._supplied_amount = Decimal("0")
        strategy._borrowed_amount = Decimal("0")
        strategy._swapped_amount = Decimal("0")

        intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT)
        assert len(intents) == 0


# ===========================================================================
# STATUS
# ===========================================================================


class TestStatus:
    def test_get_status_fields(self, strategy):
        status = strategy.get_status()
        assert status["strategy"] == "aave_v3_pancakeswap_teardown_bsc"
        assert status["chain"] == "bsc"
        assert status["state"] == "idle"

    def test_get_status_with_positions(self, strategy):
        strategy._state = "swapped"
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("45.00")
        strategy._swapped_amount = Decimal("44.90")
        status = strategy.get_status()
        assert status["supplied_wbnb"] == "0.5"
        assert status["borrowed_usdc"] == "45.00"
        assert status["swapped_usdt"] == "44.90"


# ===========================================================================
# FULL LIFECYCLE SIMULATION
# ===========================================================================


class TestFullLifecycle:
    """Simulate the complete entry + teardown lifecycle."""

    def test_full_lifecycle(self, strategy):
        market = _mock_market(wbnb_price=300.0)

        # Phase 1: IDLE -> SUPPLY
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"
        assert intent.token == "WBNB"
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"
        mock_intent.amount = Decimal("0.5")
        strategy.on_intent_executed(mock_intent, True, MagicMock())
        assert strategy._state == "supplied"

        # Phase 2: SUPPLIED -> BORROW
        intent = strategy.decide(market)
        assert intent.intent_type.value == "BORROW"
        assert intent.collateral_amount == Decimal("0")
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "BORROW"
        mock_intent.borrow_amount = Decimal("45.00")
        strategy.on_intent_executed(mock_intent, True, MagicMock())
        assert strategy._state == "borrowed"

        # Phase 3: BORROWED -> SWAP
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        result = MagicMock()
        result.swap_amounts = None
        strategy.on_intent_executed(mock_intent, True, result)
        assert strategy._state == "swapped"

        # Phase 3: SWAPPED -> SWAP_BACK
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDT"
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        strategy.on_intent_executed(mock_intent, True, MagicMock())
        assert strategy._state == "swap_back"

        # Phase 4: SWAP_BACK -> REPAY
        intent = strategy.decide(market)
        assert intent.intent_type.value == "REPAY"
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "REPAY"
        strategy.on_intent_executed(mock_intent, True, MagicMock())
        assert strategy._state == "repaid"

        # Phase 5: REPAID -> WITHDRAW
        intent = strategy.decide(market)
        assert intent.intent_type.value == "WITHDRAW"
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "WITHDRAW"
        strategy.on_intent_executed(mock_intent, True, MagicMock())
        assert strategy._state == "complete"

        # Phase 6: COMPLETE -> HOLD
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "complete" in intent.reason.lower()
