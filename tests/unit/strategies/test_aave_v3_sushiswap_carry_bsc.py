"""Tests for Aave V3 + SushiSwap V3 carry trade on BSC.

Validates:
1. State machine transitions: idle -> borrowing -> borrowed -> swapping -> swapped
   -> swapping_back -> swap_back -> repaying -> repaid -> withdrawing -> complete
2. Intent generation with correct protocol and parameters per phase
3. on_intent_executed callback advances state correctly
4. Failure recovery: revert to previous stable state
5. Teardown: positions reported per state, intents generated per mode
6. Persistence: save/load state round-trip

Kitchen Loop iteration 151.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def config():
    return {
        "collateral_token": "WETH",
        "collateral_amount": "0.1",
        "borrow_token": "USDC",
        "swap_to_token": "USDT",
        "ltv_target": "0.3",
    }


@pytest.fixture
def strategy(config):
    from strategies.incubating.aave_v3_sushiswap_carry_bsc.strategy import (
        AaveV3SushiswapCarryBscStrategy,
    )

    strat = AaveV3SushiswapCarryBscStrategy.__new__(AaveV3SushiswapCarryBscStrategy)
    strat.config = config
    strat._chain = "bsc"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-aave-sushi-bsc"

    strat.collateral_token = config["collateral_token"]
    strat.collateral_amount = Decimal(config["collateral_amount"])
    strat.borrow_token = config["borrow_token"]
    strat.swap_to_token = config["swap_to_token"]
    strat.ltv_target = Decimal(config["ltv_target"])

    strat._state = "idle"
    strat._previous_stable = "idle"
    strat._supplied_amount = Decimal("0")
    strat._borrowed_amount = Decimal("0")
    strat._swapped_amount = Decimal("0")
    return strat


def _mock_market(weth_price: float = 2500.0, usdc_price: float = 1.0, usdt_price: float = 1.0) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        prices = {"WETH": weth_price, "ETH": weth_price, "USDC": usdc_price, "USDT": usdt_price}
        if token in prices:
            return Decimal(str(prices[token]))
        raise ValueError(f"Unknown token: {token}")

    market.price = MagicMock(side_effect=price_fn)
    return market


def _make_intent(intent_type="BORROW", borrow_amount=None):
    intent = MagicMock()
    intent.intent_type.value = intent_type
    if borrow_amount is not None:
        intent.borrow_amount = borrow_amount
    return intent


def _make_swap_result(amount_out=61.5, token_in="USDC", token_out="USDT"):
    result = MagicMock()
    result.swap_amounts = MagicMock()
    result.swap_amounts.amount_out_decimal = Decimal(str(amount_out))
    result.swap_amounts.token_in = token_in
    result.swap_amounts.token_out = token_out
    return result


# =========================================================================
# State Machine Tests
# =========================================================================


class TestStateMachine:
    """Test the full lifecycle state machine."""

    def test_idle_returns_borrow_intent(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "BORROW"
        assert strategy._state == "borrowing"

    def test_borrow_intent_params(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "aave_v3"
        assert intent.collateral_token == "WETH"
        assert intent.collateral_amount == Decimal("0.1")
        assert intent.borrow_token == "USDC"
        # 0.1 WETH * 2500 * 0.3 / 1.0 = 75.0
        assert intent.borrow_amount == Decimal("75.00")

    def test_borrowed_returns_swap_intent(self, strategy):
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("75.00")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "USDT"
        assert intent.protocol == "sushiswap_v3"
        assert strategy._state == "swapping"

    def test_swapped_returns_swap_back_intent(self, strategy):
        strategy._state = "swapped"
        strategy._swapped_amount = Decimal("61.50")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDT"
        assert intent.to_token == "USDC"
        assert intent.protocol == "sushiswap_v3"
        assert strategy._state == "swapping_back"

    def test_swap_back_returns_repay_intent(self, strategy):
        strategy._state = "swap_back"
        strategy._borrowed_amount = Decimal("75.00")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "REPAY"
        assert intent.token == "USDC"
        assert intent.protocol == "aave_v3"
        assert intent.repay_full is True
        assert strategy._state == "repaying"

    def test_repaid_returns_withdraw_intent(self, strategy):
        strategy._state = "repaid"
        strategy._supplied_amount = Decimal("0.1")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "WITHDRAW"
        assert intent.token == "WETH"
        assert intent.protocol == "aave_v3"
        assert intent.withdraw_all is True
        assert strategy._state == "withdrawing"

    def test_complete_returns_hold(self, strategy):
        strategy._state = "complete"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "complete" in intent.reason.lower()


class TestStuckRecovery:
    """Test transitional state recovery."""

    def test_stuck_borrowing_reverts_to_idle(self, strategy):
        strategy._state = "borrowing"
        strategy._previous_stable = "idle"
        market = _mock_market()
        strategy.decide(market)
        # After reverting to idle, it should try to borrow again
        assert strategy._state == "borrowing"  # Re-entered borrow

    def test_stuck_swapping_reverts_to_borrowed(self, strategy):
        strategy._state = "swapping"
        strategy._previous_stable = "borrowed"
        strategy._borrowed_amount = Decimal("75")
        market = _mock_market()
        strategy.decide(market)
        # After reverting to borrowed, it should try to swap again
        assert strategy._state == "swapping"

    def test_stuck_repaying_reverts_to_swap_back(self, strategy):
        strategy._state = "repaying"
        strategy._previous_stable = "swap_back"
        strategy._borrowed_amount = Decimal("75")
        market = _mock_market()
        strategy.decide(market)
        assert strategy._state == "repaying"


# =========================================================================
# on_intent_executed Tests
# =========================================================================


class TestOnIntentExecuted:
    """Test state machine advances on execution callbacks."""

    def test_borrow_success(self, strategy):
        strategy._state = "borrowing"
        intent = _make_intent("BORROW", borrow_amount=Decimal("75.00"))
        strategy.on_intent_executed(intent, True, None)
        assert strategy._state == "borrowed"
        assert strategy._supplied_amount == Decimal("0.1")
        assert strategy._borrowed_amount == Decimal("75.00")

    def test_swap_success(self, strategy):
        strategy._state = "swapping"
        result = _make_swap_result(amount_out=61.5)
        strategy.on_intent_executed(_make_intent("SWAP"), True, result)
        assert strategy._state == "swapped"
        assert strategy._swapped_amount == Decimal("61.5")

    def test_swap_back_success(self, strategy):
        strategy._state = "swapping_back"
        strategy._swapped_amount = Decimal("61.50")
        strategy.on_intent_executed(_make_intent("SWAP"), True, _make_swap_result())
        assert strategy._state == "swap_back"
        assert strategy._swapped_amount == Decimal("0")

    def test_repay_success(self, strategy):
        strategy._state = "repaying"
        strategy._borrowed_amount = Decimal("75.00")
        strategy.on_intent_executed(_make_intent("REPAY"), True, None)
        assert strategy._state == "repaid"
        assert strategy._borrowed_amount == Decimal("0")

    def test_withdraw_success(self, strategy):
        strategy._state = "withdrawing"
        strategy._supplied_amount = Decimal("0.1")
        strategy.on_intent_executed(_make_intent("WITHDRAW"), True, None)
        assert strategy._state == "complete"
        assert strategy._supplied_amount == Decimal("0")

    def test_borrow_failure_reverts(self, strategy):
        strategy._state = "borrowing"
        strategy._previous_stable = "idle"
        strategy.on_intent_executed(_make_intent("BORROW"), False, MagicMock())
        assert strategy._state == "idle"

    def test_swap_failure_reverts(self, strategy):
        strategy._state = "swapping"
        strategy._previous_stable = "borrowed"
        strategy.on_intent_executed(_make_intent("SWAP"), False, MagicMock())
        assert strategy._state == "borrowed"

    def test_swap_without_enrichment(self, strategy):
        strategy._state = "swapping"
        strategy._borrowed_amount = Decimal("75.00")
        result = MagicMock()
        result.swap_amounts = None
        strategy.on_intent_executed(_make_intent("SWAP"), True, result)
        assert strategy._state == "swapped"
        assert strategy._swapped_amount == Decimal("75.00")  # Falls back to borrowed amount


# =========================================================================
# Teardown Tests
# =========================================================================


class TestTeardown:
    """Test teardown position reporting and intent generation."""

    def test_no_positions_when_idle(self, strategy):
        strategy._state = "idle"
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_no_positions_when_complete(self, strategy):
        strategy._state = "complete"
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_supply_position_when_borrowed(self, strategy):
        strategy._state = "borrowed"
        strategy._supplied_amount = Decimal("0.1")
        strategy._borrowed_amount = Decimal("75.00")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 2
        supply_pos = [p for p in summary.positions if p.position_type.value == "SUPPLY"]
        borrow_pos = [p for p in summary.positions if p.position_type.value == "BORROW"]
        assert len(supply_pos) == 1
        assert len(borrow_pos) == 1
        assert supply_pos[0].protocol == "aave_v3"
        assert borrow_pos[0].protocol == "aave_v3"

    def test_swap_position_when_swapped(self, strategy):
        strategy._state = "swapped"
        strategy._supplied_amount = Decimal("0.1")
        strategy._borrowed_amount = Decimal("75.00")
        strategy._swapped_amount = Decimal("61.50")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 3
        token_pos = [p for p in summary.positions if p.position_type.value == "TOKEN"]
        assert len(token_pos) == 1
        assert token_pos[0].protocol == "sushiswap_v3"

    def test_teardown_intents_soft_mode(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._state = "swapped"
        strategy._supplied_amount = Decimal("0.1")
        strategy._borrowed_amount = Decimal("75.00")
        strategy._swapped_amount = Decimal("61.50")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 3
        # 1. Swap USDT -> USDC
        assert intents[0].intent_type.value == "SWAP"
        assert intents[0].from_token == "USDT"
        assert intents[0].to_token == "USDC"
        assert intents[0].max_slippage == Decimal("0.01")
        # 2. Repay USDC
        assert intents[1].intent_type.value == "REPAY"
        assert intents[1].repay_full is True
        # 3. Withdraw WETH
        assert intents[2].intent_type.value == "WITHDRAW"
        assert intents[2].withdraw_all is True

    def test_teardown_intents_hard_mode(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._state = "swapped"
        strategy._supplied_amount = Decimal("0.1")
        strategy._borrowed_amount = Decimal("75.00")
        strategy._swapped_amount = Decimal("61.50")

        intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        assert intents[0].max_slippage == Decimal("0.03")

    def test_teardown_intents_when_repaid(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._state = "repaid"
        strategy._supplied_amount = Decimal("0.1")
        strategy._borrowed_amount = Decimal("0")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"

    def test_no_teardown_when_idle(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._state = "idle"
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0


# =========================================================================
# Persistence Tests
# =========================================================================


class TestPersistence:
    """Test state save/load round-trip."""

    def test_save_load_round_trip(self, strategy):
        strategy._state = "swapped"
        strategy._previous_stable = "borrowed"
        strategy._supplied_amount = Decimal("0.1")
        strategy._borrowed_amount = Decimal("75.00")
        strategy._swapped_amount = Decimal("61.50")

        state = strategy.get_persistent_state()

        # Create fresh strategy
        from strategies.incubating.aave_v3_sushiswap_carry_bsc.strategy import (
            AaveV3SushiswapCarryBscStrategy,
        )

        strat2 = AaveV3SushiswapCarryBscStrategy.__new__(AaveV3SushiswapCarryBscStrategy)
        strat2._state = "idle"
        strat2._previous_stable = "idle"
        strat2._supplied_amount = Decimal("0")
        strat2._borrowed_amount = Decimal("0")
        strat2._swapped_amount = Decimal("0")

        strat2.load_persistent_state(state)

        assert strat2._state == "swapped"
        assert strat2._previous_stable == "borrowed"
        assert strat2._supplied_amount == Decimal("0.1")
        assert strat2._borrowed_amount == Decimal("75.00")
        assert strat2._swapped_amount == Decimal("61.50")


# =========================================================================
# Full Lifecycle Integration Test
# =========================================================================


class TestFullLifecycle:
    """Test the complete entry + teardown flow."""

    def test_full_lifecycle(self, strategy):
        market = _mock_market()

        # Phase 1: BORROW
        intent1 = strategy.decide(market)
        assert intent1.intent_type.value == "BORROW"
        strategy.on_intent_executed(
            _make_intent("BORROW", borrow_amount=Decimal("75.00")), True, None
        )
        assert strategy._state == "borrowed"

        # Phase 2: SWAP USDC -> USDT
        intent2 = strategy.decide(market)
        assert intent2.intent_type.value == "SWAP"
        assert intent2.protocol == "sushiswap_v3"
        strategy.on_intent_executed(
            _make_intent("SWAP"), True, _make_swap_result(61.5)
        )
        assert strategy._state == "swapped"

        # Phase 3: SWAP USDT -> USDC
        intent3 = strategy.decide(market)
        assert intent3.intent_type.value == "SWAP"
        assert intent3.from_token == "USDT"
        strategy.on_intent_executed(
            _make_intent("SWAP"), True, _make_swap_result(74.5, "USDT", "USDC")
        )
        assert strategy._state == "swap_back"

        # Phase 4: REPAY
        intent4 = strategy.decide(market)
        assert intent4.intent_type.value == "REPAY"
        strategy.on_intent_executed(_make_intent("REPAY"), True, None)
        assert strategy._state == "repaid"

        # Phase 5: WITHDRAW
        intent5 = strategy.decide(market)
        assert intent5.intent_type.value == "WITHDRAW"
        strategy.on_intent_executed(_make_intent("WITHDRAW"), True, None)
        assert strategy._state == "complete"

        # Phase 6: HOLD
        intent6 = strategy.decide(market)
        assert intent6.intent_type.value == "HOLD"


class TestEdgeCases:
    """Test edge cases."""

    def test_price_unavailable_holds(self, strategy):
        market = _mock_market()
        market.price = MagicMock(side_effect=ValueError("No price"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_zero_borrow_amount_holds(self, strategy):
        # WETH price is 0 -> borrow amount is 0
        market = _mock_market(weth_price=0.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_unknown_state_holds(self, strategy):
        strategy._state = "garbage"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
