"""Tests for the Aave V3 + Uniswap V3 Yield Stack strategy on Optimism.

Validates:
1. Strategy initialization with Optimism chain config
2. State machine transitions through all 5 phases
3. Intent generation for SUPPLY, BORROW, SWAP, LP_OPEN, HOLD
4. on_intent_executed callbacks and state transitions
5. State persistence and restoration
6. Teardown support

Kitchen Loop iteration 88, VIB-1259.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def config():
    return {
        "collateral_token": "WETH",
        "collateral_amount": "0.5",
        "borrow_token": "USDC",
        "ltv_target": 0.3,
        "interest_rate_mode": "variable",
        "lp_pool": "WETH/USDC/500",
        "lp_range_width_pct": 0.2,
    }


@pytest.fixture
def strategy(config):
    from strategies.incubating.aave_uniswap_yield_stack_optimism.strategy import (
        AaveUniswapYieldStackOptimismStrategy,
    )

    return AaveUniswapYieldStackOptimismStrategy(
        config=config,
        chain="optimism",
        wallet_address="0x" + "0" * 40,
    )


def _mock_market(weth_balance: float = 1.0, weth_price: float = 2500.0, usdc_price: float = 1.0) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        if token == "WETH":
            return Decimal(str(weth_price))
        return Decimal(str(usdc_price))

    market.price = MagicMock(side_effect=price_fn)

    def balance_fn(token):
        bal = MagicMock()
        if token == "WETH":
            bal.balance = Decimal(str(weth_balance))
        else:
            bal.balance = Decimal("10000")
        return bal

    market.balance = MagicMock(side_effect=balance_fn)
    return market


class TestInitialization:
    def test_chain_is_optimism(self, strategy):
        assert strategy.chain == "optimism"

    def test_initial_state_is_idle(self, strategy):
        assert strategy._loop_state == "idle"

    def test_config_values(self, strategy):
        assert strategy.collateral_token == "WETH"
        assert strategy.collateral_amount == Decimal("0.5")
        assert strategy.borrow_token == "USDC"
        assert strategy.ltv_target == Decimal("0.3")
        assert strategy.lp_pool == "WETH/USDC/500"


class TestStateMachine:
    def test_phase1_supply_from_idle(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._loop_state == "supplying"

    def test_phase2_borrow_from_supplied(self, strategy):
        strategy._loop_state = "supplied"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "BORROW"
        assert strategy._loop_state == "borrowing"

    def test_phase3_swap_from_borrowed(self, strategy):
        strategy._loop_state = "borrowed"
        strategy._borrowed_amount = Decimal("375")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert strategy._loop_state == "swapping"

    def test_phase4_lp_open_from_swapped(self, strategy):
        strategy._loop_state = "swapped"
        strategy._borrowed_amount = Decimal("375")
        strategy._swapped_weth_amount = Decimal("0.075")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "LP_OPEN"
        assert strategy._loop_state == "lp_opening"

    def test_hold_when_complete(self, strategy):
        strategy._loop_state = "complete"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_revert_from_transitional_state(self, strategy):
        strategy._loop_state = "supplying"
        strategy._previous_stable_state = "idle"
        market = _mock_market()
        intent = strategy.decide(market)
        assert strategy._loop_state == "idle"
        assert intent.intent_type.value == "HOLD"


class TestOnIntentExecuted:
    def test_supply_success_transitions_to_supplied(self, strategy):
        strategy._loop_state = "supplying"
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "supplied"

    def test_borrow_success_transitions_to_borrowed(self, strategy):
        strategy._loop_state = "borrowing"
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.borrow_amount = Decimal("375")
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "borrowed"
        assert strategy._borrowed_amount == Decimal("375")

    def test_swap_success_transitions_to_swapped(self, strategy):
        strategy._loop_state = "swapping"
        strategy._borrowed_amount = Decimal("375")
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "swapped"

    def test_lp_open_success_transitions_to_complete(self, strategy):
        strategy._loop_state = "lp_opening"
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        result = MagicMock()
        result.position_id = 12345
        strategy.on_intent_executed(intent, success=True, result=result)
        assert strategy._loop_state == "complete"
        assert strategy._lp_position_id == 12345

    def test_failure_reverts_to_previous_state(self, strategy):
        strategy._loop_state = "borrowing"
        strategy._previous_stable_state = "supplied"
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._loop_state == "supplied"


class TestIntentGeneration:
    def test_supply_intent_uses_aave_v3(self, strategy):
        intent = strategy._create_supply_intent()
        assert intent.protocol == "aave_v3"
        assert intent.token == "WETH"
        assert intent.amount == Decimal("0.5")

    def test_borrow_intent_calculates_correct_amount(self, strategy):
        intent = strategy._create_borrow_intent(
            collateral_price=Decimal("2500"),
            borrow_price=Decimal("1"),
        )
        assert intent.protocol == "aave_v3"
        # 0.5 WETH * $2500 * 0.3 LTV / $1 = $375
        assert intent.borrow_amount == Decimal("375.00")

    def test_swap_intent_halves_borrowed(self, strategy):
        strategy._borrowed_amount = Decimal("375")
        intent = strategy._create_swap_intent()
        assert intent.amount == Decimal("187.50")
        assert intent.protocol == "uniswap_v3"

    def test_lp_open_intent_has_range(self, strategy):
        strategy._borrowed_amount = Decimal("375")
        strategy._swapped_weth_amount = Decimal("0.075")
        intent = strategy._create_lp_open_intent(collateral_price=Decimal("2500"))
        assert intent.protocol == "uniswap_v3"
        assert intent.range_lower < intent.range_upper


class TestStatePersistence:
    def test_get_persistent_state(self, strategy):
        strategy._loop_state = "borrowed"
        strategy._borrowed_amount = Decimal("375")
        state = strategy.get_persistent_state()
        assert state["loop_state"] == "borrowed"
        assert state["borrowed_amount"] == "375"

    def test_load_persistent_state(self, strategy):
        strategy.load_persistent_state({
            "loop_state": "complete",
            "supplied_amount": "0.5",
            "borrowed_amount": "375",
            "lp_position_id": 999,
        })
        assert strategy._loop_state == "complete"
        assert strategy._supplied_amount == Decimal("0.5")
        assert strategy._lp_position_id == 999


class TestTeardown:
    def test_supports_teardown(self, strategy):
        # LP_CLOSE blocked by VIB-572, so teardown is not fully supported yet
        assert strategy.supports_teardown() is False

    def test_teardown_intents_with_positions(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._borrowed_amount = Decimal("375")
        strategy._supplied_amount = Decimal("0.5")
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 3  # repay + withdraw + swap
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"
        assert intents[2].intent_type.value == "SWAP"

    def test_teardown_intents_empty_when_no_positions(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0


class TestHoldOnInsufficientBalance:
    def test_hold_when_insufficient_collateral(self, strategy):
        market = _mock_market(weth_balance=0.1)
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "Insufficient" in intent.reason

    def test_hold_when_balance_check_fails(self, strategy):
        market = _mock_market()
        market.balance = MagicMock(side_effect=ValueError("gateway unavailable"))
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "Cannot verify balance" in intent.reason


class TestSwapEnrichmentFailure:
    def test_swap_without_enrichment_sets_flag(self, strategy):
        strategy._loop_state = "swapping"
        strategy._borrowed_amount = Decimal("375")
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == "swapped"
        assert strategy._swap_enrichment_failed is True

    def test_lp_open_queries_balance_when_enrichment_failed(self, strategy):
        strategy._loop_state = "swapped"
        strategy._borrowed_amount = Decimal("375")
        strategy._swapped_weth_amount = Decimal("0")
        strategy._swap_enrichment_failed = True
        # Pre-swap balance was 0.5 WETH, post-swap is 0.575 (delta = 0.075)
        strategy._pre_swap_weth_balance = Decimal("0.5")
        market = _mock_market(weth_balance=0.575)
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "LP_OPEN"
        assert strategy._swapped_weth_amount == Decimal("0.075")
