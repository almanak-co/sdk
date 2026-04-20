"""Unit tests for Compound V3 + Velodrome V2 Carry Trade on Optimism (VIB-2367).

Tests the state machine, intent generation, callback handling, teardown,
and state persistence without requiring a gateway or Anvil fork.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.vocabulary import IntentType


# =============================================================================
# Helpers
# =============================================================================


def _create_strategy(config_overrides: dict | None = None):
    """Create a CompoundV3VelodromeCarryOptimismStrategy with mocked framework deps."""
    from strategies.incubating.compound_v3_velodrome_carry_optimism.strategy import (
        CompoundV3VelodromeCarryOptimismStrategy,
    )

    config = {
        "strategy_id": "test-compound-v3-velo-carry",
        "strategy_name": "test-compound-v3-velo-carry",
        "chain": "optimism",
        "collateral_token": "WETH",
        "collateral_amount": "0.5",
        "borrow_token": "USDC",
        "borrow_amount": "150",
        "market_id": "usdc",
        "lp_weth_amount": "0.001",
        "lp_usdc_amount": "3",
        "force_action": "lifecycle",
    }
    if config_overrides:
        config.update(config_overrides)

    with patch.object(CompoundV3VelodromeCarryOptimismStrategy, "__init__", lambda self, *a, **kw: None):
        strategy = CompoundV3VelodromeCarryOptimismStrategy.__new__(CompoundV3VelodromeCarryOptimismStrategy)

    strategy._strategy_id = config["strategy_id"]
    strategy._chain = config["chain"]
    strategy.collateral_token = config["collateral_token"]
    strategy.collateral_amount = Decimal(config["collateral_amount"])
    strategy.borrow_token = config["borrow_token"]
    strategy.borrow_amount = Decimal(config["borrow_amount"])
    strategy.market_id = config["market_id"]
    strategy.lp_weth_amount = Decimal(config["lp_weth_amount"])
    strategy.lp_usdc_amount = Decimal(config["lp_usdc_amount"])
    strategy.force_action = config["force_action"]
    strategy._state = "idle"
    strategy._prev_stable_state = "idle"
    strategy._supplied_amount = Decimal("0")
    strategy._borrowed_amount = Decimal("0")
    strategy._lp_position_id = None

    return strategy


def _mock_market(weth_price=Decimal("3000"), usdc_price=Decimal("1")):
    """Create a mock MarketSnapshot."""
    market = MagicMock()

    def price_fn(token):
        prices = {"WETH": weth_price, "USDC": usdc_price}
        if token not in prices:
            raise ValueError(f"No price for {token}")
        return prices[token]

    market.price = MagicMock(side_effect=price_fn)
    return market


def _mock_intent_with_type(intent_type_str: str):
    """Create a mock intent with the given type string."""
    intent = MagicMock()
    intent.intent_type = MagicMock()
    intent.intent_type.value = intent_type_str
    return intent


# =============================================================================
# State Machine Tests
# =============================================================================


class TestLifecycleStateMachine:
    """Test the full lifecycle state machine."""

    def test_idle_emits_supply(self):
        strategy = _create_strategy()
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type == IntentType.SUPPLY

    def test_supplied_emits_borrow(self):
        strategy = _create_strategy()
        strategy._state = "supplied"
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type == IntentType.BORROW

    def test_borrowed_emits_lp_open(self):
        strategy = _create_strategy()
        strategy._state = "borrowed"
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type == IntentType.LP_OPEN

    def test_lp_open_emits_lp_close(self):
        strategy = _create_strategy()
        strategy._state = "lp_open"
        strategy._lp_position_id = "12345"
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type == IntentType.LP_CLOSE

    def test_lp_closed_emits_repay(self):
        strategy = _create_strategy()
        strategy._state = "lp_closed"
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type == IntentType.REPAY

    def test_repaid_emits_withdraw(self):
        strategy = _create_strategy()
        strategy._state = "repaid"
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type == IntentType.WITHDRAW

    def test_complete_emits_hold(self):
        strategy = _create_strategy()
        strategy._state = "complete"
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type == IntentType.HOLD


# =============================================================================
# Callback Tests
# =============================================================================


class TestCallbacks:
    """Test on_intent_executed callback updates state correctly."""

    def test_supply_callback_updates_state(self):
        strategy = _create_strategy()
        strategy._state = "supplying"
        intent = _mock_intent_with_type("SUPPLY")

        strategy.on_intent_executed(intent, success=True, result=None)

        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("0.5")

    def test_borrow_callback_updates_state(self):
        strategy = _create_strategy()
        strategy._state = "borrowing"
        intent = _mock_intent_with_type("BORROW")

        strategy.on_intent_executed(intent, success=True, result=None)

        assert strategy._state == "borrowed"
        assert strategy._borrowed_amount == Decimal("150")

    def test_lp_open_callback_captures_position_id(self):
        strategy = _create_strategy()
        strategy._state = "lp_opening"
        intent = _mock_intent_with_type("LP_OPEN")
        result = MagicMock()
        result.position_id = "67890"

        strategy.on_intent_executed(intent, success=True, result=result)

        assert strategy._state == "lp_open"
        assert strategy._lp_position_id == "67890"

    def test_failure_reverts_to_prev_stable(self):
        strategy = _create_strategy()
        strategy._state = "borrowing"
        strategy._prev_stable_state = "supplied"
        intent = _mock_intent_with_type("BORROW")

        strategy.on_intent_executed(intent, success=False, result=None)

        assert strategy._state == "supplied"


# =============================================================================
# Persistence Tests
# =============================================================================


class TestPersistence:
    """Test state save/load roundtrip."""

    def test_state_roundtrip(self):
        strategy = _create_strategy()
        strategy._state = "borrowed"
        strategy._prev_stable_state = "supplied"
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("150")
        strategy._lp_position_id = "12345"

        saved = strategy.get_persistent_state()

        strategy2 = _create_strategy()
        strategy2.load_persistent_state(saved)

        assert strategy2._state == "borrowed"
        assert strategy2._prev_stable_state == "supplied"
        assert strategy2._supplied_amount == Decimal("0.5")
        assert strategy2._borrowed_amount == Decimal("150")
        assert strategy2._lp_position_id == "12345"


# =============================================================================
# Teardown Tests
# =============================================================================


class TestTeardown:
    """Test teardown intent generation."""

    def test_teardown_from_lp_open_state(self):
        strategy = _create_strategy()
        strategy._state = "lp_open"
        strategy._lp_position_id = "12345"
        strategy._borrowed_amount = Decimal("150")
        strategy._supplied_amount = Decimal("0.5")

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        # Should produce: LP_CLOSE, REPAY, WITHDRAW (in that order)
        assert len(intents) == 3
        assert intents[0].intent_type == IntentType.LP_CLOSE
        assert intents[1].intent_type == IntentType.REPAY
        assert intents[2].intent_type == IntentType.WITHDRAW

    def test_teardown_from_repaid_state(self):
        strategy = _create_strategy()
        strategy._state = "repaid"
        strategy._supplied_amount = Decimal("0.5")

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        # Only WITHDRAW needed
        assert len(intents) == 1
        assert intents[0].intent_type == IntentType.WITHDRAW

    def test_get_open_positions_in_borrowed_state(self):
        strategy = _create_strategy()
        strategy._state = "borrowed"

        summary = strategy.get_open_positions()

        # Should have BORROW + SUPPLY positions
        position_types = {p.position_type.value for p in summary.positions}
        assert "BORROW" in position_types
        assert "SUPPLY" in position_types


# =============================================================================
# Intent Content Tests
# =============================================================================


class TestIntentContent:
    """Test that generated intents have correct protocol and parameters."""

    def test_supply_intent_uses_compound_v3(self):
        strategy = _create_strategy()
        strategy._state = "idle"
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent.protocol == "compound_v3"
        assert intent.token == "WETH"
        assert intent.amount == Decimal("0.5")

    def test_borrow_intent_uses_compound_v3(self):
        strategy = _create_strategy()
        strategy._state = "supplied"
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent.protocol == "compound_v3"
        assert intent.borrow_amount == Decimal("150")

    def test_lp_open_intent_uses_aerodrome(self):
        strategy = _create_strategy()
        strategy._state = "borrowed"
        market = _mock_market()

        intent = strategy.decide(market)

        assert intent.protocol == "aerodrome"
        assert intent.pool == "WETH/USDC/volatile"
