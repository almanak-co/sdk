"""Unit tests for Compound V3 + Aerodrome Yield Farm on Base strategy (VIB-2126).

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

STRATEGY_MODULE = "strategies.incubating.compound_v3_aerodrome_yield_farm_base.strategy"


def _create_strategy(config_overrides: dict | None = None):
    """Create a CompoundV3AerodromeYieldFarmBaseStrategy with mocked framework deps."""
    import sys

    sys.path.insert(0, "strategies/incubating/compound_v3_aerodrome_yield_farm_base")
    from strategies.incubating.compound_v3_aerodrome_yield_farm_base.strategy import (
        CompoundV3AerodromeYieldFarmBaseStrategy,
    )

    config = {
        "deployment_id": "test-compound-aero-yield",
        "strategy_name": "test-compound-aero-yield",
        "chain": "base",
        "collateral_token": "WETH",
        "collateral_amount": "0.05",
        "borrow_token": "USDC",
        "compound_market": "usdc",
        "ltv_target": "0.3",
        "lp_pool": "WETH/USDC",
        "lp_amount0_weth": "0.005",
        "lp_amount1_usdc": "10",
    }
    if config_overrides:
        config.update(config_overrides)

    with patch.object(CompoundV3AerodromeYieldFarmBaseStrategy, "__init__", lambda self, *a, **kw: None):
        strategy = CompoundV3AerodromeYieldFarmBaseStrategy.__new__(CompoundV3AerodromeYieldFarmBaseStrategy)

    strategy._deployment_id = config["deployment_id"]
    strategy._chain = config["chain"]
    strategy.collateral_token = config["collateral_token"]
    strategy.collateral_amount = Decimal(config["collateral_amount"])
    strategy.borrow_token = config["borrow_token"]
    strategy.compound_market = config["compound_market"]
    strategy.ltv_target = Decimal(config["ltv_target"])
    strategy.lp_pool = config["lp_pool"]
    strategy.lp_amount0_weth = Decimal(config["lp_amount0_weth"])
    strategy.lp_amount1_usdc = Decimal(config["lp_amount1_usdc"])
    strategy._state = "idle"
    strategy._previous_stable_state = "idle"
    strategy._supplied_amount = Decimal("0")
    strategy._borrowed_amount = Decimal("0")
    strategy._lp_position_active = False
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


def _mock_intent_with_type(intent_type_val: str, **extra):
    """Create a mock intent with the given type."""
    intent = MagicMock()
    intent.intent_type = MagicMock()
    intent.intent_type.value = intent_type_val
    for k, v in extra.items():
        setattr(intent, k, v)
    return intent


# =============================================================================
# State Machine Tests
# =============================================================================


class TestStateMachine:
    """Test state transitions and stuck-state recovery."""

    def test_initial_state_is_idle(self):
        strategy = _create_strategy()
        assert strategy._state == "idle"

    def test_idle_emits_supply(self):
        strategy = _create_strategy()
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type == IntentType.SUPPLY
        assert intent.protocol == "compound_v3"
        assert intent.token == "WETH"
        assert intent.amount == Decimal("0.05")
        assert intent.use_as_collateral is True
        assert strategy._state == "supplying"

    def test_supplied_emits_borrow(self):
        strategy = _create_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.05")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type == IntentType.BORROW
        assert strategy._state == "borrowing"

    def test_borrow_uses_compound_v3_protocol(self):
        strategy = _create_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.05")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "compound_v3"

    def test_borrow_collateral_already_supplied(self):
        strategy = _create_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.05")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.collateral_token == "WETH"
        # Collateral supplied by the standalone SUPPLY intent (VIB-3586)
        assert intent.collateral_amount == Decimal("0")

    def test_borrow_amount_is_30pct_ltv(self):
        strategy = _create_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.05")
        market = _mock_market(weth_price=Decimal("3000"))
        intent = strategy.decide(market)
        # 0.05 WETH * $3000 = $150 collateral value
        # 30% LTV = $45 borrow
        # $45 / $1 USDC = 45 USDC
        assert intent.borrow_amount == Decimal("45.00")

    def test_borrowed_state_emits_lp_open(self):
        strategy = _create_strategy()
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("45.00")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type == IntentType.LP_OPEN
        assert intent.protocol == "aerodrome"

    def test_lp_open_uses_volatile_pool(self):
        strategy = _create_strategy()
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("45.00")
        market = _mock_market()
        intent = strategy.decide(market)
        assert "volatile" in intent.pool

    def test_lp_open_usdc_capped_by_borrowed(self):
        strategy = _create_strategy()
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("5.00")  # Less than lp_amount1_usdc=10
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.amount1 == Decimal("5.00")

    def test_complete_state_emits_hold(self):
        strategy = _create_strategy()
        strategy._state = "complete"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type == IntentType.HOLD

    def test_stuck_supplying_reverts_to_idle(self):
        strategy = _create_strategy()
        strategy._state = "supplying"
        strategy._previous_stable_state = "idle"
        market = _mock_market()
        intent = strategy.decide(market)
        # Should revert to idle, then emit supply
        assert intent.intent_type == IntentType.SUPPLY

    def test_stuck_borrowing_reverts_to_supplied(self):
        strategy = _create_strategy()
        strategy._state = "borrowing"
        strategy._previous_stable_state = "supplied"
        strategy._supplied_amount = Decimal("0.05")
        market = _mock_market()
        intent = strategy.decide(market)
        # Should revert to supplied, then emit borrow
        assert intent.intent_type == IntentType.BORROW

    def test_stuck_opening_lp_reverts_to_borrowed(self):
        strategy = _create_strategy()
        strategy._state = "opening_lp"
        strategy._previous_stable_state = "borrowed"
        strategy._borrowed_amount = Decimal("45.00")
        market = _mock_market()
        intent = strategy.decide(market)
        # Should revert to borrowed, then emit LP_OPEN
        assert intent.intent_type == IntentType.LP_OPEN

    def test_zero_borrow_emits_hold(self):
        strategy = _create_strategy({"collateral_amount": "0.0000001"})
        # Borrow amount is computed in the BORROW phase (SUPPLIED state).
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.0000001")
        market = _mock_market(weth_price=Decimal("1"))
        intent = strategy.decide(market)
        # Collateral value so small that borrow rounds to 0
        assert intent.intent_type == IntentType.HOLD


# =============================================================================
# Callback Tests
# =============================================================================


class TestCallbacks:
    """Test on_intent_executed callbacks."""

    def test_supply_success_transitions_to_supplied(self):
        strategy = _create_strategy()
        strategy._state = "supplying"
        intent = _mock_intent_with_type("SUPPLY", amount=Decimal("0.05"))
        result = MagicMock()
        strategy.on_intent_executed(intent, success=True, result=result)
        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("0.05")

    def test_borrow_success_transitions_to_borrowed(self):
        strategy = _create_strategy()
        strategy._state = "borrowing"
        strategy._supplied_amount = Decimal("0.05")
        intent = _mock_intent_with_type("BORROW", borrow_amount=Decimal("45.00"))
        result = MagicMock()
        strategy.on_intent_executed(intent, success=True, result=result)
        assert strategy._state == "borrowed"
        assert strategy._supplied_amount == Decimal("0.05")
        assert strategy._borrowed_amount == Decimal("45.00")

    def test_lp_open_success_transitions_to_complete(self):
        strategy = _create_strategy()
        strategy._state = "opening_lp"
        intent = _mock_intent_with_type("LP_OPEN")
        result = MagicMock()
        result.position_id = "aerodrome-lp-12345"
        strategy.on_intent_executed(intent, success=True, result=result)
        assert strategy._state == "complete"
        assert strategy._lp_position_active is True
        assert strategy._lp_position_id == "aerodrome-lp-12345"

    def test_lp_open_success_without_position_id(self):
        strategy = _create_strategy()
        strategy._state = "opening_lp"
        intent = _mock_intent_with_type("LP_OPEN")
        result = MagicMock(spec=[])  # No position_id attr
        strategy.on_intent_executed(intent, success=True, result=result)
        assert strategy._state == "complete"
        assert strategy._lp_position_active is True
        assert strategy._lp_position_id is None

    def test_borrow_failure_reverts_state(self):
        strategy = _create_strategy()
        strategy._state = "borrowing"
        strategy._previous_stable_state = "idle"
        intent = _mock_intent_with_type("BORROW")
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "idle"

    def test_lp_open_failure_reverts_state(self):
        strategy = _create_strategy()
        strategy._state = "opening_lp"
        strategy._previous_stable_state = "borrowed"
        intent = _mock_intent_with_type("LP_OPEN")
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._state == "borrowed"

    def test_repay_clears_borrowed_amount(self):
        strategy = _create_strategy()
        strategy._borrowed_amount = Decimal("45.00")
        intent = _mock_intent_with_type("REPAY")
        strategy.on_intent_executed(intent, success=True, result=MagicMock())
        assert strategy._borrowed_amount == Decimal("0")

    def test_withdraw_clears_supplied_amount(self):
        strategy = _create_strategy()
        strategy._supplied_amount = Decimal("0.05")
        intent = _mock_intent_with_type("WITHDRAW")
        strategy.on_intent_executed(intent, success=True, result=MagicMock())
        assert strategy._supplied_amount == Decimal("0")

    def test_lp_close_clears_position(self):
        strategy = _create_strategy()
        strategy._lp_position_active = True
        strategy._lp_position_id = "test-id"
        intent = _mock_intent_with_type("LP_CLOSE")
        strategy.on_intent_executed(intent, success=True, result=MagicMock())
        assert strategy._lp_position_active is False
        assert strategy._lp_position_id is None


# =============================================================================
# State Persistence Tests
# =============================================================================


class TestStatePersistence:
    """Test state save/restore."""

    def test_get_persistent_state(self):
        strategy = _create_strategy()
        strategy._state = "borrowed"
        strategy._supplied_amount = Decimal("0.05")
        strategy._borrowed_amount = Decimal("45.00")
        state = strategy.get_persistent_state()
        assert state["state"] == "borrowed"
        assert state["supplied_amount"] == "0.05"
        assert state["borrowed_amount"] == "45.00"
        assert state["lp_position_active"] is False

    def test_load_persistent_state(self):
        strategy = _create_strategy()
        strategy.load_persistent_state({
            "state": "complete",
            "previous_stable_state": "borrowed",
            "supplied_amount": "0.05",
            "borrowed_amount": "45.00",
            "lp_position_active": True,
            "lp_position_id": "lp-123",
        })
        assert strategy._state == "complete"
        assert strategy._previous_stable_state == "borrowed"
        assert strategy._supplied_amount == Decimal("0.05")
        assert strategy._borrowed_amount == Decimal("45.00")
        assert strategy._lp_position_active is True
        assert strategy._lp_position_id == "lp-123"

    def test_roundtrip_persistence(self):
        strategy = _create_strategy()
        strategy._state = "complete"
        strategy._supplied_amount = Decimal("0.05")
        strategy._borrowed_amount = Decimal("45.00")
        strategy._lp_position_active = True
        strategy._lp_position_id = "aerodrome-lp-42"

        state = strategy.get_persistent_state()

        strategy2 = _create_strategy()
        strategy2.load_persistent_state(state)
        assert strategy2._state == strategy._state
        assert strategy2._supplied_amount == strategy._supplied_amount
        assert strategy2._borrowed_amount == strategy._borrowed_amount
        assert strategy2._lp_position_active == strategy._lp_position_active
        assert strategy2._lp_position_id == strategy._lp_position_id


# =============================================================================
# Teardown Tests
# =============================================================================


class TestTeardown:
    """Test teardown intent generation."""

    def test_supports_teardown(self):
        strategy = _create_strategy()
        assert strategy.supports_teardown() is True

    def test_full_teardown_order(self):
        """Teardown should close LP, then repay, then withdraw."""
        from almanak.framework.teardown import TeardownMode

        strategy = _create_strategy()
        strategy._lp_position_active = True
        strategy._lp_position_id = "aerodrome-lp-42"
        strategy._borrowed_amount = Decimal("45.00")
        strategy._supplied_amount = Decimal("0.05")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 3
        assert intents[0].intent_type == IntentType.LP_CLOSE
        assert intents[0].protocol == "aerodrome"
        assert intents[1].intent_type == IntentType.REPAY
        assert intents[1].protocol == "compound_v3"
        assert intents[2].intent_type == IntentType.WITHDRAW
        assert intents[2].protocol == "compound_v3"

    def test_teardown_no_lp(self):
        """Teardown without LP should only repay + withdraw."""
        from almanak.framework.teardown import TeardownMode

        strategy = _create_strategy()
        strategy._borrowed_amount = Decimal("45.00")
        strategy._supplied_amount = Decimal("0.05")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 2
        assert intents[0].intent_type == IntentType.REPAY
        assert intents[1].intent_type == IntentType.WITHDRAW

    def test_teardown_empty(self):
        """Teardown with no positions should return empty list."""
        from almanak.framework.teardown import TeardownMode

        strategy = _create_strategy()
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0

    def test_teardown_repay_full_flag(self):
        from almanak.framework.teardown import TeardownMode

        strategy = _create_strategy()
        strategy._borrowed_amount = Decimal("45.00")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        repay_intent = intents[0]
        assert repay_intent.repay_full is True


# =============================================================================
# Open Positions Tests
# =============================================================================


class TestOpenPositions:
    """Test get_open_positions."""

    def test_full_positions(self):
        strategy = _create_strategy()
        strategy._supplied_amount = Decimal("0.05")
        strategy._borrowed_amount = Decimal("45.00")
        strategy._lp_position_active = True

        with patch.object(strategy, "create_market_snapshot") as mock_snap:
            mock_snap.return_value = _mock_market()
            summary = strategy.get_open_positions()

        assert len(summary.positions) == 3
        types = [p.position_type.value for p in summary.positions]
        assert "SUPPLY" in types
        assert "BORROW" in types
        assert "LP" in types

    def test_no_positions(self):
        strategy = _create_strategy()

        with patch.object(strategy, "create_market_snapshot") as mock_snap:
            mock_snap.return_value = _mock_market()
            summary = strategy.get_open_positions()

        assert len(summary.positions) == 0

    def test_positions_use_compound_v3_protocol(self):
        strategy = _create_strategy()
        strategy._supplied_amount = Decimal("0.05")
        strategy._borrowed_amount = Decimal("45.00")

        with patch.object(strategy, "create_market_snapshot") as mock_snap:
            mock_snap.return_value = _mock_market()
            summary = strategy.get_open_positions()

        for p in summary.positions:
            assert p.protocol == "compound_v3"


# =============================================================================
# Status Tests
# =============================================================================


class TestStatus:
    """Test get_status."""

    def test_status_includes_compound_market(self):
        strategy = _create_strategy()
        status = strategy.get_status()
        assert status["compound_market"] == "usdc"
        assert status["strategy"] == "compound_v3_aerodrome_yield_farm_base"

    def test_status_reflects_state(self):
        strategy = _create_strategy()
        strategy._state = "complete"
        strategy._lp_position_active = True
        status = strategy.get_status()
        assert status["state"] == "complete"
        assert status["lp_position_active"] is True


# =============================================================================
# Price Error Handling
# =============================================================================


class TestPriceErrors:
    """Test behavior when price data is unavailable."""

    def test_price_error_emits_hold(self):
        # Price is read in the BORROW phase (SUPPLIED state), not SUPPLY.
        strategy = _create_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.05")
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("No price"))
        intent = strategy.decide(market)
        assert intent.intent_type == IntentType.HOLD

    def test_exception_in_decide_emits_hold(self):
        strategy = _create_strategy()
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("0.05")
        market = MagicMock()
        market.price = MagicMock(side_effect=RuntimeError("Unexpected"))
        intent = strategy.decide(market)
        assert intent.intent_type == IntentType.HOLD
