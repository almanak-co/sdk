"""Unit tests for the Aave V3 Lending Sonic demo strategy.

Tests the strategy's decision logic, state machine, lifecycle progression,
teardown, and state persistence without requiring a gateway or Anvil.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.demo_strategies.aave_v3_lending_sonic.strategy import AaveV3LendingSonicStrategy


# =============================================================================
# Fixtures
# =============================================================================


def _make_strategy(**config_overrides) -> AaveV3LendingSonicStrategy:
    """Create a strategy instance with mocked framework dependencies."""
    default_config = {
        "collateral_token": "USDC",
        "collateral_amount": "100",
        "borrow_token": "WETH",
        "ltv_target": 0.3,
        "borrow_amount_override": "0.01",
        "force_action": "",
    }
    default_config.update(config_overrides)

    with patch.object(AaveV3LendingSonicStrategy, "__init__", lambda self, *a, **kw: None):
        strategy = AaveV3LendingSonicStrategy.__new__(AaveV3LendingSonicStrategy)

    # Set required base class attributes
    strategy._strategy_id = "test-aave-v3-sonic"
    strategy._chain = "sonic"
    strategy._wallet_address = "0x1234567890123456789012345678901234567890"
    strategy._config = default_config
    strategy._hot_config = None

    # Set strategy-specific attributes
    strategy.collateral_token = str(default_config["collateral_token"])
    strategy.collateral_amount = Decimal(str(default_config["collateral_amount"]))
    strategy.borrow_token = str(default_config["borrow_token"])
    strategy.ltv_target = Decimal(str(default_config["ltv_target"]))
    strategy.force_action = str(default_config.get("force_action", "")).lower()
    borrow_override = default_config.get("borrow_amount_override", "")
    strategy.borrow_amount_override = Decimal(str(borrow_override)) if borrow_override else None

    strategy._state = "idle"
    strategy._previous_stable_state = "idle"
    strategy._supplied_amount = Decimal("0")
    strategy._borrowed_amount = Decimal("0")

    return strategy


def _make_market(collateral_price=Decimal("1"), borrow_price=Decimal("3000")):
    """Create a mock MarketSnapshot."""
    market = MagicMock()

    def price_side_effect(token):
        if token in ("USDC", "USDT"):
            return collateral_price
        if token in ("WETH", "ETH"):
            return borrow_price
        raise ValueError(f"Unknown token: {token}")

    market.price.side_effect = price_side_effect
    market.balance.return_value = MagicMock(balance=Decimal("10000"))
    return market


# =============================================================================
# Metadata
# =============================================================================


class TestStrategyMetadata:
    """Test strategy decorator metadata."""

    def test_strategy_name(self):
        assert AaveV3LendingSonicStrategy.STRATEGY_NAME == "aave_v3_lending_sonic"

    def test_supported_chains(self):
        assert "sonic" in AaveV3LendingSonicStrategy.STRATEGY_METADATA.supported_chains

    def test_supported_protocols(self):
        assert "aave_v3" in AaveV3LendingSonicStrategy.STRATEGY_METADATA.supported_protocols

    def test_intent_types(self):
        types = AaveV3LendingSonicStrategy.STRATEGY_METADATA.intent_types
        assert "SUPPLY" in types
        assert "BORROW" in types
        assert "REPAY" in types
        assert "WITHDRAW" in types
        assert "HOLD" in types


# =============================================================================
# Decide - Force Actions
# =============================================================================


class TestForceActions:
    """Test forced action modes."""

    def test_force_supply(self):
        strategy = _make_strategy(force_action="supply")
        market = _make_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"

    def test_force_borrow(self):
        strategy = _make_strategy(force_action="borrow")
        market = _make_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "BORROW"

    def test_force_repay(self):
        strategy = _make_strategy(force_action="repay")
        strategy._borrowed_amount = Decimal("0.01")
        market = _make_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "REPAY"

    def test_force_withdraw(self):
        strategy = _make_strategy(force_action="withdraw")
        strategy._supplied_amount = Decimal("100")
        market = _make_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "WITHDRAW"

    def test_force_borrow_with_zero_price_holds(self):
        strategy = _make_strategy(force_action="borrow", borrow_amount_override="")
        market = _make_market()
        market.price.side_effect = ValueError("No price")

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_force_borrow_with_override_bypasses_price_check(self):
        strategy = _make_strategy(force_action="borrow", borrow_amount_override="0.01")
        market = _make_market()
        market.price.side_effect = ValueError("No price")

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "BORROW"


# =============================================================================
# Lifecycle Mode
# =============================================================================


class TestLifecycleMode:
    """Test the lifecycle state machine: supply -> borrow -> repay -> withdraw."""

    def test_lifecycle_starts_with_supply(self):
        strategy = _make_strategy(force_action="lifecycle")
        market = _make_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"

    def test_lifecycle_supplied_to_borrow(self):
        strategy = _make_strategy(force_action="lifecycle")
        strategy._state = "supplied"
        strategy._supplied_amount = Decimal("100")
        market = _make_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "BORROW"
        assert strategy._state == "borrowing"

    def test_lifecycle_borrowed_to_repay(self):
        strategy = _make_strategy(force_action="lifecycle")
        strategy._state = "borrowed"
        strategy._borrowed_amount = Decimal("0.01")
        market = _make_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "REPAY"
        assert strategy._state == "repaying"

    def test_lifecycle_repaid_to_withdraw(self):
        strategy = _make_strategy(force_action="lifecycle")
        strategy._state = "repaid"
        strategy._supplied_amount = Decimal("100")
        market = _make_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "WITHDRAW"
        assert strategy._state == "withdrawing"

    def test_lifecycle_complete_holds(self):
        strategy = _make_strategy(force_action="lifecycle")
        strategy._state = "complete"
        market = _make_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "complete" in intent.reason.lower()


# =============================================================================
# on_intent_executed
# =============================================================================


class TestOnIntentExecuted:
    """Test state transitions on intent execution callbacks."""

    def test_supply_success_transitions_to_supplied(self):
        strategy = _make_strategy()
        strategy._state = "supplying"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("100")

    def test_borrow_success_transitions_to_borrowed(self):
        strategy = _make_strategy()
        strategy._state = "borrowing"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "BORROW"
        mock_intent.borrow_amount = Decimal("0.01")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == "borrowed"
        assert strategy._borrowed_amount == Decimal("0.01")

    def test_repay_success_clears_debt(self):
        strategy = _make_strategy()
        strategy._state = "repaying"
        strategy._borrowed_amount = Decimal("0.01")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "REPAY"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == "repaid"
        assert strategy._borrowed_amount == Decimal("0")

    def test_withdraw_success_completes_lifecycle(self):
        strategy = _make_strategy()
        strategy._state = "withdrawing"
        strategy._supplied_amount = Decimal("100")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "WITHDRAW"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == "complete"
        assert strategy._supplied_amount == Decimal("0")

    def test_failure_reverts_to_previous_state(self):
        strategy = _make_strategy()
        strategy._state = "supplying"
        strategy._previous_stable_state = "idle"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"

        strategy.on_intent_executed(mock_intent, success=False, result=None)

        assert strategy._state == "idle"


# =============================================================================
# Teardown
# =============================================================================


class TestTeardown:
    """Test teardown interface."""

    def test_no_positions_returns_empty(self):
        strategy = _make_strategy()
        positions = strategy.get_open_positions()

        assert len(positions.positions) == 0

    def test_supplied_position_reported(self):
        strategy = _make_strategy()
        strategy._supplied_amount = Decimal("100")

        positions = strategy.get_open_positions()

        assert len(positions.positions) == 1
        assert positions.positions[0].protocol == "aave_v3"
        assert positions.positions[0].chain == "sonic"

    def test_borrowed_position_reported(self):
        strategy = _make_strategy()
        strategy._supplied_amount = Decimal("100")
        strategy._borrowed_amount = Decimal("0.01")

        positions = strategy.get_open_positions()

        assert len(positions.positions) == 2

    def test_teardown_generates_repay_then_withdraw(self):
        strategy = _make_strategy()
        strategy._supplied_amount = Decimal("100")
        strategy._borrowed_amount = Decimal("0.01")

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"

    def test_teardown_no_positions_empty_intents(self):
        strategy = _make_strategy()

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 0


# =============================================================================
# State Persistence
# =============================================================================


class TestStatePersistence:
    """Test state save/restore."""

    def test_get_persistent_state(self):
        strategy = _make_strategy()
        strategy._state = "borrowed"
        strategy._supplied_amount = Decimal("100")
        strategy._borrowed_amount = Decimal("0.01")

        state = strategy.get_persistent_state()

        assert state["state"] == "borrowed"
        assert state["supplied_amount"] == "100"
        assert state["borrowed_amount"] == "0.01"

    def test_load_persistent_state(self):
        strategy = _make_strategy()

        strategy.load_persistent_state({
            "state": "supplied",
            "previous_stable_state": "idle",
            "supplied_amount": "200",
            "borrowed_amount": "0",
        })

        assert strategy._state == "supplied"
        assert strategy._supplied_amount == Decimal("200")

    def test_roundtrip_persistence(self):
        strategy = _make_strategy()
        strategy._state = "repaid"
        strategy._previous_stable_state = "borrowed"
        strategy._supplied_amount = Decimal("100")
        strategy._borrowed_amount = Decimal("0")

        saved = strategy.get_persistent_state()

        strategy2 = _make_strategy()
        strategy2.load_persistent_state(saved)

        assert strategy2._state == strategy._state
        assert strategy2._previous_stable_state == strategy._previous_stable_state
        assert strategy2._supplied_amount == strategy._supplied_amount
        assert strategy2._borrowed_amount == strategy._borrowed_amount
