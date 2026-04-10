"""Unit tests for JoeLendAvaxBorrowRepayLifecycleAvalancheStrategy.

Tests the strategy's decision logic, state machine, lifecycle progression,
teardown, and state persistence without requiring a gateway or Anvil.
Validates native AVAX borrow/repay code path coverage (VIB-2656).
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from strategies.incubating.joelend_avax_borrow_repay_lifecycle_avalanche.strategy import (
    BORROWED,
    BORROWING,
    COMPLETE,
    IDLE,
    REPAID,
    REPAYING,
    STABLE_STATES,
    TRANSITIONAL_STATES,
    WITHDRAWING,
    JoeLendAvaxBorrowRepayLifecycleAvalancheStrategy,
)


# =============================================================================
# Fixtures
# =============================================================================


def _make_strategy(**config_overrides) -> JoeLendAvaxBorrowRepayLifecycleAvalancheStrategy:
    """Create a strategy instance with mocked framework dependencies."""
    default_config = {
        "collateral_token": "USDC.e",
        "collateral_amount": "100",
        "borrow_token": "AVAX",
        "ltv_target": "0.3",
        "borrow_amount_override": "1",
    }
    default_config.update(config_overrides)

    with patch.object(
        JoeLendAvaxBorrowRepayLifecycleAvalancheStrategy, "__init__", lambda self, *a, **kw: None
    ):
        strategy = JoeLendAvaxBorrowRepayLifecycleAvalancheStrategy.__new__(
            JoeLendAvaxBorrowRepayLifecycleAvalancheStrategy
        )

    # Set required base class attributes
    strategy._strategy_id = "test-joelend-avax-borrow-repay"
    strategy._chain = "avalanche"
    strategy._wallet_address = "0x1234567890123456789012345678901234567890"
    strategy._config = default_config
    strategy._hot_config = None

    # Set strategy-specific attributes from config
    strategy.collateral_token = str(default_config["collateral_token"])
    strategy.collateral_amount = Decimal(str(default_config["collateral_amount"]))
    strategy.borrow_token = str(default_config["borrow_token"])
    strategy.ltv_target = Decimal(str(default_config["ltv_target"]))
    borrow_override = default_config.get("borrow_amount_override", "")
    strategy.borrow_amount_override = Decimal(str(borrow_override)) if borrow_override else None

    # State machine
    strategy._loop_state = IDLE
    strategy._previous_stable_state = IDLE
    strategy._supplied_amount = Decimal("0")
    strategy._borrowed_amount = Decimal("0")

    return strategy


def _make_market(collateral_price=Decimal("1"), borrow_price=Decimal("25")):
    """Create a mock MarketSnapshot with USDC.e and AVAX prices."""
    market = MagicMock()

    def price_side_effect(token):
        if token in ("USDC.e",):
            return collateral_price
        if token in ("AVAX",):
            return borrow_price
        raise ValueError(f"Unknown token: {token}")

    market.price.side_effect = price_side_effect
    market.balance.return_value = MagicMock(balance=Decimal("1000"))
    return market


def _make_intent_mock(intent_type_val: str, **kwargs):
    """Create a mock intent with the given type."""
    intent = MagicMock()
    intent.intent_type.value = intent_type_val
    for k, v in kwargs.items():
        setattr(intent, k, v)
    return intent


# =============================================================================
# Metadata
# =============================================================================


class TestStrategyMetadata:
    """Test strategy decorator metadata."""

    def test_strategy_name(self):
        assert (
            JoeLendAvaxBorrowRepayLifecycleAvalancheStrategy.STRATEGY_NAME
            == "joelend_avax_borrow_repay_lifecycle_avalanche"
        )

    def test_supported_chains(self):
        assert "avalanche" in JoeLendAvaxBorrowRepayLifecycleAvalancheStrategy.STRATEGY_METADATA.supported_chains

    def test_supported_protocols(self):
        assert "joelend" in JoeLendAvaxBorrowRepayLifecycleAvalancheStrategy.STRATEGY_METADATA.supported_protocols

    def test_intent_types(self):
        types = JoeLendAvaxBorrowRepayLifecycleAvalancheStrategy.STRATEGY_METADATA.intent_types
        assert "BORROW" in types
        assert "REPAY" in types
        assert "WITHDRAW" in types
        assert "HOLD" in types

    def test_default_chain(self):
        assert JoeLendAvaxBorrowRepayLifecycleAvalancheStrategy.STRATEGY_METADATA.default_chain == "avalanche"

    def test_supports_teardown(self):
        strategy = _make_strategy()
        assert strategy.supports_teardown() is True


# =============================================================================
# State Machine Constants
# =============================================================================


class TestStateConstants:
    """Verify state machine constants are well-formed."""

    def test_stable_states(self):
        assert STABLE_STATES == {IDLE, BORROWED, REPAID, COMPLETE}

    def test_transitional_states(self):
        assert TRANSITIONAL_STATES == {BORROWING, REPAYING, WITHDRAWING}

    def test_no_overlap(self):
        assert STABLE_STATES & TRANSITIONAL_STATES == set()


# =============================================================================
# Lifecycle: IDLE -> BORROW -> REPAY -> WITHDRAW -> COMPLETE
# =============================================================================


class TestLifecycle:
    """Test the full lifecycle state machine."""

    def test_idle_emits_borrow(self):
        strategy = _make_strategy()
        market = _make_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "BORROW"
        assert strategy._loop_state == BORROWING

    def test_borrow_intent_uses_joelend_protocol(self):
        strategy = _make_strategy()
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.protocol == "joelend"

    def test_borrow_intent_uses_avax_borrow_token(self):
        strategy = _make_strategy()
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.borrow_token == "AVAX"

    def test_borrow_intent_uses_usdc_collateral(self):
        strategy = _make_strategy()
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.collateral_token == "USDC.e"
        assert intent.collateral_amount == Decimal("100")

    def test_borrow_intent_uses_override_amount(self):
        strategy = _make_strategy(borrow_amount_override="2.5")
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.borrow_amount == Decimal("2.5")

    def test_borrow_computes_amount_from_ltv_when_no_override(self):
        strategy = _make_strategy(borrow_amount_override="")
        market = _make_market(collateral_price=Decimal("1"), borrow_price=Decimal("25"))
        intent = strategy.decide(market)
        # 100 USDC.e * 1 = 100 USD, * 0.3 LTV = 30 USD, / 25 AVAX = 1.200000
        assert intent.borrow_amount == Decimal("1.200000")

    def test_borrow_holds_when_price_unavailable(self):
        strategy = _make_strategy(borrow_amount_override="")
        market = _make_market()
        market.price.side_effect = ValueError("No price")
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_borrow_intent_uses_avalanche_chain(self):
        strategy = _make_strategy()
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.chain == "avalanche"

    def test_borrowed_state_emits_repay(self):
        strategy = _make_strategy()
        strategy._loop_state = BORROWED
        strategy._borrowed_amount = Decimal("1")
        market = _make_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "REPAY"
        assert strategy._loop_state == REPAYING

    def test_repay_uses_avax_token(self):
        strategy = _make_strategy()
        strategy._loop_state = BORROWED
        strategy._borrowed_amount = Decimal("1")
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.token == "AVAX"

    def test_repay_uses_repay_full(self):
        strategy = _make_strategy()
        strategy._loop_state = BORROWED
        strategy._borrowed_amount = Decimal("1")
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.repay_full is True
        assert intent.amount == Decimal("1")  # explicit amount needed for native AVAX msg.value recovery

    def test_repay_uses_joelend_protocol(self):
        strategy = _make_strategy()
        strategy._loop_state = BORROWED
        strategy._borrowed_amount = Decimal("1")
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.protocol == "joelend"

    def test_repaid_state_emits_withdraw(self):
        strategy = _make_strategy()
        strategy._loop_state = REPAID
        strategy._supplied_amount = Decimal("100")
        market = _make_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "WITHDRAW"
        assert strategy._loop_state == WITHDRAWING

    def test_withdraw_uses_collateral_token(self):
        strategy = _make_strategy()
        strategy._loop_state = REPAID
        strategy._supplied_amount = Decimal("100")
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.token == "USDC.e"

    def test_withdraw_does_not_use_withdraw_all(self):
        strategy = _make_strategy()
        strategy._loop_state = REPAID
        strategy._supplied_amount = Decimal("100")
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.withdraw_all is False

    def test_complete_state_emits_hold(self):
        strategy = _make_strategy()
        strategy._loop_state = COMPLETE
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_full_lifecycle_sequence(self):
        """Test the complete lifecycle: IDLE -> BORROW -> REPAY -> WITHDRAW -> COMPLETE."""
        strategy = _make_strategy()
        market = _make_market()

        # Step 1: BORROW
        intent = strategy.decide(market)
        assert intent.intent_type.value == "BORROW"
        strategy.on_intent_executed(
            _make_intent_mock("BORROW", borrow_amount=Decimal("1")), True, MagicMock()
        )
        assert strategy._loop_state == BORROWED

        # Step 2: REPAY
        intent = strategy.decide(market)
        assert intent.intent_type.value == "REPAY"
        strategy.on_intent_executed(_make_intent_mock("REPAY"), True, MagicMock())
        assert strategy._loop_state == REPAID

        # Step 3: WITHDRAW
        intent = strategy.decide(market)
        assert intent.intent_type.value == "WITHDRAW"
        strategy.on_intent_executed(_make_intent_mock("WITHDRAW"), True, MagicMock())
        assert strategy._loop_state == COMPLETE

        # Step 4: HOLD
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"


# =============================================================================
# Callback: on_intent_executed
# =============================================================================


class TestOnIntentExecuted:
    """Test state transitions on intent execution results."""

    def test_borrow_success_sets_borrowed_state(self):
        strategy = _make_strategy()
        strategy._loop_state = BORROWING
        strategy.on_intent_executed(
            _make_intent_mock("BORROW", borrow_amount=Decimal("1.5")), True, MagicMock()
        )
        assert strategy._loop_state == BORROWED
        assert strategy._supplied_amount == Decimal("100")
        assert strategy._borrowed_amount == Decimal("1.5")

    def test_repay_success_clears_debt(self):
        strategy = _make_strategy()
        strategy._loop_state = REPAYING
        strategy._borrowed_amount = Decimal("1.5")
        strategy.on_intent_executed(_make_intent_mock("REPAY"), True, MagicMock())
        assert strategy._loop_state == REPAID
        assert strategy._borrowed_amount == Decimal("0")

    def test_withdraw_success_clears_collateral(self):
        strategy = _make_strategy()
        strategy._loop_state = WITHDRAWING
        strategy._supplied_amount = Decimal("100")
        strategy.on_intent_executed(_make_intent_mock("WITHDRAW"), True, MagicMock())
        assert strategy._loop_state == COMPLETE
        assert strategy._supplied_amount == Decimal("0")

    def test_borrow_failure_reverts_to_previous_stable(self):
        strategy = _make_strategy()
        strategy._loop_state = BORROWING
        strategy._previous_stable_state = IDLE
        strategy.on_intent_executed(_make_intent_mock("BORROW"), False, None)
        assert strategy._loop_state == IDLE

    def test_repay_failure_reverts_to_borrowed(self):
        strategy = _make_strategy()
        strategy._loop_state = REPAYING
        strategy._previous_stable_state = BORROWED
        strategy.on_intent_executed(_make_intent_mock("REPAY"), False, None)
        assert strategy._loop_state == BORROWED

    def test_withdraw_failure_reverts_to_repaid(self):
        strategy = _make_strategy()
        strategy._loop_state = WITHDRAWING
        strategy._previous_stable_state = REPAID
        strategy.on_intent_executed(_make_intent_mock("WITHDRAW"), False, None)
        assert strategy._loop_state == REPAID

    def test_none_intent_type_is_ignored(self):
        strategy = _make_strategy()
        strategy._loop_state = BORROWING
        intent = MagicMock()
        intent.intent_type = None
        strategy.on_intent_executed(intent, True, MagicMock())
        assert strategy._loop_state == BORROWING  # unchanged


# =============================================================================
# Stuck Transitional State Recovery
# =============================================================================


class TestStuckRecovery:
    """Test recovery from stuck transitional states."""

    @pytest.mark.parametrize("stuck_state,expected_revert", [
        (BORROWING, IDLE),
        (REPAYING, BORROWED),
        (WITHDRAWING, REPAID),
    ])
    def test_stuck_state_reverts_and_holds(self, stuck_state, expected_revert):
        strategy = _make_strategy()
        strategy._loop_state = stuck_state
        strategy._previous_stable_state = expected_revert
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._loop_state == expected_revert


# =============================================================================
# State Persistence
# =============================================================================


class TestPersistence:
    """Test get/load persistent state round-trip."""

    def test_get_persistent_state(self):
        strategy = _make_strategy()
        strategy._loop_state = BORROWED
        strategy._previous_stable_state = BORROWED
        strategy._supplied_amount = Decimal("100")
        strategy._borrowed_amount = Decimal("1.5")
        state = strategy.get_persistent_state()
        assert state["state"] == BORROWED
        assert state["previous_stable_state"] == BORROWED
        assert state["supplied_amount"] == "100"
        assert state["borrowed_amount"] == "1.5"

    def test_load_persistent_state(self):
        strategy = _make_strategy()
        strategy.load_persistent_state({
            "state": REPAID,
            "previous_stable_state": REPAID,
            "supplied_amount": "100",
            "borrowed_amount": "0",
        })
        assert strategy._loop_state == REPAID
        assert strategy._supplied_amount == Decimal("100")
        assert strategy._borrowed_amount == Decimal("0")

    def test_load_defaults_to_idle(self):
        strategy = _make_strategy()
        strategy.load_persistent_state({})
        assert strategy._loop_state == IDLE
        assert strategy._supplied_amount == Decimal("0")
        assert strategy._borrowed_amount == Decimal("0")

    def test_round_trip(self):
        strategy = _make_strategy()
        strategy._loop_state = BORROWED
        strategy._supplied_amount = Decimal("100")
        strategy._borrowed_amount = Decimal("1.5")
        state = strategy.get_persistent_state()

        strategy2 = _make_strategy()
        strategy2.load_persistent_state(state)
        assert strategy2._loop_state == strategy._loop_state
        assert strategy2._supplied_amount == strategy._supplied_amount
        assert strategy2._borrowed_amount == strategy._borrowed_amount


# =============================================================================
# Status
# =============================================================================


class TestStatus:
    """Test get_status output."""

    def test_status_includes_strategy_name(self):
        strategy = _make_strategy()
        status = strategy.get_status()
        assert status["strategy"] == "joelend_avax_borrow_repay_lifecycle_avalanche"

    def test_status_includes_chain(self):
        strategy = _make_strategy()
        status = strategy.get_status()
        assert status["chain"] == "avalanche"

    def test_status_includes_state(self):
        strategy = _make_strategy()
        strategy._loop_state = BORROWED
        status = strategy.get_status()
        assert status["state"] == BORROWED

    def test_status_includes_collateral(self):
        strategy = _make_strategy()
        strategy._supplied_amount = Decimal("100")
        status = strategy.get_status()
        assert "100" in status["collateral"]
        assert "USDC.e" in status["collateral"]

    def test_status_includes_borrowed(self):
        strategy = _make_strategy()
        strategy._borrowed_amount = Decimal("1.5")
        status = strategy.get_status()
        assert "1.5" in status["borrowed"]
        assert "AVAX" in status["borrowed"]


# =============================================================================
# Teardown
# =============================================================================


class TestTeardown:
    """Test teardown methods."""

    def test_get_open_positions_empty_when_idle(self):
        strategy = _make_strategy()
        with patch.object(strategy, "create_market_snapshot", return_value=_make_market()):
            summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_get_open_positions_with_supply(self):
        strategy = _make_strategy()
        strategy._supplied_amount = Decimal("100")
        with patch.object(strategy, "create_market_snapshot", return_value=_make_market()):
            summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].protocol == "joelend"
        assert summary.positions[0].details["asset"] == "USDC.e"

    def test_get_open_positions_with_supply_and_borrow(self):
        strategy = _make_strategy()
        strategy._supplied_amount = Decimal("100")
        strategy._borrowed_amount = Decimal("1.5")
        with patch.object(strategy, "create_market_snapshot", return_value=_make_market()):
            summary = strategy.get_open_positions()
        assert len(summary.positions) == 2
        types = {p.details.get("asset") for p in summary.positions}
        assert "USDC.e" in types
        assert "AVAX" in types

    def test_get_open_positions_handles_price_failure(self):
        strategy = _make_strategy()
        strategy._supplied_amount = Decimal("100")
        mock_market = MagicMock()
        mock_market.price.side_effect = Exception("No price")
        with patch.object(strategy, "create_market_snapshot", side_effect=Exception("No market")):
            summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].value_usd == Decimal("0")

    def test_generate_teardown_repay_then_withdraw(self):
        strategy = _make_strategy()
        strategy._supplied_amount = Decimal("100")
        strategy._borrowed_amount = Decimal("1.5")
        intents = strategy.generate_teardown_intents(mode=MagicMock())
        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[0].token == "AVAX"
        assert intents[0].repay_full is True
        assert intents[1].intent_type.value == "WITHDRAW"
        assert intents[1].token == "USDC.e"

    def test_generate_teardown_only_withdraw_when_no_debt(self):
        strategy = _make_strategy()
        strategy._supplied_amount = Decimal("100")
        strategy._borrowed_amount = Decimal("0")
        intents = strategy.generate_teardown_intents(mode=MagicMock())
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"

    def test_generate_teardown_empty_when_complete(self):
        strategy = _make_strategy()
        intents = strategy.generate_teardown_intents(mode=MagicMock())
        assert len(intents) == 0


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_unknown_state_returns_hold(self):
        strategy = _make_strategy()
        strategy._loop_state = "unknown_state"
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_zero_borrow_amount_returns_hold(self):
        strategy = _make_strategy(borrow_amount_override="0")
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_exception_in_decide_returns_hold(self):
        strategy = _make_strategy(borrow_amount_override="")
        market = MagicMock()
        market.price.side_effect = RuntimeError("unexpected")
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_transition_preserves_previous_stable(self):
        strategy = _make_strategy()
        strategy._loop_state = BORROWED
        strategy._previous_stable_state = BORROWED
        strategy._transition(REPAYING)
        assert strategy._loop_state == REPAYING
        assert strategy._previous_stable_state == BORROWED

    def test_transition_from_transitional_does_not_update_previous(self):
        strategy = _make_strategy()
        strategy._loop_state = BORROWING
        strategy._previous_stable_state = IDLE
        strategy._transition(REPAYING)
        assert strategy._previous_stable_state == IDLE  # not updated
