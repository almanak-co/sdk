"""Unit tests for the Aave V3 + Enso carry trade on Polygon strategy.

Tests the strategy's decision logic, state machine, lifecycle progression,
teardown, and state persistence without requiring a gateway or Anvil.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from strategies.incubating.aave_enso_carry_polygon.strategy import (
    BORROWED,
    BORROWING,
    IDLE,
    SWAPPED,
    SWAPPING,
    AaveEnsoCarryPolygonStrategy,
)


# =============================================================================
# Fixtures
# =============================================================================


def _make_strategy(**config_overrides) -> AaveEnsoCarryPolygonStrategy:
    """Create a strategy instance with mocked framework dependencies."""
    default_config = {
        "collateral_token": "WETH",
        "collateral_amount": "0.5",
        "borrow_token": "USDC",
        "ltv_target": "0.5",
        "borrow_amount_override": "300",
        "swap_to": "WETH",
        "max_slippage_pct": "3.0",
    }
    default_config.update(config_overrides)

    with patch.object(AaveEnsoCarryPolygonStrategy, "__init__", lambda self, *a, **kw: None):
        strategy = AaveEnsoCarryPolygonStrategy.__new__(AaveEnsoCarryPolygonStrategy)

    # Set required base class attributes
    strategy._strategy_id = "test-aave-enso-carry-polygon"
    strategy._chain = "polygon"
    strategy._wallet_address = "0x1234567890123456789012345678901234567890"
    strategy._config = default_config
    strategy._hot_config = None

    # Set strategy-specific attributes
    strategy.collateral_token = str(default_config["collateral_token"])
    strategy.collateral_amount = Decimal(str(default_config["collateral_amount"]))
    strategy.borrow_token = str(default_config["borrow_token"])
    strategy.ltv_target = Decimal(str(default_config["ltv_target"]))
    strategy.swap_to = str(default_config["swap_to"])
    strategy.max_slippage_pct = Decimal(str(default_config["max_slippage_pct"]))
    borrow_override = default_config.get("borrow_amount_override", "")
    strategy.borrow_amount_override = Decimal(str(borrow_override)) if borrow_override else None

    strategy._state = IDLE
    strategy._previous_stable_state = IDLE
    strategy._supplied_amount = Decimal("0")
    strategy._borrowed_amount = Decimal("0")
    strategy._swap_amount_out = Decimal("0")

    return strategy


def _make_market(weth_price=Decimal("2400"), usdc_price=Decimal("1")):
    """Create a mock MarketSnapshot."""
    market = MagicMock()

    def price_side_effect(token):
        prices = {
            "WETH": weth_price,
            "ETH": weth_price,
            "USDC": usdc_price,
        }
        if token in prices:
            return prices[token]
        raise ValueError(f"Unknown token: {token}")

    market.price.side_effect = price_side_effect
    market.balance.return_value = MagicMock(balance=Decimal("10000"))
    return market


def _mock_intent(intent_type_val: str, **attrs):
    """Create a mock intent with given type."""
    intent = MagicMock()
    intent.intent_type = MagicMock()
    intent.intent_type.value = intent_type_val
    for k, v in attrs.items():
        setattr(intent, k, v)
    return intent


def _mock_result(success=True, swap_amounts=None, extracted_data=None, tx_results=None):
    """Create a mock execution result."""
    result = MagicMock()
    result.success = success
    result.swap_amounts = swap_amounts
    result.extracted_data = extracted_data
    result.transaction_results = tx_results or []
    return result


# =============================================================================
# Metadata
# =============================================================================


class TestStrategyMetadata:
    """Test strategy metadata and decorator configuration."""

    def test_strategy_name(self):
        assert AaveEnsoCarryPolygonStrategy.STRATEGY_NAME == "aave_enso_carry_polygon"

    def test_supported_chains(self):
        metadata = AaveEnsoCarryPolygonStrategy.STRATEGY_METADATA
        assert "polygon" in metadata.supported_chains

    def test_supported_protocols(self):
        metadata = AaveEnsoCarryPolygonStrategy.STRATEGY_METADATA
        assert "aave_v3" in metadata.supported_protocols
        assert "enso" in metadata.supported_protocols

    def test_intent_types(self):
        types = AaveEnsoCarryPolygonStrategy.STRATEGY_METADATA.intent_types
        assert "BORROW" in types
        assert "SWAP" in types
        assert "REPAY" in types
        assert "WITHDRAW" in types
        assert "HOLD" in types

    def test_supports_teardown(self):
        strategy = _make_strategy()
        assert strategy.supports_teardown() is True


# =============================================================================
# Initialization
# =============================================================================


class TestInitialization:
    """Test strategy initialization with various configs."""

    def test_default_config(self):
        strategy = _make_strategy()
        assert strategy.collateral_token == "WETH"
        assert strategy.collateral_amount == Decimal("0.5")
        assert strategy.borrow_token == "USDC"
        assert strategy.ltv_target == Decimal("0.5")
        assert strategy.swap_to == "WETH"
        assert strategy.max_slippage_pct == Decimal("3.0")
        assert strategy._state == IDLE

    def test_custom_config(self):
        strategy = _make_strategy(
            collateral_amount="1.0",
            borrow_amount_override="600",
            max_slippage_pct="5.0",
        )
        assert strategy.collateral_amount == Decimal("1.0")
        assert strategy.borrow_amount_override == Decimal("600")
        assert strategy.max_slippage_pct == Decimal("5.0")

    def test_no_borrow_override(self):
        strategy = _make_strategy(borrow_amount_override="")
        assert strategy.borrow_amount_override is None


# =============================================================================
# State Machine
# =============================================================================


class TestStateMachine:
    """Test state transitions and stuck-state recovery."""

    def test_initial_state_is_idle(self):
        strategy = _make_strategy()
        assert strategy._state == IDLE

    def test_transition_updates_previous(self):
        strategy = _make_strategy()
        strategy._transition(BORROWING)
        assert strategy._state == BORROWING
        assert strategy._previous_stable_state == IDLE

    def test_transition_from_stable_to_transitional(self):
        strategy = _make_strategy()
        strategy._state = BORROWED
        strategy._previous_stable_state = BORROWED
        strategy._transition(SWAPPING)
        assert strategy._state == SWAPPING
        assert strategy._previous_stable_state == BORROWED

    def test_stuck_borrowing_reverts_to_idle(self):
        strategy = _make_strategy()
        strategy._state = BORROWING
        strategy._previous_stable_state = IDLE
        market = _make_market()
        intent = strategy.decide(market)
        # Should revert to IDLE and then return an IntentSequence (BORROW + SWAP)
        assert intent is not None
        assert hasattr(intent, "intents")  # IntentSequence

    def test_stuck_swapping_reverts_to_borrowed_then_retries_swap(self):
        strategy = _make_strategy()
        strategy._state = SWAPPING
        strategy._previous_stable_state = BORROWED
        strategy._borrowed_amount = Decimal("300")
        market = _make_market()
        intent = strategy.decide(market)
        # Reverts to BORROWED, then retries the swap
        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"


# =============================================================================
# Decision Logic - Phase 1 (Borrow)
# =============================================================================


class TestDecideBorrowAndSwapSequence:
    """Test the bundled BORROW + SWAP IntentSequence from IDLE state."""

    def test_idle_returns_intent_sequence_with_override(self):
        strategy = _make_strategy(borrow_amount_override="300")
        market = _make_market()
        intent = strategy.decide(market)
        assert intent is not None
        # IntentSequence has intent_type SEQUENCE
        assert hasattr(intent, "intents")  # IntentSequence
        assert strategy._state == BORROWING

    def test_sequence_contains_borrow_and_swap(self):
        strategy = _make_strategy(borrow_amount_override="300")
        market = _make_market()
        intent = strategy.decide(market)
        # IntentSequence has .intents list
        assert len(intent.intents) == 2
        borrow = intent.intents[0]
        swap = intent.intents[1]
        assert borrow.intent_type.value == "BORROW"
        assert borrow.protocol == "aave_v3"
        assert borrow.collateral_token == "WETH"
        assert borrow.collateral_amount == Decimal("0.5")
        assert borrow.borrow_token == "USDC"
        assert borrow.borrow_amount == Decimal("300")
        assert swap.intent_type.value == "SWAP"
        assert swap.from_token == "USDC"
        assert swap.to_token == "WETH"
        assert swap.amount == Decimal("300")
        assert swap.protocol == "enso"

    def test_computed_borrow_amount_from_market(self):
        strategy = _make_strategy(borrow_amount_override="")
        market = _make_market(weth_price=Decimal("2400"), usdc_price=Decimal("1"))
        intent = strategy.decide(market)
        assert hasattr(intent, "intents")  # IntentSequence
        borrow = intent.intents[0]
        # 0.5 WETH * 2400 = 1200 USD * 0.5 LTV = 600 / 1 = 600 USDC
        assert borrow.borrow_amount == Decimal("600.00")

    def test_price_unavailable_returns_hold(self):
        from almanak.framework.data.market_snapshot import PriceUnavailableError

        strategy = _make_strategy(borrow_amount_override="")
        market = MagicMock()
        market.price.side_effect = PriceUnavailableError("WETH", "No oracle")
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_borrow_chain_is_polygon(self):
        strategy = _make_strategy()
        market = _make_market()
        intent = strategy.decide(market)
        borrow = intent.intents[0]
        assert borrow.chain == "polygon"

    def test_swap_slippage_from_config(self):
        strategy = _make_strategy(borrow_amount_override="300", max_slippage_pct="5.0")
        market = _make_market()
        intent = strategy.decide(market)
        swap = intent.intents[1]
        assert swap.max_slippage == Decimal("0.05")


# =============================================================================
# Decision Logic - Phase 3 (Hold)
# =============================================================================


class TestDecidePhase3Hold:
    """Test the HOLD phase."""

    def test_swapped_state_returns_hold(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("300")
        strategy._swap_amount_out = Decimal("0.125")
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "carry position active" in intent.reason.lower()


# =============================================================================
# Intent Execution Callbacks
# =============================================================================


class TestOnIntentExecuted:
    """Test the on_intent_executed callback for all intent types."""

    def test_borrow_success_transitions_to_borrowed(self):
        strategy = _make_strategy()
        strategy._state = BORROWING
        strategy._previous_stable_state = IDLE
        intent = _mock_intent("BORROW", borrow_amount=Decimal("300"))
        result = _mock_result(success=True)
        strategy.on_intent_executed(intent, True, result)
        assert strategy._state == BORROWED
        assert strategy._supplied_amount == Decimal("0.5")
        assert strategy._borrowed_amount == Decimal("300")

    def test_swap_success_opening_transitions_to_swapped(self):
        strategy = _make_strategy()
        strategy._state = BORROWED
        strategy._previous_stable_state = BORROWED
        sa = MagicMock()
        sa.amount_out_decimal = Decimal("0.125")
        sa.amount_in_decimal = Decimal("300")
        sa.effective_price = Decimal("2400")
        intent = _mock_intent("SWAP")
        result = _mock_result(success=True, swap_amounts=sa)
        strategy.on_intent_executed(intent, True, result)
        assert strategy._state == SWAPPED
        assert strategy._swap_amount_out == Decimal("0.125")

    def test_swap_success_no_amounts(self):
        strategy = _make_strategy()
        strategy._state = BORROWED
        strategy._previous_stable_state = BORROWED
        intent = _mock_intent("SWAP")
        result = _mock_result(success=True, swap_amounts=None)
        strategy.on_intent_executed(intent, True, result)
        assert strategy._state == SWAPPED
        assert strategy._swap_amount_out == Decimal("0")

    def test_repay_clears_debt(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._borrowed_amount = Decimal("300")
        intent = _mock_intent("REPAY")
        result = _mock_result(success=True)
        strategy.on_intent_executed(intent, True, result)
        assert strategy._borrowed_amount == Decimal("0")

    def test_withdraw_clears_collateral(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        # Simulate full teardown: SWAP and REPAY already cleared these
        strategy._borrowed_amount = Decimal("0")
        strategy._swap_amount_out = Decimal("0")
        intent = _mock_intent("WITHDRAW")
        result = _mock_result(success=True)
        strategy.on_intent_executed(intent, True, result)
        assert strategy._supplied_amount == Decimal("0")
        # After full teardown, state resets to IDLE
        assert strategy._state == IDLE

    def test_borrow_failure_reverts_state(self):
        strategy = _make_strategy()
        strategy._state = BORROWING
        strategy._previous_stable_state = IDLE
        intent = _mock_intent("BORROW")
        result = _mock_result(success=False)
        strategy.on_intent_executed(intent, False, result)
        assert strategy._state == IDLE

    def test_swap_failure_reverts_state(self):
        strategy = _make_strategy()
        strategy._state = SWAPPING
        strategy._previous_stable_state = BORROWED
        intent = _mock_intent("SWAP")
        result = _mock_result(success=False)
        strategy.on_intent_executed(intent, False, result)
        assert strategy._state == BORROWED

    def test_intent_without_type_is_noop(self):
        strategy = _make_strategy()
        strategy._state = BORROWED
        intent = MagicMock(spec=[])  # no intent_type attribute
        result = _mock_result(success=True)
        strategy.on_intent_executed(intent, True, result)
        assert strategy._state == BORROWED


# =============================================================================
# State Persistence
# =============================================================================


class TestStatePersistence:
    """Test get_persistent_state and load_persistent_state round-trip."""

    def test_round_trip_idle(self):
        strategy = _make_strategy()
        state = strategy.get_persistent_state()
        assert state["state"] == IDLE
        assert state["supplied_amount"] == "0"
        assert state["borrowed_amount"] == "0"
        assert state["swap_amount_out"] == "0"

    def test_round_trip_swapped(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._previous_stable_state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("300")
        strategy._swap_amount_out = Decimal("0.125")
        state = strategy.get_persistent_state()

        strategy2 = _make_strategy()
        strategy2.load_persistent_state(state)
        assert strategy2._state == SWAPPED
        assert strategy2._supplied_amount == Decimal("0.5")
        assert strategy2._borrowed_amount == Decimal("300")
        assert strategy2._swap_amount_out == Decimal("0.125")

    def test_load_partial_state_defaults(self):
        strategy = _make_strategy()
        strategy.load_persistent_state({"state": BORROWED})
        assert strategy._state == BORROWED
        assert strategy._previous_stable_state == IDLE
        assert strategy._supplied_amount == Decimal("0")


# =============================================================================
# Teardown
# =============================================================================


class TestTeardown:
    """Test teardown intent generation."""

    def test_full_teardown_intents_order(self):
        from almanak.framework.teardown import TeardownMode

        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("300")
        strategy._swap_amount_out = Decimal("0.125")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 3
        # Order: swap back, repay, withdraw
        assert intents[0].intent_type.value == "SWAP"
        assert intents[0].from_token == "WETH"
        assert intents[0].to_token == "USDC"
        assert intents[0].amount == Decimal("0.125")
        assert intents[0].protocol == "enso"

        assert intents[1].intent_type.value == "REPAY"
        assert intents[1].token == "USDC"
        assert intents[1].repay_full is True
        assert intents[1].protocol == "aave_v3"

        assert intents[2].intent_type.value == "WITHDRAW"
        assert intents[2].token == "WETH"
        assert intents[2].withdraw_all is True
        assert intents[2].protocol == "aave_v3"

    def test_hard_teardown_uses_wider_slippage(self):
        from almanak.framework.teardown import TeardownMode

        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._swap_amount_out = Decimal("0.125")
        strategy._borrowed_amount = Decimal("300")
        strategy._supplied_amount = Decimal("0.5")

        intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        assert intents[0].max_slippage == Decimal("0.03")

    def test_soft_teardown_uses_tighter_slippage(self):
        from almanak.framework.teardown import TeardownMode

        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._swap_amount_out = Decimal("0.125")
        strategy._borrowed_amount = Decimal("300")
        strategy._supplied_amount = Decimal("0.5")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert intents[0].max_slippage == Decimal("0.005")

    def test_teardown_no_swap_skips_swap_intent(self):
        from almanak.framework.teardown import TeardownMode

        strategy = _make_strategy()
        strategy._state = BORROWED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("300")
        strategy._swap_amount_out = Decimal("0")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"

    def test_teardown_idle_returns_empty(self):
        from almanak.framework.teardown import TeardownMode

        strategy = _make_strategy()
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0

    def test_teardown_chain_is_polygon(self):
        from almanak.framework.teardown import TeardownMode

        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("300")
        strategy._swap_amount_out = Decimal("0.125")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert intents[0].chain == "polygon"  # SWAP intent
        assert intents[1].chain == "polygon"
        assert intents[2].chain == "polygon"


# =============================================================================
# Open Positions
# =============================================================================


class TestOpenPositions:
    """Test get_open_positions for teardown introspection."""

    def test_idle_returns_empty_positions(self):
        strategy = _make_strategy()
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_swapped_returns_all_positions(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("300")
        strategy._swap_amount_out = Decimal("0.125")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 3
        types = {p.position_type.value for p in summary.positions}
        assert "SUPPLY" in types
        assert "BORROW" in types
        assert "TOKEN" in types

    def test_borrowed_no_swap_returns_two_positions(self):
        strategy = _make_strategy()
        strategy._state = BORROWED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("300")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 2

    def test_position_ids_contain_polygon(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("300")
        strategy._swap_amount_out = Decimal("0.125")
        summary = strategy.get_open_positions()
        for pos in summary.positions:
            assert "polygon" in pos.position_id


# =============================================================================
# Get Status
# =============================================================================


class TestGetStatus:
    """Test the get_status method."""

    def test_status_fields(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("300")
        strategy._swap_amount_out = Decimal("0.125")
        status = strategy.get_status()
        assert status["strategy"] == "aave_enso_carry_polygon"
        assert status["chain"] == "polygon"
        assert status["state"] == SWAPPED
        assert "0.5" in status["supplied"]
        assert "300" in status["borrowed"]
        assert "0.125" in status["swap_amount_out"]


# =============================================================================
# Full Lifecycle Integration
# =============================================================================


class TestFullLifecycle:
    """Test full strategy lifecycle: IDLE -> BORROW -> SWAP -> HOLD."""

    def test_full_lifecycle(self):
        strategy = _make_strategy(borrow_amount_override="300")
        market = _make_market()

        # Step 1: IDLE -> IntentSequence (BORROW + SWAP)
        intent = strategy.decide(market)
        assert hasattr(intent, "intents")  # IntentSequence
        assert strategy._state == BORROWING

        # Simulate BORROW success (first intent in sequence)
        borrow_intent = _mock_intent("BORROW", borrow_amount=Decimal("300"))
        strategy.on_intent_executed(borrow_intent, True, _mock_result())
        assert strategy._state == BORROWED
        assert strategy._supplied_amount == Decimal("0.5")
        assert strategy._borrowed_amount == Decimal("300")

        # Simulate SWAP success (second intent in sequence)
        sa = MagicMock()
        sa.amount_out_decimal = Decimal("0.125")
        sa.amount_in_decimal = Decimal("300")
        sa.effective_price = Decimal("2400")
        swap_intent = _mock_intent("SWAP")
        strategy.on_intent_executed(swap_intent, True, _mock_result(swap_amounts=sa))
        assert strategy._state == SWAPPED
        assert strategy._swap_amount_out == Decimal("0.125")

        # Step 2: SWAPPED -> HOLD
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "carry position active" in intent.reason.lower()

    def test_lifecycle_with_borrow_failure_recovery(self):
        strategy = _make_strategy(borrow_amount_override="300")
        market = _make_market()

        # Step 1: IDLE -> IntentSequence
        intent = strategy.decide(market)
        assert strategy._state == BORROWING

        # Simulate BORROW failure
        borrow_intent = _mock_intent("BORROW")
        strategy.on_intent_executed(borrow_intent, False, _mock_result(success=False))
        assert strategy._state == IDLE

        # Step 2: Retry -> should issue sequence again
        intent = strategy.decide(market)
        assert hasattr(intent, "intents")  # IntentSequence

    def test_lifecycle_with_swap_failure_recovery(self):
        strategy = _make_strategy(borrow_amount_override="300")
        market = _make_market()

        # Step 1: Issue sequence
        intent = strategy.decide(market)
        assert strategy._state == BORROWING

        # BORROW succeeds
        borrow_intent = _mock_intent("BORROW", borrow_amount=Decimal("300"))
        strategy.on_intent_executed(borrow_intent, True, _mock_result())
        assert strategy._state == BORROWED

        # SWAP fails in sequence
        swap_intent = _mock_intent("SWAP")
        strategy.on_intent_executed(swap_intent, False, _mock_result(success=False))
        assert strategy._state == BORROWED

        # Step 2: Retry from BORROWED -> should issue SWAP only (not full sequence)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_decide_catches_exceptions(self):
        strategy = _make_strategy(borrow_amount_override="")
        market = MagicMock()
        market.price.side_effect = RuntimeError("Network error")
        intent = strategy.decide(market)
        # Should catch and return HOLD
        assert intent.intent_type.value == "HOLD"

    def test_unknown_state_returns_hold(self):
        strategy = _make_strategy()
        strategy._state = "totally_unknown"
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unknown state" in intent.reason.lower()

    def test_log_result_details_with_none(self):
        strategy = _make_strategy()
        # Should not raise
        strategy._log_result_details("BORROW", None)

    def test_log_result_details_with_tx_results(self):
        strategy = _make_strategy()
        tx1 = MagicMock()
        tx1.tx_hash = "0xabc"
        tx1.gas_used = 100000
        result = _mock_result(tx_results=[tx1])
        # Should not raise
        strategy._log_result_details("BORROW", result)
