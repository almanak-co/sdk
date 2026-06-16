"""Unit tests for the PancakeSwap V3 + Aave V3 Carry Trade on BSC demo strategy.

Tests the T2 composition lifecycle:
supply -> borrow -> swap -> swap_back -> repay -> withdraw.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.demo_strategies.pancakeswap_aave_carry_bsc.strategy import (
    BORROWED,
    BORROWING,
    COMPLETE,
    IDLE,
    REPAID,
    REPAYING,
    SUPPLIED,
    SUPPLYING,
    SWAP_BACK,
    SWAPPED,
    SWAPPING,
    SWAPPING_BACK,
    WITHDRAWING,
    PancakeswapAaveCarryBscStrategy,
)


# =============================================================================
# Fixtures
# =============================================================================


def _make_strategy(**config_overrides) -> PancakeswapAaveCarryBscStrategy:
    """Create a strategy instance with mocked framework dependencies."""
    default_config = {
        "collateral_token": "WBNB",
        "collateral_amount": "0.5",
        "borrow_token": "USDC",
        "swap_to_token": "USDT",
        "ltv_target": "0.3",
    }
    default_config.update(config_overrides)

    with patch.object(PancakeswapAaveCarryBscStrategy, "__init__", lambda self, *a, **kw: None):
        strategy = PancakeswapAaveCarryBscStrategy.__new__(PancakeswapAaveCarryBscStrategy)

    strategy._deployment_id = "test-pancakeswap-aave-bsc"
    strategy._chain = "bsc"
    strategy._wallet_address = "0x1234567890123456789012345678901234567890"
    strategy._config = default_config
    strategy._hot_config = None

    strategy.collateral_token = str(default_config["collateral_token"])
    strategy.collateral_amount = Decimal(str(default_config["collateral_amount"]))
    strategy.borrow_token = str(default_config["borrow_token"])
    strategy.swap_to_token = str(default_config["swap_to_token"])
    strategy.ltv_target = Decimal(str(default_config["ltv_target"]))

    strategy._state = IDLE
    strategy._previous_stable = IDLE
    strategy._supplied_amount = Decimal("0")
    strategy._borrowed_amount = Decimal("0")
    strategy._swapped_amount = Decimal("0")

    return strategy


def _make_market(wbnb_price=Decimal("600"), usdc_price=Decimal("1"), usdt_price=Decimal("1")):
    """Create a mock MarketSnapshot with BSC token prices."""
    market = MagicMock()

    def price_side_effect(token):
        prices = {"WBNB": wbnb_price, "BNB": wbnb_price, "USDC": usdc_price, "USDT": usdt_price}
        if token in prices:
            return prices[token]
        raise ValueError(f"Unknown token: {token}")

    market.price.side_effect = price_side_effect
    return market


# =============================================================================
# Metadata
# =============================================================================


class TestStrategyMetadata:
    def test_strategy_name(self):
        assert PancakeswapAaveCarryBscStrategy.STRATEGY_NAME == "pancakeswap_aave_carry_bsc"

    def test_supported_chains(self):
        assert "bsc" in PancakeswapAaveCarryBscStrategy.STRATEGY_METADATA.supported_chains

    def test_supported_protocols(self):
        protocols = PancakeswapAaveCarryBscStrategy.STRATEGY_METADATA.supported_protocols
        assert "aave_v3" in protocols
        assert "pancakeswap_v3" in protocols

    def test_intent_types(self):
        types = PancakeswapAaveCarryBscStrategy.STRATEGY_METADATA.intent_types
        assert "BORROW" in types
        assert "SWAP" in types
        assert "REPAY" in types
        assert "WITHDRAW" in types
        assert "HOLD" in types

    def test_supports_teardown(self):
        strategy = _make_strategy()
        assert strategy.supports_teardown() is True


# =============================================================================
# Lifecycle: Entry Phase
# =============================================================================


def _advance_to_supplied(strategy) -> None:
    """Drive the strategy from IDLE through a successful SUPPLY to SUPPLIED.

    The first decide() from IDLE now emits the standalone SUPPLY intent
    (VIB-3586); the BORROW is only emitted afterwards from the SUPPLIED state.
    """
    supply_intent = strategy.decide(_make_market())
    assert supply_intent.intent_type.value == "SUPPLY"
    strategy.on_intent_executed(supply_intent, success=True, result=None)
    assert strategy._state == SUPPLIED


class TestEntryPhase:
    def test_idle_emits_supply(self):
        strategy = _make_strategy()
        market = _make_market()

        intent = strategy.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == SUPPLYING

    def test_supply_uses_aave_v3_as_collateral(self):
        strategy = _make_strategy()
        market = _make_market()

        intent = strategy.decide(market)

        assert intent.protocol == "aave_v3"
        assert intent.token == "WBNB"
        assert intent.amount == Decimal("0.5")
        assert intent.use_as_collateral is True

    def test_supplied_emits_borrow(self):
        strategy = _make_strategy()
        _advance_to_supplied(strategy)

        intent = strategy.decide(_make_market())

        assert intent.intent_type.value == "BORROW"
        assert strategy._state == BORROWING

    def test_borrow_uses_aave_v3(self):
        strategy = _make_strategy()
        _advance_to_supplied(strategy)

        intent = strategy.decide(_make_market())

        assert intent.protocol == "aave_v3"

    def test_borrow_does_not_bundle_collateral(self):
        """VIB-3586: collateral is supplied by the SUPPLY phase, so the BORROW
        intent must carry collateral_amount == 0 (the fail-closed guard rejects
        a bundled Intent.borrow(collateral_amount > 0))."""
        strategy = _make_strategy()
        _advance_to_supplied(strategy)

        intent = strategy.decide(_make_market())

        assert intent.collateral_amount == Decimal("0")

    def test_borrow_amount_calculation(self):
        """0.5 WBNB * $600 = $300 collateral, 30% LTV = $90 USDC at $1.

        Borrow amount is computed on the post-supply BORROW intent.
        """
        strategy = _make_strategy()
        _advance_to_supplied(strategy)

        intent = strategy.decide(_make_market(wbnb_price=Decimal("600")))

        assert intent.borrow_amount == Decimal("90.00")

    def test_borrow_with_zero_collateral_price_holds(self):
        strategy = _make_strategy()
        _advance_to_supplied(strategy)
        market = _make_market()
        market.price.side_effect = ValueError("No price")

        intent = strategy.decide(market)

        assert intent.intent_type.value == "HOLD"

    def test_borrowed_emits_swap(self):
        strategy = _make_strategy()
        strategy._state = BORROWED
        strategy._borrowed_amount = Decimal("90")

        intent = strategy.decide(_make_market())

        assert intent.intent_type.value == "SWAP"
        assert strategy._state == SWAPPING

    def test_swap_uses_pancakeswap_v3(self):
        strategy = _make_strategy()
        strategy._state = BORROWED
        strategy._borrowed_amount = Decimal("90")

        intent = strategy.decide(_make_market())

        assert intent.protocol == "pancakeswap_v3"

    def test_swap_from_usdc_to_usdt(self):
        strategy = _make_strategy()
        strategy._state = BORROWED
        strategy._borrowed_amount = Decimal("90")

        intent = strategy.decide(_make_market())

        assert intent.from_token == "USDC"
        assert intent.to_token == "USDT"


# =============================================================================
# Lifecycle: Teardown Phase
# =============================================================================


class TestTeardownPhase:
    def test_swapped_emits_swap_back(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._swapped_amount = Decimal("89.50")

        intent = strategy.decide(_make_market())

        assert intent.intent_type.value == "SWAP"
        assert strategy._state == SWAPPING_BACK

    def test_swap_back_from_usdt_to_usdc(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._swapped_amount = Decimal("89.50")

        intent = strategy.decide(_make_market())

        assert intent.from_token == "USDT"
        assert intent.to_token == "USDC"

    def test_swap_back_emits_repay(self):
        strategy = _make_strategy()
        strategy._state = SWAP_BACK
        strategy._borrowed_amount = Decimal("90")

        intent = strategy.decide(_make_market())

        assert intent.intent_type.value == "REPAY"
        assert strategy._state == REPAYING

    def test_repay_uses_repay_full(self):
        strategy = _make_strategy()
        strategy._state = SWAP_BACK
        strategy._borrowed_amount = Decimal("90")

        intent = strategy.decide(_make_market())

        assert intent.repay_full is True

    def test_repaid_emits_withdraw(self):
        strategy = _make_strategy()
        strategy._state = REPAID
        strategy._supplied_amount = Decimal("0.5")

        intent = strategy.decide(_make_market())

        assert intent.intent_type.value == "WITHDRAW"
        assert strategy._state == WITHDRAWING

    def test_withdraw_uses_withdraw_all(self):
        strategy = _make_strategy()
        strategy._state = REPAID
        strategy._supplied_amount = Decimal("0.5")

        intent = strategy.decide(_make_market())

        assert intent.withdraw_all is True

    def test_complete_holds(self):
        strategy = _make_strategy()
        strategy._state = COMPLETE

        intent = strategy.decide(_make_market())

        assert intent.intent_type.value == "HOLD"
        assert "complete" in intent.reason.lower()


# =============================================================================
# Transitional State Recovery
# =============================================================================


class TestTransitionalRecovery:
    def test_stuck_supplying_reverts_to_idle(self):
        strategy = _make_strategy()
        strategy._state = SUPPLYING
        strategy._previous_stable = IDLE

        intent = strategy.decide(_make_market())

        # After revert to idle, should try supply again
        assert intent.intent_type.value == "SUPPLY"

    def test_stuck_borrowing_reverts_to_supplied(self):
        strategy = _make_strategy()
        strategy._state = BORROWING
        strategy._previous_stable = SUPPLIED

        intent = strategy.decide(_make_market())

        # After revert to supplied, should try borrow again
        assert intent.intent_type.value == "BORROW"
        assert intent.collateral_amount == Decimal("0")

    def test_stuck_swapping_reverts_to_borrowed(self):
        strategy = _make_strategy()
        strategy._state = SWAPPING
        strategy._previous_stable = BORROWED
        strategy._borrowed_amount = Decimal("90")

        intent = strategy.decide(_make_market())

        assert intent.intent_type.value == "SWAP"

    def test_stuck_repaying_reverts_to_swap_back(self):
        strategy = _make_strategy()
        strategy._state = REPAYING
        strategy._previous_stable = SWAP_BACK
        strategy._borrowed_amount = Decimal("90")

        intent = strategy.decide(_make_market())

        assert intent.intent_type.value == "REPAY"


# =============================================================================
# on_intent_executed
# =============================================================================


class TestOnIntentExecuted:
    def test_supply_success(self):
        strategy = _make_strategy()
        strategy._state = SUPPLYING

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"
        mock_intent.amount = Decimal("0.5")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == SUPPLIED
        assert strategy._supplied_amount == Decimal("0.5")

    def test_borrow_success(self):
        strategy = _make_strategy()
        strategy._state = BORROWING
        # Collateral was already booked by the preceding SUPPLY phase.
        strategy._supplied_amount = Decimal("0.5")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "BORROW"
        mock_intent.borrow_amount = Decimal("90")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == BORROWED
        assert strategy._supplied_amount == Decimal("0.5")
        assert strategy._borrowed_amount == Decimal("90")

    def test_swap_success(self):
        strategy = _make_strategy()
        strategy._state = SWAPPING
        strategy._borrowed_amount = Decimal("90")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == SWAPPED
        assert strategy._swapped_amount == Decimal("90")

    def test_swap_success_with_result_amounts(self):
        strategy = _make_strategy()
        strategy._state = SWAPPING
        strategy._borrowed_amount = Decimal("90")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        mock_result = MagicMock()
        mock_result.swap_amounts.amount_out_decimal = Decimal("89.50")

        strategy.on_intent_executed(mock_intent, success=True, result=mock_result)

        assert strategy._swapped_amount == Decimal("89.50")

    def test_swap_back_success(self):
        strategy = _make_strategy()
        strategy._state = SWAPPING_BACK
        strategy._swapped_amount = Decimal("89.50")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == SWAP_BACK
        assert strategy._swapped_amount == Decimal("0")

    def test_repay_success(self):
        strategy = _make_strategy()
        strategy._state = REPAYING
        strategy._borrowed_amount = Decimal("90")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "REPAY"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == REPAID
        assert strategy._borrowed_amount == Decimal("0")

    def test_withdraw_success(self):
        strategy = _make_strategy()
        strategy._state = WITHDRAWING
        strategy._supplied_amount = Decimal("0.5")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "WITHDRAW"

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._state == COMPLETE
        assert strategy._supplied_amount == Decimal("0")

    def test_failure_reverts_to_previous_stable(self):
        strategy = _make_strategy()
        strategy._state = SWAPPING
        strategy._previous_stable = BORROWED

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"

        strategy.on_intent_executed(mock_intent, success=False, result=None)

        assert strategy._state == BORROWED


# =============================================================================
# Teardown Interface
# =============================================================================


class TestTeardownInterface:
    def test_no_positions_empty(self):
        strategy = _make_strategy()
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 0

    def test_supplied_position_reported(self):
        strategy = _make_strategy()
        strategy._supplied_amount = Decimal("0.5")

        positions = strategy.get_open_positions()

        assert len(positions.positions) == 1
        assert positions.positions[0].protocol == "aave_v3"

    def test_all_positions_reported_in_swapped_state(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("90")
        strategy._swapped_amount = Decimal("89.50")

        positions = strategy.get_open_positions()

        assert len(positions.positions) == 3  # supply + borrow + swap

    def test_teardown_generates_correct_order(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("90")
        strategy._swapped_amount = Decimal("89.50")

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 3
        assert intents[0].intent_type.value == "SWAP"  # swap back
        assert intents[1].intent_type.value == "REPAY"
        assert intents[2].intent_type.value == "WITHDRAW"

    def test_teardown_hard_mode_higher_slippage(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("90")
        strategy._swapped_amount = Decimal("89.50")

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.HARD)

        assert intents[0].max_slippage == Decimal("0.03")

    def test_teardown_soft_mode_normal_slippage(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("90")
        strategy._swapped_amount = Decimal("89.50")

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert intents[0].max_slippage == Decimal("0.005")

    def test_teardown_no_positions_empty(self):
        strategy = _make_strategy()

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 0


# =============================================================================
# State Persistence
# =============================================================================


class TestStatePersistence:
    def test_get_persistent_state(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("90")
        strategy._swapped_amount = Decimal("89.50")

        state = strategy.get_persistent_state()

        assert state["state"] == SWAPPED
        assert state["supplied_amount"] == "0.5"
        assert state["borrowed_amount"] == "90"
        assert state["swapped_amount"] == "89.50"

    def test_load_persistent_state(self):
        strategy = _make_strategy()

        strategy.load_persistent_state({
            "state": BORROWED,
            "previous_stable": IDLE,
            "supplied_amount": "0.5",
            "borrowed_amount": "90",
            "swapped_amount": "0",
        })

        assert strategy._state == BORROWED
        assert strategy._supplied_amount == Decimal("0.5")
        assert strategy._borrowed_amount == Decimal("90")

    def test_roundtrip_persistence(self):
        strategy = _make_strategy()
        strategy._state = REPAID
        strategy._previous_stable = SWAP_BACK
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("0")
        strategy._swapped_amount = Decimal("0")

        saved = strategy.get_persistent_state()

        strategy2 = _make_strategy()
        strategy2.load_persistent_state(saved)

        assert strategy2._state == strategy._state
        assert strategy2._previous_stable == strategy._previous_stable
        assert strategy2._supplied_amount == strategy._supplied_amount
