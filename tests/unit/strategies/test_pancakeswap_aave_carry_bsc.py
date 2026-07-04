"""Unit tests for the PancakeSwap V3 + Aave V3 Carry Trade on BSC demo strategy.

Tests the T2 composition: decide() establishes the carry (supply -> borrow -> swap)
then HOLDs; the unwind is teardown-owned and routes through the HF-safe
``generate_lending_unwind`` primitive (VIB-5637 / VIB-5448).
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

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
    PancakeswapAaveCarryBscStrategy,
)
from almanak.framework.market import HealthUnavailableError

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
        "max_borrow_fraction": "0.5",
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
    strategy.max_borrow_fraction = Decimal(str(default_config["max_borrow_fraction"]))

    strategy._state = IDLE
    strategy._previous_stable = IDLE
    strategy._supplied_amount = Decimal("0")
    strategy._borrowed_amount = Decimal("0")
    strategy._swapped_amount = Decimal("0")

    return strategy


def _make_market(
    wbnb_price=Decimal("600"),
    usdc_price=Decimal("1"),
    usdt_price=Decimal("1"),
    max_borrow_usd=Decimal("100000"),
    health_factor=Decimal("2.5"),
):
    """Create a mock MarketSnapshot with BSC token prices.

    ``max_borrow_usd`` defaults high enough that the live borrow-capacity guard
    in ``_do_borrow`` does not clamp the config-sized borrow (preserving the
    base-case borrow-amount assertions). Override it to exercise the clamp path.
    """
    market = MagicMock()

    def price_side_effect(token):
        prices = {"WBNB": wbnb_price, "BNB": wbnb_price, "USDC": usdc_price, "USDT": usdt_price}
        if token in prices:
            return prices[token]
        raise ValueError(f"Unknown token: {token}")

    market.price.side_effect = price_side_effect

    health = MagicMock()
    health.max_borrow_usd = max_borrow_usd
    health.health_factor = health_factor
    market.position_health.return_value = health
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

    def test_borrow_clamped_by_live_capacity(self):
        """The live borrow-capacity guard clamps the config-sized borrow when it
        exceeds ``max_borrow_fraction`` of Aave's available capacity.

        Config borrow is $90 (0.5 WBNB * $600 * 30% LTV). With live
        max_borrow_usd=$100 and the default 0.5 fraction, the safe ceiling is
        $50, so the borrow is clamped to 50.00 USDC.
        """
        strategy = _make_strategy()
        _advance_to_supplied(strategy)

        intent = strategy.decide(_make_market(max_borrow_usd=Decimal("100")))

        assert intent.intent_type.value == "BORROW"
        assert intent.borrow_amount == Decimal("50.00")

    def test_borrow_holds_when_health_unavailable(self):
        """The borrow-capacity guard FAILS CLOSED: when live health data is
        unavailable, it HOLDs (retries next iteration) rather than borrowing
        without the safety signal."""
        strategy = _make_strategy()
        _advance_to_supplied(strategy)
        market = _make_market()
        market.position_health.side_effect = HealthUnavailableError("no health data")

        intent = strategy.decide(market)

        assert intent.intent_type.value == "HOLD"

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
# Lifecycle: Carry established -> HOLD (unwind is teardown-owned, VIB-5637)
# =============================================================================


class TestCarryHold:
    """Once the carry is established, decide() HOLDs — it never hand-rolls the
    unwind in the iteration lane (which bypassed the HF-safe primitive and stranded
    the collateral on dust debt; VIB-5637 / VIB-5448)."""

    def test_swapped_holds(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._swapped_amount = Decimal("89.50")

        intent = strategy.decide(_make_market())

        assert intent.intent_type.value == "HOLD"
        assert "carry established" in intent.reason.lower()

    def test_swapped_never_emits_repay_or_withdraw(self):
        """The dust-debt strand came from decide() emitting REPAY/WITHDRAW; it must
        only ever HOLD once the carry is on."""
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._swapped_amount = Decimal("89.50")
        strategy._borrowed_amount = Decimal("90")
        strategy._supplied_amount = Decimal("0.5")

        intent = strategy.decide(_make_market())

        assert intent.intent_type.value == "HOLD"

    def test_legacy_teardown_state_holds(self):
        """A state persisted by a pre-VIB-5637 build that unwound in decide() (e.g.
        ``repaid``) degrades to HOLD — the unwind is now teardown-owned."""
        strategy = _make_strategy()
        strategy._state = REPAID
        strategy._supplied_amount = Decimal("0.5")

        intent = strategy.decide(_make_market())

        assert intent.intent_type.value == "HOLD"
        assert "teardown-owned" in intent.reason.lower()

    def test_legacy_complete_state_holds(self):
        strategy = _make_strategy()
        strategy._state = COMPLETE

        intent = strategy.decide(_make_market())

        assert intent.intent_type.value == "HOLD"


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

    def test_stuck_legacy_repaying_reverts_then_holds(self):
        """``repaying`` is a legacy (pre-VIB-5637) transitional state. Restored from
        persisted state it reverts to its stable predecessor and then HOLDs — the
        unwind is teardown-owned, decide() no longer re-emits a REPAY."""
        strategy = _make_strategy()
        strategy._state = REPAYING
        strategy._previous_stable = SWAP_BACK
        strategy._borrowed_amount = Decimal("90")

        intent = strategy.decide(_make_market())

        assert intent.intent_type.value == "HOLD"
        assert strategy._state == SWAP_BACK


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

    def test_teardown_lane_intents_do_not_transition_entry_state(self):
        """The unwind is teardown-owned; a REPAY/WITHDRAW/swap-back surfaced to this
        hook must NOT advance the entry state machine (only entry SUPPLY/BORROW/SWAP
        do). Guards against a teardown-lane intent spuriously mutating cached state."""
        for intent_type_val, state in (("SWAP", SWAPPED), ("REPAY", SWAPPED), ("WITHDRAW", SWAPPED)):
            strategy = _make_strategy()
            strategy._state = state
            strategy._borrowed_amount = Decimal("90")
            strategy._supplied_amount = Decimal("0.5")
            strategy._swapped_amount = Decimal("89.50")

            mock_intent = MagicMock()
            mock_intent.intent_type.value = intent_type_val

            strategy.on_intent_executed(mock_intent, success=True, result=None)

            # No entry transition fired; cached amounts untouched.
            assert strategy._state == state
            assert strategy._borrowed_amount == Decimal("90")
            assert strategy._supplied_amount == Decimal("0.5")

    def test_entry_failure_reverts_to_previous_stable(self):
        strategy = _make_strategy()
        strategy._state = SWAPPING
        strategy._previous_stable = BORROWED

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"

        strategy.on_intent_executed(mock_intent, success=False, result=None)

        assert strategy._state == BORROWED

    def test_teardown_lane_failure_does_not_revert_entry_state(self):
        """A failure of a non-entry (teardown-lane) intent must not revert the entry
        state machine — the failure revert is scoped to entry transitional states."""
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._previous_stable = BORROWED

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "WITHDRAW"

        strategy.on_intent_executed(mock_intent, success=False, result=None)

        assert strategy._state == SWAPPED


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

    def test_teardown_closes_swap_leg_then_delegates_to_hf_safe_primitive(self):
        """VIB-5637: the unwind is (1) close the swap leg, then (2) delegate the
        lending unwind to the HF-safe ``generate_lending_unwind`` primitive with the
        correct legs — never a hand-rolled repay_full/withdraw_all."""
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("90")
        strategy._swapped_amount = Decimal("89.50")

        from almanak.framework.teardown import TeardownMode

        sentinel = [MagicMock(name="hf_safe_unwind_intent")]
        market = _make_market()
        with patch(
            "almanak.framework.teardown.generate_lending_unwind", return_value=sentinel
        ) as mock_unwind:
            intents = strategy.generate_teardown_intents(TeardownMode.SOFT, market=market)

        # (1) leading intent closes the swap leg USDT -> USDC
        assert intents[0].intent_type.value == "SWAP"
        assert intents[0].from_token == "USDT"
        assert intents[0].to_token == "USDC"
        # (2) the rest is exactly the primitive's HF-safe staircase
        assert intents[1:] == sentinel

        mock_unwind.assert_called_once()
        kwargs = mock_unwind.call_args.kwargs
        assert kwargs["protocol"] == "aave_v3"
        assert kwargs["collateral_token"] == "WBNB"
        assert kwargs["borrow_token"] == "USDC"
        assert kwargs["chain"] == "bsc"
        assert kwargs["mode"] == TeardownMode.SOFT
        assert kwargs["market"] is market
        # The primitive's own collateral->debt swaps are pinned to this BSC venue,
        # not the compiler's uniswap_v3 default (Codex review / robustness).
        assert kwargs["swap_protocol"] == "pancakeswap_v3"

    def test_teardown_never_hand_rolls_repay_or_withdraw(self):
        """Regression guard for the VIB-5637 dust-debt strand: the demo must not emit
        its own REPAY/WITHDRAW — those flow through the HF-safe primitive."""
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("90")
        strategy._swapped_amount = Decimal("89.50")

        from almanak.framework.teardown import TeardownMode

        with patch("almanak.framework.teardown.generate_lending_unwind", return_value=[]):
            intents = strategy.generate_teardown_intents(TeardownMode.SOFT, market=_make_market())

        assert all(i.intent_type.value not in ("REPAY", "WITHDRAW") for i in intents)

    def test_teardown_hard_mode_higher_slippage(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("90")
        strategy._swapped_amount = Decimal("89.50")

        from almanak.framework.teardown import TeardownMode

        with patch("almanak.framework.teardown.generate_lending_unwind", return_value=[]):
            intents = strategy.generate_teardown_intents(TeardownMode.HARD, market=_make_market())

        assert intents[0].max_slippage == Decimal("0.03")

    def test_teardown_soft_mode_normal_slippage(self):
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("90")
        strategy._swapped_amount = Decimal("89.50")

        from almanak.framework.teardown import TeardownMode

        with patch("almanak.framework.teardown.generate_lending_unwind", return_value=[]):
            intents = strategy.generate_teardown_intents(TeardownMode.SOFT, market=_make_market())

        assert intents[0].max_slippage == Decimal("0.005")

    def test_teardown_no_positions_empty(self):
        """No swap leg, and the primitive (delegated to UNCONDITIONALLY — reference
        pattern, robust to a cached/on-chain desync) returns [] when the live position
        is flat, so the whole teardown is empty."""
        strategy = _make_strategy()

        from almanak.framework.teardown import TeardownMode

        with patch(
            "almanak.framework.teardown.generate_lending_unwind", return_value=[]
        ) as mock_unwind:
            intents = strategy.generate_teardown_intents(TeardownMode.SOFT, market=_make_market())

        assert intents == []
        # Delegated unconditionally so a crash-desync (on-chain position, cached
        # amounts == 0) still unwinds live state rather than stranding.
        mock_unwind.assert_called_once()

    def test_teardown_low_hf_falls_back_to_risk_reducing_repay(self):
        """When the primitive cannot size a collateral-sourced unwind from the
        pre-swap snapshot (HF too low -> LendingUnwindError), teardown degrades to a
        risk-reducing repay_full (never a reverting withdraw_all), funded by the
        close-leg swap. Teardown's first job is removing on-chain risk (Codex P1)."""
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("90")
        strategy._swapped_amount = Decimal("89.50")

        from almanak.framework.teardown import LendingUnwindError, TeardownMode

        with patch(
            "almanak.framework.teardown.generate_lending_unwind",
            side_effect=LendingUnwindError("health factor too low"),
        ):
            intents = strategy.generate_teardown_intents(TeardownMode.SOFT, market=_make_market())

        # Close-leg SWAP still emitted; lending unwind degrades to repay_full only.
        assert intents[0].intent_type.value == "SWAP"
        repays = [i for i in intents if i.intent_type.value == "REPAY"]
        assert len(repays) == 1
        assert repays[0].repay_full is True
        assert repays[0].token == "USDC"
        # NO withdraw_all is emitted while HF is too low (that is the revert we avoid).
        assert all(i.intent_type.value != "WITHDRAW" for i in intents)

    def test_teardown_lending_only_no_swap_leg_delegates_to_primitive(self):
        """A borrow with no held swap_to_token (e.g. already swapped back) emits no
        swap leg and delegates straight to the HF-safe primitive."""
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("90")
        strategy._swapped_amount = Decimal("0")  # nothing held to swap back

        from almanak.framework.teardown import TeardownMode

        sentinel = [MagicMock(name="hf_safe_unwind_intent")]
        with patch(
            "almanak.framework.teardown.generate_lending_unwind", return_value=sentinel
        ) as mock_unwind:
            intents = strategy.generate_teardown_intents(TeardownMode.SOFT, market=_make_market())

        assert intents == sentinel
        mock_unwind.assert_called_once()

    def test_on_teardown_completed_success_clears_tracked_amounts(self):
        """After a successful teardown, the cached amounts are cleared so
        get_open_positions() no longer reports stale positions (Gemini review)."""
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("90")
        strategy._swapped_amount = Decimal("89.50")

        strategy.on_teardown_completed(success=True, recovered_usd=Decimal("100"))

        assert strategy._supplied_amount == Decimal("0")
        assert strategy._borrowed_amount == Decimal("0")
        assert strategy._swapped_amount == Decimal("0")
        assert strategy._state == COMPLETE
        # No stale positions reported after a clean teardown.
        assert len(strategy.get_open_positions().positions) == 0

    def test_on_teardown_completed_failure_keeps_amounts_for_retry(self):
        """A failed teardown leaves tracked amounts intact so a retry still sees the
        open position (does not silently zero it out)."""
        strategy = _make_strategy()
        strategy._state = SWAPPED
        strategy._supplied_amount = Decimal("0.5")
        strategy._borrowed_amount = Decimal("90")
        strategy._swapped_amount = Decimal("89.50")

        strategy.on_teardown_completed(success=False, recovered_usd=Decimal("0"))

        assert strategy._supplied_amount == Decimal("0.5")
        assert strategy._borrowed_amount == Decimal("90")
        assert strategy._swapped_amount == Decimal("89.50")


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
