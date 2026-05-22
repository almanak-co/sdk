from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.demo_strategies.aave_loop_mantle.strategy import AaveLoopMantleStrategy
from almanak.demo_strategies.aave_paper_trade_leverage_polygon.strategy import (
    AavePaperTradeLeveragePolygonStrategy,
)
from almanak.demo_strategies.morpho_looping.strategy import MorphoLoopingStrategy
from almanak.framework.intents import RepayIntent, SwapIntent, WithdrawIntent
from almanak.framework.teardown import TeardownMode


def _market(prices: dict[str, Decimal]) -> MagicMock:
    market = MagicMock()
    market.price.side_effect = lambda token: prices[token]
    return market


def test_aave_loop_mantle_teardown_withdraws_before_swap_and_repay() -> None:
    strategy = AaveLoopMantleStrategy.__new__(AaveLoopMantleStrategy)
    strategy._chain = "mantle"
    strategy.supply_token = "WETH"
    strategy.borrow_token = "USDC"
    strategy._total_supplied = Decimal("1")
    strategy._total_borrowed = Decimal("1000")
    strategy._supply_price_usd = Decimal("2000")
    strategy._borrow_price_usd = Decimal("1")

    intents = strategy.generate_teardown_intents(
        TeardownMode.SOFT,
        market=_market({"USDC": Decimal("1"), "WETH": Decimal("2000")}),
    )

    assert [intent.intent_type.value for intent in intents] == [
        "WITHDRAW",
        "SWAP",
        "REPAY",
        "WITHDRAW",
    ]

    partial_withdraw = intents[0]
    assert isinstance(partial_withdraw, WithdrawIntent)
    assert partial_withdraw.token == "WETH"
    assert partial_withdraw.amount == Decimal("0.55")
    assert partial_withdraw.withdraw_all is False

    swap = intents[1]
    assert isinstance(swap, SwapIntent)
    assert swap.from_token == "WETH"
    assert swap.to_token == "USDC"
    assert swap.amount == Decimal("0.55")

    repay = intents[2]
    assert isinstance(repay, RepayIntent)
    assert repay.token == "USDC"
    assert repay.repay_full is True

    final_withdraw = intents[3]
    assert isinstance(final_withdraw, WithdrawIntent)
    assert final_withdraw.token == "WETH"
    assert final_withdraw.withdraw_all is True


def test_morpho_looping_teardown_no_liquid_wallet_asset_fallback() -> None:
    """Fallback teardown when neither liquid borrow_token nor liquid
    collateral_token is in the wallet. The teardown still emits the
    WITHDRAW → SWAP → REPAY → WITHDRAW → SWAP sequence with a 10% slippage
    buffer on the first WITHDRAW. Note: this fallback can still trip Morpho's
    LLTV check on tight-LTV positions because nothing reduces debt before the
    first WITHDRAW — VIB-4466 tracks the framework primitive that would size
    the WITHDRAW iteratively for that case.
    """
    strategy = MorphoLoopingStrategy.__new__(MorphoLoopingStrategy)
    strategy._chain = "ethereum"
    strategy.market_id = "0xmarket"
    strategy.collateral_token = "wstETH"
    strategy.borrow_token = "USDC"
    strategy.swap_slippage = Decimal("0.005")
    strategy._total_collateral = Decimal("1")
    strategy._total_borrowed = Decimal("1700")
    strategy._pending_wallet_collateral = Decimal("0")
    strategy._pending_swap_amount = Decimal("0")
    strategy._loop_state = "idle"

    intents = strategy.generate_teardown_intents(
        TeardownMode.SOFT,
        market=_market({"USDC": Decimal("1"), "wstETH": Decimal("3400")}),
    )

    assert [intent.intent_type.value for intent in intents] == [
        "WITHDRAW",
        "SWAP",
        "REPAY",
        "WITHDRAW",
        "SWAP",
    ]

    partial_withdraw = intents[0]
    assert isinstance(partial_withdraw, WithdrawIntent)
    assert partial_withdraw.token == "wstETH"
    # collateral_for_repay = 1700 × 1.10 / 3400 = 0.55; withdraw = 0.55 × 1.10 = 0.605
    assert partial_withdraw.amount == Decimal("0.605")
    assert partial_withdraw.withdraw_all is False

    repay_funding_swap = intents[1]
    assert isinstance(repay_funding_swap, SwapIntent)
    assert repay_funding_swap.from_token == "wstETH"
    assert repay_funding_swap.to_token == "USDC"
    assert repay_funding_swap.amount == Decimal("0.55")

    repay = intents[2]
    assert isinstance(repay, RepayIntent)
    assert repay.repay_full is True

    final_withdraw = intents[3]
    assert isinstance(final_withdraw, WithdrawIntent)
    assert final_withdraw.withdraw_all is True

    final_recovery_swap = intents[4]
    assert isinstance(final_recovery_swap, SwapIntent)
    assert final_recovery_swap.from_token == "wstETH"
    assert final_recovery_swap.to_token == "USDC"
    assert final_recovery_swap.amount == "all"


def test_morpho_looping_teardown_complete_state_swaps_liquid_collateral_first() -> None:
    """End-of-loop teardown (loop_state="complete"). The final loop's SWAP
    output sits in the wallet as collateral_token in ``_pending_swap_amount``.
    Teardown must SWAP it to borrow_token and REPAY before any WITHDRAW, so
    the WITHDRAW that follows operates against reduced debt and stays under
    Morpho's LLTV. This matches the real on-chain state observed during the
    2026-05-15 E2E validation run (incident captured in VIB-4466).
    """
    strategy = MorphoLoopingStrategy.__new__(MorphoLoopingStrategy)
    strategy._chain = "ethereum"
    strategy.market_id = "0xmarket"
    strategy.collateral_token = "wstETH"
    strategy.borrow_token = "USDC"
    strategy.swap_slippage = Decimal("0.005")
    strategy._total_collateral = Decimal("0.0238")     # supplied to Morpho
    strategy._total_borrowed = Decimal("45.88")         # outstanding debt
    strategy._pending_wallet_collateral = Decimal("0")
    strategy._pending_swap_amount = Decimal("0.0068")   # liquid wstETH in wallet
    strategy._loop_state = "complete"

    intents = strategy.generate_teardown_intents(
        TeardownMode.SOFT,
        market=_market({"USDC": Decimal("1"), "wstETH": Decimal("2741")}),
    )

    # Step 2: SWAP + partial REPAY using wallet wstETH.
    # Step 3: WITHDRAW + SWAP + REPAY (full) for remaining debt.
    # Step 4: WITHDRAW remaining collateral.
    # Step 5: SWAP residual collateral.
    assert [intent.intent_type.value for intent in intents] == [
        "SWAP",
        "REPAY",
        "WITHDRAW",
        "SWAP",
        "REPAY",
        "WITHDRAW",
        "SWAP",
    ]

    step2_swap = intents[0]
    assert isinstance(step2_swap, SwapIntent)
    assert step2_swap.from_token == "wstETH"
    assert step2_swap.to_token == "USDC"
    assert step2_swap.amount == Decimal("0.0068")  # full liquid wallet amount

    step2_repay = intents[1]
    assert isinstance(step2_repay, RepayIntent)
    assert step2_repay.repay_full is False
    # estimated_yield = 0.0068 × (1 - 0.005) × 2741 / 1
    assert step2_repay.amount == (
        Decimal("0.0068")
        * (Decimal("1") - Decimal("0.005"))
        * Decimal("2741")
        / Decimal("1")
    )

    step3_withdraw = intents[2]
    assert isinstance(step3_withdraw, WithdrawIntent)
    assert step3_withdraw.token == "wstETH"
    assert step3_withdraw.withdraw_all is False

    step3_swap = intents[3]
    assert isinstance(step3_swap, SwapIntent)
    assert step3_swap.from_token == "wstETH"
    assert step3_swap.to_token == "USDC"

    step3_repay = intents[4]
    assert isinstance(step3_repay, RepayIntent)
    assert step3_repay.repay_full is True

    step4_withdraw = intents[5]
    assert isinstance(step4_withdraw, WithdrawIntent)
    assert step4_withdraw.withdraw_all is True

    step5_swap = intents[6]
    assert isinstance(step5_swap, SwapIntent)
    assert step5_swap.from_token == "wstETH"
    assert step5_swap.to_token == "USDC"
    assert step5_swap.amount == "all"


def test_morpho_looping_teardown_borrowed_state_repays_liquid_borrow_first() -> None:
    """Mid-loop teardown in ``borrowed`` state: a BORROW just succeeded and
    its proceeds are liquid in the wallet awaiting the loop SWAP. Teardown
    must REPAY that liquid borrow_token directly (no swap needed) to drop
    health-factor pressure before any WITHDRAW.
    """
    strategy = MorphoLoopingStrategy.__new__(MorphoLoopingStrategy)
    strategy._chain = "ethereum"
    strategy.market_id = "0xmarket"
    strategy.collateral_token = "wstETH"
    strategy.borrow_token = "USDC"
    strategy.swap_slippage = Decimal("0.005")
    strategy._total_collateral = Decimal("0.014")
    strategy._total_borrowed = Decimal("27.0")          # just borrowed
    strategy._pending_wallet_collateral = Decimal("0")
    strategy._pending_swap_amount = Decimal("27.0")     # liquid USDC in wallet
    strategy._loop_state = "borrowed"

    intents = strategy.generate_teardown_intents(
        TeardownMode.SOFT,
        market=_market({"USDC": Decimal("1"), "wstETH": Decimal("2741")}),
    )

    # Liquid USDC (27.0) covers the full debt (27.0), so debt_remaining = 0
    # after step 1. Steps 2 and 3 are skipped. Only the final WITHDRAW + SWAP
    # remain.
    assert [intent.intent_type.value for intent in intents] == [
        "REPAY",
        "WITHDRAW",
        "SWAP",
    ]

    repay = intents[0]
    assert isinstance(repay, RepayIntent)
    assert repay.repay_full is False
    assert repay.amount == Decimal("27.0")

    withdraw = intents[1]
    assert isinstance(withdraw, WithdrawIntent)
    assert withdraw.withdraw_all is True

    swap = intents[2]
    assert isinstance(swap, SwapIntent)
    assert swap.from_token == "wstETH"
    assert swap.to_token == "USDC"
    assert swap.amount == "all"


def test_morpho_looping_teardown_only_collateral_no_debt() -> None:
    """Early-stage teardown: collateral has been supplied but no BORROW has
    happened yet. No REPAY or first-pass WITHDRAW needed.
    """
    strategy = MorphoLoopingStrategy.__new__(MorphoLoopingStrategy)
    strategy._chain = "ethereum"
    strategy.market_id = "0xmarket"
    strategy.collateral_token = "wstETH"
    strategy.borrow_token = "USDC"
    strategy.swap_slippage = Decimal("0.005")
    strategy._total_collateral = Decimal("0.014")
    strategy._total_borrowed = Decimal("0")
    strategy._pending_wallet_collateral = Decimal("0")
    strategy._pending_swap_amount = Decimal("0")
    strategy._loop_state = "supplied"

    intents = strategy.generate_teardown_intents(
        TeardownMode.SOFT,
        market=_market({"USDC": Decimal("1"), "wstETH": Decimal("2741")}),
    )

    assert [intent.intent_type.value for intent in intents] == [
        "WITHDRAW",
        "SWAP",
    ]

    withdraw = intents[0]
    assert isinstance(withdraw, WithdrawIntent)
    assert withdraw.withdraw_all is True

    swap = intents[1]
    assert isinstance(swap, SwapIntent)
    assert swap.from_token == "wstETH"
    assert swap.to_token == "USDC"
    assert swap.amount == "all"


def test_morpho_looping_teardown_caps_withdraw_at_total_collateral_on_tight_ltv() -> None:
    """High-leverage teardown: the un-capped ``withdraw_for_swap`` would exceed
    the strategy's tracked ``_total_collateral`` (which on-chain is the
    Morpho-supplied amount). Morpho's ``withdrawCollateral`` reverts when asked
    for more than is supplied, so cap both the withdrawal and the swap. Gemini
    flagged this on PR #2330 — high-priority safety guard.
    """
    strategy = MorphoLoopingStrategy.__new__(MorphoLoopingStrategy)
    strategy._chain = "ethereum"
    strategy.market_id = "0xmarket"
    strategy.collateral_token = "wstETH"
    strategy.borrow_token = "USDC"
    strategy.swap_slippage = Decimal("0.005")
    # LTV = 1700 / (0.5 × 3400) = 100% — well above Morpho's 86% LLTV. The
    # uncapped withdraw_for_swap = 0.55 × 1.10 = 0.605 exceeds _total_collateral
    # = 0.5; the cap forces both withdraw and swap to 0.5.
    strategy._total_collateral = Decimal("0.5")
    strategy._total_borrowed = Decimal("1700")
    strategy._pending_wallet_collateral = Decimal("0")
    strategy._pending_swap_amount = Decimal("0")
    strategy._loop_state = "idle"

    intents = strategy.generate_teardown_intents(
        TeardownMode.SOFT,
        market=_market({"USDC": Decimal("1"), "wstETH": Decimal("3400")}),
    )

    assert [intent.intent_type.value for intent in intents] == [
        "WITHDRAW",
        "SWAP",
        "REPAY",
        "WITHDRAW",
        "SWAP",
    ]

    capped_withdraw = intents[0]
    assert isinstance(capped_withdraw, WithdrawIntent)
    assert capped_withdraw.amount == Decimal("0.5")  # capped at _total_collateral
    assert capped_withdraw.withdraw_all is False

    capped_swap = intents[1]
    assert isinstance(capped_swap, SwapIntent)
    assert capped_swap.amount == Decimal("0.5")  # capped to match capped withdraw


def test_morpho_looping_teardown_hard_mode_uses_emergency_slippage() -> None:
    """TeardownMode.HARD escalates the swap slippage ceiling to 3% to absorb
    price moves in an emergency unwind.
    """
    strategy = MorphoLoopingStrategy.__new__(MorphoLoopingStrategy)
    strategy._chain = "ethereum"
    strategy.market_id = "0xmarket"
    strategy.collateral_token = "wstETH"
    strategy.borrow_token = "USDC"
    strategy.swap_slippage = Decimal("0.005")
    strategy._total_collateral = Decimal("0.0238")
    strategy._total_borrowed = Decimal("45.88")
    strategy._pending_wallet_collateral = Decimal("0")
    strategy._pending_swap_amount = Decimal("0.0068")
    strategy._loop_state = "complete"

    intents = strategy.generate_teardown_intents(
        TeardownMode.HARD,
        market=_market({"USDC": Decimal("1"), "wstETH": Decimal("2741")}),
    )

    swaps = [intent for intent in intents if isinstance(intent, SwapIntent)]
    # complete-state teardown emits exactly 3 swaps (step 2 wallet, step 3
    # withdrawn-collateral, step 5 residual). Asserting the count catches
    # sequence regressions, not just slippage-field regressions.
    assert len(swaps) == 3
    for swap in swaps:
        assert swap.max_slippage == Decimal("0.03")


# ---------------------------------------------------------------------------
# State hygiene: ensure re-entrant teardown does NOT double-count liquid wallet
# balances. The teardown manager may call generate_teardown_intents() again
# after a partial failure; without these drain-after-execute updates the same
# wallet balance would be reported twice. Codex flagged this on PR #2330 (P2).
# ---------------------------------------------------------------------------


def _build_strategy_for_bookkeeping() -> MorphoLoopingStrategy:
    strategy = MorphoLoopingStrategy.__new__(MorphoLoopingStrategy)
    # `deployment_id` is a read-only property backed by `_deployment_id`; set the
    # underlying field for tests that bypass __init__ via __new__.
    strategy._deployment_id = "test-morpho-looping"
    strategy._chain = "ethereum"
    strategy.market_id = "0xmarket"
    strategy.collateral_token = "wstETH"
    strategy.borrow_token = "USDC"
    return strategy


def test_morpho_looping_repay_drains_pending_swap_amount_when_borrowed() -> None:
    """A REPAY consuming the just-borrowed wallet balance must drain
    ``_pending_swap_amount`` in addition to ``_total_borrowed``. Without this,
    a re-entrant teardown would re-emit the same partial REPAY against an
    empty wallet, which the chain would reject.
    """
    strategy = _build_strategy_for_bookkeeping()
    strategy._total_borrowed = Decimal("27.0")
    strategy._pending_swap_amount = Decimal("27.0")
    strategy._loop_state = "borrowed"
    strategy._previous_stable_state = "borrowed"

    intent = MagicMock()
    intent.intent_type.value = "REPAY"
    intent.repay_full = False
    intent.amount = Decimal("27.0")

    with patch("almanak.demo_strategies.morpho_looping.strategy.add_event"):
        strategy.on_intent_executed(intent, success=True, result=None)

    assert strategy._total_borrowed == Decimal("0")
    assert strategy._pending_swap_amount == Decimal("0")


def test_morpho_looping_repay_full_zeros_pending_swap_when_borrowed() -> None:
    """``repay_full=True`` from teardown also clears ``_pending_swap_amount``
    so re-entrant teardown sees a clean slate.
    """
    strategy = _build_strategy_for_bookkeeping()
    strategy._total_borrowed = Decimal("27.0")
    strategy._pending_swap_amount = Decimal("27.0")
    strategy._loop_state = "borrowed"
    strategy._previous_stable_state = "borrowed"

    intent = MagicMock()
    intent.intent_type.value = "REPAY"
    intent.repay_full = True
    intent.amount = Decimal("0")  # ignored when repay_full=True

    with patch("almanak.demo_strategies.morpho_looping.strategy.add_event"):
        strategy.on_intent_executed(intent, success=True, result=None)

    assert strategy._total_borrowed == Decimal("0")
    assert strategy._pending_swap_amount == Decimal("0")


def test_morpho_looping_repay_outside_borrowed_state_leaves_pending_swap_alone() -> None:
    """When the strategy is NOT in 'borrowed' state, ``_pending_swap_amount``
    holds the collateral_token (loop-SWAP output), not borrow_token. A REPAY
    should not touch it.
    """
    strategy = _build_strategy_for_bookkeeping()
    strategy._total_borrowed = Decimal("45.88")
    strategy._pending_swap_amount = Decimal("0.0068")  # collateral_token
    strategy._loop_state = "complete"
    strategy._previous_stable_state = "swapped"

    intent = MagicMock()
    intent.intent_type.value = "REPAY"
    intent.repay_full = False
    intent.amount = Decimal("18.5")

    with patch("almanak.demo_strategies.morpho_looping.strategy.add_event"):
        strategy.on_intent_executed(intent, success=True, result=None)

    assert strategy._total_borrowed == Decimal("45.88") - Decimal("18.5")
    assert strategy._pending_swap_amount == Decimal("0.0068")  # unchanged


def test_morpho_looping_wallet_collateral_swap_drains_both_sources() -> None:
    """Teardown step 2 swaps ``_pending_wallet_collateral + _pending_swap_amount``
    in a single intent. The bookkeeping update must drain
    ``_pending_wallet_collateral`` first and then bleed the overflow into
    ``_pending_swap_amount``. Otherwise re-entry sees stale liquid balance.
    """
    strategy = _build_strategy_for_bookkeeping()
    strategy._pending_wallet_collateral = Decimal("0.001")
    strategy._pending_swap_amount = Decimal("0.0068")
    strategy._loop_state = "complete"
    strategy._loops_completed = 2

    intent = MagicMock()
    intent.intent_type.value = "SWAP"
    intent.from_token = "wstETH"
    intent.to_token = "USDC"
    intent.amount = Decimal("0.0078")  # the sum

    with patch("almanak.demo_strategies.morpho_looping.strategy.add_event"):
        strategy.on_intent_executed(intent, success=True, result=None)

    assert strategy._pending_wallet_collateral == Decimal("0")
    assert strategy._pending_swap_amount == Decimal("0")


def test_morpho_looping_wallet_collateral_swap_all_clears_both_sources() -> None:
    """``amount="all"`` SWAP in the final teardown step clears both pending
    counters when state is complete/swapped.
    """
    strategy = _build_strategy_for_bookkeeping()
    strategy._pending_wallet_collateral = Decimal("0.001")
    strategy._pending_swap_amount = Decimal("0.0068")
    strategy._loop_state = "complete"
    strategy._loops_completed = 2

    intent = MagicMock()
    intent.intent_type.value = "SWAP"
    intent.from_token = "wstETH"
    intent.to_token = "USDC"
    intent.amount = "all"

    with patch("almanak.demo_strategies.morpho_looping.strategy.add_event"):
        strategy.on_intent_executed(intent, success=True, result=None)

    assert strategy._pending_wallet_collateral == Decimal("0")
    assert strategy._pending_swap_amount == Decimal("0")


def test_morpho_looping_wallet_collateral_swap_outside_complete_keeps_pending_swap() -> None:
    """Outside of swapped/complete state, ``_pending_swap_amount`` does NOT
    hold the teardown-source collateral, so a wallet-collateral SWAP must not
    touch it. (This guards against accidentally clobbering loop bookkeeping.)
    """
    strategy = _build_strategy_for_bookkeeping()
    strategy._pending_wallet_collateral = Decimal("0.005")
    strategy._pending_swap_amount = Decimal("99")  # holdover, should NOT be drained
    strategy._loop_state = "borrowed"
    strategy._loops_completed = 1

    intent = MagicMock()
    intent.intent_type.value = "SWAP"
    intent.from_token = "wstETH"
    intent.to_token = "USDC"
    intent.amount = Decimal("0.005")

    with patch("almanak.demo_strategies.morpho_looping.strategy.add_event"):
        strategy.on_intent_executed(intent, success=True, result=None)

    assert strategy._pending_wallet_collateral == Decimal("0")
    assert strategy._pending_swap_amount == Decimal("99")  # unchanged


def test_aave_paper_trade_leverage_metadata_includes_teardown_intents() -> None:
    intent_types = AavePaperTradeLeveragePolygonStrategy.STRATEGY_METADATA.intent_types

    assert "REPAY" in intent_types
    assert "WITHDRAW" in intent_types
