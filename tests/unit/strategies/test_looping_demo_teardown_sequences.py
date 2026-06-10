from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

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


def _market_with_health(
    *,
    collateral_usd: str,
    debt_usd: str,
    lltv: str = "0.86",
    col_price: str = "3400",
    borrow_price: str = "1",
    wallet_borrow: str = "0",
    wallet_collateral: str = "0",
) -> MagicMock:
    """MagicMock market exposing price/balance/position_health for the staircase.

    MorphoLooping teardown now delegates to ``framework.teardown.leverage_loop``,
    which sizes the unwind from the live on-chain position (read via
    ``position_health``) rather than the strategy's internal tracking. These
    tests therefore drive the staircase by mocking the health read, not by
    setting ``_total_collateral`` / ``_total_borrowed``.
    """
    market = MagicMock()
    market.price.side_effect = lambda t: {
        "wstETH": Decimal(col_price),
        "USDC": Decimal(borrow_price),
    }.get(t, Decimal("1"))

    def _bal(t: str) -> MagicMock:
        b = MagicMock()
        b.balance = Decimal(wallet_borrow) if t == "USDC" else Decimal(wallet_collateral)
        return b

    market.balance.side_effect = _bal

    health = MagicMock()
    health.collateral_value_usd = Decimal(collateral_usd)
    health.debt_value_usd = Decimal(debt_usd)
    health.lltv = Decimal(lltv)
    health.health_factor = (
        Decimal(collateral_usd) * Decimal(lltv) / Decimal(debt_usd) if Decimal(debt_usd) > 0 else Decimal("Infinity")
    )
    market.position_health.return_value = health
    return market


def _new_morpho_strategy() -> MorphoLoopingStrategy:
    strategy = MorphoLoopingStrategy.__new__(MorphoLoopingStrategy)
    strategy._chain = "ethereum"
    strategy.market_id = "0xmarket"
    strategy.collateral_token = "wstETH"
    strategy.borrow_token = "USDC"
    strategy.swap_slippage = Decimal("0.005")
    return strategy


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


def test_morpho_looping_teardown_emits_hf_aware_staircase() -> None:
    """Teardown delegates to the framework staircase helper, which sizes the
    unwind from the live on-chain position (read via ``position_health``). For a
    healthy position it emits one or more WITHDRAW -> SWAP -> REPAY rounds and
    ends consolidated to the borrow token. (The staircase math itself is proven
    exhaustively in tests/unit/teardown/test_leverage_loop_unwind.py.)
    """
    strategy = _new_morpho_strategy()

    # Live position: ~$3,400 wstETH collateral, $1,700 USDC debt (HF ~1.72).
    intents = strategy.generate_teardown_intents(
        TeardownMode.SOFT,
        market=_market_with_health(collateral_usd="3400", debt_usd="1700"),
    )

    kinds = [intent.intent_type.value for intent in intents]
    assert "WITHDRAW" in kinds and "SWAP" in kinds and "REPAY" in kinds
    assert kinds[-1] == "SWAP"  # ends consolidated to borrow_token

    # Every REPAY is morpho_blue on the configured isolated market.
    for intent in intents:
        if isinstance(intent, RepayIntent):
            assert intent.protocol == "morpho_blue"
            assert intent.market_id == "0xmarket"


def test_morpho_looping_teardown_no_debt_just_withdraws_and_consolidates() -> None:
    """No outstanding debt: withdraw all collateral, consolidate to borrow token.
    No REPAY needed.
    """
    strategy = _new_morpho_strategy()

    intents = strategy.generate_teardown_intents(
        TeardownMode.SOFT,
        market=_market_with_health(collateral_usd="3400", debt_usd="0"),
    )

    assert [intent.intent_type.value for intent in intents] == ["WITHDRAW", "SWAP"]
    assert intents[0].withdraw_all is True
    assert intents[1].amount == "all"


def test_morpho_looping_teardown_no_position_is_empty() -> None:
    """Nothing supplied or borrowed -> no teardown intents."""
    strategy = _new_morpho_strategy()

    intents = strategy.generate_teardown_intents(
        TeardownMode.SOFT,
        market=_market_with_health(collateral_usd="0", debt_usd="0"),
    )

    assert intents == []


def test_morpho_looping_teardown_repays_liquid_wallet_borrow_first() -> None:
    """A wallet holding the full debt in borrow_token is repaid first (one
    REPAY), so the staircase does zero withdraw->swap->repay rounds before the
    final withdraw-all + consolidate.
    """
    strategy = _new_morpho_strategy()

    intents = strategy.generate_teardown_intents(
        TeardownMode.SOFT,
        market=_market_with_health(collateral_usd="3400", debt_usd="1700", wallet_borrow="1700"),
    )

    kinds = [intent.intent_type.value for intent in intents]
    assert kinds == ["REPAY", "WITHDRAW", "SWAP"]
    assert intents[0].repay_full is True  # wallet covers the full debt
    assert intents[1].withdraw_all is True


def test_morpho_looping_teardown_hard_mode_uses_emergency_slippage() -> None:
    """TeardownMode.HARD escalates the swap slippage ceiling to 3% to absorb
    price moves in an emergency unwind.
    """
    strategy = _new_morpho_strategy()

    intents = strategy.generate_teardown_intents(
        TeardownMode.HARD,
        market=_market_with_health(collateral_usd="3400", debt_usd="1700"),
    )

    swaps = [intent for intent in intents if isinstance(intent, SwapIntent)]
    assert swaps, "expected at least one swap in the unwind"
    for swap in swaps:
        assert swap.max_slippage == Decimal("0.03")


def test_morpho_looping_teardown_too_unhealthy_fails_loud() -> None:
    """A position whose HF is already below the withdraw floor cannot be
    unwound withdraw-first; teardown must raise rather than emit a reverting
    intent. (HF = 3400 * 0.86 / 3300 ~= 0.886 < floor.)
    """
    from almanak.framework.teardown.leverage_loop import LeverageUnwindError

    strategy = _new_morpho_strategy()

    with pytest.raises(LeverageUnwindError):
        strategy.generate_teardown_intents(
            TeardownMode.SOFT,
            market=_market_with_health(collateral_usd="3400", debt_usd="3300"),
        )


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
