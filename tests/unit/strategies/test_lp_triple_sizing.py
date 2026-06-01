"""Unit tests for the lp_triple money-path sizing (VIB-4787).

The fixture used to size each LP leg off the LIVE WALLET balance
(``amount0 = token0_balance * commit_pct``), so an over-funded wallet
requested far more than the configured ``total_value_usd`` — Σ requested
across A/B/C reached ~14x the cap, and only Uniswap V3 range-binding refund
kept the realized deployment near the cap (accidental, funding/price
dependent). VIB-4787 reinterprets the capital-split fractions as fractions of
the *remaining budget* and splits each leg's USD budget 50/50 across the two
pool tokens, so Σ requested USD <= ``total_value_usd`` deterministically while
preserving the A/B/C weighting and the no-false-floor behaviour.

These tests pin:
  * the deterministic per-leg budget chain (``_leg_budget_usd``),
  * the budget -> capped two-sided amount conversion (``_budget_to_amounts``),
  * Σ requested USD <= cap through the real ``_build_lp_open`` path,
  * the no-false-floor regression (wallet < budget uses available wallet),
  * the broadened zero-price guard (token0 entered the division path here).

Like the phase-machine tests, we construct the strategy via ``__new__`` and
inject only the scalar attributes ``_build_lp_open`` reads — no runner /
gateway scaffold.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from strategies.accounting.lp_triple.strategy import (
    _LEG_C_BUDGET_FRACTION,
    PHASE_SWAPPED_IN,
    AccountingQuantLPTripleStrategy,
)

# Configured commit fractions (config.json: A=0.33, B=0.50; C = the named
# leg-C "take the rest w/ 1% margin" constant, referenced not re-hardcoded).
_FRACTIONS = (Decimal("0.33"), Decimal("0.50"), _LEG_C_BUDGET_FRACTION)
_TOTAL = Decimal("12.0")
# A symmetric WETH/USDC fixture: token0=WETH (~$3000), token1=USDC (~$1).
_P0 = Decimal("3000")
_P1 = Decimal("1")


class _MockMarket:
    """Minimal MarketSnapshot stand-in: ``.balance(sym)`` returns an object
    with a ``.balance`` attribute; ``.price(sym)`` returns a USD price."""

    def __init__(self, balances: dict[str, Decimal], prices: dict[str, Decimal]):
        self._balances = balances
        self._prices = prices

    def balance(self, symbol: str):
        return SimpleNamespace(balance=self._balances[symbol])

    def price(self, symbol: str):
        return self._prices[symbol]


def _sizing_strategy(*, total_value_usd: Decimal = _TOTAL) -> AccountingQuantLPTripleStrategy:
    """Bare strategy with only the attributes ``_build_lp_open`` reads."""
    obj = AccountingQuantLPTripleStrategy.__new__(AccountingQuantLPTripleStrategy)
    obj._phase = PHASE_SWAPPED_IN
    obj.token0_symbol = "WETH"
    obj.token1_symbol = "USDC"
    obj.fee_tier = 500
    obj.pool = "WETH/USDC/500"
    obj.total_value_usd = total_value_usd
    obj.lp_a_range_width_pct = Decimal("0.10")
    obj.lp_b_range_width_pct = Decimal("0.20")
    obj.lp_c_range_width_pct = Decimal("0.40")
    obj.lp_a_capital_split_pct = Decimal("0.33")
    obj.lp_b_capital_split_pct = Decimal("0.50")
    return obj


def _market(weth: Decimal, usdc: Decimal) -> _MockMarket:
    return _MockMarket(
        balances={"WETH": weth, "USDC": usdc},
        prices={"WETH": _P0, "USDC": _P1},
    )


def _leg_usd(intent) -> Decimal:
    """USD value of an LP_OPEN intent's two requested sides."""
    return Decimal(str(intent.amount0)) * _P0 + Decimal(str(intent.amount1)) * _P1


# ---------------------------------------------------------------------------
# _leg_budget_usd — the deterministic remaining-budget chain
# ---------------------------------------------------------------------------


def test_leg_budgets_match_remaining_chain():
    budgets = [AccountingQuantLPTripleStrategy._leg_budget_usd(_TOTAL, _FRACTIONS, i) for i in range(3)]
    a, b, c = budgets
    # A = 12*0.33; B = 12*0.67*0.50; C = 12*0.335*0.99.
    assert a == pytest.approx(Decimal("3.96"), abs=Decimal("0.001"))
    assert b == pytest.approx(Decimal("4.02"), abs=Decimal("0.001"))
    assert c == pytest.approx(Decimal("3.9803"), abs=Decimal("0.001"))
    # The whole point of the fix: the three budgets sum to <= the cap.
    assert sum(budgets) <= _TOTAL
    # Leg C ("0.99 of the rest") leaves only a ~1% dust margin.
    remaining_after_ab = _TOTAL - a - b
    assert c == pytest.approx(remaining_after_ab * Decimal("0.99"), abs=Decimal("0.0001"))


def test_weighting_preserved():
    a, b, c = (AccountingQuantLPTripleStrategy._leg_budget_usd(_TOTAL, _FRACTIONS, i) for i in range(3))
    # B is the largest (0.50 of the post-A remainder); A and C straddle it but
    # all three stay within ~2% of an equal third — the configured A/B/C
    # weighting intent, now scoped to the budget rather than the wallet.
    assert a < b
    assert c < b
    third = _TOTAL / Decimal("3")
    for leg in (a, b, c):
        assert abs(leg - third) < third * Decimal("0.05")


# ---------------------------------------------------------------------------
# _budget_to_amounts — 50/50 split, wallet cap, no false floor
# ---------------------------------------------------------------------------


def test_budget_split_5050_when_wallet_abundant():
    # Wallet far larger than the budget -> neither side is capped.
    amount0, amount1 = AccountingQuantLPTripleStrategy._budget_to_amounts(
        Decimal("4.0"), _P0, _P1, Decimal("100"), Decimal("100000")
    )
    # 50/50 split: $2 each side -> 2/3000 WETH and 2/1 USDC.
    assert amount0 == pytest.approx(Decimal("2") / _P0, rel=Decimal("1e-9"))
    assert amount1 == pytest.approx(Decimal("2"), rel=Decimal("1e-9"))
    # Combined requested USD equals the budget (no over-request).
    assert (amount0 * _P0 + amount1 * _P1) == pytest.approx(Decimal("4.0"), rel=Decimal("1e-9"))


def test_budget_to_amounts_symmetric_in_tokens():
    # Swapping the (price, balance) pairs swaps the outputs — proves the
    # helper is symmetric in token0/token1 (generality for starting_asset).
    a0, a1 = AccountingQuantLPTripleStrategy._budget_to_amounts(
        Decimal("4.0"), _P0, _P1, Decimal("100"), Decimal("100000")
    )
    b1, b0 = AccountingQuantLPTripleStrategy._budget_to_amounts(
        Decimal("4.0"), _P1, _P0, Decimal("100000"), Decimal("100")
    )
    assert a0 == b0
    assert a1 == b1


def test_zero_budget_yields_zero_amounts():
    # A misconfigured total_value_usd=0 makes every leg budget 0 -> both sides
    # 0. Pins the fail-loud: Intent.lp_open's __post_init__ rejects a
    # both-amounts-zero mint, so the strategy surfaces the misconfig rather
    # than silently minting nothing.
    amount0, amount1 = AccountingQuantLPTripleStrategy._budget_to_amounts(
        Decimal("0"), _P0, _P1, Decimal("100"), Decimal("100000")
    )
    assert amount0 == Decimal("0")
    assert amount1 == Decimal("0")


def test_no_false_floor_when_wallet_below_budget():
    # Both sides smaller than the per-side budget -> use the available wallet,
    # never floor up to the budget.
    tiny_weth = Decimal("0.0001")  # ~$0.30, well below the $1.98 per-side
    tiny_usdc = Decimal("0.50")  # below the $1.98 per-side
    amount0, amount1 = AccountingQuantLPTripleStrategy._budget_to_amounts(
        Decimal("3.96"), _P0, _P1, tiny_weth, tiny_usdc
    )
    assert amount0 == tiny_weth
    assert amount1 == tiny_usdc


# ---------------------------------------------------------------------------
# _build_lp_open — Σ requested USD <= cap end-to-end
# ---------------------------------------------------------------------------


def test_sum_requested_usd_within_budget_over_funded_wallet():
    # The repro condition: total wallet (~$6 WETH + $94 USDC = ~$100) >> the $12 cap.
    strat = _sizing_strategy()
    market = _market(weth=Decimal("0.002"), usdc=Decimal("94"))  # ~$6 WETH + $94 USDC
    total_requested = sum(_leg_usd(strat._build_lp_open(market, slot_index=i)) for i in range(3))
    # Deterministically within the cap (small tolerance for the leg-C dust margin).
    assert total_requested <= _TOTAL
    # And not collapsed to ~0 — it genuinely deploys close to the cap.
    assert total_requested > _TOTAL * Decimal("0.9")


def test_per_leg_requested_usd_tracks_budget():
    strat = _sizing_strategy()
    # Abundant both sides -> requested == budget per leg.
    market = _market(weth=Decimal("100"), usdc=Decimal("100000"))
    for i in range(3):
        expected = AccountingQuantLPTripleStrategy._leg_budget_usd(_TOTAL, _FRACTIONS, i)
        got = _leg_usd(strat._build_lp_open(market, slot_index=i))
        assert got == pytest.approx(expected, rel=Decimal("1e-6"))


def test_build_lp_open_no_false_floor_through_public_path():
    # Wallet below the budget on the scarce side -> intent uses the wallet.
    strat = _sizing_strategy()
    market = _market(weth=Decimal("0.0001"), usdc=Decimal("0.5"))
    intent = strat._build_lp_open(market, slot_index=0)
    assert Decimal(str(intent.amount0)) == Decimal("0.0001")
    assert Decimal(str(intent.amount1)) == Decimal("0.5")


# ---------------------------------------------------------------------------
# Broadened zero-price guard (token0 now divides too)
# ---------------------------------------------------------------------------


def test_zero_token0_price_raises():
    strat = _sizing_strategy()
    market = _MockMarket(
        balances={"WETH": Decimal("1"), "USDC": Decimal("100")},
        prices={"WETH": Decimal("0"), "USDC": _P1},
    )
    with pytest.raises(ValueError, match="Invalid price for LP sizing"):
        strat._build_lp_open(market, slot_index=0)


def test_zero_token1_price_raises():
    strat = _sizing_strategy()
    market = _MockMarket(
        balances={"WETH": Decimal("1"), "USDC": Decimal("100")},
        prices={"WETH": _P0, "USDC": Decimal("0")},
    )
    with pytest.raises(ValueError, match="Invalid price for LP sizing"):
        strat._build_lp_open(market, slot_index=0)
