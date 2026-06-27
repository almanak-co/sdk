"""Deterministic simulation of the leverage-loop staircase unwind.

Runs the intents emitted by ``generate_leverage_loop_teardown`` through a fake
lending model that enforces the *real* on-chain semantics the staircase exists
to respect:

  * a withdraw reverts if it would leave the post-withdraw HF < 1.0 (LLTV check),
  * ``repay_full`` on Morpho/Compound pulls the ENTIRE outstanding debt from the
    wallet and reverts on a shortfall (Aave caps at the wallet balance),
  * a partial repay pulls ``min(amount, debt)`` and reverts if that exceeds the
    wallet balance.

The protocol-specific repay semantics are the crux: a naive staircase that uses
``repay_full`` on every round repays only a partial slice's worth of swap output
on Morpho/Compound and reverts. The simulation below makes that failure mode a
hard test error, so the multi-round Morpho/Compound paths are actually proven.

Invariants proven:
  * every withdraw keeps post-withdraw HF >= 1.0 (no LLTV revert),
  * no repay ever pulls more loan token than the wallet holds (no repay revert),
  * the position fully unwinds (debt -> 0, collateral -> 0),
  * tight positions take multiple rounds and still converge,
  * wallet-held debt token is repaid first (fewer rounds), partial or full,
  * positions too unhealthy to withdraw safely fail loud.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.teardown.leverage_loop import (
    LeverageUnwindError,
    generate_leverage_loop_teardown,
)

LLTV = Decimal("0.83")
_EPS = Decimal("1e-9")

# Protocols whose repay_full pulls the FULL outstanding debt from the wallet
# (Morpho via full borrow shares, Compound via MAX_UINT256) and revert on a
# shortfall. Aave caps repay at the wallet balance.
_STRICT_FULL_REPAY = {"morpho_blue", "compound_v3"}

# market_id is required by morpho_blue/compound_v3 intents, ignored by aave_v3.
_MARKET_ID = {"morpho_blue": "0xmarket", "compound_v3": "0xcomet"}


class _FakeHealth:
    def __init__(self, collateral_usd: Decimal, debt_usd: Decimal, lltv: Decimal) -> None:
        self.collateral_value_usd = collateral_usd
        self.debt_value_usd = debt_usd
        self.lltv = lltv


class _Bal:
    def __init__(self, amount: Decimal) -> None:
        self.balance = amount


class _LendingSim:
    """Minimal lending model. 1 token == $1 so USD and token amounts coincide.

    ``apply`` mutates state and raises on any on-chain revert the staircase is
    supposed to avoid: an LLTV-breaching withdraw, or a repay that pulls more
    loan token than the wallet holds (with protocol-specific full-repay rules).
    """

    def __init__(self, protocol: str, collateral_usd: str, debt_usd: str, wallet_borrow: str = "0") -> None:
        self.protocol = protocol
        self.collateral_usd = Decimal(collateral_usd)
        self.debt_usd = Decimal(debt_usd)
        self.wallet_borrow = Decimal(wallet_borrow)
        self.wallet_collateral = Decimal("0")
        self.realized_slippage = Decimal("0.005")  # at the SOFT max_slippage bound

    # ---- MarketSnapshot surface ----
    def position_health(self, protocol: str, market_id: str, **kwargs: object) -> _FakeHealth:
        return _FakeHealth(self.collateral_usd, self.debt_usd, LLTV)

    def price(self, token: str) -> Decimal:
        return Decimal("1")

    def balance(self, token: str, *, chain: str | None = None) -> _Bal:
        return _Bal(self.wallet_borrow if token == "USDC" else self.wallet_collateral)

    # ---- execution ----
    def apply(self, intent: Any) -> None:
        kind = type(intent).__name__
        if kind == "WithdrawIntent":
            w = self.collateral_usd if getattr(intent, "withdraw_all", False) else Decimal(str(intent.amount))
            if self.debt_usd > 0 and (self.collateral_usd - w) * LLTV < self.debt_usd:
                raise AssertionError(
                    f"LLTV revert: withdraw {w} leaves HF "
                    f"{((self.collateral_usd - w) * LLTV / self.debt_usd):.3f} < 1.0"
                )
            self.collateral_usd -= w
            self.wallet_collateral += w
        elif kind == "SwapIntent":
            if getattr(intent, "from_token", None) == "USDC":
                # Residual-sweep direction: borrow -> collateral
                # (consolidate_to=collateral_token).
                amt = self.wallet_borrow if intent.amount == "all" else Decimal(str(intent.amount))
                assert amt <= self.wallet_borrow + _EPS, "swap exceeds wallet borrow"
                self.wallet_borrow -= amt
                self.wallet_collateral += amt * (Decimal("1") - self.realized_slippage)
            else:
                amt = self.wallet_collateral if intent.amount == "all" else Decimal(str(intent.amount))
                assert amt <= self.wallet_collateral + _EPS, "swap exceeds wallet collateral"
                self.wallet_collateral -= amt
                self.wallet_borrow += amt * (Decimal("1") - self.realized_slippage)
        elif kind == "RepayIntent":
            self._apply_repay(intent)

    def _apply_repay(self, intent: Any) -> None:
        if getattr(intent, "repay_full", False):
            if self.protocol in _STRICT_FULL_REPAY:
                # Pull the ENTIRE debt from the wallet; revert if it falls short.
                if self.wallet_borrow + _EPS < self.debt_usd:
                    raise AssertionError(
                        f"{self.protocol} repay_full pulls full debt ${self.debt_usd} "
                        f"but wallet holds only ${self.wallet_borrow} -> on-chain revert"
                    )
                pay = self.debt_usd
            else:
                pay = min(self.wallet_borrow, self.debt_usd)  # Aave caps at wallet
        else:
            # Partial repay pulls min(amount, debt); revert if that exceeds wallet.
            amount = Decimal(str(intent.amount))
            pay = min(amount, self.debt_usd)
            if pay > self.wallet_borrow + _EPS:
                raise AssertionError(
                    f"partial repay pulls ${pay} but wallet holds only ${self.wallet_borrow} -> revert"
                )
        self.debt_usd -= pay
        self.wallet_borrow -= pay


def _run(sim: _LendingSim, consolidate_to: str | None = None) -> list:
    intents = generate_leverage_loop_teardown(
        market=sim,
        protocol=sim.protocol,
        collateral_token="WETH",
        borrow_token="USDC",
        consolidate_to=consolidate_to,
        market_id=_MARKET_ID.get(sim.protocol),
        chain="arbitrum",
    )
    for intent in intents:
        sim.apply(intent)  # raises on any LLTV or repay-shortfall revert
    return intents


_PROTOCOLS = ["aave_v3", "morpho_blue", "compound_v3"]


@pytest.mark.parametrize("protocol", _PROTOCOLS)
@pytest.mark.parametrize(
    ("collateral", "debt", "min_rounds"),
    [
        ("10000", "5000", 1),  # HF 1.66
        ("10000", "6000", 1),  # HF 1.38
        ("10000", "6900", 2),  # HF 1.20
        ("10000", "7500", 2),  # HF 1.10 — near liquidation, multi-round
    ],
)
def test_tight_position_unwinds_without_lltv_revert(protocol: str, collateral: str, debt: str, min_rounds: int) -> None:
    sim = _LendingSim(protocol, collateral, debt)
    intents = _run(sim)
    repays = sum(1 for i in intents if type(i).__name__ == "RepayIntent")
    assert repays >= min_rounds, f"expected >= {min_rounds} repays, got {repays}"
    assert sim.debt_usd <= Decimal("0.5"), f"debt not cleared: {sim.debt_usd}"
    assert sim.collateral_usd <= Decimal("0.5"), f"collateral not withdrawn: {sim.collateral_usd}"


@pytest.mark.parametrize("protocol", _PROTOCOLS)
def test_near_liquidation_takes_multiple_rounds(protocol: str) -> None:
    # HF 1.10 must require the HF-constrained partial-repay staircase, NOT a
    # single repay_full (which would revert on Morpho/Compound). Prove > 1 round.
    sim = _LendingSim(protocol, "10000", "7500")
    intents = _run(sim)
    repays = sum(1 for i in intents if type(i).__name__ == "RepayIntent")
    assert repays >= 2, f"tight position should need a multi-round staircase, got {repays}"
    assert sim.debt_usd <= Decimal("0.5")


@pytest.mark.parametrize("protocol", _PROTOCOLS)
def test_loose_position_unwinds_in_one_round(protocol: str) -> None:
    sim = _LendingSim(protocol, "10000", "2440")  # HF ~3.4 (the forked-chain test level)
    intents = _run(sim)
    repays = sum(1 for i in intents if type(i).__name__ == "RepayIntent")
    assert repays == 1
    assert sim.debt_usd <= Decimal("0.5")
    assert sim.collateral_usd <= Decimal("0.5")


@pytest.mark.parametrize("protocol", _PROTOCOLS)
def test_wallet_debt_token_fully_covers_debt_is_repaid_first(protocol: str) -> None:
    # Idle USDC COMFORTABLY above the debt (beyond the wallet-first buffer) -> a
    # single wallet-first repay_full clears it, so the staircase does zero withdraw
    # rounds. The wallet must exceed debt * _WALLET_FIRST_BUFFER (1%) to take the
    # shortcut; 5100 > 5000 * 1.01 = 5050.
    sim = _LendingSim(protocol, "10000", "5000", wallet_borrow="5100")
    intents = _run(sim)
    repays = sum(1 for i in intents if type(i).__name__ == "RepayIntent")
    withdraws = sum(1 for i in intents if type(i).__name__ == "WithdrawIntent")
    assert repays == 1, "should repay idle wallet debt token directly"
    assert withdraws == 1, "only the final withdraw_all, no staircase withdraws"
    assert sim.debt_usd <= Decimal("0.5")
    assert sim.collateral_usd <= Decimal("0.5")


@pytest.mark.parametrize("protocol", _PROTOCOLS)
def test_wallet_at_exact_debt_parity_uses_staircase_not_dust_prone_repay_full(protocol: str) -> None:
    # Wallet EXACTLY equal to snapshot debt is within the wallet-first buffer:
    # interest accruing between the snapshot read and execution would push debt
    # above the wallet, so a bare repay_full would leave dust and revert the final
    # withdraw-all. The planner instead routes through the staircase (partial repay
    # + withdraw->swap->repay), sourcing the shortfall from collateral. (VIB-4466 /
    # CodeRabbit: the wallet-first repay_full must require buffered coverage.)
    sim = _LendingSim(protocol, "10000", "5000", wallet_borrow="5000")
    intents = _run(sim)
    repays = sum(1 for i in intents if type(i).__name__ == "RepayIntent")
    assert repays >= 2, "exact-parity wallet must route through the buffered staircase, not a bare repay_full"
    assert sim.debt_usd <= Decimal("0.5")
    assert sim.collateral_usd <= Decimal("0.5")


@pytest.mark.parametrize("protocol", _PROTOCOLS)
def test_partial_wallet_debt_token_uses_partial_repay_not_full(protocol: str) -> None:
    # Wallet holds SOME debt token but less than the full debt. The wallet-first
    # repay must be a PARTIAL repay -- a repay_full here pulls the entire debt and
    # reverts on Morpho/Compound (the strict sim raises if the helper regresses).
    sim = _LendingSim(protocol, "10000", "6000", wallet_borrow="2000")
    _run(sim)  # raises if any repay pulls more than the wallet holds
    assert sim.debt_usd <= Decimal("0.5"), f"debt not cleared: {sim.debt_usd}"
    assert sim.collateral_usd <= Decimal("0.5")


@pytest.mark.parametrize("protocol", _PROTOCOLS)
def test_no_debt_just_withdraws_and_consolidates(protocol: str) -> None:
    sim = _LendingSim(protocol, "10000", "0")
    intents = _run(sim)
    assert [type(i).__name__ for i in intents] == ["WithdrawIntent", "SwapIntent"]
    assert sim.collateral_usd <= Decimal("0.5")


@pytest.mark.parametrize("protocol", _PROTOCOLS)
def test_swap_protocol_routes_every_swap(protocol: str) -> None:
    # Exotic collateral (e.g. sUSDe) has no default-router pool against the debt
    # token, so the teardown must route EVERY collateral->debt swap through the
    # given aggregator. A multi-round (HF-constrained) position exercises both the
    # per-round swaps and the final consolidating swap_all.
    sim = _LendingSim(protocol, "10000", "7500")  # HF 1.10 -> multi-round staircase
    intents = generate_leverage_loop_teardown(
        market=sim,
        protocol=protocol,
        collateral_token="WETH",
        borrow_token="USDC",
        market_id=_MARKET_ID.get(protocol),
        chain="arbitrum",
        swap_protocol="enso",
    )
    swaps = [i for i in intents if type(i).__name__ == "SwapIntent"]
    assert len(swaps) >= 2, f"multi-round position should emit several swaps, got {len(swaps)}"
    assert all(s.protocol == "enso" for s in swaps), "every swap must route through swap_protocol"
    # The routed sequence must still be executable end-to-end, not just carry the
    # right field: run it through the sim and assert the position fully unwinds.
    for intent in intents:
        sim.apply(intent)  # raises on any LLTV / repay-shortfall revert
    assert sim.debt_usd <= Decimal("0.5"), f"debt not cleared: {sim.debt_usd}"
    assert sim.collateral_usd <= Decimal("0.5"), f"collateral not withdrawn: {sim.collateral_usd}"


@pytest.mark.parametrize("protocol", _PROTOCOLS)
def test_swap_protocol_defaults_to_router(protocol: str) -> None:
    # Without swap_protocol the swaps carry no protocol override (default router).
    sim = _LendingSim(protocol, "10000", "5000")
    intents = _run(sim)
    swaps = [i for i in intents if type(i).__name__ == "SwapIntent"]
    assert swaps and all(s.protocol is None for s in swaps)


@pytest.mark.parametrize("protocol", _PROTOCOLS)
def test_unhealthy_position_fails_loud(protocol: str) -> None:
    # HF 0.87 < hf_floor: no collateral can be withdrawn safely.
    sim = _LendingSim(protocol, "10000", "9500")
    with pytest.raises(LeverageUnwindError):
        generate_leverage_loop_teardown(
            market=sim,
            protocol=protocol,
            collateral_token="WETH",
            borrow_token="USDC",
            market_id=_MARKET_ID.get(protocol),
            chain="arbitrum",
        )


# ---------------------------------------------------------------------------
# consolidate_to — residual-sweep target (AccountingStrats.md D1)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("protocol", _PROTOCOLS)
def test_consolidate_to_collateral_sweeps_borrow_residue(protocol: str) -> None:
    """consolidate_to=collateral flips the final sweep: residual borrow token is
    swapped back into the collateral/base asset, the recovered collateral stack
    is NOT converted into the debt token, and the unwind still completes."""
    sim = _LendingSim(protocol, "10000", "6000")
    intents = _run(sim, consolidate_to="WETH")

    assert sim.debt_usd == 0, "debt must be fully repaid"
    assert sim.collateral_usd == 0, "collateral must be fully withdrawn"

    last = intents[-1]
    assert type(last).__name__ == "SwapIntent"
    assert last.from_token == "USDC" and last.to_token == "WETH"
    assert last.amount == "all"
    # Every residual borrow-token buffer leftover landed back in collateral.
    assert sim.wallet_borrow == 0
    assert sim.wallet_collateral > 0


def test_consolidate_to_borrow_token_matches_default() -> None:
    """Passing the borrow token explicitly is identical to the default sweep."""
    default_intents = _run(_LendingSim("aave_v3", "10000", "6000"))
    explicit_intents = _run(_LendingSim("aave_v3", "10000", "6000"), consolidate_to="USDC")

    assert [type(i).__name__ for i in default_intents] == [type(i).__name__ for i in explicit_intents]
    assert default_intents[-1].from_token == explicit_intents[-1].from_token == "WETH"
    assert default_intents[-1].to_token == explicit_intents[-1].to_token == "USDC"


def test_consolidate_to_invalid_token_raises() -> None:
    sim = _LendingSim("aave_v3", "10000", "6000")
    with pytest.raises(ValueError, match="consolidate_to"):
        generate_leverage_loop_teardown(
            market=sim,
            protocol="aave_v3",
            collateral_token="WETH",
            borrow_token="USDC",
            consolidate_to="DAI",
        )


def test_no_debt_consolidate_to_collateral_sweeps_only_held_borrow() -> None:
    """Debt-free position with target=collateral: withdraw-all lands in the
    target already, so a sweep is emitted only when the wallet actually holds
    stray borrow token."""
    sim = _LendingSim("aave_v3", "10000", "0", wallet_borrow="25")
    intents = _run(sim, consolidate_to="WETH")
    assert [type(i).__name__ for i in intents] == ["WithdrawIntent", "SwapIntent"]
    assert intents[1].from_token == "USDC" and intents[1].to_token == "WETH"
    assert sim.wallet_borrow == 0

    sim_clean = _LendingSim("aave_v3", "10000", "0")
    intents_clean = _run(sim_clean, consolidate_to="WETH")
    assert [type(i).__name__ for i in intents_clean] == ["WithdrawIntent"]
