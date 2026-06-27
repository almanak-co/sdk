"""Unit tests for the first-class lending unwind primitive (VIB-5467 / TD-09).

The HF-safe ``WITHDRAW -> SWAP -> REPAY`` staircase math is exercised exhaustively
in ``test_leverage_loop_unwind.py`` (which now drives it through the back-compat
alias). This module covers the *promotion* contract specific to TD-09:

* the canonical name ``generate_lending_unwind`` is importable from the package and
  the module, and is the SAME object as the historical
  ``leverage_loop.generate_leverage_loop_teardown`` alias;
* ``LendingUnwindError`` / ``LeverageUnwindError`` are the same class;
* a **plain cross-asset borrow** (collateral != borrow, wallet holds only the
  borrowed principal while it owes principal + accrued interest) unwinds to ZERO
  residual debt with NO ``withdraw_all`` issued while debt remains — i.e. the
  ``0x6679996d`` dust-debt strand (VIB-589 / ALM-2875 / VIB-5448) cannot happen.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.teardown import (
    LendingUnwindError,
    generate_lending_unwind,
    hf_safe_withdraw_slice_usd,
)

_EPS = Decimal("1e-9")
_DUST = Decimal("0.01")


def test_canonical_name_is_package_exported() -> None:
    """``generate_lending_unwind`` is reachable from the package front door."""
    import almanak.framework.teardown as teardown_pkg

    assert teardown_pkg.generate_lending_unwind is generate_lending_unwind
    assert teardown_pkg.hf_safe_withdraw_slice_usd is hf_safe_withdraw_slice_usd
    assert teardown_pkg.LendingUnwindError is LendingUnwindError


def test_back_compat_aliases_are_identical_objects() -> None:
    """Existing ``leverage_loop`` imports resolve to the SAME objects (no fork)."""
    from almanak.framework.teardown import lending_unwind, leverage_loop

    assert leverage_loop.generate_leverage_loop_teardown is generate_lending_unwind
    assert leverage_loop.generate_leverage_loop_teardown is lending_unwind.generate_lending_unwind
    assert leverage_loop.LeverageUnwindError is LendingUnwindError
    assert leverage_loop.hf_safe_withdraw_slice_usd is hf_safe_withdraw_slice_usd


# ---------------------------------------------------------------------------
# Price-aware cross-asset plain-borrow simulation
# ---------------------------------------------------------------------------


class _FakeHealth:
    def __init__(self, collateral_usd: Decimal, debt_usd: Decimal, lltv: Decimal) -> None:
        self.collateral_value_usd = collateral_usd
        self.debt_value_usd = debt_usd
        self.lltv = lltv


class _Bal:
    def __init__(self, amount: Decimal) -> None:
        self.balance = amount


class _PlainBorrowSim:
    """A plain borrow: ``collateral_token`` (priced != $1) backing a ``borrow_token``.

    Models a wallet that holds the borrowed principal while the live debt is
    principal + accrued interest (so ``wallet < debt``). ``apply`` mutates state
    and raises on exactly the on-chain reverts the staircase must avoid: an
    LLTV-breaching withdraw (the ``0x6679996d`` strand) or a repay that pulls more
    debt token than the wallet holds.

    Prices are real (collateral != $1), so the test catches USD<->token unit bugs
    that a "1 token == $1" sim cannot.
    """

    LLTV = Decimal("0.80")

    def __init__(
        self,
        *,
        collateral_token: str,
        borrow_token: str,
        collateral_price: str,
        collateral_amount: str,
        debt_usd: str,
        wallet_borrow_tokens: str,
        slippage: str = "0.005",
    ) -> None:
        self.collateral_token = collateral_token
        self.borrow_token = borrow_token
        self.collateral_price = Decimal(collateral_price)
        self.collateral_tokens = Decimal(collateral_amount)
        self.debt_usd = Decimal(debt_usd)  # borrow priced at $1
        self.wallet_borrow = Decimal(wallet_borrow_tokens)
        self.wallet_collateral = Decimal("0")  # collateral tokens in the wallet
        self.realized_slippage = Decimal(slippage)
        # Capture the construction args so a replay check can rebuild a FRESH
        # copy from THIS sim's own scenario (not a shared module-global fixture).
        self._init_args = {
            "collateral_token": collateral_token,
            "borrow_token": borrow_token,
            "collateral_price": collateral_price,
            "collateral_amount": collateral_amount,
            "debt_usd": debt_usd,
            "wallet_borrow_tokens": wallet_borrow_tokens,
            "slippage": slippage,
        }

    # ---- MarketSnapshot surface ----
    def position_health(self, protocol: str, market_id: str, **kwargs: object) -> _FakeHealth:
        return _FakeHealth(self.collateral_tokens * self.collateral_price, self.debt_usd, self.LLTV)

    def price(self, token: str) -> Decimal:
        return self.collateral_price if token == self.collateral_token else Decimal("1")

    def balance(self, token: str, *, chain: str | None = None) -> _Bal:
        return _Bal(self.wallet_borrow if token == self.borrow_token else self.wallet_collateral)

    # ---- execution ----
    @property
    def collateral_usd(self) -> Decimal:
        return self.collateral_tokens * self.collateral_price

    def apply(self, intent: Any) -> None:
        kind = type(intent).__name__
        if kind == "WithdrawIntent":
            self._apply_withdraw(intent)
        elif kind == "SwapIntent":
            self._apply_swap(intent)
        elif kind == "RepayIntent":
            self._apply_repay(intent)

    def _apply_withdraw(self, intent: Any) -> None:
        if getattr(intent, "withdraw_all", False):
            w_tokens = self.collateral_tokens
        else:
            w_tokens = Decimal(str(intent.amount))
        w_usd = w_tokens * self.collateral_price
        if self.debt_usd > _DUST and (self.collateral_usd - w_usd) * self.LLTV < self.debt_usd - _EPS:
            raise AssertionError(
                f"0x6679996d: withdraw {w_tokens} {self.collateral_token} leaves HF "
                f"{((self.collateral_usd - w_usd) * self.LLTV / self.debt_usd):.3f} < 1.0 with debt remaining"
            )
        self.collateral_tokens -= w_tokens
        self.wallet_collateral += w_tokens

    def _apply_swap(self, intent: Any) -> None:
        if getattr(intent, "from_token", None) == self.collateral_token:
            amt = self.wallet_collateral if intent.amount == "all" else Decimal(str(intent.amount))
            assert amt <= self.wallet_collateral + _EPS, "swap exceeds wallet collateral"
            self.wallet_collateral -= amt
            self.wallet_borrow += amt * self.collateral_price * (Decimal("1") - self.realized_slippage)
        else:  # borrow -> collateral residual sweep
            amt = self.wallet_borrow if intent.amount == "all" else Decimal(str(intent.amount))
            assert amt <= self.wallet_borrow + _EPS, "swap exceeds wallet borrow"
            self.wallet_borrow -= amt
            self.wallet_collateral += (amt / self.collateral_price) * (Decimal("1") - self.realized_slippage)

    def _apply_repay(self, intent: Any) -> None:
        # Aave caps repay at the wallet balance; a partial repay over the wallet reverts.
        if getattr(intent, "repay_full", False):
            pay = min(self.wallet_borrow, self.debt_usd)
        else:
            amount = Decimal(str(intent.amount))
            pay = min(amount, self.debt_usd)
            if pay > self.wallet_borrow + _EPS:
                raise AssertionError(
                    f"partial repay pulls ${pay} but wallet holds only ${self.wallet_borrow} -> revert"
                )
        self.debt_usd -= pay
        self.wallet_borrow -= pay


def _drive(sim: _PlainBorrowSim, **kwargs: Any) -> list:
    intents = generate_lending_unwind(
        market=sim,
        protocol="aave_v3",
        collateral_token=sim.collateral_token,
        borrow_token=sim.borrow_token,
        chain="arbitrum",
        **kwargs,
    )
    for intent in intents:
        sim.apply(intent)  # raises on any LLTV / repay-shortfall revert
    return intents


def _no_withdraw_all_while_debt(sim: _PlainBorrowSim, intents: list) -> None:
    """Replay-checking that no ``withdraw_all`` is issued while debt > dust.

    Re-runs the plan against a fresh copy and asserts the invariant at each step
    (the single legitimate ``withdraw_all`` must land only after debt is zero).

    The fresh copy is rebuilt from the PASSED ``sim``'s own scenario, so a future
    caller that drives a different scenario validates that scenario — not a shared
    module-global fixture (which would silently produce a false-green replay).
    """
    fresh = _PlainBorrowSim(**sim._init_args)
    for intent in intents:
        if type(intent).__name__ == "WithdrawIntent" and getattr(intent, "withdraw_all", False):
            assert fresh.debt_usd <= _DUST, (
                f"withdraw_all issued while debt ${fresh.debt_usd} remains -> 0x6679996d strand"
            )
        fresh.apply(intent)


# wstETH-collateral / USDC-borrow plain borrow, wallet holds only the principal
# (< live debt by the accrued interest) — the VIB-4466 / ALM-2875 strand setup.
sim_args = {
    "collateral_token": "wstETH",
    "borrow_token": "USDC",
    "collateral_price": "4000",
    "collateral_amount": "0.1",  # $400 collateral
    "debt_usd": "200.5",  # principal $200 + $0.50 accrued interest
    "wallet_borrow_tokens": "200",  # wallet holds only the borrowed principal
}


def test_plain_cross_asset_borrow_unwinds_to_zero_debt_no_strand() -> None:
    sim = _PlainBorrowSim(**sim_args)
    intents = _drive(sim)

    # Debt fully cleared, collateral fully recovered — no dust-debt strand.
    assert sim.debt_usd <= _DUST, f"residual debt not cleared: ${sim.debt_usd}"
    assert sim.collateral_tokens <= Decimal("1e-6"), f"collateral not withdrawn: {sim.collateral_tokens}"

    # Exactly one withdraw_all, and it is the LAST collateral withdraw.
    withdraws = [i for i in intents if type(i).__name__ == "WithdrawIntent"]
    withdraw_all = [i for i in withdraws if getattr(i, "withdraw_all", False)]
    assert len(withdraw_all) == 1, f"expected exactly one withdraw_all, got {len(withdraw_all)}"
    assert getattr(withdraws[-1], "withdraw_all", False), (
        "the withdraw_all must be the LAST collateral withdraw (a partial withdraw after it would re-strand collateral)"
    )


def test_plain_cross_asset_borrow_no_withdraw_all_while_debt_remains() -> None:
    sim = _PlainBorrowSim(**sim_args)
    intents = _drive(sim)
    _no_withdraw_all_while_debt(sim, intents)


def test_default_consolidate_sweeps_recovered_collateral_into_borrow_asset() -> None:
    """Default ``consolidate_to`` ends the plain borrow in the borrow asset (USDC)."""
    sim = _PlainBorrowSim(**sim_args)
    intents = _drive(sim)
    final = intents[-1]
    assert type(final).__name__ == "SwapIntent"
    assert final.from_token == "wstETH" and final.to_token == "USDC"
    assert final.amount == "all"


def test_consolidate_to_collateral_leaves_collateral_for_token_consolidation() -> None:
    """``consolidate_to=collateral_token`` only sweeps the residual borrow buffer back."""
    sim = _PlainBorrowSim(**sim_args)
    intents = _drive(sim, consolidate_to="wstETH")
    final = intents[-1]
    assert type(final).__name__ == "SwapIntent"
    # Final sweep converts leftover USDC buffer back to wstETH, not the reverse.
    assert final.from_token == "USDC" and final.to_token == "wstETH"


def test_invalid_consolidate_to_rejected() -> None:
    sim = _PlainBorrowSim(**sim_args)
    with pytest.raises(ValueError, match="consolidate_to"):
        generate_lending_unwind(
            market=sim,
            protocol="aave_v3",
            collateral_token="wstETH",
            borrow_token="USDC",
            consolidate_to="DAI",
        )
