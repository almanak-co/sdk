"""First-class, strategy-callable HF-safe lending unwind primitive (VIB-5467 / TD-09).

> **Knowing how to tear down a lending position = calling this primitive, not
> re-deriving health-factor math in every strategy.**

A plain borrow (collateral + a single debt) and a leverage loop (the borrow swapped
back into collateral and re-supplied N times) share ONE correct unwind shape, and one
notorious failure mode:

* The naive ``REPAY(all) → WITHDRAW(all)`` cannot fully unwind a borrow whose wallet
  does not hold enough debt token. A plain borrow holds only the borrowed *principal*
  while it owes principal **+ accrued interest**; a leverage loop holds ~no debt token
  at all. Repaying the wallet balance leaves *dust debt*, and ``withdraw_all``
  (``MAX_UINT256``) then reverts ``HealthFactorLowerThanLiquidationThreshold``
  (``0x6679996d``) because no collateral can be withdrawn while ANY debt remains. That
  reverting withdraw strands the collateral — the 8-month dust-debt deadlock
  (VIB-589 / VIB-2288 / VIB-2355 / VIB-2650 / ALM-2875 / VIB-5448).

:func:`generate_lending_unwind` eliminates that deadlock by sizing **every leg from the
LIVE on-chain position** rather than a strategy's cached snapshot, then emitting a
*staircase* of ``WITHDRAW → SWAP → REPAY`` rounds that drives debt to a true zero
BEFORE the final ``withdraw_all``:

* **Debt** is read live from the protocol's ``variableDebt`` balance (via
  ``MarketSnapshot.position_health`` → gateway), so interest accrued during HOLD is
  repaid too — never the stale "amount I borrowed".
* **Collateral** is read live from the aToken / supply ``balanceOf`` (same
  ``position_health`` read), so the staircase sizes withdraw slices against what is
  actually on-chain.
* The **wallet** debt-token ``balanceOf`` is read live and repaid first (free
  health-factor relief, no withdraw/swap gas), with a small **repay buffer**
  (``_REPAY_SAFETY_HAIRCUT`` / ``_WALLET_FIRST_BUFFER`` / ``_SETTLE_BUFFER``) so a
  near-parity wallet whose debt accrues interest between the read and execution sources
  the shortfall from collateral instead of leaving revert-inducing dust.

Each round withdraws only as much collateral as keeps the post-withdraw health factor
above a floor, swaps it to the debt token, and repays. As debt shrinks, the next
round's safe withdraw grows, so the position unwinds in a small bounded number of rounds
before the final ``WITHDRAW(all)`` (now safe — debt is zero) + residual sweep. The whole
staircase is computed up front from the current health snapshot, so it executes as one
intent list.

If the position's health factor is already so low that no collateral can be withdrawn
safely, no withdraw-first sequence can unwind it (only an atomic flash-loan repay can)
and the helper raises :class:`LendingUnwindError` rather than emitting an intent that
would revert on-chain.

Strategy usage (the front door — see blueprint 14 §"Lending unwind primitive")::

    from almanak.framework.teardown import generate_lending_unwind

    def generate_teardown_intents(self, mode, market=None):
        return generate_lending_unwind(
            market=market or self.create_market_snapshot(),
            protocol="aave_v3",
            collateral_token="wstETH",
            borrow_token="USDC",
            chain=self.chain,
            mode=mode,
        )

This is a **pure builder**: it reads live state and returns a typed ``Intent`` list. It
never executes, signs, or commits — the returned intents flow through the normal
teardown dispatch funnel (``runner_helpers.commit`` pairing, VIB-3773 anti-bypass
guards). Live reads are gateway-routed via ``MarketSnapshot`` — no direct network.

Back-compat: ``leverage_loop.generate_leverage_loop_teardown`` is a thin alias of this
function (a leverage-loop unwind *is* a lending unwind); existing imports keep working.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents.base import BaseIntent
from almanak.framework.intents.vocabulary import Intent

if TYPE_CHECKING:  # pragma: no cover
    from almanak.framework.teardown.models import TeardownMode


class LendingUnwindError(RuntimeError):
    """Raised when a lending position cannot be unwound by withdraw-first.

    Signals that the health factor is too low for any safe collateral
    withdrawal — the caller should escalate (flash-loan deleverage or manual
    intervention) rather than emit a reverting teardown.
    """


# Debt below this USD value is treated as fully repaid.
_DUST_USD = Decimal("0.01")

# Extra margin below the swap's guaranteed min-out so an intermediate PARTIAL
# repay never asks for more loan token than the round's swap actually delivered.
_REPAY_SAFETY_HAIRCUT = Decimal("0.01")

# Extra collateral withdrawn on the debt-clearing round so the final full repay
# stays funded despite swap slippage and interest accrued during teardown.
_SETTLE_BUFFER = Decimal("0.02")

# The wallet must exceed the live (variableDebt) debt by this margin before we
# take the wallet-first ``repay_full`` shortcut. Below the margin we route through
# the staircase (partial repay + withdraw→swap→repay) so a near-parity wallet whose
# debt accrues interest between the read and execution sources the shortfall from
# collateral instead of leaving dust that reverts the final withdraw-all. Mirrors
# ``lending_unwind_guard._STRAND_PREDICATE_BUFFER`` (1%) so the guard's strand buffer
# is not silently undone here.
_WALLET_FIRST_BUFFER = Decimal("1.01")


def hf_safe_withdraw_slice_usd(
    *,
    collateral_usd: Decimal,
    debt_usd: Decimal,
    lltv: Decimal,
    hf_floor: Decimal,
) -> Decimal:
    """Largest collateral USD slice whose post-withdraw health factor stays >= ``hf_floor``.

    Solves ``(collateral - slice) * lltv / debt == hf_floor`` for ``slice``. A
    value <= 0 means no collateral can be withdrawn while keeping the position
    above the floor (the position needs a flash-loan deleverage). ``lltv`` must
    be positive (callers validate). Single source of truth for the safe-slice
    math: the staircase loop below and the lending-unwind guard's degrade path
    both call it so their sizing never drifts (VIB-4466).
    """
    return collateral_usd - (hf_floor * debt_usd / lltv)


def generate_lending_unwind(
    *,
    market: Any,
    protocol: str,
    collateral_token: str,
    borrow_token: str,
    market_id: str | None = None,
    chain: str | None = None,
    mode: TeardownMode | None = None,
    hf_floor: Decimal = Decimal("1.05"),
    hf_floor_hard: Decimal = Decimal("1.01"),
    max_slippage: Decimal = Decimal("0.005"),
    max_slippage_hard: Decimal = Decimal("0.03"),
    max_rounds: int = 12,
    swap_protocol: str | None = None,
    consolidate_to: str | None = None,
) -> list[BaseIntent]:
    """Build a health-factor-aware unwind for any lending borrow / leverage loop.

    Sizes every leg from the LIVE on-chain position — ``variableDebt`` and supply
    ``balanceOf`` via ``position_health`` (gateway-routed), plus the wallet's live
    debt-token ``balanceOf`` — never a cached strategy snapshot, so accrued interest
    is repaid and the final ``withdraw_all`` is only ever emitted once debt is a true
    zero (eliminating the ``0x6679996d`` dust-debt strand; VIB-5467 / VIB-589).

    Args:
        market: MarketSnapshot exposing ``position_health(...)``, ``price(...)`` and
            ``balance(...)`` (all gateway-routed).
        protocol: Lending protocol ("aave_v3", "spark", "morpho_blue", "compound_v3",
            "fluid_vault", ...).
        collateral_token / borrow_token: Position legs.
        market_id: Isolated-market id (required for morpho_blue, ignored for aave_v3).
        chain: Execution chain (defaults to the strategy's primary chain).
        mode: TeardownMode; HARD uses a lower HF floor and wider slippage so an
            emergency unwind makes progress even close to liquidation.
        hf_floor / hf_floor_hard: Minimum post-withdraw health factor per round.
        max_slippage / max_slippage_hard: Swap slippage cap per round.
        max_rounds: Backstop on the number of staircase rounds.
        swap_protocol: Optional aggregator to route every collateral->debt swap
            through (e.g. "enso"). Required for exotic collateral with no direct
            DEX pool against the debt token (e.g. sUSDe/USDC, which has no
            Uniswap V3 pool); left None the swap uses the default router.
        consolidate_to: Which leg token the teardown's residual sweep lands in.
            Defaults to ``borrow_token`` (the historical behavior: the final
            withdraw-all collateral is swapped into the debt token). Strategies
            whose accounting/base asset IS the collateral (e.g. a USDC-collateral
            loop that must finish in USDC) pass ``collateral_token``; the final
            sweep then converts residual *borrow*-token buffer leftovers back
            into collateral instead. Must be one of the two leg tokens.

    Returns:
        Ordered list of intents: N x (withdraw, swap, repay) then withdraw-all +
        residual swap. Empty when there is no position.

    Raises:
        LendingUnwindError: Health factor too low to withdraw any collateral
            safely (needs flash-loan deleverage).
        ValueError: Health or price data unavailable (cannot size the unwind), or
            ``consolidate_to`` is not one of the two leg tokens.
    """
    if consolidate_to is not None and consolidate_to not in (collateral_token, borrow_token):
        raise ValueError(
            f"consolidate_to {consolidate_to!r} must be one of the position legs "
            f"({collateral_token!r}, {borrow_token!r})"
        )
    sweep_to_collateral = consolidate_to == collateral_token and collateral_token != borrow_token

    is_hard = _is_hard_mode(mode)
    floor = hf_floor_hard if is_hard else hf_floor
    slippage = max_slippage_hard if is_hard else max_slippage
    _validate_unwind_bounds(floor=floor, slippage=slippage, max_rounds=max_rounds)

    # Prices are needed both to size the staircase and as overrides for
    # cross-asset Morpho markets (collateral != debt token), where
    # position_health cannot derive USD values on its own.
    collateral_price = _price(market, collateral_token)
    borrow_price = _price(market, borrow_token)

    health = market.position_health(
        protocol=protocol,
        market_id=market_id or "",
        collateral_price_usd=collateral_price if collateral_price > 0 else None,
        debt_price_usd=borrow_price if borrow_price > 0 else None,
    )
    collateral_usd = Decimal(str(health.collateral_value_usd))
    debt_usd = Decimal(str(health.debt_value_usd))
    lltv = Decimal(str(health.lltv))

    intents: list[BaseIntent] = []

    if debt_usd <= _DUST_USD:
        if collateral_usd > _DUST_USD:
            intents.append(_withdraw(protocol, collateral_token, withdraw_all=True, market_id=market_id, chain=chain))
            if sweep_to_collateral:
                # Withdraw-all already lands in the target asset; only stray
                # wallet borrow-token needs consolidating.
                if _wallet_balance(market, borrow_token, chain=chain) > 0:
                    intents.append(_swap_all(borrow_token, collateral_token, slippage, chain, swap_protocol))
            else:
                intents.append(_swap_all(collateral_token, borrow_token, slippage, chain, swap_protocol))
        return intents

    if lltv <= 0:
        raise ValueError(f"position_health returned non-positive LLTV ({lltv}); cannot size unwind")

    if collateral_price <= 0 or borrow_price <= 0:
        raise ValueError(
            f"Missing oracle price (collateral={collateral_price}, borrow={borrow_price}); cannot size unwind"
        )

    # Wallet-first: repay any debt token already sitting in the wallet before
    # withdrawing collateral. This reduces health-factor pressure for free (no
    # withdraw/swap gas) and shrinks what the staircase has to unwind.
    #
    # CRITICAL: only emit repay_full when the wallet is known to cover the whole
    # debt. ``repay_full`` sends MAX_UINT256 (Aave/Compound) or the position's
    # full borrow shares (Morpho), which makes Morpho and Compound pull the
    # ENTIRE outstanding debt from the wallet and revert on a shortfall. (Aave
    # caps at the wallet balance, but the helper must not rely on that.) When the
    # wallet covers only part of the debt, repay an explicit partial amount.
    wallet_borrow = _wallet_balance(market, borrow_token, chain=chain)
    remaining_debt_usd = debt_usd
    if wallet_borrow > 0:
        if wallet_borrow * borrow_price >= debt_usd * _WALLET_FIRST_BUFFER:
            intents.append(_repay_full(protocol, borrow_token, market_id=market_id, chain=chain))
            remaining_debt_usd = Decimal("0")
        else:
            repay_tokens = wallet_borrow * (Decimal("1") - _REPAY_SAFETY_HAIRCUT)
            intents.append(_repay(protocol, borrow_token, amount=repay_tokens, market_id=market_id, chain=chain))
            remaining_debt_usd -= repay_tokens * borrow_price

    # Staircase: each round withdraws collateral, swaps it to the debt token, and
    # repays. When the position is healthy enough that one round's swap can fund
    # the whole remaining debt, that round withdraws a buffered slice and repays
    # in full (the wallet then covers it). While the health factor still caps the
    # withdraw below what the full debt needs, the round repays ONLY what the swap
    # is guaranteed to deliver -- an explicit PARTIAL repay. Using repay_full on a
    # partial slice would pull the entire debt and revert on Morpho/Compound.
    remaining_collateral_usd = collateral_usd
    for _ in range(max_rounds):
        if remaining_debt_usd <= _DUST_USD:
            break

        # safe slice solves (collateral - slice) * lltv / debt == floor
        safe_slice_usd = hf_safe_withdraw_slice_usd(
            collateral_usd=remaining_collateral_usd,
            debt_usd=remaining_debt_usd,
            lltv=lltv,
            hf_floor=floor,
        )
        if safe_slice_usd <= _DUST_USD:
            raise LendingUnwindError(
                f"Cannot unwind {protocol} {collateral_token}/{borrow_token}: health factor too low "
                f"for a safe withdrawal (collateral=${remaining_collateral_usd:.2f}, "
                f"debt=${remaining_debt_usd:.2f}, lltv={lltv}, hf_floor={floor}). "
                "Needs flash-loan deleverage or manual intervention."
            )

        # Slice whose swap proceeds cover the WHOLE remaining debt, plus a buffer
        # so the final full repay stays funded despite slippage and interest.
        settle_slice_usd = (remaining_debt_usd / (Decimal("1") - slippage)) * (Decimal("1") + _SETTLE_BUFFER)

        if safe_slice_usd >= settle_slice_usd:
            # Healthy enough to clear the debt this round: buffered withdraw, swap,
            # then repay the full remaining debt (the wallet now covers it).
            slice_tokens = settle_slice_usd / collateral_price
            intents.append(_withdraw(protocol, collateral_token, amount=slice_tokens, market_id=market_id, chain=chain))
            intents.append(_swap(collateral_token, borrow_token, slice_tokens, slippage, chain, swap_protocol))
            intents.append(_repay_full(protocol, borrow_token, market_id=market_id, chain=chain))
            remaining_collateral_usd -= settle_slice_usd
            remaining_debt_usd = Decimal("0")
            break

        # HF-constrained round: withdraw the largest safe slice and repay only the
        # swap's guaranteed output (explicit partial amount, never repay_full).
        slice_tokens = safe_slice_usd / collateral_price
        repay_usd = safe_slice_usd * (Decimal("1") - slippage) * (Decimal("1") - _REPAY_SAFETY_HAIRCUT)
        repay_tokens = repay_usd / borrow_price
        intents.append(_withdraw(protocol, collateral_token, amount=slice_tokens, market_id=market_id, chain=chain))
        intents.append(_swap(collateral_token, borrow_token, slice_tokens, slippage, chain, swap_protocol))
        intents.append(_repay(protocol, borrow_token, amount=repay_tokens, market_id=market_id, chain=chain))

        remaining_debt_usd -= repay_usd
        remaining_collateral_usd -= safe_slice_usd
    else:
        raise LendingUnwindError(
            f"Could not unwind {protocol} {collateral_token}/{borrow_token} within {max_rounds} rounds "
            f"(remaining debt ~${remaining_debt_usd:.2f}). Increase max_rounds or use flash-loan deleverage."
        )

    intents.append(_withdraw(protocol, collateral_token, withdraw_all=True, market_id=market_id, chain=chain))
    if sweep_to_collateral:
        # The staircase's buffers (settle + slippage) intentionally over-fund the
        # final repay, so the wallet ends holding residual borrow token; sweep it
        # back into the collateral/base asset rather than converting the whole
        # recovered collateral stack into the debt token.
        intents.append(_swap_all(borrow_token, collateral_token, slippage, chain, swap_protocol))
    else:
        intents.append(_swap_all(collateral_token, borrow_token, slippage, chain, swap_protocol))
    return intents


def _validate_unwind_bounds(*, floor: Decimal, slippage: Decimal, max_rounds: int) -> None:
    """Reject sizing inputs that would crash or emit unsafe amounts.

    The staircase divides by ``1 - slippage`` (settle slice) and treats slippage
    as a guaranteed-output haircut, and solves the safe slice against ``floor``;
    a non-positive floor, slippage outside ``[0, 1)``, or a non-positive round
    backstop would crash or emit negative / unbounded amounts.
    """
    if floor <= 0:
        raise ValueError(f"hf_floor must be positive (got {floor})")
    if not (Decimal("0") <= slippage < Decimal("1")):
        raise ValueError(f"max_slippage must be in [0, 1) (got {slippage})")
    if max_rounds <= 0:
        raise ValueError(f"max_rounds must be positive (got {max_rounds})")


def _price(market: Any, token: str) -> Decimal:
    """Oracle price of ``token``; 0 if unavailable."""
    try:
        p = market.price(token)
        return Decimal(str(p)) if p else Decimal("0")
    except Exception:
        return Decimal("0")


def _wallet_balance(market: Any, token: str, *, chain: str | None = None) -> Decimal:
    """Token amount of ``token`` already in the wallet on ``chain``; 0 if unavailable.

    Scoped to the execution ``chain`` (keyword-only) so a multi-chain snapshot
    sizes the wallet-first repay from the balance on the chain the unwind
    actually executes on, not a same-symbol balance on another chain.
    """
    try:
        bal = market.balance(token, chain=chain) if chain else market.balance(token)
        amount = getattr(bal, "balance", bal)
        return Decimal(str(amount))
    except Exception:
        return Decimal("0")


def _is_hard_mode(mode: TeardownMode | None) -> bool:
    if mode is None:
        return False
    return getattr(mode, "name", "").upper() == "HARD" or str(mode).upper().endswith("HARD")


def _withdraw(
    protocol: str,
    token: str,
    *,
    amount: Decimal | None = None,
    withdraw_all: bool = False,
    market_id: str | None,
    chain: str | None,
) -> BaseIntent:
    kwargs: dict[str, Any] = {
        "protocol": protocol,
        "token": token,
        "amount": amount if amount is not None else Decimal("0"),
        "withdraw_all": withdraw_all,
    }
    if market_id:
        kwargs["market_id"] = market_id
    if chain:
        kwargs["chain"] = chain
    return Intent.withdraw(**kwargs)


def _repay_full(protocol: str, token: str, *, market_id: str | None, chain: str | None) -> BaseIntent:
    kwargs: dict[str, Any] = {"protocol": protocol, "token": token, "repay_full": True}
    if market_id:
        kwargs["market_id"] = market_id
    if chain:
        kwargs["chain"] = chain
    return Intent.repay(**kwargs)


def _repay(protocol: str, token: str, *, amount: Decimal, market_id: str | None, chain: str | None) -> BaseIntent:
    kwargs: dict[str, Any] = {"protocol": protocol, "token": token, "amount": amount, "repay_full": False}
    if market_id:
        kwargs["market_id"] = market_id
    if chain:
        kwargs["chain"] = chain
    return Intent.repay(**kwargs)


def _swap(
    from_token: str,
    to_token: str,
    amount: Decimal,
    slippage: Decimal,
    chain: str | None,
    protocol: str | None = None,
) -> BaseIntent:
    return Intent.swap(
        from_token=from_token,
        to_token=to_token,
        amount=amount,
        max_slippage=slippage,
        protocol=protocol,
        chain=chain,
    )


def _swap_all(
    from_token: str,
    to_token: str,
    slippage: Decimal,
    chain: str | None,
    protocol: str | None = None,
) -> BaseIntent:
    return Intent.swap(
        from_token=from_token,
        to_token=to_token,
        amount="all",
        max_slippage=slippage,
        protocol=protocol,
        chain=chain,
    )
