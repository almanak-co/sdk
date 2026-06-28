"""Oracle-vs-pool swap-execution divergence guard (VIB-5439 / Curve audit P0-8).

An AMM swap's min-out floor is canonically ``pool_quote × (1 − slippage)`` — a
percentage off the pool's *own* ``get_dy`` / ``balances()`` read. That floor is
**pool-self-referential**: if the pool is **already displaced** (a coin that has
lost its peg, a persistent imbalance, a pool moved in an earlier block), the
``get_dy`` already reflects the bad price, so the floor is a percentage off an
*already-bad* number and the swapper accepts a "within tolerance" bad fill
(audit P0-8). The fix is to cross-check the pool's quote against an INDEPENDENT
price oracle and fail/flag a displaced pool *before* the tx is built, rather than
trusting a number the (already-moved) pool reports.

**Scope (what this does and does not close).** This is a *compile-time detection*
guard over the ``get_dy`` snapshot taken while building the bundle. It closes the
**already-displaced-pool** subset of P0-8 — stale depeg, persistent imbalance, a
prior-block move. It does NOT by itself stop the *atomic same-block sandwich*,
where the attacker brackets the victim's pending tx in one block: at quote time
the pool is clean, the guard passes, and the on-chain floor is still the
pool-derived ``min_amount_out``. Anchoring the *executed* floor to the oracle
(``min_out = max(pool_floor, oracle_fair × (1 − tol))``) is the complementary
hardening tracked as a follow-up — kept separate because clamping the executed
floor carries on-chain-revert risk that needs its own per-pool-type validation.

This module is the **pool-agnostic, protocol-agnostic** core of that check: a
pure value function over ``(amount_in, pool_quoted_out, oracle_ratio)``. It holds
no Curve / AMM knowledge, opens no sockets, and reads no oracle itself — the
caller supplies the pool quote (from the connector's own ``get_dy``) and the
oracle ratio (sourced gateway-side through the framework price path, never by
this module). Any exact-input swap on any venue — Curve, Uniswap, Balancer —
calls the same :func:`check_swap_oracle_divergence`. It is the swap-execution
sibling of :func:`almanak.framework.valuation.peg_divergence.check_peg_divergence`
(which guards LP *marking*); this one guards swap *execution*.

The signal is a one-sided **shortfall**: how far the pool's quoted output sits
*below* the oracle-fair output ``amount_in × (price_in / price_out)``.

* A pool moved against the swap direction (a front-run that pushed ``token_out``
  up, or a ``token_out`` that has depegged so the pool over-prices it) quotes
  fewer ``token_out`` than oracle-fair → a positive shortfall → the guard fires.
* A pool that quotes *more* than oracle-fair (a fill in the swapper's favour —
  e.g. dumping a depegged ``token_in`` into the pool at par) is never blocked:
  shortfall is clamped at zero. The guard protects the swapper from bad fills; it
  does not second-guess good ones.
* Normal fee + price-impact (a few bps on a deep stable pool) sits far below the
  threshold, so healthy swaps pass untouched.

Empty ≠ Zero: a missing / non-positive oracle ratio (no price feed) is
*unmeasured*, reported distinctly from a real divergence so an oracle outage is
never mis-blamed as a sandwich. On unmeasured, the guard **degrades open by
default** (``ok=True``) — preserving today's availability, since a swap with no
oracle is no worse off than before this guard existed — while an operator who
would rather not trade blind can flip ``strict_when_unmeasured`` to fail closed.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

# A stable-pool quote more than ~1.5% below oracle-fair is a displaced-pool
# signal, not routine cost: a deep stable pool's fee + impact for a sane trade
# size is a few bps to low tens of bps, while the dislocations that matter (a
# depeg like USDC SVB ≈ 1200 bps) sit far past 1.5%. The default leaves headroom
# over legitimate fee + impact so a healthy swap never false-fires (itself a
# money-path failure — a stranded risk-reducing swap), and is overridable
# per-intent by the caller. It is a SEPARATE knob from the swap's own
# ``max_slippage``: slippage buffers the floor below the pool quote, this caps how
# far that quote may itself sit below the oracle.
DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS = 150

# NOTE: this execution-rate-vs-oracle check is applied by the Curve connector to
# StableSwap pools only. On CryptoSwap/Tricrypto pools the get_dy vs oracle-mid
# gap legitimately includes genuine, size-scaling price impact (unbounded), so no
# fixed threshold separates a bad fill from a large-but-fair one — the connector
# skips volatile pools here and protects them via the slippage floor + a future
# impact-immune spot-price-vs-oracle guard.

_BPS = Decimal(10_000)


@dataclass(frozen=True)
class SwapOracleGuard:
    """Result of an oracle-vs-pool swap-execution cross-check.

    ``ok`` is the only field a caller must branch on; the rest are diagnostics to
    log / stamp on the rejection so an operator can see WHY a swap was blocked and
    by how much.
    """

    ok: bool
    # How far the pool quote sits below oracle-fair, in basis points. Clamped at
    # zero when the pool quotes at or above oracle-fair (a good fill).
    shortfall_bps: int
    # Oracle-fair output in token_out human units (``amount_in × price_ratio``) —
    # ``None`` only when unmeasured (no oracle ratio). NOT a fabricated zero.
    oracle_fair_out: Decimal | None
    # The pool's own quoted output in token_out human units, echoed for the log.
    pool_quoted_out: Decimal
    # ``None`` when ok; otherwise ``"pool_below_oracle"`` (a real moved-pool /
    # depeg-into divergence) or ``"oracle_unmeasured"`` (could not price —
    # Empty ≠ Zero, distinct cause).
    reason: str | None


def check_swap_oracle_divergence(
    *,
    amount_in: Decimal,
    pool_quoted_out: Decimal,
    price_ratio: Decimal | None,
    threshold_bps: int = DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS,
    strict_when_unmeasured: bool = False,
) -> SwapOracleGuard:
    """Cross-check a pool's swap quote against the oracle-fair output.

    Args:
        amount_in: input amount in token_in *human* units (not wei).
        pool_quoted_out: the pool's own quoted output in token_out *human* units
            (i.e. ``get_dy`` result scaled by token_out decimals) — the number the
            min-out floor is derived from.
        price_ratio: the INDEPENDENT oracle rate ``price_in / price_out`` (both in
            the same numeraire), so ``amount_in × price_ratio`` is the oracle-fair
            output. ``None`` (or non-positive) when the oracle could not price the
            pair — treated as unmeasured (Empty ≠ Zero).
        threshold_bps: shortfall (in basis points) above which the pool is ruled
            pre-moved. Caller resolves precedence (intent > pool > default).
        strict_when_unmeasured: when the oracle is unmeasured, fail closed
            (``ok=False``) instead of the default degrade-open. For desks that
            refuse to trade without an oracle reference.

    Returns:
        A :class:`SwapOracleGuard`. ``ok=False`` with ``reason="pool_below_oracle"``
        when the shortfall exceeds the threshold; ``ok=False`` with
        ``reason="oracle_unmeasured"`` only when ``strict_when_unmeasured`` and the
        oracle is unmeasured; ``ok=True`` otherwise.
    """
    # Empty ≠ Zero: no oracle ratio (or a degenerate input) is unmeasured, NOT a
    # zero-divergence pass. Distinct reason so an oracle outage is never reported
    # as a sandwich; degrade-open by default to preserve availability.
    if price_ratio is None or price_ratio <= 0 or amount_in <= 0:
        return SwapOracleGuard(
            ok=not strict_when_unmeasured,
            shortfall_bps=0,
            oracle_fair_out=None,
            pool_quoted_out=pool_quoted_out,
            reason="oracle_unmeasured",
        )

    oracle_fair_out = amount_in * price_ratio
    if oracle_fair_out <= 0:  # defensive — positive inputs give a positive product
        return SwapOracleGuard(
            ok=not strict_when_unmeasured,
            shortfall_bps=0,
            oracle_fair_out=None,
            pool_quoted_out=pool_quoted_out,
            reason="oracle_unmeasured",
        )

    # One-sided shortfall: only a pool quote BELOW oracle-fair is a risk. A quote
    # at or above oracle-fair is a good fill — clamp to zero, never block it.
    shortfall = (oracle_fair_out - pool_quoted_out) / oracle_fair_out
    shortfall_bps = int(shortfall * _BPS) if shortfall > 0 else 0
    if shortfall_bps > threshold_bps:
        return SwapOracleGuard(
            ok=False,
            shortfall_bps=shortfall_bps,
            oracle_fair_out=oracle_fair_out,
            pool_quoted_out=pool_quoted_out,
            reason="pool_below_oracle",
        )
    return SwapOracleGuard(
        ok=True,
        shortfall_bps=shortfall_bps,
        oracle_fair_out=oracle_fair_out,
        pool_quoted_out=pool_quoted_out,
        reason=None,
    )


__all__ = [
    "DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS",
    "SwapOracleGuard",
    "check_swap_oracle_divergence",
]
