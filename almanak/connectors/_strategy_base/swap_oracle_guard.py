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
hardening, now implemented as :func:`clamp_min_out_to_oracle` (VIB-5490) — kept
in a separate function because clamping the executed floor carries on-chain-
revert risk and so uses its own pool-type-aware tolerance and a cap at
``pool_quote × (1 − residual)`` that preserves a benign-drift buffer (so a
genuine >tolerance-impact swap still fills against a pool that drifted between
build and execution, while sandwich extraction is bounded to ``residual`` below
the clean quote).

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

# --- Executed-floor oracle anchor (VIB-5490) --------------------------------
# The detection guard above (VIB-5439) reads the pool's ``get_dy`` snapshot at
# *build* time and blocks an already-displaced pool. It does NOT bind the floor
# that lands ON-CHAIN: that floor is still ``pool_quote × (1 − slippage)`` — a
# percentage off the pool's *own* number. An atomic same-block sandwich passes
# the build-time check (pool is clean when quoted) and then extracts value up to
# the operator's ``max_slippage`` at execution, because the on-chain floor never
# references anything but the pool. :func:`clamp_min_out_to_oracle` closes that
# by raising the executed floor toward the INDEPENDENT oracle:
#
#     min_out = max(pool_floor, min(oracle_fair × (1 − tolerance),
#                                    pool_quote × (1 − residual)))
#
# The oracle floor is capped at ``pool_quote × (1 − residual)`` — a benign
# inter-block-drift buffer BELOW the clean quote, NOT the raw quote. Capping at
# the raw quote would pin ``min_out`` to the exact clean build-time output with
# zero buffer, so a genuine >tolerance-impact swap (whose quote already sits below
# oracle-fair) would revert on the drift that always occurs between quote and
# execution. The honest guarantee is NOT "revert-safe by construction": it is that
# atomic-sandwich extraction is bounded to ``residual`` below the clean quote,
# while a swap still fills as long as benign drift stays within ``residual`` — no
# matter how wide ``max_slippage`` is.

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

# Executed-floor clamp tolerances (VIB-5490), pool-type-aware because the clamp
# raises the on-chain floor and so carries revert risk that the compile-time
# detection guard does not:
#
# * STABLE pools: reuse the SAME tolerance as the detection shortfall threshold
#   (default 150 bps). On a stable pool fee + impact for a sane trade is a few
#   bps, and the detection guard has ALREADY ensured the pool quote sits within
#   the tolerance of oracle-fair (else the swap was blocked). The clamp bites when
#   the operator's ``max_slippage`` is wider than this tolerance — exactly the
#   loose-floor window a sandwich would exploit.
# * VOLATILE (CryptoSwap/Tricrypto) pools: genuine price impact is unbounded and
#   the detection guard is skipped, so a TIGHT oracle floor would false-revert a
#   large-but-fair swap (the 637 bps arb-tricrypto lesson, now as an on-chain
#   revert instead of a compile block). A WIDE default (500 bps) caps only
#   egregious extraction.
DEFAULT_STABLE_ORACLE_FLOOR_TOLERANCE_BPS = DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS  # 150
DEFAULT_VOLATILE_ORACLE_FLOOR_TOLERANCE_BPS = 500

# Benign inter-block-drift residual (VIB-5490). The clamp caps the oracle floor at
# ``pool_quote × (1 − residual)``, NOT at the raw quote, because a swap builds one
# block and executes the next: the pool always drifts a little between the two, so
# raising the floor to the exact clean quote leaves ZERO buffer and a genuine
# >tolerance-impact swap would revert on benign drift (Curve's ``assert dy >=
# min_dy``) — worse than the pre-anchor floor, which preserved the operator's
# slippage buffer. The residual re-introduces that buffer so benign drift fills
# while sandwich extraction is bounded to ``residual_bps`` below the clean quote.
#
# Calibrated on real mainnet forks (see tests/reports/…-realfork.md):
# * STABLE pools barely drift block-to-block, so a small residual suffices.
# * VOLATILE pools drift materially; the residual is the smallest value that
#   reliably fills a genuine >tolerance-impact tricrypto swap against a realistically
#   drifted pool while still bounding sandwich extraction.
#
# Self-scoping: because the final floor is ``max(pool_floor, capped_oracle_floor)``,
# the residual cap only TIGHTENS when the operator's slippage is wider than
# ``residual_bps`` (the sandwich-exploitable window) and is a no-op when the operator
# already asked for a tighter floor — it never loosens below the original pool floor.
DEFAULT_STABLE_ORACLE_FLOOR_RESIDUAL_BPS = 50
DEFAULT_VOLATILE_ORACLE_FLOOR_RESIDUAL_BPS = 200

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


@dataclass(frozen=True)
class OracleFloorClamp:
    """Result of anchoring an executed min-out floor to the oracle (VIB-5490).

    ``min_out_wei`` is the floor the caller writes into the on-chain calldata —
    ``max``'d against the incoming pool floor (so it never lowers the existing
    floor) and capped at ``pool_quote × (1 − residual)`` (so it preserves a
    benign inter-block-drift buffer below the clean quote). The property is NOT
    "never reverts against any pool" — it is: sandwich extraction is bounded to
    ``residual_bps`` below the clean quote, and a genuine >tolerance-impact swap
    still fills as long as benign drift stays within ``residual_bps``. The rest
    are diagnostics for the log / report.
    """

    # The final floor to use on-chain (wei). Never below ``pool_floor_wei``.
    min_out_wei: int
    # Whether the oracle anchor actually raised the floor above the pool floor.
    clamped: bool
    # The pool-self-referential floor passed in (``pool_quote × (1 − slippage)``).
    pool_floor_wei: int
    # The oracle-anchored floor (``oracle_fair × (1 − tol)``, capped at
    # ``pool_quote × (1 − residual)``), in wei — ``None`` when unmeasured (no
    # oracle / bad quote → degrade to pool floor).
    oracle_floor_wei: int | None
    # ``None`` when the oracle anchored the floor; otherwise why it did not:
    # ``"oracle_unmeasured"`` (no oracle / unusable quote — Empty ≠ Zero) or
    # ``"oracle_config_invalid"`` (a bps knob outside (0, 10_000] — fail-loud so a
    # fat-fingered ``oracle_guard_bps`` override does not silently disable the
    # anchor). Both degrade to the pool floor.
    reason: str | None


def clamp_min_out_to_oracle(
    *,
    pool_floor_wei: int,
    pool_quoted_out_wei: int,
    amount_in: Decimal,
    price_ratio: Decimal | None,
    token_out_decimals: int,
    tolerance_bps: int,
    residual_bps: int,
) -> OracleFloorClamp:
    """Raise an executed min-out floor toward the independent oracle (VIB-5490).

    Computes ``oracle_fair × (1 − tolerance)`` from the SAME independent oracle
    ratio the detection guard uses, converts it to token_out wei, caps it at
    ``pool_quote × (1 − residual)`` (NOT the raw quote — a residual buffer for
    benign inter-block drift, see below), and returns
    ``max(pool_floor, capped_oracle_floor)``.

    **Why the residual cap, not a raw-quote cap.** A swap is quoted one block and
    executes the next; a pool always drifts a little in between. When a swap's
    GENUINE price impact ≥ ``tolerance`` (unbounded on volatile pools, where the
    detection guard is skipped), ``oracle_fair × (1 − tol)`` sits ABOVE the pool
    quote, so a raw-quote cap would pin ``min_out`` to the exact clean quote with
    ZERO slippage buffer — and benign drift then trips Curve's ``assert dy >=
    min_dy``, reverting a legit (often risk-reducing teardown) swap and burning
    gas. That is strictly worse than the pre-anchor floor, which kept the
    operator's slippage buffer. Capping at ``pool_quote × (1 − residual)`` keeps a
    benign-drift buffer while still bounding sandwich extraction to ``residual``
    below the clean quote.

    **Self-scoping.** Because the result is ``max(pool_floor, capped_oracle_floor)``
    and ``pool_floor = pool_quote × (1 − slippage)``, the residual cap only
    TIGHTENS the floor when the operator's ``slippage`` is wider than ``residual``
    (the sandwich-exploitable window); when the operator already asked for a floor
    tighter than ``residual``, ``pool_floor`` dominates and the clamp is a no-op.
    It never loosens below the original pool floor.

    Degrade-open (Empty ≠ Zero): a missing / non-positive oracle ratio, a
    non-positive amount, a non-positive tolerance, OR a non-positive pool quote
    leaves the pool floor untouched — a placeholder / unmeasured oracle (or an
    unusable quote) must NEVER fabricate a higher floor.

    Args:
        pool_floor_wei: the existing pool-self-referential floor
            (``pool_quote × (1 − slippage)``) in token_out wei.
        pool_quoted_out_wei: the pool's own quoted output in token_out wei. The
            oracle floor is capped at ``pool_quote × (1 − residual)`` off this, so
            a legit high-impact swap keeps a benign-drift buffer. Non-positive
            (unusable quote) degrades open.
        amount_in: input amount in token_in *human* units.
        price_ratio: the INDEPENDENT oracle rate ``price_in / price_out`` — the
            SAME reference the detection guard consumes. ``None`` / non-positive
            is unmeasured → degrade to the pool floor.
        token_out_decimals: decimals of token_out, to scale the oracle-fair human
            output into wei.
        tolerance_bps: how far below oracle-fair the executed floor may sit
            (pool-type-aware — stable tight, volatile wide). Comes from the
            unbounded per-intent ``oracle_guard_bps`` override; a value outside
            ``(0, 10_000]`` is a misconfig and degrades open with
            ``reason="oracle_config_invalid"`` (fail-loud) rather than silently
            disabling the anchor via a negative floor.
        residual_bps: benign inter-block-drift buffer preserved below the clean
            quote (pool-type-aware — stable tight, volatile wider). A value
            outside ``(0, 10_000]`` degrades open with
            ``reason="oracle_config_invalid"`` rather than a zero-buffer raw-quote
            cap.

    Returns:
        An :class:`OracleFloorClamp`. ``min_out_wei`` is never below
        ``pool_floor_wei``.
    """
    # Empty ≠ Zero: no usable oracle / inputs / quote → never fabricate a higher
    # floor. A non-positive quote is included: without a real quote to bound it,
    # an uncapped oracle floor could exceed any real fill → guaranteed revert.
    if price_ratio is None or price_ratio <= 0 or amount_in <= 0 or pool_quoted_out_wei <= 0:
        return OracleFloorClamp(
            min_out_wei=pool_floor_wei,
            clamped=False,
            pool_floor_wei=pool_floor_wei,
            oracle_floor_wei=None,
            reason="oracle_unmeasured",
        )

    # A bps knob outside (0, 10_000] is a MISCONFIG, distinct from "no oracle".
    # ``tolerance_bps`` comes from the unbounded per-intent ``oracle_guard_bps``
    # override; ``residual_bps`` is a fixed constant (defensive). Left unchecked,
    # ``_BPS - bps`` goes negative → ``oracle_floor_wei`` negative → the outer
    # ``max(pool_floor, …)`` silently falls back to the pool floor, i.e. the
    # security anchor SILENTLY DISABLES on a fat-fingered wide override. Fail loud
    # (distinct reason so the caller can WARN) instead of clamping quietly.
    _BPS_INT = int(_BPS)
    if not (0 < tolerance_bps <= _BPS_INT) or not (0 < residual_bps <= _BPS_INT):
        return OracleFloorClamp(
            min_out_wei=pool_floor_wei,
            clamped=False,
            pool_floor_wei=pool_floor_wei,
            oracle_floor_wei=None,
            reason="oracle_config_invalid",
        )

    oracle_fair_out = amount_in * price_ratio  # token_out human units
    if oracle_fair_out <= 0:  # defensive — positive inputs give a positive product
        return OracleFloorClamp(
            min_out_wei=pool_floor_wei,
            clamped=False,
            pool_floor_wei=pool_floor_wei,
            oracle_floor_wei=None,
            reason="oracle_unmeasured",
        )

    # tolerance_bps and residual_bps are guaranteed in (0, 10_000] above, so both
    # ``_BPS_INT - bps`` factors are in [0, _BPS_INT) — never negative.
    # oracle_fair × (1 − tol), in wei.
    oracle_fair_wei = int(oracle_fair_out * Decimal(10**token_out_decimals))
    oracle_floor_wei = oracle_fair_wei * (_BPS_INT - tolerance_bps) // _BPS_INT

    # Cap at pool_quote × (1 − residual), NOT the raw quote: raising the floor to
    # the exact clean quote leaves zero buffer for the benign block-to-block drift
    # that always occurs between quote and execution, so a genuine >tolerance-impact
    # swap would revert on that drift. The residual preserves a drift buffer while
    # still bounding sandwich extraction to ``residual`` below the clean quote.
    quote_cap_wei = pool_quoted_out_wei * (_BPS_INT - residual_bps) // _BPS_INT
    oracle_floor_wei = min(oracle_floor_wei, quote_cap_wei)

    min_out_wei = max(pool_floor_wei, oracle_floor_wei)
    return OracleFloorClamp(
        min_out_wei=min_out_wei,
        clamped=min_out_wei > pool_floor_wei,
        pool_floor_wei=pool_floor_wei,
        oracle_floor_wei=oracle_floor_wei,
        reason=None,
    )


__all__ = [
    "DEFAULT_STABLE_ORACLE_FLOOR_RESIDUAL_BPS",
    "DEFAULT_STABLE_ORACLE_FLOOR_TOLERANCE_BPS",
    "DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS",
    "DEFAULT_VOLATILE_ORACLE_FLOOR_RESIDUAL_BPS",
    "DEFAULT_VOLATILE_ORACLE_FLOOR_TOLERANCE_BPS",
    "OracleFloorClamp",
    "SwapOracleGuard",
    "check_swap_oracle_divergence",
    "clamp_min_out_to_oracle",
]
