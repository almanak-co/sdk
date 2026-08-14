"""Shared concentrated-liquidity compiler math."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.slippage import compute_min_amount_out
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.min_out_guard import UnprotectedTradeError

logger = logging.getLogger(__name__)

_SLOT0_NOT_FETCHED = object()


def maybe_recompute_lp_amounts_from_slot0(
    *,
    fetch_slot0: Any,
    pool_check: Any,
    tick_lower: int,
    tick_upper: int,
    amount0_desired: int,
    amount1_desired: int,
    intent_id: str,
    slot0: tuple[int, int] | None | Any = _SLOT0_NOT_FETCHED,
) -> tuple[int, int] | CompilationResult:
    """Align desired LP amounts to the pool's live sqrt price when available."""
    from almanak.framework.intents.lp_math import recompute_lp_amounts

    if slot0 is _SLOT0_NOT_FETCHED:
        slot0 = fetch_slot0(pool_check)
    if slot0 is None:
        return amount0_desired, amount1_desired

    sqrt_price_x96, current_tick = slot0
    a0_corrected, a1_corrected = recompute_lp_amounts(
        sqrt_price_x96,
        tick_lower,
        tick_upper,
        amount0_desired,
        amount1_desired,
        current_tick=current_tick,
    )
    if a0_corrected == 0 and a1_corrected == 0 and (amount0_desired > 0 or amount1_desired > 0):
        from almanak.framework.intents.intent_errors import LpOpenZeroLiquidityError

        err = LpOpenZeroLiquidityError(
            amount0_desired=amount0_desired,
            amount1_desired=amount1_desired,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            reason=(
                "Live pool sqrt-price + supplied amounts produced zero "
                "liquidity. Widen the tick range or increase amounts."
            ),
        )
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=str(err),
            intent_id=intent_id,
        )
    if a0_corrected > 0 or a1_corrected > 0:
        logger.debug(
            "LP amounts recomputed from on-chain price: (%s, %s) -> (%s, %s)",
            amount0_desired,
            amount1_desired,
            a0_corrected,
            a1_corrected,
        )
        return a0_corrected, a1_corrected
    return amount0_desired, amount1_desired


def lp_range_excludes_spot_warning(
    *,
    tick_lower: int,
    tick_upper: int,
    slot0: tuple[int, int] | None,
    range_lower: Decimal | None = None,
    range_upper: Decimal | None = None,
    pool_address: str | None = None,
    protocol: str | None = None,
) -> str | None:
    """Warn (never block) when the live pool tick sits outside the requested range.

    VIB-exp19: LP range bounds are frequently computed from
    ``market.price()`` -- a USD *valuation* oracle (hardcoded ``1.0`` for
    stablecoins, ``source: stablecoin_peg``) -- instead of the pool's own
    price via ``market.pool_price()``. Oracle and pool price are not
    guaranteed to agree (e.g. a real USDC/USDT V3 pool sits at tick 4-7,
    never exactly the peg), so a range centered on the oracle can miss live
    spot entirely. The position then mints **single-sided** and earns
    **zero fees** with no error raised anywhere -- exactly the failure mode
    this check surfaces.

    Deliberately a WARNING, not a fail-closed refusal: one-sided /
    out-of-range LP_OPEN is an established, intentional pattern in this SDK
    (uniswap_v3 has never required a straddling band -- see
    ``_compute_lp_ticks`` -- and existing pinned characterization tests mint
    with an out-of-range slot0 tick and expect SUCCESS). Failing closed here
    would silently invalidate that supported use case. Callers that already
    enforce a hard straddle requirement (e.g. Aerodrome Slipstream's
    ``_slipstream_tick_straddle_failure``, ALM-2891) run this check
    afterward only to restore visibility for their own opt-out path
    (``allow_out_of_range=True``), which otherwise suppresses the failure
    with no warning left in its place.

    Returns the warning message (also logged) when ``slot0``'s current tick
    is outside ``[tick_lower, tick_upper)``, or ``None`` when it's inside,
    when ``slot0`` is unavailable (can't check what we can't read), or when
    ``tick_lower >= tick_upper`` (degenerate range already rejected
    upstream).
    """
    if slot0 is None:
        return None
    if tick_lower >= tick_upper:
        return None
    _sqrt_price_x96, current_tick = slot0
    if tick_lower <= current_tick < tick_upper:
        return None

    # Describe the CURRENT TICK's position relative to the requested range
    # (contrast Aerodrome's _slipstream_tick_straddle_failure, which describes
    # the range's position relative to the tick -- inverted phrasing, easy to
    # cross-wire when adapting the pattern).
    side = "below" if current_tick < tick_lower else "above"
    range_desc = f"[{range_lower}, {range_upper}]" if range_lower is not None and range_upper is not None else None
    protocol_desc = f"{protocol} " if protocol else ""
    pool_desc = f" pool {pool_address}" if pool_address else ""
    where = (
        f"requested range {range_desc} (ticks [{tick_lower}, {tick_upper}))"
        if range_desc
        else (f"requested tick range [{tick_lower}, {tick_upper})")
    )
    message = (
        f"LP_OPEN {where} does not contain the {protocol_desc}{pool_desc} current spot "
        f"(tick {current_tick}, {side} the range). This position will mint SINGLE-SIDED "
        f"and earn ZERO fees until price re-enters the range -- silently, with no error. "
        f"If the range bounds were derived from market.price() (a USD valuation oracle; "
        f"hardcoded 1.0 for stablecoins) rather than market.pool_price() (the pool's own "
        f"live price), that divergence is the likely cause."
    )
    logger.warning(message)
    return message


def compute_lp_slippage_mins(
    *,
    intent: Any,
    amount0_desired: int,
    amount1_desired: int,
    default_lp_slippage: Decimal,
    sqrt_price_x96: int | None = None,
    tick_lower: int | None = None,
    tick_upper: int | None = None,
) -> tuple[int, int]:
    """Compute LP minimum amounts from the effective LP slippage.

    The tolerance is read as a PRICE BAND and mapped into per-leg minimums by
    ``lp_mint_mins_for_price_band`` (VIB-6269) whenever ``sqrt_price_x96`` plus the
    tick range are supplied. Otherwise the legacy flat per-leg haircut is used, so
    a caller that cannot read slot0 keeps working.

    Why the price-band form is the correct instrument for a MINT: ``amount0Min``
    / ``amount1Min`` on ``mint`` constrain the deposit SPLIT, and the split is an
    amplified function of price (elasticity 5x-400x for ordinary ranges), so a
    flat haircut of ``s`` really tolerates only ``s / A`` of price movement. That
    made a 0.5% tolerance mean 0.024% on a live Arbitrum WETH/USDC position and
    revert on ordinary compile-to-submit staleness. See the decision doc
    ``docs/internal/plans/vib-6269-cl-lp-open-minimum-decision.md``.

    **ALM-3186 / VIB-6225 removed the provenance gate.** VIB-6269 additionally
    required the tolerance to have been DECLARED (``protocol_params["lp_slippage"]``
    or ``intent.max_slippage``) before it would build the band, because a tolerance
    that fell through to ``default_lp_slippage`` was then the ``0.99`` placeholder
    and "feeding a placeholder to a correct instrument yields a correct-but-
    meaningless answer". Its decision doc scoped that gate explicitly: the class
    "closes when VIB-6225 sets a real default and the fallback stops firing".
    ``LP_SLIPPAGE_DEFAULT`` is now ``0.01`` — a real price tolerance — so the gate
    is gone and an undeclared LP_OPEN gets the same band-aware treatment as a
    declared one. There is no longer a placeholder to guard against.

    **One trigger still selects the flat haircut: both legs zero.** A tolerance
    wide enough to reach BOTH range bounds yields ``(0, 0)`` -- truthful (nothing
    is guaranteed across such a band) but it would ship a mint with no on-chain
    floor, so it falls back. Note this is the path a caller who still configures a
    wide ``default_lp_slippage`` (e.g. 0.99) lands on for most ranges; on a WIDE
    range such a band crosses only the LOWER bound and one leg is floored at zero
    with a warning, which is the honest reading of a 99% tolerance rather than the
    1%-of-desired rubber stamp it used to produce.

    **A SINGLE zero leg is kept only for a DECLARED tolerance** (ALM-3186 P1).
    That is the legitimate band-aware case VIB-6269 (Defect B) measured and which
    its Q5 ``max(amount0_min, amount1_min)``-shaped reasoning endorses -- but the
    endorsement rests on the caller having *stated* the tolerance. A zeroed leg
    does not bound price to the advertised percentage: it removes the floor in one
    DIRECTION, so the mint stays acceptable after an arbitrarily large adverse
    move. When the tolerance was INHERITED from ``default_lp_slippage`` and the
    zeroed leg has a positive desired amount, this raises
    ``UnprotectedTradeError`` instead of warning.
    Callers may opt into ``intent.require_two_sided_minimums`` when their strategy
    contract instead requires both legs to stay protected. That check happens
    here, after the compiler has read live slot0 and realised the tick range, so
    oracle drift between strategy decision and compilation cannot bypass it.

    ``protocol_params["lp_slippage"]`` FAILS CLOSED (VIB-6217). It used to be
    clamped with ``min(max(x, 0), 1)``, which turned every out-of-range value into
    a legal-looking one: a fat-fingered ``5`` became exactly ``1``, and a slippage
    of 1 makes ``compute_min_amount_out`` return 0 for both tokens — the mint is
    then encoded with ``amount0Min = amount1Min = 0`` and will accept any amount of
    LP for the tokens deposited. Maximum harm from a typo, with no signal. A
    tolerance outside ``[0, 1)`` is now refused rather than silently repaired.

    Note this raises ``UnprotectedTradeError``, so connector compilers calling this
    should surface it as ``CompilationStatus.FAILED`` with
    ``is_safety_refusal=True`` rather than letting it escape as a crash.
    """
    protocol_lp_slippage = (intent.protocol_params or {}).get("lp_slippage")
    intent_max_slippage = getattr(intent, "max_slippage", None)
    if protocol_lp_slippage is not None:
        lp_slippage = Decimal(str(protocol_lp_slippage))
        if lp_slippage < Decimal("0") or lp_slippage >= Decimal("1"):
            raise UnprotectedTradeError(
                "LP mint (protocol_params.lp_slippage)",
                f"lp_slippage must be in [0, 1) (got {lp_slippage}); a tolerance of "
                f"1 or more sizes both minimum amounts at zero, which accepts any "
                f"mint outcome. Set an explicit tolerance such as 0.05.",
            )
    else:
        lp_slippage = intent_max_slippage if intent_max_slippage is not None else default_lp_slippage
    # ALM-3186 / VIB-6225: every tolerance reaching here is now a real PRICE
    # tolerance, whether declared on the intent or inherited from
    # `LP_SLIPPAGE_DEFAULT` (0.01), so the band CONSTRUCTION applies uniformly.
    # VIB-6269's provenance gate existed only to keep the 0.99 PLACEHOLDER out of
    # a correct instrument; with the placeholder gone the gate would just deny the
    # default path the protection it was added to provide. The `(0, 0)` fallback
    # below still catches a tolerance too wide to floor anything.
    #
    # Provenance still decides ONE thing (ALM-3186 P1): whether a SINGLE-zero band
    # may ship. See the refusal at the zero-leg guard below -- an inherited
    # tolerance may not, because a zeroed leg drops the floor in one direction
    # entirely and nobody chose that trade.
    tolerance_is_declared = protocol_lp_slippage is not None or intent_max_slippage is not None
    amount0_min = compute_min_amount_out(amount0_desired, lp_slippage)
    amount1_min = compute_min_amount_out(amount1_desired, lp_slippage)

    if sqrt_price_x96 is not None and sqrt_price_x96 > 0 and tick_lower is not None and tick_upper is not None:
        from almanak.framework.intents.lp_math import lp_mint_mins_for_price_band

        band0, band1 = lp_mint_mins_for_price_band(
            sqrt_price_x96,
            tick_lower,
            tick_upper,
            amount0_desired,
            amount1_desired,
            lp_slippage,
        )
        if getattr(intent, "require_two_sided_minimums", False) and (band0 <= 0 or band1 <= 0):
            raise UnprotectedTradeError(
                "LP mint (two-sided minimums)",
                f"the effective {lp_slippage:.3%} price tolerance reaches a live range bound "
                f"at ticks [{tick_lower}, {tick_upper}], producing amount0Min={band0} and "
                f"amount1Min={band1}. Narrow max_slippage or widen/recenter the LP range "
                "so both token minimums remain positive.",
            )
        if band0 > 0 or band1 > 0:
            # NEVER TIGHTER THAN THE LEGACY HAIRCUT. Analytically redundant --
            # the amplification A >= 1 always, so the band-aware minimum is
            # already <= the flat one -- but ``recompute_lp_amounts`` RETURNS ITS
            # INPUTS UNCHANGED on its bail-out paths (out-of-bounds ticks,
            # degenerate range, ZeroDivisionError). Without this clamp such a
            # bail-out would emit ``min == desired``: the tightest possible
            # floor, reverting on any drift at all, and strictly worse than what
            # this change replaces. The clamp makes "no mint that compiles today
            # gets a tighter floor tomorrow" a property of the code rather than
            # of an argument about the math.
            band0, band1 = min(band0, amount0_min), min(band1, amount1_min)
            if (band0 == 0) != (band1 == 0):
                # A ZERO MINIMUM IS SHIPPING. Truthful -- the band reaches that
                # leg's range bound, so nothing is guaranteed for it and any
                # positive floor would be a lie -- but `amount*Min = 0` is the
                # exact shape VIB-6220 / VIB-6226 / VIB-6235 file as defects, so
                # it must never appear silently.
                zero_leg = 0 if band0 == 0 else 1
                zero_leg_desired = amount0_desired if band0 == 0 else amount1_desired

                # ALM-3186 P1 -- A SINGLE-ZERO BAND DOES NOT BOUND PRICE TO THE
                # ADVERTISED TOLERANCE, so an INHERITED tolerance may not ship one.
                #
                # A zeroed leg is not merely "that leg is unprotected": it removes
                # the floor in one whole DIRECTION. Measured on ticks [-50, 1000]
                # compiled at tick 0, the 1% default emits (299650034208966015, 0);
                # as price falls the position converts to token0, so the surviving
                # token0 floor gets EASIER to satisfy while the zeroed token1 floor
                # can never bind. The mint still passes at tick -1000 (-9.5%) and at
                # tick -5000 (-39.3%) -- and at every price below that, forever.
                # Only the upward direction is constrained. The old 0.99 default
                # emitted a small but POSITIVE floor on both legs, so it always
                # retained a two-sided tripwire; removing VIB-6269's provenance gate
                # is what let this shape reach the default path, so this PR owns it.
                #
                # A WARNING cannot discharge that on the default path: nobody
                # declared this tolerance, and an autonomous runner does not read
                # logs. A DECLARED tolerance keeps the warning -- the caller stated
                # the number, and one-sided flooring is its honest consequence
                # (VIB-6269 Defect B / Q5, unchanged).
                #
                # `zero_leg_desired > 0` scopes the refusal to a real protection
                # gap. When that leg's DESIRED amount is itself 0 (an intentionally
                # single-sided mint) a zero minimum guarantees nothing because there
                # is nothing to guarantee, and VIB-6269 explicitly supports it.
                if zero_leg_desired > 0 and not tolerance_is_declared:
                    raise UnprotectedTradeError(
                        "LP mint (inherited tolerance zeroes a leg)",
                        f"the INHERITED {lp_slippage:.3%} LP tolerance reaches a range bound at "
                        f"ticks [{tick_lower}, {tick_upper}], so amount{zero_leg}Min would ship as "
                        f"0 while depositing {zero_leg_desired} of token{zero_leg}. That removes "
                        f"the price floor in one direction entirely -- the mint would still be "
                        f"accepted after an arbitrarily large adverse move, not merely a "
                        f"{lp_slippage:.3%} one. Refusing rather than warning because nothing "
                        f"declared this tolerance. Fix by setting LPOpenIntent.max_slippage "
                        f"SMALLER than this position's range half-width, or by declaring "
                        f"max_slippage explicitly at this value to accept one-sided flooring.",
                    )

                # The alternative is not a better floor: on a range this narrow the
                # flat haircut reverts on ~0.001% of price movement, i.e. on
                # essentially every mint. The actionable remedy is the tolerance.
                logger.warning(
                    "LP mint: amount%dMin = 0. The %.3f%% price band reaches the range bound "
                    "of ticks [%s, %s], so token%d is not guaranteed at all across the effective "
                    "tolerance and the mint will accept any amount of it. Set an LP tolerance "
                    "SMALLER than the position's range half-width to keep both legs floored.",
                    zero_leg,
                    float(lp_slippage) * 100,
                    tick_lower,
                    tick_upper,
                    zero_leg,
                )
            logger.debug(
                "LP mint (price-band %.3f%%): amount0=%s (min=%s), amount1=%s (min=%s)",
                float(lp_slippage) * 100,
                amount0_desired,
                band0,
                amount1_desired,
                band1,
            )
            return band0, band1
        logger.debug(
            "LP mint: price band %.3f%% spans the whole tick range [%s, %s]; "
            "falling back to the flat per-leg haircut so the mint is not shipped unprotected",
            float(lp_slippage) * 100,
            tick_lower,
            tick_upper,
        )

    logger.debug(
        "LP mint: slippage=%.1f%%, amount0=%s (min=%s), amount1=%s (min=%s)",
        float(lp_slippage) * 100,
        amount0_desired,
        amount0_min,
        amount1_desired,
        amount1_min,
    )
    return amount0_min, amount1_min


__all__ = [
    "_SLOT0_NOT_FETCHED",
    "compute_lp_slippage_mins",
    "lp_range_excludes_spot_warning",
    "maybe_recompute_lp_amounts_from_slot0",
]
