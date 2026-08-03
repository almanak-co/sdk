"""Shared concentrated-liquidity compiler math."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from almanak.framework.intents._compiler_helpers import compute_min_amount_out
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
    ``lp_mint_mins_for_price_band`` (VIB-6269) when BOTH hold: the tolerance was
    DECLARED (``protocol_params["lp_slippage"]`` or ``intent.max_slippage``), and
    ``sqrt_price_x96`` plus the tick range are supplied. Otherwise the legacy flat
    per-leg haircut is used -- so a caller that cannot read slot0 keeps working,
    and a caller that declared nothing keeps byte-identical calldata.

    Why the price-band form is the correct instrument for a MINT: ``amount0Min``
    / ``amount1Min`` on ``mint`` constrain the deposit SPLIT, and the split is an
    amplified function of price (elasticity 5x-400x for ordinary ranges), so a
    flat haircut of ``s`` really tolerates only ``s / A`` of price movement. That
    made a 0.5% tolerance mean 0.024% on a live Arbitrum WETH/USDC position and
    revert on ordinary compile-to-submit staleness. See the decision doc
    ``docs/internal/plans/vib-6269-cl-lp-open-minimum-decision.md``.

    **Two triggers select the flat haircut instead, and the order matters.**

    1. *Provenance (primary).* A tolerance that fell through to
       ``default_lp_slippage`` is a permissive PLACEHOLDER (0.99, VIB-6225), not a
       declared price tolerance, so it never reaches the band construction at all.
    2. *Both legs zero (secondary).* A DECLARED tolerance wide enough to reach BOTH
       range bounds yields ``(0, 0)`` -- truthful (nothing is guaranteed across such
       a band) but it would ship a mint with no on-chain floor, so it falls back.

    Do NOT collapse these into "both-zero handles it". An earlier revision assumed a
    99% band always reaches both bounds and keyed the fallback on ``(0, 0)`` alone.
    It does not: on a wide range the band crosses only the LOWER bound, so the
    default path returned a changed pair -- measured at 4 of 7 sampled range widths,
    including a token1 floor of literally ``0`` at ticks [-10000, 10000]. See the
    inline comment at the guard below.

    A SINGLE zero leg from a DECLARED tolerance is kept: that is the legitimate
    band-aware case VIB-6269 (Defect B) measured and which its Q5
    ``max(amount0_min, amount1_min)``-shaped reasoning explicitly endorses.

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
    # PROVENANCE, not the band result, selects the instrument. `default_lp_slippage`
    # is a permissive placeholder (0.99, VIB-6225), not a declared price tolerance,
    # so it must never reach the price-band construction: feeding a placeholder to a
    # correct instrument yields a correct-but-meaningless answer. Keying the fallback
    # on `(0, 0)` instead was WRONG -- a 99% band does not cross BOTH bounds on a wide
    # range, so at e.g. ticks [-10000, 10000] it returned one positive leg and one
    # ZERO leg, silently dropping a default caller's token1 floor from 1%-of-desired
    # to 0 and breaking the byte-identical-default contract this change promises.
    tolerance_is_declared = protocol_lp_slippage is not None or intent_max_slippage is not None
    amount0_min = compute_min_amount_out(amount0_desired, lp_slippage)
    amount1_min = compute_min_amount_out(amount1_desired, lp_slippage)

    if (
        tolerance_is_declared
        and sqrt_price_x96 is not None
        and sqrt_price_x96 > 0
        and tick_lower is not None
        and tick_upper is not None
    ):
        from almanak.framework.intents.lp_math import lp_mint_mins_for_price_band

        band0, band1 = lp_mint_mins_for_price_band(
            sqrt_price_x96,
            tick_lower,
            tick_upper,
            amount0_desired,
            amount1_desired,
            lp_slippage,
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
                # A ZERO MINIMUM IS SHIPPING. Legitimate here -- the band reaches
                # that leg's range bound, so nothing is guaranteed for it and any
                # positive floor would be a lie -- but `amount*Min = 0` is the
                # exact shape VIB-6220 / VIB-6226 / VIB-6235 file as defects, so
                # it must never appear silently. The alternative is not a better
                # floor: on a range this narrow the flat haircut reverts on ~0.001%
                # of price movement, i.e. on essentially every mint. The actionable
                # remedy is the tolerance, not the instrument.
                zero_leg = 0 if band0 == 0 else 1
                logger.warning(
                    "LP mint: amount%dMin = 0. The %.3f%% price band reaches the range bound "
                    "of ticks [%s, %s], so token%d is not guaranteed at all across the declared "
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
