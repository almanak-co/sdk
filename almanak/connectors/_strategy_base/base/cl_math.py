"""Shared concentrated-liquidity compiler math."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from almanak.framework.intents._compiler_helpers import compute_min_amount_out
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus

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
) -> tuple[int, int]:
    """Compute LP minimum amounts from the effective LP slippage."""
    protocol_lp_slippage = (intent.protocol_params or {}).get("lp_slippage")
    intent_max_slippage = getattr(intent, "max_slippage", None)
    lp_slippage = (
        min(max(Decimal(str(protocol_lp_slippage)), Decimal("0")), Decimal("1"))
        if protocol_lp_slippage is not None
        else (intent_max_slippage if intent_max_slippage is not None else default_lp_slippage)
    )
    amount0_min = compute_min_amount_out(amount0_desired, lp_slippage)
    amount1_min = compute_min_amount_out(amount1_desired, lp_slippage)
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
