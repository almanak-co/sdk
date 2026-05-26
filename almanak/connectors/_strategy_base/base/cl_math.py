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
    "maybe_recompute_lp_amounts_from_slot0",
]
