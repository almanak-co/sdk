"""Pendle position valuation: PT pull-to-par and LP component decomposition.

Pure deterministic math where possible; gateway/on-chain reads are isolated
to the reader layer so the math functions remain testable without I/O.

Supports three Pendle position types:
  - PT (Principal Token): value = pt_amount × underlying_price × pt_to_asset_rate
  - SY (Standardized Yield): value = sy_amount × underlying_price
  - Pendle LP: value = sy_component_value + pt_component_value (weighted by pool ratio)

The LP decomposition uses the Pendle pool reserves read from the market contract.
When on-chain reads fail, it falls back to spot-price valuation using the SY price.

References:
  - Pendle whitepaper: https://github.com/pendle-finance/pendle-core-v2-public
  - RouterStatic: getPtToAssetRate, readTokens
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.data.pendle.on_chain_reader import PendleOnChainReader

logger = logging.getLogger(__name__)

# Maximum reasonable implied APR in basis points (50 000 = 500%).
_APR_BPS_CAP = 50_000


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class PendlePositionValue:
    """Valued Pendle position with component breakdown.

    Attributes:
        current_value_usd: Total current market value in USD.
        sy_component_usd: USD value of the SY component (LP positions only).
        pt_component_usd: USD value of the PT component (LP positions only).
        underlying_price_usd: Price of the underlying asset per unit.
        pt_to_asset_rate: PT-to-underlying exchange rate (< 1.0 before maturity).
        implied_apy_bps: Implied APY derived from pt_to_asset_rate and days_to_maturity.
            None when maturity is unknown or market is expired.
        days_to_maturity: Calendar days until PT maturity. 0 when expired.
        confidence: Valuation confidence (from ValueConfidence enum).
            HIGH when all inputs are available; ESTIMATED when falling back
            (e.g., pt_to_asset_rate defaulted to 1.0); UNAVAILABLE when the
            position cannot be valued at all.
        unavailable_reason: Non-empty when confidence is ESTIMATED or UNAVAILABLE.
    """

    current_value_usd: Decimal
    sy_component_usd: Decimal | None
    pt_component_usd: Decimal | None
    underlying_price_usd: Decimal | None
    pt_to_asset_rate: Decimal | None
    implied_apy_bps: int | None
    days_to_maturity: int | None
    confidence: Any  # ValueConfidence enum — imported lazily to avoid circular deps
    unavailable_reason: str


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------


def compute_pt_implied_apy_bps(
    pt_to_asset_rate: Decimal,
    days_to_maturity: int,
) -> int | None:
    """Compute implied APY in basis points from the PT discount and time to maturity.

    Formula (simple annualization):
        discount = 1 - pt_to_asset_rate
        apy = (discount / pt_to_asset_rate) × (365 / days_to_maturity)
        apy_bps = round(apy × 10_000)

    Returns None when days_to_maturity <= 0 (expired / at maturity).
    Caps at _APR_BPS_CAP to handle near-maturity edge cases.

    Args:
        pt_to_asset_rate: Current PT / underlying exchange rate (0 < rate <= 1).
        days_to_maturity: Days remaining until PT maturity (must be > 0).

    Returns:
        Implied APY in basis points, or None if not computable.
    """
    if days_to_maturity <= 0:
        return None
    if pt_to_asset_rate <= 0 or pt_to_asset_rate > Decimal("1"):
        return None

    try:
        discount = Decimal("1") - pt_to_asset_rate
        apy = (discount / pt_to_asset_rate) * (Decimal("365") / Decimal(str(days_to_maturity)))
        apy_bps = int(apy * Decimal("10000"))
        return min(apy_bps, _APR_BPS_CAP)
    except Exception:
        return None


def value_pt_position(
    pt_amount: Decimal,
    underlying_price_usd: Decimal,
    pt_to_asset_rate: Decimal,
) -> Decimal:
    """Value a PT (Principal Token) position.

    PT value = pt_amount × underlying_price × pt_to_asset_rate

    At maturity pt_to_asset_rate → 1.0, so the position pulls to par
    (full underlying redemption value).  Before maturity it is discounted.

    Args:
        pt_amount: Human-readable PT amount (NOT wei).
        underlying_price_usd: USD price of the SY underlying asset.
        pt_to_asset_rate: Current PT/asset exchange rate from RouterStatic.

    Returns:
        USD value of the PT position.
    """
    return pt_amount * underlying_price_usd * pt_to_asset_rate


def value_sy_position(
    sy_amount: Decimal,
    underlying_price_usd: Decimal,
) -> Decimal:
    """Value an SY (Standardized Yield) position.

    SY wraps a yield-bearing token at roughly 1:1 exchange rate with the underlying.
    Value = sy_amount × underlying_price.

    Note: strictly speaking, SY accrues a small exchange-rate premium over time
    as yield is captured.  For current purposes, 1 SY ≈ 1 underlying is accurate
    to within typical yield rates over short periods.

    Args:
        sy_amount: Human-readable SY amount (NOT wei).
        underlying_price_usd: USD price of the SY underlying asset.

    Returns:
        USD value of the SY position.
    """
    return sy_amount * underlying_price_usd


def value_pendle_lp_from_components(
    sy_amount: Decimal,
    pt_amount: Decimal,
    underlying_price_usd: Decimal,
    pt_to_asset_rate: Decimal,
) -> tuple[Decimal, Decimal, Decimal]:
    """Value a Pendle LP position from its SY and PT component amounts.

    Pendle LP holds SY and PT in a fixed ratio determined by the pool's
    invariant.  Given the decomposed amounts, total value is:
        sy_value = sy_amount × underlying_price
        pt_value = pt_amount × underlying_price × pt_to_asset_rate
        total    = sy_value + pt_value

    Args:
        sy_amount: Human-readable SY amount in the LP (NOT wei).
        pt_amount: Human-readable PT amount in the LP (NOT wei).
        underlying_price_usd: USD price of the SY underlying asset.
        pt_to_asset_rate: Current PT/asset exchange rate.

    Returns:
        (total_value_usd, sy_value_usd, pt_value_usd)
    """
    sy_val = value_sy_position(sy_amount, underlying_price_usd)
    pt_val = value_pt_position(pt_amount, underlying_price_usd, pt_to_asset_rate)
    return sy_val + pt_val, sy_val, pt_val


# ---------------------------------------------------------------------------
# High-level valuer (with gateway/on-chain reads)
# ---------------------------------------------------------------------------


def value_pendle_position(
    *,
    chain: str,
    market_address: str,
    lp_amount: Decimal | None = None,
    pt_amount: Decimal | None = None,
    sy_amount: Decimal | None = None,
    lp_pool_sy_amount: Decimal | None = None,
    lp_pool_pt_amount: Decimal | None = None,
    lp_total_supply: Decimal | None = None,
    underlying_price_usd: Decimal | None = None,
    on_chain_reader: PendleOnChainReader | None = None,
) -> PendlePositionValue:
    """Value a Pendle LP or PT/SY position.

    Two paths:
    1. **LP with pool decomposition** (lp_amount + pool reserves): decomposes
       LP tokens into SY + PT components using the pool ratio, then prices each.
    2. **LP spot fallback** (lp_amount + sy_price only): uses sy_price ×
       lp_amount as a rough approximation when pool reserves are unavailable.

    For PT-only positions: uses pt_amount + on-chain pt_to_asset_rate.
    For SY-only positions: uses sy_amount × underlying_price.

    Args:
        chain: Chain name (e.g., "arbitrum", "ethereum").
        market_address: Pendle market contract address.
        lp_amount: Human-readable LP token balance.  Provide when valuing LP.
        pt_amount: Human-readable PT balance.  Provide when valuing PT directly.
        sy_amount: Human-readable SY balance.  Provide when valuing SY directly.
        lp_pool_sy_amount: Total SY in the LP pool (human-readable).
        lp_pool_pt_amount: Total PT in the LP pool (human-readable).
        lp_total_supply: Total LP token supply (human-readable).
        underlying_price_usd: USD price of the SY underlying asset.
            Resolved externally from market.price() or price_oracle.
        on_chain_reader: PendleOnChainReader for pt_to_asset_rate and implied APY.
            If None, pt_to_asset_rate defaults to 1.0 (at-par, conservative).

    Returns:
        PendlePositionValue with USD breakdown and confidence.
    """
    from almanak.framework.portfolio.models import ValueConfidence

    unavailable_reasons: list[str] = []

    # ----------------------------------------------------------------
    # 0. Input validation: reject ambiguous multi-type combinations.
    # ----------------------------------------------------------------
    position_kinds = sum(
        [
            lp_amount is not None,
            pt_amount is not None,
            sy_amount is not None,
        ]
    )
    if position_kinds > 1:
        # Mixing lp_amount with pt_amount or sy_amount is not supported.
        # Each call must represent exactly one position type.
        raise ValueError(
            "value_pendle_position: at most one of lp_amount, pt_amount, sy_amount may be provided. "
            f"Got lp_amount={lp_amount!r}, pt_amount={pt_amount!r}, sy_amount={sy_amount!r}."
        )

    # ----------------------------------------------------------------
    # 1. Guard: underlying price is required for any USD valuation
    # ----------------------------------------------------------------
    if underlying_price_usd is None or underlying_price_usd <= 0:
        return PendlePositionValue(
            current_value_usd=Decimal("0"),
            sy_component_usd=None,
            pt_component_usd=None,
            underlying_price_usd=underlying_price_usd,
            pt_to_asset_rate=None,
            implied_apy_bps=None,
            days_to_maturity=None,
            confidence=ValueConfidence.UNAVAILABLE,
            unavailable_reason="underlying_price_usd not provided or non-positive",
        )

    # ----------------------------------------------------------------
    # 2. SY-only position — no PT rate needed, skip on-chain reads.
    # ----------------------------------------------------------------
    if sy_amount is not None:
        sy_val = value_sy_position(sy_amount, underlying_price_usd)
        return PendlePositionValue(
            current_value_usd=sy_val,
            sy_component_usd=sy_val,
            pt_component_usd=None,
            underlying_price_usd=underlying_price_usd,
            pt_to_asset_rate=None,
            implied_apy_bps=None,
            days_to_maturity=None,
            confidence=ValueConfidence.HIGH,
            unavailable_reason="",
        )

    # ----------------------------------------------------------------
    # 3. Fetch pt_to_asset_rate and days_to_maturity for PT / LP paths.
    # ----------------------------------------------------------------
    pt_to_asset_rate: Decimal | None = None
    days_to_maturity: int | None = None
    implied_apy_bps: int | None = None

    if on_chain_reader is not None:
        try:
            pt_to_asset_rate = on_chain_reader.get_pt_to_asset_rate(market_address)
        except Exception as e:
            logger.debug("pendle_valuer: pt_to_asset_rate read failed for %s: %s", market_address, e)
            unavailable_reasons.append("pt_to_asset_rate unavailable")

        days_to_maturity = on_chain_reader.get_days_to_maturity(market_address)

        if pt_to_asset_rate is not None and days_to_maturity is not None:
            implied_apy_bps = compute_pt_implied_apy_bps(pt_to_asset_rate, days_to_maturity)

    # Default pt_to_asset_rate to 1.0 when unavailable (at-par assumption).
    # This is a conservative upper bound: PT can only trade at ≤ par before maturity.
    effective_rate = pt_to_asset_rate if pt_to_asset_rate is not None else Decimal("1")

    # ----------------------------------------------------------------
    # 4. PT-only position
    # ----------------------------------------------------------------
    if pt_amount is not None:
        pt_val = value_pt_position(pt_amount, underlying_price_usd, effective_rate)
        conf = ValueConfidence.HIGH if pt_to_asset_rate is not None else ValueConfidence.ESTIMATED
        reason = "; ".join(unavailable_reasons)
        if pt_to_asset_rate is None:
            reason = (reason + "; pt_to_asset_rate defaulted to 1.0 (at-par)").lstrip("; ")
        return PendlePositionValue(
            current_value_usd=pt_val,
            sy_component_usd=None,
            pt_component_usd=pt_val,
            underlying_price_usd=underlying_price_usd,
            pt_to_asset_rate=pt_to_asset_rate,
            implied_apy_bps=implied_apy_bps,
            days_to_maturity=days_to_maturity,
            confidence=conf,
            unavailable_reason=reason,
        )

    # ----------------------------------------------------------------
    # 5. LP position — decomposition path
    # ----------------------------------------------------------------
    if lp_amount is not None:
        # Path A: pool reserves provided → decompose LP tokens into SY + PT
        if (
            lp_pool_sy_amount is not None
            and lp_pool_pt_amount is not None
            and lp_total_supply is not None
            and lp_total_supply > 0
        ):
            lp_ratio = lp_amount / lp_total_supply
            my_sy = lp_pool_sy_amount * lp_ratio
            my_pt = lp_pool_pt_amount * lp_ratio
            total_val, sy_val, pt_val = value_pendle_lp_from_components(
                my_sy, my_pt, underlying_price_usd, effective_rate
            )
            conf = ValueConfidence.HIGH if pt_to_asset_rate is not None else ValueConfidence.ESTIMATED
            reason = "; ".join(unavailable_reasons)
            if pt_to_asset_rate is None:
                reason = (reason + "; pt_to_asset_rate defaulted to 1.0").lstrip("; ")
            return PendlePositionValue(
                current_value_usd=total_val,
                sy_component_usd=sy_val,
                pt_component_usd=pt_val,
                underlying_price_usd=underlying_price_usd,
                pt_to_asset_rate=pt_to_asset_rate,
                implied_apy_bps=implied_apy_bps,
                days_to_maturity=days_to_maturity,
                confidence=conf,
                unavailable_reason=reason,
            )

        # Path B: no pool reserves — fall back to spot valuation using SY price.
        # 1 LP ≈ 1 underlying (rough approximation; actual ratio varies with pool).
        # Component breakdown is not available in this path — both sy_component_usd
        # and pt_component_usd are left None to reflect that decomposition was skipped.
        fallback_val = lp_amount * underlying_price_usd
        unavailable_reasons.append("lp_pool_reserves not provided; using lp_amount × sy_price approximation")
        return PendlePositionValue(
            current_value_usd=fallback_val,
            sy_component_usd=None,
            pt_component_usd=None,
            underlying_price_usd=underlying_price_usd,
            pt_to_asset_rate=pt_to_asset_rate,
            implied_apy_bps=implied_apy_bps,
            days_to_maturity=days_to_maturity,
            confidence=ValueConfidence.ESTIMATED,
            unavailable_reason="; ".join(unavailable_reasons),
        )

    # ----------------------------------------------------------------
    # 6. No position data
    # ----------------------------------------------------------------
    return PendlePositionValue(
        current_value_usd=Decimal("0"),
        sy_component_usd=None,
        pt_component_usd=None,
        underlying_price_usd=underlying_price_usd,
        pt_to_asset_rate=None,
        implied_apy_bps=None,
        days_to_maturity=None,
        confidence=ValueConfidence.UNAVAILABLE,
        unavailable_reason="No position data provided (lp_amount, pt_amount, or sy_amount required)",
    )
