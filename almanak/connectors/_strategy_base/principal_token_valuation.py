"""Principal-token (PT) position valuation: pure math over the gateway price.

Connector-layer skeleton (allowed home — no protocol-name coupling) for valuing
principal-token positions. The USD price is sourced from the **gateway price
authority** (``MarketSnapshot.pt_price`` → ``PtPriceData``, VIB-5310/5311): the
gateway composes ``PT/USD = pt_to_asset_rate × underlying/USD``, sources both
legs, and originates the confidence band + staleness. This module does NOT read
prices on-chain and does NOT re-derive the composition — it only multiplies the
position quantity by the authority's mark (design spine §0/§1, VIB-5313).

Pendle re-exports these symbols under its connector-flavoured names
(``almanak/connectors/pendle/valuation.py``); the framework portfolio valuer
imports the generic names here, keeping the (framework → connector) coupling
ratchet green (``scripts/ci/scan_chain_protocol_coupling.py``).

Supported position kinds:
  - PT (Principal Token): value = pt_amount × pt_price.price
  - YT (Yield Token): value = yt_amount × pt_price.price, where the gateway has
    composed ``pt_price.price`` as the YT/USD mark for a YT symbol (VIB-5322:
    ``yt_usd = (1 − pt_to_asset_rate) × underlying/USD``). YT decays to zero at
    maturity, so a post-maturity YT marks at the gateway's measured $0 — never a
    stale non-zero price (the gateway floors the complement rate at zero).
  - SY (Standardized Yield): value = sy_amount × pt_price.underlying_price
  - PT/SY LP: value = sy_component (underlying/USD) + pt_component (PT/USD)

Empty ≠ Zero (CLAUDE.md §Accounting, spine §3.3): when the gateway price is
unmeasured (``PtPriceData.price is None`` / ``confidence == UNAVAILABLE``), the
position USD value is **unmeasured** (``current_value_usd = None``) — never
``Decimal("0")``, never a fabricated number. A degraded-but-measured price
(``ESTIMATED`` / ``STALE``) is valued, and its confidence is propagated onto the
position verbatim — a consumer must not upgrade it (spine §3.4).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.market.models import PtPriceData

logger = logging.getLogger(__name__)

# Maximum reasonable implied APR in basis points (50 000 = 500%).
_APR_BPS_CAP = 50_000

__all__ = [
    "PrincipalTokenPositionValue",
    "compute_pt_implied_apy_bps",
    "value_pt_position",
    "value_yt_position",
    "value_sy_position",
    "value_principal_token_lp_from_components",
    "value_principal_token_position",
]


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class PrincipalTokenPositionValue:
    """Valued principal-token position with component breakdown.

    Attributes:
        current_value_usd: Total current market value in USD, or ``None`` when
            the position cannot be valued because the gateway price is
            unmeasured (Empty ≠ Zero — never ``Decimal("0")`` for unmeasured).
        sy_component_usd: USD value of the SY component (LP positions only).
        pt_component_usd: USD value of the PT component (LP / PT positions).
        underlying_price_usd: SY underlying/USD price echoed from the gateway
            composition leg. ``None`` when the gateway did not measure it.
        pt_to_asset_rate: PT-to-underlying exchange rate (< 1.0 before maturity),
            echoed from the gateway composition leg. ``None`` when unmeasured.
        implied_apy_bps: Implied APY derived from pt_to_asset_rate and
            days_to_maturity. ``None`` when either is unknown or the market is
            expired.
        days_to_maturity: Calendar days until PT maturity (from the gateway).
            ``None`` when not reported.
        confidence: Valuation confidence (``ValueConfidence`` enum). Propagated
            verbatim from the gateway price (HIGH / ESTIMATED / STALE) — a
            consumer never upgrades it. ``UNAVAILABLE`` when the position cannot
            be valued.
        unavailable_reason: Non-empty when confidence is not ``HIGH`` (the
            degradation/unmeasured reason).
    """

    current_value_usd: Decimal | None
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
    pt_price_usd: Decimal,
) -> Decimal:
    """Value a PT (Principal Token) position.

    PT value = pt_amount × pt_price_usd

    ``pt_price_usd`` is the gateway-composed PT/USD mark
    (``pt_to_asset_rate × underlying/USD``, stamped + confidence-rated
    gateway-side, spine §0/§1). The valuer does NOT re-derive the composition
    from raw inputs — it only multiplies quantity by the authority's mark. At
    maturity the gateway's mark pulls to par (full underlying redemption value);
    before maturity it is discounted.

    Args:
        pt_amount: Human-readable PT amount (NOT wei).
        pt_price_usd: Gateway-composed PT/USD price (per 1 PT).

    Returns:
        USD value of the PT position.
    """
    return pt_amount * pt_price_usd


def value_yt_position(
    yt_amount: Decimal,
    yt_price_usd: Decimal,
) -> Decimal:
    """Value a YT (Yield Token) position (VIB-5322).

    YT value = yt_amount × yt_price_usd

    ``yt_price_usd`` is the gateway-composed YT/USD mark — for a YT symbol the
    gateway composes ``yt_usd = (1 − pt_to_asset_rate) × underlying/USD`` and
    stamps confidence + staleness (spine §0/§1, mirroring the PT path). The
    valuer does NOT re-derive the composition — it only multiplies quantity by
    the authority's mark. A YT decays to zero as its market approaches maturity
    (``pt_to_asset_rate → 1``), so at/after maturity the gateway mark is a
    measured $0 and a worthless YT is valued at exactly zero (never a stale
    non-zero price).

    Args:
        yt_amount: Human-readable YT amount (NOT wei).
        yt_price_usd: Gateway-composed YT/USD price (per 1 YT).

    Returns:
        USD value of the YT position.
    """
    return yt_amount * yt_price_usd


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
        underlying_price_usd: USD price of the SY underlying asset (gateway leg).

    Returns:
        USD value of the SY position.
    """
    return sy_amount * underlying_price_usd


def value_principal_token_lp_from_components(
    sy_amount: Decimal,
    pt_amount: Decimal,
    underlying_price_usd: Decimal,
    pt_price_usd: Decimal,
) -> tuple[Decimal, Decimal, Decimal]:
    """Value a PT/SY LP position from its SY and PT component amounts.

    A PT/SY LP holds SY and PT in a fixed ratio determined by the pool's
    invariant.  Given the decomposed amounts, total value is:
        sy_value = sy_amount × underlying_price   (gateway underlying/USD leg)
        pt_value = pt_amount × pt_price_usd        (gateway composed PT/USD)
        total    = sy_value + pt_value

    Both prices come from the gateway authority (spine §1): the SY component is
    priced from the underlying/USD leg, the PT component from the composed
    PT/USD mark.

    Args:
        sy_amount: Human-readable SY amount in the LP (NOT wei).
        pt_amount: Human-readable PT amount in the LP (NOT wei).
        underlying_price_usd: USD price of the SY underlying asset (gateway leg).
        pt_price_usd: Gateway-composed PT/USD price (per 1 PT).

    Returns:
        (total_value_usd, sy_value_usd, pt_value_usd)
    """
    sy_val = value_sy_position(sy_amount, underlying_price_usd)
    pt_val = value_pt_position(pt_amount, pt_price_usd)
    return sy_val + pt_val, sy_val, pt_val


# ---------------------------------------------------------------------------
# High-level valuer (consumes the gateway price authority)
# ---------------------------------------------------------------------------


def _confidence_note(confidence: Any) -> str:
    """Render the degradation note for a non-HIGH gateway confidence.

    Empty string for HIGH (clean price); otherwise a stable, human-readable
    note the dashboard / Accountant Test can surface (spine §2 VIB-5313).
    """
    from almanak.framework.portfolio.models import ValueConfidence

    if confidence == ValueConfidence.HIGH:
        return ""
    return f"gateway price confidence: {confidence}"


def value_principal_token_position(
    *,
    pt_price: PtPriceData,
    pt_amount: Decimal | None = None,
    yt_amount: Decimal | None = None,
    sy_amount: Decimal | None = None,
    lp_amount: Decimal | None = None,
    lp_pool_sy_amount: Decimal | None = None,
    lp_pool_pt_amount: Decimal | None = None,
    lp_total_supply: Decimal | None = None,
) -> PrincipalTokenPositionValue:
    """Value a PT / SY / PT-SY-LP position from the gateway price authority.

    The gateway is the single PT/USD price authority (spine §0/§1): it composes
    ``PT/USD = pt_to_asset_rate × underlying/USD``, sources both legs, and stamps
    the confidence band + staleness. This valuer consumes that one number
    (``pt_price``) and owns only the **position math** — it performs no on-chain
    reads and no price composition.

    Paths:
      - **PT-only** (``pt_amount``): ``value = pt_amount × pt_price.price``.
      - **YT-only** (``yt_amount``): ``value = yt_amount × pt_price.price`` —
        ``pt_price`` is the gateway's YT/USD mark for a YT symbol (VIB-5322).
        Unlike PT, a measured ``price == 0`` is a VALID YT value (a post-maturity
        YT is worth exactly $0), so the YT path accepts a measured zero — it only
        rejects an UNAVAILABLE/absent mark (Empty ≠ Zero).
      - **SY-only** (``sy_amount``): ``value = sy_amount × underlying/USD``.
      - **LP** (``lp_amount`` + pool reserves): SY + PT decomposition, SY priced
        from underlying/USD and PT from the composed PT/USD. Without reserves,
        an ``lp_amount × underlying/USD`` approximation (ESTIMATED).

    Empty ≠ Zero (spine §3.3): when the gateway price is unmeasured
    (``pt_price.price is None`` / ``confidence == UNAVAILABLE`` — the corrected
    VIB-5310 model returns no number when ``pt_to_asset_rate`` or the underlying
    is missing, NEVER an at-par fabrication), the position value is
    ``current_value_usd = None`` with ``UNAVAILABLE`` confidence — never
    ``Decimal("0")`` and never a guessed figure. A degraded-but-measured price
    (``ESTIMATED`` / ``STALE``) is valued and its confidence propagated verbatim
    (spine §3.4 — never upgraded).

    Args:
        pt_price: Gateway PT/USD (or YT/USD for a YT symbol) price object
            (``MarketSnapshot.pt_price``).
        pt_amount: Human-readable PT balance. Provide when valuing PT directly.
        yt_amount: Human-readable YT balance. Provide when valuing YT directly
            (``pt_price`` must be the gateway's YT/USD mark for the YT symbol).
        sy_amount: Human-readable SY balance. Provide when valuing SY directly.
        lp_amount: Human-readable LP token balance. Provide when valuing LP.
        lp_pool_sy_amount: Total SY in the LP pool (human-readable).
        lp_pool_pt_amount: Total PT in the LP pool (human-readable).
        lp_total_supply: Total LP token supply (human-readable).

    Returns:
        PrincipalTokenPositionValue with USD breakdown and propagated confidence.
    """
    from almanak.framework.portfolio.models import ValueConfidence

    # Composition legs echoed for transparency (spine §2). These are None
    # whenever the gateway price is unmeasured — MarketSnapshot drops the legs
    # on any non-AVAILABLE response, so an unmeasured PT yields None legs too.
    underlying_price = pt_price.underlying_price
    rate = pt_price.pt_to_asset_rate
    days = pt_price.days_to_maturity
    gateway_confidence = pt_price.confidence
    implied_apy_bps = compute_pt_implied_apy_bps(rate, days) if rate is not None and days is not None else None

    def _unavailable(reason: str) -> PrincipalTokenPositionValue:
        """Empty ≠ Zero: unmeasured price → no number, fail-closed confidence."""
        return PrincipalTokenPositionValue(
            current_value_usd=None,
            sy_component_usd=None,
            pt_component_usd=None,
            underlying_price_usd=underlying_price,
            pt_to_asset_rate=rate,
            implied_apy_bps=implied_apy_bps,
            days_to_maturity=days,
            confidence=ValueConfidence.UNAVAILABLE,
            unavailable_reason=reason,
        )

    # ----------------------------------------------------------------
    # 0. Input validation: reject ambiguous multi-type combinations.
    # ----------------------------------------------------------------
    position_kinds = sum(
        [
            lp_amount is not None,
            pt_amount is not None,
            yt_amount is not None,
            sy_amount is not None,
        ]
    )
    if position_kinds > 1:
        # Mixing lp_amount with pt_amount / yt_amount / sy_amount is not supported.
        # Each call must represent exactly one position type.
        raise ValueError(
            "value_principal_token_position: at most one of lp_amount, pt_amount, yt_amount, sy_amount "
            f"may be provided. Got lp_amount={lp_amount!r}, pt_amount={pt_amount!r}, "
            f"yt_amount={yt_amount!r}, sy_amount={sy_amount!r}."
        )

    pt_usd = pt_price.price
    # Fail closed (Gemini, VIB-5313): a non-positive PT/USD is not a measured
    # mark (a PT trades at > 0 before redemption), and an UNAVAILABLE band means
    # the number is unmeasured even if a stray price leaked through. Empty ≠ Zero.
    pt_measured = pt_usd is not None and pt_usd > 0 and gateway_confidence != ValueConfidence.UNAVAILABLE

    # ----------------------------------------------------------------
    # 1. PT-only position — needs the composed PT/USD mark.
    # ----------------------------------------------------------------
    if pt_amount is not None:
        if not pt_measured:
            return _unavailable("pt price unmeasured (gateway UNAVAILABLE)")
        pt_val = value_pt_position(pt_amount, pt_usd)  # type: ignore[arg-type]
        return PrincipalTokenPositionValue(
            current_value_usd=pt_val,
            sy_component_usd=None,
            pt_component_usd=pt_val,
            underlying_price_usd=underlying_price,
            pt_to_asset_rate=rate,
            implied_apy_bps=implied_apy_bps,
            days_to_maturity=days,
            confidence=gateway_confidence,  # propagate verbatim, never upgrade
            unavailable_reason=_confidence_note(gateway_confidence),
        )

    # ----------------------------------------------------------------
    # 1b. YT-only position — needs the composed YT/USD mark (VIB-5322).
    #     ``pt_price`` IS the gateway's YT/USD mark for a YT symbol (the gateway
    #     composes ``yt_usd = (1 − pt_to_asset_rate) × underlying/USD`` and floors
    #     it at zero past maturity). Unlike PT, a measured ``price == 0`` is a
    #     VALID YT value — a fully-decayed (post-maturity) YT is worth exactly
    #     $0 — so the YT measured-test accepts a measured zero and only rejects an
    #     UNAVAILABLE/absent mark (Empty ≠ Zero: an unmeasured mark is None, a
    #     worthless YT is a measured 0). The composed value is booked into the PT
    #     component slot (the single "principal-token component" of the result);
    #     ``pt_to_asset_rate`` echoes the YT complement rate the gateway stamped.
    #     ``implied_apy_bps`` is left None: the PT-discount-to-par APY formula does
    #     NOT describe a YT's return (a YT earns the yield stream, not a
    #     pull-to-par), so a PT-style APY off the YT rate would be a misleading
    #     number — Empty ≠ Zero (unmeasured, not a wrong figure).
    # ----------------------------------------------------------------
    if yt_amount is not None:
        yt_measured = pt_usd is not None and pt_usd >= 0 and gateway_confidence != ValueConfidence.UNAVAILABLE
        if not yt_measured:
            return _unavailable("yt price unmeasured (gateway UNAVAILABLE)")
        yt_val = value_yt_position(yt_amount, pt_usd)  # type: ignore[arg-type]
        return PrincipalTokenPositionValue(
            current_value_usd=yt_val,
            sy_component_usd=None,
            pt_component_usd=yt_val,
            underlying_price_usd=underlying_price,
            pt_to_asset_rate=rate,
            implied_apy_bps=None,
            days_to_maturity=days,
            confidence=gateway_confidence,  # propagate verbatim, never upgrade
            unavailable_reason=_confidence_note(gateway_confidence),
        )

    # ----------------------------------------------------------------
    # 2. SY-only position — needs only the underlying/USD leg.
    # ----------------------------------------------------------------
    if sy_amount is not None:
        # Fail closed (Gemini, VIB-5313): an UNAVAILABLE band means the underlying
        # leg is unmeasured even if a price leaked through — Empty ≠ Zero.
        if gateway_confidence == ValueConfidence.UNAVAILABLE or underlying_price is None or underlying_price <= 0:
            return _unavailable("sy underlying price unmeasured (gateway UNAVAILABLE)")
        sy_val = value_sy_position(sy_amount, underlying_price)
        return PrincipalTokenPositionValue(
            current_value_usd=sy_val,
            sy_component_usd=sy_val,
            pt_component_usd=None,
            underlying_price_usd=underlying_price,
            pt_to_asset_rate=None,
            implied_apy_bps=None,
            days_to_maturity=days,
            confidence=gateway_confidence,  # propagate verbatim, never upgrade
            unavailable_reason=_confidence_note(gateway_confidence),
        )

    # ----------------------------------------------------------------
    # 3. LP position — needs BOTH the underlying/USD leg and the PT/USD mark.
    #    Empty ≠ Zero: if either leg is unmeasured the whole LP is unmeasured
    #    (valuing only the SY half would understate the position).
    # ----------------------------------------------------------------
    if lp_amount is not None:
        if not pt_measured or underlying_price is None or underlying_price <= 0:
            return _unavailable("lp price legs unmeasured (gateway UNAVAILABLE)")

        # Path A: pool reserves provided → decompose LP tokens into SY + PT.
        if (
            lp_pool_sy_amount is not None
            and lp_pool_pt_amount is not None
            and lp_total_supply is not None
            and lp_total_supply > 0
        ):
            lp_ratio = lp_amount / lp_total_supply
            my_sy = lp_pool_sy_amount * lp_ratio
            my_pt = lp_pool_pt_amount * lp_ratio
            total_val, sy_val, pt_val = value_principal_token_lp_from_components(
                my_sy,
                my_pt,
                underlying_price,
                pt_usd,  # type: ignore[arg-type]
            )
            return PrincipalTokenPositionValue(
                current_value_usd=total_val,
                sy_component_usd=sy_val,
                pt_component_usd=pt_val,
                underlying_price_usd=underlying_price,
                pt_to_asset_rate=rate,
                implied_apy_bps=implied_apy_bps,
                days_to_maturity=days,
                confidence=gateway_confidence,  # propagate verbatim, never upgrade
                unavailable_reason=_confidence_note(gateway_confidence),
            )

        # Path B: no pool reserves — approximate with the underlying/USD leg.
        # 1 LP ≈ 1 underlying (rough; actual ratio varies with pool). Component
        # breakdown is unavailable here so both component fields stay None. The
        # approximation downgrades a HIGH gateway price to ESTIMATED, but never
        # upgrades a worse one (confidence only degrades — spine §3.4).
        fallback_val = lp_amount * underlying_price
        conf = ValueConfidence.ESTIMATED if gateway_confidence == ValueConfidence.HIGH else gateway_confidence
        return PrincipalTokenPositionValue(
            current_value_usd=fallback_val,
            sy_component_usd=None,
            pt_component_usd=None,
            underlying_price_usd=underlying_price,
            pt_to_asset_rate=rate,
            implied_apy_bps=implied_apy_bps,
            days_to_maturity=days,
            confidence=conf,
            unavailable_reason="lp_pool_reserves not provided; using lp_amount × sy_price approximation",
        )

    # ----------------------------------------------------------------
    # 4. No position data
    # ----------------------------------------------------------------
    return _unavailable("No position data provided (lp_amount, pt_amount, or sy_amount required)")
