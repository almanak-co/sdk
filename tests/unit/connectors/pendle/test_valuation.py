"""Tests for Pendle position valuation (VIB-3487, rewired VIB-5313).

VIB-5313 routes Pendle PT / LP valuation through the **gateway price authority**
(``MarketSnapshot.pt_price`` → ``PtPriceData``) instead of ad-hoc
``underlying_price_usd`` + ``on_chain_reader`` inputs. The gateway composes
``PT/USD = pt_to_asset_rate × underlying/USD`` and stamps confidence + staleness;
the valuer only does position math (``pt_amount × pt_price.price``).

Characterization matrix:
  - PT valued from a HIGH gateway price → value = pt_amount × price, HIGH.
  - PT with UNAVAILABLE gateway price → value None (Empty ≠ Zero, NOT 0).
  - PT with ESTIMATED / STALE gateway price → value present, confidence propagated.
  - SY valued from the underlying/USD leg.
  - LP decomposition uses underlying/USD (SY) + PT/USD (PT) gateway legs.
  - LP with an unmeasured gateway price → value None.
  - Pure-math helpers + implied-APY computation.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.connectors.pendle.valuation import (
    _APR_BPS_CAP,
    PendlePositionValue,
    compute_pt_implied_apy_bps,
    value_pendle_lp_from_components,
    value_pendle_position,
    value_pt_position,
    value_sy_position,
    value_yt_position,
)
from almanak.framework.market.models import PtPriceData
from almanak.framework.portfolio.models import ValueConfidence


def _pt_price(
    *,
    price: Decimal | None,
    confidence: ValueConfidence,
    underlying_price: Decimal | None = None,
    pt_to_asset_rate: Decimal | None = None,
    days_to_maturity: int | None = None,
    symbol: str = "PT-sUSDe-26DEC2024",
    chain: str = "ethereum",
) -> PtPriceData:
    """Build a gateway PtPriceData stand-in for the valuer under test.

    Mirrors the MarketSnapshot.pt_price contract: an UNAVAILABLE price drops the
    composition legs (price=None), an AVAILABLE/STALE price carries them.
    """
    return PtPriceData(
        symbol=symbol,
        chain=chain,
        price=price,
        confidence=confidence,
        underlying_price=underlying_price,
        pt_to_asset_rate=pt_to_asset_rate,
        days_to_maturity=days_to_maturity,
    )


# ---------------------------------------------------------------------------
# Pure math unit tests
# ---------------------------------------------------------------------------


class TestComputePtImpliedApyBps:
    """Tests for the PT implied APY computation."""

    def test_standard_case(self):
        """pt_to_asset_rate=0.95, days=180 → ~1067 bps."""
        result = compute_pt_implied_apy_bps(Decimal("0.95"), 180)
        assert result is not None
        assert 1050 <= result <= 1090, f"Expected ~1067, got {result}"

    def test_zero_days_returns_none(self):
        assert compute_pt_implied_apy_bps(Decimal("0.95"), 0) is None

    def test_negative_days_returns_none(self):
        assert compute_pt_implied_apy_bps(Decimal("0.95"), -5) is None

    def test_at_par_returns_zero(self):
        result = compute_pt_implied_apy_bps(Decimal("1.0"), 180)
        assert result == 0

    def test_near_maturity_capped(self):
        result = compute_pt_implied_apy_bps(Decimal("0.5"), 1)
        assert result == _APR_BPS_CAP

    def test_full_year_small_discount(self):
        result = compute_pt_implied_apy_bps(Decimal("0.999"), 365)
        assert result is not None
        assert 8 <= result <= 12, f"Expected ~10, got {result}"


class TestValuePtPosition:
    """PT position math: value = pt_amount × gateway PT/USD mark."""

    def test_pt_basic(self):
        # gateway PT/USD mark = $3325 (= 3500 × 0.95, composed gateway-side)
        assert value_pt_position(Decimal("2.0"), Decimal("3325")) == Decimal("6650")

    def test_pt_at_par_mark(self):
        # At maturity the gateway mark pulls to par ($3500).
        assert value_pt_position(Decimal("1.0"), Decimal("3500")) == Decimal("3500")


class TestValueYtPosition:
    """YT position math: value = yt_amount × gateway YT/USD mark (VIB-5322)."""

    def test_yt_basic(self):
        # gateway YT/USD mark = $0.10 (= (1 − 0.95) × 2.00, composed gateway-side)
        assert value_yt_position(Decimal("100"), Decimal("0.10")) == Decimal("10.0")

    def test_yt_at_maturity_zero_mark(self):
        # At maturity the gateway YT mark is exactly $0 (rate pulled to par).
        assert value_yt_position(Decimal("100"), Decimal("0")) == Decimal("0")


class TestValueSyPosition:
    """SY position math: value = sy_amount × underlying/USD."""

    def test_sy_position(self):
        assert value_sy_position(Decimal("5.0"), Decimal("100")) == Decimal("500")

    def test_zero_sy(self):
        assert value_sy_position(Decimal("0"), Decimal("3500")) == Decimal("0")


class TestValuePendleLpFromComponents:
    """LP component decomposition math: SY from underlying, PT from PT/USD mark."""

    def test_decomposition(self):
        # 100 SY @ $1000 underlying + 50 PT @ $950 PT/USD mark
        total, sy_val, pt_val = value_pendle_lp_from_components(
            sy_amount=Decimal("100"),
            pt_amount=Decimal("50"),
            underlying_price_usd=Decimal("1000"),
            pt_price_usd=Decimal("950"),
        )
        assert sy_val == Decimal("100000")  # 100 × $1000
        assert pt_val == Decimal("47500")   # 50 × $950
        assert total == Decimal("147500")

    def test_at_par(self):
        total, sy_val, pt_val = value_pendle_lp_from_components(
            sy_amount=Decimal("10"),
            pt_amount=Decimal("10"),
            underlying_price_usd=Decimal("100"),
            pt_price_usd=Decimal("100"),
        )
        assert sy_val == Decimal("1000")
        assert pt_val == Decimal("1000")
        assert total == Decimal("2000")


# ---------------------------------------------------------------------------
# value_pendle_position — gateway-price-authority contract (VIB-5313)
# ---------------------------------------------------------------------------


class TestValuePendlePositionPT:
    """PT-only valuation from the gateway PT/USD mark."""

    def test_pt_high_confidence(self):
        """HIGH gateway price → value = pt_amount × price, confidence propagated."""
        result = value_pendle_position(
            pt_price=_pt_price(
                price=Decimal("950"),
                confidence=ValueConfidence.HIGH,
                underlying_price=Decimal("1000"),
                pt_to_asset_rate=Decimal("0.95"),
                days_to_maturity=180,
            ),
            pt_amount=Decimal("2.0"),
        )
        assert result.current_value_usd == Decimal("1900")  # 2 × $950
        assert result.pt_component_usd == Decimal("1900")
        assert result.sy_component_usd is None
        assert result.confidence == ValueConfidence.HIGH
        assert result.unavailable_reason == ""
        assert result.pt_to_asset_rate == Decimal("0.95")
        assert result.underlying_price_usd == Decimal("1000")
        assert result.days_to_maturity == 180
        assert result.implied_apy_bps is not None  # rate + days both present

    def test_pt_unavailable_is_unmeasured_not_zero(self):
        """UNAVAILABLE gateway price → value None (Empty ≠ Zero), never 0."""
        result = value_pendle_position(
            pt_price=_pt_price(price=None, confidence=ValueConfidence.UNAVAILABLE),
            pt_amount=Decimal("2.0"),
        )
        assert result.current_value_usd is None  # NOT Decimal("0")
        assert result.current_value_usd != Decimal("0")
        assert result.confidence == ValueConfidence.UNAVAILABLE
        assert result.pt_component_usd is None
        assert result.unavailable_reason

    def test_pt_estimated_confidence_propagated(self):
        """ESTIMATED gateway price → valued, confidence propagated (not upgraded)."""
        result = value_pendle_position(
            pt_price=_pt_price(
                price=Decimal("980"),
                confidence=ValueConfidence.ESTIMATED,
                underlying_price=Decimal("1000"),
                pt_to_asset_rate=Decimal("0.98"),
            ),
            pt_amount=Decimal("1.0"),
        )
        assert result.current_value_usd == Decimal("980")
        assert result.confidence == ValueConfidence.ESTIMATED
        assert result.unavailable_reason  # non-empty degradation note

    def test_pt_stale_confidence_propagated(self):
        """STALE gateway price → valued, STALE confidence stamped (not folded into HIGH)."""
        result = value_pendle_position(
            pt_price=_pt_price(
                price=Decimal("970"),
                confidence=ValueConfidence.STALE,
                underlying_price=Decimal("1000"),
                pt_to_asset_rate=Decimal("0.97"),
            ),
            pt_amount=Decimal("3.0"),
        )
        assert result.current_value_usd == Decimal("2910")  # 3 × $970
        assert result.confidence == ValueConfidence.STALE
        assert result.unavailable_reason

    def test_pt_value_does_not_re_derive_composition(self):
        """Valuer uses pt_price.price directly, not underlying × rate.

        Guards spine §0: no consumer re-derives price. Here price (960) is
        deliberately != underlying × rate (1000 × 0.95 = 950) — the valuer must
        trust the gateway's composed mark.
        """
        result = value_pendle_position(
            pt_price=_pt_price(
                price=Decimal("960"),
                confidence=ValueConfidence.HIGH,
                underlying_price=Decimal("1000"),
                pt_to_asset_rate=Decimal("0.95"),
            ),
            pt_amount=Decimal("1.0"),
        )
        assert result.current_value_usd == Decimal("960")  # price, not 950

    def test_pt_non_positive_price_is_unavailable(self):
        """Fail closed (Gemini, VIB-5313): a 0/negative PT/USD is not a measured
        mark (PT trades at > 0 before redemption) → value None, never 0."""
        for bad in (Decimal("0"), Decimal("-5")):
            result = value_pendle_position(
                pt_price=_pt_price(
                    price=bad,
                    confidence=ValueConfidence.HIGH,  # band says HIGH, but price is junk
                    underlying_price=Decimal("1000"),
                    pt_to_asset_rate=Decimal("0.95"),
                ),
                pt_amount=Decimal("2.0"),
            )
            assert result.current_value_usd is None, bad
            assert result.current_value_usd != Decimal("0"), bad
            assert result.confidence == ValueConfidence.UNAVAILABLE, bad


class TestValuePendlePositionYT:
    """YT-only valuation from the gateway YT/USD mark (VIB-5322).

    For a YT symbol ``pt_price.price`` IS the composed YT/USD mark
    (``(1 − pt_to_asset_rate) × underlying/USD``). The valuer multiplies the
    held quantity by that mark and propagates confidence verbatim. The defining
    YT difference vs PT: a MEASURED ``price == 0`` (a fully-decayed post-maturity
    YT) is a VALID value, not an unmeasured/unavailable one.
    """

    def test_yt_high_confidence(self):
        """HIGH gateway YT mark → value = yt_amount × price, confidence propagated."""
        result = value_pendle_position(
            pt_price=_pt_price(
                price=Decimal("0.10"),
                confidence=ValueConfidence.HIGH,
                underlying_price=Decimal("2.00"),
                pt_to_asset_rate=Decimal("0.05"),  # YT complement rate echoed by the gateway
                days_to_maturity=120,
                symbol="YT-sUSDe-13AUG2026",
            ),
            yt_amount=Decimal("100"),
        )
        assert result.current_value_usd == Decimal("10.00")  # 100 × $0.10
        assert result.pt_component_usd == Decimal("10.00")
        assert result.sy_component_usd is None
        assert result.confidence == ValueConfidence.HIGH
        assert result.unavailable_reason == ""
        assert result.days_to_maturity == 120
        # The PT pull-to-par APY formula does not describe a YT's return → None,
        # never a misleading PT-style number off the YT rate (Empty ≠ Zero).
        assert result.implied_apy_bps is None

    def test_yt_at_maturity_measured_zero_is_valid(self):
        """A MEASURED $0 mark (post-maturity worthless YT) → value Decimal("0"), HIGH.

        Empty ≠ Zero in the OTHER direction: a YT mark of exactly 0 is a real,
        measured value (the gateway composed it), NOT an unmeasured price. The
        value must be ``Decimal("0")`` with the gateway confidence, never None.
        """
        result = value_pendle_position(
            pt_price=_pt_price(
                price=Decimal("0"),
                confidence=ValueConfidence.HIGH,
                underlying_price=Decimal("1.50"),
                pt_to_asset_rate=Decimal("0"),
                days_to_maturity=0,
                symbol="YT-sUSDe-13AUG2026",
            ),
            yt_amount=Decimal("100"),
        )
        assert result.current_value_usd == Decimal("0")
        assert result.current_value_usd is not None  # measured zero, NOT unmeasured None
        assert result.confidence == ValueConfidence.HIGH

    def test_yt_unavailable_is_unmeasured_not_zero(self):
        """UNAVAILABLE gateway price → value None (Empty ≠ Zero), distinct from a measured 0."""
        result = value_pendle_position(
            pt_price=_pt_price(price=None, confidence=ValueConfidence.UNAVAILABLE),
            yt_amount=Decimal("100"),
        )
        assert result.current_value_usd is None  # NOT Decimal("0")
        assert result.confidence == ValueConfidence.UNAVAILABLE
        assert result.unavailable_reason

    def test_yt_negative_price_is_unavailable(self):
        """A negative YT mark is never a measured value → unavailable (Empty ≠ Zero).

        The gateway floors YT at $0, so a negative price can only arrive on a
        corrupt/leaked response; the valuer fails closed rather than book a
        negative NAV contribution.
        """
        result = value_pendle_position(
            pt_price=_pt_price(
                price=Decimal("-0.01"),
                confidence=ValueConfidence.HIGH,
                underlying_price=Decimal("2.00"),
                pt_to_asset_rate=Decimal("1.005"),
                symbol="YT-sUSDe-13AUG2026",
            ),
            yt_amount=Decimal("100"),
        )
        assert result.current_value_usd is None
        assert result.confidence == ValueConfidence.UNAVAILABLE

    def test_yt_estimated_and_stale_confidence_propagated(self):
        """ESTIMATED / STALE gateway price → valued, confidence propagated (not upgraded)."""
        for conf in (ValueConfidence.ESTIMATED, ValueConfidence.STALE):
            result = value_pendle_position(
                pt_price=_pt_price(
                    price=Decimal("0.20"),
                    confidence=conf,
                    underlying_price=Decimal("2.00"),
                    pt_to_asset_rate=Decimal("0.90"),
                    symbol="YT-sUSDe-13AUG2026",
                ),
                yt_amount=Decimal("50"),
            )
            assert result.current_value_usd == Decimal("10.00"), conf  # 50 × $0.20
            assert result.confidence == conf
            assert result.unavailable_reason  # non-empty degradation note


class TestValuePendlePositionSY:
    """SY-only valuation from the underlying/USD leg."""

    def test_sy_from_underlying_leg(self):
        result = value_pendle_position(
            pt_price=_pt_price(
                price=Decimal("950"),
                confidence=ValueConfidence.HIGH,
                underlying_price=Decimal("1000"),
            ),
            sy_amount=Decimal("3.0"),
        )
        assert result.current_value_usd == Decimal("3000")  # 3 × $1000
        assert result.sy_component_usd == Decimal("3000")
        assert result.pt_component_usd is None
        assert result.confidence == ValueConfidence.HIGH

    def test_sy_underlying_unmeasured_is_unavailable(self):
        result = value_pendle_position(
            pt_price=_pt_price(price=None, confidence=ValueConfidence.UNAVAILABLE),
            sy_amount=Decimal("3.0"),
        )
        assert result.current_value_usd is None
        assert result.confidence == ValueConfidence.UNAVAILABLE

    def test_sy_unavailable_confidence_with_leaked_price_is_unavailable(self):
        """Fail closed (Gemini, VIB-5313): even if an underlying_price leaks through
        on an UNAVAILABLE band, the SY value must be None (Empty ≠ Zero) — the band
        is authoritative, never overridden by a stray measured-looking leg."""
        result = value_pendle_position(
            pt_price=_pt_price(
                price=None,
                confidence=ValueConfidence.UNAVAILABLE,
                underlying_price=Decimal("1000"),  # leaked despite UNAVAILABLE band
            ),
            sy_amount=Decimal("3.0"),
        )
        assert result.current_value_usd is None
        assert result.current_value_usd != Decimal("0")
        assert result.confidence == ValueConfidence.UNAVAILABLE


class TestValuePendlePositionLP:
    """LP valuation: SY from underlying/USD, PT from the PT/USD mark."""

    def test_lp_decomposition(self):
        result = value_pendle_position(
            pt_price=_pt_price(
                price=Decimal("950"),
                confidence=ValueConfidence.HIGH,
                underlying_price=Decimal("1000"),
                pt_to_asset_rate=Decimal("0.95"),
            ),
            lp_amount=Decimal("10"),
            lp_pool_sy_amount=Decimal("1000"),
            lp_pool_pt_amount=Decimal("500"),
            lp_total_supply=Decimal("100"),
        )
        # 10% of pool = 100 SY + 50 PT
        # SY = 100 × $1000 = $100_000 ; PT = 50 × $950 = $47_500
        assert result.current_value_usd == Decimal("147500")
        assert result.sy_component_usd == Decimal("100000")
        assert result.pt_component_usd == Decimal("47500")
        assert result.confidence == ValueConfidence.HIGH

    def test_lp_unmeasured_price_is_unavailable(self):
        """UNAVAILABLE gateway price → LP value None (Empty ≠ Zero), never 0."""
        result = value_pendle_position(
            pt_price=_pt_price(price=None, confidence=ValueConfidence.UNAVAILABLE),
            lp_amount=Decimal("10"),
            lp_pool_sy_amount=Decimal("1000"),
            lp_pool_pt_amount=Decimal("500"),
            lp_total_supply=Decimal("100"),
        )
        assert result.current_value_usd is None
        assert result.current_value_usd != Decimal("0")
        assert result.confidence == ValueConfidence.UNAVAILABLE

    def test_lp_fallback_without_reserves_is_estimated(self):
        """No pool reserves → lp_amount × underlying approximation, ESTIMATED."""
        result = value_pendle_position(
            pt_price=_pt_price(
                price=Decimal("1900"),
                confidence=ValueConfidence.HIGH,
                underlying_price=Decimal("2000"),
            ),
            lp_amount=Decimal("5.0"),
        )
        assert result.current_value_usd == Decimal("10000")  # 5 × $2000
        assert result.confidence == ValueConfidence.ESTIMATED
        assert "lp_pool_reserves not provided" in result.unavailable_reason

    def test_lp_fallback_keeps_stale_confidence(self):
        """Fallback never upgrades a worse gateway confidence (STALE stays STALE)."""
        result = value_pendle_position(
            pt_price=_pt_price(
                price=Decimal("1900"),
                confidence=ValueConfidence.STALE,
                underlying_price=Decimal("2000"),
            ),
            lp_amount=Decimal("5.0"),
        )
        assert result.current_value_usd == Decimal("10000")
        assert result.confidence == ValueConfidence.STALE


class TestValuePendlePositionGuards:
    """Input-validation + no-data guards."""

    def test_no_position_data_returns_unavailable(self):
        result = value_pendle_position(
            pt_price=_pt_price(price=Decimal("950"), confidence=ValueConfidence.HIGH),
        )
        assert result.confidence == ValueConfidence.UNAVAILABLE
        assert result.current_value_usd is None

    def test_ambiguous_multi_type_raises(self):
        try:
            value_pendle_position(
                pt_price=_pt_price(price=Decimal("950"), confidence=ValueConfidence.HIGH),
                pt_amount=Decimal("1"),
                sy_amount=Decimal("1"),
            )
        except ValueError as e:
            assert "at most one of" in str(e)
        else:
            raise AssertionError("expected ValueError for multi-type input")

    def test_ambiguous_yt_plus_pt_raises(self):
        """Mixing yt_amount with pt_amount is ambiguous → ValueError (VIB-5322)."""
        try:
            value_pendle_position(
                pt_price=_pt_price(price=Decimal("0.10"), confidence=ValueConfidence.HIGH),
                pt_amount=Decimal("1"),
                yt_amount=Decimal("1"),
            )
        except ValueError as e:
            assert "at most one of" in str(e)
            assert "yt_amount" in str(e)
        else:
            raise AssertionError("expected ValueError for yt+pt multi-type input")

    def test_result_type(self):
        result = value_pendle_position(
            pt_price=_pt_price(price=Decimal("950"), confidence=ValueConfidence.HIGH),
            pt_amount=Decimal("1"),
        )
        assert isinstance(result, PendlePositionValue)
