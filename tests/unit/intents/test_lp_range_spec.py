"""Typed RangeSpec on LPOpenIntent — VIB-5555 / FOUND-1.

Covers the discriminated union (:class:`PriceBand` | :class:`TickBand`), the
backward-compat bridge from the legacy ``range_lower``/``range_upper`` fields,
the Slipstream legacy-tick deprecation path, serialize/deserialize round-trips
for both variants, and rejection of malformed shapes.
"""

import warnings
from decimal import Decimal

import pytest
from pydantic import ValidationError

from almanak.framework.intents import (
    Intent,
    LPOpenIntent,
    PriceBand,
    TickBand,
)

POOL = "0x" + "a" * 40


def _legacy(**overrides):
    base = {
        "pool": POOL,
        "amount0": Decimal("1"),
        "amount1": Decimal("1000"),
        "range_lower": Decimal("1800"),
        "range_upper": Decimal("2200"),
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Variant construct + validate
# ---------------------------------------------------------------------------


class TestRangeSpecVariants:
    def test_price_band_construct(self):
        band = PriceBand(lower=Decimal("1800"), upper=Decimal("2200"))
        assert band.kind == "price"
        assert band.lower == Decimal("1800")
        assert band.upper == Decimal("2200")

    def test_tick_band_construct_negative(self):
        band = TickBand(lower=-5000, upper=2000)
        assert band.kind == "tick"
        assert band.lower == -5000
        assert band.upper == 2000

    def test_price_band_non_positive_rejected(self):
        with pytest.raises(ValidationError, match="PriceBand.lower must be positive"):
            PriceBand(lower=Decimal("0"), upper=Decimal("1"))

    def test_price_band_order_rejected(self):
        with pytest.raises(ValidationError, match="PriceBand.lower must be less than PriceBand.upper"):
            PriceBand(lower=Decimal("5"), upper=Decimal("1"))

    def test_tick_band_order_rejected(self):
        with pytest.raises(ValidationError, match="TickBand.lower must be less than TickBand.upper"):
            TickBand(lower=100, upper=100)

    def test_tick_band_rejects_non_int(self):
        # Strict mode: bool / float / str are not valid ints.
        with pytest.raises(ValidationError):
            TickBand(lower=True, upper=10)

    def test_price_band_rejects_float(self):
        # SafeDecimal rejects float input for precision safety.
        with pytest.raises(ValidationError):
            PriceBand(lower=1800.5, upper=2200.0)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Canonical path: range_spec drives derived legacy bounds
# ---------------------------------------------------------------------------


class TestCanonicalRangeSpec:
    def test_price_band_derives_legacy_bounds(self):
        intent = Intent.lp_open(
            pool=POOL,
            amount0=Decimal("1"),
            range_spec=PriceBand(lower=Decimal("1800"), upper=Decimal("2200")),
        )
        assert isinstance(intent.range_spec, PriceBand)
        assert intent.range_lower == Decimal("1800")
        assert intent.range_upper == Decimal("2200")

    def test_tick_band_derives_legacy_bounds(self):
        intent = Intent.lp_open(
            pool=POOL,
            amount0=Decimal("1"),
            range_spec=TickBand(lower=-5000, upper=2000),
            protocol="aerodrome_slipstream",
        )
        assert isinstance(intent.range_spec, TickBand)
        assert intent.range_lower == Decimal("-5000")
        assert intent.range_upper == Decimal("2000")

    def test_tick_band_skips_price_positivity_on_tick_protocol(self):
        # A TickBand with a negative lower must not trip "range_lower must be
        # positive" on a tick-based protocol whose compiler consumes raw ticks.
        intent = LPOpenIntent(
            pool=POOL,
            amount0=Decimal("1"),
            amount1=Decimal("1"),
            range_spec=TickBand(lower=-200, upper=200),
            protocol="aerodrome_slipstream",
        )
        assert intent.range_lower == Decimal("-200")

    def test_tick_band_rejected_on_price_protocol(self):
        # Fail-closed (VIB-5555): a TickBand on a price-based protocol would be
        # misexecuted as a price by the compiler (which has no range_spec seam
        # yet — VIB-5556), so it is rejected rather than silently accepted with
        # the positivity guard disabled.
        with pytest.raises(ValidationError, match="TickBand range_spec is only valid for tick-based"):
            LPOpenIntent(
                pool=POOL,
                amount0=Decimal("1"),
                amount1=Decimal("1"),
                range_spec=TickBand(lower=-200, upper=200),
                protocol="uniswap_v3",
            )

    def test_conflicting_legacy_bounds_rejected(self):
        with pytest.raises(ValidationError, match="range_spec conflicts with range_lower"):
            LPOpenIntent(
                pool=POOL,
                amount0=Decimal("1"),
                amount1=Decimal("1"),
                range_lower=Decimal("999"),
                range_upper=Decimal("2200"),
                range_spec=PriceBand(lower=Decimal("1800"), upper=Decimal("2200")),
            )

    def test_agreeing_legacy_bounds_accepted(self):
        # Round-trip shape: spec + matching legacy bounds is fine.
        intent = LPOpenIntent(
            pool=POOL,
            amount0=Decimal("1"),
            amount1=Decimal("1"),
            range_lower=Decimal("1800"),
            range_upper=Decimal("2200"),
            range_spec=PriceBand(lower=Decimal("1800"), upper=Decimal("2200")),
        )
        assert isinstance(intent.range_spec, PriceBand)


# ---------------------------------------------------------------------------
# Legacy bridge: range_lower/range_upper -> synthesised range_spec
# ---------------------------------------------------------------------------


class TestLegacyBridge:
    def test_price_protocol_maps_to_price_band(self):
        intent = LPOpenIntent(**_legacy())
        assert isinstance(intent.range_spec, PriceBand)
        assert intent.range_spec.lower == Decimal("1800")
        assert intent.range_spec.upper == Decimal("2200")

    def test_price_protocol_integer_bounds_stay_price(self):
        # Integer-valued bounds on a non-tick protocol remain prices (the tick
        # heuristic only applies to tick-based protocols).
        intent = LPOpenIntent(**_legacy(range_lower=Decimal("1800"), range_upper=Decimal("2200")))
        assert isinstance(intent.range_spec, PriceBand)

    def test_factory_defaults_map_to_price_band(self):
        intent = Intent.lp_open(pool=POOL, amount0=Decimal("1"))
        assert isinstance(intent.range_spec, PriceBand)
        assert intent.range_lower == Decimal("1")
        assert intent.range_upper == Decimal("2")

    def test_slipstream_tick_shaped_maps_to_tick_band_with_warning(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            intent = Intent.lp_open(
                pool=POOL,
                amount0=Decimal("1"),
                range_lower=Decimal("-5000"),
                range_upper=Decimal("-2000"),
                protocol="aerodrome_slipstream",
            )
        assert isinstance(intent.range_spec, TickBand)
        assert intent.range_spec.lower == -5000
        assert intent.range_spec.upper == -2000
        # Legacy on-chain tick semantics preserved on the legacy fields.
        assert intent.range_lower == Decimal("-5000")
        assert intent.range_upper == Decimal("-2000")
        assert any(issubclass(w.category, DeprecationWarning) for w in caught)

    def test_slipstream_positive_integer_bounds_are_ambiguous_and_rejected(self):
        """BEHAVIOUR CHANGE (VIB-5867). Was: silently interpreted as ticks.

        This test previously asserted that ``[2000, 5000]`` on Slipstream maps to
        a ``TickBand`` -- i.e. the bridge GUESSED "ticks" for a pair that is an
        equally valid, and far more natural, WETH/USDC *price* band. That guess is
        a money bug: the same user input mints two completely different positions
        depending on a coin-flip the caller never sees, and it flips based on
        nothing more than whether the live price happens to be a round number
        (``[2000, 5000]`` -> ticks, ``[2000.5, 5000.5]`` -> prices).

        The design (``docs/internal/unified-lp-range-ux-design.md`` §Migration
        Step 1) specifies rejection here: "zero / positive-integer values are
        ambiguous -> require the explicit flag and reject otherwise ... a
        whole-number price band like range_lower=2000.0 must never be silently
        reinterpreted as a tick". The landed Step-1 heuristic deviated from that;
        this restores it. Unambiguous forms (negative -> ticks, fractional ->
        prices) are unchanged, and an explicit PriceBand/TickBand always wins.
        """
        with pytest.raises(ValidationError, match="Ambiguous LP range"):
            Intent.lp_open(
                pool=POOL,
                amount0=Decimal("1"),
                range_lower=Decimal("2000"),
                range_upper=Decimal("5000"),
                protocol="aerodrome_slipstream",
            )

    def test_slipstream_positive_integer_bounds_accepted_when_form_is_explicit(self):
        """The escape from the ambiguity: say which form you meant."""
        as_ticks = Intent.lp_open(
            pool=POOL,
            amount0=Decimal("1"),
            range_spec=TickBand(lower=2000, upper=5000),
            protocol="aerodrome_slipstream",
        )
        assert isinstance(as_ticks.range_spec, TickBand)

        as_prices = Intent.lp_open(
            pool=POOL,
            amount0=Decimal("1"),
            range_spec=PriceBand(lower=Decimal("2000"), upper=Decimal("5000")),
            protocol="aerodrome_slipstream",
        )
        assert isinstance(as_prices.range_spec, PriceBand)

    def test_slipstream_non_integral_tick_bound_rejected(self):
        # A non-positive (tick-shaped) but fractional legacy bound on a tick-based
        # protocol would be silently truncated by int(), breaking the
        # serialize/deserialize round-trip. Reject fail-closed instead.
        with pytest.raises(ValidationError, match="must be integer-valued"):
            Intent.lp_open(
                pool=POOL,
                amount0=Decimal("1"),
                range_lower=Decimal("-5000.5"),
                range_upper=Decimal("-2000.5"),
                protocol="aerodrome_slipstream",
            )

    def test_slipstream_fractional_maps_to_price_band_no_warning(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            intent = Intent.lp_open(
                pool=POOL,
                amount0=Decimal("1"),
                range_lower=Decimal("1800.5"),
                range_upper=Decimal("2200.5"),
                protocol="aerodrome_slipstream",
            )
        assert isinstance(intent.range_spec, PriceBand)
        assert not any(issubclass(w.category, DeprecationWarning) for w in caught)


# ---------------------------------------------------------------------------
# Serialize / deserialize round-trip
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_price_band_round_trip(self):
        intent = Intent.lp_open(
            pool=POOL,
            amount0=Decimal("1"),
            range_spec=PriceBand(lower=Decimal("1800"), upper=Decimal("2200")),
        )
        data = intent.serialize()
        assert data["range_spec"] == {"kind": "price", "lower": "1800", "upper": "2200"}
        rebuilt = LPOpenIntent.deserialize(data)
        assert rebuilt.range_spec == intent.range_spec
        assert rebuilt.range_lower == intent.range_lower
        assert rebuilt.range_upper == intent.range_upper

    def test_tick_band_round_trip(self):
        intent = Intent.lp_open(
            pool=POOL,
            amount0=Decimal("1"),
            range_spec=TickBand(lower=-5000, upper=2000),
            protocol="aerodrome_slipstream",
        )
        data = intent.serialize()
        assert data["range_spec"] == {"kind": "tick", "lower": -5000, "upper": 2000}
        rebuilt = LPOpenIntent.deserialize(data)
        assert isinstance(rebuilt.range_spec, TickBand)
        assert rebuilt.range_spec == intent.range_spec
        assert rebuilt.range_lower == intent.range_lower

    def test_malformed_serialized_range_spec_rejected(self):
        # A serialized range_spec dict missing lower/upper must surface a clean
        # ValidationError, not a raw KeyError, on the deserialize path.
        data = LPOpenIntent(**_legacy()).serialize()
        data["range_spec"] = {"kind": "price"}  # missing lower/upper
        with pytest.raises(ValidationError, match="range_spec must include 'kind', 'lower', and 'upper'"):
            LPOpenIntent.deserialize(data)

    def test_legacy_intent_round_trip_includes_spec(self):
        intent = LPOpenIntent(**_legacy(chain="arbitrum"))
        rebuilt = LPOpenIntent.deserialize(intent.serialize())
        assert isinstance(rebuilt.range_spec, PriceBand)
        assert rebuilt.range_spec == intent.range_spec
