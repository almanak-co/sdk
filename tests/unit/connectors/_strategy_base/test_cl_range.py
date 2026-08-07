"""Unit table for the shared cl_range price-band -> tick-range seam (VIB-5556).

This is the single place the ALM-2901 orientation/decimals math is asserted.
Covers: asymmetric decimals (USDC6/WETH18) in BOTH orientations, negative
current ticks, tick spacings 1/10/50/60/200, collapse rejection, the straddle
invariant (pass/fail), and the allow_out_of_range opt-out.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors._strategy_base.cl_range import (
    PriceBandToTicksError,
    TickRange,
    price_band_to_ticks,
)
from almanak.connectors._strategy_base.concentrated_liquidity_math import price_to_tick


def _expected(price_lower, price_upper, *, d0, d1, spacing):
    """Reference floor-aligned ticks computed straight from the math core."""
    tl = (price_to_tick(Decimal(str(price_lower)), decimals0=d0, decimals1=d1) // spacing) * spacing
    tu = (price_to_tick(Decimal(str(price_upper)), decimals0=d0, decimals1=d1) // spacing) * spacing
    return tl, tu


class TestOrientationAndDecimals:
    def test_symmetric_decimals_no_swap(self):
        result = price_band_to_ticks(
            range_lower=Decimal("1500"),
            range_upper=Decimal("2500"),
            token0_decimals=18,
            token1_decimals=18,
            tokens_swapped=False,
            tick_spacing=60,
        )
        assert isinstance(result, TickRange)
        assert result == TickRange(*_expected("1500", "2500", d0=18, d1=18, spacing=60))
        assert result.tick_lower < result.tick_upper

    def test_asymmetric_usdc6_weth18_no_swap(self):
        # token0=USDC(6), token1=WETH(18): the decimal shift is load-bearing.
        result = price_band_to_ticks(
            range_lower=Decimal("0.0004"),
            range_upper=Decimal("0.0007"),
            token0_decimals=6,
            token1_decimals=18,
            tokens_swapped=False,
            tick_spacing=10,
        )
        assert result == TickRange(*_expected("0.0004", "0.0007", d0=6, d1=18, spacing=10))
        assert result.tick_lower < result.tick_upper

    def test_asymmetric_weth18_usdc6_swapped(self):
        # User states the WETH/USDC band [1500, 2500]; pool order is USDC<WETH so
        # tokens_swapped=True inverts to the reciprocal [1/2500, 1/1500] before
        # the decimals-correct price->tick (d0=USDC 6, d1=WETH 18).
        result = price_band_to_ticks(
            range_lower=Decimal("1500"),
            range_upper=Decimal("2500"),
            token0_decimals=6,
            token1_decimals=18,
            tokens_swapped=True,
            tick_spacing=10,
        )
        inv_lower = Decimal(1) / Decimal("2500")
        inv_upper = Decimal(1) / Decimal("1500")
        assert result == TickRange(*_expected(inv_lower, inv_upper, d0=6, d1=18, spacing=10))
        assert result.tick_lower < result.tick_upper

    def test_alm_2901_usdc6_cbbtc8_matches_the_onchain_tick(self):
        """Bit-exact anchor against the real ALM-2901 pool (VIB-5867).

        Base ``0x4e962bb3...``: token0=USDC(6), token1=cbBTC(8), live tick
        -64744 at a human price of 1.543067e-05. A band straddling that price
        must produce ticks straddling that tick. The user's hand-rolled
        conversion returned -110797 for the same price -- 46,054 ticks (~100x in
        price) away, which is exactly |6-8| * log_1.0001(10).
        """
        result = price_band_to_ticks(
            range_lower=Decimal("1.5e-05"),
            range_upper=Decimal("1.6e-05"),
            token0_decimals=6,
            token1_decimals=8,
            tokens_swapped=False,
            tick_spacing=100,
            current_tick=-64744,
        )
        assert result.tick_lower <= -64744 < result.tick_upper
        # And the same band with symmetric decimals is ~46k ticks off -- i.e. the
        # decimals term is doing real work, not decoration.
        symmetric = price_band_to_ticks(
            range_lower=Decimal("1.5e-05"),
            range_upper=Decimal("1.6e-05"),
            token0_decimals=18,
            token1_decimals=18,
            tokens_swapped=False,
            tick_spacing=100,
        )
        assert result.tick_lower - symmetric.tick_lower == pytest.approx(46054, abs=100)

    def test_alm_2901_pair_swapped_orientation(self):
        """Same pair stated as cbBTC/USDC: the reciprocal band, still straddling."""
        result = price_band_to_ticks(
            range_lower=Decimal(1) / Decimal("1.6e-05"),
            range_upper=Decimal(1) / Decimal("1.5e-05"),
            token0_decimals=6,
            token1_decimals=8,
            tokens_swapped=True,
            tick_spacing=100,
            current_tick=-64744,
        )
        assert result.tick_lower <= -64744 < result.tick_upper

    def test_swap_inverts_band(self):
        # The swapped result must equal feeding the reciprocal band un-swapped.
        swapped = price_band_to_ticks(
            range_lower=Decimal("550"),
            range_upper=Decimal("670"),
            token0_decimals=18,
            token1_decimals=18,
            tokens_swapped=True,
            tick_spacing=10,
        )
        manual = price_band_to_ticks(
            range_lower=Decimal(1) / Decimal("670"),
            range_upper=Decimal(1) / Decimal("550"),
            token0_decimals=18,
            token1_decimals=18,
            tokens_swapped=False,
            tick_spacing=10,
        )
        assert swapped == manual


class TestTickSpacings:
    @pytest.mark.parametrize("spacing", [1, 10, 50, 60, 200])
    def test_outputs_aligned_to_spacing(self, spacing):
        result = price_band_to_ticks(
            range_lower=Decimal("1200"),
            range_upper=Decimal("3400"),
            token0_decimals=6,
            token1_decimals=18,
            tokens_swapped=False,
            tick_spacing=spacing,
        )
        assert result.tick_lower % spacing == 0
        assert result.tick_upper % spacing == 0
        assert result == TickRange(*_expected("1200", "3400", d0=6, d1=18, spacing=spacing))

    def test_non_positive_spacing_rejected(self):
        with pytest.raises(PriceBandToTicksError, match="tick_spacing must be positive"):
            price_band_to_ticks(
                range_lower=Decimal("1500"),
                range_upper=Decimal("2500"),
                token0_decimals=18,
                token1_decimals=18,
                tokens_swapped=False,
                tick_spacing=0,
            )


class TestNegativeCurrentTick:
    def test_negative_band_and_current_tick_straddle(self):
        # token0=WETH(18), token1=USDC(6): a ~2000 USDC/WETH price lands deep in
        # negative tick territory (the decimal shift dominates).
        kwargs = {
            "range_lower": Decimal("1500"),
            "range_upper": Decimal("2500"),
            "token0_decimals": 18,
            "token1_decimals": 6,
            "tokens_swapped": False,
            "tick_spacing": 10,
        }
        result = price_band_to_ticks(**kwargs)
        assert result.tick_lower < 0
        assert result.tick_upper < 0
        mid = result.tick_lower + (result.tick_upper - result.tick_lower) // 2
        # current_tick inside the (negative) band passes the straddle invariant.
        ok = price_band_to_ticks(**kwargs, current_tick=mid)
        assert ok == result


class TestCollapseRejection:
    def test_collapse_after_spacing_rejected(self):
        # A band narrower than one spacing bucket floors to the same tick.
        with pytest.raises(PriceBandToTicksError, match="collapsed"):
            price_band_to_ticks(
                range_lower=Decimal("2000.00"),
                range_upper=Decimal("2000.01"),
                token0_decimals=18,
                token1_decimals=18,
                tokens_swapped=False,
                tick_spacing=200,
            )

    def test_non_positive_price_fails_closed(self):
        with pytest.raises(ValueError):
            price_band_to_ticks(
                range_lower=Decimal("0"),
                range_upper=Decimal("2500"),
                token0_decimals=18,
                token1_decimals=18,
                tokens_swapped=False,
                tick_spacing=60,
            )

    def test_non_invertible_swapped_band_rejected(self):
        with pytest.raises(PriceBandToTicksError, match="positive to invert"):
            price_band_to_ticks(
                range_lower=Decimal("0"),
                range_upper=Decimal("2500"),
                token0_decimals=18,
                token1_decimals=18,
                tokens_swapped=True,
                tick_spacing=60,
            )


class TestStraddleInvariant:
    def _band(self, **kw):
        base = {
            "range_lower": Decimal("1500"),
            "range_upper": Decimal("2500"),
            "token0_decimals": 18,
            "token1_decimals": 18,
            "tokens_swapped": False,
            "tick_spacing": 60,
        }
        base.update(kw)
        return price_band_to_ticks(**base)

    def test_straddle_passes_when_current_tick_inside(self):
        ref = self._band()
        mid = ref.tick_lower + (ref.tick_upper - ref.tick_lower) // 2
        assert self._band(current_tick=mid) == ref

    def test_straddle_fails_when_current_tick_below(self):
        ref = self._band()
        with pytest.raises(PriceBandToTicksError, match="does not straddle"):
            self._band(current_tick=ref.tick_lower - 60)

    def test_straddle_fails_when_current_tick_at_upper_bound(self):
        # Upper bound is exclusive: current_tick == tick_upper is out of range.
        ref = self._band()
        with pytest.raises(PriceBandToTicksError, match="does not straddle"):
            self._band(current_tick=ref.tick_upper)

    def test_current_tick_none_skips_straddle(self):
        # Out-of-range band with no current_tick supplied -> no straddle check.
        result = self._band(current_tick=None)
        assert isinstance(result, TickRange)

    def test_allow_out_of_range_opt_out(self):
        ref = self._band()
        # current_tick below the band would normally fail, but the opt-out allows
        # a one-sided open.
        result = self._band(current_tick=ref.tick_lower - 600, allow_out_of_range=True)
        assert result == ref

    def test_require_straddle_false_opt_out(self):
        ref = self._band()
        result = self._band(current_tick=ref.tick_upper + 600, require_straddle=False)
        assert result == ref


# ---------------------------------------------------------------------------
# require_lp_tolerance_fits_range (VIB-6567)
# ---------------------------------------------------------------------------


class TestRequireLpToleranceFitsRange:
    """An LP price band must fit inside the range it is meant to protect.

    The predicate these tests pin was DERIVED FROM MEASUREMENT, not from algebra:
    the original sweep drove the real ``price_band_to_ticks`` ->
    ``compute_lp_slippage_mins`` path at 400 spot offsets per cell and recorded
    where a leg's minimum came out zero. The committed regression sweep below
    covers all 5 canonical V3 tick spacings x 11 range widths x 120 offsets per
    admitted cell. It proves the one-way safety guarantee: every admitted
    configuration produces a positive minimum on both legs. It deliberately does
    not claim completeness for refused cells; a boot-time guard that cannot read
    spot may refuse earlier than live geometry requires.

    Mutation matrix, so the coverage claim is checkable rather than asserted:
    dropping the ``- (spacing - 1)`` term, reading a fee tier as a raw spacing,
    and removing the scaffold's call all turn this suite RED.

    ONE MUTANT SURVIVES AND IS ACCEPTED: weakening ``>=`` to ``>``. It is
    equivalent for every reachable input -- both sides are ratios of logarithms,
    so exact ``Decimal`` equality does not occur. (The previous price-domain guard
    DID have a reachable equality at ``range_width_pct=0.5`` / ``max_slippage
    =0.005``, which is why that one was pinned.) ``>=`` is still the correct
    comparison: at equality the band edge lands exactly on the realised bound and
    the leg is worth zero there. Recorded rather than papered over with a test
    that cannot actually discriminate.
    """

    @staticmethod
    def _call(**kw):
        from almanak.connectors._strategy_base.cl_range import require_lp_tolerance_fits_range

        base = {
            "max_slippage": Decimal("0.005"),
            "range_half_width_frac": Decimal("0.05"),
            "pool": "WETH/USDC/3000",
            "pool_encodes_tick_spacing": False,
        }
        return require_lp_tolerance_fits_range(**{**base, **kw})

    def test_undeclared_tolerance_is_not_checked(self):
        """None means the price-band instrument is never selected — nothing to check.

        This must NOT refuse: it is the pre-existing default path, and turning it
        into a boot failure would break every strategy that omits the field.
        """
        self._call(max_slippage=None)

    def test_scaffold_defaults_are_accepted(self):
        """+-5% range at 0.5% tolerance on both default pools."""
        self._call()
        self._call(pool="WETH/USDC/200", pool_encodes_tick_spacing=True)

    def test_spacing_is_what_makes_the_check_exact(self):
        """Same range, same tolerance, different spacing -> different verdict.

        This is the whole point of VIB-6567 and the one behaviour a
        requested-half-width comparison cannot produce. A +-1% range at 0.5%
        tolerance is SAFE on a 10-tick spacing and UNSAFE on a 60-tick one,
        because flooring eats up to `spacing - 1` ticks of the realised bound.
        """
        from almanak.connectors._strategy_base.cl_range import LpToleranceOutOfRangeError

        self._call(pool="WETH/USDC/500", range_half_width_frac=Decimal("0.01"))
        with pytest.raises(LpToleranceOutOfRangeError, match="does not fit inside the range"):
            self._call(pool="WETH/USDC/3000", range_half_width_frac=Decimal("0.01"))

    def test_fee_tier_is_not_read_as_a_tick_spacing(self):
        """`3000` is a fee tier (spacing 60), not a 3000-tick spacing.

        Reading it as a spacing would demand ~3000 ticks of headroom and refuse
        the scaffold's own default. The caller states the encoding; it is never
        guessed from the number.
        """
        self._call(pool="WETH/USDC/3000", pool_encodes_tick_spacing=False)

    @pytest.mark.parametrize("bad", [Decimal("0"), Decimal("1"), Decimal("1.5"), Decimal("-0.01")])
    def test_tolerance_outside_open_unit_interval_is_refused(self, bad):
        """Closes the gap `cl_math`'s `[0, 1)` refusal leaves on `intent.max_slippage`."""
        from almanak.connectors._strategy_base.cl_range import LpToleranceOutOfRangeError

        with pytest.raises(LpToleranceOutOfRangeError, match=r"positive fraction below 1"):
            self._call(max_slippage=bad)

    @pytest.mark.parametrize("bad", ["not-a-number", "NaN", "Infinity"])
    def test_malformed_or_non_finite_tolerance_is_actionable(self, bad):
        from almanak.connectors._strategy_base.cl_range import LpToleranceOutOfRangeError

        with pytest.raises(LpToleranceOutOfRangeError, match="max_slippage.*positive fraction below 1"):
            self._call(max_slippage=bad)

    @pytest.mark.parametrize(
        "bad",
        [Decimal("0"), Decimal("-0.05"), Decimal("1"), Decimal("1.01"), "NaN", "Infinity", "not-a-number"],
    )
    def test_non_positive_range_names_the_range_not_the_tolerance(self, bad):
        """A non-finite or non-positive-lower-bound range is its own defect."""
        from almanak.connectors._strategy_base.cl_range import LpToleranceOutOfRangeError

        with pytest.raises(LpToleranceOutOfRangeError, match="range half-width.*between 0 and 100"):
            self._call(range_half_width_frac=bad)

    @pytest.mark.parametrize("pool", ["WETHUSDC", "WETH/USDC", "WETH/USDC/wide"])
    def test_unresolvable_spacing_fails_closed(self, pool):
        """If spacing cannot be resolved the check cannot run — refuse, never assume."""
        from almanak.connectors._strategy_base.cl_range import LpToleranceOutOfRangeError

        with pytest.raises(LpToleranceOutOfRangeError):
            self._call(pool=pool)

    def test_message_names_both_numbers_and_all_three_remedies(self):
        """A refusal a user cannot act on is a stall, not a guard."""
        from almanak.connectors._strategy_base.cl_range import LpToleranceOutOfRangeError

        with pytest.raises(LpToleranceOutOfRangeError) as exc:
            self._call(pool="WETH/USDC/10000", range_half_width_frac=Decimal("0.02"))
        msg = str(exc.value)
        assert "ticks" in msg and "200-tick spacing" in msg
        assert "Widen the range" in msg and "tighten max_slippage" in msg and "finer spacing" in msg

    def test_guard_admits_nothing_that_measurably_zeroes_a_leg(self):
        """Negative control against the REAL path, not against the predicate.

        Drives price_band_to_ticks -> compute_lp_slippage_mins across spot offsets
        and asserts: every configuration the guard ADMITS produces a positive
        minimum on both legs at every offset. A guard that agrees with its own
        formula proves nothing; this compares it to emitted calldata.
        """
        from almanak.connectors._strategy_base.base.cl_math import compute_lp_slippage_mins
        from almanak.connectors._strategy_base.cl_range import (
            LpToleranceOutOfRangeError,
            price_band_to_ticks,
        )
        from almanak.connectors._strategy_base.concentrated_liquidity_math import (
            V3_TICK_SPACING,
            price_to_tick,
        )
        from almanak.framework.intents.compiler import LP_SLIPPAGE_DEFAULT
        from almanak.framework.intents.lp_math import tick_to_sqrt_ratio_x96

        class _Intent:
            protocol_params = None

            def __init__(self, ms):
                self.max_slippage = ms

        tol = Decimal("0.005")
        a0, a1 = 500 * 10**15, 1000 * 10**6
        spacings = tuple(sorted(set(V3_TICK_SPACING.values())))
        admitted = 0
        for spacing in spacings:
            for rwp in (
                "10.0",
                "5.0",
                "3.0",
                "2.5",
                "2.0",
                "1.5",
                "1.2",
                "1.0",
                "0.8",
                "0.6",
                "0.4",
            ):
                half = Decimal(rwp) / Decimal("100")
                try:
                    self._call(
                        max_slippage=tol,
                        range_half_width_frac=half,
                        pool=f"WETH/USDC/{spacing}",
                        pool_encodes_tick_spacing=True,
                    )
                except LpToleranceOutOfRangeError:
                    continue  # refused: not this test's subject
                admitted += 1
                evaluated_offsets = 0
                for k in range(120):
                    p = Decimal("3000") * (Decimal(1) + Decimal(k) / Decimal(9000))
                    band = price_band_to_ticks(
                        range_lower=p * (1 - half),
                        range_upper=p * (1 + half),
                        token0_decimals=18,
                        token1_decimals=6,
                        tokens_swapped=True,
                        tick_spacing=spacing,
                        current_tick=None,
                        require_straddle=False,
                    )
                    spot = price_to_tick(Decimal(1) / p, decimals0=18, decimals1=6)
                    if not (band.tick_lower <= spot < band.tick_upper):
                        continue
                    evaluated_offsets += 1
                    m0, m1 = compute_lp_slippage_mins(
                        intent=_Intent(tol),
                        amount0_desired=a0,
                        amount1_desired=a1,
                        default_lp_slippage=LP_SLIPPAGE_DEFAULT,
                        sqrt_price_x96=tick_to_sqrt_ratio_x96(spot),
                        tick_lower=band.tick_lower,
                        tick_upper=band.tick_upper,
                    )
                    assert m0 > 0 and m1 > 0, (
                        f"guard ADMITTED spacing={spacing} half={half} but the emitted "
                        f"minimums were ({m0}, {m1}) at spot tick {spot} — an admitted "
                        f"configuration shipped an unfloored leg, which is the hole "
                        f"VIB-6567 exists to close."
                    )
                assert evaluated_offsets > 0, (
                    f"sweep degenerate: admitted spacing={spacing} half={half}, but "
                    f"none of its spot offsets exercised the minimum-amount assertion"
                )
        assert admitted >= 25, (
            f"sweep degenerate: only {admitted} configurations admitted out of "
            f"{len(spacings) * 11} — "
            f"a guard that refuses nearly everything would vacuously satisfy the "
            f"assertion above, since it is only checked on ADMITTED cells"
        )
