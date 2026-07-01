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
