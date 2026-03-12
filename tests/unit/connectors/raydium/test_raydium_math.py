"""Tests for Raydium CLMM tick and liquidity math (VIB-371).

Verifies:
1. Price ↔ tick conversion (including decimal adjustment)
2. Tick alignment to spacing
3. Sqrt price conversions (Q64.64)
4. Liquidity calculation from amounts
5. Tick array start index computation
"""

from decimal import Decimal

import pytest

from almanak.framework.connectors.raydium.constants import MAX_TICK, MIN_TICK, Q64
from almanak.framework.connectors.raydium.exceptions import RaydiumTickError
from almanak.framework.connectors.raydium.math import (
    align_tick_to_spacing,
    get_amounts_from_liquidity,
    get_liquidity_from_amounts,
    price_to_tick,
    sqrt_price_x64_to_tick,
    tick_array_start_index,
    tick_to_price,
    tick_to_sqrt_price_x64,
)


class TestPriceToTick:
    """price_to_tick() converts decimal prices to tick indices."""

    def test_sol_usdc_price_150(self):
        """SOL at $150 USDC (9 decimals SOL, 6 decimals USDC)."""
        tick = price_to_tick(Decimal("150"), decimals_a=9, decimals_b=6)
        # SOL/USDC with decimal adjustment: 150 * 10^(6-9) = 0.15
        # log(0.15) / log(1.0001) ≈ -18973
        assert -19100 < tick < -18900

    def test_price_of_1_same_decimals(self):
        """Price=1 with same decimals should give tick≈0."""
        tick = price_to_tick(Decimal("1"), decimals_a=6, decimals_b=6)
        assert tick == 0

    def test_very_small_price(self):
        """Very small price should give negative tick."""
        tick = price_to_tick(Decimal("0.001"), decimals_a=6, decimals_b=6)
        assert tick < 0

    def test_negative_price_raises(self):
        with pytest.raises(RaydiumTickError, match="positive"):
            price_to_tick(Decimal("-1"), decimals_a=6, decimals_b=6)

    def test_zero_price_raises(self):
        with pytest.raises(RaydiumTickError, match="positive"):
            price_to_tick(Decimal("0"), decimals_a=6, decimals_b=6)


class TestTickToPrice:
    """tick_to_price() converts tick indices back to prices."""

    def test_roundtrip_same_decimals(self):
        """Price → tick → price roundtrip with same decimals."""
        original = Decimal("100")
        tick = price_to_tick(original, 6, 6)
        recovered = tick_to_price(tick, 6, 6)
        # Should be within 0.01% of original (tick quantization)
        assert abs(recovered - original) / original < Decimal("0.001")

    def test_tick_zero_is_price_one(self):
        """Tick 0 corresponds to price 1.0 (adjusted for decimals)."""
        price = tick_to_price(0, 6, 6)
        assert abs(price - Decimal("1")) < Decimal("0.001")


class TestAlignTickToSpacing:
    """align_tick_to_spacing() rounds ticks to valid spacing boundaries."""

    def test_positive_tick_round_down(self):
        aligned = align_tick_to_spacing(65, 60, round_up=False)
        assert aligned == 60

    def test_positive_tick_round_up(self):
        aligned = align_tick_to_spacing(61, 60, round_up=True)
        assert aligned == 120

    def test_already_aligned(self):
        aligned = align_tick_to_spacing(120, 60, round_up=False)
        assert aligned == 120

    def test_negative_tick_round_down(self):
        aligned = align_tick_to_spacing(-65, 60, round_up=False)
        assert aligned == -120

    def test_negative_tick_round_up(self):
        aligned = align_tick_to_spacing(-65, 60, round_up=True)
        assert aligned == -60

    def test_zero_tick(self):
        aligned = align_tick_to_spacing(0, 60)
        assert aligned == 0

    def test_clamp_to_max(self):
        """Very large tick should be clamped to MAX_TICK."""
        aligned = align_tick_to_spacing(MAX_TICK + 100, 60, round_up=True)
        assert aligned <= MAX_TICK

    def test_clamp_to_min(self):
        """Very negative tick should be clamped to MIN_TICK."""
        aligned = align_tick_to_spacing(MIN_TICK - 100, 60, round_up=False)
        assert aligned >= MIN_TICK

    def test_zero_spacing_raises(self):
        with pytest.raises(RaydiumTickError, match="positive"):
            align_tick_to_spacing(100, 0)


class TestSqrtPriceConversions:
    """tick_to_sqrt_price_x64() and inverse."""

    def test_tick_zero(self):
        """Tick 0 → sqrt(1.0) * 2^64 = 2^64."""
        sqrt_price = tick_to_sqrt_price_x64(0)
        assert abs(sqrt_price - Q64) < Q64 * 0.001

    def test_positive_tick(self):
        """Positive tick should give sqrt price > Q64."""
        sqrt_price = tick_to_sqrt_price_x64(100)
        assert sqrt_price > Q64

    def test_negative_tick(self):
        """Negative tick should give sqrt price < Q64."""
        sqrt_price = tick_to_sqrt_price_x64(-100)
        assert sqrt_price < Q64

    def test_roundtrip(self):
        """tick → sqrt_price → tick should be close to original."""
        for tick in [-1000, -100, 0, 100, 1000]:
            sqrt_price = tick_to_sqrt_price_x64(tick)
            recovered = sqrt_price_x64_to_tick(sqrt_price)
            assert abs(recovered - tick) <= 1, f"Tick {tick} → {sqrt_price} → {recovered}"


class TestLiquidityCalculation:
    """get_liquidity_from_amounts() and get_amounts_from_liquidity()."""

    def test_symmetric_range(self):
        """Both tokens contribute when price is in range."""
        sqrt_current = tick_to_sqrt_price_x64(0)
        sqrt_lower = tick_to_sqrt_price_x64(-100)
        sqrt_upper = tick_to_sqrt_price_x64(100)

        liquidity = get_liquidity_from_amounts(
            sqrt_current, sqrt_lower, sqrt_upper,
            amount_a=1_000_000, amount_b=1_000_000,
        )
        assert liquidity > 0

    def test_below_range_only_token_a(self):
        """When price < lower, only token A is needed."""
        sqrt_current = tick_to_sqrt_price_x64(-200)
        sqrt_lower = tick_to_sqrt_price_x64(-100)
        sqrt_upper = tick_to_sqrt_price_x64(100)

        liquidity = get_liquidity_from_amounts(
            sqrt_current, sqrt_lower, sqrt_upper,
            amount_a=1_000_000, amount_b=0,
        )
        assert liquidity > 0

    def test_above_range_only_token_b(self):
        """When price > upper, only token B is needed."""
        sqrt_current = tick_to_sqrt_price_x64(200)
        sqrt_lower = tick_to_sqrt_price_x64(-100)
        sqrt_upper = tick_to_sqrt_price_x64(100)

        liquidity = get_liquidity_from_amounts(
            sqrt_current, sqrt_lower, sqrt_upper,
            amount_a=0, amount_b=1_000_000,
        )
        assert liquidity > 0

    def test_amounts_roundtrip(self):
        """liquidity → amounts → liquidity should be consistent."""
        sqrt_current = tick_to_sqrt_price_x64(0)
        sqrt_lower = tick_to_sqrt_price_x64(-1000)
        sqrt_upper = tick_to_sqrt_price_x64(1000)

        liquidity_in = get_liquidity_from_amounts(
            sqrt_current, sqrt_lower, sqrt_upper,
            amount_a=1_000_000_000, amount_b=1_000_000_000,
        )

        amount_a, amount_b = get_amounts_from_liquidity(
            sqrt_current, sqrt_lower, sqrt_upper, liquidity_in
        )
        assert amount_a > 0
        assert amount_b > 0

    def test_invalid_range_raises(self):
        with pytest.raises(RaydiumTickError):
            get_liquidity_from_amounts(
                tick_to_sqrt_price_x64(0),
                tick_to_sqrt_price_x64(100),  # lower > upper!
                tick_to_sqrt_price_x64(-100),
                1_000_000, 1_000_000,
            )


class TestTickArrayStartIndex:
    """tick_array_start_index() computes the containing tick array."""

    def test_positive_tick(self):
        start = tick_array_start_index(100, tick_spacing=60)
        # Array size = 60 * 60 = 3600, 100 is in the first array (0)
        assert start == 0

    def test_large_positive_tick(self):
        start = tick_array_start_index(4000, tick_spacing=60)
        # Array size = 3600, 4000 is in the second array (3600)
        assert start == 3600

    def test_negative_tick(self):
        start = tick_array_start_index(-100, tick_spacing=60)
        # Negative tick: should be in array starting at -3600
        assert start == -3600

    def test_tick_on_boundary(self):
        start = tick_array_start_index(3600, tick_spacing=60)
        assert start == 3600

    def test_tick_spacing_1(self):
        start = tick_array_start_index(100, tick_spacing=1)
        # Array size = 60 * 1 = 60, 100 is in array starting at 60
        assert start == 60
