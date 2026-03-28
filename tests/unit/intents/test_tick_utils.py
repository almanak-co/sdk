"""Unit tests for public tick/price conversion utilities."""

from decimal import Decimal

import pytest

from almanak.framework.intents import (
    get_max_tick,
    get_min_tick,
    get_tick_spacing,
    price_to_tick,
    snap_to_tick_spacing,
    tick_to_price,
)


class TestGetTickSpacing:
    """Tests for get_tick_spacing()."""

    def test_fee_100(self):
        assert get_tick_spacing(100) == 1

    def test_fee_500(self):
        assert get_tick_spacing(500) == 10

    def test_fee_3000(self):
        assert get_tick_spacing(3000) == 60

    def test_fee_2500_pancakeswap(self):
        assert get_tick_spacing(2500) == 50

    def test_fee_10000(self):
        assert get_tick_spacing(10000) == 200

    def test_unknown_fee_defaults_to_60(self):
        """Unknown fee tiers should default to 60 (matching compiler behavior)."""
        assert get_tick_spacing(999) == 60


class TestPriceToTick:
    """Tests for price_to_tick()."""

    def test_price_1_symmetric_decimals(self):
        """Price 1.0 with equal decimals should be tick 0."""
        tick = price_to_tick(Decimal("1.0"), decimals0=18, decimals1=18)
        assert tick == 0

    def test_positive_price(self):
        """Higher prices should produce positive ticks."""
        tick = price_to_tick(Decimal("2.0"), decimals0=18, decimals1=18)
        assert tick > 0

    def test_fractional_price(self):
        """Prices below 1 should produce negative ticks."""
        tick = price_to_tick(Decimal("0.5"), decimals0=18, decimals1=18)
        assert tick < 0

    def test_asymmetric_decimals(self):
        """USDC(6)/WETH(18) price conversion."""
        tick = price_to_tick(Decimal("2000"), decimals0=6, decimals1=18)
        # Roundtrip should be close
        price_back = tick_to_price(tick, decimals0=6, decimals1=18)
        assert abs(float(price_back) - 2000) / 2000 < 0.01  # within 1%

    def test_zero_price_raises(self):
        """Zero price must raise ValueError instead of silently returning MIN_TICK."""
        with pytest.raises(ValueError, match="price must be positive"):
            price_to_tick(Decimal("0"), decimals0=18, decimals1=18)

    def test_negative_price_raises(self):
        """Negative price must raise ValueError."""
        with pytest.raises(ValueError, match="price must be positive"):
            price_to_tick(Decimal("-1"), decimals0=18, decimals1=18)


class TestTickToPrice:
    """Tests for tick_to_price()."""

    def test_tick_zero(self):
        """Tick 0 should produce price 1.0."""
        price = tick_to_price(0, decimals0=18, decimals1=18)
        assert abs(float(price) - 1.0) < 0.001

    def test_positive_tick(self):
        """Positive ticks should produce prices > 1."""
        price = tick_to_price(1000, decimals0=18, decimals1=18)
        assert price > Decimal("1.0")

    def test_negative_tick(self):
        """Negative ticks should produce prices < 1."""
        price = tick_to_price(-1000, decimals0=18, decimals1=18)
        assert price < Decimal("1.0")


class TestSnapToTickSpacing:
    """Tests for snap_to_tick_spacing()."""

    def test_already_aligned(self):
        """Tick already on spacing boundary stays unchanged."""
        assert snap_to_tick_spacing(600, fee_tier=3000) == 600

    def test_rounds_to_nearest(self):
        """Tick should snap to nearest valid tick."""
        result = snap_to_tick_spacing(601, fee_tier=3000)
        assert result == 600

    def test_rounds_up(self):
        """Tick closer to upper boundary should round up."""
        result = snap_to_tick_spacing(650, fee_tier=3000)
        assert result == 660


class TestMinMaxTick:
    """Tests for get_min_tick() and get_max_tick()."""

    def test_min_tick_is_negative(self):
        assert get_min_tick(3000) < 0

    def test_max_tick_is_positive(self):
        assert get_max_tick(3000) > 0

    def test_min_tick_aligned_to_spacing(self):
        for fee in [100, 500, 3000, 10000]:
            spacing = get_tick_spacing(fee)
            assert get_min_tick(fee) % spacing == 0

    def test_max_tick_aligned_to_spacing(self):
        for fee in [100, 500, 3000, 10000]:
            spacing = get_tick_spacing(fee)
            assert get_max_tick(fee) % spacing == 0


class TestRoundtrip:
    """Tests for price -> tick -> price roundtrip consistency."""

    @pytest.mark.parametrize(
        "price,d0,d1",
        [
            (Decimal("1.0"), 18, 18),
            (Decimal("2000"), 6, 18),
            (Decimal("0.0005"), 18, 6),
            (Decimal("50000"), 8, 18),
        ],
    )
    def test_roundtrip_within_tolerance(self, price, d0, d1):
        """price_to_tick -> tick_to_price should roundtrip within 0.01%."""
        tick = price_to_tick(price, decimals0=d0, decimals1=d1)
        price_back = tick_to_price(tick, decimals0=d0, decimals1=d1)
        # Ticks are discrete, so small error is expected
        relative_error = abs(float(price_back) - float(price)) / float(price)
        assert relative_error < 0.01, f"Roundtrip error {relative_error:.4%} for price={price}"
