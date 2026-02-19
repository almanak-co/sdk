"""Unit tests for liquidation price calculations.

This module tests the LiquidationCalculator class including:
- Liquidation price for long positions
- Liquidation price for short positions
- Various leverage levels
- Liquidation warning thresholds
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.calculators.liquidation import (
    LiquidationCalculator,
    LiquidationWarning,
)
from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
    SimulatedPosition,
)


class TestLiquidationPriceLongPositions:
    """Tests for liquidation price calculation on long positions."""

    def test_long_position_5x_leverage(self):
        """Test liquidation price for 5x long position.

        Formula: liq_price = entry_price * (1 - 1/leverage + maintenance_margin)
        For 5x leverage with 5% maintenance margin:
        liq_price = 2000 * (1 - 0.2 + 0.05) = 2000 * 0.85 = $1700
        """
        calculator = LiquidationCalculator()
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("5"),
            maintenance_margin=Decimal("0.05"),
            is_long=True,
        )
        assert liq_price == Decimal("1700")

    def test_long_position_10x_leverage(self):
        """Test liquidation price for 10x long position.

        For 10x leverage with 5% maintenance margin:
        liq_price = 2000 * (1 - 0.1 + 0.05) = 2000 * 0.95 = $1900
        """
        calculator = LiquidationCalculator()
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("10"),
            maintenance_margin=Decimal("0.05"),
            is_long=True,
        )
        assert liq_price == Decimal("1900")

    def test_long_position_2x_leverage(self):
        """Test liquidation price for 2x long position.

        For 2x leverage with 5% maintenance margin:
        liq_price = 2000 * (1 - 0.5 + 0.05) = 2000 * 0.55 = $1100
        """
        calculator = LiquidationCalculator()
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("2"),
            maintenance_margin=Decimal("0.05"),
            is_long=True,
        )
        assert liq_price == Decimal("1100")

    def test_long_position_1x_leverage(self):
        """Test liquidation price for 1x long position (no leverage).

        For 1x leverage with 5% maintenance margin:
        liq_price = 2000 * (1 - 1.0 + 0.05) = 2000 * 0.05 = $100
        This means the price would need to fall 95% for liquidation.
        """
        calculator = LiquidationCalculator()
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("1"),
            maintenance_margin=Decimal("0.05"),
            is_long=True,
        )
        assert liq_price == Decimal("100")

    def test_long_position_20x_leverage(self):
        """Test liquidation price for 20x long position.

        For 20x leverage with 5% maintenance margin:
        liq_price = 2000 * (1 - 0.05 + 0.05) = 2000 * 1.0 = $2000
        At 20x leverage, even a small drop can trigger liquidation.
        """
        calculator = LiquidationCalculator()
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("20"),
            maintenance_margin=Decimal("0.05"),
            is_long=True,
        )
        assert liq_price == Decimal("2000")

    def test_long_position_50x_leverage(self):
        """Test liquidation price for 50x long position.

        For 50x leverage with 5% maintenance margin:
        liq_price = 2000 * (1 - 0.02 + 0.05) = 2000 * 1.03 = $2060
        At extreme leverage, liquidation price can be above entry price.
        """
        calculator = LiquidationCalculator()
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("50"),
            maintenance_margin=Decimal("0.05"),
            is_long=True,
        )
        assert liq_price == Decimal("2060")


class TestLiquidationPriceShortPositions:
    """Tests for liquidation price calculation on short positions."""

    def test_short_position_5x_leverage(self):
        """Test liquidation price for 5x short position.

        Formula: liq_price = entry_price * (1 + 1/leverage - maintenance_margin)
        For 5x leverage with 5% maintenance margin:
        liq_price = 2000 * (1 + 0.2 - 0.05) = 2000 * 1.15 = $2300
        """
        calculator = LiquidationCalculator()
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("5"),
            maintenance_margin=Decimal("0.05"),
            is_long=False,
        )
        assert liq_price == Decimal("2300")

    def test_short_position_10x_leverage(self):
        """Test liquidation price for 10x short position.

        For 10x leverage with 5% maintenance margin:
        liq_price = 2000 * (1 + 0.1 - 0.05) = 2000 * 1.05 = $2100
        """
        calculator = LiquidationCalculator()
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("10"),
            maintenance_margin=Decimal("0.05"),
            is_long=False,
        )
        assert liq_price == Decimal("2100")

    def test_short_position_2x_leverage(self):
        """Test liquidation price for 2x short position.

        For 2x leverage with 5% maintenance margin:
        liq_price = 2000 * (1 + 0.5 - 0.05) = 2000 * 1.45 = $2900
        """
        calculator = LiquidationCalculator()
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("2"),
            maintenance_margin=Decimal("0.05"),
            is_long=False,
        )
        assert liq_price == Decimal("2900")

    def test_short_position_1x_leverage(self):
        """Test liquidation price for 1x short position (no leverage).

        For 1x leverage with 5% maintenance margin:
        liq_price = 2000 * (1 + 1.0 - 0.05) = 2000 * 1.95 = $3900
        """
        calculator = LiquidationCalculator()
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("1"),
            maintenance_margin=Decimal("0.05"),
            is_long=False,
        )
        assert liq_price == Decimal("3900")

    def test_short_position_20x_leverage(self):
        """Test liquidation price for 20x short position.

        For 20x leverage with 5% maintenance margin:
        liq_price = 2000 * (1 + 0.05 - 0.05) = 2000 * 1.0 = $2000
        """
        calculator = LiquidationCalculator()
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("20"),
            maintenance_margin=Decimal("0.05"),
            is_long=False,
        )
        assert liq_price == Decimal("2000")


class TestLiquidationPriceVariousLeverageLevels:
    """Tests for liquidation price calculation with various leverage levels."""

    @pytest.mark.parametrize(
        "leverage,expected_long_liq,expected_short_liq",
        [
            # leverage, long_liq_price, short_liq_price
            # Formula: Long = entry * (1 - 1/lev + margin), Short = entry * (1 + 1/lev - margin)
            (Decimal("1"), Decimal("100"), Decimal("3900")),
            (Decimal("2"), Decimal("1100"), Decimal("2900")),
            (Decimal("3"), Decimal("1433.333333333333333333333333"), Decimal("2566.666666666666666666666667")),
            (Decimal("5"), Decimal("1700"), Decimal("2300")),
            (Decimal("10"), Decimal("1900"), Decimal("2100")),
            (Decimal("20"), Decimal("2000"), Decimal("2000")),
            (Decimal("50"), Decimal("2060"), Decimal("1940")),
            (Decimal("100"), Decimal("2080"), Decimal("1920")),
        ],
    )
    def test_various_leverage_levels(
        self, leverage: Decimal, expected_long_liq: Decimal, expected_short_liq: Decimal
    ):
        """Test liquidation prices at various leverage levels."""
        calculator = LiquidationCalculator()
        entry_price = Decimal("2000")
        maintenance_margin = Decimal("0.05")

        # Long position
        long_liq = calculator.calculate_liquidation_price(
            entry_price=entry_price,
            leverage=leverage,
            maintenance_margin=maintenance_margin,
            is_long=True,
        )
        assert long_liq == pytest.approx(expected_long_liq, rel=Decimal("0.0001"))

        # Short position
        short_liq = calculator.calculate_liquidation_price(
            entry_price=entry_price,
            leverage=leverage,
            maintenance_margin=maintenance_margin,
            is_long=False,
        )
        assert short_liq == pytest.approx(expected_short_liq, rel=Decimal("0.0001"))

    def test_fractional_leverage(self):
        """Test liquidation price with fractional leverage (e.g., 1.5x)."""
        calculator = LiquidationCalculator()
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("1.5"),
            maintenance_margin=Decimal("0.05"),
            is_long=True,
        )
        # liq = 2000 * (1 - 1/1.5 + 0.05) = 2000 * (1 - 0.6667 + 0.05) = 2000 * 0.3833 = 766.67
        expected = Decimal("2000") * (
            Decimal("1") - Decimal("1") / Decimal("1.5") + Decimal("0.05")
        )
        assert liq_price == pytest.approx(expected, rel=Decimal("0.0001"))

    def test_zero_leverage_raises_error(self):
        """Test that zero leverage raises ValueError."""
        calculator = LiquidationCalculator()
        with pytest.raises(ValueError, match="Leverage must be greater than 0"):
            calculator.calculate_liquidation_price(
                entry_price=Decimal("2000"),
                leverage=Decimal("0"),
                maintenance_margin=Decimal("0.05"),
                is_long=True,
            )

    def test_negative_leverage_raises_error(self):
        """Test that negative leverage raises ValueError."""
        calculator = LiquidationCalculator()
        with pytest.raises(ValueError, match="Leverage must be greater than 0"):
            calculator.calculate_liquidation_price(
                entry_price=Decimal("2000"),
                leverage=Decimal("-5"),
                maintenance_margin=Decimal("0.05"),
                is_long=True,
            )


class TestMaintenanceMarginVariations:
    """Tests for various maintenance margin levels."""

    def test_zero_maintenance_margin(self):
        """Test liquidation price with zero maintenance margin.

        For 5x long with 0% maintenance margin:
        liq_price = 2000 * (1 - 0.2 + 0) = 2000 * 0.8 = $1600
        """
        calculator = LiquidationCalculator()
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("5"),
            maintenance_margin=Decimal("0"),
            is_long=True,
        )
        assert liq_price == Decimal("1600")

    def test_high_maintenance_margin(self):
        """Test liquidation price with high (10%) maintenance margin.

        For 5x long with 10% maintenance margin:
        liq_price = 2000 * (1 - 0.2 + 0.1) = 2000 * 0.9 = $1800
        """
        calculator = LiquidationCalculator()
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("5"),
            maintenance_margin=Decimal("0.10"),
            is_long=True,
        )
        assert liq_price == Decimal("1800")

    def test_protocol_specific_margin_gmx(self):
        """Test that GMX uses 1% maintenance margin."""
        calculator = LiquidationCalculator()
        margin = calculator.get_maintenance_margin_for_protocol("gmx")
        assert margin == Decimal("0.01")

    def test_protocol_specific_margin_hyperliquid(self):
        """Test that Hyperliquid uses 0.5% maintenance margin."""
        calculator = LiquidationCalculator()
        margin = calculator.get_maintenance_margin_for_protocol("hyperliquid")
        assert margin == Decimal("0.005")

    def test_protocol_specific_margin_binance(self):
        """Test that Binance uses 4% maintenance margin."""
        calculator = LiquidationCalculator()
        margin = calculator.get_maintenance_margin_for_protocol("binance_perp")
        assert margin == Decimal("0.04")

    def test_unknown_protocol_uses_default(self):
        """Test that unknown protocols use default 5% margin."""
        calculator = LiquidationCalculator()
        margin = calculator.get_maintenance_margin_for_protocol("unknown_protocol")
        assert margin == Decimal("0.05")


class TestLiquidationWarningThreshold:
    """Tests for liquidation warning threshold functionality."""

    def test_warning_threshold_within_10_percent(self):
        """Test that warning is triggered when within 10% of liquidation."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        calculator = LiquidationCalculator()

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # Liq at ~$1620 with GMX 1% margin
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        # Price 8% above liquidation should trigger warning but not critical
        # Liquidation is at $1620, so 8% above = $1749.60
        liq_price = position.liquidation_price
        assert liq_price is not None

        # Calculate price 8% above liquidation (within 10% threshold but outside 5% critical)
        test_price = liq_price * Decimal("1.08")

        warning = calculator.check_liquidation_proximity(
            position=position,
            current_price=test_price,
            emit_warning=False,
        )

        assert warning is not None
        assert warning.is_critical is False
        assert warning.distance_pct < Decimal("0.10")

    def test_critical_threshold_within_5_percent(self):
        """Test that critical warning is triggered when within 5% of liquidation."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        calculator = LiquidationCalculator()

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        # Price 2% above liquidation should trigger critical warning
        liq_price = position.liquidation_price
        assert liq_price is not None

        test_price = liq_price * Decimal("1.02")

        warning = calculator.check_liquidation_proximity(
            position=position,
            current_price=test_price,
            emit_warning=False,
        )

        assert warning is not None
        assert warning.is_critical is True
        assert warning.distance_pct < Decimal("0.05")

    def test_no_warning_outside_threshold(self):
        """Test that no warning is triggered when price is far from liquidation."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        calculator = LiquidationCalculator()

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        # Price at entry (far from liquidation)
        warning = calculator.check_liquidation_proximity(
            position=position,
            current_price=Decimal("2000"),
            emit_warning=False,
        )

        assert warning is None

    def test_short_position_warning(self):
        """Test warning threshold for short positions (liquidation above entry)."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        calculator = LiquidationCalculator()

        position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),  # Liq at ~$2180 with GMX 1% margin
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        # For short, liquidation is above entry price
        liq_price = position.liquidation_price
        assert liq_price is not None
        assert liq_price > Decimal("2000")

        # Price 5% below liquidation should trigger warning
        test_price = liq_price * Decimal("0.95")

        warning = calculator.check_liquidation_proximity(
            position=position,
            current_price=test_price,
            emit_warning=False,
        )

        assert warning is not None
        assert warning.distance_pct < Decimal("0.10")

    def test_custom_warning_thresholds(self):
        """Test that custom warning thresholds work correctly."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        calculator = LiquidationCalculator()

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        liq_price = position.liquidation_price
        assert liq_price is not None

        # Price 8% above liquidation
        test_price = liq_price * Decimal("1.08")

        # With default 10% threshold, this should trigger warning
        warning_default = calculator.check_liquidation_proximity(
            position=position,
            current_price=test_price,
            emit_warning=False,
        )
        assert warning_default is not None

        # With custom 5% threshold, this should NOT trigger warning
        warning_custom = calculator.check_liquidation_proximity(
            position=position,
            current_price=test_price,
            warning_threshold=Decimal("0.05"),
            emit_warning=False,
        )
        assert warning_custom is None


class TestLiquidationWarningMessage:
    """Tests for LiquidationWarning message generation."""

    def test_warning_message_format(self):
        """Test that warning message format is correct."""
        warning = LiquidationWarning(
            position_id="pos-123",
            current_price=Decimal("1750"),
            liquidation_price=Decimal("1700"),
            distance_pct=Decimal("0.0294"),
            is_critical=False,
        )

        message = warning.message
        assert "[WARNING]" in message
        assert "pos-123" in message
        assert "1750" in message
        assert "1700" in message

    def test_critical_message_format(self):
        """Test that critical warning message format is correct."""
        warning = LiquidationWarning(
            position_id="pos-456",
            current_price=Decimal("1720"),
            liquidation_price=Decimal("1700"),
            distance_pct=Decimal("0.0117"),
            is_critical=True,
        )

        message = warning.message
        assert "[CRITICAL]" in message
        assert "pos-456" in message


class TestLiquidationPriceForPosition:
    """Tests for calculate_liquidation_price_for_position method."""

    def test_long_position_liquidation_price(self):
        """Test liquidation price calculation from SimulatedPosition for long."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        calculator = LiquidationCalculator()

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        liq_price = calculator.calculate_liquidation_price_for_position(position)

        # With GMX 1% margin and 5x leverage:
        # liq = 2000 * (1 - 0.2 + 0.01) = 2000 * 0.81 = $1620
        expected = Decimal("2000") * Decimal("0.81")
        assert liq_price == expected

    def test_short_position_liquidation_price(self):
        """Test liquidation price calculation from SimulatedPosition for short."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        calculator = LiquidationCalculator()

        position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        liq_price = calculator.calculate_liquidation_price_for_position(position)

        # With GMX 1% margin and 5x leverage:
        # liq = 2000 * (1 + 0.2 - 0.01) = 2000 * 1.19 = $2380
        expected = Decimal("2000") * Decimal("1.19")
        assert liq_price == expected

    def test_non_perp_position_returns_none(self):
        """Test that non-perp positions return None for liquidation price."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        calculator = LiquidationCalculator()

        # Create a spot position
        position = SimulatedPosition(
            position_type=PositionType.SPOT,
            protocol="spot",
            tokens=["ETH"],
            amounts={"ETH": Decimal("1")},
            entry_price=Decimal("2000"),
            entry_time=entry_time,
        )

        liq_price = calculator.calculate_liquidation_price_for_position(position)
        assert liq_price is None

    def test_override_maintenance_margin(self):
        """Test that maintenance margin can be overridden."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        calculator = LiquidationCalculator()

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",  # GMX default is 1%
        )

        # Override with 5% margin
        liq_price = calculator.calculate_liquidation_price_for_position(
            position, maintenance_margin=Decimal("0.05")
        )

        # With 5% margin and 5x leverage:
        # liq = 2000 * (1 - 0.2 + 0.05) = 2000 * 0.85 = $1700
        expected = Decimal("2000") * Decimal("0.85")
        assert liq_price == expected


class TestEstimateSafeLeverage:
    """Tests for estimate_safe_leverage method."""

    def test_safe_leverage_for_10_percent_stop_loss_long(self):
        """Test safe leverage estimation for a 10% stop loss on long position."""
        calculator = LiquidationCalculator()

        max_leverage = calculator.estimate_safe_leverage(
            entry_price=Decimal("2000"),
            stop_loss_price=Decimal("1800"),  # 10% below entry
            is_long=True,
        )

        # Verify the calculated leverage keeps liquidation below stop loss
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=max_leverage,
            maintenance_margin=Decimal("0.05"),
            is_long=True,
        )

        # Liquidation should be below stop loss
        assert liq_price < Decimal("1800")

    def test_safe_leverage_for_5_percent_stop_loss_short(self):
        """Test safe leverage estimation for a 5% stop loss on short position."""
        calculator = LiquidationCalculator()

        max_leverage = calculator.estimate_safe_leverage(
            entry_price=Decimal("2000"),
            stop_loss_price=Decimal("2100"),  # 5% above entry
            is_long=False,
        )

        # Verify the calculated leverage keeps liquidation above stop loss
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=max_leverage,
            maintenance_margin=Decimal("0.05"),
            is_long=False,
        )

        # Liquidation should be above stop loss
        assert liq_price > Decimal("2100")

    def test_invalid_stop_loss_long_returns_1x(self):
        """Test that invalid stop loss (above entry for long) returns 1x leverage."""
        calculator = LiquidationCalculator()

        max_leverage = calculator.estimate_safe_leverage(
            entry_price=Decimal("2000"),
            stop_loss_price=Decimal("2100"),  # Above entry (invalid for long)
            is_long=True,
        )

        assert max_leverage == Decimal("1")

    def test_invalid_stop_loss_short_returns_1x(self):
        """Test that invalid stop loss (below entry for short) returns 1x leverage."""
        calculator = LiquidationCalculator()

        max_leverage = calculator.estimate_safe_leverage(
            entry_price=Decimal("2000"),
            stop_loss_price=Decimal("1900"),  # Below entry (invalid for short)
            is_long=False,
        )

        assert max_leverage == Decimal("1")


class TestLiquidationCalculatorSerialization:
    """Tests for LiquidationCalculator serialization."""

    def test_to_dict(self):
        """Test that calculator serializes correctly."""
        calculator = LiquidationCalculator()
        data = calculator.to_dict()

        assert data["calculator_name"] == "liquidation"
        assert data["default_maintenance_margin"] == "0.05"
        assert data["warning_threshold"] == "0.10"
        assert data["critical_threshold"] == "0.05"
        assert "gmx" in data["protocol_margins"]

    def test_to_dict_custom_values(self):
        """Test serialization with custom values."""
        calculator = LiquidationCalculator(
            default_maintenance_margin=Decimal("0.10"),
            warning_threshold=Decimal("0.15"),
            critical_threshold=Decimal("0.08"),
        )
        data = calculator.to_dict()

        assert data["default_maintenance_margin"] == "0.10"
        assert data["warning_threshold"] == "0.15"
        assert data["critical_threshold"] == "0.08"


class TestLiquidationPriceIntegration:
    """Integration tests for liquidation price in SimulatedPosition."""

    def test_position_has_liquidation_price_on_creation(self):
        """Test that perp positions have liquidation_price set on creation."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

        long_position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        assert long_position.liquidation_price is not None
        assert isinstance(long_position.liquidation_price, Decimal)

        short_position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        assert short_position.liquidation_price is not None
        assert isinstance(short_position.liquidation_price, Decimal)

    def test_long_liquidation_below_entry(self):
        """Test that long position liquidation price is below entry price at reasonable leverage."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        assert position.liquidation_price is not None
        assert position.liquidation_price < position.entry_price

    def test_short_liquidation_above_entry(self):
        """Test that short position liquidation price is above entry price at reasonable leverage."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

        position = SimulatedPosition.perp_short(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        assert position.liquidation_price is not None
        assert position.liquidation_price > position.entry_price

    def test_position_serialization_includes_liquidation_price(self):
        """Test that position serialization includes liquidation_price."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        data = position.to_dict()
        assert "liquidation_price" in data
        assert data["liquidation_price"] == str(position.liquidation_price)

    def test_position_deserialization_restores_liquidation_price(self):
        """Test that position deserialization restores liquidation_price."""
        entry_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

        position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("10000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=entry_time,
            protocol="gmx",
        )

        # Roundtrip
        data = position.to_dict()
        restored = SimulatedPosition.from_dict(data)

        assert restored.liquidation_price == position.liquidation_price
