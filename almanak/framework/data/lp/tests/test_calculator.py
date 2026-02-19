"""Unit tests for the IL Calculator module.

This module tests impermanent loss calculations for various AMM pool types
including constant product, weighted, stable, and concentrated liquidity pools.
"""

from decimal import Decimal

import pytest

from ..calculator import (
    COMMON_PRICE_CHANGES,
    ILCalculator,
    ILExposureUnavailableError,
    InvalidPriceError,
    InvalidWeightError,
    LPPosition,
    PoolType,
    PositionNotFoundError,
    calculate_il_simple,
    project_il_table,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def calculator() -> ILCalculator:
    """Create a basic IL calculator."""
    return ILCalculator()


@pytest.fixture
def calculator_with_mock_prices() -> ILCalculator:
    """Create an IL calculator with mock prices."""
    return ILCalculator(
        mock_prices={
            "WETH": Decimal("2500"),
            "USDC": Decimal("1"),
            "WBTC": Decimal("45000"),
        }
    )


@pytest.fixture
def sample_position() -> LPPosition:
    """Create a sample LP position."""
    return LPPosition(
        position_id="test-position-1",
        pool_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        token_a="WETH",
        token_b="USDC",
        entry_price_a=Decimal("2000"),
        entry_price_b=Decimal("1"),
        amount_a=Decimal("1"),
        amount_b=Decimal("2000"),
        weight_a=Decimal("0.5"),
        weight_b=Decimal("0.5"),
        pool_type=PoolType.CONSTANT_PRODUCT,
        chain="ethereum",
    )


@pytest.fixture
def concentrated_position() -> LPPosition:
    """Create a concentrated liquidity position."""
    return LPPosition(
        position_id="test-concentrated-1",
        pool_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        token_a="WETH",
        token_b="USDC",
        entry_price_a=Decimal("2000"),
        entry_price_b=Decimal("1"),
        amount_a=Decimal("1"),
        amount_b=Decimal("2000"),
        pool_type=PoolType.CONCENTRATED,
        chain="ethereum",
        tick_lower=-887220,  # ~$1000
        tick_upper=-201180,  # ~$3000
    )


# =============================================================================
# Basic IL Calculation Tests
# =============================================================================


class TestBasicILCalculation:
    """Tests for basic IL calculation functionality."""

    def test_no_price_change(self, calculator: ILCalculator) -> None:
        """IL should be zero when prices don't change."""
        result = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("2000"),
            current_price_b=Decimal("1"),
        )

        assert result.il_ratio == Decimal("0")
        assert result.il_percent == Decimal("0")
        assert result.il_bps == 0
        assert result.price_ratio == Decimal("1")
        assert not result.is_loss
        assert not result.is_gain

    def test_price_increase_50_percent(self, calculator: ILCalculator) -> None:
        """Test IL for 50% price increase."""
        result = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("3000"),  # +50%
            current_price_b=Decimal("1"),
        )

        # For 50% increase, IL ≈ -2.02%
        assert result.il_ratio < 0  # IL is a loss (negative value in our convention)
        assert Decimal("-0.025") < result.il_ratio < Decimal("-0.015")
        assert result.is_loss
        assert result.price_ratio == Decimal("1.5")

    def test_price_decrease_50_percent(self, calculator: ILCalculator) -> None:
        """Test IL for 50% price decrease."""
        result = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("1000"),  # -50%
            current_price_b=Decimal("1"),
        )

        # For 50% decrease, IL ≈ -5.72%
        assert result.il_ratio < 0
        assert Decimal("-0.065") < result.il_ratio < Decimal("-0.050")
        assert result.is_loss
        assert result.price_ratio == Decimal("0.5")

    def test_price_double(self, calculator: ILCalculator) -> None:
        """Test IL for 100% price increase (2x)."""
        result = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("4000"),  # +100%
            current_price_b=Decimal("1"),
        )

        # For 2x price, IL ≈ -5.72%
        assert result.il_ratio < 0
        assert Decimal("-0.065") < result.il_ratio < Decimal("-0.050")
        assert result.price_ratio == Decimal("2")

    def test_price_5x(self, calculator: ILCalculator) -> None:
        """Test IL for 5x price increase."""
        result = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("10000"),  # +400% (5x)
            current_price_b=Decimal("1"),
        )

        # For 5x price, IL ≈ -25.46%
        assert result.il_ratio < 0
        assert Decimal("-0.30") < result.il_ratio < Decimal("-0.20")
        assert result.price_ratio == Decimal("5")

    def test_symmetric_il(self, calculator: ILCalculator) -> None:
        """IL for 2x and 0.5x should be the same (symmetric)."""
        result_up = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("4000"),  # 2x
            current_price_b=Decimal("1"),
        )

        result_down = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("1000"),  # 0.5x
            current_price_b=Decimal("1"),
        )

        # IL should be the same for 2x and 0.5x
        assert abs(result_up.il_ratio - result_down.il_ratio) < Decimal("0.001")

    def test_entry_value_provided(self, calculator: ILCalculator) -> None:
        """Test IL calculation with explicit entry value."""
        result = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("3000"),
            current_price_b=Decimal("1"),
            entry_value=Decimal("10000"),  # $10,000 position
        )

        # Value if held should be based on entry value
        expected_held_value = Decimal("10000") * (
            Decimal("0.5") * Decimal("1.5")  # ETH went up 50%
            + Decimal("0.5") * Decimal("1")  # USDC stayed same
        )
        assert abs(result.value_if_held - expected_held_value) < Decimal("1")


# =============================================================================
# Weighted Pool Tests
# =============================================================================


class TestWeightedPoolIL:
    """Tests for weighted pool IL calculations (Balancer style)."""

    def test_80_20_pool_no_change(self, calculator: ILCalculator) -> None:
        """80/20 pool with no price change should have zero IL."""
        result = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("2000"),
            current_price_b=Decimal("1"),
            weight_a=Decimal("0.8"),
            weight_b=Decimal("0.2"),
            pool_type=PoolType.WEIGHTED,
        )

        assert result.il_ratio == Decimal("0")
        assert result.weight_a == Decimal("0.8")
        assert result.weight_b == Decimal("0.2")

    def test_80_20_pool_price_increase(self, calculator: ILCalculator) -> None:
        """80/20 pool should have lower IL than 50/50 when dominant asset rises."""
        result_80_20 = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("4000"),  # 2x
            current_price_b=Decimal("1"),
            weight_a=Decimal("0.8"),
            weight_b=Decimal("0.2"),
            pool_type=PoolType.WEIGHTED,
        )

        result_50_50 = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("4000"),
            current_price_b=Decimal("1"),
            weight_a=Decimal("0.5"),
            weight_b=Decimal("0.5"),
            pool_type=PoolType.CONSTANT_PRODUCT,
        )

        # 80/20 should have lower IL when the 80% asset goes up
        assert abs(result_80_20.il_ratio) < abs(result_50_50.il_ratio)


# =============================================================================
# Stable Pool Tests
# =============================================================================


class TestStablePoolIL:
    """Tests for stable pool IL calculations (Curve style)."""

    def test_stable_pool_low_il(self, calculator: ILCalculator) -> None:
        """Stable pools should have much lower IL than constant product."""
        result_stable = calculator.calculate_il(
            entry_price_a=Decimal("1"),  # USDC
            entry_price_b=Decimal("1"),  # USDT
            current_price_a=Decimal("1.01"),  # 1% depeg
            current_price_b=Decimal("1"),
            pool_type=PoolType.STABLE,
        )

        result_cp = calculator.calculate_il(
            entry_price_a=Decimal("1"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("1.01"),
            current_price_b=Decimal("1"),
            pool_type=PoolType.CONSTANT_PRODUCT,
        )

        # Stable pool IL should be ~10x lower than constant product
        assert abs(result_stable.il_ratio) < abs(result_cp.il_ratio)


# =============================================================================
# Concentrated Liquidity Tests
# =============================================================================


class TestConcentratedLiquidityIL:
    """Tests for concentrated liquidity IL calculations (Uniswap V3 style)."""

    def test_concentrated_il_in_range(self, calculator: ILCalculator) -> None:
        """IL for concentrated position when price stays in range."""
        result = calculator.calculate_il_concentrated(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("2200"),  # +10%, still in range
            current_price_b=Decimal("1"),
            tick_lower=-887220,
            tick_upper=-201180,
        )

        assert result.pool_type == PoolType.CONCENTRATED
        # IL should be calculable
        assert result.il_ratio is not None

    def test_concentrated_vs_full_range(self, calculator: ILCalculator) -> None:
        """Concentrated positions can have higher IL than full range."""
        # Full range (constant product)
        result_full = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("3000"),
            current_price_b=Decimal("1"),
            pool_type=PoolType.CONSTANT_PRODUCT,
        )

        # Narrow concentrated range
        result_concentrated = calculator.calculate_il_concentrated(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("3000"),
            current_price_b=Decimal("1"),
            tick_lower=-200000,  # Very narrow range
            tick_upper=-190000,
        )

        # Both should have IL
        assert result_full.il_ratio < 0
        assert result_concentrated.il_ratio < 0


# =============================================================================
# IL Projection Tests
# =============================================================================


class TestILProjection:
    """Tests for IL projection functionality."""

    def test_project_zero_change(self, calculator: ILCalculator) -> None:
        """Zero price change should project zero IL."""
        result = calculator.project_il(price_change_pct=Decimal("0"))

        assert result.il_ratio == Decimal("0")
        assert result.price_change_pct == Decimal("0")

    def test_project_positive_change(self, calculator: ILCalculator) -> None:
        """Project IL for positive price change."""
        result = calculator.project_il(price_change_pct=Decimal("50"))

        assert result.il_ratio < 0
        assert result.price_change_pct == Decimal("50")
        assert result.pool_type == PoolType.CONSTANT_PRODUCT

    def test_project_negative_change(self, calculator: ILCalculator) -> None:
        """Project IL for negative price change."""
        result = calculator.project_il(price_change_pct=Decimal("-30"))

        assert result.il_ratio < 0
        assert result.price_change_pct == Decimal("-30")

    def test_project_invalid_change(self, calculator: ILCalculator) -> None:
        """Price change of -100% or less should raise error."""
        with pytest.raises(InvalidPriceError):
            calculator.project_il(price_change_pct=Decimal("-100"))

        with pytest.raises(InvalidPriceError):
            calculator.project_il(price_change_pct=Decimal("-150"))

    def test_project_weighted_pool(self, calculator: ILCalculator) -> None:
        """Project IL for weighted pool."""
        result = calculator.project_il(
            price_change_pct=Decimal("100"),
            weight_a=Decimal("0.8"),
            weight_b=Decimal("0.2"),
            pool_type=PoolType.WEIGHTED,
        )

        assert result.weight_a == Decimal("0.8")
        assert result.weight_b == Decimal("0.2")
        assert result.pool_type == PoolType.WEIGHTED


# =============================================================================
# Validation Tests
# =============================================================================


class TestValidation:
    """Tests for input validation."""

    def test_zero_price(self, calculator: ILCalculator) -> None:
        """Zero prices should raise InvalidPriceError."""
        with pytest.raises(InvalidPriceError):
            calculator.calculate_il(
                entry_price_a=Decimal("0"),
                entry_price_b=Decimal("1"),
                current_price_a=Decimal("2000"),
                current_price_b=Decimal("1"),
            )

    def test_negative_price(self, calculator: ILCalculator) -> None:
        """Negative prices should raise InvalidPriceError."""
        with pytest.raises(InvalidPriceError):
            calculator.calculate_il(
                entry_price_a=Decimal("-100"),
                entry_price_b=Decimal("1"),
                current_price_a=Decimal("2000"),
                current_price_b=Decimal("1"),
            )

    def test_invalid_weights_not_sum_to_one(self, calculator: ILCalculator) -> None:
        """Weights not summing to 1 should raise InvalidWeightError."""
        with pytest.raises(InvalidWeightError):
            calculator.calculate_il(
                entry_price_a=Decimal("2000"),
                entry_price_b=Decimal("1"),
                current_price_a=Decimal("3000"),
                current_price_b=Decimal("1"),
                weight_a=Decimal("0.6"),
                weight_b=Decimal("0.6"),  # Sum = 1.2
            )

    def test_invalid_weights_zero(self, calculator: ILCalculator) -> None:
        """Zero weight should raise InvalidWeightError."""
        with pytest.raises(InvalidWeightError):
            calculator.calculate_il(
                entry_price_a=Decimal("2000"),
                entry_price_b=Decimal("1"),
                current_price_a=Decimal("3000"),
                current_price_b=Decimal("1"),
                weight_a=Decimal("0"),
                weight_b=Decimal("1"),
            )


# =============================================================================
# Position Tracking Tests
# =============================================================================


class TestPositionTracking:
    """Tests for LP position tracking functionality."""

    def test_add_and_get_position(self, calculator: ILCalculator, sample_position: LPPosition) -> None:
        """Test adding and retrieving a position."""
        calculator.add_position(sample_position)

        retrieved = calculator.get_position(sample_position.position_id)
        assert retrieved.position_id == sample_position.position_id
        assert retrieved.pool_address == sample_position.pool_address

    def test_remove_position(self, calculator: ILCalculator, sample_position: LPPosition) -> None:
        """Test removing a position."""
        calculator.add_position(sample_position)
        calculator.remove_position(sample_position.position_id)

        with pytest.raises(PositionNotFoundError):
            calculator.get_position(sample_position.position_id)

    def test_get_nonexistent_position(self, calculator: ILCalculator) -> None:
        """Getting a non-existent position should raise error."""
        with pytest.raises(PositionNotFoundError):
            calculator.get_position("nonexistent-id")

    def test_remove_nonexistent_position(self, calculator: ILCalculator) -> None:
        """Removing a non-existent position should raise error."""
        with pytest.raises(PositionNotFoundError):
            calculator.remove_position("nonexistent-id")

    def test_get_all_positions(self, calculator: ILCalculator, sample_position: LPPosition) -> None:
        """Test retrieving all positions."""
        position2 = LPPosition(
            position_id="test-position-2",
            pool_address="0x1234567890123456789012345678901234567890",
            token_a="WBTC",
            token_b="USDC",
            entry_price_a=Decimal("45000"),
            entry_price_b=Decimal("1"),
            amount_a=Decimal("0.1"),
            amount_b=Decimal("4500"),
        )

        calculator.add_position(sample_position)
        calculator.add_position(position2)

        positions = calculator.get_all_positions()
        assert len(positions) == 2


# =============================================================================
# IL Exposure Tests
# =============================================================================


class TestILExposure:
    """Tests for IL exposure calculation on tracked positions."""

    def test_calculate_il_exposure(
        self, calculator_with_mock_prices: ILCalculator, sample_position: LPPosition
    ) -> None:
        """Test calculating IL exposure for a tracked position."""
        calculator_with_mock_prices.add_position(sample_position)

        exposure = calculator_with_mock_prices.calculate_il_exposure(
            position_id=sample_position.position_id,
        )

        assert exposure.position_id == sample_position.position_id
        assert exposure.position == sample_position
        assert exposure.current_il is not None
        assert exposure.entry_value == sample_position.entry_value

    def test_il_exposure_with_fees(
        self, calculator_with_mock_prices: ILCalculator, sample_position: LPPosition
    ) -> None:
        """Test IL exposure calculation including fees earned."""
        calculator_with_mock_prices.add_position(sample_position)

        exposure = calculator_with_mock_prices.calculate_il_exposure(
            position_id=sample_position.position_id,
            fees_earned=Decimal("100"),  # $100 in fees
        )

        assert exposure.fees_earned == Decimal("100")
        assert exposure.net_pnl is not None

    def test_il_exposure_nonexistent_position(self, calculator_with_mock_prices: ILCalculator) -> None:
        """Getting exposure for non-existent position should raise error."""
        with pytest.raises(PositionNotFoundError):
            calculator_with_mock_prices.calculate_il_exposure("nonexistent-id")

    def test_il_exposure_no_prices(self, calculator: ILCalculator, sample_position: LPPosition) -> None:
        """Exposure without mock prices and no price oracle should raise error."""
        calculator.add_position(sample_position)

        with pytest.raises(ILExposureUnavailableError):
            calculator.calculate_il_exposure(sample_position.position_id)

    def test_il_exposure_explicit_prices(self, calculator: ILCalculator, sample_position: LPPosition) -> None:
        """Test exposure calculation with explicitly provided prices."""
        calculator.add_position(sample_position)

        exposure = calculator.calculate_il_exposure(
            position_id=sample_position.position_id,
            current_price_a=Decimal("2500"),
            current_price_b=Decimal("1"),
        )

        assert exposure.current_il is not None
        # Price went from 2000 to 2500 = 25% increase
        assert exposure.current_il.price_ratio > Decimal("1")


# =============================================================================
# Convenience Function Tests
# =============================================================================


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_calculate_il_simple(self) -> None:
        """Test simple IL calculation function."""
        il_percent = calculate_il_simple(
            entry_price=Decimal("2000"),
            current_price=Decimal("3000"),
        )

        # IL should be around -2% for 50% price increase
        assert il_percent < 0
        assert Decimal("-3") < il_percent < Decimal("-1")

    def test_project_il_table(self) -> None:
        """Test IL projection table generation."""
        results = project_il_table([Decimal("0"), Decimal("50"), Decimal("100")])

        assert len(results) == 3
        assert results[0].il_ratio == Decimal("0")  # 0% change = 0 IL
        assert results[1].il_ratio < 0  # 50% change = negative IL
        assert results[2].il_ratio < 0  # 100% change = negative IL

    def test_common_price_changes_constant(self) -> None:
        """Verify COMMON_PRICE_CHANGES constant is properly defined."""
        assert len(COMMON_PRICE_CHANGES) == 10
        assert Decimal("-50") in COMMON_PRICE_CHANGES
        assert Decimal("0") in COMMON_PRICE_CHANGES
        assert Decimal("100") in COMMON_PRICE_CHANGES


# =============================================================================
# Data Class Tests
# =============================================================================


class TestDataClasses:
    """Tests for data class functionality."""

    def test_il_result_to_dict(self, calculator: ILCalculator) -> None:
        """Test ILResult serialization."""
        result = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("3000"),
            current_price_b=Decimal("1"),
        )

        data = result.to_dict()
        assert "il_ratio" in data
        assert "il_percent" in data
        assert "il_bps" in data
        assert "pool_type" in data
        assert data["pool_type"] == "constant_product"

    def test_projected_il_result_to_dict(self, calculator: ILCalculator) -> None:
        """Test ProjectedILResult serialization."""
        result = calculator.project_il(price_change_pct=Decimal("50"))

        data = result.to_dict()
        assert "price_change_pct" in data
        assert "il_ratio" in data
        assert data["price_change_pct"] == "50"

    def test_lp_position_entry_value(self, sample_position: LPPosition) -> None:
        """Test LPPosition entry value calculation."""
        expected_value = (
            sample_position.amount_a * sample_position.entry_price_a
            + sample_position.amount_b * sample_position.entry_price_b
        )
        assert sample_position.entry_value == expected_value

    def test_lp_position_is_concentrated(self, sample_position: LPPosition, concentrated_position: LPPosition) -> None:
        """Test LPPosition is_concentrated property."""
        assert not sample_position.is_concentrated
        assert concentrated_position.is_concentrated

    def test_lp_position_to_dict(self, sample_position: LPPosition) -> None:
        """Test LPPosition serialization."""
        data = sample_position.to_dict()
        assert data["position_id"] == sample_position.position_id
        assert data["pool_type"] == "constant_product"
        assert data["tick_lower"] is None

    def test_il_exposure_offset_by_fees(
        self, calculator_with_mock_prices: ILCalculator, sample_position: LPPosition
    ) -> None:
        """Test IL exposure fee offset property."""
        calculator_with_mock_prices.add_position(sample_position)

        # With large fees that offset IL
        exposure = calculator_with_mock_prices.calculate_il_exposure(
            position_id=sample_position.position_id,
            fees_earned=Decimal("1000"),  # Large fees
        )

        # Check if fees offset the IL (depends on actual IL)
        assert exposure.net_pnl is not None


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_very_small_price_change(self, calculator: ILCalculator) -> None:
        """Test IL for very small price changes."""
        result = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("2001"),  # +0.05%
            current_price_b=Decimal("1"),
        )

        # IL should be very small
        assert abs(result.il_ratio) < Decimal("0.0001")

    def test_very_large_price_change(self, calculator: ILCalculator) -> None:
        """Test IL for very large price changes (100x)."""
        result = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("200000"),  # 100x
            current_price_b=Decimal("1"),
        )

        # IL should be significant but calculable
        assert result.il_ratio < 0
        assert result.il_ratio > Decimal("-1")  # IL can't exceed 100%

    def test_both_prices_change(self, calculator: ILCalculator) -> None:
        """Test IL when both token prices change."""
        result = calculator.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("3000"),  # +50%
            current_price_b=Decimal("2"),  # +100%
        )

        # This is equivalent to A going down 25% relative to B
        # Because ratio was 2000:1, now 3000:2 = 1500:1
        assert result.price_ratio < 1

    def test_mock_prices_set_clear(self, calculator: ILCalculator) -> None:
        """Test setting and clearing mock prices."""
        calculator.set_mock_prices({"WETH": Decimal("2500")})
        assert calculator._mock_prices is not None
        assert calculator._mock_prices["WETH"] == Decimal("2500")

        calculator.clear_mock_prices()
        assert calculator._mock_prices is None


# =============================================================================
# Known IL Values Tests (Verification against known formulas)
# =============================================================================


class TestKnownILValues:
    """Tests verifying IL calculations against known values."""

    def test_il_2x_price(self, calculator: ILCalculator) -> None:
        """2x price should result in ~5.72% IL."""
        result = calculator.calculate_il(
            entry_price_a=Decimal("100"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("200"),  # 2x
            current_price_b=Decimal("1"),
        )

        # IL for 2x = 2 * sqrt(2) / (1 + 2) - 1 = 2 * 1.414 / 3 - 1 ≈ -0.0572
        expected_il = Decimal("-0.0572")
        assert abs(result.il_ratio - expected_il) < Decimal("0.001")

    def test_il_4x_price(self, calculator: ILCalculator) -> None:
        """4x price should result in ~20% IL."""
        result = calculator.calculate_il(
            entry_price_a=Decimal("100"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("400"),  # 4x
            current_price_b=Decimal("1"),
        )

        # IL for 4x = 2 * sqrt(4) / (1 + 4) - 1 = 2 * 2 / 5 - 1 = -0.2
        expected_il = Decimal("-0.2")
        assert abs(result.il_ratio - expected_il) < Decimal("0.001")

    def test_il_0_25x_price(self, calculator: ILCalculator) -> None:
        """0.25x price (75% drop) should result in ~20% IL."""
        result = calculator.calculate_il(
            entry_price_a=Decimal("400"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("100"),  # 0.25x
            current_price_b=Decimal("1"),
        )

        # IL for 0.25x = 2 * sqrt(0.25) / (1 + 0.25) - 1 = 2 * 0.5 / 1.25 - 1 = -0.2
        expected_il = Decimal("-0.2")
        assert abs(result.il_ratio - expected_il) < Decimal("0.001")
