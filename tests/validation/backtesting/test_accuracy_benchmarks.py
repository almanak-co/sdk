"""Accuracy validation benchmarks for backtesting calculations.

This module contains validation tests that verify backtesting calculations
match known mathematical reference values. Tests ensure:

1. Impermanent Loss (IL) calculations match the standard V2/V3 formulas
2. Funding rate calculations match protocol documentation
3. Results are within specified tolerance thresholds

Reference Formulas:
    - V2 IL Formula: IL = 1 - 2 * sqrt(k) / (1 + k) where k = price_ratio
    - This formula is symmetric: 50% gain and 66.67% loss give the same IL
    - IL is always non-negative (represents value loss vs holding)

Standard IL Reference Values (V2/full-range):
    Price Change  |  Price Ratio (k)  |  IL (loss)
    -------------|-------------------|------------
    +25%         |  1.25             |  0.62%
    +50%         |  1.50             |  2.0%
    +100%        |  2.00             |  5.7%
    +200%        |  3.00             |  13.4%
    -25%         |  0.75             |  1.03%
    -50%         |  0.50             |  5.7%

Note: 50% price increase and 50% price decrease give DIFFERENT IL values
because the formula is based on price RATIO, not percentage change.
- 50% increase: new_price = 1.5 * old_price, ratio k = 1.5
- 50% decrease: new_price = 0.5 * old_price, ratio k = 0.5

To run:
    uv run pytest tests/validation/backtesting/test_accuracy_benchmarks.py -v

Markers:
    @pytest.mark.validation - All tests in this module are validation tests
"""

from datetime import datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
    MAX_TICK,
    MIN_TICK,
    ImpermanentLossCalculator,
)

# =============================================================================
# Mathematical Reference Values for IL
# =============================================================================
# These values are derived from the standard Uniswap V2 impermanent loss formula:
#   IL = 1 - 2 * sqrt(k) / (1 + k)
# where k = current_price / entry_price (the price ratio)
#
# The formula produces these canonical values:
#   - k = 1.5 (50% increase): IL = 1 - 2*sqrt(1.5)/2.5 = 1 - 2*1.2247/2.5 = 0.0202 (2.02%)
#   - k = 2.0 (100% increase): IL = 1 - 2*sqrt(2.0)/3.0 = 1 - 2*1.4142/3.0 = 0.0572 (5.72%)
#   - k = 0.5 (50% decrease): IL = 1 - 2*sqrt(0.5)/1.5 = 1 - 2*0.7071/1.5 = 0.0572 (5.72%)
#
# Note: The PRD specified -0.0132 for 50% price increase, but the mathematically
# correct value is 0.0202 (2.02%). The value 0.0132 corresponds to approximately
# a 30% price change. These tests use the mathematically correct reference values.

# Reference IL values for common price changes (mathematically derived)
IL_REFERENCE_VALUES = {
    # (price_ratio, expected_il, description)
    "50_pct_increase": (Decimal("1.5"), Decimal("0.02020"), "50% price increase (k=1.5)"),
    "100_pct_increase": (Decimal("2.0"), Decimal("0.05719"), "100% price increase (k=2.0)"),
    "50_pct_decrease": (Decimal("0.5"), Decimal("0.05719"), "50% price decrease (k=0.5)"),
    "25_pct_increase": (Decimal("1.25"), Decimal("0.00621"), "25% price increase (k=1.25)"),
    "25_pct_decrease": (Decimal("0.75"), Decimal("0.01026"), "25% price decrease (k=0.75)"),
    "200_pct_increase": (Decimal("3.0"), Decimal("0.13397"), "200% price increase (k=3.0)"),
    "no_change": (Decimal("1.0"), Decimal("0.0"), "No price change (k=1.0)"),
}

# Tolerance for IL validation (0.1% = 0.001 in decimal form)
# Per acceptance criteria: "Test results within 0.1% of reference values"
IL_TOLERANCE = Decimal("0.001")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def il_calculator() -> ImpermanentLossCalculator:
    """Create ImpermanentLossCalculator instance for tests."""
    return ImpermanentLossCalculator(precision=28)


# =============================================================================
# IL Calculation Validation Tests (US-038)
# =============================================================================


@pytest.mark.validation
class TestILCalculationAccuracyBenchmarks:
    """Benchmark tests validating IL calculations match mathematical reference values.

    These tests verify that the ImpermanentLossCalculator produces results
    that are within 0.1% of the mathematically derived reference values
    for the standard Uniswap V2 impermanent loss formula.
    """

    def test_il_50_percent_price_increase(self, il_calculator: ImpermanentLossCalculator):
        """Test IL calculation for 50% price increase matches expected ~0.0202.

        Reference: k = 1.5
        IL = 1 - 2*sqrt(1.5)/2.5 = 1 - 0.9798 = 0.0202 (2.02%)
        """
        price_ratio, expected_il, description = IL_REFERENCE_VALUES["50_pct_increase"]

        # Use full range ticks for V2-equivalent calculation
        calculated_il = il_calculator.calculate_il_for_price_change(
            price_ratio=price_ratio,
            tick_lower=MIN_TICK,
            tick_upper=MAX_TICK,
        )

        error = abs(calculated_il - expected_il)

        assert error <= IL_TOLERANCE, (
            f"IL for {description} exceeds tolerance.\n"
            f"  Calculated: {calculated_il:.6f} ({float(calculated_il) * 100:.4f}%)\n"
            f"  Expected:   {expected_il:.6f} ({float(expected_il) * 100:.4f}%)\n"
            f"  Error:      {error:.6f} (tolerance: {IL_TOLERANCE})"
        )

    def test_il_100_percent_price_increase(self, il_calculator: ImpermanentLossCalculator):
        """Test IL calculation for 100% price increase matches expected ~0.0572.

        Reference: k = 2.0
        IL = 1 - 2*sqrt(2.0)/3.0 = 1 - 0.9428 = 0.0572 (5.72%)
        """
        price_ratio, expected_il, description = IL_REFERENCE_VALUES["100_pct_increase"]

        calculated_il = il_calculator.calculate_il_for_price_change(
            price_ratio=price_ratio,
            tick_lower=MIN_TICK,
            tick_upper=MAX_TICK,
        )

        error = abs(calculated_il - expected_il)

        assert error <= IL_TOLERANCE, (
            f"IL for {description} exceeds tolerance.\n"
            f"  Calculated: {calculated_il:.6f} ({float(calculated_il) * 100:.4f}%)\n"
            f"  Expected:   {expected_il:.6f} ({float(expected_il) * 100:.4f}%)\n"
            f"  Error:      {error:.6f} (tolerance: {IL_TOLERANCE})"
        )

    def test_il_50_percent_price_decrease(self, il_calculator: ImpermanentLossCalculator):
        """Test IL calculation for 50% price decrease matches expected ~0.0572.

        Reference: k = 0.5
        IL = 1 - 2*sqrt(0.5)/1.5 = 1 - 0.9428 = 0.0572 (5.72%)

        Note: 50% price DECREASE (k=0.5) has the same IL as 100% price INCREASE (k=2.0)
        because sqrt(0.5) = 1/sqrt(2) and the formula is symmetric around these points.
        """
        price_ratio, expected_il, description = IL_REFERENCE_VALUES["50_pct_decrease"]

        calculated_il = il_calculator.calculate_il_for_price_change(
            price_ratio=price_ratio,
            tick_lower=MIN_TICK,
            tick_upper=MAX_TICK,
        )

        error = abs(calculated_il - expected_il)

        assert error <= IL_TOLERANCE, (
            f"IL for {description} exceeds tolerance.\n"
            f"  Calculated: {calculated_il:.6f} ({float(calculated_il) * 100:.4f}%)\n"
            f"  Expected:   {expected_il:.6f} ({float(expected_il) * 100:.4f}%)\n"
            f"  Error:      {error:.6f} (tolerance: {IL_TOLERANCE})"
        )

    def test_il_no_price_change(self, il_calculator: ImpermanentLossCalculator):
        """Test IL calculation for no price change equals zero.

        Reference: k = 1.0
        IL = 1 - 2*sqrt(1.0)/2.0 = 1 - 1.0 = 0.0 (0%)

        When price doesn't change, there is no impermanent loss.
        """
        price_ratio, expected_il, description = IL_REFERENCE_VALUES["no_change"]

        calculated_il = il_calculator.calculate_il_for_price_change(
            price_ratio=price_ratio,
            tick_lower=MIN_TICK,
            tick_upper=MAX_TICK,
        )

        # For zero, we check absolute value is very small
        assert calculated_il <= Decimal("0.0001"), (
            f"IL for {description} should be zero.\n"
            f"  Calculated: {calculated_il:.6f} ({float(calculated_il) * 100:.4f}%)\n"
            f"  Expected:   0.0 (0%)"
        )

    def test_il_25_percent_price_increase(self, il_calculator: ImpermanentLossCalculator):
        """Test IL calculation for 25% price increase matches expected ~0.0062.

        Reference: k = 1.25
        IL = 1 - 2*sqrt(1.25)/2.25 = 0.0062 (0.62%)
        """
        price_ratio, expected_il, description = IL_REFERENCE_VALUES["25_pct_increase"]

        calculated_il = il_calculator.calculate_il_for_price_change(
            price_ratio=price_ratio,
            tick_lower=MIN_TICK,
            tick_upper=MAX_TICK,
        )

        error = abs(calculated_il - expected_il)

        assert error <= IL_TOLERANCE, (
            f"IL for {description} exceeds tolerance.\n"
            f"  Calculated: {calculated_il:.6f} ({float(calculated_il) * 100:.4f}%)\n"
            f"  Expected:   {expected_il:.6f} ({float(expected_il) * 100:.4f}%)\n"
            f"  Error:      {error:.6f} (tolerance: {IL_TOLERANCE})"
        )

    def test_il_25_percent_price_decrease(self, il_calculator: ImpermanentLossCalculator):
        """Test IL calculation for 25% price decrease matches expected ~0.0103.

        Reference: k = 0.75
        IL = 1 - 2*sqrt(0.75)/1.75 = 0.0103 (1.03%)

        Note: 25% increase (k=1.25) and 25% decrease (k=0.75) have DIFFERENT IL
        because they are not inverse ratios. The inverse of 1.25 is 0.8, not 0.75.
        """
        price_ratio, expected_il, description = IL_REFERENCE_VALUES["25_pct_decrease"]

        calculated_il = il_calculator.calculate_il_for_price_change(
            price_ratio=price_ratio,
            tick_lower=MIN_TICK,
            tick_upper=MAX_TICK,
        )

        error = abs(calculated_il - expected_il)

        assert error <= IL_TOLERANCE, (
            f"IL for {description} exceeds tolerance.\n"
            f"  Calculated: {calculated_il:.6f} ({float(calculated_il) * 100:.4f}%)\n"
            f"  Expected:   {expected_il:.6f} ({float(expected_il) * 100:.4f}%)\n"
            f"  Error:      {error:.6f} (tolerance: {IL_TOLERANCE})"
        )

    def test_il_200_percent_price_increase(self, il_calculator: ImpermanentLossCalculator):
        """Test IL calculation for 200% price increase (3x) matches expected ~0.134.

        Reference: k = 3.0
        IL = 1 - 2*sqrt(3.0)/4.0 = 1 - 0.866 = 0.134 (13.4%)
        """
        price_ratio, expected_il, description = IL_REFERENCE_VALUES["200_pct_increase"]

        calculated_il = il_calculator.calculate_il_for_price_change(
            price_ratio=price_ratio,
            tick_lower=MIN_TICK,
            tick_upper=MAX_TICK,
        )

        error = abs(calculated_il - expected_il)

        assert error <= IL_TOLERANCE, (
            f"IL for {description} exceeds tolerance.\n"
            f"  Calculated: {calculated_il:.6f} ({float(calculated_il) * 100:.4f}%)\n"
            f"  Expected:   {expected_il:.6f} ({float(expected_il) * 100:.4f}%)\n"
            f"  Error:      {error:.6f} (tolerance: {IL_TOLERANCE})"
        )

    @pytest.mark.parametrize(
        "test_id,price_ratio,expected_il,description",
        [
            ("50_pct_increase", *IL_REFERENCE_VALUES["50_pct_increase"]),
            ("100_pct_increase", *IL_REFERENCE_VALUES["100_pct_increase"]),
            ("50_pct_decrease", *IL_REFERENCE_VALUES["50_pct_decrease"]),
            ("25_pct_increase", *IL_REFERENCE_VALUES["25_pct_increase"]),
            ("25_pct_decrease", *IL_REFERENCE_VALUES["25_pct_decrease"]),
            ("200_pct_increase", *IL_REFERENCE_VALUES["200_pct_increase"]),
        ],
        ids=["50%+", "100%+", "50%-", "25%+", "25%-", "200%+"],
    )
    def test_il_parametrized_all_reference_values(
        self,
        il_calculator: ImpermanentLossCalculator,
        test_id: str,
        price_ratio: Decimal,
        expected_il: Decimal,
        description: str,
    ):
        """Parametrized test verifying all IL reference values within tolerance."""
        calculated_il = il_calculator.calculate_il_for_price_change(
            price_ratio=price_ratio,
            tick_lower=MIN_TICK,
            tick_upper=MAX_TICK,
        )

        error = abs(calculated_il - expected_il)

        assert error <= IL_TOLERANCE, (
            f"IL for {description} exceeds tolerance.\n"
            f"  Test ID:    {test_id}\n"
            f"  Calculated: {calculated_il:.6f} ({float(calculated_il) * 100:.4f}%)\n"
            f"  Expected:   {expected_il:.6f} ({float(expected_il) * 100:.4f}%)\n"
            f"  Error:      {error:.6f} (tolerance: {IL_TOLERANCE})"
        )


@pytest.mark.validation
class TestILSymmetryProperties:
    """Tests validating mathematical properties of IL formula."""

    def test_il_symmetric_for_inverse_ratios(self, il_calculator: ImpermanentLossCalculator):
        """Test that IL for k and 1/k are equal (formula symmetry property).

        The V2 IL formula has the property that IL(k) = IL(1/k), meaning:
        - 100% increase (k=2) and 50% decrease (k=0.5) have the same IL
        - 300% increase (k=4) and 75% decrease (k=0.25) have the same IL
        """
        test_cases = [
            (Decimal("2.0"), Decimal("0.5"), "2x and 0.5x"),
            (Decimal("4.0"), Decimal("0.25"), "4x and 0.25x"),
            (Decimal("1.5"), Decimal("0.6666666667"), "1.5x and 0.667x"),
        ]

        for k, inverse_k, description in test_cases:
            il_k = il_calculator.calculate_il_for_price_change(k, MIN_TICK, MAX_TICK)
            il_inv_k = il_calculator.calculate_il_for_price_change(inverse_k, MIN_TICK, MAX_TICK)

            # They should be very close (within tolerance)
            difference = abs(il_k - il_inv_k)
            assert difference <= IL_TOLERANCE, (
                f"IL symmetry failed for {description}.\n"
                f"  IL(k={k}):     {il_k:.6f}\n"
                f"  IL(1/k={inverse_k}): {il_inv_k:.6f}\n"
                f"  Difference:   {difference:.6f}"
            )

    def test_il_always_non_negative(self, il_calculator: ImpermanentLossCalculator):
        """Test that IL is always non-negative for any price change."""
        test_ratios = [
            Decimal("0.1"),  # 90% crash
            Decimal("0.5"),  # 50% drop
            Decimal("0.9"),  # 10% drop
            Decimal("1.0"),  # no change
            Decimal("1.1"),  # 10% gain
            Decimal("2.0"),  # 100% gain
            Decimal("10.0"),  # 900% gain
        ]

        for ratio in test_ratios:
            il = il_calculator.calculate_il_for_price_change(ratio, MIN_TICK, MAX_TICK)
            assert il >= Decimal("0"), f"IL for k={ratio} should be non-negative, got {il}"

    def test_il_increases_with_price_deviation(self, il_calculator: ImpermanentLossCalculator):
        """Test that IL increases as price deviates further from entry.

        IL should increase monotonically as the price ratio moves away from 1.0
        in either direction.
        """
        # Test increasing price
        il_small = il_calculator.calculate_il_for_price_change(Decimal("1.1"), MIN_TICK, MAX_TICK)
        il_medium = il_calculator.calculate_il_for_price_change(Decimal("1.5"), MIN_TICK, MAX_TICK)
        il_large = il_calculator.calculate_il_for_price_change(Decimal("2.0"), MIN_TICK, MAX_TICK)

        assert il_small < il_medium < il_large, (
            f"IL should increase with deviation.\n"
            f"  IL(1.1x): {il_small:.6f}\n"
            f"  IL(1.5x): {il_medium:.6f}\n"
            f"  IL(2.0x): {il_large:.6f}"
        )

        # Test decreasing price
        il_small_down = il_calculator.calculate_il_for_price_change(Decimal("0.9"), MIN_TICK, MAX_TICK)
        il_medium_down = il_calculator.calculate_il_for_price_change(Decimal("0.7"), MIN_TICK, MAX_TICK)
        il_large_down = il_calculator.calculate_il_for_price_change(Decimal("0.5"), MIN_TICK, MAX_TICK)

        assert il_small_down < il_medium_down < il_large_down, (
            f"IL should increase with deviation (downward).\n"
            f"  IL(0.9x): {il_small_down:.6f}\n"
            f"  IL(0.7x): {il_medium_down:.6f}\n"
            f"  IL(0.5x): {il_large_down:.6f}"
        )


@pytest.mark.validation
class TestILV3CalculationAccuracy:
    """Tests validating V3 IL calculation with concentrated liquidity positions."""

    def test_v3_full_range_matches_v2_formula(self, il_calculator: ImpermanentLossCalculator):
        """Test that V3 with full range matches V2 formula results.

        A V3 position with MIN_TICK to MAX_TICK should behave exactly like V2.
        """
        entry_price = Decimal("2000")
        current_price = Decimal("3000")  # 50% increase
        liquidity = Decimal("1000000")

        il_pct, _, _ = il_calculator.calculate_il_v3(
            entry_price=entry_price,
            current_price=current_price,
            tick_lower=MIN_TICK,
            tick_upper=MAX_TICK,
            liquidity=liquidity,
        )

        # Compare to V2 formula
        price_ratio = current_price / entry_price
        il_v2 = il_calculator.calculate_il_for_price_change(price_ratio, MIN_TICK, MAX_TICK)

        error = abs(il_pct - il_v2)
        assert error <= IL_TOLERANCE, (
            f"V3 full range should match V2 formula.\n"
            f"  V3 IL: {il_pct:.6f}\n"
            f"  V2 IL: {il_v2:.6f}\n"
            f"  Error: {error:.6f}"
        )

    def test_v3_concentrated_range_higher_il(self, il_calculator: ImpermanentLossCalculator):
        """Test that concentrated V3 position has higher IL when price exits range.

        Concentrated liquidity positions experience higher IL when the price
        moves outside their range compared to full-range positions.
        """
        entry_price = Decimal("2000")
        current_price = Decimal("3000")  # Price moves significantly
        liquidity = Decimal("1000000")

        # Full range position
        il_full, _, _ = il_calculator.calculate_il_v3(
            entry_price=entry_price,
            current_price=current_price,
            tick_lower=MIN_TICK,
            tick_upper=MAX_TICK,
            liquidity=liquidity,
        )

        # Narrow range position (roughly ±10% around entry)
        # tick = log(price) / log(1.0001)
        # For entry_price=2000: tick ≈ 76012
        # ±10% range: [1800, 2200] -> ticks [74500, 77500] approximately
        il_narrow, _, _ = il_calculator.calculate_il_v3(
            entry_price=entry_price,
            current_price=current_price,
            tick_lower=74500,
            tick_upper=77500,
            liquidity=liquidity,
        )

        # Narrow range should have higher IL when price exits the range
        assert il_narrow >= il_full, (
            f"Concentrated position should have >= IL when price exits range.\n"
            f"  Full range IL:   {il_full:.6f}\n"
            f"  Narrow range IL: {il_narrow:.6f}"
        )

    def test_v3_position_in_range_reasonable_il(self, il_calculator: ImpermanentLossCalculator):
        """Test that V3 position within range has reasonable IL."""
        entry_price = Decimal("2000")
        current_price = Decimal("2100")  # 5% increase, stays in range
        liquidity = Decimal("1000000")

        # Range that includes both entry and current price
        # For ±20% range around $2000: [1600, 2400]
        il_pct, token0, token1 = il_calculator.calculate_il_v3(
            entry_price=entry_price,
            current_price=current_price,
            tick_lower=71000,  # ~$1600
            tick_upper=78000,  # ~$2400
            liquidity=liquidity,
        )

        # IL should be small for small price move within range
        assert il_pct < Decimal("0.01"), f"IL for small in-range move should be < 1%, got {il_pct:.4%}"

        # Token amounts should be positive
        assert token0 >= Decimal("0"), f"Token0 amount should be non-negative, got {token0}"
        assert token1 >= Decimal("0"), f"Token1 amount should be non-negative, got {token1}"


# =============================================================================
# Funding Rate Calculation Validation Tests (US-039)
# =============================================================================


# Mathematical Reference Values for Funding Calculations
# Funding payment formula: payment = position_value * funding_rate * time_hours
# Sign convention:
#   - PERP_LONG with positive rate: PAYS (negative payment)
#   - PERP_SHORT with positive rate: RECEIVES (positive payment)
#
# Reference calculation per US-039 acceptance criteria:
#   $10,000 position at 0.01%/hr for 24 hours:
#   payment = $10,000 * 0.0001 * 24 = $24
#
# For PERP_LONG: pays $24 (payment = -$24)
# For PERP_SHORT: receives $24 (payment = +$24)

FUNDING_REFERENCE_VALUES = {
    # (position_value_usd, funding_rate_hourly, hours, expected_payment_magnitude)
    "10k_24h_standard": (
        Decimal("10000"),
        Decimal("0.0001"),  # 0.01% per hour
        Decimal("24"),
        Decimal("24"),  # $24 (absolute value)
    ),
    "50k_24h_standard": (
        Decimal("50000"),
        Decimal("0.0001"),  # 0.01% per hour
        Decimal("24"),
        Decimal("120"),  # $120 (absolute value)
    ),
    "10k_24h_high_rate": (
        Decimal("10000"),
        Decimal("0.0005"),  # 0.05% per hour
        Decimal("24"),
        Decimal("120"),  # $120 (absolute value)
    ),
    "10k_1h_standard": (
        Decimal("10000"),
        Decimal("0.0001"),  # 0.01% per hour
        Decimal("1"),
        Decimal("1"),  # $1 (absolute value)
    ),
    "100k_8h_binance": (
        Decimal("100000"),
        Decimal("0.000125"),  # 0.0125% per hour (~0.1% per 8h)
        Decimal("8"),
        Decimal("100"),  # $100 (absolute value)
    ),
}


@pytest.fixture
def funding_calculator():
    """Create FundingCalculator instance for tests."""
    from almanak.framework.backtesting.pnl.calculators.funding import FundingCalculator

    return FundingCalculator()


@pytest.fixture
def funding_rate_handler():
    """Create FundingRateHandler instance for tests."""
    from almanak.framework.backtesting.pnl.calculators.funding import FundingRateHandler

    return FundingRateHandler()


def create_perp_position(
    position_type_str: str,
    notional_usd: Decimal,
    protocol: str = "gmx",
) -> "SimulatedPosition":
    """Helper to create a perp position for testing.

    Args:
        position_type_str: "PERP_LONG" or "PERP_SHORT"
        notional_usd: Notional position value in USD
        protocol: Protocol name (default "gmx")

    Returns:
        SimulatedPosition configured for perp testing
    """
    from datetime import timezone

    from almanak.framework.backtesting.pnl.portfolio import PositionType, SimulatedPosition

    position_type = PositionType(position_type_str)
    leverage = Decimal("5")
    collateral_usd = notional_usd / leverage

    return SimulatedPosition(
        position_type=position_type,
        protocol=protocol,
        tokens=["ETH"],
        amounts={"ETH": notional_usd / Decimal("2000")},  # Assume $2000/ETH
        entry_price=Decimal("2000"),
        entry_time=datetime.now(timezone.utc),
        leverage=leverage,
        collateral_usd=collateral_usd,
        notional_usd=notional_usd,
        entry_funding_index=Decimal("0"),
        accumulated_funding=Decimal("0"),
        cumulative_funding_paid=Decimal("0"),
        cumulative_funding_received=Decimal("0"),
    )


# Import SimulatedPosition for type hints
from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition


@pytest.mark.validation
class TestFundingCalculationAccuracyBenchmarks:
    """Benchmark tests validating funding calculations match protocol documentation.

    These tests verify that the FundingCalculator produces results
    that exactly match the expected funding payment calculations.

    Per US-039 acceptance criteria:
    - $10k position at 0.01%/hr for 24hrs = $24 funding
    - Both long and short positions handled correctly
    - Results match expected values exactly
    """

    def test_funding_10k_position_24h_at_001_pct(self, funding_calculator):
        """Test funding calculation for $10k position at 0.01%/hr for 24hrs equals $24.

        Reference calculation:
            payment = $10,000 * 0.0001 * 24 = $24

        This is the primary acceptance criteria test from US-039.
        """
        position_value, funding_rate, hours, expected_magnitude = FUNDING_REFERENCE_VALUES[
            "10k_24h_standard"
        ]

        # Create a long position
        long_position = create_perp_position("PERP_LONG", position_value)

        result = funding_calculator.calculate_funding_payment(
            position=long_position,
            funding_rate=funding_rate,
            time_delta_hours=hours,
        )

        # For PERP_LONG with positive funding rate, payment should be negative (pays)
        assert result.payment == -expected_magnitude, (
            f"Long position funding payment incorrect.\n"
            f"  Expected: -${expected_magnitude} (pays)\n"
            f"  Got:      ${result.payment}\n"
            f"  Position value: ${position_value}\n"
            f"  Rate: {funding_rate} ({float(funding_rate) * 100:.4f}%/hr)\n"
            f"  Hours: {hours}"
        )

    def test_funding_long_position_pays_when_rate_positive(self, funding_calculator):
        """Test that PERP_LONG pays funding when rate is positive."""
        position_value, funding_rate, hours, expected_magnitude = FUNDING_REFERENCE_VALUES[
            "10k_24h_standard"
        ]

        long_position = create_perp_position("PERP_LONG", position_value)

        result = funding_calculator.calculate_funding_payment(
            position=long_position,
            funding_rate=funding_rate,
            time_delta_hours=hours,
        )

        # Long pays when rate is positive (payment is negative)
        assert result.payment < Decimal("0"), (
            f"Long position should PAY (negative) when rate is positive.\n"
            f"  Got payment: ${result.payment}"
        )
        assert result.is_payer is True, "Long position should be marked as payer"
        assert abs(result.payment) == expected_magnitude

    def test_funding_short_position_receives_when_rate_positive(self, funding_calculator):
        """Test that PERP_SHORT receives funding when rate is positive."""
        position_value, funding_rate, hours, expected_magnitude = FUNDING_REFERENCE_VALUES[
            "10k_24h_standard"
        ]

        short_position = create_perp_position("PERP_SHORT", position_value)

        result = funding_calculator.calculate_funding_payment(
            position=short_position,
            funding_rate=funding_rate,
            time_delta_hours=hours,
        )

        # Short receives when rate is positive (payment is positive)
        assert result.payment > Decimal("0"), (
            f"Short position should RECEIVE (positive) when rate is positive.\n"
            f"  Got payment: ${result.payment}"
        )
        assert result.is_payer is False, "Short position should NOT be marked as payer"
        assert result.payment == expected_magnitude

    def test_funding_long_receives_when_rate_negative(self, funding_calculator):
        """Test that PERP_LONG receives funding when rate is negative."""
        position_value = Decimal("10000")
        funding_rate = Decimal("-0.0001")  # Negative rate (shorts pay longs)
        hours = Decimal("24")
        expected_magnitude = Decimal("24")

        long_position = create_perp_position("PERP_LONG", position_value)

        result = funding_calculator.calculate_funding_payment(
            position=long_position,
            funding_rate=funding_rate,
            time_delta_hours=hours,
        )

        # Long receives when rate is negative (payment is positive)
        assert result.payment > Decimal("0"), (
            f"Long position should RECEIVE (positive) when rate is negative.\n"
            f"  Got payment: ${result.payment}"
        )
        assert result.payment == expected_magnitude

    def test_funding_short_pays_when_rate_negative(self, funding_calculator):
        """Test that PERP_SHORT pays funding when rate is negative."""
        position_value = Decimal("10000")
        funding_rate = Decimal("-0.0001")  # Negative rate (shorts pay longs)
        hours = Decimal("24")
        expected_magnitude = Decimal("24")

        short_position = create_perp_position("PERP_SHORT", position_value)

        result = funding_calculator.calculate_funding_payment(
            position=short_position,
            funding_rate=funding_rate,
            time_delta_hours=hours,
        )

        # Short pays when rate is negative (payment is negative)
        assert result.payment < Decimal("0"), (
            f"Short position should PAY (negative) when rate is negative.\n"
            f"  Got payment: ${result.payment}"
        )
        assert result.payment == -expected_magnitude

    @pytest.mark.parametrize(
        "test_id,position_value,funding_rate,hours,expected_magnitude",
        [
            ("10k_24h_standard", *FUNDING_REFERENCE_VALUES["10k_24h_standard"]),
            ("50k_24h_standard", *FUNDING_REFERENCE_VALUES["50k_24h_standard"]),
            ("10k_24h_high_rate", *FUNDING_REFERENCE_VALUES["10k_24h_high_rate"]),
            ("10k_1h_standard", *FUNDING_REFERENCE_VALUES["10k_1h_standard"]),
            ("100k_8h_binance", *FUNDING_REFERENCE_VALUES["100k_8h_binance"]),
        ],
        ids=["$10k/24h/0.01%", "$50k/24h/0.01%", "$10k/24h/0.05%", "$10k/1h/0.01%", "$100k/8h/binance"],
    )
    def test_funding_parametrized_all_reference_values_long(
        self,
        funding_calculator,
        test_id: str,
        position_value: Decimal,
        funding_rate: Decimal,
        hours: Decimal,
        expected_magnitude: Decimal,
    ):
        """Parametrized test verifying funding calculations for PERP_LONG positions."""
        long_position = create_perp_position("PERP_LONG", position_value)

        result = funding_calculator.calculate_funding_payment(
            position=long_position,
            funding_rate=funding_rate,
            time_delta_hours=hours,
        )

        # Long pays when rate is positive
        assert result.payment == -expected_magnitude, (
            f"Funding for {test_id} (LONG) incorrect.\n"
            f"  Expected: -${expected_magnitude}\n"
            f"  Got:      ${result.payment}\n"
            f"  Position: ${position_value}, Rate: {funding_rate}, Hours: {hours}"
        )

    @pytest.mark.parametrize(
        "test_id,position_value,funding_rate,hours,expected_magnitude",
        [
            ("10k_24h_standard", *FUNDING_REFERENCE_VALUES["10k_24h_standard"]),
            ("50k_24h_standard", *FUNDING_REFERENCE_VALUES["50k_24h_standard"]),
            ("10k_24h_high_rate", *FUNDING_REFERENCE_VALUES["10k_24h_high_rate"]),
            ("10k_1h_standard", *FUNDING_REFERENCE_VALUES["10k_1h_standard"]),
            ("100k_8h_binance", *FUNDING_REFERENCE_VALUES["100k_8h_binance"]),
        ],
        ids=["$10k/24h/0.01%", "$50k/24h/0.01%", "$10k/24h/0.05%", "$10k/1h/0.01%", "$100k/8h/binance"],
    )
    def test_funding_parametrized_all_reference_values_short(
        self,
        funding_calculator,
        test_id: str,
        position_value: Decimal,
        funding_rate: Decimal,
        hours: Decimal,
        expected_magnitude: Decimal,
    ):
        """Parametrized test verifying funding calculations for PERP_SHORT positions."""
        short_position = create_perp_position("PERP_SHORT", position_value)

        result = funding_calculator.calculate_funding_payment(
            position=short_position,
            funding_rate=funding_rate,
            time_delta_hours=hours,
        )

        # Short receives when rate is positive
        assert result.payment == expected_magnitude, (
            f"Funding for {test_id} (SHORT) incorrect.\n"
            f"  Expected: +${expected_magnitude}\n"
            f"  Got:      ${result.payment}\n"
            f"  Position: ${position_value}, Rate: {funding_rate}, Hours: {hours}"
        )


@pytest.mark.validation
class TestFundingSymmetryProperties:
    """Tests validating mathematical properties of funding calculations."""

    def test_funding_long_short_net_to_zero(self, funding_calculator):
        """Test that matching long and short positions net to zero funding.

        In the real market, funding is a zero-sum game between longs and shorts.
        The sum of all funding paid should equal the sum of all funding received.
        """
        position_value = Decimal("10000")
        funding_rate = Decimal("0.0001")
        hours = Decimal("24")

        long_position = create_perp_position("PERP_LONG", position_value)
        short_position = create_perp_position("PERP_SHORT", position_value)

        long_result = funding_calculator.calculate_funding_payment(
            position=long_position,
            funding_rate=funding_rate,
            time_delta_hours=hours,
        )

        short_result = funding_calculator.calculate_funding_payment(
            position=short_position,
            funding_rate=funding_rate,
            time_delta_hours=hours,
        )

        # Net funding should be zero
        net_funding = long_result.payment + short_result.payment
        assert net_funding == Decimal("0"), (
            f"Long and short funding should net to zero.\n"
            f"  Long payment:  ${long_result.payment}\n"
            f"  Short payment: ${short_result.payment}\n"
            f"  Net:           ${net_funding}"
        )

    def test_funding_scales_linearly_with_position_size(self, funding_calculator):
        """Test that funding scales linearly with position size."""
        base_value = Decimal("10000")
        funding_rate = Decimal("0.0001")
        hours = Decimal("24")

        position_1x = create_perp_position("PERP_LONG", base_value)
        position_2x = create_perp_position("PERP_LONG", base_value * 2)
        position_5x = create_perp_position("PERP_LONG", base_value * 5)

        result_1x = funding_calculator.calculate_funding_payment(
            position=position_1x,
            funding_rate=funding_rate,
            time_delta_hours=hours,
        )

        result_2x = funding_calculator.calculate_funding_payment(
            position=position_2x,
            funding_rate=funding_rate,
            time_delta_hours=hours,
        )

        result_5x = funding_calculator.calculate_funding_payment(
            position=position_5x,
            funding_rate=funding_rate,
            time_delta_hours=hours,
        )

        # 2x position should have 2x funding
        assert result_2x.payment == result_1x.payment * 2, (
            f"2x position should have 2x funding.\n"
            f"  1x payment: ${result_1x.payment}\n"
            f"  2x payment: ${result_2x.payment} (expected: ${result_1x.payment * 2})"
        )

        # 5x position should have 5x funding
        assert result_5x.payment == result_1x.payment * 5, (
            f"5x position should have 5x funding.\n"
            f"  1x payment: ${result_1x.payment}\n"
            f"  5x payment: ${result_5x.payment} (expected: ${result_1x.payment * 5})"
        )

    def test_funding_scales_linearly_with_time(self, funding_calculator):
        """Test that funding scales linearly with time."""
        position_value = Decimal("10000")
        funding_rate = Decimal("0.0001")

        position = create_perp_position("PERP_LONG", position_value)

        result_1h = funding_calculator.calculate_funding_payment(
            position=position,
            funding_rate=funding_rate,
            time_delta_hours=Decimal("1"),
        )

        result_24h = funding_calculator.calculate_funding_payment(
            position=position,
            funding_rate=funding_rate,
            time_delta_hours=Decimal("24"),
        )

        result_168h = funding_calculator.calculate_funding_payment(
            position=position,
            funding_rate=funding_rate,
            time_delta_hours=Decimal("168"),  # 1 week
        )

        # 24h should be 24x of 1h
        assert result_24h.payment == result_1h.payment * 24, (
            f"24h funding should be 24x of 1h funding.\n"
            f"  1h payment:  ${result_1h.payment}\n"
            f"  24h payment: ${result_24h.payment} (expected: ${result_1h.payment * 24})"
        )

        # 168h should be 168x of 1h
        assert result_168h.payment == result_1h.payment * 168, (
            f"168h funding should be 168x of 1h funding.\n"
            f"  1h payment:   ${result_1h.payment}\n"
            f"  168h payment: ${result_168h.payment} (expected: ${result_1h.payment * 168})"
        )

    def test_funding_zero_rate_means_zero_payment(self, funding_calculator):
        """Test that zero funding rate results in zero payment."""
        position_value = Decimal("10000")
        hours = Decimal("24")

        long_position = create_perp_position("PERP_LONG", position_value)
        short_position = create_perp_position("PERP_SHORT", position_value)

        long_result = funding_calculator.calculate_funding_payment(
            position=long_position,
            funding_rate=Decimal("0"),
            time_delta_hours=hours,
        )

        short_result = funding_calculator.calculate_funding_payment(
            position=short_position,
            funding_rate=Decimal("0"),
            time_delta_hours=hours,
        )

        assert long_result.payment == Decimal("0"), (
            f"Long with zero rate should have zero payment, got ${long_result.payment}"
        )
        assert short_result.payment == Decimal("0"), (
            f"Short with zero rate should have zero payment, got ${short_result.payment}"
        )


@pytest.mark.validation
class TestFundingRateHandlerAccuracy:
    """Tests validating FundingRateHandler calculations using funding index."""

    def test_handler_estimate_matches_calculator(self, funding_rate_handler, funding_calculator):
        """Test that FundingRateHandler.estimate_funding_for_period matches FundingCalculator."""
        position_value = Decimal("10000")
        funding_rate = Decimal("0.0001")
        hours = 24

        long_position = create_perp_position("PERP_LONG", position_value)

        # Using handler's estimate method
        handler_estimate = funding_rate_handler.estimate_funding_for_period(
            position=long_position,
            hours=hours,
            funding_rate=funding_rate,
        )

        # Using calculator
        calculator_result = funding_calculator.calculate_funding_payment(
            position=long_position,
            funding_rate=funding_rate,
            time_delta_hours=Decimal(str(hours)),
        )

        assert handler_estimate == calculator_result.payment, (
            f"Handler and Calculator should produce same result.\n"
            f"  Handler estimate: ${handler_estimate}\n"
            f"  Calculator result: ${calculator_result.payment}"
        )

    def test_handler_index_based_calculation(self, funding_rate_handler):
        """Test FundingRateHandler.calculate_funding_payment using funding index difference."""
        position_value = Decimal("50000")
        entry_index = Decimal("0.001")
        current_index = Decimal("0.002")  # 0.1% accumulated funding
        index_change = current_index - entry_index  # 0.001

        # Create position with entry funding index
        from datetime import timezone

        from almanak.framework.backtesting.pnl.portfolio import PositionType, SimulatedPosition

        long_position = SimulatedPosition(
            position_type=PositionType.PERP_LONG,
            protocol="gmx",
            tokens=["ETH"],
            amounts={"ETH": Decimal("25")},
            entry_price=Decimal("2000"),
            entry_time=datetime.now(timezone.utc),
            leverage=Decimal("5"),
            collateral_usd=Decimal("10000"),
            notional_usd=position_value,
            entry_funding_index=entry_index,
            accumulated_funding=Decimal("0"),
        )

        payment = funding_rate_handler.calculate_funding_payment(
            position=long_position,
            current_funding_index=current_index,
            position_value_usd=position_value,
        )

        # Expected: position_value * index_change * (-1 for long)
        # = 50000 * 0.001 * (-1) = -$50
        expected = -position_value * index_change

        assert payment == expected, (
            f"Index-based funding calculation incorrect.\n"
            f"  Position value: ${position_value}\n"
            f"  Index change: {index_change} ({entry_index} -> {current_index})\n"
            f"  Expected: ${expected}\n"
            f"  Got: ${payment}"
        )


__all__ = [
    "IL_REFERENCE_VALUES",
    "IL_TOLERANCE",
    "FUNDING_REFERENCE_VALUES",
    "TestILCalculationAccuracyBenchmarks",
    "TestILSymmetryProperties",
    "TestILV3CalculationAccuracy",
    "TestFundingCalculationAccuracyBenchmarks",
    "TestFundingSymmetryProperties",
    "TestFundingRateHandlerAccuracy",
]
