"""Unit tests for interest calculation in lending positions.

This module tests the InterestCalculator class, covering:
- Simple interest calculation
- Compound interest calculation
- Interest over multiple time periods
- Variable APY handling
- Protocol-specific APY lookups
- Edge cases and boundary conditions
"""

from datetime import timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.calculators.interest import (
    InterestCalculator,
    InterestRateSource,
    InterestResult,
)


class TestSimpleInterestCalculation:
    """Tests for simple interest calculations (non-compounding)."""

    def test_simple_interest_basic(self):
        """Test basic simple interest calculation.

        Formula: interest = principal * apy * (days / 365)
        $10,000 at 5% APY for 365 days = $500
        """
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("365"),
            compound=False,
        )

        assert result.interest == Decimal("500")
        assert result.principal == Decimal("10000")
        assert result.apy == Decimal("0.05")
        assert result.time_days == Decimal("365")
        assert result.compound is False

    def test_simple_interest_30_days(self):
        """Test simple interest over 30 days.

        $10,000 at 5% APY for 30 days ≈ $41.10
        """
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("30"),
            compound=False,
        )

        expected = Decimal("10000") * Decimal("0.05") * Decimal("30") / Decimal("365")
        assert result.interest == pytest.approx(expected, rel=Decimal("0.0001"))
        assert result.interest == pytest.approx(Decimal("41.096"), rel=Decimal("0.01"))

    def test_simple_interest_high_apy(self):
        """Test simple interest with high APY (20%)."""
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("50000"),
            apy=Decimal("0.20"),
            time_delta=Decimal("90"),
            compound=False,
        )

        # $50,000 * 0.20 * (90/365) = $2,465.75
        expected = Decimal("50000") * Decimal("0.20") * Decimal("90") / Decimal("365")
        assert result.interest == pytest.approx(expected, rel=Decimal("0.0001"))

    def test_simple_interest_small_principal(self):
        """Test simple interest with small principal."""
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("100"),
            apy=Decimal("0.03"),
            time_delta=Decimal("7"),
            compound=False,
        )

        # $100 * 0.03 * (7/365) ≈ $0.0575
        expected = Decimal("100") * Decimal("0.03") * Decimal("7") / Decimal("365")
        assert result.interest == pytest.approx(expected, rel=Decimal("0.0001"))

    def test_simple_interest_fractional_days(self):
        """Test simple interest with fractional days."""
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("1.5"),
            compound=False,
        )

        expected = Decimal("10000") * Decimal("0.05") * Decimal("1.5") / Decimal("365")
        assert result.interest == pytest.approx(expected, rel=Decimal("0.0001"))

    def test_simple_interest_timedelta_input(self):
        """Test simple interest accepts timedelta input."""
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=timedelta(days=30),
            compound=False,
        )

        expected = Decimal("10000") * Decimal("0.05") * Decimal("30") / Decimal("365")
        assert result.interest == pytest.approx(expected, rel=Decimal("0.01"))

    def test_simple_interest_effective_rate(self):
        """Test effective rate calculation for simple interest."""
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("30"),
            compound=False,
        )

        expected_rate = result.interest / result.principal
        assert result.effective_rate == pytest.approx(expected_rate, rel=Decimal("0.0001"))


class TestCompoundInterestCalculation:
    """Tests for compound interest calculations."""

    def test_compound_interest_basic_annual(self):
        """Test compound interest over one year with daily compounding.

        For daily compounding (365 periods):
        interest = principal * ((1 + apy/365)^365 - 1)
        At 5% APY for one year ≈ 5.13% effective
        """
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("365"),
            compound=True,
        )

        # With daily compounding, effective rate slightly higher than APY
        # 10000 * ((1 + 0.05/365)^365 - 1) ≈ 512.67
        assert result.interest > Decimal("500")  # More than simple interest
        assert result.interest == pytest.approx(Decimal("512.67"), rel=Decimal("0.01"))
        assert result.compound is True

    def test_compound_interest_30_days(self):
        """Test compound interest over 30 days.

        $10,000 at 5% APY for 30 days with daily compounding
        """
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("30"),
            compound=True,
        )

        # For short periods, compound and simple are very close
        simple_result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("30"),
            compound=False,
        )

        # Compound interest should be slightly higher
        assert result.interest >= simple_result.interest
        # Both should be approximately $41
        assert result.interest == pytest.approx(Decimal("41.1"), rel=Decimal("0.01"))

    def test_compound_interest_high_apy_long_period(self):
        """Test compound interest significantly exceeds simple with high APY over long period."""
        calculator = InterestCalculator()

        # 20% APY over 2 years
        compound_result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.20"),
            time_delta=Decimal("730"),
            compound=True,
        )
        simple_result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.20"),
            time_delta=Decimal("730"),
            compound=False,
        )

        # Simple: 10000 * 0.20 * 2 = 4000
        assert simple_result.interest == Decimal("4000")

        # Compound should be significantly higher
        assert compound_result.interest > simple_result.interest
        # With daily compounding: ~4918
        assert compound_result.interest == pytest.approx(Decimal("4918"), rel=Decimal("0.02"))

    def test_compound_interest_continuous(self):
        """Test continuous compounding (n=0)."""
        calculator = InterestCalculator(compounding_periods_per_year=0)
        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("365"),
            compound=True,
        )

        # Continuous compounding: P * (e^(r*t) - 1)
        # e^0.05 - 1 ≈ 0.05127
        assert result.interest == pytest.approx(Decimal("512.7"), rel=Decimal("0.01"))

    def test_compound_interest_hourly(self):
        """Test hourly compounding (8760 periods per year)."""
        calculator = InterestCalculator(compounding_periods_per_year=8760)
        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("365"),
            compound=True,
        )

        # Hourly compounding is very close to continuous
        assert result.interest == pytest.approx(Decimal("512.7"), rel=Decimal("0.01"))

    def test_compound_interest_final_balance(self):
        """Test final_balance property includes interest."""
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("365"),
            compound=True,
        )

        assert result.final_balance == result.principal + result.interest
        assert result.final_balance == pytest.approx(Decimal("10512.67"), rel=Decimal("0.01"))


class TestInterestOverMultiplePeriods:
    """Tests for calculating interest over multiple separate time periods."""

    def test_multiple_periods_same_apy(self):
        """Test interest calculation over multiple periods with same APY."""
        calculator = InterestCalculator()

        # Calculate interest for 3 separate 30-day periods
        total_interest = Decimal("0")
        principal = Decimal("10000")
        apy = Decimal("0.05")

        for _ in range(3):
            result = calculator.calculate_interest(
                principal=principal,
                apy=apy,
                time_delta=Decimal("30"),
                compound=True,
            )
            total_interest += result.interest
            principal += result.interest  # Compound across periods

        # Compare to single 90-day calculation
        single_result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("90"),
            compound=True,
        )

        # Should be very close (small difference due to period boundaries)
        assert total_interest == pytest.approx(single_result.interest, rel=Decimal("0.01"))

    def test_interest_accumulation_simulated_backtest(self):
        """Simulate interest accumulation like in a backtest with daily ticks."""
        calculator = InterestCalculator()

        principal = Decimal("10000")
        apy = Decimal("0.05")
        total_interest = Decimal("0")

        # Simulate 30 daily ticks
        for _ in range(30):
            result = calculator.calculate_interest(
                principal=principal + total_interest,  # Accumulate
                apy=apy,
                time_delta=Decimal("1"),
                compound=True,
            )
            total_interest += result.interest

        # Compare to single 30-day calculation
        single_result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("30"),
            compound=True,
        )

        # Results should match (both compound daily)
        assert total_interest == pytest.approx(single_result.interest, rel=Decimal("0.001"))

    def test_hourly_ticks_vs_daily(self):
        """Test that hourly tick accumulation matches daily calculation."""
        calculator = InterestCalculator()

        principal = Decimal("10000")
        apy = Decimal("0.05")
        total_interest_hourly = Decimal("0")

        # Simulate 24 hourly ticks (1 day)
        for _ in range(24):
            result = calculator.calculate_interest(
                principal=principal + total_interest_hourly,
                apy=apy,
                time_delta=Decimal("1") / Decimal("24"),  # 1/24 day = 1 hour
                compound=True,
            )
            total_interest_hourly += result.interest

        # Compare to single day calculation
        daily_result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("1"),
            compound=True,
        )

        # Results should be very close
        assert total_interest_hourly == pytest.approx(daily_result.interest, rel=Decimal("0.01"))


class TestVariableAPYHandling:
    """Tests for variable APY calculations over different periods."""

    def test_variable_apy_basic(self):
        """Test interest with variable APY over two periods."""
        calculator = InterestCalculator()

        # 10 days at 3%, then 20 days at 5%
        result = calculator.calculate_interest_variable_apy(
            principal=Decimal("10000"),
            apy_schedule=[
                (Decimal("10"), Decimal("0.03")),
                (Decimal("20"), Decimal("0.05")),
            ],
            compound=True,
        )

        assert result.time_days == Decimal("30")
        assert result.principal == Decimal("10000")

        # Calculate expected interest separately
        calc = InterestCalculator()
        first_result = calc.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.03"),
            time_delta=Decimal("10"),
            compound=True,
        )
        second_result = calc.calculate_interest(
            principal=Decimal("10000") + first_result.interest,
            apy=Decimal("0.05"),
            time_delta=Decimal("20"),
            compound=True,
        )

        expected_total = first_result.interest + second_result.interest
        assert result.interest == pytest.approx(expected_total, rel=Decimal("0.001"))

    def test_variable_apy_three_periods(self):
        """Test variable APY with three different periods."""
        calculator = InterestCalculator()

        # Rising APY: 2%, 5%, 8% over 30 days each
        result = calculator.calculate_interest_variable_apy(
            principal=Decimal("50000"),
            apy_schedule=[
                (Decimal("30"), Decimal("0.02")),
                (Decimal("30"), Decimal("0.05")),
                (Decimal("30"), Decimal("0.08")),
            ],
            compound=True,
        )

        assert result.time_days == Decimal("90")

        # Weighted average APY: (30*0.02 + 30*0.05 + 30*0.08) / 90 = 0.05
        # Use approx comparison due to Decimal precision
        assert result.apy == pytest.approx(Decimal("0.05"), rel=Decimal("0.0001"))

        # Interest should be positive
        assert result.interest > Decimal("0")

    def test_variable_apy_simple_interest(self):
        """Test variable APY with simple interest (no compounding between periods)."""
        calculator = InterestCalculator()

        result = calculator.calculate_interest_variable_apy(
            principal=Decimal("10000"),
            apy_schedule=[
                (Decimal("30"), Decimal("0.03")),
                (Decimal("30"), Decimal("0.06")),
            ],
            compound=False,
        )

        # For simple interest, periods are independent
        calc = InterestCalculator()
        first = calc.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.03"),
            time_delta=Decimal("30"),
            compound=False,
        )
        second = calc.calculate_interest(
            principal=Decimal("10000"),  # No accumulation for simple
            apy=Decimal("0.06"),
            time_delta=Decimal("30"),
            compound=False,
        )

        expected = first.interest + second.interest
        assert result.interest == pytest.approx(expected, rel=Decimal("0.001"))

    def test_variable_apy_empty_schedule(self):
        """Test variable APY with empty schedule returns zero."""
        calculator = InterestCalculator()

        result = calculator.calculate_interest_variable_apy(
            principal=Decimal("10000"),
            apy_schedule=[],
            compound=True,
        )

        assert result.interest == Decimal("0")
        assert result.time_days == Decimal("0")
        assert result.apy == Decimal("0")

    def test_variable_apy_single_period(self):
        """Test variable APY with single period matches regular calculation."""
        calculator = InterestCalculator()

        variable_result = calculator.calculate_interest_variable_apy(
            principal=Decimal("10000"),
            apy_schedule=[(Decimal("30"), Decimal("0.05"))],
            compound=True,
        )

        regular_result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("30"),
            compound=True,
        )

        assert variable_result.interest == pytest.approx(
            regular_result.interest, rel=Decimal("0.001")
        )

    def test_variable_apy_effective_rate(self):
        """Test effective rate calculation for variable APY."""
        calculator = InterestCalculator()

        result = calculator.calculate_interest_variable_apy(
            principal=Decimal("10000"),
            apy_schedule=[
                (Decimal("15"), Decimal("0.03")),
                (Decimal("15"), Decimal("0.07")),
            ],
            compound=True,
        )

        expected_rate = result.interest / result.principal
        assert result.effective_rate == pytest.approx(expected_rate, rel=Decimal("0.0001"))


class TestProtocolSpecificAPY:
    """Tests for protocol-specific APY lookups."""

    def test_get_supply_apy_aave(self):
        """Test getting Aave V3 supply APY."""
        calculator = InterestCalculator()
        apy = calculator.get_supply_apy_for_protocol("aave_v3")
        assert apy == Decimal("0.03")  # 3%

    def test_get_supply_apy_compound(self):
        """Test getting Compound V3 supply APY."""
        calculator = InterestCalculator()
        apy = calculator.get_supply_apy_for_protocol("compound_v3")
        assert apy == Decimal("0.025")  # 2.5%

    def test_get_supply_apy_morpho(self):
        """Test getting Morpho supply APY."""
        calculator = InterestCalculator()
        apy = calculator.get_supply_apy_for_protocol("morpho")
        assert apy == Decimal("0.035")  # 3.5%

    def test_get_supply_apy_spark(self):
        """Test getting Spark supply APY (DSR)."""
        calculator = InterestCalculator()
        apy = calculator.get_supply_apy_for_protocol("spark")
        assert apy == Decimal("0.05")  # 5%

    def test_get_supply_apy_unknown_protocol(self):
        """Test default APY is returned for unknown protocol."""
        calculator = InterestCalculator()
        apy = calculator.get_supply_apy_for_protocol("unknown_protocol")
        assert apy == calculator.default_supply_apy

    def test_get_supply_apy_case_insensitive(self):
        """Test protocol name lookup is case-insensitive."""
        calculator = InterestCalculator()
        apy = calculator.get_supply_apy_for_protocol("AAVE_V3")
        assert apy == Decimal("0.03")

    def test_get_borrow_apy_aave(self):
        """Test getting Aave V3 borrow APY."""
        calculator = InterestCalculator()
        apy = calculator.get_borrow_apy_for_protocol("aave_v3")
        assert apy == Decimal("0.05")  # 5%

    def test_get_borrow_apy_compound(self):
        """Test getting Compound V3 borrow APY."""
        calculator = InterestCalculator()
        apy = calculator.get_borrow_apy_for_protocol("compound_v3")
        assert apy == Decimal("0.045")  # 4.5%

    def test_get_borrow_apy_morpho(self):
        """Test getting Morpho borrow APY."""
        calculator = InterestCalculator()
        apy = calculator.get_borrow_apy_for_protocol("morpho")
        assert apy == Decimal("0.04")  # 4%

    def test_get_borrow_apy_unknown_protocol(self):
        """Test default borrow APY is returned for unknown protocol."""
        calculator = InterestCalculator()
        apy = calculator.get_borrow_apy_for_protocol("unknown")
        assert apy == calculator.default_borrow_apy


class TestEstimationMethods:
    """Tests for convenience estimation methods."""

    def test_estimate_annual_interest_compound(self):
        """Test annual interest estimation with compounding."""
        calculator = InterestCalculator()
        annual = calculator.estimate_annual_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            compound=True,
        )

        # Should equal full year compound interest
        full_result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("365"),
            compound=True,
        )
        assert annual == full_result.interest

    def test_estimate_annual_interest_simple(self):
        """Test annual interest estimation without compounding."""
        calculator = InterestCalculator()
        annual = calculator.estimate_annual_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            compound=False,
        )

        # Simple: principal * apy = 500
        assert annual == Decimal("500")

    def test_estimate_daily_interest(self):
        """Test daily interest estimation."""
        calculator = InterestCalculator()
        daily = calculator.estimate_daily_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            compound=True,
        )

        # Daily interest should be positive and reasonable
        # For $10,000 at 5% APY, daily interest ≈ $1.37
        assert daily > Decimal("0")
        assert daily == pytest.approx(Decimal("1.37"), rel=Decimal("0.01"))

        # Verify it's calculated for exactly 1 day
        one_day_result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("1"),
            compound=True,
        )
        assert daily == one_day_result.interest


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_zero_principal(self):
        """Test interest with zero principal."""
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("0"),
            apy=Decimal("0.05"),
            time_delta=Decimal("30"),
            compound=True,
        )

        assert result.interest == Decimal("0")
        assert result.effective_rate == Decimal("0")

    def test_zero_apy(self):
        """Test interest with zero APY."""
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0"),
            time_delta=Decimal("30"),
            compound=True,
        )

        assert result.interest == Decimal("0")

    def test_zero_time(self):
        """Test interest with zero time."""
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("0"),
            compound=True,
        )

        assert result.interest == Decimal("0")

    def test_negative_principal_returns_zero(self):
        """Test that negative principal is handled gracefully."""
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("-1000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("30"),
            compound=True,
        )

        # Should return zero interest for invalid input
        assert result.interest == Decimal("0")

    def test_very_small_time_period(self):
        """Test interest over very small time period (1 minute)."""
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("1") / Decimal("1440"),  # 1 minute = 1/1440 day
            compound=True,
        )

        assert result.interest > Decimal("0")
        assert result.interest < Decimal("1")  # Very small but positive

    def test_very_large_principal(self):
        """Test interest with very large principal."""
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("1000000000"),  # $1 billion
            apy=Decimal("0.05"),
            time_delta=Decimal("30"),
            compound=True,
        )

        # Should handle large numbers without overflow
        assert result.interest > Decimal("4000000")  # ~$4.1 million

    def test_timedelta_with_hours(self):
        """Test that timedelta with hours is converted correctly."""
        calculator = InterestCalculator()
        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=timedelta(hours=24),
            compound=True,
        )

        # 24 hours = 1 day
        one_day_result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("1"),
            compound=True,
        )

        assert result.interest == pytest.approx(one_day_result.interest, rel=Decimal("0.001"))


class TestInterestRateSourceEnum:
    """Tests for InterestRateSource enum."""

    def test_interest_rate_source_values(self):
        """Test InterestRateSource enum values."""
        assert InterestRateSource.FIXED.value == "fixed"
        assert InterestRateSource.HISTORICAL.value == "historical"
        assert InterestRateSource.PROTOCOL.value == "protocol"
        assert InterestRateSource.VARIABLE.value == "variable"

    def test_calculator_default_source(self):
        """Test calculator uses FIXED as default source."""
        calculator = InterestCalculator()
        assert calculator.interest_rate_source == InterestRateSource.FIXED


class TestInterestResultDataclass:
    """Tests for InterestResult dataclass."""

    def test_interest_result_fields(self):
        """Test InterestResult has all expected fields."""
        result = InterestResult(
            interest=Decimal("100"),
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_days=Decimal("30"),
            compound=True,
            effective_rate=Decimal("0.01"),
        )

        assert result.interest == Decimal("100")
        assert result.principal == Decimal("10000")
        assert result.apy == Decimal("0.05")
        assert result.time_days == Decimal("30")
        assert result.compound is True
        assert result.effective_rate == Decimal("0.01")

    def test_final_balance_property(self):
        """Test final_balance property calculation."""
        result = InterestResult(
            interest=Decimal("123.45"),
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_days=Decimal("30"),
            compound=True,
            effective_rate=Decimal("0.012345"),
        )

        assert result.final_balance == Decimal("10123.45")


class TestCalculatorSerialization:
    """Tests for calculator serialization."""

    def test_to_dict(self):
        """Test InterestCalculator.to_dict() serialization."""
        calculator = InterestCalculator(
            interest_rate_source=InterestRateSource.PROTOCOL,
            default_supply_apy=Decimal("0.04"),
            default_borrow_apy=Decimal("0.06"),
            compounding_periods_per_year=365,
        )

        data = calculator.to_dict()

        assert data["calculator_name"] == "interest"
        assert data["interest_rate_source"] == "protocol"
        assert data["default_supply_apy"] == "0.04"
        assert data["default_borrow_apy"] == "0.06"
        assert data["compounding_periods_per_year"] == 365
        assert "protocol_supply_apys" in data
        assert "protocol_borrow_apys" in data

    def test_to_dict_protocol_apys(self):
        """Test protocol APYs are serialized correctly."""
        calculator = InterestCalculator()
        data = calculator.to_dict()

        assert "aave_v3" in data["protocol_supply_apys"]
        assert data["protocol_supply_apys"]["aave_v3"] == "0.03"
        assert "aave_v3" in data["protocol_borrow_apys"]
        assert data["protocol_borrow_apys"]["aave_v3"] == "0.05"


class TestCustomCalculatorConfiguration:
    """Tests for custom calculator configurations."""

    def test_custom_default_apys(self):
        """Test calculator with custom default APYs."""
        calculator = InterestCalculator(
            default_supply_apy=Decimal("0.08"),
            default_borrow_apy=Decimal("0.12"),
        )

        assert calculator.get_supply_apy_for_protocol("unknown") == Decimal("0.08")
        assert calculator.get_borrow_apy_for_protocol("unknown") == Decimal("0.12")

    def test_custom_protocol_apys(self):
        """Test calculator with custom protocol APYs."""
        calculator = InterestCalculator(
            protocol_supply_apys={
                "my_protocol": Decimal("0.10"),
            },
            protocol_borrow_apys={
                "my_protocol": Decimal("0.15"),
            },
        )

        assert calculator.get_supply_apy_for_protocol("my_protocol") == Decimal("0.10")
        assert calculator.get_borrow_apy_for_protocol("my_protocol") == Decimal("0.15")

    def test_custom_compounding_periods(self):
        """Test calculator with custom compounding periods."""
        # Monthly compounding (12 times per year)
        calculator = InterestCalculator(compounding_periods_per_year=12)

        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("365"),
            compound=True,
        )

        # Monthly compounding produces slightly less than daily
        daily_calc = InterestCalculator(compounding_periods_per_year=365)
        daily_result = daily_calc.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("365"),
            compound=True,
        )

        # Monthly < Daily (more frequent compounding = more interest)
        assert result.interest < daily_result.interest
        # But both should be close to ~$512
        assert result.interest == pytest.approx(Decimal("511.6"), rel=Decimal("0.01"))


__all__ = [
    "TestSimpleInterestCalculation",
    "TestCompoundInterestCalculation",
    "TestInterestOverMultiplePeriods",
    "TestVariableAPYHandling",
    "TestProtocolSpecificAPY",
    "TestEstimationMethods",
    "TestEdgeCases",
    "TestInterestRateSourceEnum",
    "TestInterestResultDataclass",
    "TestCalculatorSerialization",
    "TestCustomCalculatorConfiguration",
]
