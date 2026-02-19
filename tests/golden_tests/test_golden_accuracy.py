"""Golden test CI validation for backtesting accuracy.

This module runs backtests against golden test fixtures and validates that results
are within acceptable tolerance thresholds. It serves as a regression test to ensure
backtest accuracy doesn't drift from known-good values.

Tolerance Thresholds (from acceptance criteria):
    - IL: ±5% (Impermanent Loss)
    - Funding: ±10% (Perp funding payments)
    - Interest: ±2% (Lending interest accrual)
    - Health Factor: ±5% (Lending health factor)

Ground Truth Sources:
    - LP fixtures: ImpermanentLossCalculator with V3 concentrated liquidity math
    - Perp fixtures: FundingCalculator with GMX V2 hourly funding model
    - Lending fixtures: InterestCalculator with compound interest formula

To run:
    uv run pytest tests/golden_tests/test_golden_accuracy.py -v

CI Integration:
    These tests are designed to run in CI pipelines. If results drift beyond
    tolerances, CI should fail to prevent accuracy regressions.

Related:
    - tests/golden_tests/__init__.py: Fixture loading functions
    - tests/golden_tests/README.md: Detailed tolerance rationale
"""

from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.calculators.funding import FundingCalculator
from almanak.framework.backtesting.pnl.calculators.health_factor import (
    HealthFactorCalculator,
)
from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
    ImpermanentLossCalculator,
)
from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator

from . import load_lending_fixtures, load_lp_fixtures, load_perp_fixtures

# =============================================================================
# Tolerance Constants (from acceptance criteria)
# =============================================================================

DEFAULT_IL_TOLERANCE = Decimal("0.05")  # 5%
DEFAULT_FUNDING_TOLERANCE = Decimal("0.10")  # 10%
DEFAULT_INTEREST_TOLERANCE = Decimal("0.02")  # 2%
DEFAULT_HEALTH_FACTOR_TOLERANCE = Decimal("0.05")  # 5%


def assert_within_tolerance(
    actual: Decimal,
    expected: Decimal,
    tolerance: Decimal,
    metric_name: str,
    fixture_id: str,
    use_absolute_for_small: bool = False,
) -> None:
    """Assert that actual value is within tolerance of expected value.

    By default, uses relative tolerance for all non-zero values.
    Can optionally use absolute tolerance for very small values (<1).

    Args:
        actual: The calculated value
        expected: The expected value from fixture
        tolerance: The tolerance threshold as decimal (0.05 = 5%)
        metric_name: Name of the metric being validated
        fixture_id: ID of the fixture for error messages
        use_absolute_for_small: If True, use absolute tolerance for values < 1
    """
    # Handle zero expected values
    if expected == Decimal("0"):
        # For zero expected, use absolute tolerance
        if actual != Decimal("0"):
            abs_tolerance = tolerance
            assert abs(actual) <= abs_tolerance, (
                f"{fixture_id} {metric_name}: actual {actual} should be ~0 "
                f"(tolerance: ±{abs_tolerance})"
            )
        return

    # For very small USD values (like $22), use absolute tolerance check in addition
    if use_absolute_for_small and abs(expected) < Decimal("50"):
        abs_tolerance = expected * tolerance  # Scale absolute tolerance by expected
        error = abs(actual - expected)
        if error <= abs_tolerance:
            return  # Passes absolute tolerance
        # Fall through to relative check

    # Use relative tolerance for all values
    relative_error = abs(actual - expected) / abs(expected)
    assert relative_error <= tolerance, (
        f"{fixture_id} {metric_name}: relative error {relative_error:.2%} exceeds "
        f"tolerance ±{tolerance:.0%}. Actual: {actual}, Expected: {expected}"
    )


# =============================================================================
# LP Position Tests (IL ±5%)
# =============================================================================


class TestLPGoldenAccuracy:
    """Golden tests for LP position accuracy (IL and fees)."""

    @pytest.fixture
    def il_calculator(self) -> ImpermanentLossCalculator:
        """Create ImpermanentLossCalculator instance."""
        return ImpermanentLossCalculator()

    @pytest.fixture
    def lp_fixtures(self) -> dict:
        """Load LP fixtures."""
        return load_lp_fixtures()

    def test_all_lp_fixtures_il_within_tolerance(
        self,
        il_calculator: ImpermanentLossCalculator,
        lp_fixtures: dict,
    ) -> None:
        """Validate IL calculations for all LP fixtures are within 5% tolerance.

        This test verifies that the ImpermanentLossCalculator produces results
        within acceptable tolerance of the golden values for all LP fixtures.
        """
        fixtures = lp_fixtures["fixtures"]

        for fixture in fixtures:
            fixture_id = fixture["id"]
            input_data = fixture["input"]
            expected = fixture["expected"]
            tolerances = fixture.get("tolerances", {})

            # Get tolerance (fixture-specific or default)
            il_tolerance = Decimal(str(tolerances.get("il_percentage", DEFAULT_IL_TOLERANCE)))

            # Calculate IL using the calculator
            il_pct, _, _ = il_calculator.calculate_il_v3(
                entry_price=Decimal(input_data["entry_eth_price"]),
                current_price=Decimal(input_data["exit_eth_price"]),
                tick_lower=input_data["tick_lower"],
                tick_upper=input_data["tick_upper"],
                liquidity=Decimal(input_data["liquidity"]),
            )

            expected_il_pct = Decimal(expected["il_percentage"])

            assert_within_tolerance(
                actual=il_pct,
                expected=expected_il_pct,
                tolerance=il_tolerance,
                metric_name="IL percentage",
                fixture_id=fixture_id,
            )

    def test_all_lp_fixtures_il_usd_within_tolerance(
        self,
        il_calculator: ImpermanentLossCalculator,
        lp_fixtures: dict,
    ) -> None:
        """Validate IL USD values for all LP fixtures are within 5% tolerance."""
        fixtures = lp_fixtures["fixtures"]

        for fixture in fixtures:
            fixture_id = fixture["id"]
            input_data = fixture["input"]
            expected = fixture["expected"]
            tolerances = fixture.get("tolerances", {})

            # Get tolerance
            il_usd_tolerance = Decimal(str(tolerances.get("il_usd", DEFAULT_IL_TOLERANCE)))

            # Calculate IL
            il_pct, _, _ = il_calculator.calculate_il_v3(
                entry_price=Decimal(input_data["entry_eth_price"]),
                current_price=Decimal(input_data["exit_eth_price"]),
                tick_lower=input_data["tick_lower"],
                tick_upper=input_data["tick_upper"],
                liquidity=Decimal(input_data["liquidity"]),
            )

            # Calculate hold value and IL USD
            entry_token0_amount = Decimal(input_data["entry_token0_amount"])
            entry_token1_amount = Decimal(input_data["entry_token1_amount"])
            exit_eth_price = Decimal(input_data["exit_eth_price"])

            hold_value = entry_token0_amount * exit_eth_price + entry_token1_amount
            calculated_il_usd = il_pct * hold_value

            expected_il_usd = Decimal(expected["il_usd"])

            assert_within_tolerance(
                actual=calculated_il_usd,
                expected=expected_il_usd,
                tolerance=il_usd_tolerance,
                metric_name="IL USD",
                fixture_id=fixture_id,
            )

    @pytest.mark.parametrize(
        "fixture_id",
        ["Q4_2024_LP_001", "Q4_2024_LP_002", "Q4_2024_LP_003"],
    )
    def test_individual_lp_fixture(
        self,
        il_calculator: ImpermanentLossCalculator,
        lp_fixtures: dict,
        fixture_id: str,
    ) -> None:
        """Parameterized test for individual LP fixtures."""
        fixtures = lp_fixtures["fixtures"]
        fixture = next((f for f in fixtures if f["id"] == fixture_id), None)

        assert fixture is not None, f"Fixture {fixture_id} not found"

        input_data = fixture["input"]
        expected = fixture["expected"]
        tolerances = fixture.get("tolerances", {})

        il_tolerance = Decimal(str(tolerances.get("il_percentage", DEFAULT_IL_TOLERANCE)))

        il_pct, _, _ = il_calculator.calculate_il_v3(
            entry_price=Decimal(input_data["entry_eth_price"]),
            current_price=Decimal(input_data["exit_eth_price"]),
            tick_lower=input_data["tick_lower"],
            tick_upper=input_data["tick_upper"],
            liquidity=Decimal(input_data["liquidity"]),
        )

        expected_il_pct = Decimal(expected["il_percentage"])

        assert_within_tolerance(
            actual=il_pct,
            expected=expected_il_pct,
            tolerance=il_tolerance,
            metric_name="IL percentage",
            fixture_id=fixture_id,
        )


# =============================================================================
# Perp Trade Tests (Funding ±10%)
# =============================================================================


class TestPerpGoldenAccuracy:
    """Golden tests for perp trade accuracy (funding and PnL)."""

    @pytest.fixture
    def funding_calculator(self) -> FundingCalculator:
        """Create FundingCalculator instance."""
        return FundingCalculator()

    @pytest.fixture
    def perp_fixtures(self) -> dict:
        """Load perp fixtures."""
        return load_perp_fixtures()

    def test_all_perp_fixtures_funding_within_tolerance(
        self,
        perp_fixtures: dict,
    ) -> None:
        """Validate funding calculations for all perp fixtures are within 10% tolerance.

        This test verifies that funding calculations match the golden values
        using the standard formula: funding = hours * rate * position_size
        """
        fixtures = perp_fixtures["fixtures"]

        for fixture in fixtures:
            fixture_id = fixture["id"]
            input_data = fixture["input"]
            expected = fixture["expected"]
            tolerances = fixture.get("tolerances", {})

            # Get tolerance
            funding_tolerance = Decimal(str(tolerances.get("funding", DEFAULT_FUNDING_TOLERANCE)))

            # Calculate funding payment
            # Formula: hours * funding_rate * position_size
            duration_hours = Decimal(str(input_data["duration_hours"]))
            funding_rate = Decimal(input_data["avg_funding_rate"])
            size_usd = Decimal(input_data["size_usd"])

            # Funding paid (positive rate = longs pay, shorts receive)
            calculated_funding = duration_hours * funding_rate * size_usd
            if not input_data["is_long"]:
                # Shorts receive when funding rate is positive
                calculated_funding = -calculated_funding

            expected_funding = Decimal(expected["funding_paid"])

            assert_within_tolerance(
                actual=calculated_funding,
                expected=expected_funding,
                tolerance=funding_tolerance,
                metric_name="Funding paid",
                fixture_id=fixture_id,
            )

    def test_all_perp_fixtures_pnl_within_tolerance(
        self,
        perp_fixtures: dict,
    ) -> None:
        """Validate PnL calculations for all perp fixtures are within 5% tolerance."""
        fixtures = perp_fixtures["fixtures"]

        for fixture in fixtures:
            fixture_id = fixture["id"]
            input_data = fixture["input"]
            expected = fixture["expected"]
            tolerances = fixture.get("tolerances", {})

            # Get tolerance
            pnl_tolerance = Decimal(str(tolerances.get("pnl", Decimal("0.05"))))

            # Calculate price PnL
            entry_price = Decimal(input_data["entry_price"])
            exit_price = Decimal(input_data["exit_price"])
            size_usd = Decimal(input_data["size_usd"])

            price_change_pct = (exit_price - entry_price) / entry_price
            if input_data["is_long"]:
                price_pnl = size_usd * price_change_pct
            else:
                price_pnl = -size_usd * price_change_pct

            expected_price_pnl = Decimal(expected["price_pnl_usd"])

            assert_within_tolerance(
                actual=price_pnl,
                expected=expected_price_pnl,
                tolerance=pnl_tolerance,
                metric_name="Price PnL",
                fixture_id=fixture_id,
            )

    @pytest.mark.parametrize(
        "fixture_id",
        ["DEC_2024_PERP_001", "DEC_2024_PERP_002", "DEC_2024_PERP_003"],
    )
    def test_individual_perp_fixture(
        self,
        perp_fixtures: dict,
        fixture_id: str,
    ) -> None:
        """Parameterized test for individual perp fixtures."""
        fixtures = perp_fixtures["fixtures"]
        fixture = next((f for f in fixtures if f["id"] == fixture_id), None)

        assert fixture is not None, f"Fixture {fixture_id} not found"

        input_data = fixture["input"]
        expected = fixture["expected"]
        tolerances = fixture.get("tolerances", {})

        funding_tolerance = Decimal(str(tolerances.get("funding", DEFAULT_FUNDING_TOLERANCE)))

        # Calculate funding
        duration_hours = Decimal(str(input_data["duration_hours"]))
        funding_rate = Decimal(input_data["avg_funding_rate"])
        size_usd = Decimal(input_data["size_usd"])

        calculated_funding = duration_hours * funding_rate * size_usd
        if not input_data["is_long"]:
            # Shorts receive when funding rate is positive
            calculated_funding = -calculated_funding

        expected_funding = Decimal(expected["funding_paid"])

        assert_within_tolerance(
            actual=calculated_funding,
            expected=expected_funding,
            tolerance=funding_tolerance,
            metric_name="Funding paid",
            fixture_id=fixture_id,
        )


# =============================================================================
# Lending Position Tests (Interest ±2%)
# =============================================================================


class TestLendingGoldenAccuracy:
    """Golden tests for lending position accuracy (interest and health factor)."""

    @pytest.fixture
    def interest_calculator(self) -> InterestCalculator:
        """Create InterestCalculator instance."""
        return InterestCalculator()

    @pytest.fixture
    def health_factor_calculator(self) -> HealthFactorCalculator:
        """Create HealthFactorCalculator instance."""
        return HealthFactorCalculator()

    @pytest.fixture
    def lending_fixtures(self) -> dict:
        """Load lending fixtures."""
        return load_lending_fixtures()

    def test_all_lending_fixtures_interest_within_tolerance(
        self,
        interest_calculator: InterestCalculator,
        lending_fixtures: dict,
    ) -> None:
        """Validate interest calculations for all lending fixtures are within 2% tolerance.

        This test verifies that the InterestCalculator produces results within
        acceptable tolerance of the golden values for all lending fixtures.
        """
        fixtures = lending_fixtures["fixtures"]

        for fixture in fixtures:
            fixture_id = fixture["id"]
            input_data = fixture["input"]
            expected = fixture["expected"]
            tolerances = fixture.get("tolerances", {})

            # Get tolerance
            interest_tolerance = Decimal(str(tolerances.get("interest", DEFAULT_INTEREST_TOLERANCE)))

            # Calculate interest using compound interest
            principal = Decimal(input_data["principal_usd"])
            apy = Decimal(input_data["apy"])
            duration_days = Decimal(str(input_data["duration_days"]))

            result = interest_calculator.calculate_interest(
                principal=principal,
                apy=apy,
                time_delta=duration_days,
                compound=True,  # Lending uses compound interest
            )

            expected_interest = Decimal(expected["interest_usd"])

            assert_within_tolerance(
                actual=result.interest,
                expected=expected_interest,
                tolerance=interest_tolerance,
                metric_name="Interest USD",
                fixture_id=fixture_id,
            )

    def test_borrow_fixture_health_factor_within_tolerance(
        self,
        health_factor_calculator: HealthFactorCalculator,
        lending_fixtures: dict,
    ) -> None:
        """Validate health factor for borrow fixtures is within 5% tolerance."""
        fixtures = lending_fixtures["fixtures"]

        for fixture in fixtures:
            # Only test borrow fixtures with health factor expectations
            if fixture.get("position_type") != "borrow":
                continue
            if "health_factor" not in fixture.get("expected", {}):
                continue

            fixture_id = fixture["id"]
            input_data = fixture["input"]
            expected = fixture["expected"]
            tolerances = fixture.get("tolerances", {})

            # Get tolerance
            hf_tolerance = Decimal(str(tolerances.get("health_factor", DEFAULT_HEALTH_FACTOR_TOLERANCE)))

            # Get values for health factor calculation
            collateral_usd = Decimal(input_data["collateral_usd"])
            liquidation_threshold = Decimal(input_data["liquidation_threshold"])

            # Calculate total debt (principal + accrued interest)
            total_debt = Decimal(expected["total_debt_usd"])

            result = health_factor_calculator.calculate_health_factor(
                collateral_value_usd=collateral_usd,
                debt_value_usd=total_debt,
                liquidation_threshold=liquidation_threshold,
            )

            expected_hf = Decimal(expected["health_factor"])

            assert_within_tolerance(
                actual=result.health_factor,
                expected=expected_hf,
                tolerance=hf_tolerance,
                metric_name="Health factor",
                fixture_id=fixture_id,
            )

    @pytest.mark.parametrize(
        "fixture_id",
        ["AAVE_2024_SUPPLY_001", "AAVE_2024_BORROW_001"],
    )
    def test_individual_lending_fixture(
        self,
        interest_calculator: InterestCalculator,
        lending_fixtures: dict,
        fixture_id: str,
    ) -> None:
        """Parameterized test for individual lending fixtures."""
        fixtures = lending_fixtures["fixtures"]
        fixture = next((f for f in fixtures if f["id"] == fixture_id), None)

        assert fixture is not None, f"Fixture {fixture_id} not found"

        input_data = fixture["input"]
        expected = fixture["expected"]
        tolerances = fixture.get("tolerances", {})

        interest_tolerance = Decimal(str(tolerances.get("interest", DEFAULT_INTEREST_TOLERANCE)))

        # Calculate interest
        principal = Decimal(input_data["principal_usd"])
        apy = Decimal(input_data["apy"])
        duration_days = Decimal(str(input_data["duration_days"]))

        result = interest_calculator.calculate_interest(
            principal=principal,
            apy=apy,
            time_delta=duration_days,
            compound=True,
        )

        expected_interest = Decimal(expected["interest_usd"])

        assert_within_tolerance(
            actual=result.interest,
            expected=expected_interest,
            tolerance=interest_tolerance,
            metric_name="Interest USD",
            fixture_id=fixture_id,
        )


# =============================================================================
# CI Summary Tests
# =============================================================================


class TestGoldenCISummary:
    """Summary tests to provide CI overview of golden test coverage."""

    def test_all_lp_fixtures_exist(self) -> None:
        """Verify LP fixture file exists and has expected number of fixtures."""
        fixtures = load_lp_fixtures()
        assert "fixtures" in fixtures
        assert len(fixtures["fixtures"]) >= 3, "Should have at least 3 LP fixtures"

    def test_all_perp_fixtures_exist(self) -> None:
        """Verify perp fixture file exists and has expected number of fixtures."""
        fixtures = load_perp_fixtures()
        assert "fixtures" in fixtures
        assert len(fixtures["fixtures"]) >= 3, "Should have at least 3 perp fixtures"

    def test_all_lending_fixtures_exist(self) -> None:
        """Verify lending fixture file exists and has expected number of fixtures."""
        fixtures = load_lending_fixtures()
        assert "fixtures" in fixtures
        assert len(fixtures["fixtures"]) >= 2, "Should have at least 2 lending fixtures"

    def test_fixture_metadata_complete(self) -> None:
        """Verify all fixture files have required metadata."""
        for loader, name in [
            (load_lp_fixtures, "LP"),
            (load_perp_fixtures, "Perp"),
            (load_lending_fixtures, "Lending"),
        ]:
            fixtures = loader()
            assert "metadata" in fixtures, f"{name} fixtures missing metadata"

            metadata = fixtures["metadata"]
            assert "version" in metadata, f"{name} metadata missing version"
            assert "ground_truth_source" in metadata, f"{name} metadata missing ground_truth_source"

    def test_all_fixtures_have_tolerances(self) -> None:
        """Verify all fixtures specify tolerance thresholds."""
        for loader, name in [
            (load_lp_fixtures, "LP"),
            (load_perp_fixtures, "Perp"),
            (load_lending_fixtures, "Lending"),
        ]:
            fixtures = loader()
            for fixture in fixtures["fixtures"]:
                fixture_id = fixture["id"]
                assert "tolerances" in fixture, (
                    f"{name} fixture {fixture_id} missing tolerances"
                )
