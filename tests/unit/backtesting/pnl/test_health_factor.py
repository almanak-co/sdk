"""Unit tests for health factor calculation in lending positions.

This module tests the HealthFactorCalculator class, covering:
- Health factor calculation formula
- Health factor updates with price changes
- Warning threshold triggers
- Protocol-specific liquidation thresholds
- Edge cases and boundary conditions

User Story: US-010c - Unit tests for health factor
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.calculators.health_factor import (
    HealthFactorCalculator,
    HealthFactorResult,
    HealthFactorWarning,
)
from almanak.framework.backtesting.pnl.portfolio import (
    SimulatedPortfolio,
    SimulatedPosition,
)


class MockMarketState:
    """Mock market state for testing."""

    def __init__(self, prices: dict[str, Decimal] | None = None):
        self._prices = prices or {}

    def get_price(self, token: str) -> Decimal:
        """Get price for a token."""
        return self._prices.get(token, Decimal("0"))

    def get_prices(self, tokens: list[str]) -> dict[str, Decimal]:
        """Get prices for multiple tokens."""
        return {t: self._prices.get(t, Decimal("0")) for t in tokens}


class TestHealthFactorCalculationFormula:
    """Tests for health factor calculation formula.

    Formula: HF = (collateral_value * liquidation_threshold) / debt_value
    """

    def test_basic_health_factor_calculation(self):
        """Test basic health factor calculation.

        $10,000 collateral at 82.5% LTV with $6,000 debt
        HF = (10000 * 0.825) / 6000 = 1.375
        """
        calculator = HealthFactorCalculator()
        result = calculator.calculate_health_factor(
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("6000"),
            liquidation_threshold=Decimal("0.825"),
        )

        assert result.health_factor == pytest.approx(Decimal("1.375"), rel=Decimal("0.0001"))
        assert result.is_safe is True
        assert result.distance_to_liquidation == pytest.approx(Decimal("0.375"), rel=Decimal("0.0001"))

    def test_health_factor_at_liquidation_boundary(self):
        """Test health factor exactly at 1.0 (liquidation boundary).

        $8,250 collateral at 82.5% LTV with $6,806.25 debt
        HF = (8250 * 0.825) / 6806.25 = 1.0
        """
        calculator = HealthFactorCalculator()
        # Calculate debt that gives HF = 1.0
        collateral = Decimal("8250")
        liquidation_threshold = Decimal("0.825")
        debt = collateral * liquidation_threshold  # 6806.25

        result = calculator.calculate_health_factor(
            collateral_value_usd=collateral,
            debt_value_usd=debt,
            liquidation_threshold=liquidation_threshold,
        )

        assert result.health_factor == pytest.approx(Decimal("1.0"), rel=Decimal("0.0001"))
        assert result.is_safe is True  # HF >= 1.0 is safe
        assert result.distance_to_liquidation == pytest.approx(Decimal("0"), rel=Decimal("0.0001"))

    def test_health_factor_below_liquidation(self):
        """Test health factor below 1.0 (liquidation triggered).

        $10,000 collateral at 82.5% LTV with $10,000 debt
        HF = (10000 * 0.825) / 10000 = 0.825
        """
        calculator = HealthFactorCalculator()
        result = calculator.calculate_health_factor(
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("10000"),
            liquidation_threshold=Decimal("0.825"),
        )

        assert result.health_factor == pytest.approx(Decimal("0.825"), rel=Decimal("0.0001"))
        assert result.is_safe is False
        assert result.distance_to_liquidation == pytest.approx(Decimal("-0.175"), rel=Decimal("0.0001"))

    def test_health_factor_high_collateralization(self):
        """Test health factor with very high collateralization.

        $100,000 collateral at 82.5% LTV with $10,000 debt
        HF = (100000 * 0.825) / 10000 = 8.25
        """
        calculator = HealthFactorCalculator()
        result = calculator.calculate_health_factor(
            collateral_value_usd=Decimal("100000"),
            debt_value_usd=Decimal("10000"),
            liquidation_threshold=Decimal("0.825"),
        )

        assert result.health_factor == pytest.approx(Decimal("8.25"), rel=Decimal("0.0001"))
        assert result.is_safe is True
        assert result.distance_to_liquidation == pytest.approx(Decimal("7.25"), rel=Decimal("0.0001"))

    def test_health_factor_different_liquidation_thresholds(self):
        """Test health factor with different liquidation thresholds."""
        calculator = HealthFactorCalculator()
        collateral = Decimal("10000")
        debt = Decimal("7000")

        # Higher threshold = higher HF (safer)
        result_high = calculator.calculate_health_factor(
            collateral_value_usd=collateral,
            debt_value_usd=debt,
            liquidation_threshold=Decimal("0.90"),  # 90% LTV
        )

        # Lower threshold = lower HF (riskier)
        result_low = calculator.calculate_health_factor(
            collateral_value_usd=collateral,
            debt_value_usd=debt,
            liquidation_threshold=Decimal("0.75"),  # 75% LTV
        )

        # HF_high = (10000 * 0.90) / 7000 = 1.286
        # HF_low = (10000 * 0.75) / 7000 = 1.071
        assert result_high.health_factor > result_low.health_factor
        assert result_high.health_factor == pytest.approx(Decimal("1.2857"), rel=Decimal("0.01"))
        assert result_low.health_factor == pytest.approx(Decimal("1.0714"), rel=Decimal("0.01"))

    def test_health_factor_result_serialization(self):
        """Test HealthFactorResult to_dict serialization."""
        calculator = HealthFactorCalculator()
        result = calculator.calculate_health_factor(
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("6000"),
            liquidation_threshold=Decimal("0.825"),
        )

        data = result.to_dict()
        assert data["health_factor"] == str(result.health_factor)
        assert data["collateral_value_usd"] == str(result.collateral_value_usd)
        assert data["debt_value_usd"] == str(result.debt_value_usd)
        assert data["liquidation_threshold"] == str(result.liquidation_threshold)
        assert data["is_safe"] is True


class TestHealthFactorEdgeCases:
    """Tests for health factor edge cases."""

    def test_zero_debt_returns_infinite_health_factor(self):
        """Test that zero debt returns high health factor (safe position)."""
        calculator = HealthFactorCalculator()
        result = calculator.calculate_health_factor(
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("0"),
            liquidation_threshold=Decimal("0.825"),
        )

        assert result.health_factor == Decimal("999")
        assert result.is_safe is True

    def test_negative_debt_returns_infinite_health_factor(self):
        """Test that negative debt (credit) returns high health factor."""
        calculator = HealthFactorCalculator()
        result = calculator.calculate_health_factor(
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("-1000"),
            liquidation_threshold=Decimal("0.825"),
        )

        assert result.health_factor == Decimal("999")
        assert result.is_safe is True

    def test_zero_collateral_returns_zero_health_factor(self):
        """Test that zero collateral returns zero health factor (instant liquidation)."""
        calculator = HealthFactorCalculator()
        result = calculator.calculate_health_factor(
            collateral_value_usd=Decimal("0"),
            debt_value_usd=Decimal("6000"),
            liquidation_threshold=Decimal("0.825"),
        )

        assert result.health_factor == Decimal("0")
        assert result.is_safe is False
        assert result.distance_to_liquidation == Decimal("-1")

    def test_negative_collateral_returns_zero_health_factor(self):
        """Test that negative collateral returns zero health factor."""
        calculator = HealthFactorCalculator()
        result = calculator.calculate_health_factor(
            collateral_value_usd=Decimal("-5000"),
            debt_value_usd=Decimal("6000"),
            liquidation_threshold=Decimal("0.825"),
        )

        assert result.health_factor == Decimal("0")
        assert result.is_safe is False

    def test_very_small_debt(self):
        """Test health factor with very small debt (high HF)."""
        calculator = HealthFactorCalculator()
        result = calculator.calculate_health_factor(
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("0.01"),  # Very small debt
            liquidation_threshold=Decimal("0.825"),
        )

        # HF = (10000 * 0.825) / 0.01 = 825,000
        expected = Decimal("10000") * Decimal("0.825") / Decimal("0.01")
        assert result.health_factor == pytest.approx(expected, rel=Decimal("0.0001"))
        assert result.is_safe is True

    def test_very_small_collateral(self):
        """Test health factor with very small collateral (low HF)."""
        calculator = HealthFactorCalculator()
        result = calculator.calculate_health_factor(
            collateral_value_usd=Decimal("0.01"),
            debt_value_usd=Decimal("10000"),
            liquidation_threshold=Decimal("0.825"),
        )

        # HF = (0.01 * 0.825) / 10000 = 0.000000825
        expected = Decimal("0.01") * Decimal("0.825") / Decimal("10000")
        assert result.health_factor == pytest.approx(expected, rel=Decimal("0.0001"))
        assert result.is_safe is False


class TestHealthFactorWarningThreshold:
    """Tests for health factor warning threshold triggers."""

    def test_no_warning_above_threshold(self):
        """Test that no warning is emitted when HF is above threshold."""
        calculator = HealthFactorCalculator(warning_threshold=Decimal("1.2"))

        warning = calculator.check_health_factor_warning(
            health_factor=Decimal("1.5"),
            position_id="test_position",
            emit_warning=False,
        )

        assert warning is None
        assert calculator.warning_count == 0

    def test_warning_below_threshold(self):
        """Test warning emitted when HF drops below threshold."""
        calculator = HealthFactorCalculator(warning_threshold=Decimal("1.2"))

        warning = calculator.check_health_factor_warning(
            health_factor=Decimal("1.15"),
            position_id="test_position",
            emit_warning=False,
        )

        assert warning is not None
        assert warning.health_factor == Decimal("1.15")
        assert warning.warning_threshold == Decimal("1.2")
        assert warning.position_id == "test_position"
        assert warning.is_critical is False
        assert "WARNING" in warning.message

    def test_critical_warning_below_critical_threshold(self):
        """Test critical warning emitted when HF is critically low."""
        calculator = HealthFactorCalculator(
            warning_threshold=Decimal("1.2"),
            critical_threshold=Decimal("1.05"),
        )

        warning = calculator.check_health_factor_warning(
            health_factor=Decimal("1.02"),
            position_id="test_position",
            emit_warning=False,
        )

        assert warning is not None
        assert warning.is_critical is True
        assert "CRITICAL" in warning.message
        assert "imminent" in warning.message.lower()

    def test_warning_exactly_at_threshold(self):
        """Test no warning when HF is exactly at threshold."""
        calculator = HealthFactorCalculator(warning_threshold=Decimal("1.2"))

        warning = calculator.check_health_factor_warning(
            health_factor=Decimal("1.2"),
            position_id="test_position",
            emit_warning=False,
        )

        assert warning is None

    def test_warning_count_increments(self):
        """Test that warning count increments when emitting warnings."""
        calculator = HealthFactorCalculator(warning_threshold=Decimal("1.2"))

        assert calculator.warning_count == 0

        calculator.check_health_factor_warning(
            health_factor=Decimal("1.1"),
            position_id="test_position_1",
            emit_warning=True,
        )
        assert calculator.warning_count == 1

        calculator.check_health_factor_warning(
            health_factor=Decimal("1.05"),
            position_id="test_position_2",
            emit_warning=True,
        )
        assert calculator.warning_count == 2

    def test_warning_not_counted_when_emit_false(self):
        """Test that warning count doesn't increment when emit_warning=False."""
        calculator = HealthFactorCalculator(warning_threshold=Decimal("1.2"))

        calculator.check_health_factor_warning(
            health_factor=Decimal("1.1"),
            position_id="test_position",
            emit_warning=False,
        )
        assert calculator.warning_count == 0

    def test_warning_logged(self, caplog):
        """Test that warning is logged when emitting."""
        calculator = HealthFactorCalculator(warning_threshold=Decimal("1.2"))

        with caplog.at_level(logging.WARNING, logger="almanak.framework.backtesting.pnl.calculators.health_factor"):
            calculator.check_health_factor_warning(
                health_factor=Decimal("1.1"),
                position_id="test_position",
                emit_warning=True,
            )

        assert "WARNING" in caplog.text
        assert "1.1" in caplog.text
        assert "test_position" in caplog.text

    def test_custom_thresholds(self):
        """Test custom warning and critical thresholds."""
        calculator = HealthFactorCalculator(
            warning_threshold=Decimal("1.5"),
            critical_threshold=Decimal("1.1"),
        )

        # Below 1.5 but above 1.1 = warning
        warning1 = calculator.check_health_factor_warning(
            health_factor=Decimal("1.3"),
            position_id="test1",
            emit_warning=False,
        )
        assert warning1 is not None
        assert warning1.is_critical is False

        # Below 1.1 = critical
        warning2 = calculator.check_health_factor_warning(
            health_factor=Decimal("1.05"),
            position_id="test2",
            emit_warning=False,
        )
        assert warning2 is not None
        assert warning2.is_critical is True


class TestHealthFactorPriceUpdates:
    """Tests for health factor updates when prices change."""

    def test_health_factor_improves_when_collateral_rises(self):
        """Test that HF improves when collateral price increases."""
        calculator = HealthFactorCalculator()
        debt = Decimal("6000")
        liquidation_threshold = Decimal("0.825")

        # Initial state: $10,000 collateral
        result1 = calculator.calculate_health_factor(
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=debt,
            liquidation_threshold=liquidation_threshold,
        )

        # After price increase: $15,000 collateral
        result2 = calculator.calculate_health_factor(
            collateral_value_usd=Decimal("15000"),
            debt_value_usd=debt,
            liquidation_threshold=liquidation_threshold,
        )

        assert result2.health_factor > result1.health_factor
        # HF1 = (10000 * 0.825) / 6000 = 1.375
        # HF2 = (15000 * 0.825) / 6000 = 2.0625
        assert result1.health_factor == pytest.approx(Decimal("1.375"), rel=Decimal("0.0001"))
        assert result2.health_factor == pytest.approx(Decimal("2.0625"), rel=Decimal("0.0001"))

    def test_health_factor_worsens_when_collateral_drops(self):
        """Test that HF worsens when collateral price decreases."""
        calculator = HealthFactorCalculator()
        debt = Decimal("6000")
        liquidation_threshold = Decimal("0.825")

        # Initial state: $10,000 collateral
        result1 = calculator.calculate_health_factor(
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=debt,
            liquidation_threshold=liquidation_threshold,
        )

        # After price decrease: $7,000 collateral
        result2 = calculator.calculate_health_factor(
            collateral_value_usd=Decimal("7000"),
            debt_value_usd=debt,
            liquidation_threshold=liquidation_threshold,
        )

        assert result2.health_factor < result1.health_factor
        # HF2 = (7000 * 0.825) / 6000 = 0.9625 (below liquidation!)
        assert result2.health_factor == pytest.approx(Decimal("0.9625"), rel=Decimal("0.0001"))
        assert result2.is_safe is False

    def test_health_factor_tracks_minimum_observed(self):
        """Test that calculator tracks minimum health factor observed."""
        calculator = HealthFactorCalculator()
        liquidation_threshold = Decimal("0.825")
        debt = Decimal("6000")

        # Initial - HF = 1.375
        calculator.calculate_health_factor(
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=debt,
            liquidation_threshold=liquidation_threshold,
        )
        assert calculator.min_health_factor_observed == pytest.approx(Decimal("1.375"), rel=Decimal("0.0001"))

        # Drop to lower HF = 1.1
        calculator.calculate_health_factor(
            collateral_value_usd=Decimal("8000"),
            debt_value_usd=debt,
            liquidation_threshold=liquidation_threshold,
        )
        # HF = (8000 * 0.825) / 6000 = 1.1
        assert calculator.min_health_factor_observed == pytest.approx(Decimal("1.1"), rel=Decimal("0.0001"))

        # Recover to higher HF - min should stay at 1.1
        calculator.calculate_health_factor(
            collateral_value_usd=Decimal("12000"),
            debt_value_usd=debt,
            liquidation_threshold=liquidation_threshold,
        )
        assert calculator.min_health_factor_observed == pytest.approx(Decimal("1.1"), rel=Decimal("0.0001"))

    def test_reset_tracking_clears_minimum(self):
        """Test that reset_tracking clears minimum health factor."""
        calculator = HealthFactorCalculator()

        # Record a low health factor
        calculator.calculate_health_factor(
            collateral_value_usd=Decimal("6000"),
            debt_value_usd=Decimal("5000"),
            liquidation_threshold=Decimal("0.825"),
        )
        # HF = (6000 * 0.825) / 5000 = 0.99
        assert calculator.min_health_factor_observed < Decimal("1.0")

        # Reset tracking
        calculator.reset_tracking()

        assert calculator.min_health_factor_observed == Decimal("999")
        assert calculator.warning_count == 0


class TestHealthFactorWithPortfolio:
    """Tests for health factor integration with SimulatedPortfolio."""

    def test_portfolio_updates_health_factors_on_mark_to_market(self):
        """Test that portfolio updates position health factors during mark_to_market."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("20000"))
        timestamp = datetime.now(UTC)

        # Create supply position (collateral) - 5 ETH at $2,000 = $10,000
        supply_position = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("5"),  # 5 ETH
            entry_price=Decimal("2000"),  # $2,000 per ETH
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )

        # Create borrow position (debt) - $6,000 USDC
        borrow_position = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("6000"),
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.05"),
            health_factor=Decimal("1.5"),  # Initial estimate
        )

        portfolio.positions = [supply_position, borrow_position]

        # Initial market state
        market = MockMarketState({"WETH": Decimal("2000"), "USDC": Decimal("1")})

        # Mark to market
        portfolio.mark_to_market(market, timestamp)

        # Check that health factor was updated on the borrow position
        # HF = (10000 * 0.825) / 6000 = 1.375
        assert borrow_position.health_factor is not None
        assert borrow_position.health_factor == pytest.approx(Decimal("1.375"), rel=Decimal("0.01"))

    def test_portfolio_tracks_min_health_factor(self):
        """Test that portfolio tracks minimum health factor in metrics."""
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("20000"))
        timestamp = datetime.now(UTC)

        # Create supply position (collateral) - 5 ETH at $2,000 = $10,000
        supply_position = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )

        # Create borrow position (debt) - $8,000 for lower HF
        borrow_position = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("8000"),  # Higher debt for lower HF
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.05"),
            health_factor=Decimal("1.5"),
        )

        portfolio.positions = [supply_position, borrow_position]

        # Mark to market
        market = MockMarketState({"WETH": Decimal("2000"), "USDC": Decimal("1")})
        portfolio.mark_to_market(market, timestamp)

        # HF = (10000 * 0.825) / 8000 = 1.03125
        assert portfolio._min_health_factor == pytest.approx(Decimal("1.03125"), rel=Decimal("0.01"))

        # Verify metrics include min health factor
        metrics = portfolio.get_metrics()
        assert metrics.min_health_factor == pytest.approx(Decimal("1.03125"), rel=Decimal("0.01"))

    def test_portfolio_emits_health_factor_warnings(self, caplog):
        """Test that portfolio emits warnings when health factor is low."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("20000"),
            health_factor_warning_threshold=Decimal("1.2"),
        )
        timestamp = datetime.now(UTC)

        # Create supply position (collateral) - 5 ETH at $2,000 = $10,000
        supply_position = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )

        # Create borrow position with high debt (low HF) - $8,000 USDC
        borrow_position = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("8000"),  # HF will be ~1.03
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.05"),
            health_factor=Decimal("1.5"),
        )

        portfolio.positions = [supply_position, borrow_position]

        market = MockMarketState({"WETH": Decimal("2000"), "USDC": Decimal("1")})

        with caplog.at_level(logging.WARNING, logger="almanak.framework.backtesting.pnl.calculators.health_factor"):
            portfolio.mark_to_market(market, timestamp)

        # HF = 1.03125 is below threshold 1.2, should have warning
        assert portfolio._health_factor_warnings >= 1

    def test_health_factor_warning_count_in_metrics(self):
        """Test that health factor warning count is included in metrics."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("20000"),
            health_factor_warning_threshold=Decimal("1.2"),
        )
        timestamp = datetime.now(UTC)

        supply_position = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )

        borrow_position = SimulatedPosition.borrow(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("7500"),  # HF ~ 1.1, below warning threshold
            entry_price=Decimal("1"),
            entry_time=timestamp,
            apy=Decimal("0.05"),
            health_factor=Decimal("1.5"),
        )

        portfolio.positions = [supply_position, borrow_position]

        market = MockMarketState({"WETH": Decimal("2000"), "USDC": Decimal("1")})
        portfolio.mark_to_market(market, timestamp)

        metrics = portfolio.get_metrics()
        assert metrics.health_factor_warnings >= 1


class TestProtocolSpecificThresholds:
    """Tests for protocol-specific liquidation thresholds."""

    def test_aave_v3_liquidation_threshold(self):
        """Test Aave V3 liquidation threshold lookup."""
        calculator = HealthFactorCalculator()
        threshold = calculator.get_liquidation_threshold_for_protocol("aave_v3")
        assert threshold == Decimal("0.825")

    def test_compound_v3_liquidation_threshold(self):
        """Test Compound V3 liquidation threshold lookup."""
        calculator = HealthFactorCalculator()
        threshold = calculator.get_liquidation_threshold_for_protocol("compound_v3")
        assert threshold == Decimal("0.85")

    def test_morpho_liquidation_threshold(self):
        """Test Morpho liquidation threshold lookup."""
        calculator = HealthFactorCalculator()
        threshold = calculator.get_liquidation_threshold_for_protocol("morpho")
        assert threshold == Decimal("0.825")

    def test_spark_liquidation_threshold(self):
        """Test Spark liquidation threshold lookup."""
        calculator = HealthFactorCalculator()
        threshold = calculator.get_liquidation_threshold_for_protocol("spark")
        assert threshold == Decimal("0.80")

    def test_unknown_protocol_defaults(self):
        """Test unknown protocol returns default threshold."""
        calculator = HealthFactorCalculator()
        threshold = calculator.get_liquidation_threshold_for_protocol("unknown_protocol")
        assert threshold == Decimal("0.825")

    def test_case_insensitive_protocol_lookup(self):
        """Test protocol lookup is case-insensitive."""
        calculator = HealthFactorCalculator()
        assert calculator.get_liquidation_threshold_for_protocol("AAVE_V3") == Decimal("0.825")
        assert calculator.get_liquidation_threshold_for_protocol("Compound_V3") == Decimal("0.85")

    def test_custom_protocol_thresholds(self):
        """Test custom protocol thresholds can be configured."""
        calculator = HealthFactorCalculator(
            protocol_liquidation_thresholds={
                "custom_protocol": Decimal("0.90"),
                "another_protocol": Decimal("0.75"),
            }
        )
        assert calculator.get_liquidation_threshold_for_protocol("custom_protocol") == Decimal("0.90")
        assert calculator.get_liquidation_threshold_for_protocol("another_protocol") == Decimal("0.75")


class TestCalculatorHelperMethods:
    """Tests for calculator helper methods."""

    def test_calculate_max_borrow(self):
        """Test max borrow calculation for target health factor."""
        calculator = HealthFactorCalculator()

        # $10,000 collateral at 82.5% LTV, target HF 1.5
        # max_debt = (10000 * 0.825) / 1.5 = 5500
        max_borrow = calculator.calculate_max_borrow(
            collateral_value_usd=Decimal("10000"),
            liquidation_threshold=Decimal("0.825"),
            target_health_factor=Decimal("1.5"),
        )

        assert max_borrow == pytest.approx(Decimal("5500"), rel=Decimal("0.0001"))

    def test_calculate_max_borrow_target_1_0(self):
        """Test max borrow at liquidation boundary (HF = 1.0)."""
        calculator = HealthFactorCalculator()

        # At HF = 1.0, max_debt = collateral * threshold
        max_borrow = calculator.calculate_max_borrow(
            collateral_value_usd=Decimal("10000"),
            liquidation_threshold=Decimal("0.825"),
            target_health_factor=Decimal("1.0"),
        )

        assert max_borrow == pytest.approx(Decimal("8250"), rel=Decimal("0.0001"))

    def test_calculate_max_borrow_zero_target(self):
        """Test max borrow with zero target health factor returns 0."""
        calculator = HealthFactorCalculator()

        max_borrow = calculator.calculate_max_borrow(
            collateral_value_usd=Decimal("10000"),
            liquidation_threshold=Decimal("0.825"),
            target_health_factor=Decimal("0"),
        )

        assert max_borrow == Decimal("0")

    def test_calculate_required_collateral(self):
        """Test required collateral calculation for given debt and target HF."""
        calculator = HealthFactorCalculator()

        # $5,000 debt, 82.5% LTV, target HF 1.5
        # required = (5000 * 1.5) / 0.825 = 9090.91
        required = calculator.calculate_required_collateral(
            debt_value_usd=Decimal("5000"),
            liquidation_threshold=Decimal("0.825"),
            target_health_factor=Decimal("1.5"),
        )

        assert required == pytest.approx(Decimal("9090.909"), rel=Decimal("0.001"))

    def test_calculate_required_collateral_zero_threshold(self):
        """Test required collateral with zero threshold returns large value."""
        calculator = HealthFactorCalculator()

        required = calculator.calculate_required_collateral(
            debt_value_usd=Decimal("5000"),
            liquidation_threshold=Decimal("0"),
            target_health_factor=Decimal("1.5"),
        )

        assert required == Decimal("999999999")

    def test_calculator_to_dict(self):
        """Test calculator serialization."""
        calculator = HealthFactorCalculator(
            warning_threshold=Decimal("1.3"),
            critical_threshold=Decimal("1.1"),
        )

        # Record some state
        calculator.calculate_health_factor(
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("6000"),
            liquidation_threshold=Decimal("0.825"),
        )
        calculator.warning_count = 5

        data = calculator.to_dict()

        assert data["calculator_name"] == "health_factor"
        assert data["warning_threshold"] == "1.3"
        assert data["critical_threshold"] == "1.1"
        assert data["warning_count"] == 5
        assert "min_health_factor_observed" in data
        assert "protocol_liquidation_thresholds" in data


class TestHealthFactorDataclasses:
    """Tests for health factor dataclasses."""

    def test_health_factor_result_fields(self):
        """Test HealthFactorResult has all required fields."""
        result = HealthFactorResult(
            health_factor=Decimal("1.375"),
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("6000"),
            liquidation_threshold=Decimal("0.825"),
            is_safe=True,
            distance_to_liquidation=Decimal("0.375"),
        )

        assert result.health_factor == Decimal("1.375")
        assert result.collateral_value_usd == Decimal("10000")
        assert result.debt_value_usd == Decimal("6000")
        assert result.liquidation_threshold == Decimal("0.825")
        assert result.is_safe is True
        assert result.distance_to_liquidation == Decimal("0.375")

    def test_health_factor_warning_fields(self):
        """Test HealthFactorWarning has all required fields."""
        warning = HealthFactorWarning(
            health_factor=Decimal("1.1"),
            warning_threshold=Decimal("1.2"),
            position_id="test_position",
            message="Test warning message",
            is_critical=False,
        )

        assert warning.health_factor == Decimal("1.1")
        assert warning.warning_threshold == Decimal("1.2")
        assert warning.position_id == "test_position"
        assert warning.message == "Test warning message"
        assert warning.is_critical is False


class TestPortfolioSerialization:
    """Tests for portfolio serialization with health factor fields."""

    def test_portfolio_to_dict_includes_health_factor_fields(self):
        """Test that portfolio to_dict includes health factor tracking fields."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
            health_factor_warning_threshold=Decimal("1.3"),
        )
        portfolio._min_health_factor = Decimal("1.15")
        portfolio._health_factor_warnings = 3

        data = portfolio.to_dict()

        assert data["health_factor_warning_threshold"] == "1.3"
        assert data["min_health_factor"] == "1.15"
        assert data["health_factor_warnings"] == 3

    def test_portfolio_from_dict_restores_health_factor_fields(self):
        """Test that portfolio from_dict restores health factor tracking fields."""
        original = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
            health_factor_warning_threshold=Decimal("1.3"),
        )
        original._min_health_factor = Decimal("1.15")
        original._health_factor_warnings = 3

        data = original.to_dict()
        restored = SimulatedPortfolio.from_dict(data)

        assert restored.health_factor_warning_threshold == Decimal("1.3")
        assert restored._min_health_factor == Decimal("1.15")
        assert restored._health_factor_warnings == 3

    def test_portfolio_roundtrip_serialization(self):
        """Test complete portfolio roundtrip with health factor fields."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
            health_factor_warning_threshold=Decimal("1.25"),
        )
        portfolio._min_health_factor = Decimal("0.98")
        portfolio._health_factor_warnings = 7
        timestamp = datetime.now(UTC)

        # Add a supply position with health factor
        supply_position = SimulatedPosition.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=timestamp,
            apy=Decimal("0.02"),
        )
        portfolio.positions.append(supply_position)

        # Roundtrip
        data = portfolio.to_dict()
        restored = SimulatedPortfolio.from_dict(data)

        assert restored.health_factor_warning_threshold == portfolio.health_factor_warning_threshold
        assert restored._min_health_factor == portfolio._min_health_factor
        assert restored._health_factor_warnings == portfolio._health_factor_warnings


__all__ = [
    "TestHealthFactorCalculationFormula",
    "TestHealthFactorEdgeCases",
    "TestHealthFactorWarningThreshold",
    "TestHealthFactorPriceUpdates",
    "TestHealthFactorWithPortfolio",
    "TestProtocolSpecificThresholds",
    "TestCalculatorHelperMethods",
    "TestHealthFactorDataclasses",
    "TestPortfolioSerialization",
]
