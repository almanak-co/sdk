"""Unit tests for margin validation checks.

This module tests the MarginValidator class and margin validation
in SimulatedPortfolio including:
- Margin validation accepts valid positions (sufficient collateral)
- Margin validation rejects insufficient collateral
- Margin utilization tracking
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.calculators.margin import (
    MarginUtilization,
    MarginValidationResult,
    MarginValidator,
)
from almanak.framework.backtesting.pnl.portfolio import (
    SimulatedPortfolio,
    SimulatedPosition,
)


class TestMarginValidationAcceptsValid:
    """Tests that margin validation accepts valid positions with sufficient collateral."""

    def test_exact_margin_requirement_is_valid(self):
        """Test that exact margin requirement (10% collateral) is accepted.

        10% margin ratio means collateral / position_size >= 0.1
        For $10,000 position with $1,000 collateral: 1000/10000 = 0.1 = 10%
        """
        validator = MarginValidator()
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("1000"),
            margin_ratio=Decimal("0.1"),
        )
        assert result.is_valid
        assert result.actual_margin_ratio == Decimal("0.1")
        assert result.shortfall == Decimal("0")

    def test_excess_collateral_is_valid(self):
        """Test that excess collateral (above margin requirement) is accepted.

        15% margin when only 10% required should pass.
        """
        validator = MarginValidator()
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("1500"),
            margin_ratio=Decimal("0.1"),
        )
        assert result.is_valid
        assert result.actual_margin_ratio == Decimal("0.15")
        assert result.shortfall == Decimal("0")
        assert "requirement met" in result.message.lower()

    def test_50_percent_margin_is_valid(self):
        """Test that 50% margin (2x leverage) passes with 10% requirement."""
        validator = MarginValidator()
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("5000"),
            margin_ratio=Decimal("0.1"),
        )
        assert result.is_valid
        assert result.actual_margin_ratio == Decimal("0.5")

    def test_full_collateral_is_valid(self):
        """Test that 100% margin (1x leverage, no leverage) is valid."""
        validator = MarginValidator()
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("10000"),
            margin_ratio=Decimal("0.1"),
        )
        assert result.is_valid
        assert result.actual_margin_ratio == Decimal("1")

    def test_gmx_margin_requirement_valid(self):
        """Test valid position with GMX margin requirement (1% = 100x max leverage).

        GMX allows 100x leverage, so 1% margin is required.
        $100 collateral for $10,000 position = 1% margin = exactly at requirement.
        """
        validator = MarginValidator()
        gmx_margins = validator.get_margin_for_protocol("gmx")
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("100"),
            margin_ratio=gmx_margins["initial"],
        )
        assert result.is_valid
        assert result.actual_margin_ratio == Decimal("0.01")

    def test_hyperliquid_margin_requirement_valid(self):
        """Test valid position with Hyperliquid margin requirement."""
        validator = MarginValidator()
        hl_margins = validator.get_margin_for_protocol("hyperliquid")
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("150"),  # 1.5% > 1% required
            margin_ratio=hl_margins["initial"],
        )
        assert result.is_valid
        assert result.actual_margin_ratio == Decimal("0.015")

    def test_zero_position_size_is_valid(self):
        """Test that zero position size is always valid (nothing to validate)."""
        validator = MarginValidator()
        result = validator.validate_margin(
            position_size=Decimal("0"),
            collateral=Decimal("1000"),
            margin_ratio=Decimal("0.1"),
        )
        assert result.is_valid
        assert result.message == "No position to validate"

    def test_default_margin_ratio_used(self):
        """Test that default margin ratio (10%) is used when not specified."""
        validator = MarginValidator()
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("1000"),
            # No margin_ratio specified
        )
        assert result.is_valid
        assert result.required_margin_ratio == Decimal("0.1")

    def test_custom_default_margin_ratio(self):
        """Test validator with custom default margin ratios."""
        validator = MarginValidator(
            default_initial_margin_ratio=Decimal("0.2"),  # 20% = 5x max leverage
        )
        # 15% margin should fail with 20% requirement
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("1500"),
        )
        assert not result.is_valid

        # 20% margin should pass
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("2000"),
        )
        assert result.is_valid


class TestMarginValidationRejectsInsufficient:
    """Tests that margin validation rejects positions with insufficient collateral."""

    def test_insufficient_collateral_rejected(self):
        """Test that insufficient collateral (below margin requirement) is rejected.

        5% margin when 10% required should fail.
        """
        validator = MarginValidator()
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("500"),  # 5% < 10% required
            margin_ratio=Decimal("0.1"),
        )
        assert not result.is_valid
        assert result.actual_margin_ratio == Decimal("0.05")
        assert result.shortfall == Decimal("500")  # Need $500 more
        assert "insufficient margin" in result.message.lower()

    def test_zero_collateral_rejected(self):
        """Test that zero collateral is rejected."""
        validator = MarginValidator()
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("0"),
            margin_ratio=Decimal("0.1"),
        )
        assert not result.is_valid
        assert result.actual_margin_ratio == Decimal("0")
        assert result.shortfall == Decimal("1000")  # Need $1000

    def test_nearly_sufficient_collateral_rejected(self):
        """Test that nearly sufficient collateral (e.g., 9.9%) is rejected.

        9.9% margin when 10% required should fail.
        """
        validator = MarginValidator()
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("990"),  # 9.9% < 10% required
            margin_ratio=Decimal("0.1"),
        )
        assert not result.is_valid
        assert result.shortfall == Decimal("10")  # Need $10 more

    def test_extreme_leverage_rejected(self):
        """Test that extremely high leverage (e.g., 200x) is rejected.

        0.5% margin when 1% (GMX) required should fail.
        """
        validator = MarginValidator()
        gmx_margins = validator.get_margin_for_protocol("gmx")
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("50"),  # 0.5% < 1% required
            margin_ratio=gmx_margins["initial"],
        )
        assert not result.is_valid
        assert result.shortfall == Decimal("50")  # Need $50 more for 1%

    def test_shortfall_calculation_correct(self):
        """Test that shortfall is calculated correctly.

        For $10,000 position with 10% margin, need $1,000.
        With $750 collateral, shortfall should be $250.
        """
        validator = MarginValidator()
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("750"),
            margin_ratio=Decimal("0.1"),
        )
        assert not result.is_valid
        assert result.shortfall == Decimal("250")

    def test_negative_position_size_raises_error(self):
        """Test that negative position size raises ValueError."""
        validator = MarginValidator()
        with pytest.raises(ValueError, match="position_size cannot be negative"):
            validator.validate_margin(
                position_size=Decimal("-10000"),
                collateral=Decimal("1000"),
                margin_ratio=Decimal("0.1"),
            )

    def test_negative_collateral_raises_error(self):
        """Test that negative collateral raises ValueError."""
        validator = MarginValidator()
        with pytest.raises(ValueError, match="collateral cannot be negative"):
            validator.validate_margin(
                position_size=Decimal("10000"),
                collateral=Decimal("-1000"),
                margin_ratio=Decimal("0.1"),
            )

    def test_check_sufficient_collateral_returns_false(self):
        """Test the convenience method returns False for insufficient margin."""
        validator = MarginValidator()
        is_sufficient = validator.check_sufficient_collateral(
            position_size=Decimal("10000"),
            collateral=Decimal("500"),
            margin_ratio=Decimal("0.1"),
            log_warning=False,
        )
        assert not is_sufficient

    def test_check_sufficient_collateral_returns_true(self):
        """Test the convenience method returns True for sufficient margin."""
        validator = MarginValidator()
        is_sufficient = validator.check_sufficient_collateral(
            position_size=Decimal("10000"),
            collateral=Decimal("1500"),
            margin_ratio=Decimal("0.1"),
            log_warning=False,
        )
        assert is_sufficient


class TestMarginUtilizationTracking:
    """Tests for margin utilization calculation and tracking."""

    def test_calculate_margin_utilization_50_percent(self):
        """Test margin utilization calculation at 50%.

        With $5,000 margin used and $5,000 available = 50% utilization.
        """
        validator = MarginValidator()
        utilization = validator.calculate_margin_utilization(
            total_margin_used=Decimal("5000"),
            total_notional=Decimal("50000"),
            available_capital=Decimal("5000"),
        )
        assert utilization.utilization_ratio == Decimal("0.5")
        assert utilization.total_margin_used == Decimal("5000")
        assert utilization.available_capital == Decimal("5000")

    def test_calculate_margin_utilization_zero(self):
        """Test margin utilization at 0% (no positions)."""
        validator = MarginValidator()
        utilization = validator.calculate_margin_utilization(
            total_margin_used=Decimal("0"),
            total_notional=Decimal("0"),
            available_capital=Decimal("10000"),
        )
        assert utilization.utilization_ratio == Decimal("0")

    def test_calculate_margin_utilization_90_percent(self):
        """Test margin utilization at 90% (near max)."""
        validator = MarginValidator()
        utilization = validator.calculate_margin_utilization(
            total_margin_used=Decimal("9000"),
            total_notional=Decimal("90000"),
            available_capital=Decimal("1000"),
        )
        assert utilization.utilization_ratio == Decimal("0.9")

    def test_calculate_margin_utilization_no_capital(self):
        """Test margin utilization when no capital at all (edge case)."""
        validator = MarginValidator()
        utilization = validator.calculate_margin_utilization(
            total_margin_used=Decimal("0"),
            total_notional=Decimal("0"),
            available_capital=Decimal("0"),
        )
        assert utilization.utilization_ratio == Decimal("0")

    def test_can_open_position_succeeds(self):
        """Test can_open_position returns True for valid position."""
        validator = MarginValidator()
        can_open, reason = validator.can_open_position(
            position_size=Decimal("10000"),
            collateral=Decimal("1000"),
            available_capital=Decimal("5000"),
            current_margin_used=Decimal("1000"),
            margin_ratio=Decimal("0.1"),
        )
        assert can_open
        assert "can be opened" in reason.lower()

    def test_can_open_position_fails_insufficient_margin(self):
        """Test can_open_position returns False for insufficient margin."""
        validator = MarginValidator()
        can_open, reason = validator.can_open_position(
            position_size=Decimal("10000"),
            collateral=Decimal("500"),  # 5% < 10% required
            available_capital=Decimal("5000"),
            current_margin_used=Decimal("0"),
            margin_ratio=Decimal("0.1"),
        )
        assert not can_open
        assert "insufficient margin" in reason.lower()

    def test_can_open_position_fails_insufficient_capital(self):
        """Test can_open_position returns False when not enough capital."""
        validator = MarginValidator()
        can_open, reason = validator.can_open_position(
            position_size=Decimal("10000"),
            collateral=Decimal("1000"),
            available_capital=Decimal("500"),  # Not enough cash
            current_margin_used=Decimal("0"),
            margin_ratio=Decimal("0.1"),
        )
        assert not can_open
        assert "insufficient available capital" in reason.lower()

    def test_can_open_position_fails_exceeds_utilization(self):
        """Test can_open_position returns False when would exceed max utilization."""
        validator = MarginValidator(
            max_margin_utilization_ratio=Decimal("0.5"),  # 50% max
        )
        # Already have $4,000 margin used, with $6,000 total capital
        # Current utilization: 4000/10000 = 40%
        # Adding $2,000 more would make it: 6000/10000 = 60% > 50% max
        can_open, reason = validator.can_open_position(
            position_size=Decimal("20000"),
            collateral=Decimal("2000"),
            available_capital=Decimal("6000"),
            current_margin_used=Decimal("4000"),
            margin_ratio=Decimal("0.1"),
        )
        assert not can_open
        assert "exceed max margin utilization" in reason.lower()

    def test_position_increase_validation(self):
        """Test validating margin for position size increase."""
        validator = MarginValidator()

        # Adding $5000 to existing $10000 position with $500 more collateral
        result = validator.validate_position_increase(
            current_position_size=Decimal("10000"),
            current_collateral=Decimal("1000"),
            additional_size=Decimal("5000"),
            additional_collateral=Decimal("500"),
            margin_ratio=Decimal("0.1"),
        )
        # Total: $15,000 position with $1,500 collateral = 10% margin
        assert result.is_valid
        assert result.position_size == Decimal("15000")
        assert result.collateral == Decimal("1500")

    def test_position_increase_validation_fails(self):
        """Test position increase fails when insufficient additional collateral."""
        validator = MarginValidator()

        result = validator.validate_position_increase(
            current_position_size=Decimal("10000"),
            current_collateral=Decimal("1000"),
            additional_size=Decimal("5000"),
            additional_collateral=Decimal("0"),  # No additional collateral
            margin_ratio=Decimal("0.1"),
        )
        # Total: $15,000 position with $1,000 collateral = 6.67% < 10% required
        assert not result.is_valid


class TestSimulatedPortfolioMarginValidation:
    """Tests for margin validation in SimulatedPortfolio."""

    def test_portfolio_validate_margin_for_perp_valid(self):
        """Test portfolio validates perp margin correctly for valid position."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
            initial_margin_ratio=Decimal("0.1"),
            maintenance_margin_ratio=Decimal("0.05"),
        )
        is_valid, msg = portfolio.validate_margin_for_perp(
            position_size=Decimal("10000"),
            collateral=Decimal("1500"),  # 15% > 10% required
        )
        assert is_valid
        assert "can be opened" in msg.lower()

    def test_portfolio_validate_margin_for_perp_invalid(self):
        """Test portfolio rejects perp with insufficient margin."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
            initial_margin_ratio=Decimal("0.1"),
            maintenance_margin_ratio=Decimal("0.05"),
        )
        is_valid, msg = portfolio.validate_margin_for_perp(
            position_size=Decimal("10000"),
            collateral=Decimal("500"),  # 5% < 10% required
        )
        assert not is_valid
        assert "insufficient margin" in msg.lower()

    def test_portfolio_margin_utilization_tracking(self):
        """Test portfolio tracks margin utilization correctly."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
        )

        # Initially 0% utilization
        assert portfolio.get_margin_utilization() == Decimal("0")

        # Add a perp position
        perp_position = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("2000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=datetime.now(UTC),
            protocol="gmx",
        )
        portfolio.positions.append(perp_position)
        # Deduct collateral from cash
        portfolio.cash_usd -= Decimal("2000")

        # Now should have 2000/(2000+8000) = 20% utilization
        utilization = portfolio.get_margin_utilization()
        assert utilization == Decimal("0.2")

    def test_portfolio_max_margin_utilization_tracking(self):
        """Test portfolio tracks maximum margin utilization."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
        )

        # Initial max utilization is 0
        assert portfolio._max_margin_utilization == Decimal("0")

        # Add first perp position (20% utilization)
        perp1 = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("2000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=datetime.now(UTC),
            protocol="gmx",
        )
        portfolio.positions.append(perp1)
        portfolio.cash_usd -= Decimal("2000")
        portfolio.update_max_margin_utilization()
        assert portfolio._max_margin_utilization == Decimal("0.2")

        # Add second perp position (50% utilization)
        perp2 = SimulatedPosition.perp_short(
            token="BTC",
            collateral_usd=Decimal("3000"),
            leverage=Decimal("3"),
            entry_price=Decimal("40000"),
            entry_time=datetime.now(UTC),
            protocol="gmx",
        )
        portfolio.positions.append(perp2)
        portfolio.cash_usd -= Decimal("3000")
        portfolio.update_max_margin_utilization()
        assert portfolio._max_margin_utilization == Decimal("0.5")

        # Close one position, utilization drops but max stays
        portfolio.positions.remove(perp2)
        portfolio.cash_usd += Decimal("3000")
        portfolio.update_max_margin_utilization()
        # Max should stay at 0.5 (peak), not drop to 0.2
        assert portfolio._max_margin_utilization == Decimal("0.5")

    def test_portfolio_check_can_open_perp_position(self):
        """Test portfolio check_can_open_perp_position method."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
            initial_margin_ratio=Decimal("0.1"),
        )

        # Valid perp position
        perp = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("1000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=datetime.now(UTC),
            protocol="gmx",
        )
        can_open, reason = portfolio.check_can_open_perp_position(perp)
        assert can_open

    def test_portfolio_margin_in_metrics(self):
        """Test that margin utilization is included in metrics."""
        from almanak.framework.backtesting.models import EquityPoint

        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
        )

        # Add an equity point so get_metrics doesn't return early
        portfolio.equity_curve.append(
            EquityPoint(timestamp=datetime.now(UTC), value_usd=Decimal("10000"))
        )

        # Add a perp position for 50% utilization
        perp = SimulatedPosition.perp_long(
            token="ETH",
            collateral_usd=Decimal("5000"),
            leverage=Decimal("5"),
            entry_price=Decimal("2000"),
            entry_time=datetime.now(UTC),
            protocol="gmx",
        )
        portfolio.positions.append(perp)
        portfolio.cash_usd -= Decimal("5000")
        portfolio.update_max_margin_utilization()

        # Get metrics and verify max_margin_utilization
        metrics = portfolio.get_metrics()
        assert metrics.max_margin_utilization == Decimal("0.5")


class TestMarginValidatorHelperMethods:
    """Tests for MarginValidator helper methods."""

    def test_get_max_leverage_for_margin(self):
        """Test calculating max leverage from margin ratio."""
        validator = MarginValidator()

        # 10% margin = 10x leverage
        assert validator.get_max_leverage_for_margin(Decimal("0.1")) == Decimal("10")

        # 1% margin = 100x leverage
        assert validator.get_max_leverage_for_margin(Decimal("0.01")) == Decimal("100")

        # 50% margin = 2x leverage
        assert validator.get_max_leverage_for_margin(Decimal("0.5")) == Decimal("2")

    def test_get_max_leverage_raises_for_zero(self):
        """Test that zero margin ratio raises ValueError."""
        validator = MarginValidator()
        with pytest.raises(ValueError):
            validator.get_max_leverage_for_margin(Decimal("0"))

    def test_get_required_collateral(self):
        """Test calculating required collateral for a position."""
        validator = MarginValidator()

        # $10,000 position with 10% margin = $1,000 collateral
        collateral = validator.get_required_collateral(
            position_size=Decimal("10000"),
            margin_ratio=Decimal("0.1"),
        )
        assert collateral == Decimal("1000")

        # $50,000 position with 5% margin = $2,500 collateral
        collateral = validator.get_required_collateral(
            position_size=Decimal("50000"),
            margin_ratio=Decimal("0.05"),
        )
        assert collateral == Decimal("2500")

    def test_get_margin_for_protocol(self):
        """Test getting protocol-specific margin requirements."""
        validator = MarginValidator()

        # GMX margins
        gmx = validator.get_margin_for_protocol("gmx")
        assert gmx["initial"] == Decimal("0.01")
        assert gmx["maintenance"] == Decimal("0.01")

        # Hyperliquid margins
        hl = validator.get_margin_for_protocol("hyperliquid")
        assert hl["initial"] == Decimal("0.01")
        assert hl["maintenance"] == Decimal("0.005")

        # Unknown protocol gets defaults
        unknown = validator.get_margin_for_protocol("unknown_protocol")
        assert unknown["initial"] == validator.default_initial_margin_ratio
        assert unknown["maintenance"] == validator.default_maintenance_margin_ratio

    def test_validation_result_to_dict(self):
        """Test MarginValidationResult serialization."""
        result = MarginValidationResult(
            is_valid=True,
            position_size=Decimal("10000"),
            collateral=Decimal("1000"),
            required_margin_ratio=Decimal("0.1"),
            actual_margin_ratio=Decimal("0.1"),
            shortfall=Decimal("0"),
            message="Valid",
        )
        d = result.to_dict()
        assert d["is_valid"] is True
        assert d["position_size"] == "10000"
        assert d["shortfall"] == "0"

    def test_margin_utilization_to_dict(self):
        """Test MarginUtilization serialization."""
        utilization = MarginUtilization(
            total_margin_used=Decimal("5000"),
            total_notional=Decimal("50000"),
            available_capital=Decimal("5000"),
            utilization_ratio=Decimal("0.5"),
        )
        d = utilization.to_dict()
        assert d["total_margin_used"] == "5000"
        assert d["utilization_ratio"] == "0.5"

    def test_validator_to_dict(self):
        """Test MarginValidator serialization."""
        validator = MarginValidator()
        d = validator.to_dict()
        assert d["validator_name"] == "margin"
        assert d["default_initial_margin_ratio"] == "0.1"
        assert "gmx" in d["protocol_margins"]


class TestMarginEdgeCases:
    """Tests for edge cases in margin validation."""

    def test_very_small_position(self):
        """Test margin validation for very small position sizes."""
        validator = MarginValidator()
        result = validator.validate_margin(
            position_size=Decimal("0.01"),  # $0.01 position
            collateral=Decimal("0.001"),  # $0.001 collateral
            margin_ratio=Decimal("0.1"),
        )
        assert result.is_valid
        assert result.actual_margin_ratio == Decimal("0.1")

    def test_very_large_position(self):
        """Test margin validation for very large position sizes."""
        validator = MarginValidator()
        result = validator.validate_margin(
            position_size=Decimal("1000000000"),  # $1B position
            collateral=Decimal("100000000"),  # $100M collateral
            margin_ratio=Decimal("0.1"),
        )
        assert result.is_valid
        assert result.actual_margin_ratio == Decimal("0.1")

    def test_fractional_margin_ratios(self):
        """Test margin validation with fractional/decimal margin ratios."""
        validator = MarginValidator()
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("555"),  # 5.55%
            margin_ratio=Decimal("0.055"),  # 5.5% required
        )
        # 5.55% > 5.5% should pass
        assert result.is_valid
        assert result.actual_margin_ratio > Decimal("0.055")

    def test_multiple_perp_positions_margin_tracking(self):
        """Test margin tracking with multiple perp positions."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("20000"),
        )

        # Add multiple positions
        for i in range(3):
            perp = SimulatedPosition.perp_long(
                token=f"TOKEN{i}",
                collateral_usd=Decimal("2000"),
                leverage=Decimal("5"),
                entry_price=Decimal("100"),
                entry_time=datetime.now(UTC),
                protocol="gmx",
            )
            portfolio.positions.append(perp)
            portfolio.cash_usd -= Decimal("2000")
            portfolio.update_max_margin_utilization()

        # Total margin used: 3 * $2000 = $6000
        # Total capital: $20000
        # Utilization: 6000/20000 = 30%
        # But after deducting cash: margin_used / (margin_used + remaining_cash)
        # = 6000 / (6000 + 14000) = 30%
        assert portfolio.get_margin_utilization() == Decimal("0.3")
        assert portfolio._max_margin_utilization == Decimal("0.3")
