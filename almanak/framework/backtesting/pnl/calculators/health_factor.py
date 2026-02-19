"""Health factor calculator for lending positions.

This module provides tools for calculating and monitoring health factors for
lending protocol positions during backtesting. The health factor indicates how
safe a borrow position is from liquidation.

Key Concepts:
    - Health Factor: Ratio of collateral value (adjusted by liquidation threshold)
      to debt value. HF >= 1.0 means position is safe, HF < 1.0 triggers liquidation.
    - Liquidation Threshold: The percentage of collateral value that can be borrowed
      (e.g., 0.825 means 82.5% LTV at liquidation)
    - Warning Threshold: A configurable HF level below which warnings are emitted

Health Factor Formula:
    health_factor = (collateral_value * liquidation_threshold) / debt_value

Example:
    from almanak.framework.backtesting.pnl.calculators.health_factor import (
        HealthFactorCalculator,
        HealthFactorResult,
    )

    calculator = HealthFactorCalculator()

    # Calculate health factor
    result = calculator.calculate_health_factor(
        collateral_value_usd=Decimal("10000"),
        debt_value_usd=Decimal("6000"),
        liquidation_threshold=Decimal("0.825"),
    )
    print(f"Health Factor: {result.health_factor}")  # ~1.375
    print(f"Safe: {result.is_safe}")  # True

References:
    - Aave V3 Health Factor: https://docs.aave.com/developers/guides/liquidations
    - Compound V3: https://docs.compound.finance/collateral-and-borrowing/
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class HealthFactorResult:
    """Result of a health factor calculation.

    Attributes:
        health_factor: The calculated health factor (>= 1.0 is safe)
        collateral_value_usd: The collateral value used in calculation
        debt_value_usd: The debt value used in calculation
        liquidation_threshold: The liquidation threshold applied
        is_safe: Whether the position is safe (health factor >= 1.0)
        distance_to_liquidation: How far from liquidation (HF - 1.0)
    """

    health_factor: Decimal
    collateral_value_usd: Decimal
    debt_value_usd: Decimal
    liquidation_threshold: Decimal
    is_safe: bool
    distance_to_liquidation: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "health_factor": str(self.health_factor),
            "collateral_value_usd": str(self.collateral_value_usd),
            "debt_value_usd": str(self.debt_value_usd),
            "liquidation_threshold": str(self.liquidation_threshold),
            "is_safe": self.is_safe,
            "distance_to_liquidation": str(self.distance_to_liquidation),
        }


@dataclass
class HealthFactorWarning:
    """Warning emitted when health factor drops below threshold.

    Attributes:
        health_factor: Current health factor value
        warning_threshold: The threshold that was breached
        position_id: ID of the position in danger
        message: Human-readable warning message
        is_critical: Whether the warning is critical (very close to liquidation)
    """

    health_factor: Decimal
    warning_threshold: Decimal
    position_id: str
    message: str
    is_critical: bool


@dataclass
class HealthFactorCalculator:
    """Calculator for health factors on lending positions.

    This class provides the primary API for calculating health factors during
    backtesting. It tracks minimum health factors observed and emits warnings
    when positions become dangerous.

    Health factor formula:
        HF = (collateral_value_usd * liquidation_threshold) / debt_value_usd

    Where:
        - collateral_value_usd: Total value of supplied collateral
        - debt_value_usd: Total value of borrowed debt
        - liquidation_threshold: Protocol's liquidation LTV (e.g., 0.825 for 82.5%)

    Protocol-specific liquidation thresholds (examples):
        - Aave V3: 0.80-0.86 depending on asset
        - Compound V3: 0.83-0.90 depending on asset
        - Morpho: Uses underlying protocol's thresholds

    Attributes:
        warning_threshold: Health factor below which to emit warnings (default 1.2)
        critical_threshold: Health factor below which to emit critical warnings (default 1.05)
        protocol_liquidation_thresholds: Protocol-specific default thresholds
        min_health_factor_observed: Minimum health factor seen (for metrics)
        warning_count: Number of warnings emitted

    Example:
        calculator = HealthFactorCalculator()

        result = calculator.calculate_health_factor(
            collateral_value_usd=Decimal("10000"),
            debt_value_usd=Decimal("6000"),
            liquidation_threshold=Decimal("0.825"),
        )
        # result.health_factor = 1.375
        # result.is_safe = True
    """

    warning_threshold: Decimal = Decimal("1.2")
    critical_threshold: Decimal = Decimal("1.05")
    protocol_liquidation_thresholds: dict[str, Decimal] = field(default_factory=dict)
    min_health_factor_observed: Decimal = Decimal("999")
    warning_count: int = 0

    def __post_init__(self) -> None:
        """Initialize protocol-specific liquidation thresholds."""
        if not self.protocol_liquidation_thresholds:
            self.protocol_liquidation_thresholds = {
                "aave_v3": Decimal("0.825"),  # Average across assets
                "compound_v3": Decimal("0.85"),
                "morpho": Decimal("0.825"),  # Uses Aave thresholds
                "spark": Decimal("0.80"),
            }

    def calculate_health_factor(
        self,
        collateral_value_usd: Decimal,
        debt_value_usd: Decimal,
        liquidation_threshold: Decimal,
    ) -> HealthFactorResult:
        """Calculate health factor for a lending position.

        The health factor indicates how safe a position is from liquidation.
        A health factor >= 1.0 means the position is safe, while < 1.0 means
        the position can be liquidated.

        Formula:
            health_factor = (collateral_value * liquidation_threshold) / debt_value

        Args:
            collateral_value_usd: Total collateral value in USD
            debt_value_usd: Total debt value in USD
            liquidation_threshold: The liquidation threshold as a decimal (e.g., 0.825)

        Returns:
            HealthFactorResult with health factor and safety status

        Example:
            # Position with $10,000 collateral and $6,000 debt at 82.5% LTV
            result = calculator.calculate_health_factor(
                collateral_value_usd=Decimal("10000"),
                debt_value_usd=Decimal("6000"),
                liquidation_threshold=Decimal("0.825"),
            )
            # result.health_factor = 1.375
        """
        # Handle edge cases
        if debt_value_usd <= Decimal("0"):
            # No debt means infinite health factor (use large number)
            return HealthFactorResult(
                health_factor=Decimal("999"),
                collateral_value_usd=collateral_value_usd,
                debt_value_usd=debt_value_usd,
                liquidation_threshold=liquidation_threshold,
                is_safe=True,
                distance_to_liquidation=Decimal("998"),
            )

        if collateral_value_usd <= Decimal("0"):
            # No collateral means zero health factor (instant liquidation)
            return HealthFactorResult(
                health_factor=Decimal("0"),
                collateral_value_usd=collateral_value_usd,
                debt_value_usd=debt_value_usd,
                liquidation_threshold=liquidation_threshold,
                is_safe=False,
                distance_to_liquidation=Decimal("-1"),
            )

        # Calculate health factor
        # HF = (collateral * liquidation_threshold) / debt
        adjusted_collateral = collateral_value_usd * liquidation_threshold
        health_factor = adjusted_collateral / debt_value_usd

        # Determine safety status
        is_safe = health_factor >= Decimal("1.0")
        distance_to_liquidation = health_factor - Decimal("1.0")

        # Track minimum health factor
        if health_factor < self.min_health_factor_observed:
            self.min_health_factor_observed = health_factor

        return HealthFactorResult(
            health_factor=health_factor,
            collateral_value_usd=collateral_value_usd,
            debt_value_usd=debt_value_usd,
            liquidation_threshold=liquidation_threshold,
            is_safe=is_safe,
            distance_to_liquidation=distance_to_liquidation,
        )

    def check_health_factor_warning(
        self,
        health_factor: Decimal,
        position_id: str,
        emit_warning: bool = True,
    ) -> HealthFactorWarning | None:
        """Check if health factor is below warning threshold and emit warning.

        This method checks if the current health factor is below the warning
        threshold and optionally emits a warning log message.

        Args:
            health_factor: Current health factor value
            position_id: ID of the position being checked
            emit_warning: Whether to log a warning message (default True)

        Returns:
            HealthFactorWarning if below threshold, None otherwise

        Example:
            warning = calculator.check_health_factor_warning(
                health_factor=Decimal("1.15"),
                position_id="BORROW_aave_v3_USDC_123456",
            )
            if warning:
                print(f"Warning: {warning.message}")
        """
        if health_factor >= self.warning_threshold:
            return None

        # Determine if critical
        is_critical = health_factor < self.critical_threshold

        # Build warning message
        if is_critical:
            severity = "CRITICAL"
            message = (
                f"{severity}: Health factor {health_factor:.4f} for position "
                f"{position_id} is critically low! Liquidation imminent."
            )
        else:
            severity = "WARNING"
            message = (
                f"{severity}: Health factor {health_factor:.4f} for position "
                f"{position_id} is below threshold {self.warning_threshold}. "
                f"Consider adding collateral or repaying debt."
            )

        # Emit warning if requested
        if emit_warning:
            self.warning_count += 1
            if is_critical:
                logger.warning(message)
            else:
                logger.warning(message)

        return HealthFactorWarning(
            health_factor=health_factor,
            warning_threshold=self.warning_threshold,
            position_id=position_id,
            message=message,
            is_critical=is_critical,
        )

    def get_liquidation_threshold_for_protocol(self, protocol: str) -> Decimal:
        """Get the default liquidation threshold for a specific protocol.

        Args:
            protocol: Protocol name (e.g., "aave_v3", "compound_v3")

        Returns:
            The protocol's default liquidation threshold, or 0.825 if not found
        """
        return self.protocol_liquidation_thresholds.get(protocol.lower(), Decimal("0.825"))

    def calculate_max_borrow(
        self,
        collateral_value_usd: Decimal,
        liquidation_threshold: Decimal,
        target_health_factor: Decimal = Decimal("1.5"),
    ) -> Decimal:
        """Calculate maximum safe borrow amount for a target health factor.

        This is useful for determining how much can be borrowed while
        maintaining a safe health factor.

        Formula:
            max_debt = (collateral * liquidation_threshold) / target_hf

        Args:
            collateral_value_usd: Total collateral value in USD
            liquidation_threshold: The liquidation threshold as a decimal
            target_health_factor: Desired health factor after borrowing (default 1.5)

        Returns:
            Maximum debt value in USD that maintains the target health factor

        Example:
            # How much can I borrow with $10,000 collateral at 82.5% LTV
            # while maintaining 1.5 HF?
            max_borrow = calculator.calculate_max_borrow(
                collateral_value_usd=Decimal("10000"),
                liquidation_threshold=Decimal("0.825"),
                target_health_factor=Decimal("1.5"),
            )
            # max_borrow = $5,500
        """
        if target_health_factor <= Decimal("0"):
            return Decimal("0")

        adjusted_collateral = collateral_value_usd * liquidation_threshold
        return adjusted_collateral / target_health_factor

    def calculate_required_collateral(
        self,
        debt_value_usd: Decimal,
        liquidation_threshold: Decimal,
        target_health_factor: Decimal = Decimal("1.5"),
    ) -> Decimal:
        """Calculate required collateral for a given debt and target health factor.

        Formula:
            required_collateral = (debt * target_hf) / liquidation_threshold

        Args:
            debt_value_usd: Total debt value in USD
            liquidation_threshold: The liquidation threshold as a decimal
            target_health_factor: Desired health factor (default 1.5)

        Returns:
            Required collateral value in USD

        Example:
            # How much collateral do I need for $5,000 debt to have 1.5 HF?
            required = calculator.calculate_required_collateral(
                debt_value_usd=Decimal("5000"),
                liquidation_threshold=Decimal("0.825"),
                target_health_factor=Decimal("1.5"),
            )
            # required = ~$9,091
        """
        if liquidation_threshold <= Decimal("0"):
            return Decimal("999999999")  # Infinite collateral needed

        return (debt_value_usd * target_health_factor) / liquidation_threshold

    def reset_tracking(self) -> None:
        """Reset min health factor and warning count tracking.

        Call this at the start of a new backtest run to reset accumulated
        tracking data.
        """
        self.min_health_factor_observed = Decimal("999")
        self.warning_count = 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "calculator_name": "health_factor",
            "warning_threshold": str(self.warning_threshold),
            "critical_threshold": str(self.critical_threshold),
            "min_health_factor_observed": str(self.min_health_factor_observed),
            "warning_count": self.warning_count,
            "protocol_liquidation_thresholds": {k: str(v) for k, v in self.protocol_liquidation_thresholds.items()},
        }


__all__ = [
    "HealthFactorCalculator",
    "HealthFactorResult",
    "HealthFactorWarning",
]
