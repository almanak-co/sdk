"""Margin validation calculator for perpetual futures positions.

This module provides tools for validating margin requirements before opening
or increasing perpetual positions during backtesting. It ensures that positions
have sufficient collateral relative to their size and tracks margin utilization.

Key Concepts:
    - Initial Margin: Minimum collateral required to open a position
    - Margin Ratio: collateral / position_size (inverse of leverage)
    - Margin Utilization: How much of available capital is used as margin

Margin Validation Formula:
    validate_margin(position_size, collateral, margin_ratio) returns True if:
        collateral / position_size >= margin_ratio

Example:
    from almanak.framework.backtesting.pnl.calculators.margin import (
        MarginValidator,
        MarginValidationResult,
    )

    validator = MarginValidator()

    # Validate margin for a $10,000 position with $1,000 collateral (10x leverage)
    result = validator.validate_margin(
        position_size=Decimal("10000"),
        collateral=Decimal("1000"),
        margin_ratio=Decimal("0.1"),  # 10% initial margin required
    )
    # result.is_valid = True (1000/10000 = 0.1 >= 0.1)

References:
    - GMX V2 Margin: https://docs.gmx.io/docs/trading/v2#margin
    - Hyperliquid: https://hyperliquid.gitbook.io/hyperliquid-docs
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class MarginValidationResult:
    """Result of a margin validation check.

    Attributes:
        is_valid: Whether the margin requirement is met
        position_size: The position size being validated
        collateral: The collateral amount
        required_margin_ratio: The minimum margin ratio required
        actual_margin_ratio: The actual margin ratio (collateral / position_size)
        shortfall: Amount of additional collateral needed (0 if valid)
        message: Human-readable validation message
    """

    is_valid: bool
    position_size: Decimal
    collateral: Decimal
    required_margin_ratio: Decimal
    actual_margin_ratio: Decimal
    shortfall: Decimal
    message: str

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "is_valid": self.is_valid,
            "position_size": str(self.position_size),
            "collateral": str(self.collateral),
            "required_margin_ratio": str(self.required_margin_ratio),
            "actual_margin_ratio": str(self.actual_margin_ratio),
            "shortfall": str(self.shortfall),
            "message": self.message,
        }


@dataclass
class MarginUtilization:
    """Current margin utilization state.

    Attributes:
        total_margin_used: Total collateral locked in perp positions
        total_notional: Total notional value of all perp positions
        available_capital: Capital available for new positions
        utilization_ratio: total_margin_used / (total_margin_used + available_capital)
    """

    total_margin_used: Decimal
    total_notional: Decimal
    available_capital: Decimal
    utilization_ratio: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "total_margin_used": str(self.total_margin_used),
            "total_notional": str(self.total_notional),
            "available_capital": str(self.available_capital),
            "utilization_ratio": str(self.utilization_ratio),
        }


@dataclass
class MarginValidator:
    """Validator for margin requirements on perpetual positions.

    This class provides the primary API for validating margin requirements
    and tracking margin utilization during backtesting.

    The margin validation formula:
        is_valid = collateral / position_size >= required_margin_ratio

    For leverage:
        leverage = position_size / collateral = 1 / margin_ratio
        So 10% margin ratio = 10x max leverage

    Attributes:
        default_initial_margin_ratio: Default initial margin ratio (default 0.1 = 10%)
        default_maintenance_margin_ratio: Default maintenance margin (default 0.05 = 5%)
        max_margin_utilization_ratio: Maximum allowed margin utilization (default 0.9 = 90%)
        protocol_margins: Protocol-specific margin requirements

    Example:
        validator = MarginValidator()

        # Check if we can open a $10,000 position with $1,500 collateral
        result = validator.validate_margin(
            position_size=Decimal("10000"),
            collateral=Decimal("1500"),
            margin_ratio=Decimal("0.1"),  # 10% = 10x max leverage
        )
        # result.is_valid = True (15% margin > 10% required)

        # Track margin utilization
        utilization = validator.calculate_margin_utilization(
            total_margin_used=Decimal("5000"),
            total_notional=Decimal("50000"),
            available_capital=Decimal("5000"),
        )
        # utilization.utilization_ratio = 0.5 (50% utilized)
    """

    default_initial_margin_ratio: Decimal = Decimal("0.1")  # 10%
    default_maintenance_margin_ratio: Decimal = Decimal("0.05")  # 5%
    max_margin_utilization_ratio: Decimal = Decimal("0.9")  # 90% max utilization
    protocol_margins: dict[str, dict[str, Decimal]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Initialize protocol-specific margin requirements."""
        if not self.protocol_margins:
            self.protocol_margins = {
                "gmx": {
                    "initial": Decimal("0.01"),  # 1% = 100x max leverage
                    "maintenance": Decimal("0.01"),
                },
                "gmx_v2": {
                    "initial": Decimal("0.01"),
                    "maintenance": Decimal("0.01"),
                },
                "hyperliquid": {
                    "initial": Decimal("0.01"),  # 1% = 100x max leverage
                    "maintenance": Decimal("0.005"),
                },
                "binance_perp": {
                    "initial": Decimal("0.05"),  # 5% = 20x max leverage
                    "maintenance": Decimal("0.04"),
                },
                "bybit": {
                    "initial": Decimal("0.05"),
                    "maintenance": Decimal("0.05"),
                },
                "dydx": {
                    "initial": Decimal("0.05"),
                    "maintenance": Decimal("0.03"),
                },
            }

    def validate_margin(
        self,
        position_size: Decimal,
        collateral: Decimal,
        margin_ratio: Decimal | None = None,
    ) -> MarginValidationResult:
        """Validate that margin requirements are met for a position.

        This is the core validation function that checks if the provided
        collateral is sufficient for the given position size.

        Formula:
            is_valid = collateral / position_size >= required_margin_ratio

        Args:
            position_size: The notional size of the position in USD
            collateral: The collateral amount in USD
            margin_ratio: Required margin ratio (default: self.default_initial_margin_ratio)

        Returns:
            MarginValidationResult with validation details

        Raises:
            ValueError: If position_size or collateral is negative

        Example:
            result = validator.validate_margin(
                position_size=Decimal("10000"),
                collateral=Decimal("1000"),
                margin_ratio=Decimal("0.1"),
            )
            if result.is_valid:
                # Proceed with opening position
                pass
            else:
                # Reject position, need more collateral
                print(f"Need ${result.shortfall} more collateral")
        """
        if position_size < Decimal("0"):
            raise ValueError(f"position_size cannot be negative, got {position_size}")
        if collateral < Decimal("0"):
            raise ValueError(f"collateral cannot be negative, got {collateral}")

        required_ratio = margin_ratio or self.default_initial_margin_ratio

        # Handle edge cases
        if position_size == Decimal("0"):
            return MarginValidationResult(
                is_valid=True,
                position_size=position_size,
                collateral=collateral,
                required_margin_ratio=required_ratio,
                actual_margin_ratio=Decimal("1"),  # No position = 100% margin
                shortfall=Decimal("0"),
                message="No position to validate",
            )

        # Calculate actual margin ratio
        actual_ratio = collateral / position_size

        # Check if margin requirement is met
        is_valid = actual_ratio >= required_ratio

        # Calculate shortfall if invalid
        shortfall = Decimal("0")
        if not is_valid:
            required_collateral = position_size * required_ratio
            shortfall = required_collateral - collateral

        # Generate message
        if is_valid:
            message = f"Margin requirement met: {actual_ratio * 100:.2f}% >= {required_ratio * 100:.2f}% required"
        else:
            message = (
                f"Insufficient margin: {actual_ratio * 100:.2f}% < "
                f"{required_ratio * 100:.2f}% required. "
                f"Need ${shortfall:.2f} more collateral"
            )

        return MarginValidationResult(
            is_valid=is_valid,
            position_size=position_size,
            collateral=collateral,
            required_margin_ratio=required_ratio,
            actual_margin_ratio=actual_ratio,
            shortfall=shortfall,
            message=message,
        )

    def validate_position_increase(
        self,
        current_position_size: Decimal,
        current_collateral: Decimal,
        additional_size: Decimal,
        additional_collateral: Decimal,
        margin_ratio: Decimal | None = None,
    ) -> MarginValidationResult:
        """Validate margin for a position size increase.

        This checks if adding to an existing position maintains
        sufficient margin requirements.

        Args:
            current_position_size: Current notional position size
            current_collateral: Current collateral amount
            additional_size: Amount to add to position
            additional_collateral: Additional collateral being added
            margin_ratio: Required margin ratio (default: self.default_initial_margin_ratio)

        Returns:
            MarginValidationResult for the combined position

        Example:
            # Can I add $5000 to my position with $500 more collateral?
            result = validator.validate_position_increase(
                current_position_size=Decimal("10000"),
                current_collateral=Decimal("1000"),
                additional_size=Decimal("5000"),
                additional_collateral=Decimal("500"),
            )
        """
        new_position_size = current_position_size + additional_size
        new_collateral = current_collateral + additional_collateral

        return self.validate_margin(
            position_size=new_position_size,
            collateral=new_collateral,
            margin_ratio=margin_ratio,
        )

    def check_sufficient_collateral(
        self,
        position_size: Decimal,
        collateral: Decimal,
        margin_ratio: Decimal | None = None,
        log_warning: bool = True,
    ) -> bool:
        """Check if collateral is sufficient and optionally log a warning.

        Convenience method for simple yes/no margin checks with optional logging.

        Args:
            position_size: The notional size of the position in USD
            collateral: The collateral amount in USD
            margin_ratio: Required margin ratio (default: self.default_initial_margin_ratio)
            log_warning: If True and insufficient, log a warning

        Returns:
            True if margin is sufficient, False otherwise

        Example:
            if not validator.check_sufficient_collateral(
                position_size=Decimal("10000"),
                collateral=Decimal("500"),
            ):
                return  # Reject position
        """
        result = self.validate_margin(position_size, collateral, margin_ratio)

        if not result.is_valid and log_warning:
            logger.warning(f"Insufficient margin for position: {result.message}")

        return result.is_valid

    def calculate_margin_utilization(
        self,
        total_margin_used: Decimal,
        total_notional: Decimal,
        available_capital: Decimal,
    ) -> MarginUtilization:
        """Calculate current margin utilization.

        Margin utilization shows how much of the available capital is
        currently locked as collateral for perp positions.

        Formula:
            utilization_ratio = total_margin_used / (total_margin_used + available_capital)

        Args:
            total_margin_used: Total collateral locked in perp positions
            total_notional: Total notional value of all perp positions
            available_capital: Cash available for new positions

        Returns:
            MarginUtilization with current state

        Example:
            utilization = validator.calculate_margin_utilization(
                total_margin_used=Decimal("5000"),
                total_notional=Decimal("50000"),
                available_capital=Decimal("5000"),
            )
            print(f"Margin utilization: {utilization.utilization_ratio * 100:.1f}%")
            # "Margin utilization: 50.0%"
        """
        total_capital = total_margin_used + available_capital

        if total_capital == Decimal("0"):
            utilization_ratio = Decimal("0")
        else:
            utilization_ratio = total_margin_used / total_capital

        return MarginUtilization(
            total_margin_used=total_margin_used,
            total_notional=total_notional,
            available_capital=available_capital,
            utilization_ratio=utilization_ratio,
        )

    def can_open_position(
        self,
        position_size: Decimal,
        collateral: Decimal,
        available_capital: Decimal,
        current_margin_used: Decimal = Decimal("0"),
        margin_ratio: Decimal | None = None,
    ) -> tuple[bool, str]:
        """Check if a new position can be opened given capital constraints.

        This combines margin validation with utilization checks to determine
        if opening a position is allowed.

        Checks:
        1. Is collateral sufficient for the position size?
        2. Is there enough available capital for the collateral?
        3. Would this exceed maximum margin utilization?

        Args:
            position_size: Notional size of proposed position
            collateral: Collateral required for the position
            available_capital: Cash available for margin
            current_margin_used: Margin already locked in other positions
            margin_ratio: Required margin ratio

        Returns:
            Tuple of (can_open: bool, reason: str)

        Example:
            can_open, reason = validator.can_open_position(
                position_size=Decimal("10000"),
                collateral=Decimal("1000"),
                available_capital=Decimal("2000"),
                current_margin_used=Decimal("1500"),
            )
            if not can_open:
                print(f"Cannot open position: {reason}")
        """
        required_ratio = margin_ratio or self.default_initial_margin_ratio

        # Check 1: Is collateral sufficient for position size?
        margin_result = self.validate_margin(position_size, collateral, required_ratio)
        if not margin_result.is_valid:
            return False, margin_result.message

        # Check 2: Is there enough available capital?
        if collateral > available_capital:
            return False, (
                f"Insufficient available capital: need ${collateral:.2f} but only ${available_capital:.2f} available"
            )

        # Check 3: Would this exceed max utilization?
        new_margin_used = current_margin_used + collateral
        new_available = available_capital - collateral
        utilization = self.calculate_margin_utilization(
            total_margin_used=new_margin_used,
            total_notional=position_size,  # Simplified - doesn't include other positions
            available_capital=new_available,
        )

        if utilization.utilization_ratio > self.max_margin_utilization_ratio:
            return False, (
                f"Would exceed max margin utilization: "
                f"{utilization.utilization_ratio * 100:.1f}% > "
                f"{self.max_margin_utilization_ratio * 100:.1f}% max"
            )

        return True, "Position can be opened"

    def get_margin_for_protocol(self, protocol: str) -> dict[str, Decimal]:
        """Get margin requirements for a specific protocol.

        Args:
            protocol: Protocol name (e.g., "gmx", "hyperliquid")

        Returns:
            Dict with "initial" and "maintenance" margin ratios
        """
        default = {
            "initial": self.default_initial_margin_ratio,
            "maintenance": self.default_maintenance_margin_ratio,
        }
        return self.protocol_margins.get(protocol.lower(), default)

    def get_max_leverage_for_margin(self, margin_ratio: Decimal) -> Decimal:
        """Calculate maximum leverage for a given margin ratio.

        Args:
            margin_ratio: The margin ratio (e.g., 0.1 = 10%)

        Returns:
            Maximum leverage (e.g., 10 for 10% margin)

        Example:
            max_leverage = validator.get_max_leverage_for_margin(Decimal("0.1"))
            # max_leverage = 10
        """
        if margin_ratio <= Decimal("0"):
            raise ValueError("margin_ratio must be greater than 0")
        return Decimal("1") / margin_ratio

    def get_required_collateral(
        self,
        position_size: Decimal,
        margin_ratio: Decimal | None = None,
    ) -> Decimal:
        """Calculate required collateral for a position size.

        Args:
            position_size: The notional size of the position
            margin_ratio: The margin ratio (default: self.default_initial_margin_ratio)

        Returns:
            Required collateral amount

        Example:
            collateral = validator.get_required_collateral(
                position_size=Decimal("10000"),
                margin_ratio=Decimal("0.1"),
            )
            # collateral = 1000
        """
        required_ratio = margin_ratio or self.default_initial_margin_ratio
        return position_size * required_ratio

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "validator_name": "margin",
            "default_initial_margin_ratio": str(self.default_initial_margin_ratio),
            "default_maintenance_margin_ratio": str(self.default_maintenance_margin_ratio),
            "max_margin_utilization_ratio": str(self.max_margin_utilization_ratio),
            "protocol_margins": {k: {mk: str(mv) for mk, mv in v.items()} for k, v in self.protocol_margins.items()},
        }


__all__ = [
    "MarginValidator",
    "MarginValidationResult",
    "MarginUtilization",
]
