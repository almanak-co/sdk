"""Liquidation price calculator for perpetual futures positions.

This module provides tools for calculating liquidation prices for perpetual futures
positions during backtesting. Liquidation occurs when a position's losses consume
the collateral beyond the maintenance margin threshold.

Key Concepts:
    - Liquidation Price: The price at which a position would be liquidated
    - Maintenance Margin: Minimum collateral required to keep position open
    - Entry Price: The price at which the position was opened
    - Leverage: Position size multiplier (e.g., 5x = 5)

Liquidation Price Formulas:
    For LONG positions:
        liq_price = entry_price * (1 - (1 / leverage) + maintenance_margin)

    For SHORT positions:
        liq_price = entry_price * (1 + (1 / leverage) - maintenance_margin)

Example:
    from almanak.framework.backtesting.pnl.calculators.liquidation import (
        LiquidationCalculator,
    )

    calculator = LiquidationCalculator()

    # Calculate liquidation price for a 5x long position
    liq_price = calculator.calculate_liquidation_price(
        entry_price=Decimal("2000"),
        leverage=Decimal("5"),
        maintenance_margin=Decimal("0.05"),  # 5%
        is_long=True,
    )
    # liq_price = 2000 * (1 - 0.2 + 0.05) = 2000 * 0.85 = $1700

References:
    - GMX V2 Liquidation: https://docs.gmx.io/docs/trading/v2#liquidation
    - Hyperliquid: https://hyperliquid.gitbook.io/hyperliquid-docs
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.framework.backtesting.pnl.portfolio import PositionType, SimulatedPosition

logger = logging.getLogger(__name__)


@dataclass
class LiquidationWarning:
    """Warning when price approaches liquidation level.

    Attributes:
        position_id: ID of the position at risk
        current_price: Current market price
        liquidation_price: Price at which liquidation occurs
        distance_pct: Percentage distance from liquidation (e.g., 0.10 = 10%)
        is_critical: True if within critical threshold (default 5%)
    """

    position_id: str
    current_price: Decimal
    liquidation_price: Decimal
    distance_pct: Decimal
    is_critical: bool

    @property
    def message(self) -> str:
        """Generate a warning message."""
        severity = "CRITICAL" if self.is_critical else "WARNING"
        return (
            f"[{severity}] Position {self.position_id} is {self.distance_pct * 100:.1f}% "
            f"from liquidation. Current: ${self.current_price}, Liquidation: ${self.liquidation_price}"
        )


@dataclass
class LiquidationCalculator:
    """Calculator for liquidation prices on perpetual positions.

    This class provides the primary API for calculating liquidation prices
    during backtesting and monitoring positions for liquidation risk.

    The liquidation price formula:
        LONG: liq_price = entry_price * (1 - (1 / leverage) + maintenance_margin)
        SHORT: liq_price = entry_price * (1 + (1 / leverage) - maintenance_margin)

    The formula accounts for:
        - Initial margin = 1 / leverage (e.g., 5x leverage = 20% margin)
        - Maintenance margin = minimum required to keep position open
        - Position direction (long vs short)

    Attributes:
        default_maintenance_margin: Default maintenance margin ratio (default 0.05 = 5%)
        warning_threshold: Distance from liquidation to emit warning (default 0.10 = 10%)
        critical_threshold: Distance from liquidation for critical warning (default 0.05 = 5%)
        protocol_margins: Protocol-specific maintenance margin rates

    Example:
        calculator = LiquidationCalculator()

        # Calculate liquidation price for a 10x long
        liq_price = calculator.calculate_liquidation_price(
            entry_price=Decimal("2000"),
            leverage=Decimal("10"),
            maintenance_margin=Decimal("0.05"),
            is_long=True,
        )
        # liq_price = 2000 * (1 - 0.1 + 0.05) = 2000 * 0.95 = $1900

        # Check if current price is near liquidation
        warning = calculator.check_liquidation_proximity(
            position=position,
            current_price=Decimal("1950"),
        )
        if warning:
            print(warning.message)  # Emits warning if within threshold
    """

    default_maintenance_margin: Decimal = Decimal("0.05")  # 5%
    warning_threshold: Decimal = Decimal("0.10")  # Warn at 10% from liquidation
    critical_threshold: Decimal = Decimal("0.05")  # Critical at 5% from liquidation
    protocol_margins: dict[str, Decimal] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Initialize protocol-specific maintenance margins."""
        if not self.protocol_margins:
            self.protocol_margins = {
                "gmx": Decimal("0.01"),  # 1% maintenance margin
                "gmx_v2": Decimal("0.01"),
                "hyperliquid": Decimal("0.005"),  # 0.5% maintenance margin
                "binance_perp": Decimal("0.04"),  # 4% maintenance margin
                "bybit": Decimal("0.05"),  # 5% maintenance margin
                "dydx": Decimal("0.03"),  # 3% maintenance margin
            }

    def calculate_liquidation_price(
        self,
        entry_price: Decimal,
        leverage: Decimal,
        maintenance_margin: Decimal,
        is_long: bool,
    ) -> Decimal:
        """Calculate the liquidation price for a perpetual position.

        The liquidation price is calculated based on the entry price, leverage,
        and maintenance margin requirement. This is the price at which the
        position would be forcibly closed to prevent further losses.

        Formula:
            LONG: liq_price = entry_price * (1 - (1 / leverage) + maintenance_margin)
            SHORT: liq_price = entry_price * (1 + (1 / leverage) - maintenance_margin)

        Args:
            entry_price: The price at which the position was opened
            leverage: The leverage multiplier (e.g., 5 for 5x leverage)
            maintenance_margin: The maintenance margin ratio (e.g., 0.05 for 5%)
            is_long: True for long position, False for short position

        Returns:
            The liquidation price in the same currency as entry_price

        Raises:
            ValueError: If leverage is less than or equal to 0

        Example:
            # 5x long position with 5% maintenance margin
            liq_price = calc.calculate_liquidation_price(
                entry_price=Decimal("2000"),
                leverage=Decimal("5"),
                maintenance_margin=Decimal("0.05"),
                is_long=True,
            )
            # liq_price = 2000 * (1 - 0.2 + 0.05) = 2000 * 0.85 = $1700

            # 10x short position with 5% maintenance margin
            liq_price = calc.calculate_liquidation_price(
                entry_price=Decimal("2000"),
                leverage=Decimal("10"),
                maintenance_margin=Decimal("0.05"),
                is_long=False,
            )
            # liq_price = 2000 * (1 + 0.1 - 0.05) = 2000 * 1.05 = $2100
        """
        if leverage <= Decimal("0"):
            raise ValueError(f"Leverage must be greater than 0, got {leverage}")

        # Initial margin ratio = 1 / leverage
        initial_margin_ratio = Decimal("1") / leverage

        if is_long:
            # Long liquidation: price falls below this level
            # Loss = entry_price - current_price (as % of entry)
            # Liquidation when loss = initial_margin - maintenance_margin
            # So: (entry - liq) / entry = 1/leverage - maintenance
            # liq = entry * (1 - 1/leverage + maintenance)
            liquidation_price = entry_price * (Decimal("1") - initial_margin_ratio + maintenance_margin)
        else:
            # Short liquidation: price rises above this level
            # Profit when price falls, loss when price rises
            # Loss = current_price - entry_price (as % of entry)
            # Liquidation when loss = initial_margin - maintenance_margin
            # So: (liq - entry) / entry = 1/leverage - maintenance
            # liq = entry * (1 + 1/leverage - maintenance)
            liquidation_price = entry_price * (Decimal("1") + initial_margin_ratio - maintenance_margin)

        return liquidation_price

    def calculate_liquidation_price_for_position(
        self,
        position: SimulatedPosition,
        maintenance_margin: Decimal | None = None,
    ) -> Decimal | None:
        """Calculate the liquidation price for a SimulatedPosition.

        Convenience method that extracts the necessary parameters from
        a SimulatedPosition object.

        Args:
            position: The perpetual position (PERP_LONG or PERP_SHORT)
            maintenance_margin: Override maintenance margin (uses protocol default if None)

        Returns:
            The liquidation price, or None if not a perpetual position

        Example:
            liq_price = calc.calculate_liquidation_price_for_position(
                position=perp_long_position,
            )
        """
        if position.position_type not in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
            return None

        # Use provided margin or get protocol default
        margin = maintenance_margin or self.get_maintenance_margin_for_protocol(position.protocol)

        is_long = position.position_type == PositionType.PERP_LONG

        return self.calculate_liquidation_price(
            entry_price=position.entry_price,
            leverage=position.leverage,
            maintenance_margin=margin,
            is_long=is_long,
        )

    def update_position_liquidation_price(
        self,
        position: SimulatedPosition,
        maintenance_margin: Decimal | None = None,
    ) -> None:
        """Update a position's liquidation_price field.

        This method should be called whenever position parameters change
        (size, collateral, leverage) to keep the liquidation price current.

        Args:
            position: The position to update (modified in place)
            maintenance_margin: Override maintenance margin (uses protocol default if None)

        Example:
            # After position parameters change
            calc.update_position_liquidation_price(position)
            # position.liquidation_price is now updated
        """
        liq_price = self.calculate_liquidation_price_for_position(position, maintenance_margin)
        if liq_price is not None:
            position.liquidation_price = liq_price

    def check_liquidation_proximity(
        self,
        position: SimulatedPosition,
        current_price: Decimal,
        warning_threshold: Decimal | None = None,
        critical_threshold: Decimal | None = None,
        emit_warning: bool = True,
    ) -> LiquidationWarning | None:
        """Check if the current price is within a configurable % of liquidation.

        This method monitors positions and emits warnings when they approach
        liquidation levels. It calculates the distance from liquidation and
        optionally logs a warning.

        Args:
            position: The perpetual position to check
            current_price: The current market price
            warning_threshold: % distance from liquidation to warn (default: self.warning_threshold)
            critical_threshold: % distance for critical warning (default: self.critical_threshold)
            emit_warning: If True, log a warning message (default: True)

        Returns:
            LiquidationWarning if within threshold, None otherwise

        Example:
            warning = calc.check_liquidation_proximity(
                position=long_position,
                current_price=Decimal("1750"),  # Close to $1700 liquidation
            )
            if warning:
                print(warning.message)  # "[WARNING] Position ... is 2.9% from liquidation"
        """
        # Get liquidation price
        liq_price = position.liquidation_price
        if liq_price is None:
            # Calculate if not set
            liq_price = self.calculate_liquidation_price_for_position(position)
            if liq_price is None:
                return None

        # Use provided thresholds or defaults
        warn_thresh = warning_threshold or self.warning_threshold
        crit_thresh = critical_threshold or self.critical_threshold

        # Calculate distance to liquidation
        is_long = position.position_type == PositionType.PERP_LONG

        if is_long:
            # For longs, liquidation is below current price
            # Distance = (current - liq) / current
            if current_price <= Decimal("0"):
                return None
            distance = (current_price - liq_price) / current_price
        else:
            # For shorts, liquidation is above current price
            # Distance = (liq - current) / current
            if current_price <= Decimal("0"):
                return None
            distance = (liq_price - current_price) / current_price

        # Check if within warning threshold
        if distance <= warn_thresh:
            is_critical = distance <= crit_thresh
            warning = LiquidationWarning(
                position_id=position.position_id,
                current_price=current_price,
                liquidation_price=liq_price,
                distance_pct=distance,
                is_critical=is_critical,
            )

            # Emit warning log if requested
            if emit_warning:
                if is_critical:
                    logger.warning(warning.message)
                else:
                    logger.info(warning.message)

            return warning

        return None

    def get_maintenance_margin_for_protocol(self, protocol: str) -> Decimal:
        """Get the maintenance margin for a specific protocol.

        Args:
            protocol: Protocol name (e.g., "gmx", "hyperliquid")

        Returns:
            The protocol's maintenance margin, or default if not found
        """
        return self.protocol_margins.get(protocol.lower(), self.default_maintenance_margin)

    def estimate_safe_leverage(
        self,
        entry_price: Decimal,
        stop_loss_price: Decimal,
        maintenance_margin: Decimal | None = None,
        is_long: bool = True,
        safety_buffer: Decimal = Decimal("0.02"),
    ) -> Decimal:
        """Estimate the maximum safe leverage given a stop loss price.

        This helps traders determine the maximum leverage they can use
        while ensuring their stop loss triggers before liquidation.

        Args:
            entry_price: The intended entry price
            stop_loss_price: The price at which to exit
            maintenance_margin: Maintenance margin ratio (default: 5%)
            is_long: True for long position
            safety_buffer: Extra buffer above liquidation (default 2%)

        Returns:
            Maximum leverage that keeps liquidation below stop loss

        Example:
            # What leverage can I use with a 10% stop loss?
            max_leverage = calc.estimate_safe_leverage(
                entry_price=Decimal("2000"),
                stop_loss_price=Decimal("1800"),  # 10% below entry
                is_long=True,
            )
            # max_leverage ≈ 8x (liquidation at ~$1750, below $1800 stop)
        """
        margin = maintenance_margin or self.default_maintenance_margin

        if is_long:
            # For long: liq_price should be below stop_loss
            # We want: entry * (1 - 1/lev + margin) < stop_loss - buffer * entry
            # Solving for leverage:
            # 1 - 1/lev + margin < (stop - buffer * entry) / entry
            # 1 + margin - stop/entry + buffer < 1/lev
            # lev < 1 / (1 + margin - stop/entry + buffer)
            if stop_loss_price >= entry_price:
                # Stop loss above entry doesn't make sense for long
                return Decimal("1")

            price_ratio = stop_loss_price / entry_price
            denominator = Decimal("1") + margin - price_ratio + safety_buffer
            if denominator <= Decimal("0"):
                return Decimal("100")  # Very high leverage possible
            max_leverage = Decimal("1") / denominator
        else:
            # For short: liq_price should be above stop_loss
            # We want: entry * (1 + 1/lev - margin) > stop_loss + buffer * entry
            # Solving for leverage:
            # 1 + 1/lev - margin > stop/entry + buffer
            # 1/lev > stop/entry + buffer - 1 + margin
            # lev < 1 / (stop/entry + buffer - 1 + margin)
            if stop_loss_price <= entry_price:
                # Stop loss below entry doesn't make sense for short
                return Decimal("1")

            price_ratio = stop_loss_price / entry_price
            denominator = price_ratio + safety_buffer - Decimal("1") + margin
            if denominator <= Decimal("0"):
                return Decimal("100")  # Very high leverage possible
            max_leverage = Decimal("1") / denominator

        # Ensure at least 1x leverage
        return max(Decimal("1"), max_leverage)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "calculator_name": "liquidation",
            "default_maintenance_margin": str(self.default_maintenance_margin),
            "warning_threshold": str(self.warning_threshold),
            "critical_threshold": str(self.critical_threshold),
            "protocol_margins": {k: str(v) for k, v in self.protocol_margins.items()},
        }


__all__ = [
    "LiquidationCalculator",
    "LiquidationWarning",
]
