"""Funding rate handler for perpetual futures positions.

This module provides tools for calculating funding payments for perpetual futures
positions during backtesting. Funding rates are the mechanism that keeps perpetual
futures prices anchored to spot prices.

Key Concepts:
    - Funding Rate: Periodic payment between longs and shorts
    - Funding Index: Cumulative sum of funding rates over time
    - Funding Payment: The actual USD amount paid/received based on position size

How Funding Works:
    - When funding rate is positive: Longs pay shorts (bullish market)
    - When funding rate is negative: Shorts pay longs (bearish market)
    - Payment = position_value * (current_funding_index - entry_funding_index)

Example:
    from almanak.framework.backtesting.pnl.calculators.funding import (
        FundingRateHandler,
        FundingCalculator,
    )

    handler = FundingRateHandler()

    # Calculate funding payment for a position
    payment = handler.calculate_funding_payment(
        position=perp_long_position,
        current_funding_index=Decimal("0.0015"),
        position_value_usd=Decimal("10000"),
    )
    # Positive payment = received, Negative payment = paid

    # Using FundingCalculator with time_delta-based calculation
    calculator = FundingCalculator()
    result = calculator.calculate_funding_payment(
        position=perp_long_position,
        funding_rate=Decimal("0.0001"),  # 0.01% per hour
        time_delta_hours=Decimal("24"),  # 24 hours
    )
    # result.payment = funding amount (positive = received, negative = paid)

References:
    - GMX V2 Funding: https://docs.gmx.io/docs/trading/v2#funding-fees
    - Perpetual Protocol: https://docs.perp.com/docs/concepts/funding-payments
"""

from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

from almanak.framework.backtesting.pnl.portfolio import PositionType, SimulatedPosition


class FundingRateSource(StrEnum):
    """Source for funding rate data.

    Attributes:
        FIXED: Use a fixed funding rate (default, good for backtesting)
        HISTORICAL: Use historical funding rates from data provider
        PROTOCOL: Use protocol-specific rates (requires API access)
        SIMULATED: Simulate funding rates based on market conditions
    """

    FIXED = "fixed"
    HISTORICAL = "historical"
    PROTOCOL = "protocol"
    SIMULATED = "simulated"


@dataclass
class FundingPaymentResult:
    """Result of a funding payment calculation.

    Attributes:
        payment: The funding payment amount in USD (positive = received, negative = paid)
        funding_rate: The funding rate used for calculation
        position_value_usd: The notional position value used
        time_hours: The time period in hours
        is_payer: True if this position paid funding, False if received
    """

    payment: Decimal
    funding_rate: Decimal
    position_value_usd: Decimal
    time_hours: Decimal
    is_payer: bool

    @property
    def abs_payment(self) -> Decimal:
        """Get absolute value of payment."""
        return abs(self.payment)


@dataclass
class FundingRateHandler:
    """Handler for calculating funding payments on perpetual positions.

    This handler implements funding rate logic for perpetual futures positions.
    Funding payments are based on the difference between the current funding
    index and the position's entry funding index.

    The funding index is a cumulative sum of funding rates. For a position:
    - funding_payment = position_value * (current_index - entry_index)
    - For PERP_LONG: positive funding rate means you PAY (negative payment)
    - For PERP_SHORT: positive funding rate means you RECEIVE (positive payment)

    Attributes:
        default_funding_rate: Default hourly funding rate for simulation (default 0.0001 = 0.01%)
        funding_interval_hours: How often funding is applied (default 1 hour for GMX, 8 hours for others)
        max_funding_rate: Maximum absolute funding rate cap (default 0.01 = 1%)

    Example:
        handler = FundingRateHandler()

        # For a long position when funding rate is positive
        # Longs pay shorts, so payment is negative
        payment = handler.calculate_funding_payment(
            position=long_position,
            current_funding_index=Decimal("0.002"),
            position_value_usd=Decimal("50000"),
        )
        # payment = -50000 * (0.002 - 0.001) = -$50 (paid)

        # For a short position when funding rate is positive
        # Shorts receive, so payment is positive
        payment = handler.calculate_funding_payment(
            position=short_position,
            current_funding_index=Decimal("0.002"),
            position_value_usd=Decimal("50000"),
        )
        # payment = +50000 * (0.002 - 0.001) = +$50 (received)
    """

    default_funding_rate: Decimal = Decimal("0.0001")  # 0.01% per hour
    funding_interval_hours: int = 1  # GMX uses hourly funding
    max_funding_rate: Decimal = Decimal("0.01")  # 1% max per interval
    protocol_configs: dict[str, dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Initialize protocol-specific configurations."""
        # Default protocol configurations
        if not self.protocol_configs:
            self.protocol_configs = {
                "gmx": {
                    "funding_interval_hours": 1,
                    "max_funding_rate": Decimal("0.01"),
                },
                "hyperliquid": {
                    "funding_interval_hours": 1,
                    "max_funding_rate": Decimal("0.01"),
                },
                "binance_perp": {
                    "funding_interval_hours": 8,
                    "max_funding_rate": Decimal("0.03"),
                },
            }

    def calculate_funding_payment(
        self,
        position: SimulatedPosition,
        current_funding_index: Decimal,
        position_value_usd: Decimal,
    ) -> Decimal:
        """Calculate funding payment for a perpetual position.

        Computes the funding payment based on the difference between the current
        funding index and the position's entry funding index.

        The sign convention:
        - PERP_LONG with positive index change: PAYS funding (returns negative)
        - PERP_LONG with negative index change: RECEIVES funding (returns positive)
        - PERP_SHORT with positive index change: RECEIVES funding (returns positive)
        - PERP_SHORT with negative index change: PAYS funding (returns negative)

        This follows the standard perpetual futures convention where:
        - Positive funding rate = longs pay shorts (market is bullish)
        - Negative funding rate = shorts pay longs (market is bearish)

        Args:
            position: The perpetual position (PERP_LONG or PERP_SHORT)
            current_funding_index: The current cumulative funding index
            position_value_usd: The notional value of the position in USD
                (typically position.notional_usd)

        Returns:
            Funding payment in USD:
            - Positive value = received (profit)
            - Negative value = paid (cost)

        Raises:
            ValueError: If position is not a perpetual position

        Example:
            # Long position, funding index increased (longs pay shorts)
            long = SimulatedPosition.perp_long(
                token="ETH",
                collateral_usd=Decimal("10000"),
                leverage=Decimal("5"),
                entry_price=Decimal("2000"),
                entry_time=datetime.now(timezone.utc),
                entry_funding_index=Decimal("0.001"),
            )
            payment = handler.calculate_funding_payment(
                position=long,
                current_funding_index=Decimal("0.002"),
                position_value_usd=Decimal("50000"),
            )
            # payment = -$50 (longs pay when index increases)

            # Short position, funding index increased (shorts receive)
            short = SimulatedPosition.perp_short(...)
            payment = handler.calculate_funding_payment(
                position=short,
                current_funding_index=Decimal("0.002"),
                position_value_usd=Decimal("50000"),
            )
            # payment = +$50 (shorts receive when index increases)
        """
        # Validate position type
        if position.position_type not in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
            raise ValueError(f"Position must be PERP_LONG or PERP_SHORT, got {position.position_type}")

        # Calculate the funding index change
        index_change = current_funding_index - position.entry_funding_index

        # Calculate raw funding payment based on notional value
        # funding_payment = position_value * index_change
        raw_payment = position_value_usd * index_change

        # Apply sign convention based on position type
        # PERP_LONG: pays when index is positive (funding rate positive = longs pay)
        # PERP_SHORT: receives when index is positive (funding rate positive = shorts receive)
        if position.position_type == PositionType.PERP_LONG:
            # Longs pay when funding index increases (positive index change = negative payment)
            funding_payment = -raw_payment
        else:
            # Shorts receive when funding index increases (positive index change = positive payment)
            funding_payment = raw_payment

        return funding_payment

    def calculate_accumulated_funding(
        self,
        position: SimulatedPosition,
        current_funding_index: Decimal,
    ) -> Decimal:
        """Calculate total accumulated funding including any previously accumulated.

        This method is useful for updating a position's accumulated_funding field
        during mark-to-market operations.

        Args:
            position: The perpetual position
            current_funding_index: The current cumulative funding index

        Returns:
            Total accumulated funding (position's existing + new payment)
        """
        # Use position's notional value
        position_value_usd = position.notional_usd

        # Calculate new funding payment
        new_payment = self.calculate_funding_payment(
            position=position,
            current_funding_index=current_funding_index,
            position_value_usd=position_value_usd,
        )

        # Add to existing accumulated funding
        return position.accumulated_funding + new_payment

    def estimate_funding_for_period(
        self,
        position: SimulatedPosition,
        hours: int,
        funding_rate: Decimal | None = None,
    ) -> Decimal:
        """Estimate funding payment over a period using a constant funding rate.

        Useful for projecting funding costs when planning to hold a position
        for a certain duration.

        Args:
            position: The perpetual position
            hours: Number of hours to estimate funding for
            funding_rate: Hourly funding rate (uses default if not provided)

        Returns:
            Estimated funding payment in USD over the period

        Example:
            # Estimate 24 hours of funding for a $50,000 long position
            # at 0.01% hourly funding rate
            est = handler.estimate_funding_for_period(
                position=long_position,
                hours=24,
                funding_rate=Decimal("0.0001"),
            )
            # est = -50000 * 0.0001 * 24 = -$120 (long pays ~$120 over 24h)
        """
        # Validate position type
        if position.position_type not in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
            raise ValueError(f"Position must be PERP_LONG or PERP_SHORT, got {position.position_type}")

        rate = funding_rate if funding_rate is not None else self.default_funding_rate

        # Calculate total index change over the period
        # Assuming constant funding rate
        total_index_change = rate * Decimal(hours)

        # Calculate raw funding amount
        raw_funding = position.notional_usd * total_index_change

        # Apply sign convention
        if position.position_type == PositionType.PERP_LONG:
            return -raw_funding  # Longs pay when rate is positive
        else:
            return raw_funding  # Shorts receive when rate is positive

    def get_funding_rate_for_protocol(
        self,
        protocol: str,
        key: str = "default_funding_rate",
    ) -> Decimal:
        """Get a configuration value for a specific protocol.

        Args:
            protocol: Protocol name (e.g., "gmx", "hyperliquid")
            key: Configuration key to retrieve

        Returns:
            The configuration value, or default if not found
        """
        protocol_config = self.protocol_configs.get(protocol.lower(), {})
        value = protocol_config.get(key, getattr(self, key, Decimal("0")))
        return Decimal(str(value)) if not isinstance(value, Decimal) else value

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "handler_name": "funding_rate",
            "default_funding_rate": str(self.default_funding_rate),
            "funding_interval_hours": self.funding_interval_hours,
            "max_funding_rate": str(self.max_funding_rate),
            "protocol_configs": {
                k: {kk: str(vv) if isinstance(vv, Decimal) else vv for kk, vv in v.items()}
                for k, v in self.protocol_configs.items()
            },
        }


@dataclass
class FundingCalculator:
    """Calculator for funding payments on perpetual positions.

    This class provides the primary API for calculating funding payments
    during backtesting. It supports configurable funding rate sources
    and properly tracks cumulative funding paid/received.

    Funding is calculated as:
        payment = position_value * funding_rate * time_hours

    The sign convention:
        - PERP_LONG with positive funding rate: PAYS (negative payment)
        - PERP_LONG with negative funding rate: RECEIVES (positive payment)
        - PERP_SHORT with positive funding rate: RECEIVES (positive payment)
        - PERP_SHORT with negative funding rate: PAYS (negative payment)

    Attributes:
        funding_rate_source: Source of funding rate data (default FIXED)
        default_funding_rate: Default hourly funding rate (default 0.0001 = 0.01%)
        protocol_rates: Protocol-specific default rates
        min_funding_rate: Minimum funding rate floor
        max_funding_rate: Maximum funding rate cap

    Example:
        calculator = FundingCalculator()

        # Calculate funding for a 24-hour period
        result = calculator.calculate_funding_payment(
            position=long_position,
            funding_rate=Decimal("0.0001"),
            time_delta_hours=Decimal("24"),
        )
        print(f"Funding payment: ${result.payment}")  # Negative for long paying

        # Apply funding to position and update cumulative fields
        calculator.apply_funding_to_position(position, result)
    """

    funding_rate_source: FundingRateSource = FundingRateSource.FIXED
    default_funding_rate: Decimal = Decimal("0.0001")  # 0.01% per hour
    protocol_rates: dict[str, Decimal] = field(default_factory=dict)
    min_funding_rate: Decimal = Decimal("-0.01")  # -1% per hour (shorts pay longs)
    max_funding_rate: Decimal = Decimal("0.01")  # +1% per hour (longs pay shorts)

    def __post_init__(self) -> None:
        """Initialize protocol-specific funding rates."""
        if not self.protocol_rates:
            self.protocol_rates = {
                "gmx": Decimal("0.0001"),  # 0.01% per hour
                "gmx_v2": Decimal("0.0001"),
                "hyperliquid": Decimal("0.0001"),
                "binance_perp": Decimal("0.000125"),  # 0.0125% per hour (~0.1% per 8h)
                "bybit": Decimal("0.000125"),
                "dydx": Decimal("0.0001"),
            }

    def calculate_funding_payment(
        self,
        position: SimulatedPosition,
        funding_rate: Decimal,
        time_delta_hours: Decimal | timedelta,
    ) -> FundingPaymentResult:
        """Calculate funding payment for a perpetual position over a time period.

        This is the primary method for calculating funding during mark_to_market.
        It computes the funding payment based on the funding rate and time elapsed.

        Args:
            position: The perpetual position (PERP_LONG or PERP_SHORT)
            funding_rate: The hourly funding rate (positive = longs pay, negative = shorts pay)
            time_delta_hours: Time period in hours (Decimal) or as timedelta

        Returns:
            FundingPaymentResult with payment amount and metadata

        Raises:
            ValueError: If position is not a perpetual position

        Example:
            # Long position with 0.01% hourly funding rate over 24 hours
            result = calculator.calculate_funding_payment(
                position=long_position,
                funding_rate=Decimal("0.0001"),
                time_delta_hours=Decimal("24"),
            )
            # result.payment = -$120 for a $50,000 position (long pays)
        """
        # Validate position type
        if position.position_type not in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
            raise ValueError(f"Position must be PERP_LONG or PERP_SHORT, got {position.position_type}")

        # Convert timedelta to hours if needed
        if isinstance(time_delta_hours, timedelta):
            hours = Decimal(str(time_delta_hours.total_seconds())) / Decimal("3600")
        else:
            hours = time_delta_hours

        # Clamp funding rate to bounds
        clamped_rate = max(self.min_funding_rate, min(self.max_funding_rate, funding_rate))

        # Get position notional value
        position_value = position.notional_usd

        # Calculate raw funding amount
        raw_funding = position_value * clamped_rate * hours

        # Apply sign convention based on position type
        # PERP_LONG: pays when funding rate is positive
        # PERP_SHORT: receives when funding rate is positive
        if position.position_type == PositionType.PERP_LONG:
            payment = -raw_funding  # Longs pay when rate is positive
            is_payer = raw_funding > Decimal("0")
        else:
            payment = raw_funding  # Shorts receive when rate is positive
            is_payer = raw_funding < Decimal("0")

        return FundingPaymentResult(
            payment=payment,
            funding_rate=clamped_rate,
            position_value_usd=position_value,
            time_hours=hours,
            is_payer=is_payer,
        )

    def apply_funding_to_position(
        self,
        position: SimulatedPosition,
        result: FundingPaymentResult,
    ) -> None:
        """Apply a funding payment result to update position's cumulative fields.

        This method updates the position's:
        - accumulated_funding: Net funding balance
        - cumulative_funding_paid: Total funding paid (always positive)
        - cumulative_funding_received: Total funding received (always positive)

        Args:
            position: The position to update (modified in place)
            result: The funding payment result from calculate_funding_payment

        Example:
            result = calculator.calculate_funding_payment(position, rate, hours)
            calculator.apply_funding_to_position(position, result)
            # Position's funding fields are now updated
        """
        # Update accumulated funding (net balance)
        position.accumulated_funding += result.payment

        # Update cumulative fields based on payment direction
        if result.is_payer:
            # Position paid funding
            position.cumulative_funding_paid += result.abs_payment
        else:
            # Position received funding
            position.cumulative_funding_received += result.abs_payment

    def get_funding_rate_for_protocol(self, protocol: str) -> Decimal:
        """Get the default funding rate for a specific protocol.

        Args:
            protocol: Protocol name (e.g., "gmx", "hyperliquid")

        Returns:
            The protocol's default funding rate, or global default if not found
        """
        return self.protocol_rates.get(protocol.lower(), self.default_funding_rate)

    def estimate_daily_funding_cost(
        self,
        position: SimulatedPosition,
        funding_rate: Decimal | None = None,
    ) -> Decimal:
        """Estimate daily funding cost/income for a position.

        Useful for risk management and strategy planning.

        Args:
            position: The perpetual position
            funding_rate: Funding rate to use (defaults to protocol rate)

        Returns:
            Daily funding in USD (negative = cost, positive = income)
        """
        rate = funding_rate or self.get_funding_rate_for_protocol(position.protocol)
        result = self.calculate_funding_payment(
            position=position,
            funding_rate=rate,
            time_delta_hours=Decimal("24"),
        )
        return result.payment

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "calculator_name": "funding",
            "funding_rate_source": self.funding_rate_source.value,
            "default_funding_rate": str(self.default_funding_rate),
            "min_funding_rate": str(self.min_funding_rate),
            "max_funding_rate": str(self.max_funding_rate),
            "protocol_rates": {k: str(v) for k, v in self.protocol_rates.items()},
        }


__all__ = [
    "FundingCalculator",
    "FundingPaymentResult",
    "FundingRateHandler",
    "FundingRateSource",
]
