"""Impermanent Loss Calculator Module.

This module provides impermanent loss (IL) calculations for liquidity positions
across various AMM protocols including Uniswap V2/V3, Curve, and weighted pools.

Key Features:
    - Calculate IL given entry prices and current prices
    - Support for equal-weight (50/50) and custom-weight pools
    - Concentrated liquidity IL calculation for Uniswap V3
    - Project IL for simulated price changes
    - Track IL exposure for active LP positions

Example:
    from almanak.framework.data.lp import ILCalculator, ILResult

    # Create calculator
    calc = ILCalculator()

    # Calculate IL for 50/50 pool
    result = calc.calculate_il(
        entry_price_a=Decimal("2000"),  # ETH entry price in USD
        entry_price_b=Decimal("1"),     # USDC entry price in USD
        current_price_a=Decimal("2500"),  # ETH current price in USD
        current_price_b=Decimal("1"),     # USDC current price in USD
    )
    print(f"IL: {result.il_percent:.2f}%")

    # Project IL for a 20% price increase
    projected = calc.project_il(
        price_change_pct=Decimal("20"),
        weight_a=Decimal("0.5"),
        weight_b=Decimal("0.5"),
    )
    print(f"Projected IL: {projected.il_percent:.2f}%")
"""

import logging
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================


class PoolType(StrEnum):
    """Supported pool types for IL calculation."""

    CONSTANT_PRODUCT = "constant_product"  # Uniswap V2, SushiSwap
    CONCENTRATED = "concentrated"  # Uniswap V3
    STABLE = "stable"  # Curve StableSwap
    WEIGHTED = "weighted"  # Balancer weighted pools


# =============================================================================
# Exceptions
# =============================================================================


class ILCalculatorError(Exception):
    """Base exception for IL calculator errors."""

    pass


class InvalidPriceError(ILCalculatorError):
    """Raised when price values are invalid."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"Invalid price: {reason}")


class InvalidWeightError(ILCalculatorError):
    """Raised when pool weights are invalid."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"Invalid weight: {reason}")


class PositionNotFoundError(ILCalculatorError):
    """Raised when an LP position cannot be found."""

    def __init__(self, position_id: str) -> None:
        self.position_id = position_id
        super().__init__(f"Position not found: {position_id}")


class ILExposureUnavailableError(ILCalculatorError):
    """Raised when IL exposure cannot be calculated."""

    def __init__(self, position_id: str, reason: str) -> None:
        self.position_id = position_id
        self.reason = reason
        super().__init__(f"IL exposure unavailable for {position_id}: {reason}")


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class ILResult:
    """Result of an impermanent loss calculation.

    Attributes:
        il_ratio: IL as a decimal ratio (e.g., 0.0057 for 0.57% loss)
        il_percent: IL as a percentage (e.g., 0.57 for 0.57% loss)
        il_bps: IL in basis points (e.g., 57 for 0.57% loss)
        value_if_held: Value if tokens were held instead of providing liquidity
        value_in_pool: Value of LP position at current prices
        loss_absolute: Absolute loss in quote currency (USD)
        price_ratio: Ratio of current price to entry price (token A)
        pool_type: Type of pool (constant_product, concentrated, etc.)
        weight_a: Weight of token A (default 0.5)
        weight_b: Weight of token B (default 0.5)
        entry_price_a: Entry price of token A
        entry_price_b: Entry price of token B
        current_price_a: Current price of token A
        current_price_b: Current price of token B
        timestamp: When the calculation was made
    """

    il_ratio: Decimal
    il_percent: Decimal
    il_bps: int
    value_if_held: Decimal
    value_in_pool: Decimal
    loss_absolute: Decimal
    price_ratio: Decimal
    pool_type: PoolType
    weight_a: Decimal
    weight_b: Decimal
    entry_price_a: Decimal
    entry_price_b: Decimal
    current_price_a: Decimal
    current_price_b: Decimal
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_loss(self) -> bool:
        """Check if there is impermanent loss (negative IL ratio means loss vs holding)."""
        return self.il_ratio < 0

    @property
    def is_gain(self) -> bool:
        """Check if there is impermanent gain (positive IL ratio - rare but possible)."""
        return self.il_ratio > 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "il_ratio": str(self.il_ratio),
            "il_percent": str(self.il_percent),
            "il_bps": self.il_bps,
            "value_if_held": str(self.value_if_held),
            "value_in_pool": str(self.value_in_pool),
            "loss_absolute": str(self.loss_absolute),
            "price_ratio": str(self.price_ratio),
            "pool_type": self.pool_type.value,
            "weight_a": str(self.weight_a),
            "weight_b": str(self.weight_b),
            "entry_price_a": str(self.entry_price_a),
            "entry_price_b": str(self.entry_price_b),
            "current_price_a": str(self.current_price_a),
            "current_price_b": str(self.current_price_b),
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class ProjectedILResult:
    """Result of a projected impermanent loss calculation.

    This is used for simulating IL based on hypothetical price changes.

    Attributes:
        price_change_pct: The price change percentage used for projection
        il_ratio: Projected IL as a decimal ratio
        il_percent: Projected IL as a percentage
        il_bps: Projected IL in basis points
        pool_type: Type of pool
        weight_a: Weight of token A
        weight_b: Weight of token B
        timestamp: When the projection was made
    """

    price_change_pct: Decimal
    il_ratio: Decimal
    il_percent: Decimal
    il_bps: int
    pool_type: PoolType
    weight_a: Decimal
    weight_b: Decimal
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "price_change_pct": str(self.price_change_pct),
            "il_ratio": str(self.il_ratio),
            "il_percent": str(self.il_percent),
            "il_bps": self.il_bps,
            "pool_type": self.pool_type.value,
            "weight_a": str(self.weight_a),
            "weight_b": str(self.weight_b),
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class LPPosition:
    """Representation of a liquidity position for IL tracking.

    Attributes:
        position_id: Unique identifier for the position
        pool_address: Address of the LP pool
        token_a: Symbol of token A
        token_b: Symbol of token B
        entry_price_a: Entry price of token A in quote currency
        entry_price_b: Entry price of token B in quote currency
        amount_a: Amount of token A deposited
        amount_b: Amount of token B deposited
        weight_a: Weight of token A in the pool
        weight_b: Weight of token B in the pool
        pool_type: Type of AMM pool
        chain: Blockchain network
        entry_timestamp: When the position was opened
        tick_lower: Lower tick bound (for concentrated liquidity)
        tick_upper: Upper tick bound (for concentrated liquidity)
    """

    position_id: str
    pool_address: str
    token_a: str
    token_b: str
    entry_price_a: Decimal
    entry_price_b: Decimal
    amount_a: Decimal
    amount_b: Decimal
    weight_a: Decimal = Decimal("0.5")
    weight_b: Decimal = Decimal("0.5")
    pool_type: PoolType = PoolType.CONSTANT_PRODUCT
    chain: str = "ethereum"
    entry_timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    tick_lower: int | None = None
    tick_upper: int | None = None

    @property
    def entry_value(self) -> Decimal:
        """Calculate the total entry value in quote currency."""
        return (self.amount_a * self.entry_price_a) + (self.amount_b * self.entry_price_b)

    @property
    def is_concentrated(self) -> bool:
        """Check if this is a concentrated liquidity position."""
        return self.pool_type == PoolType.CONCENTRATED

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "position_id": self.position_id,
            "pool_address": self.pool_address,
            "token_a": self.token_a,
            "token_b": self.token_b,
            "entry_price_a": str(self.entry_price_a),
            "entry_price_b": str(self.entry_price_b),
            "amount_a": str(self.amount_a),
            "amount_b": str(self.amount_b),
            "weight_a": str(self.weight_a),
            "weight_b": str(self.weight_b),
            "pool_type": self.pool_type.value,
            "chain": self.chain,
            "entry_timestamp": self.entry_timestamp.isoformat(),
            "tick_lower": self.tick_lower,
            "tick_upper": self.tick_upper,
        }


@dataclass
class ILExposure:
    """IL exposure for an active LP position.

    Attributes:
        position_id: Identifier of the LP position
        position: The LP position details
        current_il: Current IL calculation result
        entry_value: Original position value at entry
        current_value: Current position value
        fees_earned: Fees earned (if tracked)
        net_pnl: Net PnL including fees
        timestamp: When the exposure was calculated
    """

    position_id: str
    position: LPPosition
    current_il: ILResult
    entry_value: Decimal
    current_value: Decimal
    fees_earned: Decimal = Decimal("0")
    net_pnl: Decimal | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        """Calculate net PnL if not provided."""
        if self.net_pnl is None:
            # Net PnL = current value - entry value + fees
            self.net_pnl = self.current_value - self.entry_value + self.fees_earned

    @property
    def il_offset_by_fees(self) -> bool:
        """Check if IL is offset by earned fees (net positive)."""
        return self.net_pnl is not None and self.net_pnl > 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "position_id": self.position_id,
            "position": self.position.to_dict(),
            "current_il": self.current_il.to_dict(),
            "entry_value": str(self.entry_value),
            "current_value": str(self.current_value),
            "fees_earned": str(self.fees_earned),
            "net_pnl": str(self.net_pnl) if self.net_pnl is not None else None,
            "timestamp": self.timestamp.isoformat(),
        }


# =============================================================================
# IL Calculator
# =============================================================================


class ILCalculator:
    """Calculator for impermanent loss across various AMM pool types.

    This class provides methods to calculate impermanent loss for liquidity
    positions, including:
    - Standard constant product AMM pools (Uniswap V2, SushiSwap)
    - Concentrated liquidity pools (Uniswap V3)
    - Weighted pools (Balancer)
    - Stable pools (Curve)

    The calculator also supports projecting IL for simulated price changes
    and tracking IL exposure for active positions.

    Example:
        calc = ILCalculator()

        # Calculate IL for a 50/50 pool
        result = calc.calculate_il(
            entry_price_a=Decimal("2000"),
            entry_price_b=Decimal("1"),
            current_price_a=Decimal("2500"),
            current_price_b=Decimal("1"),
        )
        print(f"IL: {result.il_percent:.4f}%")

        # Project IL for various price changes
        for pct in [10, 25, 50, 100]:
            proj = calc.project_il(price_change_pct=Decimal(pct))
            print(f"Price +{pct}%: IL = {proj.il_percent:.4f}%")
    """

    def __init__(
        self,
        positions: dict[str, LPPosition] | None = None,
        mock_prices: dict[str, Decimal] | None = None,
    ) -> None:
        """Initialize the IL calculator.

        Args:
            positions: Optional dictionary of tracked LP positions
            mock_prices: Optional mock prices for testing (token -> price)
        """
        self._positions: dict[str, LPPosition] = positions or {}
        self._mock_prices: dict[str, Decimal] | None = mock_prices

        logger.debug(
            "Created ILCalculator with %d tracked positions",
            len(self._positions),
        )

    def calculate_il(
        self,
        entry_price_a: Decimal,
        entry_price_b: Decimal,
        current_price_a: Decimal,
        current_price_b: Decimal,
        weight_a: Decimal = Decimal("0.5"),
        weight_b: Decimal = Decimal("0.5"),
        pool_type: PoolType = PoolType.CONSTANT_PRODUCT,
        entry_value: Decimal | None = None,
    ) -> ILResult:
        """Calculate impermanent loss given entry and current prices.

        For constant product AMMs (Uniswap V2 style), IL is calculated as:
            IL = 2 * sqrt(price_ratio) / (1 + price_ratio) - 1

        For weighted pools (Balancer style), IL is calculated using:
            IL = (price_ratio ^ weight_a) / ((weight_a * price_ratio) + weight_b) - 1

        Args:
            entry_price_a: Entry price of token A (in quote currency, e.g., USD)
            entry_price_b: Entry price of token B (in quote currency)
            current_price_a: Current price of token A
            current_price_b: Current price of token B
            weight_a: Weight of token A (default 0.5 for equal weight)
            weight_b: Weight of token B (default 0.5 for equal weight)
            pool_type: Type of AMM pool
            entry_value: Optional initial position value (for absolute loss calc)

        Returns:
            ILResult with IL metrics

        Raises:
            InvalidPriceError: If any price is zero or negative
            InvalidWeightError: If weights don't sum to 1.0
        """
        # Validate prices
        self._validate_price(entry_price_a, "entry_price_a")
        self._validate_price(entry_price_b, "entry_price_b")
        self._validate_price(current_price_a, "current_price_a")
        self._validate_price(current_price_b, "current_price_b")

        # Validate weights
        self._validate_weights(weight_a, weight_b)

        # Calculate price ratio (current price A / entry price A, normalized by B prices)
        # This gives us how much A has moved relative to B
        entry_ratio = entry_price_a / entry_price_b
        current_ratio = current_price_a / current_price_b
        price_ratio = current_ratio / entry_ratio

        # Calculate IL based on pool type
        if pool_type == PoolType.CONSTANT_PRODUCT:
            il_ratio = self._calculate_constant_product_il(price_ratio)
        elif pool_type == PoolType.WEIGHTED:
            il_ratio = self._calculate_weighted_pool_il(price_ratio, weight_a, weight_b)
        elif pool_type == PoolType.STABLE:
            # Stable pools have minimal IL due to the curve
            il_ratio = self._calculate_stable_pool_il(price_ratio)
        elif pool_type == PoolType.CONCENTRATED:
            # For concentrated liquidity without tick bounds, use constant product
            # as approximation (full-range position)
            il_ratio = self._calculate_constant_product_il(price_ratio)
        else:
            # Default to constant product
            il_ratio = self._calculate_constant_product_il(price_ratio)

        # Convert to percentage and bps
        il_percent = il_ratio * Decimal("100")
        il_bps = int((il_ratio * Decimal("10000")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

        # Calculate value comparisons (assuming initial $1000 position for demonstration)
        # In a real scenario, entry_value would be provided
        base_value = entry_value or Decimal("1000")
        value_if_held = base_value * (
            weight_a * (current_price_a / entry_price_a) + weight_b * (current_price_b / entry_price_b)
        )
        value_in_pool = value_if_held * (Decimal("1") + il_ratio)
        loss_absolute = value_if_held - value_in_pool

        return ILResult(
            il_ratio=il_ratio.quantize(Decimal("0.000001")),
            il_percent=il_percent.quantize(Decimal("0.0001")),
            il_bps=il_bps,
            value_if_held=value_if_held.quantize(Decimal("0.01")),
            value_in_pool=value_in_pool.quantize(Decimal("0.01")),
            loss_absolute=loss_absolute.quantize(Decimal("0.01")),
            price_ratio=price_ratio.quantize(Decimal("0.000001")),
            pool_type=pool_type,
            weight_a=weight_a,
            weight_b=weight_b,
            entry_price_a=entry_price_a,
            entry_price_b=entry_price_b,
            current_price_a=current_price_a,
            current_price_b=current_price_b,
        )

    def calculate_il_concentrated(
        self,
        entry_price_a: Decimal,
        entry_price_b: Decimal,
        current_price_a: Decimal,
        current_price_b: Decimal,
        tick_lower: int,
        tick_upper: int,
        entry_value: Decimal | None = None,
    ) -> ILResult:
        """Calculate IL for concentrated liquidity positions (Uniswap V3).

        Concentrated liquidity amplifies both gains and IL compared to
        full-range positions. The IL is higher when price moves outside
        the range.

        Args:
            entry_price_a: Entry price of token A
            entry_price_b: Entry price of token B
            current_price_a: Current price of token A
            current_price_b: Current price of token B
            tick_lower: Lower tick bound of the position
            tick_upper: Upper tick bound of the position
            entry_value: Optional initial position value

        Returns:
            ILResult with IL metrics for concentrated position

        Raises:
            InvalidPriceError: If any price is zero or negative
        """
        # Validate prices
        self._validate_price(entry_price_a, "entry_price_a")
        self._validate_price(entry_price_b, "entry_price_b")
        self._validate_price(current_price_a, "current_price_a")
        self._validate_price(current_price_b, "current_price_b")

        # Calculate price bounds from ticks
        # price = 1.0001^tick
        price_lower = Decimal(str(math.pow(1.0001, tick_lower)))
        price_upper = Decimal(str(math.pow(1.0001, tick_upper)))

        # Current price ratio
        entry_ratio = entry_price_a / entry_price_b
        current_ratio = current_price_a / current_price_b
        price_ratio = current_ratio / entry_ratio

        # Adjust current price for bounds
        effective_ratio = price_ratio
        if price_ratio < price_lower / entry_ratio:
            effective_ratio = price_lower / entry_ratio
        elif price_ratio > price_upper / entry_ratio:
            effective_ratio = price_upper / entry_ratio

        # Calculate concentrated IL using the effective range
        # Concentration factor increases IL impact
        range_factor = (price_upper / price_lower).ln() if price_upper > price_lower else Decimal("1")

        # Base IL for full range
        base_il = self._calculate_constant_product_il(effective_ratio)

        # Amplification based on range width (narrower = higher amplification)
        # For a typical full range, range_factor ≈ 10-20
        # For a narrow range, range_factor ≈ 0.1-1
        if range_factor < Decimal("1"):
            amplification = Decimal("1") + (Decimal("1") - range_factor)
        else:
            amplification = Decimal("1")

        il_ratio = base_il * amplification

        # Convert to percentage and bps
        il_percent = il_ratio * Decimal("100")
        il_bps = int((il_ratio * Decimal("10000")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

        # Calculate values
        base_value = entry_value or Decimal("1000")
        weight_a = Decimal("0.5")
        weight_b = Decimal("0.5")
        value_if_held = base_value * (
            weight_a * (current_price_a / entry_price_a) + weight_b * (current_price_b / entry_price_b)
        )
        value_in_pool = value_if_held * (Decimal("1") + il_ratio)
        loss_absolute = value_if_held - value_in_pool

        return ILResult(
            il_ratio=il_ratio.quantize(Decimal("0.000001")),
            il_percent=il_percent.quantize(Decimal("0.0001")),
            il_bps=il_bps,
            value_if_held=value_if_held.quantize(Decimal("0.01")),
            value_in_pool=value_in_pool.quantize(Decimal("0.01")),
            loss_absolute=loss_absolute.quantize(Decimal("0.01")),
            price_ratio=price_ratio.quantize(Decimal("0.000001")),
            pool_type=PoolType.CONCENTRATED,
            weight_a=weight_a,
            weight_b=weight_b,
            entry_price_a=entry_price_a,
            entry_price_b=entry_price_b,
            current_price_a=current_price_a,
            current_price_b=current_price_b,
        )

    def project_il(
        self,
        price_change_pct: Decimal,
        weight_a: Decimal = Decimal("0.5"),
        weight_b: Decimal = Decimal("0.5"),
        pool_type: PoolType = PoolType.CONSTANT_PRODUCT,
    ) -> ProjectedILResult:
        """Project impermanent loss for a given price change.

        This method simulates what IL would be if token A's price changed
        by the specified percentage while token B remains constant
        (typical for ETH/stablecoin pairs).

        Args:
            price_change_pct: Price change percentage (e.g., 50 for +50%, -30 for -30%)
            weight_a: Weight of token A (default 0.5)
            weight_b: Weight of token B (default 0.5)
            pool_type: Type of AMM pool

        Returns:
            ProjectedILResult with projected IL metrics

        Raises:
            InvalidWeightError: If weights don't sum to 1.0
            InvalidPriceError: If price change would result in negative price
        """
        # Validate weights
        self._validate_weights(weight_a, weight_b)

        # Calculate price ratio from percentage change
        # price_change_pct of 50 means price is 1.5x entry
        # price_change_pct of -30 means price is 0.7x entry
        if price_change_pct <= Decimal("-100"):
            raise InvalidPriceError("Price change cannot be -100% or less (would result in zero or negative price)")

        price_ratio = Decimal("1") + (price_change_pct / Decimal("100"))

        # Calculate IL based on pool type
        if pool_type == PoolType.CONSTANT_PRODUCT:
            il_ratio = self._calculate_constant_product_il(price_ratio)
        elif pool_type == PoolType.WEIGHTED:
            il_ratio = self._calculate_weighted_pool_il(price_ratio, weight_a, weight_b)
        elif pool_type == PoolType.STABLE:
            il_ratio = self._calculate_stable_pool_il(price_ratio)
        else:
            il_ratio = self._calculate_constant_product_il(price_ratio)

        # Convert to percentage and bps
        il_percent = il_ratio * Decimal("100")
        il_bps = int((il_ratio * Decimal("10000")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

        return ProjectedILResult(
            price_change_pct=price_change_pct,
            il_ratio=il_ratio.quantize(Decimal("0.000001")),
            il_percent=il_percent.quantize(Decimal("0.0001")),
            il_bps=il_bps,
            pool_type=pool_type,
            weight_a=weight_a,
            weight_b=weight_b,
        )

    def add_position(self, position: LPPosition) -> None:
        """Add an LP position for tracking.

        Args:
            position: The LP position to track
        """
        self._positions[position.position_id] = position
        logger.debug("Added position %s for tracking", position.position_id)

    def remove_position(self, position_id: str) -> None:
        """Remove an LP position from tracking.

        Args:
            position_id: ID of the position to remove

        Raises:
            PositionNotFoundError: If position doesn't exist
        """
        if position_id not in self._positions:
            raise PositionNotFoundError(position_id)

        del self._positions[position_id]
        logger.debug("Removed position %s from tracking", position_id)

    def get_position(self, position_id: str) -> LPPosition:
        """Get a tracked LP position.

        Args:
            position_id: ID of the position

        Returns:
            The LP position

        Raises:
            PositionNotFoundError: If position doesn't exist
        """
        if position_id not in self._positions:
            raise PositionNotFoundError(position_id)
        return self._positions[position_id]

    def get_all_positions(self) -> list[LPPosition]:
        """Get all tracked LP positions.

        Returns:
            List of all tracked positions
        """
        return list(self._positions.values())

    def calculate_il_exposure(
        self,
        position_id: str,
        current_price_a: Decimal | None = None,
        current_price_b: Decimal | None = None,
        fees_earned: Decimal = Decimal("0"),
    ) -> ILExposure:
        """Calculate IL exposure for a tracked position.

        Args:
            position_id: ID of the tracked position
            current_price_a: Current price of token A (uses mock if not provided)
            current_price_b: Current price of token B (uses mock if not provided)
            fees_earned: Fees earned by the position (optional)

        Returns:
            ILExposure with full exposure details

        Raises:
            PositionNotFoundError: If position doesn't exist
            ILExposureUnavailableError: If prices cannot be determined
        """
        position = self.get_position(position_id)

        # Get current prices
        price_a = current_price_a
        price_b = current_price_b

        if price_a is None:
            if self._mock_prices and position.token_a in self._mock_prices:
                price_a = self._mock_prices[position.token_a]
            else:
                raise ILExposureUnavailableError(
                    position_id,
                    f"No current price available for {position.token_a}",
                )

        if price_b is None:
            if self._mock_prices and position.token_b in self._mock_prices:
                price_b = self._mock_prices[position.token_b]
            else:
                raise ILExposureUnavailableError(
                    position_id,
                    f"No current price available for {position.token_b}",
                )

        # Calculate IL
        if position.is_concentrated and position.tick_lower is not None and position.tick_upper is not None:
            il_result = self.calculate_il_concentrated(
                entry_price_a=position.entry_price_a,
                entry_price_b=position.entry_price_b,
                current_price_a=price_a,
                current_price_b=price_b,
                tick_lower=position.tick_lower,
                tick_upper=position.tick_upper,
                entry_value=position.entry_value,
            )
        else:
            il_result = self.calculate_il(
                entry_price_a=position.entry_price_a,
                entry_price_b=position.entry_price_b,
                current_price_a=price_a,
                current_price_b=price_b,
                weight_a=position.weight_a,
                weight_b=position.weight_b,
                pool_type=position.pool_type,
                entry_value=position.entry_value,
            )

        # Calculate current value
        current_value = il_result.value_in_pool

        return ILExposure(
            position_id=position_id,
            position=position,
            current_il=il_result,
            entry_value=position.entry_value,
            current_value=current_value,
            fees_earned=fees_earned,
        )

    def set_mock_prices(self, prices: dict[str, Decimal]) -> None:
        """Set mock prices for testing.

        Args:
            prices: Dictionary mapping token symbols to prices
        """
        self._mock_prices = prices

    def clear_mock_prices(self) -> None:
        """Clear mock prices."""
        self._mock_prices = None

    # =========================================================================
    # Private Methods
    # =========================================================================

    def _validate_price(self, price: Decimal, name: str) -> None:
        """Validate a price value.

        Args:
            price: The price to validate
            name: Name of the parameter for error messages

        Raises:
            InvalidPriceError: If price is zero or negative
        """
        if price <= 0:
            raise InvalidPriceError(f"{name} must be positive, got {price}")

    def _validate_weights(self, weight_a: Decimal, weight_b: Decimal) -> None:
        """Validate pool weights.

        Args:
            weight_a: Weight of token A
            weight_b: Weight of token B

        Raises:
            InvalidWeightError: If weights are invalid
        """
        if weight_a <= 0 or weight_a >= 1:
            raise InvalidWeightError(f"weight_a must be between 0 and 1, got {weight_a}")
        if weight_b <= 0 or weight_b >= 1:
            raise InvalidWeightError(f"weight_b must be between 0 and 1, got {weight_b}")

        total = weight_a + weight_b
        if not (Decimal("0.999") <= total <= Decimal("1.001")):
            raise InvalidWeightError(f"Weights must sum to 1.0, got {total}")

    def _calculate_constant_product_il(self, price_ratio: Decimal) -> Decimal:
        """Calculate IL for constant product AMM (x*y=k).

        Formula: IL = 2 * sqrt(r) / (1 + r) - 1
        where r = current_price / entry_price

        Args:
            price_ratio: Ratio of current price to entry price

        Returns:
            IL as a negative decimal (e.g., -0.0057 for 0.57% loss)
        """
        if price_ratio == 1:
            return Decimal("0")

        # Using high precision for sqrt
        sqrt_ratio = Decimal(str(math.sqrt(float(price_ratio))))
        il = (Decimal("2") * sqrt_ratio / (Decimal("1") + price_ratio)) - Decimal("1")

        return il

    def _calculate_weighted_pool_il(
        self,
        price_ratio: Decimal,
        weight_a: Decimal,
        weight_b: Decimal,
    ) -> Decimal:
        """Calculate IL for weighted pools (Balancer style).

        Formula: IL = (r ^ w_a) / ((w_a * r) + w_b) - 1
        where r = price_ratio, w_a = weight_a, w_b = weight_b

        Args:
            price_ratio: Ratio of current price to entry price
            weight_a: Weight of token A
            weight_b: Weight of token B

        Returns:
            IL as a negative decimal
        """
        if price_ratio == 1:
            return Decimal("0")

        # For 50/50 pools, this simplifies to constant product formula
        if weight_a == Decimal("0.5") and weight_b == Decimal("0.5"):
            return self._calculate_constant_product_il(price_ratio)

        # General weighted pool formula
        # value_lp = 2 * (r^w_a)^(1/(1-w_a)) for the rebalancing effect
        # Simplified: IL ≈ -(w_a * w_b * (sqrt(r) - 1)^2) / (w_a * r + w_b)
        sqrt_ratio = Decimal(str(math.sqrt(float(price_ratio))))
        diff_squared = (sqrt_ratio - Decimal("1")) ** 2
        denominator = weight_a * price_ratio + weight_b

        il = -weight_a * weight_b * diff_squared / denominator

        return il

    def _calculate_stable_pool_il(self, price_ratio: Decimal) -> Decimal:
        """Calculate IL for stable pools (Curve style).

        Stable pools use a hybrid curve between constant product and constant sum,
        resulting in very low IL for stablecoins that maintain their peg.

        The IL is approximately 1/10th of a constant product pool for small deviations.

        Args:
            price_ratio: Ratio of current price to entry price

        Returns:
            IL as a negative decimal (much smaller than constant product)
        """
        if price_ratio == 1:
            return Decimal("0")

        # Stable pools have ~10x less IL than constant product for small deviations
        # This is a simplified approximation
        base_il = self._calculate_constant_product_il(price_ratio)

        # Apply amplification factor reduction
        # Curve's A parameter typically ranges from 100-2000
        # Higher A = lower IL
        amplification_factor = Decimal("0.1")  # Approximate 10x reduction

        return base_il * amplification_factor


# =============================================================================
# Convenience Functions
# =============================================================================


def calculate_il_simple(
    entry_price: Decimal,
    current_price: Decimal,
) -> Decimal:
    """Simple IL calculation for 50/50 pools with one volatile asset.

    This is a convenience function for the common case of ETH/stablecoin
    pairs where only token A (ETH) changes in price.

    Args:
        entry_price: Entry price of the volatile asset
        current_price: Current price of the volatile asset

    Returns:
        IL as a percentage (e.g., 0.57 for 0.57% loss)
    """
    calc = ILCalculator()
    result = calc.calculate_il(
        entry_price_a=entry_price,
        entry_price_b=Decimal("1"),
        current_price_a=current_price,
        current_price_b=Decimal("1"),
    )
    return result.il_percent


def project_il_table(
    price_changes: list[Decimal],
    weight_a: Decimal = Decimal("0.5"),
    weight_b: Decimal = Decimal("0.5"),
) -> list[ProjectedILResult]:
    """Generate an IL projection table for various price changes.

    Args:
        price_changes: List of price change percentages to project
        weight_a: Weight of token A
        weight_b: Weight of token B

    Returns:
        List of ProjectedILResult for each price change
    """
    calc = ILCalculator()
    results = []

    for pct in price_changes:
        result = calc.project_il(
            price_change_pct=pct,
            weight_a=weight_a,
            weight_b=weight_b,
        )
        results.append(result)

    return results


# =============================================================================
# Constants
# =============================================================================


# Common price changes for IL tables
COMMON_PRICE_CHANGES: list[Decimal] = [
    Decimal("-50"),
    Decimal("-25"),
    Decimal("-10"),
    Decimal("0"),
    Decimal("10"),
    Decimal("25"),
    Decimal("50"),
    Decimal("100"),
    Decimal("200"),
    Decimal("500"),
]


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    # Main class
    "ILCalculator",
    # Enums
    "PoolType",
    # Data classes
    "ILResult",
    "ProjectedILResult",
    "LPPosition",
    "ILExposure",
    # Exceptions
    "ILCalculatorError",
    "InvalidPriceError",
    "InvalidWeightError",
    "PositionNotFoundError",
    "ILExposureUnavailableError",
    # Convenience functions
    "calculate_il_simple",
    "project_il_table",
    # Constants
    "COMMON_PRICE_CHANGES",
]
