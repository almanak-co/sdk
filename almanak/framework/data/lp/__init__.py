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
    )
    print(f"Projected IL: {projected.il_percent:.2f}%")
"""

from .calculator import (
    # Constants
    COMMON_PRICE_CHANGES,
    # Main class
    ILCalculator,
    # Exceptions
    ILCalculatorError,
    ILExposure,
    ILExposureUnavailableError,
    # Data classes
    ILResult,
    InvalidPriceError,
    InvalidWeightError,
    LPPosition,
    # Enums
    PoolType,
    PositionNotFoundError,
    ProjectedILResult,
    # Convenience functions
    calculate_il_simple,
    project_il_table,
)

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
