"""Public tick/price conversion utilities for concentrated liquidity strategies.

Strategy authors building CL LP positions can use these helpers to convert
between prices and ticks, look up tick spacing for fee tiers, and snap ticks
to valid spacing boundaries.

Example::

    from decimal import Decimal
    from almanak.framework.intents import (
        price_to_tick, tick_to_price, get_tick_spacing, snap_to_tick_spacing,
    )

    # Convert a human price to the nearest tick (WETH/USDC pair, 18/6 decimals)
    # price = price of token0 (WETH) in terms of token1 (USDC)
    tick = price_to_tick(Decimal("2000.50"), decimals0=18, decimals1=6)

    # Convert a tick back to a price
    price = tick_to_price(tick, decimals0=18, decimals1=6)

    # Look up tick spacing for a fee tier
    spacing = get_tick_spacing(fee_tier=3000)  # -> 60

    # Snap a tick to the nearest valid tick spacing boundary
    valid_tick = snap_to_tick_spacing(tick, fee_tier=3000)
"""

import logging
from decimal import Decimal

logger = logging.getLogger(__name__)

from almanak.framework.connectors.uniswap_v3.sdk import (
    MAX_TICK as _MAX_TICK,
)
from almanak.framework.connectors.uniswap_v3.sdk import (
    MIN_TICK as _MIN_TICK,
)
from almanak.framework.connectors.uniswap_v3.sdk import (
    price_to_tick as _price_to_tick,
)
from almanak.framework.connectors.uniswap_v3.sdk import (
    tick_to_price,
)


def price_to_tick(
    price: Decimal | float,
    decimals0: int = 18,
    decimals1: int = 18,
) -> int:
    """Convert a human-readable price to the nearest Uniswap V3 tick.

    Unlike the internal SDK helper, this public API rejects non-positive prices
    rather than silently mapping them to ``MIN_TICK``, which could cause an
    unintended full-range LP position.

    Args:
        price: Price of token0 in terms of token1 (must be > 0).
        decimals0: Decimals of token0.
        decimals1: Decimals of token1.

    Returns:
        Tick value (may not be on a valid tick spacing boundary — use
        :func:`snap_to_tick_spacing` to align it).

    Raises:
        ValueError: If *price* is zero or negative.
    """
    if price <= 0:
        raise ValueError(f"price must be positive, got {price}")
    return _price_to_tick(price, decimals0=decimals0, decimals1=decimals1)


_TICK_SPACINGS: dict[int, int] = {
    100: 1,  # 0.01%
    500: 10,  # 0.05%
    2500: 50,  # 0.25% (PancakeSwap V3)
    3000: 60,  # 0.30%
    10000: 200,  # 1.00%
}


def get_tick_spacing(fee_tier: int) -> int:
    """Get the tick spacing for a given fee tier.

    Common fee tiers and their tick spacings:
        - 100 (0.01%) -> 1
        - 500 (0.05%) -> 10
        - 2500 (0.25%) -> 50  (PancakeSwap V3)
        - 3000 (0.3%) -> 60
        - 10000 (1%) -> 200

    For unknown fee tiers (protocol-specific), defaults to 60.

    Args:
        fee_tier: Fee tier in basis points

    Returns:
        Tick spacing for the fee tier
    """
    spacing = _TICK_SPACINGS.get(fee_tier)
    if spacing is None:
        logger.warning(
            "Unknown fee tier %d -- defaulting to tick_spacing=60. Known fee tiers: %s.",
            fee_tier,
            sorted(_TICK_SPACINGS.keys()),
        )
        return 60
    return spacing


def snap_to_tick_spacing(tick: int, fee_tier: int) -> int:
    """Snap a tick to the nearest valid boundary for the given fee tier.

    Args:
        tick: The tick value to snap.
        fee_tier: Fee tier in basis points (e.g. 100, 500, 2500, 3000, 10000).

    Returns:
        The nearest tick aligned to the fee tier's tick spacing.
    """
    spacing = get_tick_spacing(fee_tier)
    rounded = round(tick / spacing) * spacing
    return max(get_min_tick(fee_tier), min(get_max_tick(fee_tier), rounded))


def get_min_tick(fee_tier: int) -> int:
    """Get the minimum valid tick for a fee tier.

    Args:
        fee_tier: Fee tier in basis points

    Returns:
        Minimum valid tick aligned to the fee tier's tick spacing
    """
    spacing = get_tick_spacing(fee_tier)
    return -(-_MIN_TICK // spacing) * spacing


def get_max_tick(fee_tier: int) -> int:
    """Get the maximum valid tick for a fee tier.

    Args:
        fee_tier: Fee tier in basis points

    Returns:
        Maximum valid tick aligned to the fee tier's tick spacing
    """
    spacing = get_tick_spacing(fee_tier)
    return (_MAX_TICK // spacing) * spacing


__all__ = [
    "price_to_tick",
    "tick_to_price",
    "get_tick_spacing",
    "snap_to_tick_spacing",
    "get_min_tick",
    "get_max_tick",
]
