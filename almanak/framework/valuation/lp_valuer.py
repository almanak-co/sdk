"""LP position valuation: Uniswap V3 concentrated liquidity math.

Pure deterministic math. No I/O, no gateway calls.

Given a V3 position's (liquidity, tick_lower, tick_upper) and current prices,
calculates the token amounts in the position and their USD value.

The math follows the Uniswap V3 whitepaper:
  - price = 1.0001^tick
  - sqrt(price) = 1.0001^(tick/2)
  - Token amounts depend on where current price sits relative to the range

References:
  - Uniswap V3 Whitepaper: https://uniswap.org/whitepaper-v3.pdf
  - V3 Math: https://atiselsts.github.io/pdfs/uniswap-v3-liquidity-math.pdf
"""

import logging
from dataclasses import dataclass
from decimal import Decimal

logger = logging.getLogger(__name__)

# Uniswap V3 tick constants
MIN_TICK = -887272
MAX_TICK = 887272

# ln(1.0001) pre-computed to high precision
_LN_TICK_BASE = Decimal("0.0000999950003333083340832824")


@dataclass
class LPTokenAmounts:
    """Token amounts in an LP position at a given price.

    Attributes:
        amount0: Amount of token0 (in token0 units, NOT wei)
        amount1: Amount of token1 (in token1 units, NOT wei)
    """

    amount0: Decimal
    amount1: Decimal


@dataclass
class LPPositionValue:
    """Valued LP position with token breakdown.

    Attributes:
        value_usd: Total position value in USD
        amount0: Amount of token0 (human-readable)
        amount1: Amount of token1 (human-readable)
        token0_value_usd: USD value of token0 portion
        token1_value_usd: USD value of token1 portion
        in_range: Whether current price is within the position's tick range
    """

    value_usd: Decimal
    amount0: Decimal
    amount1: Decimal
    token0_value_usd: Decimal
    token1_value_usd: Decimal
    in_range: bool


def get_token_amounts(
    liquidity: int | Decimal,
    tick_lower: int,
    tick_upper: int,
    current_tick: int,
) -> LPTokenAmounts:
    """Calculate token amounts in a V3 position at a given tick.

    Uses the Uniswap V3 concentrated liquidity formulas:
      - Below range: position is 100% token0
      - Above range: position is 100% token1
      - In range: mix of both tokens

    Args:
        liquidity: Position liquidity (L value from the NFT)
        tick_lower: Lower tick boundary
        tick_upper: Upper tick boundary
        current_tick: Current pool tick (from slot0)

    Returns:
        LPTokenAmounts with token0 and token1 amounts.
        Amounts are in "virtual" units scaled by liquidity.
        For human-readable amounts, divide by 10^decimals.
    """
    if tick_lower >= tick_upper:
        return LPTokenAmounts(amount0=Decimal("0"), amount1=Decimal("0"))

    liq = Decimal(str(liquidity))
    if liq <= 0:
        return LPTokenAmounts(amount0=Decimal("0"), amount1=Decimal("0"))

    sqrt_lower = _tick_to_sqrt_price(tick_lower)
    sqrt_upper = _tick_to_sqrt_price(tick_upper)
    sqrt_current = _tick_to_sqrt_price(current_tick)

    return _compute_amounts(liq, sqrt_current, sqrt_lower, sqrt_upper)


def get_token_amounts_from_sqrt_price(
    liquidity: int | Decimal,
    tick_lower: int,
    tick_upper: int,
    sqrt_price_x96: int,
) -> LPTokenAmounts:
    """Calculate token amounts using sqrtPriceX96 from pool slot0.

    More precise than tick-based calculation since it uses the exact
    pool price rather than the tick approximation.

    Args:
        liquidity: Position liquidity (L value)
        tick_lower: Lower tick boundary
        tick_upper: Upper tick boundary
        sqrt_price_x96: Current sqrtPriceX96 from pool.slot0()

    Returns:
        LPTokenAmounts with token0 and token1 amounts in wei.
    """
    if tick_lower >= tick_upper:
        return LPTokenAmounts(amount0=Decimal("0"), amount1=Decimal("0"))

    liq = Decimal(str(liquidity))
    if liq <= 0:
        return LPTokenAmounts(amount0=Decimal("0"), amount1=Decimal("0"))

    # Convert sqrtPriceX96 to actual sqrt(price)
    # sqrtPriceX96 = sqrt(price) * 2^96
    q96 = Decimal(2**96)
    sqrt_current = Decimal(str(sqrt_price_x96)) / q96

    sqrt_lower = _tick_to_sqrt_price(tick_lower)
    sqrt_upper = _tick_to_sqrt_price(tick_upper)

    return _compute_amounts(liq, sqrt_current, sqrt_lower, sqrt_upper)


def value_lp_position(
    liquidity: int | Decimal,
    tick_lower: int,
    tick_upper: int,
    current_tick: int,
    token0_price_usd: Decimal,
    token1_price_usd: Decimal,
    token0_decimals: int,
    token1_decimals: int,
    *,
    sqrt_price_x96: int | None = None,
) -> LPPositionValue:
    """Value an LP position in USD using on-chain parameters and market prices.

    This is the main entry point for LP valuation. It:
    1. Computes token amounts from V3 math
    2. Converts from wei to human-readable using decimals
    3. Prices each token in USD

    Args:
        liquidity: Position liquidity (L value from NFT)
        tick_lower: Lower tick boundary
        tick_upper: Upper tick boundary
        current_tick: Current pool tick (used when sqrt_price_x96 unavailable)
        token0_price_usd: USD price of token0
        token1_price_usd: USD price of token1
        token0_decimals: Decimals for token0 (e.g., 18 for WETH, 6 for USDC)
        token1_decimals: Decimals for token1
        sqrt_price_x96: Exact sqrtPriceX96 from pool.slot0(). Preferred over
            current_tick for mid-tick precision in narrow ranges.

    Returns:
        LPPositionValue with USD value breakdown
    """
    # Prefer exact sqrtPriceX96 over tick-based approximation
    amounts = (
        get_token_amounts_from_sqrt_price(liquidity, tick_lower, tick_upper, sqrt_price_x96)
        if sqrt_price_x96 is not None
        else get_token_amounts(liquidity, tick_lower, tick_upper, current_tick)
    )

    # Convert from wei to human-readable
    amt0 = amounts.amount0 / Decimal(10**token0_decimals)
    amt1 = amounts.amount1 / Decimal(10**token1_decimals)

    val0 = amt0 * token0_price_usd
    val1 = amt1 * token1_price_usd

    # Determine if position is in range
    in_range = tick_lower <= current_tick < tick_upper

    return LPPositionValue(
        value_usd=val0 + val1,
        amount0=amt0,
        amount1=amt1,
        token0_value_usd=val0,
        token1_value_usd=val1,
        in_range=in_range,
    )


# ---------------------------------------------------------------------------
# Internal math helpers
# ---------------------------------------------------------------------------


def _compute_amounts(
    liquidity: Decimal,
    sqrt_price: Decimal,
    sqrt_price_lower: Decimal,
    sqrt_price_upper: Decimal,
) -> LPTokenAmounts:
    """Core V3 token amount calculation.

    Three cases based on where current price sits:
    1. Below range → 100% token0
    2. Above range → 100% token1
    3. In range → mix of both
    """
    # Case 1: Price below range — all token0
    if sqrt_price <= sqrt_price_lower:
        token0 = liquidity * (Decimal("1") / sqrt_price_lower - Decimal("1") / sqrt_price_upper)
        token1 = Decimal("0")

    # Case 2: Price above range — all token1
    elif sqrt_price >= sqrt_price_upper:
        token0 = Decimal("0")
        token1 = liquidity * (sqrt_price_upper - sqrt_price_lower)

    # Case 3: Price within range — mix of both
    else:
        token0 = liquidity * (Decimal("1") / sqrt_price - Decimal("1") / sqrt_price_upper)
        token1 = liquidity * (sqrt_price - sqrt_price_lower)

    return LPTokenAmounts(amount0=max(Decimal("0"), token0), amount1=max(Decimal("0"), token1))


def _tick_to_sqrt_price(tick: int) -> Decimal:
    """Convert a V3 tick to sqrt(price).

    In Uniswap V3: price = 1.0001^tick
    Therefore: sqrt(price) = 1.0001^(tick/2) = e^(tick/2 * ln(1.0001))
    """
    half_tick = Decimal(tick) / Decimal("2")
    exponent = half_tick * _LN_TICK_BASE

    return _decimal_exp(exponent)


def _decimal_exp(x: Decimal) -> Decimal:
    """Calculate e^x using Taylor series with range reduction."""
    if abs(x) > Decimal("10"):
        ln2 = Decimal("0.693147180559945309417232121458")
        n = int(x / ln2)
        r = x - Decimal(n) * ln2
        return (Decimal("2") ** n) * _decimal_exp(r)

    result = Decimal("1")
    term = Decimal("1")

    for i in range(1, 100):
        term = term * x / Decimal(i)
        result += term
        if abs(term) < Decimal("1e-28"):
            break

    return result
