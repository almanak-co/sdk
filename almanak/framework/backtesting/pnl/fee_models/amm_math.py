"""AMM math for accurate slippage calculations.

This module implements the core AMM math for calculating price impact and slippage
for both Uniswap V2 (constant-product) and Uniswap V3 (concentrated liquidity) pools.

Key Formulas:
    V2 Constant Product (x * y = k):
        - Price impact = amount_in / (reserve_in + amount_in)
        - Output amount = reserve_out * amount_in / (reserve_in + amount_in)

    V3 Concentrated Liquidity:
        - Uses sqrtPrice and liquidity for precise calculations
        - Accounts for tick boundaries and concentrated positions
        - Price impact depends on liquidity depth at current price

Example:
    from almanak.framework.backtesting.pnl.fee_models.amm_math import (
        calculate_v2_price_impact,
        calculate_v3_price_impact,
        V2PoolState,
        V3PoolState,
    )

    # V2 pool calculation
    v2_state = V2PoolState(
        reserve_in=Decimal("1000000"),  # $1M token0
        reserve_out=Decimal("2000"),    # 2000 ETH
        fee_bps=30,  # 0.3%
    )
    impact = calculate_v2_price_impact(v2_state, trade_amount=Decimal("50000"))

    # V3 pool calculation
    v3_state = V3PoolState(
        sqrt_price_x96=sqrt_price,
        liquidity=liquidity,
        tick_lower=-887272,
        tick_upper=887272,
        fee_bps=3000,
    )
    impact = calculate_v3_price_impact(v3_state, trade_amount_usd=Decimal("50000"))
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Uniswap V3 constants
Q96 = 2**96  # 2^96 for fixed-point math
Q128 = 2**128  # 2^128 for fee growth
MIN_TICK = -887272  # Minimum tick for full range
MAX_TICK = 887272  # Maximum tick for full range
TICK_SPACING_MAP = {
    100: 1,  # 0.01% fee tier
    500: 10,  # 0.05% fee tier
    3000: 60,  # 0.3% fee tier
    10000: 200,  # 1% fee tier
}

# Price impact thresholds
LOW_LIQUIDITY_WARNING_THRESHOLD = Decimal("0.01")  # 1% price impact warning
HIGH_SLIPPAGE_WARNING_THRESHOLD = Decimal("0.05")  # 5% slippage warning


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class V2PoolState:
    """State of a Uniswap V2 (or compatible) constant-product pool.

    In V2 AMMs, the invariant x * y = k is maintained, where:
    - x = reserve of token0
    - y = reserve of token1
    - k = constant product (increases with fees)

    Attributes:
        reserve_in: Reserve amount of the input token (in token units)
        reserve_out: Reserve amount of the output token (in token units)
        fee_bps: Pool fee in basis points (default 30 = 0.3%)
        reserve_in_usd: USD value of input reserve (for USD-based calculations)
        reserve_out_usd: USD value of output reserve (for USD-based calculations)
    """

    reserve_in: Decimal
    reserve_out: Decimal
    fee_bps: int = 30  # 0.3% default for Uniswap V2
    reserve_in_usd: Decimal | None = None
    reserve_out_usd: Decimal | None = None

    @property
    def fee_factor(self) -> Decimal:
        """Fee factor as a decimal (1 - fee). For 0.3% fee: 0.997"""
        return Decimal("1") - Decimal(self.fee_bps) / Decimal("10000")

    @property
    def k(self) -> Decimal:
        """Constant product invariant k = x * y."""
        return self.reserve_in * self.reserve_out

    @property
    def spot_price(self) -> Decimal:
        """Spot price of out token in terms of in token (reserve_in/reserve_out)."""
        if self.reserve_out == 0:
            return Decimal("0")
        return self.reserve_in / self.reserve_out

    @property
    def total_liquidity_usd(self) -> Decimal | None:
        """Total pool TVL in USD if USD values are known."""
        if self.reserve_in_usd is not None and self.reserve_out_usd is not None:
            return self.reserve_in_usd + self.reserve_out_usd
        return None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "reserve_in": str(self.reserve_in),
            "reserve_out": str(self.reserve_out),
            "fee_bps": self.fee_bps,
            "reserve_in_usd": str(self.reserve_in_usd) if self.reserve_in_usd else None,
            "reserve_out_usd": str(self.reserve_out_usd) if self.reserve_out_usd else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "V2PoolState":
        """Deserialize from dictionary."""
        return cls(
            reserve_in=Decimal(data["reserve_in"]),
            reserve_out=Decimal(data["reserve_out"]),
            fee_bps=data.get("fee_bps", 30),
            reserve_in_usd=Decimal(data["reserve_in_usd"]) if data.get("reserve_in_usd") else None,
            reserve_out_usd=Decimal(data["reserve_out_usd"]) if data.get("reserve_out_usd") else None,
        )


@dataclass
class V3PoolState:
    """State of a Uniswap V3 concentrated liquidity pool.

    V3 uses sqrtPriceX96 for price representation and liquidity L for
    measuring liquidity depth at the current price.

    Key relationships:
    - sqrtPriceX96 = sqrt(price) * 2^96
    - price = (sqrtPriceX96 / 2^96)^2
    - For a swap: delta_x = L * (1/sqrt_price_after - 1/sqrt_price_before)
                  delta_y = L * (sqrt_price_after - sqrt_price_before)

    Attributes:
        sqrt_price_x96: Current sqrtPriceX96 from pool.slot0()
        liquidity: Current active liquidity (uint128)
        tick: Current tick from pool.slot0()
        tick_lower: Lower tick bound for position (default MIN_TICK)
        tick_upper: Upper tick bound for position (default MAX_TICK)
        fee_bps: Pool fee tier in basis points (100, 500, 3000, or 10000)
        liquidity_usd: Estimated USD value of active liquidity
    """

    sqrt_price_x96: int
    liquidity: int
    tick: int | None = None
    tick_lower: int = MIN_TICK
    tick_upper: int = MAX_TICK
    fee_bps: int = 3000  # 0.3% default
    liquidity_usd: Decimal | None = None

    @property
    def fee_factor(self) -> Decimal:
        """Fee factor as a decimal (1 - fee). For 0.3% fee: 0.997"""
        return Decimal("1") - Decimal(self.fee_bps) / Decimal("1000000")

    @property
    def sqrt_price(self) -> Decimal:
        """sqrt(price) as a Decimal."""
        return Decimal(self.sqrt_price_x96) / Decimal(Q96)

    @property
    def price(self) -> Decimal:
        """Current price (token1/token0) as a Decimal."""
        sqrt_p = self.sqrt_price
        return sqrt_p * sqrt_p

    @property
    def tick_spacing(self) -> int:
        """Get tick spacing for this fee tier."""
        return TICK_SPACING_MAP.get(self.fee_bps, 60)

    @property
    def is_full_range(self) -> bool:
        """Check if position covers full tick range."""
        return self.tick_lower <= MIN_TICK and self.tick_upper >= MAX_TICK

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "sqrt_price_x96": str(self.sqrt_price_x96),
            "liquidity": str(self.liquidity),
            "tick": self.tick,
            "tick_lower": self.tick_lower,
            "tick_upper": self.tick_upper,
            "fee_bps": self.fee_bps,
            "liquidity_usd": str(self.liquidity_usd) if self.liquidity_usd else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "V3PoolState":
        """Deserialize from dictionary."""
        return cls(
            sqrt_price_x96=int(data["sqrt_price_x96"]),
            liquidity=int(data["liquidity"]),
            tick=data.get("tick"),
            tick_lower=data.get("tick_lower", MIN_TICK),
            tick_upper=data.get("tick_upper", MAX_TICK),
            fee_bps=data.get("fee_bps", 3000),
            liquidity_usd=Decimal(data["liquidity_usd"]) if data.get("liquidity_usd") else None,
        )


@dataclass
class PriceImpactResult:
    """Result of a price impact calculation.

    Attributes:
        price_impact: Price impact as a decimal (0.01 = 1%)
        effective_price: Effective execution price including impact
        amount_out: Expected output amount
        slippage_bps: Price impact in basis points
        pool_type: Type of pool ("v2" or "v3")
        warning: Optional warning message for high impact trades
    """

    price_impact: Decimal
    effective_price: Decimal
    amount_out: Decimal
    slippage_bps: int
    pool_type: str
    warning: str | None = None

    @property
    def slippage_pct(self) -> Decimal:
        """Price impact as a percentage (1.0 = 1%)."""
        return self.price_impact * Decimal("100")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "price_impact": str(self.price_impact),
            "effective_price": str(self.effective_price),
            "amount_out": str(self.amount_out),
            "slippage_bps": self.slippage_bps,
            "slippage_pct": str(self.slippage_pct),
            "pool_type": self.pool_type,
            "warning": self.warning,
        }


# =============================================================================
# Uniswap V2 Constant-Product Math
# =============================================================================


def calculate_v2_output_amount(
    pool_state: V2PoolState,
    amount_in: Decimal,
) -> Decimal:
    """Calculate output amount for a V2 swap using constant-product formula.

    The V2 AMM formula (with fees):
        amount_out = reserve_out * amount_in_after_fee / (reserve_in + amount_in_after_fee)

    Where:
        amount_in_after_fee = amount_in * (1 - fee)

    This is derived from x * y = k where:
        (reserve_in + amount_in_after_fee) * (reserve_out - amount_out) = reserve_in * reserve_out

    Args:
        pool_state: Current pool reserves and fee configuration
        amount_in: Amount of input token to swap

    Returns:
        Output amount of the other token

    Example:
        pool = V2PoolState(reserve_in=Decimal("1000000"), reserve_out=Decimal("2000"))
        amount_out = calculate_v2_output_amount(pool, Decimal("10000"))
        # For $10k into $1M reserves: expect ~19.8 output tokens
    """
    if amount_in <= 0:
        return Decimal("0")

    if pool_state.reserve_in <= 0 or pool_state.reserve_out <= 0:
        return Decimal("0")

    # Apply fee to input amount
    amount_in_after_fee = amount_in * pool_state.fee_factor

    # V2 constant product formula
    numerator = pool_state.reserve_out * amount_in_after_fee
    denominator = pool_state.reserve_in + amount_in_after_fee

    return numerator / denominator


def calculate_v2_price_impact(
    pool_state: V2PoolState,
    amount_in: Decimal,
    amount_in_usd: Decimal | None = None,
) -> PriceImpactResult:
    """Calculate price impact for a V2 constant-product pool swap.

    Price impact formula for V2:
        price_impact = amount_in / (reserve_in + amount_in)

    This represents how much the price moves due to the trade size
    relative to pool liquidity.

    Args:
        pool_state: Current pool reserves and fee configuration
        amount_in: Amount of input token to swap
        amount_in_usd: USD value of input amount (for USD-based calculations)

    Returns:
        PriceImpactResult with calculated price impact

    Example:
        pool = V2PoolState(
            reserve_in=Decimal("1000000"),
            reserve_out=Decimal("2000"),
            fee_bps=30,
        )
        result = calculate_v2_price_impact(pool, Decimal("50000"))
        print(f"Price impact: {result.slippage_pct}%")  # ~4.76%
    """
    if amount_in <= 0:
        return PriceImpactResult(
            price_impact=Decimal("0"),
            effective_price=pool_state.spot_price,
            amount_out=Decimal("0"),
            slippage_bps=0,
            pool_type="v2",
        )

    # Calculate output amount
    amount_out = calculate_v2_output_amount(pool_state, amount_in)

    # Calculate spot price (without trade)
    spot_price = pool_state.spot_price

    # Calculate effective price (amount_in / amount_out)
    if amount_out > 0:
        effective_price = amount_in / amount_out
    else:
        effective_price = Decimal("0")

    # Price impact = (effective_price - spot_price) / spot_price
    # This represents how much worse the execution price is compared to spot
    if spot_price > 0:
        price_impact = (effective_price - spot_price) / spot_price
    else:
        price_impact = Decimal("0")

    # Alternative formula (more intuitive for reserves):
    # price_impact = amount_in / (reserve_in + amount_in)
    if pool_state.reserve_in > 0:
        reserve_based_impact = amount_in / (pool_state.reserve_in + amount_in)
    else:
        reserve_based_impact = Decimal("0")

    # Use the reserve-based formula as it's more stable for large trades
    # The price-ratio formula can give negative values for certain edge cases
    price_impact = reserve_based_impact

    # Convert to basis points
    slippage_bps = int(price_impact * Decimal("10000"))

    # Generate warning if needed
    warning = None
    if price_impact >= HIGH_SLIPPAGE_WARNING_THRESHOLD:
        trade_size = amount_in_usd if amount_in_usd else amount_in
        warning = f"High price impact: {price_impact * 100:.2f}% for trade size {trade_size}"
    elif price_impact >= LOW_LIQUIDITY_WARNING_THRESHOLD:
        warning = f"Significant price impact: {price_impact * 100:.2f}%"

    return PriceImpactResult(
        price_impact=price_impact,
        effective_price=effective_price,
        amount_out=amount_out,
        slippage_bps=slippage_bps,
        pool_type="v2",
        warning=warning,
    )


def calculate_v2_price_impact_usd(
    total_liquidity_usd: Decimal,
    trade_amount_usd: Decimal,
    fee_bps: int = 30,
) -> PriceImpactResult:
    """Calculate V2 price impact using USD values directly.

    This is a simplified calculation when you only have USD liquidity values
    and not the actual reserve amounts. It assumes a 50/50 reserve split.

    Args:
        total_liquidity_usd: Total pool TVL in USD
        trade_amount_usd: Trade size in USD
        fee_bps: Pool fee in basis points

    Returns:
        PriceImpactResult with calculated price impact

    Example:
        result = calculate_v2_price_impact_usd(
            total_liquidity_usd=Decimal("2000000"),  # $2M pool
            trade_amount_usd=Decimal("50000"),        # $50k trade
            fee_bps=30,
        )
        print(f"Price impact: {result.slippage_pct}%")  # ~4.76%
    """
    if total_liquidity_usd <= 0 or trade_amount_usd <= 0:
        return PriceImpactResult(
            price_impact=Decimal("0"),
            effective_price=Decimal("1"),
            amount_out=trade_amount_usd,
            slippage_bps=0,
            pool_type="v2",
        )

    # Assume 50/50 split, so reserve_in = TVL/2
    reserve_in_usd = total_liquidity_usd / Decimal("2")

    # Price impact = trade_amount / (reserve + trade_amount)
    price_impact = trade_amount_usd / (reserve_in_usd + trade_amount_usd)

    # Calculate output considering fees and price impact
    fee_factor = Decimal("1") - Decimal(fee_bps) / Decimal("10000")
    amount_out_usd = trade_amount_usd * fee_factor * (Decimal("1") - price_impact)

    # Effective price (amount paid per unit received)
    if amount_out_usd > 0:
        effective_price = trade_amount_usd / amount_out_usd
    else:
        effective_price = Decimal("0")

    slippage_bps = int(price_impact * Decimal("10000"))

    warning = None
    if price_impact >= HIGH_SLIPPAGE_WARNING_THRESHOLD:
        warning = f"High price impact: {price_impact * 100:.2f}% for ${trade_amount_usd:,.0f} trade"
    elif price_impact >= LOW_LIQUIDITY_WARNING_THRESHOLD:
        warning = f"Significant price impact: {price_impact * 100:.2f}%"

    return PriceImpactResult(
        price_impact=price_impact,
        effective_price=effective_price,
        amount_out=amount_out_usd,
        slippage_bps=slippage_bps,
        pool_type="v2",
        warning=warning,
    )


# =============================================================================
# Uniswap V3 Concentrated Liquidity Math
# =============================================================================


def tick_to_sqrt_price_x96(tick: int) -> int:
    """Convert a tick to sqrtPriceX96.

    The price at a tick is: price = 1.0001^tick
    So sqrtPrice = sqrt(1.0001^tick) = 1.0001^(tick/2)
    sqrtPriceX96 = sqrtPrice * 2^96

    Args:
        tick: Tick value (int24 in Solidity)

    Returns:
        sqrtPriceX96 as an integer
    """
    # Use high precision calculation
    # sqrt_price = 1.0001^(tick/2)
    import math

    sqrt_price = math.pow(1.0001, tick / 2)
    return int(sqrt_price * Q96)


def sqrt_price_x96_to_tick(sqrt_price_x96: int) -> int:
    """Convert sqrtPriceX96 to the nearest tick.

    The inverse of tick_to_sqrt_price_x96.
    tick = log_1.0001(sqrtPrice^2) = 2 * log_1.0001(sqrtPrice)

    Args:
        sqrt_price_x96: sqrtPriceX96 value

    Returns:
        Tick value
    """
    import math

    sqrt_price = sqrt_price_x96 / Q96
    if sqrt_price <= 0:
        return MIN_TICK
    # tick = log(price) / log(1.0001)
    # price = sqrtPrice^2
    # tick = 2 * log(sqrtPrice) / log(1.0001)
    tick = int(2 * math.log(sqrt_price) / math.log(1.0001))
    return max(MIN_TICK, min(MAX_TICK, tick))


def sqrt_price_x96_to_price(sqrt_price_x96: int) -> Decimal:
    """Convert sqrtPriceX96 to price (token1/token0).

    Args:
        sqrt_price_x96: sqrtPriceX96 value from pool.slot0()

    Returns:
        Price as a Decimal
    """
    sqrt_price = Decimal(sqrt_price_x96) / Decimal(Q96)
    return sqrt_price * sqrt_price


def calculate_v3_delta_amounts(
    liquidity: int,
    sqrt_price_x96_lower: int,
    sqrt_price_x96_upper: int,
) -> tuple[Decimal, Decimal]:
    """Calculate token amounts for a liquidity position in V3.

    For a position with liquidity L between sqrt prices P_a and P_b:
    - delta_x = L * (1/sqrt(P_a) - 1/sqrt(P_b))  [token0 amount]
    - delta_y = L * (sqrt(P_b) - sqrt(P_a))      [token1 amount]

    Args:
        liquidity: Liquidity amount (L)
        sqrt_price_x96_lower: Lower sqrtPriceX96 (P_a)
        sqrt_price_x96_upper: Upper sqrtPriceX96 (P_b)

    Returns:
        Tuple of (delta_token0, delta_token1)
    """
    sqrt_lower = Decimal(sqrt_price_x96_lower) / Decimal(Q96)
    sqrt_upper = Decimal(sqrt_price_x96_upper) / Decimal(Q96)
    L = Decimal(liquidity)

    if sqrt_lower <= 0 or sqrt_upper <= 0:
        return Decimal("0"), Decimal("0")

    # delta_x = L * (1/sqrt_lower - 1/sqrt_upper)
    delta_x = L * (Decimal("1") / sqrt_lower - Decimal("1") / sqrt_upper)

    # delta_y = L * (sqrt_upper - sqrt_lower)
    delta_y = L * (sqrt_upper - sqrt_lower)

    return max(delta_x, Decimal("0")), max(delta_y, Decimal("0"))


def calculate_v3_swap_output(
    pool_state: V3PoolState,
    amount_in: Decimal,
    is_token0_in: bool = True,
) -> tuple[Decimal, int]:
    """Calculate output amount and new sqrtPrice for a V3 swap.

    V3 swap formulas:
    For token0 -> token1 (x -> y, price decreases):
        delta_y = L * (sqrt_P_after - sqrt_P_before)
        sqrt_P_after = sqrt_P_before - delta_x / L

    For token1 -> token0 (y -> x, price increases):
        delta_x = L * (1/sqrt_P_before - 1/sqrt_P_after)
        sqrt_P_after = sqrt_P_before + delta_y / L

    This is a simplified calculation that assumes we stay within one tick range.
    Real V3 swaps may cross multiple tick boundaries.

    Args:
        pool_state: Current pool state
        amount_in: Input amount in token units
        is_token0_in: True if swapping token0 for token1

    Returns:
        Tuple of (amount_out, new_sqrt_price_x96)
    """
    L = Decimal(pool_state.liquidity)
    sqrt_price = pool_state.sqrt_price

    if L <= 0 or sqrt_price <= 0 or amount_in <= 0:
        return Decimal("0"), pool_state.sqrt_price_x96

    # Apply fee to input
    amount_in_after_fee = amount_in * pool_state.fee_factor

    if is_token0_in:
        # Swapping token0 for token1
        # sqrt_P_after = L * sqrt_P_before / (L + delta_x * sqrt_P_before)
        sqrt_price_after = (L * sqrt_price) / (L + amount_in_after_fee * sqrt_price)

        # delta_y = L * (sqrt_P_before - sqrt_P_after)
        amount_out = L * (sqrt_price - sqrt_price_after)
    else:
        # Swapping token1 for token0
        # sqrt_P_after = sqrt_P_before + delta_y / L
        sqrt_price_after = sqrt_price + amount_in_after_fee / L

        # delta_x = L * (1/sqrt_P_before - 1/sqrt_P_after)
        if sqrt_price_after > 0:
            amount_out = L * (Decimal("1") / sqrt_price - Decimal("1") / sqrt_price_after)
        else:
            amount_out = Decimal("0")

    # Convert back to sqrtPriceX96
    new_sqrt_price_x96 = int(sqrt_price_after * Q96)

    return max(amount_out, Decimal("0")), new_sqrt_price_x96


def calculate_v3_price_impact(
    pool_state: V3PoolState,
    amount_in: Decimal,
    is_token0_in: bool = True,
    amount_in_usd: Decimal | None = None,
) -> PriceImpactResult:
    """Calculate price impact for a V3 concentrated liquidity swap.

    Price impact in V3 depends on:
    1. Trade size relative to liquidity depth
    2. Current price position within tick range
    3. Fee tier

    For concentrated liquidity, the effective depth is higher than V2
    when trading within the concentrated range, but can be lower if
    approaching tick boundaries.

    Args:
        pool_state: Current pool state (sqrtPrice, liquidity, ticks)
        amount_in: Amount of input token to swap
        is_token0_in: True if swapping token0 for token1
        amount_in_usd: USD value of input amount (for warnings)

    Returns:
        PriceImpactResult with calculated price impact

    Example:
        v3_state = V3PoolState(
            sqrt_price_x96=79228162514264337593543950336,  # price = 1.0
            liquidity=1000000000000000000,  # 1e18 liquidity
            fee_bps=3000,
        )
        result = calculate_v3_price_impact(v3_state, Decimal("10000"))
        print(f"Price impact: {result.slippage_pct}%")
    """
    if amount_in <= 0 or pool_state.liquidity <= 0:
        return PriceImpactResult(
            price_impact=Decimal("0"),
            effective_price=pool_state.price,
            amount_out=Decimal("0"),
            slippage_bps=0,
            pool_type="v3",
        )

    # Get spot price before trade
    spot_price = pool_state.price

    # Calculate output and new price
    amount_out, new_sqrt_price_x96 = calculate_v3_swap_output(pool_state, amount_in, is_token0_in)

    # Calculate effective price
    if amount_out > 0:
        effective_price = amount_in / amount_out
    else:
        effective_price = Decimal("0")

    # Price impact = |new_price - old_price| / old_price
    new_price = sqrt_price_x96_to_price(new_sqrt_price_x96)
    if spot_price > 0:
        price_impact = abs(new_price - spot_price) / spot_price
    else:
        price_impact = Decimal("0")

    slippage_bps = int(price_impact * Decimal("10000"))

    # Generate warning
    warning = None
    if price_impact >= HIGH_SLIPPAGE_WARNING_THRESHOLD:
        trade_size = amount_in_usd if amount_in_usd else amount_in
        warning = f"High price impact: {price_impact * 100:.2f}% for trade size {trade_size}"
    elif price_impact >= LOW_LIQUIDITY_WARNING_THRESHOLD:
        warning = f"Significant price impact: {price_impact * 100:.2f}%"

    return PriceImpactResult(
        price_impact=price_impact,
        effective_price=effective_price,
        amount_out=amount_out,
        slippage_bps=slippage_bps,
        pool_type="v3",
        warning=warning,
    )


def calculate_v3_price_impact_usd(
    liquidity_usd: Decimal,
    trade_amount_usd: Decimal,
    fee_bps: int = 3000,
    concentration_factor: Decimal = Decimal("1.0"),
) -> PriceImpactResult:
    """Calculate V3 price impact using USD values directly.

    This is a simplified calculation when you only have USD liquidity values.
    It accounts for V3's concentrated liquidity by applying a concentration factor.

    In V3, liquidity is concentrated around the current price, so the effective
    depth is higher than V2 for the same TVL. The concentration_factor adjusts
    for this:
    - 1.0: Full range (similar to V2)
    - 2.0-5.0: Typical concentrated positions
    - 10.0+: Very tight range (higher depth but limited range)

    Price impact formula for V3 (simplified):
        price_impact = sqrt(trade_amount / (liquidity * concentration_factor))

    This sqrt scaling reflects V3's constant product at sqrt price level.

    Args:
        liquidity_usd: Active liquidity in USD (TVL in current tick)
        trade_amount_usd: Trade size in USD
        fee_bps: Pool fee in basis points
        concentration_factor: Multiplier for effective liquidity depth

    Returns:
        PriceImpactResult with calculated price impact

    Example:
        result = calculate_v3_price_impact_usd(
            liquidity_usd=Decimal("5000000"),    # $5M active liquidity
            trade_amount_usd=Decimal("50000"),   # $50k trade
            fee_bps=3000,
            concentration_factor=Decimal("3.0"),  # 3x effective depth
        )
        print(f"Price impact: {result.slippage_pct}%")
    """
    if liquidity_usd <= 0 or trade_amount_usd <= 0:
        return PriceImpactResult(
            price_impact=Decimal("0"),
            effective_price=Decimal("1"),
            amount_out=trade_amount_usd,
            slippage_bps=0,
            pool_type="v3",
        )

    # Effective liquidity with concentration factor
    effective_liquidity = liquidity_usd * concentration_factor

    # V3 price impact scales with sqrt(amount/liquidity)
    # This is because V3 maintains x * y = k in sqrt-space
    ratio = trade_amount_usd / effective_liquidity
    price_impact = _decimal_sqrt(ratio)

    # Apply fee impact
    fee_factor = Decimal("1") - Decimal(fee_bps) / Decimal("1000000")
    amount_out_usd = trade_amount_usd * fee_factor * (Decimal("1") - price_impact)

    if amount_out_usd > 0:
        effective_price = trade_amount_usd / amount_out_usd
    else:
        effective_price = Decimal("0")

    slippage_bps = int(price_impact * Decimal("10000"))

    warning = None
    if price_impact >= HIGH_SLIPPAGE_WARNING_THRESHOLD:
        warning = f"High price impact: {price_impact * 100:.2f}% for ${trade_amount_usd:,.0f} trade"
    elif price_impact >= LOW_LIQUIDITY_WARNING_THRESHOLD:
        warning = f"Significant price impact: {price_impact * 100:.2f}%"

    return PriceImpactResult(
        price_impact=price_impact,
        effective_price=effective_price,
        amount_out=amount_out_usd,
        slippage_bps=slippage_bps,
        pool_type="v3",
        warning=warning,
    )


# =============================================================================
# Helper Functions
# =============================================================================


def _decimal_sqrt(n: Decimal) -> Decimal:
    """Calculate square root of a Decimal using Newton's method.

    Args:
        n: Non-negative Decimal to find square root of

    Returns:
        Square root as Decimal with reasonable precision
    """
    if n < 0:
        raise ValueError("Cannot calculate square root of negative number")
    if n == 0:
        return Decimal("0")

    # Newton's method for square root
    x = n
    two = Decimal("2")

    for _ in range(50):
        x_next = (x + n / x) / two
        if abs(x_next - x) < Decimal("1e-15"):
            break
        x = x_next

    return x


def estimate_concentration_factor(
    tick_lower: int,
    tick_upper: int,
    current_tick: int,
) -> Decimal:
    """Estimate the liquidity concentration factor for a V3 position.

    The concentration factor represents how much more capital efficient
    a concentrated position is compared to a full-range position.

    For a position from tick_lower to tick_upper:
        concentration = sqrt(price_upper/price_lower)

    For full range (MIN_TICK to MAX_TICK), this approaches infinity,
    but we cap it at 1.0 for practical purposes.

    Args:
        tick_lower: Lower tick bound
        tick_upper: Upper tick bound
        current_tick: Current tick (for position status)

    Returns:
        Concentration factor (1.0 for full range, higher for concentrated)
    """
    import math

    # Handle full range or invalid bounds
    if tick_lower >= tick_upper:
        return Decimal("1.0")
    if tick_lower <= MIN_TICK and tick_upper >= MAX_TICK:
        return Decimal("1.0")

    # Calculate price ratio
    # price_ratio = 1.0001^(tick_upper - tick_lower)
    tick_range = tick_upper - tick_lower
    price_ratio = math.pow(1.0001, tick_range)

    # Concentration factor = sqrt(price_ratio)
    concentration = math.sqrt(price_ratio)

    # Cap at reasonable maximum
    return Decimal(str(min(concentration, 100.0)))


def get_pool_type_from_protocol(protocol: str) -> str:
    """Determine pool type (v2 or v3) from protocol name.

    Args:
        protocol: Protocol identifier (e.g., "uniswap_v3", "pancakeswap_v2")

    Returns:
        "v2" or "v3"
    """
    protocol_lower = protocol.lower()

    # V3 protocols
    v3_protocols = [
        "uniswap_v3",
        "uniswapv3",
        "uni_v3",
        "pancakeswap_v3",
        "pancakeswapv3",
        "pcs_v3",
        "sushiswap_v3",
        "sushi_v3",
    ]

    # V2 protocols (includes constant-product forks)
    v2_protocols = [
        "uniswap_v2",
        "uniswapv2",
        "uni_v2",
        "pancakeswap_v2",
        "pancakeswapv2",
        "pcs_v2",
        "sushiswap",
        "sushi",
        "sushiswap_v2",
        "quickswap",
        "spookyswap",
        "traderjoe",
        "traderjoe_v1",
    ]

    if any(p in protocol_lower for p in v3_protocols):
        return "v3"
    if any(p in protocol_lower for p in v2_protocols):
        return "v2"

    # Default to v3 for unknown (more common now)
    return "v3"


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Data classes
    "V2PoolState",
    "V3PoolState",
    "PriceImpactResult",
    # V2 math
    "calculate_v2_output_amount",
    "calculate_v2_price_impact",
    "calculate_v2_price_impact_usd",
    # V3 math
    "tick_to_sqrt_price_x96",
    "sqrt_price_x96_to_tick",
    "sqrt_price_x96_to_price",
    "calculate_v3_delta_amounts",
    "calculate_v3_swap_output",
    "calculate_v3_price_impact",
    "calculate_v3_price_impact_usd",
    # Helper functions
    "estimate_concentration_factor",
    "get_pool_type_from_protocol",
    # Constants
    "Q96",
    "MIN_TICK",
    "MAX_TICK",
    "TICK_SPACING_MAP",
]
