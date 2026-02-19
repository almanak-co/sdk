"""Impermanent Loss calculator for Uniswap V3 concentrated liquidity positions.

This module provides tools for calculating impermanent loss (IL) for Uniswap V3
LP positions. Unlike V2 where IL is straightforward, V3's concentrated liquidity
means IL depends on:

1. The price range (tick_lower to tick_upper) of the position
2. Whether the current price is within, above, or below the range
3. The liquidity amount in the position

Key Concepts:
    - Impermanent Loss: The difference between holding tokens vs providing liquidity
    - Concentrated Liquidity: V3 allows LPs to concentrate liquidity in price ranges
    - Tick: V3 prices are discretized into ticks (price = 1.0001^tick)
    - sqrt(P): Uniswap V3 uses sqrt(price) internally for calculations

Example:
    from almanak.framework.backtesting.pnl.calculators.impermanent_loss import ImpermanentLossCalculator

    calc = ImpermanentLossCalculator()

    # Calculate IL for a position
    il_pct, token0_amt, token1_amt = calc.calculate_il_v3(
        entry_price=Decimal("2000"),
        current_price=Decimal("2200"),
        tick_lower=-887220,
        tick_upper=887220,
        liquidity=Decimal("1000000"),
    )

    # Calculate fee APR
    apr = calc.calculate_fee_apr(
        fees_earned_usd=Decimal("500"),
        position_value_usd=Decimal("10000"),
        duration_days=Decimal("30"),
    )

References:
    - Uniswap V3 Whitepaper: https://uniswap.org/whitepaper-v3.pdf
    - V3 Math: https://atiselsts.github.io/pdfs/uniswap-v3-liquidity-math.pdf
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

# Uniswap V3 tick constants
TICK_BASE = Decimal("1.0001")
MIN_TICK = -887272
MAX_TICK = 887272


@dataclass
class ImpermanentLossCalculator:
    """Calculator for impermanent loss in Uniswap V3 positions.

    This calculator implements the Uniswap V3 concentrated liquidity math
    to determine:
    - Impermanent loss percentage compared to holding
    - Current token amounts in the position
    - Fee APR based on earned fees and position value

    The key insight is that in V3, a position only provides liquidity
    within its tick range. Outside that range, the position is 100%
    in one token (the one that depreciated in value).

    Attributes:
        precision: Number of decimal places for calculations (default 28)

    Example:
        calc = ImpermanentLossCalculator()

        # Full range position (like V2)
        il, t0, t1 = calc.calculate_il_v3(
            entry_price=Decimal("2000"),
            current_price=Decimal("2500"),
            tick_lower=MIN_TICK,
            tick_upper=MAX_TICK,
            liquidity=Decimal("1000000"),
        )
        print(f"IL: {il:.2%}, Token0: {t0}, Token1: {t1}")
    """

    precision: int = 28

    def calculate_il_v3(
        self,
        entry_price: Decimal,
        current_price: Decimal,
        tick_lower: int,
        tick_upper: int,
        liquidity: Decimal,
    ) -> tuple[Decimal, Decimal, Decimal]:
        """Calculate impermanent loss for a Uniswap V3 position.

        Computes the impermanent loss percentage and current token amounts
        for a V3 concentrated liquidity position. IL is calculated as:

        IL = 1 - (current_position_value / hold_value)

        Where:
        - current_position_value = value of tokens in the LP position now
        - hold_value = value if you had just held the original tokens

        Args:
            entry_price: Price of token0 in terms of token1 at entry
                (e.g., 2000 for ETH if token1 is USDC)
            current_price: Current price of token0 in terms of token1
            tick_lower: Lower tick boundary of the position
            tick_upper: Upper tick boundary of the position
            liquidity: Liquidity units of the position (L in V3 math)

        Returns:
            Tuple of:
            - il_percentage: Impermanent loss as a decimal (0.05 = 5% loss)
            - token0_amount: Current amount of token0 in the position
            - token1_amount: Current amount of token1 in the position

        Notes:
            - IL is always >= 0 (it's a loss, not gain)
            - If current_price is outside the range, the position is 100%
              in one token
            - Full range positions (MIN_TICK to MAX_TICK) behave like V2
        """
        # Validate inputs
        if entry_price <= 0 or current_price <= 0:
            return Decimal("0"), Decimal("0"), Decimal("0")

        if liquidity <= 0:
            return Decimal("0"), Decimal("0"), Decimal("0")

        if tick_lower >= tick_upper:
            return Decimal("0"), Decimal("0"), Decimal("0")

        # Convert ticks to sqrt prices
        sqrt_price_lower = self._tick_to_sqrt_price(tick_lower)
        sqrt_price_upper = self._tick_to_sqrt_price(tick_upper)
        sqrt_price_entry = self._decimal_sqrt(entry_price)
        sqrt_price_current = self._decimal_sqrt(current_price)

        # Calculate entry token amounts
        entry_token0, entry_token1 = self._get_token_amounts(
            liquidity, sqrt_price_entry, sqrt_price_lower, sqrt_price_upper
        )

        # Calculate current token amounts
        current_token0, current_token1 = self._get_token_amounts(
            liquidity, sqrt_price_current, sqrt_price_lower, sqrt_price_upper
        )

        # Calculate hold value (what you would have if you just held the tokens)
        # If you held entry tokens, their value now is:
        # hold_value = entry_token0 * current_price + entry_token1
        hold_value = entry_token0 * current_price + entry_token1

        # Calculate current position value
        # current_value = current_token0 * current_price + current_token1
        current_value = current_token0 * current_price + current_token1

        # Calculate IL percentage
        # IL = (hold_value - current_value) / hold_value
        if hold_value > 0:
            il_percentage = (hold_value - current_value) / hold_value
            # IL should be non-negative (it's always a loss or zero)
            il_percentage = max(Decimal("0"), il_percentage)
        else:
            il_percentage = Decimal("0")

        return il_percentage, current_token0, current_token1

    def _get_token_amounts(
        self,
        liquidity: Decimal,
        sqrt_price: Decimal,
        sqrt_price_lower: Decimal,
        sqrt_price_upper: Decimal,
    ) -> tuple[Decimal, Decimal]:
        """Calculate token amounts for a position at a given price.

        Uses the Uniswap V3 formulas for token amounts based on the
        current price relative to the position's range.

        Args:
            liquidity: The L value for the position
            sqrt_price: Current sqrt(price)
            sqrt_price_lower: sqrt(price) at lower tick
            sqrt_price_upper: sqrt(price) at upper tick

        Returns:
            Tuple of (token0_amount, token1_amount)
        """
        # Case 1: Price below range - all token0
        if sqrt_price <= sqrt_price_lower:
            # token0 = L * (1/sqrt_price_lower - 1/sqrt_price_upper)
            # token1 = 0
            token0 = liquidity * (Decimal("1") / sqrt_price_lower - Decimal("1") / sqrt_price_upper)
            token1 = Decimal("0")

        # Case 2: Price above range - all token1
        elif sqrt_price >= sqrt_price_upper:
            # token0 = 0
            # token1 = L * (sqrt_price_upper - sqrt_price_lower)
            token0 = Decimal("0")
            token1 = liquidity * (sqrt_price_upper - sqrt_price_lower)

        # Case 3: Price within range - mix of both tokens
        else:
            # token0 = L * (1/sqrt_price - 1/sqrt_price_upper)
            # token1 = L * (sqrt_price - sqrt_price_lower)
            token0 = liquidity * (Decimal("1") / sqrt_price - Decimal("1") / sqrt_price_upper)
            token1 = liquidity * (sqrt_price - sqrt_price_lower)

        return token0, token1

    def _tick_to_sqrt_price(self, tick: int) -> Decimal:
        """Convert a tick to sqrt(price).

        In Uniswap V3: price = 1.0001^tick
        Therefore: sqrt(price) = 1.0001^(tick/2)

        Args:
            tick: The tick value

        Returns:
            sqrt(price) as a Decimal
        """
        # Use the formula: sqrt(price) = 1.0001^(tick/2)
        # We compute this as: 1.0001^tick then take sqrt
        # Or equivalently: (1.0001^0.5)^tick

        # For precision, we use: sqrt(price) = 1.0001^(tick/2)
        half_tick = Decimal(tick) / Decimal("2")

        # Compute 1.0001^(tick/2) using exp/ln for precision
        # ln(1.0001) ≈ 0.00009999500033
        ln_tick_base = Decimal("0.0000999950003333083340832824")
        exponent = half_tick * ln_tick_base

        # e^x approximation using Taylor series for small x
        result = self._decimal_exp(exponent)

        return result

    def _decimal_sqrt(self, n: Decimal) -> Decimal:
        """Calculate square root of a Decimal using Newton's method.

        Args:
            n: Non-negative Decimal to find square root of

        Returns:
            Square root as Decimal
        """
        if n < 0:
            raise ValueError("Cannot calculate square root of negative number")
        if n == 0:
            return Decimal("0")

        # Newton's method: x_{n+1} = (x_n + n/x_n) / 2
        x = n
        two = Decimal("2")

        for _ in range(100):
            x_next = (x + n / x) / two
            if abs(x_next - x) < Decimal(f"1e-{self.precision}"):
                break
            x = x_next

        return x

    def _decimal_exp(self, x: Decimal) -> Decimal:
        """Calculate e^x for a Decimal using Taylor series.

        Args:
            x: The exponent

        Returns:
            e^x as Decimal
        """
        # For very small or moderate x, use Taylor series
        # e^x = 1 + x + x^2/2! + x^3/3! + ...

        # If x is too large, we need range reduction
        # e^x = e^(n*ln2) * e^r = 2^n * e^r where r is small
        if abs(x) > Decimal("10"):
            # Range reduction for large exponents
            ln2 = Decimal("0.693147180559945309417232121458")
            n = int(x / ln2)
            r = x - Decimal(n) * ln2
            return (Decimal("2") ** n) * self._decimal_exp(r)

        result = Decimal("1")
        term = Decimal("1")

        for i in range(1, 150):
            term = term * x / Decimal(i)
            result += term
            if abs(term) < Decimal(f"1e-{self.precision}"):
                break

        return result

    def calculate_fee_apr(
        self,
        fees_earned_usd: Decimal,
        position_value_usd: Decimal,
        duration_days: Decimal,
    ) -> Decimal:
        """Calculate annualized fee APR for an LP position.

        Computes the annualized percentage return from trading fees
        earned by the position, based on actual earnings and duration.

        Formula: APR = (fees / position_value) * (365 / duration_days)

        Args:
            fees_earned_usd: Total fees earned in USD
            position_value_usd: Total position value in USD
            duration_days: Number of days the position has been open

        Returns:
            Annualized APR as a decimal (0.10 = 10% APR)

        Example:
            # $500 earned on $10,000 over 30 days
            apr = calc.calculate_fee_apr(
                fees_earned_usd=Decimal("500"),
                position_value_usd=Decimal("10000"),
                duration_days=Decimal("30"),
            )
            # apr = 0.05 * (365/30) = 0.608 = 60.8% APR
        """
        if position_value_usd <= 0 or duration_days <= 0:
            return Decimal("0")

        # Calculate periodic return
        periodic_return = fees_earned_usd / position_value_usd

        # Annualize
        periods_per_year = Decimal("365") / duration_days
        annual_apr = periodic_return * periods_per_year

        return annual_apr

    def calculate_il_for_price_change(self, price_ratio: Decimal, tick_lower: int, tick_upper: int) -> Decimal:
        """Calculate IL for a given price change ratio within a range.

        This is a simplified IL calculation useful for quick estimates
        when you only know the price ratio (current/entry) and tick range.

        For a full-range V3 position (equivalent to V2), IL is:
        IL = 2 * sqrt(price_ratio) / (1 + price_ratio) - 1

        For concentrated liquidity, the IL depends on where the price
        moves relative to the range.

        Args:
            price_ratio: Ratio of current_price / entry_price
            tick_lower: Lower tick boundary
            tick_upper: Upper tick boundary

        Returns:
            IL as a decimal (0.05 = 5% loss)
        """
        if price_ratio <= 0:
            return Decimal("0")

        # For full range (or if we want simplified V2-style IL)
        # IL = 2 * sqrt(k) / (1 + k) - 1 where k = price_ratio
        sqrt_ratio = self._decimal_sqrt(price_ratio)
        il_v2 = Decimal("2") * sqrt_ratio / (Decimal("1") + price_ratio) - Decimal("1")

        # Return absolute value (IL is always positive or zero)
        return abs(il_v2)

    def get_position_value_usd(
        self,
        token0_amount: Decimal,
        token1_amount: Decimal,
        token0_price_usd: Decimal,
        token1_price_usd: Decimal = Decimal("1"),
    ) -> Decimal:
        """Calculate the USD value of a position given token amounts.

        Args:
            token0_amount: Amount of token0
            token1_amount: Amount of token1
            token0_price_usd: Price of token0 in USD
            token1_price_usd: Price of token1 in USD (default 1 for stablecoins)

        Returns:
            Total position value in USD
        """
        return token0_amount * token0_price_usd + token1_amount * token1_price_usd

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "calculator_name": "impermanent_loss",
            "precision": self.precision,
        }


__all__ = [
    "ImpermanentLossCalculator",
    "TICK_BASE",
    "MIN_TICK",
    "MAX_TICK",
]
