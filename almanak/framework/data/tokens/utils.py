"""Utility functions for token amount normalization.

This module provides functions to convert between raw blockchain amounts
(in wei/smallest units) and human-readable decimal amounts.

Key Functions:
    - normalize: Convert raw amount to human-readable Decimal
    - denormalize: Convert human-readable Decimal to raw int

Example:
    from decimal import Decimal
    from almanak.framework.data.tokens.utils import normalize, denormalize

    # Convert 1 USDC (6 decimals) from raw to human-readable
    raw_usdc = 1_000_000  # 1 USDC in raw units
    amount = normalize(raw_usdc, 6)  # Decimal("1.000000")

    # Convert back to raw
    raw_again = denormalize(amount, 6)  # 1000000
"""

from decimal import Decimal


def normalize(raw_amount: int, decimals: int) -> Decimal:
    """Convert a raw blockchain amount to a human-readable Decimal.

    Converts an integer amount (in wei or smallest token units) to a
    human-readable Decimal value by dividing by 10^decimals.

    Args:
        raw_amount: The raw amount in smallest units (e.g., wei)
        decimals: Number of decimal places for the token (e.g., 18 for ETH, 6 for USDC)

    Returns:
        Decimal representation of the human-readable amount

    Raises:
        ValueError: If decimals is negative or exceeds 77

    Example:
        # 1 ETH = 10^18 wei
        normalize(1_000_000_000_000_000_000, 18)  # Decimal("1")

        # 1 USDC = 10^6 units
        normalize(1_000_000, 6)  # Decimal("1")

        # 0.5 WBTC = 0.5 * 10^8 = 50_000_000 satoshis
        normalize(50_000_000, 8)  # Decimal("0.5")
    """
    if decimals < 0:
        raise ValueError(f"Decimals cannot be negative: {decimals}")
    if decimals > 77:
        raise ValueError(f"Decimals cannot exceed 77: {decimals}")

    if decimals == 0:
        return Decimal(raw_amount)

    divisor = Decimal(10) ** decimals
    return Decimal(raw_amount) / divisor


def denormalize(amount: Decimal, decimals: int) -> int:
    """Convert a human-readable Decimal to a raw blockchain amount.

    Converts a human-readable Decimal value to an integer amount
    (in wei or smallest token units) by multiplying by 10^decimals.

    Args:
        amount: Human-readable Decimal amount (e.g., Decimal("1.5"))
        decimals: Number of decimal places for the token (e.g., 18 for ETH, 6 for USDC)

    Returns:
        Integer raw amount in smallest units (e.g., wei)

    Raises:
        ValueError: If decimals is negative or exceeds 77

    Example:
        # 1 ETH to wei
        denormalize(Decimal("1"), 18)  # 1_000_000_000_000_000_000

        # 1.5 USDC to raw units
        denormalize(Decimal("1.5"), 6)  # 1_500_000

        # 0.00000001 BTC to satoshis
        denormalize(Decimal("0.00000001"), 8)  # 1
    """
    if decimals < 0:
        raise ValueError(f"Decimals cannot be negative: {decimals}")
    if decimals > 77:
        raise ValueError(f"Decimals cannot exceed 77: {decimals}")

    if decimals == 0:
        return int(amount)

    multiplier = Decimal(10) ** decimals
    return int(amount * multiplier)


__all__ = [
    "normalize",
    "denormalize",
]
