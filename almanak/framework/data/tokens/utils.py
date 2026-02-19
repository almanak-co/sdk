"""Utility functions for token amount normalization.

This module provides functions to convert between raw blockchain amounts
(in wei/smallest units) and human-readable decimal amounts.

Key Functions:
    - normalize: Convert raw amount to human-readable Decimal
    - denormalize: Convert human-readable Decimal to raw int
    - normalize_token: Normalize using token registry lookup

Example:
    from decimal import Decimal
    from almanak.framework.data.tokens.utils import normalize, denormalize, normalize_token
    from almanak.framework.data.tokens import get_default_registry

    # Convert 1 USDC (6 decimals) from raw to human-readable
    raw_usdc = 1_000_000  # 1 USDC in raw units
    amount = normalize(raw_usdc, 6)  # Decimal("1.000000")

    # Convert back to raw
    raw_again = denormalize(amount, 6)  # 1000000

    # Using registry for automatic decimal lookup
    registry = get_default_registry()
    amount = normalize_token(1_000_000_000_000_000_000, "ETH", registry)  # Decimal("1")
"""

from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .registry import TokenRegistry


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


def normalize_token(raw_amount: int, token: str, registry: "TokenRegistry") -> Decimal:
    """Convert a raw amount to human-readable using token registry.

    Looks up the token in the registry to get its decimal places,
    then converts the raw amount to a human-readable Decimal.

    Args:
        raw_amount: The raw amount in smallest units (e.g., wei)
        token: Token symbol (e.g., "ETH", "USDC", "WBTC")
        registry: TokenRegistry containing token metadata

    Returns:
        Decimal representation of the human-readable amount

    Raises:
        KeyError: If token is not found in the registry

    Example:
        from almanak.framework.data.tokens import get_default_registry

        registry = get_default_registry()

        # 1 ETH from wei
        normalize_token(1_000_000_000_000_000_000, "ETH", registry)  # Decimal("1")

        # 1 USDC from raw units (6 decimals)
        normalize_token(1_000_000, "USDC", registry)  # Decimal("1")
    """
    token_info = registry.get(token)
    if token_info is None:
        raise KeyError(f"Token not found in registry: {token}")

    return normalize(raw_amount, token_info.decimals)


def denormalize_token(amount: Decimal, token: str, registry: "TokenRegistry") -> int:
    """Convert a human-readable amount to raw using token registry.

    Looks up the token in the registry to get its decimal places,
    then converts the human-readable Decimal to raw units.

    Args:
        amount: Human-readable Decimal amount
        token: Token symbol (e.g., "ETH", "USDC", "WBTC")
        registry: TokenRegistry containing token metadata

    Returns:
        Integer raw amount in smallest units (e.g., wei)

    Raises:
        KeyError: If token is not found in the registry

    Example:
        from almanak.framework.data.tokens import get_default_registry

        registry = get_default_registry()

        # 1 ETH to wei
        denormalize_token(Decimal("1"), "ETH", registry)  # 1_000_000_000_000_000_000

        # 1 USDC to raw units (6 decimals)
        denormalize_token(Decimal("1"), "USDC", registry)  # 1_000_000
    """
    token_info = registry.get(token)
    if token_info is None:
        raise KeyError(f"Token not found in registry: {token}")

    return denormalize(amount, token_info.decimals)


__all__ = [
    "normalize",
    "denormalize",
    "normalize_token",
    "denormalize_token",
]
