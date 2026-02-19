"""Formatting utilities for user-friendly log output.

UX-First Logging: All amounts shown with USD context where possible.
Safety-First: Clear visibility into expected vs actual values.

This module provides formatting functions used throughout the framework
to ensure consistent, human-readable log output that users can understand
at a glance without manual calculations.

Usage:
    from almanak.framework.utils.log_formatters import (
        format_usd,
        format_token_amount,
        format_token_with_usd,
        format_gas_cost,
        format_slippage,
        format_balance_delta,
    )

    # Format USD value
    >>> format_usd(1234.56)
    '$1,234.56'

    # Format token amount
    >>> format_token_amount(1_500_000_000_000_000_000, "ETH", 18)
    '1.5000 ETH'

    # Format with USD context
    >>> format_token_with_usd(1_500_000_000_000_000_000, "ETH", 18, Decimal("3400"))
    '1.5000 ETH ($5,100.00)'

    # Format slippage
    >>> format_slippage(Decimal("100"), Decimal("98"))
    '-2.00% (worse)'
    >>> format_slippage(Decimal("100"), Decimal("102"))
    '+2.00% (better)'
"""

from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


def format_usd(amount: Decimal | float | int | None) -> str:
    """Format amount as USD string with comma separators.

    Args:
        amount: The amount in USD (can be Decimal, float, int, or None)

    Returns:
        Formatted USD string like '$1,234.56' or 'N/A' if amount is None

    Examples:
        >>> format_usd(1234.56)
        '$1,234.56'
        >>> format_usd(Decimal("0.01"))
        '$0.01'
        >>> format_usd(1000000)
        '$1,000,000.00'
        >>> format_usd(None)
        'N/A'
    """
    if amount is None:
        return "N/A"

    value = Decimal(str(amount))

    # Handle very small amounts
    if abs(value) < Decimal("0.01") and value != 0:
        return f"${value:.6f}"

    # Standard formatting with 2 decimal places
    return f"${value:,.2f}"


def format_token_amount(
    amount: Decimal | int | None,
    symbol: str,
    decimals: int = 18,
    max_decimals: int = 4,
) -> str:
    """Format token amount with symbol in human-readable units.

    Converts from smallest units (wei) to human units.

    Args:
        amount: The amount in smallest units (wei for ETH, etc.)
        symbol: The token symbol (e.g., "ETH", "USDC")
        decimals: Token decimals (default 18 for ETH)
        max_decimals: Maximum decimal places to show (default 4)

    Returns:
        Formatted token string like '1.5000 ETH' or 'N/A' if amount is None

    Examples:
        >>> format_token_amount(1_500_000_000_000_000_000, "ETH", 18)
        '1.5000 ETH'
        >>> format_token_amount(100_000_000, "USDC", 6)
        '100.0000 USDC'
        >>> format_token_amount(None, "ETH", 18)
        'N/A ETH'
    """
    if amount is None:
        return f"N/A {symbol}"

    # Convert from smallest units to human units
    human_amount = Decimal(str(amount)) / Decimal(10**decimals)

    # Determine appropriate decimal places
    if human_amount == 0:
        return f"0 {symbol}"

    # For very small amounts, show more precision
    if abs(human_amount) < Decimal("0.0001"):
        return f"{human_amount:.8f} {symbol}"

    # Standard formatting
    return f"{human_amount:.{max_decimals}f} {symbol}"


def format_token_amount_human(
    amount: Decimal | float | int | None,
    symbol: str,
    max_decimals: int = 4,
) -> str:
    """Format token amount that's already in human units (not wei).

    Use this when the amount is already converted (e.g., from strategy configs).

    Args:
        amount: The amount in human units (e.g., 1.5 for 1.5 ETH)
        symbol: The token symbol
        max_decimals: Maximum decimal places to show

    Returns:
        Formatted token string

    Examples:
        >>> format_token_amount_human(1.5, "ETH")
        '1.5000 ETH'
        >>> format_token_amount_human(Decimal("100.50"), "USDC")
        '100.5000 USDC'
    """
    if amount is None:
        return f"N/A {symbol}"

    value = Decimal(str(amount))

    if value == 0:
        return f"0 {symbol}"

    if abs(value) < Decimal("0.0001"):
        return f"{value:.8f} {symbol}"

    return f"{value:.{max_decimals}f} {symbol}"


def format_token_with_usd(
    amount: Decimal | int | None,
    symbol: str,
    decimals: int,
    usd_price: Decimal | float | None,
    max_decimals: int = 4,
) -> str:
    """Format token amount with USD value in parentheses.

    Args:
        amount: The amount in smallest units (wei)
        symbol: The token symbol
        decimals: Token decimals
        usd_price: Price per token in USD
        max_decimals: Maximum decimal places for token amount

    Returns:
        Formatted string like '1.5000 ETH ($5,100.00)'

    Examples:
        >>> format_token_with_usd(1_500_000_000_000_000_000, "ETH", 18, Decimal("3400"))
        '1.5000 ETH ($5,100.00)'
        >>> format_token_with_usd(100_000_000, "USDC", 6, Decimal("1"))
        '100.0000 USDC ($100.00)'
    """
    token_str = format_token_amount(amount, symbol, decimals, max_decimals)

    if amount is None or usd_price is None:
        return token_str

    human_amount = Decimal(str(amount)) / Decimal(10**decimals)
    usd_value = human_amount * Decimal(str(usd_price))
    usd_str = format_usd(usd_value)

    return f"{token_str} ({usd_str})"


def format_token_with_usd_human(
    amount: Decimal | float | int | None,
    symbol: str,
    usd_price: Decimal | float | None,
    max_decimals: int = 4,
) -> str:
    """Format token amount (in human units) with USD value.

    Args:
        amount: The amount in human units (not wei)
        symbol: The token symbol
        usd_price: Price per token in USD
        max_decimals: Maximum decimal places for token amount

    Returns:
        Formatted string like '1.5000 ETH ($5,100.00)'
    """
    token_str = format_token_amount_human(amount, symbol, max_decimals)

    if amount is None or usd_price is None:
        return token_str

    usd_value = Decimal(str(amount)) * Decimal(str(usd_price))
    usd_str = format_usd(usd_value)

    return f"{token_str} ({usd_str})"


def format_gas_cost(
    gas_used: int,
    gas_price_gwei: float | Decimal | None = None,
    eth_price_usd: Decimal | float | None = None,
) -> str:
    """Format gas usage with optional USD cost.

    Args:
        gas_used: Gas units used
        gas_price_gwei: Gas price in Gwei (optional)
        eth_price_usd: ETH price in USD (optional)

    Returns:
        Formatted string like '131,114 gas (~$2.45)' or '131,114 gas'

    Examples:
        >>> format_gas_cost(131114, 20, Decimal("3400"))
        '131,114 gas (~$8.92)'
        >>> format_gas_cost(131114)
        '131,114 gas'
    """
    gas_str = f"{gas_used:,} gas"

    if gas_price_gwei is None or eth_price_usd is None:
        return gas_str

    # Calculate gas cost in ETH then USD
    # gas_used * gas_price_gwei * 1e-9 = gas cost in ETH
    gas_cost_eth = Decimal(str(gas_used)) * Decimal(str(gas_price_gwei)) * Decimal("1e-9")
    gas_cost_usd = gas_cost_eth * Decimal(str(eth_price_usd))

    return f"{gas_str} (~{format_usd(gas_cost_usd)})"


def format_slippage(
    expected: Decimal | float | int,
    actual: Decimal | float | int,
) -> str:
    """Format slippage comparison between expected and actual.

    Positive slippage means actual > expected (better for receives).
    Negative slippage means actual < expected (worse for receives).

    Args:
        expected: Expected amount
        actual: Actual amount received

    Returns:
        Formatted slippage string like '+2.00% (better)' or '-1.50% (worse)'

    Examples:
        >>> format_slippage(100, 102)
        '+2.00% (better)'
        >>> format_slippage(100, 98)
        '-2.00% (worse)'
        >>> format_slippage(100, 100)
        '0.00% (exact)'
    """
    expected_d = Decimal(str(expected))
    actual_d = Decimal(str(actual))

    if expected_d == 0:
        return "N/A (expected was 0)"

    slippage_pct = ((actual_d - expected_d) / expected_d) * 100

    if slippage_pct > 0:
        return f"+{slippage_pct:.2f}% (better)"
    elif slippage_pct < 0:
        return f"{slippage_pct:.2f}% (worse)"
    else:
        return "0.00% (exact)"


def format_slippage_bps(bps: int | float) -> str:
    """Format slippage in basis points.

    Args:
        bps: Slippage in basis points (1 bp = 0.01%)

    Returns:
        Formatted string like '50bp (0.50%)'
    """
    pct = Decimal(str(bps)) / 100
    return f"{int(bps)}bp ({pct:.2f}%)"


def format_balance_delta(
    before: dict[str, Decimal],
    after: dict[str, Decimal],
    prices: dict[str, Decimal] | None = None,
) -> str:
    """Format balance changes between before and after states.

    Args:
        before: Token balances before (symbol -> amount in human units)
        after: Token balances after (symbol -> amount in human units)
        prices: Optional token prices for USD values (symbol -> USD price)

    Returns:
        Formatted delta string like 'USDC: -$100.00 | WETH: +0.034 (+$115.60)'

    Examples:
        >>> format_balance_delta(
        ...     {"USDC": Decimal("1000"), "WETH": Decimal("0.5")},
        ...     {"USDC": Decimal("900"), "WETH": Decimal("0.534")},
        ...     {"USDC": Decimal("1"), "WETH": Decimal("3400")}
        ... )
        'USDC: -100.0000 (-$100.00) | WETH: +0.0340 (+$115.60)'
    """
    parts = []
    all_tokens = set(before.keys()) | set(after.keys())

    for token in sorted(all_tokens):
        before_amt = before.get(token, Decimal("0"))
        after_amt = after.get(token, Decimal("0"))
        delta = after_amt - before_amt

        if delta == 0:
            continue

        # Format delta amount
        sign = "+" if delta > 0 else ""
        delta_str = f"{sign}{delta:.4f}"

        # Add USD value if price available
        if prices and token in prices:
            delta_usd = delta * prices[token]
            usd_sign = "+" if delta_usd > 0 else ""
            delta_str += f" ({usd_sign}{format_usd(delta_usd)})"

        parts.append(f"{token}: {delta_str}")

    return " | ".join(parts) if parts else "No changes"


def format_balance_summary(
    balances: dict[str, Decimal],
    prices: dict[str, Decimal] | None = None,
    max_tokens: int = 5,
) -> str:
    """Format wallet balance summary.

    Args:
        balances: Token balances (symbol -> amount in human units)
        prices: Optional token prices for USD values
        max_tokens: Maximum number of tokens to show

    Returns:
        Formatted balance string like 'USDC: $1,000.00 | WETH: 0.500 ($1,700.00)'
    """
    parts = []

    # Sort by USD value if prices available, otherwise alphabetically
    if prices:
        tokens = sorted(
            balances.keys(),
            key=lambda t: balances.get(t, 0) * prices.get(t, 0),
            reverse=True,
        )
    else:
        tokens = sorted(balances.keys())

    for token in tokens[:max_tokens]:
        amount = balances.get(token, Decimal("0"))

        if amount == 0:
            continue

        if prices and token in prices:
            usd_value = amount * prices[token]
            parts.append(f"{token}: {amount:.4f} ({format_usd(usd_value)})")
        else:
            parts.append(f"{token}: {amount:.4f}")

    if len(tokens) > max_tokens:
        parts.append(f"... +{len(tokens) - max_tokens} more")

    return " | ".join(parts) if parts else "Empty wallet"


def format_price(
    price: Decimal | float | None,
    base: str,
    quote: str = "USD",
) -> str:
    """Format price with base/quote pair.

    Args:
        price: The price value
        base: Base token symbol
        quote: Quote token symbol (default USD)

    Returns:
        Formatted price like '3,456.78 USD/ETH'
    """
    if price is None:
        return f"N/A {quote}/{base}"

    price_d = Decimal(str(price))
    return f"{price_d:,.2f} {quote}/{base}"


def format_health_factor(hf: Decimal | float | None) -> str:
    """Format Aave health factor with warning indicators.

    Args:
        hf: Health factor value (> 1 is safe, < 1 is liquidatable)

    Returns:
        Formatted string like '1.85' or '1.15 (low)'
    """
    if hf is None:
        return "N/A"

    hf_d = Decimal(str(hf))

    if hf_d < Decimal("1.0"):
        return f"{hf_d:.2f} (LIQUIDATABLE)"
    elif hf_d < Decimal("1.25"):
        return f"{hf_d:.2f} (low)"
    elif hf_d < Decimal("1.5"):
        return f"{hf_d:.2f} (moderate)"
    else:
        return f"{hf_d:.2f}"


def format_leverage(leverage: Decimal | float | None) -> str:
    """Format leverage multiplier.

    Args:
        leverage: Leverage value (e.g., 2.0 for 2x)

    Returns:
        Formatted string like '2.0x'
    """
    if leverage is None:
        return "N/A"

    return f"{float(leverage):.1f}x"


def format_percentage(
    value: Decimal | float | None,
    decimals: int = 2,
) -> str:
    """Format value as percentage.

    Args:
        value: The percentage value (e.g., 0.05 for 5%)
        decimals: Number of decimal places

    Returns:
        Formatted percentage like '5.00%'
    """
    if value is None:
        return "N/A"

    pct = Decimal(str(value)) * 100
    return f"{pct:.{decimals}f}%"


def format_tx_hash(tx_hash: str, truncate: bool = True) -> str:
    """Format transaction hash, optionally truncating for display.

    Args:
        tx_hash: Full transaction hash
        truncate: Whether to truncate (default True)

    Returns:
        Formatted hash like '0xabc...123' or full hash
    """
    if not tx_hash:
        return "N/A"

    if truncate and len(tx_hash) > 16:
        return f"{tx_hash[:6]}...{tx_hash[-4:]}"

    return tx_hash


def format_address(address: str, truncate: bool = True) -> str:
    """Format wallet/contract address, optionally truncating.

    Args:
        address: Full address
        truncate: Whether to truncate (default True)

    Returns:
        Formatted address like '0xabc...123'
    """
    return format_tx_hash(address, truncate)


def format_intent_type_emoji(intent_type: str) -> str:
    """Get emoji prefix for intent type.

    Args:
        intent_type: The intent type string (e.g., "SWAP", "SUPPLY")

    Returns:
        Emoji-prefixed intent type
    """
    emoji_map = {
        "SWAP": "🔄",
        "SUPPLY": "📥",
        "BORROW": "💳",
        "REPAY": "💰",
        "WITHDRAW": "📤",
        "LP_OPEN": "🏊",
        "LP_CLOSE": "🚪",
        "LP_REBALANCE": "⚖️",
        "PERP_OPEN": "📈",
        "PERP_CLOSE": "📉",
        "PERP_MODIFY": "🔧",
        "BRIDGE": "🌉",
        "HOLD": "⏸️",
        "APPROVE": "✅",
    }
    emoji = emoji_map.get(intent_type.upper(), "📋")
    return f"{emoji} {intent_type}"


def format_execution_status(success: bool) -> str:
    """Format execution status with emoji.

    Args:
        success: Whether execution succeeded

    Returns:
        Status string like '✅ SUCCESS' or '❌ FAILED'
    """
    if success:
        return "✅ SUCCESS"
    else:
        return "❌ FAILED"


def format_warning(message: str) -> str:
    """Format warning message with emoji.

    Args:
        message: Warning message

    Returns:
        Warning string like '⚠️ message'
    """
    return f"⚠️ {message}"


def format_error(message: str) -> str:
    """Format error message with emoji.

    Args:
        message: Error message

    Returns:
        Error string like '❌ message'
    """
    return f"❌ {message}"


def format_info(message: str) -> str:
    """Format info message with emoji.

    Args:
        message: Info message

    Returns:
        Info string like 'ℹ️ message'
    """
    return f"ℹ️ {message}"


def wei_to_human(amount_wei: int, decimals: int = 18) -> Decimal:
    """Convert wei amount to human-readable decimal.

    Args:
        amount_wei: Amount in smallest units
        decimals: Token decimals

    Returns:
        Decimal in human units
    """
    return Decimal(str(amount_wei)) / Decimal(10**decimals)


def human_to_wei(amount: Decimal | float, decimals: int = 18) -> int:
    """Convert human-readable amount to wei.

    Args:
        amount: Amount in human units
        decimals: Token decimals

    Returns:
        Integer in smallest units (wei)
    """
    return int(Decimal(str(amount)) * Decimal(10**decimals))
