"""Market making utilities for Polymarket.

This module provides utilities for building market making strategies on
Polymarket prediction markets. It includes functions for:

- Calculating optimal bid/ask spreads based on inventory and risk parameters
- Generating quote ladders with multiple price levels
- Computing inventory skew adjustments
- Determining when to update quotes based on market changes

These utilities are designed to work with the Polymarket ClobClient and
support the unique characteristics of prediction markets where prices
represent probabilities (0.01 to 0.99).

Example:
    from almanak.framework.connectors.polymarket.market_making import (
        Quote,
        RiskParameters,
        calculate_optimal_spread,
        generate_quote_ladder,
        calculate_inventory_skew,
        should_requote,
    )

    # Get current market state
    orderbook = client.get_orderbook(token_id)
    position = Decimal("100")  # Long 100 shares
    max_position = Decimal("1000")

    # Calculate inventory skew
    skew = calculate_inventory_skew(position, max_position)

    # Calculate optimal spread
    risk_params = RiskParameters(base_spread=Decimal("0.02"), skew_factor=Decimal("0.5"))
    bid, ask = calculate_optimal_spread(orderbook, position, risk_params)

    # Generate quote ladder
    mid_price = (orderbook.best_bid + orderbook.best_ask) / 2
    quotes = generate_quote_ladder(
        mid_price=mid_price,
        spread=Decimal("0.02"),
        num_levels=3,
        size_per_level=Decimal("10"),
    )
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from .models import OrderBook


@dataclass
class Quote:
    """A single quote with price, size, and side.

    Represents a single order that can be placed on the orderbook.
    Used in quote ladders for market making.

    Attributes:
        price: Quote price (0.01 to 0.99 for prediction markets)
        size: Number of shares to quote
        side: "BUY" for bid, "SELL" for ask

    Example:
        >>> quote = Quote(price=Decimal("0.50"), size=Decimal("100"), side="BUY")
        >>> print(f"Bid {quote.size} @ {quote.price}")
        Bid 100 @ 0.50
    """

    price: Decimal
    size: Decimal
    side: Literal["BUY", "SELL"]

    def __post_init__(self) -> None:
        """Validate quote parameters."""
        if self.price < Decimal("0.01") or self.price > Decimal("0.99"):
            raise ValueError(f"Price {self.price} must be between 0.01 and 0.99")
        if self.size <= Decimal("0"):
            raise ValueError(f"Size {self.size} must be positive")

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "price": str(self.price),
            "size": str(self.size),
            "side": self.side,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Quote":
        """Create from dictionary."""
        return cls(
            price=Decimal(str(data["price"])),
            size=Decimal(str(data["size"])),
            side=data["side"],
        )


@dataclass
class RiskParameters:
    """Parameters for market making risk management.

    These parameters control how the market maker adjusts spreads and
    quotes based on inventory position and market conditions.

    Attributes:
        base_spread: Minimum spread to maintain (e.g., 0.02 = 2%)
        skew_factor: How much to skew quotes based on inventory (0-1).
            Higher values mean more aggressive skewing to reduce inventory.
        max_position: Maximum position size allowed. Orders beyond this
            size will not be quoted on the accumulating side.
        min_edge: Minimum expected edge (profit margin) per trade.
            Quotes will not be placed if edge falls below this threshold.
        volatility_multiplier: Multiplier for spread based on volatility.
            Higher volatility leads to wider spreads.
        tick_size: Minimum price increment (default 0.01).

    Example:
        >>> params = RiskParameters(
        ...     base_spread=Decimal("0.02"),
        ...     skew_factor=Decimal("0.5"),
        ...     max_position=Decimal("1000"),
        ... )
    """

    base_spread: Decimal = Decimal("0.02")
    skew_factor: Decimal = Decimal("0.5")
    max_position: Decimal = Decimal("1000")
    min_edge: Decimal = Decimal("0.001")
    volatility_multiplier: Decimal = Decimal("1.0")
    tick_size: Decimal = Decimal("0.01")

    def __post_init__(self) -> None:
        """Validate risk parameters."""
        if self.base_spread < Decimal("0"):
            raise ValueError("base_spread must be non-negative")
        if not (Decimal("0") <= self.skew_factor <= Decimal("1")):
            raise ValueError("skew_factor must be between 0 and 1")
        if self.max_position <= Decimal("0"):
            raise ValueError("max_position must be positive")
        if self.min_edge < Decimal("0"):
            raise ValueError("min_edge must be non-negative")
        if self.volatility_multiplier <= Decimal("0"):
            raise ValueError("volatility_multiplier must be positive")
        if self.tick_size <= Decimal("0"):
            raise ValueError("tick_size must be positive")


# Price bounds for prediction markets
MIN_PRICE = Decimal("0.01")
MAX_PRICE = Decimal("0.99")


def calculate_inventory_skew(
    position: Decimal,
    max_position: Decimal,
) -> Decimal:
    """Calculate inventory skew factor based on current position.

    The skew factor represents how much the position deviates from neutral
    as a proportion of the maximum allowed position. It's used to adjust
    bid/ask prices to encourage trades that reduce inventory.

    Returns:
        Skew factor from -1.0 (max short) to +1.0 (max long).
        - Positive: Long position, should tighten asks and widen bids
        - Negative: Short position, should tighten bids and widen asks
        - Zero: Neutral position, symmetric quotes

    Args:
        position: Current position in shares. Positive = long, negative = short.
        max_position: Maximum allowed position size (absolute value).

    Raises:
        ValueError: If max_position is not positive

    Example:
        >>> skew = calculate_inventory_skew(Decimal("500"), Decimal("1000"))
        >>> print(f"Skew: {skew}")  # 0.5 (50% of max long position)
        Skew: 0.5

        >>> skew = calculate_inventory_skew(Decimal("-250"), Decimal("1000"))
        >>> print(f"Skew: {skew}")  # -0.25 (25% of max short position)
        Skew: -0.25
    """
    if max_position <= Decimal("0"):
        raise ValueError("max_position must be positive")

    # Calculate skew as ratio of position to max, clamped to [-1, 1]
    raw_skew = position / max_position
    clamped_skew = max(Decimal("-1"), min(Decimal("1"), raw_skew))

    return clamped_skew


def calculate_optimal_spread(
    orderbook: OrderBook,
    inventory: Decimal,
    risk_params: RiskParameters,
) -> tuple[Decimal, Decimal]:
    """Calculate optimal bid and ask prices based on inventory and market state.

    Uses the Avellaneda-Stoikov market making model adapted for prediction
    markets. The spread is adjusted based on:
    1. Base spread requirement (minimum profit margin)
    2. Inventory skew (encourage position reduction)
    3. Market volatility (wider spreads in volatile markets)

    The algorithm:
    1. Calculate mid price from orderbook
    2. Calculate base half-spread
    3. Apply inventory skew adjustment
    4. Apply volatility adjustment
    5. Clamp to valid price range [0.01, 0.99]

    Args:
        orderbook: Current orderbook with bids and asks
        inventory: Current position (positive = long, negative = short)
        risk_params: Risk management parameters

    Returns:
        Tuple of (bid_price, ask_price) representing optimal quotes.
        Returns (MIN_PRICE, MAX_PRICE) if orderbook is empty.

    Example:
        >>> orderbook = client.get_orderbook(token_id)
        >>> risk_params = RiskParameters(base_spread=Decimal("0.02"))
        >>> bid, ask = calculate_optimal_spread(orderbook, Decimal("100"), risk_params)
        >>> print(f"Quote: {bid} - {ask}")
    """
    # Handle empty orderbook
    if orderbook.best_bid is None or orderbook.best_ask is None:
        return (MIN_PRICE, MAX_PRICE)

    # Calculate mid price
    mid_price = (orderbook.best_bid + orderbook.best_ask) / Decimal("2")

    # Calculate base half-spread
    half_spread = (risk_params.base_spread * risk_params.volatility_multiplier) / Decimal("2")

    # Calculate inventory skew adjustment
    skew = calculate_inventory_skew(inventory, risk_params.max_position)
    skew_adjustment = skew * risk_params.skew_factor * half_spread

    # Calculate bid and ask prices
    # When long (positive skew): lower bid more, raise ask less -> encourage selling
    # When short (negative skew): lower bid less, raise ask more -> encourage buying
    bid_price = mid_price - half_spread - skew_adjustment
    ask_price = mid_price + half_spread - skew_adjustment

    # Round to tick size
    bid_price = (bid_price / risk_params.tick_size).quantize(Decimal("1")) * risk_params.tick_size
    ask_price = (ask_price / risk_params.tick_size).quantize(Decimal("1")) * risk_params.tick_size

    # Clamp to valid price range
    bid_price = max(MIN_PRICE, min(MAX_PRICE, bid_price))
    ask_price = max(MIN_PRICE, min(MAX_PRICE, ask_price))

    # Ensure bid < ask (avoid crossed quotes)
    if bid_price >= ask_price:
        # Adjust to minimum spread
        mid = (bid_price + ask_price) / Decimal("2")
        half_min_spread = risk_params.tick_size
        bid_price = max(MIN_PRICE, mid - half_min_spread)
        ask_price = min(MAX_PRICE, mid + half_min_spread)

    return (bid_price, ask_price)


def generate_quote_ladder(
    mid_price: Decimal,
    spread: Decimal,
    num_levels: int,
    size_per_level: Decimal,
    tick_size: Decimal = Decimal("0.01"),
    skew: Decimal = Decimal("0"),
) -> list[Quote]:
    """Generate a quote ladder with multiple price levels.

    Creates a symmetric ladder of bids and asks around the mid price,
    with optional skew adjustment for inventory management.

    The ladder places quotes at increasing distances from mid:
    - Level 1: mid +/- spread/2
    - Level 2: mid +/- spread
    - Level 3: mid +/- spread*1.5
    - etc.

    Args:
        mid_price: Center price for the ladder
        spread: Base spread between best bid and ask
        num_levels: Number of price levels on each side (1-10)
        size_per_level: Size to quote at each price level
        tick_size: Minimum price increment (default 0.01)
        skew: Inventory skew factor (-1 to +1) to shift the ladder.
            Positive skew shifts asks closer (encourage selling).
            Negative skew shifts bids closer (encourage buying).

    Returns:
        List of Quote objects, sorted by price (highest first).
        Bids have side="BUY", asks have side="SELL".

    Raises:
        ValueError: If num_levels is not between 1 and 10,
            or if parameters produce invalid prices.

    Example:
        >>> quotes = generate_quote_ladder(
        ...     mid_price=Decimal("0.50"),
        ...     spread=Decimal("0.02"),
        ...     num_levels=3,
        ...     size_per_level=Decimal("10"),
        ... )
        >>> for q in quotes:
        ...     print(f"{q.side} {q.size} @ {q.price}")
        SELL 10 @ 0.53
        SELL 10 @ 0.52
        SELL 10 @ 0.51
        BUY 10 @ 0.49
        BUY 10 @ 0.48
        BUY 10 @ 0.47
    """
    if not 1 <= num_levels <= 10:
        raise ValueError("num_levels must be between 1 and 10")
    if spread <= Decimal("0"):
        raise ValueError("spread must be positive")
    if size_per_level <= Decimal("0"):
        raise ValueError("size_per_level must be positive")
    if tick_size <= Decimal("0"):
        raise ValueError("tick_size must be positive")
    if not (Decimal("-1") <= skew <= Decimal("1")):
        raise ValueError("skew must be between -1 and 1")

    quotes: list[Quote] = []
    half_spread = spread / Decimal("2")

    # Apply skew adjustment to shift the ladder
    # Positive skew: shift towards selling (asks closer to mid)
    skew_adjustment = skew * half_spread * Decimal("0.5")

    for level in range(1, num_levels + 1):
        # Distance from mid increases with level
        level_multiplier = Decimal(str(level)) * Decimal("0.5")
        level_offset = half_spread * level_multiplier

        # Calculate bid and ask prices for this level
        bid_raw = mid_price - level_offset - skew_adjustment
        ask_raw = mid_price + level_offset - skew_adjustment

        # Round to tick size (bid floors, ask ceils)
        bid_price = (bid_raw / tick_size).to_integral_value() * tick_size
        ask_price = ((ask_raw / tick_size).to_integral_value() + 1) * tick_size
        # Correct ceiling: if ask_raw is exact multiple, don't add tick
        if ask_raw == ask_price - tick_size:
            ask_price = ask_raw

        # Clamp to valid range
        bid_price = max(MIN_PRICE, min(MAX_PRICE, bid_price))
        ask_price = max(MIN_PRICE, min(MAX_PRICE, ask_price))

        # Only add valid quotes (skip if price would be invalid)
        if MIN_PRICE <= bid_price <= MAX_PRICE:
            quotes.append(
                Quote(
                    price=bid_price,
                    size=size_per_level,
                    side="BUY",
                )
            )

        if MIN_PRICE <= ask_price <= MAX_PRICE:
            quotes.append(
                Quote(
                    price=ask_price,
                    size=size_per_level,
                    side="SELL",
                )
            )

    # Sort by price descending (asks first, then bids)
    quotes.sort(key=lambda q: (q.side == "BUY", -q.price))

    return quotes


def should_requote(
    current_quotes: list[Quote],
    orderbook: OrderBook,
    threshold: Decimal = Decimal("0.01"),
) -> bool:
    """Determine if quotes should be updated based on market changes.

    Compares current quotes against the orderbook to decide if requoting
    is necessary. Requoting is recommended when:

    1. The market has moved significantly (mid price change > threshold)
    2. Our quotes are no longer at competitive prices
    3. Our quotes have been fully filled (detected by absence in orderbook)

    This function helps avoid excessive requoting while ensuring quotes
    remain competitive.

    Args:
        current_quotes: List of currently active quotes
        orderbook: Current orderbook state
        threshold: Price movement threshold that triggers requote (default 0.01).
            A threshold of 0.01 means requote if mid price moves by 1% or more.

    Returns:
        True if quotes should be updated, False otherwise.

    Example:
        >>> quotes = [Quote(Decimal("0.49"), Decimal("10"), "BUY"),
        ...           Quote(Decimal("0.51"), Decimal("10"), "SELL")]
        >>> orderbook = client.get_orderbook(token_id)
        >>> if should_requote(quotes, orderbook, threshold=Decimal("0.01")):
        ...     # Cancel and replace quotes
        ...     pass
    """
    if not current_quotes:
        return True  # No quotes, should quote

    # Handle empty orderbook
    if orderbook.best_bid is None or orderbook.best_ask is None:
        return True  # Market conditions changed, requote

    # Calculate current market mid
    market_mid = (orderbook.best_bid + orderbook.best_ask) / Decimal("2")

    # Find our current quote mid
    our_bids = [q for q in current_quotes if q.side == "BUY"]
    our_asks = [q for q in current_quotes if q.side == "SELL"]

    if not our_bids or not our_asks:
        return True  # Missing one side, should requote

    best_our_bid = max(q.price for q in our_bids)
    best_our_ask = min(q.price for q in our_asks)
    our_mid = (best_our_bid + best_our_ask) / Decimal("2")

    # Check if market mid has moved significantly from our mid
    mid_diff = abs(market_mid - our_mid)
    if mid_diff >= threshold:
        return True

    # Check if our best quotes are competitive
    # If market best bid > our best bid, we're not competitive on bids
    if orderbook.best_bid > best_our_bid + threshold:
        return True

    # If market best ask < our best ask, we're not competitive on asks
    if orderbook.best_ask < best_our_ask - threshold:
        return True

    return False


__all__ = [
    "Quote",
    "RiskParameters",
    "MIN_PRICE",
    "MAX_PRICE",
    "calculate_inventory_skew",
    "calculate_optimal_spread",
    "generate_quote_ladder",
    "should_requote",
]
