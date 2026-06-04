"""Venue-neutral prediction-market result dataclasses.

This module owns the framework-side, venue-neutral data models that the
prediction-market read surface exposes to strategies — ``PredictionMarket``,
``PredictionPosition``, ``PredictionOrder``, ``HistoricalPrice``, ``PriceHistory``,
``HistoricalTrade``, ``ArbitrageOpportunity``, and ``CorrelationResult``. They are
plain data holders (fields + ``to_dict`` serialization + derived properties) with
**no** venue/connector dependency, so the framework data layer carries no connector
import.

VIB-4989 (epic VIB-4851 self-containment): the Polymarket CLOB provider
implementation that used to live here — ``PredictionMarketDataProvider`` — now lives
in the connector folder (``almanak/connectors/polymarket/prediction_provider.py``)
and is reached through the ``PredictionReadRegistry`` seam. The
Polymarket-SDK-to-neutral converters (formerly ``PredictionMarket.from_gamma_market``
& friends) moved with it as connector-side ``to_prediction_market(...)`` functions —
the conversion is connector knowledge, so it does not belong on these neutral types.

The concrete result types stay in the framework because they are the seam's public
interface: the connector imports them back (connector→framework is allowed) and
returns them, and ``MarketSnapshot`` / the prediction monitor consume them through
the duck-typed ``PredictionProvider`` Protocol.
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

# =============================================================================
# Data Models
# =============================================================================


@dataclass
class PredictionMarket:
    """Prediction market data for strategy consumption.

    This is a simplified view of a prediction market with the key fields
    that strategies need for decision-making.

    Attributes:
        market_id: Internal market ID
        condition_id: CTF condition ID (0x...)
        question: Market question text
        slug: URL slug for the market
        yes_price: Current YES outcome price (0-1)
        no_price: Current NO outcome price (0-1)
        yes_token_id: CLOB token ID for YES outcome
        no_token_id: CLOB token ID for NO outcome
        spread: Bid-ask spread
        volume_24h: 24-hour trading volume in USDC
        liquidity: Current liquidity
        end_date: Resolution deadline
        is_active: Whether market is accepting orders
        is_resolved: Whether market has been resolved
        event_id: Parent event ID (for related markets)
        event_slug: Parent event slug
        tags: Market tags/categories
        fetched_at: Timestamp when data was fetched
    """

    market_id: str
    condition_id: str
    question: str
    slug: str
    yes_price: Decimal
    no_price: Decimal
    yes_token_id: str | None
    no_token_id: str | None
    spread: Decimal
    volume_24h: Decimal
    liquidity: Decimal
    end_date: datetime | None
    is_active: bool
    is_resolved: bool
    event_id: str | None = None
    event_slug: str | None = None
    tags: list[str] = field(default_factory=list)
    fetched_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "market_id": self.market_id,
            "condition_id": self.condition_id,
            "question": self.question,
            "slug": self.slug,
            "yes_price": str(self.yes_price),
            "no_price": str(self.no_price),
            "yes_token_id": self.yes_token_id,
            "no_token_id": self.no_token_id,
            "spread": str(self.spread),
            "volume_24h": str(self.volume_24h),
            "liquidity": str(self.liquidity),
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "is_active": self.is_active,
            "is_resolved": self.is_resolved,
            "event_id": self.event_id,
            "event_slug": self.event_slug,
            "tags": self.tags,
            "fetched_at": self.fetched_at.isoformat(),
        }


@dataclass
class PredictionPosition:
    """Prediction market position for strategy consumption.

    Represents a position in a prediction market with size, prices, and PnL.

    Attributes:
        market_id: Market ID
        condition_id: CTF condition ID
        token_id: CLOB token ID
        outcome: Position outcome (YES or NO)
        size: Number of shares held
        avg_price: Average entry price
        current_price: Current market price
        unrealized_pnl: Unrealized profit/loss
        realized_pnl: Realized profit/loss
        value: Current position value (size * current_price)
    """

    market_id: str
    condition_id: str
    token_id: str
    outcome: Literal["YES", "NO"]
    size: Decimal
    avg_price: Decimal
    current_price: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal

    @property
    def value(self) -> Decimal:
        """Calculate current position value."""
        return self.size * self.current_price

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "market_id": self.market_id,
            "condition_id": self.condition_id,
            "token_id": self.token_id,
            "outcome": self.outcome,
            "size": str(self.size),
            "avg_price": str(self.avg_price),
            "current_price": str(self.current_price),
            "unrealized_pnl": str(self.unrealized_pnl),
            "realized_pnl": str(self.realized_pnl),
            "value": str(self.value),
        }


@dataclass
class PredictionOrder:
    """Open prediction market order.

    Attributes:
        order_id: Order ID
        market_id: Market ID (token ID)
        outcome: Order outcome (YES, NO, or None if unknown)
        side: Order side (BUY or SELL)
        price: Order price
        size: Order size in shares
        filled_size: Filled amount
        created_at: Order creation timestamp

    Note:
        outcome may be None when market lookup by token_id fails.
        This avoids incorrect assumptions about order direction.
    """

    order_id: str
    market_id: str
    outcome: Literal["YES", "NO"] | None
    side: Literal["BUY", "SELL"]
    price: Decimal
    size: Decimal
    filled_size: Decimal
    created_at: datetime | None

    @property
    def remaining_size(self) -> Decimal:
        """Calculate remaining unfilled size."""
        return self.size - self.filled_size

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "order_id": self.order_id,
            "market_id": self.market_id,
            "outcome": self.outcome,
            "side": self.side,
            "price": str(self.price),
            "size": str(self.size),
            "filled_size": str(self.filled_size),
            "remaining_size": str(self.remaining_size),
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


@dataclass
class HistoricalPrice:
    """Historical price point for strategy consumption.

    Represents a price at a specific timestamp.

    Attributes:
        timestamp: When the price was recorded (UTC)
        price: Price value (0.0 to 1.0 for prediction markets)
    """

    timestamp: datetime
    price: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "price": str(self.price),
        }


@dataclass
class PriceHistory:
    """Historical price data for strategy consumption.

    Contains a time series of prices with OHLC-style accessors.

    Attributes:
        market_id: Market ID or slug
        outcome: Outcome (YES or NO)
        interval: Time interval for the data
        prices: List of historical price points
        start_time: Start of the time range
        end_time: End of the time range
    """

    market_id: str
    outcome: Literal["YES", "NO"]
    interval: str
    prices: list[HistoricalPrice]
    start_time: datetime | None = None
    end_time: datetime | None = None

    @property
    def open_price(self) -> Decimal | None:
        """Get opening price (first in series)."""
        return self.prices[0].price if self.prices else None

    @property
    def close_price(self) -> Decimal | None:
        """Get closing price (last in series)."""
        return self.prices[-1].price if self.prices else None

    @property
    def high_price(self) -> Decimal | None:
        """Get highest price in series."""
        return max(p.price for p in self.prices) if self.prices else None

    @property
    def low_price(self) -> Decimal | None:
        """Get lowest price in series."""
        return min(p.price for p in self.prices) if self.prices else None

    @property
    def price_change(self) -> Decimal | None:
        """Calculate price change (close - open)."""
        if self.open_price is not None and self.close_price is not None:
            return self.close_price - self.open_price
        return None

    @property
    def price_change_pct(self) -> Decimal | None:
        """Calculate price change percentage."""
        if self.open_price and self.price_change is not None:
            return (self.price_change / self.open_price) * 100
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "market_id": self.market_id,
            "outcome": self.outcome,
            "interval": self.interval,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "open_price": str(self.open_price) if self.open_price else None,
            "close_price": str(self.close_price) if self.close_price else None,
            "high_price": str(self.high_price) if self.high_price else None,
            "low_price": str(self.low_price) if self.low_price else None,
            "price_change": str(self.price_change) if self.price_change else None,
            "price_change_pct": str(self.price_change_pct) if self.price_change_pct else None,
            "point_count": len(self.prices),
        }


@dataclass
class HistoricalTrade:
    """Historical trade for strategy consumption.

    Represents a single executed trade from the trade tape.

    Attributes:
        id: Trade ID
        market_id: Market ID
        outcome: Trade outcome (YES or NO)
        side: Trade side (BUY or SELL)
        price: Execution price
        size: Trade size in shares
        timestamp: When the trade occurred
        value: Trade value in USDC (size * price)
    """

    id: str
    market_id: str
    outcome: Literal["YES", "NO"]
    side: Literal["BUY", "SELL"]
    price: Decimal
    size: Decimal
    timestamp: datetime

    @property
    def value(self) -> Decimal:
        """Calculate trade value in USDC."""
        return self.size * self.price

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "id": self.id,
            "market_id": self.market_id,
            "outcome": self.outcome,
            "side": self.side,
            "price": str(self.price),
            "size": str(self.size),
            "value": str(self.value),
            "timestamp": self.timestamp.isoformat(),
        }


# =============================================================================
# Correlation Data Models
# =============================================================================


# =============================================================================
# Arbitrage Data Models
# =============================================================================


@dataclass
class ArbitrageOpportunity:
    """Detected arbitrage opportunity in prediction markets.

    Represents a potential arbitrage opportunity when the sum of YES and NO
    prices is less than 1.00, guaranteeing a profit if both positions can
    be acquired at the quoted prices.

    Attributes:
        market_id: Market ID
        market_slug: Market URL slug
        question: Market question text
        yes_price: Current YES price from orderbook (best ask)
        no_price: Current NO price from orderbook (best ask)
        total_cost: Sum of YES and NO prices
        expected_profit: Guaranteed profit per share (1.0 - total_cost)
        expected_profit_pct: Profit as percentage of investment
        max_size: Maximum shares limited by orderbook depth
        yes_available: Shares available at YES price
        no_available: Shares available at NO price
        confidence: Confidence level (HIGH, MEDIUM, LOW)
        spread_yes: YES token bid-ask spread
        spread_no: NO token bid-ask spread
        detected_at: When the opportunity was detected

    Example:
        >>> opp = detect_yes_no_arbitrage("btc-100k")
        >>> if opp and opp.expected_profit > Decimal("0.01"):
        ...     print(f"Arb: Buy YES @ {opp.yes_price} + NO @ {opp.no_price}")
        ...     print(f"Profit: ${opp.expected_profit * opp.max_size}")
    """

    market_id: str
    market_slug: str
    question: str
    yes_price: Decimal
    no_price: Decimal
    total_cost: Decimal
    expected_profit: Decimal
    expected_profit_pct: Decimal
    max_size: Decimal
    yes_available: Decimal
    no_available: Decimal
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    spread_yes: Decimal
    spread_no: Decimal
    detected_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "market_id": self.market_id,
            "market_slug": self.market_slug,
            "question": self.question,
            "yes_price": str(self.yes_price),
            "no_price": str(self.no_price),
            "total_cost": str(self.total_cost),
            "expected_profit": str(self.expected_profit),
            "expected_profit_pct": str(self.expected_profit_pct),
            "max_size": str(self.max_size),
            "yes_available": str(self.yes_available),
            "no_available": str(self.no_available),
            "confidence": self.confidence,
            "spread_yes": str(self.spread_yes),
            "spread_no": str(self.spread_no),
            "detected_at": self.detected_at.isoformat(),
        }


@dataclass
class CorrelationResult:
    """Result of correlation calculation between two markets.

    Represents the Pearson correlation coefficient and metadata about
    the correlation calculation.

    Attributes:
        market_1_id: First market ID
        market_2_id: Second market ID
        correlation: Pearson correlation coefficient (-1 to 1)
        p_value: Statistical p-value (if calculable)
        sample_size: Number of price points used in calculation
        window_hours: Time window used for calculation
        calculated_at: Timestamp when correlation was calculated
    """

    market_1_id: str
    market_2_id: str
    correlation: Decimal
    p_value: Decimal | None
    sample_size: int
    window_hours: int
    calculated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "market_1_id": self.market_1_id,
            "market_2_id": self.market_2_id,
            "correlation": str(self.correlation),
            "p_value": str(self.p_value) if self.p_value else None,
            "sample_size": self.sample_size,
            "window_hours": self.window_hours,
            "calculated_at": self.calculated_at.isoformat(),
        }


__all__ = [
    "ArbitrageOpportunity",
    "CorrelationResult",
    "HistoricalPrice",
    "HistoricalTrade",
    "PredictionMarket",
    "PredictionOrder",
    "PredictionPosition",
    "PriceHistory",
]
