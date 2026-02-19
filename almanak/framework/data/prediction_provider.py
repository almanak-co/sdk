"""Prediction Market Data Provider.

Provides prediction market data through a unified interface for strategy
decision-making. Wraps the Polymarket CLOB client with caching and convenience
methods.

IMPORTANT - POLYMARKET-SPECIFIC IMPLEMENTATION:
    This module is currently tightly coupled to Polymarket's infrastructure.
    It directly depends on Polymarket-specific concepts and APIs:

    - ClobClient: Polymarket's Central Limit Order Book client
    - GammaMarket: Polymarket's market data model
    - Conditional Token Framework (CTF): Polymarket's token standard
    - CLOB token IDs: Polymarket-specific market identifiers
    - Polymarket Data API endpoints and data structures

    While the provider interface appears generic (e.g., PredictionMarket,
    PredictionPosition), the implementation assumes Polymarket semantics
    throughout. This design works well for Polymarket-only strategies but
    creates technical debt if we want to support other prediction markets
    (e.g., Augur, Gnosis Conditional Tokens, Zeitgeist).

    TODO: Refactor to support multiple prediction market protocols
    See notes/tech-debt/prediction-provider-coupling.md for detailed analysis
    and suggested refactoring approach using factory pattern and abstract
    protocol interfaces.

Key Features:
    - Unified access to prediction market data
    - Response caching with configurable TTL
    - Position tracking with unrealized PnL
    - Orderbook and spread calculations
    - Multi-market correlation analysis
    - Related markets discovery

Example:
    from almanak.framework.data.prediction_provider import PredictionMarketDataProvider
    from almanak.framework.connectors.polymarket import ClobClient, PolymarketConfig

    config = PolymarketConfig.from_env()
    client = ClobClient(config)
    provider = PredictionMarketDataProvider(client)

    # Get market data
    market = provider.get_market("will-bitcoin-exceed-100k-2025")
    print(f"YES: {market.yes_price}, NO: {market.no_price}")

    # Get positions
    positions = provider.get_positions(wallet="0x...")
    for pos in positions:
        print(f"{pos.outcome}: {pos.size} @ {pos.avg_price}")

    # Get related markets
    related = provider.get_related_markets("market-id")
    for market in related:
        print(f"Related: {market.question}")

    # Calculate correlation
    corr = provider.calculate_correlation("market-1", "market-2", window_hours=24)
    print(f"Correlation: {corr.correlation}")
"""

import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from ..connectors.polymarket.clob_client import ClobClient
from ..connectors.polymarket.models import (
    GammaMarket,
    MarketFilters,
    OrderBook,
    Position,
    PositionFilters,
)
from ..connectors.polymarket.models import (
    HistoricalPrice as ClobHistoricalPrice,
)
from ..connectors.polymarket.models import (
    HistoricalTrade as ClobHistoricalTrade,
)
from ..connectors.polymarket.models import (
    PriceHistory as ClobPriceHistory,
)

logger = logging.getLogger(__name__)


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

    @classmethod
    def from_gamma_market(cls, market: GammaMarket) -> "PredictionMarket":
        """Create from GammaMarket object."""
        # Calculate spread from best bid/ask if available
        spread = Decimal("0")
        if market.best_bid is not None and market.best_ask is not None:
            spread = market.best_ask - market.best_bid

        return cls(
            market_id=market.id,
            condition_id=market.condition_id,
            question=market.question,
            slug=market.slug,
            yes_price=market.yes_price,
            no_price=market.no_price,
            yes_token_id=market.yes_token_id,
            no_token_id=market.no_token_id,
            spread=spread,
            volume_24h=market.volume_24hr,
            liquidity=market.liquidity,
            end_date=market.end_date,
            is_active=market.active,
            is_resolved=market.closed,
            event_id=market.event_id,
            event_slug=market.event_slug,
            tags=market.tags,
        )

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

    @classmethod
    def from_position(cls, pos: Position) -> "PredictionPosition":
        """Create from Position object."""
        return cls(
            market_id=pos.market_id,
            condition_id=pos.condition_id,
            token_id=pos.token_id,
            outcome=pos.outcome,
            size=pos.size,
            avg_price=pos.avg_price,
            current_price=pos.current_price,
            unrealized_pnl=pos.unrealized_pnl,
            realized_pnl=pos.realized_pnl,
        )

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

    @classmethod
    def from_clob_price(cls, price: ClobHistoricalPrice) -> "HistoricalPrice":
        """Create from CLOB historical price."""
        return cls(
            timestamp=price.timestamp,
            price=price.price,
        )

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

    @classmethod
    def from_clob_history(
        cls,
        history: ClobPriceHistory,
        market_id: str,
        outcome: Literal["YES", "NO"],
    ) -> "PriceHistory":
        """Create from CLOB price history."""
        return cls(
            market_id=market_id,
            outcome=outcome,
            interval=history.interval,
            prices=[HistoricalPrice.from_clob_price(p) for p in history.prices],
            start_time=history.start_time,
            end_time=history.end_time,
        )

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

    @classmethod
    def from_clob_trade(
        cls,
        trade: ClobHistoricalTrade,
        outcome: Literal["YES", "NO"],
        market_id: str,
    ) -> "HistoricalTrade":
        """Create from CLOB historical trade."""
        return cls(
            id=trade.id,
            market_id=market_id,
            outcome=outcome,
            side=trade.side,
            price=trade.price,
            size=trade.size,
            timestamp=trade.timestamp,
        )

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


# =============================================================================
# Cache Entry
# =============================================================================


@dataclass
class CacheEntry:
    """Cache entry with expiration tracking."""

    value: Any
    expires_at: float

    def is_expired(self) -> bool:
        """Check if cache entry has expired."""
        return time.time() >= self.expires_at


# =============================================================================
# Prediction Market Data Provider
# =============================================================================


class PredictionMarketDataProvider:
    """Data provider for prediction market information.

    Wraps the Polymarket CLOB client to provide a clean interface for
    strategies to access prediction market data. Includes caching to
    reduce API calls and improve performance.

    ARCHITECTURAL LIMITATION - POLYMARKET COUPLING:
        This class is tightly coupled to Polymarket's CLOB client and data
        models. All methods assume Polymarket-specific semantics:

        - Constructor requires ClobClient (Polymarket-specific)
        - Market lookups use Polymarket's slug/ID system
        - Position tracking uses Polymarket's Data API
        - Token IDs are CLOB-specific identifiers
        - Orderbook structure follows CLOB API format

        To support other prediction markets (Augur, Gnosis, etc.), this
        would need significant refactoring to use protocol-agnostic
        abstractions. The current design prioritizes simplicity for
        Polymarket-only use cases over multi-protocol extensibility.

        TODO: Extract a PredictionMarketProtocol interface and create
        protocol-specific implementations (PolymarketProvider, AugurProvider,
        etc.). See notes/tech-debt/prediction-provider-coupling.md for
        detailed refactoring plan.

    Attributes:
        client: Polymarket CLOB client
        cache_ttl: Cache TTL in seconds (default 5)

    Thread Safety:
        This class is NOT thread-safe. Use separate instances per thread.

    Example:
        >>> provider = PredictionMarketDataProvider(clob_client)
        >>> market = provider.get_market("btc-100k")
        >>> print(f"YES: {market.yes_price}")
        >>>
        >>> positions = provider.get_positions(wallet)
        >>> for pos in positions:
        ...     print(f"{pos.outcome}: {pos.unrealized_pnl}")
    """

    def __init__(
        self,
        client: ClobClient,
        cache_ttl: int = 5,
    ) -> None:
        """Initialize prediction market data provider.

        Args:
            client: Polymarket CLOB client for API access
            cache_ttl: Cache TTL in seconds (default 5)
        """
        self.client = client
        self.cache_ttl = cache_ttl
        self._cache: dict[str, CacheEntry] = {}

        logger.info(
            "PredictionMarketDataProvider initialized",
            extra={"cache_ttl": cache_ttl},
        )

    # =========================================================================
    # Caching
    # =========================================================================

    def _get_cached(self, key: str) -> Any | None:
        """Get cached value if not expired."""
        if key in self._cache:
            entry = self._cache[key]
            if not entry.is_expired():
                return entry.value
            del self._cache[key]
        return None

    def _set_cached(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Set cached value with TTL."""
        if ttl is None:
            ttl = self.cache_ttl
        self._cache[key] = CacheEntry(
            value=value,
            expires_at=time.time() + ttl,
        )

    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._cache.clear()
        logger.debug("Cache cleared")

    # =========================================================================
    # Market Data Methods
    # =========================================================================

    def get_market(self, market_id_or_slug: str) -> PredictionMarket:
        """Get full market details.

        Fetches market data by ID or slug. Results are cached for the
        configured TTL.

        Args:
            market_id_or_slug: Market ID or URL slug

        Returns:
            PredictionMarket with full details

        Raises:
            PolymarketAPIError: If API request fails

        Example:
            >>> market = provider.get_market("will-bitcoin-exceed-100k-2025")
            >>> print(f"YES: {market.yes_price}, Volume: {market.volume_24h}")
        """
        cache_key = f"market:{market_id_or_slug}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        # Try as slug first (contains dashes), then as ID
        if "-" in market_id_or_slug:
            gamma_market = self.client.get_market_by_slug(market_id_or_slug)
            if gamma_market is None:
                # Fall back to treating as ID
                gamma_market = self.client.get_market(market_id_or_slug)
        else:
            gamma_market = self.client.get_market(market_id_or_slug)

        market = PredictionMarket.from_gamma_market(gamma_market)
        self._set_cached(cache_key, market)

        # Also cache by ID and slug for faster lookups
        self._set_cached(f"market:{market.market_id}", market)
        if market.slug:
            self._set_cached(f"market:{market.slug}", market)

        logger.debug(
            "Fetched market",
            extra={"market_id": market.market_id, "question": market.question[:50]},
        )

        return market

    def get_market_by_token_id(self, token_id: str) -> PredictionMarket | None:
        """Get market by CLOB token ID.

        Looks up a market using its YES or NO token ID. Useful when you have
        a token ID but not the market ID (e.g., from open orders).

        Results are cached to avoid repeated API calls.

        Args:
            token_id: CLOB token ID (YES or NO token)

        Returns:
            PredictionMarket if found, None otherwise

        Example:
            >>> market = provider.get_market_by_token_id("123456...")
            >>> if market:
            ...     print(f"Market: {market.question}")
        """
        cache_key = f"market_by_token:{token_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            # Cache can contain either PredictionMarket or None (for failed lookups)
            return cached if isinstance(cached, PredictionMarket) else None

        # Query Gamma API for market with this token ID
        markets = self.client.get_markets(MarketFilters(clob_token_ids=[token_id], limit=1))

        if not markets:
            # Cache the negative result to avoid repeated API calls
            self._set_cached(cache_key, False)  # Use False as sentinel for "not found"
            logger.debug(
                "No market found for token ID",
                extra={"token_id": token_id},
            )
            return None

        gamma_market = markets[0]
        market = PredictionMarket.from_gamma_market(gamma_market)

        # Cache by token_id, market_id, and slug for future lookups
        self._set_cached(cache_key, market)
        self._set_cached(f"market:{market.market_id}", market)
        if market.slug:
            self._set_cached(f"market:{market.slug}", market)
        # Also cache the other token ID if available
        if market.yes_token_id and market.yes_token_id != token_id:
            self._set_cached(f"market_by_token:{market.yes_token_id}", market)
        if market.no_token_id and market.no_token_id != token_id:
            self._set_cached(f"market_by_token:{market.no_token_id}", market)

        logger.debug(
            "Found market by token ID",
            extra={
                "token_id": token_id,
                "market_id": market.market_id,
                "question": market.question[:50],
            },
        )

        return market

    def _resolve_outcome_from_token_id(self, token_id: str) -> Literal["YES", "NO"] | None:
        """Resolve outcome (YES/NO) from a token ID.

        Looks up the market for a token ID and determines whether
        it's the YES or NO token.

        Args:
            token_id: CLOB token ID

        Returns:
            "YES", "NO", or None if market lookup fails
        """
        market = self.get_market_by_token_id(token_id)
        if market is None:
            return None

        if token_id == market.yes_token_id:
            return "YES"
        if token_id == market.no_token_id:
            return "NO"

        # Token ID matched a market but doesn't match either token
        # This shouldn't happen, but return None to be safe
        logger.warning(
            "Token ID matched market but not YES or NO token",
            extra={
                "token_id": token_id,
                "market_id": market.market_id,
                "yes_token_id": market.yes_token_id,
                "no_token_id": market.no_token_id,
            },
        )
        return None

    def get_price(self, market_id_or_slug: str, outcome: Literal["YES", "NO"]) -> Decimal:
        """Get current price for YES or NO outcome.

        Args:
            market_id_or_slug: Market ID or URL slug
            outcome: Outcome to get price for

        Returns:
            Current price (0.01 to 0.99)

        Example:
            >>> yes_price = provider.get_price("btc-100k", "YES")
            >>> print(f"YES probability: {yes_price * 100}%")
        """
        market = self.get_market(market_id_or_slug)
        if outcome == "YES":
            return market.yes_price
        return market.no_price

    def get_orderbook(
        self,
        market_id_or_slug: str,
        outcome: Literal["YES", "NO"],
    ) -> OrderBook:
        """Get orderbook for specific outcome.

        Fetches the full orderbook for the YES or NO token of a market.

        Args:
            market_id_or_slug: Market ID or URL slug
            outcome: Outcome to get orderbook for

        Returns:
            OrderBook with bids and asks

        Raises:
            ValueError: If token ID not found for outcome
            PolymarketAPIError: If API request fails

        Example:
            >>> book = provider.get_orderbook("btc-100k", "YES")
            >>> print(f"Best bid: {book.best_bid}, Best ask: {book.best_ask}")
        """
        market = self.get_market(market_id_or_slug)
        token_id = market.yes_token_id if outcome == "YES" else market.no_token_id

        if token_id is None:
            raise ValueError(f"No token ID found for {outcome} outcome in market {market_id_or_slug}")

        cache_key = f"orderbook:{token_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        orderbook = self.client.get_orderbook(token_id)
        self._set_cached(cache_key, orderbook)

        logger.debug(
            "Fetched orderbook",
            extra={
                "market_id": market.market_id,
                "outcome": outcome,
                "bid_levels": len(orderbook.bids),
                "ask_levels": len(orderbook.asks),
            },
        )

        return orderbook

    def get_spread(self, market_id_or_slug: str, outcome: Literal["YES", "NO"] = "YES") -> Decimal:
        """Get bid-ask spread for an outcome.

        Args:
            market_id_or_slug: Market ID or URL slug
            outcome: Outcome to get spread for (default YES)

        Returns:
            Bid-ask spread

        Example:
            >>> spread = provider.get_spread("btc-100k", "YES")
            >>> print(f"Spread: {spread * 100:.2f}%")
        """
        orderbook = self.get_orderbook(market_id_or_slug, outcome)
        if orderbook.spread is not None:
            return orderbook.spread
        return Decimal("0")

    def get_volume_24h(self, market_id_or_slug: str) -> Decimal:
        """Get 24-hour trading volume.

        Args:
            market_id_or_slug: Market ID or URL slug

        Returns:
            24-hour volume in USDC

        Example:
            >>> volume = provider.get_volume_24h("btc-100k")
            >>> print(f"24h Volume: ${volume:,.2f}")
        """
        market = self.get_market(market_id_or_slug)
        return market.volume_24h

    def get_liquidity(self, market_id_or_slug: str) -> Decimal:
        """Get current market liquidity.

        Args:
            market_id_or_slug: Market ID or URL slug

        Returns:
            Current liquidity in USDC

        Example:
            >>> liquidity = provider.get_liquidity("btc-100k")
            >>> print(f"Liquidity: ${liquidity:,.2f}")
        """
        market = self.get_market(market_id_or_slug)
        return market.liquidity

    # =========================================================================
    # Position Methods
    # =========================================================================

    def get_positions(
        self,
        wallet: str | None = None,
        market_id: str | None = None,
        outcome: Literal["YES", "NO"] | None = None,
    ) -> list[PredictionPosition]:
        """Get all open prediction positions.

        Fetches positions from the Polymarket Data API with optional filters.

        Args:
            wallet: Wallet address (defaults to client config wallet)
            market_id: Optional market ID to filter by
            outcome: Optional outcome to filter by

        Returns:
            List of PredictionPosition objects

        Example:
            >>> positions = provider.get_positions()
            >>> total_value = sum(p.value for p in positions)
            >>> print(f"Total position value: ${total_value:,.2f}")
        """
        filters = None
        if market_id or outcome:
            filters = PositionFilters(market=market_id, outcome=outcome)

        # Use client's get_positions which handles wallet default
        raw_positions = self.client.get_positions(wallet=wallet, filters=filters)

        positions = [PredictionPosition.from_position(p) for p in raw_positions]

        logger.debug(
            "Fetched positions",
            extra={
                "wallet": wallet or "default",
                "count": len(positions),
                "filters": {"market_id": market_id, "outcome": outcome},
            },
        )

        return positions

    def get_position(
        self,
        market_id_or_slug: str,
        wallet: str | None = None,
        outcome: Literal["YES", "NO"] | None = None,
    ) -> PredictionPosition | None:
        """Get position for specific market.

        Args:
            market_id_or_slug: Market ID or URL slug
            wallet: Wallet address (defaults to client config wallet)
            outcome: Optional specific outcome to get position for

        Returns:
            PredictionPosition or None if no position

        Example:
            >>> pos = provider.get_position("btc-100k", outcome="YES")
            >>> if pos:
            ...     print(f"YES position: {pos.size} shares")
        """
        # Resolve market to get ID
        market = self.get_market(market_id_or_slug)

        # Get positions filtered by market
        positions = self.get_positions(
            wallet=wallet,
            market_id=market.market_id,
            outcome=outcome,
        )

        # Return first matching position (should be at most one per outcome)
        if positions:
            return positions[0]
        return None

    def get_position_value(
        self,
        market_id_or_slug: str,
        wallet: str | None = None,
    ) -> Decimal:
        """Get total position value for a market.

        Sums the value of all positions (YES and NO) in a market.

        Args:
            market_id_or_slug: Market ID or URL slug
            wallet: Wallet address (defaults to client config wallet)

        Returns:
            Total position value in USDC

        Example:
            >>> value = provider.get_position_value("btc-100k")
            >>> print(f"Total value: ${value:,.2f}")
        """
        market = self.get_market(market_id_or_slug)
        positions = self.get_positions(wallet=wallet, market_id=market.market_id)
        return sum((p.value for p in positions), Decimal("0"))

    # =========================================================================
    # Open Orders Methods
    # =========================================================================

    def get_open_orders(
        self,
        market_id_or_slug: str | None = None,
    ) -> list[PredictionOrder]:
        """Get all open prediction market orders.

        Fetches open orders from the CLOB API, optionally filtered by market.
        Order outcomes (YES/NO) are resolved by looking up the market for each
        token ID.

        Args:
            market_id_or_slug: Optional market ID or slug to filter by

        Returns:
            List of PredictionOrder objects. Note that outcome may be None
            if market lookup fails for an order's token ID.

        Example:
            >>> orders = provider.get_open_orders("btc-100k")
            >>> for order in orders:
            ...     print(f"{order.outcome or 'UNKNOWN'} {order.side} {order.size} @ {order.price}")
        """

        # Get market to resolve token IDs for filtering
        market = None
        token_ids: set[str] = set()
        if market_id_or_slug:
            market = self.get_market(market_id_or_slug)
            if market.yes_token_id:
                token_ids.add(market.yes_token_id)
            if market.no_token_id:
                token_ids.add(market.no_token_id)

        # Fetch all open orders
        raw_orders = self.client.get_open_orders()

        orders = []
        for order in raw_orders:
            # Filter by market if specified
            if token_ids and order.market not in token_ids:
                continue

            # Determine outcome from token ID
            outcome: Literal["YES", "NO"] | None = None
            if market:
                # We have market context - use it directly
                if order.market == market.yes_token_id:
                    outcome = "YES"
                elif order.market == market.no_token_id:
                    outcome = "NO"
                # If neither matches, outcome stays None (shouldn't happen)
            else:
                # No market context - look up by token ID
                # This uses caching to avoid repeated API calls
                outcome = self._resolve_outcome_from_token_id(order.market)

            orders.append(
                PredictionOrder(
                    order_id=order.order_id,
                    market_id=order.market,
                    outcome=outcome,
                    side=order.side,  # type: ignore
                    price=order.price,
                    size=order.size,
                    filled_size=order.filled_size,
                    created_at=order.created_at,
                )
            )

        logger.debug(
            "Fetched open orders",
            extra={
                "market_id": market_id_or_slug,
                "count": len(orders),
            },
        )

        return orders

    # =========================================================================
    # Historical Data Methods
    # =========================================================================

    # Historical data cache TTL (longer for older/stable data)
    HISTORICAL_CACHE_TTL = 60  # 1 minute

    def get_price_history(
        self,
        market_id_or_slug: str,
        outcome: Literal["YES", "NO"],
        interval: str = "1d",
        start_ts: int | None = None,
        end_ts: int | None = None,
        fidelity: int | None = None,
    ) -> PriceHistory:
        """Get historical price data for analysis.

        Fetches time-series price data for a prediction market outcome.
        Useful for backtesting, signal generation, and trend analysis.

        Args:
            market_id_or_slug: Market ID or URL slug
            outcome: Outcome to get history for (YES or NO)
            interval: Predefined interval (1m, 1h, 6h, 1d, 1w, max).
                Mutually exclusive with start_ts/end_ts.
            start_ts: Unix timestamp for start of range (UTC).
                Requires end_ts. Mutually exclusive with interval.
            end_ts: Unix timestamp for end of range (UTC).
                Requires start_ts. Mutually exclusive with interval.
            fidelity: Data resolution in minutes (e.g., 1, 5, 15, 60).
                Optional, controls granularity of returned data.

        Returns:
            PriceHistory with OHLC-style accessors and time series data

        Raises:
            ValueError: If market has no token ID for the outcome

        Example:
            >>> # Get last 24 hours of YES prices
            >>> history = provider.get_price_history(
            ...     "btc-100k",
            ...     outcome="YES",
            ...     interval="1d",
            ... )
            >>> print(f"Open: {history.open_price}, Close: {history.close_price}")
            >>> print(f"High: {history.high_price}, Low: {history.low_price}")
            >>> print(f"Change: {history.price_change_pct:.2f}%")
            >>>
            >>> # Get custom range with 5-minute resolution
            >>> history = provider.get_price_history(
            ...     "btc-100k",
            ...     outcome="YES",
            ...     start_ts=1700000000,
            ...     end_ts=1700100000,
            ...     fidelity=5,
            ... )
        """
        # Resolve market to get token ID
        market = self.get_market(market_id_or_slug)
        token_id = market.yes_token_id if outcome == "YES" else market.no_token_id

        if token_id is None:
            raise ValueError(f"No token ID found for {outcome} outcome in market {market_id_or_slug}")

        # Build cache key
        cache_key = f"price_history:{token_id}:{interval}:{start_ts}:{end_ts}:{fidelity}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        # Fetch from CLOB client
        clob_history = self.client.get_price_history(
            token_id=token_id,
            interval=interval if not start_ts else None,
            start_ts=start_ts,
            end_ts=end_ts,
            fidelity=fidelity,
        )

        # Convert to provider model
        result = PriceHistory.from_clob_history(
            history=clob_history,
            market_id=market.market_id,
            outcome=outcome,
        )

        # Cache with longer TTL for historical data
        self._set_cached(cache_key, result, ttl=self.HISTORICAL_CACHE_TTL)

        logger.debug(
            "Fetched price history",
            extra={
                "market_id": market.market_id,
                "outcome": outcome,
                "interval": interval,
                "points": len(result.prices),
            },
        )

        return result

    def get_trade_tape(
        self,
        market_id_or_slug: str,
        outcome: Literal["YES", "NO"] | None = None,
        limit: int = 100,
    ) -> list[HistoricalTrade]:
        """Get recent executed trades (trade tape).

        Fetches the most recent trades for a market. Useful for analyzing
        market activity, momentum, and trade flow.

        Args:
            market_id_or_slug: Market ID or URL slug
            outcome: Optional outcome to filter trades (YES or NO).
                If None, returns trades for both outcomes.
            limit: Maximum number of trades to return (default 100, max 500)

        Returns:
            List of HistoricalTrade objects, newest first

        Raises:
            ValueError: If market has no token ID for the outcome

        Example:
            >>> # Get recent YES trades
            >>> trades = provider.get_trade_tape("btc-100k", outcome="YES", limit=50)
            >>> for trade in trades:
            ...     print(f"{trade.side} {trade.size} @ {trade.price} = ${trade.value}")
            >>>
            >>> # Analyze recent activity
            >>> buys = [t for t in trades if t.side == "BUY"]
            >>> sells = [t for t in trades if t.side == "SELL"]
            >>> print(f"Buys: {len(buys)}, Sells: {len(sells)}")
        """
        # Resolve market to get token IDs
        market = self.get_market(market_id_or_slug)

        # Determine which token IDs to query
        if outcome == "YES":
            token_ids = [market.yes_token_id] if market.yes_token_id else []
        elif outcome == "NO":
            token_ids = [market.no_token_id] if market.no_token_id else []
        else:
            # Both outcomes
            token_ids = [tid for tid in [market.yes_token_id, market.no_token_id] if tid]

        if not token_ids:
            raise ValueError(f"No token IDs found for market {market_id_or_slug}")

        all_trades: list[HistoricalTrade] = []

        for token_id in token_ids:
            # Determine outcome for this token
            token_outcome: Literal["YES", "NO"] = "YES" if token_id == market.yes_token_id else "NO"

            # Fetch trades for this token
            clob_trades = self.client.get_trade_tape(
                token_id=token_id,
                limit=limit,
            )

            # Convert to provider models
            for trade in clob_trades:
                all_trades.append(
                    HistoricalTrade.from_clob_trade(
                        trade=trade,
                        outcome=token_outcome,
                        market_id=market.market_id,
                    )
                )

        # Sort by timestamp descending (newest first) and limit
        all_trades.sort(key=lambda t: t.timestamp, reverse=True)
        all_trades = all_trades[:limit]

        logger.debug(
            "Fetched trade tape",
            extra={
                "market_id": market.market_id,
                "outcome": outcome,
                "count": len(all_trades),
            },
        )

        return all_trades

    # =========================================================================
    # Multi-Market Correlation Methods
    # =========================================================================

    # Correlation cache TTL (longer for computed correlations)
    CORRELATION_CACHE_TTL = 300  # 5 minutes

    def get_related_markets(
        self,
        market_id_or_slug: str,
        include_same_event: bool = True,
        include_same_tags: bool = True,
    ) -> list[PredictionMarket]:
        """Get markets related to a given market.

        Finds markets in the same event or with overlapping tags/categories.
        Useful for finding markets that may be correlated or for diversification.

        Args:
            market_id_or_slug: Market ID or URL slug
            include_same_event: Include markets from the same event (default True)
            include_same_tags: Include markets with overlapping tags (default True)

        Returns:
            List of related PredictionMarket objects (excluding the input market)

        Example:
            >>> related = provider.get_related_markets("trump-win-2024")
            >>> for market in related:
            ...     print(f"Related: {market.question[:50]}")
        """
        # Get the source market
        source_market = self.get_market(market_id_or_slug)

        cache_key = f"related_markets:{source_market.market_id}:{include_same_event}:{include_same_tags}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        related_markets: list[PredictionMarket] = []
        seen_ids: set[str] = {source_market.market_id}

        # Get markets from the same event
        if include_same_event and source_market.event_id:
            event_markets = self.client.get_markets(MarketFilters(event_id=source_market.event_id, limit=100))
            for gamma_market in event_markets:
                if gamma_market.id not in seen_ids:
                    seen_ids.add(gamma_market.id)
                    related_markets.append(PredictionMarket.from_gamma_market(gamma_market))

        # Get markets with the same tags
        if include_same_tags and source_market.tags:
            for tag in source_market.tags:
                tag_markets = self.client.get_markets(MarketFilters(tag=tag, limit=50))
                for gamma_market in tag_markets:
                    if gamma_market.id not in seen_ids:
                        seen_ids.add(gamma_market.id)
                        related_markets.append(PredictionMarket.from_gamma_market(gamma_market))

        logger.debug(
            "Found related markets",
            extra={
                "source_market": source_market.market_id,
                "related_count": len(related_markets),
            },
        )

        self._set_cached(cache_key, related_markets, ttl=self.HISTORICAL_CACHE_TTL)
        return related_markets

    def get_markets_by_category(
        self,
        category: str,
        active_only: bool = True,
        limit: int = 100,
    ) -> list[PredictionMarket]:
        """Get markets by category/tag.

        Fetches markets that match a specific category (tag). Common categories
        include: politics, crypto, sports, entertainment, finance, etc.

        Args:
            category: Category/tag to filter by (e.g., "politics", "crypto")
            active_only: Only return active markets (default True)
            limit: Maximum number of markets to return (default 100)

        Returns:
            List of PredictionMarket objects matching the category

        Example:
            >>> crypto_markets = provider.get_markets_by_category("crypto")
            >>> print(f"Found {len(crypto_markets)} crypto markets")
            >>> for market in crypto_markets[:5]:
            ...     print(f"  - {market.question[:50]}")
        """
        cache_key = f"markets_by_category:{category}:{active_only}:{limit}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        filters = MarketFilters(
            tag=category,
            active=active_only if active_only else None,
            limit=limit,
        )

        gamma_markets = self.client.get_markets(filters)
        markets = [PredictionMarket.from_gamma_market(m) for m in gamma_markets]

        logger.debug(
            "Fetched markets by category",
            extra={
                "category": category,
                "count": len(markets),
                "active_only": active_only,
            },
        )

        self._set_cached(cache_key, markets, ttl=self.HISTORICAL_CACHE_TTL)
        return markets

    def calculate_correlation(
        self,
        market_1_id_or_slug: str,
        market_2_id_or_slug: str,
        window_hours: int = 24,
        outcome_1: Literal["YES", "NO"] = "YES",
        outcome_2: Literal["YES", "NO"] = "YES",
    ) -> CorrelationResult:
        """Calculate Pearson correlation between two markets.

        Computes the correlation coefficient between price movements of
        two markets over a specified time window.

        Args:
            market_1_id_or_slug: First market ID or slug
            market_2_id_or_slug: Second market ID or slug
            window_hours: Time window in hours for correlation (default 24)
            outcome_1: Outcome for first market (default YES)
            outcome_2: Outcome for second market (default YES)

        Returns:
            CorrelationResult with correlation coefficient and metadata

        Raises:
            ValueError: If insufficient data for correlation calculation

        Example:
            >>> corr = provider.calculate_correlation(
            ...     "trump-win-2024",
            ...     "republican-senate-2024",
            ...     window_hours=168,  # 1 week
            ... )
            >>> if corr.correlation > Decimal("0.7"):
            ...     print("Markets are strongly positively correlated")
            >>> elif corr.correlation < Decimal("-0.7"):
            ...     print("Markets are strongly negatively correlated")
        """
        # Create cache key
        market_1 = self.get_market(market_1_id_or_slug)
        market_2 = self.get_market(market_2_id_or_slug)
        cache_key = f"correlation:{market_1.market_id}:{outcome_1}:{market_2.market_id}:{outcome_2}:{window_hours}"

        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        # Map window hours to interval
        if window_hours <= 1:
            interval = "1m"
        elif window_hours <= 6:
            interval = "1h"
        elif window_hours <= 24:
            interval = "6h"
        elif window_hours <= 168:  # 1 week
            interval = "1d"
        else:
            interval = "1w"

        # Get price history for both markets
        history_1 = self.get_price_history(
            market_id_or_slug=market_1.market_id,
            outcome=outcome_1,
            interval=interval,
        )
        history_2 = self.get_price_history(
            market_id_or_slug=market_2.market_id,
            outcome=outcome_2,
            interval=interval,
        )

        # Align prices by timestamp
        prices_1 = {p.timestamp: float(p.price) for p in history_1.prices}
        prices_2 = {p.timestamp: float(p.price) for p in history_2.prices}

        # Find common timestamps
        common_timestamps = set(prices_1.keys()) & set(prices_2.keys())

        if len(common_timestamps) < 3:
            raise ValueError(
                f"Insufficient overlapping data for correlation calculation. "
                f"Found {len(common_timestamps)} common points, need at least 3."
            )

        # Extract aligned price series
        series_1 = [prices_1[ts] for ts in sorted(common_timestamps)]
        series_2 = [prices_2[ts] for ts in sorted(common_timestamps)]

        # Calculate Pearson correlation
        correlation, p_value = self._pearson_correlation(series_1, series_2)

        result = CorrelationResult(
            market_1_id=market_1.market_id,
            market_2_id=market_2.market_id,
            correlation=Decimal(str(round(correlation, 4))),
            p_value=Decimal(str(round(p_value, 4))) if p_value is not None else None,
            sample_size=len(series_1),
            window_hours=window_hours,
        )

        logger.debug(
            "Calculated correlation",
            extra={
                "market_1": market_1.market_id,
                "market_2": market_2.market_id,
                "correlation": float(result.correlation),
                "sample_size": result.sample_size,
            },
        )

        self._set_cached(cache_key, result, ttl=self.CORRELATION_CACHE_TTL)
        return result

    def _pearson_correlation(
        self,
        x: list[float],
        y: list[float],
    ) -> tuple[float, float | None]:
        """Calculate Pearson correlation coefficient.

        Args:
            x: First data series
            y: Second data series (must be same length)

        Returns:
            Tuple of (correlation coefficient, p-value)
            p-value may be None if calculation fails
        """
        n = len(x)
        if n != len(y) or n < 2:
            return 0.0, None

        # Calculate means
        mean_x = sum(x) / n
        mean_y = sum(y) / n

        # Calculate covariance and standard deviations
        cov = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y, strict=False))
        std_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
        std_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))

        if std_x == 0 or std_y == 0:
            return 0.0, None

        correlation = cov / (std_x * std_y)

        # Calculate p-value using t-distribution approximation
        # t = r * sqrt(n-2) / sqrt(1-r^2)
        if abs(correlation) < 1.0 and n > 2:
            try:
                t_stat = correlation * math.sqrt(n - 2) / math.sqrt(1 - correlation**2)
                # Approximate p-value using two-tailed test
                # For large n, use normal approximation
                p_value = 2 * (1 - self._normal_cdf(abs(t_stat)))
            except (ZeroDivisionError, ValueError):
                p_value = None
        else:
            p_value = None

        return correlation, p_value

    def _normal_cdf(self, x: float) -> float:
        """Approximate cumulative distribution function for standard normal.

        Uses error function approximation.

        Args:
            x: Value to evaluate

        Returns:
            Probability P(X <= x) for standard normal X
        """
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))

    # =========================================================================
    # Arbitrage Detection Methods
    # =========================================================================

    def detect_yes_no_arbitrage(
        self,
        market_id_or_slug: str,
    ) -> ArbitrageOpportunity | None:
        """Detect YES/NO arbitrage opportunity in a single market.

        Checks if buying both YES and NO outcomes costs less than $1.00,
        which would guarantee a profit regardless of outcome.

        The opportunity exists when:
        - YES ask + NO ask < 1.00 (guaranteed profit)

        Confidence levels:
        - HIGH: Profit > 1%, sufficient depth on both sides, tight spreads
        - MEDIUM: Profit > 0.5%, moderate depth
        - LOW: Any positive profit but limited depth or wide spreads

        Args:
            market_id_or_slug: Market ID or URL slug

        Returns:
            ArbitrageOpportunity if found, None otherwise

        Example:
            >>> opp = provider.detect_yes_no_arbitrage("btc-100k")
            >>> if opp:
            ...     print(f"Arb found! Buy YES @ {opp.yes_price} + NO @ {opp.no_price}")
            ...     print(f"Profit: {opp.expected_profit_pct:.2f}% on {opp.max_size} shares")
        """
        try:
            market = self.get_market(market_id_or_slug)

            # Need both token IDs to check orderbooks
            if not market.yes_token_id or not market.no_token_id:
                return None

            # Get orderbooks for both outcomes
            yes_orderbook = self.get_orderbook(market_id_or_slug, "YES")
            no_orderbook = self.get_orderbook(market_id_or_slug, "NO")

            # Need asks on both sides (we want to BUY both)
            if not yes_orderbook.asks or not no_orderbook.asks:
                return None

            # Get best ask prices (what we'd pay to buy)
            yes_ask = yes_orderbook.asks[0].price
            no_ask = no_orderbook.asks[0].price
            yes_available = yes_orderbook.asks[0].size
            no_available = no_orderbook.asks[0].size

            # Calculate total cost to buy both
            total_cost = yes_ask + no_ask

            # Arbitrage exists if total cost < 1.00
            if total_cost >= Decimal("1.00"):
                return None

            # Calculate profit
            expected_profit = Decimal("1.00") - total_cost
            expected_profit_pct = (expected_profit / total_cost) * Decimal("100")

            # Max size limited by smaller orderbook depth
            max_size = min(yes_available, no_available)

            # Calculate spreads
            spread_yes = yes_orderbook.spread or Decimal("0")
            spread_no = no_orderbook.spread or Decimal("0")

            # Determine confidence
            confidence = self._calculate_arb_confidence(
                expected_profit_pct=expected_profit_pct,
                max_size=max_size,
                spread_yes=spread_yes,
                spread_no=spread_no,
            )

            opportunity = ArbitrageOpportunity(
                market_id=market.market_id,
                market_slug=market.slug,
                question=market.question,
                yes_price=yes_ask,
                no_price=no_ask,
                total_cost=total_cost,
                expected_profit=expected_profit,
                expected_profit_pct=expected_profit_pct,
                max_size=max_size,
                yes_available=yes_available,
                no_available=no_available,
                confidence=confidence,
                spread_yes=spread_yes,
                spread_no=spread_no,
            )

            logger.info(
                "Arbitrage opportunity detected",
                extra={
                    "market_id": market.market_id,
                    "yes_price": float(yes_ask),
                    "no_price": float(no_ask),
                    "profit_pct": float(expected_profit_pct),
                    "max_size": float(max_size),
                    "confidence": confidence,
                },
            )

            return opportunity

        except Exception as e:
            logger.warning(
                "Failed to detect arbitrage",
                extra={
                    "market_id": market_id_or_slug,
                    "error": str(e),
                },
            )
            return None

    def detect_cross_market_arbitrage(
        self,
        market_ids: list[str],
        min_profit_pct: Decimal = Decimal("0.5"),
    ) -> list[ArbitrageOpportunity]:
        """Detect arbitrage opportunities across multiple markets.

        Scans a list of markets to find any with YES/NO arbitrage opportunities.
        Useful for monitoring a portfolio of markets or a category.

        Args:
            market_ids: List of market IDs or slugs to scan
            min_profit_pct: Minimum profit percentage to include (default 0.5%)

        Returns:
            List of ArbitrageOpportunity objects, sorted by profit percentage descending

        Example:
            >>> markets = provider.get_markets_by_category("crypto")
            >>> opps = provider.detect_cross_market_arbitrage(
            ...     [m.market_id for m in markets],
            ...     min_profit_pct=Decimal("0.5"),
            ... )
            >>> for opp in opps:
            ...     print(f"{opp.question[:30]}: {opp.expected_profit_pct:.2f}% profit")
        """
        opportunities: list[ArbitrageOpportunity] = []

        for market_id in market_ids:
            opp = self.detect_yes_no_arbitrage(market_id)
            if opp and opp.expected_profit_pct >= min_profit_pct:
                opportunities.append(opp)

        # Sort by profit percentage descending
        opportunities.sort(key=lambda x: x.expected_profit_pct, reverse=True)

        logger.info(
            "Cross-market arbitrage scan complete",
            extra={
                "markets_scanned": len(market_ids),
                "opportunities_found": len(opportunities),
                "min_profit_pct": float(min_profit_pct),
            },
        )

        return opportunities

    def calculate_implied_probability(
        self,
        price: Decimal,
        fee_rate_bps: int = 0,
        spread: Decimal = Decimal("0"),
    ) -> Decimal:
        """Calculate implied probability from market price.

        Converts a prediction market price to an implied probability,
        adjusting for fees and spread if provided.

        The raw price IS the implied probability in an efficient market.
        Adjustments account for:
        - Trading fees (reduces effective probability)
        - Bid-ask spread (uncertainty in true probability)

        Args:
            price: Market price (0.01 to 0.99)
            fee_rate_bps: Fee rate in basis points (default 0)
            spread: Bid-ask spread (default 0)

        Returns:
            Implied probability as Decimal (0 to 1)

        Example:
            >>> # Price of 0.65 implies 65% probability
            >>> prob = provider.calculate_implied_probability(Decimal("0.65"))
            >>> print(f"Implied probability: {prob * 100:.1f}%")
            >>>
            >>> # With fees, effective probability is lower
            >>> prob = provider.calculate_implied_probability(
            ...     Decimal("0.65"),
            ...     fee_rate_bps=200,  # 2% fee
            ... )
        """
        # Base implied probability is the price itself
        implied_prob = price

        # Adjust for fees (fees reduce expected value)
        if fee_rate_bps > 0:
            fee_rate = Decimal(fee_rate_bps) / Decimal("10000")
            # Fee affects both winning and losing sides
            # For a fair bet: price * (1 - fee) = probability
            implied_prob = price / (Decimal("1") - fee_rate)

        # Adjust for spread (midpoint is more accurate)
        if spread > Decimal("0"):
            # Price is typically the ask (what you pay)
            # True probability is closer to midpoint
            implied_prob = price - (spread / Decimal("2"))

        # Clamp to valid probability range
        implied_prob = max(Decimal("0"), min(Decimal("1"), implied_prob))

        return implied_prob

    def _calculate_arb_confidence(
        self,
        expected_profit_pct: Decimal,
        max_size: Decimal,
        spread_yes: Decimal,
        spread_no: Decimal,
    ) -> Literal["HIGH", "MEDIUM", "LOW"]:
        """Calculate confidence level for arbitrage opportunity.

        Args:
            expected_profit_pct: Expected profit percentage
            max_size: Maximum executable size
            spread_yes: YES token spread
            spread_no: NO token spread

        Returns:
            Confidence level: HIGH, MEDIUM, or LOW
        """
        # High confidence: >1% profit, good depth, tight spreads
        if (
            expected_profit_pct > Decimal("1.0")
            and max_size >= Decimal("100")
            and spread_yes <= Decimal("0.02")
            and spread_no <= Decimal("0.02")
        ):
            return "HIGH"

        # Medium confidence: >0.5% profit, moderate conditions
        if (
            expected_profit_pct > Decimal("0.5")
            and max_size >= Decimal("50")
            and spread_yes <= Decimal("0.05")
            and spread_no <= Decimal("0.05")
        ):
            return "MEDIUM"

        # Low confidence: any positive profit
        return "LOW"


__all__ = [
    "ArbitrageOpportunity",
    "CorrelationResult",
    "HistoricalPrice",
    "HistoricalTrade",
    "PredictionMarket",
    "PredictionMarketDataProvider",
    "PredictionOrder",
    "PredictionPosition",
    "PriceHistory",
]
