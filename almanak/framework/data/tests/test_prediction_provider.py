"""Tests for PredictionMarketDataProvider.

Tests the prediction market data provider including:
- Market data fetching
- Price lookups
- Orderbook retrieval
- Position tracking
- Caching behavior
- Historical data (price history and trade tape)
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.polymarket.models import (
    GammaMarket,
    OpenOrder,
    OrderBook,
    Position,
    PriceLevel,
)
from almanak.framework.connectors.polymarket.models import (
    HistoricalPrice as ClobHistoricalPrice,
)
from almanak.framework.connectors.polymarket.models import (
    HistoricalTrade as ClobHistoricalTrade,
)
from almanak.framework.connectors.polymarket.models import (
    PriceHistory as ClobPriceHistory,
)
from almanak.framework.data.prediction_provider import (
    HistoricalPrice,
    HistoricalTrade,
    PredictionMarket,
    PredictionMarketDataProvider,
    PredictionOrder,
    PredictionPosition,
    PriceHistory,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_gamma_market() -> GammaMarket:
    """Create a mock GammaMarket for testing."""
    return GammaMarket(
        id="12345",
        condition_id="0x9915bea232fa12b20058f9cea1187ea51366352bf833393676cd0db557a58249",
        question="Will Bitcoin exceed $100,000 by end of 2025?",
        slug="will-bitcoin-exceed-100000-by-end-of-2025",
        outcomes=["Yes", "No"],
        outcome_prices=[Decimal("0.65"), Decimal("0.35")],
        clob_token_ids=[
            "19045189272319329424023217822141741659150265216200539353252147725932663608488",
            "28164726938309329424023217822141741659150265216200539353252147725932663608489",
        ],
        volume=Decimal("1500000"),
        volume_24hr=Decimal("125000"),
        liquidity=Decimal("50000"),
        end_date=datetime(2025, 12, 31, 23, 59, 59),
        active=True,
        closed=False,
        enable_order_book=True,
        order_price_min_tick_size=Decimal("0.01"),
        order_min_size=Decimal("5"),
        best_bid=Decimal("0.64"),
        best_ask=Decimal("0.66"),
        last_trade_price=Decimal("0.65"),
    )


@pytest.fixture
def mock_orderbook() -> OrderBook:
    """Create a mock OrderBook for testing."""
    return OrderBook(
        market="19045189272319329424023217822141741659150265216200539353252147725932663608488",
        asset_id="19045189272319329424023217822141741659150265216200539353252147725932663608488",
        bids=[
            PriceLevel(price=Decimal("0.64"), size=Decimal("1000")),
            PriceLevel(price=Decimal("0.63"), size=Decimal("2500")),
            PriceLevel(price=Decimal("0.62"), size=Decimal("5000")),
        ],
        asks=[
            PriceLevel(price=Decimal("0.66"), size=Decimal("1500")),
            PriceLevel(price=Decimal("0.67"), size=Decimal("3000")),
            PriceLevel(price=Decimal("0.68"), size=Decimal("4500")),
        ],
        hash="0xabc123",
    )


@pytest.fixture
def mock_position() -> Position:
    """Create a mock Position for testing."""
    return Position(
        market_id="12345",
        condition_id="0x9915bea",
        token_id="19045189272319329424023217822141741659150265216200539353252147725932663608488",
        outcome="YES",
        size=Decimal("100"),
        avg_price=Decimal("0.50"),
        current_price=Decimal("0.65"),
        unrealized_pnl=Decimal("15"),  # (0.65 - 0.50) * 100
        realized_pnl=Decimal("0"),
    )


@pytest.fixture
def mock_open_order() -> OpenOrder:
    """Create a mock OpenOrder for testing."""
    return OpenOrder(
        order_id="order123",
        market="19045189272319329424023217822141741659150265216200539353252147725932663608488",
        side="BUY",
        price=Decimal("0.60"),
        size=Decimal("50"),
        filled_size=Decimal("10"),
        created_at=datetime(2025, 1, 15, 10, 30, 0),
        expiration=None,
    )


@pytest.fixture
def mock_clob_client(mock_gamma_market, mock_orderbook, mock_position, mock_open_order):
    """Create a mock ClobClient for testing."""
    client = MagicMock()
    client.get_market.return_value = mock_gamma_market
    client.get_market_by_slug.return_value = mock_gamma_market
    client.get_orderbook.return_value = mock_orderbook
    client.get_positions.return_value = [mock_position]
    client.get_open_orders.return_value = [mock_open_order]
    return client


@pytest.fixture
def provider(mock_clob_client) -> PredictionMarketDataProvider:
    """Create a PredictionMarketDataProvider with mocked client."""
    return PredictionMarketDataProvider(mock_clob_client, cache_ttl=5)


# =============================================================================
# PredictionMarket Tests
# =============================================================================


class TestPredictionMarket:
    """Tests for PredictionMarket model."""

    def test_from_gamma_market(self, mock_gamma_market):
        """Test creating PredictionMarket from GammaMarket."""
        market = PredictionMarket.from_gamma_market(mock_gamma_market)

        assert market.market_id == "12345"
        assert market.condition_id == "0x9915bea232fa12b20058f9cea1187ea51366352bf833393676cd0db557a58249"
        assert market.question == "Will Bitcoin exceed $100,000 by end of 2025?"
        assert market.slug == "will-bitcoin-exceed-100000-by-end-of-2025"
        assert market.yes_price == Decimal("0.65")
        assert market.no_price == Decimal("0.35")
        assert market.yes_token_id is not None
        assert market.no_token_id is not None
        assert market.spread == Decimal("0.02")  # 0.66 - 0.64
        assert market.volume_24h == Decimal("125000")
        assert market.liquidity == Decimal("50000")
        assert market.is_active is True
        assert market.is_resolved is False

    def test_from_gamma_market_no_spread(self):
        """Test creating PredictionMarket when best_bid/ask are None."""
        gamma = GammaMarket(
            id="123",
            condition_id="0xabc",
            question="Test?",
            slug="test",
            outcomes=["Yes", "No"],
            outcome_prices=[Decimal("0.5"), Decimal("0.5")],
            clob_token_ids=["token1", "token2"],
            volume=Decimal("0"),
            liquidity=Decimal("0"),
            active=True,
            closed=False,
            enable_order_book=True,
            best_bid=None,
            best_ask=None,
        )

        market = PredictionMarket.from_gamma_market(gamma)
        assert market.spread == Decimal("0")

    def test_to_dict(self, mock_gamma_market):
        """Test converting PredictionMarket to dictionary."""
        market = PredictionMarket.from_gamma_market(mock_gamma_market)
        data = market.to_dict()

        assert data["market_id"] == "12345"
        assert data["yes_price"] == "0.65"
        assert data["no_price"] == "0.35"
        assert data["spread"] == "0.02"
        assert data["is_active"] is True
        assert data["is_resolved"] is False


# =============================================================================
# PredictionPosition Tests
# =============================================================================


class TestPredictionPosition:
    """Tests for PredictionPosition model."""

    def test_from_position(self, mock_position):
        """Test creating PredictionPosition from Position."""
        pos = PredictionPosition.from_position(mock_position)

        assert pos.market_id == "12345"
        assert pos.outcome == "YES"
        assert pos.size == Decimal("100")
        assert pos.avg_price == Decimal("0.50")
        assert pos.current_price == Decimal("0.65")
        assert pos.unrealized_pnl == Decimal("15")

    def test_value_property(self, mock_position):
        """Test position value calculation."""
        pos = PredictionPosition.from_position(mock_position)
        assert pos.value == Decimal("65")  # 100 * 0.65

    def test_to_dict(self, mock_position):
        """Test converting PredictionPosition to dictionary."""
        pos = PredictionPosition.from_position(mock_position)
        data = pos.to_dict()

        assert data["market_id"] == "12345"
        assert data["outcome"] == "YES"
        assert data["size"] == "100"
        assert Decimal(data["value"]) == Decimal("65")
        assert data["unrealized_pnl"] == "15"


# =============================================================================
# PredictionOrder Tests
# =============================================================================


class TestPredictionOrder:
    """Tests for PredictionOrder model."""

    def test_remaining_size(self):
        """Test remaining size calculation."""
        order = PredictionOrder(
            order_id="123",
            market_id="token123",
            outcome="YES",
            side="BUY",
            price=Decimal("0.60"),
            size=Decimal("100"),
            filled_size=Decimal("30"),
            created_at=None,
        )
        assert order.remaining_size == Decimal("70")

    def test_to_dict(self):
        """Test converting PredictionOrder to dictionary."""
        order = PredictionOrder(
            order_id="123",
            market_id="token123",
            outcome="YES",
            side="BUY",
            price=Decimal("0.60"),
            size=Decimal("100"),
            filled_size=Decimal("30"),
            created_at=datetime(2025, 1, 15),
        )
        data = order.to_dict()

        assert data["order_id"] == "123"
        assert data["outcome"] == "YES"
        assert data["side"] == "BUY"
        assert data["price"] == "0.60"
        assert data["size"] == "100"
        assert data["remaining_size"] == "70"


# =============================================================================
# PredictionMarketDataProvider Tests
# =============================================================================


class TestPredictionMarketDataProvider:
    """Tests for PredictionMarketDataProvider."""

    def test_init(self, mock_clob_client):
        """Test provider initialization."""
        provider = PredictionMarketDataProvider(mock_clob_client, cache_ttl=10)
        assert provider.client == mock_clob_client
        assert provider.cache_ttl == 10

    def test_get_market_by_id(self, provider, mock_gamma_market):
        """Test fetching market by ID."""
        market = provider.get_market("12345")

        assert market.market_id == "12345"
        assert market.yes_price == Decimal("0.65")
        provider.client.get_market.assert_called_once_with("12345")

    def test_get_market_by_slug(self, provider, mock_gamma_market):
        """Test fetching market by slug."""
        market = provider.get_market("will-bitcoin-exceed-100000-by-end-of-2025")

        assert market.market_id == "12345"
        assert market.slug == "will-bitcoin-exceed-100000-by-end-of-2025"
        provider.client.get_market_by_slug.assert_called_once()

    def test_get_market_caching(self, provider):
        """Test that market data is cached."""
        # First call
        market1 = provider.get_market("12345")
        # Second call (should use cache)
        market2 = provider.get_market("12345")

        assert market1.market_id == market2.market_id
        # Client should only be called once due to caching
        assert provider.client.get_market.call_count == 1

    def test_get_price_yes(self, provider):
        """Test getting YES price."""
        price = provider.get_price("12345", "YES")
        assert price == Decimal("0.65")

    def test_get_price_no(self, provider):
        """Test getting NO price."""
        price = provider.get_price("12345", "NO")
        assert price == Decimal("0.35")

    def test_get_orderbook(self, provider, mock_orderbook):
        """Test fetching orderbook."""
        orderbook = provider.get_orderbook("12345", "YES")

        assert len(orderbook.bids) == 3
        assert len(orderbook.asks) == 3
        assert orderbook.best_bid == Decimal("0.64")
        assert orderbook.best_ask == Decimal("0.66")

    def test_get_orderbook_no_token_id(self, provider, mock_gamma_market):
        """Test error when token ID not found."""
        # Create market with missing token IDs
        mock_gamma_market.clob_token_ids = []
        provider.client.get_market.return_value = mock_gamma_market

        with pytest.raises(ValueError, match="No token ID found"):
            provider.get_orderbook("12345", "YES")

    def test_get_spread(self, provider):
        """Test getting bid-ask spread."""
        spread = provider.get_spread("12345", "YES")
        assert spread == Decimal("0.02")  # 0.66 - 0.64

    def test_get_volume_24h(self, provider):
        """Test getting 24h volume."""
        volume = provider.get_volume_24h("12345")
        assert volume == Decimal("125000")

    def test_get_liquidity(self, provider):
        """Test getting liquidity."""
        liquidity = provider.get_liquidity("12345")
        assert liquidity == Decimal("50000")

    def test_get_positions(self, provider, mock_position):
        """Test fetching positions."""
        positions = provider.get_positions()

        assert len(positions) == 1
        assert positions[0].market_id == "12345"
        assert positions[0].outcome == "YES"
        assert positions[0].size == Decimal("100")

    def test_get_positions_with_filters(self, provider):
        """Test fetching positions with filters."""
        provider.get_positions(
            wallet="0x123",
            market_id="12345",
            outcome="YES",
        )

        provider.client.get_positions.assert_called_once()
        call_args = provider.client.get_positions.call_args
        assert call_args.kwargs["wallet"] == "0x123"
        assert call_args.kwargs["filters"].market == "12345"
        assert call_args.kwargs["filters"].outcome == "YES"

    def test_get_position(self, provider, mock_position):
        """Test fetching single position."""
        position = provider.get_position("12345")

        assert position is not None
        assert position.market_id == "12345"
        assert position.outcome == "YES"

    def test_get_position_not_found(self, provider):
        """Test getting position when none exists."""
        provider.client.get_positions.return_value = []

        position = provider.get_position("12345")
        assert position is None

    def test_get_position_value(self, provider, mock_position):
        """Test getting total position value."""
        value = provider.get_position_value("12345")
        assert value == Decimal("65")  # 100 * 0.65

    def test_get_open_orders(self, provider, mock_gamma_market, mock_open_order):
        """Test fetching open orders."""
        orders = provider.get_open_orders("12345")

        assert len(orders) == 1
        assert orders[0].order_id == "order123"
        assert orders[0].outcome == "YES"
        assert orders[0].side == "BUY"
        assert orders[0].remaining_size == Decimal("40")

    def test_get_open_orders_filters_by_market(self, provider, mock_gamma_market):
        """Test that orders are filtered by market token IDs."""
        # Create an order for a different market
        other_order = OpenOrder(
            order_id="other123",
            market="different_token_id",
            side="SELL",
            price=Decimal("0.70"),
            size=Decimal("25"),
            filled_size=Decimal("0"),
            created_at=None,
            expiration=None,
        )
        provider.client.get_open_orders.return_value = [other_order]

        orders = provider.get_open_orders("12345")
        assert len(orders) == 0  # Should filter out non-matching market

    def test_get_open_orders_without_market_context_resolves_outcome(
        self, provider, mock_gamma_market, mock_open_order
    ):
        """Test that orders without market context resolve outcome via token ID lookup."""
        # Setup: get_open_orders without market_id calls _resolve_outcome_from_token_id
        # which calls get_market_by_token_id, which calls client.get_markets
        provider.client.get_markets.return_value = [mock_gamma_market]

        orders = provider.get_open_orders()  # No market_id filter

        assert len(orders) == 1
        # Outcome should be resolved via market lookup (token is YES token)
        assert orders[0].outcome == "YES"
        assert orders[0].order_id == "order123"

    def test_get_open_orders_without_market_context_no_outcome(self, provider, mock_open_order):
        """Test that outcome is None when market lookup fails."""
        # Setup: get_markets returns empty list (token not found)
        provider.client.get_markets.return_value = []

        orders = provider.get_open_orders()  # No market_id filter

        assert len(orders) == 1
        # Outcome should be None since market lookup failed
        assert orders[0].outcome is None
        assert orders[0].order_id == "order123"

    def test_get_open_orders_market_context_yes_token(self, provider, mock_gamma_market):
        """Test YES outcome with market context."""
        # Order for YES token
        yes_order = OpenOrder(
            order_id="yes_order",
            market=mock_gamma_market.clob_token_ids[0],  # YES token
            side="BUY",
            price=Decimal("0.60"),
            size=Decimal("50"),
            filled_size=Decimal("0"),
            created_at=None,
            expiration=None,
        )
        provider.client.get_open_orders.return_value = [yes_order]

        orders = provider.get_open_orders("12345")

        assert len(orders) == 1
        assert orders[0].outcome == "YES"

    def test_get_open_orders_market_context_no_token(self, provider, mock_gamma_market):
        """Test NO outcome with market context."""
        # Order for NO token
        no_order = OpenOrder(
            order_id="no_order",
            market=mock_gamma_market.clob_token_ids[1],  # NO token
            side="SELL",
            price=Decimal("0.40"),
            size=Decimal("30"),
            filled_size=Decimal("0"),
            created_at=None,
            expiration=None,
        )
        provider.client.get_open_orders.return_value = [no_order]

        orders = provider.get_open_orders("12345")

        assert len(orders) == 1
        assert orders[0].outcome == "NO"

    def test_clear_cache(self, provider):
        """Test clearing cache."""
        # Populate cache
        provider.get_market("12345")
        assert len(provider._cache) > 0

        # Clear cache
        provider.clear_cache()
        assert len(provider._cache) == 0


# =============================================================================
# Cache Tests
# =============================================================================


class TestProviderCaching:
    """Tests for provider caching behavior."""

    def test_cache_entry_expiration(self, provider):
        """Test cache entry expiration."""
        import time

        from almanak.framework.data.prediction_provider import CacheEntry

        # Create expired entry
        entry = CacheEntry(value="test", expires_at=time.time() - 1)
        assert entry.is_expired() is True

        # Create valid entry
        entry = CacheEntry(value="test", expires_at=time.time() + 10)
        assert entry.is_expired() is False

    def test_get_cached_returns_none_for_expired(self, provider):
        """Test that expired cache entries return None."""
        import time

        from almanak.framework.data.prediction_provider import CacheEntry

        # Set an expired entry
        provider._cache["test"] = CacheEntry(
            value="stale_data",
            expires_at=time.time() - 1,
        )

        result = provider._get_cached("test")
        assert result is None
        assert "test" not in provider._cache  # Should be cleaned up

    def test_set_cached_with_custom_ttl(self, provider):
        """Test setting cache with custom TTL."""
        provider._set_cached("test", "value", ttl=60)

        import time

        entry = provider._cache["test"]
        # Should expire approximately 60 seconds from now
        assert entry.expires_at > time.time() + 55


# =============================================================================
# Market Lookup by Token ID Tests
# =============================================================================


class TestGetMarketByTokenId:
    """Tests for market lookup by token ID."""

    @pytest.fixture
    def mock_gamma_market(self) -> GammaMarket:
        """Create a mock GammaMarket for testing."""
        return GammaMarket(
            id="12345",
            condition_id="0x9915bea",
            question="Will Bitcoin exceed $100,000?",
            slug="btc-100k",
            outcomes=["Yes", "No"],
            outcome_prices=[Decimal("0.65"), Decimal("0.35")],
            clob_token_ids=["yes_token_12345", "no_token_12345"],
            volume=Decimal("1000000"),
            volume_24hr=Decimal("50000"),
            liquidity=Decimal("25000"),
            end_date=None,
            active=True,
            closed=False,
            enable_order_book=True,
        )

    @pytest.fixture
    def provider(self, mock_gamma_market) -> PredictionMarketDataProvider:
        """Create a provider for testing."""
        client = MagicMock()
        client.get_markets.return_value = [mock_gamma_market]
        return PredictionMarketDataProvider(client, cache_ttl=5)

    def test_get_market_by_token_id_found(self, provider, mock_gamma_market):
        """Test finding market by YES token ID."""
        market = provider.get_market_by_token_id("yes_token_12345")

        assert market is not None
        assert market.market_id == "12345"
        assert market.yes_token_id == "yes_token_12345"
        assert market.no_token_id == "no_token_12345"

    def test_get_market_by_token_id_not_found(self, provider):
        """Test when token ID doesn't match any market."""
        provider.client.get_markets.return_value = []

        market = provider.get_market_by_token_id("unknown_token")

        assert market is None

    def test_get_market_by_token_id_caching(self, provider, mock_gamma_market):
        """Test that market lookups are cached."""
        # First call
        market1 = provider.get_market_by_token_id("yes_token_12345")
        # Second call (should use cache)
        market2 = provider.get_market_by_token_id("yes_token_12345")

        assert market1 is not None
        assert market2 is not None
        assert market1.market_id == market2.market_id
        # get_markets should only be called once
        assert provider.client.get_markets.call_count == 1

    def test_get_market_by_token_id_caches_both_tokens(self, provider, mock_gamma_market):
        """Test that finding by YES token also caches NO token."""
        # Find by YES token
        provider.get_market_by_token_id("yes_token_12345")

        # Finding by NO token should use cache
        market = provider.get_market_by_token_id("no_token_12345")

        assert market is not None
        assert market.market_id == "12345"
        # get_markets should only be called once (first lookup cached both)
        assert provider.client.get_markets.call_count == 1

    def test_get_market_by_token_id_caches_negative_result(self, provider):
        """Test that failed lookups are cached."""
        provider.client.get_markets.return_value = []

        # First call
        market1 = provider.get_market_by_token_id("unknown_token")
        # Second call (should use cached negative result)
        market2 = provider.get_market_by_token_id("unknown_token")

        assert market1 is None
        assert market2 is None
        # get_markets should only be called once
        assert provider.client.get_markets.call_count == 1


class TestResolveOutcomeFromTokenId:
    """Tests for resolving outcome (YES/NO) from token ID."""

    @pytest.fixture
    def mock_gamma_market(self) -> GammaMarket:
        """Create a mock GammaMarket for testing."""
        return GammaMarket(
            id="12345",
            condition_id="0x9915bea",
            question="Will Bitcoin exceed $100,000?",
            slug="btc-100k",
            outcomes=["Yes", "No"],
            outcome_prices=[Decimal("0.65"), Decimal("0.35")],
            clob_token_ids=["yes_token_12345", "no_token_12345"],
            volume=Decimal("1000000"),
            volume_24hr=Decimal("50000"),
            liquidity=Decimal("25000"),
            end_date=None,
            active=True,
            closed=False,
            enable_order_book=True,
        )

    @pytest.fixture
    def provider(self, mock_gamma_market) -> PredictionMarketDataProvider:
        """Create a provider for testing."""
        client = MagicMock()
        client.get_markets.return_value = [mock_gamma_market]
        return PredictionMarketDataProvider(client, cache_ttl=5)

    def test_resolve_outcome_yes_token(self, provider):
        """Test resolving YES token to YES outcome."""
        outcome = provider._resolve_outcome_from_token_id("yes_token_12345")
        assert outcome == "YES"

    def test_resolve_outcome_no_token(self, provider):
        """Test resolving NO token to NO outcome."""
        outcome = provider._resolve_outcome_from_token_id("no_token_12345")
        assert outcome == "NO"

    def test_resolve_outcome_unknown_token(self, provider):
        """Test resolving unknown token returns None."""
        provider.client.get_markets.return_value = []

        outcome = provider._resolve_outcome_from_token_id("unknown_token")
        assert outcome is None

    def test_resolve_outcome_caching(self, provider):
        """Test that outcome resolution uses cached market lookups."""
        # First call
        outcome1 = provider._resolve_outcome_from_token_id("yes_token_12345")
        # Second call (should use cache)
        outcome2 = provider._resolve_outcome_from_token_id("yes_token_12345")

        assert outcome1 == "YES"
        assert outcome2 == "YES"
        # get_markets should only be called once
        assert provider.client.get_markets.call_count == 1


class TestPredictionOrderOutcomeNone:
    """Tests for PredictionOrder with None outcome."""

    def test_prediction_order_with_none_outcome(self):
        """Test creating PredictionOrder with None outcome."""
        order = PredictionOrder(
            order_id="123",
            market_id="token123",
            outcome=None,
            side="BUY",
            price=Decimal("0.60"),
            size=Decimal("100"),
            filled_size=Decimal("30"),
            created_at=None,
        )
        assert order.outcome is None

    def test_prediction_order_to_dict_with_none_outcome(self):
        """Test to_dict includes None outcome."""
        order = PredictionOrder(
            order_id="123",
            market_id="token123",
            outcome=None,
            side="BUY",
            price=Decimal("0.60"),
            size=Decimal("100"),
            filled_size=Decimal("30"),
            created_at=None,
        )
        data = order.to_dict()

        assert data["outcome"] is None
        assert data["order_id"] == "123"


# =============================================================================
# Integration-style Tests
# =============================================================================


class TestProviderWorkflows:
    """Integration-style tests for common provider workflows."""

    def test_market_lookup_and_position_check(self, provider, mock_position):
        """Test typical workflow: lookup market and check position."""
        # Get market data
        market = provider.get_market("12345")
        assert market.yes_price == Decimal("0.65")

        # Check existing position
        position = provider.get_position("12345", outcome="YES")
        assert position is not None
        assert position.size == Decimal("100")

        # Calculate current value and PnL
        assert position.value == Decimal("65")
        assert position.unrealized_pnl == Decimal("15")

    def test_orderbook_analysis(self, provider, mock_orderbook):
        """Test orderbook analysis workflow."""
        orderbook = provider.get_orderbook("12345", "YES")

        # Check spread
        spread = orderbook.spread
        assert spread == Decimal("0.02")

        # Check depth at each level
        total_bid_size = sum(level.size for level in orderbook.bids)
        total_ask_size = sum(level.size for level in orderbook.asks)

        assert total_bid_size == Decimal("8500")
        assert total_ask_size == Decimal("9000")

    def test_price_comparison_workflow(self, provider):
        """Test workflow comparing YES vs NO prices."""
        yes_price = provider.get_price("12345", "YES")
        no_price = provider.get_price("12345", "NO")

        # Prices should sum to approximately 1.0 (may have small arbitrage opportunity)
        total = yes_price + no_price
        assert Decimal("0.95") <= total <= Decimal("1.05")


# =============================================================================
# Historical Data Models Tests
# =============================================================================


class TestHistoricalPrice:
    """Tests for HistoricalPrice model."""

    def test_from_clob_price(self):
        """Test creating HistoricalPrice from CLOB price."""
        clob_price = ClobHistoricalPrice(
            timestamp=datetime(2025, 1, 15, 10, 30, tzinfo=UTC),
            price=Decimal("0.65"),
        )

        price = HistoricalPrice.from_clob_price(clob_price)
        assert price.timestamp == datetime(2025, 1, 15, 10, 30, tzinfo=UTC)
        assert price.price == Decimal("0.65")

    def test_to_dict(self):
        """Test converting HistoricalPrice to dictionary."""
        price = HistoricalPrice(
            timestamp=datetime(2025, 1, 15, 10, 30, tzinfo=UTC),
            price=Decimal("0.65"),
        )
        data = price.to_dict()

        assert data["price"] == "0.65"
        assert "2025-01-15" in data["timestamp"]


class TestPriceHistory:
    """Tests for PriceHistory model."""

    @pytest.fixture
    def sample_prices(self) -> list[HistoricalPrice]:
        """Create sample price history."""
        return [
            HistoricalPrice(
                timestamp=datetime(2025, 1, 15, 10, 0, tzinfo=UTC),
                price=Decimal("0.50"),
            ),
            HistoricalPrice(
                timestamp=datetime(2025, 1, 15, 11, 0, tzinfo=UTC),
                price=Decimal("0.65"),
            ),
            HistoricalPrice(
                timestamp=datetime(2025, 1, 15, 12, 0, tzinfo=UTC),
                price=Decimal("0.45"),
            ),
            HistoricalPrice(
                timestamp=datetime(2025, 1, 15, 13, 0, tzinfo=UTC),
                price=Decimal("0.60"),
            ),
        ]

    def test_ohlc_properties(self, sample_prices):
        """Test OHLC-style property accessors."""
        history = PriceHistory(
            market_id="12345",
            outcome="YES",
            interval="1d",
            prices=sample_prices,
        )

        assert history.open_price == Decimal("0.50")  # First
        assert history.close_price == Decimal("0.60")  # Last
        assert history.high_price == Decimal("0.65")  # Max
        assert history.low_price == Decimal("0.45")  # Min

    def test_price_change(self, sample_prices):
        """Test price change calculation."""
        history = PriceHistory(
            market_id="12345",
            outcome="YES",
            interval="1d",
            prices=sample_prices,
        )

        assert history.price_change == Decimal("0.10")  # 0.60 - 0.50
        assert history.price_change_pct == Decimal("20")  # 10% change

    def test_empty_prices(self):
        """Test handling of empty price list."""
        history = PriceHistory(
            market_id="12345",
            outcome="YES",
            interval="1d",
            prices=[],
        )

        assert history.open_price is None
        assert history.close_price is None
        assert history.high_price is None
        assert history.low_price is None
        assert history.price_change is None
        assert history.price_change_pct is None

    def test_from_clob_history(self):
        """Test creating PriceHistory from CLOB history."""
        clob_history = ClobPriceHistory(
            token_id="token123",
            interval="1d",
            prices=[
                ClobHistoricalPrice(
                    timestamp=datetime(2025, 1, 15, 10, 0, tzinfo=UTC),
                    price=Decimal("0.65"),
                ),
            ],
            start_time=datetime(2025, 1, 15, 10, 0, tzinfo=UTC),
            end_time=datetime(2025, 1, 15, 11, 0, tzinfo=UTC),
        )

        history = PriceHistory.from_clob_history(
            clob_history,
            market_id="12345",
            outcome="YES",
        )

        assert history.market_id == "12345"
        assert history.outcome == "YES"
        assert history.interval == "1d"
        assert len(history.prices) == 1
        assert history.prices[0].price == Decimal("0.65")

    def test_to_dict(self, sample_prices):
        """Test converting PriceHistory to dictionary."""
        history = PriceHistory(
            market_id="12345",
            outcome="YES",
            interval="1d",
            prices=sample_prices,
            start_time=sample_prices[0].timestamp,
            end_time=sample_prices[-1].timestamp,
        )

        data = history.to_dict()

        assert data["market_id"] == "12345"
        assert data["outcome"] == "YES"
        assert data["interval"] == "1d"
        assert data["open_price"] == "0.50"
        assert data["close_price"] == "0.60"
        assert data["high_price"] == "0.65"
        assert data["low_price"] == "0.45"
        assert data["price_change"] == "0.10"
        assert Decimal(data["price_change_pct"]) == Decimal("20")
        assert data["point_count"] == 4


class TestHistoricalTrade:
    """Tests for HistoricalTrade model."""

    def test_from_clob_trade(self):
        """Test creating HistoricalTrade from CLOB trade."""
        clob_trade = ClobHistoricalTrade(
            id="trade123",
            token_id="token123",
            side="BUY",
            price=Decimal("0.65"),
            size=Decimal("100"),
            timestamp=datetime(2025, 1, 15, 10, 30, tzinfo=UTC),
        )

        trade = HistoricalTrade.from_clob_trade(
            clob_trade,
            outcome="YES",
            market_id="12345",
        )

        assert trade.id == "trade123"
        assert trade.market_id == "12345"
        assert trade.outcome == "YES"
        assert trade.side == "BUY"
        assert trade.price == Decimal("0.65")
        assert trade.size == Decimal("100")

    def test_value_property(self):
        """Test trade value calculation."""
        trade = HistoricalTrade(
            id="trade123",
            market_id="12345",
            outcome="YES",
            side="BUY",
            price=Decimal("0.65"),
            size=Decimal("100"),
            timestamp=datetime(2025, 1, 15, 10, 30, tzinfo=UTC),
        )

        assert trade.value == Decimal("65")  # 100 * 0.65

    def test_to_dict(self):
        """Test converting HistoricalTrade to dictionary."""
        trade = HistoricalTrade(
            id="trade123",
            market_id="12345",
            outcome="YES",
            side="BUY",
            price=Decimal("0.65"),
            size=Decimal("100"),
            timestamp=datetime(2025, 1, 15, 10, 30, tzinfo=UTC),
        )

        data = trade.to_dict()

        assert data["id"] == "trade123"
        assert data["market_id"] == "12345"
        assert data["outcome"] == "YES"
        assert data["side"] == "BUY"
        assert data["price"] == "0.65"
        assert data["size"] == "100"
        assert Decimal(data["value"]) == Decimal("65")


# =============================================================================
# Historical Data Provider Tests
# =============================================================================


class TestPredictionMarketDataProviderHistorical:
    """Tests for PredictionMarketDataProvider historical data methods."""

    @pytest.fixture
    def mock_clob_price_history(self) -> ClobPriceHistory:
        """Create mock CLOB price history."""
        return ClobPriceHistory(
            token_id="19045189272319329424023217822141741659150265216200539353252147725932663608488",
            interval="1d",
            prices=[
                ClobHistoricalPrice(
                    timestamp=datetime(2025, 1, 15, 10, 0, tzinfo=UTC),
                    price=Decimal("0.50"),
                ),
                ClobHistoricalPrice(
                    timestamp=datetime(2025, 1, 15, 14, 0, tzinfo=UTC),
                    price=Decimal("0.65"),
                ),
                ClobHistoricalPrice(
                    timestamp=datetime(2025, 1, 15, 18, 0, tzinfo=UTC),
                    price=Decimal("0.60"),
                ),
            ],
            start_time=datetime(2025, 1, 15, 10, 0, tzinfo=UTC),
            end_time=datetime(2025, 1, 15, 18, 0, tzinfo=UTC),
        )

    @pytest.fixture
    def mock_clob_trades(self) -> list[ClobHistoricalTrade]:
        """Create mock CLOB trades."""
        return [
            ClobHistoricalTrade(
                id="trade1",
                token_id="token_yes",
                side="BUY",
                price=Decimal("0.65"),
                size=Decimal("100"),
                timestamp=datetime(2025, 1, 15, 12, 0, tzinfo=UTC),
            ),
            ClobHistoricalTrade(
                id="trade2",
                token_id="token_yes",
                side="SELL",
                price=Decimal("0.64"),
                size=Decimal("50"),
                timestamp=datetime(2025, 1, 15, 11, 0, tzinfo=UTC),
            ),
        ]

    @pytest.fixture
    def provider_with_history(
        self,
        mock_gamma_market,
        mock_clob_price_history,
        mock_clob_trades,
    ) -> PredictionMarketDataProvider:
        """Create provider with mocked historical data."""
        client = MagicMock()
        client.get_market.return_value = mock_gamma_market
        client.get_market_by_slug.return_value = mock_gamma_market
        client.get_price_history.return_value = mock_clob_price_history
        client.get_trade_tape.return_value = mock_clob_trades
        return PredictionMarketDataProvider(client, cache_ttl=5)

    def test_get_price_history(self, provider_with_history, mock_clob_price_history):
        """Test fetching price history."""
        history = provider_with_history.get_price_history(
            market_id_or_slug="12345",
            outcome="YES",
            interval="1d",
        )

        assert history.market_id == "12345"
        assert history.outcome == "YES"
        assert history.interval == "1d"
        assert len(history.prices) == 3
        assert history.open_price == Decimal("0.50")
        assert history.close_price == Decimal("0.60")
        assert history.high_price == Decimal("0.65")

    def test_get_price_history_by_slug(self, provider_with_history):
        """Test fetching price history by slug."""
        history = provider_with_history.get_price_history(
            market_id_or_slug="will-bitcoin-exceed-100000-by-end-of-2025",
            outcome="YES",
            interval="1d",
        )

        assert history.market_id == "12345"
        provider_with_history.client.get_market_by_slug.assert_called()

    def test_get_price_history_custom_range(self, provider_with_history):
        """Test fetching price history with custom time range."""
        provider_with_history.get_price_history(
            market_id_or_slug="12345",
            outcome="YES",
            start_ts=1700000000,
            end_ts=1700100000,
            fidelity=5,
        )

        provider_with_history.client.get_price_history.assert_called_once()
        call_args = provider_with_history.client.get_price_history.call_args
        assert call_args.kwargs["start_ts"] == 1700000000
        assert call_args.kwargs["end_ts"] == 1700100000
        assert call_args.kwargs["fidelity"] == 5

    def test_get_price_history_no_outcome(self, provider_with_history):
        """Test fetching price history for NO outcome."""
        provider_with_history.get_price_history(
            market_id_or_slug="12345",
            outcome="NO",
            interval="1d",
        )

        # Should use NO token ID
        provider_with_history.client.get_price_history.assert_called_once()
        call_args = provider_with_history.client.get_price_history.call_args
        assert "token_id" in call_args.kwargs

    def test_get_price_history_no_token_id(self, provider_with_history, mock_gamma_market):
        """Test error when token ID not found."""
        mock_gamma_market.clob_token_ids = []
        provider_with_history.client.get_market.return_value = mock_gamma_market

        with pytest.raises(ValueError, match="No token ID found"):
            provider_with_history.get_price_history(
                market_id_or_slug="12345",
                outcome="YES",
                interval="1d",
            )

    def test_get_price_history_caching(self, provider_with_history):
        """Test that price history is cached."""
        # First call
        provider_with_history.get_price_history(
            market_id_or_slug="12345",
            outcome="YES",
            interval="1d",
        )
        # Second call (should use cache)
        provider_with_history.get_price_history(
            market_id_or_slug="12345",
            outcome="YES",
            interval="1d",
        )

        # CLOB client should only be called once for history
        assert provider_with_history.client.get_price_history.call_count == 1

    def test_get_trade_tape(self, provider_with_history, mock_clob_trades):
        """Test fetching trade tape."""
        trades = provider_with_history.get_trade_tape(
            market_id_or_slug="12345",
            outcome="YES",
            limit=50,
        )

        assert len(trades) == 2
        # Should be sorted by timestamp descending
        assert trades[0].timestamp > trades[1].timestamp
        assert trades[0].id == "trade1"
        assert trades[0].outcome == "YES"

    def test_get_trade_tape_both_outcomes(self, provider_with_history, mock_gamma_market):
        """Test fetching trade tape for both outcomes."""
        # Create separate trades for YES and NO
        yes_trade = ClobHistoricalTrade(
            id="yes_trade",
            token_id=mock_gamma_market.yes_token_id,
            side="BUY",
            price=Decimal("0.65"),
            size=Decimal("100"),
            timestamp=datetime(2025, 1, 15, 12, 0, tzinfo=UTC),
        )
        no_trade = ClobHistoricalTrade(
            id="no_trade",
            token_id=mock_gamma_market.no_token_id,
            side="SELL",
            price=Decimal("0.35"),
            size=Decimal("50"),
            timestamp=datetime(2025, 1, 15, 11, 0, tzinfo=UTC),
        )

        def mock_get_trade_tape(token_id, limit):
            if token_id == mock_gamma_market.yes_token_id:
                return [yes_trade]
            return [no_trade]

        provider_with_history.client.get_trade_tape.side_effect = mock_get_trade_tape

        trades = provider_with_history.get_trade_tape(
            market_id_or_slug="12345",
            outcome=None,  # Both outcomes
            limit=100,
        )

        # Should have trades from both outcomes
        assert len(trades) == 2
        outcomes = {t.outcome for t in trades}
        assert outcomes == {"YES", "NO"}

    def test_get_trade_tape_no_token_ids(self, provider_with_history, mock_gamma_market):
        """Test error when no token IDs available."""
        mock_gamma_market.clob_token_ids = []
        provider_with_history.client.get_market.return_value = mock_gamma_market

        with pytest.raises(ValueError, match="No token IDs found"):
            provider_with_history.get_trade_tape(
                market_id_or_slug="12345",
                outcome="YES",
            )

    def test_get_trade_tape_limit(self, provider_with_history):
        """Test trade tape respects limit."""
        # Create 5 trades
        many_trades = [
            ClobHistoricalTrade(
                id=f"trade{i}",
                token_id="token_yes",
                side="BUY",
                price=Decimal("0.65"),
                size=Decimal("100"),
                timestamp=datetime(2025, 1, 15, i, 0, tzinfo=UTC),
            )
            for i in range(5)
        ]
        provider_with_history.client.get_trade_tape.return_value = many_trades

        trades = provider_with_history.get_trade_tape(
            market_id_or_slug="12345",
            outcome="YES",
            limit=3,
        )

        assert len(trades) == 3


# =============================================================================
# Historical Data Workflow Tests
# =============================================================================


class TestHistoricalDataWorkflows:
    """Integration-style tests for historical data workflows."""

    @pytest.fixture
    def full_provider(self, mock_gamma_market) -> PredictionMarketDataProvider:
        """Create fully mocked provider for workflow tests."""
        client = MagicMock()
        client.get_market.return_value = mock_gamma_market
        client.get_market_by_slug.return_value = mock_gamma_market

        # Mock price history
        client.get_price_history.return_value = ClobPriceHistory(
            token_id="token_yes",
            interval="1d",
            prices=[
                ClobHistoricalPrice(
                    timestamp=datetime(2025, 1, 14, 0, 0, tzinfo=UTC),
                    price=Decimal("0.45"),
                ),
                ClobHistoricalPrice(
                    timestamp=datetime(2025, 1, 14, 12, 0, tzinfo=UTC),
                    price=Decimal("0.55"),
                ),
                ClobHistoricalPrice(
                    timestamp=datetime(2025, 1, 15, 0, 0, tzinfo=UTC),
                    price=Decimal("0.65"),
                ),
            ],
            start_time=datetime(2025, 1, 14, 0, 0, tzinfo=UTC),
            end_time=datetime(2025, 1, 15, 0, 0, tzinfo=UTC),
        )

        # Mock trade tape
        client.get_trade_tape.return_value = [
            ClobHistoricalTrade(
                id="trade1",
                token_id="token_yes",
                side="BUY",
                price=Decimal("0.65"),
                size=Decimal("200"),
                timestamp=datetime(2025, 1, 15, 10, 0, tzinfo=UTC),
            ),
            ClobHistoricalTrade(
                id="trade2",
                token_id="token_yes",
                side="SELL",
                price=Decimal("0.64"),
                size=Decimal("100"),
                timestamp=datetime(2025, 1, 15, 9, 0, tzinfo=UTC),
            ),
        ]

        return PredictionMarketDataProvider(client, cache_ttl=5)

    def test_trend_analysis_workflow(self, full_provider):
        """Test workflow: analyze price trend."""
        history = full_provider.get_price_history(
            market_id_or_slug="12345",
            outcome="YES",
            interval="1d",
        )

        # Analyze trend
        assert history.open_price == Decimal("0.45")
        assert history.close_price == Decimal("0.65")
        assert history.price_change == Decimal("0.20")
        assert history.price_change_pct > 0  # Positive trend

        # Determine trend direction
        trend = "bullish" if history.price_change > 0 else "bearish"
        assert trend == "bullish"

    def test_trade_flow_analysis(self, full_provider):
        """Test workflow: analyze recent trade flow."""
        trades = full_provider.get_trade_tape(
            market_id_or_slug="12345",
            outcome="YES",
            limit=100,
        )

        # Separate buys and sells
        buys = [t for t in trades if t.side == "BUY"]
        sells = [t for t in trades if t.side == "SELL"]

        # Calculate volumes
        buy_volume = sum(t.value for t in buys)
        sell_volume = sum(t.value for t in sells)

        assert len(buys) == 1
        assert len(sells) == 1
        assert buy_volume == Decimal("130")  # 200 * 0.65
        assert sell_volume == Decimal("64")  # 100 * 0.64

        # Buy pressure > sell pressure
        assert buy_volume > sell_volume

    def test_market_momentum_workflow(self, full_provider):
        """Test workflow: assess market momentum."""
        # Get current price
        current_price = full_provider.get_price("12345", "YES")

        # Get historical data
        history = full_provider.get_price_history(
            market_id_or_slug="12345",
            outcome="YES",
            interval="1d",
        )

        # Compare current to historical range
        assert current_price == Decimal("0.65")
        assert history.high_price == Decimal("0.65")
        assert history.low_price == Decimal("0.45")

        # Current is at the high - strong momentum
        is_at_high = current_price >= history.high_price
        assert is_at_high is True


# =============================================================================
# Correlation Tests
# =============================================================================


class TestCorrelationResult:
    """Tests for CorrelationResult model."""

    def test_correlation_result_to_dict(self):
        """Test converting CorrelationResult to dictionary."""
        from almanak.framework.data.prediction_provider import CorrelationResult

        result = CorrelationResult(
            market_1_id="market1",
            market_2_id="market2",
            correlation=Decimal("0.85"),
            p_value=Decimal("0.001"),
            sample_size=24,
            window_hours=24,
        )

        data = result.to_dict()
        assert data["market_1_id"] == "market1"
        assert data["market_2_id"] == "market2"
        assert data["correlation"] == "0.85"
        assert data["p_value"] == "0.001"
        assert data["sample_size"] == 24
        assert data["window_hours"] == 24

    def test_correlation_result_none_pvalue(self):
        """Test CorrelationResult with None p-value."""
        from almanak.framework.data.prediction_provider import CorrelationResult

        result = CorrelationResult(
            market_1_id="market1",
            market_2_id="market2",
            correlation=Decimal("1.0"),
            p_value=None,
            sample_size=5,
            window_hours=1,
        )

        data = result.to_dict()
        assert data["p_value"] is None


class TestMultiMarketCorrelation:
    """Tests for multi-market correlation methods."""

    @pytest.fixture
    def mock_gamma_market_with_event(self) -> GammaMarket:
        """Create a mock GammaMarket with event info for testing."""
        return GammaMarket(
            id="12345",
            condition_id="0x9915bea232fa12b20058f9cea1187ea51366352bf833393676cd0db557a58249",
            question="Will Bitcoin exceed $100,000 by end of 2025?",
            slug="will-bitcoin-exceed-100000-by-end-of-2025",
            outcomes=["Yes", "No"],
            outcome_prices=[Decimal("0.65"), Decimal("0.35")],
            clob_token_ids=[
                "19045189272319329424023217822141741659150265216200539353252147725932663608488",
                "28164726938309329424023217822141741659150265216200539353252147725932663608489",
            ],
            volume=Decimal("1500000"),
            volume_24hr=Decimal("125000"),
            liquidity=Decimal("50000"),
            end_date=datetime(2025, 12, 31, 23, 59, 59),
            active=True,
            closed=False,
            enable_order_book=True,
            order_price_min_tick_size=Decimal("0.01"),
            order_min_size=Decimal("5"),
            best_bid=Decimal("0.64"),
            best_ask=Decimal("0.66"),
            last_trade_price=Decimal("0.65"),
            event_id="event123",
            event_slug="crypto-2025",
            tags=["crypto", "bitcoin"],
        )

    @pytest.fixture
    def mock_related_market(self) -> GammaMarket:
        """Create a related mock market."""
        return GammaMarket(
            id="12346",
            condition_id="0xabc123",
            question="Will Ethereum exceed $10,000 by end of 2025?",
            slug="will-ethereum-exceed-10000-by-end-of-2025",
            outcomes=["Yes", "No"],
            outcome_prices=[Decimal("0.40"), Decimal("0.60")],
            clob_token_ids=["token_eth_yes", "token_eth_no"],
            volume=Decimal("500000"),
            volume_24hr=Decimal("50000"),
            liquidity=Decimal("20000"),
            end_date=datetime(2025, 12, 31, 23, 59, 59),
            active=True,
            closed=False,
            enable_order_book=True,
            event_id="event123",
            event_slug="crypto-2025",
            tags=["crypto", "ethereum"],
        )

    @pytest.fixture
    def correlation_provider(
        self,
        mock_gamma_market_with_event,
        mock_related_market,
    ) -> PredictionMarketDataProvider:
        """Create provider for correlation testing."""
        client = MagicMock()

        # Return different markets based on ID
        def mock_get_market(market_id):
            if market_id == "12346":
                return mock_related_market
            return mock_gamma_market_with_event

        client.get_market.side_effect = mock_get_market
        client.get_market_by_slug.return_value = mock_gamma_market_with_event
        client.get_markets.return_value = [mock_related_market]

        # Mock price history with overlapping timestamps
        def mock_price_history(token_id, interval=None, start_ts=None, end_ts=None, fidelity=None):
            base_price = Decimal("0.50") if "19045" in str(token_id) else Decimal("0.40")
            return ClobPriceHistory(
                token_id=str(token_id),
                interval=interval or "1d",
                prices=[
                    ClobHistoricalPrice(
                        timestamp=datetime(2025, 1, 14, 0, 0, tzinfo=UTC),
                        price=base_price,
                    ),
                    ClobHistoricalPrice(
                        timestamp=datetime(2025, 1, 14, 6, 0, tzinfo=UTC),
                        price=base_price + Decimal("0.05"),
                    ),
                    ClobHistoricalPrice(
                        timestamp=datetime(2025, 1, 14, 12, 0, tzinfo=UTC),
                        price=base_price + Decimal("0.10"),
                    ),
                    ClobHistoricalPrice(
                        timestamp=datetime(2025, 1, 14, 18, 0, tzinfo=UTC),
                        price=base_price + Decimal("0.15"),
                    ),
                    ClobHistoricalPrice(
                        timestamp=datetime(2025, 1, 15, 0, 0, tzinfo=UTC),
                        price=base_price + Decimal("0.20"),
                    ),
                ],
                start_time=datetime(2025, 1, 14, 0, 0, tzinfo=UTC),
                end_time=datetime(2025, 1, 15, 0, 0, tzinfo=UTC),
            )

        client.get_price_history.side_effect = mock_price_history
        return PredictionMarketDataProvider(client, cache_ttl=5)

    def test_get_related_markets_by_event(self, correlation_provider, mock_related_market):
        """Test finding related markets by event."""
        related = correlation_provider.get_related_markets(
            "12345",
            include_same_event=True,
            include_same_tags=False,
        )

        assert len(related) == 1
        assert related[0].market_id == "12346"
        assert related[0].question == "Will Ethereum exceed $10,000 by end of 2025?"

    def test_get_related_markets_by_tags(self, correlation_provider, mock_related_market):
        """Test finding related markets by tags."""
        related = correlation_provider.get_related_markets(
            "12345",
            include_same_event=False,
            include_same_tags=True,
        )

        # Should find markets for both "crypto" and "bitcoin" tags
        assert len(related) >= 1

    def test_get_related_markets_excludes_source(self, correlation_provider, mock_gamma_market_with_event):
        """Test that source market is excluded from related markets."""
        # Include the source market in the results
        correlation_provider.client.get_markets.return_value = [mock_gamma_market_with_event]

        related = correlation_provider.get_related_markets("12345")

        # Source market should not be in the results
        assert all(m.market_id != "12345" for m in related)

    def test_get_related_markets_caching(self, correlation_provider):
        """Test that related markets are cached."""
        # First call - gets markets for event_id and 2 tags
        result1 = correlation_provider.get_related_markets("12345")
        initial_count = correlation_provider.client.get_markets.call_count

        # Second call (should use cache)
        result2 = correlation_provider.get_related_markets("12345")

        # Client calls should not increase on second call due to caching
        assert correlation_provider.client.get_markets.call_count == initial_count
        assert len(result1) == len(result2)

    def test_get_markets_by_category(self, correlation_provider, mock_related_market):
        """Test fetching markets by category."""
        markets = correlation_provider.get_markets_by_category("crypto")

        assert len(markets) == 1
        correlation_provider.client.get_markets.assert_called()

    def test_get_markets_by_category_active_only(self, correlation_provider):
        """Test category filter with active_only."""
        correlation_provider.get_markets_by_category("crypto", active_only=True)

        # Check that the filter was passed correctly
        call_args = correlation_provider.client.get_markets.call_args
        filters = call_args.args[0] if call_args.args else call_args.kwargs.get("filters")
        assert filters.active is True

    def test_get_markets_by_category_caching(self, correlation_provider):
        """Test that category results are cached."""
        # First call
        correlation_provider.get_markets_by_category("crypto")
        # Second call (should use cache)
        correlation_provider.get_markets_by_category("crypto")

        # Client should only be called once
        assert correlation_provider.client.get_markets.call_count == 1

    def test_calculate_correlation(self, correlation_provider):
        """Test calculating correlation between markets."""
        from almanak.framework.data.prediction_provider import CorrelationResult

        result = correlation_provider.calculate_correlation(
            market_1_id_or_slug="12345",
            market_2_id_or_slug="12346",
            window_hours=24,
        )

        assert isinstance(result, CorrelationResult)
        assert result.market_1_id == "12345"
        assert result.market_2_id == "12346"
        # Both markets have same price trend so correlation should be high
        assert result.correlation >= Decimal("0.9")
        assert result.sample_size == 5
        assert result.window_hours == 24

    def test_calculate_correlation_caching(self, correlation_provider):
        """Test that correlation results are cached."""
        # First call - fetches price history for both markets
        correlation_provider.calculate_correlation("12345", "12346", 24)
        initial_count = correlation_provider.client.get_price_history.call_count

        # Second call (should use cache)
        correlation_provider.calculate_correlation("12345", "12346", 24)

        # Price history calls should not increase on second call due to caching
        assert correlation_provider.client.get_price_history.call_count == initial_count

    def test_calculate_correlation_insufficient_data(self, correlation_provider):
        """Test error when insufficient data for correlation."""
        # Mock with only 2 data points
        correlation_provider.client.get_price_history.side_effect = lambda **kwargs: ClobPriceHistory(
            token_id="test",
            interval="1d",
            prices=[
                ClobHistoricalPrice(
                    timestamp=datetime(2025, 1, 14, 0, 0, tzinfo=UTC),
                    price=Decimal("0.50"),
                ),
            ],
            start_time=datetime(2025, 1, 14, 0, 0, tzinfo=UTC),
            end_time=datetime(2025, 1, 14, 0, 0, tzinfo=UTC),
        )

        # Clear cache first
        correlation_provider.clear_cache()

        with pytest.raises(ValueError, match="Insufficient overlapping data"):
            correlation_provider.calculate_correlation("12345", "12346", 24)


class TestPredictionMarketEventFields:
    """Tests for event/category fields on PredictionMarket."""

    def test_prediction_market_event_fields(self):
        """Test that PredictionMarket includes event fields."""
        market = GammaMarket(
            id="123",
            condition_id="0xabc",
            question="Test?",
            slug="test",
            outcomes=["Yes", "No"],
            outcome_prices=[Decimal("0.5"), Decimal("0.5")],
            clob_token_ids=["token1", "token2"],
            volume=Decimal("0"),
            liquidity=Decimal("0"),
            active=True,
            closed=False,
            enable_order_book=True,
            event_id="event123",
            event_slug="test-event",
            tags=["crypto", "defi"],
        )

        pred_market = PredictionMarket.from_gamma_market(market)

        assert pred_market.event_id == "event123"
        assert pred_market.event_slug == "test-event"
        assert pred_market.tags == ["crypto", "defi"]

    def test_prediction_market_event_fields_in_dict(self):
        """Test that event fields are included in to_dict."""
        market = GammaMarket(
            id="123",
            condition_id="0xabc",
            question="Test?",
            slug="test",
            outcomes=["Yes", "No"],
            outcome_prices=[Decimal("0.5"), Decimal("0.5")],
            clob_token_ids=["token1", "token2"],
            volume=Decimal("0"),
            liquidity=Decimal("0"),
            active=True,
            closed=False,
            enable_order_book=True,
            event_id="event123",
            event_slug="test-event",
            tags=["crypto"],
        )

        pred_market = PredictionMarket.from_gamma_market(market)
        data = pred_market.to_dict()

        assert data["event_id"] == "event123"
        assert data["event_slug"] == "test-event"
        assert data["tags"] == ["crypto"]


class TestPearsonCorrelation:
    """Tests for the Pearson correlation helper method."""

    @pytest.fixture
    def provider(self) -> PredictionMarketDataProvider:
        """Create provider for correlation testing."""
        client = MagicMock()
        return PredictionMarketDataProvider(client, cache_ttl=5)

    def test_perfect_positive_correlation(self, provider):
        """Test perfect positive correlation."""
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [2.0, 4.0, 6.0, 8.0, 10.0]

        corr, p_value = provider._pearson_correlation(x, y)
        assert abs(corr - 1.0) < 0.0001

    def test_perfect_negative_correlation(self, provider):
        """Test perfect negative correlation."""
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [10.0, 8.0, 6.0, 4.0, 2.0]

        corr, p_value = provider._pearson_correlation(x, y)
        assert abs(corr - (-1.0)) < 0.0001

    def test_no_correlation(self, provider):
        """Test zero correlation."""
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [3.0, 1.0, 4.0, 5.0, 2.0]

        corr, _ = provider._pearson_correlation(x, y)
        # Should be close to 0 for uncorrelated data
        assert abs(corr) < 0.5

    def test_constant_series(self, provider):
        """Test with constant series (no variance)."""
        x = [1.0, 1.0, 1.0, 1.0, 1.0]
        y = [2.0, 4.0, 6.0, 8.0, 10.0]

        corr, p_value = provider._pearson_correlation(x, y)
        assert corr == 0.0
        assert p_value is None

    def test_empty_series(self, provider):
        """Test with empty series."""
        corr, p_value = provider._pearson_correlation([], [])
        assert corr == 0.0
        assert p_value is None

    def test_single_element(self, provider):
        """Test with single element series."""
        corr, p_value = provider._pearson_correlation([1.0], [2.0])
        assert corr == 0.0
        assert p_value is None


# =============================================================================
# Arbitrage Detection Tests
# =============================================================================


class TestArbitrageOpportunity:
    """Tests for ArbitrageOpportunity model."""

    def test_arbitrage_opportunity_creation(self):
        """Test creating ArbitrageOpportunity object."""
        from almanak.framework.data.prediction_provider import ArbitrageOpportunity

        opp = ArbitrageOpportunity(
            market_id="12345",
            market_slug="btc-100k",
            question="Will BTC exceed $100k?",
            yes_price=Decimal("0.48"),
            no_price=Decimal("0.48"),
            total_cost=Decimal("0.96"),
            expected_profit=Decimal("0.04"),
            expected_profit_pct=Decimal("4.17"),
            max_size=Decimal("100"),
            yes_available=Decimal("150"),
            no_available=Decimal("100"),
            confidence="HIGH",
            spread_yes=Decimal("0.01"),
            spread_no=Decimal("0.02"),
        )

        assert opp.market_id == "12345"
        assert opp.total_cost == Decimal("0.96")
        assert opp.expected_profit == Decimal("0.04")
        assert opp.confidence == "HIGH"

    def test_arbitrage_opportunity_to_dict(self):
        """Test converting ArbitrageOpportunity to dictionary."""
        from almanak.framework.data.prediction_provider import ArbitrageOpportunity

        opp = ArbitrageOpportunity(
            market_id="12345",
            market_slug="btc-100k",
            question="Will BTC exceed $100k?",
            yes_price=Decimal("0.48"),
            no_price=Decimal("0.48"),
            total_cost=Decimal("0.96"),
            expected_profit=Decimal("0.04"),
            expected_profit_pct=Decimal("4.17"),
            max_size=Decimal("100"),
            yes_available=Decimal("150"),
            no_available=Decimal("100"),
            confidence="HIGH",
            spread_yes=Decimal("0.01"),
            spread_no=Decimal("0.02"),
        )

        data = opp.to_dict()

        assert data["market_id"] == "12345"
        assert data["market_slug"] == "btc-100k"
        assert data["total_cost"] == "0.96"
        assert data["expected_profit"] == "0.04"
        assert data["expected_profit_pct"] == "4.17"
        assert data["max_size"] == "100"
        assert data["confidence"] == "HIGH"
        assert "detected_at" in data


class TestArbitrageDetection:
    """Tests for arbitrage detection methods."""

    @pytest.fixture
    def mock_gamma_market_for_arb(self) -> GammaMarket:
        """Create a mock GammaMarket for arbitrage testing."""
        return GammaMarket(
            id="12345",
            condition_id="0x9915bea232fa12b20058f9cea1187ea51366352bf833393676cd0db557a58249",
            question="Will Bitcoin exceed $100,000 by end of 2025?",
            slug="btc-100k",
            outcomes=["Yes", "No"],
            outcome_prices=[Decimal("0.48"), Decimal("0.48")],
            clob_token_ids=["token_yes", "token_no"],
            volume=Decimal("1500000"),
            volume_24hr=Decimal("125000"),
            liquidity=Decimal("50000"),
            end_date=datetime(2025, 12, 31, 23, 59, 59),
            active=True,
            closed=False,
            enable_order_book=True,
            order_price_min_tick_size=Decimal("0.01"),
            order_min_size=Decimal("5"),
            best_bid=Decimal("0.47"),
            best_ask=Decimal("0.49"),
            last_trade_price=Decimal("0.48"),
        )

    @pytest.fixture
    def mock_orderbook_with_arb(self) -> tuple[OrderBook, OrderBook]:
        """Create mock orderbooks with arbitrage opportunity."""
        yes_orderbook = OrderBook(
            market="token_yes",
            asset_id="token_yes",
            bids=[
                PriceLevel(price=Decimal("0.46"), size=Decimal("200")),
            ],
            asks=[
                PriceLevel(price=Decimal("0.48"), size=Decimal("150")),
            ],
            hash="0xyes",
        )
        no_orderbook = OrderBook(
            market="token_no",
            asset_id="token_no",
            bids=[
                PriceLevel(price=Decimal("0.46"), size=Decimal("100")),
            ],
            asks=[
                PriceLevel(price=Decimal("0.48"), size=Decimal("100")),
            ],
            hash="0xno",
        )
        return yes_orderbook, no_orderbook

    @pytest.fixture
    def arb_provider(
        self,
        mock_gamma_market_for_arb,
        mock_orderbook_with_arb,
    ) -> PredictionMarketDataProvider:
        """Create provider for arbitrage testing."""
        client = MagicMock()
        client.get_market.return_value = mock_gamma_market_for_arb
        client.get_market_by_slug.return_value = mock_gamma_market_for_arb

        yes_book, no_book = mock_orderbook_with_arb

        def mock_get_orderbook(token_id):
            if token_id == "token_yes":
                return yes_book
            return no_book

        client.get_orderbook.side_effect = mock_get_orderbook

        return PredictionMarketDataProvider(client, cache_ttl=5)

    def test_detect_yes_no_arbitrage_found(self, arb_provider):
        """Test detecting YES/NO arbitrage opportunity."""
        opp = arb_provider.detect_yes_no_arbitrage("12345")

        assert opp is not None
        assert opp.market_id == "12345"
        assert opp.yes_price == Decimal("0.48")
        assert opp.no_price == Decimal("0.48")
        assert opp.total_cost == Decimal("0.96")
        assert opp.expected_profit == Decimal("0.04")
        # Max size is limited by smaller orderbook depth
        assert opp.max_size == Decimal("100")

    def test_detect_yes_no_arbitrage_not_found(self, arb_provider):
        """Test when no arbitrage exists (YES + NO >= 1)."""
        # Update orderbooks so there's no arb
        no_arb_yes_book = OrderBook(
            market="token_yes",
            asset_id="token_yes",
            bids=[PriceLevel(price=Decimal("0.49"), size=Decimal("100"))],
            asks=[PriceLevel(price=Decimal("0.51"), size=Decimal("100"))],
            hash="0xyes",
        )
        no_arb_no_book = OrderBook(
            market="token_no",
            asset_id="token_no",
            bids=[PriceLevel(price=Decimal("0.49"), size=Decimal("100"))],
            asks=[PriceLevel(price=Decimal("0.51"), size=Decimal("100"))],
            hash="0xno",
        )

        def mock_get_orderbook(token_id):
            if token_id == "token_yes":
                return no_arb_yes_book
            return no_arb_no_book

        arb_provider.client.get_orderbook.side_effect = mock_get_orderbook
        arb_provider.clear_cache()

        opp = arb_provider.detect_yes_no_arbitrage("12345")
        assert opp is None  # 0.51 + 0.51 = 1.02 > 1.00

    def test_detect_yes_no_arbitrage_confidence_high(self, arb_provider):
        """Test high confidence arbitrage (good profit, depth, spreads)."""
        # Set up high confidence scenario
        high_conf_yes_book = OrderBook(
            market="token_yes",
            asset_id="token_yes",
            bids=[PriceLevel(price=Decimal("0.46"), size=Decimal("200"))],
            asks=[PriceLevel(price=Decimal("0.47"), size=Decimal("150"))],
            hash="0xyes",
        )
        high_conf_no_book = OrderBook(
            market="token_no",
            asset_id="token_no",
            bids=[PriceLevel(price=Decimal("0.46"), size=Decimal("200"))],
            asks=[PriceLevel(price=Decimal("0.47"), size=Decimal("150"))],
            hash="0xno",
        )

        def mock_get_orderbook(token_id):
            if token_id == "token_yes":
                return high_conf_yes_book
            return high_conf_no_book

        arb_provider.client.get_orderbook.side_effect = mock_get_orderbook
        arb_provider.clear_cache()

        opp = arb_provider.detect_yes_no_arbitrage("12345")

        assert opp is not None
        assert opp.total_cost == Decimal("0.94")  # 0.47 + 0.47
        assert opp.expected_profit == Decimal("0.06")  # 1.0 - 0.94
        # Profit > 1%, depth >= 100, spreads <= 2% -> HIGH
        assert opp.confidence == "HIGH"

    def test_detect_yes_no_arbitrage_confidence_low(self, arb_provider):
        """Test low confidence arbitrage (small profit, limited depth)."""
        # Set up low confidence scenario
        low_conf_yes_book = OrderBook(
            market="token_yes",
            asset_id="token_yes",
            bids=[PriceLevel(price=Decimal("0.44"), size=Decimal("20"))],
            asks=[PriceLevel(price=Decimal("0.49"), size=Decimal("25"))],
            hash="0xyes",
        )
        low_conf_no_book = OrderBook(
            market="token_no",
            asset_id="token_no",
            bids=[PriceLevel(price=Decimal("0.44"), size=Decimal("20"))],
            asks=[PriceLevel(price=Decimal("0.499"), size=Decimal("25"))],
            hash="0xno",
        )

        def mock_get_orderbook(token_id):
            if token_id == "token_yes":
                return low_conf_yes_book
            return low_conf_no_book

        arb_provider.client.get_orderbook.side_effect = mock_get_orderbook
        arb_provider.clear_cache()

        opp = arb_provider.detect_yes_no_arbitrage("12345")

        assert opp is not None
        # 0.49 + 0.499 = 0.989 < 1.0 but small profit, low depth
        assert opp.confidence == "LOW"

    def test_detect_yes_no_arbitrage_no_asks(self, arb_provider):
        """Test when orderbook has no asks."""
        no_asks_book = OrderBook(
            market="token_yes",
            asset_id="token_yes",
            bids=[PriceLevel(price=Decimal("0.50"), size=Decimal("100"))],
            asks=[],  # No asks
            hash="0xyes",
        )

        # Must use side_effect to properly override the fixture's side_effect
        arb_provider.client.get_orderbook.side_effect = lambda _: no_asks_book
        arb_provider.clear_cache()

        opp = arb_provider.detect_yes_no_arbitrage("12345")
        assert opp is None

    def test_detect_yes_no_arbitrage_no_token_ids(self, arb_provider, mock_gamma_market_for_arb):
        """Test when market has no token IDs."""
        mock_gamma_market_for_arb.clob_token_ids = []
        arb_provider.client.get_market.return_value = mock_gamma_market_for_arb
        arb_provider.clear_cache()

        opp = arb_provider.detect_yes_no_arbitrage("12345")
        assert opp is None

    def test_detect_yes_no_arbitrage_profit_calculation(self, arb_provider):
        """Test profit percentage calculation."""
        opp = arb_provider.detect_yes_no_arbitrage("12345")

        assert opp is not None
        # Profit = 1 - 0.96 = 0.04
        # Profit % = 0.04 / 0.96 * 100 = 4.166...%
        assert opp.expected_profit == Decimal("0.04")
        expected_pct = (Decimal("0.04") / Decimal("0.96")) * Decimal("100")
        assert abs(opp.expected_profit_pct - expected_pct) < Decimal("0.01")


class TestCrossMarketArbitrage:
    """Tests for cross-market arbitrage detection."""

    @pytest.fixture
    def multi_market_provider(self) -> PredictionMarketDataProvider:
        """Create provider with multiple markets for testing."""
        client = MagicMock()

        # Create markets with different arb opportunities
        markets = {
            "market1": GammaMarket(
                id="market1",
                condition_id="0x111",
                question="Market 1?",
                slug="market-1",
                outcomes=["Yes", "No"],
                outcome_prices=[Decimal("0.45"), Decimal("0.45")],
                clob_token_ids=["m1_yes", "m1_no"],
                volume=Decimal("100000"),
                liquidity=Decimal("10000"),
                active=True,
                closed=False,
                enable_order_book=True,
            ),
            "market2": GammaMarket(
                id="market2",
                condition_id="0x222",
                question="Market 2?",
                slug="market-2",
                outcomes=["Yes", "No"],
                outcome_prices=[Decimal("0.50"), Decimal("0.50")],
                clob_token_ids=["m2_yes", "m2_no"],
                volume=Decimal("100000"),
                liquidity=Decimal("10000"),
                active=True,
                closed=False,
                enable_order_book=True,
            ),
            "market3": GammaMarket(
                id="market3",
                condition_id="0x333",
                question="Market 3?",
                slug="market-3",
                outcomes=["Yes", "No"],
                outcome_prices=[Decimal("0.55"), Decimal("0.55")],
                clob_token_ids=["m3_yes", "m3_no"],
                volume=Decimal("100000"),
                liquidity=Decimal("10000"),
                active=True,
                closed=False,
                enable_order_book=True,
            ),
        }

        def mock_get_market(market_id):
            return markets.get(market_id)

        client.get_market.side_effect = mock_get_market

        # Create orderbooks with varying arb opportunities
        orderbooks = {
            "m1_yes": OrderBook(
                market="m1_yes",
                asset_id="m1_yes",
                bids=[PriceLevel(price=Decimal("0.44"), size=Decimal("200"))],
                asks=[PriceLevel(price=Decimal("0.45"), size=Decimal("200"))],
                hash="0x1y",
            ),
            "m1_no": OrderBook(
                market="m1_no",
                asset_id="m1_no",
                bids=[PriceLevel(price=Decimal("0.44"), size=Decimal("200"))],
                asks=[PriceLevel(price=Decimal("0.45"), size=Decimal("200"))],
                hash="0x1n",
            ),
            "m2_yes": OrderBook(
                market="m2_yes",
                asset_id="m2_yes",
                bids=[PriceLevel(price=Decimal("0.49"), size=Decimal("100"))],
                asks=[PriceLevel(price=Decimal("0.50"), size=Decimal("100"))],
                hash="0x2y",
            ),
            "m2_no": OrderBook(
                market="m2_no",
                asset_id="m2_no",
                bids=[PriceLevel(price=Decimal("0.49"), size=Decimal("100"))],
                asks=[PriceLevel(price=Decimal("0.50"), size=Decimal("100"))],
                hash="0x2n",
            ),
            # Market 3 has no arb (cost >= 1)
            "m3_yes": OrderBook(
                market="m3_yes",
                asset_id="m3_yes",
                bids=[PriceLevel(price=Decimal("0.54"), size=Decimal("100"))],
                asks=[PriceLevel(price=Decimal("0.55"), size=Decimal("100"))],
                hash="0x3y",
            ),
            "m3_no": OrderBook(
                market="m3_no",
                asset_id="m3_no",
                bids=[PriceLevel(price=Decimal("0.44"), size=Decimal("100"))],
                asks=[PriceLevel(price=Decimal("0.50"), size=Decimal("100"))],
                hash="0x3n",
            ),
        }

        def mock_get_orderbook(token_id):
            return orderbooks.get(token_id)

        client.get_orderbook.side_effect = mock_get_orderbook

        return PredictionMarketDataProvider(client, cache_ttl=5)

    def test_detect_cross_market_arbitrage(self, multi_market_provider):
        """Test scanning multiple markets for arbitrage."""
        opps = multi_market_provider.detect_cross_market_arbitrage(
            market_ids=["market1", "market2", "market3"],
            min_profit_pct=Decimal("0"),
        )

        # Market1: 0.45 + 0.45 = 0.90 -> 10% profit
        # Market2: 0.50 + 0.50 = 1.00 -> 0% profit (no arb, exact 1.00)
        # Market3: 0.55 + 0.50 = 1.05 -> no arb
        assert len(opps) == 1  # Only market1 has arb
        assert opps[0].market_id == "market1"

    def test_detect_cross_market_arbitrage_min_profit_filter(self, multi_market_provider):
        """Test filtering by minimum profit percentage."""
        # First, fix market2 to have a small arb
        multi_market_provider.client.get_orderbook.side_effect = lambda token_id: {
            "m1_yes": OrderBook(
                market="m1_yes",
                asset_id="m1_yes",
                bids=[PriceLevel(price=Decimal("0.44"), size=Decimal("200"))],
                asks=[PriceLevel(price=Decimal("0.45"), size=Decimal("200"))],
                hash="0x1y",
            ),
            "m1_no": OrderBook(
                market="m1_no",
                asset_id="m1_no",
                bids=[PriceLevel(price=Decimal("0.44"), size=Decimal("200"))],
                asks=[PriceLevel(price=Decimal("0.45"), size=Decimal("200"))],
                hash="0x1n",
            ),
            "m2_yes": OrderBook(
                market="m2_yes",
                asset_id="m2_yes",
                bids=[PriceLevel(price=Decimal("0.48"), size=Decimal("100"))],
                asks=[PriceLevel(price=Decimal("0.49"), size=Decimal("100"))],
                hash="0x2y",
            ),
            "m2_no": OrderBook(
                market="m2_no",
                asset_id="m2_no",
                bids=[PriceLevel(price=Decimal("0.48"), size=Decimal("100"))],
                asks=[PriceLevel(price=Decimal("0.49"), size=Decimal("100"))],
                hash="0x2n",
            ),
        }.get(token_id)
        multi_market_provider.clear_cache()

        # With 5% minimum, only market1 (10%) should be returned
        opps = multi_market_provider.detect_cross_market_arbitrage(
            market_ids=["market1", "market2"],
            min_profit_pct=Decimal("5.0"),
        )

        # Market1: 0.45 + 0.45 = 0.90 -> ~11% profit
        # Market2: 0.49 + 0.49 = 0.98 -> ~2% profit
        assert len(opps) == 1
        assert opps[0].market_id == "market1"

    def test_detect_cross_market_arbitrage_sorted_by_profit(self, multi_market_provider):
        """Test results are sorted by profit percentage descending."""
        # Set up multiple markets with arb
        multi_market_provider.client.get_orderbook.side_effect = lambda token_id: {
            "m1_yes": OrderBook(
                market="m1_yes",
                asset_id="m1_yes",
                bids=[PriceLevel(price=Decimal("0.44"), size=Decimal("100"))],
                asks=[PriceLevel(price=Decimal("0.48"), size=Decimal("100"))],
                hash="0x1y",
            ),
            "m1_no": OrderBook(
                market="m1_no",
                asset_id="m1_no",
                bids=[PriceLevel(price=Decimal("0.44"), size=Decimal("100"))],
                asks=[PriceLevel(price=Decimal("0.48"), size=Decimal("100"))],
                hash="0x1n",
            ),
            "m2_yes": OrderBook(
                market="m2_yes",
                asset_id="m2_yes",
                bids=[PriceLevel(price=Decimal("0.44"), size=Decimal("100"))],
                asks=[PriceLevel(price=Decimal("0.45"), size=Decimal("100"))],
                hash="0x2y",
            ),
            "m2_no": OrderBook(
                market="m2_no",
                asset_id="m2_no",
                bids=[PriceLevel(price=Decimal("0.44"), size=Decimal("100"))],
                asks=[PriceLevel(price=Decimal("0.45"), size=Decimal("100"))],
                hash="0x2n",
            ),
        }.get(token_id)
        multi_market_provider.clear_cache()

        opps = multi_market_provider.detect_cross_market_arbitrage(
            market_ids=["market1", "market2"],
            min_profit_pct=Decimal("0"),
        )

        # Market2 has better arb (0.90 cost) than market1 (0.96 cost)
        assert len(opps) == 2
        assert opps[0].market_id == "market2"  # Higher profit first
        assert opps[0].expected_profit_pct > opps[1].expected_profit_pct

    def test_detect_cross_market_arbitrage_empty_list(self, multi_market_provider):
        """Test with empty market list."""
        opps = multi_market_provider.detect_cross_market_arbitrage(
            market_ids=[],
            min_profit_pct=Decimal("0"),
        )

        assert opps == []


class TestCalculateImpliedProbability:
    """Tests for implied probability calculation."""

    @pytest.fixture
    def provider(self) -> PredictionMarketDataProvider:
        """Create provider for testing."""
        client = MagicMock()
        return PredictionMarketDataProvider(client, cache_ttl=5)

    def test_basic_implied_probability(self, provider):
        """Test basic price to probability conversion."""
        prob = provider.calculate_implied_probability(Decimal("0.65"))
        assert prob == Decimal("0.65")

    def test_implied_probability_with_fees(self, provider):
        """Test probability adjustment with fees."""
        prob = provider.calculate_implied_probability(
            price=Decimal("0.65"),
            fee_rate_bps=200,  # 2% fee
        )

        # With 2% fee, probability should be higher
        # prob = 0.65 / (1 - 0.02) = 0.65 / 0.98 ≈ 0.6633
        expected = Decimal("0.65") / Decimal("0.98")
        assert abs(prob - expected) < Decimal("0.001")

    def test_implied_probability_with_spread(self, provider):
        """Test probability adjustment with spread."""
        prob = provider.calculate_implied_probability(
            price=Decimal("0.65"),
            spread=Decimal("0.04"),
        )

        # Midpoint adjustment: 0.65 - 0.04/2 = 0.63
        assert prob == Decimal("0.63")

    def test_implied_probability_with_fees_and_spread(self, provider):
        """Test probability adjustment with both fees and spread."""
        prob = provider.calculate_implied_probability(
            price=Decimal("0.65"),
            fee_rate_bps=100,  # 1% fee
            spread=Decimal("0.02"),
        )

        # First apply fee: 0.65 / 0.99 ≈ 0.6566
        # Then apply spread: 0.6566 - 0.01 ≈ 0.6466
        assert prob >= Decimal("0.64")
        assert prob < Decimal("0.67")

    def test_implied_probability_clamped_high(self, provider):
        """Test probability is clamped to 1.0 maximum."""
        prob = provider.calculate_implied_probability(
            price=Decimal("0.99"),
            fee_rate_bps=500,  # 5% fee would push above 1.0
        )

        # Should be clamped to 1.0
        assert prob <= Decimal("1.0")

    def test_implied_probability_clamped_low(self, provider):
        """Test probability is clamped to 0.0 minimum."""
        prob = provider.calculate_implied_probability(
            price=Decimal("0.01"),
            spread=Decimal("0.10"),  # Large spread would push negative
        )

        # Should be clamped to 0.0
        assert prob >= Decimal("0.0")

    def test_implied_probability_edge_cases(self, provider):
        """Test edge case prices."""
        # Minimum price
        prob_min = provider.calculate_implied_probability(Decimal("0.01"))
        assert prob_min == Decimal("0.01")

        # Maximum price
        prob_max = provider.calculate_implied_probability(Decimal("0.99"))
        assert prob_max == Decimal("0.99")

        # 50/50 price
        prob_50 = provider.calculate_implied_probability(Decimal("0.50"))
        assert prob_50 == Decimal("0.50")
