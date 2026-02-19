"""Tests for MarketSnapshot prediction market extensions.

Tests the prediction market methods added to MarketSnapshot in US-011:
- prediction() - Get full market details
- prediction_positions() - Get open positions
- prediction_orders() - Get open orders
"""

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.market_snapshot import (
    MarketSnapshot,
    PredictionUnavailableError,
)
from almanak.framework.data.prediction_provider import (
    PredictionMarket,
    PredictionMarketDataProvider,
    PredictionOrder,
    PredictionPosition,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_prediction_market() -> PredictionMarket:
    """Create a mock PredictionMarket for testing."""
    return PredictionMarket(
        market_id="12345",
        condition_id="0x9915bea232fa12b20058f9cea1187ea51366352bf833393676cd0db557a58249",
        question="Will Bitcoin exceed $100,000 by end of 2025?",
        slug="will-bitcoin-exceed-100000-by-end-of-2025",
        yes_price=Decimal("0.65"),
        no_price=Decimal("0.35"),
        yes_token_id="19045189272319329424023217822141741659150265216200539353252147725932663608488",
        no_token_id="28164726938309329424023217822141741659150265216200539353252147725932663608489",
        spread=Decimal("0.02"),
        volume_24h=Decimal("125000"),
        liquidity=Decimal("50000"),
        end_date=datetime(2025, 12, 31, 23, 59, 59),
        is_active=True,
        is_resolved=False,
    )


@pytest.fixture
def mock_prediction_position() -> PredictionPosition:
    """Create a mock PredictionPosition for testing."""
    return PredictionPosition(
        market_id="12345",
        condition_id="0x9915bea",
        token_id="19045189272319329424023217822141741659150265216200539353252147725932663608488",
        outcome="YES",
        size=Decimal("100"),
        avg_price=Decimal("0.50"),
        current_price=Decimal("0.65"),
        unrealized_pnl=Decimal("15"),
        realized_pnl=Decimal("0"),
    )


@pytest.fixture
def mock_prediction_order() -> PredictionOrder:
    """Create a mock PredictionOrder for testing."""
    return PredictionOrder(
        order_id="order123",
        market_id="19045189272319329424023217822141741659150265216200539353252147725932663608488",
        outcome="YES",
        side="BUY",
        price=Decimal("0.60"),
        size=Decimal("50"),
        filled_size=Decimal("10"),
        created_at=datetime(2025, 1, 15, 10, 30, 0),
    )


@pytest.fixture
def mock_prediction_provider(
    mock_prediction_market, mock_prediction_position, mock_prediction_order
) -> PredictionMarketDataProvider:
    """Create a mock PredictionMarketDataProvider for testing."""
    provider = MagicMock(spec=PredictionMarketDataProvider)
    provider.get_market.return_value = mock_prediction_market
    provider.get_positions.return_value = [mock_prediction_position]
    provider.get_open_orders.return_value = [mock_prediction_order]
    return provider


@pytest.fixture
def snapshot_with_prediction(mock_prediction_provider) -> MarketSnapshot:
    """Create a MarketSnapshot with prediction provider configured."""
    return MarketSnapshot(
        chain="polygon",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        prediction_provider=mock_prediction_provider,
    )


@pytest.fixture
def snapshot_without_prediction() -> MarketSnapshot:
    """Create a MarketSnapshot without prediction provider."""
    return MarketSnapshot(
        chain="polygon",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
    )


# =============================================================================
# prediction() Method Tests
# =============================================================================


class TestPredictionMethod:
    """Tests for MarketSnapshot.prediction() method."""

    def test_prediction_returns_market(self, snapshot_with_prediction, mock_prediction_market):
        """Test that prediction() returns market data."""
        market = snapshot_with_prediction.prediction("12345")

        assert market.market_id == "12345"
        assert market.yes_price == Decimal("0.65")
        assert market.no_price == Decimal("0.35")
        assert market.question == "Will Bitcoin exceed $100,000 by end of 2025?"

    def test_prediction_by_slug(self, snapshot_with_prediction, mock_prediction_provider):
        """Test that prediction() works with market slug."""
        snapshot_with_prediction.prediction("will-bitcoin-exceed-100000-by-end-of-2025")

        mock_prediction_provider.get_market.assert_called_once_with("will-bitcoin-exceed-100000-by-end-of-2025")

    def test_prediction_without_provider_raises_value_error(self, snapshot_without_prediction):
        """Test that prediction() raises ValueError when no provider configured."""
        with pytest.raises(ValueError, match="No prediction provider configured"):
            snapshot_without_prediction.prediction("12345")

    def test_prediction_error_raises_prediction_unavailable_error(
        self, snapshot_with_prediction, mock_prediction_provider
    ):
        """Test that prediction() raises PredictionUnavailableError on failure."""
        mock_prediction_provider.get_market.side_effect = Exception("API error")

        with pytest.raises(PredictionUnavailableError) as exc_info:
            snapshot_with_prediction.prediction("12345")

        assert "12345" in str(exc_info.value)
        assert "API error" in str(exc_info.value)

    def test_prediction_market_attributes(self, snapshot_with_prediction, mock_prediction_market):
        """Test that returned market has all expected attributes."""
        market = snapshot_with_prediction.prediction("12345")

        assert market.condition_id == mock_prediction_market.condition_id
        assert market.slug == mock_prediction_market.slug
        assert market.spread == mock_prediction_market.spread
        assert market.volume_24h == mock_prediction_market.volume_24h
        assert market.liquidity == mock_prediction_market.liquidity
        assert market.is_active == mock_prediction_market.is_active
        assert market.is_resolved == mock_prediction_market.is_resolved


# =============================================================================
# prediction_positions() Method Tests
# =============================================================================


class TestPredictionPositionsMethod:
    """Tests for MarketSnapshot.prediction_positions() method."""

    def test_prediction_positions_returns_list(self, snapshot_with_prediction, mock_prediction_position):
        """Test that prediction_positions() returns list of positions."""
        positions = snapshot_with_prediction.prediction_positions()

        assert len(positions) == 1
        assert positions[0].market_id == "12345"
        assert positions[0].outcome == "YES"
        assert positions[0].size == Decimal("100")

    def test_prediction_positions_uses_wallet_address(self, snapshot_with_prediction, mock_prediction_provider):
        """Test that prediction_positions() uses snapshot wallet address."""
        snapshot_with_prediction.prediction_positions()

        mock_prediction_provider.get_positions.assert_called_once_with(
            wallet="0x1234567890abcdef1234567890abcdef12345678"
        )

    def test_prediction_positions_with_market_filter(
        self, snapshot_with_prediction, mock_prediction_provider, mock_prediction_market
    ):
        """Test that prediction_positions() filters by market."""
        snapshot_with_prediction.prediction_positions(market_id="12345")

        # Should resolve market first, then filter
        mock_prediction_provider.get_market.assert_called_once_with("12345")
        mock_prediction_provider.get_positions.assert_called_once_with(
            wallet="0x1234567890abcdef1234567890abcdef12345678",
            market_id="12345",
        )

    def test_prediction_positions_without_provider_raises_value_error(self, snapshot_without_prediction):
        """Test that prediction_positions() raises ValueError when no provider."""
        with pytest.raises(ValueError, match="No prediction provider configured"):
            snapshot_without_prediction.prediction_positions()

    def test_prediction_positions_error_raises_prediction_unavailable_error(
        self, snapshot_with_prediction, mock_prediction_provider
    ):
        """Test that prediction_positions() raises PredictionUnavailableError on failure."""
        mock_prediction_provider.get_positions.side_effect = Exception("Network error")

        with pytest.raises(PredictionUnavailableError) as exc_info:
            snapshot_with_prediction.prediction_positions()

        assert "Network error" in str(exc_info.value)

    def test_prediction_positions_returns_empty_list_when_no_positions(
        self, snapshot_with_prediction, mock_prediction_provider
    ):
        """Test that prediction_positions() returns empty list when no positions."""
        mock_prediction_provider.get_positions.return_value = []

        positions = snapshot_with_prediction.prediction_positions()

        assert positions == []

    def test_prediction_position_value_property(self, snapshot_with_prediction, mock_prediction_position):
        """Test that position value property is calculated correctly."""
        positions = snapshot_with_prediction.prediction_positions()

        # value = size * current_price = 100 * 0.65 = 65
        assert positions[0].value == Decimal("65")

    def test_prediction_positions_unrealized_pnl(self, snapshot_with_prediction, mock_prediction_position):
        """Test that unrealized PnL is available on position."""
        positions = snapshot_with_prediction.prediction_positions()

        # unrealized_pnl = (current_price - avg_price) * size = (0.65 - 0.50) * 100 = 15
        assert positions[0].unrealized_pnl == Decimal("15")


# =============================================================================
# prediction_orders() Method Tests
# =============================================================================


class TestPredictionOrdersMethod:
    """Tests for MarketSnapshot.prediction_orders() method."""

    def test_prediction_orders_returns_list(self, snapshot_with_prediction, mock_prediction_order):
        """Test that prediction_orders() returns list of orders."""
        orders = snapshot_with_prediction.prediction_orders()

        assert len(orders) == 1
        assert orders[0].order_id == "order123"
        assert orders[0].side == "BUY"
        assert orders[0].price == Decimal("0.60")

    def test_prediction_orders_with_market_filter(self, snapshot_with_prediction, mock_prediction_provider):
        """Test that prediction_orders() passes market filter to provider."""
        snapshot_with_prediction.prediction_orders(market_id="12345")

        mock_prediction_provider.get_open_orders.assert_called_once_with("12345")

    def test_prediction_orders_without_filter(self, snapshot_with_prediction, mock_prediction_provider):
        """Test that prediction_orders() without filter passes None."""
        snapshot_with_prediction.prediction_orders()

        mock_prediction_provider.get_open_orders.assert_called_once_with(None)

    def test_prediction_orders_without_provider_raises_value_error(self, snapshot_without_prediction):
        """Test that prediction_orders() raises ValueError when no provider."""
        with pytest.raises(ValueError, match="No prediction provider configured"):
            snapshot_without_prediction.prediction_orders()

    def test_prediction_orders_error_raises_prediction_unavailable_error(
        self, snapshot_with_prediction, mock_prediction_provider
    ):
        """Test that prediction_orders() raises PredictionUnavailableError on failure."""
        mock_prediction_provider.get_open_orders.side_effect = Exception("Auth error")

        with pytest.raises(PredictionUnavailableError) as exc_info:
            snapshot_with_prediction.prediction_orders()

        assert "Auth error" in str(exc_info.value)

    def test_prediction_orders_returns_empty_list_when_no_orders(
        self, snapshot_with_prediction, mock_prediction_provider
    ):
        """Test that prediction_orders() returns empty list when no orders."""
        mock_prediction_provider.get_open_orders.return_value = []

        orders = snapshot_with_prediction.prediction_orders()

        assert orders == []

    def test_prediction_order_remaining_size_property(self, snapshot_with_prediction, mock_prediction_order):
        """Test that order remaining_size property is calculated correctly."""
        orders = snapshot_with_prediction.prediction_orders()

        # remaining_size = size - filled_size = 50 - 10 = 40
        assert orders[0].remaining_size == Decimal("40")

    def test_prediction_order_attributes(self, snapshot_with_prediction, mock_prediction_order):
        """Test that returned order has all expected attributes."""
        orders = snapshot_with_prediction.prediction_orders()
        order = orders[0]

        assert order.order_id == mock_prediction_order.order_id
        assert order.market_id == mock_prediction_order.market_id
        assert order.outcome == mock_prediction_order.outcome
        assert order.side == mock_prediction_order.side
        assert order.price == mock_prediction_order.price
        assert order.size == mock_prediction_order.size
        assert order.filled_size == mock_prediction_order.filled_size
        assert order.created_at == mock_prediction_order.created_at


# =============================================================================
# PredictionUnavailableError Tests
# =============================================================================


class TestPredictionUnavailableError:
    """Tests for PredictionUnavailableError exception."""

    def test_error_contains_market_id(self):
        """Test that error message contains market ID."""
        error = PredictionUnavailableError("12345", "API error")

        assert error.market_id == "12345"
        assert "12345" in str(error)

    def test_error_contains_reason(self):
        """Test that error message contains reason."""
        error = PredictionUnavailableError("12345", "Network timeout")

        assert error.reason == "Network timeout"
        assert "Network timeout" in str(error)

    def test_error_is_market_snapshot_error(self):
        """Test that error is a subclass of MarketSnapshotError."""
        from almanak.framework.data.market_snapshot import MarketSnapshotError

        error = PredictionUnavailableError("12345", "Error")

        assert isinstance(error, MarketSnapshotError)


# =============================================================================
# Integration Tests
# =============================================================================


class TestMarketSnapshotPredictionIntegration:
    """Integration tests for MarketSnapshot prediction methods."""

    def test_snapshot_with_all_prediction_methods(self, snapshot_with_prediction, mock_prediction_market):
        """Test using all prediction methods together."""
        # Get market data
        market = snapshot_with_prediction.prediction("12345")
        assert market.yes_price == Decimal("0.65")

        # Get positions
        positions = snapshot_with_prediction.prediction_positions()
        assert len(positions) == 1

        # Get orders
        orders = snapshot_with_prediction.prediction_orders()
        assert len(orders) == 1

    def test_snapshot_chain_and_wallet_preserved(self, snapshot_with_prediction):
        """Test that chain and wallet are preserved in snapshot."""
        assert snapshot_with_prediction.chain == "polygon"
        assert snapshot_with_prediction.wallet_address == "0x1234567890abcdef1234567890abcdef12345678"

    def test_prediction_methods_independent_of_other_providers(self, mock_prediction_provider):
        """Test that prediction methods work without other providers."""
        # Create snapshot with only prediction provider
        snapshot = MarketSnapshot(
            chain="polygon",
            wallet_address="0xtest",
            prediction_provider=mock_prediction_provider,
            # No other providers
        )

        # Should work fine
        market = snapshot.prediction("12345")
        positions = snapshot.prediction_positions()
        orders = snapshot.prediction_orders()

        assert market is not None
        assert isinstance(positions, list)
        assert isinstance(orders, list)

    def test_multiple_positions_and_orders(
        self, mock_prediction_provider, mock_prediction_position, mock_prediction_order
    ):
        """Test handling multiple positions and orders."""
        # Setup multiple items
        pos2 = PredictionPosition(
            market_id="12345",
            condition_id="0x9915bea",
            token_id="28164726938309329424023217822141741659150265216200539353252147725932663608489",
            outcome="NO",
            size=Decimal("50"),
            avg_price=Decimal("0.30"),
            current_price=Decimal("0.35"),
            unrealized_pnl=Decimal("2.5"),
            realized_pnl=Decimal("0"),
        )
        order2 = PredictionOrder(
            order_id="order456",
            market_id="28164726938309329424023217822141741659150265216200539353252147725932663608489",
            outcome="NO",
            side="SELL",
            price=Decimal("0.40"),
            size=Decimal("25"),
            filled_size=Decimal("0"),
            created_at=datetime(2025, 1, 15, 11, 0, 0),
        )

        mock_prediction_provider.get_positions.return_value = [mock_prediction_position, pos2]
        mock_prediction_provider.get_open_orders.return_value = [mock_prediction_order, order2]

        snapshot = MarketSnapshot(
            chain="polygon",
            wallet_address="0xtest",
            prediction_provider=mock_prediction_provider,
        )

        positions = snapshot.prediction_positions()
        orders = snapshot.prediction_orders()

        assert len(positions) == 2
        assert len(orders) == 2
        assert positions[0].outcome == "YES"
        assert positions[1].outcome == "NO"
        assert orders[0].side == "BUY"
        assert orders[1].side == "SELL"
