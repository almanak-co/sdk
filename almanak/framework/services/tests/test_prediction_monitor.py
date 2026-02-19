"""Tests for the Prediction Position Monitor service."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.services.prediction_monitor import (
    MonitoredPosition,
    MonitoringResult,
    PositionSnapshot,
    PredictionEvent,
    PredictionExitConditions,
    PredictionPositionMonitor,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def monitor() -> PredictionPositionMonitor:
    """Create a monitor instance for testing."""
    return PredictionPositionMonitor(
        strategy_id="test-strategy",
        check_interval=60,
        emit_events=False,
    )


@pytest.fixture
def basic_position() -> MonitoredPosition:
    """Create a basic position for testing."""
    return MonitoredPosition(
        market_id="test-market",
        condition_id="0x1234",
        token_id="12345",
        outcome="YES",
        size=Decimal("100"),
        entry_price=Decimal("0.65"),
        entry_time=datetime.now(UTC),
    )


@pytest.fixture
def position_with_conditions() -> MonitoredPosition:
    """Create a position with exit conditions."""
    return MonitoredPosition(
        market_id="test-market",
        condition_id="0x1234",
        token_id="12345",
        outcome="YES",
        size=Decimal("100"),
        entry_price=Decimal("0.65"),
        entry_time=datetime.now(UTC),
        exit_conditions=PredictionExitConditions(
            stop_loss_price=Decimal("0.50"),
            take_profit_price=Decimal("0.85"),
            exit_before_resolution_hours=24,
            trailing_stop_pct=Decimal("0.10"),
            max_spread_pct=Decimal("0.05"),
            min_liquidity_usd=Decimal("1000"),
        ),
    )


@pytest.fixture
def basic_snapshot() -> PositionSnapshot:
    """Create a basic snapshot for testing."""
    return PositionSnapshot(
        market_id="test-market",
        current_price=Decimal("0.70"),
        bid_price=Decimal("0.69"),
        ask_price=Decimal("0.71"),
        liquidity_usd=Decimal("5000"),
        is_resolved=False,
    )


# =============================================================================
# Test MonitoredPosition
# =============================================================================


class TestMonitoredPosition:
    """Tests for MonitoredPosition dataclass."""

    def test_basic_creation(self) -> None:
        """Test creating a basic position."""
        position = MonitoredPosition(
            market_id="test-market",
            condition_id="0x1234",
            token_id="12345",
            outcome="YES",
            size=Decimal("100"),
            entry_price=Decimal("0.65"),
            entry_time=datetime.now(UTC),
        )

        assert position.market_id == "test-market"
        assert position.outcome == "YES"
        assert position.size == Decimal("100")
        assert position.exit_conditions is None

    def test_to_dict(self, basic_position: MonitoredPosition) -> None:
        """Test serialization to dictionary."""
        data = basic_position.to_dict()

        assert data["market_id"] == "test-market"
        assert data["outcome"] == "YES"
        assert data["size"] == "100"
        assert data["entry_price"] == "0.65"
        assert data["exit_conditions"] is None

    def test_to_dict_with_conditions(self, position_with_conditions: MonitoredPosition) -> None:
        """Test serialization with exit conditions."""
        data = position_with_conditions.to_dict()

        assert data["exit_conditions"] is not None
        assert data["exit_conditions"]["stop_loss_price"] == "0.50"
        assert data["exit_conditions"]["take_profit_price"] == "0.85"


# =============================================================================
# Test PredictionExitConditions
# =============================================================================


class TestPredictionExitConditions:
    """Tests for PredictionExitConditions dataclass."""

    def test_default_values(self) -> None:
        """Test default values are None."""
        conditions = PredictionExitConditions()

        assert conditions.stop_loss_price is None
        assert conditions.take_profit_price is None
        assert conditions.exit_before_resolution_hours is None
        assert conditions.trailing_stop_pct is None

    def test_to_dict(self) -> None:
        """Test serialization."""
        conditions = PredictionExitConditions(
            stop_loss_price=Decimal("0.50"),
            take_profit_price=Decimal("0.85"),
        )

        data = conditions.to_dict()

        assert data["stop_loss_price"] == "0.50"
        assert data["take_profit_price"] == "0.85"
        assert data["exit_before_resolution_hours"] is None


# =============================================================================
# Test PredictionPositionMonitor - Position Management
# =============================================================================


class TestPredictionPositionMonitorManagement:
    """Tests for position management methods."""

    def test_add_position(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test adding a position."""
        monitor.add_position(basic_position)

        assert "test-market" in monitor.positions
        assert monitor.positions["test-market"] == basic_position

    def test_add_position_initializes_highest_price(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test that adding a position initializes highest_price."""
        assert basic_position.highest_price is None

        monitor.add_position(basic_position)

        # highest_price should be set to entry_price
        assert basic_position.highest_price == basic_position.entry_price

    def test_remove_position(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test removing a position."""
        monitor.add_position(basic_position)
        removed = monitor.remove_position("test-market")

        assert removed == basic_position
        assert "test-market" not in monitor.positions

    def test_remove_nonexistent_position(
        self,
        monitor: PredictionPositionMonitor,
    ) -> None:
        """Test removing a position that doesn't exist."""
        removed = monitor.remove_position("nonexistent")
        assert removed is None

    def test_get_position(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test getting a position by market ID."""
        monitor.add_position(basic_position)

        position = monitor.get_position("test-market")
        assert position == basic_position

        position = monitor.get_position("nonexistent")
        assert position is None

    def test_update_position_price(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test updating position price."""
        monitor.add_position(basic_position)

        # Update to higher price
        monitor.update_position_price("test-market", Decimal("0.75"))

        position = monitor.get_position("test-market")
        assert position is not None
        assert position.current_price == Decimal("0.75")
        assert position.highest_price == Decimal("0.75")

        # Update to lower price - highest should not change
        monitor.update_position_price("test-market", Decimal("0.70"))

        position = monitor.get_position("test-market")
        assert position is not None
        assert position.current_price == Decimal("0.70")
        assert position.highest_price == Decimal("0.75")

    def test_clear(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test clearing all positions."""
        monitor.add_position(basic_position)
        monitor.clear()

        assert len(monitor.positions) == 0


# =============================================================================
# Test PredictionPositionMonitor - Market Resolution
# =============================================================================


class TestMarketResolution:
    """Tests for market resolution detection."""

    def test_market_resolved_winner(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test detecting market resolution with winning position."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("1.00"),
            is_resolved=True,
            winning_outcome="YES",
        )

        result = monitor.check_position(basic_position, snapshot)

        assert result.triggered is True
        assert result.event == PredictionEvent.MARKET_RESOLVED
        assert result.details["is_winner"] is True
        assert result.suggested_action == "REDEEM"

    def test_market_resolved_loser(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test detecting market resolution with losing position."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.00"),
            is_resolved=True,
            winning_outcome="NO",
        )

        result = monitor.check_position(basic_position, snapshot)

        assert result.triggered is True
        assert result.event == PredictionEvent.MARKET_RESOLVED
        assert result.details["is_winner"] is False
        assert result.suggested_action is None

    def test_market_not_resolved(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
        basic_snapshot: PositionSnapshot,
    ) -> None:
        """Test that non-resolved market doesn't trigger."""
        result = monitor.check_position(basic_position, basic_snapshot)

        assert result.triggered is False
        assert result.event is None


# =============================================================================
# Test PredictionPositionMonitor - Stop Loss
# =============================================================================


class TestStopLoss:
    """Tests for stop-loss detection."""

    def test_stop_loss_triggered(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test stop-loss trigger when price drops below threshold."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.45"),  # Below 0.50 stop-loss
        )

        result = monitor.check_position(position_with_conditions, snapshot)

        assert result.triggered is True
        assert result.event == PredictionEvent.STOP_LOSS_TRIGGERED
        assert result.suggested_action == "SELL"
        assert result.details["stop_loss_price"] == "0.50"
        assert result.details["current_price"] == "0.45"

    def test_stop_loss_not_triggered(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test stop-loss not triggered when price is above threshold."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.55"),  # Above 0.50 stop-loss
        )

        result = monitor.check_position(position_with_conditions, snapshot)

        # Could be triggered by other conditions, so just check stop-loss didn't trigger
        assert result.event != PredictionEvent.STOP_LOSS_TRIGGERED

    def test_stop_loss_no_conditions(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test that stop-loss doesn't trigger without conditions."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.45"),
        )

        result = monitor.check_position(basic_position, snapshot)

        assert result.event != PredictionEvent.STOP_LOSS_TRIGGERED


# =============================================================================
# Test PredictionPositionMonitor - Take Profit
# =============================================================================


class TestTakeProfit:
    """Tests for take-profit detection."""

    def test_take_profit_triggered(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test take-profit trigger when price rises above threshold."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.90"),  # Above 0.85 take-profit
        )

        result = monitor.check_position(position_with_conditions, snapshot)

        assert result.triggered is True
        assert result.event == PredictionEvent.TAKE_PROFIT_TRIGGERED
        assert result.suggested_action == "SELL"

    def test_take_profit_at_threshold(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test take-profit triggers at exact threshold."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.85"),  # Exactly at take-profit
        )

        result = monitor.check_position(position_with_conditions, snapshot)

        assert result.triggered is True
        assert result.event == PredictionEvent.TAKE_PROFIT_TRIGGERED


# =============================================================================
# Test PredictionPositionMonitor - Trailing Stop
# =============================================================================


class TestTrailingStop:
    """Tests for trailing stop detection."""

    def test_trailing_stop_triggered(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test trailing stop trigger after price drop from high."""
        # Set a high price first
        position_with_conditions.highest_price = Decimal("0.80")

        # Price drops more than 10%
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),  # 12.5% below 0.80
        )

        result = monitor.check_position(position_with_conditions, snapshot)

        assert result.triggered is True
        assert result.event == PredictionEvent.TRAILING_STOP_TRIGGERED
        assert result.details["highest_price"] == "0.80"
        assert result.details["trailing_stop_pct"] == "0.10"

    def test_trailing_stop_not_triggered(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test trailing stop not triggered when within threshold."""
        position_with_conditions.highest_price = Decimal("0.80")

        # Price drops less than 10%
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.75"),  # 6.25% below 0.80
        )

        result = monitor.check_position(position_with_conditions, snapshot)

        assert result.event != PredictionEvent.TRAILING_STOP_TRIGGERED

    def test_trailing_stop_updates_highest(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test that highest_price is updated when price rises."""
        position_with_conditions.highest_price = Decimal("0.70")

        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.80"),  # New high
        )

        monitor.check_position(position_with_conditions, snapshot)

        assert position_with_conditions.highest_price == Decimal("0.80")


# =============================================================================
# Test PredictionPositionMonitor - Resolution Approaching
# =============================================================================


class TestResolutionApproaching:
    """Tests for resolution approaching detection."""

    def test_resolution_approaching_triggered(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test alert when resolution is within threshold."""
        # Set end date to 12 hours from now (within 24 hour threshold)
        end_date = datetime.now(UTC) + timedelta(hours=12)

        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            market_end_date=end_date,
        )

        result = monitor.check_position(position_with_conditions, snapshot)

        assert result.triggered is True
        assert result.event == PredictionEvent.RESOLUTION_APPROACHING
        assert result.suggested_action == "SELL"

    def test_resolution_not_approaching(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test no alert when resolution is far away."""
        # Set end date to 48 hours from now (outside 24 hour threshold)
        end_date = datetime.now(UTC) + timedelta(hours=48)

        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            market_end_date=end_date,
        )

        result = monitor.check_position(position_with_conditions, snapshot)

        assert result.event != PredictionEvent.RESOLUTION_APPROACHING


# =============================================================================
# Test PredictionPositionMonitor - Liquidity Warning
# =============================================================================


class TestLiquidityWarning:
    """Tests for liquidity warning detection."""

    def test_low_liquidity_triggered(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test warning when liquidity is below threshold."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            liquidity_usd=Decimal("500"),  # Below 1000 threshold
        )

        result = monitor.check_position(position_with_conditions, snapshot)

        assert result.triggered is True
        assert result.event == PredictionEvent.LOW_LIQUIDITY
        assert result.suggested_action is None  # Warning only

    def test_liquidity_sufficient(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test no warning when liquidity is sufficient."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            liquidity_usd=Decimal("5000"),
        )

        result = monitor.check_position(position_with_conditions, snapshot)

        assert result.event != PredictionEvent.LOW_LIQUIDITY


# =============================================================================
# Test PredictionPositionMonitor - Spread Warning
# =============================================================================


class TestSpreadWarning:
    """Tests for spread warning detection."""

    def test_spread_too_wide(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test warning when spread exceeds threshold."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            bid_price=Decimal("0.60"),
            ask_price=Decimal("0.70"),  # 14% spread
            liquidity_usd=Decimal("5000"),
        )

        result = monitor.check_position(position_with_conditions, snapshot)

        assert result.triggered is True
        assert result.event == PredictionEvent.SPREAD_TOO_WIDE
        assert result.suggested_action is None  # Warning only

    def test_spread_acceptable(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test no warning when spread is acceptable."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            bid_price=Decimal("0.69"),
            ask_price=Decimal("0.71"),  # ~3% spread
            liquidity_usd=Decimal("5000"),
        )

        result = monitor.check_position(position_with_conditions, snapshot)

        assert result.event != PredictionEvent.SPREAD_TOO_WIDE


# =============================================================================
# Test PredictionPositionMonitor - check_positions
# =============================================================================


class TestCheckPositions:
    """Tests for bulk position checking."""

    def test_check_multiple_positions(
        self,
        monitor: PredictionPositionMonitor,
    ) -> None:
        """Test checking multiple positions at once."""
        position1 = MonitoredPosition(
            market_id="market-1",
            condition_id="0x1234",
            token_id="12345",
            outcome="YES",
            size=Decimal("100"),
            entry_price=Decimal("0.65"),
            entry_time=datetime.now(UTC),
        )

        position2 = MonitoredPosition(
            market_id="market-2",
            condition_id="0x5678",
            token_id="67890",
            outcome="NO",
            size=Decimal("200"),
            entry_price=Decimal("0.35"),
            entry_time=datetime.now(UTC),
        )

        monitor.add_position(position1)
        monitor.add_position(position2)

        snapshots = {
            "market-1": PositionSnapshot(
                market_id="market-1",
                current_price=Decimal("0.70"),
            ),
            "market-2": PositionSnapshot(
                market_id="market-2",
                current_price=Decimal("0.30"),
            ),
        }

        results = monitor.check_positions(snapshots)

        assert len(results) == 2

    def test_missing_snapshot(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test that missing snapshots are handled gracefully."""
        monitor.add_position(basic_position)

        results = monitor.check_positions({})  # No snapshots provided

        assert len(results) == 0


# =============================================================================
# Test PredictionPositionMonitor - Event Callback
# =============================================================================


class TestEventCallback:
    """Tests for event callback functionality."""

    def test_callback_called_on_event(self) -> None:
        """Test that callback is called when event is triggered."""
        callback = MagicMock()

        monitor = PredictionPositionMonitor(
            strategy_id="test-strategy",
            emit_events=False,
            event_callback=callback,
        )

        position = MonitoredPosition(
            market_id="test-market",
            condition_id="0x1234",
            token_id="12345",
            outcome="YES",
            size=Decimal("100"),
            entry_price=Decimal("0.65"),
            entry_time=datetime.now(UTC),
        )

        monitor.add_position(position)

        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("1.00"),
            is_resolved=True,
            winning_outcome="YES",
        )

        monitor.check_positions({"test-market": snapshot})

        callback.assert_called_once()
        call_args = callback.call_args
        assert call_args[0][0] == position
        assert call_args[0][1] == PredictionEvent.MARKET_RESOLVED

    def test_callback_exception_handled(self) -> None:
        """Test that callback exceptions don't break monitoring."""
        callback = MagicMock(side_effect=Exception("Callback error"))

        monitor = PredictionPositionMonitor(
            strategy_id="test-strategy",
            emit_events=False,
            event_callback=callback,
        )

        position = MonitoredPosition(
            market_id="test-market",
            condition_id="0x1234",
            token_id="12345",
            outcome="YES",
            size=Decimal("100"),
            entry_price=Decimal("0.65"),
            entry_time=datetime.now(UTC),
        )

        monitor.add_position(position)

        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("1.00"),
            is_resolved=True,
            winning_outcome="YES",
        )

        # Should not raise
        results = monitor.check_positions({"test-market": snapshot})
        assert len(results) == 1


# =============================================================================
# Test PredictionPositionMonitor - Timeline Events
# =============================================================================


class TestTimelineEvents:
    """Tests for timeline event emission."""

    def test_emit_timeline_event(self) -> None:
        """Test that timeline events are emitted."""
        with patch("almanak.framework.services.prediction_monitor.add_event") as mock_add:
            monitor = PredictionPositionMonitor(
                strategy_id="test-strategy",
                emit_events=True,
            )

            position = MonitoredPosition(
                market_id="test-market",
                condition_id="0x1234",
                token_id="12345",
                outcome="YES",
                size=Decimal("100"),
                entry_price=Decimal("0.65"),
                entry_time=datetime.now(UTC),
            )

            monitor.add_position(position)

            snapshot = PositionSnapshot(
                market_id="test-market",
                current_price=Decimal("1.00"),
                is_resolved=True,
                winning_outcome="YES",
            )

            monitor.check_positions({"test-market": snapshot})

            mock_add.assert_called_once()
            event = mock_add.call_args[0][0]
            assert event.strategy_id == "test-strategy"
            assert "MARKET_RESOLVED" in event.description

    def test_no_emit_when_disabled(self) -> None:
        """Test that events are not emitted when disabled."""
        with patch("almanak.framework.services.prediction_monitor.add_event") as mock_add:
            monitor = PredictionPositionMonitor(
                strategy_id="test-strategy",
                emit_events=False,
            )

            position = MonitoredPosition(
                market_id="test-market",
                condition_id="0x1234",
                token_id="12345",
                outcome="YES",
                size=Decimal("100"),
                entry_price=Decimal("0.65"),
                entry_time=datetime.now(UTC),
            )

            monitor.add_position(position)

            snapshot = PositionSnapshot(
                market_id="test-market",
                current_price=Decimal("1.00"),
                is_resolved=True,
                winning_outcome="YES",
            )

            monitor.check_positions({"test-market": snapshot})

            mock_add.assert_not_called()


# =============================================================================
# Test MonitoringResult
# =============================================================================


class TestMonitoringResult:
    """Tests for MonitoringResult dataclass."""

    def test_to_dict(self, basic_position: MonitoredPosition) -> None:
        """Test serialization to dictionary."""
        result = MonitoringResult(
            position=basic_position,
            event=PredictionEvent.STOP_LOSS_TRIGGERED,
            triggered=True,
            details={"price": "0.45"},
            suggested_action="SELL",
        )

        data = result.to_dict()

        assert data["event"] == "STOP_LOSS_TRIGGERED"
        assert data["triggered"] is True
        assert data["suggested_action"] == "SELL"
        assert data["position"]["market_id"] == "test-market"

    def test_to_dict_no_event(self, basic_position: MonitoredPosition) -> None:
        """Test serialization when no event triggered."""
        result = MonitoringResult(position=basic_position)

        data = result.to_dict()

        assert data["event"] is None
        assert data["triggered"] is False


# =============================================================================
# Test generate_sell_intent (US-015)
# =============================================================================


class TestGenerateSellIntent:
    """Tests for the generate_sell_intent method."""

    def test_generate_sell_intent_for_stop_loss(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test generating sell intent when stop-loss is triggered."""
        from almanak.framework.intents.vocabulary import PredictionSellIntent

        result = MonitoringResult(
            position=position_with_conditions,
            event=PredictionEvent.STOP_LOSS_TRIGGERED,
            triggered=True,
            details={
                "current_price": "0.45",
                "stop_loss_price": "0.50",
            },
            suggested_action="SELL",
        )

        sell_intent = monitor.generate_sell_intent(result)

        assert sell_intent is not None
        assert isinstance(sell_intent, PredictionSellIntent)
        assert sell_intent.market_id == "test-market"
        assert sell_intent.outcome == "YES"
        assert sell_intent.shares == "all"
        assert sell_intent.order_type == "limit"
        assert sell_intent.time_in_force == "IOC"
        # min_price should be 95% of stop_loss_price
        assert sell_intent.min_price == Decimal("0.50") * Decimal("0.95")

    def test_generate_sell_intent_for_take_profit(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test generating sell intent when take-profit is triggered."""
        from almanak.framework.intents.vocabulary import PredictionSellIntent

        result = MonitoringResult(
            position=position_with_conditions,
            event=PredictionEvent.TAKE_PROFIT_TRIGGERED,
            triggered=True,
            details={
                "current_price": "0.90",
                "take_profit_price": "0.85",
            },
            suggested_action="SELL",
        )

        sell_intent = monitor.generate_sell_intent(result)

        assert sell_intent is not None
        assert isinstance(sell_intent, PredictionSellIntent)
        assert sell_intent.min_price == Decimal("0.85")
        assert sell_intent.order_type == "limit"

    def test_generate_sell_intent_for_trailing_stop(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test generating sell intent when trailing stop is triggered."""
        from almanak.framework.intents.vocabulary import PredictionSellIntent

        result = MonitoringResult(
            position=position_with_conditions,
            event=PredictionEvent.TRAILING_STOP_TRIGGERED,
            triggered=True,
            details={
                "current_price": "0.60",
                "highest_price": "0.75",
                "trailing_stop_price": "0.675",  # 75 * 0.9
            },
            suggested_action="SELL",
        )

        sell_intent = monitor.generate_sell_intent(result)

        assert sell_intent is not None
        assert isinstance(sell_intent, PredictionSellIntent)
        # min_price should be 95% of trailing_stop_price
        expected_min = Decimal("0.675") * Decimal("0.95")
        assert sell_intent.min_price == expected_min
        assert sell_intent.order_type == "limit"

    def test_generate_sell_intent_for_resolution_approaching(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test generating sell intent when resolution is approaching."""
        from almanak.framework.intents.vocabulary import PredictionSellIntent

        result = MonitoringResult(
            position=position_with_conditions,
            event=PredictionEvent.RESOLUTION_APPROACHING,
            triggered=True,
            details={
                "hours_until_end": 12,
                "current_price": "0.70",
            },
            suggested_action="SELL",
        )

        sell_intent = monitor.generate_sell_intent(result)

        assert sell_intent is not None
        assert isinstance(sell_intent, PredictionSellIntent)
        # Pre-resolution exit uses market order (no min_price)
        assert sell_intent.min_price is None
        assert sell_intent.order_type == "market"
        assert sell_intent.time_in_force == "IOC"

    def test_generate_sell_intent_returns_none_for_non_triggered(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test that no sell intent is generated when no event is triggered."""
        result = MonitoringResult(
            position=basic_position,
            event=None,
            triggered=False,
        )

        sell_intent = monitor.generate_sell_intent(result)

        assert sell_intent is None

    def test_generate_sell_intent_returns_none_for_warning_events(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test that no sell intent is generated for warning events."""
        # LOW_LIQUIDITY is a warning, not an actionable sell event
        result = MonitoringResult(
            position=position_with_conditions,
            event=PredictionEvent.LOW_LIQUIDITY,
            triggered=True,
            details={"current_liquidity_usd": "500"},
            suggested_action=None,  # No action suggested for warnings
        )

        sell_intent = monitor.generate_sell_intent(result)

        assert sell_intent is None

    def test_generate_sell_intent_returns_none_for_redeem_action(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test that no sell intent is generated for REDEEM actions."""
        result = MonitoringResult(
            position=basic_position,
            event=PredictionEvent.MARKET_RESOLVED,
            triggered=True,
            details={"winning_outcome": "YES"},
            suggested_action="REDEEM",  # Not SELL
        )

        sell_intent = monitor.generate_sell_intent(result)

        assert sell_intent is None

    def test_generate_sell_intent_preserves_outcome(
        self,
        monitor: PredictionPositionMonitor,
    ) -> None:
        """Test that sell intent preserves the position's outcome."""

        # Create a NO position
        no_position = MonitoredPosition(
            market_id="test-market",
            condition_id="0x1234",
            token_id="67890",
            outcome="NO",
            size=Decimal("50"),
            entry_price=Decimal("0.35"),
            entry_time=datetime.now(UTC),
            exit_conditions=PredictionExitConditions(
                stop_loss_price=Decimal("0.20"),
            ),
        )

        result = MonitoringResult(
            position=no_position,
            event=PredictionEvent.STOP_LOSS_TRIGGERED,
            triggered=True,
            details={"current_price": "0.15"},
            suggested_action="SELL",
        )

        sell_intent = monitor.generate_sell_intent(result)

        assert sell_intent is not None
        assert sell_intent.outcome == "NO"

    def test_generate_sell_intent_without_exit_conditions(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test generating sell intent when position has no exit conditions."""
        from almanak.framework.intents.vocabulary import PredictionSellIntent

        # basic_position has no exit_conditions
        result = MonitoringResult(
            position=basic_position,
            event=PredictionEvent.RESOLUTION_APPROACHING,
            triggered=True,
            details={"hours_until_end": 6},
            suggested_action="SELL",
        )

        sell_intent = monitor.generate_sell_intent(result)

        assert sell_intent is not None
        assert isinstance(sell_intent, PredictionSellIntent)
        # Without exit conditions, uses market order
        assert sell_intent.min_price is None
        assert sell_intent.order_type == "market"


# =============================================================================
# Test Partial Exit Functionality (US-016)
# =============================================================================


class TestPartialExits:
    """Tests for partial exit functionality when liquidity is insufficient."""

    def test_calculate_safe_exit_size_full_liquidity(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test that (None, False) is returned when full liquidity is available."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            orderbook_depth_shares=Decimal("200"),  # More than position size (100)
        )

        safe_size, is_constrained = monitor.calculate_safe_exit_size(basic_position, snapshot)

        # (None, False) means "use all" - full liquidity available
        assert safe_size is None
        assert is_constrained is False

    def test_calculate_safe_exit_size_partial_liquidity(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test calculation when only partial liquidity is available."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            orderbook_depth_shares=Decimal("50"),  # Half of position size (100)
        )

        safe_size, is_constrained = monitor.calculate_safe_exit_size(basic_position, snapshot)

        assert safe_size is not None
        assert is_constrained is True
        # Should be 95% of available (50 * 0.95 = 47.5, rounded to 47.50)
        assert safe_size == Decimal("47.50")

    def test_calculate_safe_exit_size_insufficient_liquidity(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test that (None, True) is returned when liquidity is too low for meaningful exit."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            orderbook_depth_shares=Decimal("5"),  # Only 5% of position (below 10% threshold)
        )

        safe_size, is_constrained = monitor.calculate_safe_exit_size(basic_position, snapshot)

        # (None, True) means liquidity is insufficient - no exit possible
        assert safe_size is None
        assert is_constrained is True

    def test_calculate_safe_exit_size_no_orderbook_data(
        self,
        monitor: PredictionPositionMonitor,
        basic_position: MonitoredPosition,
    ) -> None:
        """Test that (None, False) is returned when orderbook depth data is not available."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            orderbook_depth_shares=None,  # No orderbook data
        )

        safe_size, is_constrained = monitor.calculate_safe_exit_size(basic_position, snapshot)

        # (None, False) means no data - proceed with "all"
        assert safe_size is None
        assert is_constrained is False

    def test_generate_sell_intent_partial_exit(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test generating partial sell intent when liquidity is limited."""
        from almanak.framework.intents.vocabulary import PredictionSellIntent

        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            orderbook_depth_shares=Decimal("40"),  # 40% of position size
        )

        result = MonitoringResult(
            position=position_with_conditions,
            event=PredictionEvent.RESOLUTION_APPROACHING,
            triggered=True,
            details={"hours_until_end": 6},
            suggested_action="SELL",
        )

        sell_intent = monitor.generate_sell_intent(result, snapshot)

        assert sell_intent is not None
        assert isinstance(sell_intent, PredictionSellIntent)
        # Should be partial (40 * 0.95 = 38.00)
        assert sell_intent.shares == Decimal("38.00")
        assert sell_intent.order_type == "market"

    def test_generate_sell_intent_full_exit_with_snapshot(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test that full exit is generated when liquidity is sufficient."""
        from almanak.framework.intents.vocabulary import PredictionSellIntent

        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            orderbook_depth_shares=Decimal("500"),  # More than enough
        )

        result = MonitoringResult(
            position=position_with_conditions,
            event=PredictionEvent.STOP_LOSS_TRIGGERED,
            triggered=True,
            details={"current_price": "0.45"},
            suggested_action="SELL",
        )

        sell_intent = monitor.generate_sell_intent(result, snapshot)

        assert sell_intent is not None
        assert isinstance(sell_intent, PredictionSellIntent)
        assert sell_intent.shares == "all"

    def test_generate_sell_intent_returns_none_for_zero_liquidity(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Test that no intent is generated when liquidity is too low."""
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            orderbook_depth_shares=Decimal("5"),  # Below 10% threshold
        )

        result = MonitoringResult(
            position=position_with_conditions,
            event=PredictionEvent.STOP_LOSS_TRIGGERED,
            triggered=True,
            details={"current_price": "0.45"},
            suggested_action="SELL",
        )

        sell_intent = monitor.generate_sell_intent(result, snapshot)

        # Should return None when liquidity is too low
        assert sell_intent is None

    def test_partial_exits_disabled(self) -> None:
        """Test that partial exits can be disabled."""
        from almanak.framework.intents.vocabulary import PredictionSellIntent

        monitor = PredictionPositionMonitor(
            strategy_id="test-strategy",
            emit_events=False,
            allow_partial_exits=False,
        )

        position = MonitoredPosition(
            market_id="test-market",
            condition_id="0x1234",
            token_id="12345",
            outcome="YES",
            size=Decimal("100"),
            entry_price=Decimal("0.65"),
            entry_time=datetime.now(UTC),
        )

        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            orderbook_depth_shares=Decimal("40"),  # Partial liquidity
        )

        result = MonitoringResult(
            position=position,
            event=PredictionEvent.RESOLUTION_APPROACHING,
            triggered=True,
            details={"hours_until_end": 6},
            suggested_action="SELL",
        )

        sell_intent = monitor.generate_sell_intent(result, snapshot)

        assert sell_intent is not None
        assert isinstance(sell_intent, PredictionSellIntent)
        # With partial exits disabled, should still try to sell "all"
        assert sell_intent.shares == "all"


# =============================================================================
# Test Strategy-Level Defaults (US-016)
# =============================================================================


class TestStrategyLevelDefaults:
    """Tests for strategy-level configuration defaults."""

    def test_default_exit_before_resolution_hours(self) -> None:
        """Test that strategy-level default is used when position has no condition."""
        monitor = PredictionPositionMonitor(
            strategy_id="test-strategy",
            emit_events=False,
            default_exit_before_resolution_hours=24,
        )

        # Position with NO exit_conditions
        position = MonitoredPosition(
            market_id="test-market",
            condition_id="0x1234",
            token_id="12345",
            outcome="YES",
            size=Decimal("100"),
            entry_price=Decimal("0.65"),
            entry_time=datetime.now(UTC),
            exit_conditions=None,
        )

        # Set end date to 12 hours from now (within 24 hour threshold)
        end_date = datetime.now(UTC) + timedelta(hours=12)

        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            market_end_date=end_date,
        )

        result = monitor.check_position(position, snapshot)

        # Should trigger using strategy-level default
        assert result.triggered is True
        assert result.event == PredictionEvent.RESOLUTION_APPROACHING
        assert result.details["exit_before_hours"] == 24

    def test_position_exit_hours_overrides_default(self) -> None:
        """Test that position-level setting overrides strategy default."""
        monitor = PredictionPositionMonitor(
            strategy_id="test-strategy",
            emit_events=False,
            default_exit_before_resolution_hours=48,  # 48 hour default
        )

        # Position with its own exit condition (12 hours)
        position = MonitoredPosition(
            market_id="test-market",
            condition_id="0x1234",
            token_id="12345",
            outcome="YES",
            size=Decimal("100"),
            entry_price=Decimal("0.65"),
            entry_time=datetime.now(UTC),
            exit_conditions=PredictionExitConditions(
                exit_before_resolution_hours=12,  # Position-specific setting
            ),
        )

        # Set end date to 18 hours from now
        # This should NOT trigger with position's 12hr setting
        # But WOULD trigger with strategy's 48hr default
        end_date = datetime.now(UTC) + timedelta(hours=18)

        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            market_end_date=end_date,
        )

        result = monitor.check_position(position, snapshot)

        # Should NOT trigger because position has 12hr setting
        # and we're still 18 hours away
        assert result.triggered is False
        assert result.event is None

    def test_no_default_no_trigger(self) -> None:
        """Test that no trigger occurs when no defaults are set."""
        monitor = PredictionPositionMonitor(
            strategy_id="test-strategy",
            emit_events=False,
            default_exit_before_resolution_hours=None,  # No default
        )

        # Position with NO exit_conditions
        position = MonitoredPosition(
            market_id="test-market",
            condition_id="0x1234",
            token_id="12345",
            outcome="YES",
            size=Decimal("100"),
            entry_price=Decimal("0.65"),
            entry_time=datetime.now(UTC),
            exit_conditions=None,
        )

        # Set end date to 1 hour from now
        end_date = datetime.now(UTC) + timedelta(hours=1)

        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            market_end_date=end_date,
        )

        result = monitor.check_position(position, snapshot)

        # Should NOT trigger for RESOLUTION_APPROACHING
        # (might trigger other conditions, so just check this specific one)
        assert result.event != PredictionEvent.RESOLUTION_APPROACHING
