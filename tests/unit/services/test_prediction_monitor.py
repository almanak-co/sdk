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
        deployment_id="test-strategy",
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
            deployment_id="test-strategy",
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
            deployment_id="test-strategy",
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
                deployment_id="test-strategy",
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
            assert event.deployment_id == "test-strategy"
            assert "MARKET_RESOLVED" in event.description

    def test_no_emit_when_disabled(self) -> None:
        """Test that events are not emitted when disabled."""
        with patch("almanak.framework.services.prediction_monitor.add_event") as mock_add:
            monitor = PredictionPositionMonitor(
                deployment_id="test-strategy",
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
        # VIB-3217: min_price is the 95% safety margin snapped DOWN to the
        # market tick (0.01 default). 0.50 * 0.95 = 0.475, floored to 0.47.
        # Snapping down preserves the "ensure execution" intent of the
        # multiplier -- ceiling-rounding would tighten the margin.
        assert sell_intent.min_price == Decimal("0.47")

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
        # VIB-3217: trailing-stop applies the 95% safety margin and then
        # snaps DOWN to the default 0.01 tick. 0.675 * 0.95 = 0.64125 ->
        # floored to 0.64. A fine-tick market (0.001) is covered below.
        assert sell_intent.min_price == Decimal("0.64")
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
        # Pre-resolution exit uses an explicit floor (0.01 = CLOB tick floor)
        # so the adapter's mandatory-anchor check passes; the LIMIT+IOC path
        # still fills at any available price (PM Exp 14 / VIB-3131).
        assert sell_intent.min_price == Decimal("0.01")
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
        # Without exit conditions, falls back to the CLOB floor (0.01) so
        # the adapter's mandatory-anchor check passes.
        assert sell_intent.min_price == Decimal("0.01")
        assert sell_intent.order_type == "market"


# =============================================================================
# Test VIB-3217: Off-tick min_price snap
# =============================================================================


class TestOffTickMinPriceSnap:
    """Regression tests for VIB-3217.

    `generate_sell_intent` used to multiply a threshold by 0.95 and return it
    verbatim. On a 0.01-tick market that produces 0.475 (off-tick); on a
    0.001-tick market that produces 0.64125 (also off-tick). The adapter's
    preflight raises PolymarketInvalidTickSizeError on either, so the first
    stop-loss / trailing-stop trigger would fail to submit.
    """

    def test_stop_loss_snapped_to_default_0_01_tick(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """0.50 * 0.95 = 0.475 -> snapped DOWN to 0.47 on default 0.01 tick."""
        from almanak.framework.intents.vocabulary import PredictionSellIntent

        result = MonitoringResult(
            position=position_with_conditions,
            event=PredictionEvent.STOP_LOSS_TRIGGERED,
            triggered=True,
            details={"current_price": "0.48", "stop_loss_price": "0.50"},
            suggested_action="SELL",
        )
        # No snapshot -> default tick 0.01 applied.
        sell_intent = monitor.generate_sell_intent(result)
        assert isinstance(sell_intent, PredictionSellIntent)
        assert sell_intent.min_price == Decimal("0.47")
        # The whole point of the fix: min_price must be a clean 0.01 multiple.
        assert (sell_intent.min_price * Decimal("100")) % Decimal("1") == Decimal("0")

    def test_trailing_stop_snapped_to_fine_0_001_tick(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Trailing-stop path: 0.675 * 0.95 = 0.64125 -> 0.641 on a 0.001-tick market.

        Renamed from ``test_stop_loss_snapped_to_fine_0_001_tick`` per CodeRabbit
        review of PR #1610: the test exercises ``TRAILING_STOP_TRIGGERED``, not
        ``STOP_LOSS_TRIGGERED``. A dedicated stop-loss fine-tick test lives
        below.
        """
        from almanak.framework.intents.vocabulary import PredictionSellIntent

        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.60"),
            tick_size=Decimal("0.001"),
        )
        result = MonitoringResult(
            position=position_with_conditions,
            event=PredictionEvent.TRAILING_STOP_TRIGGERED,
            triggered=True,
            details={
                "current_price": "0.60",
                "highest_price": "0.75",
                "trailing_stop_price": "0.675",
            },
            suggested_action="SELL",
        )
        sell_intent = monitor.generate_sell_intent(result, snapshot=snapshot)
        assert isinstance(sell_intent, PredictionSellIntent)
        assert sell_intent.min_price == Decimal("0.641")
        # Snapped price must be a clean 0.001 multiple.
        remainder = (sell_intent.min_price * Decimal("1000")) % Decimal("1")
        assert remainder == Decimal("0")

    def test_stop_loss_snapped_to_fine_0_001_tick(
        self,
        monitor: PredictionPositionMonitor,
        position_with_conditions: MonitoredPosition,
    ) -> None:
        """Dedicated stop-loss test on a 0.001-tick market.

        ``stop_loss_price = 0.50`` from the fixture. 0.50 * 0.95 = 0.475,
        floored onto the 0.001 tick grid = 0.475 (already on-tick at 0.001
        resolution). Verifies the stop-loss path uses the snapshot's
        ``tick_size`` field, not the default 0.01.
        """
        from almanak.framework.intents.vocabulary import PredictionSellIntent

        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.48"),
            tick_size=Decimal("0.001"),
        )
        result = MonitoringResult(
            position=position_with_conditions,
            event=PredictionEvent.STOP_LOSS_TRIGGERED,
            triggered=True,
            details={"current_price": "0.48", "stop_loss_price": "0.50"},
            suggested_action="SELL",
        )
        sell_intent = monitor.generate_sell_intent(result, snapshot=snapshot)
        assert isinstance(sell_intent, PredictionSellIntent)
        # 0.50 * 0.95 = 0.475 -> on 0.001 tick -> 0.475
        assert sell_intent.min_price == Decimal("0.475")
        remainder = (sell_intent.min_price * Decimal("1000")) % Decimal("1")
        assert remainder == Decimal("0")

    def test_snap_never_drops_below_clob_floor(self) -> None:
        """Floor to 0.01 even when the computed value would be below it."""
        # Direct helper test: arbitrary tiny input should clamp to 0.01.
        snapped = PredictionPositionMonitor._snap_sell_min_price_to_tick(Decimal("0.002"), Decimal("0.01"))
        assert snapped == Decimal("0.01")

    def test_snap_is_idempotent_on_valid_tick(self) -> None:
        """A price already on the tick passes through unchanged."""
        snapped = PredictionPositionMonitor._snap_sell_min_price_to_tick(Decimal("0.47"), Decimal("0.01"))
        assert snapped == Decimal("0.47")

    def test_snap_clamps_to_on_tick_floor_for_non_divisor_tick(self) -> None:
        """Gemini PR #1610 concern: tick_size that doesn't divide 0.01 evenly.

        Polymarket tick sizes today are 0.01 / 0.001 / 0.0001, all divisors
        of the CLOB minimum (0.01). But if the exchange ever emits a 0.1
        tick, a naive ``max(snapped, 0.01)`` clamp would return 0.01, which
        is OFF the 0.1 tick grid and would be rejected by the adapter. The
        clamp must compute ``ceil(0.01 / 0.1) * 0.1 = 0.1`` instead.
        """
        snapped = PredictionPositionMonitor._snap_sell_min_price_to_tick(Decimal("0.005"), Decimal("0.1"))
        # 0.005 floors to 0 on a 0.1 tick; clamp must bring us up to the
        # smallest on-tick value at or above the CLOB min -> 0.1.
        assert snapped == Decimal("0.1")

    def test_non_positive_tick_size_falls_back_to_clob_minimum(self, caplog) -> None:
        """CodeRabbit PR #1610 round 2: a non-positive tick must NOT bypass snapping.

        Upstream market metadata parsers don't enforce positivity, so if a
        bad tick_size slipped through (0 or negative) and we returned the
        raw price, we'd recreate the exact off-tick failure this PR fixes.
        The helper must fall back to the CLOB minimum tick (0.01) and log
        a warning.
        """
        import logging

        with caplog.at_level(logging.WARNING):
            snapped = PredictionPositionMonitor._snap_sell_min_price_to_tick(Decimal("0.475"), Decimal("0"))

        # Raw price was off-tick for 0.01 (0.475); fallback snaps to 0.47.
        assert snapped == Decimal("0.47")
        assert any("non-positive tick_size" in r.message for r in caplog.records), (
            "expected a warning log identifying the bad tick_size"
        )

    def test_negative_tick_size_falls_back_to_clob_minimum(self) -> None:
        """Mirror of the zero-tick case with an explicitly negative input."""
        snapped = PredictionPositionMonitor._snap_sell_min_price_to_tick(Decimal("0.475"), Decimal("-0.01"))
        assert snapped == Decimal("0.47")


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
            deployment_id="test-strategy",
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
            deployment_id="test-strategy",
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
            deployment_id="test-strategy",
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
            deployment_id="test-strategy",
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


# =============================================================================
# Test exit_before_resolution_seconds (VIB-3771)
# =============================================================================


class TestExitBeforeResolutionSeconds:
    """Tests for sub-hour pre-close timeouts (VIB-3771).

    The legacy ``exit_before_resolution_hours`` field has a minimum
    representable value of 1 hour, which fires immediately on entry for
    short-horizon markets (e.g. ``btc-updown-5m``). VIB-3771 adds a
    seconds-granularity field while preserving full backward compatibility.
    """

    def _make_position(
        self,
        exit_conditions: PredictionExitConditions | None,
    ) -> MonitoredPosition:
        return MonitoredPosition(
            market_id="test-market",
            condition_id="0x1234",
            token_id="12345",
            outcome="YES",
            size=Decimal("100"),
            entry_price=Decimal("0.65"),
            entry_time=datetime.now(UTC),
            exit_conditions=exit_conditions,
        )

    def test_effective_seconds_neither_set(self) -> None:
        """Neither field set -> None."""
        ec = PredictionExitConditions()
        assert ec.effective_exit_before_resolution_seconds() is None

    def test_effective_seconds_hours_only(self) -> None:
        """hours=2 -> 7200 seconds."""
        ec = PredictionExitConditions(exit_before_resolution_hours=2)
        assert ec.effective_exit_before_resolution_seconds() == 7200

    def test_effective_seconds_seconds_only(self) -> None:
        """seconds=60 -> 60."""
        ec = PredictionExitConditions(exit_before_resolution_seconds=60)
        assert ec.effective_exit_before_resolution_seconds() == 60

    def test_effective_seconds_both_set_seconds_wins(self) -> None:
        """Both set: seconds takes precedence over hours."""
        ec = PredictionExitConditions(
            exit_before_resolution_hours=24,
            exit_before_resolution_seconds=60,
        )
        # Seconds wins -- 60 seconds, not 24 * 3600.
        assert ec.effective_exit_before_resolution_seconds() == 60

    def test_zero_seconds_is_valid(self) -> None:
        """Zero is a degenerate but well-defined value (exit at resolution)."""
        ec = PredictionExitConditions(exit_before_resolution_seconds=0)
        assert ec.effective_exit_before_resolution_seconds() == 0

    def test_zero_hours_is_valid(self) -> None:
        """Zero hours is also valid."""
        ec = PredictionExitConditions(exit_before_resolution_hours=0)
        assert ec.effective_exit_before_resolution_seconds() == 0

    def test_negative_seconds_rejected(self) -> None:
        """Negative seconds is a config typo -- reject at construction."""
        with pytest.raises(ValueError, match="exit_before_resolution_seconds"):
            PredictionExitConditions(exit_before_resolution_seconds=-1)

    def test_negative_hours_rejected(self) -> None:
        """Negative hours is a config typo -- reject at construction."""
        with pytest.raises(ValueError, match="exit_before_resolution_hours"):
            PredictionExitConditions(exit_before_resolution_hours=-5)

    def test_to_dict_includes_seconds(self) -> None:
        """Serialization must include the new field."""
        ec = PredictionExitConditions(exit_before_resolution_seconds=300)
        data = ec.to_dict()
        assert data["exit_before_resolution_seconds"] == 300
        assert data["exit_before_resolution_hours"] is None

    def test_to_dict_seconds_none_when_unset(self) -> None:
        """Field absent -> None in dict (not missing key)."""
        ec = PredictionExitConditions(exit_before_resolution_hours=24)
        data = ec.to_dict()
        assert data["exit_before_resolution_hours"] == 24
        assert data["exit_before_resolution_seconds"] is None

    def test_seconds_triggers_for_sub_hour_market(self) -> None:
        """The motivating case: a 5-minute market with a 60s pre-close exit.

        Without seconds-granularity, ``exit_before_resolution_hours=1``
        fires immediately on entry (5 minutes < 1 hour) and defeats the
        purpose of the field. Seconds=60 triggers correctly only when
        market is within 60s of close.
        """
        monitor = PredictionPositionMonitor(deployment_id="t", emit_events=False)
        position = self._make_position(
            PredictionExitConditions(exit_before_resolution_seconds=60)
        )

        # 5 minutes from close -- should NOT trigger (5min > 60s).
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            market_end_date=datetime.now(UTC) + timedelta(minutes=5),
        )
        result = monitor.check_position(position, snapshot)
        assert result.event != PredictionEvent.RESOLUTION_APPROACHING

        # 30 seconds from close -- SHOULD trigger.
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            market_end_date=datetime.now(UTC) + timedelta(seconds=30),
        )
        result = monitor.check_position(position, snapshot)
        assert result.triggered is True
        assert result.event == PredictionEvent.RESOLUTION_APPROACHING
        assert result.details["exit_before_seconds"] == 60

    def test_legacy_hours_path_still_works(self) -> None:
        """Backward compat: pure-hours strategies behave unchanged."""
        monitor = PredictionPositionMonitor(deployment_id="t", emit_events=False)
        position = self._make_position(
            PredictionExitConditions(exit_before_resolution_hours=24)
        )

        # 12 hours away -> trigger.
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            market_end_date=datetime.now(UTC) + timedelta(hours=12),
        )
        result = monitor.check_position(position, snapshot)
        assert result.triggered is True
        assert result.event == PredictionEvent.RESOLUTION_APPROACHING
        # Legacy detail key preserved (now derived from seconds).
        # Backward-compat: whole-hour thresholds still surface as `int`,
        # not `float`, so consumers comparing against integer hour budgets
        # behave identically to the pre-VIB-3771 implementation.
        assert result.details["exit_before_hours"] == 24
        assert isinstance(result.details["exit_before_hours"], int)
        assert result.details["exit_before_seconds"] == 24 * 3600

    def test_sub_hour_exit_before_hours_is_float(self) -> None:
        """Sub-hour thresholds surface as float on the legacy hours key."""
        monitor = PredictionPositionMonitor(deployment_id="t", emit_events=False)
        position = self._make_position(
            PredictionExitConditions(exit_before_resolution_seconds=300)
        )
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            market_end_date=datetime.now(UTC) + timedelta(seconds=60),
        )
        result = monitor.check_position(position, snapshot)
        assert result.triggered is True
        assert result.details["exit_before_seconds"] == 300
        # Non-whole-hour threshold: emit as float so the value is lossless.
        assert isinstance(result.details["exit_before_hours"], float)
        assert result.details["exit_before_hours"] == pytest.approx(300 / 3600)

    def test_position_seconds_overrides_strategy_default_hours(self) -> None:
        """Position seconds beats strategy hours default."""
        monitor = PredictionPositionMonitor(
            deployment_id="t",
            emit_events=False,
            default_exit_before_resolution_hours=24,
        )
        position = self._make_position(
            PredictionExitConditions(exit_before_resolution_seconds=60)
        )

        # 1 hour from close -- with strategy default of 24h this would
        # trigger; with the position's 60s override it must NOT.
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            market_end_date=datetime.now(UTC) + timedelta(hours=1),
        )
        result = monitor.check_position(position, snapshot)
        assert result.event != PredictionEvent.RESOLUTION_APPROACHING

    def test_strategy_default_seconds(self) -> None:
        """Strategy-level seconds default applies when position has none."""
        monitor = PredictionPositionMonitor(
            deployment_id="t",
            emit_events=False,
            default_exit_before_resolution_seconds=120,
        )
        position = self._make_position(exit_conditions=None)

        # 60s away -- within 120s default, should trigger.
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            market_end_date=datetime.now(UTC) + timedelta(seconds=60),
        )
        result = monitor.check_position(position, snapshot)
        assert result.triggered is True
        assert result.event == PredictionEvent.RESOLUTION_APPROACHING
        assert result.details["exit_before_seconds"] == 120

    def test_strategy_default_seconds_beats_strategy_default_hours(self) -> None:
        """When both strategy-level defaults are set, seconds wins."""
        monitor = PredictionPositionMonitor(
            deployment_id="t",
            emit_events=False,
            default_exit_before_resolution_hours=24,
            default_exit_before_resolution_seconds=60,
        )
        position = self._make_position(exit_conditions=None)

        # 1 hour from close -- 24h default would trigger; 60s default
        # should NOT.
        snapshot = PositionSnapshot(
            market_id="test-market",
            current_price=Decimal("0.70"),
            market_end_date=datetime.now(UTC) + timedelta(hours=1),
        )
        result = monitor.check_position(position, snapshot)
        assert result.event != PredictionEvent.RESOLUTION_APPROACHING

    def test_negative_strategy_defaults_rejected(self) -> None:
        """Negative strategy-level defaults are rejected at construction."""
        with pytest.raises(ValueError, match="default_exit_before_resolution_hours"):
            PredictionPositionMonitor(default_exit_before_resolution_hours=-1)
        with pytest.raises(
            ValueError, match="default_exit_before_resolution_seconds"
        ):
            PredictionPositionMonitor(default_exit_before_resolution_seconds=-1)

    def test_intent_serialization_round_trip_with_seconds(self) -> None:
        """The intent (de)serializer must preserve the new field."""
        from almanak.framework.intents.prediction_intents import PredictionBuyIntent

        intent = PredictionBuyIntent(
            market_id="test-market",
            outcome="YES",
            amount_usd=Decimal("10"),
            exit_conditions=PredictionExitConditions(
                exit_before_resolution_seconds=60,
                exit_before_resolution_hours=24,  # Both set; seconds wins.
            ),
            protocol="polymarket",
        )
        data = intent.serialize()
        restored = PredictionBuyIntent.deserialize(data)
        assert restored.exit_conditions is not None
        assert restored.exit_conditions.exit_before_resolution_seconds == 60
        assert restored.exit_conditions.exit_before_resolution_hours == 24
        assert (
            restored.exit_conditions.effective_exit_before_resolution_seconds()
            == 60
        )
