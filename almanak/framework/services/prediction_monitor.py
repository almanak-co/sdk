"""Prediction Position Monitor Service.

This module provides automatic monitoring of prediction market positions,
detecting lifecycle events such as market resolution, price threshold breaches
(stop-loss/take-profit), and approaching resolution deadlines.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import PredictionSellIntent

from ..api.timeline import TimelineEvent, TimelineEventType, add_event

logger = logging.getLogger(__name__)


class PredictionEvent(StrEnum):
    """Events that can occur for prediction market positions."""

    MARKET_RESOLVED = "MARKET_RESOLVED"
    """Market has been resolved and winning positions can be redeemed."""

    STOP_LOSS_TRIGGERED = "STOP_LOSS_TRIGGERED"
    """Price dropped below stop-loss threshold."""

    TAKE_PROFIT_TRIGGERED = "TAKE_PROFIT_TRIGGERED"
    """Price rose above take-profit threshold."""

    RESOLUTION_APPROACHING = "RESOLUTION_APPROACHING"
    """Market is approaching its resolution deadline."""

    POSITION_EXPIRED = "POSITION_EXPIRED"
    """Position has passed its configured expiration time."""

    TRAILING_STOP_TRIGGERED = "TRAILING_STOP_TRIGGERED"
    """Price dropped below trailing stop threshold."""

    SPREAD_TOO_WIDE = "SPREAD_TOO_WIDE"
    """Bid-ask spread is too wide for safe exit."""

    LOW_LIQUIDITY = "LOW_LIQUIDITY"
    """Orderbook depth insufficient for position exit."""


@dataclass
class PredictionExitConditions:
    """Exit conditions for prediction positions.

    These conditions define when a position should be automatically exited
    to manage risk and lock in profits.
    """

    stop_loss_price: Decimal | None = None
    """Exit if price drops below this threshold."""

    take_profit_price: Decimal | None = None
    """Exit if price rises above this threshold."""

    exit_before_resolution_hours: int | None = None
    """Exit N hours before market resolution to avoid binary risk."""

    trailing_stop_pct: Decimal | None = None
    """Trailing stop percentage (e.g., 0.10 for 10% trailing stop)."""

    max_spread_pct: Decimal | None = None
    """Maximum acceptable bid-ask spread for exit (e.g., 0.05 for 5%)."""

    min_liquidity_usd: Decimal | None = None
    """Minimum orderbook depth required for exit."""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "stop_loss_price": str(self.stop_loss_price) if self.stop_loss_price else None,
            "take_profit_price": str(self.take_profit_price) if self.take_profit_price else None,
            "exit_before_resolution_hours": self.exit_before_resolution_hours,
            "trailing_stop_pct": str(self.trailing_stop_pct) if self.trailing_stop_pct else None,
            "max_spread_pct": str(self.max_spread_pct) if self.max_spread_pct else None,
            "min_liquidity_usd": str(self.min_liquidity_usd) if self.min_liquidity_usd else None,
        }


@dataclass
class MonitoredPosition:
    """A prediction position being monitored.

    Contains all information needed to evaluate exit conditions
    and track position state over time.
    """

    market_id: str
    """Market ID or slug."""

    condition_id: str
    """CTF condition ID."""

    token_id: str
    """CLOB token ID for the position."""

    outcome: str
    """Position outcome: YES or NO."""

    size: Decimal
    """Number of shares held."""

    entry_price: Decimal
    """Average entry price."""

    entry_time: datetime
    """When the position was opened."""

    exit_conditions: PredictionExitConditions | None = None
    """Optional exit conditions for automatic monitoring."""

    # Tracking fields
    highest_price: Decimal | None = None
    """Highest price seen since entry (for trailing stop)."""

    last_checked: datetime | None = None
    """Last time this position was checked."""

    market_end_date: datetime | None = None
    """Market resolution deadline."""

    is_resolved: bool = False
    """Whether the market has been resolved."""

    current_price: Decimal | None = None
    """Latest observed price."""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "market_id": self.market_id,
            "condition_id": self.condition_id,
            "token_id": self.token_id,
            "outcome": self.outcome,
            "size": str(self.size),
            "entry_price": str(self.entry_price),
            "entry_time": self.entry_time.isoformat(),
            "exit_conditions": self.exit_conditions.to_dict() if self.exit_conditions else None,
            "highest_price": str(self.highest_price) if self.highest_price else None,
            "last_checked": self.last_checked.isoformat() if self.last_checked else None,
            "market_end_date": self.market_end_date.isoformat() if self.market_end_date else None,
            "is_resolved": self.is_resolved,
            "current_price": str(self.current_price) if self.current_price else None,
        }


@dataclass
class PositionSnapshot:
    """Snapshot of market data for position monitoring.

    Contains all the information needed to evaluate a position's
    exit conditions and health status.
    """

    market_id: str
    """Market ID."""

    current_price: Decimal
    """Current market price for the position's outcome."""

    bid_price: Decimal | None = None
    """Best bid price (for spread calculation)."""

    ask_price: Decimal | None = None
    """Best ask price (for spread calculation)."""

    liquidity_usd: Decimal | None = None
    """Available liquidity in USD."""

    orderbook_depth_shares: Decimal | None = None
    """Available shares at bid price for selling."""

    is_resolved: bool = False
    """Whether the market has been resolved."""

    winning_outcome: str | None = None
    """If resolved, which outcome won (YES/NO)."""

    market_end_date: datetime | None = None
    """Market resolution deadline."""

    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    """When this snapshot was taken."""


@dataclass
class MonitoringResult:
    """Result of monitoring a single position.

    Contains the detected event (if any) and relevant context
    for taking action on the position.
    """

    position: MonitoredPosition
    """The monitored position."""

    event: PredictionEvent | None = None
    """Event detected, if any."""

    triggered: bool = False
    """Whether any exit condition was triggered."""

    details: dict[str, Any] = field(default_factory=dict)
    """Additional context about the event."""

    suggested_action: str | None = None
    """Suggested action to take (e.g., 'SELL', 'REDEEM')."""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "position": self.position.to_dict(),
            "event": self.event.value if self.event else None,
            "triggered": self.triggered,
            "details": self.details,
            "suggested_action": self.suggested_action,
        }


# Type alias for event callbacks
EventCallback = Callable[[MonitoredPosition, PredictionEvent, dict[str, Any]], None]


class PredictionPositionMonitor:
    """Monitors prediction market positions for lifecycle events.

    The monitor tracks positions and evaluates exit conditions:
    - Market resolution detection
    - Price threshold alerts (stop-loss, take-profit)
    - Time-based alerts (approaching resolution)
    - Trailing stop calculations
    - Liquidity and spread warnings
    - Partial exits when liquidity is insufficient

    Example:
        monitor = PredictionPositionMonitor(
            strategy_id="my-strategy",
            check_interval=60,
            default_exit_before_resolution_hours=24,  # Strategy-level default
        )

        # Add a position to monitor
        monitor.add_position(
            position=MonitoredPosition(
                market_id="will-btc-reach-100k",
                condition_id="0x...",
                token_id="12345...",
                outcome="YES",
                size=Decimal("100"),
                entry_price=Decimal("0.65"),
                entry_time=datetime.now(UTC),
                exit_conditions=PredictionExitConditions(
                    stop_loss_price=Decimal("0.50"),
                    take_profit_price=Decimal("0.85"),
                    exit_before_resolution_hours=24,
                ),
            ),
        )

        # Check positions (call periodically)
        results = monitor.check_positions(snapshots)
        for result in results:
            if result.triggered:
                print(f"Event: {result.event}, Action: {result.suggested_action}")
    """

    # Default check interval in seconds
    DEFAULT_CHECK_INTERVAL = 60

    # Default hours before resolution to warn
    DEFAULT_RESOLUTION_WARNING_HOURS = 24

    # Default spread threshold for warnings
    DEFAULT_MAX_SPREAD_PCT = Decimal("0.05")

    # Default minimum liquidity threshold
    DEFAULT_MIN_LIQUIDITY_USD = Decimal("1000")

    # Minimum partial exit percentage (don't exit less than 10% of position)
    MIN_PARTIAL_EXIT_PCT = Decimal("0.10")

    def __init__(
        self,
        strategy_id: str = "",
        check_interval: int = DEFAULT_CHECK_INTERVAL,
        emit_events: bool = True,
        event_callback: EventCallback | None = None,
        default_exit_before_resolution_hours: int | None = None,
        allow_partial_exits: bool = True,
    ) -> None:
        """Initialize the position monitor.

        Args:
            strategy_id: Strategy identifier for event emission.
            check_interval: Seconds between position checks.
            emit_events: Whether to emit timeline events.
            event_callback: Optional callback for events.
            default_exit_before_resolution_hours: Strategy-level default for
                exit_before_resolution_hours. Applied to positions that don't
                have their own exit conditions or don't specify this value.
            allow_partial_exits: If True, generate partial sell intents when
                orderbook liquidity is insufficient for the full position.
                Defaults to True.
        """
        self.strategy_id = strategy_id
        self.check_interval = check_interval
        self.emit_events = emit_events
        self.event_callback = event_callback
        self.default_exit_before_resolution_hours = default_exit_before_resolution_hours
        self.allow_partial_exits = allow_partial_exits

        # Monitored positions keyed by market_id
        self._positions: dict[str, MonitoredPosition] = {}

        # Track last check time
        self._last_check: datetime | None = None

    @property
    def positions(self) -> dict[str, MonitoredPosition]:
        """Get all monitored positions."""
        return self._positions.copy()

    def add_position(
        self,
        position: MonitoredPosition,
    ) -> None:
        """Add a position to monitor.

        Args:
            position: The position to monitor.
        """
        # Initialize highest_price for trailing stop tracking
        if position.highest_price is None:
            position.highest_price = position.entry_price

        self._positions[position.market_id] = position
        logger.info(
            "Added position to monitor: market_id=%s, outcome=%s, size=%s",
            position.market_id,
            position.outcome,
            position.size,
        )

    def remove_position(self, market_id: str) -> MonitoredPosition | None:
        """Remove a position from monitoring.

        Args:
            market_id: Market ID to remove.

        Returns:
            The removed position, or None if not found.
        """
        position = self._positions.pop(market_id, None)
        if position:
            logger.info("Removed position from monitor: market_id=%s", market_id)
        return position

    def get_position(self, market_id: str) -> MonitoredPosition | None:
        """Get a monitored position by market ID.

        Args:
            market_id: Market ID to look up.

        Returns:
            The monitored position, or None if not found.
        """
        return self._positions.get(market_id)

    def update_position_price(self, market_id: str, current_price: Decimal) -> None:
        """Update the current price for a position.

        This also updates the highest_price for trailing stop tracking.

        Args:
            market_id: Market ID to update.
            current_price: New current price.
        """
        position = self._positions.get(market_id)
        if position:
            position.current_price = current_price
            if position.highest_price is None or current_price > position.highest_price:
                position.highest_price = current_price

    def check_position(
        self,
        position: MonitoredPosition,
        snapshot: PositionSnapshot,
    ) -> MonitoringResult:
        """Check a single position against its exit conditions.

        Evaluates all exit conditions and returns the most urgent
        triggered event, if any.

        Args:
            position: The position to check.
            snapshot: Current market data snapshot.

        Returns:
            MonitoringResult with any triggered events.
        """
        now = datetime.now(UTC)

        # Update position state
        position.current_price = snapshot.current_price
        position.last_checked = now
        position.is_resolved = snapshot.is_resolved
        if snapshot.market_end_date:
            position.market_end_date = snapshot.market_end_date

        # Update highest price for trailing stop
        if position.highest_price is None or snapshot.current_price > position.highest_price:
            position.highest_price = snapshot.current_price

        # Check conditions in priority order
        result = self._check_market_resolution(position, snapshot)
        if result.triggered:
            return result

        result = self._check_stop_loss(position, snapshot)
        if result.triggered:
            return result

        result = self._check_take_profit(position, snapshot)
        if result.triggered:
            return result

        result = self._check_trailing_stop(position, snapshot)
        if result.triggered:
            return result

        result = self._check_resolution_approaching(position, snapshot)
        if result.triggered:
            return result

        result = self._check_liquidity(position, snapshot)
        if result.triggered:
            return result

        result = self._check_spread(position, snapshot)
        if result.triggered:
            return result

        # No events triggered
        return MonitoringResult(position=position)

    def check_positions(
        self,
        snapshots: dict[str, PositionSnapshot],
    ) -> list[MonitoringResult]:
        """Check all monitored positions against provided snapshots.

        Args:
            snapshots: Market data snapshots keyed by market_id.

        Returns:
            List of monitoring results for all positions.
        """
        results: list[MonitoringResult] = []
        self._last_check = datetime.now(UTC)

        for market_id, position in self._positions.items():
            snapshot = snapshots.get(market_id)
            if snapshot is None:
                logger.warning("No snapshot for monitored position: %s", market_id)
                continue

            result = self.check_position(position, snapshot)
            results.append(result)

            # Emit event if triggered
            if result.triggered and result.event:
                self._handle_event(position, result)

        return results

    def _check_market_resolution(
        self,
        position: MonitoredPosition,
        snapshot: PositionSnapshot,
    ) -> MonitoringResult:
        """Check if the market has been resolved."""
        if not snapshot.is_resolved:
            return MonitoringResult(position=position)

        is_winner = snapshot.winning_outcome == position.outcome
        return MonitoringResult(
            position=position,
            event=PredictionEvent.MARKET_RESOLVED,
            triggered=True,
            details={
                "winning_outcome": snapshot.winning_outcome,
                "position_outcome": position.outcome,
                "is_winner": is_winner,
                "size": str(position.size),
                "potential_payout": str(position.size) if is_winner else "0",
            },
            suggested_action="REDEEM" if is_winner else None,
        )

    def _check_stop_loss(
        self,
        position: MonitoredPosition,
        snapshot: PositionSnapshot,
    ) -> MonitoringResult:
        """Check if stop-loss price has been breached."""
        if position.exit_conditions is None:
            return MonitoringResult(position=position)

        stop_loss = position.exit_conditions.stop_loss_price
        if stop_loss is None:
            return MonitoringResult(position=position)

        if snapshot.current_price < stop_loss:
            return MonitoringResult(
                position=position,
                event=PredictionEvent.STOP_LOSS_TRIGGERED,
                triggered=True,
                details={
                    "current_price": str(snapshot.current_price),
                    "stop_loss_price": str(stop_loss),
                    "entry_price": str(position.entry_price),
                    "loss_pct": str((position.entry_price - snapshot.current_price) / position.entry_price * 100),
                },
                suggested_action="SELL",
            )

        return MonitoringResult(position=position)

    def _check_take_profit(
        self,
        position: MonitoredPosition,
        snapshot: PositionSnapshot,
    ) -> MonitoringResult:
        """Check if take-profit price has been reached."""
        if position.exit_conditions is None:
            return MonitoringResult(position=position)

        take_profit = position.exit_conditions.take_profit_price
        if take_profit is None:
            return MonitoringResult(position=position)

        if snapshot.current_price >= take_profit:
            return MonitoringResult(
                position=position,
                event=PredictionEvent.TAKE_PROFIT_TRIGGERED,
                triggered=True,
                details={
                    "current_price": str(snapshot.current_price),
                    "take_profit_price": str(take_profit),
                    "entry_price": str(position.entry_price),
                    "profit_pct": str((snapshot.current_price - position.entry_price) / position.entry_price * 100),
                },
                suggested_action="SELL",
            )

        return MonitoringResult(position=position)

    def _check_trailing_stop(
        self,
        position: MonitoredPosition,
        snapshot: PositionSnapshot,
    ) -> MonitoringResult:
        """Check if trailing stop has been triggered."""
        if position.exit_conditions is None:
            return MonitoringResult(position=position)

        trailing_stop_pct = position.exit_conditions.trailing_stop_pct
        if trailing_stop_pct is None:
            return MonitoringResult(position=position)

        if position.highest_price is None:
            return MonitoringResult(position=position)

        # Calculate trailing stop price
        trailing_stop_price = position.highest_price * (Decimal("1") - trailing_stop_pct)

        if snapshot.current_price < trailing_stop_price:
            return MonitoringResult(
                position=position,
                event=PredictionEvent.TRAILING_STOP_TRIGGERED,
                triggered=True,
                details={
                    "current_price": str(snapshot.current_price),
                    "highest_price": str(position.highest_price),
                    "trailing_stop_pct": str(trailing_stop_pct),
                    "trailing_stop_price": str(trailing_stop_price),
                },
                suggested_action="SELL",
            )

        return MonitoringResult(position=position)

    def _check_resolution_approaching(
        self,
        position: MonitoredPosition,
        snapshot: PositionSnapshot,
    ) -> MonitoringResult:
        """Check if market resolution is approaching.

        Uses position-level exit_before_resolution_hours if set,
        otherwise falls back to the strategy-level default.
        """
        # Get exit hours from position conditions or strategy default
        exit_hours = None
        if position.exit_conditions is not None:
            exit_hours = position.exit_conditions.exit_before_resolution_hours

        # Fall back to strategy-level default
        if exit_hours is None:
            exit_hours = self.default_exit_before_resolution_hours

        if exit_hours is None:
            return MonitoringResult(position=position)

        market_end = snapshot.market_end_date or position.market_end_date
        if market_end is None:
            return MonitoringResult(position=position)

        now = datetime.now(UTC)
        time_until_end = market_end - now
        hours_until_end = time_until_end.total_seconds() / 3600

        if hours_until_end <= exit_hours:
            return MonitoringResult(
                position=position,
                event=PredictionEvent.RESOLUTION_APPROACHING,
                triggered=True,
                details={
                    "market_end_date": market_end.isoformat(),
                    "hours_until_end": hours_until_end,
                    "exit_before_hours": exit_hours,
                    "current_price": str(snapshot.current_price),
                },
                suggested_action="SELL",
            )

        return MonitoringResult(position=position)

    def _check_liquidity(
        self,
        position: MonitoredPosition,
        snapshot: PositionSnapshot,
    ) -> MonitoringResult:
        """Check if liquidity is sufficient for exit."""
        if position.exit_conditions is None:
            return MonitoringResult(position=position)

        min_liquidity = position.exit_conditions.min_liquidity_usd
        if min_liquidity is None:
            min_liquidity = self.DEFAULT_MIN_LIQUIDITY_USD

        if snapshot.liquidity_usd is None:
            return MonitoringResult(position=position)

        if snapshot.liquidity_usd < min_liquidity:
            return MonitoringResult(
                position=position,
                event=PredictionEvent.LOW_LIQUIDITY,
                triggered=True,
                details={
                    "current_liquidity_usd": str(snapshot.liquidity_usd),
                    "min_liquidity_usd": str(min_liquidity),
                    "position_size": str(position.size),
                },
                suggested_action=None,  # Warning only, no action suggested
            )

        return MonitoringResult(position=position)

    def _check_spread(
        self,
        position: MonitoredPosition,
        snapshot: PositionSnapshot,
    ) -> MonitoringResult:
        """Check if bid-ask spread is too wide."""
        if position.exit_conditions is None:
            return MonitoringResult(position=position)

        max_spread = position.exit_conditions.max_spread_pct
        if max_spread is None:
            max_spread = self.DEFAULT_MAX_SPREAD_PCT

        if snapshot.bid_price is None or snapshot.ask_price is None:
            return MonitoringResult(position=position)

        if snapshot.ask_price == Decimal("0"):
            return MonitoringResult(position=position)

        spread = snapshot.ask_price - snapshot.bid_price
        spread_pct = spread / snapshot.ask_price

        if spread_pct > max_spread:
            return MonitoringResult(
                position=position,
                event=PredictionEvent.SPREAD_TOO_WIDE,
                triggered=True,
                details={
                    "bid_price": str(snapshot.bid_price),
                    "ask_price": str(snapshot.ask_price),
                    "spread": str(spread),
                    "spread_pct": str(spread_pct),
                    "max_spread_pct": str(max_spread),
                },
                suggested_action=None,  # Warning only, no action suggested
            )

        return MonitoringResult(position=position)

    def _handle_event(
        self,
        position: MonitoredPosition,
        result: MonitoringResult,
    ) -> None:
        """Handle a triggered event.

        Emits timeline events and calls the event callback if configured.

        Args:
            position: The position that triggered the event.
            result: The monitoring result with event details.
        """
        if result.event is None:
            return

        logger.info(
            "Prediction event triggered: event=%s, market_id=%s, action=%s",
            result.event.value,
            position.market_id,
            result.suggested_action,
        )

        # Emit timeline event
        if self.emit_events:
            self._emit_timeline_event(position, result)

        # Call event callback
        if self.event_callback:
            try:
                self.event_callback(position, result.event, result.details)
            except Exception:
                logger.exception("Event callback failed for %s", result.event)

    def _emit_timeline_event(
        self,
        position: MonitoredPosition,
        result: MonitoringResult,
    ) -> None:
        """Emit a timeline event for a triggered condition.

        Args:
            position: The position that triggered.
            result: The monitoring result.
        """
        if result.event is None:
            return

        # Map prediction events to timeline event types
        event_type_map: dict[PredictionEvent, TimelineEventType] = {
            PredictionEvent.MARKET_RESOLVED: TimelineEventType.POSITION_CLOSED,
            PredictionEvent.STOP_LOSS_TRIGGERED: TimelineEventType.RISK_GUARD_TRIGGERED,
            PredictionEvent.TAKE_PROFIT_TRIGGERED: TimelineEventType.POSITION_MODIFIED,
            PredictionEvent.TRAILING_STOP_TRIGGERED: TimelineEventType.RISK_GUARD_TRIGGERED,
            PredictionEvent.RESOLUTION_APPROACHING: TimelineEventType.ALERT_SENT,
            PredictionEvent.LOW_LIQUIDITY: TimelineEventType.ALERT_SENT,
            PredictionEvent.SPREAD_TOO_WIDE: TimelineEventType.ALERT_SENT,
        }

        timeline_type = event_type_map.get(result.event, TimelineEventType.CUSTOM)

        description = f"Prediction position {result.event.value}: {position.market_id}"
        if result.suggested_action:
            description += f" (suggested: {result.suggested_action})"

        event = TimelineEvent(
            timestamp=datetime.now(UTC),
            event_type=timeline_type,
            description=description,
            strategy_id=self.strategy_id,
            chain="polygon",  # Polymarket is on Polygon
            details={
                "prediction_event": result.event.value,
                "market_id": position.market_id,
                "outcome": position.outcome,
                "size": str(position.size),
                "entry_price": str(position.entry_price),
                "current_price": str(position.current_price) if position.current_price else None,
                "suggested_action": result.suggested_action,
                **result.details,
            },
        )

        add_event(event)

    def clear(self) -> None:
        """Clear all monitored positions."""
        self._positions.clear()
        logger.info("Cleared all monitored positions")

    def calculate_safe_exit_size(
        self,
        position: MonitoredPosition,
        snapshot: PositionSnapshot,
    ) -> tuple[Decimal | None, bool]:
        """Calculate the safe exit size based on available liquidity.

        When orderbook depth is insufficient for the full position, this method
        calculates a partial exit size that respects the available liquidity.

        Args:
            position: The position to potentially exit.
            snapshot: Current market data including orderbook depth.

        Returns:
            A tuple of (safe_size, is_insufficient):
            - (None, False): No orderbook data available, proceed with "all"
            - (None, True): Liquidity is too low for even a partial exit
            - (Decimal, False): Full position can be exited (safe_size >= position.size)
            - (Decimal, True): Partial exit size (liquidity limited)

            The first element is the recommended exit size, or None if "all".
            The second element indicates whether liquidity is insufficient.
        """
        if snapshot.orderbook_depth_shares is None:
            return None, False

        available_shares = snapshot.orderbook_depth_shares

        # If full position can be exited, return None (use "all")
        if available_shares >= position.size:
            return None, False

        # Calculate minimum exit size (at least MIN_PARTIAL_EXIT_PCT of position)
        min_exit_size = position.size * self.MIN_PARTIAL_EXIT_PCT

        # If available liquidity is less than minimum, indicate insufficient
        if available_shares < min_exit_size:
            return None, True  # Insufficient liquidity

        # Use 95% of available depth to leave some buffer
        safe_size = available_shares * Decimal("0.95")

        # Round down to avoid overfilling
        safe_size = safe_size.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

        logger.info(
            "Calculated safe exit size: position_size=%s, available=%s, safe_size=%s",
            position.size,
            available_shares,
            safe_size,
        )

        return safe_size, True  # Partial exit

    def generate_sell_intent(
        self,
        result: MonitoringResult,
        snapshot: PositionSnapshot | None = None,
    ) -> "PredictionSellIntent | None":
        """Generate a PredictionSellIntent for a triggered exit condition.

        When an exit condition is triggered (stop-loss, take-profit, trailing stop,
        or pre-resolution exit), this method generates the appropriate sell intent
        to exit the position.

        If partial exits are enabled and the snapshot indicates insufficient liquidity,
        this method will generate a partial sell intent instead of a full exit.

        Args:
            result: The monitoring result with a triggered exit condition.
            snapshot: Optional market snapshot for calculating partial exit size.
                When provided and allow_partial_exits is True, the method will
                check orderbook depth and generate partial exits if needed.

        Returns:
            A PredictionSellIntent to exit the position, or None if no sell is needed
            (e.g., for warning events like LOW_LIQUIDITY or SPREAD_TOO_WIDE, or if
            available liquidity is too low for even a partial exit).

        Example:
            results = monitor.check_positions(snapshots)
            for result in results:
                if result.triggered and result.suggested_action == "SELL":
                    snapshot = snapshots.get(result.position.market_id)
                    sell_intent = monitor.generate_sell_intent(result, snapshot)
                    if sell_intent:
                        # Execute the sell intent
                        compiler.compile(sell_intent)
        """
        from almanak.framework.intents.vocabulary import PredictionSellIntent

        if not result.triggered or result.event is None:
            return None

        # Only generate sell intents for events that suggest selling
        if result.suggested_action != "SELL":
            return None

        position = result.position

        # Determine min_price for the sell order based on the event type
        min_price = None

        if result.event == PredictionEvent.STOP_LOSS_TRIGGERED:
            # For stop-loss, use the stop-loss price as a floor
            if position.exit_conditions and position.exit_conditions.stop_loss_price:
                # Use a slightly lower price to ensure execution
                min_price = position.exit_conditions.stop_loss_price * Decimal("0.95")

        elif result.event == PredictionEvent.TAKE_PROFIT_TRIGGERED:
            # For take-profit, use the take-profit price as a floor
            if position.exit_conditions and position.exit_conditions.take_profit_price:
                min_price = position.exit_conditions.take_profit_price

        elif result.event == PredictionEvent.TRAILING_STOP_TRIGGERED:
            # For trailing stop, use the calculated stop price
            if "trailing_stop_price" in result.details:
                trailing_price = Decimal(result.details["trailing_stop_price"])
                # Use a slightly lower price to ensure execution
                min_price = trailing_price * Decimal("0.95")

        elif result.event == PredictionEvent.RESOLUTION_APPROACHING:
            # For pre-resolution exit, use market order (no min_price)
            min_price = None

        # Determine exit size (full or partial)
        shares: Decimal | Literal["all"] = "all"
        is_partial = False

        if self.allow_partial_exits and snapshot is not None:
            safe_size, is_constrained = self.calculate_safe_exit_size(position, snapshot)

            if is_constrained:
                if safe_size is None:
                    # Liquidity is too low for even a partial exit
                    logger.warning(
                        "Insufficient liquidity for exit: market_id=%s, position_size=%s, available=%s",
                        position.market_id,
                        position.size,
                        snapshot.orderbook_depth_shares,
                    )
                    return None

                # Partial exit needed
                shares = safe_size
                is_partial = True

                # If safe_size is zero or negative, don't generate intent
                if safe_size <= Decimal("0"):
                    logger.warning(
                        "Calculated safe exit size is zero: market_id=%s",
                        position.market_id,
                    )
                    return None

        # Create the sell intent
        sell_intent = PredictionSellIntent(
            market_id=position.market_id,
            outcome=position.outcome,  # type: ignore[arg-type]
            shares=shares,
            min_price=min_price,
            order_type="market" if min_price is None else "limit",
            time_in_force="IOC",  # Immediate or cancel for quick exit
            protocol="polymarket",
        )

        if is_partial:
            logger.info(
                "Generated PARTIAL sell intent for position: market_id=%s, outcome=%s, event=%s, shares=%s of %s",
                position.market_id,
                position.outcome,
                result.event.value,
                shares,
                position.size,
            )
        else:
            logger.info(
                "Generated sell intent for position: market_id=%s, outcome=%s, event=%s",
                position.market_id,
                position.outcome,
                result.event.value,
            )

        return sell_intent
