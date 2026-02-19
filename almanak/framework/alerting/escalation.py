"""Escalation Policy for alert management.

This module implements the EscalationPolicy class which handles:
- Multi-level escalation based on time since alert was raised
- Alert acknowledgment tracking
- Auto-remediation triggers at final escalation level
- Escalation status tracking per strategy/alert

Escalation levels:
- Level 1 (<5 min): Telegram/Slack
- Level 2 (<15 min): Add Email
- Level 3 (<30 min): PagerDuty for HIGH+ severity
- Level 4 (30+ min): Auto-remediation or emergency pause
"""

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import IntEnum, StrEnum
from typing import Any

from ..models.operator_card import OperatorCard, Severity
from .alert_config import AlertChannel, AlertConfig

logger = logging.getLogger(__name__)


class EscalationLevel(IntEnum):
    """Escalation levels from least to most severe."""

    LEVEL_1 = 1  # Initial: Telegram/Slack
    LEVEL_2 = 2  # Add Email
    LEVEL_3 = 3  # Add PagerDuty for HIGH+
    LEVEL_4 = 4  # Auto-remediation or emergency pause


class EscalationStatus(StrEnum):
    """Status of an escalation."""

    ACTIVE = "ACTIVE"  # Escalation is active and progressing
    ACKNOWLEDGED = "ACKNOWLEDGED"  # Alert was acknowledged, escalation stopped
    RESOLVED = "RESOLVED"  # Issue was resolved
    AUTO_REMEDIATED = "AUTO_REMEDIATED"  # Auto-remediation was triggered
    EMERGENCY_PAUSED = "EMERGENCY_PAUSED"  # Emergency pause was triggered


# Time thresholds for each escalation level (in seconds)
ESCALATION_THRESHOLDS: dict[EscalationLevel, int] = {
    EscalationLevel.LEVEL_1: 0,  # Immediate
    EscalationLevel.LEVEL_2: 300,  # 5 minutes
    EscalationLevel.LEVEL_3: 900,  # 15 minutes
    EscalationLevel.LEVEL_4: 1800,  # 30 minutes
}

# Channels to use at each escalation level
ESCALATION_CHANNELS: dict[EscalationLevel, list[AlertChannel]] = {
    EscalationLevel.LEVEL_1: [AlertChannel.TELEGRAM, AlertChannel.SLACK],
    EscalationLevel.LEVEL_2: [AlertChannel.TELEGRAM, AlertChannel.SLACK, AlertChannel.EMAIL],
    EscalationLevel.LEVEL_3: [
        AlertChannel.TELEGRAM,
        AlertChannel.SLACK,
        AlertChannel.EMAIL,
        AlertChannel.PAGERDUTY,
    ],
    EscalationLevel.LEVEL_4: [
        AlertChannel.TELEGRAM,
        AlertChannel.SLACK,
        AlertChannel.EMAIL,
        AlertChannel.PAGERDUTY,
    ],
}


@dataclass
class EscalationState:
    """Tracks the escalation state for a single alert.

    Attributes:
        strategy_id: The strategy this escalation is for
        alert_id: Unique identifier for this alert instance
        card: The OperatorCard that triggered escalation
        created_at: When the alert was first raised
        current_level: Current escalation level
        status: Current status of the escalation
        acknowledged_at: When the alert was acknowledged (if applicable)
        acknowledged_by: Who acknowledged the alert (if applicable)
        last_escalation_at: When the last escalation occurred
        channels_notified: Channels that have been notified at each level
    """

    strategy_id: str
    alert_id: str
    card: OperatorCard
    created_at: datetime
    current_level: EscalationLevel = EscalationLevel.LEVEL_1
    status: EscalationStatus = EscalationStatus.ACTIVE
    acknowledged_at: datetime | None = None
    acknowledged_by: str | None = None
    last_escalation_at: datetime | None = None
    channels_notified: dict[EscalationLevel, list[AlertChannel]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Initialize channels_notified if not provided."""
        if not self.channels_notified:
            self.channels_notified = {}

    def time_since_created(self, current_time: datetime | None = None) -> float:
        """Get seconds since the alert was created.

        Args:
            current_time: Current time (defaults to now)

        Returns:
            Seconds since creation
        """
        if current_time is None:
            current_time = datetime.now(UTC)
        return (current_time - self.created_at).total_seconds()

    def is_active(self) -> bool:
        """Check if this escalation is still active."""
        return self.status == EscalationStatus.ACTIVE

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "strategy_id": self.strategy_id,
            "alert_id": self.alert_id,
            "card": self.card.to_dict(),
            "created_at": self.created_at.isoformat(),
            "current_level": self.current_level.value,
            "status": self.status.value,
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
            "acknowledged_by": self.acknowledged_by,
            "last_escalation_at": (self.last_escalation_at.isoformat() if self.last_escalation_at else None),
            "channels_notified": {
                level.value: [ch.value for ch in channels] for level, channels in self.channels_notified.items()
            },
        }


@dataclass
class EscalationResult:
    """Result of an escalation check or action.

    Attributes:
        escalated: Whether escalation occurred
        new_level: The new escalation level (if escalated)
        channels_to_notify: Channels that need to be notified
        trigger_auto_remediation: Whether auto-remediation should be triggered
        trigger_emergency_pause: Whether emergency pause should be triggered
        message: Human-readable message about what happened
    """

    escalated: bool
    new_level: EscalationLevel | None = None
    channels_to_notify: list[AlertChannel] = field(default_factory=list)
    trigger_auto_remediation: bool = False
    trigger_emergency_pause: bool = False
    message: str = ""


# Type for auto-remediation callback
AutoRemediationCallback = Callable[[str, OperatorCard], bool]
# Type for emergency pause callback
EmergencyPauseCallback = Callable[[str, OperatorCard], bool]


class EscalationPolicy:
    """Manages escalation of unacknowledged alerts.

    The EscalationPolicy tracks alerts and escalates them through multiple
    levels if they are not acknowledged within time thresholds.

    Escalation levels:
    - Level 1 (<5 min): Telegram/Slack
    - Level 2 (<15 min): Add Email
    - Level 3 (<30 min): PagerDuty for HIGH+ severity
    - Level 4 (30+ min): Auto-remediation or emergency pause

    Attributes:
        config: The AlertConfig for channel configuration
        escalations: Dict of active escalation states by alert_id
        auto_remediation_callback: Optional callback for auto-remediation
        emergency_pause_callback: Optional callback for emergency pause
    """

    def __init__(
        self,
        config: AlertConfig,
        auto_remediation_callback: AutoRemediationCallback | None = None,
        emergency_pause_callback: EmergencyPauseCallback | None = None,
        custom_thresholds: dict[EscalationLevel, int] | None = None,
    ) -> None:
        """Initialize the EscalationPolicy.

        Args:
            config: AlertConfig with channel configurations
            auto_remediation_callback: Callback to execute auto-remediation
            emergency_pause_callback: Callback to execute emergency pause
            custom_thresholds: Optional custom time thresholds for escalation levels
        """
        self.config = config
        self.escalations: dict[str, EscalationState] = {}
        self.auto_remediation_callback = auto_remediation_callback
        self.emergency_pause_callback = emergency_pause_callback
        self.thresholds = custom_thresholds or ESCALATION_THRESHOLDS

    def _generate_alert_id(self, strategy_id: str, card: OperatorCard) -> str:
        """Generate a unique alert ID for tracking.

        Args:
            strategy_id: The strategy ID
            card: The OperatorCard

        Returns:
            Unique alert ID
        """
        # Use strategy + event type + reason for uniqueness
        # This allows multiple different alerts for the same strategy
        return f"{strategy_id}:{card.event_type.value}:{card.reason.value}"

    def start_escalation(
        self,
        strategy_id: str,
        card: OperatorCard,
        current_time: datetime | None = None,
    ) -> EscalationState:
        """Start tracking escalation for a new alert.

        If an escalation already exists for this alert, returns the existing one.

        Args:
            strategy_id: The strategy ID
            card: The OperatorCard that triggered the alert
            current_time: Current time (defaults to now)

        Returns:
            The EscalationState for this alert
        """
        if current_time is None:
            current_time = datetime.now(UTC)

        alert_id = self._generate_alert_id(strategy_id, card)

        # Check if escalation already exists
        if alert_id in self.escalations:
            existing = self.escalations[alert_id]
            if existing.is_active():
                logger.debug(f"Escalation already exists for {alert_id}")
                return existing

        # Create new escalation state
        state = EscalationState(
            strategy_id=strategy_id,
            alert_id=alert_id,
            card=card,
            created_at=current_time,
            current_level=EscalationLevel.LEVEL_1,
            status=EscalationStatus.ACTIVE,
            last_escalation_at=current_time,
        )

        self.escalations[alert_id] = state
        logger.info(f"Started escalation for {strategy_id}: alert_id={alert_id}, severity={card.severity.value}")

        return state

    def acknowledge(
        self,
        alert_id: str,
        acknowledged_by: str = "operator",
        current_time: datetime | None = None,
    ) -> bool:
        """Acknowledge an alert and stop its escalation.

        Args:
            alert_id: The alert ID to acknowledge
            acknowledged_by: Who is acknowledging (for audit)
            current_time: Current time (defaults to now)

        Returns:
            True if acknowledgment succeeded, False if alert not found
        """
        if current_time is None:
            current_time = datetime.now(UTC)

        if alert_id not in self.escalations:
            logger.warning(f"Cannot acknowledge unknown alert: {alert_id}")
            return False

        state = self.escalations[alert_id]

        if not state.is_active():
            logger.info(f"Alert {alert_id} is no longer active, status={state.status.value}")
            return True

        state.status = EscalationStatus.ACKNOWLEDGED
        state.acknowledged_at = current_time
        state.acknowledged_by = acknowledged_by

        logger.info(
            f"Alert acknowledged: alert_id={alert_id}, by={acknowledged_by}, level_reached={state.current_level.value}"
        )

        return True

    def acknowledge_by_strategy(
        self,
        strategy_id: str,
        acknowledged_by: str = "operator",
        current_time: datetime | None = None,
    ) -> int:
        """Acknowledge all active alerts for a strategy.

        Args:
            strategy_id: The strategy ID
            acknowledged_by: Who is acknowledging
            current_time: Current time (defaults to now)

        Returns:
            Number of alerts acknowledged
        """
        if current_time is None:
            current_time = datetime.now(UTC)

        count = 0
        for alert_id, state in self.escalations.items():
            if state.strategy_id == strategy_id and state.is_active():
                if self.acknowledge(alert_id, acknowledged_by, current_time):
                    count += 1

        logger.info(f"Acknowledged {count} alerts for strategy {strategy_id}")
        return count

    def resolve(
        self,
        alert_id: str,
        current_time: datetime | None = None,
    ) -> bool:
        """Mark an alert as resolved.

        Args:
            alert_id: The alert ID to resolve
            current_time: Current time (defaults to now)

        Returns:
            True if resolution succeeded, False if alert not found
        """
        if alert_id not in self.escalations:
            logger.warning(f"Cannot resolve unknown alert: {alert_id}")
            return False

        state = self.escalations[alert_id]
        state.status = EscalationStatus.RESOLVED

        logger.info(f"Alert resolved: alert_id={alert_id}")
        return True

    def _get_level_for_time(self, seconds_elapsed: float) -> EscalationLevel:
        """Determine the escalation level based on elapsed time.

        Args:
            seconds_elapsed: Seconds since alert was created

        Returns:
            The appropriate escalation level
        """
        # Check thresholds from highest to lowest
        for level in reversed(list(EscalationLevel)):
            if seconds_elapsed >= self.thresholds[level]:
                return level

        return EscalationLevel.LEVEL_1

    def _get_channels_for_level(
        self,
        level: EscalationLevel,
        severity: Severity,
    ) -> list[AlertChannel]:
        """Get the channels to notify at a given escalation level.

        Args:
            level: The escalation level
            severity: The alert severity

        Returns:
            List of channels to notify
        """
        base_channels = ESCALATION_CHANNELS.get(level, [])

        # Filter channels based on configuration
        available_channels: list[AlertChannel] = []
        for channel in base_channels:
            # PagerDuty only for HIGH or CRITICAL at level 3+
            if channel == AlertChannel.PAGERDUTY:
                if level >= EscalationLevel.LEVEL_3:
                    if severity in (Severity.HIGH, Severity.CRITICAL):
                        if self.config.has_channel(channel):
                            available_channels.append(channel)
            else:
                if self.config.has_channel(channel):
                    available_channels.append(channel)

        return available_channels

    def check_escalation(
        self,
        alert_id: str,
        current_time: datetime | None = None,
    ) -> EscalationResult:
        """Check if an alert needs to be escalated.

        This method checks the time elapsed since the alert was created
        and determines if it should be escalated to the next level.

        Args:
            alert_id: The alert ID to check
            current_time: Current time (defaults to now)

        Returns:
            EscalationResult indicating what action to take
        """
        if current_time is None:
            current_time = datetime.now(UTC)

        if alert_id not in self.escalations:
            return EscalationResult(
                escalated=False,
                message=f"Unknown alert: {alert_id}",
            )

        state = self.escalations[alert_id]

        if not state.is_active():
            return EscalationResult(
                escalated=False,
                message=f"Alert is not active: status={state.status.value}",
            )

        # Calculate time since creation
        elapsed = state.time_since_created(current_time)
        target_level = self._get_level_for_time(elapsed)

        # Check if we need to escalate
        if target_level <= state.current_level:
            return EscalationResult(
                escalated=False,
                message=f"No escalation needed, current level={state.current_level.value}",
            )

        # Escalation needed
        old_level = state.current_level
        state.current_level = target_level
        state.last_escalation_at = current_time

        # Determine channels to notify (new channels not already notified)
        channels_to_notify: list[AlertChannel] = []
        for lvl in range(old_level.value + 1, target_level.value + 1):
            level = EscalationLevel(lvl)
            level_channels = self._get_channels_for_level(level, state.card.severity)
            for ch in level_channels:
                if ch not in channels_to_notify:
                    # Check if already notified at this level
                    already_notified = state.channels_notified.get(level, [])
                    if ch not in already_notified:
                        channels_to_notify.append(ch)

        # Record notified channels
        if target_level not in state.channels_notified:
            state.channels_notified[target_level] = []
        state.channels_notified[target_level].extend(channels_to_notify)

        # Check for Level 4 actions
        trigger_auto_remediation = False
        trigger_emergency_pause = False

        if target_level == EscalationLevel.LEVEL_4:
            # Check if auto-remediation is available
            if state.card.has_auto_remediation and self.auto_remediation_callback:
                trigger_auto_remediation = True
            else:
                # Fall back to emergency pause
                trigger_emergency_pause = True

        logger.info(
            f"Escalated {alert_id}: level {old_level.value} -> {target_level.value}, "
            f"elapsed={elapsed:.0f}s, channels={[c.value for c in channels_to_notify]}"
        )

        return EscalationResult(
            escalated=True,
            new_level=target_level,
            channels_to_notify=channels_to_notify,
            trigger_auto_remediation=trigger_auto_remediation,
            trigger_emergency_pause=trigger_emergency_pause,
            message=(f"Escalated from level {old_level.value} to {target_level.value} after {elapsed:.0f} seconds"),
        )

    async def process_escalation(
        self,
        alert_id: str,
        current_time: datetime | None = None,
    ) -> EscalationResult:
        """Process escalation for an alert, including executing Level 4 actions.

        This method checks escalation and executes auto-remediation or
        emergency pause if Level 4 is reached.

        Args:
            alert_id: The alert ID to process
            current_time: Current time (defaults to now)

        Returns:
            EscalationResult with action details
        """
        if current_time is None:
            current_time = datetime.now(UTC)

        result = self.check_escalation(alert_id, current_time)

        if not result.escalated:
            return result

        state = self.escalations.get(alert_id)
        if not state:
            return result

        # Execute Level 4 actions
        if result.trigger_auto_remediation and self.auto_remediation_callback:
            try:
                success = self.auto_remediation_callback(state.strategy_id, state.card)
                if success:
                    state.status = EscalationStatus.AUTO_REMEDIATED
                    logger.info(f"Auto-remediation executed for {alert_id}")
                else:
                    # Fall back to emergency pause
                    result.trigger_auto_remediation = False
                    result.trigger_emergency_pause = True
            except Exception as e:
                logger.exception(f"Auto-remediation failed for {alert_id}: {e}")
                result.trigger_auto_remediation = False
                result.trigger_emergency_pause = True

        if result.trigger_emergency_pause and self.emergency_pause_callback:
            try:
                success = self.emergency_pause_callback(state.strategy_id, state.card)
                if success:
                    state.status = EscalationStatus.EMERGENCY_PAUSED
                    logger.info(f"Emergency pause executed for {alert_id}")
            except Exception as e:
                logger.exception(f"Emergency pause failed for {alert_id}: {e}")

        return result

    def process_escalation_sync(
        self,
        alert_id: str,
        current_time: datetime | None = None,
    ) -> EscalationResult:
        """Synchronous wrapper for process_escalation.

        Args:
            alert_id: The alert ID to process
            current_time: Current time (defaults to now)

        Returns:
            EscalationResult with action details
        """
        return asyncio.run(self.process_escalation(alert_id, current_time))

    def check_all_escalations(
        self,
        current_time: datetime | None = None,
    ) -> dict[str, EscalationResult]:
        """Check all active escalations.

        Args:
            current_time: Current time (defaults to now)

        Returns:
            Dict mapping alert_id to EscalationResult
        """
        if current_time is None:
            current_time = datetime.now(UTC)

        results: dict[str, EscalationResult] = {}

        for alert_id, state in self.escalations.items():
            if state.is_active():
                results[alert_id] = self.check_escalation(alert_id, current_time)

        return results

    async def process_all_escalations(
        self,
        current_time: datetime | None = None,
    ) -> dict[str, EscalationResult]:
        """Process all active escalations.

        Args:
            current_time: Current time (defaults to now)

        Returns:
            Dict mapping alert_id to EscalationResult
        """
        if current_time is None:
            current_time = datetime.now(UTC)

        results: dict[str, EscalationResult] = {}

        for alert_id, state in list(self.escalations.items()):
            if state.is_active():
                results[alert_id] = await self.process_escalation(alert_id, current_time)

        return results

    def get_escalation_state(self, alert_id: str) -> EscalationState | None:
        """Get the current escalation state for an alert.

        Args:
            alert_id: The alert ID

        Returns:
            EscalationState or None if not found
        """
        return self.escalations.get(alert_id)

    def get_active_escalations(self) -> list[EscalationState]:
        """Get all active escalations.

        Returns:
            List of active EscalationState objects
        """
        return [state for state in self.escalations.values() if state.is_active()]

    def get_escalations_for_strategy(self, strategy_id: str) -> list[EscalationState]:
        """Get all escalations for a strategy.

        Args:
            strategy_id: The strategy ID

        Returns:
            List of EscalationState objects for the strategy
        """
        return [state for state in self.escalations.values() if state.strategy_id == strategy_id]

    def clear_resolved_escalations(self, max_age_seconds: int = 86400) -> int:
        """Clear old resolved escalations to prevent memory buildup.

        Args:
            max_age_seconds: Maximum age for resolved escalations (default 24 hours)

        Returns:
            Number of escalations cleared
        """
        current_time = datetime.now(UTC)
        to_remove: list[str] = []

        for alert_id, state in self.escalations.items():
            if not state.is_active():
                age = (current_time - state.created_at).total_seconds()
                if age > max_age_seconds:
                    to_remove.append(alert_id)

        for alert_id in to_remove:
            del self.escalations[alert_id]

        if to_remove:
            logger.info(f"Cleared {len(to_remove)} old resolved escalations")

        return len(to_remove)
