"""Alert configuration schema for multi-channel notifications.

This module defines the configuration schema for alerting, including:
- AlertChannel: Supported notification channels (Telegram, Slack, Email, PagerDuty)
- AlertCondition: Conditions that can trigger alerts
- AlertRule: Rules defining when and how to send alerts
- AlertConfig: Main configuration for a strategy's alerting setup
"""

from dataclasses import dataclass, field
from datetime import time
from decimal import Decimal
from enum import StrEnum
from typing import Any

# Import Severity from models to reuse the existing enum
from ..models.operator_card import Severity


class AlertChannel(StrEnum):
    """Supported notification channels for alerts."""

    TELEGRAM = "TELEGRAM"
    SLACK = "SLACK"
    EMAIL = "EMAIL"
    PAGERDUTY = "PAGERDUTY"


class AlertCondition(StrEnum):
    """Conditions that can trigger an alert.

    These conditions are evaluated against strategy metrics and state
    to determine when alerts should be sent.
    """

    # PnL conditions
    PNL_24H_BELOW = "PNL_24H_BELOW"
    PNL_24H_ABOVE = "PNL_24H_ABOVE"
    PNL_7D_BELOW = "PNL_7D_BELOW"
    PNL_7D_ABOVE = "PNL_7D_ABOVE"
    TOTAL_PNL_BELOW = "TOTAL_PNL_BELOW"

    # Strategy state conditions
    STRATEGY_STUCK = "STRATEGY_STUCK"
    STRATEGY_ERROR = "STRATEGY_ERROR"
    STRATEGY_PAUSED = "STRATEGY_PAUSED"
    STRATEGY_RESUMED = "STRATEGY_RESUMED"

    # Position conditions
    POSITION_SIZE_ABOVE = "POSITION_SIZE_ABOVE"
    POSITION_SIZE_BELOW = "POSITION_SIZE_BELOW"
    LEVERAGE_ABOVE = "LEVERAGE_ABOVE"

    # Health conditions
    HEALTH_FACTOR_BELOW = "HEALTH_FACTOR_BELOW"
    LIQUIDATION_RISK = "LIQUIDATION_RISK"

    # Gas conditions
    GAS_PRICE_ABOVE = "GAS_PRICE_ABOVE"
    TRANSACTION_PENDING_TIMEOUT = "TRANSACTION_PENDING_TIMEOUT"

    # Balance conditions
    BALANCE_LOW = "BALANCE_LOW"
    GAS_BALANCE_LOW = "GAS_BALANCE_LOW"

    # Market conditions
    SLIPPAGE_HIGH = "SLIPPAGE_HIGH"
    PRICE_DEVIATION = "PRICE_DEVIATION"
    ORACLE_STALE = "ORACLE_STALE"

    # Risk guard conditions
    RISK_GUARD_TRIGGERED = "RISK_GUARD_TRIGGERED"
    CIRCUIT_BREAKER_TRIGGERED = "CIRCUIT_BREAKER_TRIGGERED"

    # Custom condition for user-defined checks
    CUSTOM = "CUSTOM"


@dataclass
class TimeRange:
    """A time range for quiet hours configuration.

    Alerts during quiet hours will only be sent for CRITICAL severity.

    Attributes:
        start: Start time of the quiet period (inclusive)
        end: End time of the quiet period (exclusive)
        timezone: Timezone for the time range (e.g., "UTC", "America/New_York")
    """

    start: time
    end: time
    timezone: str = "UTC"

    def __post_init__(self) -> None:
        """Validate the time range."""
        if not isinstance(self.start, time):
            raise ValueError("start must be a time object")
        if not isinstance(self.end, time):
            raise ValueError("end must be a time object")

    def contains(self, check_time: time) -> bool:
        """Check if a given time falls within this range.

        Handles ranges that span midnight (e.g., 22:00 to 06:00).

        Args:
            check_time: The time to check

        Returns:
            True if check_time is within the range
        """
        if self.start <= self.end:
            # Normal range (e.g., 09:00 to 17:00)
            return self.start <= check_time < self.end
        else:
            # Range spans midnight (e.g., 22:00 to 06:00)
            return check_time >= self.start or check_time < self.end


@dataclass
class AlertRule:
    """A rule defining when and how to send an alert.

    Attributes:
        condition: The condition that triggers this alert
        threshold: The threshold value for the condition (interpretation depends on condition)
        severity: Severity level for alerts triggered by this rule
        channels: List of channels to send alerts to
        cooldown_seconds: Minimum seconds between alerts for this rule
        enabled: Whether this rule is active
        description: Human-readable description of the rule
        custom_message: Optional custom message template for the alert
    """

    condition: AlertCondition
    threshold: Decimal
    severity: Severity
    channels: list[AlertChannel]
    cooldown_seconds: int = 300  # 5 minutes default
    enabled: bool = True
    description: str = ""
    custom_message: str | None = None

    def __post_init__(self) -> None:
        """Validate the alert rule."""
        if not self.channels:
            raise ValueError("At least one channel must be specified")
        if self.cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be non-negative")

    def to_dict(self) -> dict[str, Any]:
        """Convert the alert rule to a dictionary for serialization."""
        return {
            "condition": self.condition.value,
            "threshold": str(self.threshold),
            "severity": self.severity.value,
            "channels": [c.value for c in self.channels],
            "cooldown_seconds": self.cooldown_seconds,
            "enabled": self.enabled,
            "description": self.description,
            "custom_message": self.custom_message,
        }


@dataclass
class AlertConfig:
    """Configuration for a strategy's alerting setup.

    This dataclass holds all the configuration needed to send alerts
    to operators via multiple channels.

    Attributes:
        telegram_chat_id: Telegram chat ID for notifications
        slack_webhook: Slack webhook URL for notifications
        email: Email address for notifications
        pagerduty_key: PagerDuty integration key for critical alerts
        rules: List of alert rules to evaluate
        quiet_hours: Optional time range during which only CRITICAL alerts are sent
        escalation_timeout_seconds: Time before escalating unacknowledged alerts
        dashboard_base_url: Base URL for dashboard links in alerts
        enabled: Global enable/disable for all alerting
    """

    # Channel configurations (all optional)
    telegram_chat_id: str | None = None
    slack_webhook: str | None = None
    email: str | None = None
    pagerduty_key: str | None = None

    # Alert rules
    rules: list[AlertRule] = field(default_factory=list)

    # Quiet hours
    quiet_hours: TimeRange | None = None

    # Escalation
    escalation_timeout_seconds: int = 900  # 15 minutes default

    # Dashboard link
    dashboard_base_url: str | None = None

    # Global enable
    enabled: bool = True

    @property
    def configured_channels(self) -> list[AlertChannel]:
        """Get the list of channels that have been configured."""
        channels: list[AlertChannel] = []
        if self.telegram_chat_id:
            channels.append(AlertChannel.TELEGRAM)
        if self.slack_webhook:
            channels.append(AlertChannel.SLACK)
        if self.email:
            channels.append(AlertChannel.EMAIL)
        if self.pagerduty_key:
            channels.append(AlertChannel.PAGERDUTY)
        return channels

    def has_channel(self, channel: AlertChannel) -> bool:
        """Check if a specific channel is configured."""
        return channel in self.configured_channels

    def get_rules_for_condition(self, condition: AlertCondition) -> list[AlertRule]:
        """Get all enabled rules for a specific condition."""
        return [rule for rule in self.rules if rule.condition == condition and rule.enabled]

    def get_rules_for_channel(self, channel: AlertChannel) -> list[AlertRule]:
        """Get all enabled rules that include a specific channel."""
        return [rule for rule in self.rules if channel in rule.channels and rule.enabled]

    def is_in_quiet_hours(self, check_time: time) -> bool:
        """Check if the given time is within quiet hours."""
        if self.quiet_hours is None:
            return False
        return self.quiet_hours.contains(check_time)

    def should_send_alert(self, severity: Severity, current_time: time) -> bool:
        """Determine if an alert should be sent based on severity and quiet hours.

        During quiet hours, only CRITICAL alerts are sent.

        Args:
            severity: The severity of the alert
            current_time: The current time to check against quiet hours

        Returns:
            True if the alert should be sent
        """
        if not self.enabled:
            return False
        if self.is_in_quiet_hours(current_time):
            return severity == Severity.CRITICAL
        return True

    def to_dict(self) -> dict[str, Any]:
        """Convert the alert config to a dictionary for serialization."""
        return {
            "telegram_chat_id": self.telegram_chat_id,
            "slack_webhook": self.slack_webhook,
            "email": self.email,
            "pagerduty_key": self.pagerduty_key,
            "rules": [rule.to_dict() for rule in self.rules],
            "quiet_hours": {
                "start": self.quiet_hours.start.isoformat(),
                "end": self.quiet_hours.end.isoformat(),
                "timezone": self.quiet_hours.timezone,
            }
            if self.quiet_hours
            else None,
            "escalation_timeout_seconds": self.escalation_timeout_seconds,
            "dashboard_base_url": self.dashboard_base_url,
            "enabled": self.enabled,
        }
