"""Alert Manager for routing alerts to configured channels.

This module implements the AlertManager class which handles:
- Routing alerts to appropriate channels (Telegram, Slack, etc.)
- Evaluating alert rules to determine when to send
- Applying cooldowns to prevent alert spam
- Respecting quiet hours (only CRITICAL during quiet hours)
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, time
from decimal import Decimal

from ..models.operator_card import OperatorCard, Severity
from .alert_config import AlertChannel, AlertCondition, AlertConfig, AlertRule
from .channels import SlackChannel, TelegramChannel

logger = logging.getLogger(__name__)


@dataclass
class AlertSendResult:
    """Result of sending an alert through AlertManager."""

    success: bool
    channels_sent: list[AlertChannel] = field(default_factory=list)
    channels_failed: list[AlertChannel] = field(default_factory=list)
    errors: dict[AlertChannel, str] = field(default_factory=dict)
    skipped_reason: str | None = None


@dataclass
class CooldownTracker:
    """Tracks cooldown state for alert rules."""

    last_alert_time: dict[str, datetime] = field(default_factory=dict)

    def get_rule_key(self, strategy_id: str, condition: AlertCondition) -> str:
        """Generate a unique key for a rule instance."""
        return f"{strategy_id}:{condition.value}"

    def is_on_cooldown(
        self,
        strategy_id: str,
        condition: AlertCondition,
        cooldown_seconds: int,
        current_time: datetime | None = None,
    ) -> bool:
        """Check if an alert is currently on cooldown.

        Args:
            strategy_id: The strategy that triggered the alert
            condition: The alert condition
            cooldown_seconds: The cooldown duration in seconds
            current_time: Current time (defaults to now)

        Returns:
            True if the alert should be skipped due to cooldown
        """
        if current_time is None:
            current_time = datetime.now(UTC)

        key = self.get_rule_key(strategy_id, condition)
        last_time = self.last_alert_time.get(key)

        if last_time is None:
            return False

        elapsed = (current_time - last_time).total_seconds()
        return elapsed < cooldown_seconds

    def record_alert(
        self,
        strategy_id: str,
        condition: AlertCondition,
        sent_time: datetime | None = None,
    ) -> None:
        """Record that an alert was sent.

        Args:
            strategy_id: The strategy that triggered the alert
            condition: The alert condition
            sent_time: When the alert was sent (defaults to now)
        """
        if sent_time is None:
            sent_time = datetime.now(UTC)

        key = self.get_rule_key(strategy_id, condition)
        self.last_alert_time[key] = sent_time

    def clear_cooldown(self, strategy_id: str, condition: AlertCondition) -> None:
        """Clear the cooldown for a specific rule.

        Args:
            strategy_id: The strategy
            condition: The alert condition
        """
        key = self.get_rule_key(strategy_id, condition)
        self.last_alert_time.pop(key, None)

    def clear_all_for_strategy(self, strategy_id: str) -> None:
        """Clear all cooldowns for a strategy.

        Args:
            strategy_id: The strategy to clear cooldowns for
        """
        prefix = f"{strategy_id}:"
        keys_to_remove = [k for k in self.last_alert_time if k.startswith(prefix)]
        for key in keys_to_remove:
            del self.last_alert_time[key]


# Mapping from AlertCondition to EventType patterns for rule matching
CONDITION_EVENT_MAPPING: dict[AlertCondition, list[str]] = {
    AlertCondition.STRATEGY_STUCK: ["STUCK"],
    AlertCondition.STRATEGY_ERROR: ["ERROR"],
    AlertCondition.STRATEGY_PAUSED: ["WARNING"],
    AlertCondition.STRATEGY_RESUMED: ["ALERT"],
    AlertCondition.RISK_GUARD_TRIGGERED: ["ERROR", "ALERT"],
    AlertCondition.CIRCUIT_BREAKER_TRIGGERED: ["ERROR"],
    AlertCondition.LIQUIDATION_RISK: ["ALERT", "WARNING"],
    AlertCondition.TRANSACTION_PENDING_TIMEOUT: ["STUCK"],
}


class AlertManager:
    """Manages alert routing and delivery to configured channels.

    The AlertManager is responsible for:
    - Evaluating alert rules against incoming events
    - Routing alerts to the appropriate channels (Telegram, Slack, etc.)
    - Applying cooldown to prevent spam
    - Respecting quiet hours
    - Logging all sent alerts

    Attributes:
        config: The AlertConfig for this manager
        telegram_channel: Optional TelegramChannel instance
        slack_channel: Optional SlackChannel instance
        cooldown_tracker: Tracks cooldown state
    """

    def __init__(
        self,
        config: AlertConfig,
        telegram_bot_token: str | None = None,
        slack_webhook_url: str | None = None,
        slack_enable_threading: bool = True,
    ) -> None:
        """Initialize the AlertManager.

        Args:
            config: The AlertConfig with channel configurations and rules
            telegram_bot_token: Bot token for Telegram (required if using Telegram)
            slack_webhook_url: Webhook URL for Slack (overrides config.slack_webhook)
            slack_enable_threading: Whether to enable threading for Slack alerts
        """
        self.config = config
        self.cooldown_tracker = CooldownTracker()
        self._telegram_channel: TelegramChannel | None = None
        self._slack_channel: SlackChannel | None = None

        # Initialize Telegram channel if configured
        if config.telegram_chat_id and telegram_bot_token:
            self._telegram_channel = TelegramChannel(
                chat_id=config.telegram_chat_id,
                bot_token=telegram_bot_token,
                dashboard_base_url=config.dashboard_base_url,
            )
            logger.info(f"Initialized Telegram channel for chat_id {config.telegram_chat_id}")

        # Initialize Slack channel if configured
        # Use explicit webhook_url parameter or fall back to config
        effective_slack_webhook = slack_webhook_url or config.slack_webhook
        if effective_slack_webhook:
            self._slack_channel = SlackChannel(
                webhook_url=effective_slack_webhook,
                dashboard_base_url=config.dashboard_base_url,
                enable_threading=slack_enable_threading,
            )
            logger.info("Initialized Slack channel")

    @property
    def telegram_channel(self) -> TelegramChannel | None:
        """Get the Telegram channel if configured."""
        return self._telegram_channel

    @property
    def slack_channel(self) -> SlackChannel | None:
        """Get the Slack channel if configured."""
        return self._slack_channel

    def _get_current_time(self) -> time:
        """Get the current time for quiet hours checking."""
        return datetime.now(UTC).time()

    def _find_matching_rules(
        self,
        card: OperatorCard,
        metric_values: dict[AlertCondition, Decimal] | None = None,
    ) -> list[AlertRule]:
        """Find rules that match the given OperatorCard.

        Args:
            card: The OperatorCard to match against
            metric_values: Optional dict of current metric values for threshold conditions

        Returns:
            List of matching AlertRule instances
        """
        matching_rules: list[AlertRule] = []

        for rule in self.config.rules:
            if not rule.enabled:
                continue

            # Check if event type matches the condition
            event_type_str = card.event_type.value
            expected_events = CONDITION_EVENT_MAPPING.get(rule.condition, [])

            if expected_events and event_type_str in expected_events:
                # For event-based conditions, match on event type
                matching_rules.append(rule)
            elif metric_values and rule.condition in metric_values:
                # For threshold-based conditions, check the value
                value = metric_values[rule.condition]

                # Conditions with "BELOW" check if value < threshold
                if "BELOW" in rule.condition.value:
                    if value < rule.threshold:
                        matching_rules.append(rule)
                # Conditions with "ABOVE" check if value > threshold
                elif "ABOVE" in rule.condition.value:
                    if value > rule.threshold:
                        matching_rules.append(rule)
                # Conditions with "LOW" check if value < threshold
                elif "LOW" in rule.condition.value:
                    if value < rule.threshold:
                        matching_rules.append(rule)
                # Conditions with "HIGH" check if value > threshold
                elif "HIGH" in rule.condition.value:
                    if value > rule.threshold:
                        matching_rules.append(rule)

        return matching_rules

    def _should_send_alert(
        self,
        rule: AlertRule,
        strategy_id: str,
        severity: Severity,
    ) -> tuple[bool, str | None]:
        """Determine if an alert should be sent based on rules and cooldown.

        Args:
            rule: The alert rule being evaluated
            strategy_id: The strategy ID
            severity: The alert severity

        Returns:
            Tuple of (should_send, skip_reason)
        """
        # Check if alerting is globally enabled
        if not self.config.enabled:
            return False, "Alerting is globally disabled"

        # Check quiet hours
        current_time = self._get_current_time()
        if not self.config.should_send_alert(severity, current_time):
            return False, f"Quiet hours active, only CRITICAL alerts sent (severity={severity.value})"

        # Check cooldown
        if self.cooldown_tracker.is_on_cooldown(
            strategy_id=strategy_id,
            condition=rule.condition,
            cooldown_seconds=rule.cooldown_seconds,
        ):
            return False, f"On cooldown for condition {rule.condition.value}"

        return True, None

    def _format_telegram_message(
        self,
        card: OperatorCard,
        rule: AlertRule | None = None,
    ) -> str:
        """Format an OperatorCard as a Telegram message.

        This is used when TelegramChannel is not initialized but we still
        need to format the message for logging or other purposes.

        Args:
            card: The OperatorCard to format
            rule: Optional rule that triggered the alert

        Returns:
            Formatted message string with emoji severity
        """
        # Severity emoji mapping
        severity_emoji = {
            Severity.LOW: "\u2139\ufe0f",  # info
            Severity.MEDIUM: "\u26a0\ufe0f",  # warning
            Severity.HIGH: "\ud83d\udea8",  # alert
            Severity.CRITICAL: "\ud83d\udd34",  # red circle
        }

        emoji = severity_emoji.get(card.severity, "\u2753")

        lines = [
            f"{emoji} <b>{card.severity.value} Alert</b>",
            "",
            f"<b>Strategy:</b> {card.strategy_id}",
            f"<b>Status:</b> {card.event_type.value}",
            f"<b>Reason:</b> {card.reason.value.replace('_', ' ').title()}",
        ]

        # Add context details
        if card.context:
            lines.append("")
            lines.append("<b>Context:</b>")
            for key, value in card.context.items():
                formatted_key = key.replace("_", " ").title()
                lines.append(f"  \u2022 {formatted_key}: {value}")

        # Add position at risk
        if card.position_summary:
            lines.append("")
            lines.append(f"<b>Position at Risk:</b> ${card.position_summary.total_value_usd}")

        # Add risk description
        if card.risk_description:
            lines.append("")
            lines.append(f"<b>Risk:</b> {card.risk_description}")

        # Add recommended action
        if card.recommended_action:
            lines.append("")
            lines.append(f"<b>Recommended:</b> {card.recommended_action.description}")

        # Add dashboard link
        if self.config.dashboard_base_url:
            dashboard_link = f"{self.config.dashboard_base_url}/strategy/{card.strategy_id}"
            lines.append("")
            lines.append(f'\ud83d\udcca <a href="{dashboard_link}">View in Dashboard</a>')

        # Add timestamp
        lines.append("")
        lines.append(f"<i>{card.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}</i>")

        return "\n".join(lines)

    async def send_alert(
        self,
        card: OperatorCard,
        metric_values: dict[AlertCondition, Decimal] | None = None,
    ) -> AlertSendResult:
        """Send an alert for the given OperatorCard.

        This method:
        1. Finds matching alert rules based on the card's event type
        2. Checks if alerts should be sent (quiet hours, cooldown)
        3. Routes to configured channels
        4. Records cooldown state
        5. Logs all sent alerts

        Args:
            card: The OperatorCard to alert on
            metric_values: Optional dict of metric values for threshold-based rules

        Returns:
            AlertSendResult with status and any errors
        """
        result = AlertSendResult(success=False)

        # Check if alerting is enabled
        if not self.config.enabled:
            result.skipped_reason = "Alerting is globally disabled"
            logger.info(f"Alert skipped for {card.strategy_id}: {result.skipped_reason}")
            return result

        # Find matching rules
        matching_rules = self._find_matching_rules(card, metric_values)

        if not matching_rules:
            result.skipped_reason = "No matching alert rules"
            logger.debug(f"Alert skipped for {card.strategy_id}: {result.skipped_reason}")
            return result

        # Get unique channels from all matching rules
        channels_to_send: set[AlertChannel] = set()
        for rule in matching_rules:
            # Check if this rule should fire
            should_send, skip_reason = self._should_send_alert(
                rule=rule,
                strategy_id=card.strategy_id,
                severity=card.severity,
            )

            if should_send:
                for channel in rule.channels:
                    channels_to_send.add(channel)
            else:
                logger.debug(f"Rule {rule.condition.value} skipped for {card.strategy_id}: {skip_reason}")

        if not channels_to_send:
            result.skipped_reason = "All matching rules on cooldown or quiet hours"
            logger.info(f"Alert skipped for {card.strategy_id}: {result.skipped_reason}")
            return result

        # Send to each channel
        for channel in channels_to_send:
            if channel == AlertChannel.TELEGRAM:
                if self._telegram_channel:
                    try:
                        send_result = await self._telegram_channel.send_alert(card)
                        if send_result.success:
                            result.channels_sent.append(channel)
                            logger.info(f"Telegram alert sent for {card.strategy_id} (severity={card.severity.value})")
                        else:
                            result.channels_failed.append(channel)
                            result.errors[channel] = send_result.error or "Unknown error"
                            logger.error(f"Telegram alert failed for {card.strategy_id}: {send_result.error}")
                    except Exception as e:
                        result.channels_failed.append(channel)
                        result.errors[channel] = str(e)
                        logger.exception(f"Exception sending Telegram alert for {card.strategy_id}: {e}")
                else:
                    result.channels_failed.append(channel)
                    result.errors[channel] = "Telegram channel not configured"
                    logger.warning(f"Telegram alert requested but channel not configured for {card.strategy_id}")

            elif channel == AlertChannel.SLACK:
                if self._slack_channel:
                    try:
                        slack_send_result = await self._slack_channel.send_alert(card)
                        if slack_send_result.success:
                            result.channels_sent.append(channel)
                            thread_info = ""
                            if slack_send_result.thread_ts:
                                thread_info = f", thread_ts={slack_send_result.thread_ts}"
                            logger.info(
                                f"Slack alert sent for {card.strategy_id} (severity={card.severity.value}{thread_info})"
                            )
                        else:
                            result.channels_failed.append(channel)
                            result.errors[channel] = slack_send_result.error or "Unknown error"
                            logger.error(f"Slack alert failed for {card.strategy_id}: {slack_send_result.error}")
                    except Exception as e:
                        result.channels_failed.append(channel)
                        result.errors[channel] = str(e)
                        logger.exception(f"Exception sending Slack alert for {card.strategy_id}: {e}")
                else:
                    result.channels_failed.append(channel)
                    result.errors[channel] = "Slack channel not configured"
                    logger.warning(f"Slack alert requested but channel not configured for {card.strategy_id}")

            # Other channels handled in future stories
            elif channel in (AlertChannel.EMAIL, AlertChannel.PAGERDUTY):
                logger.debug(f"Channel {channel.value} not yet implemented, skipping for {card.strategy_id}")

        # Record cooldown for successfully sent alerts
        if result.channels_sent:
            for rule in matching_rules:
                if any(ch in result.channels_sent for ch in rule.channels):
                    self.cooldown_tracker.record_alert(
                        strategy_id=card.strategy_id,
                        condition=rule.condition,
                    )

        # Set overall success
        result.success = len(result.channels_sent) > 0

        # Log summary
        if result.success:
            logger.info(
                f"Alert sent for {card.strategy_id}: "
                f"channels_sent={[c.value for c in result.channels_sent]}, "
                f"channels_failed={[c.value for c in result.channels_failed]}"
            )
        else:
            logger.warning(
                f"Alert failed for {card.strategy_id}: errors={result.errors}, skipped_reason={result.skipped_reason}"
            )

        return result

    def send_alert_sync(
        self,
        card: OperatorCard,
        metric_values: dict[AlertCondition, Decimal] | None = None,
    ) -> AlertSendResult:
        """Synchronous wrapper for send_alert.

        Args:
            card: The OperatorCard to alert on
            metric_values: Optional dict of metric values for threshold-based rules

        Returns:
            AlertSendResult with status and any errors
        """
        return asyncio.run(self.send_alert(card, metric_values))

    async def send_direct_telegram_alert(
        self,
        card: OperatorCard,
    ) -> AlertSendResult:
        """Send an alert directly to Telegram, bypassing rule matching.

        This is useful for critical system alerts that should always
        go through regardless of configured rules.

        Args:
            card: The OperatorCard to alert on

        Returns:
            AlertSendResult with status
        """
        result = AlertSendResult(success=False)

        # Check quiet hours - only block non-CRITICAL
        current_time = self._get_current_time()
        if not self.config.should_send_alert(card.severity, current_time):
            result.skipped_reason = f"Quiet hours active (severity={card.severity.value})"
            logger.info(f"Direct alert skipped for {card.strategy_id}: {result.skipped_reason}")
            return result

        if not self._telegram_channel:
            result.skipped_reason = "Telegram channel not configured"
            result.channels_failed.append(AlertChannel.TELEGRAM)
            result.errors[AlertChannel.TELEGRAM] = "Channel not configured"
            logger.warning("Direct Telegram alert skipped: channel not configured")
            return result

        try:
            send_result = await self._telegram_channel.send_alert(card)
            if send_result.success:
                result.success = True
                result.channels_sent.append(AlertChannel.TELEGRAM)
                logger.info(
                    f"Direct Telegram alert sent for {card.strategy_id} "
                    f"(severity={card.severity.value}, message_id={send_result.message_id})"
                )
            else:
                result.channels_failed.append(AlertChannel.TELEGRAM)
                result.errors[AlertChannel.TELEGRAM] = send_result.error or "Unknown error"
                logger.error(f"Direct Telegram alert failed for {card.strategy_id}: {send_result.error}")
        except Exception as e:
            result.channels_failed.append(AlertChannel.TELEGRAM)
            result.errors[AlertChannel.TELEGRAM] = str(e)
            logger.exception(f"Exception sending direct Telegram alert for {card.strategy_id}: {e}")

        return result

    def send_direct_telegram_alert_sync(
        self,
        card: OperatorCard,
    ) -> AlertSendResult:
        """Synchronous wrapper for send_direct_telegram_alert.

        Args:
            card: The OperatorCard to alert on

        Returns:
            AlertSendResult with status
        """
        return asyncio.run(self.send_direct_telegram_alert(card))

    async def send_direct_slack_alert(
        self,
        card: OperatorCard,
        thread_ts: str | None = None,
    ) -> AlertSendResult:
        """Send an alert directly to Slack, bypassing rule matching.

        This is useful for critical system alerts that should always
        go through regardless of configured rules. Supports threading
        for related alerts.

        Args:
            card: The OperatorCard to alert on
            thread_ts: Optional thread timestamp to reply to

        Returns:
            AlertSendResult with status
        """
        result = AlertSendResult(success=False)

        # Check quiet hours - only block non-CRITICAL
        current_time = self._get_current_time()
        if not self.config.should_send_alert(card.severity, current_time):
            result.skipped_reason = f"Quiet hours active (severity={card.severity.value})"
            logger.info(f"Direct Slack alert skipped for {card.strategy_id}: {result.skipped_reason}")
            return result

        if not self._slack_channel:
            result.skipped_reason = "Slack channel not configured"
            result.channels_failed.append(AlertChannel.SLACK)
            result.errors[AlertChannel.SLACK] = "Channel not configured"
            logger.warning("Direct Slack alert skipped: channel not configured")
            return result

        try:
            send_result = await self._slack_channel.send_alert(card, thread_ts=thread_ts)
            if send_result.success:
                result.success = True
                result.channels_sent.append(AlertChannel.SLACK)
                thread_info = ""
                if send_result.thread_ts:
                    thread_info = f", thread_ts={send_result.thread_ts}"
                logger.info(
                    f"Direct Slack alert sent for {card.strategy_id} (severity={card.severity.value}{thread_info})"
                )
            else:
                result.channels_failed.append(AlertChannel.SLACK)
                result.errors[AlertChannel.SLACK] = send_result.error or "Unknown error"
                logger.error(f"Direct Slack alert failed for {card.strategy_id}: {send_result.error}")
        except Exception as e:
            result.channels_failed.append(AlertChannel.SLACK)
            result.errors[AlertChannel.SLACK] = str(e)
            logger.exception(f"Exception sending direct Slack alert for {card.strategy_id}: {e}")

        return result

    def send_direct_slack_alert_sync(
        self,
        card: OperatorCard,
        thread_ts: str | None = None,
    ) -> AlertSendResult:
        """Synchronous wrapper for send_direct_slack_alert.

        Args:
            card: The OperatorCard to alert on
            thread_ts: Optional thread timestamp to reply to

        Returns:
            AlertSendResult with status
        """
        return asyncio.run(self.send_direct_slack_alert(card, thread_ts=thread_ts))

    def set_slack_thread(self, strategy_id: str, thread_ts: str) -> None:
        """Set the Slack thread timestamp for a strategy.

        This enables subsequent alerts for this strategy to be
        posted as thread replies.

        Args:
            strategy_id: The strategy ID
            thread_ts: The thread timestamp from Slack
        """
        if self._slack_channel:
            self._slack_channel.set_thread_for_strategy(strategy_id, thread_ts)

    def clear_slack_thread(self, strategy_id: str) -> None:
        """Clear the Slack thread context for a strategy.

        Call this when a strategy issue is resolved to start fresh
        threads for future alerts.

        Args:
            strategy_id: The strategy ID
        """
        if self._slack_channel:
            self._slack_channel.clear_thread(strategy_id)

    def clear_cooldown(
        self,
        strategy_id: str,
        condition: AlertCondition | None = None,
    ) -> None:
        """Clear cooldown state for a strategy.

        Args:
            strategy_id: The strategy to clear cooldowns for
            condition: Optional specific condition to clear (clears all if None)
        """
        if condition:
            self.cooldown_tracker.clear_cooldown(strategy_id, condition)
            logger.debug(f"Cleared cooldown for {strategy_id}:{condition.value}")
        else:
            self.cooldown_tracker.clear_all_for_strategy(strategy_id)
            logger.debug(f"Cleared all cooldowns for {strategy_id}")
