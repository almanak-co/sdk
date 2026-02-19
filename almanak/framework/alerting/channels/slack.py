"""Slack webhook integration for alert notifications.

This module implements the SlackChannel class for sending alerts
to operators via Slack incoming webhooks.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

import httpx

from ...models.operator_card import OperatorCard, Severity

logger = logging.getLogger(__name__)


# Severity to color mapping (Slack attachment colors)
SEVERITY_COLOR = {
    Severity.LOW: "#36a64f",  # green
    Severity.MEDIUM: "#ffcc00",  # yellow
    Severity.HIGH: "#ff9900",  # orange
    Severity.CRITICAL: "#ff0000",  # red
}

# Severity to emoji mapping
SEVERITY_EMOJI = {
    Severity.LOW: ":information_source:",
    Severity.MEDIUM: ":warning:",
    Severity.HIGH: ":rotating_light:",
    Severity.CRITICAL: ":red_circle:",
}


@dataclass
class SlackSendResult:
    """Result of sending a Slack message."""

    success: bool
    error: str | None = None
    retry_after: int | None = None
    thread_ts: str | None = None  # Thread timestamp for threading replies


class SlackChannel:
    """Slack notification channel for sending alerts via webhooks.

    This class implements Slack incoming webhooks for sending alert
    notifications to operators. It uses Slack Block Kit for rich
    formatting and handles rate limiting with exponential backoff.

    Supports threading for related alerts - subsequent alerts for the same
    strategy will be posted as thread replies to the original alert.

    Attributes:
        webhook_url: The Slack incoming webhook URL
        dashboard_base_url: Base URL for dashboard links in messages
        max_retries: Maximum number of retries for failed sends
        base_delay: Base delay in seconds for exponential backoff
    """

    def __init__(
        self,
        webhook_url: str,
        dashboard_base_url: str | None = None,
        max_retries: int = 3,
        base_delay: float = 1.0,
        enable_threading: bool = True,
        thread_timeout_seconds: int = 3600,  # 1 hour default
    ) -> None:
        """Initialize the Slack channel.

        Args:
            webhook_url: The Slack incoming webhook URL
            dashboard_base_url: Base URL for dashboard links in messages
            max_retries: Maximum number of retries for failed sends
            base_delay: Base delay in seconds for exponential backoff
            enable_threading: Whether to enable threading for related alerts
            thread_timeout_seconds: How long to keep thread context (default 1 hour)
        """
        if not webhook_url:
            raise ValueError("webhook_url is required")

        self.webhook_url = webhook_url
        self.dashboard_base_url = dashboard_base_url
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.enable_threading = enable_threading
        self.thread_timeout_seconds = thread_timeout_seconds

        # Track rate limiting
        self._last_send_time: float = 0.0
        self._min_interval: float = 1.0  # 1 second minimum between sends

        # Track threads per strategy (strategy_id -> (thread_ts, timestamp))
        self._strategy_threads: dict[str, tuple[str, float]] = {}

    def _get_thread_ts(self, strategy_id: str) -> str | None:
        """Get the thread_ts for a strategy if within timeout.

        Args:
            strategy_id: The strategy ID

        Returns:
            Thread timestamp if valid, None otherwise
        """
        if not self.enable_threading:
            return None

        thread_data = self._strategy_threads.get(strategy_id)
        if thread_data is None:
            return None

        thread_ts, created_at = thread_data
        now = time.time()

        # Check if thread has expired
        if now - created_at > self.thread_timeout_seconds:
            del self._strategy_threads[strategy_id]
            logger.debug(f"Thread expired for strategy {strategy_id}, will create new thread")
            return None

        return thread_ts

    def _set_thread_ts(self, strategy_id: str, thread_ts: str) -> None:
        """Store the thread_ts for a strategy.

        Args:
            strategy_id: The strategy ID
            thread_ts: The thread timestamp from Slack
        """
        if self.enable_threading:
            self._strategy_threads[strategy_id] = (thread_ts, time.time())
            logger.debug(f"Stored thread_ts {thread_ts} for strategy {strategy_id}")

    def clear_thread(self, strategy_id: str) -> None:
        """Clear the thread context for a strategy.

        Call this when a strategy issue is resolved to start fresh
        threads for future alerts.

        Args:
            strategy_id: The strategy ID
        """
        if strategy_id in self._strategy_threads:
            del self._strategy_threads[strategy_id]
            logger.debug(f"Cleared thread context for strategy {strategy_id}")

    def clear_all_threads(self) -> None:
        """Clear all thread contexts."""
        self._strategy_threads.clear()
        logger.debug("Cleared all thread contexts")

    def _build_blocks(self, card: OperatorCard) -> list[dict[str, Any]]:
        """Build Slack Block Kit blocks for an OperatorCard.

        Args:
            card: The OperatorCard to format

        Returns:
            List of Slack Block Kit blocks
        """
        emoji = SEVERITY_EMOJI.get(card.severity, ":question:")
        blocks: list[dict[str, Any]] = []

        # Header section
        blocks.append(
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} {card.severity.value} Alert",
                    "emoji": True,
                },
            }
        )

        # Strategy info section
        blocks.append(
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Strategy:*\n{card.strategy_id}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Status:*\n{card.event_type.value}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Reason:*\n{card.reason.value.replace('_', ' ').title()}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Timestamp:*\n{card.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}",
                    },
                ],
            }
        )

        # Context section (if available)
        if card.context:
            context_lines = []
            for key, value in card.context.items():
                formatted_key = key.replace("_", " ").title()
                context_lines.append(f"• {formatted_key}: {value}")

            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Context:*\n{chr(10).join(context_lines)}",
                    },
                }
            )

        # Position at risk section
        if card.position_summary:
            blocks.append(
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Position at Risk:*\n${card.position_summary.total_value_usd}",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Available Balance:*\n${card.position_summary.available_balance_usd}",
                        },
                    ],
                }
            )

        # Risk description
        if card.risk_description:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Risk:* {card.risk_description}",
                    },
                }
            )

        # Recommended action
        if card.recommended_action:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f":point_right: *Recommended Action:* {card.recommended_action.description}",
                    },
                }
            )

        # Divider before actions
        blocks.append({"type": "divider"})

        # Action buttons
        action_elements: list[dict[str, Any]] = []

        # Add dashboard link button
        if self.dashboard_base_url:
            dashboard_link = f"{self.dashboard_base_url}/strategy/{card.strategy_id}"
            action_elements.append(
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": ":bar_chart: View Dashboard",
                        "emoji": True,
                    },
                    "url": dashboard_link,
                    "action_id": "view_dashboard",
                }
            )

        # Add action buttons for available actions (up to 3 more to fit Slack limit of 5)
        action_labels = {
            "BUMP_GAS": ":zap: Bump Gas",
            "CANCEL_TX": ":x: Cancel TX",
            "PAUSE": ":pause_button: Pause",
            "RESUME": ":arrow_forward: Resume",
            "EMERGENCY_UNWIND": ":rotating_light: Emergency Unwind",
        }

        for action in card.available_actions[:4]:  # Limit to 4 actions + dashboard = 5 total
            label = action_labels.get(action.value, action.value)
            if self.dashboard_base_url:
                action_url = f"{self.dashboard_base_url}/strategy/{card.strategy_id}/action/{action.value.lower()}"
                action_elements.append(
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": label,
                            "emoji": True,
                        },
                        "url": action_url,
                        "action_id": f"action_{action.value.lower()}",
                    }
                )

        if action_elements:
            blocks.append(
                {
                    "type": "actions",
                    "elements": action_elements[:5],  # Slack limit is 5 elements per actions block
                }
            )

        return blocks

    def _build_payload(
        self,
        card: OperatorCard,
        thread_ts: str | None = None,
    ) -> dict[str, Any]:
        """Build the complete Slack webhook payload.

        Args:
            card: The OperatorCard to format
            thread_ts: Optional thread timestamp for threading replies

        Returns:
            Complete Slack webhook payload with blocks and attachments
        """
        blocks = self._build_blocks(card)
        color = SEVERITY_COLOR.get(card.severity, "#808080")

        payload: dict[str, Any] = {
            "blocks": blocks,
            "attachments": [
                {
                    "color": color,
                    "fallback": f"{card.severity.value} Alert: {card.strategy_id} - {card.reason.value}",
                }
            ],
        }

        # Add thread_ts if provided for threading
        if thread_ts:
            payload["thread_ts"] = thread_ts
            # reply_broadcast=True sends to channel AND thread
            # This ensures visibility while keeping context
            payload["reply_broadcast"] = True

        return payload

    async def _send_payload(self, payload: dict[str, Any]) -> SlackSendResult:
        """Send a payload to the Slack webhook.

        Args:
            payload: The webhook payload to send

        Returns:
            SlackSendResult with success status and details
        """
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    self.webhook_url,
                    json=payload,
                    timeout=30.0,
                )

                if response.status_code == 200:
                    return SlackSendResult(success=True)

                # Check for rate limiting (429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    return SlackSendResult(
                        success=False,
                        error="Rate limited by Slack",
                        retry_after=retry_after,
                    )

                # Other error
                return SlackSendResult(
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text}",
                )

            except httpx.TimeoutException:
                return SlackSendResult(success=False, error="Request timeout")
            except httpx.RequestError as e:
                return SlackSendResult(success=False, error=f"Request error: {e}")

    async def send_alert(
        self,
        card: OperatorCard,
        thread_ts: str | None = None,
    ) -> SlackSendResult:
        """Send an alert to Slack with exponential backoff retry.

        This method formats the OperatorCard using Slack Block Kit and
        sends it to the configured webhook. It handles rate limiting with
        exponential backoff and logs all send attempts.

        Threading support: If enable_threading is True and a thread_ts is
        provided (or stored from a previous alert for this strategy), the
        alert will be sent as a thread reply. Note that incoming webhooks
        don't return message timestamps, so for full threading support
        consider using the Slack Web API.

        Args:
            card: The OperatorCard containing alert information
            thread_ts: Optional thread timestamp to reply to

        Returns:
            SlackSendResult indicating success or failure, with thread_ts if available
        """
        # Get thread_ts from storage if not explicitly provided
        effective_thread_ts = thread_ts or self._get_thread_ts(card.strategy_id)

        # Build payload with threading if available
        payload = self._build_payload(card, thread_ts=effective_thread_ts)

        is_thread_reply = effective_thread_ts is not None

        # Enforce minimum interval between sends
        now = time.time()
        elapsed = now - self._last_send_time
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)

        # Attempt to send with exponential backoff
        last_error: str | None = None
        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                # Calculate backoff delay
                delay = self.base_delay * (2 ** (attempt - 1))
                logger.info(
                    f"Retrying Slack send (attempt {attempt + 1}/{self.max_retries + 1}) "
                    f"after {delay:.1f}s delay for strategy {card.strategy_id}"
                )
                await asyncio.sleep(delay)

            result = await self._send_payload(payload)

            self._last_send_time = time.time()

            if result.success:
                # Include thread context in result
                result.thread_ts = effective_thread_ts

                thread_info = " (thread reply)" if is_thread_reply else ""
                logger.info(
                    f"Slack alert sent successfully for strategy {card.strategy_id} "
                    f"(severity={card.severity.value}){thread_info}"
                )
                return result

            # If rate limited, use the server's retry_after value
            if result.retry_after:
                logger.warning(f"Rate limited by Slack, waiting {result.retry_after}s for strategy {card.strategy_id}")
                if attempt < self.max_retries:
                    await asyncio.sleep(result.retry_after)

            last_error = result.error
            logger.warning(
                f"Slack send failed (attempt {attempt + 1}/{self.max_retries + 1}): "
                f"{result.error} for strategy {card.strategy_id}"
            )

        # All retries exhausted
        logger.error(
            f"Failed to send Slack alert after {self.max_retries + 1} attempts "
            f"for strategy {card.strategy_id}: {last_error}"
        )
        return SlackSendResult(success=False, error=last_error)

    def send_alert_sync(
        self,
        card: OperatorCard,
        thread_ts: str | None = None,
    ) -> SlackSendResult:
        """Synchronous wrapper for send_alert.

        Args:
            card: The OperatorCard containing alert information
            thread_ts: Optional thread timestamp to reply to

        Returns:
            SlackSendResult indicating success or failure
        """
        return asyncio.run(self.send_alert(card, thread_ts=thread_ts))

    def set_thread_for_strategy(self, strategy_id: str, thread_ts: str) -> None:
        """Set the thread_ts for a strategy externally.

        This allows integration with the Slack Web API which returns
        message timestamps. After sending a message via Web API, call
        this method to enable subsequent alerts to be threaded.

        Args:
            strategy_id: The strategy ID
            thread_ts: The thread timestamp from Slack Web API
        """
        self._set_thread_ts(strategy_id, thread_ts)

    async def send_custom_message(
        self,
        strategy_id: str,
        severity: Severity,
        title: str,
        message: str,
        context: dict[str, Any] | None = None,
        thread_ts: str | None = None,
    ) -> SlackSendResult:
        """Send a custom formatted message.

        This method allows sending custom formatted messages that don't
        come from an OperatorCard. Supports threading for related messages.

        Args:
            strategy_id: The strategy ID
            severity: Alert severity level
            title: Alert title
            message: Alert message body
            context: Optional additional context
            thread_ts: Optional thread timestamp to reply to

        Returns:
            SlackSendResult indicating success or failure
        """
        emoji = SEVERITY_EMOJI.get(severity, ":question:")
        color = SEVERITY_COLOR.get(severity, "#808080")

        blocks: list[dict[str, Any]] = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} {severity.value}: {title}",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Strategy:* {strategy_id}\n\n{message}",
                },
            },
        ]

        if context:
            context_lines = []
            for key, value in context.items():
                formatted_key = key.replace("_", " ").title()
                context_lines.append(f"• {formatted_key}: {value}")

            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Details:*\n{chr(10).join(context_lines)}",
                    },
                }
            )

        # Add dashboard link button if configured
        if self.dashboard_base_url:
            dashboard_link = f"{self.dashboard_base_url}/strategy/{strategy_id}"
            blocks.append(
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": ":bar_chart: View Dashboard",
                                "emoji": True,
                            },
                            "url": dashboard_link,
                            "action_id": "view_dashboard",
                        },
                    ],
                }
            )

        # Get thread_ts from storage if not explicitly provided
        effective_thread_ts = thread_ts or self._get_thread_ts(strategy_id)

        payload: dict[str, Any] = {
            "blocks": blocks,
            "attachments": [
                {
                    "color": color,
                    "fallback": f"{severity.value}: {title} - {strategy_id}",
                }
            ],
        }

        # Add thread_ts if available for threading
        if effective_thread_ts:
            payload["thread_ts"] = effective_thread_ts
            payload["reply_broadcast"] = True

        is_thread_reply = effective_thread_ts is not None
        result = await self._send_payload(payload)

        if result.success:
            result.thread_ts = effective_thread_ts
            thread_info = " (thread reply)" if is_thread_reply else ""
            logger.info(f"Custom Slack message sent for strategy {strategy_id}{thread_info}")
        else:
            logger.error(f"Failed to send custom Slack message for strategy {strategy_id}: {result.error}")

        return result
