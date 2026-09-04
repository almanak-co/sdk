"""Telegram webhook integration for alert notifications.

This module implements the TelegramChannel class for sending alerts
to operators via Telegram bot API.
"""

import asyncio
import html
import logging
import time
from dataclasses import dataclass
from typing import Any

import httpx

from almanak.core.redaction import redact

from ...models.operator_card import OperatorCard, Severity

logger = logging.getLogger(__name__)


# Severity to emoji mapping
SEVERITY_EMOJI = {
    Severity.LOW: "\u2139\ufe0f",  # ℹ️ info
    Severity.MEDIUM: "\u26a0\ufe0f",  # ⚠️ warning
    Severity.HIGH: "\ud83d\udea8",  # 🚨 alert
    Severity.CRITICAL: "\ud83d\udd34",  # 🔴 red circle
}


def _html_text(value: object) -> str:
    """Redact and escape a dynamic value for Telegram HTML."""
    return html.escape(redact(str(value)), quote=True)


@dataclass
class TelegramSendResult:
    """Result of sending a Telegram message."""

    success: bool
    message_id: int | None = None
    error: str | None = None
    retry_after: int | None = None


class TelegramChannel:
    """Telegram notification channel for sending alerts.

    This class implements the Telegram Bot API for sending alert
    notifications to operators. It handles rate limiting with
    exponential backoff and formats messages with severity indicators.

    Attributes:
        chat_id: The Telegram chat ID to send messages to
        bot_token: The Telegram bot API token
        dashboard_base_url: Base URL for dashboard links in messages
        max_retries: Maximum number of retries for failed sends
        base_delay: Base delay in seconds for exponential backoff
    """

    TELEGRAM_API_BASE = "https://api.telegram.org"

    def __init__(
        self,
        chat_id: str,
        bot_token: str,
        dashboard_base_url: str | None = None,
        max_retries: int = 3,
        base_delay: float = 1.0,
    ) -> None:
        """Initialize the Telegram channel.

        Args:
            chat_id: The Telegram chat ID to send messages to
            bot_token: The Telegram bot API token
            dashboard_base_url: Base URL for dashboard links in messages
            max_retries: Maximum number of retries for failed sends
            base_delay: Base delay in seconds for exponential backoff
        """
        if not chat_id:
            raise ValueError("chat_id is required")
        if not bot_token:
            raise ValueError("bot_token is required")

        self.chat_id = chat_id
        self.bot_token = bot_token
        self.dashboard_base_url = dashboard_base_url
        self.max_retries = max_retries
        self.base_delay = base_delay

        # Track rate limiting
        self._last_send_time: float = 0.0
        self._min_interval: float = 0.05  # 50ms minimum between sends

    @property
    def api_url(self) -> str:
        """Get the Telegram API URL for this bot."""
        return f"{self.TELEGRAM_API_BASE}/bot{self.bot_token}"

    def _format_alert_message(self, card: OperatorCard) -> str:
        """Format an OperatorCard as a Telegram message.

        Args:
            card: The OperatorCard to format

        Returns:
            Formatted message string with HTML formatting
        """
        emoji = SEVERITY_EMOJI.get(card.severity, "\u2753")  # ❓ fallback

        lines = [
            f"{emoji} <b>{_html_text(card.severity.value)} Alert</b>",
            "",
            f"<b>Strategy:</b> {_html_text(card.deployment_id)}",
            f"<b>Status:</b> {_html_text(card.event_type.value)}",
            f"<b>Reason:</b> {_html_text(card.reason.value.replace('_', ' ').title())}",
        ]

        # Add context details if available
        if card.context:
            lines.append("")
            lines.append("<b>Context:</b>")
            for key, value in card.context.items():
                # Format the key nicely
                formatted_key = _html_text(key.replace("_", " ").title())
                lines.append(f"  \u2022 {formatted_key}: {_html_text(value)}")

        # Add position at risk
        if card.position_summary:
            lines.append("")
            lines.append(f"<b>Position at Risk:</b> ${_html_text(card.position_summary.total_value_usd)}")

        # Add risk description
        if card.risk_description:
            lines.append("")
            lines.append(f"<b>Risk:</b> {_html_text(card.risk_description)}")

        # Add recommended action
        if card.recommended_action:
            lines.append("")
            lines.append(f"<b>Recommended:</b> {_html_text(card.recommended_action.description)}")

        # Add dashboard link
        if self.dashboard_base_url:
            dashboard_link = _html_text(f"{self.dashboard_base_url}/strategy/{card.deployment_id}")
            lines.append("")
            lines.append(f'\ud83d\udcca <a href="{dashboard_link}">View in Dashboard</a>')

        # Add timestamp
        lines.append("")
        lines.append(f"<i>{card.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}</i>")

        return "\n".join(lines)

    def _redact_error(self, error: object) -> str:
        """Remove the bot credential and registered secrets from an error."""
        message = str(error).replace(self.api_url, "<redacted Telegram API URL>")
        message = message.replace(self.bot_token, "***")
        return redact(message)

    async def _send_message(
        self,
        text: str,
        parse_mode: str = "HTML",
        disable_notification: bool = False,
    ) -> TelegramSendResult:
        """Send a message to Telegram.

        Args:
            text: The message text
            parse_mode: Message parse mode (HTML or Markdown)
            disable_notification: Whether to send silently

        Returns:
            TelegramSendResult with success status and details
        """
        url = f"{self.api_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_notification": disable_notification,
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(url, json=payload, timeout=30.0)
                try:
                    data = response.json()
                except ValueError:
                    # The Telegram API itself always answers JSON; a non-JSON
                    # body means an intermediary error page (e.g. HTML 502
                    # from a proxy). Treat as a send failure, never raise.
                    return TelegramSendResult(
                        success=False,
                        error=f"Non-JSON response from Telegram (HTTP {response.status_code})",
                    )

                if not isinstance(data, dict):
                    return TelegramSendResult(
                        success=False,
                        error=f"Malformed response from Telegram (HTTP {response.status_code})",
                    )

                if response.status_code == 200 and data.get("ok"):
                    response_result = data.get("result")
                    message_id = response_result.get("message_id") if isinstance(response_result, dict) else None
                    return TelegramSendResult(success=True, message_id=message_id)

                # Check for rate limiting (429)
                if response.status_code == 429:
                    parameters = data.get("parameters")
                    retry_after_value = parameters.get("retry_after", 60) if isinstance(parameters, dict) else 60
                    try:
                        retry_after = int(retry_after_value)
                    except (TypeError, ValueError):
                        retry_after = 60
                    return TelegramSendResult(
                        success=False,
                        error="Rate limited by Telegram",
                        retry_after=retry_after,
                    )

                # Other error
                error_desc = self._redact_error(data.get("description", "Unknown error"))
                return TelegramSendResult(success=False, error=error_desc)

            except httpx.TimeoutException:
                return TelegramSendResult(success=False, error="Request timeout")
            except httpx.RequestError as e:
                return TelegramSendResult(success=False, error=f"Request error: {self._redact_error(e)}")

    async def send_alert(self, card: OperatorCard) -> TelegramSendResult:
        """Send an alert to Telegram with exponential backoff retry.

        This method formats the OperatorCard as a Telegram message and
        sends it to the configured chat. It handles rate limiting with
        exponential backoff and logs all send attempts.

        Args:
            card: The OperatorCard containing alert information

        Returns:
            TelegramSendResult indicating success or failure
        """
        message = self._format_alert_message(card)
        safe_deployment_id = redact(card.deployment_id)

        # Determine if notification should be silent (low severity)
        disable_notification = card.severity == Severity.LOW

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
                    f"Retrying Telegram send (attempt {attempt + 1}/{self.max_retries + 1}) "
                    f"after {delay:.1f}s delay for strategy {safe_deployment_id}"
                )
                await asyncio.sleep(delay)

            result = await self._send_message(
                text=message,
                parse_mode="HTML",
                disable_notification=disable_notification,
            )

            self._last_send_time = time.time()

            if result.success:
                logger.info(
                    f"Telegram alert sent successfully for strategy {safe_deployment_id} "
                    f"(message_id={result.message_id}, severity={card.severity.value})"
                )
                return result

            # If rate limited, use the server's retry_after value
            if result.retry_after:
                logger.warning(
                    f"Rate limited by Telegram, waiting {result.retry_after}s for strategy {safe_deployment_id}"
                )
                if attempt < self.max_retries:
                    await asyncio.sleep(result.retry_after)

            last_error = self._redact_error(result.error) if result.error is not None else None
            logger.warning(
                f"Telegram send failed (attempt {attempt + 1}/{self.max_retries + 1}): "
                f"{last_error} for strategy {safe_deployment_id}"
            )

        # All retries exhausted
        logger.error(
            f"Failed to send Telegram alert after {self.max_retries + 1} attempts "
            f"for strategy {safe_deployment_id}: {last_error}"
        )
        return TelegramSendResult(success=False, error=last_error)

    def send_alert_sync(self, card: OperatorCard) -> TelegramSendResult:
        """Synchronous wrapper for send_alert.

        Args:
            card: The OperatorCard containing alert information

        Returns:
            TelegramSendResult indicating success or failure
        """
        return asyncio.run(self.send_alert(card))

    def format_custom_message(
        self,
        deployment_id: str,
        severity: Severity,
        title: str,
        message: str,
        context: dict[str, Any] | None = None,
    ) -> str:
        """Format a custom alert message.

        This method allows sending custom formatted messages that don't
        come from an OperatorCard.

        Args:
            deployment_id: The deployment ID
            severity: Alert severity level
            title: Alert title
            message: Alert message body
            context: Optional additional context

        Returns:
            Formatted message string
        """
        emoji = SEVERITY_EMOJI.get(severity, "\u2753")

        lines = [
            f"{emoji} <b>{severity.value}: {title}</b>",
            "",
            f"<b>Strategy:</b> {deployment_id}",
            "",
            message,
        ]

        if context:
            lines.append("")
            lines.append("<b>Details:</b>")
            for key, value in context.items():
                formatted_key = key.replace("_", " ").title()
                lines.append(f"  \u2022 {formatted_key}: {value}")

        if self.dashboard_base_url:
            dashboard_link = f"{self.dashboard_base_url}/strategy/{deployment_id}"
            lines.append("")
            lines.append(f'\ud83d\udcca <a href="{dashboard_link}">View in Dashboard</a>')

        return "\n".join(lines)

    async def send_custom_message(
        self,
        deployment_id: str,
        severity: Severity,
        title: str,
        message: str,
        context: dict[str, Any] | None = None,
    ) -> TelegramSendResult:
        """Send a custom formatted message.

        Args:
            deployment_id: The deployment ID
            severity: Alert severity level
            title: Alert title
            message: Alert message body
            context: Optional additional context

        Returns:
            TelegramSendResult indicating success or failure
        """
        formatted = self.format_custom_message(
            deployment_id=deployment_id,
            severity=severity,
            title=title,
            message=message,
            context=context,
        )

        disable_notification = severity == Severity.LOW

        result = await self._send_message(
            text=formatted,
            parse_mode="HTML",
            disable_notification=disable_notification,
        )

        if result.success:
            logger.info(f"Custom Telegram message sent for strategy {deployment_id} (message_id={result.message_id})")
        else:
            logger.error(f"Failed to send custom Telegram message for strategy {deployment_id}: {result.error}")

        return result
