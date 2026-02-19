"""ObserveService implementation - handles logging, alerting, metrics, and timeline events.

This service provides observability capabilities for strategy containers via gRPC.
All external integrations (Slack, Telegram, metrics backends) are accessed here
in the gateway; strategy containers only send log/alert/metric/timeline requests.

Alerting channels (Slack, Telegram) are called directly from this service using
credentials from gateway settings - no external calls are made from strategy containers.
"""

import html
import json
import logging
import time
from datetime import UTC, datetime
from uuid import uuid4

import aiohttp
import grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.timeline.store import TimelineEvent, get_timeline_store

logger = logging.getLogger(__name__)

# Severity to Slack color mapping
SLACK_SEVERITY_COLORS = {
    "info": "#36a64f",  # green
    "warning": "#ffcc00",  # yellow
    "critical": "#ff0000",  # red
}

# Severity to emoji mapping
SLACK_SEVERITY_EMOJIS = {
    "info": ":information_source:",
    "warning": ":warning:",
    "critical": ":red_circle:",
}


def escape_mrkdwn(text: str) -> str:
    """Escape special characters for Slack mrkdwn format.

    Slack mrkdwn uses <, >, & as special characters that need escaping
    to prevent formatting issues or injection.

    Args:
        text: User-provided text to escape

    Returns:
        Escaped text safe for mrkdwn
    """
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class ObserveServiceServicer(gateway_pb2_grpc.ObserveServiceServicer):
    """Implements ObserveService gRPC interface.

    Provides observability for strategy containers:
    - Log: Forward log messages to platform logging
    - Alert: Send alerts via configured channels (Slack, Telegram)
    - RecordMetric: Record metrics for monitoring

    All external API calls (Slack webhook, Telegram API) are made directly
    from this service using credentials from gateway settings.
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize ObserveService.

        Args:
            settings: Gateway settings with alerting configuration.
        """
        self.settings = settings
        self._http_session: aiohttp.ClientSession | None = None
        self._metrics_buffer: list[dict] = []

        # Check which channels are configured
        self._slack_available = bool(settings.slack_webhook_url)
        self._telegram_available = bool(settings.telegram_bot_token and settings.telegram_chat_id)

        logger.info(
            "ObserveService initialized: slack=%s, telegram=%s",
            self._slack_available,
            self._telegram_available,
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30.0))
        return self._http_session

    async def close(self) -> None:
        """Close HTTP session."""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None

    async def Log(
        self,
        request: gateway_pb2.LogEntry,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.Empty:
        """Log a message from the strategy.

        Args:
            request: Log entry with level, message, context
            context: gRPC context

        Returns:
            Empty response
        """
        level = request.level.upper()
        message = request.message
        strategy_id = request.strategy_id
        log_context = dict(request.context)
        logger_name = request.logger_name or "strategy"

        # Get appropriate logger
        strategy_logger = logging.getLogger(f"almanak.strategy.{strategy_id}.{logger_name}")

        # Add context as extra
        extra = {
            "strategy_id": strategy_id,
            **log_context,
        }

        # Map level and log
        log_levels = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "WARN": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }

        log_level = log_levels.get(level, logging.INFO)
        strategy_logger.log(log_level, message, extra=extra)

        return gateway_pb2.Empty()

    async def _send_slack_alert(
        self,
        message: str,
        severity: str,
        strategy_id: str,
        metadata: dict,
    ) -> tuple[bool, str | None]:
        """Send alert to Slack webhook.

        Returns:
            Tuple of (success, error_message)
        """
        if not self._slack_available:
            return False, "Slack webhook not configured"

        emoji = SLACK_SEVERITY_EMOJIS.get(severity, ":question:")
        color = SLACK_SEVERITY_COLORS.get(severity, "#808080")

        # Escape user-provided content for Slack mrkdwn format
        escaped_strategy_id = escape_mrkdwn(strategy_id)
        escaped_message = escape_mrkdwn(message)

        # Build Slack Block Kit payload
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} {severity.upper()} Alert",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Strategy:*\n{escaped_strategy_id}"},
                    {"type": "mrkdwn", "text": f"*Severity:*\n{severity.upper()}"},
                ],
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Message:*\n{escaped_message}"},
            },
        ]

        # Add metadata if present
        if metadata:
            context_lines = [f"- {escape_mrkdwn(str(k))}: {escape_mrkdwn(str(v))}" for k, v in metadata.items()]
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Context:*\n{chr(10).join(context_lines)}"},
                }
            )

        # Use escaped values in fallback text as well
        fallback_text = f"{severity.upper()}: {escaped_strategy_id} - {escaped_message}"
        payload = {
            "blocks": blocks,
            "attachments": [{"color": color, "fallback": fallback_text}],
        }

        webhook_url = (self.settings.slack_webhook_url or "").strip()
        if not webhook_url:
            logger.warning("Slack webhook URL is empty after stripping; skipping alert")
            return False, "Slack webhook URL is empty"

        try:
            session = await self._get_session()
            async with session.post(
                webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as response:
                if response.status == 200:
                    return True, None
                else:
                    error_text = await response.text()
                    return False, f"Slack API error: HTTP {response.status}: {error_text[:200]}"
        except (TimeoutError, aiohttp.ClientError) as e:
            return False, f"Slack request failed: {e}"

    async def _send_telegram_alert(
        self,
        message: str,
        severity: str,
        strategy_id: str,
        metadata: dict,
    ) -> tuple[bool, str | None]:
        """Send alert to Telegram.

        Returns:
            Tuple of (success, error_message)
        """
        if not self._telegram_available:
            return False, "Telegram not configured"

        # Build Telegram message with HTML formatting
        # Escape user-provided content to prevent HTML injection
        severity_emoji = {"info": "ℹ️", "warning": "⚠️", "critical": "🔴"}.get(severity, "❓")  # noqa: RUF001
        escaped_strategy_id = html.escape(strategy_id)
        escaped_message = html.escape(message)

        text_lines = [
            f"{severity_emoji} <b>{severity.upper()} Alert</b>",
            f"<b>Strategy:</b> <code>{escaped_strategy_id}</code>",
            "",
            escaped_message,
        ]

        if metadata:
            text_lines.append("")
            text_lines.append("<b>Context:</b>")
            for k, v in metadata.items():
                text_lines.append(f"- {html.escape(str(k))}: {html.escape(str(v))}")

        text = "\n".join(text_lines)

        url = f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/sendMessage"
        payload = {
            "chat_id": self.settings.telegram_chat_id,
            "text": text,
            "parse_mode": "HTML",
        }

        try:
            session = await self._get_session()
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    return True, None
                else:
                    error_text = await response.text()
                    return False, f"Telegram API error: HTTP {response.status}: {error_text[:200]}"
        except (TimeoutError, aiohttp.ClientError) as e:
            return False, f"Telegram request failed: {e}"

    async def Alert(
        self,
        request: gateway_pb2.AlertRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.AlertResponse:
        """Send an alert through configured channels.

        Makes direct HTTP calls to Slack/Telegram APIs using credentials
        from gateway settings. No external calls are made from strategy containers.

        Args:
            request: Alert request with channel, message, severity
            context: gRPC context

        Returns:
            AlertResponse with success status
        """
        channel = request.channel.lower()
        message = request.message
        severity = request.severity.lower()
        strategy_id = request.strategy_id
        metadata = dict(request.metadata)

        if not message:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("message is required")
            return gateway_pb2.AlertResponse(success=False, error="message required")

        alert_id = f"alert-{uuid4()}"

        try:
            # Route to appropriate channel
            if channel == "slack":
                success, error = await self._send_slack_alert(message, severity, strategy_id, metadata)
            elif channel == "telegram":
                success, error = await self._send_telegram_alert(message, severity, strategy_id, metadata)
            else:
                # Try all configured channels
                errors = []

                if self._slack_available:
                    slack_success, slack_error = await self._send_slack_alert(message, severity, strategy_id, metadata)
                    if not slack_success:
                        errors.append(f"Slack: {slack_error}")

                if self._telegram_available:
                    tg_success, tg_error = await self._send_telegram_alert(message, severity, strategy_id, metadata)
                    if not tg_success:
                        errors.append(f"Telegram: {tg_error}")

                # If no channels configured, fall back to logging
                if not self._slack_available and not self._telegram_available:
                    logger.warning(f"Alert [{severity}] from {strategy_id}: {message}")
                    return gateway_pb2.AlertResponse(success=True, alert_id=alert_id)

                success = len(errors) == 0
                error = "; ".join(errors) if errors else None

            if success:
                logger.info(f"Alert sent via {channel}: strategy={strategy_id}, severity={severity}")
            else:
                logger.warning(f"Alert failed via {channel}: {error}")

            return gateway_pb2.AlertResponse(
                success=success,
                error=error or "",
                alert_id=alert_id,
            )

        except Exception as e:
            error_msg = str(e)
            logger.exception("Alert failed: %s", error_msg)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_msg)
            return gateway_pb2.AlertResponse(success=False, error=error_msg)

    async def RecordMetric(
        self,
        request: gateway_pb2.MetricEntry,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.Empty:
        """Record a metric value.

        Args:
            request: Metric entry with name, value, labels
            context: gRPC context

        Returns:
            Empty response
        """
        name = request.name
        value = request.value
        labels = dict(request.labels)
        metric_type = request.metric_type or "gauge"
        timestamp = request.timestamp or int(time.time())

        if not name:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("name is required")
            return gateway_pb2.Empty()

        # Store metric in buffer for batch export
        metric = {
            "name": name,
            "value": value,
            "labels": labels,
            "type": metric_type,
            "timestamp": timestamp,
        }

        self._metrics_buffer.append(metric)

        # Log metric for debugging
        logger.debug(f"Metric recorded: {name}={value} labels={labels}")

        # TODO: In production, export to Prometheus/OpenTelemetry
        # For now, just buffer and log

        # Flush buffer if it gets too large
        if len(self._metrics_buffer) > 1000:
            self._flush_metrics()

        return gateway_pb2.Empty()

    def _flush_metrics(self) -> None:
        """Flush metrics buffer to backend.

        In production, this would export to Prometheus, OpenTelemetry, etc.
        For now, just clear the buffer.
        """
        if self._metrics_buffer:
            logger.info(f"Flushing {len(self._metrics_buffer)} metrics")
            # TODO: Export to metrics backend
            self._metrics_buffer.clear()

    async def RecordTimelineEvent(
        self,
        request: gateway_pb2.RecordTimelineEventRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.RecordTimelineEventResponse:
        """Record a timeline event for a strategy.

        Args:
            request: Timeline event request
            context: gRPC context

        Returns:
            RecordTimelineEventResponse with success status and event ID
        """
        strategy_id = request.strategy_id
        event_type = request.event_type
        description = request.description

        if not strategy_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("strategy_id is required")
            return gateway_pb2.RecordTimelineEventResponse(
                success=False,
                error="strategy_id is required",
            )

        if not event_type:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("event_type is required")
            return gateway_pb2.RecordTimelineEventResponse(
                success=False,
                error="event_type is required",
            )

        # Parse details JSON if provided
        details = {}
        if request.details_json:
            try:
                details = json.loads(request.details_json)
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid details_json: {e}")

        # Use provided timestamp or current time
        if request.timestamp > 0:
            timestamp = datetime.fromtimestamp(request.timestamp, tz=UTC)
        else:
            timestamp = datetime.now(UTC)

        # Create event
        event_id = str(uuid4())
        event = TimelineEvent(
            event_id=event_id,
            strategy_id=strategy_id,
            timestamp=timestamp,
            event_type=event_type,
            description=description,
            tx_hash=request.tx_hash if request.tx_hash else None,
            chain=request.chain if request.chain else None,
            details=details,
        )

        try:
            # Store event
            store = get_timeline_store()
            store.add_event(event)

            logger.info(f"Recorded timeline event: {event_type} for {strategy_id}")

            return gateway_pb2.RecordTimelineEventResponse(
                success=True,
                event_id=event_id,
            )

        except Exception as e:
            error_msg = f"Failed to record timeline event: {e}"
            logger.error(error_msg)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_msg)
            return gateway_pb2.RecordTimelineEventResponse(
                success=False,
                error=error_msg,
            )
