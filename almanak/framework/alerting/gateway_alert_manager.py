"""Gateway-backed AlertManager implementation.

This module provides an AlertManager that sends alerts through the gateway
sidecar instead of directly accessing Slack/Telegram APIs. Used in strategy
containers that have no access to webhook URLs or bot tokens.
"""

import asyncio
import logging
import time
from dataclasses import dataclass

from almanak.framework.gateway_client import GatewayClient
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)


@dataclass
class GatewayAlertResult:
    """Result from gateway alert."""

    success: bool
    alert_id: str
    error: str | None = None


class GatewayAlertManager:
    """AlertManager that sends alerts through the gateway.

    This implementation routes all alert requests to the gateway sidecar,
    which has access to the actual alerting channels (Slack, Telegram).

    Example:
        from almanak.framework.gateway_client import GatewayClient
        from almanak.framework.alerting.gateway_alert_manager import GatewayAlertManager

        with GatewayClient() as client:
            alert_manager = GatewayAlertManager(client, strategy_id="my-strategy")
            result = await alert_manager.send_alert(
                message="Strategy executed successfully",
                severity="info",
            )
            print(f"Alert sent: {result.success}")
    """

    def __init__(
        self,
        client: GatewayClient,
        strategy_id: str = "",
        timeout: float = 30.0,
    ):
        """Initialize gateway-backed alert manager.

        Args:
            client: Connected GatewayClient instance
            strategy_id: Strategy identifier for alert context
            timeout: RPC timeout in seconds
        """
        self._client = client
        self._strategy_id = strategy_id
        self._timeout = timeout

    @property
    def strategy_id(self) -> str:
        """Get the strategy ID."""
        return self._strategy_id

    async def send_alert(
        self,
        message: str,
        severity: str = "info",
        channel: str = "slack",
        metadata: dict[str, str] | None = None,
    ) -> GatewayAlertResult:
        """Send an alert through the gateway.

        Args:
            message: Alert message text
            severity: Alert severity ("info", "warning", "critical")
            channel: Alert channel ("slack", "telegram")
            metadata: Additional metadata to include

        Returns:
            GatewayAlertResult with success status
        """
        try:
            request = gateway_pb2.AlertRequest(
                channel=channel,
                message=message,
                severity=severity,
                strategy_id=self._strategy_id,
                metadata=metadata or {},
            )

            # Run synchronous gRPC call in executor to avoid blocking event loop
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self._client.observe.Alert(request, timeout=self._timeout),
            )

            return GatewayAlertResult(
                success=response.success,
                alert_id=response.alert_id,
                error=response.error if response.error else None,
            )

        except Exception as e:
            logger.error(f"Gateway alert failed: {e}")
            return GatewayAlertResult(
                success=False,
                alert_id="",
                error=str(e),
            )

    async def log(
        self,
        message: str,
        level: str = "INFO",
        context: dict[str, str] | None = None,
        logger_name: str = "",
    ) -> None:
        """Send a log message through the gateway.

        Args:
            message: Log message text
            level: Log level ("DEBUG", "INFO", "WARNING", "ERROR")
            context: Additional context to include
            logger_name: Optional logger name for categorization
        """
        try:
            request = gateway_pb2.LogEntry(
                level=level.upper(),
                message=message,
                strategy_id=self._strategy_id,
                context=context or {},
                timestamp=int(time.time()),
                logger_name=logger_name,
            )

            # Run synchronous gRPC call in executor to avoid blocking event loop
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: self._client.observe.Log(request, timeout=self._timeout),
            )

        except Exception as e:
            # Don't fail on logging errors - just log locally
            logger.warning(f"Gateway log failed (falling back to local): {e}")
            local_logger = logging.getLogger(logger_name or "strategy")
            log_levels = {
                "DEBUG": logging.DEBUG,
                "INFO": logging.INFO,
                "WARNING": logging.WARNING,
                "ERROR": logging.ERROR,
            }
            local_logger.log(log_levels.get(level.upper(), logging.INFO), message)

    async def record_metric(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None = None,
        metric_type: str = "gauge",
    ) -> None:
        """Record a metric through the gateway.

        Args:
            name: Metric name
            value: Metric value
            labels: Metric labels/tags
            metric_type: Type of metric ("gauge", "counter", "histogram")
        """
        try:
            request = gateway_pb2.MetricEntry(
                name=name,
                value=value,
                labels=labels or {},
                timestamp=int(time.time()),
                metric_type=metric_type,
            )

            # Run synchronous gRPC call in executor to avoid blocking event loop
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: self._client.observe.RecordMetric(request, timeout=self._timeout),
            )

        except Exception as e:
            # Don't fail on metrics errors
            logger.debug(f"Gateway metric recording failed: {e}")
