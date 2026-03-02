"""Base Service Classes for Framework Services.

This module provides base classes and interfaces for framework services,
enabling consistent lifecycle management, health checking, and monitoring.

ARCHITECTURE
============

Services in the framework follow a standard lifecycle pattern:
    1. CREATED - Service instantiated but not started
    2. STARTING - Service is starting up
    3. RUNNING - Service is operational
    4. DEGRADED - Service running but with issues
    5. STOPPING - Service is shutting down
    6. STOPPED - Service cleanly stopped
    7. FAILED - Service failed and cannot operate

Not all services need full lifecycle management. Services are categorized:
    - Stateless Services: Pure functions, no base class needed
    - Stateful Services: Inherit from Service, implement start/stop
    - Orchestrator Services: Coordinate other services

USAGE
=====

For stateful services with lifecycle management:

    from almanak.framework.services.base import Service, ServiceStatus

    class MyService(Service):
        def __init__(self, config):
            super().__init__(service_name="MyService")
            self.config = config

        def start(self) -> None:
            self._status = ServiceStatus.STARTING
            # Initialize resources
            self._status = ServiceStatus.RUNNING
            self._start_time = datetime.now(UTC)

        def stop(self) -> None:
            self._status = ServiceStatus.STOPPING
            # Cleanup resources
            self._status = ServiceStatus.STOPPED

        def health_check(self) -> HealthCheckResult:
            base_health = super().health_check()
            # Add custom health checks
            return base_health

For stateless services (pure functions):

    class StatelessService:
        # No base class needed, just implement methods
        def process(self, data):
            return result

BENEFITS
========

- Consistent lifecycle management across all services
- Standard health checking interface
- Built-in metrics and monitoring support
- Easier testing with predictable start/stop behavior
- Graceful degradation and error handling

See Also:
    - almanak.framework.services module documentation
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================


class ServiceStatus(StrEnum):
    """Standard service status states.

    Lifecycle progression:
        CREATED -> STARTING -> RUNNING -> STOPPING -> STOPPED
                      |           |
                      v           v
                   FAILED      DEGRADED -> STOPPING -> STOPPED
    """

    CREATED = "CREATED"  # Service instantiated but not started
    STARTING = "STARTING"  # Service is starting up
    RUNNING = "RUNNING"  # Service is operational
    DEGRADED = "DEGRADED"  # Service running but with issues
    STOPPING = "STOPPING"  # Service is shutting down
    STOPPED = "STOPPED"  # Service cleanly stopped
    FAILED = "FAILED"  # Service failed and cannot operate


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class HealthCheckResult:
    """Result of a service health check.

    Attributes:
        healthy: Whether service is healthy
        status: Current service status
        message: Human-readable status message
        timestamp: When health check was performed
        details: Optional additional health details
        uptime_seconds: Service uptime in seconds (if started)
        last_error: Last error message (if any)
        error_count: Number of errors encountered
    """

    healthy: bool
    status: ServiceStatus
    message: str
    timestamp: datetime
    details: dict[str, Any] | None = None
    uptime_seconds: float | None = None
    last_error: str | None = None
    error_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "healthy": self.healthy,
            "status": self.status.value,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details,
            "uptime_seconds": self.uptime_seconds,
            "last_error": self.last_error,
            "error_count": self.error_count,
        }


# =============================================================================
# Base Service Class
# =============================================================================


class Service(ABC):
    """Base class for framework services.

    All stateful services should inherit from this class and implement
    the required lifecycle methods.

    Lifecycle:
        1. __init__() - Create service (CREATED state)
        2. start() - Initialize resources (STARTING -> RUNNING)
        3. [service operates]
        4. stop() - Clean shutdown (STOPPING -> STOPPED)

    Services can also implement:
        - health_check() - Report service health
        - reconfigure() - Update configuration at runtime
        - get_metrics() - Export service metrics

    Example:
        class MyService(Service):
            def __init__(self, config):
                super().__init__(service_name="MyService")
                self.config = config

            def start(self) -> None:
                self._status = ServiceStatus.STARTING
                # Initialize resources
                self._status = ServiceStatus.RUNNING
                self._start_time = datetime.now(UTC)

            def stop(self) -> None:
                self._status = ServiceStatus.STOPPING
                # Cleanup resources
                self._status = ServiceStatus.STOPPED

    Thread Safety:
        Base class is NOT thread-safe. Subclasses should implement
        their own synchronization if needed.
    """

    def __init__(self, service_name: str | None = None) -> None:
        """Initialize the service.

        Args:
            service_name: Optional service name for logging/metrics.
                Defaults to class name.
        """
        self.service_name = service_name or self.__class__.__name__
        self._status = ServiceStatus.CREATED
        self._start_time: datetime | None = None
        self._error_count = 0
        self._last_error: str | None = None

        logger.debug("Service created", extra={"service": self.service_name})

    @abstractmethod
    def start(self) -> None:
        """Start the service and initialize resources.

        This method should:
        - Validate configuration
        - Initialize connections (DB, RPC, etc.)
        - Start background tasks if needed
        - Transition to RUNNING state

        Raises:
            ServiceError: If service fails to start
        """
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stop the service and clean up resources.

        This method should:
        - Stop background tasks gracefully
        - Close connections
        - Flush any pending operations
        - Transition to STOPPED state

        This method should not raise exceptions - log errors instead.
        """
        pass

    def health_check(self) -> HealthCheckResult:
        """Check service health and return status.

        Default implementation returns basic status.
        Services should override to add specific health checks.

        Returns:
            HealthCheckResult with current health status

        Example:
            def health_check(self) -> HealthCheckResult:
                base_health = super().health_check()

                # Add custom checks
                if self._check_database_connection():
                    base_health.details = {"database": "connected"}
                else:
                    base_health.healthy = False
                    base_health.message = "Database connection failed"

                return base_health
        """
        now = datetime.now(UTC)
        uptime = None
        if self._start_time:
            uptime = (now - self._start_time).total_seconds()

        healthy = self._status == ServiceStatus.RUNNING
        message = f"{self.service_name} is {self._status.value}"

        return HealthCheckResult(
            healthy=healthy,
            status=self._status,
            message=message,
            timestamp=now,
            uptime_seconds=uptime,
            last_error=self._last_error,
            error_count=self._error_count,
        )

    @property
    def status(self) -> ServiceStatus:
        """Get current service status."""
        return self._status

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._status == ServiceStatus.RUNNING

    @property
    def is_healthy(self) -> bool:
        """Check if service is healthy (running without issues)."""
        return self._status == ServiceStatus.RUNNING and self._error_count == 0

    def reconfigure(self, config: dict[str, Any]) -> None:
        """Reconfigure the service at runtime.

        Optional method that services can override to support
        runtime configuration updates.

        Args:
            config: New configuration parameters

        Raises:
            NotImplementedError: If service doesn't support reconfiguration
        """
        raise NotImplementedError(f"{self.service_name} does not support runtime reconfiguration")

    def get_metrics(self) -> dict[str, Any]:
        """Get service metrics for monitoring.

        Optional method that services can override to export
        custom metrics.

        Returns:
            Dictionary of metric name -> value

        Example:
            def get_metrics(self) -> dict[str, Any]:
                metrics = super().get_metrics()
                metrics["requests_processed"] = self.request_count
                metrics["avg_latency_ms"] = self.avg_latency
                return metrics
        """
        uptime = 0.0
        if self._start_time:
            uptime = (datetime.now(UTC) - self._start_time).total_seconds()

        return {
            "service_name": self.service_name,
            "status": self._status.value,
            "error_count": self._error_count,
            "uptime_seconds": uptime,
            "is_healthy": self.is_healthy,
        }

    def _record_error(self, error: str | Exception) -> None:
        """Record an error in service metrics.

        Helper method for subclasses to track errors.

        Args:
            error: Error message or exception
        """
        self._error_count += 1
        self._last_error = str(error)
        logger.error(
            "Service error recorded",
            extra={
                "service": self.service_name,
                "error": self._last_error,
                "error_count": self._error_count,
            },
        )

    def _reset_error_count(self) -> None:
        """Reset error count.

        Helper method for subclasses to reset error tracking
        after successful recovery.
        """
        self._error_count = 0
        self._last_error = None


# =============================================================================
# Exceptions
# =============================================================================


class ServiceError(Exception):
    """Base exception for service errors."""

    pass


class ServiceNotRunningError(ServiceError):
    """Raised when operation requires service to be running but it's not."""

    pass


class ServiceStartError(ServiceError):
    """Raised when service fails to start."""

    pass


class ServiceStopError(ServiceError):
    """Raised when service fails to stop gracefully."""

    pass
