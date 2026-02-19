"""Tests for the base Service class."""

from datetime import UTC, datetime

import pytest

from almanak.framework.services.base import (
    HealthCheckResult,
    Service,
    ServiceError,
    ServiceStatus,
)


class MockService(Service):
    """Mock service for testing."""

    def __init__(self, service_name: str | None = None):
        super().__init__(service_name=service_name)
        self.start_called = False
        self.stop_called = False
        self.should_fail_start = False

    def start(self) -> None:
        self.start_called = True
        if self.should_fail_start:
            raise ServiceError("Failed to start")

        self._status = ServiceStatus.STARTING
        self._start_time = datetime.now(UTC)
        self._status = ServiceStatus.RUNNING

    def stop(self) -> None:
        self.stop_called = True
        self._status = ServiceStatus.STOPPING
        self._status = ServiceStatus.STOPPED


def test_service_initialization():
    """Test that service initializes in CREATED state."""
    service = MockService()
    assert service.status == ServiceStatus.CREATED
    assert service.is_running is False
    assert service.is_healthy is False
    assert service.service_name == "MockService"


def test_service_custom_name():
    """Test service with custom name."""
    service = MockService(service_name="CustomService")
    assert service.service_name == "CustomService"


def test_service_lifecycle():
    """Test full service lifecycle."""
    service = MockService()

    # Initially created
    assert service.status == ServiceStatus.CREATED
    assert not service.start_called
    assert not service.stop_called

    # Start service
    service.start()
    assert service.start_called
    assert service.status == ServiceStatus.RUNNING
    assert service.is_running

    # Stop service
    service.stop()
    assert service.stop_called
    assert service.status == ServiceStatus.STOPPED
    assert not service.is_running


def test_health_check_not_started():
    """Test health check before service is started."""
    service = MockService()
    health = service.health_check()

    assert not health.healthy
    assert health.status == ServiceStatus.CREATED
    assert health.uptime_seconds is None
    assert health.error_count == 0


def test_health_check_running():
    """Test health check when service is running."""
    service = MockService()
    service.start()

    health = service.health_check()
    assert health.healthy
    assert health.status == ServiceStatus.RUNNING
    assert health.uptime_seconds is not None
    assert health.uptime_seconds >= 0
    assert health.error_count == 0


def test_health_check_stopped():
    """Test health check after service is stopped."""
    service = MockService()
    service.start()
    service.stop()

    health = service.health_check()
    assert not health.healthy
    assert health.status == ServiceStatus.STOPPED


def test_error_recording():
    """Test error recording functionality."""
    service = MockService()
    service.start()

    # Record an error
    service._record_error("Test error")
    assert service._error_count == 1
    assert service._last_error == "Test error"

    # Record another error
    service._record_error(ValueError("Another error"))
    assert service._error_count == 2
    assert service._last_error == "Another error"

    # Health check should reflect errors
    health = service.health_check()
    assert health.error_count == 2
    assert health.last_error == "Another error"


def test_error_reset():
    """Test resetting error count."""
    service = MockService()
    service.start()

    service._record_error("Error 1")
    service._record_error("Error 2")
    assert service._error_count == 2

    service._reset_error_count()
    assert service._error_count == 0
    assert service._last_error is None


def test_get_metrics():
    """Test getting service metrics."""
    service = MockService(service_name="TestService")
    service.start()

    metrics = service.get_metrics()
    assert metrics["service_name"] == "TestService"
    assert metrics["status"] == "RUNNING"
    assert metrics["error_count"] == 0
    assert metrics["uptime_seconds"] >= 0
    assert metrics["is_healthy"] is True


def test_reconfigure_not_implemented():
    """Test that reconfigure raises NotImplementedError by default."""
    service = MockService()
    service.start()

    with pytest.raises(NotImplementedError, match="does not support runtime reconfiguration"):
        service.reconfigure({"key": "value"})


def test_is_healthy_with_errors():
    """Test that is_healthy returns False when there are errors."""
    service = MockService()
    service.start()

    assert service.is_healthy is True

    service._record_error("Test error")
    assert service.is_healthy is False


def test_health_check_result_to_dict():
    """Test HealthCheckResult serialization."""
    result = HealthCheckResult(
        healthy=True,
        status=ServiceStatus.RUNNING,
        message="Service is running",
        timestamp=datetime.now(UTC),
        details={"database": "connected"},
        uptime_seconds=123.45,
        last_error=None,
        error_count=0,
    )

    data = result.to_dict()
    assert data["healthy"] is True
    assert data["status"] == "RUNNING"
    assert data["message"] == "Service is running"
    assert "timestamp" in data
    assert data["details"] == {"database": "connected"}
    assert data["uptime_seconds"] == 123.45
    assert data["error_count"] == 0


def test_service_properties():
    """Test service property accessors."""
    service = MockService()

    # Test status property
    assert service.status == ServiceStatus.CREATED

    # Test is_running property
    assert service.is_running is False

    service.start()
    assert service.is_running is True
    assert service.status == ServiceStatus.RUNNING

    service.stop()
    assert service.is_running is False
    assert service.status == ServiceStatus.STOPPED
