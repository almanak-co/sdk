"""Tests for gateway audit logging module."""

from dataclasses import dataclass
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from almanak.gateway.audit import (
    AuditInterceptor,
    AuditRecord,
    _extract_strategy_id,
    _parse_method_name,
    _summarize_request,
    _summarize_response,
    configure_structlog,
    get_audit_logger,
    log_audit_record,
    reset_structlog_configuration,
)


class TestAuditRecord:
    """Tests for AuditRecord dataclass."""

    def test_to_dict_success(self):
        """Convert successful audit record to dict."""
        record = AuditRecord(
            timestamp="2024-01-01T00:00:00+00:00",
            service="MarketService",
            method="GetPrice",
            strategy_id="test-strategy",
            latency_ms=15.5,
            success=True,
            request_summary={"chain": "arbitrum"},
            response_summary={"success": True},
        )

        d = record.to_dict()

        assert d["timestamp"] == "2024-01-01T00:00:00+00:00"
        assert d["service"] == "MarketService"
        assert d["method"] == "GetPrice"
        assert d["strategy_id"] == "test-strategy"
        assert d["latency_ms"] == 15.5
        assert d["success"] is True
        assert d["error_type"] is None
        assert d["error_message"] is None
        assert d["request"] == {"chain": "arbitrum"}
        assert d["response"] == {"success": True}

    def test_to_dict_failure(self):
        """Convert failed audit record to dict."""
        record = AuditRecord(
            timestamp="2024-01-01T00:00:00+00:00",
            service="RpcService",
            method="Call",
            strategy_id=None,
            latency_ms=50.0,
            success=False,
            error_type="ValidationError",
            error_message="Invalid chain",
        )

        d = record.to_dict()

        assert d["success"] is False
        assert d["error_type"] == "ValidationError"
        assert d["error_message"] == "Invalid chain"
        assert d["strategy_id"] is None

    def test_latency_rounding(self):
        """Latency is rounded to 3 decimal places."""
        record = AuditRecord(
            timestamp="2024-01-01T00:00:00+00:00",
            service="Test",
            method="Test",
            strategy_id=None,
            latency_ms=15.123456789,
            success=True,
        )

        d = record.to_dict()
        assert d["latency_ms"] == 15.123


class TestParseMethodName:
    """Tests for _parse_method_name helper."""

    def test_full_path(self):
        """Parse full gRPC method name."""
        service, method = _parse_method_name("/almanak.gateway.MarketService/GetPrice")
        assert service == "MarketService"
        assert method == "GetPrice"

    def test_simple_path(self):
        """Parse simple method name."""
        service, method = _parse_method_name("/Health/Check")
        assert service == "Health"
        assert method == "Check"

    def test_malformed(self):
        """Handle malformed method names."""
        service, method = _parse_method_name("invalid")
        assert service == "unknown"
        assert method == "unknown"

    def test_empty(self):
        """Handle empty method names."""
        service, method = _parse_method_name("")
        assert service == "unknown"
        assert method == "unknown"


class TestExtractStrategyId:
    """Tests for _extract_strategy_id helper."""

    def test_with_strategy_id_field(self):
        """Extract strategy_id from request with strategy_id field."""

        @dataclass
        class MockRequest:
            strategy_id: str = "my-strategy"

        request = MockRequest()
        assert _extract_strategy_id(request) == "my-strategy"

    def test_with_strategyId_field(self):
        """Extract strategyId from request with camelCase field."""

        @dataclass
        class MockRequest:
            strategyId: str = "my-strategy"

        request = MockRequest()
        assert _extract_strategy_id(request) == "my-strategy"

    def test_no_strategy_id(self):
        """Return None when no strategy_id field."""

        @dataclass
        class MockRequest:
            chain: str = "arbitrum"

        request = MockRequest()
        assert _extract_strategy_id(request) is None

    def test_empty_strategy_id(self):
        """Return None for empty strategy_id."""

        @dataclass
        class MockRequest:
            strategy_id: str = ""

        request = MockRequest()
        assert _extract_strategy_id(request) is None


class TestSummarizeRequest:
    """Tests for _summarize_request helper."""

    def test_extracts_important_fields(self):
        """Extract important fields from request."""

        @dataclass
        class MockRequest:
            chain: str = "arbitrum"
            strategy_id: str = "test"
            method: str = "eth_call"
            wallet_address: str = "0x1234"

        request = MockRequest()
        summary = _summarize_request(request)

        assert "chain" in summary
        assert summary["chain"] == "arbitrum"
        assert "strategy_id" in summary
        assert "method" in summary
        assert "wallet_address" in summary

    def test_truncates_long_values(self):
        """Truncate values longer than 100 chars."""

        @dataclass
        class MockRequest:
            chain: str = "a" * 200

        request = MockRequest()
        summary = _summarize_request(request)

        assert len(summary["chain"]) == 100

    def test_limits_field_count(self):
        """Limit number of fields in summary."""

        @dataclass
        class MockRequest:
            chain: str = "arbitrum"
            strategy_id: str = "test"
            method: str = "eth_call"
            symbol: str = "BTCUSDT"
            token_id: str = "bitcoin"
            subgraph_id: str = "uniswap"
            wallet_address: str = "0x1234"
            address: str = "0x5678"

        request = MockRequest()
        summary = _summarize_request(request, max_fields=3)

        assert len(summary) <= 3

    def test_skips_empty_fields(self):
        """Skip empty field values."""

        @dataclass
        class MockRequest:
            chain: str = ""
            strategy_id: str = "test"

        request = MockRequest()
        summary = _summarize_request(request)

        assert "chain" not in summary
        assert "strategy_id" in summary


class TestSummarizeResponse:
    """Tests for _summarize_response helper."""

    def test_extracts_success_field(self):
        """Extract success field from response."""

        @dataclass
        class MockResponse:
            success: bool = True

        response = MockResponse()
        summary = _summarize_response(response)

        assert summary["success"] is True

    def test_extracts_error_field(self):
        """Extract error indicator from response."""

        @dataclass
        class MockResponse:
            error: str = "Something went wrong"

        response = MockResponse()
        summary = _summarize_response(response)

        assert summary["has_error"] is True

    def test_no_error_field(self):
        """Handle response without error field."""

        @dataclass
        class MockResponse:
            data: str = "result"

        response = MockResponse()
        summary = _summarize_response(response)

        assert "has_error" not in summary


class TestLogAuditRecord:
    """Tests for log_audit_record function."""

    @patch("almanak.gateway.audit.get_audit_logger")
    def test_logs_success(self, mock_get_logger):
        """Log successful request."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        record = AuditRecord(
            timestamp="2024-01-01T00:00:00+00:00",
            service="Test",
            method="Test",
            strategy_id=None,
            latency_ms=10.0,
            success=True,
        )

        log_audit_record(record)

        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        assert call_args[0][0] == "gateway_request"

    @patch("almanak.gateway.audit.get_audit_logger")
    def test_logs_failure(self, mock_get_logger):
        """Log failed request."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        record = AuditRecord(
            timestamp="2024-01-01T00:00:00+00:00",
            service="Test",
            method="Test",
            strategy_id=None,
            latency_ms=10.0,
            success=False,
            error_type="TestError",
        )

        log_audit_record(record)

        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args
        assert call_args[0][0] == "gateway_request_failed"


class TestAuditInterceptor:
    """Tests for AuditInterceptor class."""

    def test_init_default(self):
        """Initialize with default settings."""
        interceptor = AuditInterceptor()
        assert interceptor.enabled is True
        assert interceptor.log_level == "info"

    def test_init_custom(self):
        """Initialize with custom settings."""
        interceptor = AuditInterceptor(enabled=False, log_level="debug")
        assert interceptor.enabled is False
        assert interceptor.log_level == "debug"

    def test_init_log_level_normalization(self):
        """Log level is normalized to lowercase."""
        interceptor = AuditInterceptor(log_level="WARNING")
        assert interceptor.log_level == "warning"


class TestStructlogConfiguration:
    """Tests for structlog configuration functions."""

    def test_configure_structlog_is_idempotent(self):
        """Calling configure_structlog multiple times is safe."""
        # Reset state first to ensure clean test
        reset_structlog_configuration()

        # First call should configure
        configure_structlog()

        # Second call should not raise and should be a no-op
        configure_structlog()

        # Third call should also be fine
        configure_structlog()

    def test_reset_allows_reconfiguration(self):
        """reset_structlog_configuration allows reconfiguration."""
        # Ensure configured
        configure_structlog()

        # Reset
        reset_structlog_configuration()

        # Should be able to configure again
        configure_structlog()

    def test_get_audit_logger_configures_if_needed(self):
        """get_audit_logger configures structlog if not already configured."""
        # Reset to unconfigured state
        reset_structlog_configuration()

        # Getting logger should trigger configuration
        logger = get_audit_logger()

        # Should return a valid logger
        assert logger is not None

    def test_get_audit_logger_returns_same_instance(self):
        """get_audit_logger returns the same logger instance."""
        # Reset to ensure fresh state
        reset_structlog_configuration()

        logger1 = get_audit_logger()
        logger2 = get_audit_logger()

        assert logger1 is logger2
