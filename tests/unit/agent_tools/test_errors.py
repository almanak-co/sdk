"""Tests for agent tool error taxonomy."""

from almanak.framework.agent_tools.errors import (
    ExecutionFailedError,
    PermissionDeniedError,
    RiskBlockedError,
    SimulationFailedError,
    ToolError,
    ToolTimeoutError,
    ToolValidationError,
    UpstreamUnavailableError,
)


class TestToolError:
    def test_base_error_fields(self):
        e = ToolError("test_code", "test message", recoverable=True, suggestion="try again")
        assert e.code == "test_code"
        assert e.message == "test message"
        assert e.recoverable is True
        assert e.suggestion == "try again"
        assert e.tool_name is None
        assert "[test_code]" in str(e)

    def test_to_dict(self):
        e = ToolError("code", "msg", recoverable=False, suggestion="fix it", tool_name="get_price")
        d = e.to_dict()
        assert d["error_code"] == "code"
        assert d["message"] == "msg"
        assert d["recoverable"] is False
        assert d["suggestion"] == "fix it"
        assert d["tool_name"] == "get_price"

    def test_to_dict_minimal(self):
        e = ToolError("code", "msg")
        d = e.to_dict()
        assert "suggestion" not in d
        assert "tool_name" not in d


class TestSpecificErrors:
    def test_validation_error(self):
        e = ToolValidationError("bad input")
        assert e.code == "validation_error"
        assert e.recoverable is True

    def test_risk_blocked(self):
        e = RiskBlockedError("limit exceeded")
        assert e.code == "risk_blocked"
        assert e.recoverable is False

    def test_simulation_failed(self):
        e = SimulationFailedError("revert")
        assert e.code == "simulation_failed"
        assert e.recoverable is True

    def test_timeout(self):
        e = ToolTimeoutError("gateway timeout")
        assert e.code == "timeout"
        assert e.recoverable is True

    def test_upstream_unavailable(self):
        e = UpstreamUnavailableError("rpc down")
        assert e.code == "upstream_unavailable"
        assert e.recoverable is True

    def test_permission_denied(self):
        e = PermissionDeniedError("not allowed")
        assert e.code == "permission_denied"
        assert e.recoverable is False

    def test_execution_failed(self):
        e = ExecutionFailedError("reverted")
        assert e.code == "execution_failed"
        assert e.recoverable is False

    def test_all_are_tool_errors(self):
        for cls in [
            ToolValidationError,
            RiskBlockedError,
            SimulationFailedError,
            ToolTimeoutError,
            UpstreamUnavailableError,
            PermissionDeniedError,
            ExecutionFailedError,
        ]:
            assert issubclass(cls, ToolError)
