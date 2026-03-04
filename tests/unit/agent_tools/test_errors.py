"""Tests for agent tool error taxonomy."""

from almanak.framework.agent_tools.errors import (
    ERROR_CATEGORIES,
    AgentErrorCode,
    ErrorCategory,
    ExecutionFailedError,
    PermissionDeniedError,
    RiskBlockedError,
    SimulationFailedError,
    ToolError,
    ToolTimeoutError,
    ToolValidationError,
    UpstreamUnavailableError,
    get_error_category,
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

    def test_to_dict_includes_error_category(self):
        """to_dict() must include error_category for agent decision-making."""
        e = ToolError(AgentErrorCode.GATEWAY_ERROR, "gateway down", recoverable=True)
        d = e.to_dict()
        assert d["error_category"] == "retryable"

    def test_to_dict_unknown_code_defaults_to_non_retryable(self):
        """Unknown error codes get non_retryable category as safe default."""
        e = ToolError("some_unknown_code", "mystery error")
        d = e.to_dict()
        assert d["error_category"] == "non_retryable"


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

    def test_typed_errors_use_enum_codes(self):
        """All typed error classes should use AgentErrorCode enum values."""
        cases = [
            (ToolValidationError("x"), AgentErrorCode.VALIDATION_ERROR),
            (RiskBlockedError("x"), AgentErrorCode.RISK_BLOCKED),
            (SimulationFailedError("x"), AgentErrorCode.SIMULATION_FAILED),
            (ToolTimeoutError("x"), AgentErrorCode.TIMEOUT),
            (UpstreamUnavailableError("x"), AgentErrorCode.UPSTREAM_UNAVAILABLE),
            (PermissionDeniedError("x"), AgentErrorCode.PERMISSION_DENIED),
            (ExecutionFailedError("x"), AgentErrorCode.EXECUTION_FAILED),
        ]
        for error, expected_code in cases:
            # The code attribute should be the enum member (or its string value)
            assert error.code == expected_code.value, (
                f"{type(error).__name__}.code = {error.code!r}, expected {expected_code.value!r}"
            )

    def test_typed_errors_include_category_in_dict(self):
        """All typed errors should include error_category in to_dict()."""
        errors = [
            ToolValidationError("x"),
            RiskBlockedError("x"),
            SimulationFailedError("x"),
            ToolTimeoutError("x"),
            UpstreamUnavailableError("x"),
            PermissionDeniedError("x"),
            ExecutionFailedError("x"),
        ]
        for e in errors:
            d = e.to_dict()
            assert "error_category" in d, f"{type(e).__name__}.to_dict() missing error_category"
            # Verify the category is a valid ErrorCategory value
            assert d["error_category"] in [c.value for c in ErrorCategory], (
                f"Invalid error_category '{d['error_category']}' for {type(e).__name__}"
            )


class TestAgentErrorCode:
    def test_enum_values_are_strings(self):
        """All AgentErrorCode values must be plain strings for wire format."""
        for code in AgentErrorCode:
            assert isinstance(code.value, str)
            assert code.value == str(code)

    def test_no_duplicate_values(self):
        """Every AgentErrorCode must have a unique string value."""
        values = [code.value for code in AgentErrorCode]
        assert len(values) == len(set(values)), f"Duplicate values found: {values}"

    def test_expected_codes_exist(self):
        """Verify all documented error codes are in the enum."""
        expected = [
            "validation_error",
            "invalid_intent_type",
            "risk_blocked",
            "permission_denied",
            "execution_failed",
            "compilation_failed",
            "simulation_failed",
            "gateway_error",
            "rpc_failed",
            "timeout",
            "upstream_unavailable",
            "internal_error",
            "state_load_failed",
            "record_failed",
            "all_queries_failed",
            "empty_pool",
            "invalid_position",
            "unsupported_chain",
            "vault_read_failed",
            "vault_verification_failed",
            "preflight_failed",
            "insufficient_liquidity",
            "teardown_missing_sub_tools",
            "teardown_lp_close_failed",
            "not_implemented",
        ]
        enum_values = {code.value for code in AgentErrorCode}
        for expected_code in expected:
            assert expected_code in enum_values, f"Missing enum for error code: {expected_code}"


class TestErrorCategory:
    def test_enum_values_are_strings(self):
        for cat in ErrorCategory:
            assert isinstance(cat.value, str)

    def test_expected_categories_exist(self):
        expected = ["retryable", "non_retryable", "policy_violation", "requires_human", "configuration"]
        cat_values = {c.value for c in ErrorCategory}
        for expected_cat in expected:
            assert expected_cat in cat_values, f"Missing category: {expected_cat}"


class TestErrorCategories:
    def test_every_error_code_has_category(self):
        """Every AgentErrorCode must have an entry in ERROR_CATEGORIES."""
        for code in AgentErrorCode:
            assert code in ERROR_CATEGORIES, f"AgentErrorCode.{code.name} missing from ERROR_CATEGORIES"

    def test_retryable_codes(self):
        """Transient error codes should be categorised as retryable."""
        retryable_codes = [
            AgentErrorCode.GATEWAY_ERROR,
            AgentErrorCode.RPC_FAILED,
            AgentErrorCode.TIMEOUT,
            AgentErrorCode.UPSTREAM_UNAVAILABLE,
            AgentErrorCode.STATE_LOAD_FAILED,
            AgentErrorCode.ALL_QUERIES_FAILED,
            AgentErrorCode.SIMULATION_FAILED,
        ]
        for code in retryable_codes:
            assert get_error_category(code) == ErrorCategory.RETRYABLE, (
                f"{code.name} should be RETRYABLE but got {get_error_category(code)}"
            )

    def test_non_retryable_codes(self):
        """Deterministic failure codes should be non-retryable."""
        non_retryable_codes = [
            AgentErrorCode.VALIDATION_ERROR,
            AgentErrorCode.EXECUTION_FAILED,
            AgentErrorCode.EMPTY_POOL,
            AgentErrorCode.INVALID_POSITION,
            AgentErrorCode.NOT_IMPLEMENTED,
            AgentErrorCode.INTERNAL_ERROR,
        ]
        for code in non_retryable_codes:
            assert get_error_category(code) == ErrorCategory.NON_RETRYABLE, (
                f"{code.name} should be NON_RETRYABLE but got {get_error_category(code)}"
            )

    def test_policy_violation_codes(self):
        """Policy rejection codes should be categorised as policy_violation."""
        policy_codes = [
            AgentErrorCode.RISK_BLOCKED,
            AgentErrorCode.PERMISSION_DENIED,
        ]
        for code in policy_codes:
            assert get_error_category(code) == ErrorCategory.POLICY_VIOLATION, (
                f"{code.name} should be POLICY_VIOLATION but got {get_error_category(code)}"
            )

    def test_configuration_codes(self):
        """Setup/config error codes should be categorised as configuration."""
        config_codes = [
            AgentErrorCode.UNSUPPORTED_CHAIN,
            AgentErrorCode.TEARDOWN_MISSING_SUB_TOOLS,
            AgentErrorCode.PREFLIGHT_FAILED,
        ]
        for code in config_codes:
            assert get_error_category(code) == ErrorCategory.CONFIGURATION, (
                f"{code.name} should be CONFIGURATION but got {get_error_category(code)}"
            )

    def test_get_error_category_from_string(self):
        """get_error_category should accept string values (wire format)."""
        assert get_error_category("gateway_error") == ErrorCategory.RETRYABLE
        assert get_error_category("risk_blocked") == ErrorCategory.POLICY_VIOLATION
        assert get_error_category("validation_error") == ErrorCategory.NON_RETRYABLE

    def test_get_error_category_unknown_string(self):
        """Unknown string codes should default to non_retryable."""
        assert get_error_category("totally_unknown_code") == ErrorCategory.NON_RETRYABLE

    def test_get_error_category_from_enum(self):
        """get_error_category should accept enum values directly."""
        assert get_error_category(AgentErrorCode.TIMEOUT) == ErrorCategory.RETRYABLE
