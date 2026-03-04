"""Structured error taxonomy for agent tool calls.

Every tool error includes a machine-readable code, human message,
recoverability flag, and optional remediation suggestion so that
LLM agents can reason about failures and decide whether to retry,
adjust arguments, or abort.

The ``AgentErrorCode`` enum is the canonical set of error codes.
The ``ErrorCategory`` enum tells agents *how* to handle each error
(retry, abort, change approach, escalate to human, or fix config).
"""

from __future__ import annotations

from enum import StrEnum

# ---------------------------------------------------------------------------
# Error code enum -- single source of truth for all error_code values
# ---------------------------------------------------------------------------


class AgentErrorCode(StrEnum):
    """Standardized error codes for agent tool responses.

    Every error returned by ToolExecutor uses one of these codes.
    The string values are the wire-format values that appear in
    ``ToolResponse.error["error_code"]``.
    """

    # -- Validation errors --------------------------------------------------
    VALIDATION_ERROR = "validation_error"
    INVALID_INTENT_TYPE = "invalid_intent_type"

    # -- Policy / risk violations -------------------------------------------
    RISK_BLOCKED = "risk_blocked"
    PERMISSION_DENIED = "permission_denied"

    # -- Execution errors ---------------------------------------------------
    EXECUTION_FAILED = "execution_failed"
    COMPILATION_FAILED = "compilation_failed"
    SIMULATION_FAILED = "simulation_failed"

    # -- Infrastructure errors ----------------------------------------------
    GATEWAY_ERROR = "gateway_error"
    RPC_FAILED = "rpc_failed"
    TIMEOUT = "timeout"
    UPSTREAM_UNAVAILABLE = "upstream_unavailable"
    INTERNAL_ERROR = "internal_error"

    # -- State errors -------------------------------------------------------
    STATE_LOAD_FAILED = "state_load_failed"
    RECORD_FAILED = "record_failed"

    # -- Data / query errors ------------------------------------------------
    ALL_QUERIES_FAILED = "all_queries_failed"
    EMPTY_POOL = "empty_pool"
    INVALID_POSITION = "invalid_position"
    UNSUPPORTED_CHAIN = "unsupported_chain"

    # -- Vault-specific errors ----------------------------------------------
    VAULT_READ_FAILED = "vault_read_failed"
    VAULT_VERIFICATION_FAILED = "vault_verification_failed"
    PREFLIGHT_FAILED = "preflight_failed"
    INSUFFICIENT_LIQUIDITY = "insufficient_liquidity"

    # -- Teardown-specific errors -------------------------------------------
    TEARDOWN_MISSING_SUB_TOOLS = "teardown_missing_sub_tools"
    TEARDOWN_LP_CLOSE_FAILED = "teardown_lp_close_failed"

    # -- Feature gaps -------------------------------------------------------
    NOT_IMPLEMENTED = "not_implemented"


# ---------------------------------------------------------------------------
# Error category enum -- tells agents how to handle each error
# ---------------------------------------------------------------------------


class ErrorCategory(StrEnum):
    """Categorization to help agents decide how to handle errors.

    Agents should use this to implement retry/abort/escalation logic
    without pattern-matching on individual error codes.
    """

    RETRYABLE = "retryable"
    NON_RETRYABLE = "non_retryable"
    POLICY_VIOLATION = "policy_violation"
    REQUIRES_HUMAN = "requires_human"
    CONFIGURATION = "configuration"


# ---------------------------------------------------------------------------
# Error code -> category mapping
# ---------------------------------------------------------------------------

ERROR_CATEGORIES: dict[AgentErrorCode, ErrorCategory] = {
    # Retryable -- transient failures, try again
    AgentErrorCode.GATEWAY_ERROR: ErrorCategory.RETRYABLE,
    AgentErrorCode.RPC_FAILED: ErrorCategory.RETRYABLE,
    AgentErrorCode.TIMEOUT: ErrorCategory.RETRYABLE,
    AgentErrorCode.UPSTREAM_UNAVAILABLE: ErrorCategory.RETRYABLE,
    AgentErrorCode.STATE_LOAD_FAILED: ErrorCategory.RETRYABLE,
    AgentErrorCode.ALL_QUERIES_FAILED: ErrorCategory.RETRYABLE,
    AgentErrorCode.VAULT_READ_FAILED: ErrorCategory.RETRYABLE,
    AgentErrorCode.VAULT_VERIFICATION_FAILED: ErrorCategory.RETRYABLE,
    AgentErrorCode.RECORD_FAILED: ErrorCategory.RETRYABLE,
    AgentErrorCode.TEARDOWN_LP_CLOSE_FAILED: ErrorCategory.RETRYABLE,
    AgentErrorCode.INSUFFICIENT_LIQUIDITY: ErrorCategory.RETRYABLE,
    AgentErrorCode.SIMULATION_FAILED: ErrorCategory.RETRYABLE,
    # Non-retryable -- same inputs will produce the same failure
    AgentErrorCode.VALIDATION_ERROR: ErrorCategory.NON_RETRYABLE,
    AgentErrorCode.INVALID_INTENT_TYPE: ErrorCategory.NON_RETRYABLE,
    AgentErrorCode.EXECUTION_FAILED: ErrorCategory.NON_RETRYABLE,
    AgentErrorCode.COMPILATION_FAILED: ErrorCategory.NON_RETRYABLE,
    AgentErrorCode.EMPTY_POOL: ErrorCategory.NON_RETRYABLE,
    AgentErrorCode.INVALID_POSITION: ErrorCategory.NON_RETRYABLE,
    AgentErrorCode.NOT_IMPLEMENTED: ErrorCategory.NON_RETRYABLE,
    AgentErrorCode.INTERNAL_ERROR: ErrorCategory.NON_RETRYABLE,
    # Policy violations -- agent must change its approach
    AgentErrorCode.RISK_BLOCKED: ErrorCategory.POLICY_VIOLATION,
    AgentErrorCode.PERMISSION_DENIED: ErrorCategory.POLICY_VIOLATION,
    # Configuration issues -- requires setup changes
    AgentErrorCode.UNSUPPORTED_CHAIN: ErrorCategory.CONFIGURATION,
    AgentErrorCode.TEARDOWN_MISSING_SUB_TOOLS: ErrorCategory.CONFIGURATION,
    AgentErrorCode.PREFLIGHT_FAILED: ErrorCategory.CONFIGURATION,
}


def get_error_category(code: AgentErrorCode | str) -> ErrorCategory:
    """Look up the error category for a given error code.

    Args:
        code: An ``AgentErrorCode`` enum value or its string representation.

    Returns:
        The ``ErrorCategory`` for the code, or ``NON_RETRYABLE`` as a safe
        default for unrecognised codes.
    """
    if isinstance(code, str):
        try:
            code = AgentErrorCode(code)
        except ValueError:
            return ErrorCategory.NON_RETRYABLE
    return ERROR_CATEGORIES.get(code, ErrorCategory.NON_RETRYABLE)


# ---------------------------------------------------------------------------
# Base error class
# ---------------------------------------------------------------------------


class ToolError(Exception):
    """Base error for all agent tool failures.

    Attributes:
        code: Machine-readable error code for agent consumption.
        message: Human-readable error description.
        recoverable: Whether the agent should consider retrying.
        suggestion: Optional remediation hint for the agent.
        tool_name: Name of the tool that failed (set by executor).
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        recoverable: bool = False,
        suggestion: str | None = None,
        tool_name: str | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.recoverable = recoverable
        self.suggestion = suggestion
        self.tool_name = tool_name
        super().__init__(f"[{code}] {message}")

    def to_dict(self) -> dict:
        """Serialize to a dict suitable for tool response envelopes."""
        d: dict = {
            "error_code": self.code,
            "message": self.message,
            "recoverable": self.recoverable,
            "error_category": get_error_category(self.code).value,
        }
        if self.suggestion:
            d["suggestion"] = self.suggestion
        if self.tool_name:
            d["tool_name"] = self.tool_name
        return d


# ---------------------------------------------------------------------------
# Specific error types
# ---------------------------------------------------------------------------


class ToolValidationError(ToolError):
    """Invalid input arguments (malformed, missing, wrong type)."""

    def __init__(self, message: str, *, suggestion: str | None = None, tool_name: str | None = None) -> None:
        super().__init__(
            AgentErrorCode.VALIDATION_ERROR,
            message,
            recoverable=True,
            suggestion=suggestion or "Check argument types and required fields.",
            tool_name=tool_name,
        )


class RiskBlockedError(ToolError):
    """Policy or RiskGuard rejected the action."""

    def __init__(self, message: str, *, suggestion: str | None = None, tool_name: str | None = None) -> None:
        super().__init__(
            AgentErrorCode.RISK_BLOCKED,
            message,
            recoverable=False,
            suggestion=suggestion or "Reduce trade size or check policy constraints.",
            tool_name=tool_name,
        )


class SimulationFailedError(ToolError):
    """On-chain simulation reverted or returned unexpected results."""

    def __init__(self, message: str, *, suggestion: str | None = None, tool_name: str | None = None) -> None:
        super().__init__(
            AgentErrorCode.SIMULATION_FAILED,
            message,
            recoverable=True,
            suggestion=suggestion or "Adjust parameters (slippage, amounts) and retry.",
            tool_name=tool_name,
        )


class ToolTimeoutError(ToolError):
    """Gateway or RPC call timed out."""

    def __init__(self, message: str, *, suggestion: str | None = None, tool_name: str | None = None) -> None:
        super().__init__(
            AgentErrorCode.TIMEOUT,
            message,
            recoverable=True,
            suggestion=suggestion or "Retry after a brief delay.",
            tool_name=tool_name,
        )


class UpstreamUnavailableError(ToolError):
    """External service (RPC, price feed, gateway) is unreachable."""

    def __init__(self, message: str, *, suggestion: str | None = None, tool_name: str | None = None) -> None:
        super().__init__(
            AgentErrorCode.UPSTREAM_UNAVAILABLE,
            message,
            recoverable=True,
            suggestion=suggestion or "The service may be temporarily down. Retry later.",
            tool_name=tool_name,
        )


class PermissionDeniedError(ToolError):
    """Action not allowed by the agent's policy."""

    def __init__(self, message: str, *, suggestion: str | None = None, tool_name: str | None = None) -> None:
        super().__init__(
            AgentErrorCode.PERMISSION_DENIED,
            message,
            recoverable=False,
            suggestion=suggestion or "This action is not in the agent's allowed set.",
            tool_name=tool_name,
        )


class ExecutionFailedError(ToolError):
    """On-chain transaction execution failed (reverted, out of gas, etc.)."""

    def __init__(self, message: str, *, suggestion: str | None = None, tool_name: str | None = None) -> None:
        super().__init__(
            AgentErrorCode.EXECUTION_FAILED,
            message,
            recoverable=False,
            suggestion=suggestion or "Check transaction parameters and chain state.",
            tool_name=tool_name,
        )
