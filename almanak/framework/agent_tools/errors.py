"""Structured error taxonomy for agent tool calls.

Every tool error includes a machine-readable code, human message,
recoverability flag, and optional remediation suggestion so that
LLM agents can reason about failures and decide whether to retry,
adjust arguments, or abort.
"""

from __future__ import annotations


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
            "validation_error",
            message,
            recoverable=True,
            suggestion=suggestion or "Check argument types and required fields.",
            tool_name=tool_name,
        )


class RiskBlockedError(ToolError):
    """Policy or RiskGuard rejected the action."""

    def __init__(self, message: str, *, suggestion: str | None = None, tool_name: str | None = None) -> None:
        super().__init__(
            "risk_blocked",
            message,
            recoverable=False,
            suggestion=suggestion or "Reduce trade size or check policy constraints.",
            tool_name=tool_name,
        )


class SimulationFailedError(ToolError):
    """On-chain simulation reverted or returned unexpected results."""

    def __init__(self, message: str, *, suggestion: str | None = None, tool_name: str | None = None) -> None:
        super().__init__(
            "simulation_failed",
            message,
            recoverable=True,
            suggestion=suggestion or "Adjust parameters (slippage, amounts) and retry.",
            tool_name=tool_name,
        )


class ToolTimeoutError(ToolError):
    """Gateway or RPC call timed out."""

    def __init__(self, message: str, *, suggestion: str | None = None, tool_name: str | None = None) -> None:
        super().__init__(
            "timeout",
            message,
            recoverable=True,
            suggestion=suggestion or "Retry after a brief delay.",
            tool_name=tool_name,
        )


class UpstreamUnavailableError(ToolError):
    """External service (RPC, price feed, gateway) is unreachable."""

    def __init__(self, message: str, *, suggestion: str | None = None, tool_name: str | None = None) -> None:
        super().__init__(
            "upstream_unavailable",
            message,
            recoverable=True,
            suggestion=suggestion or "The service may be temporarily down. Retry later.",
            tool_name=tool_name,
        )


class PermissionDeniedError(ToolError):
    """Action not allowed by the agent's policy."""

    def __init__(self, message: str, *, suggestion: str | None = None, tool_name: str | None = None) -> None:
        super().__init__(
            "permission_denied",
            message,
            recoverable=False,
            suggestion=suggestion or "This action is not in the agent's allowed set.",
            tool_name=tool_name,
        )


class ExecutionFailedError(ToolError):
    """On-chain transaction execution failed (reverted, out of gas, etc.)."""

    def __init__(self, message: str, *, suggestion: str | None = None, tool_name: str | None = None) -> None:
        super().__init__(
            "execution_failed",
            message,
            recoverable=False,
            suggestion=suggestion or "Check transaction parameters and chain state.",
            tool_name=tool_name,
        )
