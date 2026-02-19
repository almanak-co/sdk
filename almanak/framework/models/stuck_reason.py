"""StuckReason enum and related mappings for classifying strategy stuck states.

This module provides a finite, predictable set of stuck state classifications
along with mappings for remediation actions.
"""

from enum import StrEnum


class StuckReason(StrEnum):
    """Classification of why a strategy is stuck.

    All stuck states are classified into one of these categories to enable
    predictable handling and automated remediation where possible.
    """

    # Transaction issues
    GAS_PRICE_BLOCKED = "GAS_PRICE_BLOCKED"
    """Transaction not being mined due to gas price being too low."""

    NONCE_CONFLICT = "NONCE_CONFLICT"
    """Multiple transactions with the same nonce or gap in nonce sequence."""

    TRANSACTION_REVERTED = "TRANSACTION_REVERTED"
    """Transaction was included but reverted during execution."""

    NOT_INCLUDED_TIMEOUT = "NOT_INCLUDED_TIMEOUT"
    """Transaction has not been included within the expected timeframe."""

    # Balance issues
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    """Insufficient token balance to execute the intended action."""

    INSUFFICIENT_GAS = "INSUFFICIENT_GAS"
    """Insufficient native token balance to pay for gas."""

    ALLOWANCE_MISSING = "ALLOWANCE_MISSING"
    """Token approval/allowance not set or insufficient."""

    # Protocol issues
    SLIPPAGE_EXCEEDED = "SLIPPAGE_EXCEEDED"
    """Trade would exceed the maximum allowed slippage."""

    POOL_LIQUIDITY_LOW = "POOL_LIQUIDITY_LOW"
    """Insufficient liquidity in the pool to execute the trade."""

    ORACLE_STALE = "ORACLE_STALE"
    """Price oracle data is stale or unavailable."""

    PROTOCOL_PAUSED = "PROTOCOL_PAUSED"
    """The target protocol is paused or in emergency mode."""

    # System issues
    RPC_FAILURE = "RPC_FAILURE"
    """RPC endpoint is unavailable or returning errors."""

    RECEIPT_PARSE_FAILED = "RECEIPT_PARSE_FAILED"
    """Failed to parse transaction receipt or decode events."""

    STATE_CONFLICT = "STATE_CONFLICT"
    """Strategy state conflicts with on-chain state (CAS failure)."""

    # Risk guard issues
    RISK_GUARD_BLOCKED = "RISK_GUARD_BLOCKED"
    """Action was blocked by risk guard validation."""

    CIRCUIT_BREAKER = "CIRCUIT_BREAKER"
    """Circuit breaker tripped due to excessive losses or anomalies."""

    # Unknown
    UNKNOWN = "UNKNOWN"
    """Reason could not be determined (rare edge cases)."""


from .actions import AvailableAction, SuggestedAction

# Mapping of each stuck reason to suggested remediation actions
REMEDIATION_MAP: dict[StuckReason, list[SuggestedAction]] = {
    # Transaction issues
    StuckReason.GAS_PRICE_BLOCKED: [
        SuggestedAction(
            action=AvailableAction.BUMP_GAS,
            description="Increase gas price to get transaction included",
            priority=1,
            is_recommended=True,
        ),
        SuggestedAction(
            action=AvailableAction.CANCEL_TX,
            description="Cancel the pending transaction",
            priority=2,
        ),
    ],
    StuckReason.NONCE_CONFLICT: [
        SuggestedAction(
            action=AvailableAction.CANCEL_TX,
            description="Cancel conflicting transaction to resolve nonce issue",
            priority=1,
            is_recommended=True,
        ),
        SuggestedAction(
            action=AvailableAction.PAUSE,
            description="Pause strategy to investigate nonce state",
            priority=2,
        ),
    ],
    StuckReason.TRANSACTION_REVERTED: [
        SuggestedAction(
            action=AvailableAction.PAUSE,
            description="Pause strategy to investigate revert reason",
            priority=1,
            is_recommended=True,
        ),
    ],
    StuckReason.NOT_INCLUDED_TIMEOUT: [
        SuggestedAction(
            action=AvailableAction.BUMP_GAS,
            description="Bump gas price to improve inclusion chances",
            priority=1,
            is_recommended=True,
        ),
        SuggestedAction(
            action=AvailableAction.CANCEL_TX,
            description="Cancel and retry with higher gas",
            priority=2,
        ),
    ],
    # Balance issues
    StuckReason.INSUFFICIENT_BALANCE: [
        SuggestedAction(
            action=AvailableAction.PAUSE,
            description="Pause strategy and top up token balance",
            priority=1,
            is_recommended=True,
        ),
    ],
    StuckReason.INSUFFICIENT_GAS: [
        SuggestedAction(
            action=AvailableAction.PAUSE,
            description="Pause strategy and add gas funds",
            priority=1,
            is_recommended=True,
        ),
    ],
    StuckReason.ALLOWANCE_MISSING: [
        SuggestedAction(
            action=AvailableAction.RESUME,
            description="Resume to automatically set token approval",
            priority=1,
            is_recommended=True,
        ),
        SuggestedAction(
            action=AvailableAction.PAUSE,
            description="Pause to manually investigate approval issue",
            priority=2,
        ),
    ],
    # Protocol issues
    StuckReason.SLIPPAGE_EXCEEDED: [
        SuggestedAction(
            action=AvailableAction.PAUSE,
            description="Pause and wait for better market conditions",
            priority=1,
            is_recommended=True,
        ),
    ],
    StuckReason.POOL_LIQUIDITY_LOW: [
        SuggestedAction(
            action=AvailableAction.PAUSE,
            description="Pause and wait for liquidity to improve",
            priority=1,
            is_recommended=True,
        ),
    ],
    StuckReason.ORACLE_STALE: [
        SuggestedAction(
            action=AvailableAction.PAUSE,
            description="Pause until oracle is updated",
            priority=1,
            is_recommended=True,
        ),
    ],
    StuckReason.PROTOCOL_PAUSED: [
        SuggestedAction(
            action=AvailableAction.PAUSE,
            description="Pause strategy until protocol resumes",
            priority=1,
            is_recommended=True,
        ),
    ],
    # System issues
    StuckReason.RPC_FAILURE: [
        SuggestedAction(
            action=AvailableAction.PAUSE,
            description="Pause until RPC connectivity is restored",
            priority=1,
            is_recommended=True,
        ),
        SuggestedAction(
            action=AvailableAction.RESUME,
            description="Resume to retry with backup RPC",
            priority=2,
        ),
    ],
    StuckReason.RECEIPT_PARSE_FAILED: [
        SuggestedAction(
            action=AvailableAction.PAUSE,
            description="Pause to investigate transaction parsing issue",
            priority=1,
            is_recommended=True,
        ),
    ],
    StuckReason.STATE_CONFLICT: [
        SuggestedAction(
            action=AvailableAction.PAUSE,
            description="Pause to resolve state conflict manually",
            priority=1,
            is_recommended=True,
        ),
    ],
    # Risk guard issues
    StuckReason.RISK_GUARD_BLOCKED: [
        SuggestedAction(
            action=AvailableAction.PAUSE,
            description="Pause and review risk parameters",
            priority=1,
            is_recommended=True,
        ),
    ],
    StuckReason.CIRCUIT_BREAKER: [
        SuggestedAction(
            action=AvailableAction.EMERGENCY_UNWIND,
            description="Emergency unwind all positions to limit losses",
            priority=1,
            is_recommended=True,
        ),
        SuggestedAction(
            action=AvailableAction.PAUSE,
            description="Pause and investigate anomaly",
            priority=2,
        ),
    ],
    # Unknown
    StuckReason.UNKNOWN: [
        SuggestedAction(
            action=AvailableAction.PAUSE,
            description="Pause strategy for manual investigation",
            priority=1,
            is_recommended=True,
        ),
    ],
}


# Set of reasons that can be auto-remediated without human intervention
AUTO_REMEDIABLE: set[StuckReason] = {
    StuckReason.GAS_PRICE_BLOCKED,
    StuckReason.NOT_INCLUDED_TIMEOUT,
    StuckReason.ALLOWANCE_MISSING,
    StuckReason.RPC_FAILURE,
}

# Set of reasons that require human intervention
NEEDS_HUMAN: set[StuckReason] = {
    StuckReason.NONCE_CONFLICT,
    StuckReason.TRANSACTION_REVERTED,
    StuckReason.INSUFFICIENT_BALANCE,
    StuckReason.INSUFFICIENT_GAS,
    StuckReason.SLIPPAGE_EXCEEDED,
    StuckReason.POOL_LIQUIDITY_LOW,
    StuckReason.ORACLE_STALE,
    StuckReason.PROTOCOL_PAUSED,
    StuckReason.RECEIPT_PARSE_FAILED,
    StuckReason.STATE_CONFLICT,
    StuckReason.RISK_GUARD_BLOCKED,
    StuckReason.CIRCUIT_BREAKER,
    StuckReason.UNKNOWN,
}
