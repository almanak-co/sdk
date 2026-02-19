"""Execution Event Types and Payloads for real-time monitoring.

This module defines structured execution events with typed payloads for:
- Dashboard and alerting system integration
- Transaction lifecycle tracking
- Swap result reporting
- Error handling and recovery

All events are compatible with:
- TimelineEmitter pattern from src/api/timeline.py
- AlertManager from src/alerting/alert_manager.py
- OperatorCard system from src/models/operator_card.py

Example:
    from almanak.framework.execution.events import (
        ExecutionEventType,
        TransactionSentPayload,
        TransactionConfirmedPayload,
        SwapResultPayload,
        ExecutionFailedPayload,
    )

    # Create a transaction sent event
    payload = TransactionSentPayload(
        tx_hash="0x123...",
        chain="arbitrum",
        from_addr="0xabc...",
        to_addr="0xdef...",
        value=1000000000000000000,
        gas_limit=150000,
        nonce=42,
    )

    # Create a swap result event
    swap_payload = SwapResultPayload(
        token_in="USDC",
        token_out="WETH",
        amount_in=Decimal("1000.00"),
        amount_out=Decimal("0.5"),
        effective_price=Decimal("2000.00"),
        slippage_bps=25,
    )
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

# =============================================================================
# ExecutionEventType Enum
# =============================================================================


class ExecutionEventType(StrEnum):
    """Types of events emitted during execution.

    This enum covers the full transaction lifecycle from validation
    through confirmation, plus swap-specific events.

    Event Categories:
        Validation: VALIDATING, RISK_BLOCKED
        Simulation: SIMULATING, SIMULATION_FAILED
        Signing: SIGNING
        Submission: SUBMITTING, TX_SENT
        Confirmation: WAITING, TX_CONFIRMED, TX_REVERTED
        Completion: EXECUTION_SUCCESS, EXECUTION_FAILED

    The enum values are strings to enable easy JSON serialization
    and compatibility with the TimelineEmitter pattern.

    Example:
        event_type = ExecutionEventType.TX_SENT
        print(event_type.value)  # "TX_SENT"
        print(event_type == "TX_SENT")  # True (str enum)
    """

    # Validation phase
    VALIDATING = "VALIDATING"
    """Validating transactions via RiskGuard."""

    RISK_BLOCKED = "RISK_BLOCKED"
    """Execution blocked by RiskGuard due to risk violations."""

    # Simulation phase
    SIMULATING = "SIMULATING"
    """Simulating transactions before execution."""

    SIMULATION_FAILED = "SIMULATION_FAILED"
    """Simulation failed (would revert or exceed limits)."""

    # Signing phase
    SIGNING = "SIGNING"
    """Signing transactions with signer."""

    # Submission phase
    SUBMITTING = "SUBMITTING"
    """Submitting signed transactions to mempool."""

    TX_SENT = "TX_SENT"
    """Transaction successfully sent to mempool."""

    # Confirmation phase
    WAITING = "WAITING"
    """Waiting for transaction confirmation."""

    TX_CONFIRMED = "TX_CONFIRMED"
    """Transaction confirmed on-chain."""

    TX_REVERTED = "TX_REVERTED"
    """Transaction reverted on-chain."""

    # Completion phase
    EXECUTION_SUCCESS = "EXECUTION_SUCCESS"
    """All transactions executed successfully."""

    EXECUTION_FAILED = "EXECUTION_FAILED"
    """Execution failed at some phase."""


# =============================================================================
# Payload Dataclasses
# =============================================================================


@dataclass
class TransactionSentPayload:
    """Payload for TX_SENT event.

    Contains all relevant information about a transaction that has been
    submitted to the mempool but not yet confirmed.

    Attributes:
        tx_hash: Transaction hash (0x-prefixed hex string)
        chain: Blockchain network (e.g., "arbitrum", "ethereum")
        from_addr: Sender address (0x-prefixed, 42 chars)
        to_addr: Recipient address (0x-prefixed, 42 chars)
        value: Value in wei (native token amount)
        gas_limit: Maximum gas units for the transaction
        nonce: Transaction nonce for the sender

    Example:
        payload = TransactionSentPayload(
            tx_hash="0x1234567890abcdef...",
            chain="arbitrum",
            from_addr="0xabc123...",
            to_addr="0xdef456...",
            value=1000000000000000000,  # 1 ETH in wei
            gas_limit=150000,
            nonce=42,
        )
    """

    tx_hash: str
    chain: str
    from_addr: str
    to_addr: str
    value: int
    gas_limit: int
    nonce: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "tx_hash": self.tx_hash,
            "chain": self.chain,
            "from_addr": self.from_addr,
            "to_addr": self.to_addr,
            "value": str(self.value),
            "gas_limit": self.gas_limit,
            "nonce": self.nonce,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransactionSentPayload":
        """Create from dictionary."""
        return cls(
            tx_hash=data["tx_hash"],
            chain=data["chain"],
            from_addr=data["from_addr"],
            to_addr=data["to_addr"],
            value=int(data["value"]),
            gas_limit=data["gas_limit"],
            nonce=data["nonce"],
        )


@dataclass
class TransactionConfirmedPayload:
    """Payload for TX_CONFIRMED event.

    Contains confirmation details for a transaction that has been
    included in a block.

    Attributes:
        tx_hash: Transaction hash (0x-prefixed hex string)
        block_number: Block number where transaction was included
        gas_used: Actual gas consumed by the transaction
        status: Transaction status (1 = success, 0 = reverted)
        logs: Event logs emitted by the transaction

    Example:
        payload = TransactionConfirmedPayload(
            tx_hash="0x1234567890abcdef...",
            block_number=12345678,
            gas_used=85000,
            status=1,
            logs=[
                {
                    "address": "0x...",
                    "topics": ["0x...", "0x..."],
                    "data": "0x...",
                }
            ],
        )
    """

    tx_hash: str
    block_number: int
    gas_used: int
    status: int
    logs: list[dict[str, Any]] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """Check if transaction succeeded."""
        return self.status == 1

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "tx_hash": self.tx_hash,
            "block_number": self.block_number,
            "gas_used": self.gas_used,
            "status": self.status,
            "logs": self.logs,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransactionConfirmedPayload":
        """Create from dictionary."""
        return cls(
            tx_hash=data["tx_hash"],
            block_number=data["block_number"],
            gas_used=data["gas_used"],
            status=data["status"],
            logs=data.get("logs", []),
        )


@dataclass
class SwapResultPayload:
    """Payload for swap execution results.

    Contains detailed information about a swap that has been executed,
    useful for reporting, analytics, and slippage tracking.

    Attributes:
        token_in: Input token symbol (e.g., "USDC")
        token_out: Output token symbol (e.g., "WETH")
        amount_in: Amount of input token (human-readable, not wei)
        amount_out: Amount of output token received (human-readable)
        effective_price: Actual execution price (amount_in / amount_out)
        slippage_bps: Actual slippage in basis points vs quoted price

    Example:
        payload = SwapResultPayload(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000.00"),
            amount_out=Decimal("0.5"),
            effective_price=Decimal("2000.00"),
            slippage_bps=25,  # 0.25% slippage
        )
    """

    token_in: str
    token_out: str
    amount_in: Decimal
    amount_out: Decimal
    effective_price: Decimal
    slippage_bps: int

    @property
    def slippage_percent(self) -> float:
        """Get slippage as a percentage."""
        return self.slippage_bps / 100.0

    @property
    def is_favorable(self) -> bool:
        """Check if slippage was favorable (negative or zero)."""
        return self.slippage_bps <= 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "effective_price": str(self.effective_price),
            "slippage_bps": self.slippage_bps,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SwapResultPayload":
        """Create from dictionary."""
        return cls(
            token_in=data["token_in"],
            token_out=data["token_out"],
            amount_in=Decimal(data["amount_in"]),
            amount_out=Decimal(data["amount_out"]),
            effective_price=Decimal(data["effective_price"]),
            slippage_bps=data["slippage_bps"],
        )


@dataclass
class ExecutionFailedPayload:
    """Payload for EXECUTION_FAILED event.

    Contains detailed information about an execution failure,
    including error classification and recovery guidance.

    Attributes:
        error_type: Classification of the error
        error_message: Human-readable error description
        recoverable: Whether the error can be automatically recovered
        suggested_action: Recommended action to resolve the issue

    Error Types:
        - VALIDATION_ERROR: RiskGuard blocked execution
        - SIMULATION_ERROR: Simulation failed (would revert)
        - SIGNING_ERROR: Transaction signing failed
        - SUBMISSION_ERROR: Failed to submit to mempool
        - NONCE_ERROR: Nonce conflict or invalid nonce
        - GAS_ERROR: Gas estimation or limit issue
        - REVERT_ERROR: Transaction reverted on-chain
        - TIMEOUT_ERROR: Confirmation timed out
        - RPC_ERROR: RPC connection or response error
        - INSUFFICIENT_FUNDS: Not enough balance for execution
        - UNKNOWN_ERROR: Unclassified error

    Example:
        payload = ExecutionFailedPayload(
            error_type="NONCE_ERROR",
            error_message="Nonce too low: expected 43, got 42",
            recoverable=True,
            suggested_action="Retry with updated nonce",
        )
    """

    error_type: str
    error_message: str
    recoverable: bool
    suggested_action: str

    def __post_init__(self) -> None:
        """Validate error type."""
        valid_types = {
            "VALIDATION_ERROR",
            "SIMULATION_ERROR",
            "SIGNING_ERROR",
            "SUBMISSION_ERROR",
            "NONCE_ERROR",
            "GAS_ERROR",
            "REVERT_ERROR",
            "TIMEOUT_ERROR",
            "RPC_ERROR",
            "INSUFFICIENT_FUNDS",
            "UNKNOWN_ERROR",
        }
        if self.error_type not in valid_types:
            # Allow custom error types but log a warning
            pass

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "error_type": self.error_type,
            "error_message": self.error_message,
            "recoverable": self.recoverable,
            "suggested_action": self.suggested_action,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExecutionFailedPayload":
        """Create from dictionary."""
        return cls(
            error_type=data["error_type"],
            error_message=data["error_message"],
            recoverable=data["recoverable"],
            suggested_action=data["suggested_action"],
        )


# =============================================================================
# Execution Event Wrapper
# =============================================================================


@dataclass
class ExecutionEvent:
    """Complete execution event with type and payload.

    This wrapper combines an event type with its typed payload and
    additional context for full observability.

    Attributes:
        event_type: Type of execution event
        timestamp: When the event occurred
        strategy_id: Strategy that triggered the event
        chain: Blockchain network
        correlation_id: Unique identifier for the execution
        payload: Typed payload for this event type
        metadata: Additional context

    Example:
        event = ExecutionEvent(
            event_type=ExecutionEventType.TX_SENT,
            timestamp=datetime.now(timezone.utc),
            strategy_id="momentum-arb-001",
            chain="arbitrum",
            correlation_id="abc123",
            payload=TransactionSentPayload(...),
        )

        # Emit to timeline
        from almanak.framework.api.timeline import add_event, TimelineEvent, TimelineEventType
        timeline_event = TimelineEvent(
            timestamp=event.timestamp,
            event_type=TimelineEventType.TRANSACTION_SUBMITTED,
            description=f"Transaction sent: {event.payload.tx_hash}",
            strategy_id=event.strategy_id,
            chain=event.chain,
            details=event.payload.to_dict(),
        )
        add_event(timeline_event)
    """

    event_type: ExecutionEventType
    timestamp: datetime
    strategy_id: str
    chain: str
    correlation_id: str
    payload: (
        TransactionSentPayload | TransactionConfirmedPayload | SwapResultPayload | ExecutionFailedPayload | None
    ) = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "event_type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
            "strategy_id": self.strategy_id,
            "chain": self.chain,
            "correlation_id": self.correlation_id,
            "payload": self.payload.to_dict() if self.payload else None,
            "metadata": self.metadata,
        }


# =============================================================================
# Timeline Integration Helpers
# =============================================================================


# Mapping from ExecutionEventType to TimelineEventType values
# This enables seamless integration with the existing timeline system
EXECUTION_TO_TIMELINE_MAP: dict[ExecutionEventType, str] = {
    ExecutionEventType.VALIDATING: "CUSTOM",
    ExecutionEventType.RISK_BLOCKED: "RISK_GUARD_TRIGGERED",
    ExecutionEventType.SIMULATING: "CUSTOM",
    ExecutionEventType.SIMULATION_FAILED: "CUSTOM",
    ExecutionEventType.SIGNING: "CUSTOM",
    ExecutionEventType.SUBMITTING: "CUSTOM",
    ExecutionEventType.TX_SENT: "TRANSACTION_SUBMITTED",
    ExecutionEventType.WAITING: "CUSTOM",
    ExecutionEventType.TX_CONFIRMED: "TRANSACTION_CONFIRMED",
    ExecutionEventType.TX_REVERTED: "TRANSACTION_REVERTED",
    ExecutionEventType.EXECUTION_SUCCESS: "CUSTOM",
    ExecutionEventType.EXECUTION_FAILED: "TRANSACTION_FAILED",
}


# Mapping from error types to suggested actions
# Used by ExecutionFailedPayload for recovery guidance
ERROR_RECOVERY_MAP: dict[str, tuple[bool, str]] = {
    "VALIDATION_ERROR": (False, "Review risk guard configuration"),
    "SIMULATION_ERROR": (True, "Retry with adjusted parameters"),
    "SIGNING_ERROR": (False, "Check signer configuration"),
    "SUBMISSION_ERROR": (True, "Retry submission"),
    "NONCE_ERROR": (True, "Retry with updated nonce"),
    "GAS_ERROR": (True, "Retry with higher gas limit"),
    "REVERT_ERROR": (True, "Review transaction parameters"),
    "TIMEOUT_ERROR": (True, "Check transaction status and retry if needed"),
    "RPC_ERROR": (True, "Retry with different RPC endpoint"),
    "INSUFFICIENT_FUNDS": (False, "Add funds to wallet"),
    "UNKNOWN_ERROR": (False, "Investigate error details"),
}


def get_recovery_info(error_type: str) -> tuple[bool, str]:
    """Get recovery information for an error type.

    Args:
        error_type: The type of error

    Returns:
        Tuple of (recoverable, suggested_action)

    Example:
        recoverable, action = get_recovery_info("NONCE_ERROR")
        # recoverable = True, action = "Retry with updated nonce"
    """
    return ERROR_RECOVERY_MAP.get(error_type, (False, "Investigate error details"))


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    # Enum
    "ExecutionEventType",
    # Payload dataclasses
    "TransactionSentPayload",
    "TransactionConfirmedPayload",
    "SwapResultPayload",
    "ExecutionFailedPayload",
    # Event wrapper
    "ExecutionEvent",
    # Integration maps
    "EXECUTION_TO_TIMELINE_MAP",
    "ERROR_RECOVERY_MAP",
    "get_recovery_info",
]
