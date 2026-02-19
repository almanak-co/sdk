"""Execution Session model for crash recovery.

This module provides dataclasses to capture execution state for crash recovery,
enabling the system to resume incomplete executions after a restart.

The ExecutionSession tracks:
- Session identity (session_id, strategy_id, intent_id)
- Current execution phase (PREPARING, SIGNING, SUBMITTED, CONFIRMING)
- Transaction states (tx_hash, nonce, status for each transaction)
- Retry tracking (attempt_number, last_error)
- ActionBundle snapshot for replay

Example:
    from almanak.framework.execution.session import ExecutionSession, ExecutionPhase, TransactionState

    # Create a new session
    session = ExecutionSession(
        session_id="sess_123",
        strategy_id="strategy_a",
        intent_id="intent_456",
        phase=ExecutionPhase.PREPARING,
    )

    # Update as execution progresses
    session.phase = ExecutionPhase.SIGNING
    session.transactions.append(TransactionState(
        tx_hash="0xabc...",
        nonce=42,
        status=TransactionStatus.PENDING,
    ))
"""

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

# =============================================================================
# Enums
# =============================================================================


class ExecutionPhase(StrEnum):
    """Phase of execution for crash recovery tracking.

    These phases represent the high-level execution stages:
    - PREPARING: Building and validating the ActionBundle
    - SIGNING: Signing transactions
    - SUBMITTED: Transaction submitted to mempool, awaiting inclusion
    - CONFIRMING: Transaction included, waiting for confirmations
    """

    PREPARING = "PREPARING"
    SIGNING = "SIGNING"
    SUBMITTED = "SUBMITTED"
    CONFIRMING = "CONFIRMING"


class TransactionStatus(StrEnum):
    """Status of an individual transaction within a session.

    Tracks the lifecycle of each transaction:
    - PENDING: Transaction created but not yet submitted
    - SUBMITTED: Transaction sent to mempool
    - CONFIRMED: Transaction confirmed on chain (success)
    - FAILED: Transaction failed (reverted or dropped)
    """

    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class TransactionState:
    """State of an individual transaction for tracking.

    Attributes:
        tx_hash: Transaction hash (empty if not yet submitted)
        nonce: Transaction nonce
        status: Current status of the transaction
        gas_used: Gas used (populated after confirmation)
        block_number: Block number where confirmed (populated after confirmation)
        error: Error message if transaction failed
        submitted_at: When the transaction was submitted
        confirmed_at: When the transaction was confirmed
    """

    tx_hash: str = ""
    nonce: int = 0
    status: TransactionStatus = TransactionStatus.PENDING
    gas_used: int = 0
    block_number: int = 0
    error: str | None = None
    submitted_at: datetime | None = None
    confirmed_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "tx_hash": self.tx_hash,
            "nonce": self.nonce,
            "status": self.status.value,
            "gas_used": self.gas_used,
            "block_number": self.block_number,
            "error": self.error,
            "submitted_at": self.submitted_at.isoformat() if self.submitted_at else None,
            "confirmed_at": self.confirmed_at.isoformat() if self.confirmed_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransactionState":
        """Create from dictionary."""
        return cls(
            tx_hash=data.get("tx_hash", ""),
            nonce=data.get("nonce", 0),
            status=TransactionStatus(data.get("status", "PENDING")),
            gas_used=data.get("gas_used", 0),
            block_number=data.get("block_number", 0),
            error=data.get("error"),
            submitted_at=(datetime.fromisoformat(data["submitted_at"]) if data.get("submitted_at") else None),
            confirmed_at=(datetime.fromisoformat(data["confirmed_at"]) if data.get("confirmed_at") else None),
        )


@dataclass
class ExecutionSession:
    """Execution session for crash recovery.

    Captures all state needed to recover or resume an incomplete execution
    after a system restart or crash.

    Attributes:
        session_id: Unique identifier for this execution session
        strategy_id: ID of the strategy being executed
        intent_id: ID of the intent being executed
        phase: Current execution phase
        transactions: List of transaction states
        attempt_number: Current retry attempt (0 for first attempt)
        last_error: Most recent error message
        action_bundle_snapshot: Serialized ActionBundle for replay
        created_at: When the session was created
        updated_at: When the session was last updated
        completed: Whether the session has completed (success or terminal failure)
        success: Whether the session completed successfully
    """

    session_id: str
    strategy_id: str
    intent_id: str
    phase: ExecutionPhase = ExecutionPhase.PREPARING
    transactions: list[TransactionState] = field(default_factory=list)
    attempt_number: int = 0
    last_error: str | None = None
    action_bundle_snapshot: str | None = None  # JSON string
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    completed: bool = False
    success: bool = False

    def __post_init__(self) -> None:
        """Ensure updated_at is set."""
        if self.updated_at is None:
            self.updated_at = datetime.now(UTC)

    def touch(self) -> None:
        """Update the updated_at timestamp."""
        self.updated_at = datetime.now(UTC)

    def set_phase(self, phase: ExecutionPhase) -> None:
        """Update the execution phase and timestamp.

        Args:
            phase: New execution phase
        """
        self.phase = phase
        self.touch()

    def add_transaction(self, tx_state: TransactionState) -> None:
        """Add a transaction to track.

        Args:
            tx_state: Transaction state to add
        """
        self.transactions.append(tx_state)
        self.touch()

    def update_transaction(
        self,
        tx_hash: str,
        status: TransactionStatus | None = None,
        gas_used: int | None = None,
        block_number: int | None = None,
        error: str | None = None,
    ) -> bool:
        """Update a transaction's state by hash.

        Args:
            tx_hash: Transaction hash to update
            status: New status
            gas_used: Gas used
            block_number: Block number
            error: Error message

        Returns:
            True if transaction was found and updated
        """
        for tx in self.transactions:
            if tx.tx_hash == tx_hash:
                if status is not None:
                    tx.status = status
                    if status == TransactionStatus.CONFIRMED:
                        tx.confirmed_at = datetime.now(UTC)
                if gas_used is not None:
                    tx.gas_used = gas_used
                if block_number is not None:
                    tx.block_number = block_number
                if error is not None:
                    tx.error = error
                self.touch()
                return True
        return False

    def mark_complete(self, success: bool) -> None:
        """Mark the session as complete.

        Args:
            success: Whether the execution was successful
        """
        self.completed = True
        self.success = success
        self.touch()

    def set_error(self, error: str) -> None:
        """Set the last error message.

        Args:
            error: Error message
        """
        self.last_error = error
        self.touch()

    def increment_attempt(self) -> None:
        """Increment the attempt counter for retries."""
        self.attempt_number += 1
        self.touch()

    def set_action_bundle(self, action_bundle_dict: dict[str, Any]) -> None:
        """Set the action bundle snapshot from a dictionary.

        Args:
            action_bundle_dict: ActionBundle as dictionary
        """
        self.action_bundle_snapshot = json.dumps(action_bundle_dict)
        self.touch()

    def get_action_bundle(self) -> dict[str, Any] | None:
        """Get the action bundle snapshot as a dictionary.

        Returns:
            ActionBundle dictionary or None
        """
        if self.action_bundle_snapshot:
            return json.loads(self.action_bundle_snapshot)
        return None

    def is_terminal(self) -> bool:
        """Check if session is in a terminal state.

        Returns:
            True if session is completed
        """
        return self.completed

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "session_id": self.session_id,
            "strategy_id": self.strategy_id,
            "intent_id": self.intent_id,
            "phase": self.phase.value,
            "transactions": [tx.to_dict() for tx in self.transactions],
            "attempt_number": self.attempt_number,
            "last_error": self.last_error,
            "action_bundle_snapshot": self.action_bundle_snapshot,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "completed": self.completed,
            "success": self.success,
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExecutionSession":
        """Create an ExecutionSession from a dictionary.

        Args:
            data: Dictionary containing session data

        Returns:
            ExecutionSession instance
        """
        transactions = [TransactionState.from_dict(tx) for tx in data.get("transactions", [])]

        return cls(
            session_id=data["session_id"],
            strategy_id=data["strategy_id"],
            intent_id=data["intent_id"],
            phase=ExecutionPhase(data.get("phase", "PREPARING")),
            transactions=transactions,
            attempt_number=data.get("attempt_number", 0),
            last_error=data.get("last_error"),
            action_bundle_snapshot=data.get("action_bundle_snapshot"),
            created_at=(datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now(UTC)),
            updated_at=(datetime.fromisoformat(data["updated_at"]) if data.get("updated_at") else datetime.now(UTC)),
            completed=data.get("completed", False),
            success=data.get("success", False),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "ExecutionSession":
        """Create an ExecutionSession from a JSON string.

        Args:
            json_str: JSON string containing session data

        Returns:
            ExecutionSession instance
        """
        return cls.from_dict(json.loads(json_str))


# =============================================================================
# Factory Functions
# =============================================================================


def create_session(
    strategy_id: str,
    intent_id: str,
    session_id: str | None = None,
) -> ExecutionSession:
    """Create a new execution session.

    Args:
        strategy_id: Strategy identifier
        intent_id: Intent identifier
        session_id: Optional session ID (generated if not provided)

    Returns:
        New ExecutionSession
    """
    import uuid

    if session_id is None:
        session_id = f"sess_{uuid.uuid4().hex[:12]}"

    return ExecutionSession(
        session_id=session_id,
        strategy_id=strategy_id,
        intent_id=intent_id,
        phase=ExecutionPhase.PREPARING,
    )


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    # Enums
    "ExecutionPhase",
    "TransactionStatus",
    # Data classes
    "TransactionState",
    "ExecutionSession",
    # Factory functions
    "create_session",
]
