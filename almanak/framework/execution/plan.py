"""Execution Plan Dataclasses for Cross-Chain Coordination.

This module provides dataclasses for representing execution plans for
cross-chain transactions. Plans enable:
- Deterministic replay of multi-step operations
- Resumability after interruptions
- Step-level status tracking
- Remediation handling for failures

Key Components:
    - StepStatus: Enum tracking step execution states
    - RemediationAction: Enum for failure recovery options
    - StepArtifacts: Dataclass for step execution artifacts
    - PlanStep: Dataclass representing a single execution step
    - PlanBundle: Dataclass representing a complete execution plan

Example:
    from almanak.framework.execution.plan import (
        PlanBundle,
        PlanStep,
        StepStatus,
        RemediationAction,
        StepArtifacts,
    )

    # Create steps for a cross-chain operation
    step1 = PlanStep(
        step_id="step-001",
        chain="base",
        intent=swap_intent,
        dependencies=[],
        status=StepStatus.PENDING,
    )

    step2 = PlanStep(
        step_id="step-002",
        chain="arbitrum",
        intent=supply_intent,
        dependencies=["step-001"],
        status=StepStatus.PENDING,
    )

    # Create the plan bundle
    plan = PlanBundle(
        plan_id="plan-001",
        steps=[step1, step2],
    )
"""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

# =============================================================================
# Enums
# =============================================================================


class StepStatus(Enum):
    """Status of an execution plan step.

    States:
        PENDING: Step not yet started
        SUBMITTING: Transaction being submitted
        SUBMITTED: Transaction submitted, awaiting inclusion
        CONFIRMING: Transaction included, awaiting confirmations
        CONFIRMED: Transaction confirmed on chain
        COMPLETED: Step fully completed (including any bridge fills)
        FAILED: Step failed (may require remediation)
        STUCK: Step stuck and requires operator intervention
    """

    PENDING = "pending"
    SUBMITTING = "submitting"
    SUBMITTED = "submitted"
    CONFIRMING = "confirming"
    CONFIRMED = "confirmed"
    COMPLETED = "completed"
    FAILED = "failed"
    STUCK = "stuck"


class RemediationAction(Enum):
    """Available actions for handling step failures.

    Actions:
        RETRY: Retry the failed step with same parameters
        BRIDGE_BACK: Bridge assets back to source chain
        SWAP_TO_STABLE: Convert assets to stablecoin for safety
        HOLD: Maintain current position, take no action
        OPERATOR_INTERVENTION: Escalate to operator for manual handling
    """

    RETRY = "retry"
    BRIDGE_BACK = "bridge_back"
    SWAP_TO_STABLE = "swap_to_stable"
    HOLD = "hold"
    OPERATOR_INTERVENTION = "operator_intervention"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class StepArtifacts:
    """Artifacts produced during step execution.

    Contains all transaction hashes, bridge identifiers, and other
    data needed to verify and resume execution.

    Attributes:
        tx_hash: Transaction hash on the execution chain
        bridge_deposit_id: Bridge-specific deposit identifier (for bridge steps)
        relay_id: Relayer-specific identifier (for bridge steps)
        destination_credit_tx: Transaction hash of credit on destination chain
        pinned_quote: Frozen quote data at plan creation time
        pinned_at: Timestamp when quote was pinned (for staleness check)
        quote_hash: Hash of pinned quote parameters for verification
        actual_amount_received: Actual amount received after slippage/fees
        gas_used: Actual gas consumed by the transaction
        effective_gas_price: Gas price paid for the transaction
        block_number: Block number where transaction was included
        confirmed_at: Timestamp when transaction was confirmed
    """

    tx_hash: str | None = None
    bridge_deposit_id: str | None = None
    relay_id: str | None = None
    destination_credit_tx: str | None = None
    pinned_quote: dict[str, Any] | None = None
    pinned_at: datetime | None = None
    quote_hash: str | None = None
    actual_amount_received: Decimal | None = None
    gas_used: int | None = None
    effective_gas_price: int | None = None
    block_number: int | None = None
    confirmed_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "tx_hash": self.tx_hash,
            "bridge_deposit_id": self.bridge_deposit_id,
            "relay_id": self.relay_id,
            "destination_credit_tx": self.destination_credit_tx,
            "pinned_quote": self.pinned_quote,
            "pinned_at": self.pinned_at.isoformat() if self.pinned_at is not None else None,
            "quote_hash": self.quote_hash,
            "actual_amount_received": str(self.actual_amount_received)
            if self.actual_amount_received is not None
            else None,
            "gas_used": self.gas_used,
            "effective_gas_price": self.effective_gas_price,
            "block_number": self.block_number,
            "confirmed_at": self.confirmed_at.isoformat() if self.confirmed_at is not None else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StepArtifacts":
        """Create StepArtifacts from dictionary."""
        return cls(
            tx_hash=data.get("tx_hash"),
            bridge_deposit_id=data.get("bridge_deposit_id"),
            relay_id=data.get("relay_id"),
            destination_credit_tx=data.get("destination_credit_tx"),
            pinned_quote=data.get("pinned_quote"),
            pinned_at=datetime.fromisoformat(data["pinned_at"]) if data.get("pinned_at") is not None else None,
            quote_hash=data.get("quote_hash"),
            actual_amount_received=Decimal(data["actual_amount_received"])
            if data.get("actual_amount_received") is not None
            else None,
            gas_used=data.get("gas_used"),
            effective_gas_price=data.get("effective_gas_price"),
            block_number=data.get("block_number"),
            confirmed_at=datetime.fromisoformat(data["confirmed_at"]) if data.get("confirmed_at") is not None else None,
        )

    @property
    def has_tx(self) -> bool:
        """Check if a transaction hash is available."""
        return self.tx_hash is not None

    @property
    def is_bridge_step(self) -> bool:
        """Check if this is a bridge step based on artifacts."""
        return self.bridge_deposit_id is not None

    @property
    def bridge_completed(self) -> bool:
        """Check if bridge transfer is complete."""
        return self.is_bridge_step and self.destination_credit_tx is not None

    @property
    def has_pinned_quote(self) -> bool:
        """Check if a quote is pinned."""
        return self.pinned_quote is not None and self.pinned_at is not None

    def is_quote_stale(self, stale_threshold_seconds: int = 300) -> bool:
        """Check if pinned quote is stale.

        A quote is considered stale if it was pinned more than
        `stale_threshold_seconds` ago (default 5 minutes).

        Args:
            stale_threshold_seconds: Threshold in seconds (default 300 = 5 min)

        Returns:
            True if quote is stale or not pinned
        """
        if not self.has_pinned_quote or self.pinned_at is None:
            return True
        elapsed = (datetime.now(UTC) - self.pinned_at).total_seconds()
        return elapsed > stale_threshold_seconds

    def quote_age_seconds(self) -> float | None:
        """Get age of pinned quote in seconds.

        Returns:
            Age in seconds, or None if no quote pinned
        """
        if self.pinned_at is None:
            return None
        return (datetime.now(UTC) - self.pinned_at).total_seconds()

    @staticmethod
    def compute_quote_hash(quote_data: dict[str, Any]) -> str:
        """Compute deterministic hash of quote parameters.

        The hash includes key quote parameters for verification that
        the quote hasn't changed. This is used to detect if a re-quoted
        quote differs from the original pinned quote.

        Args:
            quote_data: Quote dictionary to hash

        Returns:
            Hex string hash (first 16 chars of SHA-256)
        """
        # Extract key parameters that affect the quote
        hash_params = {
            "bridge_name": quote_data.get("bridge_name"),
            "token": quote_data.get("token"),
            "input_amount": str(quote_data.get("input_amount")),
            "from_chain": quote_data.get("from_chain"),
            "to_chain": quote_data.get("to_chain"),
            "slippage_tolerance": str(quote_data.get("slippage_tolerance")),
        }
        hash_str = json.dumps(hash_params, sort_keys=True)
        return hashlib.sha256(hash_str.encode()).hexdigest()[:16]

    def pin_quote(self, quote_data: dict[str, Any]) -> None:
        """Pin a quote at the current time.

        Stores the quote data and records the timestamp and hash
        for staleness checking and verification.

        Args:
            quote_data: Quote dictionary to pin
        """
        self.pinned_quote = quote_data
        self.pinned_at = datetime.now(UTC)
        self.quote_hash = self.compute_quote_hash(quote_data)

    def verify_quote_hash(self, new_quote_data: dict[str, Any]) -> bool:
        """Verify that a new quote matches the pinned quote hash.

        Used when re-quoting to check if the new quote differs
        significantly from the original pinned quote.

        Args:
            new_quote_data: New quote to verify against pinned

        Returns:
            True if quote hash matches, False otherwise
        """
        if self.quote_hash is None:
            return False
        new_hash = self.compute_quote_hash(new_quote_data)
        return new_hash == self.quote_hash


@dataclass
class PlanStep:
    """A single step in an execution plan.

    Represents one discrete operation (transaction, bridge transfer, etc.)
    within a larger multi-step execution plan.

    Attributes:
        step_id: Unique identifier for this step within the plan
        chain: Chain where this step executes
        intent: The intent being executed (serializable)
        dependencies: List of step_ids that must complete before this step
        status: Current execution status
        artifacts: Execution artifacts (tx hash, receipts, etc.)
        remediation: Configured remediation action on failure
        remediation_intent: Optional serialized intent for remediation (e.g., bridge_back intent)
        retry_count: Number of retries attempted
        max_retries: Maximum retries before escalation
        error_message: Error details if step failed
        started_at: When step execution began
        completed_at: When step execution completed
        description: Human-readable description of the step
    """

    step_id: str
    chain: str
    intent: dict[str, Any]  # Serialized intent
    dependencies: list[str] = field(default_factory=list)
    status: StepStatus = StepStatus.PENDING
    artifacts: StepArtifacts = field(default_factory=StepArtifacts)
    remediation: RemediationAction = RemediationAction.HOLD
    remediation_intent: dict[str, Any] | None = None  # Optional remediation intent
    retry_count: int = 0
    max_retries: int = 3
    error_message: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    description: str | None = None

    def __post_init__(self) -> None:
        """Ensure artifacts is a StepArtifacts instance."""
        if isinstance(self.artifacts, dict):
            self.artifacts = StepArtifacts.from_dict(self.artifacts)

    @property
    def is_pending(self) -> bool:
        """Check if step hasn't started."""
        return self.status == StepStatus.PENDING

    @property
    def is_in_progress(self) -> bool:
        """Check if step is currently executing."""
        return self.status in (
            StepStatus.SUBMITTING,
            StepStatus.SUBMITTED,
            StepStatus.CONFIRMING,
        )

    @property
    def is_complete(self) -> bool:
        """Check if step is in a terminal state."""
        return self.status in (
            StepStatus.COMPLETED,
            StepStatus.FAILED,
            StepStatus.STUCK,
        )

    @property
    def is_success(self) -> bool:
        """Check if step completed successfully."""
        return self.status == StepStatus.COMPLETED

    @property
    def is_failed(self) -> bool:
        """Check if step failed."""
        return self.status in (StepStatus.FAILED, StepStatus.STUCK)

    @property
    def can_retry(self) -> bool:
        """Check if step can be retried."""
        return self.status == StepStatus.FAILED and self.retry_count < self.max_retries

    @property
    def needs_operator(self) -> bool:
        """Check if step requires operator intervention."""
        return self.status == StepStatus.STUCK or self.remediation == RemediationAction.OPERATOR_INTERVENTION

    @property
    def duration(self) -> float | None:
        """Get execution duration in seconds."""
        if self.started_at is None:
            return None
        end_time = self.completed_at or datetime.now(UTC)
        return (end_time - self.started_at).total_seconds()

    def dependencies_satisfied(self, completed_steps: set[str]) -> bool:
        """Check if all dependencies are satisfied.

        Args:
            completed_steps: Set of step_ids that have completed successfully

        Returns:
            True if all dependencies are in completed_steps
        """
        return all(dep in completed_steps for dep in self.dependencies)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "step_id": self.step_id,
            "chain": self.chain,
            "intent": self.intent,
            "dependencies": self.dependencies,
            "status": self.status.value,
            "artifacts": self.artifacts.to_dict(),
            "remediation": self.remediation.value,
            "remediation_intent": self.remediation_intent,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "error_message": self.error_message,
            "started_at": self.started_at.isoformat() if self.started_at is not None else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at is not None else None,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PlanStep":
        """Create PlanStep from dictionary."""
        return cls(
            step_id=data["step_id"],
            chain=data["chain"],
            intent=data["intent"],
            dependencies=data.get("dependencies", []),
            status=StepStatus(data.get("status", "pending")),
            artifacts=StepArtifacts.from_dict(data.get("artifacts", {})),
            remediation=RemediationAction(data.get("remediation", "hold")),
            remediation_intent=data.get("remediation_intent"),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
            error_message=data.get("error_message"),
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            description=data.get("description"),
        )


@dataclass
class PlanBundle:
    """A complete execution plan with multiple steps.

    Represents a deterministic execution plan for multi-step,
    potentially cross-chain operations. Plans can be persisted
    and resumed for fault tolerance.

    Attributes:
        plan_id: Unique identifier for this plan
        steps: List of steps in the plan
        execution_order: Ordered list of step_ids for execution
        plan_hash: Hash of plan parameters for verification
        created_at: When plan was created
        started_at: When plan execution began
        completed_at: When plan execution completed
        strategy_id: Associated strategy identifier
        description: Human-readable plan description
        metadata: Additional plan metadata
    """

    plan_id: str
    steps: list[PlanStep] = field(default_factory=list)
    execution_order: list[str] = field(default_factory=list)
    plan_hash: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    strategy_id: str | None = None
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Compute execution order and plan hash if not provided."""
        # Convert dict steps to PlanStep instances
        self.steps = [PlanStep.from_dict(s) if isinstance(s, dict) else s for s in self.steps]

        # Compute execution order if not provided
        if not self.execution_order and self.steps:
            self.execution_order = self._compute_execution_order()

        # Compute plan hash if not provided
        if self.plan_hash is None and self.steps:
            self.plan_hash = self._compute_plan_hash()

    def _compute_execution_order(self) -> list[str]:
        """Compute topological execution order based on dependencies.

        Returns:
            Ordered list of step_ids respecting dependencies
        """
        # Build dependency graph
        step_map = {step.step_id: step for step in self.steps}
        remaining = set(step_map.keys())
        order: list[str] = []

        while remaining:
            # Find steps with no unsatisfied dependencies
            ready = [
                step_id for step_id in remaining if all(dep not in remaining for dep in step_map[step_id].dependencies)
            ]

            if not ready:
                # Circular dependency detected
                raise ValueError(f"Circular dependency detected in plan. Remaining: {remaining}")

            # Sort ready steps for determinism (by step_id)
            ready.sort()
            order.extend(ready)
            remaining -= set(ready)

        return order

    def _compute_plan_hash(self) -> str:
        """Compute deterministic hash of plan parameters.

        The hash includes:
        - Step IDs and their execution order
        - Intent data for each step
        - Pinned quotes (if any)

        Returns:
            Hex string hash of plan parameters
        """
        hash_data = {
            "plan_id": self.plan_id,
            "execution_order": self.execution_order,
            "steps": [
                {
                    "step_id": step.step_id,
                    "chain": step.chain,
                    "intent": step.intent,
                    "dependencies": step.dependencies,
                    "pinned_quote": step.artifacts.pinned_quote,
                }
                for step in self.steps
            ],
        }
        hash_str = json.dumps(hash_data, sort_keys=True, default=str)
        return hashlib.sha256(hash_str.encode()).hexdigest()[:16]

    @property
    def step_count(self) -> int:
        """Get number of steps in the plan."""
        return len(self.steps)

    @property
    def completed_step_count(self) -> int:
        """Get number of completed steps."""
        return sum(1 for step in self.steps if step.is_success)

    @property
    def failed_step_count(self) -> int:
        """Get number of failed steps."""
        return sum(1 for step in self.steps if step.is_failed)

    @property
    def pending_step_count(self) -> int:
        """Get number of pending steps."""
        return sum(1 for step in self.steps if step.is_pending)

    @property
    def progress(self) -> float:
        """Get plan progress as percentage (0.0 to 1.0)."""
        if not self.steps:
            return 1.0
        return self.completed_step_count / len(self.steps)

    @property
    def is_complete(self) -> bool:
        """Check if all steps are in terminal state."""
        return all(step.is_complete for step in self.steps)

    @property
    def is_success(self) -> bool:
        """Check if all steps completed successfully."""
        return all(step.is_success for step in self.steps)

    @property
    def is_failed(self) -> bool:
        """Check if any step has failed."""
        return any(step.is_failed for step in self.steps)

    @property
    def needs_operator(self) -> bool:
        """Check if any step requires operator intervention."""
        return any(step.needs_operator for step in self.steps)

    @property
    def chains_involved(self) -> set[str]:
        """Get set of all chains involved in the plan."""
        return {step.chain for step in self.steps}

    @property
    def duration(self) -> float | None:
        """Get plan execution duration in seconds."""
        if self.started_at is None:
            return None
        end_time = self.completed_at or datetime.now(UTC)
        return (end_time - self.started_at).total_seconds()

    def get_step(self, step_id: str) -> PlanStep | None:
        """Get a step by ID.

        Args:
            step_id: Step identifier

        Returns:
            PlanStep if found, None otherwise
        """
        for step in self.steps:
            if step.step_id == step_id:
                return step
        return None

    def get_next_step(self) -> PlanStep | None:
        """Get next step to execute based on execution order.

        Returns:
            Next pending step with satisfied dependencies, or None
        """
        completed = {step.step_id for step in self.steps if step.is_success}

        for step_id in self.execution_order:
            step = self.get_step(step_id)
            if step and step.is_pending and step.dependencies_satisfied(completed):
                return step

        return None

    def get_steps_by_chain(self, chain: str) -> list[PlanStep]:
        """Get all steps for a specific chain.

        Args:
            chain: Chain identifier

        Returns:
            List of steps executing on that chain
        """
        return [step for step in self.steps if step.chain == chain]

    def get_failed_steps(self) -> list[PlanStep]:
        """Get all failed steps.

        Returns:
            List of steps in FAILED or STUCK status
        """
        return [step for step in self.steps if step.is_failed]

    def get_completed_steps(self) -> list[PlanStep]:
        """Get all completed steps.

        Returns:
            List of steps in COMPLETED status
        """
        return [step for step in self.steps if step.is_success]

    def get_steps_with_pinned_quotes(self) -> list[PlanStep]:
        """Get all steps that have pinned quotes.

        Returns:
            List of steps with pinned quotes (typically bridge steps)
        """
        return [step for step in self.steps if step.artifacts.has_pinned_quote]

    def get_stale_quote_steps(self, stale_threshold_seconds: int = 300) -> list[PlanStep]:
        """Get all steps with stale pinned quotes.

        Args:
            stale_threshold_seconds: Threshold for staleness (default 5 min)

        Returns:
            List of pending steps with stale quotes
        """
        return [
            step
            for step in self.steps
            if step.is_pending
            and step.artifacts.has_pinned_quote
            and step.artifacts.is_quote_stale(stale_threshold_seconds)
        ]

    def verify_plan_integrity(self) -> bool:
        """Verify plan hash matches current plan state.

        Recomputes the plan hash and compares against stored hash
        to detect if the plan has been modified.

        Returns:
            True if plan hash matches, False if modified or no hash
        """
        if self.plan_hash is None:
            return False
        current_hash = self._compute_plan_hash()
        return current_hash == self.plan_hash

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "plan_id": self.plan_id,
            "steps": [step.to_dict() for step in self.steps],
            "execution_order": self.execution_order,
            "plan_hash": self.plan_hash,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at is not None else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at is not None else None,
            "strategy_id": self.strategy_id,
            "description": self.description,
            "metadata": self.metadata,
            "step_count": self.step_count,
            "completed_step_count": self.completed_step_count,
            "failed_step_count": self.failed_step_count,
            "progress": self.progress,
            "is_complete": self.is_complete,
            "is_success": self.is_success,
            "chains_involved": list(self.chains_involved),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PlanBundle":
        """Create PlanBundle from dictionary."""
        return cls(
            plan_id=data["plan_id"],
            steps=[PlanStep.from_dict(s) for s in data.get("steps", [])],
            execution_order=data.get("execution_order", []),
            plan_hash=data.get("plan_hash"),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now(UTC),
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            strategy_id=data.get("strategy_id"),
            description=data.get("description"),
            metadata=data.get("metadata", {}),
        )


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Enums
    "StepStatus",
    "RemediationAction",
    # Data classes
    "StepArtifacts",
    "PlanStep",
    "PlanBundle",
]
