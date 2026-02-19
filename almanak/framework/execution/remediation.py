"""Cross-Chain Remediation Logic for Failed Operations.

This module provides the remediation state machine and operator card generation
for handling failures in multi-step cross-chain operations.

Key Components:
    - RemediationState: State machine states for remediation handling
    - RemediationStateMachine: Handles state transitions and remediation logic
    - OperatorCard: Generated alert card for stuck operations requiring intervention
    - RemediationResult: Result of executing a remediation action

Remediation Options:
    - RETRY: Retry the failed step with same parameters
    - BRIDGE_BACK: Bridge assets back to source chain
    - SWAP_TO_STABLE: Convert assets to stablecoin for safety
    - HOLD: Maintain current position, take no action
    - OPERATOR_INTERVENTION: Escalate to operator for manual handling

Example:
    from almanak.framework.execution.remediation import (
        RemediationStateMachine,
        OperatorCard,
    )

    # Create state machine for a plan
    state_machine = RemediationStateMachine(
        plan=plan,
        risk_guard=risk_guard,
    )

    # Handle a failed step
    result = await state_machine.handle_failure(failed_step)

    # If operator intervention needed
    if result.requires_operator:
        card = state_machine.generate_operator_card(failed_step)
        alert_operator(card)
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol

from almanak.framework.execution.plan import (
    PlanBundle,
    PlanStep,
    RemediationAction,
    StepStatus,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================


class RemediationState(StrEnum):
    """State machine states for remediation handling."""

    # Initial states
    PENDING = "pending"  # Step failed, remediation pending
    EVALUATING = "evaluating"  # Evaluating remediation options

    # Active remediation states
    RETRYING = "retrying"  # Retry attempt in progress
    BRIDGING_BACK = "bridging_back"  # Bridge back in progress
    SWAPPING_TO_STABLE = "swapping_to_stable"  # Swap to stable in progress
    HOLDING = "holding"  # No action, position held

    # Terminal states
    RESOLVED = "resolved"  # Remediation successful
    ESCALATED = "escalated"  # Escalated to operator
    ABANDONED = "abandoned"  # Remediation abandoned (manual resolution)


class RemediationTrigger(StrEnum):
    """Triggers for remediation state transitions."""

    STEP_FAILED = "step_failed"
    RETRIES_EXHAUSTED = "retries_exhausted"
    REMEDIATION_STARTED = "remediation_started"
    REMEDIATION_SUCCEEDED = "remediation_succeeded"
    REMEDIATION_FAILED = "remediation_failed"
    OPERATOR_RESOLVED = "operator_resolved"
    OPERATOR_ABANDONED = "operator_abandoned"
    RISK_CHECK_FAILED = "risk_check_failed"


class OperatorCardStatus(StrEnum):
    """Status of an operator card."""

    PENDING = "pending"  # Awaiting operator action
    ACKNOWLEDGED = "acknowledged"  # Operator has acknowledged
    IN_PROGRESS = "in_progress"  # Operator is working on it
    RESOLVED = "resolved"  # Operator resolved the issue
    ABANDONED = "abandoned"  # Operator marked as abandoned


class OperatorCardPriority(StrEnum):
    """Priority levels for operator cards."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


# =============================================================================
# Protocols
# =============================================================================


class RiskGuardProtocol(Protocol):
    """Protocol for risk validation during remediation."""

    def validate_intent(self, intent: dict[str, Any]) -> "RiskValidationResult":
        """Validate an intent against risk rules."""
        ...


@dataclass
class RiskValidationResult:
    """Result of risk validation."""

    allowed: bool = True
    violations: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class RemediationResult:
    """Result of a remediation attempt.

    Attributes:
        success: Whether remediation succeeded
        state: Current remediation state
        action_taken: Remediation action that was taken
        new_tx_hash: Transaction hash if remediation created new tx
        error: Error message if remediation failed
        requires_operator: Whether operator intervention is needed
        artifacts: Any artifacts produced during remediation
        timestamp: When remediation was attempted
    """

    success: bool
    state: RemediationState
    action_taken: RemediationAction
    new_tx_hash: str | None = None
    error: str | None = None
    requires_operator: bool = False
    artifacts: dict[str, Any] | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "state": self.state.value,
            "action_taken": self.action_taken.value,
            "new_tx_hash": self.new_tx_hash,
            "error": self.error,
            "requires_operator": self.requires_operator,
            "artifacts": self.artifacts,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class OperatorCard:
    """Alert card for stuck cross-chain operations requiring operator intervention.

    Operator cards are generated when automated remediation cannot resolve
    a failed operation and human intervention is required.

    Attributes:
        card_id: Unique identifier for this card
        plan_id: Associated plan identifier
        step_id: Failed step identifier
        status: Current card status
        priority: Card priority level
        title: Brief title describing the issue
        description: Detailed description of the problem
        chain: Chain where failure occurred
        affected_assets: List of assets affected by the failure
        estimated_value_at_risk_usd: Estimated USD value at risk
        failure_reason: Why the step failed
        attempted_remediation: What remediation was attempted
        recommended_actions: List of recommended operator actions
        context: Additional context for the operator
        created_at: When card was created
        acknowledged_at: When operator acknowledged
        resolved_at: When issue was resolved
        resolved_by: Who resolved the issue
        resolution_notes: Notes from the resolver
    """

    card_id: str
    plan_id: str
    step_id: str
    status: OperatorCardStatus = OperatorCardStatus.PENDING
    priority: OperatorCardPriority = OperatorCardPriority.HIGH
    title: str = ""
    description: str = ""
    chain: str = ""
    affected_assets: list[dict[str, Any]] = field(default_factory=list)
    estimated_value_at_risk_usd: Decimal | None = None
    failure_reason: str = ""
    attempted_remediation: str | None = None
    recommended_actions: list[str] = field(default_factory=list)
    context: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    acknowledged_at: datetime | None = None
    resolved_at: datetime | None = None
    resolved_by: str | None = None
    resolution_notes: str | None = None

    def acknowledge(self) -> None:
        """Mark card as acknowledged by operator."""
        self.status = OperatorCardStatus.ACKNOWLEDGED
        self.acknowledged_at = datetime.now(UTC)

    def mark_in_progress(self) -> None:
        """Mark card as in progress."""
        self.status = OperatorCardStatus.IN_PROGRESS

    def resolve(self, resolved_by: str, notes: str | None = None) -> None:
        """Mark card as resolved.

        Args:
            resolved_by: Who resolved the issue
            notes: Optional resolution notes
        """
        self.status = OperatorCardStatus.RESOLVED
        self.resolved_at = datetime.now(UTC)
        self.resolved_by = resolved_by
        self.resolution_notes = notes

    def abandon(self, resolved_by: str, notes: str | None = None) -> None:
        """Mark card as abandoned.

        Args:
            resolved_by: Who abandoned the issue
            notes: Optional notes explaining why
        """
        self.status = OperatorCardStatus.ABANDONED
        self.resolved_at = datetime.now(UTC)
        self.resolved_by = resolved_by
        self.resolution_notes = notes

    @property
    def age_seconds(self) -> float:
        """Get age of card in seconds."""
        return (datetime.now(UTC) - self.created_at).total_seconds()

    @property
    def is_pending(self) -> bool:
        """Check if card is pending action."""
        return self.status == OperatorCardStatus.PENDING

    @property
    def is_resolved(self) -> bool:
        """Check if card is resolved."""
        return self.status in (OperatorCardStatus.RESOLVED, OperatorCardStatus.ABANDONED)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "card_id": self.card_id,
            "plan_id": self.plan_id,
            "step_id": self.step_id,
            "status": self.status.value,
            "priority": self.priority.value,
            "title": self.title,
            "description": self.description,
            "chain": self.chain,
            "affected_assets": self.affected_assets,
            "estimated_value_at_risk_usd": str(self.estimated_value_at_risk_usd)
            if self.estimated_value_at_risk_usd is not None
            else None,
            "failure_reason": self.failure_reason,
            "attempted_remediation": self.attempted_remediation,
            "recommended_actions": self.recommended_actions,
            "context": self.context,
            "created_at": self.created_at.isoformat(),
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at is not None else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at is not None else None,
            "resolved_by": self.resolved_by,
            "resolution_notes": self.resolution_notes,
            "age_seconds": self.age_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OperatorCard":
        """Create OperatorCard from dictionary."""
        return cls(
            card_id=data["card_id"],
            plan_id=data["plan_id"],
            step_id=data["step_id"],
            status=OperatorCardStatus(data.get("status", "pending")),
            priority=OperatorCardPriority(data.get("priority", "high")),
            title=data.get("title", ""),
            description=data.get("description", ""),
            chain=data.get("chain", ""),
            affected_assets=data.get("affected_assets", []),
            estimated_value_at_risk_usd=Decimal(data["estimated_value_at_risk_usd"])
            if data.get("estimated_value_at_risk_usd") is not None
            else None,
            failure_reason=data.get("failure_reason", ""),
            attempted_remediation=data.get("attempted_remediation"),
            recommended_actions=data.get("recommended_actions", []),
            context=data.get("context", {}),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now(UTC),
            acknowledged_at=datetime.fromisoformat(data["acknowledged_at"]) if data.get("acknowledged_at") else None,
            resolved_at=datetime.fromisoformat(data["resolved_at"]) if data.get("resolved_at") else None,
            resolved_by=data.get("resolved_by"),
            resolution_notes=data.get("resolution_notes"),
        )


@dataclass
class RemediationStateRecord:
    """Record of remediation state transition.

    Attributes:
        step_id: Step being remediated
        from_state: Previous state
        to_state: New state
        trigger: What triggered the transition
        timestamp: When transition occurred
        details: Additional details
    """

    step_id: str
    from_state: RemediationState
    to_state: RemediationState
    trigger: RemediationTrigger
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    details: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "step_id": self.step_id,
            "from_state": self.from_state.value,
            "to_state": self.to_state.value,
            "trigger": self.trigger.value,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details,
        }


# =============================================================================
# Remediation State Machine
# =============================================================================


class RemediationStateMachine:
    """State machine for handling cross-chain remediation.

    The state machine manages the remediation process for failed steps:
    1. Evaluates failure and determines appropriate remediation
    2. Executes remediation actions (retry, bridge back, swap to stable, etc.)
    3. Validates remediation through risk checks
    4. Escalates to operator if automated remediation fails

    State Transitions:
        PENDING -> EVALUATING (on failure detection)
        EVALUATING -> RETRYING (if retry chosen and retries available)
        EVALUATING -> BRIDGING_BACK (if bridge_back chosen)
        EVALUATING -> SWAPPING_TO_STABLE (if swap_to_stable chosen)
        EVALUATING -> HOLDING (if hold chosen)
        EVALUATING -> ESCALATED (if operator_intervention chosen)
        RETRYING -> RESOLVED (if retry succeeds)
        RETRYING -> EVALUATING (if retry fails)
        BRIDGING_BACK -> RESOLVED (if bridge back succeeds)
        BRIDGING_BACK -> ESCALATED (if bridge back fails)
        SWAPPING_TO_STABLE -> RESOLVED (if swap succeeds)
        SWAPPING_TO_STABLE -> ESCALATED (if swap fails)
        ESCALATED -> RESOLVED (operator resolves)
        ESCALATED -> ABANDONED (operator abandons)

    Example:
        state_machine = RemediationStateMachine(plan, risk_guard)

        # Handle failure
        result = await state_machine.handle_failure(failed_step)

        if result.requires_operator:
            card = state_machine.generate_operator_card(failed_step)
    """

    # Valid state transitions
    VALID_TRANSITIONS: dict[RemediationState, list[RemediationState]] = {
        RemediationState.PENDING: [RemediationState.EVALUATING],
        RemediationState.EVALUATING: [
            RemediationState.RETRYING,
            RemediationState.BRIDGING_BACK,
            RemediationState.SWAPPING_TO_STABLE,
            RemediationState.HOLDING,
            RemediationState.ESCALATED,
        ],
        RemediationState.RETRYING: [
            RemediationState.RESOLVED,
            RemediationState.EVALUATING,
        ],
        RemediationState.BRIDGING_BACK: [
            RemediationState.RESOLVED,
            RemediationState.ESCALATED,
        ],
        RemediationState.SWAPPING_TO_STABLE: [
            RemediationState.RESOLVED,
            RemediationState.ESCALATED,
        ],
        RemediationState.HOLDING: [
            RemediationState.RESOLVED,
        ],
        RemediationState.ESCALATED: [
            RemediationState.RESOLVED,
            RemediationState.ABANDONED,
        ],
        RemediationState.RESOLVED: [],  # Terminal state
        RemediationState.ABANDONED: [],  # Terminal state
    }

    def __init__(
        self,
        plan: PlanBundle,
        risk_guard: RiskGuardProtocol | None = None,
        intent_executor: Any | None = None,
    ) -> None:
        """Initialize the remediation state machine.

        Args:
            plan: The plan being executed
            risk_guard: Optional risk guard for validating remediation
            intent_executor: Optional executor for running remediation intents
        """
        self._plan = plan
        self._risk_guard = risk_guard
        self._intent_executor = intent_executor

        # Track state per step
        self._step_states: dict[str, RemediationState] = {}
        self._state_history: list[RemediationStateRecord] = []
        self._operator_cards: dict[str, OperatorCard] = {}

        logger.info(f"RemediationStateMachine initialized for plan {plan.plan_id}")

    @property
    def plan(self) -> PlanBundle:
        """Get the associated plan."""
        return self._plan

    @property
    def operator_cards(self) -> dict[str, OperatorCard]:
        """Get all operator cards."""
        return self._operator_cards

    def get_state(self, step_id: str) -> RemediationState:
        """Get current remediation state for a step.

        Args:
            step_id: Step identifier

        Returns:
            Current remediation state (PENDING if not tracked)
        """
        return self._step_states.get(step_id, RemediationState.PENDING)

    def get_state_history(self, step_id: str | None = None) -> list[RemediationStateRecord]:
        """Get state transition history.

        Args:
            step_id: Optional step to filter by

        Returns:
            List of state transition records
        """
        if step_id is None:
            return self._state_history

        return [r for r in self._state_history if r.step_id == step_id]

    def _transition(
        self,
        step_id: str,
        to_state: RemediationState,
        trigger: RemediationTrigger,
        details: str | None = None,
    ) -> bool:
        """Transition step to new state.

        Args:
            step_id: Step to transition
            to_state: Target state
            trigger: Trigger for transition
            details: Optional details

        Returns:
            True if transition was valid and executed
        """
        from_state = self.get_state(step_id)

        # Check if transition is valid
        valid_targets = self.VALID_TRANSITIONS.get(from_state, [])
        if to_state not in valid_targets:
            logger.warning(f"Invalid transition for step {step_id}: {from_state} -> {to_state}")
            return False

        # Record transition
        record = RemediationStateRecord(
            step_id=step_id,
            from_state=from_state,
            to_state=to_state,
            trigger=trigger,
            details=details,
        )
        self._state_history.append(record)
        self._step_states[step_id] = to_state

        logger.info(
            f"Remediation state transition: step={step_id}, "
            f"{from_state.value} -> {to_state.value} (trigger={trigger.value})"
        )

        return True

    async def handle_failure(
        self,
        step: PlanStep,
    ) -> RemediationResult:
        """Handle a failed step by executing appropriate remediation.

        This is the main entry point for remediation. It:
        1. Transitions to EVALUATING state
        2. Determines remediation action based on step configuration
        3. Validates remediation through risk checks
        4. Executes remediation or escalates to operator

        Args:
            step: The failed step to remediate

        Returns:
            RemediationResult with outcome
        """
        step_id = step.step_id

        # Transition to evaluating
        self._transition(step_id, RemediationState.EVALUATING, RemediationTrigger.STEP_FAILED)

        # Check if retries are available
        if step.can_retry and step.remediation != RemediationAction.OPERATOR_INTERVENTION:
            return await self._attempt_retry(step)

        # Retries exhausted - use configured remediation
        self._transition(
            step_id,
            RemediationState.EVALUATING,
            RemediationTrigger.RETRIES_EXHAUSTED,
        )

        return await self._execute_remediation(step)

    async def _attempt_retry(self, step: PlanStep) -> RemediationResult:
        """Attempt to retry a failed step.

        Args:
            step: Step to retry

        Returns:
            RemediationResult
        """
        step_id = step.step_id

        # Transition to retrying
        self._transition(
            step_id,
            RemediationState.RETRYING,
            RemediationTrigger.REMEDIATION_STARTED,
        )

        # Increment retry count
        step.retry_count += 1

        # Execute retry (in real implementation, this would call the executor)
        if self._intent_executor is not None:
            try:
                # Execute the original intent
                result = await self._intent_executor.execute_intent(step.intent)

                if result.get("success", False):
                    # Retry succeeded
                    step.status = StepStatus.COMPLETED
                    step.artifacts.tx_hash = result.get("tx_hash")
                    step.artifacts.actual_amount_received = result.get("amount_received")

                    self._transition(
                        step_id,
                        RemediationState.RESOLVED,
                        RemediationTrigger.REMEDIATION_SUCCEEDED,
                    )

                    logger.info(f"Retry succeeded for step {step_id}")

                    return RemediationResult(
                        success=True,
                        state=RemediationState.RESOLVED,
                        action_taken=RemediationAction.RETRY,
                        new_tx_hash=result.get("tx_hash"),
                    )
                else:
                    raise Exception(result.get("error", "Unknown error"))

            except Exception as e:
                logger.warning(f"Retry {step.retry_count} failed for step {step_id}: {e}")

                # Check if more retries available
                if step.can_retry:
                    self._transition(
                        step_id,
                        RemediationState.EVALUATING,
                        RemediationTrigger.REMEDIATION_FAILED,
                        details=str(e),
                    )
                    # Recursive retry
                    return await self._attempt_retry(step)
                else:
                    # Move to configured remediation
                    self._transition(
                        step_id,
                        RemediationState.EVALUATING,
                        RemediationTrigger.RETRIES_EXHAUSTED,
                    )
                    return await self._execute_remediation(step)

        # No executor - simulate success for testing
        logger.debug(f"No executor configured, simulating retry for step {step_id}")

        return RemediationResult(
            success=False,
            state=RemediationState.EVALUATING,
            action_taken=RemediationAction.RETRY,
            error="No executor configured",
        )

    async def _execute_remediation(self, step: PlanStep) -> RemediationResult:
        """Execute configured remediation action for a step.

        Args:
            step: Step to remediate

        Returns:
            RemediationResult
        """
        step_id = step.step_id
        action = step.remediation

        logger.info(f"Executing remediation {action.value} for step {step_id}")

        if action == RemediationAction.HOLD:
            return await self._execute_hold(step)
        elif action == RemediationAction.BRIDGE_BACK:
            return await self._execute_bridge_back(step)
        elif action == RemediationAction.SWAP_TO_STABLE:
            return await self._execute_swap_to_stable(step)
        elif action == RemediationAction.OPERATOR_INTERVENTION:
            return await self._escalate_to_operator(step)
        else:  # RETRY already handled
            return await self._escalate_to_operator(step)

    async def _execute_hold(self, step: PlanStep) -> RemediationResult:
        """Execute HOLD remediation - maintain current position.

        Args:
            step: Step to hold

        Returns:
            RemediationResult
        """
        step_id = step.step_id

        self._transition(
            step_id,
            RemediationState.HOLDING,
            RemediationTrigger.REMEDIATION_STARTED,
        )

        # HOLD is a no-op - just mark as resolved
        self._transition(
            step_id,
            RemediationState.RESOLVED,
            RemediationTrigger.REMEDIATION_SUCCEEDED,
            details="Position held, no action taken",
        )

        return RemediationResult(
            success=True,
            state=RemediationState.RESOLVED,
            action_taken=RemediationAction.HOLD,
        )

    async def _execute_bridge_back(self, step: PlanStep) -> RemediationResult:
        """Execute BRIDGE_BACK remediation - bridge assets back to source.

        Args:
            step: Step with bridge_back remediation

        Returns:
            RemediationResult
        """
        step_id = step.step_id

        self._transition(
            step_id,
            RemediationState.BRIDGING_BACK,
            RemediationTrigger.REMEDIATION_STARTED,
        )

        # Get remediation intent if configured
        remediation_intent = step.remediation_intent

        if remediation_intent is None:
            # No remediation intent configured
            logger.warning(f"No remediation_intent for BRIDGE_BACK on step {step_id}")
            return await self._escalate_to_operator(
                step,
                reason="No remediation intent configured for BRIDGE_BACK",
            )

        # Validate through risk guard
        if self._risk_guard is not None:
            validation = self._risk_guard.validate_intent(remediation_intent)
            if not validation.allowed:
                logger.warning(f"Bridge back for step {step_id} blocked by risk guard: {validation.violations}")
                return await self._escalate_to_operator(
                    step,
                    reason=f"Risk check failed: {', '.join(validation.violations)}",
                )

        # Execute bridge back (with executor if available)
        if self._intent_executor is not None:
            try:
                result = await self._intent_executor.execute_intent(remediation_intent)

                if result.get("success", False):
                    self._transition(
                        step_id,
                        RemediationState.RESOLVED,
                        RemediationTrigger.REMEDIATION_SUCCEEDED,
                    )

                    return RemediationResult(
                        success=True,
                        state=RemediationState.RESOLVED,
                        action_taken=RemediationAction.BRIDGE_BACK,
                        new_tx_hash=result.get("tx_hash"),
                        artifacts={"remediation_intent": remediation_intent},
                    )
                else:
                    raise Exception(result.get("error", "Unknown error"))

            except Exception as e:
                logger.error(f"Bridge back failed for step {step_id}: {e}")
                return await self._escalate_to_operator(
                    step,
                    reason=f"Bridge back execution failed: {e}",
                )

        # No executor - simulate for testing
        logger.debug(f"No executor configured, simulating bridge_back for step {step_id}")

        self._transition(
            step_id,
            RemediationState.RESOLVED,
            RemediationTrigger.REMEDIATION_SUCCEEDED,
        )

        return RemediationResult(
            success=True,
            state=RemediationState.RESOLVED,
            action_taken=RemediationAction.BRIDGE_BACK,
            artifacts={"remediation_intent": remediation_intent},
        )

    async def _execute_swap_to_stable(self, step: PlanStep) -> RemediationResult:
        """Execute SWAP_TO_STABLE remediation - convert to stablecoin.

        Args:
            step: Step with swap_to_stable remediation

        Returns:
            RemediationResult
        """
        step_id = step.step_id

        self._transition(
            step_id,
            RemediationState.SWAPPING_TO_STABLE,
            RemediationTrigger.REMEDIATION_STARTED,
        )

        # Get remediation intent if configured
        remediation_intent = step.remediation_intent

        if remediation_intent is None:
            logger.warning(f"No remediation_intent for SWAP_TO_STABLE on step {step_id}")
            return await self._escalate_to_operator(
                step,
                reason="No remediation intent configured for SWAP_TO_STABLE",
            )

        # Validate through risk guard
        if self._risk_guard is not None:
            validation = self._risk_guard.validate_intent(remediation_intent)
            if not validation.allowed:
                logger.warning(f"Swap to stable for step {step_id} blocked by risk guard: {validation.violations}")
                return await self._escalate_to_operator(
                    step,
                    reason=f"Risk check failed: {', '.join(validation.violations)}",
                )

        # Execute swap (with executor if available)
        if self._intent_executor is not None:
            try:
                result = await self._intent_executor.execute_intent(remediation_intent)

                if result.get("success", False):
                    self._transition(
                        step_id,
                        RemediationState.RESOLVED,
                        RemediationTrigger.REMEDIATION_SUCCEEDED,
                    )

                    return RemediationResult(
                        success=True,
                        state=RemediationState.RESOLVED,
                        action_taken=RemediationAction.SWAP_TO_STABLE,
                        new_tx_hash=result.get("tx_hash"),
                        artifacts={"remediation_intent": remediation_intent},
                    )
                else:
                    raise Exception(result.get("error", "Unknown error"))

            except Exception as e:
                logger.error(f"Swap to stable failed for step {step_id}: {e}")
                return await self._escalate_to_operator(
                    step,
                    reason=f"Swap to stable execution failed: {e}",
                )

        # No executor - simulate for testing
        logger.debug(f"No executor configured, simulating swap_to_stable for step {step_id}")

        self._transition(
            step_id,
            RemediationState.RESOLVED,
            RemediationTrigger.REMEDIATION_SUCCEEDED,
        )

        return RemediationResult(
            success=True,
            state=RemediationState.RESOLVED,
            action_taken=RemediationAction.SWAP_TO_STABLE,
            artifacts={"remediation_intent": remediation_intent},
        )

    async def _escalate_to_operator(
        self,
        step: PlanStep,
        reason: str | None = None,
    ) -> RemediationResult:
        """Escalate to operator intervention.

        Args:
            step: Step requiring operator
            reason: Optional reason for escalation

        Returns:
            RemediationResult with requires_operator=True
        """
        step_id = step.step_id

        self._transition(
            step_id,
            RemediationState.ESCALATED,
            RemediationTrigger.REMEDIATION_FAILED if reason else RemediationTrigger.REMEDIATION_STARTED,
            details=reason,
        )

        # Mark step as stuck
        step.status = StepStatus.STUCK

        # Generate operator card
        card = self.generate_operator_card(step, escalation_reason=reason)
        self._operator_cards[step_id] = card

        logger.warning(f"Escalated step {step_id} to operator: {reason or 'operator intervention configured'}")

        return RemediationResult(
            success=False,
            state=RemediationState.ESCALATED,
            action_taken=RemediationAction.OPERATOR_INTERVENTION,
            requires_operator=True,
            error=reason or "Operator intervention required",
            artifacts={"operator_card_id": card.card_id},
        )

    def generate_operator_card(
        self,
        step: PlanStep,
        escalation_reason: str | None = None,
    ) -> OperatorCard:
        """Generate an operator card for a stuck step.

        Args:
            step: The stuck step
            escalation_reason: Optional reason for escalation

        Returns:
            OperatorCard for operator action
        """
        import uuid

        card_id = f"card-{uuid.uuid4().hex[:12]}"

        # Extract asset info from intent
        intent = step.intent
        affected_assets = []
        if "token_in" in intent:
            affected_assets.append(
                {
                    "token": intent.get("token_in"),
                    "amount": str(intent.get("amount_in", "unknown")),
                    "chain": step.chain,
                }
            )
        elif "token" in intent:
            affected_assets.append(
                {
                    "token": intent.get("token"),
                    "amount": str(intent.get("amount", "unknown")),
                    "chain": step.chain,
                }
            )

        # Build recommended actions
        recommended_actions = self._build_recommended_actions(step)

        # Determine priority
        priority = self._determine_priority(step)

        card = OperatorCard(
            card_id=card_id,
            plan_id=self._plan.plan_id,
            step_id=step.step_id,
            status=OperatorCardStatus.PENDING,
            priority=priority,
            title=f"Cross-Chain Rescue Required - {step.chain}",
            description=self._build_card_description(step, escalation_reason),
            chain=step.chain,
            affected_assets=affected_assets,
            failure_reason=step.error_message or "Unknown failure",
            attempted_remediation=escalation_reason,
            recommended_actions=recommended_actions,
            context={
                "plan_id": self._plan.plan_id,
                "step_id": step.step_id,
                "intent_type": intent.get("type", "unknown"),
                "retry_count": step.retry_count,
                "max_retries": step.max_retries,
                "configured_remediation": step.remediation.value,
                "artifacts": step.artifacts.to_dict(),
            },
        )

        logger.info(
            f"Generated operator card {card_id} for step {step.step_id}: priority={priority.value}, chain={step.chain}"
        )

        return card

    def _build_card_description(
        self,
        step: PlanStep,
        escalation_reason: str | None,
    ) -> str:
        """Build description for operator card.

        Args:
            step: Failed step
            escalation_reason: Reason for escalation

        Returns:
            Human-readable description
        """
        lines = [
            f"Step '{step.description or step.step_id}' has failed and requires operator intervention.",
            "",
            f"Chain: {step.chain}",
            f"Intent Type: {step.intent.get('type', 'unknown')}",
            f"Retry Attempts: {step.retry_count}/{step.max_retries}",
        ]

        if step.error_message:
            lines.extend(["", f"Error: {step.error_message}"])

        if escalation_reason:
            lines.extend(["", f"Escalation Reason: {escalation_reason}"])

        if step.artifacts.tx_hash:
            lines.extend(["", f"Last Transaction: {step.artifacts.tx_hash}"])

        return "\n".join(lines)

    def _build_recommended_actions(self, step: PlanStep) -> list[str]:
        """Build recommended actions for operator.

        Args:
            step: Failed step

        Returns:
            List of recommended actions
        """
        actions = []

        # Check if there's a tx to verify
        if step.artifacts.tx_hash:
            actions.append(f"Verify transaction status on chain: {step.artifacts.tx_hash}")

        # Check bridge status if applicable
        if step.artifacts.bridge_deposit_id:
            actions.append(f"Check bridge transfer status: {step.artifacts.bridge_deposit_id}")

        # Generic actions based on remediation type
        if step.remediation == RemediationAction.BRIDGE_BACK:
            actions.append("Consider manually bridging assets back to source chain")
        elif step.remediation == RemediationAction.SWAP_TO_STABLE:
            actions.append("Consider manually swapping assets to stablecoin")

        # Always add these
        actions.extend(
            [
                "Review error logs for root cause",
                "Verify wallet balances on affected chains",
                "Mark as resolved or abandoned once addressed",
            ]
        )

        return actions

    def _determine_priority(self, step: PlanStep) -> OperatorCardPriority:
        """Determine priority for operator card.

        Args:
            step: Failed step

        Returns:
            Priority level
        """
        # Critical if bridge transfer is in flight
        if step.artifacts.bridge_deposit_id and not step.artifacts.destination_credit_tx:
            return OperatorCardPriority.CRITICAL

        # High if significant value at risk (would need value info)
        # For now, default to HIGH for all stuck steps
        return OperatorCardPriority.HIGH

    def resolve_operator_card(
        self,
        step_id: str,
        resolved_by: str,
        notes: str | None = None,
    ) -> bool:
        """Resolve an operator card (mark as handled).

        Args:
            step_id: Step identifier
            resolved_by: Who resolved it
            notes: Optional resolution notes

        Returns:
            True if card was resolved
        """
        card = self._operator_cards.get(step_id)
        if card is None:
            return False

        card.resolve(resolved_by, notes)

        # Transition state to resolved
        self._transition(
            step_id,
            RemediationState.RESOLVED,
            RemediationTrigger.OPERATOR_RESOLVED,
            details=notes,
        )

        # Update step status
        step = self._plan.get_step(step_id)
        if step:
            step.status = StepStatus.COMPLETED
            step.completed_at = datetime.now(UTC)

        logger.info(f"Operator card {card.card_id} resolved by {resolved_by}")

        return True

    def abandon_operator_card(
        self,
        step_id: str,
        resolved_by: str,
        notes: str | None = None,
    ) -> bool:
        """Abandon an operator card (mark as unresolvable).

        Args:
            step_id: Step identifier
            resolved_by: Who abandoned it
            notes: Optional notes

        Returns:
            True if card was abandoned
        """
        card = self._operator_cards.get(step_id)
        if card is None:
            return False

        card.abandon(resolved_by, notes)

        # Transition state to abandoned
        self._transition(
            step_id,
            RemediationState.ABANDONED,
            RemediationTrigger.OPERATOR_ABANDONED,
            details=notes,
        )

        logger.info(f"Operator card {card.card_id} abandoned by {resolved_by}")

        return True


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Enums
    "RemediationState",
    "RemediationTrigger",
    "OperatorCardStatus",
    "OperatorCardPriority",
    # Data classes
    "RemediationResult",
    "OperatorCard",
    "RemediationStateRecord",
    "RiskValidationResult",
    # State machine
    "RemediationStateMachine",
]
