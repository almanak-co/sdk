"""Plan Executor for Deterministic Cross-Chain Execution.

This module provides the PlanExecutor class that handles deterministic
execution of cross-chain plans with quote pinning, staleness checking,
and restart reconciliation.

Key Features:
    - Quote pinning at plan creation time for determinism
    - Staleness checking with configurable thresholds (default 5 min)
    - Re-quoting for stale quotes with change logging
    - Restart reconciliation comparing persisted plan against on-chain state
    - Deterministic replay using pinned quotes

Example:
    from almanak.framework.execution.plan_executor import PlanExecutor, PlanExecutorConfig

    # Create executor with config
    executor = PlanExecutor(
        config=PlanExecutorConfig(
            stale_quote_threshold_seconds=300,  # 5 minutes
            auto_requote_stale=True,
        ),
        bridge_selector=bridge_selector,
    )

    # Create deterministic plan with pinned quotes
    plan = await executor.create_plan(intents, strategy_id="my-strategy")

    # On restart, reconcile plan with on-chain state
    reconciliation = await executor.reconcile_plan(plan, chain_states)

    # Execute with determinism
    result = await executor.execute_plan(plan)
"""

import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

from almanak.framework.execution.plan import (
    PlanBundle,
    PlanStep,
    RemediationAction,
    StepStatus,
)

if TYPE_CHECKING:
    from almanak.framework.execution.clob_handler import ClobActionHandler
    from almanak.framework.execution.handler_registry import ExecutionHandlerRegistry
    from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Default staleness threshold (5 minutes)
DEFAULT_STALE_QUOTE_THRESHOLD_SECONDS = 300


# =============================================================================
# Enums
# =============================================================================


class ReconciliationStatus(StrEnum):
    """Status of plan reconciliation after restart."""

    VALID = "valid"  # Plan matches on-chain state, can resume
    STALE_QUOTES = "stale_quotes"  # Quotes need refreshing
    STATE_MISMATCH = "state_mismatch"  # On-chain state doesn't match plan
    NEEDS_RECOVERY = "needs_recovery"  # Plan needs manual recovery
    INVALID = "invalid"  # Plan is invalid and cannot proceed


class QuoteRefreshResult(StrEnum):
    """Result of refreshing a stale quote."""

    UNCHANGED = "unchanged"  # New quote matches original
    CHANGED = "changed"  # New quote differs from original (logged)
    FAILED = "failed"  # Failed to get new quote


# =============================================================================
# Protocols (for dependency injection)
# =============================================================================


class BridgeQuoteProvider(Protocol):
    """Protocol for getting bridge quotes."""

    async def get_quote(
        self,
        token: str,
        amount: Decimal,
        from_chain: str,
        to_chain: str,
        max_slippage: Decimal,
    ) -> dict[str, Any]:
        """Get a bridge quote."""
        ...


class OnChainStateProvider(Protocol):
    """Protocol for checking on-chain state."""

    async def get_transaction_status(self, chain: str, tx_hash: str) -> dict[str, Any]:
        """Get transaction status from chain."""
        ...

    async def get_bridge_transfer_status(self, bridge_name: str, deposit_id: str) -> dict[str, Any]:
        """Get bridge transfer status."""
        ...

    async def get_balance(self, chain: str, token: str, address: str) -> Decimal:
        """Get token balance on chain."""
        ...


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class PlanExecutorConfig:
    """Configuration for the plan executor.

    Attributes:
        stale_quote_threshold_seconds: Seconds before a quote is considered stale
        auto_requote_stale: Whether to automatically re-quote stale quotes
        log_quote_changes: Whether to log when re-quoted quotes differ
        max_quote_age_for_execution_seconds: Max quote age to allow execution
        verify_plan_hash_on_load: Whether to verify plan hash when loading
    """

    stale_quote_threshold_seconds: int = DEFAULT_STALE_QUOTE_THRESHOLD_SECONDS
    auto_requote_stale: bool = True
    log_quote_changes: bool = True
    max_quote_age_for_execution_seconds: int = 600  # 10 minutes
    verify_plan_hash_on_load: bool = True


@dataclass
class QuoteRefreshInfo:
    """Information about a quote refresh operation.

    Attributes:
        step_id: Step that was refreshed
        result: Result of the refresh
        original_quote: Original pinned quote
        new_quote: New quote (if refreshed)
        change_details: Details of changes (if any)
        refreshed_at: Timestamp of refresh
    """

    step_id: str
    result: QuoteRefreshResult
    original_quote: dict[str, Any] | None = None
    new_quote: dict[str, Any] | None = None
    change_details: dict[str, Any] | None = None
    refreshed_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class StepReconciliation:
    """Reconciliation result for a single step.

    Attributes:
        step_id: Step identifier
        persisted_status: Status from persisted plan
        on_chain_status: Status from on-chain state
        matches: Whether persisted and on-chain status match
        details: Additional details about discrepancies
        recommended_action: Recommended action for mismatches
    """

    step_id: str
    persisted_status: StepStatus
    on_chain_status: str | None = None
    matches: bool = True
    details: str | None = None
    recommended_action: str | None = None


@dataclass
class PlanReconciliation:
    """Result of reconciling a plan with on-chain state.

    Attributes:
        plan_id: Plan identifier
        status: Overall reconciliation status
        step_reconciliations: Per-step reconciliation results
        stale_steps: Steps with stale quotes
        quote_refreshes: Quote refresh operations performed
        can_resume: Whether plan can be safely resumed
        resume_from_step: Step to resume from (if can_resume)
        reconciled_at: Timestamp of reconciliation
        warnings: Non-fatal issues found
        errors: Fatal issues found
    """

    plan_id: str
    status: ReconciliationStatus
    step_reconciliations: list[StepReconciliation] = field(default_factory=list)
    stale_steps: list[str] = field(default_factory=list)
    quote_refreshes: list[QuoteRefreshInfo] = field(default_factory=list)
    can_resume: bool = False
    resume_from_step: str | None = None
    reconciled_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "plan_id": self.plan_id,
            "status": self.status.value,
            "step_reconciliations": [
                {
                    "step_id": s.step_id,
                    "persisted_status": s.persisted_status.value,
                    "on_chain_status": s.on_chain_status,
                    "matches": s.matches,
                    "details": s.details,
                    "recommended_action": s.recommended_action,
                }
                for s in self.step_reconciliations
            ],
            "stale_steps": self.stale_steps,
            "quote_refreshes": [
                {
                    "step_id": q.step_id,
                    "result": q.result.value,
                    "refreshed_at": q.refreshed_at.isoformat(),
                }
                for q in self.quote_refreshes
            ],
            "can_resume": self.can_resume,
            "resume_from_step": self.resume_from_step,
            "reconciled_at": self.reconciled_at.isoformat(),
            "warnings": self.warnings,
            "errors": self.errors,
        }


class RehydrationStatus(StrEnum):
    """Status of plan rehydration."""

    VALID = "valid"  # Plan matches on-chain state
    STATE_UPDATED = "state_updated"  # Some states were updated
    NEEDS_REMEDIATION = "needs_remediation"  # Some steps need remediation
    INVALID = "invalid"  # Plan is invalid


class PlanExecutionStatus(StrEnum):
    """Status of plan execution."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    NEEDS_OPERATOR = "needs_operator"


@dataclass
class StepRehydrationResult:
    """Result of rehydrating a single step.

    Attributes:
        step_id: Step identifier
        persisted_status: Status from persisted plan
        on_chain_status: Status from on-chain state
        valid: Whether step state is valid
        status_updated: Whether step status was updated
        needs_remediation: Whether step needs remediation
        details: Additional details
    """

    step_id: str
    persisted_status: StepStatus
    on_chain_status: str | None = None
    valid: bool = True
    status_updated: bool = False
    needs_remediation: bool = False
    details: str | None = None


@dataclass
class RehydrationResult:
    """Result of plan rehydration.

    Attributes:
        plan_id: Plan identifier
        status: Overall rehydration status
        step_results: Per-step rehydration results
        steps_needing_remediation: Steps that need remediation
        stale_quote_steps: Steps with stale quotes
        quote_refreshes: Quote refresh operations
        warnings: Non-fatal issues
        errors: Fatal issues
    """

    plan_id: str
    status: RehydrationStatus = RehydrationStatus.VALID
    step_results: list[StepRehydrationResult] = field(default_factory=list)
    steps_needing_remediation: list[str] = field(default_factory=list)
    stale_quote_steps: list[str] = field(default_factory=list)
    quote_refreshes: list[QuoteRefreshInfo] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "plan_id": self.plan_id,
            "status": self.status.value,
            "step_results": [
                {
                    "step_id": s.step_id,
                    "persisted_status": s.persisted_status.value,
                    "on_chain_status": s.on_chain_status,
                    "valid": s.valid,
                    "status_updated": s.status_updated,
                    "needs_remediation": s.needs_remediation,
                    "details": s.details,
                }
                for s in self.step_results
            ],
            "steps_needing_remediation": self.steps_needing_remediation,
            "stale_quote_steps": self.stale_quote_steps,
            "warnings": self.warnings,
            "errors": self.errors,
        }


@dataclass
class RemediationHandlingResult:
    """Result of handling a step failure with remediation.

    Attributes:
        step_id: Step identifier
        action: Remediation action taken
        success: Whether remediation succeeded
        new_tx_hash: Transaction hash if remediation created tx
        requires_operator: Whether operator intervention needed
        operator_card: Operator card if escalated
        state: Remediation state
        error: Error message if failed
    """

    step_id: str
    action: RemediationAction
    success: bool = False
    new_tx_hash: str | None = None
    requires_operator: bool = False
    operator_card: dict[str, Any] | None = None
    state: str | None = None
    error: str | None = None


class ExecutionPath(StrEnum):
    """Path taken for step execution."""

    ON_CHAIN = "on_chain"  # Standard on-chain transaction
    CLOB = "clob"  # Off-chain CLOB order (e.g., Polymarket)
    SIMULATED = "simulated"  # Simulated/test execution


@dataclass
class StepExecutionResult:
    """Result of executing a single step.

    Attributes:
        step_id: Step identifier
        success: Whether step succeeded
        tx_hash: Transaction hash if successful (on-chain)
        order_id: Order ID if CLOB execution
        execution_path: Path taken (on_chain, clob, simulated)
        requires_operator: Whether operator intervention needed
        operator_card: Operator card if escalated
        remediation_result: Result of remediation if attempted
    """

    step_id: str
    success: bool = False
    tx_hash: str | None = None
    order_id: str | None = None
    execution_path: ExecutionPath = ExecutionPath.ON_CHAIN
    requires_operator: bool = False
    operator_card: dict[str, Any] | None = None
    remediation_result: RemediationHandlingResult | None = None


@dataclass
class PlanExecutionResult:
    """Result of executing a plan.

    Attributes:
        plan_id: Plan identifier
        status: Execution status
        step_results: Per-step execution results
        operator_cards: Operator cards generated
        error: Error message if failed
    """

    plan_id: str
    status: PlanExecutionStatus = PlanExecutionStatus.PENDING
    step_results: list[StepExecutionResult] = field(default_factory=list)
    operator_cards: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "plan_id": self.plan_id,
            "status": self.status.value,
            "step_results": [
                {
                    "step_id": s.step_id,
                    "success": s.success,
                    "tx_hash": s.tx_hash,
                    "order_id": s.order_id,
                    "execution_path": s.execution_path.value,
                    "requires_operator": s.requires_operator,
                }
                for s in self.step_results
            ],
            "operator_cards": self.operator_cards,
            "error": self.error,
        }


# =============================================================================
# Plan Executor
# =============================================================================


class PlanExecutor:
    """Executes cross-chain plans with determinism and reconciliation.

    The PlanExecutor handles:
    1. Plan Creation: Creates plans with quotes pinned at creation time
    2. Quote Management: Checks staleness, re-quotes when needed, logs changes
    3. Reconciliation: Compares persisted plan against on-chain state on restart
    4. Execution: Executes plans using pinned quotes for determinism

    Example:
        executor = PlanExecutor(config, bridge_selector)

        # Create plan with pinned quotes
        plan = await executor.create_plan(intents, "strategy-1")

        # Later, on restart
        plan = PlanBundle.from_dict(load_from_storage())
        reconciliation = await executor.reconcile_plan(plan, state_provider)

        if reconciliation.can_resume:
            result = await executor.execute_plan(plan)
    """

    def __init__(
        self,
        config: PlanExecutorConfig | None = None,
        quote_provider: BridgeQuoteProvider | None = None,
        state_provider: OnChainStateProvider | None = None,
        clob_handler: "ClobActionHandler | None" = None,
        handler_registry: "ExecutionHandlerRegistry | None" = None,
    ) -> None:
        """Initialize the plan executor.

        Args:
            config: Executor configuration
            quote_provider: Provider for bridge quotes
            state_provider: Provider for on-chain state
            clob_handler: (Deprecated) Handler for off-chain CLOB order execution.
                         Use handler_registry instead.
            handler_registry: Registry for routing bundles to execution handlers.
                             If not provided, a default registry will be created
                             and clob_handler will be registered if provided.
        """
        self._config = config or PlanExecutorConfig()
        self._quote_provider = quote_provider
        self._state_provider = state_provider

        # Setup handler registry
        if handler_registry is not None:
            self._handler_registry = handler_registry
        else:
            # Create default registry for backward compatibility
            from almanak.framework.execution.handler_registry import ExecutionHandlerRegistry

            self._handler_registry = ExecutionHandlerRegistry()

            # Register clob_handler if provided (backward compatibility)
            if clob_handler is not None:
                self._handler_registry.register(clob_handler)

        # Keep reference to clob_handler for backward compatibility
        # This is used in _execute_clob_bundle for now
        self._clob_handler = clob_handler

        logger.info(
            "PlanExecutor initialized",
            extra={
                "stale_threshold_seconds": self._config.stale_quote_threshold_seconds,
                "auto_requote": self._config.auto_requote_stale,
                "has_clob_handler": clob_handler is not None,
                "has_registry": handler_registry is not None,
                "registered_protocols": self._handler_registry.get_registered_protocols(),
            },
        )

    # =========================================================================
    # Plan Creation with Quote Pinning
    # =========================================================================

    def create_plan_from_steps(
        self,
        steps: list[PlanStep],
        strategy_id: str | None = None,
        description: str | None = None,
    ) -> PlanBundle:
        """Create a plan bundle from pre-built steps.

        This method creates a PlanBundle with plan_id, execution order,
        and plan hash computed. Quotes should already be pinned on steps
        that need them.

        Args:
            steps: List of plan steps
            strategy_id: Optional strategy identifier
            description: Optional plan description

        Returns:
            PlanBundle with computed hash and execution order
        """
        plan_id = f"plan-{uuid.uuid4().hex[:12]}"

        plan = PlanBundle(
            plan_id=plan_id,
            steps=steps,
            strategy_id=strategy_id,
            description=description,
        )

        logger.info(
            f"Created plan {plan_id}: {len(steps)} steps, hash={plan.plan_hash}, chains={list(plan.chains_involved)}"
        )

        return plan

    def pin_quote_to_step(
        self,
        step: PlanStep,
        quote: dict[str, Any],
    ) -> None:
        """Pin a bridge quote to a step at the current time.

        This should be called at plan creation time to freeze the quote
        for deterministic replay.

        Args:
            step: The step to pin the quote to
            quote: Quote dictionary from bridge adapter
        """
        step.artifacts.pin_quote(quote)

        logger.debug(
            f"Pinned quote to step {step.step_id}: bridge={quote.get('bridge_name')}, quote_hash={step.artifacts.quote_hash}"
        )

    # =========================================================================
    # Quote Staleness and Refresh
    # =========================================================================

    def check_quote_staleness(
        self,
        plan: PlanBundle,
        threshold_seconds: int | None = None,
    ) -> list[str]:
        """Check for stale quotes in a plan.

        Args:
            plan: The plan to check
            threshold_seconds: Custom staleness threshold (uses config default if None)

        Returns:
            List of step_ids with stale quotes
        """
        threshold = threshold_seconds or self._config.stale_quote_threshold_seconds
        stale_steps = plan.get_stale_quote_steps(threshold)
        return [step.step_id for step in stale_steps]

    async def refresh_stale_quotes(
        self,
        plan: PlanBundle,
        threshold_seconds: int | None = None,
    ) -> list[QuoteRefreshInfo]:
        """Refresh all stale quotes in a plan.

        For each stale quote:
        1. Gets a new quote from the bridge
        2. Compares to original pinned quote
        3. Logs if quote changed significantly
        4. Updates the step with new pinned quote

        Args:
            plan: The plan with potentially stale quotes
            threshold_seconds: Custom staleness threshold

        Returns:
            List of refresh results
        """
        if self._quote_provider is None:
            logger.warning("No quote provider configured, cannot refresh quotes")
            return []

        threshold = threshold_seconds or self._config.stale_quote_threshold_seconds
        stale_step_ids = self.check_quote_staleness(plan, threshold)
        refresh_results: list[QuoteRefreshInfo] = []

        for step_id in stale_step_ids:
            step = plan.get_step(step_id)
            if step is None or step.artifacts.pinned_quote is None:
                continue

            original_quote = step.artifacts.pinned_quote
            refresh_info = await self._refresh_step_quote(step, original_quote)
            refresh_results.append(refresh_info)

        return refresh_results

    async def _refresh_step_quote(
        self,
        step: PlanStep,
        original_quote: dict[str, Any],
    ) -> QuoteRefreshInfo:
        """Refresh the quote for a single step.

        Args:
            step: Step needing quote refresh
            original_quote: Original pinned quote

        Returns:
            QuoteRefreshInfo with refresh result
        """
        if self._quote_provider is None:
            return QuoteRefreshInfo(
                step_id=step.step_id,
                result=QuoteRefreshResult.FAILED,
                original_quote=original_quote,
            )

        try:
            # Get new quote with same parameters
            new_quote = await self._quote_provider.get_quote(
                token=original_quote.get("token", ""),
                amount=Decimal(str(original_quote.get("input_amount", "0"))),
                from_chain=original_quote.get("from_chain", ""),
                to_chain=original_quote.get("to_chain", ""),
                max_slippage=Decimal(str(original_quote.get("slippage_tolerance", "0.005"))),
            )

            # Check if quote changed
            if step.artifacts.verify_quote_hash(new_quote):
                # Quote parameters match
                result = QuoteRefreshResult.UNCHANGED
                change_details = None
            else:
                # Quote changed - compute and log differences
                result = QuoteRefreshResult.CHANGED
                change_details = self._compute_quote_changes(original_quote, new_quote)

                if self._config.log_quote_changes:
                    logger.warning(f"Quote changed for step {step.step_id}: {change_details}")

            # Update step with new pinned quote
            step.artifacts.pin_quote(new_quote)

            return QuoteRefreshInfo(
                step_id=step.step_id,
                result=result,
                original_quote=original_quote,
                new_quote=new_quote,
                change_details=change_details,
            )

        except Exception as e:
            logger.error(f"Failed to refresh quote for step {step.step_id}: {e}")
            return QuoteRefreshInfo(
                step_id=step.step_id,
                result=QuoteRefreshResult.FAILED,
                original_quote=original_quote,
                change_details={"error": str(e)},
            )

    def _compute_quote_changes(
        self,
        original: dict[str, Any],
        new: dict[str, Any],
    ) -> dict[str, Any]:
        """Compute differences between original and new quotes.

        Args:
            original: Original quote
            new: New quote

        Returns:
            Dictionary of changes
        """
        changes: dict[str, Any] = {}

        # Compare key parameters
        compare_fields = [
            "output_amount",
            "fee_amount",
            "estimated_time_seconds",
        ]

        for field_name in compare_fields:
            orig_val = original.get(field_name)
            new_val = new.get(field_name)
            if orig_val != new_val:
                changes[field_name] = {
                    "original": str(orig_val),
                    "new": str(new_val),
                }

        # Calculate percentage change in output amount
        orig_output = Decimal(str(original.get("output_amount", "0")))
        new_output = Decimal(str(new.get("output_amount", "0")))
        if orig_output > 0:
            pct_change = ((new_output - orig_output) / orig_output) * 100
            changes["output_change_pct"] = float(pct_change)

        return changes

    # =========================================================================
    # Plan Reconciliation
    # =========================================================================

    async def reconcile_plan(
        self,
        plan: PlanBundle,
    ) -> PlanReconciliation:
        """Reconcile a persisted plan with on-chain state.

        On restart, this method:
        1. Verifies plan hash integrity
        2. Checks each step's on-chain status vs persisted status
        3. Identifies stale quotes that need refreshing
        4. Determines if plan can be safely resumed and from which step

        Args:
            plan: The persisted plan to reconcile

        Returns:
            PlanReconciliation with status and recommendations
        """
        reconciliation = PlanReconciliation(
            plan_id=plan.plan_id,
            status=ReconciliationStatus.VALID,
        )

        # Verify plan hash if configured
        if self._config.verify_plan_hash_on_load:
            if not plan.verify_plan_integrity():
                reconciliation.status = ReconciliationStatus.INVALID
                reconciliation.errors.append("Plan hash verification failed - plan may have been modified")
                reconciliation.can_resume = False
                return reconciliation

        # Check for stale quotes
        stale_step_ids = self.check_quote_staleness(plan)
        reconciliation.stale_steps = stale_step_ids

        if stale_step_ids:
            reconciliation.warnings.append(f"Found {len(stale_step_ids)} steps with stale quotes")

            # Auto-refresh if configured
            if self._config.auto_requote_stale:
                refresh_results = await self.refresh_stale_quotes(plan)
                reconciliation.quote_refreshes = refresh_results

                # Check if any refreshes failed
                failed_refreshes = [r for r in refresh_results if r.result == QuoteRefreshResult.FAILED]
                if failed_refreshes:
                    reconciliation.status = ReconciliationStatus.STALE_QUOTES
                    reconciliation.errors.append(f"Failed to refresh {len(failed_refreshes)} quotes")
            else:
                reconciliation.status = ReconciliationStatus.STALE_QUOTES

        # Reconcile each step with on-chain state
        if self._state_provider is not None:
            for step in plan.steps:
                step_recon = await self._reconcile_step(step)
                reconciliation.step_reconciliations.append(step_recon)

                if not step_recon.matches:
                    reconciliation.warnings.append(f"Step {step.step_id} state mismatch: {step_recon.details}")

            # Check for critical mismatches
            mismatches = [s for s in reconciliation.step_reconciliations if not s.matches]
            if mismatches:
                reconciliation.status = ReconciliationStatus.STATE_MISMATCH

        # Determine if can resume and from which step
        self._determine_resume_point(plan, reconciliation)

        logger.info(
            f"Reconciled plan {plan.plan_id}: status={reconciliation.status.value}, can_resume={reconciliation.can_resume}, resume_from={reconciliation.resume_from_step}"
        )

        return reconciliation

    async def _reconcile_step(self, step: PlanStep) -> StepReconciliation:
        """Reconcile a single step with on-chain state.

        Args:
            step: The step to reconcile

        Returns:
            StepReconciliation result
        """
        recon = StepReconciliation(
            step_id=step.step_id,
            persisted_status=step.status,
        )

        if self._state_provider is None:
            return recon

        try:
            # Check transaction status if we have a tx hash
            if step.artifacts.tx_hash:
                tx_status = await self._state_provider.get_transaction_status(step.chain, step.artifacts.tx_hash)
                recon.on_chain_status = tx_status.get("status", "unknown")

                # Check for mismatches
                if step.is_success and recon.on_chain_status != "confirmed":
                    recon.matches = False
                    recon.details = f"Step marked SUCCESS but tx status is {recon.on_chain_status}"
                    recon.recommended_action = "Verify transaction on chain"

            # Check bridge status if applicable
            if step.artifacts.bridge_deposit_id:
                bridge_name = (
                    step.artifacts.pinned_quote.get("bridge_name", "unknown")
                    if step.artifacts.pinned_quote
                    else "unknown"
                )
                bridge_status = await self._state_provider.get_bridge_transfer_status(
                    bridge_name, step.artifacts.bridge_deposit_id
                )

                if step.status == StepStatus.COMPLETED and bridge_status.get("status") != "completed":
                    recon.matches = False
                    recon.details = f"Step marked COMPLETED but bridge status is {bridge_status.get('status')}"
                    recon.recommended_action = "Check bridge transfer status"

        except Exception as e:
            logger.warning(f"Error reconciling step {step.step_id}: {e}")
            recon.details = f"Error checking on-chain state: {e}"

        return recon

    def _determine_resume_point(
        self,
        plan: PlanBundle,
        reconciliation: PlanReconciliation,
    ) -> None:
        """Determine if and where a plan can be resumed.

        Args:
            plan: The plan being reconciled
            reconciliation: Reconciliation result to update
        """
        # Cannot resume if invalid
        if reconciliation.status == ReconciliationStatus.INVALID:
            reconciliation.can_resume = False
            return

        # Find first non-completed step
        for step_id in plan.execution_order:
            step = plan.get_step(step_id)
            if step and not step.is_success:
                # Check if this step had a state mismatch
                step_recon = next(
                    (s for s in reconciliation.step_reconciliations if s.step_id == step_id),
                    None,
                )

                if step_recon and not step_recon.matches:
                    # Can't safely resume due to state mismatch
                    reconciliation.can_resume = False
                    reconciliation.errors.append(f"Cannot resume: step {step_id} has state mismatch")
                    return

                # This is the resume point
                reconciliation.resume_from_step = step_id
                reconciliation.can_resume = True
                return

        # All steps completed
        reconciliation.can_resume = False
        reconciliation.resume_from_step = None

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def get_plan_summary(self, plan: PlanBundle) -> dict[str, Any]:
        """Get a summary of plan status.

        Args:
            plan: The plan to summarize

        Returns:
            Dictionary with plan summary
        """
        stale_quotes = self.check_quote_staleness(plan)

        return {
            "plan_id": plan.plan_id,
            "plan_hash": plan.plan_hash,
            "created_at": plan.created_at.isoformat(),
            "step_count": plan.step_count,
            "completed_count": plan.completed_step_count,
            "failed_count": plan.failed_step_count,
            "pending_count": plan.pending_step_count,
            "progress": plan.progress,
            "chains": list(plan.chains_involved),
            "is_complete": plan.is_complete,
            "is_success": plan.is_success,
            "stale_quote_count": len(stale_quotes),
            "hash_valid": plan.verify_plan_integrity(),
        }

    # =========================================================================
    # Plan Rehydration
    # =========================================================================

    async def rehydrate_plan(
        self,
        plan: PlanBundle,
    ) -> "RehydrationResult":
        """Rehydrate a plan on restart by validating artifacts against on-chain state.

        This method:
        1. Loads persisted plan and validates integrity
        2. Re-checks each step's artifacts against on-chain state
        3. Updates step statuses based on actual on-chain state
        4. Identifies steps that need remediation

        Args:
            plan: The persisted plan to rehydrate

        Returns:
            RehydrationResult with status and any needed actions
        """
        result = RehydrationResult(
            plan_id=plan.plan_id,
            status=RehydrationStatus.VALID,
        )

        # Verify plan hash integrity
        if self._config.verify_plan_hash_on_load:
            if not plan.verify_plan_integrity():
                result.status = RehydrationStatus.INVALID
                result.errors.append("Plan hash verification failed")
                return result

        # Check each step's artifacts against on-chain state
        if self._state_provider is not None:
            for step in plan.steps:
                step_result = await self._rehydrate_step(step)
                result.step_results.append(step_result)

                if not step_result.valid:
                    result.warnings.append(f"Step {step.step_id}: {step_result.details}")

                if step_result.needs_remediation:
                    result.steps_needing_remediation.append(step.step_id)

        # Determine overall status
        if result.steps_needing_remediation:
            result.status = RehydrationStatus.NEEDS_REMEDIATION
        elif result.warnings:
            result.status = RehydrationStatus.STATE_UPDATED

        # Check for stale quotes
        stale_step_ids = self.check_quote_staleness(plan)
        if stale_step_ids:
            result.stale_quote_steps = stale_step_ids
            result.warnings.append(f"Found {len(stale_step_ids)} steps with stale quotes")

            if self._config.auto_requote_stale:
                refreshes = await self.refresh_stale_quotes(plan)
                result.quote_refreshes = refreshes

        logger.info(
            f"Rehydrated plan {plan.plan_id}: status={result.status.value}, steps_needing_remediation={len(result.steps_needing_remediation)}"
        )

        return result

    async def _rehydrate_step(self, step: PlanStep) -> "StepRehydrationResult":
        """Rehydrate a single step by checking artifacts against on-chain state.

        Args:
            step: Step to rehydrate

        Returns:
            StepRehydrationResult
        """
        result = StepRehydrationResult(
            step_id=step.step_id,
            persisted_status=step.status,
            valid=True,
        )

        if self._state_provider is None:
            return result

        # Check transaction status if we have a tx hash
        if step.artifacts.tx_hash:
            try:
                tx_status = await self._state_provider.get_transaction_status(step.chain, step.artifacts.tx_hash)

                on_chain_status = tx_status.get("status", "unknown")
                result.on_chain_status = on_chain_status

                # Reconcile status
                if step.status == StepStatus.SUBMITTED:
                    if on_chain_status == "confirmed":
                        # Transaction confirmed since we last checked
                        step.status = StepStatus.CONFIRMED
                        step.artifacts.block_number = tx_status.get("block_number")
                        result.status_updated = True
                        result.details = "Transaction confirmed on chain"
                    elif on_chain_status == "failed":
                        # Transaction failed
                        step.status = StepStatus.FAILED
                        step.error_message = tx_status.get("error", "Transaction reverted")
                        result.status_updated = True
                        result.needs_remediation = True
                        result.details = "Transaction failed on chain"

                elif step.status == StepStatus.CONFIRMING:
                    if on_chain_status == "confirmed":
                        step.status = StepStatus.CONFIRMED
                        result.status_updated = True

                elif step.status == StepStatus.COMPLETED:
                    if on_chain_status != "confirmed":
                        # Step marked complete but tx not confirmed
                        result.valid = False
                        result.details = f"Step marked COMPLETED but tx status is {on_chain_status}"

            except Exception as e:
                logger.warning(f"Error checking tx status for step {step.step_id}: {e}")
                result.details = f"Error checking tx status: {e}"

        # Check bridge status if applicable
        if step.artifacts.bridge_deposit_id:
            try:
                bridge_name = (
                    step.artifacts.pinned_quote.get("bridge_name", "unknown")
                    if step.artifacts.pinned_quote
                    else "unknown"
                )
                bridge_status = await self._state_provider.get_bridge_transfer_status(
                    bridge_name, step.artifacts.bridge_deposit_id
                )

                bridge_state = bridge_status.get("status", "unknown")

                if step.status == StepStatus.CONFIRMED:
                    # Step confirmed on source, check destination
                    if bridge_state == "completed":
                        step.status = StepStatus.COMPLETED
                        step.artifacts.destination_credit_tx = bridge_status.get("destination_tx")
                        result.status_updated = True
                        result.details = "Bridge transfer completed"
                    elif bridge_state == "failed":
                        step.status = StepStatus.FAILED
                        step.error_message = bridge_status.get("error", "Bridge transfer failed")
                        result.status_updated = True
                        result.needs_remediation = True
                        result.details = "Bridge transfer failed"

                elif step.status == StepStatus.COMPLETED:
                    if bridge_state != "completed":
                        # Step marked complete but bridge not completed
                        result.valid = False
                        result.details = f"Step marked COMPLETED but bridge status is {bridge_state}"

            except Exception as e:
                logger.warning(f"Error checking bridge status for step {step.step_id}: {e}")
                result.details = f"Error checking bridge status: {e}"

        return result

    # =========================================================================
    # Remediation Handling
    # =========================================================================

    async def handle_step_failure(
        self,
        plan: PlanBundle,
        step: PlanStep,
        remediation_executor: Any | None = None,
    ) -> "RemediationHandlingResult":
        """Handle a failed step by executing configured remediation.

        This method:
        1. Checks if retries are available
        2. Executes configured remediation action
        3. Validates remediation through risk checks
        4. Generates operator card if escalation needed

        Args:
            plan: The plan containing the step
            step: The failed step
            remediation_executor: Optional executor for remediation intents

        Returns:
            RemediationHandlingResult
        """
        from almanak.framework.execution.remediation import (
            RemediationStateMachine,
        )

        result = RemediationHandlingResult(
            step_id=step.step_id,
            action=step.remediation,
        )

        # Create state machine for this plan
        state_machine = RemediationStateMachine(
            plan=plan,
            intent_executor=remediation_executor,
        )

        # Handle the failure
        remediation_result = await state_machine.handle_failure(step)

        result.success = remediation_result.success
        result.new_tx_hash = remediation_result.new_tx_hash
        result.requires_operator = remediation_result.requires_operator

        if remediation_result.requires_operator:
            # Get operator card
            operator_card = state_machine.operator_cards.get(step.step_id)
            if operator_card:
                result.operator_card = operator_card.to_dict()

        result.state = remediation_result.state.value
        result.error = remediation_result.error

        logger.info(
            f"Handled failure for step {step.step_id}: action={step.remediation.value}, success={result.success}, requires_operator={result.requires_operator}"
        )

        return result

    async def execute_plan_with_remediation(
        self,
        plan: PlanBundle,
        step_executor: Callable[[PlanStep], Any] | None = None,
        remediation_executor: Any | None = None,
    ) -> "PlanExecutionResult":
        """Execute a plan with full remediation support.

        This method:
        1. Executes steps in dependency order
        2. Handles failures with configured remediation
        3. Persists step-level status with artifacts
        4. Generates operator cards for stuck operations

        Args:
            plan: Plan to execute
            step_executor: Function to execute individual steps
            remediation_executor: Optional executor for remediation intents

        Returns:
            PlanExecutionResult
        """
        result = PlanExecutionResult(
            plan_id=plan.plan_id,
            status=PlanExecutionStatus.RUNNING,
        )

        plan.started_at = datetime.now(UTC)

        try:
            while True:
                # Get next step to execute
                next_step = plan.get_next_step()

                if next_step is None:
                    # No more steps to execute
                    break

                # Execute the step
                step_result = await self._execute_step_with_remediation(
                    plan,
                    next_step,
                    step_executor,
                    remediation_executor,
                )

                result.step_results.append(step_result)

                # Check if we need to stop
                if step_result.requires_operator:
                    result.status = PlanExecutionStatus.NEEDS_OPERATOR
                    if step_result.operator_card is not None:
                        result.operator_cards.append(step_result.operator_card)
                    break

                if not step_result.success and next_step.status == StepStatus.FAILED:
                    # Step failed without remediation
                    result.status = PlanExecutionStatus.FAILED
                    break

            # Check final status
            if plan.is_success:
                result.status = PlanExecutionStatus.COMPLETED
            elif plan.is_failed:
                if plan.needs_operator:
                    result.status = PlanExecutionStatus.NEEDS_OPERATOR
                else:
                    result.status = PlanExecutionStatus.FAILED

            plan.completed_at = datetime.now(UTC)

        except Exception as e:
            logger.error(f"Error executing plan {plan.plan_id}: {e}")
            result.status = PlanExecutionStatus.FAILED
            result.error = str(e)

        return result

    async def _execute_step_with_remediation(
        self,
        plan: PlanBundle,
        step: PlanStep,
        step_executor: Callable[[PlanStep], Any] | None,
        remediation_executor: Any | None,
    ) -> "StepExecutionResult":
        """Execute a single step with remediation support.

        Args:
            plan: The plan
            step: Step to execute
            step_executor: Function to execute the step
            remediation_executor: Executor for remediation

        Returns:
            StepExecutionResult
        """
        result = StepExecutionResult(
            step_id=step.step_id,
            success=False,
        )

        step.started_at = datetime.now(UTC)
        step.status = StepStatus.SUBMITTING

        try:
            # Execute the step
            if step_executor is not None:
                execution_result = await step_executor(step)

                if execution_result.get("success", False):
                    step.status = StepStatus.COMPLETED
                    step.artifacts.tx_hash = execution_result.get("tx_hash")
                    step.artifacts.actual_amount_received = execution_result.get("amount_received")
                    step.completed_at = datetime.now(UTC)
                    result.success = True
                    result.tx_hash = execution_result.get("tx_hash")
                else:
                    raise Exception(execution_result.get("error", "Unknown error"))
            else:
                # No executor - simulate success for testing
                step.status = StepStatus.COMPLETED
                step.completed_at = datetime.now(UTC)
                result.success = True

        except Exception as e:
            logger.warning(f"Step {step.step_id} failed: {e}")
            step.status = StepStatus.FAILED
            step.error_message = str(e)

            # Handle remediation
            remediation_result = await self.handle_step_failure(plan, step, remediation_executor)

            result.remediation_result = remediation_result
            result.success = remediation_result.success
            result.requires_operator = remediation_result.requires_operator

            if remediation_result.operator_card:
                result.operator_card = remediation_result.operator_card

        return result

    # =========================================================================
    # CLOB / Off-Chain Order Execution
    # =========================================================================

    def _is_clob_bundle(self, bundle: "ActionBundle") -> bool:
        """(Deprecated) Check if an ActionBundle should be routed to CLOB handler.

        This method is maintained for backward compatibility but is deprecated.
        Use handler_registry.get_handler() instead for protocol-agnostic routing.

        Args:
            bundle: ActionBundle to check

        Returns:
            True if handler registry finds a CLOB handler for this bundle
        """
        # Use registry to determine if there's a handler
        handler = self._handler_registry.get_handler(bundle)
        return handler is not None and handler == self._clob_handler

    async def execute_bundle(
        self,
        bundle: "ActionBundle",
        on_chain_executor: Callable[["ActionBundle"], Any] | None = None,
    ) -> StepExecutionResult:
        """Execute an ActionBundle, routing to the appropriate handler.

        This method uses the handler registry to route bundles to the appropriate
        execution handler based on protocol and bundle characteristics:
        1. Query registry for matching handler
        2. Route to handler (e.g., CLOB handler for Polymarket orders)
        3. Fall back to on-chain executor if no handler found

        Args:
            bundle: ActionBundle to execute
            on_chain_executor: Executor for on-chain transactions (fallback)

        Returns:
            StepExecutionResult with success status and relevant IDs
        """
        result = StepExecutionResult(
            step_id=bundle.metadata.get("intent_id", "unknown"),
            success=False,
        )

        # Use handler registry for routing
        handler = self._handler_registry.get_handler(bundle)

        if handler is not None and handler == self._clob_handler:
            # CLOB handler found (e.g., Polymarket off-chain orders)
            logger.info(
                "Routing bundle to CLOB handler via registry",
                extra={
                    "intent_type": bundle.intent_type,
                    "protocol": bundle.metadata.get("protocol"),
                    "intent_id": bundle.metadata.get("intent_id"),
                    "handler": handler.__class__.__name__,
                },
            )
            result.execution_path = ExecutionPath.CLOB
            return await self._execute_clob_bundle(bundle, result)

        # Standard on-chain execution (including Polymarket redemptions)
        # This is the fallback path when no specialized handler is found
        logger.info(
            "Routing bundle to on-chain executor",
            extra={
                "intent_type": bundle.intent_type,
                "protocol": bundle.metadata.get("protocol"),
                "transaction_count": len(bundle.transactions),
            },
        )
        result.execution_path = ExecutionPath.ON_CHAIN
        return await self._execute_onchain_bundle(bundle, on_chain_executor, result)

    async def _execute_clob_bundle(
        self,
        bundle: "ActionBundle",
        result: StepExecutionResult,
    ) -> StepExecutionResult:
        """Execute a CLOB order bundle via ClobActionHandler.

        Args:
            bundle: ActionBundle with CLOB order payload
            result: StepExecutionResult to populate

        Returns:
            Updated StepExecutionResult
        """
        if self._clob_handler is None:
            logger.error(
                "CLOB handler not configured but bundle requires CLOB execution",
                extra={"intent_id": bundle.metadata.get("intent_id")},
            )
            result.success = False
            return result

        try:
            # Execute via CLOB handler
            clob_result = await self._clob_handler.execute(bundle)

            result.success = clob_result.success
            result.order_id = clob_result.order_id

            if clob_result.success:
                logger.info(
                    "CLOB order executed successfully",
                    extra={
                        "order_id": clob_result.order_id,
                        "status": clob_result.status.value,
                        "intent_id": bundle.metadata.get("intent_id"),
                    },
                )
            else:
                logger.warning(
                    "CLOB order execution failed",
                    extra={
                        "error": clob_result.error,
                        "intent_id": bundle.metadata.get("intent_id"),
                    },
                )

        except Exception:
            logger.exception(
                "Exception during CLOB bundle execution",
                extra={"intent_id": bundle.metadata.get("intent_id")},
            )
            result.success = False

        return result

    async def _execute_onchain_bundle(
        self,
        bundle: "ActionBundle",
        executor: Callable[["ActionBundle"], Any] | None,
        result: StepExecutionResult,
    ) -> StepExecutionResult:
        """Execute a bundle via on-chain executor.

        This handles standard on-chain transactions including CTF redemptions
        for Polymarket (which require smart contract calls).

        Args:
            bundle: ActionBundle with transactions
            executor: On-chain transaction executor
            result: StepExecutionResult to populate

        Returns:
            Updated StepExecutionResult
        """
        if executor is None:
            # No executor - simulate success for testing
            logger.debug(
                "No on-chain executor provided, simulating success",
                extra={"intent_type": bundle.intent_type},
            )
            result.success = True
            result.execution_path = ExecutionPath.SIMULATED
            return result

        try:
            execution_result = await executor(bundle)

            result.success = execution_result.get("success", False)
            result.tx_hash = execution_result.get("tx_hash")

            if result.success:
                logger.info(
                    "On-chain bundle executed successfully",
                    extra={
                        "tx_hash": result.tx_hash,
                        "intent_type": bundle.intent_type,
                    },
                )
            else:
                logger.warning(
                    "On-chain bundle execution failed",
                    extra={
                        "error": execution_result.get("error"),
                        "intent_type": bundle.intent_type,
                    },
                )

        except Exception:
            logger.exception(
                "Exception during on-chain bundle execution",
                extra={"intent_type": bundle.intent_type},
            )
            result.success = False

        return result


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Config
    "PlanExecutorConfig",
    # Enums
    "ReconciliationStatus",
    "QuoteRefreshResult",
    "RehydrationStatus",
    "PlanExecutionStatus",
    "ExecutionPath",
    # Data classes
    "QuoteRefreshInfo",
    "StepReconciliation",
    "PlanReconciliation",
    "StepRehydrationResult",
    "RehydrationResult",
    "RemediationHandlingResult",
    "StepExecutionResult",
    "PlanExecutionResult",
    # Executor
    "PlanExecutor",
    # Protocols
    "BridgeQuoteProvider",
    "OnChainStateProvider",
    # Constants
    "DEFAULT_STALE_QUOTE_THRESHOLD_SECONDS",
]
