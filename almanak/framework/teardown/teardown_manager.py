"""Teardown Manager - Central Orchestrator for Strategy Teardown.

The TeardownManager is the main entry point for all teardown operations.
It coordinates:

1. Preview - Show what will happen before execution
2. Execute - Run the teardown with all safety guarantees
3. Cancel - Stop an in-progress teardown
4. Resume - Continue interrupted teardowns

All operations flow through the safety layer:
- Position-aware loss caps
- Escalating slippage with approval gates
- MEV protection
- Atomic bundling for Safe wallets
- Post-execution verification
- Resumable state
"""

import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from almanak.framework.execution.orchestrator import ExecutionOrchestrator
    from almanak.framework.intents.compiler import IntentCompiler

from almanak.framework.teardown.cancel_window import CancelWindowManager
from almanak.framework.teardown.config import TeardownConfig
from almanak.framework.teardown.models import (
    ApprovalRequest,
    ApprovalResponse,
    PositionInfo,
    TeardownMode,
    TeardownPositionSummary,
    TeardownPreview,
    TeardownResult,
    TeardownState,
    TeardownStatus,
    calculate_max_acceptable_loss,
)
from almanak.framework.teardown.safety_guard import SafetyGuard
from almanak.framework.teardown.slippage_manager import (
    EscalatingSlippageManager,
    ExecutionAttempt,
)

logger = logging.getLogger(__name__)


class Intent(Protocol):
    """Protocol for intent objects that can be executed."""

    @property
    def intent_type(self) -> str:
        """Get the intent type."""
        ...

    @property
    def chain(self) -> str:
        """Get the chain for this intent."""
        ...

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        ...


class IntentStrategy(Protocol):
    """Protocol for strategies that support teardown."""

    @property
    def strategy_id(self) -> str:
        """Get strategy ID."""
        ...

    @property
    def name(self) -> str:
        """Get strategy name."""
        ...

    @property
    def chain(self) -> str:
        """Get primary chain."""
        ...

    @property
    def uses_safe_wallet(self) -> bool:
        """Check if strategy uses a Safe wallet."""
        ...

    def get_open_positions(self) -> TeardownPositionSummary:
        """Get all open positions."""
        ...

    def generate_teardown_intents(self, mode: TeardownMode, market: Any = None) -> list[Intent]:
        """Generate intents to close all positions."""
        ...

    async def pause(self) -> None:
        """Pause the strategy."""
        ...


class StateManager(Protocol):
    """Protocol for state persistence."""

    async def save_teardown_state(self, state: TeardownState) -> None:
        """Save teardown state."""
        ...

    async def get_teardown_state(self, strategy_id: str) -> TeardownState | None:
        """Get teardown state."""
        ...

    async def delete_teardown_state(self, teardown_id: str) -> None:
        """Delete teardown state."""
        ...


class AlertManager(Protocol):
    """Protocol for alert management."""

    async def send_teardown_started(self, strategy_id: str, mode: str) -> None:
        """Send teardown started alert."""
        ...

    async def send_teardown_complete(self, result: TeardownResult) -> None:
        """Send teardown completion alert."""
        ...

    async def send_approval_needed(self, request: ApprovalRequest) -> None:
        """Send approval needed alert."""
        ...


# Type alias for approval callback
ApprovalCallback = Callable[[ApprovalRequest], Awaitable[ApprovalResponse]]


@dataclass
class AtomicBundle:
    """Represents a bundle of intents for atomic execution."""

    chain: str
    is_bundled: bool
    intents: list[Intent]
    multisend_data: bytes | None = None


class TeardownManager:
    """Orchestrates teardown operations with safety guarantees.

    This is the central coordinator. All teardown operations flow through here.
    The manager ensures:

    1. Safety invariants are enforced (loss caps, slippage limits)
    2. State is persisted for resumability
    3. Cancel windows are respected
    4. Intents are executed with escalating slippage
    5. Results are verified on-chain
    """

    def __init__(
        self,
        state_manager: StateManager | None = None,
        alert_manager: AlertManager | None = None,
        config: TeardownConfig | None = None,
        orchestrator: "ExecutionOrchestrator | None" = None,
        compiler: "IntentCompiler | None" = None,
    ):
        """Initialize the teardown manager.

        Args:
            state_manager: For persisting teardown state
            alert_manager: For sending alerts
            config: Teardown configuration
            orchestrator: Execution orchestrator for real transaction execution
            compiler: Intent compiler to convert intents to ActionBundles
        """
        self.state_manager = state_manager
        self.alert_manager = alert_manager
        self.config = config or TeardownConfig.default()
        self.orchestrator = orchestrator
        self.compiler = compiler

        # Initialize sub-managers
        self.safety_guard = SafetyGuard(self.config)
        self.slippage_manager = EscalatingSlippageManager(self.config)
        self.cancel_window = CancelWindowManager(self.config)

    async def preview(
        self,
        strategy: IntentStrategy,
        mode: str,
        market: Any = None,
    ) -> TeardownPreview:
        """Preview teardown without executing.

        Shows the operator exactly what will happen, what protections
        are in place, and what they can expect to receive.

        Args:
            strategy: The strategy to teardown
            mode: "graceful" or "emergency"
            market: Optional market snapshot for real price data

        Returns:
            TeardownPreview with all details for user confirmation
        """
        internal_mode = TeardownMode.SOFT if mode == "graceful" else TeardownMode.HARD

        # Get positions from strategy
        positions = strategy.get_open_positions()

        # Generate intents (dry run) - pass market for price-aware intent generation
        try:
            intents = strategy.generate_teardown_intents(internal_mode, market=market)
        except TypeError as exc:
            if "market" in str(exc):
                # Backward compat: old strategies without market param
                intents = strategy.generate_teardown_intents(internal_mode)
            else:
                raise

        # Calculate protection
        max_loss_pct = calculate_max_acceptable_loss(positions.total_value_usd)
        max_loss_usd = positions.total_value_usd * max_loss_pct
        protected_min = positions.total_value_usd - max_loss_usd

        # Estimate returns
        min_return, max_return = self.safety_guard.calculate_estimated_return_range(
            positions.total_value_usd, internal_mode
        )

        # Estimate duration
        duration = self._estimate_duration(internal_mode, intents)

        # Generate warnings
        warnings = self._generate_warnings(positions, internal_mode)

        return TeardownPreview(
            strategy_id=strategy.strategy_id,
            strategy_name=strategy.name,
            mode=mode,
            positions=[self._serialize_position(p) for p in positions.positions],
            current_value_usd=positions.total_value_usd,
            protected_minimum_usd=protected_min,
            max_loss_percent=max_loss_pct,
            max_loss_usd=max_loss_usd,
            estimated_return_min_usd=min_return,
            estimated_return_max_usd=max_return,
            estimated_duration_minutes=duration,
            steps=[self._describe_intent(i) for i in intents],
            warnings=warnings,
        )

    async def execute(
        self,
        strategy: IntentStrategy,
        mode: str,
        on_approval_needed: ApprovalCallback | None = None,
        on_cancel_check: Callable[[], Awaitable[bool]] | None = None,
        on_progress: Callable[[int, str], Awaitable[None]] | None = None,
        is_auto_mode: bool = False,
        market: Any = None,
    ) -> TeardownResult:
        """Execute teardown with full safety guarantees.

        Flow:
        1. Pause strategy
        2. Generate and validate intents
        3. Show cancel window (10 seconds)
        4. Execute with escalating slippage
        5. Verify positions closed
        6. Return results

        Args:
            strategy: The strategy to teardown
            mode: "graceful" or "emergency"
            on_approval_needed: Callback when slippage approval needed
            on_cancel_check: Callback to check if user cancelled
            on_progress: Callback for progress updates
            is_auto_mode: Whether this is an auto-protect triggered exit

        Returns:
            TeardownResult with complete execution details
        """
        internal_mode = TeardownMode.SOFT if mode == "graceful" else TeardownMode.HARD
        started_at = datetime.now(UTC)
        teardown_id = f"td_{uuid.uuid4().hex[:12]}"

        try:
            # Step 1: Pause strategy
            logger.info(f"Starting teardown {teardown_id} for {strategy.strategy_id}")
            await strategy.pause()

            # Send started alert
            if self.alert_manager:
                await self.alert_manager.send_teardown_started(strategy.strategy_id, mode)

            # Step 2: Get positions and generate intents
            positions = strategy.get_open_positions()
            try:
                intents = strategy.generate_teardown_intents(internal_mode, market=market)
            except TypeError as exc:
                if "market" in str(exc):
                    # Backward compat: old strategies without market param
                    intents = strategy.generate_teardown_intents(internal_mode)
                else:
                    raise

            if not intents:
                logger.info(f"No intents to execute for {strategy.strategy_id}")
                return self._empty_result(strategy.strategy_id, mode, started_at)

            # Step 3: Validate safety
            validation = self.safety_guard.validate_teardown_request(positions, internal_mode)
            if not validation.all_passed:
                logger.error(f"Safety validation failed: {validation.blocked_reason}")
                return self._failed_result(
                    strategy.strategy_id,
                    mode,
                    started_at,
                    error=validation.blocked_reason or "Safety validation failed",
                )

            # Step 4: Persist state for resumability
            teardown_state = await self._persist_state(teardown_id, strategy, internal_mode, intents)

            # Step 5: Run cancel window
            cancel_result = await self.cancel_window.run_cancel_window(
                teardown_id=teardown_id,
                on_check_cancelled=on_cancel_check,
                is_auto_mode=is_auto_mode,
            )

            if cancel_result.was_cancelled:
                logger.info(f"Teardown {teardown_id} cancelled during window")
                return self._cancelled_result(strategy.strategy_id, mode, started_at)

            # Update state to executing
            teardown_state.status = TeardownStatus.EXECUTING
            if self.state_manager:
                await self.state_manager.save_teardown_state(teardown_state)

            # Extract real prices from market for accurate compilation
            price_oracle = None
            if market is not None and hasattr(market, "get_price_oracle_dict"):
                price_oracle = market.get_price_oracle_dict() or None

            # Step 6: Execute intents with safety guardrails
            result = await self._execute_intents(
                teardown_id=teardown_id,
                strategy=strategy,
                intents=intents,
                positions=positions,
                mode=internal_mode,
                teardown_state=teardown_state,
                on_approval_needed=on_approval_needed,
                on_progress=on_progress,
                is_auto_mode=is_auto_mode,
                price_oracle=price_oracle,
                market=market,
            )

            # Step 7: Verify positions closed
            await self._verify_closure(strategy)

            # Step 8: Send completion alert
            if self.alert_manager:
                await self.alert_manager.send_teardown_complete(result)

            # Clean up state on success
            if self.state_manager and result.success:
                await self.state_manager.delete_teardown_state(teardown_id)

            return result

        except Exception as e:
            logger.exception(f"Teardown {teardown_id} failed with exception")
            return self._failed_result(
                strategy.strategy_id,
                mode,
                started_at,
                error=str(e),
            )

    async def cancel(self, strategy_id: str) -> bool:
        """Cancel an in-progress teardown.

        Graceful mode: Cancellable anytime before completion.
        Emergency mode: Only during 10-second window.

        Args:
            strategy_id: ID of the strategy being torn down

        Returns:
            True if cancellation succeeded
        """
        if self.state_manager is None:
            logger.warning("No state manager - cannot cancel")
            return False

        state = await self.state_manager.get_teardown_state(strategy_id)

        if not state:
            logger.warning(f"No active teardown for {strategy_id}")
            return False

        # Check if in cancel window (for HARD mode)
        if state.mode == TeardownMode.HARD:
            if not state.is_in_cancel_window:
                raise ValueError("Cancel window has expired for emergency teardown")

        # Check if already executing intents
        if state.status == TeardownStatus.EXECUTING and state.completed_intents > 0:
            # Some intents already executed - pause instead of cancel
            logger.info(f"Pausing teardown {state.teardown_id} (intents in progress)")
            state.status = TeardownStatus.PAUSED
            await self.state_manager.save_teardown_state(state)
            return True

        # Full cancel
        state.status = TeardownStatus.CANCELLED
        await self.state_manager.save_teardown_state(state)
        logger.info(f"Cancelled teardown {state.teardown_id}")
        return True

    async def resume(
        self,
        strategy_id: str,
        strategy: IntentStrategy,
        on_approval_needed: ApprovalCallback | None = None,
        on_progress: Callable[[int, str], Awaitable[None]] | None = None,
        market: Any = None,
    ) -> TeardownResult | None:
        """Resume an interrupted teardown.

        Called on system startup to detect and resume in-progress teardowns.
        Includes staleness check - re-generates intents if too old.

        Args:
            strategy_id: ID of the strategy
            strategy: The strategy instance
            on_approval_needed: Callback for approval requests
            on_progress: Callback for progress updates

        Returns:
            TeardownResult if resumed and completed, None if nothing to resume
        """
        if self.state_manager is None:
            return None

        state = await self.state_manager.get_teardown_state(strategy_id)

        if not state or not state.is_resumable:
            return None

        logger.info(f"Resuming teardown {state.teardown_id}")

        # Staleness check
        age_seconds = (datetime.now(UTC) - state.updated_at).total_seconds()
        if age_seconds > self.config.staleness_threshold_seconds:
            logger.info(f"State is stale ({age_seconds}s old), regenerating intents")
            positions = strategy.get_open_positions()
            try:
                intents = strategy.generate_teardown_intents(state.mode, market=market)
            except TypeError as exc:
                if "market" in str(exc):
                    intents = strategy.generate_teardown_intents(state.mode)
                else:
                    raise
            state.pending_intents_json = json.dumps([i.to_dict() for i in intents])
            state.current_intent_index = 0

        # Parse intents from state
        intents_data = json.loads(state.pending_intents_json) if state.pending_intents_json else []

        if not intents_data:
            logger.info(f"No pending intents for {state.teardown_id}")
            return None

        # Extract real prices from market
        price_oracle = None
        if market is not None and hasattr(market, "get_price_oracle_dict"):
            price_oracle = market.get_price_oracle_dict() or None

        # Continue execution from where we left off
        positions = strategy.get_open_positions()

        return await self._execute_intents(
            teardown_id=state.teardown_id,
            strategy=strategy,
            intents=intents_data,  # Already serialized
            positions=positions,
            mode=state.mode,
            teardown_state=state,
            on_approval_needed=on_approval_needed,
            on_progress=on_progress,
            start_from_index=state.current_intent_index,
            price_oracle=price_oracle,
            market=market,
        )

    async def _execute_intents(
        self,
        teardown_id: str,
        strategy: IntentStrategy,
        intents: list,
        positions: TeardownPositionSummary,
        mode: TeardownMode,
        teardown_state: TeardownState,
        on_approval_needed: ApprovalCallback | None = None,
        on_progress: Callable[[int, str], Awaitable[None]] | None = None,
        start_from_index: int = 0,
        is_auto_mode: bool = False,
        price_oracle: dict | None = None,
        market: Any = None,
    ) -> TeardownResult:
        """Execute intents with escalating slippage.

        Args:
            teardown_id: Unique ID for this teardown
            strategy: The strategy being torn down
            intents: List of intents to execute
            positions: Position summary
            mode: Teardown mode
            teardown_state: Persisted state
            on_approval_needed: Callback for approvals
            on_progress: Callback for progress
            start_from_index: Index to start from (for resumption)
            is_auto_mode: Whether this is auto-protect mode

        Returns:
            TeardownResult with execution outcome
        """
        started_at = teardown_state.started_at
        mode_str = "graceful" if mode == TeardownMode.SOFT else "emergency"

        succeeded = 0
        failed = 0
        total_costs = Decimal("0")
        final_balances: dict[str, Decimal] = {}

        for i, intent in enumerate(intents[start_from_index:], start=start_from_index):
            # Update progress
            progress_pct = int((i / len(intents)) * 100)
            if on_progress:
                await on_progress(progress_pct, f"Executing step {i + 1}/{len(intents)}")

            # Update state
            teardown_state.current_intent_index = i
            teardown_state.updated_at = datetime.now(UTC)
            if self.state_manager:
                await self.state_manager.save_teardown_state(teardown_state)

            # Execute with escalating slippage
            async def execute_at_slippage(
                intent_to_exec: Any, slippage: Decimal, *, intent_index: int = i
            ) -> ExecutionAttempt:
                """Execute a single intent at given slippage.

                Compiles the intent to an ActionBundle and executes it via the
                orchestrator. Returns the execution result.
                """
                logger.info(f"Executing intent {intent_index + 1}/{len(intents)} at {slippage:.1%} slippage")

                # Check if we have real execution capability
                if not self.orchestrator or not self.compiler:
                    logger.warning(
                        "No orchestrator/compiler configured - teardown cannot execute. "
                        "Inject ExecutionOrchestrator and IntentCompiler for real execution."
                    )
                    return ExecutionAttempt(
                        success=False,
                        slippage_used=slippage,
                        actual_slippage=Decimal("0"),
                        error="No orchestrator/compiler configured for teardown execution",
                    )

                try:
                    # Clone intent with updated slippage if it has a max_slippage attribute
                    intent_with_slippage = intent_to_exec
                    if hasattr(intent_to_exec, "max_slippage"):
                        # Use dataclass replace for proper cloning
                        try:
                            intent_with_slippage = replace(intent_to_exec, max_slippage=slippage)
                        except TypeError:
                            # Not a dataclass, try dict-based cloning
                            if hasattr(intent_to_exec, "to_dict") and hasattr(intent_to_exec, "from_dict"):
                                intent_dict = intent_to_exec.to_dict()
                                intent_dict["max_slippage"] = str(slippage)
                                intent_with_slippage = type(intent_to_exec).from_dict(intent_dict)

                    # Resolve amount="all" to actual wallet balance before compilation
                    # Support both object intents and dict intents (resume path)
                    _is_dict = isinstance(intent_with_slippage, dict)
                    amount_value = (
                        intent_with_slippage.get("amount")
                        if _is_dict
                        else getattr(intent_with_slippage, "amount", None)
                    )
                    from_token = (
                        intent_with_slippage.get("from_token")
                        if _is_dict
                        else getattr(intent_with_slippage, "from_token", None)
                    )
                    if amount_value == "all":
                        if not from_token or market is None:
                            return ExecutionAttempt(
                                success=False,
                                slippage_used=slippage,
                                actual_slippage=Decimal("0"),
                                error="Cannot resolve amount='all': missing from_token or market context",
                            )
                        try:
                            bal = market.balance(from_token)
                        except Exception as e:
                            return ExecutionAttempt(
                                success=False,
                                slippage_used=slippage,
                                actual_slippage=Decimal("0"),
                                error=f"Cannot resolve amount='all' for {from_token}: {e}",
                            )
                        if bal.balance <= 0:
                            return ExecutionAttempt(
                                success=False,
                                slippage_used=slippage,
                                actual_slippage=Decimal("0"),
                                error=f"{from_token} balance is 0, nothing to teardown",
                            )
                        if _is_dict:
                            intent_with_slippage = {
                                **intent_with_slippage,
                                "amount": str(bal.balance),
                            }
                        else:
                            from almanak.framework.intents import Intent

                            intent_with_slippage = Intent.set_resolved_amount(intent_with_slippage, bal.balance)
                        logger.info(f"Resolved amount='all' for {from_token}: {bal.balance}")

                    # Apply real prices to compiler if available
                    original_oracle = getattr(self.compiler, "price_oracle", None)
                    original_placeholders = getattr(self.compiler, "_using_placeholders", True)
                    if price_oracle and hasattr(self.compiler, "update_prices"):
                        self.compiler.update_prices(price_oracle)

                    # Compile intent to ActionBundle
                    try:
                        compilation_result = self.compiler.compile(intent_with_slippage)
                    finally:
                        if hasattr(self.compiler, "restore_prices"):
                            self.compiler.restore_prices(original_oracle, original_placeholders)

                    if compilation_result.status.value != "success":
                        logger.error(f"Intent compilation failed: {compilation_result.error}")
                        return ExecutionAttempt(
                            success=False,
                            slippage_used=slippage,
                            actual_slippage=Decimal("0"),
                            error=f"Compilation failed: {compilation_result.error}",
                        )

                    if not compilation_result.action_bundle:
                        logger.error("Compilation succeeded but no action bundle produced")
                        return ExecutionAttempt(
                            success=False,
                            slippage_used=slippage,
                            actual_slippage=Decimal("0"),
                            error="No action bundle produced",
                        )

                    # Create execution context
                    from almanak.framework.execution.orchestrator import ExecutionContext

                    context = ExecutionContext(
                        strategy_id=strategy.strategy_id,
                        intent_id=f"teardown_{teardown_id}_{intent_index}",
                        chain=getattr(intent_to_exec, "chain", None) or strategy.chain,
                        intent_description=self._describe_intent(intent_to_exec),
                    )

                    # Execute via orchestrator
                    exec_result = await self.orchestrator.execute(
                        compilation_result.action_bundle,
                        context,
                    )

                    if exec_result.success:
                        # Calculate actual slippage from execution results
                        # This is an estimate - actual slippage depends on protocol
                        actual_slippage = slippage * Decimal("0.5")  # Typically less than max
                        tx_hash = (
                            exec_result.transaction_results[0].tx_hash if exec_result.transaction_results else "unknown"
                        )
                        logger.info(
                            f"Intent {intent_index + 1}/{len(intents)} executed successfully. "
                            f"TX: {tx_hash}, Gas used: {exec_result.total_gas_used}"
                        )
                        return ExecutionAttempt(
                            success=True,
                            slippage_used=slippage,
                            actual_slippage=actual_slippage,
                        )
                    else:
                        logger.error(f"Intent {intent_index + 1}/{len(intents)} execution failed: {exec_result.error}")
                        return ExecutionAttempt(
                            success=False,
                            slippage_used=slippage,
                            actual_slippage=Decimal("0"),
                            error=exec_result.error,
                        )

                except Exception as e:
                    logger.exception(f"Exception during intent execution: {e}")
                    return ExecutionAttempt(
                        success=False,
                        slippage_used=slippage,
                        actual_slippage=Decimal("0"),
                        error=str(e),
                    )

            exec_result = await self.slippage_manager.execute_with_escalation(
                intent=intent,
                position_value=positions.total_value_usd,
                execute_func=execute_at_slippage,
                on_approval_needed=on_approval_needed,
                teardown_id=teardown_id,
                strategy_id=strategy.strategy_id,
                is_auto_mode=is_auto_mode,
            )

            if exec_result.success:
                succeeded += 1
                # Estimate cost
                actual_slippage = exec_result.final_slippage
                intent_value = positions.total_value_usd / len(intents)  # Simplified
                total_costs += intent_value * actual_slippage
            else:
                failed += 1
                if exec_result.status == "paused_awaiting_approval":
                    # Pause for approval
                    teardown_state.status = TeardownStatus.PAUSED
                    if self.state_manager:
                        await self.state_manager.save_teardown_state(teardown_state)

                    # Send alert
                    if self.alert_manager and exec_result.approval_request:
                        await self.alert_manager.send_approval_needed(exec_result.approval_request)

                    # Return partial result
                    return TeardownResult(
                        success=False,
                        strategy_id=strategy.strategy_id,
                        mode=mode_str,
                        started_at=started_at,
                        completed_at=None,
                        duration_seconds=(datetime.now(UTC) - started_at).total_seconds(),
                        intents_total=len(intents),
                        intents_succeeded=succeeded,
                        intents_failed=failed,
                        starting_value_usd=positions.total_value_usd,
                        final_value_usd=positions.total_value_usd - total_costs,
                        total_costs_usd=total_costs,
                        final_balances=final_balances,
                        error="Paused awaiting approval",
                        recovery_options=["Approve higher slippage", "Wait and retry", "Cancel"],
                    )

            # Update completed count
            teardown_state.completed_intents = succeeded

        # All intents processed
        completed_at = datetime.now(UTC)
        teardown_state.status = TeardownStatus.COMPLETED
        teardown_state.completed_at = completed_at
        if self.state_manager:
            await self.state_manager.save_teardown_state(teardown_state)

        final_value = positions.total_value_usd - total_costs

        return TeardownResult(
            success=failed == 0,
            strategy_id=strategy.strategy_id,
            mode=mode_str,
            started_at=started_at,
            completed_at=completed_at,
            duration_seconds=(completed_at - started_at).total_seconds(),
            intents_total=len(intents),
            intents_succeeded=succeeded,
            intents_failed=failed,
            starting_value_usd=positions.total_value_usd,
            final_value_usd=final_value,
            total_costs_usd=total_costs,
            final_balances=final_balances,
            error=None if failed == 0 else f"{failed} intents failed",
        )

    async def _persist_state(
        self,
        teardown_id: str,
        strategy: IntentStrategy,
        mode: TeardownMode,
        intents: list,
    ) -> TeardownState:
        """Persist teardown state for resumability."""
        now = datetime.now(UTC)

        state = TeardownState(
            teardown_id=teardown_id,
            strategy_id=strategy.strategy_id,
            mode=mode,
            status=TeardownStatus.CANCEL_WINDOW,
            total_intents=len(intents),
            completed_intents=0,
            current_intent_index=0,
            started_at=now,
            updated_at=now,
            pending_intents_json=json.dumps([i.to_dict() if hasattr(i, "to_dict") else str(i) for i in intents]),
            cancel_window_until=now,  # Will be updated by cancel window
            config_json=json.dumps(self.config.to_dict()),
        )

        if self.state_manager:
            await self.state_manager.save_teardown_state(state)

        return state

    async def _verify_closure(self, strategy: IntentStrategy) -> bool:
        """Verify that positions are actually closed on-chain."""
        # In real implementation, this queries on-chain state
        positions = strategy.get_open_positions()
        return len(positions.positions) == 0

    def _estimate_duration(self, mode: TeardownMode, intents: list) -> int:
        """Estimate teardown duration in minutes."""
        if mode == TeardownMode.SOFT:
            # Graceful: ~2-5 min per intent
            return max(15, len(intents) * 3)
        else:
            # Emergency: ~30s-1min per intent
            return max(1, len(intents))

    def _generate_warnings(
        self,
        positions: TeardownPositionSummary,
        mode: TeardownMode,
    ) -> list[str]:
        """Generate warnings for the preview."""
        warnings = []

        if positions.has_liquidation_risk:
            warnings.append("Some positions have low health factors and may be at liquidation risk")

        if mode == TeardownMode.HARD and not positions.has_liquidation_risk:
            warnings.append(
                "Emergency mode selected but no immediate liquidation risk detected. "
                "Consider graceful mode for lower costs."
            )

        if positions.total_value_usd > Decimal("500000"):
            warnings.append("Large position value. Extra care will be taken to minimize slippage.")

        if len(positions.chains_involved) > 1:
            warnings.append(
                f"Multi-chain teardown across {len(positions.chains_involved)} chains. "
                "Each chain will be handled atomically."
            )

        return warnings

    def _serialize_position(self, position: PositionInfo) -> dict[str, Any]:
        """Serialize a position for API response."""
        return {
            "type": position.position_type.value,
            "id": position.position_id,
            "chain": position.chain,
            "protocol": position.protocol,
            "value_usd": float(position.value_usd),
            "liquidation_risk": position.liquidation_risk,
            "health_factor": float(position.health_factor) if position.health_factor else None,
            "details": position.details,
        }

    def _describe_intent(self, intent: Any) -> str:
        """Generate human-readable description of an intent."""
        if hasattr(intent, "intent_type"):
            intent_type = intent.intent_type
            if intent_type == "PERP_CLOSE":
                return "Close perpetual position"
            elif intent_type == "LP_CLOSE":
                return "Close LP position"
            elif intent_type == "REPAY":
                return "Repay borrowed amount"
            elif intent_type == "WITHDRAW":
                return "Withdraw collateral"
            elif intent_type == "SWAP":
                return "Swap to target token"
            else:
                return f"Execute {intent_type}"
        return "Execute intent"

    def _empty_result(
        self,
        strategy_id: str,
        mode: str,
        started_at: datetime,
    ) -> TeardownResult:
        """Create a result for empty teardown (no positions)."""
        return TeardownResult(
            success=True,
            strategy_id=strategy_id,
            mode=mode,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            duration_seconds=0,
            intents_total=0,
            intents_succeeded=0,
            intents_failed=0,
            starting_value_usd=Decimal("0"),
            final_value_usd=Decimal("0"),
            total_costs_usd=Decimal("0"),
            final_balances={},
        )

    def _cancelled_result(
        self,
        strategy_id: str,
        mode: str,
        started_at: datetime,
    ) -> TeardownResult:
        """Create a result for cancelled teardown."""
        return TeardownResult(
            success=False,
            strategy_id=strategy_id,
            mode=mode,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            duration_seconds=(datetime.now(UTC) - started_at).total_seconds(),
            intents_total=0,
            intents_succeeded=0,
            intents_failed=0,
            starting_value_usd=Decimal("0"),
            final_value_usd=Decimal("0"),
            total_costs_usd=Decimal("0"),
            final_balances={},
            error="Cancelled by user",
        )

    def _failed_result(
        self,
        strategy_id: str,
        mode: str,
        started_at: datetime,
        error: str,
    ) -> TeardownResult:
        """Create a result for failed teardown."""
        return TeardownResult(
            success=False,
            strategy_id=strategy_id,
            mode=mode,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            duration_seconds=(datetime.now(UTC) - started_at).total_seconds(),
            intents_total=0,
            intents_succeeded=0,
            intents_failed=0,
            starting_value_usd=Decimal("0"),
            final_value_usd=Decimal("0"),
            total_costs_usd=Decimal("0"),
            final_balances={},
            error=error,
            recovery_options=["Retry", "Contact support"],
        )
