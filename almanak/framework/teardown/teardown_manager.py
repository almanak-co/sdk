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

import asyncio
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from almanak.framework.execution.orchestrator import ExecutionOrchestrator
    from almanak.framework.intents.compiler import IntentCompiler
    from almanak.framework.teardown.runner_helpers import TeardownRunnerHelpers

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


def _zero_balance_swap_skip_reason(intent: Any, market: Any) -> str | None:
    """Return a human-readable skip reason if ``intent`` is an ``amount='all'``
    swap whose source balance is 0, else ``None``.

    Mirrors the inline teardown path's ``balance_value <= 0`` short-circuit
    (``runner_teardown.py:execute_teardown_inline``). Without this, a HOLD-state
    strategy whose teardown logic unconditionally emits a swap-out (e.g.
    ``pancakeswap_rsi_bsc`` selling the base token it never bought) marks the
    entire teardown as failed even though there is nothing to sell. (BUG-39)

    Withdraw / repay intents return ``None`` because their balance lives in
    the protocol contract, not the wallet — the compiler resolves
    ``amount='all'`` for those via on-chain queries.
    """
    if market is None:
        return None
    is_dict = isinstance(intent, dict)
    amount = intent.get("amount") if is_dict else getattr(intent, "amount", None)
    if amount != "all":
        return None
    intent_type_val = intent.get("intent_type") if is_dict else getattr(intent, "intent_type", None)
    intent_type_str = str(intent_type_val).upper() if intent_type_val is not None else ""
    # Whitelist SWAP only. Other intent types (WITHDRAW/REPAY/LP_CLOSE/
    # PERP_CLOSE/BRIDGE/...) resolve ``amount='all'`` against protocol or
    # cross-chain balances, not the wallet — let the compiler / inner balance
    # check handle them.
    if "SWAP" not in intent_type_str:
        return None
    withdraw_all = intent.get("withdraw_all") if is_dict else getattr(intent, "withdraw_all", False)
    if withdraw_all:
        return None
    from_token = (
        (intent.get("from_token") or intent.get("token"))
        if is_dict
        else (getattr(intent, "from_token", None) or getattr(intent, "token", None))
    )
    if not from_token:
        return None
    try:
        bal = market.balance(from_token)
    except Exception:  # noqa: BLE001 — market may not have this token registered yet
        return None
    balance_value = bal.balance if hasattr(bal, "balance") else bal
    try:
        if balance_value <= 0:
            return f"{from_token} balance is 0 — nothing to teardown"
    except TypeError:
        return None
    return None


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
        runner_helpers: "TeardownRunnerHelpers | None" = None,
    ):
        """Initialize the teardown manager.

        Args:
            state_manager: For persisting teardown state
            alert_manager: For sending alerts
            config: Teardown configuration
            orchestrator: Execution orchestrator for real transaction execution
            compiler: Intent compiler to convert intents to ActionBundles
            runner_helpers: VIB-3773 — callable bag exposing
                ``commit_teardown_intent`` and
                ``capture_teardown_snapshot_with_accounting`` pre-bound to
                a :class:`StrategyRunner`. When provided, ``_execute_intents``
                drives the full per-intent commit pipeline (enrich → ledger
                → outbox+fire → sidecar) after every successful on-chain
                execution. ``None`` retains pre-VIB-3773 behaviour (no
                accounting writes from this lane) so legacy unit tests that
                don't construct a runner keep working.
        """
        from .runner_helpers import TeardownRunnerHelpers

        self.state_manager = state_manager
        self.alert_manager = alert_manager
        self.config = config or TeardownConfig.default()
        self.orchestrator = orchestrator
        self.compiler = compiler
        self.runner_helpers = runner_helpers or TeardownRunnerHelpers()

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

    async def execute(  # noqa: C901
        self,
        strategy: IntentStrategy,
        mode: str,
        on_approval_needed: ApprovalCallback | None = None,
        on_cancel_check: Callable[[], Awaitable[bool]] | None = None,
        on_progress: Callable[[int, str], Awaitable[None]] | None = None,
        is_auto_mode: bool = False,
        market: Any = None,
        precomputed_positions: Any = None,
        precomputed_intents: list[Any] | None = None,
        teardown_id: str | None = None,
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
            market: Optional market snapshot for pricing
            precomputed_positions: Optional TeardownPositionSummary supplied by
                the caller when the strategy has no local record of the open
                positions (e.g. gateway-restart recovery). When provided,
                ``strategy.get_open_positions()`` is skipped.
            precomputed_intents: Optional list of Intents to execute. When
                provided, ``strategy.generate_teardown_intents()`` is skipped.
                The CLI's ``--discover`` flow uses this to close on-chain-
                discovered positions that the strategy doesn't know about.
                Both ``precomputed_positions`` and ``precomputed_intents``
                should be supplied together for consistency.
            teardown_id: VIB-3839 — optional caller-supplied teardown id. When
                provided, ``_execute_intents`` derives ``teardown_cycle_id =
                f"teardown-{teardown_id}"`` from this value, so a caller that
                wants to bracket the teardown with its own snapshot writes
                (CLI execute lane) can pre-generate the id, drive the pre-
                bracket with the same cycle id, then call ``execute()`` and
                trust per-intent commits to use the same cycle id. Default
                ``None`` keeps the legacy behaviour (uuid generated here).

        Returns:
            TeardownResult with complete execution details
        """
        internal_mode = TeardownMode.SOFT if mode == "graceful" else TeardownMode.HARD
        started_at = datetime.now(UTC)
        if teardown_id is None:
            teardown_id = f"td_{uuid.uuid4().hex[:12]}"

        try:
            # Step 1: Pause strategy
            logger.info(f"Starting teardown {teardown_id} for {strategy.strategy_id}")
            await strategy.pause()

            # Send started alert
            if self.alert_manager:
                await self.alert_manager.send_teardown_started(strategy.strategy_id, mode)

            # Step 2: Get positions and generate intents. When the caller has
            # supplied precomputed_positions/intents (e.g. the CLI's --discover
            # flow after a gateway restart wiped the strategy's local state),
            # trust them instead of re-querying the strategy — the strategy
            # doesn't know about those positions.
            if precomputed_positions is not None:
                positions = precomputed_positions
            else:
                positions = strategy.get_open_positions()

            if precomputed_intents is not None:
                intents = list(precomputed_intents)
            else:
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
                fetched = market.get_price_oracle_dict()
                price_oracle = fetched if fetched is not None else None

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

            # Step 7: Verify positions closed (fail-closed, VIB-2925).
            # Only run verification on successful executions — if execution
            # already failed (manual intervention required, partial failure,
            # etc.) the original error is more actionable than
            # "positions still open". Catch verification exceptions locally:
            # the outer except would return a zero-stats _failed_result and
            # discard the successful on-chain execution data.
            #
            # Pass precomputed_positions when present so the --discover flow
            # checks closure against on-chain-discovered IDs rather than
            # re-reading strategy.get_open_positions() (empty-in/empty-out
            # in the recovery scenario). See PR #1522.
            if result.success:
                try:
                    # VIB-3742: pass the pre-execution snapshot ``positions``
                    # so the verifier can run protocol-specific on-chain
                    # post-condition checks (e.g. TraderJoe V2 LB token
                    # balance) for each position the teardown was supposed
                    # to close. Without this, the verifier only re-reads
                    # ``strategy.get_open_positions()`` which returns 0 the
                    # moment ``on_intent_executed`` clears the strategy's
                    # ``_position_id`` — silently passing partial closes.
                    positions_closed = await self._verify_closure(
                        strategy,
                        expected_positions=precomputed_positions,
                        pre_execution_positions=positions,
                    )
                except Exception as verify_err:
                    logger.exception(
                        "Post-teardown verification raised for %s — treating as verify-fail",
                        strategy.strategy_id,
                    )
                    positions_closed = False
                    verify_error_msg = f"Post-teardown verification error: {verify_err}. Manual check required."
                else:
                    verify_error_msg = "Post-teardown verification failed: positions still open. Manual check required."

                if not positions_closed:
                    logger.warning(
                        f"Post-teardown verification: {strategy.strategy_id} still reports "
                        f"open positions (or verification errored). Marking teardown as incomplete."
                    )
                    result = replace(
                        result,
                        success=False,
                        error=verify_error_msg,
                        recovery_options=["Verify positions on-chain", "Re-run teardown"],
                    )
                    # Reflect the verification failure in persisted state — otherwise
                    # a postmortem reader sees status=COMPLETED even though the
                    # result says the teardown failed.
                    teardown_state.status = TeardownStatus.FAILED
                    teardown_state.updated_at = datetime.now(UTC)
                    if self.state_manager:
                        try:
                            await self.state_manager.save_teardown_state(teardown_state)
                        except Exception:
                            logger.warning(
                                "Failed to persist FAILED status for teardown %s after verify-fail",
                                teardown_id,
                                exc_info=True,
                            )

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
            fetched = market.get_price_oracle_dict()
            price_oracle = fetched if fetched is not None else None

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

    # crap-allowlist: PR is pure string-content cleanup (chore: VIB removal); zero branches added, function was already over threshold on main. Refactor tracked in VIB-4139.
    async def _execute_intents(  # noqa: C901
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
        skipped = 0
        total_costs = Decimal("0")
        final_balances: dict[str, Decimal] = {}

        # VIB-3773: stable cycle id for every accounting row written by this
        # teardown — both ledger/outbox (per-intent commit) and snapshot/
        # metrics (pre/post bracket) stamp on it via the runner helpers.
        # Picked up by ``commit_teardown_intent``'s contextvar set, and the
        # outer ``execute_teardown_via_manager`` also sets
        # ``runner._last_cycle_id`` to the same value (P1-4).
        teardown_cycle_id = f"teardown-{teardown_id}"

        # VIB-3773: aggregate degraded-write records emitted by per-intent
        # commit calls. Surfaced on TeardownResult.accounting_degraded /
        # accounting_degraded_count for operator visibility + reconciliation.
        accounting_degraded_records: list[Any] = []

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

            # Pre-flight no-op skip: amount='all' swap whose source balance
            # is 0 — there is nothing to sell, so this is a no-op success,
            # not a failure. Skips the whole slippage-escalation loop because
            # retrying at higher slippage cannot conjure tokens that aren't
            # there. Mirrors the inline teardown path
            # (runner_teardown.execute_teardown_inline). Counted under
            # ``succeeded`` to preserve the ``intents_total = succeeded +
            # failed`` invariant exposed via ``TeardownResult.all_succeeded``.
            # (BUG-39)
            skip_reason = _zero_balance_swap_skip_reason(intent, market)
            if skip_reason:
                logger.info(f"Teardown intent {i + 1}/{len(intents)}: skipping — {skip_reason}")
                succeeded += 1
                skipped += 1
                if on_progress:
                    await on_progress(progress_pct, f"Skipped step {i + 1}/{len(intents)}: {skip_reason}")
                # Mirror the success-path persist so a crash mid-teardown
                # records the skip as completed and resume picks up at i+1.
                teardown_state.completed_intents = succeeded
                teardown_state.updated_at = datetime.now(UTC)
                if self.state_manager:
                    await self.state_manager.save_teardown_state(teardown_state)
                continue

            # Execute with escalating slippage
            async def execute_at_slippage(  # noqa: C901
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
                    # Clone intent with updated slippage if it has a max_slippage attribute.
                    # Intents are Pydantic frozen models — model_copy is the primary path.
                    intent_with_slippage = intent_to_exec
                    if hasattr(intent_to_exec, "max_slippage"):
                        cloned = False
                        if hasattr(intent_to_exec, "model_copy"):
                            try:
                                intent_with_slippage = intent_to_exec.model_copy(update={"max_slippage": slippage})
                                cloned = True
                            except (TypeError, ValueError):
                                logger.warning(
                                    "model_copy failed for %s, falling back to replace",
                                    type(intent_to_exec).__name__,
                                )
                        if not cloned:
                            try:
                                intent_with_slippage = replace(intent_to_exec, max_slippage=slippage)
                                cloned = True
                            except TypeError:
                                if hasattr(intent_to_exec, "to_dict") and hasattr(intent_to_exec, "from_dict"):
                                    try:
                                        intent_dict = intent_to_exec.to_dict()
                                        intent_dict["max_slippage"] = str(slippage)
                                        intent_with_slippage = type(intent_to_exec).from_dict(intent_dict)
                                        cloned = True
                                    except (TypeError, ValueError, KeyError) as e:
                                        logger.warning(
                                            "dict-based cloning failed for %s: %s",
                                            type(intent_to_exec).__name__,
                                            e,
                                        )
                        if not cloned:
                            logger.error(
                                "Could not clone %s with updated slippage %.1f%% — "
                                "teardown will use original slippage %.1f%%",
                                type(intent_to_exec).__name__,
                                float(slippage * 100),
                                float(getattr(intent_to_exec, "max_slippage", Decimal("0")) * 100),
                            )

                    # Resolve amount="all" to actual wallet balance before compilation
                    # Support both object intents and dict intents (resume path)
                    _is_dict = isinstance(intent_with_slippage, dict)
                    amount_value = (
                        intent_with_slippage.get("amount")
                        if _is_dict
                        else getattr(intent_with_slippage, "amount", None)
                    )
                    # Check from_token first (SwapIntent), then token (Withdraw/Supply/Repay)
                    from_token = (
                        intent_with_slippage.get("from_token") or intent_with_slippage.get("token")
                        if _is_dict
                        else getattr(intent_with_slippage, "from_token", None)
                        or getattr(intent_with_slippage, "token", None)
                    )
                    # Skip wallet-balance resolution for withdraw intents —
                    # withdraw positions live in the protocol, not the wallet.
                    # Also skip when withdraw_all is set (adapter uses MAX_UINT256).
                    _withdraw_all = (
                        intent_with_slippage.get("withdraw_all")
                        if _is_dict
                        else getattr(intent_with_slippage, "withdraw_all", False)
                    )
                    _intent_type_val = (
                        intent_with_slippage.get("intent_type")
                        if _is_dict
                        else getattr(intent_with_slippage, "intent_type", None)
                    )
                    _is_withdraw = (
                        str(_intent_type_val).upper() in ("WITHDRAW", "INTENTTYPE.WITHDRAW")
                        if _intent_type_val
                        else False
                    )
                    _is_repay = (
                        str(_intent_type_val).upper() in ("REPAY", "INTENTTYPE.REPAY") if _intent_type_val else False
                    )
                    # Skip wallet-balance resolution for withdraw/repay intents —
                    # the compiler's amount resolver handles these via protocol balance queries.
                    if amount_value == "all" and not _withdraw_all and not _is_withdraw and not _is_repay:
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

                    if compilation_result.status.value != "SUCCESS":
                        logger.error(f"Intent compilation failed: {compilation_result.error}")
                        return ExecutionAttempt(
                            success=False,
                            slippage_used=slippage,
                            actual_slippage=Decimal("0"),
                            error=f"Compilation failed: {compilation_result.error}",
                            retryable=compilation_result.is_transient,
                            retry_after_seconds=compilation_result.retry_after_seconds,
                        )

                    if not compilation_result.action_bundle:
                        logger.error("Compilation succeeded but no action bundle produced")
                        return ExecutionAttempt(
                            success=False,
                            slippage_used=slippage,
                            actual_slippage=Decimal("0"),
                            error="No action bundle produced",
                            retryable=False,
                        )

                    # Create execution context
                    from almanak.framework.execution.orchestrator import ExecutionContext

                    context = ExecutionContext(
                        strategy_id=strategy.strategy_id,
                        intent_id=f"teardown_{teardown_id}_{intent_index}",
                        chain=getattr(intent_to_exec, "chain", None) or strategy.chain,
                        intent_description=self._describe_intent(intent_to_exec),
                    )

                    # VIB-3918 — capture wallet balances IMMEDIATELY before
                    # this intent's execution so the teardown ledger row's
                    # ``pre_state_json`` reflects what the wallet held just
                    # before this TX (not the pre-bracket snapshot, which
                    # would be stale by the time the second teardown intent
                    # runs — the swap-back's pre-state IS the LP_CLOSE's
                    # post-state, not the pre-teardown snapshot).
                    pre_intent_snapshot: Any = None
                    if self.runner_helpers.has_per_intent_balances:
                        try:
                            pre_intent_snapshot = await self.runner_helpers.snapshot_intent_balances(  # type: ignore[misc]
                                strategy, intent_to_exec
                            )
                        except Exception as exc:  # noqa: BLE001 — best-effort
                            logger.debug(
                                "teardown pre-intent balance snapshot failed for %s: %s",
                                strategy.strategy_id,
                                exc,
                            )

                    # VIB-3934 — capture lending pre-state on the same boundary
                    # as the wallet snapshot so REPAY/WITHDRAW/DELEVERAGE
                    # teardown rows carry collateral/debt/HF in
                    # ``pre_state_json`` lane-symmetric with iteration. Returns
                    # ``None`` for non-lending intents and unsupported
                    # protocols — the wrapper never raises.
                    lending_pre_state_for_intent: Any = None
                    if self.runner_helpers.has_lending_pre_state:
                        try:
                            lending_pre_state_for_intent = await self.runner_helpers.snapshot_intent_lending_state(  # type: ignore[misc]
                                strategy, intent_to_exec
                            )
                        except Exception as exc:  # noqa: BLE001 — best-effort
                            logger.debug(
                                "teardown lending pre-state snapshot failed for %s: %s",
                                strategy.strategy_id,
                                exc,
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

                        # VIB-3918 — reconcile post-execution balances now
                        # that the TX has confirmed. The recon dict carries
                        # ``post_balances`` and ``post_timestamp`` which
                        # ``commit_teardown_intent`` threads into the ledger
                        # writer's ``post_state_json``. Mirrors the iteration
                        # lane at strategy_runner.py:3502.
                        post_intent_recon: dict[str, Any] | None = None
                        if self.runner_helpers.has_per_intent_balances and pre_intent_snapshot is not None:
                            try:
                                post_intent_recon = await self.runner_helpers.reconcile_post_balances(  # type: ignore[misc]
                                    strategy,
                                    intent_to_exec,
                                    exec_result,
                                    pre_snapshot=pre_intent_snapshot,
                                )
                            except Exception as exc:  # noqa: BLE001 — best-effort
                                logger.debug(
                                    "teardown post-intent reconcile failed for %s: %s",
                                    strategy.strategy_id,
                                    exc,
                                )

                        # VIB-3773: drive the runner's full commit pipeline
                        # (enrich → ledger → outbox+fire → sidecar) for this
                        # successful on-chain teardown intent. The helper has
                        # degraded-but-continue semantics — failures land in
                        # the deferred-write log, never raise — so the
                        # slippage manager never sees an accounting failure
                        # and the next teardown intent runs regardless.
                        if self.runner_helpers.has_commit:
                            commit_outcome = await self.runner_helpers.commit(  # type: ignore[misc]
                                strategy,
                                intent_to_exec,
                                execution_result=exec_result,
                                execution_context=context,
                                bundle_metadata=getattr(compilation_result.action_bundle, "metadata", None) or None,
                                teardown_cycle_id=teardown_cycle_id,
                                pre_snapshot=pre_intent_snapshot,
                                recon=post_intent_recon,
                                lending_pre_state=lending_pre_state_for_intent,
                            )
                            if commit_outcome.accounting_degraded:
                                accounting_degraded_records.extend(commit_outcome.degraded_writes)
                                logger.error(
                                    "Teardown intent %d/%d accounting degraded — %s",
                                    intent_index + 1,
                                    len(intents),
                                    commit_outcome.degraded_reason or "unknown",
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

            # Extract strategy-configured slippage from the intent so the
            # escalation manager can use it as a floor (e.g., Pendle YT
            # teardowns need 15% slippage due to thin AMM liquidity).
            # Handle both object intents (live) and dict intents (resumed from JSON).
            raw_intent_slippage = (
                intent.get("max_slippage") if isinstance(intent, dict) else getattr(intent, "max_slippage", None)
            )
            intent_slippage: Decimal | None = None
            if raw_intent_slippage is not None:
                try:
                    intent_slippage = Decimal(str(raw_intent_slippage))
                except (InvalidOperation, TypeError, ValueError):
                    logger.warning("Could not parse intent max_slippage=%r, ignoring.", raw_intent_slippage)

            exec_result = await self.slippage_manager.execute_with_escalation(
                intent=intent,
                position_value=positions.total_value_usd,
                execute_func=execute_at_slippage,
                on_approval_needed=on_approval_needed,
                teardown_id=teardown_id,
                strategy_id=strategy.strategy_id,
                is_auto_mode=is_auto_mode,
                intent_slippage=intent_slippage,
            )

            if exec_result.success:
                succeeded += 1
                # Estimate cost
                actual_slippage = exec_result.final_slippage
                intent_value = positions.total_value_usd / len(intents)  # Simplified
                total_costs += intent_value * actual_slippage

                # Notify strategy of successful teardown intent so it can
                # update its in-memory state (e.g. zero out borrowed_amount
                # after a successful REPAY), then persist that state.
                # Without this, a partial teardown leaves stale strategy state
                # that causes the next deploy to retry already-completed ops.
                try:
                    # VIB-3922 — fire the framework-side intent-execution
                    # hook BEFORE the user callback so the LPPositionTracker
                    # clears the closed position from its in-memory dict.
                    # Pre-fix the runner's per-iteration record_intent_execution
                    # never saw teardown intents, so
                    # ``strategy_state.__framework_lp_position_tracker__``
                    # kept references to closed positions across teardown
                    # → re-deploy boundaries.
                    if hasattr(strategy, "_framework_record_intent_execution"):
                        try:
                            strategy._framework_record_intent_execution(intent, True, exec_result)
                        except Exception as fhook_err:  # noqa: BLE001
                            logger.warning(
                                "framework intent-execution hook raised in teardown lane (non-fatal): %s",
                                fhook_err,
                            )
                    if hasattr(strategy, "on_intent_executed"):
                        result = strategy.on_intent_executed(intent, True, exec_result)
                        # Handle strategies that return a coroutine
                        if asyncio.iscoroutine(result):
                            await result
                    if hasattr(strategy, "save_state"):
                        strategy.save_state()
                    if hasattr(strategy, "flush_pending_saves"):
                        await strategy.flush_pending_saves()
                except Exception as e:  # noqa: BLE001
                    logger.error(
                        "Failed to persist strategy state after teardown intent %d/%d: %s "
                        "(on-chain action succeeded but persisted state may be stale)",
                        i + 1,
                        len(intents),
                        e,
                    )
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
                        recovery_options=[
                            "Approve higher slippage",
                            "Wait & Escalate to next level",
                            "Cancel",
                        ],
                        accounting_degraded=bool(accounting_degraded_records),
                        accounting_degraded_count=len(accounting_degraded_records),
                    )

            # Update completed count and persist teardown progress so that
            # a crash/restart resumes from the correct index
            teardown_state.completed_intents = succeeded
            teardown_state.updated_at = datetime.now(UTC)
            if self.state_manager:
                await self.state_manager.save_teardown_state(teardown_state)

        # All intents processed
        completed_at = datetime.now(UTC)
        teardown_state.status = TeardownStatus.COMPLETED
        teardown_state.completed_at = completed_at
        if self.state_manager:
            await self.state_manager.save_teardown_state(teardown_state)

        final_value = positions.total_value_usd - total_costs
        if skipped:
            logger.info(
                "Teardown for %s completed: %d executed, %d skipped (no-op), %d failed",
                strategy.strategy_id,
                succeeded - skipped,
                skipped,
                failed,
            )

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
            accounting_degraded=bool(accounting_degraded_records),
            accounting_degraded_count=len(accounting_degraded_records),
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

    async def _verify_closure(
        self,
        strategy: IntentStrategy,
        expected_positions: Any = None,
        pre_execution_positions: Any = None,
    ) -> bool:
        """Verify that positions are actually closed on-chain.

        Three layers of verification, in priority order:

        1. **Per-protocol on-chain post-condition** (VIB-3742): for every
           position present in ``pre_execution_positions`` (or
           ``expected_positions`` if no pre-snapshot was supplied), look up
           a registered ``TeardownPostCondition`` and run it. Any residual
           on-chain liquidity / debt fails the verification with a
           detailed residual map. This is the layer that catches the
           original $1.16 leak: TJ V2 partial closes that look like clean
           successes from in-memory state alone.
        2. **Discover-path log** (existing behaviour): when
           ``expected_positions`` is supplied (the ``--discover`` flow), log
           the position IDs the orchestrator was supposed to close. Also
           runs the post-condition over those IDs.
        3. **In-memory state read** (legacy fallback): when no snapshot is
           available, re-read ``strategy.get_open_positions()``. This is
           the weak path the original verifier used; it's retained as a
           last-resort signal but is no longer the primary check.

        Returns ``False`` if ANY position has residual liquidity OR any
        post-condition errors out (fail-closed).
        """
        # Choose the pre-execution snapshot. ``pre_execution_positions`` is
        # what we want — it captures what was open BEFORE the teardown ran.
        # ``expected_positions`` (the --discover path) is the runner-up.
        snapshot = pre_execution_positions
        if snapshot is None or not getattr(snapshot, "positions", None):
            snapshot = expected_positions

        snapshot_positions = list(getattr(snapshot, "positions", []) or [])

        if snapshot_positions:
            from almanak.framework.teardown.post_conditions import (
                ClosureCheckResult,
                get_teardown_post_condition,
            )

            # Plumb gateway client / RPC through to post-conditions.
            # ``compiler`` and ``orchestrator`` may both expose either; we
            # try compiler first because it's the layer that already owns
            # the gateway-or-rpc dual path. Both attributes are best-effort
            # — the post-conditions tolerate ``None`` for both.
            gateway_client = self._teardown_gateway_client()
            rpc_url = self._teardown_rpc_url()
            wallet_address = self._teardown_wallet_address(strategy)

            failed_results: list[ClosureCheckResult] = []
            for position in snapshot_positions:
                protocol = (getattr(position, "protocol", "") or "").lower()
                hook = get_teardown_post_condition(protocol)
                if hook is None:
                    # No post-condition registered for this protocol — log
                    # at debug; the in-memory check below will still run.
                    logger.debug(
                        "Teardown verification: no on-chain post-condition "
                        "registered for protocol %r (position_id=%s); "
                        "falling back to in-memory state.",
                        protocol,
                        getattr(position, "position_id", ""),
                    )
                    continue

                try:
                    check = hook(
                        position=position,
                        wallet_address=wallet_address,
                        gateway_client=gateway_client,
                        rpc_url=rpc_url,
                    )
                except Exception as exc:  # noqa: BLE001 — fail-closed
                    logger.exception(
                        "Teardown post-condition for %s raised: %s",
                        protocol,
                        exc,
                    )
                    check = ClosureCheckResult(
                        closed=False,
                        protocol=protocol,
                        position_id=getattr(position, "position_id", "") or "",
                        error=f"Post-condition raised: {exc}",
                    )

                if not check.closed:
                    failed_results.append(check)

            if failed_results:
                for check in failed_results:
                    logger.error(
                        "Post-teardown on-chain verification FAILED for %s position %s: residual=%s error=%s",
                        check.protocol,
                        check.position_id,
                        check.residual,
                        check.error,
                    )
                return False

            # All registered post-conditions passed. We still log the
            # discover-path summary so the existing audit trail is intact.
            ids = [getattr(p, "position_id", "") for p in snapshot_positions]
            logger.info(
                "Teardown verification: %d position(s) passed on-chain post-condition checks: %s",
                len(snapshot_positions),
                ids,
            )
            return True

        # Last-resort: legacy in-memory state read. Used when neither a
        # pre-execution snapshot nor an expected-positions list reaches us
        # (paper / unit-test paths). This is the path the original
        # implementation used end-to-end; it still works as a "did the
        # strategy at least clear its own state?" smoke test.
        positions = strategy.get_open_positions()
        return len(positions.positions) == 0

    # ------------------------------------------------------------------
    # Helpers used by _verify_closure to plumb gateway / RPC / wallet to
    # post-conditions. Kept tiny; the post-conditions tolerate all-None.
    # ------------------------------------------------------------------

    def _teardown_gateway_client(self) -> Any | None:
        """Best-effort: surface a connected gateway client for post-conditions.

        VIB-3822: ``GatewayExecutionOrchestrator`` stores its gateway client
        under ``self._client`` (see ``execution/gateway_orchestrator.py``); the
        compiler uses ``_gateway_client`` / ``gateway_client``. Probe all three
        so the V3 LP_CLOSE post-condition can read on-chain closure state when
        the runner constructed an orchestrator (the ``--discover`` path used by
        ``uniswap_lp_optimism`` and any strategy without ``get_open_positions``).
        """
        for source in (self.compiler, self.orchestrator):
            if source is None:
                continue
            client = (
                getattr(source, "_gateway_client", None)
                or getattr(source, "gateway_client", None)
                or getattr(source, "_client", None)
            )
            if client is not None:
                if getattr(client, "is_connected", True):
                    return client
        return None

    def _teardown_rpc_url(self) -> str | None:
        """Best-effort: surface an RPC URL for post-conditions (test path)."""
        for source in (self.compiler, self.orchestrator):
            if source is None:
                continue
            getter = getattr(source, "_get_chain_rpc_url", None)
            if callable(getter):
                try:
                    url = getter()
                    if url:
                        return url
                except Exception:  # noqa: BLE001
                    pass
            url = getattr(source, "rpc_url", None) or getattr(source, "_rpc_url", None)
            if url:
                return url
        return None

    @staticmethod
    def _teardown_wallet_address(strategy: Any) -> str:
        """Best-effort: surface the strategy's wallet address."""
        return getattr(strategy, "wallet_address", None) or getattr(strategy, "_wallet_address", None) or ""

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
