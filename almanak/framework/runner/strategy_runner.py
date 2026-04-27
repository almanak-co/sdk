"""Strategy Runner for executing trading strategies in a loop.

This module implements the StrategyRunner class which orchestrates the
execution of trading strategies by:
1. Wiring up dependencies (PriceOracle, BalanceProvider, Orchestrator, etc.)
2. Running single iterations of strategy logic
3. Managing continuous execution loops with graceful shutdown

The runner is the main entry point for running strategies in production,
handling the lifecycle from market data fetching through execution.

Example:
    from almanak.framework.runner import StrategyRunner
    from almanak.framework.strategies import MomentumStrategy

    runner = StrategyRunner(
        price_oracle=price_oracle,
        balance_provider=balance_provider,
        execution_orchestrator=orchestrator,
        state_manager=state_manager,
        alert_manager=alert_manager,
    )

    # Run a single iteration
    result = await runner.run_iteration(strategy)

    # Or run continuously
    await runner.run_loop(strategy, interval_seconds=60)
"""

import asyncio
import logging
import os
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import grpc

if TYPE_CHECKING:
    from ..services.emergency_manager import EmergencyManager
    from ..services.operator_card_generator import OperatorCardGenerator
    from ..services.stuck_detector import StuckDetector
    from ..teardown import TeardownMode
    from ..vault.lifecycle import VaultLifecycleManager

from ..alerting.alert_manager import AlertManager
from ..api.timeline import TimelineEvent, TimelineEventType, add_event
from ..data.interfaces import BalanceProvider, PriceOracle
from ..execution.circuit_breaker import CircuitBreaker
from ..execution.enso_state_provider import EnsoStateProvider
from ..execution.extract_result import CriticalAccountingError
from ..execution.interfaces import TransactionReceipt as FullTransactionReceipt
from ..execution.multichain import (
    MultiChainOrchestrator,
)
from ..execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
    TransactionResult,
)
from ..execution.plan_builder import (
    get_intent_destination_chain,
    get_intent_destination_token,
    is_cross_chain_intent,
)
from ..execution.result_enricher import ResultEnricher
from ..execution.revert_diagnostics import diagnose_revert
from ..execution.session_store import ExecutionSessionStore
from ..intents.compiler import IntentCompiler, IntentCompilerConfig
from ..intents.state_machine import (
    IntentStateMachine,
    RetryConfig,
    SadflowAction,
    SadflowContext,
    StateMachineConfig,
    TransactionReceipt,
)
from ..intents.vocabulary import AnyIntent, HoldIntent, Intent, IntentSequence, IntentType
from ..models.actions import AvailableAction, SuggestedAction
from ..models.operator_card import EventType, OperatorCard, PositionSummary, Severity
from ..models.stuck_reason import StuckReason
from ..state.exceptions import AccountingPersistenceError
from ..state.state_manager import StateManager
from ..utils.log_formatters import (
    _emojis_enabled,
)
from ..utils.logging import add_context, clear_context
from ..valuation.portfolio_valuer import PortfolioValuer
from . import _run_loop_helpers

# ---- Re-exports from runner_models (keeps all existing import paths working) ----
from .runner_models import (  # noqa: F401
    CriticalCallbackError,
    ExecutionProgress,
    IterationResult,
    IterationStatus,
    RunnerConfig,
    StatefulActivityProviderProtocol,
    StrategyProtocol,
    _extract_tokens_from_intent,
    _format_intent_for_log,
)

logger = logging.getLogger(__name__)


# Transient gRPC status codes that are worth retrying during the
# ``GetTransactionStatus`` poll loop in ``_bridge_wait_verify_source_tx``.
# Permanent codes (UNAUTHENTICATED, PERMISSION_DENIED, INVALID_ARGUMENT,
# UNIMPLEMENTED, ...) indicate a config or auth defect that will not
# resolve with more attempts, so they must propagate immediately rather
# than silently consume the full 60-second retry budget. See PR #1676.
_TRANSIENT_GRPC_CODES: frozenset[grpc.StatusCode] = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNKNOWN,
    }
)


# =============================================================================
# Mode derivation (VIB-3157)
# =============================================================================


class ExecutionMode(StrEnum):
    """Tri-state execution mode for accounting stamping.

    Single source of truth for the runner-mode label written onto ledger
    entries, portfolio snapshots, and portfolio metrics. Using an enum
    (instead of bare strings) catches typos and makes downstream
    comparisons typo-safe — a misspelled ``"liev"`` would silently store
    a bad row otherwise.
    """

    DRY_RUN = "dry_run"
    PAPER = "paper"
    LIVE = "live"


def derive_execution_mode_from_config(config: Any) -> ExecutionMode:
    """Return the canonical execution-mode label for a runner config.

    The accounting layer needs a single, authoritative mapping from runner
    state to the tri-state label stamped on ledger entries, portfolio
    snapshots, and portfolio metrics. Keeping the branch logic here means
    :meth:`StrategyRunner._is_live_mode`, ``_write_ledger_entry`` and
    ``runner_state._build_metrics_for_snapshot`` cannot drift apart the
    next time a new mode is introduced.

    Args:
        config: A ``RunnerConfig`` (or subclass) object.

    Returns:
        ``ExecutionMode.DRY_RUN`` when ``config.dry_run`` is set,
        ``ExecutionMode.PAPER`` when ``config.paper_mode`` is truthy,
        otherwise ``ExecutionMode.LIVE``. The returned value is a
        ``StrEnum`` so it serialises as the bare label (``"dry_run"`` etc.)
        for ledger / snapshot persistence.
    """
    if getattr(config, "dry_run", False):
        return ExecutionMode.DRY_RUN
    if getattr(config, "paper_mode", False):
        return ExecutionMode.PAPER
    return ExecutionMode.LIVE


# =============================================================================
# Per-iteration mutable state (Phase 3b refactor)
# =============================================================================


@dataclass
class RunIterationState:
    """Mutable bag of per-iteration values threaded through step helpers.

    ``StrategyRunner.run_iteration`` was previously a single ~600 line method
    with CC=107. Phase 3b splits it into small step helpers on the runner
    that each receive this state object, mutate it, and return either
    ``None`` (continue to the next step) or an ``IterationResult`` early-exit.

    This mirrors the pipeline-state pattern introduced in Phase 3a for
    ``ExecutionOrchestrator.execute``. The dataclass is internal to the
    runner — it is **not** part of the public API.
    """

    strategy: "StrategyProtocol"
    strategy_id: str
    start_time: datetime
    market: Any | None = None
    decide_result: Any | None = None
    intents: list["AnyIntent"] = field(default_factory=list)
    teardown_mode: "TeardownMode | None" = None
    pre_balances: dict[str, Decimal] = field(default_factory=dict)
    intent_tokens: list[str] = field(default_factory=list)


@dataclass
class SingleChainExecutionState:
    """Mutable bag threaded through ``_execute_single_chain``'s step helpers.

    Phase 3c splits ``_execute_single_chain`` (CC=118, 751 lines) into a thin
    driver plus per-phase step helpers. Those helpers receive this state
    object, mutate it, and return either ``None`` (continue) or an
    ``IterationResult`` early-exit. The dataclass is internal to the runner
    and is **not** part of the public API.

    Lifecycle:
      - ``_init_single_chain_state`` populates the setup fields
        (compiler, state machine, clob client, bundle metadata, pre-snapshot).
      - ``_single_chain_state_machine_loop`` drives the state machine and
        records the last execution result/context and last bundle metadata.
      - ``_single_chain_handle_success`` / ``_single_chain_handle_failure``
        read the accumulated state to build the final ``IterationResult``.
    """

    # --- Inputs ---
    strategy: "StrategyProtocol"
    intent: "AnyIntent"
    start_time: datetime
    total_intents: int = 1
    market: Any | None = None
    record_metrics: bool = True

    # --- Derived runtime handles (populated by init) ---
    # Fields populated unconditionally by ``_init_single_chain_state`` are
    # typed as ``Any`` (not ``Any | None``) so mypy does not complain about
    # ``union-attr`` at read sites after init has run. The runtime default is
    # still ``None`` -- the contract is "readers only touch these after init".
    strategy_id: str = ""
    gateway_client: Any = None
    rpc_url: str | None = None
    price_oracle: dict | None = None
    polymarket_config: Any = None
    clob_handler: Any = None
    clob_client: Any = None
    compiler: Any = None
    state_machine: Any = None
    pre_snapshot: Any | None = None
    # --- Running bookkeeping (updated by state-machine loop) ---
    last_execution_result: Any | None = None
    last_execution_context: Any | None = None
    last_bundle_metadata: dict[str, Any] | None = None


@dataclass
class BridgeWaitState:
    """Mutable bag threaded through ``_execute_with_bridge_waiting``'s helpers.

    Phase 3c splits the cross-chain bridge-waiting path (CC=79, 534 lines) into
    a per-intent loop driver plus step helpers for source-TX verification,
    bridge polling, and finalization. Each helper mutates this state and
    either returns ``None`` to continue or records a failure that the loop
    picks up via the ``failed_step`` sentinel.
    """

    # --- Inputs ---
    strategy: "StrategyProtocol"
    intents: list["AnyIntent"]
    orchestrator: "MultiChainOrchestrator"
    start_time: datetime
    resume_progress: "ExecutionProgress | None" = None
    price_map: dict[str, str] | None = None
    price_oracle: dict | None = None

    # --- Derived (populated by init) ---
    # Fields populated unconditionally by ``_init_bridge_wait_state`` use
    # ``Any`` (not ``Any | None``) so mypy does not warn about ``union-attr``
    # at read sites. The contract is "readers only touch these after init".
    strategy_id: str = ""
    first_intent: "AnyIntent | None" = None
    wallet_address: str = ""
    rpc_urls: dict[str, str] = field(default_factory=dict)
    gateway_client: Any = None
    state_provider: Any = None
    start_step_index: int = 0
    previous_amount_received: Decimal | None = None
    progress: "ExecutionProgress | None" = None

    # --- Running bookkeeping (updated while iterating intents) ---
    successful_count: int = 0
    failed_step: str | None = None
    error_message: str | None = None
    failed_result: Any | None = None
    callback_fired: bool = False
    # Tracks the intent currently being processed so the finalization block
    # can fire ``on_intent_executed`` for break-exit paths that did not fire
    # the callback inline.
    current_intent: "AnyIntent | None" = None


# =============================================================================
# Strategy Runner
# =============================================================================


class StrategyRunner:
    """Orchestrates strategy execution with full dependency injection.

    The StrategyRunner is the main entry point for running trading strategies.
    It handles:
    - Creating market snapshots with injected data providers
    - Calling strategy.decide() with market data
    - Compiling intents to ActionBundles
    - Executing through the ExecutionOrchestrator
    - Persisting state via StateManager
    - Alerting on errors via AlertManager
    - Graceful shutdown handling

    Attributes:
        price_oracle: Provider for price data
        balance_provider: Provider for balance data
        execution_orchestrator: Handles transaction execution
        state_manager: Manages strategy state persistence
        alert_manager: Sends alerts on errors
        config: Runner configuration
    """

    def __init__(
        self,
        price_oracle: PriceOracle,
        balance_provider: BalanceProvider,
        execution_orchestrator: ExecutionOrchestrator | MultiChainOrchestrator,
        state_manager: StateManager,
        alert_manager: AlertManager | None = None,
        config: RunnerConfig | None = None,
        session_store: ExecutionSessionStore | None = None,
        vault_lifecycle: "VaultLifecycleManager | None" = None,
        circuit_breaker: CircuitBreaker | None = None,
        stuck_detector: "StuckDetector | None" = None,
        operator_card_generator: "OperatorCardGenerator | None" = None,
        emergency_manager: "EmergencyManager | None" = None,
    ) -> None:
        """Initialize the StrategyRunner.

        Args:
            price_oracle: Provider for aggregated price data
            balance_provider: Provider for on-chain balances
            execution_orchestrator: Handles transaction execution pipeline.
                Can be ExecutionOrchestrator (single-chain) or
                MultiChainOrchestrator (multi-chain).
            state_manager: Manages state persistence across tiers
            alert_manager: Optional alert manager for error notifications
            config: Optional runner configuration
            session_store: Optional ExecutionSessionStore for crash recovery
            vault_lifecycle: Optional VaultLifecycleManager for vault-wrapped strategies
            circuit_breaker: Optional circuit breaker for fail-closed execution safety.
                When provided, execution is blocked after consecutive failures or
                cumulative loss thresholds are exceeded.
            stuck_detector: Optional StuckDetector for intelligent failure classification.
                When provided, consecutive error alerts include root-cause analysis.
            operator_card_generator: Optional OperatorCardGenerator for rich actionable cards.
                When provided, alerts include auto-detected severity, suggested actions,
                and auto-remediation where applicable.
            emergency_manager: Optional EmergencyManager for auto-triggering emergency stops.
                When provided, the runner automatically triggers emergency_stop when the
                circuit breaker trips to OPEN, pausing the strategy and sending CRITICAL alerts.
        """
        self.price_oracle = price_oracle
        self.balance_provider = balance_provider
        self.execution_orchestrator = execution_orchestrator
        self.state_manager = state_manager
        self.alert_manager = alert_manager
        self.config = config or RunnerConfig()
        self._session_store = session_store
        self._vault_lifecycle = vault_lifecycle
        self._circuit_breaker = circuit_breaker
        self._stuck_detector = stuck_detector
        self._operator_card_generator = operator_card_generator
        self._emergency_manager = emergency_manager
        self._emergency_triggered_for_open = False  # Track once-per-OPEN-episode firing
        self._decide_in_progress = False  # Guard against overlapping decide() calls after timeout
        self._decide_timed_out_at: float | None = None  # Monotonic timestamp of last timeout

        # Detect if we're in multi-chain mode
        self._is_multi_chain = isinstance(execution_orchestrator, MultiChainOrchestrator)

        # Shutdown control
        self._shutdown_requested = False
        self._signal_received = False
        self._terminal_lifecycle_state: str | None = None
        self._terminal_lifecycle_error_message: str | None = None
        self._current_loop_task: asyncio.Task[None] | None = None

        # Metrics tracking
        self._consecutive_errors = 0
        self._first_error_at: datetime | None = None  # Timestamp of first error in current streak
        self._total_iterations = 0
        self._successful_iterations = 0

        # Track recovered session tx_hashes to prevent duplicates
        self._recovered_tx_hashes: set[str] = set()
        self._recovered_nonces: dict[str, set[int]] = {}  # strategy_id -> set of nonces

        # Portfolio snapshot tracking
        self._last_snapshot_time: datetime | None = None
        self._snapshot_interval_seconds = 300  # Capture time-series snapshot every 5 min
        self._portfolio_valuer = PortfolioValuer()
        self._iteration_had_trade = False  # Set by _write_ledger_entry on success

        # Optional explicit gateway client (set via set_gateway_client for multi-chain)
        self._gateway_client: Any | None = None
        # Track pause log state to avoid repetitive per-iteration info spam.
        self._logged_paused_strategy_ids: set[str] = set()

        # VIB-3418: FIFO basis store for lending interest attribution.
        # Lives for the runner's lifetime so BORROW lots are available when REPAY arrives.
        # Reconstructable from accounting_events if the runner restarts.
        from ..accounting.basis import FIFOBasisStore

        self._lending_basis_store = FIFOBasisStore()

        # VIB-3467: AccountingProcessor — drains accounting_outbox after each execution.
        # Initialised with an empty deployment_id; updated in run_loop once strategy_id is known.
        from ..accounting.processor import AccountingProcessor

        self._accounting_processor = AccountingProcessor(
            state_manager=self.state_manager,
            basis_store=self._lending_basis_store,
        )
        # Strong-ref set for drain tasks so they cannot be GC'd before completion.
        self._pending_drain_tasks: set[asyncio.Task] = set()

        mode = "multi-chain" if self._is_multi_chain else "single-chain"
        logger.info(
            f"StrategyRunner initialized ({mode} mode) with config: "
            f"interval={self.config.default_interval_seconds}s, "
            f"dry_run={self.config.dry_run}, "
            f"session_store={'enabled' if session_store else 'disabled'}"
        )

    def _query_portfolio_value(self, strategy: Any) -> tuple[Decimal, Decimal]:
        """Query actual portfolio value from the strategy, with graceful fallback.

        Attempts to call strategy.get_portfolio_snapshot() to get real exposure data.
        Falls back to (Decimal("0"), Decimal("0")) if the query fails for any reason.

        Args:
            strategy: The strategy instance to query

        Returns:
            Tuple of (total_value_usd, available_balance_usd)
        """

        def _safe_decimal(value: Any) -> Decimal:
            if isinstance(value, Decimal):
                return value
            if value is None:
                return Decimal("0")
            try:
                return Decimal(str(value))
            except Exception:  # noqa: BLE001
                return Decimal("0")

        try:
            if hasattr(strategy, "get_portfolio_snapshot"):
                snapshot = strategy.get_portfolio_snapshot()
                return (
                    _safe_decimal(getattr(snapshot, "total_value_usd", None)),
                    _safe_decimal(getattr(snapshot, "available_cash_usd", None)),
                )
        except Exception as e:  # noqa: BLE001
            logger.debug(f"Could not query portfolio value for OperatorCard: {e}")
        return (Decimal("0"), Decimal("0"))

    def _get_gateway_client(self) -> Any | None:
        from .runner_gateway import get_gateway_client

        return get_gateway_client(self)

    def _register_with_gateway(self, strategy: StrategyProtocol) -> None:
        from .runner_gateway import register_with_gateway

        register_with_gateway(self, strategy)

    def _deregister_from_gateway(self, strategy_id: str) -> None:
        from .runner_gateway import deregister_from_gateway

        deregister_from_gateway(self, strategy_id)

    def _gateway_update_status(self, strategy_id: str, status: str) -> None:
        from .runner_gateway import gateway_update_status

        gateway_update_status(self, strategy_id, status)

    def _gateway_heartbeat(self, strategy_id: str, positions: list | None = None) -> None:
        from .runner_gateway import gateway_heartbeat

        gateway_heartbeat(self, strategy_id, positions)

    def _collect_position_snapshot(self, strategy: "StrategyProtocol") -> list | None:
        from .runner_gateway import collect_position_snapshot

        return collect_position_snapshot(self, strategy)

    def _lifecycle_write_state(self, agent_id: str, state: str, error_message: str | None = None) -> None:
        from .runner_gateway import lifecycle_write_state

        lifecycle_write_state(self, agent_id, state, error_message)

    def _lifecycle_heartbeat(self, agent_id: str) -> None:
        from .runner_gateway import lifecycle_heartbeat

        lifecycle_heartbeat(self, agent_id)

    def _lifecycle_poll_command(self, agent_id: str) -> str | None:
        from .runner_gateway import lifecycle_poll_command

        return lifecycle_poll_command(self, agent_id)

    def _lifecycle_handle_stop(self, strategy_id: str, strategy: Any) -> None:
        from .runner_gateway import lifecycle_handle_stop

        lifecycle_handle_stop(self, strategy_id, strategy)

    def set_gateway_client(self, client: Any) -> None:
        from .runner_gateway import set_gateway_client

        set_gateway_client(self, client)

    def setup_gateway_integration(self, strategy: StrategyProtocol) -> None:
        from .runner_gateway import setup_gateway_integration

        setup_gateway_integration(self, strategy)

    def teardown_gateway_integration(self, strategy_id: str) -> None:
        from .runner_gateway import teardown_gateway_integration

        teardown_gateway_integration(self, strategy_id)

    async def run_iteration(self, strategy: StrategyProtocol) -> IterationResult:
        """Run a single iteration of the strategy.

        This method:
        1. Creates a market snapshot with current prices and balances
        2. Calls strategy.decide(market) to get an intent
        3. If not a HOLD intent, compiles to ActionBundle
        4. Executes through the orchestrator (unless dry_run)
        5. Updates state and metrics

        The body is a small driver that threads :class:`RunIterationState`
        through a sequence of step helpers (``_step_*`` methods). Each step
        returns either ``None`` (continue) or an :class:`IterationResult`
        that terminates the iteration early (pause gate, circuit breaker,
        teardown, decide failure, etc.). Phase 3b refactor preserves every
        log line, timeline event, and state-manager write ordering.

        Args:
            strategy: The strategy to execute

        Returns:
            IterationResult with status and any execution results
        """
        start_time = datetime.now(UTC)
        strategy_id = strategy.strategy_id

        # Bind correlation ID for all log messages during this iteration
        iteration_id = f"{strategy_id}_{self._total_iterations + 1}_{int(start_time.timestamp())}"
        add_context(correlation_id=iteration_id, strategy_id=strategy_id)

        # Generate cycle_id for forensic event correlation across phases
        from almanak.framework.observability.context import clear_cycle_id, new_cycle_id

        cycle_id = new_cycle_id()
        self._last_cycle_id = cycle_id  # Phase 4: preserve for snapshot capture after iteration
        add_context(cycle_id=cycle_id)

        logger.info(f"Starting iteration for strategy: {strategy_id}")

        state = RunIterationState(
            strategy=strategy,
            strategy_id=strategy_id,
            start_time=start_time,
        )

        try:
            # Step 0: Honor operator pause before any strategy logic/execution.
            early = await self._step_pause_gate(state)
            if early is not None:
                return early

            # Step 0a/0c/0b/0.5: teardown detection, multi-chain stuck
            # execution resume (pre-CB, #1665), circuit-breaker pre-gate,
            # and teardown routing.
            early = await self._step_teardown_and_cb_gate(state)
            if early is not None:
                return early

            # Periodic hooks that run every iteration but never early-exit.
            await self._step_periodic_hooks(state)

            # Step 1: Build market snapshot (+ dry-run balance injection +
            # price cache pre-warm).
            early = await self._step_build_snapshot(state)
            if early is not None:
                return early

            # Step 2: Call strategy.decide() with timeout + overlap guard.
            early = await self._step_decide(state)
            if early is not None:
                return early

            # Step 3+4: Extract intents and short-circuit on HOLD/no-action.
            early = self._step_extract_intents(state)
            if early is not None:
                return early

            # Step 5 + 5.5: Log intents and run the late circuit-breaker gate
            # now that a real intent exists.
            self._step_log_intents(state)
            early = self._step_circuit_breaker_pre_execute(state)
            if early is not None:
                return early

            # Step 5.9: Snapshot pre-execution balances for delta logging.
            await self._step_snapshot_pre_balances(state)

            # Step 6: Execute based on orchestrator type.
            return await self._step_execute(state)

        except Exception as e:
            # VIB-3157: accounting persistence failure -- on-chain execution may
            # have succeeded but the durable record is missing. Halt the
            # iteration with ACCOUNTING_FAILED so run_loop's consecutive-error
            # handler kicks in, and alert the operator before books drift.
            from ..state.exceptions import AccountingPersistenceError

            if isinstance(e, AccountingPersistenceError):
                logger.exception(
                    "Accounting persistence failed in live mode for %s (write_kind=%s)",
                    strategy_id,
                    e.write_kind,
                )
                await self._alert_accounting_failure(strategy, e)
                return self._create_error_result(
                    strategy_id,
                    IterationStatus.ACCOUNTING_FAILED,
                    f"Accounting persistence failed ({e.write_kind}): {e}",
                    start_time,
                )
            # VIB-3180: receipt parse failure in the enrichment layer. The
            # on-chain transaction succeeded but we cannot reliably report what
            # happened — ghost-position territory. Treat exactly like an
            # AccountingPersistenceError: ACCOUNTING_FAILED result so
            # run_loop's consecutive-error handler kicks in and the operator
            # is alerted before the strategy continues trading on stale state.
            if isinstance(e, CriticalAccountingError):
                logger.exception(
                    "Receipt enrichment failed in live mode for %s (field=%s, intent=%s, protocol=%s)",
                    strategy_id,
                    e.field_name,
                    e.intent_type,
                    e.protocol,
                )
                await self._alert_enrichment_failure(strategy, e)
                return self._create_error_result(
                    strategy_id,
                    IterationStatus.ACCOUNTING_FAILED,
                    f"Receipt enrichment failed (field={e.field_name}, intent={e.intent_type}): {e}",
                    start_time,
                )
            logger.exception(f"Unexpected error in iteration for {strategy_id}: {e}")
            return self._create_error_result(
                strategy_id,
                IterationStatus.STRATEGY_ERROR,
                f"Unexpected error: {e}",
                start_time,
            )
        finally:
            # Clear correlation context to prevent bleed across iterations
            clear_context()
            clear_cycle_id()

    # -------------------------------------------------------------------------
    # run_iteration step helpers (Phase 3b refactor)
    #
    # Each helper takes the ``RunIterationState`` for the current iteration,
    # mutates it in place, and returns either ``None`` (continue to the next
    # step) or an :class:`IterationResult` to terminate the iteration early.
    # Helpers are intentionally conservative: the original code paths, log
    # messages, and timeline events are preserved verbatim.
    # -------------------------------------------------------------------------

    async def _step_pause_gate(self, state: RunIterationState) -> IterationResult | None:
        """Honor operator pause before any strategy logic/execution runs."""
        strategy_id = state.strategy_id
        paused, pause_reason = await self._is_strategy_paused(strategy_id)
        if paused:
            if strategy_id not in self._logged_paused_strategy_ids:
                logger.info(
                    "%s %s is paused by operator%s",
                    "[PAUSED]" if not _emojis_enabled() else "⏸️",
                    strategy_id,
                    f" ({pause_reason})" if pause_reason else "",
                )
                self._logged_paused_strategy_ids.add(strategy_id)
            self._record_success()
            return IterationResult(
                status=IterationStatus.HOLD,
                intent=HoldIntent(reason=pause_reason or "Paused by operator"),
                strategy_id=strategy_id,
                duration_ms=self._calculate_duration_ms(state.start_time),
            )

        # Strategy resumed: clear pause log marker.
        self._logged_paused_strategy_ids.discard(strategy_id)
        return None

    async def _step_teardown_and_cb_gate(self, state: RunIterationState) -> IterationResult | None:
        """Teardown detection, stuck-execution recovery, and early CB gate.

        Covers the original Step 0a (teardown detection), Step 0c (stuck
        execution resumption for multi-chain, #1665: runs BEFORE the CB
        gate so an open/paused breaker cannot strand saved mid-sequence
        progress), Step 0b (circuit breaker early check, skipped during
        teardown or when resume fired), and Step 0.5 (teardown dispatch).

        Ordering rationale (issue #1665): resuming a saved multi-chain
        flow is continuation of already-started work. It must not be
        blocked by a tripped breaker, for the same reason teardowns
        bypass the CB — both are about finishing work that is already
        in flight. The CB gate still applies to NEW work and to the
        single-chain path unchanged.
        """
        strategy = state.strategy
        strategy_id = state.strategy_id
        start_time = state.start_time

        # Step 0a: Check for teardown early — needed to gate circuit breaker
        # Called once here and reused below to avoid double-invocation
        # (acknowledge_teardown_request has side effects).
        teardown_mode = self._check_teardown_requested(strategy)
        state.teardown_mode = teardown_mode

        # Step 0c (pre-CB for multi-chain, #1665): Check for stuck execution
        # that needs resumption BEFORE the circuit-breaker gate. A tripped
        # breaker must not strand partial bridge/cross-chain flows with
        # saved progress -- finishing in-flight work is independent of
        # whether NEW work is allowed. If resume fires, return directly;
        # the CB gate below only applies to NEW work.
        if self._is_multi_chain:
            stuck_result = await self._check_and_resume_stuck_execution(
                strategy=strategy,
                start_time=start_time,
            )
            if stuck_result is not None:
                return stuck_result

        # Step 0b: Circuit breaker check — block execution if breaker is OPEN/PAUSED
        # Skip when a teardown is pending — teardown must always be allowed to run
        # so operators can safely close positions even after consecutive failures.
        if self._circuit_breaker is not None and teardown_mode is None:
            cb_result = self._circuit_breaker.check()
            if not cb_result.can_execute:
                logger.warning(
                    "Circuit breaker blocking execution for %s: %s (state=%s, failures=%d)",
                    strategy_id,
                    cb_result.reason,
                    cb_result.state.value,
                    cb_result.consecutive_failures,
                )
                cb_state_label = cb_result.state.value  # "open" or "paused"
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.STRATEGY_STUCK,
                        description=f"Circuit breaker {cb_state_label}: {cb_result.reason}",
                        strategy_id=strategy_id,
                        details=cb_result.to_dict(),
                    )
                )
                # Issue #1780: count every iteration that produces an
                # IterationResult in the lifetime total. The CB-open
                # short-circuit IS a completed iteration from the runner's
                # perspective -- run_loop still receives the result, still
                # emits a summary, still calls handle_iteration_failure.
                self._record_failure()
                return IterationResult(
                    status=IterationStatus.CIRCUIT_BREAKER_OPEN,
                    error=cb_result.reason,
                    strategy_id=strategy_id,
                    duration_ms=self._calculate_duration_ms(start_time),
                )

        # Step 0.5: Check for teardown request (reuses result from Step 0a)
        # If teardown is requested, intercept the iteration and execute teardown.
        # Single-chain teardowns route through TeardownManager for full safety
        # (loss caps, escalating slippage, cancel window, post-execution verification).
        # Multi-chain teardowns use the inline path until TeardownManager supports it.
        if teardown_mode is not None:
            return await self._execute_teardown(strategy, teardown_mode, start_time)

        return None

    async def _step_periodic_hooks(self, state: RunIterationState) -> None:
        """Copy trading polling + vault settlement hook.

        Never early-exits: errors are logged and iteration continues.
        """
        strategy = state.strategy
        strategy_id = state.strategy_id

        # Step 0b: Poll copy trading wallet activity (if configured)
        activity_provider = getattr(strategy, "_wallet_activity_provider", None)
        if activity_provider is not None:
            try:
                activity_provider.poll_and_process()
                logger.debug("Copy trading: polled wallet activity")
                self._invoke_optional_hook(strategy, "on_copy_activity_polled", activity_provider)
            except Exception as e:
                logger.error(f"Copy trading poll failed (continuing): {e}")

        # Step 0c: Vault settlement lifecycle hook (if configured)
        if self._vault_lifecycle is not None:
            try:
                from ..vault.config import VaultAction
                from ..vault.lifecycle import VAULT_STATE_KEY

                vault_action = self._vault_lifecycle.pre_decide_hook(strategy)
                if vault_action in (VaultAction.SETTLE, VaultAction.RESUME_SETTLE):
                    logger.info("Vault settlement triggered (%s), running settlement cycle", vault_action.value)
                    settlement = await self._vault_lifecycle.run_settlement_cycle(strategy)
                    if settlement.success:
                        try:
                            if hasattr(strategy, "on_vault_settled"):
                                strategy.on_vault_settled(settlement)
                        except Exception as cb_err:
                            logger.warning("on_vault_settled callback failed: %s", cb_err)
                        logger.info(
                            "Vault settlement completed: epoch=%d, total_assets=%d",
                            settlement.epoch_id,
                            settlement.new_total_assets,
                        )
                    else:
                        logger.warning("Vault settlement failed, continuing to decide()")
            except Exception as e:
                logger.error(f"Vault settlement error (continuing): {e}")
            finally:
                # Always persist vault state, even if callback or settlement fails.
                # Re-import here because the Exception branch above may have
                # triggered before VAULT_STATE_KEY was bound in the try scope.
                if self.config.enable_state_persistence:
                    try:
                        from ..vault.lifecycle import VAULT_STATE_KEY

                        vault_state_dict = self._vault_lifecycle.get_vault_state_dict()
                        if vault_state_dict is not None:
                            await self._persist_vault_state(strategy_id, vault_state_dict, VAULT_STATE_KEY)
                    except Exception as persist_err:
                        logger.warning("Failed to persist vault state: %s", persist_err)

    async def _step_build_snapshot(self, state: RunIterationState) -> IterationResult | None:
        """Create market snapshot, inject dry-run balances, pre-warm prices."""
        strategy = state.strategy
        strategy_id = state.strategy_id

        # Step 1: Create market snapshot
        try:
            market = strategy.create_market_snapshot()
            logger.debug(f"Created market snapshot for {strategy_id}")
        except Exception as e:
            logger.error(f"Failed to create market snapshot: {e}")
            return self._create_error_result(
                strategy_id,
                IterationStatus.DATA_ERROR,
                f"Market snapshot failed: {e}",
                state.start_time,
            )

        state.market = market

        # Step 1a: Inject simulated balances for dry-run mode (VIB-2329)
        # When running --dry-run --no-gateway, balance providers return 0 or error
        # for chains where the wallet has no positions. simulated_balances in config
        # lets strategy authors test logic without needing real on-chain funds.
        if self.config.dry_run:
            self._inject_simulated_balances(market, strategy)

        # Step 1b: Pre-warm price cache (VIB-2568)
        # On cold Anvil forks, gateway price fetches can take 15-30s each.
        # If decide() makes multiple market.price() calls, the total easily
        # exceeds the 30s decide_timeout. Pre-warming populates the snapshot's
        # _price_cache OUTSIDE the timeout budget so decide() hits cache.
        await self._pre_warm_prices(market, strategy)

        # Step 1c: Reset any critical-data-failure markers left by pre-warming.
        # Pre-warm failures are expected (the snapshot retries inside decide())
        # and should not be counted against the HOLD-escalation check, which is
        # only meaningful for failures that occurred during decide() itself.
        if hasattr(market, "clear_critical_data_failures"):
            market.clear_critical_data_failures()

        return None

    async def _step_decide(self, state: RunIterationState) -> IterationResult | None:
        """Call ``strategy.decide(market)`` with timeout + overlap guard.

        Returns an early-exit ``IterationResult`` on overlap, timeout, or
        raised exception. Otherwise stores the raw decide result on ``state``
        and returns ``None``.
        """
        strategy = state.strategy
        strategy_id = state.strategy_id
        market = state.market
        start_time = state.start_time

        # Step 2: Get strategy decision (with hard timeout)
        # NOTE: asyncio.to_thread runs decide() in a worker thread. If decide()
        # times out, the worker thread continues running (Python limitation).
        # The _decide_in_progress guard prevents overlapping decide() calls.
        decide_timeout = self.config.decide_timeout_seconds
        if self._decide_in_progress:
            # Allow recovery after 2x timeout -- the orphan thread has had plenty of time
            if self._decide_timed_out_at is not None:
                elapsed = time.monotonic() - self._decide_timed_out_at
                if elapsed > 2 * decide_timeout:
                    logger.warning(
                        f"Resetting decide guard after {elapsed:.1f}s (timeout was {decide_timeout}s) for {strategy_id}"
                    )
                    self._decide_in_progress = False
                    self._decide_timed_out_at = None
            if self._decide_in_progress:
                msg = "strategy.decide() still running from previous timed-out call"
                logger.error(f"OVERLAP: {msg} for {strategy_id}")
                if self._circuit_breaker is not None:
                    self._circuit_breaker.record_failure(error_message=msg)
                return self._create_error_result(
                    strategy_id,
                    IterationStatus.STRATEGY_TIMEOUT,
                    msg,
                    start_time,
                )
        try:
            self._decide_in_progress = True
            from almanak.framework.observability.emitter import emit_phase_event
            from almanak.framework.observability.events import StrategyPhase

            emit_phase_event(
                strategy_id=strategy_id,
                phase=StrategyPhase.DECIDE,
                event_type="STATE_CHANGE",
                description="decide() started",
            )
            if decide_timeout <= 0:
                # Timeout disabled -- run decide() without a time limit
                decide_result = await asyncio.to_thread(strategy.decide, market)
            else:
                decide_result = await asyncio.wait_for(
                    asyncio.to_thread(strategy.decide, market),
                    timeout=decide_timeout,
                )
            self._decide_in_progress = False
            emit_phase_event(
                strategy_id=strategy_id,
                phase=StrategyPhase.DECIDE,
                event_type="STATE_CHANGE",
                description=f"decide() returned {type(decide_result).__name__}",
            )
        except TimeoutError:
            # Worker thread may still be running; _decide_in_progress stays True
            # to block overlapping calls. Recovery allowed after 2x timeout elapsed.
            self._decide_timed_out_at = time.monotonic()
            msg = f"strategy.decide() timed out after {decide_timeout}s"
            logger.error(f"TIMEOUT: {msg} for {strategy_id}")
            if self._circuit_breaker is not None:
                self._circuit_breaker.record_failure(error_message=msg)
            return self._create_error_result(
                strategy_id,
                IterationStatus.STRATEGY_TIMEOUT,
                msg,
                start_time,
            )
        except Exception as e:
            self._decide_in_progress = False  # Normal exceptions complete; reset guard
            logger.error(f"Strategy decision failed: {e}")
            if self._circuit_breaker is not None:
                self._circuit_breaker.record_failure(f"decide() error: {e}")
            return self._create_error_result(
                strategy_id,
                IterationStatus.STRATEGY_ERROR,
                f"Strategy decision failed: {e}",
                start_time,
            )

        state.decide_result = decide_result
        return None

    def _step_extract_intents(self, state: RunIterationState) -> IterationResult | None:
        """Normalise ``decide_result`` into ``state.intents`` and handle HOLD."""
        strategy = state.strategy
        strategy_id = state.strategy_id
        decide_result = state.decide_result

        # Step 3: Extract intents from DecideResult
        intents: list[AnyIntent] = []
        if decide_result is None:
            intents = []
        elif isinstance(decide_result, IntentSequence):
            intents = list(decide_result)
        elif isinstance(decide_result, list):
            for item in decide_result:
                if isinstance(item, IntentSequence):
                    intents.extend(list(item))
                else:
                    intents.append(item)
        else:
            intents = [decide_result]

        # Filter out None values and check for HOLD
        intents = [i for i in intents if i is not None]
        self._invoke_optional_hook(strategy, "on_copy_decision_output", decide_result, intents)

        state.intents = intents

        # Step 4: Handle HOLD or no intent
        if not intents or (len(intents) == 1 and isinstance(intents[0], HoldIntent)):
            hold_intent = intents[0] if intents else None
            reason = hold_intent.reason if isinstance(hold_intent, HoldIntent) else "No action"

            # HOLD should only be considered healthy when the strategy had
            # valid data to make that decision. If market-data provider calls
            # failed unexpectedly, route this cycle into the regular failure
            # path (SadFlow/consecutive-error escalation) instead of silently
            # counting it as success forever.
            market = state.market
            if (
                market is not None
                and hasattr(market, "has_critical_data_failures")
                and callable(market.has_critical_data_failures)
                and market.has_critical_data_failures()
            ):
                classification = "unknown"
                if hasattr(market, "classify_critical_data_failures") and callable(
                    market.classify_critical_data_failures
                ):
                    classification = market.classify_critical_data_failures()
                details = ""
                if hasattr(market, "summarize_critical_data_failures") and callable(
                    market.summarize_critical_data_failures
                ):
                    details = market.summarize_critical_data_failures(limit=3)
                error = f"Critical market-data failures while strategy returned HOLD (classification={classification})"
                if details:
                    error = f"{error}: {details}"
                logger.error("%s", error)
                return self._create_error_result(
                    strategy_id,
                    IterationStatus.DATA_ERROR,
                    error,
                    state.start_time,
                    intent=hold_intent,
                )

            hold_prefix = "⏸️" if _emojis_enabled() else "[HOLD]"
            logger.info(f"{hold_prefix} {strategy_id} HOLD: {reason}")
            self._record_success()
            return IterationResult(
                status=IterationStatus.HOLD,
                intent=hold_intent,
                strategy_id=strategy_id,
                duration_ms=self._calculate_duration_ms(state.start_time),
            )
        return None

    def _step_log_intents(self, state: RunIterationState) -> None:
        """Log the intent or intent sequence with human-readable formatting."""
        strategy = state.strategy
        strategy_id = state.strategy_id
        intents = state.intents

        _chain = getattr(strategy, "chain", "")
        if len(intents) == 1:
            intent_summary = _format_intent_for_log(intents[0], chain=_chain)
            intent_prefix = "📈" if _emojis_enabled() else "[INTENT]"
            logger.info(f"{intent_prefix} {strategy_id} intent: {intent_summary}")
        else:
            # Log intent sequence with details for each step
            intent_prefix = "📈" if _emojis_enabled() else "[INTENT]"
            logger.info(f"{intent_prefix} {strategy_id} intent sequence ({len(intents)} steps):")
            for i, intent in enumerate(intents, 1):
                intent_summary = _format_intent_for_log(intent, chain=_chain)
                logger.info(f"   {i}. {intent_summary}")

    def _step_circuit_breaker_pre_execute(self, state: RunIterationState) -> IterationResult | None:
        """Late circuit-breaker gate: block execution if breaker is open.

        Runs after ``decide()`` succeeded and a real (non-HOLD) intent has
        been produced. Emits an ``ERROR`` timeline event so operators can
        distinguish this from the pre-decide gate.
        """
        if self._circuit_breaker is None:
            return None

        strategy = state.strategy
        strategy_id = state.strategy_id
        intents = state.intents

        cb_check = self._circuit_breaker.check()
        if not cb_check.can_execute:
            logger.warning(
                f"Circuit breaker BLOCKED execution for {strategy_id}: "
                f"state={cb_check.state.value}, reason={cb_check.reason}"
            )
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.ERROR,
                    description=f"Circuit breaker blocked execution: {cb_check.reason}",
                    strategy_id=strategy_id,
                    chain=getattr(strategy, "chain", ""),
                    details={
                        "circuit_breaker_state": cb_check.state.value,
                        "trip_reason": cb_check.trip_reason.value if cb_check.trip_reason else None,
                        "consecutive_failures": cb_check.consecutive_failures,
                        "cumulative_loss_usd": str(cb_check.cumulative_loss_usd),
                        "cooldown_remaining_seconds": cb_check.cooldown_remaining_seconds,
                    },
                )
            )
            # Issue #1780: count the CB-blocked iteration in the lifetime
            # total. The late CB gate produces an IterationResult that
            # run_loop processes like any other failure result.
            self._record_failure()
            return IterationResult(
                status=IterationStatus.CIRCUIT_BREAKER_OPEN,
                intent=intents[0] if intents else None,
                error=f"Circuit breaker open: {cb_check.reason}",
                strategy_id=strategy_id,
                duration_ms=self._calculate_duration_ms(state.start_time),
            )
        return None

    async def _step_snapshot_pre_balances(self, state: RunIterationState) -> None:
        """Snapshot wallet balances for all tokens referenced by the intents.

        Populates ``state.pre_balances`` and ``state.intent_tokens`` for the
        post-execution delta log. Failures are swallowed at debug level so
        balance-provider glitches never block execution.
        """
        intents = state.intents
        pre_balances: dict[str, Decimal] = {}
        intent_tokens: list[str] = []
        try:
            for _intent in intents:
                intent_tokens.extend(_extract_tokens_from_intent(_intent))
            intent_tokens = list(set(intent_tokens))  # dedupe
            if intent_tokens:
                for token in intent_tokens:
                    try:
                        bal = await self.balance_provider.get_balance(token)
                        pre_balances[token] = bal.balance
                    except Exception:
                        pass  # Token balance unavailable, skip delta for this token
        except Exception:
            logger.debug("Failed to snapshot pre-execution balances", exc_info=True)

        state.pre_balances = pre_balances
        state.intent_tokens = intent_tokens

    async def _step_execute(self, state: RunIterationState) -> IterationResult:
        """Dispatch execution to the multi-chain or single-chain path.

        Multi-chain orchestration lives in ``_execute_multi_chain``; the
        single-chain path (amount='all' resolution + sequential intent loop
        + multi-intent metrics + balance deltas) lives in
        ``_run_single_chain_intents``. Both are out of scope for Phase 3b
        and are called as-is.
        """
        if self._is_multi_chain:
            return await self._execute_multi_chain(
                strategy=state.strategy,
                intents=state.intents,
                start_time=state.start_time,
                market=state.market,
            )
        return await self._run_single_chain_intents(state)

    async def _run_single_chain_intents(self, state: RunIterationState) -> IterationResult:
        """Sequentially execute intents through the single-chain orchestrator.

        Handles amount='all' resolution (from previous step output or wallet
        balance), stops on first failure, records multi-intent metrics once
        per iteration, and logs balance deltas. Behaviour is identical to
        the inline code it replaces.
        """
        strategy = state.strategy
        intents = state.intents
        market = state.market
        start_time = state.start_time
        pre_balances = state.pre_balances
        intent_tokens = state.intent_tokens

        # Single-chain execution path
        # Execute all intents sequentially, stopping on first failure
        if len(intents) > 1:
            logger.info(f"Executing {len(intents)} intents sequentially for {strategy.strategy_id}")

        _chain = getattr(strategy, "chain", "")
        intent_result: IterationResult | None = None
        # Issue #1780: track whether the final ``intent_result`` came
        # from the amount='all' resolver short-circuit (no
        # ``_execute_single_chain`` call). Single-intent iterations that
        # short-circuit here never reach a helper that records metrics,
        # so ``_run_single_chain_intents`` must record on their behalf.
        result_from_early_shortcut = False
        is_multi_intent = len(intents) > 1
        previous_amount_received: Decimal | None = None
        for idx, intent in enumerate(intents):
            # Resolve amount="all" from previous step's output or wallet balance.
            # Returns (intent_to_execute, early_result, should_continue) where
            # early_result is a failure/dry-run sentinel and should_continue
            # signals whether to skip this step without breaking the loop.
            (
                intent_to_execute,
                early_result,
                should_continue,
            ) = self._resolve_chained_amount_for_intent(
                intent=intent,
                idx=idx,
                intents=intents,
                is_multi_intent=is_multi_intent,
                previous_amount_received=previous_amount_received,
                market=market,
                strategy=strategy,
                start_time=start_time,
            )
            if early_result is not None:
                intent_result = early_result
                result_from_early_shortcut = True
                if should_continue:
                    continue
                break

            if is_multi_intent:
                logger.info(
                    f"  Executing intent {idx + 1}/{len(intents)}: {_format_intent_for_log(intent_to_execute, chain=_chain)}"
                )

            intent_result = await self._execute_single_chain(
                strategy=strategy,
                intent=intent_to_execute,
                start_time=start_time,
                total_intents=len(intents),
                market=market,
                record_metrics=not is_multi_intent,
            )
            # Once _execute_single_chain ran, it owns metrics for this
            # step (via record_metrics=True on single-intent). Flip the
            # flag off so a later iteration's early_result in a
            # multi-intent sequence doesn't mis-attribute ownership.
            result_from_early_shortcut = False

            # Track amount received for chaining to next step
            if intent_result.status == IterationStatus.SUCCESS and intent_result.execution_result:
                er = intent_result.execution_result
                if er.swap_amounts and er.swap_amounts.amount_out_decimal is not None:
                    previous_amount_received = er.swap_amounts.amount_out_decimal
                else:
                    # No output amount extracted -- do NOT fall back to input amount
                    # (input and output can differ wildly, e.g. 1000 USDC -> 0.5 ETH).
                    # Reset to None so the next chained step fails explicitly
                    # if it uses amount="all" (prevents stale value reuse).
                    previous_amount_received = None
                    # Only warn when there's actually a next step that could need chaining.
                    # Single intents (LP_OPEN, LP_CLOSE, etc.) don't chain amounts.
                    if is_multi_intent and idx < len(intents) - 1:
                        logger.warning(
                            "Amount chaining: no output amount extracted from step %d; "
                            "subsequent amount='all' steps will fail",
                            idx + 1,
                        )

            # Stop on failure - don't execute subsequent intents
            if not intent_result.success:
                if is_multi_intent:
                    logger.warning(
                        f"  Intent {idx + 1}/{len(intents)} failed with {intent_result.status.value}, "
                        "skipping remaining intents"
                    )
                break

        # Record metrics for paths that do NOT go through a helper that
        # already records them:
        #   - multi-intent sequences always record here (the per-step
        #     ``_execute_single_chain`` calls run with record_metrics=False).
        #   - single-intent iterations that short-circuited via
        #     ``_resolve_chained_amount_*`` (e.g. COMPILATION_FAILED when
        #     wallet balance is 0) never reach ``_execute_single_chain``
        #     and therefore no helper recorded them -- fix for issue
        #     #1780, which flagged those as invisible in the lifetime
        #     total. ``consecutive_errors`` and the circuit breaker are
        #     still handled by ``handle_iteration_failure`` in the outer
        #     run loop.
        needs_record_here = is_multi_intent or result_from_early_shortcut
        if needs_record_here and intent_result is not None:
            if intent_result.success:
                self._record_success(execution_proved=intent_result.status == IterationStatus.SUCCESS)
            else:
                self._record_failure()

        # Step 6.9: Compute and log balance deltas after execution
        if pre_balances and intent_result is not None and intent_result.success:
            try:
                self.balance_provider.invalidate_cache()
                post_balances: dict[str, Decimal] = {}
                for token in intent_tokens:
                    try:
                        bal = await self.balance_provider.get_balance(token)
                        post_balances[token] = bal.balance
                    except Exception:
                        pass
                deltas = {}
                for token in intent_tokens:
                    if token in pre_balances and token in post_balances:
                        delta = post_balances[token] - pre_balances[token]
                        if delta != 0:
                            deltas[token] = f"{delta:+.6g}"
                if deltas:
                    delta_str = ", ".join(f"{t}: {v}" for t, v in deltas.items())
                    logger.info(f"Balance delta: {delta_str}")
            except Exception:
                logger.debug("Failed to compute balance deltas", exc_info=True)

        return intent_result  # type: ignore[return-value]

    def _resolve_chained_amount_for_intent(
        self,
        *,
        intent: "AnyIntent",
        idx: int,
        intents: list["AnyIntent"],
        is_multi_intent: bool,
        previous_amount_received: Decimal | None,
        market: Any,
        strategy: "StrategyProtocol",
        start_time: datetime,
    ) -> tuple["AnyIntent", IterationResult | None, bool]:
        """Resolve an ``amount="all"`` intent to a concrete amount.

        Returns a 3-tuple ``(intent_to_execute, early_result, should_continue)``:

        * ``intent_to_execute`` — the (possibly) rewritten intent to send to
          ``_execute_single_chain``. When ``early_result`` is non-None this is
          the raw input intent and the caller should use ``early_result`` as
          this step's result instead of executing.
        * ``early_result`` — ``None`` when resolution succeeded (or when the
          intent does not use ``amount="all"``). Otherwise an
          ``IterationResult`` sentinel (DRY_RUN / COMPILATION_FAILED) that
          the caller should record as ``intent_result`` and either skip
          (``should_continue=True``) or stop the loop for.
        * ``should_continue`` — when ``True`` the caller should ``continue``
          the loop to the next intent; when ``False`` and ``early_result`` is
          set, the caller should ``break``.

        Behaviour is identical to the original inline resolution logic.
        """
        if not Intent.has_chained_amount(intent):
            return intent, None, False

        if is_multi_intent and previous_amount_received is not None:
            # Multi-intent chain: resolve from previous step output
            logger.info(f"  Resolving amount='all' to {previous_amount_received} for intent {idx + 1}/{len(intents)}")
            return Intent.set_resolved_amount(intent, previous_amount_received), None, False

        if is_multi_intent and previous_amount_received is None and idx > 0:
            # Multi-intent but no previous output (dry-run or error)
            if self.config.dry_run:
                logger.warning(
                    f"  Intent {idx + 1}/{len(intents)} uses amount='all' "
                    "but no previous step output available (dry-run mode). "
                    "Skipping compilation of this step."
                )
                result = IterationResult(
                    status=IterationStatus.DRY_RUN,
                    intent=intent,
                    strategy_id=strategy.strategy_id,
                    duration_ms=self._calculate_duration_ms(start_time),
                )
                return intent, result, True  # continue

            logger.error(f"  Intent {idx + 1}/{len(intents)} uses amount='all' but no previous step amount available")
            result = IterationResult(
                status=IterationStatus.COMPILATION_FAILED,
                intent=intent,
                error="amount='all' used but no previous step amount available",
                strategy_id=strategy.strategy_id,
                duration_ms=self._calculate_duration_ms(start_time),
            )
            return intent, result, False  # break

        # Single intent or first intent in multi-sequence: resolve amount='all'
        # from wallet balance for wallet-funded intents. Protocol-position
        # intents (withdraw, repay, unstake) use amount='all' to mean "all
        # from the protocol position" — let the compiler handle those.
        return self._resolve_chained_amount_from_wallet(
            intent=intent,
            market=market,
            strategy=strategy,
            start_time=start_time,
        )

    def _resolve_chained_amount_from_wallet(
        self,
        *,
        intent: "AnyIntent",
        market: Any,
        strategy: "StrategyProtocol",
        start_time: datetime,
    ) -> tuple["AnyIntent", IterationResult | None, bool]:
        """Resolve ``amount="all"`` from the wallet balance for the intent.

        Mirrors the inline wallet-balance fallback used for single intents
        and the first step of a multi-intent sequence. Returns the same
        3-tuple contract as :meth:`_resolve_chained_amount_for_intent`.
        """
        _WALLET_FUNDED_TYPES = {
            IntentType.SWAP,
            IntentType.SUPPLY,
            IntentType.BORROW,
            IntentType.STAKE,
            IntentType.LP_OPEN,
            IntentType.PERP_OPEN,
            IntentType.VAULT_DEPOSIT,
            IntentType.BRIDGE,
        }
        intent_type = getattr(intent, "intent_type", None)
        if intent_type not in _WALLET_FUNDED_TYPES:
            # Protocol-position or unknown intent — let compiler handle natively
            logger.debug(f"  amount='all' for {intent_type} — passing to compiler as-is")
            return intent, None, False

        balance_token = (
            getattr(intent, "from_token", None)
            or getattr(intent, "token", None)
            or getattr(intent, "token_in", None)
            or getattr(intent, "collateral_token", None)
        )

        if balance_token and market is not None:
            try:
                bal = market.balance(balance_token)
                # market.balance() may return TokenBalance or Decimal
                balance_value = bal.balance if hasattr(bal, "balance") else bal
                if balance_value <= 0:
                    logger.warning(f"  amount='all' for {balance_token} but balance is 0")
                    result = IterationResult(
                        status=IterationStatus.COMPILATION_FAILED,
                        intent=intent,
                        error=f"amount='all' for {balance_token} but balance is 0",
                        strategy_id=strategy.strategy_id,
                        duration_ms=self._calculate_duration_ms(start_time),
                    )
                    return intent, result, False  # break
                resolved = Intent.set_resolved_amount(intent, balance_value)
                logger.info(f"  Resolved amount='all' for {balance_token} from wallet: {balance_value}")
                return resolved, None, False
            except Exception as e:  # noqa: BLE001
                logger.error(f"  Failed to resolve amount='all' for {balance_token}: {e}")
                result = IterationResult(
                    status=IterationStatus.COMPILATION_FAILED,
                    intent=intent,
                    error=f"Cannot resolve amount='all' for {balance_token}: {e}",
                    strategy_id=strategy.strategy_id,
                    duration_ms=self._calculate_duration_ms(start_time),
                )
                return intent, result, False  # break

        if balance_token is None:
            # No token field found — let compiler handle
            logger.debug("  amount='all' with no token field, passing to compiler as-is")
            return intent, None, False

        # Have token but no market — cannot resolve
        logger.error(f"  amount='all' for {balance_token} but no market context available")
        result = IterationResult(
            status=IterationStatus.COMPILATION_FAILED,
            intent=intent,
            error=(f"amount='all' for {balance_token} but no market context available"),
            strategy_id=strategy.strategy_id,
            duration_ms=self._calculate_duration_ms(start_time),
        )
        return intent, result, False  # break

    async def run_loop(
        self,
        strategy: StrategyProtocol,
        interval_seconds: int | None = None,
        iteration_callback: Callable[[IterationResult], None] | None = None,
        pre_iteration_callback: Callable[[], None] | None = None,
        max_iterations: int | None = None,
    ) -> None:
        """Run the strategy in a continuous loop.

        This method runs the strategy continuously with the specified interval,
        handling graceful shutdown via request_shutdown().

        Args:
            strategy: The strategy to execute
            interval_seconds: Seconds between iterations (uses config default if None)
            iteration_callback: Optional callback called after each iteration
            pre_iteration_callback: Optional callback called before each iteration
                (e.g., to reset Anvil forks for live paper trading). Regular errors
                are logged but do not stop the loop. To signal a fail-closed
                condition, raise CriticalCallbackError instead.
            max_iterations: Maximum number of iterations to run. None means run indefinitely.
        """
        interval = interval_seconds or self.config.default_interval_seconds
        strategy_id = strategy.strategy_id

        max_iter_msg = f", max_iterations={max_iterations}" if max_iterations else ""
        logger.info(f"Starting run loop for strategy {strategy_id} with interval={interval}s{max_iter_msg}")

        # Phase 1: setup (state manager init, session recovery, copy-trading
        # restore, shutdown flag reset, gateway wiring, RUNNING write,
        # STRATEGY_STARTED event).
        activity_provider = await _run_loop_helpers.initialize_run_loop(self, strategy, strategy_id, interval)

        loop_iteration_count = 0
        while not self._shutdown_requested:
            try:
                # Phase 3: pre-iteration callback (e.g., reset Anvil forks).
                _run_loop_helpers.invoke_pre_iteration_callback(pre_iteration_callback)

                # Snapshot the error-streak flag BEFORE the iteration runs. Successful
                # iterations reset `_consecutive_errors` to 0 inside `run_iteration`
                # (via `_record_success`), so we must capture "were we in an error
                # streak?" before that reset happens — otherwise the recovery branch
                # below is unreachable.
                was_in_error_streak = self._consecutive_errors >= self.config.max_consecutive_errors

                # Anchor wall-clock for the full iteration + snapshot phase. Used
                # by ``capture_snapshot_with_accounting`` to report a complete
                # ``duration_ms`` on ACCOUNTING_FAILED results (issue #1782
                # follow-up to #1770 -- #1770 preserved iteration-body duration,
                # but the snapshot phase that actually failed still wasn't
                # included in the reported duration).
                iteration_start_monotonic = time.monotonic()

                # Phase 4: run one iteration.
                result = await self.run_iteration(strategy)

                # Capture portfolio snapshot (possibly rebuilding `result` into
                # ACCOUNTING_FAILED in live mode on AccountingPersistenceError).
                #
                # The iteration_summary emission and state-persistence calls
                # below are intentionally sequenced AFTER the snapshot phase
                # so they observe the FINAL result (including the
                # ACCOUNTING_FAILED rebuild + full iteration+snapshot
                # duration_ms). Emitting before the snapshot would leak a
                # misleading SUCCESS row into operator dashboards whenever
                # the live-mode snapshot persistence fails (issue #1782,
                # Gemini review of PR #1786).
                result = await _run_loop_helpers.capture_snapshot_with_accounting(
                    self,
                    strategy,
                    strategy_id,
                    result,
                    iteration_start_monotonic=iteration_start_monotonic,
                )

                # Emit structured iteration summary for JSONL log analysis
                self._emit_iteration_summary(result, chain=getattr(strategy, "chain", None))

                # Update state
                if self.config.enable_state_persistence:
                    await self._update_state(strategy_id, result, strategy=strategy)

                # Persist copy trading cursor state (if configured)
                if activity_provider is not None and self.config.enable_state_persistence:
                    try:
                        await self._persist_copy_trading_state(strategy_id, activity_provider)
                    except Exception as e:
                        logger.warning(f"Failed to persist copy trading state: {e}")

                # Call callback if provided
                if iteration_callback:
                    try:
                        iteration_callback(result)
                    except Exception as e:
                        logger.error(f"Iteration callback error: {e}")

                # Phase 8: post-iteration bookkeeping (consecutive-errors,
                # circuit breaker, lifecycle recovery writes).
                if not result.success:
                    await _run_loop_helpers.handle_iteration_failure(self, strategy, strategy_id, result)
                else:
                    _run_loop_helpers.handle_iteration_success(self, strategy_id, was_in_error_streak)

                # Report positions and send heartbeat to gateway after each iteration
                position_protos = self._collect_position_snapshot(strategy)
                self._gateway_heartbeat(strategy_id, positions=position_protos)

                # Send lifecycle heartbeat
                self._lifecycle_heartbeat(strategy_id)

                # Poll for + route lifecycle commands (PAUSE, RESUME, STOP).
                command = self._lifecycle_poll_command(strategy_id)
                await _run_loop_helpers.handle_lifecycle_command(self, strategy, strategy_id, command)

                # Check max iterations limit
                loop_iteration_count += 1
                if max_iterations is not None and loop_iteration_count >= max_iterations:
                    logger.info(f"Reached max iterations ({max_iterations}) for {strategy_id}. Stopping.")
                    break

                # Sleep until next iteration (unless shutdown requested)
                if not self._shutdown_requested:
                    logger.debug(f"Sleeping for {interval}s before next iteration")
                    await asyncio.sleep(interval)

            except asyncio.CancelledError:
                logger.info(f"Run loop cancelled for {strategy_id}")
                break
            except CriticalCallbackError:
                logger.error("Critical callback error — stopping strategy loop")
                break
            except Exception as e:
                logger.exception(f"Unexpected error in run loop: {e}")
                self._consecutive_errors += 1
                if not self._shutdown_requested:
                    await asyncio.sleep(interval)

        # Phase 12: shutdown drain (final lifecycle write, deregister,
        # STRATEGY_STOPPED event, flush, state manager close).
        await _run_loop_helpers.finalize_run_loop(self, strategy, strategy_id)

    def _emit_execution_timeline_event(
        self,
        strategy: StrategyProtocol,
        intent: AnyIntent,
        success: bool,
        result: Any | None,
    ) -> None:
        """Emit a timeline event for an intent execution (success or failure)."""
        try:
            strategy_id = strategy.strategy_id
            intent_type = getattr(intent, "intent_type", None)
            intent_type_value = getattr(intent_type, "value", None)
            intent_type_str = intent_type_value if isinstance(intent_type_value, str) else str(intent_type)

            # Map intent type to timeline event type
            event_type_map = {
                "SWAP": TimelineEventType.SWAP,
                "LP_OPEN": TimelineEventType.LP_OPEN,
                "LP_CLOSE": TimelineEventType.LP_CLOSE,
            }
            event_type = event_type_map.get(
                intent_type_str,
                TimelineEventType.TRADE,
            )
            if not success:
                event_type = TimelineEventType.TRANSACTION_FAILED

            # Build description
            tx_hash = ""
            gas_used = 0
            if result:
                if hasattr(result, "transaction_results") and result.transaction_results:
                    tx_hash = result.transaction_results[0].tx_hash or ""
                gas_used = getattr(result, "total_gas_used", 0)

            if success:
                description = f"{intent_type_str} executed successfully ({gas_used:,} gas)"
            else:
                error = getattr(result, "error", "Unknown error") if result else "Unknown error"
                description = f"{intent_type_str} failed: {error}"

            details: dict[str, Any] = {
                "intent_type": intent_type_str,
                "success": success,
                "gas_used": gas_used,
            }

            # Enrich details with position/swap data extracted by ResultEnricher
            # so downstream consumers (teardown, audits, PM dashboard) can recover
            # position IDs and ranges directly from timeline events without
            # reparsing receipts. Bug 4 of the 0G DogFooding report (2026-04-16).
            if success and result is not None:
                position_id = getattr(result, "position_id", None)
                if position_id is not None:
                    # NFT tokenIds can exceed JS's safe-integer range on chains
                    # with high-throughput NPMs (Solana ALT indices, V4 salt-
                    # derived IDs). Stringify oversized ints so dashboards
                    # and webhooks that consume details_json as JSON don't
                    # silently truncate them — matches the safeguard below
                    # for liquidity / amount values.
                    if isinstance(position_id, int) and abs(position_id) >= 2**53:
                        details["position_id"] = str(position_id)
                    elif isinstance(position_id, int | str):
                        details["position_id"] = position_id
                    else:
                        details["position_id"] = str(position_id)
                extracted = getattr(result, "extracted_data", None) or {}
                for key in ("tick_lower", "tick_upper", "liquidity", "amount0", "amount1"):
                    value = extracted.get(key)
                    if value is None:
                        continue
                    # liquidity/amount0/amount1 can exceed JSON's safe integer range
                    details[key] = str(value) if isinstance(value, int) and abs(value) >= 2**53 else value
                lp_close_data = getattr(result, "lp_close_data", None)
                if lp_close_data is not None and hasattr(lp_close_data, "to_dict"):
                    details["lp_close"] = lp_close_data.to_dict()
                swap_amounts = getattr(result, "swap_amounts", None)
                if swap_amounts is not None and hasattr(swap_amounts, "to_dict"):
                    details["swap"] = swap_amounts.to_dict()

            event = TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=event_type,
                description=description,
                strategy_id=strategy_id,
                chain=getattr(strategy, "chain", "") or getattr(self.config, "chain", ""),
                tx_hash=tx_hash,
                details=details,
            )
            add_event(event)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"Failed to emit execution timeline event: {e}")

    def _is_live_mode(self) -> bool:
        """Return True when ledger/snapshot/metrics writes are mandatory.

        Live mode = real execution against a real chain. Dry-run and paper
        modes may drop writes on failure, but they still log at ERROR so the
        drift is visible before it reaches production.

        Paper-trading runners are subclasses that set ``config.paper_mode =
        True`` (checked via ``getattr`` so the base ``RunnerConfig`` doesn't
        need to know about paper trading). Backtest runners bypass the
        StrategyRunner entirely.
        """
        return derive_execution_mode_from_config(self.config) is ExecutionMode.LIVE

    def _derive_execution_mode(self) -> ExecutionMode:
        """Tri-state mode label for accounting rows (dry_run / live / paper).

        Centralised so ledger entries, portfolio snapshots, and portfolio
        metrics all stamp the same value and the runner's mode semantics
        cannot drift across these surfaces. Returns a ``StrEnum`` so callers
        that stringify it (e.g. ``entry.execution_mode = mode``) get the
        bare label back for persistence.
        """
        return derive_execution_mode_from_config(self.config)

    async def _write_ledger_entry(
        self,
        strategy: StrategyProtocol,
        intent: AnyIntent,
        result: Any | None,
        success: bool,
        error: str = "",
    ) -> str | None:
        """Returns the persisted LedgerEntry.id on success, None on non-live failure."""
        """Write a structured trade record to the transaction ledger.

        VIB-3157: in live mode a persistence failure raises
        ``AccountingPersistenceError`` so the caller (run_iteration) can halt
        the cycle and alert the operator. In paper/dry-run mode we log ERROR
        and continue -- the drift is visible but does not block the loop.
        """

        try:
            from ..observability.context import get_cycle_id
            from ..observability.ledger import build_ledger_entry

            cycle_id = get_cycle_id() or ""
            chain = getattr(strategy, "chain", "") or getattr(self.config, "chain", "")
            entry = build_ledger_entry(
                strategy_id=strategy.strategy_id,
                cycle_id=cycle_id,
                intent=intent,
                result=result,
                chain=chain,
                success=success,
                error=error,
            )

            # Phase 4: stamp deployment_id and execution_mode onto the entry (VIB-2835/2837).
            # VIB-3157: tri-state (dry_run / live / paper) via the shared
            # ``derive_execution_mode_from_config`` helper so ledger,
            # snapshot, and metrics stamping stay in lockstep.
            deployment_id = getattr(strategy, "deployment_id", "") or strategy.strategy_id
            execution_mode = self._derive_execution_mode()
            entry.deployment_id = deployment_id
            entry.execution_mode = execution_mode

            # VIB-3157: fail-closed live path. A missing state manager or a
            # state manager without ledger support in live mode is a
            # misconfiguration that would let trades land with no durable
            # accounting record -- exactly the footgun VIB-3157 is closing.
            # In paper/dry-run we log at ERROR and continue so pre-prod drift
            # is visible but the loop keeps moving.
            if not self.state_manager or not hasattr(self.state_manager, "save_ledger_entry"):
                if self._is_live_mode():
                    raise AccountingPersistenceError(
                        write_kind="ledger",
                        strategy_id=strategy.strategy_id,
                        message="State manager does not provide save_ledger_entry",
                    )
                logger.error(
                    "Ledger write unavailable in non-live mode for %s "
                    "(continuing, pre-prod drift; fix before promoting to live)",
                    strategy.strategy_id,
                )
            else:
                # VIB-3201 closed the gateway ledger gap (SaveLedgerEntry RPC).
                # The fail-closed contract now applies uniformly: any exception
                # propagates to the AccountingPersistenceError path below. No
                # backend-specific NotImplementedError escape hatch remains.
                await self.state_manager.save_ledger_entry(entry)

            # Emit position event for LP/perp intents (Phase 2, VIB-2775)
            if success and self.state_manager and hasattr(self.state_manager, "save_position_event"):
                try:
                    from ..observability.position_events import build_position_event_from_intent

                    pos_event = build_position_event_from_intent(
                        deployment_id=deployment_id,
                        intent=intent,
                        result=result,
                        ledger_entry_id=entry.id,
                        chain=chain,
                    )
                    if pos_event is not None:
                        # Phase 4: stamp cycle_id and execution_mode (VIB-2835/2837)
                        pos_event.cycle_id = cycle_id
                        pos_event.execution_mode = execution_mode
                        await self.state_manager.save_position_event(pos_event)
                        logger.debug(
                            "Position event %s emitted for %s (position=%s)",
                            pos_event.event_type,
                            pos_event.position_type,
                            pos_event.position_id,
                        )
                        # VIB-3205: stamp entry_state on OPEN events so subsequent
                        # CLOSE-time IL attribution can evaluate HODL value.
                        if pos_event.event_type == "OPEN" and pos_event.position_id:
                            try:
                                from ..observability.pnl_attributor import stamp_entry_state_on_open

                                await stamp_entry_state_on_open(
                                    self.state_manager,
                                    pos_event,
                                    price_oracle=self.price_oracle,
                                )
                            except Exception:  # noqa: BLE001
                                # Entry-state stamping is best-effort but NOT trivial: if it
                                # silently drops, close-time IL attribution later short-circuits
                                # to None with no hint as to why. Escalate to WARNING with a
                                # traceback so operators notice the first time a class of
                                # position fails to record its entry price basis.
                                logger.warning(
                                    "Entry-state stamp failed (non-blocking) for position=%s",
                                    pos_event.position_id,
                                    exc_info=True,
                                )
                        # Run PnL attribution on CLOSE events (VIB-2776, v2 VIB-3205)
                        if pos_event.event_type == "CLOSE" and pos_event.position_id:
                            try:
                                from ..observability.pnl_attributor import run_attribution_on_close

                                await run_attribution_on_close(self.state_manager, pos_event)
                            except Exception as attr_err:  # noqa: BLE001
                                logger.debug("Attribution failed (non-blocking): %s", attr_err)
                except Exception as pe:  # noqa: BLE001
                    logger.debug("Failed to emit position event: %s", pe)

            # Signal that this iteration executed a trade — forces snapshot
            if success:
                self._iteration_had_trade = True
            return entry.id
        except AccountingPersistenceError:
            # Live mode: propagate so run_iteration halts the cycle and alerts.
            # Paper/dry-run: swallow but log ERROR (not debug) so drift is visible.
            if self._is_live_mode():
                raise
            logger.error(
                "Ledger write failed in non-live mode for %s (continuing, pre-prod drift): "
                "fix before promoting to live",
                strategy.strategy_id,
            )
        except Exception as e:  # noqa: BLE001
            # Unexpected failure outside the persistence path (build_ledger_entry
            # raised, position_event emission re-raised, etc.). Live mode still
            # escalates -- a trade happened with no durable record.
            if self._is_live_mode():
                raise AccountingPersistenceError(
                    write_kind="ledger",
                    strategy_id=strategy.strategy_id,
                    cause=e,
                ) from e
            logger.error(f"Failed to write ledger entry (non-live): {e}")
        return None

    async def _write_outbox_and_fire_processor(
        self,
        strategy: "StrategyProtocol",
        intent: "AnyIntent",
        ledger_entry_id: str,
    ) -> None:
        """Write accounting_outbox row and fire asyncio task to drain it (VIB-3467).

        In live mode: raises AccountingPersistenceError on outbox write failure so
        run_iteration routes to ACCOUNTING_FAILED and alerts operators.
        In non-live modes: logs a warning and continues (best-effort).
        The async drain task is always fire-and-forget — durability is provided by
        the outbox row, not the task. The processor is the sole accounting write path
        (VIB-3478 removed the legacy _try_write_* inline writers).
        """
        try:
            from ..accounting.processor import write_outbox_entry
            from ..observability.context import get_cycle_id
            from ..state.exceptions import AccountingPersistenceError

            intent_type_str = ""
            it = getattr(intent, "intent_type", None)
            if it is not None:
                intent_type_str = it.value if hasattr(it, "value") else str(it)

            if not intent_type_str:
                return

            chain = getattr(strategy, "chain", "") or getattr(self.config, "chain", "")
            wallet_address = getattr(strategy, "wallet_address", "") or ""
            deployment_id = getattr(strategy, "deployment_id", "") or strategy.strategy_id
            cycle_id = get_cycle_id() or ""

            # Compute position_key and market_id for each supported category
            position_key, market_id = self._compute_outbox_position_key(intent, intent_type_str, chain, wallet_address)

            # Update processor deployment_id (set once per strategy run)
            if self._accounting_processor._deployment_id != deployment_id:
                self._accounting_processor._deployment_id = deployment_id

            outbox_id = await write_outbox_entry(
                self.state_manager,
                deployment_id=deployment_id,
                strategy_id=strategy.strategy_id,
                cycle_id=cycle_id,
                ledger_entry_id=ledger_entry_id,
                intent_type=intent_type_str,
                wallet_address=wallet_address,
                position_key=position_key,
                market_id=market_id,
            )

            if outbox_id:
                task = asyncio.create_task(
                    self._accounting_processor.drain_one(ledger_entry_id),
                    name=f"accounting_drain_{ledger_entry_id[:8]}",
                )
                self._pending_drain_tasks.add(task)
                task.add_done_callback(self._pending_drain_tasks.discard)
            else:
                # outbox_id is None — write_outbox_entry returned without persisting.
                # In live mode this is a data-loss event; raise AccountingPersistenceError
                # so run_iteration routes to ACCOUNTING_FAILED and alerts operators.
                if self._is_live_mode():
                    raise AccountingPersistenceError(
                        f"write_outbox_entry returned None for ledger_entry_id={ledger_entry_id!r} "
                        f"— accounting event will be lost"
                    )
                logger.warning(
                    "_write_outbox_and_fire_processor: outbox write returned None for %s (non-live — continuing)",
                    ledger_entry_id,
                )
        except AccountingPersistenceError:
            raise
        except Exception:
            if self._is_live_mode():
                raise AccountingPersistenceError(
                    f"_write_outbox_and_fire_processor failed for {ledger_entry_id!r}"
                ) from None
            logger.warning("_write_outbox_and_fire_processor failed (non-blocking)", exc_info=True)

    def _compute_outbox_position_key(
        self,
        intent: "AnyIntent",
        intent_type_str: str,
        chain: str,
        wallet_address: str,
    ) -> tuple[str, str]:
        """Return (position_key, market_id) for the given intent.

        Mirrors the position_key derivation logic in the inline accounting builders
        so the outbox row and accounting_events row use identical keys.
        """
        try:
            protocol = (getattr(intent, "protocol", "") or "").lower()
            t = intent_type_str.upper()

            # Lending (SUPPLY / BORROW / REPAY / DELEVERAGE / WITHDRAW)
            if t in {"SUPPLY", "BORROW", "REPAY", "DELEVERAGE", "WITHDRAW"}:
                from ..accounting.lending_accounting import _derive_position_key, _intent_asset, _intent_market_id

                market_id = _intent_market_id(intent) or ""
                asset = _intent_asset(intent)
                position_key = _derive_position_key(protocol, chain, wallet_address, market_id or None, asset)
                return position_key, market_id

            # Pendle LP (LP_OPEN / LP_CLOSE for pendle protocol)
            if t in {"LP_OPEN", "LP_CLOSE"} and "pendle" in protocol:
                from ..accounting.pendle_accounting import _derive_pendle_position_key, _get_market_address

                market_address = _get_market_address(intent)
                position_key = (
                    _derive_pendle_position_key(chain, wallet_address, market_address) if market_address else ""
                )
                return position_key, market_address

            # Pendle PT (SWAP for pendle protocol)
            if t == "SWAP" and "pendle" in protocol:
                market_address = (getattr(intent, "pool", None) or "").lower()
                position_key = (
                    f"pendle_pt:{chain.lower()}:{wallet_address.lower()}:{market_address}" if market_address else ""
                )
                return position_key, market_address

            # Non-Pendle SWAP — position key groups by chain+wallet for FIFO lot tracking.
            if t == "SWAP":
                position_key = (
                    f"swap:{chain.lower().strip()}:{wallet_address.lower().strip()}"
                    if (chain and wallet_address)
                    else ""
                )
                return position_key, ""

        except Exception:
            logger.debug("_compute_outbox_position_key failed", exc_info=True)

        return "", ""

    def _accounting_context(self, strategy: "StrategyProtocol") -> tuple[str, str, str, str, str]:
        """Return (deployment_id, cycle_id, execution_mode, chain, wallet_address) for accounting builders."""
        from ..observability.context import get_cycle_id

        deployment_id = getattr(strategy, "deployment_id", "") or strategy.strategy_id
        cycle_id = get_cycle_id() or ""
        execution_mode = self._derive_execution_mode()
        chain = getattr(strategy, "chain", "") or getattr(self.config, "chain", "")
        wallet_address = getattr(strategy, "wallet_address", "")
        return deployment_id, cycle_id, execution_mode, chain, wallet_address

    def _maybe_warn_deleverage(self, intent: "AnyIntent", strategy: "StrategyProtocol") -> None:
        """Log WARNING when a DELEVERAGE intent was successfully executed.

        DELEVERAGE is a notable risk event — surfaces to operators even when
        they are not actively monitoring DEBUG logs.
        """
        it = getattr(intent, "intent_type", None)
        intent_type_str = (it.value if hasattr(it, "value") else str(it)) if it is not None else ""
        if intent_type_str != "DELEVERAGE":
            return
        logger.warning(
            "DELEVERAGE intent executed for strategy=%s — trigger=%r observed_hf=%s target_hf=%s",
            getattr(strategy, "strategy_id", ""),
            getattr(intent, "trigger_reason", "") or "",
            getattr(intent, "observed_hf", None),
            getattr(intent, "target_hf", None),
        )

    def request_shutdown(self) -> None:
        """Request graceful shutdown of the run loop.

        This sets a flag that causes run_loop() to exit after the
        current iteration completes.
        """
        logger.info("Shutdown requested for strategy runner")
        self._shutdown_requested = True

    def _request_teardown_failure_shutdown(self, error_message: str) -> None:
        """Record error terminal state and request shutdown after teardown failure.

        In managed deployments (K8s pods), this writes ERROR state so the platform
        picks up the failure, then shuts down to free cluster resources.

        In local development, the runner stays alive so the developer can inspect
        state or retry — matching the circuit breaker pattern (see _check_circuit_breaker).
        """
        if not self._is_managed_deployment():
            logger.warning("Teardown failed in local mode — runner stays alive for debugging: %s", error_message)
            return
        self._terminal_lifecycle_state = "ERROR"
        self._terminal_lifecycle_error_message = error_message
        self.request_shutdown()

    def _is_managed_deployment(self) -> bool:
        """Return True if running as a deployed agent (not local development).

        The deployer injects AGENT_ID into pod containers. Local runs don't
        have this env var, so this is a clean, zero-config detection mechanism.
        """
        return bool(os.environ.get("AGENT_ID", "").strip())

    def setup_signal_handlers(self) -> None:
        """Set up signal handlers for graceful shutdown.

        Registers handlers for SIGINT and SIGTERM that call request_shutdown().
        Should be called before run_loop() in production deployments.
        """

        def handle_signal(signum: int, frame: Any) -> None:
            signal_name = signal.Signals(signum).name
            logger.info(f"Received {signal_name}, requesting shutdown...")
            self._signal_received = True
            self.request_shutdown()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
        logger.info("Signal handlers registered for SIGINT and SIGTERM")

    # =========================================================================
    # Private Methods
    # =========================================================================

    def _on_sadflow_enter(
        self,
        error_type: str | None,
        attempt: int,
        context: SadflowContext,
    ) -> SadflowAction | None:
        """Apply retry policy for known deterministic failures.

        Some errors are not likely to succeed by immediate retry in the same loop
        (for example zero native gas balance). Abort early to reduce noise and
        surface the root cause faster.
        """
        _ = attempt  # Included for callback compatibility
        non_retryable_types = {"INSUFFICIENT_FUNDS", "NONCE_ERROR", "COMPILATION_PERMANENT", "REVERT"}
        if error_type in non_retryable_types:
            logger.warning(
                f"Non-retryable error ({error_type}): {context.error_message}. "
                "Skipping retries — this error will not resolve by retrying."
            )
            return SadflowAction.abort(context.error_message)
        return None

    def _invoke_optional_hook(self, strategy: StrategyProtocol, hook_name: str, *args: Any) -> None:
        """Invoke a strategy hook if present, swallowing callback errors."""
        if not hasattr(strategy, hook_name):
            return
        try:
            getattr(strategy, hook_name)(*args)
        except Exception as e:
            logger.warning(f"Error in strategy hook {hook_name}: {e}")

    async def _execute_single_chain(
        self,
        strategy: StrategyProtocol,
        intent: AnyIntent,
        start_time: datetime,
        total_intents: int = 1,
        market: Any | None = None,
        record_metrics: bool = True,
    ) -> IterationResult:
        """Execute a single intent through the single-chain orchestrator using IntentStateMachine.

        Uses IntentStateMachine for automatic retry logic with exponential backoff.
        The state machine handles:
        - PREPARING: Compile intent to ActionBundle
        - VALIDATING: Execute and check transaction receipt
        - SADFLOW: Handle failures with automatic retries

        Retries occur automatically per state machine configuration (default 3).
        Operator escalation only happens after state machine reaches FAILED state.

        Phase 3c: This is now a thin driver that sets up a
        ``SingleChainExecutionState`` and threads it through per-phase step
        helpers. Behaviour is identical to the pre-refactor inline code.

        Args:
            strategy: The strategy being executed
            intent: The intent to execute
            start_time: When the iteration started
            total_intents: Total intents in the decide result (for logging)
            market: Optional market snapshot with real prices for accurate compilation
            record_metrics: Whether to record success/failure metrics (False for multi-intent
                sequences where metrics are recorded once per iteration by the caller)

        Returns:
            IterationResult with execution details
        """
        if total_intents > 1:
            logger.debug(f"Executing intent as part of a {total_intents}-intent sequence")

        state = SingleChainExecutionState(
            strategy=strategy,
            intent=intent,
            start_time=start_time,
            total_intents=total_intents,
            market=market,
            record_metrics=record_metrics,
            strategy_id=strategy.strategy_id,
        )

        # Setup: build compiler, state machine, pre-balance snapshot. If a
        # setup step returns an early-exit result (currently only dry-run is
        # possible later), propagate it.
        try:
            await self._init_single_chain_state(state)
        except Exception:
            if state.clob_client is not None:
                state.clob_client.close()
            raise

        # Drive the state-machine loop. Dry-run short-circuits return an
        # IterationResult early.
        early = await self._single_chain_state_machine_loop(state)
        if early is not None:
            return early

        # Close ClobClient to release httpx connection pool resources
        if state.clob_client is not None:
            try:
                state.clob_client.close()
            except Exception:
                logger.debug("Failed to close ClobClient", exc_info=True)

        # Always invalidate balance cache after execution (success or failure)
        # to prevent stale reads on the next decide() cycle.
        self.balance_provider.invalidate_cache()

        if state.state_machine.success:
            return await self._single_chain_handle_success(state)
        return await self._single_chain_handle_failure(state)

    # -------------------------------------------------------------------------
    # _execute_single_chain step helpers (Phase 3c)
    # -------------------------------------------------------------------------
    #
    # Each helper takes the ``SingleChainExecutionState`` for the current
    # execution, mutates it, and either returns ``None`` (continue to the
    # next step) or an ``IterationResult`` early-exit. The helper names are
    # not load-bearing -- they are descriptive boundaries around pieces of
    # the original inline code.

    async def _init_single_chain_state(self, state: SingleChainExecutionState) -> None:
        """Populate runtime handles on ``state`` (compiler, state machine, etc.).

        Builds: gateway_client / rpc_url, price_oracle, polymarket config,
        clob handler, IntentCompiler, IntentStateMachine, pre-execution
        balance snapshot. Emits the COMPILE phase event.
        """
        strategy = state.strategy
        intent = state.intent
        strategy_id = state.strategy_id

        # Resolve gateway client from any available source (GatewayExecutionOrchestrator,
        # MultiChainOrchestrator with _gateway_client, or explicit set_gateway_client()).
        state.gateway_client = self._get_gateway_client()
        if state.gateway_client is not None:
            logger.debug("Gateway client available — RPC queries go through gateway")
        else:
            # Fallback to direct RPC (deprecated for production)
            state.rpc_url = getattr(self.execution_orchestrator, "rpc_url", None)
            if state.rpc_url:
                logger.warning("Using direct RPC URL - this is deprecated for production use")

        # Extract real prices from market snapshot for accurate slippage calculations
        # Without this, IntentCompiler uses hardcoded default prices which causes
        # min_output calculations to be wrong (e.g., ETH at $2000 vs real $3117)
        state.price_oracle = self._build_single_chain_price_oracle(state.market, intent)

        # Build gateway-backed Polymarket execution handles for Polygon.
        if strategy.chain.lower() == "polygon" and state.gateway_client is not None:
            from ..connectors.polymarket.gateway_client import GatewayPolymarketClient
            from ..execution.clob_handler import ClobActionHandler

            state.clob_client = GatewayPolymarketClient(state.gateway_client)
            state.clob_handler = ClobActionHandler(clob_client=state.clob_client)

        # Build compiler config
        # Allow placeholder prices when no real prices are available (empty oracle).
        # This happens legitimately when the strategy uses indicators (RSI, BB)
        # instead of calling market.price() directly.  Placeholder prices are only
        # used as fallback for tokens not in the oracle dict, so an empty oracle
        # with placeholders enabled is safe -- the compiler will use conservative
        # hardcoded estimates for slippage calculations.
        if state.price_oracle is None:
            logger.debug(
                "No prices in market snapshot -- compiler will use placeholder prices. "
                "This is normal for strategies that use indicators instead of market.price()."
            )
        compiler_config = IntentCompilerConfig(
            allow_placeholder_prices=state.price_oracle is None,
        )

        state.compiler = IntentCompiler(
            chain=strategy.chain,
            wallet_address=strategy.wallet_address,
            rpc_url=state.rpc_url,
            price_oracle=state.price_oracle,
            config=compiler_config,
            gateway_client=state.gateway_client,
            chain_wallets=getattr(strategy, "_chain_wallets", None),
        )

        state_machine_config = StateMachineConfig(
            retry_config=RetryConfig(
                max_retries=self.config.max_retries,
                initial_delay_seconds=self.config.initial_retry_delay,
                max_delay_seconds=self.config.max_retry_delay,
            ),
            emit_metrics=True,
        )

        state.state_machine = IntentStateMachine(
            intent=intent,
            compiler=state.compiler,
            config=state_machine_config,
            on_sadflow_enter=self._on_sadflow_enter,
        )

        logger.info(
            f"Created IntentStateMachine for {strategy_id} "
            f"(intent={intent.intent_id}, max_retries={self.config.max_retries})"
        )

        from almanak.framework.observability.emitter import emit_phase_event
        from almanak.framework.observability.events import StrategyPhase

        emit_phase_event(
            strategy_id=strategy_id,
            phase=StrategyPhase.COMPILE,
            event_type="STATE_CHANGE",
            description=f"Compiling intent {intent.intent_id} ({getattr(intent, 'intent_type', 'unknown')})",
            chain=strategy.chain,
        )

        # Capture pre-execution balance snapshot for real reconciliation (VIB-3158).
        # Non-fatal: on failure we fall back to the legacy post-only mode.
        state.pre_snapshot = await self._snapshot_balances_for_intent(intent)

    @staticmethod
    def _build_single_chain_price_oracle(market: Any | None, intent: AnyIntent) -> dict | None:
        """Extract and normalize the price oracle dict from a market snapshot.

        Pre-fetches prices for tokens named by the intent that aren't already
        in the oracle. Returns ``None`` when no oracle is available or the
        oracle is empty after pre-fetch (so the compiler falls back to
        placeholder prices).
        """
        if market is None or not hasattr(market, "get_price_oracle_dict"):
            return None

        price_oracle: dict | None = market.get_price_oracle_dict()
        # Pre-fetch prices for intent tokens that aren't already in the oracle.
        # This covers two cases:
        # 1. Oracle is empty (strategy didn't call market.price() in decide())
        # 2. Oracle has some tokens but FlashLoanIntent callbacks reference
        #    additional tokens (e.g., WETH) not fetched by decide().
        if hasattr(market, "price"):
            intent_tokens = _extract_tokens_from_intent(intent)
            missing_tokens = [t for t in intent_tokens if not price_oracle or t not in price_oracle]
            if missing_tokens:
                for token in missing_tokens:
                    try:
                        market.price(token)
                    except Exception:
                        pass  # Token price unavailable, compiler will use placeholder
                price_oracle = market.get_price_oracle_dict()
                if price_oracle:
                    logger.debug(f"Pre-fetched prices for intent tokens: {list(price_oracle.keys())}")
        if price_oracle is None:
            return None
        if not price_oracle:
            # Oracle exists but empty after pre-fetch -- no usable prices
            return None
        logger.debug(f"Using real prices from market snapshot: {list(price_oracle.keys())}")
        return price_oracle

    async def _single_chain_state_machine_loop(self, state: SingleChainExecutionState) -> IterationResult | None:
        """Drive the IntentStateMachine until it reaches a terminal state.

        Handles retry delays, dry-run short-circuit, and per-step execution
        (including the pre-retry "previously-submitted tx" check, CLOB vs
        on-chain routing, receipt conversion, phase-event emission, and
        cache invalidation on failure). Returns an IterationResult only when
        the loop terminates early via dry-run; otherwise returns None and
        lets the caller inspect ``state.state_machine.success``.
        """
        state_machine = state.state_machine

        while not state_machine.is_complete:
            step_result = state_machine.step()

            # Handle retry delay from sadflow state
            if step_result.retry_delay is not None:
                logger.debug(
                    f"Retry delay: sleeping for {step_result.retry_delay:.2f}s "
                    f"(attempt {state_machine.retry_count}/{self.config.max_retries})"
                )
                await asyncio.sleep(step_result.retry_delay)
                continue

            # If we need to execute an action bundle
            if step_result.needs_execution and step_result.action_bundle:
                early = await self._single_chain_execute_step(state, step_result)
                if early is not None:
                    return early
                continue

            if step_result.error and not step_result.is_complete:
                # If execution already logged this exact error, keep this line at debug
                # to avoid duplicate warning spam in the same retry cycle.
                if state.last_execution_result and state.last_execution_result.error == step_result.error:
                    logger.debug(
                        f"Step error (already logged): {step_result.error} "
                        f"(retry {state_machine.retry_count}/{self.config.max_retries})"
                    )
                else:
                    logger.warning(
                        f"Step error: {step_result.error} (retry {state_machine.retry_count}/{self.config.max_retries})"
                    )

        return None

    async def _single_chain_execute_step(
        self, state: SingleChainExecutionState, step_result: Any
    ) -> IterationResult | None:
        """Execute one action bundle step from the state machine loop.

        Returns an IterationResult only for dry-run short-circuit; otherwise
        mutates ``state.last_execution_result`` / ``last_execution_context``
        / ``last_bundle_metadata`` and returns ``None`` so the loop advances
        to the next state-machine step.
        """
        strategy = state.strategy
        intent = state.intent
        strategy_id = state.strategy_id
        state_machine = state.state_machine
        compiler = state.compiler

        # VIB-3203: Persist this step's metadata at the moment of
        # execution so enrichment below can access ``expected_output_human``
        # even if a later no-op step is terminal.
        state.last_bundle_metadata = getattr(step_result.action_bundle, "metadata", None)

        # Dry run mode - skip actual execution
        if self.config.dry_run:
            logger.info(
                f"Dry run mode - skipping execution for {strategy_id}. "
                f"Would execute {len(step_result.action_bundle.transactions)} transactions."
            )
            if state.clob_client is not None:
                state.clob_client.close()
            if state.record_metrics:
                self._record_success()
            return IterationResult(
                status=IterationStatus.DRY_RUN,
                intent=intent,
                strategy_id=strategy_id,
                duration_ms=self._calculate_duration_ms(state.start_time),
            )

        # Execute the action bundle through orchestrator
        # Resolve protocol for result enrichment (intent is frozen, so we pass via context)
        resolved_protocol = getattr(intent, "protocol", None) or compiler.default_protocol
        from almanak.framework.observability.context import get_cycle_id

        execution_context = ExecutionContext(
            strategy_id=strategy_id,
            chain=strategy.chain,
            wallet_address=strategy.wallet_address,
            correlation_id=intent.intent_id,
            cycle_id=get_cycle_id() or "",
            protocol=resolved_protocol,
        )
        state.last_execution_context = execution_context

        try:
            # Execute through orchestrator (single-chain path)
            # Note: _is_multi_chain flag guarantees this is ExecutionOrchestrator
            # but we use cast for type checker since orchestrator is Union type
            single_chain_orch = cast(ExecutionOrchestrator, self.execution_orchestrator)

            # Pre-retry check: if previous attempt timed out and we have
            # submitted tx_hashes, check if they've since confirmed to avoid
            # duplicate swaps from retrying already-confirmed transactions.
            if await self._single_chain_pre_retry_confirmed(state, single_chain_orch):
                return None  # Treated as success; continue state-machine loop

            # Route CLOB bundles to ClobActionHandler (off-chain orders),
            # all other bundles to the on-chain ExecutionOrchestrator.
            if state.clob_handler and state.clob_handler.can_handle(step_result.action_bundle):
                execution_result = await self._single_chain_execute_clob(state, step_result)
            else:
                execution_result = await self._single_chain_execute_onchain(
                    state, step_result, execution_context, single_chain_orch
                )

            # Convert ExecutionResult to TransactionReceipt for state machine
            tx_hash = ""
            if execution_result.transaction_results:
                tx_hash = execution_result.transaction_results[0].tx_hash

            receipt = TransactionReceipt(
                success=execution_result.success,
                tx_hash=tx_hash,
                gas_used=execution_result.total_gas_used,
                error=execution_result.error,
            )

            # Set receipt for state machine validation
            state_machine.set_receipt(receipt)

            from almanak.framework.observability.emitter import emit_phase_event
            from almanak.framework.observability.events import StrategyPhase

            emit_phase_event(
                strategy_id=strategy_id,
                phase=StrategyPhase.EXECUTE,
                event_type="TRANSACTION_CONFIRMED" if execution_result.success else "TRANSACTION_FAILED",
                description=f"Execution {'succeeded' if execution_result.success else 'failed'} "
                f"(gas={execution_result.total_gas_used})",
                chain=strategy.chain,
                tx_hash=tx_hash,
                details={
                    "success": execution_result.success,
                    "gas_used": execution_result.total_gas_used,
                    "tx_count": len(execution_result.transaction_results),
                    "error": execution_result.error or "",
                },
            )

            if execution_result.success:
                logger.info(
                    f"Execution successful for {strategy_id}: "
                    f"gas_used={execution_result.total_gas_used}, "
                    f"tx_count={len(execution_result.transaction_results)}"
                )
            else:
                logger.warning(
                    f"Execution failed for {strategy_id}: {execution_result.error} "
                    f"(retry {state_machine.retry_count}/{self.config.max_retries})"
                )
                # On timeout, approvals likely succeeded -- keep cache valid.
                # On other failures, clear cache since approvals may not have
                # succeeded or may have been consumed.
                is_timeout = execution_result.error and "timeout" in execution_result.error.lower()
                if not is_timeout:
                    compiler.clear_allowance_cache()
                else:
                    logger.info("Timeout error -- preserving allowance cache for retry")
                # Reset nonce cache on failure to force fresh on-chain
                # query on retry. Prevents nonce drift. (VIB-1449)
                if hasattr(self.execution_orchestrator, "reset_nonce_cache"):
                    self.execution_orchestrator.reset_nonce_cache()

        except Exception as e:
            logger.error(f"Execution error: {e}", exc_info=True)
            # On timeout exceptions, approvals likely succeeded -- keep cache.
            is_timeout = "timeout" in str(e).lower()
            if not is_timeout:
                compiler.clear_allowance_cache()
            # Set failed receipt to trigger sadflow
            state_machine.set_receipt(
                TransactionReceipt(
                    success=False,
                    error=str(e),
                )
            )

        return None

    async def _single_chain_pre_retry_confirmed(
        self, state: SingleChainExecutionState, single_chain_orch: ExecutionOrchestrator
    ) -> bool:
        """Check whether the previous timed-out attempt has since confirmed.

        On a retry after a timeout, poll receipts for the previously-submitted
        tx hashes. If every one confirms, synthesise a success
        ``ExecutionResult`` into ``state.last_execution_result`` and push a
        success receipt into the state machine so the loop treats this as a
        success without re-submitting. Returns ``True`` when the retry was
        short-circuited, ``False`` otherwise.
        """
        state_machine = state.state_machine
        last = state.last_execution_result
        if not (
            state_machine.retry_count > 0
            and last
            and last.transaction_results
            and last.error
            and "timeout" in last.error.lower()
        ):
            return False

        prev_hashes = [tr.tx_hash for tr in last.transaction_results if tr.tx_hash]
        if not prev_hashes:
            return False

        logger.info(f"Pre-retry check: verifying {len(prev_hashes)} previously-submitted tx(es) before retrying")
        all_confirmed = True
        prev_receipts: list[FullTransactionReceipt] = []
        for prev_hash in prev_hashes:
            try:
                prev_receipt = await single_chain_orch.submitter.get_receipt(prev_hash, timeout=30.0)
                prev_receipts.append(prev_receipt)
                if prev_receipt.success:
                    logger.info(f"Previously-submitted tx {prev_hash[:10]}... confirmed")
                else:
                    logger.warning(f"Previously-submitted tx {prev_hash[:10]}... reverted")
                    all_confirmed = False
            except Exception:
                logger.warning(f"Could not get receipt for {prev_hash[:10]}..., proceeding with retry")
                all_confirmed = False

        if not (all_confirmed and prev_receipts):
            return False

        logger.info("All previously-submitted transactions confirmed -- skipping retry, treating as success")
        # Update last_execution_result so downstream consumers
        # (timeline, callbacks, IterationResult) see a successful
        # result instead of the stale timeout failure.
        # Preserve receipt data so ResultEnricher can extract
        # swap amounts, position IDs, and other enriched data.
        state.last_execution_result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            transaction_results=[
                TransactionResult(
                    tx_hash=r.tx_hash,
                    success=r.success,
                    receipt=r,
                    gas_used=r.gas_used,
                    gas_cost_wei=r.gas_cost_wei,
                    logs=r.logs,
                )
                for r in prev_receipts
            ],
            total_gas_used=sum(r.gas_used for r in prev_receipts),
            total_gas_cost_wei=sum(r.gas_cost_wei for r in prev_receipts),
            completed_at=datetime.now(UTC),
        )
        # Convert to simplified receipt for state machine
        state_machine.set_receipt(
            TransactionReceipt(
                success=True,
                tx_hash=prev_receipts[0].tx_hash,
                gas_used=sum(r.gas_used for r in prev_receipts),
            )
        )
        return True

    async def _single_chain_execute_clob(self, state: SingleChainExecutionState, step_result: Any) -> ExecutionResult:
        """Execute a Polymarket CLOB bundle via the ClobActionHandler."""
        clob_result = await state.clob_handler.execute(step_result.action_bundle)
        execution_result = ExecutionResult(
            success=clob_result.success,
            phase=ExecutionPhase.COMPLETE,
            completed_at=datetime.now(UTC),
            error=clob_result.error,
        )
        execution_result.extracted_data = {
            "clob_status": clob_result.status.value,
        }
        if clob_result.order_id:
            execution_result.extracted_data["order_id"] = clob_result.order_id
        # VIB-3218: attach PredictionFill so strategies can
        # distinguish "order accepted" from "order filled"
        # without reaching into clob_handler internals.
        # requested_size may be absent (e.g. SELL "all") --
        # skip PredictionFill if we don't have it; strategies
        # should then rely on post-execution balance reads.
        prediction_fill = clob_result.to_prediction_fill()
        if prediction_fill is not None:
            execution_result.prediction_fill = prediction_fill
        state.last_execution_result = execution_result
        return execution_result

    async def _single_chain_execute_onchain(
        self,
        state: SingleChainExecutionState,
        step_result: Any,
        execution_context: ExecutionContext,
        single_chain_orch: ExecutionOrchestrator,
    ) -> ExecutionResult:
        """Execute an on-chain bundle through the single-chain orchestrator.

        Refreshes the tx-risk config's native token price before calling
        ``single_chain_orch.execute``. Populates
        ``state.last_execution_result`` with the result.
        """
        strategy = state.strategy
        # Update native token price for USD-denominated risk guards
        # (max_value_usd, max_gas_cost_usd).
        # tx_risk_config only exists on local ExecutionOrchestrator,
        # not GatewayExecutionOrchestrator. Reset BEFORE the fetch
        # attempt so a missed/failed oracle reliably trips fail-closed
        # in the validator instead of reusing the prior cycle's price.
        tx_risk_cfg = getattr(single_chain_orch, "tx_risk_config", None)
        if tx_risk_cfg is not None and (tx_risk_cfg.max_gas_cost_usd > 0 or tx_risk_cfg.max_value_usd > 0):
            tx_risk_cfg.native_token_price_usd = 0.0
            if state.price_oracle:
                from almanak.gateway.data.balance.web3_provider import (
                    NATIVE_TOKEN_SYMBOLS,
                )

                native_symbol = NATIVE_TOKEN_SYMBOLS.get(strategy.chain.lower(), "ETH")
                native_price = state.price_oracle.get(native_symbol, 0)
                if native_price:
                    tx_risk_cfg.native_token_price_usd = float(native_price)

        # VIB-3295: emit a breadcrumb right before the execute
        # gRPC call so any hang in the orchestrator (strategy
        # process or gateway-side pipeline) leaves a visible
        # last-known-good log line. Silence here historically
        # looked indistinguishable between "still compiling"
        # and "gateway hung" in shard regressions.
        _tx_count = len(getattr(step_result.action_bundle, "transactions", []) or [])
        _intent_type = getattr(step_result.action_bundle, "intent_type", "unknown")
        logger.info(
            f"Dispatching {_intent_type} ({_tx_count} tx) to execution orchestrator "
            f"(intent={execution_context.correlation_id[:8]}..., chain={strategy.chain})"
        )
        execution_result = await single_chain_orch.execute(
            action_bundle=step_result.action_bundle,
            context=execution_context,
        )
        state.last_execution_result = execution_result
        return execution_result

    async def _single_chain_handle_success(self, state: SingleChainExecutionState) -> IterationResult:
        """Enrich, slippage-check, reconcile, and commit the success path.

        Runs ResultEnricher, then the slippage circuit breaker, then the
        post-execution balance reconciliation. Any of those may steer into
        the failure path (with its own IterationResult). On a clean path
        emits the success timeline event, writes the ledger entry, fires
        on_intent_executed(success=True), saves strategy state, and returns
        IterationStatus.SUCCESS.
        """
        strategy = state.strategy
        intent = state.intent
        strategy_id = state.strategy_id
        state_machine = state.state_machine

        # Enrich result with intent-specific extracted data
        if state.last_execution_result and state.last_execution_context:
            try:
                enricher = ResultEnricher(live_mode=self._is_live_mode())
                # VIB-3203: thread compiler bundle metadata so swap_amounts
                # extractors can compute realized slippage_bps from the
                # persisted expected_output_human quote. We use the
                # metadata snapshot captured inside the state-machine loop
                # at execution time, not the terminal step_result (which
                # may be a COMPLETE state with no action_bundle).
                state.last_execution_result = enricher.enrich(
                    state.last_execution_result,
                    intent,
                    state.last_execution_context,
                    bundle_metadata=state.last_bundle_metadata,
                )
            except CriticalAccountingError:
                # VIB-3180: receipt parse failure — re-raise so run_iteration's
                # outer except-Exception handler converts it to ACCOUNTING_FAILED.
                # Must NOT be swallowed here: a stale/missing enrichment result
                # is accounting-broken and the strategy must not continue on it.
                raise
            except Exception as e:
                logger.warning(f"Result enrichment failed: {e}")

        # Slippage circuit breaker: check actual slippage against max_slippage_bps
        slippage_early = await self._single_chain_slippage_guard(state)
        if slippage_early is not None:
            return slippage_early

        # Post-execution balance reconciliation (VIB-3158).
        # Run BEFORE we commit the iteration as a success so an incident
        # (pre/post delta outside the intent's expected range) can steer
        # the iteration into the failure path -- triggering circuit-breaker
        # recording, consecutive-error alerting, and a non-success status
        # downstream. Without this gate, operators would see a green
        # iteration summary while the strategy confidently traded on
        # corrupted accounting.
        recon = await self._reconcile_post_execution_balances(
            strategy, intent, state.last_execution_result, pre_snapshot=state.pre_snapshot
        )
        recon_incident = bool(recon and recon.get("incident"))

        if recon_incident:
            if self.config.reconciliation_enforcement:
                return await self._single_chain_handle_recon_incident(state, recon)
            # Observation mode (default until VIB-3348 block-anchored
            # balance reads land): the dual-layer balance cache produces
            # false-positive incidents on confirmed-on-chain swaps, so
            # enforcement is gated off and incidents are surfaced via logs
            # + IterationResult only. The recon dict still flows onto
            # ``balance_reconciliation`` in the success path below, so
            # dashboards and metrics keep full visibility. Flip
            # ``RunnerConfig.reconciliation_enforcement`` to True
            # per-strategy (or change the default) once the block-anchored
            # read work ships and the race is closed.
            logger.warning(
                "Reconciliation incident detected (observation mode, enforcement disabled): %s",
                self._format_reconciliation_error(recon),
            )

        # Clean reconciliation (or observation-mode pass-through) -> commit the success path.
        # Emit timeline event for successful execution
        self._emit_execution_timeline_event(strategy, intent, success=True, result=state.last_execution_result)
        # DELEVERAGE is a notable risk event — log at WARNING so operators are
        # alerted even when they are not actively monitoring DEBUG logs.
        self._maybe_warn_deleverage(intent, strategy)
        # Write structured trade record to transaction ledger (VIB-2402)
        ledger_entry_id = await self._write_ledger_entry(
            strategy, intent, result=state.last_execution_result, success=True
        )
        # VIB-3467/3478: AccountingProcessor is the sole accounting write path (dual-write
        # period ended with removal of _try_write_* methods in VIB-3478).
        if ledger_entry_id:
            await self._write_outbox_and_fire_processor(strategy, intent, ledger_entry_id)
        # VIB-3454: append one JSON line to the per-strategy sidecar file so the
        # portfolio dashboard can consume execution data without touching gateway.db.
        # Best-effort: the writer swallows all exceptions internally.
        try:
            from ..accounting.sidecar import AccountingSidecarWriter

            AccountingSidecarWriter().append(
                strategy_id=strategy.strategy_id,
                intent=intent,
                result=state.last_execution_result,
                chain=getattr(strategy, "chain", "") or getattr(self.config, "chain", ""),
            )
        except Exception:  # noqa: BLE001
            logger.warning("Sidecar import/call failed (non-blocking)", exc_info=True)
        if state.record_metrics:
            self._record_success(execution_proved=True)

        # Notify strategy of successful execution
        if hasattr(strategy, "on_intent_executed"):
            try:
                strategy.on_intent_executed(intent, success=True, result=state.last_execution_result)
            except Exception as e:
                logger.warning(f"Error in on_intent_executed callback: {e}")
        self._invoke_optional_hook(
            strategy,
            "on_copy_execution_result",
            intent,
            True,
            state.last_execution_result,
        )

        if state_machine.retry_count > 0:
            logger.info(f"Intent succeeded after {state_machine.retry_count} retries")

        # Save strategy state after successful execution
        if hasattr(strategy, "save_state"):
            try:
                strategy.save_state()
            except Exception as e:
                logger.warning(f"Error saving strategy state: {e}")

        return IterationResult(
            status=IterationStatus.SUCCESS,
            intent=intent,
            execution_result=state.last_execution_result,
            strategy_id=strategy_id,
            duration_ms=self._calculate_duration_ms(state.start_time),
            balance_reconciliation=recon,
        )

    async def _single_chain_slippage_guard(self, state: SingleChainExecutionState) -> IterationResult | None:
        """Fail the iteration when realized slippage breaches the limit.

        Returns an EXECUTION_FAILED IterationResult when the actual slippage
        exceeds the configured ``max_slippage_bps``; otherwise returns None.
        On breach: emits a failure timeline event, fires on_intent_executed
        with success=False, writes the ledger entry, and saves state.
        """
        strategy = state.strategy
        intent = state.intent
        last_execution_result = state.last_execution_result

        # tx_risk_config only exists on local ExecutionOrchestrator, not GatewayExecutionOrchestrator
        if not (last_execution_result and last_execution_result.swap_amounts):
            return None

        tx_risk_cfg = getattr(self.execution_orchestrator, "tx_risk_config", None)
        if tx_risk_cfg:
            max_slippage = tx_risk_cfg.max_slippage_bps
        else:
            intent_slippage = getattr(intent, "max_slippage", None)
            if isinstance(intent_slippage, int | float | Decimal):
                max_slippage = int(Decimal(str(intent_slippage)) * 10000)
            else:
                max_slippage = 0
        actual_slippage = last_execution_result.swap_amounts.slippage_bps
        if not (max_slippage > 0 and actual_slippage is not None and actual_slippage > max_slippage):
            return None

        slippage_error = (
            f"Slippage circuit breaker: actual slippage {actual_slippage} bps "
            f"exceeds limit {max_slippage} bps "
            f"(swap: {last_execution_result.swap_amounts.token_in} -> "
            f"{last_execution_result.swap_amounts.token_out})"
        )
        logger.error(slippage_error)

        # Attach slippage error to result FIRST so the timeline event and
        # downstream consumers (UI, operator cards, Slack alerts) see the
        # real slippage-breach reason rather than "Unknown" (issue #1649).
        last_execution_result.error = slippage_error

        # Emit timeline event for failed execution
        self._emit_execution_timeline_event(strategy, intent, success=False, result=last_execution_result)

        # Notify strategy of failure due to slippage breach so strategy
        # authors can access the error on the result.
        if hasattr(strategy, "on_intent_executed"):
            try:
                strategy.on_intent_executed(intent, success=False, result=last_execution_result)
            except Exception as e:
                logger.warning(f"Error in on_intent_executed callback: {e}")
        self._invoke_optional_hook(
            strategy,
            "on_copy_execution_result",
            intent,
            False,
            last_execution_result,
        )

        # Record slippage-breach trade in ledger (VIB-2402)
        await self._write_ledger_entry(
            strategy, intent, result=last_execution_result, success=False, error=slippage_error
        )

        # Persist state even when circuit breaker fails; on-chain state already changed.
        if hasattr(strategy, "save_state"):
            try:
                strategy.save_state()
            except Exception as e:
                logger.warning(f"Error saving strategy state: {e}")

        # Issue #1780: mirror the ``state.record_metrics`` gate used by
        # ``_single_chain_handle_success`` so a slippage-breach iteration
        # is counted in the lifetime total when this helper owns metrics
        # (single-intent). Multi-intent sequences record once at the
        # caller in ``_run_single_chain_intents`` to avoid double-count.
        if state.record_metrics:
            self._record_failure()

        return IterationResult(
            status=IterationStatus.EXECUTION_FAILED,
            intent=intent,
            execution_result=last_execution_result,
            error=slippage_error,
            strategy_id=state.strategy_id,
            duration_ms=self._calculate_duration_ms(state.start_time),
        )

    async def _single_chain_handle_recon_incident(
        self, state: SingleChainExecutionState, recon: dict[str, Any]
    ) -> IterationResult:
        """Finalize a reconciliation-failure iteration.

        Attaches the recon error to the execution result, emits a failure
        timeline event, fires on_intent_executed(success=False), writes the
        ledger entry, saves state, and dispatches an operator-facing alert.
        Returns IterationStatus.RECONCILIATION_FAILED.
        """
        strategy = state.strategy
        intent = state.intent
        last_execution_result = state.last_execution_result

        recon_error = self._format_reconciliation_error(recon)
        logger.error(
            "Reconciliation enforcement tripped for %s: %s",
            state.strategy_id,
            recon_error,
        )

        # Attach error to the execution result FIRST so the timeline
        # event and downstream consumers (alerts, operator cards,
        # ledger) see the reconciliation error rather than the stale
        # execution-level error.
        if last_execution_result is not None:
            last_execution_result.error = recon_error

        # Emit timeline event as a failure so the strategy timeline
        # reflects the accounting breach, not a clean success.
        self._emit_execution_timeline_event(strategy, intent, success=False, result=last_execution_result)

        # Notify strategy of the failed outcome so it does not treat
        # the execution as clean.
        if hasattr(strategy, "on_intent_executed"):
            try:
                strategy.on_intent_executed(intent, success=False, result=last_execution_result)
            except Exception as e:
                logger.warning(f"Error in on_intent_executed callback: {e}")
        self._invoke_optional_hook(
            strategy,
            "on_copy_execution_result",
            intent,
            False,
            last_execution_result,
        )

        # Record failed trade in ledger (VIB-2402) -- on-chain state
        # changed, but the accounting outcome is a failure.
        await self._write_ledger_entry(strategy, intent, result=last_execution_result, success=False, error=recon_error)

        # Persist strategy state even on reconciliation failure: the
        # on-chain state has already moved, so any internal bookkeeping
        # the strategy captured pre-reconciliation must not be lost.
        if hasattr(strategy, "save_state"):
            try:
                strategy.save_state()
            except Exception as e:
                logger.warning(f"Error saving strategy state: {e}")

        # Operator-facing alert on this single incident (independent
        # of the consecutive-errors alert that the outer run loop
        # fires on threshold).
        if last_execution_result is not None:
            try:
                await self._handle_execution_error(strategy, last_execution_result)
            except Exception as e:
                logger.debug("reconciliation alert dispatch failed: %s", e)

        # Issue #1780: same metrics gate as _single_chain_handle_success
        # and _single_chain_slippage_guard -- single-intent owns the
        # record_metrics flag here; multi-intent records once at the
        # caller in ``_run_single_chain_intents``.
        if state.record_metrics:
            self._record_failure()

        return IterationResult(
            status=IterationStatus.RECONCILIATION_FAILED,
            intent=intent,
            execution_result=last_execution_result,
            error=recon_error,
            strategy_id=state.strategy_id,
            duration_ms=self._calculate_duration_ms(state.start_time),
            balance_reconciliation=recon,
        )

    async def _single_chain_handle_failure(self, state: SingleChainExecutionState) -> IterationResult:
        """Finalize the state-machine-FAILED path: diagnostics, alert, result.

        Emits the failure timeline event, writes the ledger entry, runs
        revert diagnostics (only when execution was actually attempted),
        dispatches the operator alert, fires on_intent_executed with
        success=False, and returns IterationStatus.EXECUTION_FAILED.
        """
        strategy = state.strategy
        intent = state.intent
        strategy_id = state.strategy_id
        state_machine = state.state_machine
        last_execution_result = state.last_execution_result

        # State machine reached FAILED state - escalate to operator
        error_msg = state_machine.error or "Unknown error after retries exhausted"
        logger.error(f"Intent failed after {state_machine.retry_count} retries: {error_msg}")

        # Emit timeline event for failed execution
        timeline_result = last_execution_result or SimpleNamespace(error=error_msg)
        self._emit_execution_timeline_event(strategy, intent, success=False, result=timeline_result)
        # Write failed trade to transaction ledger (VIB-2402)
        await self._write_ledger_entry(strategy, intent, result=last_execution_result, success=False, error=error_msg)

        # Run revert diagnostics only for on-chain execution failures.
        # Skip when no execution was attempted (compilation failure, validation
        # error, or other pre-execution issue) where balance checks and approval
        # suggestions are irrelevant.
        execution_was_attempted = last_execution_result is not None
        if not execution_was_attempted:
            logger.error(
                f"PRE-EXECUTION FAILURE: {error_msg}\n"
                f"  Intent: {intent.intent_type.value} | Chain: {strategy.chain}\n"
                f"  No on-chain transaction was attempted (compilation or validation error)."
            )
        else:
            try:
                gas_warnings = None
                if last_execution_result is not None and hasattr(last_execution_result, "gas_warnings"):
                    gas_warnings = last_execution_result.gas_warnings or None

                diagnostic = await diagnose_revert(
                    intent=intent,
                    chain=strategy.chain,
                    wallet=strategy.wallet_address,
                    web3_provider=self.balance_provider,
                    raw_error=error_msg,
                    gas_warnings=gas_warnings,
                )
                logger.error(diagnostic.format())
            except Exception as diag_error:
                logger.warning(f"Revert diagnostic failed: {diag_error}", exc_info=True)

        # Only alert/escalate after state machine has exhausted all retries
        if last_execution_result:
            await self._handle_execution_error(strategy, last_execution_result)

        # Notify strategy of failed execution
        # Ensure the result always carries the error message so strategy authors
        # can access it via result.error or str(result) instead of getting None.
        callback_result = last_execution_result or SimpleNamespace(error=error_msg)
        if last_execution_result and not last_execution_result.error:
            last_execution_result.error = error_msg
        if hasattr(strategy, "on_intent_executed"):
            try:
                strategy.on_intent_executed(intent, success=False, result=callback_result)
            except Exception as e:
                logger.warning(f"Error in on_intent_executed callback: {e}")
        self._invoke_optional_hook(
            strategy,
            "on_copy_execution_result",
            intent,
            False,
            callback_result,
        )

        # Save strategy state after failed execution (state may have changed)
        if hasattr(strategy, "save_state"):
            try:
                strategy.save_state()
            except Exception as e:
                logger.warning(f"Error saving strategy state: {e}")

        # Issue #1780: same metrics gate as _single_chain_handle_success.
        # Single-intent iterations own metrics here; multi-intent routes
        # to the caller-side record in ``_run_single_chain_intents``.
        if state.record_metrics:
            self._record_failure()

        return IterationResult(
            status=IterationStatus.EXECUTION_FAILED,
            intent=intent,
            execution_result=last_execution_result,
            error=error_msg,
            strategy_id=strategy_id,
            duration_ms=self._calculate_duration_ms(state.start_time),
        )

    async def _check_and_resume_stuck_execution(
        self,
        strategy: StrategyProtocol,
        start_time: datetime,
    ) -> IterationResult | None:
        """Check for stuck execution and resume if found.

        This method MUST be called BEFORE decide() in multi-chain strategies.
        It prevents the bug where partial execution changes world state, causing
        decide() to return different intents (or HOLD), which then causes the
        saved progress to be discarded due to intent hash mismatch.

        Args:
            strategy: The strategy being executed
            start_time: When this iteration started

        Returns:
            IterationResult if we're resuming a stuck execution (success or failure)
            None if no stuck execution found (caller should proceed with decide())
        """
        from ..intents.vocabulary import Intent

        strategy_id = strategy.strategy_id

        # Load any saved execution progress
        saved_progress = await self._load_execution_progress(strategy_id)

        if saved_progress is None:
            # No saved progress - proceed with normal decide() flow
            return None

        if not saved_progress.is_stuck:
            # Progress exists but not stuck - this is a partial completion
            # that needs to continue. We still need to verify intents match.
            # For now, let decide() run and the hash check will handle it.
            # This handles the case where we completed some steps but haven't
            # started the next one yet (clean restart scenario).
            return None

        # We have a stuck execution - check if we can resume
        if saved_progress.serialized_intents is None:
            logger.warning(
                f"Stuck execution found for {strategy_id} but no serialized intents. "
                f"Clearing progress and starting fresh."
            )
            await self._clear_execution_progress(strategy_id)
            return None

        # Deserialize the saved intents
        try:
            intents: list[AnyIntent] = [
                Intent.deserialize(intent_data) for intent_data in saved_progress.serialized_intents
            ]
        except Exception as e:
            logger.error(
                f"Failed to deserialize saved intents for {strategy_id}: {e}. Clearing progress and starting fresh."
            )
            await self._clear_execution_progress(strategy_id)
            return None

        failed_step = saved_progress.failed_at_step_index or 0
        total_steps = saved_progress.total_steps

        logger.info(
            f"Resuming stuck execution for {strategy_id}: "
            f"retrying step {failed_step + 1}/{total_steps} "
            f"(execution_id={saved_progress.execution_id}, "
            f"error was: {saved_progress.failure_error})"
        )

        # Clear the failure state so we can retry
        saved_progress.failed_at_step_index = None
        saved_progress.failure_error = None
        saved_progress.last_updated = datetime.now(UTC)
        await self._save_execution_progress(strategy_id, saved_progress)

        # Get orchestrator (must be multi-chain since we only check stuck in multi-chain mode)
        assert isinstance(self.execution_orchestrator, MultiChainOrchestrator)
        orchestrator = self.execution_orchestrator

        # Execute with the saved intents, resuming from the failed step
        return await self._execute_with_bridge_waiting(
            strategy=strategy,
            intents=intents,
            orchestrator=orchestrator,
            start_time=start_time,
            resume_progress=saved_progress,
        )

    def _check_teardown_requested(
        self,
        strategy: StrategyProtocol,
    ) -> "TeardownMode | None":
        """Check if teardown is requested and return the mode.

        Pure check with no side effects -- does NOT generate intents or inject
        compilers. Intent generation is handled in run_iteration after creating
        a market snapshot, so teardown follows the same data flow as decide().

        Args:
            strategy: The strategy to check for teardown

        Returns:
            TeardownMode if teardown is requested and supported, None otherwise
        """
        strategy_id = strategy.strategy_id

        # Check if strategy has teardown support (graceful degradation)
        if not hasattr(strategy, "should_teardown"):
            return None

        # Check if teardown is requested
        try:
            should_teardown = strategy.should_teardown()
        except Exception as e:
            logger.warning(f"Error checking teardown status for {strategy_id}: {e}")
            return None

        if not should_teardown:
            return None

        # Acknowledge teardown request
        if hasattr(strategy, "acknowledge_teardown_request"):
            try:
                strategy.acknowledge_teardown_request()
                logger.info(f"Acknowledged teardown request for {strategy_id}")
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Failed to acknowledge teardown request: {e}")

        # Import TeardownMode here to avoid circular imports
        from ..teardown import TeardownMode, get_teardown_state_manager

        # Get the requested teardown mode from the request
        manager = get_teardown_state_manager()
        request = manager.get_active_request(strategy_id)
        mode = request.mode if request else TeardownMode.SOFT

        logger.info(f"Teardown requested for {strategy_id} (mode={mode.value})")
        return mode

    async def _execute_multi_chain(
        self,
        strategy: StrategyProtocol,
        intents: list[AnyIntent],
        start_time: datetime,
        market: Any = None,
    ) -> IterationResult:
        """Execute intents through the multi-chain orchestrator with bridge waiting.

        For multi-chain strategies, this method handles:
        - Routing intents to the correct chain
        - Sequential execution with amount chaining
        - **Bridge completion waiting for cross-chain swaps**
        - Per-chain error isolation

        Cross-chain swaps (where destination_chain != chain) will wait for
        the bridge transfer to complete before proceeding to the next step.
        Same-chain operations proceed immediately.

        Args:
            strategy: The strategy being executed
            intents: List of intents to execute sequentially
            start_time: When the iteration started
            market: Optional market snapshot for price data during compilation

        Returns:
            IterationResult with execution details
        """
        strategy_id = strategy.strategy_id

        # Type assertion for multi-chain orchestrator
        assert isinstance(self.execution_orchestrator, MultiChainOrchestrator)
        orchestrator = self.execution_orchestrator

        # Detect chains involved and if any cross-chain intents exist
        chains_involved = set()
        has_cross_chain = False
        for intent in intents:
            chain = getattr(intent, "chain", None) or orchestrator.primary_chain
            chains_involved.add(chain)
            dest_chain = get_intent_destination_chain(intent)
            if dest_chain:
                chains_involved.add(dest_chain)
            if is_cross_chain_intent(intent):
                has_cross_chain = True

        # Extract real prices from market snapshot for accurate slippage calculations
        price_oracle = None
        price_map = None
        if market is not None and hasattr(market, "get_price_oracle_dict"):
            price_oracle = market.get_price_oracle_dict()
            # Pre-fetch prices for intent tokens missing from the oracle.
            # MultiChainMarketSnapshot.price() requires chain=, so we derive
            # the chain from each intent to avoid TypeError.
            if hasattr(market, "price"):
                fetched_any = False
                for i in intents:
                    intent_chain = getattr(i, "chain", None) or orchestrator.primary_chain
                    for token in _extract_tokens_from_intent(i):
                        if not price_oracle or token not in price_oracle:
                            try:
                                market.price(token, chain=intent_chain)
                                fetched_any = True
                            except Exception as e:
                                logger.warning(f"Failed to pre-fetch price for {token} on {intent_chain}: {e}")
                if fetched_any:
                    price_oracle = market.get_price_oracle_dict()
            if price_oracle:
                price_map = {k: str(v) for k, v in price_oracle.items()}
                logger.debug(f"Multi-chain: using real prices for {list(price_oracle.keys())}")
            else:
                price_oracle = None

        logger.info(
            f"Multi-chain execution for {strategy_id}: "
            f"{len(intents)} intents across {chains_involved}, "
            f"has_cross_chain={has_cross_chain}"
        )

        # Dry run mode
        if self.config.dry_run:
            logger.info(f"Dry run mode - skipping execution for {strategy_id}. Would execute {len(intents)} intents.")
            self._record_success()
            return IterationResult(
                status=IterationStatus.DRY_RUN,
                intent=intents[0] if intents else None,
                strategy_id=strategy_id,
                duration_ms=self._calculate_duration_ms(start_time),
            )

        first_intent = intents[0] if intents else None

        # If there are cross-chain intents, use PlanExecutor with bridge waiting
        if has_cross_chain:
            return await self._execute_with_bridge_waiting(
                strategy=strategy,
                intents=intents,
                orchestrator=orchestrator,
                start_time=start_time,
                price_map=price_map,
                price_oracle=price_oracle,
            )

        # For same-chain only flows, use direct execute_sequence (faster)
        multi_result = await orchestrator.execute_sequence(intents, price_map=price_map, price_oracle=price_oracle)

        # Always invalidate balance cache after execution (success or failure)
        self.balance_provider.invalidate_cache()

        if multi_result.success:
            logger.info(
                f"Multi-chain execution successful for {strategy_id}: "
                f"{multi_result.successful_count}/{len(intents)} succeeded, "
                f"chains={list(multi_result.chains_used)}, "
                f"time={multi_result.total_execution_time_ms:.0f}ms"
            )

            self._record_success(execution_proved=True)
            return IterationResult(
                status=IterationStatus.SUCCESS,
                intent=first_intent,
                strategy_id=strategy_id,
                duration_ms=self._calculate_duration_ms(start_time),
            )
        else:
            # Aggregate errors from all chains
            error_msgs = []
            for chain, errors in multi_result.errors_by_chain.items():
                error_msgs.extend([f"[{chain}] {e}" for e in errors])
            error_summary = "; ".join(error_msgs) if error_msgs else "Unknown error"

            logger.error(
                f"Multi-chain execution failed for {strategy_id}: "
                f"{multi_result.failed_count}/{len(intents)} failed: {error_summary}"
            )

            # Issue #1780: mirror the ``_record_success`` call on the
            # success branch above (line ~3080) so the failed multi-chain
            # iteration ticks the lifetime counter exactly once.
            self._record_failure()
            return IterationResult(
                status=IterationStatus.EXECUTION_FAILED,
                intent=first_intent,
                error=error_summary,
                strategy_id=strategy_id,
                duration_ms=self._calculate_duration_ms(start_time),
            )

    async def _execute_with_bridge_waiting(
        self,
        strategy: StrategyProtocol,
        intents: list[AnyIntent],
        orchestrator: MultiChainOrchestrator,
        start_time: datetime,
        resume_progress: ExecutionProgress | None = None,
        price_map: dict[str, str] | None = None,
        price_oracle: dict | None = None,
    ) -> IterationResult:
        """Execute intents with bridge completion waiting for cross-chain swaps.

        This method executes intents sequentially, but waits for bridge
        transfers to complete before proceeding to the next step.

        IMPORTANT: For cross-chain swaps, we explicitly verify the source TX
        was confirmed on-chain and didn't revert BEFORE starting to poll
        the destination chain for the bridged assets.

        Phase 3c: This is now a thin driver that sets up a ``BridgeWaitState``
        and threads it through per-intent and per-phase step helpers. The
        original sequential loop, source-TX verification, and bridge-polling
        logic all live in those helpers with identical behaviour.

        Args:
            strategy: The strategy being executed
            intents: List of intents to execute
            orchestrator: Multi-chain orchestrator
            start_time: When the iteration started
            resume_progress: If provided, resume from this progress (for stuck execution retry)

        Returns:
            IterationResult with execution details
        """
        state = BridgeWaitState(
            strategy=strategy,
            intents=intents,
            orchestrator=orchestrator,
            start_time=start_time,
            resume_progress=resume_progress,
            price_map=price_map,
            price_oracle=price_oracle,
            strategy_id=strategy.strategy_id,
            first_intent=intents[0] if intents else None,
        )

        await self._init_bridge_wait_state(state)

        # Walk each intent; each iteration either succeeds, sets
        # state.failed_step and breaks, or continues to the next intent.
        # Pre-execution RuntimeErrors (e.g. missing gateway client guard in
        # ``_bridge_wait_process_intent``) still propagate here unchanged --
        # nothing has been submitted on-chain, so escaping is safe and the
        # outer iteration error handler will turn it into a clean failure.
        # Post-submission config defects are materialised INSIDE
        # ``_bridge_wait_cross_chain`` so ``_bridge_wait_finalize`` always
        # runs and ``progress.failed_at_step_index`` is persisted.
        for i, intent in enumerate(intents):
            state.current_intent = intent
            should_break = await self._bridge_wait_process_intent(state, i)
            if should_break:
                break

        return await self._bridge_wait_finalize(state)

    # -------------------------------------------------------------------------
    # _execute_with_bridge_waiting step helpers (Phase 3c)
    # -------------------------------------------------------------------------

    async def _init_bridge_wait_state(self, state: BridgeWaitState) -> None:
        """Populate state_provider, progress, and starting step index.

        Resolves wallet address, RPC URLs, gateway client, and
        EnsoStateProvider. Determines the ``start_step_index`` and
        ``previous_amount_received`` from either ``resume_progress`` (stuck
        retry) or ``_load_execution_progress`` (restart resume). If no saved
        progress matches the current intents hash, starts fresh and persists
        the initial progress so stuck-execution recovery has serialized
        intents to work with.
        """
        import uuid

        orchestrator = state.orchestrator
        intents = state.intents
        strategy_id = state.strategy_id

        # Get wallet address from orchestrator (works for both config and gateway modes)
        state.wallet_address = orchestrator.wallet_address

        # Get RPC URLs for EnsoStateProvider - gateway mode doesn't have _config
        if hasattr(orchestrator, "_config") and orchestrator._config is not None:
            state.rpc_urls = orchestrator._config.rpc_urls
        else:
            state.rpc_urls = {}

        # Create state provider for bridge tracking
        # In gateway mode, pass gateway_client so it can use gateway RPC instead of direct Web3
        state.gateway_client = self._get_gateway_client()
        state.state_provider = EnsoStateProvider(
            rpc_urls=state.rpc_urls,
            wallet_address=state.wallet_address,
            gateway_client=state.gateway_client,
        )

        # Determine execution progress
        if state.resume_progress is not None:
            # Resuming from a stuck execution (passed from _check_and_resume_stuck_execution)
            state.start_step_index = state.resume_progress.next_step_to_execute
            state.previous_amount_received = state.resume_progress.previous_amount_received
            state.progress = state.resume_progress
            logger.info(
                f"Resuming stuck execution from step {state.start_step_index + 1}/{len(intents)} "
                f"(execution_id={state.progress.execution_id})"
            )
        else:
            # Check for saved execution progress (resumption after restart)
            intents_hash = self._compute_intents_hash(intents)
            saved_progress = await self._load_execution_progress(strategy_id)

            if saved_progress and saved_progress.intents_hash == intents_hash:
                # Resume from last completed step
                state.start_step_index = saved_progress.next_step_to_execute
                state.previous_amount_received = saved_progress.previous_amount_received
                logger.info(
                    f"Resuming execution from step {state.start_step_index + 1}/{len(intents)} "
                    f"(execution_id={saved_progress.execution_id})"
                )
                state.progress = saved_progress
            else:
                # Start fresh execution
                if saved_progress:
                    logger.info("Intents changed (hash mismatch), starting fresh execution")
                    await self._clear_execution_progress(strategy_id)

                # Serialize intents for stuck execution recovery
                serialized_intents = [intent.serialize() for intent in intents]

                state.progress = ExecutionProgress(
                    execution_id=str(uuid.uuid4())[:8],
                    strategy_id=strategy_id,
                    intents_hash=intents_hash,
                    total_steps=len(intents),
                    serialized_intents=serialized_intents,
                )
                # Save initial progress with serialized intents
                await self._save_execution_progress(strategy_id, state.progress)

        logger.info(
            f"Executing {len(intents)} intents with bridge waiting for {strategy_id} "
            f"(starting from step {state.start_step_index + 1})"
        )

        # Start the successful-count at whatever was already completed so the
        # final summary line reports the full count, not just newly-executed
        # steps.
        state.successful_count = state.start_step_index

    async def _bridge_wait_process_intent(self, state: BridgeWaitState, i: int) -> bool:
        """Execute one intent + optional bridge wait. Returns True to break.

        Mirrors the per-iteration body of the original for-loop: skip already-
        completed steps, log, resolve amount="all", validate cross-chain
        metadata, execute the intent, verify source TX + poll bridge
        completion if cross-chain, then persist progress. Any failure records
        the failure on ``state`` (``failed_step``, ``error_message``,
        ``failed_result``, ``callback_fired``) and returns True so the caller
        breaks out of the loop.
        """
        strategy = state.strategy
        intents = state.intents
        intent = intents[i]
        orchestrator = state.orchestrator
        strategy_id = state.strategy_id

        # Skip already-completed steps when resuming
        if i < state.start_step_index:
            logger.debug(f"Skipping already-completed step {i + 1}")
            return False

        step_num = i + 1
        intent_type = intent.intent_type.value
        chain = getattr(intent, "chain", None) or orchestrator.primary_chain
        is_cross_chain = is_cross_chain_intent(intent)

        logger.info(
            f"Step {step_num}/{len(intents)}: {intent_type} on {chain}" + (" (cross-chain)" if is_cross_chain else "")
        )

        # Resolve amount="all" if needed
        intent_to_execute = intent
        if Intent.has_chained_amount(intent) and state.previous_amount_received is not None:
            logger.info(f"Resolving amount='all' to {state.previous_amount_received}")
            intent_to_execute = Intent.set_resolved_amount(intent, state.previous_amount_received)

        # Get expected output for cross-chain tracking (before execution)
        dest_chain: str | None = None
        token_symbol: str | None = None

        if is_cross_chain:
            # Gateway-only boundary (fix #1647): cross-chain bridge source-TX
            # verification runs exclusively through the gateway's
            # GetTransactionStatus RPC. A missing gateway client is a
            # configuration defect; fail-fast BEFORE submitting the source
            # transaction so we never leave funds broadcast on-chain with no
            # way to verify them. See
            # ``blueprints/20-gateway-security-architecture.md``.
            if state.gateway_client is None:
                raise RuntimeError(
                    "Gateway client required for cross-chain bridge source-TX verification; "
                    "direct Web3 fallback is forbidden by gateway-only architecture. "
                    "See blueprints/20-gateway-security-architecture.md"
                )

            dest_chain = get_intent_destination_chain(intent)
            token_symbol = get_intent_destination_token(intent)
            # Defense-in-depth (VIB-3223): a cross-chain intent with no
            # resolvable destination chain/token is the exact failure mode
            # VIB-3223 fixed -- fail loudly instead of silently skipping.
            if not dest_chain or not token_symbol:
                logger.error(
                    f"Step {step_num}: cross-chain intent missing destination fields "
                    f"(dest_chain={dest_chain!r}, token_symbol={token_symbol!r}). "
                    f"Cannot track bridge completion."
                )
                state.failed_step = f"step-{step_num}"
                state.error_message = (
                    "Cross-chain intent missing destination_chain/to_chain or "
                    "to_token/token field; cannot wait for bridge completion."
                )
                return True

        # Execute the intent
        try:
            result = await orchestrator.execute(
                intent_to_execute, price_map=state.price_map, price_oracle=state.price_oracle
            )
        except Exception as e:
            logger.error(f"Step {step_num} execution failed: {e}")
            # Notify strategy of failed execution (mirrors _execute_single_chain)
            if hasattr(strategy, "on_intent_executed"):
                try:
                    strategy.on_intent_executed(intent, success=False, result=None)
                except Exception as cb_err:
                    logger.warning(f"Error in on_intent_executed callback: {cb_err}")
            state.callback_fired = True
            state.failed_step = f"step-{step_num}"
            state.error_message = str(e)
            return True

        if not result.success:
            logger.error(f"Step {step_num} failed: {result.error}")
            # Notify strategy of failed execution (mirrors _execute_single_chain)
            if hasattr(strategy, "on_intent_executed"):
                try:
                    strategy.on_intent_executed(intent, success=False, result=result)
                except Exception as cb_err:
                    logger.warning(f"Error in on_intent_executed callback: {cb_err}")
            state.callback_fired = True
            state.failed_result = result
            state.failed_step = f"step-{step_num}"
            state.error_message = result.error
            return True

        state.successful_count += 1

        # Track amount received for chaining
        if result.tx_result and hasattr(result.tx_result, "actual_amount_received"):
            state.previous_amount_received = result.tx_result.actual_amount_received
        else:
            # Fallback to intent amount
            amount_field = Intent.get_amount_field(intent_to_execute)
            if amount_field is not None and isinstance(amount_field, Decimal):
                state.previous_amount_received = amount_field

        # For cross-chain swaps, verify source TX and wait for bridge completion.
        #
        # Any config-defect exception that escapes ``_bridge_wait_cross_chain``
        # (RuntimeError from the gateway precheck, permanent gRPC codes from
        # the verify loop, proto ImportError, AttributeError/TypeError from a
        # miswired stub) is POST-SUBMISSION: ``orchestrator.execute`` above
        # has already broadcast the source transaction. If we let the
        # exception escape, ``_bridge_wait_finalize`` would never run and
        # ``progress.failed_at_step_index`` would never be persisted. The
        # next iteration would have no failure marker and could re-decide /
        # re-execute the same cross-chain step, risking duplicate source-TX
        # submissions. Materialise such failures into bridge failure state
        # and break so finalize runs. See PR #1676 review feedback.
        if is_cross_chain and dest_chain and token_symbol:
            try:
                bridge_break = await self._bridge_wait_cross_chain(
                    state,
                    result=result,
                    step_num=step_num,
                    chain=chain,
                    dest_chain=dest_chain,
                    token_symbol=token_symbol,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Step %s: post-submission failure while waiting for bridge "
                    "completion on %s -> %s (token=%s). Materialising as bridge "
                    "failure state so progress is persisted.",
                    step_num,
                    chain,
                    dest_chain,
                    token_symbol,
                )
                error_message = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
                # Use the ``-bridge`` suffix so ``_bridge_wait_build_failed_result``
                # classifies this as a bridge failure (skips revert diagnostics,
                # logs the BRIDGE FAILURE banner) rather than treating it like a
                # plain execution revert. The source tx already succeeded; what
                # failed was the cross-chain wait.
                state.failed_step = f"step-{step_num}-bridge"
                state.error_message = error_message
                # Propagate the error onto the result so downstream consumers
                # (e.g. on_intent_executed callbacks, telemetry) see the real
                # post-submission failure instead of an empty ``result.error``.
                if hasattr(result, "error"):
                    result.error = error_message
                state.failed_result = result
                return True
            if bridge_break:
                return True

        # Notify strategy of successful execution (mirrors _execute_single_chain lines 2459-2478)
        if hasattr(strategy, "on_intent_executed"):
            try:
                strategy.on_intent_executed(intent, success=True, result=result)
            except Exception as e:
                logger.warning(f"Error in on_intent_executed callback: {e}")

        # Save strategy state after successful execution
        if hasattr(strategy, "save_state"):
            try:
                strategy.save_state()
            except Exception as e:
                logger.warning(f"Error saving strategy state: {e}")

        # Save progress after each step completes successfully.
        # progress is always populated by ``_init_bridge_wait_state`` before
        # any step helper runs; the assert narrows the type for mypy.
        assert state.progress is not None
        state.progress.completed_step_index = i
        state.progress.previous_amount_received = state.previous_amount_received
        await self._save_execution_progress(strategy_id, state.progress)
        logger.info(f"Step {step_num}/{len(intents)} completed, progress saved")

        return False

    async def _bridge_wait_cross_chain(
        self,
        state: BridgeWaitState,
        *,
        result: Any,
        step_num: int,
        chain: str,
        dest_chain: str,
        token_symbol: str,
    ) -> bool:
        """Verify source TX + poll bridge for a cross-chain step. True breaks.

        Extracts the tx hash, verifies the source TX confirmed on-chain via
        the gateway ``GetTransactionStatus`` RPC, and then delegates to
        ``_bridge_wait_poll_completion`` for the destination-chain balance
        polling + amount normalization. Any failure mutates ``state`` and
        returns True so the outer loop breaks.
        """
        # Get tx hash from result
        tx_hash = None
        if result.tx_result:
            tx_hash = getattr(result.tx_result, "tx_hash", None)

        if not tx_hash:
            logger.error(f"Step {step_num}: No tx_hash in result, cannot track bridge")
            state.failed_step = f"step-{step_num}"
            state.error_message = "No transaction hash returned from execution"
            return True

        # Normalize tx_hash to include 0x prefix (some execution paths return bare hex)
        if not tx_hash.startswith("0x"):
            tx_hash = f"0x{tx_hash}"

        verified = await self._bridge_wait_verify_source_tx(state, tx_hash=tx_hash, chain=chain, step_num=step_num)
        if not verified:
            return True

        # Source TX confirmed - now wait for bridge completion
        logger.info(f"Waiting for bridge completion: {chain} -> {dest_chain}, token={token_symbol}")
        return await self._bridge_wait_poll_completion(
            state,
            result=result,
            tx_hash=tx_hash,
            chain=chain,
            dest_chain=dest_chain,
            token_symbol=token_symbol,
            step_num=step_num,
        )

    async def _bridge_wait_verify_source_tx(
        self, state: BridgeWaitState, *, tx_hash: str, chain: str, step_num: int
    ) -> bool:
        """Poll until the source TX is confirmed (or failed/timed out).

        Uses the gateway ``GetTransactionStatus`` RPC exclusively. On a
        terminal failed status (reverted/failed/invalid) or the 30-attempt
        timeout, mutates ``state.failed_step`` / ``error_message`` and
        returns False. Returns True when the TX is confirmed.

        Raises:
            RuntimeError: If ``state.gateway_client`` is None, or if the
                client is miswired (missing ``execution`` attribute or a
                non-callable ``GetTransactionStatus``). Direct Web3 fallback
                is forbidden by the gateway-only architecture (see
                ``blueprints/20-gateway-security-architecture.md``). This
                must fail loud so misconfigured hosted deployments do not
                silently fall back to an egress path that has no secrets,
                rate limits, or auth, and so shape defects surface
                immediately instead of after a 60-second retry timeout.
        """
        # Gateway-only boundary: no direct Web3 fallback. If the gateway
        # client is missing at this point, something is misconfigured and we
        # must fail loudly rather than opening an unmediated egress path.
        if state.gateway_client is None:
            raise RuntimeError(
                "Gateway client required for bridge source-TX verification; "
                "direct Web3 fallback is forbidden by gateway-only architecture. "
                "See blueprints/20-gateway-security-architecture.md"
            )

        # Pre-validate the gateway client shape BEFORE entering the retry loop.
        # A miswired client (wrong stub bound, missing ``execution`` attribute,
        # ``GetTransactionStatus`` signature wrong) is a config defect, not a
        # transient RPC error. Without this precheck, ``AttributeError`` /
        # ``TypeError`` raised inside the loop would be swallowed by the
        # per-attempt ``except`` and surface only as a 60-second timeout
        # instead of an immediate loud failure. See issue #1666.
        execution_stub = getattr(state.gateway_client, "execution", None)
        if execution_stub is None:
            raise RuntimeError(
                "Gateway client is miswired: missing ``execution`` attribute. "
                "Cannot call GetTransactionStatus for bridge source-TX verification."
            )
        if not callable(getattr(execution_stub, "GetTransactionStatus", None)):
            raise RuntimeError(
                "Gateway client is miswired: ``execution.GetTransactionStatus`` is "
                "missing or not callable. Cannot verify bridge source TX."
            )
        # Import the request proto once, before the loop - an ImportError here
        # is also a config defect, not a transient error. Convert ImportError
        # into the same fail-fast ``RuntimeError`` contract the rest of this
        # precheck enforces so a missing/renamed proto module surfaces with a
        # clear operator-facing message rather than a raw ``ImportError``.
        try:
            from almanak.gateway.proto import gateway_pb2
        except ImportError as exc:
            raise RuntimeError(
                "Gateway client is miswired: failed to import "
                "almanak.gateway.proto.gateway_pb2. Cannot verify bridge "
                "source TX."
            ) from exc

        # Also validate that TxStatusRequest is wired correctly. If the proto
        # module loads but the message class was renamed/removed, we want the
        # RuntimeError to surface here, not as a raw AttributeError on the
        # first poll attempt. See PR #1676 review feedback.
        tx_status_request_cls = getattr(gateway_pb2, "TxStatusRequest", None)
        if not callable(tx_status_request_cls):
            raise RuntimeError(
                "Gateway client is miswired: gateway_pb2.TxStatusRequest is "
                "missing or not callable. Cannot verify bridge source TX."
            )

        # CRITICAL: Verify source TX actually succeeded on-chain before polling destination
        # This prevents polling for bridged assets when the source TX reverted
        logger.info(f"Verifying source TX confirmation on {chain}: {tx_hash}")

        try:
            tx_verified = False

            for attempt in range(30):  # Max 30 attempts, ~1 minute
                try:
                    status_response = state.gateway_client.execution.GetTransactionStatus(
                        tx_status_request_cls(tx_hash=tx_hash, chain=chain),
                        timeout=15.0,
                    )
                    if status_response.status == "confirmed":
                        logger.info(
                            f"Source TX confirmed successfully on {chain}: {tx_hash}, "
                            f"block={status_response.block_number}"
                        )
                        tx_verified = True
                        break
                    elif status_response.status in ("failed", "reverted", "invalid"):
                        logger.error(f"Step {step_num}: Source TX {status_response.status} on {chain}: {tx_hash}")
                        state.failed_step = f"step-{step_num}"
                        state.error_message = f"Transaction {status_response.status} on {chain}: {tx_hash}"
                        break
                except grpc.RpcError as exc:
                    # Only TRANSIENT gRPC status codes are worth retrying.
                    # Permanent codes (UNAUTHENTICATED, PERMISSION_DENIED,
                    # INVALID_ARGUMENT, UNIMPLEMENTED, ...) indicate a config
                    # or auth defect and must propagate so they surface
                    # immediately, not after a 60-second silent retry loop.
                    # Non-RpcError exceptions (AttributeError / TypeError /
                    # ImportError) are config defects and already propagate
                    # because they do not match this except clause.
                    # See PR #1676 review feedback.
                    #
                    # ``code()`` is only defined on concrete gRPC status
                    # exceptions (``_InactiveRpcError`` etc.); bare
                    # ``grpc.RpcError`` subclasses can omit it. When the code
                    # is unknown we retry rather than crash, matching the
                    # pre-change "retry all RpcError" behaviour for the
                    # unknown-code edge case.
                    exc_code: grpc.StatusCode | None = None
                    code_fn = getattr(exc, "code", None)
                    if callable(code_fn):
                        try:
                            exc_code = code_fn()
                        except Exception:  # noqa: BLE001 - defensive: code() should never raise
                            exc_code = None
                    if exc_code is not None and exc_code not in _TRANSIENT_GRPC_CODES:
                        raise
                    logger.debug(
                        "GetTransactionStatus attempt %s failed for %s on %s (code=%s): %s",
                        attempt + 1,
                        tx_hash,
                        chain,
                        exc_code,
                        exc,
                    )
                await asyncio.sleep(2)

            if state.failed_step:
                return False

            if not tx_verified:
                logger.error(f"Step {step_num}: Could not get receipt for {tx_hash}")
                state.failed_step = f"step-{step_num}"
                state.error_message = f"Timeout waiting for transaction receipt: {tx_hash}"
                return False

            return True

        except grpc.RpcError as e:
            # Only swallow gRPC transport errors here. This catches:
            #  * Transient RPC errors re-raised after 30 attempts (timeout).
            #  * Permanent RPC codes (UNAUTHENTICATED / PERMISSION_DENIED /
            #    INVALID_ARGUMENT / ...) re-raised on the first attempt.
            # Both are materialised into ``failed_step`` / ``error_message`` so
            # ``_bridge_wait_finalize`` persists progress and fires the
            # failure callback.
            # Config defects (AttributeError / TypeError / ImportError /
            # RuntimeError from the precheck) are NOT caught here. They
            # propagate to ``_bridge_wait_process_intent`` where the
            # post-submission guard around ``_bridge_wait_cross_chain``
            # materialises them into bridge failure state so
            # ``_bridge_wait_finalize`` runs and progress is persisted. See
            # issue #1666 and PR #1676 review feedback.
            logger.error(f"Step {step_num}: Error verifying source TX: {e}")
            state.failed_step = f"step-{step_num}"
            state.error_message = f"Failed to verify source transaction: {e}"
            return False

    async def _bridge_wait_poll_completion(
        self,
        state: BridgeWaitState,
        *,
        result: Any,
        tx_hash: str,
        chain: str,
        dest_chain: str,
        token_symbol: str,
        step_num: int,
    ) -> bool:
        """Register + poll the bridge, normalize the received amount.

        Returns True when the caller must break out of the intent loop
        (bridge failed, timed out, or the destination-token metadata cannot
        be resolved for amount normalization). Returns False on successful
        completion (``state.previous_amount_received`` updated so the next
        intent can chain the received amount). Failure paths set
        ``state.failed_step`` / ``error_message`` and fire the strategy
        callback so the finalization block doesn't double-fire it.
        """
        strategy = state.strategy
        intent = state.current_intent

        # Register and wait for bridge transfer
        # expected_amount=0 means accept any positive balance increase
        deposit_id = state.state_provider.register_bridge_transfer(
            source_chain=chain,
            destination_chain=dest_chain,
            source_tx_hash=tx_hash,
            token_symbol=token_symbol,
            expected_amount=0,
        )

        try:
            bridge_status = await state.state_provider.wait_for_bridge_completion(
                deposit_id=deposit_id,
                timeout_seconds=300,  # 5 minute timeout
                poll_interval_seconds=10,
            )

            if bridge_status["status"] == "completed":
                return await self._bridge_wait_apply_completion(
                    state,
                    result=result,
                    bridge_status=bridge_status,
                    dest_chain=dest_chain,
                    token_symbol=token_symbol,
                    step_num=step_num,
                )

            logger.error(f"Bridge failed: {bridge_status}")
            # Notify strategy of bridge failure (source tx succeeded but bridge failed)
            if hasattr(strategy, "on_intent_executed"):
                try:
                    strategy.on_intent_executed(intent, success=False, result=result)
                except Exception as cb_err:
                    logger.warning(f"Error in on_intent_executed callback: {cb_err}")
            state.callback_fired = True
            state.failed_step = f"step-{step_num}-bridge"
            state.error_message = f"Bridge transfer failed: {bridge_status.get('error', 'Unknown')}"
            return True

        except TimeoutError as e:
            logger.error(f"Bridge timeout: {e}")
            # Notify strategy of bridge timeout (source tx succeeded but bridge timed out)
            if hasattr(strategy, "on_intent_executed"):
                try:
                    strategy.on_intent_executed(intent, success=False, result=result)
                except Exception as cb_err:
                    logger.warning(f"Error in on_intent_executed callback: {cb_err}")
            state.callback_fired = True
            state.failed_step = f"step-{step_num}-bridge"
            state.error_message = "Bridge transfer timed out after 5 minutes"
            return True

        except Exception as e:
            # Any non-timeout exception from wait_for_bridge_completion (connection errors,
            # protocol errors, malformed responses, etc.) must still drive the failure
            # pipeline: strategy callback, state.callback_fired, and ultimately the
            # timeline failure event via _bridge_wait_finalize. Without this branch the
            # exception would propagate up, the strategy would never be notified, and the
            # orchestrator view of the in-flight bridge would diverge from reality.
            # Note: `except Exception` intentionally does not catch KeyboardInterrupt /
            # SystemExit (those inherit from BaseException).
            logger.error(
                "Bridge wait failed with %s: %s",
                type(e).__name__,
                e,
                exc_info=True,
            )
            if hasattr(strategy, "on_intent_executed"):
                try:
                    strategy.on_intent_executed(intent, success=False, result=result)
                except Exception as cb_err:
                    logger.warning(f"Error in on_intent_executed callback: {cb_err}")
            state.callback_fired = True
            state.failed_step = f"step-{step_num}-bridge"
            state.error_message = f"Bridge wait failed ({type(e).__name__}): {e}"
            return True

    async def _bridge_wait_apply_completion(
        self,
        state: BridgeWaitState,
        *,
        result: Any,
        bridge_status: dict[str, Any],
        dest_chain: str,
        token_symbol: str,
        step_num: int,
    ) -> bool:
        """Handle a "completed" bridge status: normalize + chain amount.

        Normalizes the wei balance increase to a human-readable Decimal via
        ``_normalize_bridge_balance_increase``. On ``TokenNotFoundError``,
        fails the step and fires the strategy callback (returning True so
        the outer loop breaks). On success, updates
        ``state.previous_amount_received`` so the next intent can chain the
        received amount. When normalization returns ``None`` (token decimals
        not resolvable), logs a warning and leaves
        ``previous_amount_received`` untouched -- matching the pre-refactor
        behaviour.
        """
        strategy = state.strategy
        intent = state.current_intent

        # Update amount received with actual bridge output
        # Balance increase is in wei - normalize using TokenResolver metadata
        actual_received_wei = bridge_status.get("balance_increase")
        if actual_received_wei is None:
            return False

        from ..data.tokens.exceptions import TokenNotFoundError

        try:
            normalized_amount, normalization_metadata = self._normalize_bridge_balance_increase(
                balance_increase_wei=actual_received_wei,
                destination_chain=dest_chain,
                token_symbol=token_symbol,
                bridge_status=bridge_status,
            )
        except TokenNotFoundError as exc:
            logger.error(
                "Bridge normalization failed due to unresolved token metadata: %s",
                exc,
            )
            # Notify strategy of bridge failure (source tx succeeded but bridge normalization failed)
            if hasattr(strategy, "on_intent_executed"):
                try:
                    strategy.on_intent_executed(intent, success=False, result=result)
                except Exception as cb_err:
                    logger.warning(f"Error in on_intent_executed callback: {cb_err}")
            state.callback_fired = True
            state.failed_step = f"step-{step_num}-bridge"
            state.error_message = str(exc)
            return True

        if normalized_amount is not None:
            state.previous_amount_received = normalized_amount
            logger.info(
                "Bridge completed: received %s %s on %s (%s wei, decimals=%s, token_hint=%s)",
                state.previous_amount_received,
                token_symbol,
                dest_chain,
                normalization_metadata["raw_wei"],
                normalization_metadata["decimals"],
                normalization_metadata.get("resolved_from"),
            )
        else:
            logger.warning(
                "Unable to normalize bridge amount. Preserving raw wei metadata: %s",
                normalization_metadata,
            )
        return False

    async def _bridge_wait_finalize(self, state: BridgeWaitState) -> IterationResult:
        """Build the final IterationResult after the intent loop terminates.

        Handles: callback-dispatch for failure exits that did not fire the
        callback inline, progress persistence on failure, revert diagnostics
        for on-chain failures (skipping bridge + pre-execution failures),
        balance-cache invalidation, and the SUCCESS path (clear progress,
        record success metric).
        """
        strategy = state.strategy
        strategy_id = state.strategy_id
        intents = state.intents

        # Ensure strategy is notified of failure even for paths that didn't fire the callback
        # inline (e.g. source TX verification failures, no-tx_hash, no-RPC-URL).
        # This single finalization block covers all break exits without per-exit patching.
        if state.failed_step and not state.callback_fired:
            if hasattr(strategy, "on_intent_executed"):
                try:
                    strategy.on_intent_executed(state.current_intent, success=False, result=state.failed_result)
                except Exception as cb_err:
                    logger.warning(f"Error in on_intent_executed callback: {cb_err}")

        # Build result
        if state.failed_step:
            return await self._bridge_wait_build_failed_result(state)

        # Always invalidate balance cache after execution (success or failure)
        # to prevent stale reads on the next decide() cycle.
        self.balance_provider.invalidate_cache()

        logger.info(
            f"Multi-chain execution with bridge waiting successful for {strategy_id}: "
            f"{state.successful_count}/{len(intents)} succeeded"
        )

        # Clear execution progress on successful completion
        await self._clear_execution_progress(strategy_id)

        self._record_success(execution_proved=True)
        return IterationResult(
            status=IterationStatus.SUCCESS,
            intent=state.first_intent,
            strategy_id=strategy_id,
            duration_ms=self._calculate_duration_ms(state.start_time),
        )

    async def _bridge_wait_build_failed_result(self, state: BridgeWaitState) -> IterationResult:
        """Persist failure progress, run diagnostics, return failed result."""
        strategy = state.strategy
        strategy_id = state.strategy_id
        intents = state.intents
        # Precondition: callers only invoke this when state.failed_step is set
        # and state.progress has been populated by ``_init_bridge_wait_state``.
        assert state.failed_step is not None
        assert state.progress is not None
        failed_step = state.failed_step
        error_message = state.error_message

        logger.error(f"Multi-chain execution failed at {failed_step}: {error_message}")

        # Mark the failed step in progress so we can retry on next iteration
        # Parse failed step index from "step-N" or "step-N-bridge" format
        try:
            step_part = failed_step.split("-")[1]
            failed_intent_index = int(step_part) - 1  # Convert to 0-indexed
        except (IndexError, ValueError):
            failed_intent_index = 0

        # Save failure state for retry on next iteration
        state.progress.failed_at_step_index = failed_intent_index
        state.progress.failure_error = error_message
        state.progress.last_updated = datetime.now(UTC)
        await self._save_execution_progress(strategy_id, state.progress)
        logger.info(f"Saved failure state for retry: step {failed_intent_index + 1}, error: {error_message}")

        # Run diagnostics on the failed intent to help identify the cause
        try:
            if 0 <= failed_intent_index < len(intents):
                failed_intent = intents[failed_intent_index]
                failed_chain = getattr(failed_intent, "chain", strategy.chain)

                # Create a chain-specific balance provider for diagnostics
                from almanak.gateway.data.balance import Web3BalanceProvider

                chain_rpc = state.rpc_urls.get(failed_chain)
                if chain_rpc:
                    chain_balance_provider = Web3BalanceProvider(
                        rpc_url=chain_rpc,
                        wallet_address=strategy.wallet_address,
                        chain=failed_chain,
                    )

                    # Skip revert diagnostics when no execution result is available.
                    # This covers compilation failures AND bridge failures (where the
                    # execution itself succeeded but the bridge transfer failed).
                    is_bridge_failure = "-bridge" in (failed_step or "")
                    if state.failed_result is None and not is_bridge_failure:
                        logger.error(
                            f"PRE-EXECUTION FAILURE: {error_message}\n"
                            f"  Intent: {failed_intent.intent_type.value} | Chain: {failed_chain}\n"
                            f"  No on-chain transaction was attempted (compilation or validation error)."
                        )
                    elif is_bridge_failure:
                        logger.error(
                            f"BRIDGE FAILURE: {error_message}\n"
                            f"  Intent: {failed_intent.intent_type.value} | Chain: {failed_chain}\n"
                            f"  The on-chain transaction succeeded but the bridge transfer failed."
                        )
                    else:
                        cross_chain_gas_warnings = None
                        if state.failed_result is not None and hasattr(state.failed_result, "gas_warnings"):
                            cross_chain_gas_warnings = state.failed_result.gas_warnings or None

                        diagnostic = await diagnose_revert(
                            intent=failed_intent,
                            chain=failed_chain,
                            wallet=strategy.wallet_address,
                            web3_provider=chain_balance_provider,
                            raw_error=error_message,
                            gas_warnings=cross_chain_gas_warnings,
                        )
                        logger.error(diagnostic.format())
        except Exception as diag_error:
            logger.warning(f"Revert diagnostic failed: {diag_error}", exc_info=True)

        # Always invalidate balance cache after execution (success or failure)
        # to prevent stale reads on the next decide() cycle.
        self.balance_provider.invalidate_cache()

        # Issue #1780: the bridge-wait failed result is the terminal
        # outcome of a cross-chain iteration -- record it exactly once
        # here so the lifetime total matches the success branch that
        # ``_record_success`` handles at the end of the happy path.
        self._record_failure()
        return IterationResult(
            status=IterationStatus.EXECUTION_FAILED,
            intent=state.first_intent,
            error=f"{failed_step}: {error_message}",
            strategy_id=strategy_id,
            duration_ms=self._calculate_duration_ms(state.start_time),
        )

    # -------------------------------------------------------------------------
    # Teardown execution (delegated to runner_teardown.py)
    # -------------------------------------------------------------------------

    async def _execute_teardown(
        self,
        strategy: StrategyProtocol,
        teardown_mode: "TeardownMode",
        start_time: datetime,
    ) -> IterationResult:
        from .runner_teardown import execute_teardown

        return await execute_teardown(self, strategy, teardown_mode, start_time)

    async def _execute_teardown_via_manager(
        self, strategy, teardown_intents, teardown_mode, teardown_market, start_time, request, state_manager
    ):
        from .runner_teardown import execute_teardown_via_manager

        return await execute_teardown_via_manager(
            self, strategy, teardown_intents, teardown_mode, teardown_market, start_time, request, state_manager
        )

    async def _execute_teardown_inline(
        self, strategy, teardown_intents, teardown_market, start_time, request, state_manager
    ):
        from .runner_teardown import execute_teardown_inline

        return await execute_teardown_inline(
            self, strategy, teardown_intents, teardown_market, start_time, request, state_manager
        )

    def _build_teardown_compiler(self, strategy, market):
        from .runner_teardown import build_teardown_compiler

        return build_teardown_compiler(self, strategy, market)

    @staticmethod
    def _prefetch_teardown_prices(market, intents):
        from .runner_teardown import prefetch_teardown_prices

        prefetch_teardown_prices(market, intents)

    @staticmethod
    def _get_fallback_teardown_prices(market):
        from .runner_teardown import get_fallback_teardown_prices

        return get_fallback_teardown_prices(market)

    def _inject_simulated_balances(self, market, strategy):
        from .runner_teardown import inject_simulated_balances

        inject_simulated_balances(self, market, strategy)

    async def _pre_warm_prices(self, market, strategy) -> None:
        """Pre-warm the market snapshot's price cache before decide().

        On cold Anvil forks, gateway price fetches can take 15-30s each.
        By fetching prices BEFORE the decide() timeout starts, the
        strategy's market.price() calls hit cache instead of the gateway.

        Uses the strategy's _get_tracked_tokens() to discover which tokens
        the strategy needs. Failures are silently ignored — decide() will
        still try to fetch prices if pre-warming misses or fails.

        The entire pre-warm phase is capped at 60s to prevent stalled
        gateway calls from blocking the iteration indefinitely.
        """
        try:
            await asyncio.wait_for(self._do_pre_warm_prices(market, strategy), timeout=60.0)
        except TimeoutError:
            logger.warning("Price pre-warming timed out after 60s — proceeding to decide()")
        except Exception as e:
            logger.debug(f"Price pre-warming failed: {e}")

    async def _do_pre_warm_prices(self, market, strategy) -> None:
        """Inner implementation of price pre-warming (called with a timeout wrapper)."""
        tokens: list[str] = []
        if hasattr(strategy, "_get_tracked_tokens"):
            try:
                tokens = strategy._get_tracked_tokens()
            except Exception as e:
                logger.debug(f"Failed to get tracked tokens for pre-warming: {e}")

        if not tokens:
            return

        logger.debug(f"Pre-warming price cache for {len(tokens)} tokens: {tokens}")
        # Sequential iteration is intentional — _price_cache is not thread-safe
        for token in tokens:
            try:
                await asyncio.to_thread(market.price, token)
            except Exception as e:
                logger.debug(f"Price pre-warm failed for {token}: {e}")

    @staticmethod
    def _bridge_token_resolution_candidates(token_symbol, bridge_status):
        from .runner_teardown import bridge_token_resolution_candidates

        return bridge_token_resolution_candidates(token_symbol, bridge_status)

    @staticmethod
    def _normalize_bridge_balance_increase(balance_increase_wei, destination_chain, token_symbol, bridge_status):
        from .runner_teardown import normalize_bridge_balance_increase

        return normalize_bridge_balance_increase(balance_increase_wei, destination_chain, token_symbol, bridge_status)

    def _create_error_result(
        self,
        strategy_id: str,
        status: IterationStatus,
        error: str,
        start_time: datetime,
        intent: AnyIntent | None = None,
    ) -> IterationResult:
        """Create an error ``IterationResult`` and bump the total-iteration
        counter.

        Ownership contract (fix for issue #1771):

        * ``_total_iterations`` is incremented here. This is ONE of three
          sites that tick the lifetime counter on a failure path; the
          others are:

          - ``_run_single_chain_intents`` (multi-intent sequence failure,
            see note at ``strategy_runner.py:~1196``) which counts the
            iteration once for the whole sequence.
          - Some single-intent failure paths that return an
            ``IterationResult`` directly (e.g. inline results built in
            ``_execute_single_chain`` / ``_execute_multi_chain``) do NOT
            currently increment the counter -- consolidating those is
            tracked as a follow-up; do not widen the contract here
            unannounced.

          The success path ticks ``_total_iterations`` via
          ``_record_success`` / ``runner_state.record_success``.
        * ``_consecutive_errors`` is NOT incremented here. Every result
          this helper builds flows back to ``run_loop`` which calls
          ``_run_loop_helpers.handle_iteration_failure`` for any result
          with ``not result.success``. That helper is the single owner
          of the consecutive-error streak counter. Incrementing in both
          places (the pre-refactor behavior) double-counted every
          failure that went through both sites and pushed the
          ``max_consecutive_errors`` alarm threshold by one iteration.
        """
        self._total_iterations += 1

        return IterationResult(
            status=status,
            intent=intent,
            error=error,
            strategy_id=strategy_id,
            duration_ms=self._calculate_duration_ms(start_time),
        )

    async def _reconcile_post_execution_balances(self, strategy, intent, execution_result, pre_snapshot=None):
        from .runner_state import reconcile_post_execution_balances

        return await reconcile_post_execution_balances(
            self, strategy, intent, execution_result, pre_snapshot=pre_snapshot
        )

    @staticmethod
    def _format_reconciliation_error(recon: dict | None) -> str:
        """Compact one-line summary of reconciliation mismatches for logs/alerts."""
        if not recon:
            return "Balance reconciliation incident (no detail)"
        mismatches = recon.get("mismatches") or []
        if not mismatches:
            return "Balance reconciliation incident (no mismatch detail)"
        parts = []
        for m in mismatches:
            token = m.get("token", "?")
            actual = m.get("actual", "?")
            expected_min = m.get("expected_min", "?")
            expected_max = m.get("expected_max", "?")
            parts.append(f"{token} delta={actual} expected=[{expected_min},{expected_max}]")
        return "Balance reconciliation incident: " + "; ".join(parts)

    async def _snapshot_balances_for_intent(self, intent):
        from .runner_state import snapshot_balances_for_intent

        return await snapshot_balances_for_intent(self, intent)

    @staticmethod
    def _extract_intent_tokens(intent):
        from .runner_state import extract_intent_tokens

        return extract_intent_tokens(intent)

    def _record_success(self, *, execution_proved: bool = False) -> None:
        from .runner_state import record_success

        record_success(self, execution_proved=execution_proved)

    def _record_failure(self) -> None:
        """Thin proxy to ``runner_state.record_failure`` (issue #1780).

        Use on any failure path that builds an ``IterationResult``
        directly instead of going through ``_create_error_result``. See
        ``record_failure`` for the ownership contract.
        """
        from .runner_state import record_failure

        record_failure(self)

    def _calculate_duration_ms(self, start_time: datetime) -> float:
        from .runner_state import calculate_duration_ms

        return calculate_duration_ms(self, start_time)

    async def _detect_stuck_and_alert(self, strategy, result):
        from .runner_state import detect_stuck_and_alert

        await detect_stuck_and_alert(self, strategy, result)

    def _emit_iteration_summary(self, result, chain=None):
        from .runner_state import emit_iteration_summary

        emit_iteration_summary(self, result, chain)

    async def _is_strategy_paused(self, strategy_id):
        from .runner_state import is_strategy_paused

        return await is_strategy_paused(self, strategy_id)

    async def _update_state(self, strategy_id, result, strategy=None):
        from .runner_state import update_state

        await update_state(self, strategy_id, result, strategy)

    async def _persist_copy_trading_state(self, strategy_id, activity_provider):
        from .runner_state import persist_copy_trading_state

        await persist_copy_trading_state(self, strategy_id, activity_provider)

    async def _persist_vault_state(self, strategy_id, vault_state_dict, vault_state_key):
        from .runner_state import persist_vault_state

        await persist_vault_state(self, strategy_id, vault_state_dict, vault_state_key)

    async def _capture_portfolio_snapshot(self, strategy, iteration_number):
        from .runner_state import capture_portfolio_snapshot

        # Pass trade flag to force snapshot on trade iterations (bypass throttle).
        # Only clear the flag after successful persistence so a transient
        # snapshot failure doesn't lose the forced-snapshot opportunity.
        force = self._iteration_had_trade
        result = await capture_portfolio_snapshot(self, strategy, iteration_number, force_snapshot=force)
        if result is not None:
            self._iteration_had_trade = False
        return result

    async def _update_portfolio_metrics(self, strategy_id, snapshot):
        from .runner_state import update_portfolio_metrics

        await update_portfolio_metrics(self, strategy_id, snapshot)

    async def _handle_execution_error(
        self,
        strategy: StrategyProtocol,
        execution_result: ExecutionResult,
    ) -> None:
        """Handle execution errors with alerting.

        When an OperatorCardGenerator is configured, generates rich cards with
        auto-detected StuckReason, computed severity, and suggested actions.
        Falls back to a basic card when no generator is available.
        """
        if not self.config.enable_alerting or not self.alert_manager:
            return

        try:
            exec_total_value, exec_available = self._query_portfolio_value(strategy)
            if self._operator_card_generator is not None:
                from ..services.operator_card_generator import ErrorContext, StrategyState

                error_ctx = ErrorContext(
                    error_type=type(execution_result).__name__,
                    error_message=execution_result.error or "Unknown execution error",
                    gas_used=execution_result.total_gas_used,
                    revert_reason=getattr(execution_result, "revert_reason", None),
                )
                strategy_state = StrategyState(
                    strategy_id=strategy.strategy_id,
                    status="error",
                    total_value_usd=exec_total_value,
                    available_balance_usd=exec_available,
                    stuck_since=self._first_error_at,
                    last_successful_action=None,
                )
                card = self._operator_card_generator.generate_card(
                    strategy_state=strategy_state,
                    error_context=error_ctx,
                    event_type=EventType.ERROR,
                )
            else:
                card = OperatorCard(
                    strategy_id=strategy.strategy_id,
                    timestamp=datetime.now(UTC),
                    event_type=EventType.ERROR,
                    reason=StuckReason.TRANSACTION_REVERTED,
                    context={
                        "phase": execution_result.phase.value if execution_result.phase else "unknown",
                        "error": execution_result.error or "Unknown error",
                        "gas_used": execution_result.total_gas_used,
                    },
                    severity=Severity.HIGH,
                    position_summary=PositionSummary(
                        total_value_usd=exec_total_value,
                        available_balance_usd=exec_available,
                    ),
                    risk_description="Strategy execution failed - positions may be at risk",
                    suggested_actions=[
                        SuggestedAction(
                            action=AvailableAction.RESUME,
                            description="Resume to retry the failed transaction",
                            priority=1,
                            is_recommended=True,
                        ),
                    ],
                    available_actions=[AvailableAction.RESUME, AvailableAction.PAUSE],
                )

            await self.alert_manager.send_alert(card)

        except Exception as e:
            logger.error(f"Failed to send execution error alert: {e}")

    async def _alert_accounting_failure(
        self,
        strategy: StrategyProtocol,
        error: Exception,
    ) -> None:
        """Send a CRITICAL operator alert for accounting persistence failure.

        The on-chain state changed but the durable accounting write did not
        succeed. This is a book-keeping emergency -- paused strategy and
        manual reconciliation are required before resuming. Severity is
        CRITICAL rather than HIGH because silent accounting loss is
        irrecoverable once alerting is missed.
        """
        if not self.config.enable_alerting or not self.alert_manager:
            return

        try:
            total_value, available = self._query_portfolio_value(strategy)
            write_kind = getattr(error, "write_kind", "unknown")
            card = OperatorCard(
                strategy_id=strategy.strategy_id,
                timestamp=datetime.now(UTC),
                event_type=EventType.ERROR,
                reason=StuckReason.UNKNOWN,
                context={
                    "accounting_write_kind": write_kind,
                    "error": str(error),
                },
                severity=Severity.CRITICAL,
                position_summary=PositionSummary(
                    total_value_usd=total_value,
                    available_balance_usd=available,
                ),
                risk_description=(
                    f"Accounting persistence failed ({write_kind}). On-chain state may have "
                    "changed without a durable ledger/snapshot/metrics record. Manual "
                    "reconciliation required before resuming."
                ),
                suggested_actions=[
                    SuggestedAction(
                        action=AvailableAction.PAUSE,
                        description="Pause strategy and investigate accounting backend",
                        priority=1,
                        is_recommended=True,
                    ),
                ],
                available_actions=[AvailableAction.PAUSE, AvailableAction.RESUME],
            )
            await self.alert_manager.send_alert(card)
        except Exception as alert_err:  # noqa: BLE001
            logger.error("Failed to send accounting failure alert: %s", alert_err)

    async def _alert_enrichment_failure(
        self,
        strategy: StrategyProtocol,
        error: "CriticalAccountingError",
    ) -> None:
        """Send a CRITICAL operator alert for receipt-enrichment failure.

        The on-chain transaction succeeded but the framework cannot reliably
        parse what happened — position IDs, swap amounts, and other enriched
        fields are unavailable. Strategies that depend on these fields may
        enter a ghost-position state. Manual reconciliation is required.

        Distinct from ``_alert_accounting_failure`` (which covers ledger /
        snapshot / metrics write failures) so monitoring rules can route the
        two failure classes to the appropriate on-call rotation and runbook.
        """
        if not self.config.enable_alerting or not self.alert_manager:
            return

        try:
            total_value, available = self._query_portfolio_value(strategy)
            card = OperatorCard(
                strategy_id=strategy.strategy_id,
                timestamp=datetime.now(UTC),
                event_type=EventType.ERROR,
                reason=StuckReason.UNKNOWN,
                context={
                    "accounting_write_kind": "enrichment",
                    "field_name": error.field_name or "unknown",
                    "intent_type": error.intent_type or "unknown",
                    "protocol": error.protocol or "unknown",
                    "error": str(error),
                },
                severity=Severity.CRITICAL,
                position_summary=PositionSummary(
                    total_value_usd=total_value,
                    available_balance_usd=available,
                ),
                risk_description=(
                    f"Receipt enrichment failed (field={error.field_name}, "
                    f"intent={error.intent_type}, protocol={error.protocol}). "
                    "On-chain state changed but framework cannot parse the outcome — "
                    "ghost-position risk. Manual reconciliation required before resuming."
                ),
                suggested_actions=[
                    SuggestedAction(
                        action=AvailableAction.PAUSE,
                        description="Pause strategy and reconcile on-chain state with strategy state",
                        priority=1,
                        is_recommended=True,
                    ),
                ],
                available_actions=[AvailableAction.PAUSE, AvailableAction.RESUME],
            )
            await self.alert_manager.send_alert(card)
        except Exception as alert_err:  # noqa: BLE001
            logger.error("Failed to send enrichment failure alert: %s", alert_err)

    async def _alert_consecutive_errors(
        self,
        strategy: StrategyProtocol,
        last_result: IterationResult,
    ) -> None:
        """Send alert for consecutive errors threshold breach.

        When StuckDetector and OperatorCardGenerator are configured, produces
        intelligent failure classification with root-cause analysis and
        actionable remediation steps. Falls back to a basic card otherwise.
        """
        if not self.config.enable_alerting or not self.alert_manager:
            return

        try:
            consec_total_value, consec_available = self._query_portfolio_value(strategy)
            if self._operator_card_generator is not None:
                from ..services.operator_card_generator import ErrorContext, StrategyState

                # Build ErrorContext from the last iteration result
                error_ctx = ErrorContext(
                    error_type=last_result.status.value,
                    error_message=last_result.error or "Unknown error",
                )

                # Build StrategyState with what we know from the runner
                strategy_state = StrategyState(
                    strategy_id=strategy.strategy_id,
                    status="stuck" if self._consecutive_errors >= self.config.max_consecutive_errors else "error",
                    total_value_usd=consec_total_value,
                    available_balance_usd=consec_available,
                    stuck_since=self._first_error_at,
                    last_successful_action=None,
                )

                # Use StuckDetector for intelligent classification if available
                stuck_reason = None
                if self._stuck_detector is not None:
                    from ..execution.circuit_breaker import CircuitBreakerState
                    from ..services.stuck_detector import StrategySnapshot

                    snapshot = StrategySnapshot(
                        strategy_id=strategy.strategy_id,
                        chain=getattr(strategy, "chain", "unknown"),
                        current_state=last_result.status.value,
                        state_entered_at=self._first_error_at or datetime.now(UTC),
                        pending_transactions=[],
                        circuit_breaker_triggered=(
                            self._circuit_breaker is not None
                            and self._circuit_breaker.state == CircuitBreakerState.OPEN
                        ),
                        rpc_healthy="rpc" not in (last_result.error or "").lower(),
                        last_rpc_error=(
                            last_result.error if last_result.error and "rpc" in last_result.error.lower() else None
                        ),
                    )
                    detection = self._stuck_detector.detect_stuck(snapshot)
                    if detection.is_stuck and detection.reason:
                        stuck_reason = detection.reason
                        logger.info(
                            "StuckDetector classified %s as %s",
                            strategy.strategy_id,
                            stuck_reason.value,
                        )

                # Generate rich card via OperatorCardGenerator
                event_type = EventType.STUCK if stuck_reason else EventType.WARNING
                card = self._operator_card_generator.generate_card(
                    strategy_state=strategy_state,
                    error_context=error_ctx,
                    event_type=event_type,
                )
            else:
                # Fallback: basic card without intelligent classification
                card = OperatorCard(
                    strategy_id=strategy.strategy_id,
                    timestamp=datetime.now(UTC),
                    event_type=EventType.WARNING,
                    reason=StuckReason.UNKNOWN,
                    context={
                        "consecutive_errors": self._consecutive_errors,
                        "max_allowed": self.config.max_consecutive_errors,
                        "last_error": last_result.error or "Unknown",
                        "last_status": last_result.status.value,
                    },
                    severity=Severity.MEDIUM,
                    position_summary=PositionSummary(
                        total_value_usd=consec_total_value,
                        available_balance_usd=consec_available,
                    ),
                    risk_description=(f"Strategy has failed {self._consecutive_errors} consecutive times"),
                    suggested_actions=[
                        SuggestedAction(
                            action=AvailableAction.PAUSE,
                            description="Pause strategy to review error logs",
                            priority=1,
                            is_recommended=True,
                        ),
                    ],
                    available_actions=[AvailableAction.PAUSE, AvailableAction.RESUME],
                )

            await self.alert_manager.send_alert(card)

        except Exception as e:
            logger.error(f"Failed to send consecutive errors alert: {e}")

    async def _maybe_trigger_emergency(
        self,
        strategy: StrategyProtocol,
        last_result: IterationResult,
    ) -> None:
        """Trigger emergency stop if the circuit breaker just tripped to OPEN.

        Called after every failure recording. Only fires once per OPEN transition
        by tracking whether we've already triggered for this OPEN state via
        the _emergency_triggered_for_open flag.
        """
        if self._emergency_manager is None or self._circuit_breaker is None:
            return

        # Only trigger when breaker is OPEN
        from ..execution.circuit_breaker import CircuitBreakerState

        if self._circuit_breaker.state != CircuitBreakerState.OPEN:
            self._emergency_triggered_for_open = False
            return

        # Don't trigger more than once per OPEN episode
        if self._emergency_triggered_for_open:
            return

        try:
            cb_check = self._circuit_breaker.check()
            reason = (
                f"Circuit breaker tripped after {cb_check.consecutive_failures} "
                f"consecutive failures: {last_result.error or 'unknown error'}"
            )
            logger.warning(
                "EMERGENCY: triggering emergency stop for %s — %s",
                strategy.strategy_id,
                reason,
            )
            await self._emergency_manager.emergency_stop_async(
                strategy_id=strategy.strategy_id,
                reason=reason,
                chain=getattr(strategy, "chain", ""),
                trigger_context={
                    "consecutive_failures": cb_check.consecutive_failures,
                    "cumulative_loss_usd": str(cb_check.cumulative_loss_usd),
                    "last_status": last_result.status.value,
                    "last_error": last_result.error,
                },
            )
            # Only mark as triggered after successful emergency stop
            self._emergency_triggered_for_open = True

            # In managed deployments, write ERROR state and exit so the pod
            # terminates and K8s resources are freed.  Local development keeps
            # the loop alive for debugging.
            if self._is_managed_deployment():
                self._terminal_lifecycle_state = "ERROR"
                self._terminal_lifecycle_error_message = f"Circuit breaker tripped: {last_result.error or 'unknown'}"
                self._lifecycle_write_state(
                    strategy.strategy_id,
                    "ERROR",
                    error_message=self._terminal_lifecycle_error_message,
                )
                logger.critical("Circuit breaker tripped in managed deployment — exiting process")
                self.request_shutdown()
        except Exception as e:
            logger.error(f"Failed to trigger emergency stop for {strategy.strategy_id}: {e}")

    def get_metrics(self):
        from .runner_state import get_metrics

        return get_metrics(self)

    async def _recover_incomplete_sessions(self):
        from .runner_recovery import recover_incomplete_sessions

        return await recover_incomplete_sessions(self)

    async def _recover_session(self, session):
        from .runner_recovery import recover_session

        return await recover_session(self, session)

    async def _recover_submitted_session(self, session):
        from .runner_recovery import recover_submitted_session

        return await recover_submitted_session(self, session)

    async def _recover_early_phase_session(self, session):
        from .runner_recovery import recover_early_phase_session

        return await recover_early_phase_session(self, session)

    async def _update_recovered_state(self, session):
        from .runner_recovery import update_recovered_state

        await update_recovered_state(self, session)

    def is_duplicate_transaction(self, tx_hash=None, nonce=None, strategy_id=None):
        from .runner_recovery import is_duplicate_transaction

        return is_duplicate_transaction(self, tx_hash, nonce, strategy_id)

    # =========================================================================
    # Execution Progress Management (for resuming after restart)
    # =========================================================================

    def _compute_intents_hash(self, intents):
        from .runner_recovery import compute_intents_hash

        return compute_intents_hash(self, intents)

    async def _load_execution_progress(self, strategy_id):
        from .runner_recovery import load_execution_progress

        return await load_execution_progress(self, strategy_id)

    async def _save_execution_progress(self, strategy_id, progress):
        from .runner_recovery import save_execution_progress

        await save_execution_progress(self, strategy_id, progress)

    async def _clear_execution_progress(self, strategy_id):
        from .runner_recovery import clear_execution_progress

        await clear_execution_progress(self, strategy_id)


__all__ = [
    "StrategyRunner",
    "RunnerConfig",
    "IterationResult",
    "IterationStatus",
    "StrategyProtocol",
    "ExecutionProgress",
]
