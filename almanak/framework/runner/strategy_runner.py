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
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

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
from ..state.state_manager import StateManager
from ..utils.log_formatters import (
    _emojis_enabled,
)
from ..utils.logging import add_context, clear_context
from ..valuation.portfolio_valuer import PortfolioValuer

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

        # Phase 4: cycle_id preserved for snapshot capture after iteration
        self._last_cycle_id: str = ""

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

        try:
            # Step 0: Honor operator pause before any strategy logic/execution.
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
                    duration_ms=self._calculate_duration_ms(start_time),
                )

            # Strategy resumed: clear pause log marker.
            self._logged_paused_strategy_ids.discard(strategy_id)

            # Step 0a: Check for teardown early — needed to gate circuit breaker
            # Called once here and reused at Step 0.5 to avoid double-invocation
            # (acknowledge_teardown_request has side effects).
            teardown_mode = self._check_teardown_requested(strategy)

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
                    return IterationResult(
                        status=IterationStatus.CIRCUIT_BREAKER_OPEN,
                        error=cb_result.reason,
                        strategy_id=strategy_id,
                        duration_ms=self._calculate_duration_ms(start_time),
                    )

            # Step 0c: Check for stuck execution that needs resumption (multi-chain only)
            # This MUST happen before decide() to prevent lost progress when state changes
            if self._is_multi_chain:
                stuck_result = await self._check_and_resume_stuck_execution(
                    strategy=strategy,
                    start_time=start_time,
                )
                if stuck_result is not None:
                    return stuck_result

            # Step 0.5: Check for teardown request (reuses result from Step 0a)
            # If teardown is requested, intercept the iteration and execute teardown.
            # Single-chain teardowns route through TeardownManager for full safety
            # (loss caps, escalating slippage, cancel window, post-execution verification).
            # Multi-chain teardowns use the inline path until TeardownManager supports it.
            if teardown_mode is not None:
                return await self._execute_teardown(strategy, teardown_mode, start_time)

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
                    # Always persist vault state, even if callback or settlement fails
                    if self.config.enable_state_persistence:
                        try:
                            vault_state_dict = self._vault_lifecycle.get_vault_state_dict()
                            if vault_state_dict is not None:
                                await self._persist_vault_state(strategy_id, vault_state_dict, VAULT_STATE_KEY)
                        except Exception as persist_err:
                            logger.warning("Failed to persist vault state: %s", persist_err)

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
                    start_time,
                )

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
                            f"Resetting decide guard after {elapsed:.1f}s "
                            f"(timeout was {decide_timeout}s) for {strategy_id}"
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

            # Step 4: Handle HOLD or no intent
            if not intents or (len(intents) == 1 and isinstance(intents[0], HoldIntent)):
                hold_intent = intents[0] if intents else None
                reason = hold_intent.reason if isinstance(hold_intent, HoldIntent) else "No action"
                hold_prefix = "⏸️" if _emojis_enabled() else "[HOLD]"
                logger.info(f"{hold_prefix} {strategy_id} HOLD: {reason}")
                self._record_success()
                return IterationResult(
                    status=IterationStatus.HOLD,
                    intent=hold_intent,
                    strategy_id=strategy_id,
                    duration_ms=self._calculate_duration_ms(start_time),
                )

            # Step 5: Log intent(s) with detailed information
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

            # Step 5.5: Circuit breaker gate — block execution if breaker is open
            if self._circuit_breaker is not None:
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
                    return IterationResult(
                        status=IterationStatus.CIRCUIT_BREAKER_OPEN,
                        intent=intents[0] if intents else None,
                        error=f"Circuit breaker open: {cb_check.reason}",
                        strategy_id=strategy_id,
                        duration_ms=self._calculate_duration_ms(start_time),
                    )

            # Step 5.9: Snapshot balances before execution for delta tracking
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

            # Step 6: Execute based on orchestrator type
            if self._is_multi_chain:
                # Multi-chain execution path
                return await self._execute_multi_chain(
                    strategy=strategy,
                    intents=intents,
                    start_time=start_time,
                    market=market,
                )
            else:
                # Single-chain execution path
                # Execute all intents sequentially, stopping on first failure
                if len(intents) > 1:
                    logger.info(f"Executing {len(intents)} intents sequentially for {strategy.strategy_id}")

                intent_result: IterationResult | None = None
                is_multi_intent = len(intents) > 1
                previous_amount_received: Decimal | None = None
                for idx, intent in enumerate(intents):
                    # Resolve amount="all" from previous step's output or wallet balance
                    intent_to_execute = intent
                    if Intent.has_chained_amount(intent):
                        if is_multi_intent and previous_amount_received is not None:
                            # Multi-intent chain: resolve from previous step output
                            logger.info(
                                f"  Resolving amount='all' to {previous_amount_received} "
                                f"for intent {idx + 1}/{len(intents)}"
                            )
                            intent_to_execute = Intent.set_resolved_amount(intent, previous_amount_received)
                        elif is_multi_intent and previous_amount_received is None and idx > 0:
                            # Multi-intent but no previous output (dry-run or error)
                            if self.config.dry_run:
                                logger.warning(
                                    f"  Intent {idx + 1}/{len(intents)} uses amount='all' "
                                    "but no previous step output available (dry-run mode). "
                                    "Skipping compilation of this step."
                                )
                                intent_result = IterationResult(
                                    status=IterationStatus.DRY_RUN,
                                    intent=intent,
                                    strategy_id=strategy.strategy_id,
                                    duration_ms=self._calculate_duration_ms(start_time),
                                )
                                continue
                            else:
                                logger.error(
                                    f"  Intent {idx + 1}/{len(intents)} uses amount='all' "
                                    "but no previous step amount available"
                                )
                                intent_result = IterationResult(
                                    status=IterationStatus.COMPILATION_FAILED,
                                    intent=intent,
                                    error="amount='all' used but no previous step amount available",
                                    strategy_id=strategy.strategy_id,
                                    duration_ms=self._calculate_duration_ms(start_time),
                                )
                                break
                        else:
                            # Single intent or first intent in multi-sequence:
                            # resolve amount='all' from wallet balance for wallet-funded intents.
                            # Protocol-position intents (withdraw, repay, unstake) use amount='all'
                            # to mean "all from the protocol position" — let the compiler handle those.
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
                            else:
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
                                            intent_result = IterationResult(
                                                status=IterationStatus.COMPILATION_FAILED,
                                                intent=intent,
                                                error=f"amount='all' for {balance_token} but balance is 0",
                                                strategy_id=strategy.strategy_id,
                                                duration_ms=self._calculate_duration_ms(start_time),
                                            )
                                            break
                                        intent_to_execute = Intent.set_resolved_amount(intent, balance_value)
                                        logger.info(
                                            f"  Resolved amount='all' for {balance_token} from wallet: {balance_value}"
                                        )
                                    except Exception as e:  # noqa: BLE001
                                        logger.error(f"  Failed to resolve amount='all' for {balance_token}: {e}")
                                        intent_result = IterationResult(
                                            status=IterationStatus.COMPILATION_FAILED,
                                            intent=intent,
                                            error=f"Cannot resolve amount='all' for {balance_token}: {e}",
                                            strategy_id=strategy.strategy_id,
                                            duration_ms=self._calculate_duration_ms(start_time),
                                        )
                                        break
                                elif balance_token is None:
                                    # No token field found — let compiler handle
                                    logger.debug("  amount='all' with no token field, passing to compiler as-is")
                                else:
                                    # Have token but no market — cannot resolve
                                    logger.error(f"  amount='all' for {balance_token} but no market context available")
                                    intent_result = IterationResult(
                                        status=IterationStatus.COMPILATION_FAILED,
                                        intent=intent,
                                        error=(f"amount='all' for {balance_token} but no market context available"),
                                        strategy_id=strategy.strategy_id,
                                        duration_ms=self._calculate_duration_ms(start_time),
                                    )
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

                # For multi-intent sequences, record metrics once per iteration
                if is_multi_intent and intent_result is not None:
                    if intent_result.success:
                        self._record_success(execution_proved=intent_result.status == IterationStatus.SUCCESS)
                    else:
                        # Only track total_iterations here; consecutive_errors is
                        # already handled by run_loop when result.success is False
                        self._total_iterations += 1

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

        except Exception as e:
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

        # Initialize state if enabled
        if self.config.enable_state_persistence:
            try:
                await self.state_manager.initialize()
                logger.debug(f"State manager initialized for {strategy_id}")
            except Exception as e:
                logger.error(f"Failed to initialize state manager: {e}")

        # Recover incomplete sessions from previous runs
        try:
            recovered = await self._recover_incomplete_sessions()
            if recovered > 0:
                logger.info(f"Recovered {recovered} incomplete sessions on startup")
        except Exception as e:
            logger.error(f"Failed to recover incomplete sessions: {e}")

        # Restore copy trading cursor state if configured
        activity_provider = cast(
            StatefulActivityProviderProtocol | None, getattr(strategy, "_wallet_activity_provider", None)
        )
        if activity_provider is not None and self.config.enable_state_persistence:
            try:
                state = await self.state_manager.load_state(strategy_id)
                if state is not None and "copy_trading_state" in state.state:
                    activity_provider.set_state(state.state["copy_trading_state"])
                    logger.info("Copy trading: cursor state restored from persistence")
            except Exception as e:
                logger.warning(f"Failed to restore copy trading state: {e}")

        self._shutdown_requested = False
        self._signal_received = False
        self._terminal_lifecycle_state = None
        self._terminal_lifecycle_error_message = None

        # Set up dual-write for timeline events (gateway persistence)
        gateway_client = self._get_gateway_client()
        if gateway_client is not None:
            from ..api.timeline import set_event_gateway_client

            set_event_gateway_client(gateway_client)
            logger.debug("Enabled gateway dual-write for timeline events")

        # Register this strategy instance with the gateway
        self._register_with_gateway(strategy)

        # Write RUNNING state to LifecycleStore
        self._lifecycle_write_state(strategy_id, "RUNNING")

        # Emit strategy started event
        start_event = TimelineEvent(
            timestamp=datetime.now(UTC),
            event_type=TimelineEventType.STRATEGY_STARTED,
            description=f"Strategy {strategy_id} started with interval={interval}s",
            strategy_id=strategy_id,
            chain=getattr(self.config, "chain", ""),
            details={
                "interval_seconds": interval,
                "enable_state_persistence": self.config.enable_state_persistence,
            },
        )
        add_event(start_event)
        logger.debug(f"Emitted STRATEGY_STARTED event for {strategy_id}")

        loop_iteration_count = 0
        while not self._shutdown_requested:
            try:
                # Pre-iteration callback (e.g., reset Anvil forks)
                if pre_iteration_callback:
                    try:
                        pre_iteration_callback()
                    except CriticalCallbackError:
                        # Fail-closed: safety-critical callbacks stop the loop
                        raise
                    except Exception as e:
                        logger.error(f"Pre-iteration callback error: {e}")

                # Run iteration
                result = await self.run_iteration(strategy)

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

                # Capture portfolio snapshot for dashboard/PnL tracking.
                # Always capture regardless of iteration success — failed iterations
                # don't change the portfolio, but we need continuity in the equity curve.
                # _capture_portfolio_snapshot handles errors gracefully and persists
                # UNAVAILABLE snapshots when valuation fails.
                if self.config.enable_state_persistence:
                    await self._capture_portfolio_snapshot(
                        strategy=strategy,
                        iteration_number=self._total_iterations,
                    )

                # Call callback if provided
                if iteration_callback:
                    try:
                        iteration_callback(result)
                    except Exception as e:
                        logger.error(f"Iteration callback error: {e}")

                # Handle consecutive errors and circuit breaker recording
                if not result.success:
                    self._consecutive_errors += 1
                    if self._first_error_at is None:
                        self._first_error_at = datetime.now(UTC)

                    # Record failure in circuit breaker (skip statuses that already
                    # recorded inline to avoid double-counting)
                    if self._circuit_breaker is not None and result.status not in (
                        IterationStatus.CIRCUIT_BREAKER_OPEN,
                        IterationStatus.STRATEGY_TIMEOUT,  # already recorded in decide() handler
                        IterationStatus.STRATEGY_ERROR,  # already recorded in decide() handler
                    ):
                        self._circuit_breaker.record_failure(
                            error_message=result.error or f"Iteration failed: {result.status.value}",
                        )

                    # Auto-trigger emergency stop if breaker just tripped to OPEN
                    # (checked after both inline and run_loop recording paths)
                    if self._circuit_breaker is not None:
                        await self._maybe_trigger_emergency(strategy, result)

                    if self._consecutive_errors >= self.config.max_consecutive_errors:
                        await self._alert_consecutive_errors(strategy, result)
                        self._lifecycle_write_state(
                            strategy_id, "ERROR", error_message=str(result.error) if result.error else None
                        )
                else:
                    self._consecutive_errors = 0
                    self._first_error_at = None
                    # Reset emergency guard so a future HALF_OPEN->OPEN relapse can re-fire
                    if self._circuit_breaker is not None:
                        from ..execution.circuit_breaker import CircuitBreakerState

                        if self._circuit_breaker.state != CircuitBreakerState.OPEN:
                            self._emergency_triggered_for_open = False

                # Report positions and send heartbeat to gateway after each iteration
                position_protos = self._collect_position_snapshot(strategy)
                self._gateway_heartbeat(strategy_id, positions=position_protos)

                # Send lifecycle heartbeat
                self._lifecycle_heartbeat(strategy_id)

                # Poll for lifecycle commands (PAUSE, RESUME, STOP)
                command = self._lifecycle_poll_command(strategy_id)
                if command == "STOP":
                    logger.info("Received STOP command for %s", strategy_id)
                    self._lifecycle_handle_stop(strategy_id, strategy)
                elif command == "PAUSE":
                    logger.info("Received PAUSE command for %s", strategy_id)
                    self._lifecycle_write_state(strategy_id, "PAUSED")
                    self._gateway_update_status(strategy_id, "PAUSED")
                    # Preserve position snapshot so the dashboard doesn't lose it during pause
                    self._gateway_heartbeat(strategy_id, positions=self._collect_position_snapshot(strategy))
                    # Wait for RESUME command (send heartbeats so operator sees liveness)
                    while not self._shutdown_requested:
                        self._lifecycle_heartbeat(strategy_id)
                        resume_cmd = self._lifecycle_poll_command(strategy_id)
                        if resume_cmd == "RESUME":
                            logger.info("Received RESUME command for %s", strategy_id)
                            self._lifecycle_write_state(strategy_id, "RUNNING")
                            self._gateway_update_status(strategy_id, "RUNNING")
                            self._gateway_heartbeat(strategy_id, positions=self._collect_position_snapshot(strategy))
                            break
                        elif resume_cmd == "STOP":
                            logger.info("Received STOP command while paused for %s", strategy_id)
                            self._lifecycle_handle_stop(strategy_id, strategy)
                            break
                        await asyncio.sleep(self.config.lifecycle_poll_interval)

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

        # Write final state to LifecycleStore (preserve ERROR if set by circuit breaker)
        self._lifecycle_write_state(
            strategy_id,
            self._terminal_lifecycle_state or "TERMINATED",
            error_message=self._terminal_lifecycle_error_message,
        )

        # Deregister from gateway (mark as INACTIVE)
        self._deregister_from_gateway(strategy_id)

        # Emit strategy stopped event
        stop_event = TimelineEvent(
            timestamp=datetime.now(UTC),
            event_type=TimelineEventType.STRATEGY_STOPPED,
            description=f"Strategy {strategy_id} stopped",
            strategy_id=strategy_id,
            chain=getattr(self.config, "chain", ""),
            details={
                "shutdown_requested": self._shutdown_requested,
                "consecutive_errors": self._consecutive_errors,
            },
        )
        add_event(stop_event)
        logger.debug(f"Emitted STRATEGY_STOPPED event for {strategy_id}")

        logger.info(f"Run loop ended for strategy {strategy_id}")

        # Flush any pending state saves before cleanup
        if hasattr(strategy, "flush_pending_saves"):
            try:
                await strategy.flush_pending_saves()
            except Exception as e:
                logger.warning(f"Error flushing pending saves: {e}")

        # Cleanup
        if self.config.enable_state_persistence:
            try:
                await self.state_manager.close()
            except Exception as e:
                logger.error(f"Error closing state manager: {e}")

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

            event = TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=event_type,
                description=description,
                strategy_id=strategy_id,
                chain=getattr(strategy, "chain", "") or getattr(self.config, "chain", ""),
                tx_hash=tx_hash,
                details={
                    "intent_type": intent_type_str,
                    "success": success,
                    "gas_used": gas_used,
                },
            )
            add_event(event)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"Failed to emit execution timeline event: {e}")

    async def _write_ledger_entry(
        self,
        strategy: StrategyProtocol,
        intent: AnyIntent,
        result: Any | None,
        success: bool,
        error: str = "",
    ) -> None:
        """Write a structured trade record to the transaction ledger."""
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

            # Phase 4: stamp deployment_id and execution_mode onto the entry (VIB-2835/2837)
            deployment_id = getattr(strategy, "deployment_id", "") or strategy.strategy_id
            execution_mode = "dry_run" if self.config.dry_run else "live"
            entry.deployment_id = deployment_id
            entry.execution_mode = execution_mode

            if self.state_manager:
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
                        # Run PnL attribution on CLOSE events (VIB-2776)
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
        except Exception as e:  # noqa: BLE001
            logger.debug(f"Failed to write ledger entry: {e}")

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
        strategy_id = strategy.strategy_id

        if total_intents > 1:
            logger.debug(f"Executing intent as part of a {total_intents}-intent sequence")

        # Create compiler and state machine with retry configuration
        # Detect gateway orchestrator and use its gateway client for RPC
        from ..execution.gateway_orchestrator import GatewayExecutionOrchestrator

        gateway_client = None
        rpc_url = None

        if isinstance(self.execution_orchestrator, GatewayExecutionOrchestrator):
            # Use gateway client for RPC queries (preferred mode)
            gateway_client = self.execution_orchestrator._client
            logger.debug("Using GatewayExecutionOrchestrator - RPC queries go through gateway")
        else:
            # Fallback to direct RPC (deprecated for production)
            rpc_url = getattr(self.execution_orchestrator, "rpc_url", None)
            if rpc_url:
                logger.warning("Using direct RPC URL - this is deprecated for production use")

        # Extract real prices from market snapshot for accurate slippage calculations
        # Without this, IntentCompiler uses hardcoded default prices which causes
        # min_output calculations to be wrong (e.g., ETH at $2000 vs real $3117)
        price_oracle = None
        if market is not None and hasattr(market, "get_price_oracle_dict"):
            price_oracle = market.get_price_oracle_dict()
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
                pass  # No oracle available
            elif not price_oracle:
                # Oracle exists but empty after pre-fetch — no usable prices
                price_oracle = None
            else:
                logger.debug(f"Using real prices from market snapshot: {list(price_oracle.keys())}")

        # Initialize Polymarket config for Polygon chain (prediction market support)
        polymarket_config = None
        if strategy.chain.lower() == "polygon":
            try:
                from ..connectors.polymarket import PolymarketConfig

                polymarket_config = PolymarketConfig.from_env()
                logger.info(
                    f"PolymarketConfig loaded for wallet={polymarket_config.wallet_address[:10]}... "
                    "(prediction market intents enabled)"
                )
            except (ImportError, ValueError) as e:
                logger.debug(
                    f"PolymarketConfig not available: {e}. "
                    "Prediction market intents will not be available for this strategy."
                )

        # Build CLOB handler for Polymarket prediction market execution
        clob_handler = None
        clob_client = None
        if polymarket_config is not None:
            from ..connectors.polymarket.clob_client import ClobClient
            from ..execution.clob_handler import ClobActionHandler

            clob_client = ClobClient(polymarket_config)
            clob_handler = ClobActionHandler(clob_client=clob_client)

        # Build compiler and state machine. If setup fails, ensure ClobClient cleanup.
        try:
            # Build compiler config
            # Allow placeholder prices when no real prices are available (empty oracle).
            # This happens legitimately when the strategy uses indicators (RSI, BB)
            # instead of calling market.price() directly.  Placeholder prices are only
            # used as fallback for tokens not in the oracle dict, so an empty oracle
            # with placeholders enabled is safe -- the compiler will use conservative
            # hardcoded estimates for slippage calculations.
            if price_oracle is None:
                logger.debug(
                    "No prices in market snapshot -- compiler will use placeholder prices. "
                    "This is normal for strategies that use indicators instead of market.price()."
                )
            compiler_config = IntentCompilerConfig(
                allow_placeholder_prices=price_oracle is None,
                polymarket_config=polymarket_config,
            )

            compiler = IntentCompiler(
                chain=strategy.chain,
                wallet_address=strategy.wallet_address,
                rpc_url=rpc_url,
                price_oracle=price_oracle,
                config=compiler_config,
                gateway_client=gateway_client,
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

            state_machine = IntentStateMachine(
                intent=intent,
                compiler=compiler,
                config=state_machine_config,
                on_sadflow_enter=self._on_sadflow_enter,
            )
        except Exception:
            if clob_client is not None:
                clob_client.close()
            raise

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

        # Track the last execution result and context for final reporting
        last_execution_result: ExecutionResult | None = None
        last_execution_context: ExecutionContext | None = None

        # Execute through state machine loop
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
                # Dry run mode - skip actual execution
                if self.config.dry_run:
                    logger.info(
                        f"Dry run mode - skipping execution for {strategy_id}. "
                        f"Would execute {len(step_result.action_bundle.transactions)} transactions."
                    )
                    if clob_client is not None:
                        clob_client.close()
                    if record_metrics:
                        self._record_success()
                    return IterationResult(
                        status=IterationStatus.DRY_RUN,
                        intent=intent,
                        strategy_id=strategy_id,
                        duration_ms=self._calculate_duration_ms(start_time),
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
                last_execution_context = execution_context

                try:
                    # Execute through orchestrator (single-chain path)
                    # Note: _is_multi_chain flag guarantees this is ExecutionOrchestrator
                    # but we use cast for type checker since orchestrator is Union type
                    single_chain_orch = cast(ExecutionOrchestrator, self.execution_orchestrator)

                    # Pre-retry check: if previous attempt timed out and we have
                    # submitted tx_hashes, check if they've since confirmed to avoid
                    # duplicate swaps from retrying already-confirmed transactions.
                    if (
                        state_machine.retry_count > 0
                        and last_execution_result
                        and last_execution_result.transaction_results
                        and last_execution_result.error
                        and "timeout" in last_execution_result.error.lower()
                    ):
                        prev_hashes = [tr.tx_hash for tr in last_execution_result.transaction_results if tr.tx_hash]
                        if prev_hashes:
                            logger.info(
                                f"Pre-retry check: verifying {len(prev_hashes)} previously-submitted "
                                f"tx(es) before retrying"
                            )
                            all_confirmed = True
                            prev_receipts: list[FullTransactionReceipt] = []
                            for prev_hash in prev_hashes:
                                try:
                                    prev_receipt = await single_chain_orch.submitter.get_receipt(
                                        prev_hash, timeout=30.0
                                    )
                                    prev_receipts.append(prev_receipt)
                                    if prev_receipt.success:
                                        logger.info(f"Previously-submitted tx {prev_hash[:10]}... confirmed")
                                    else:
                                        logger.warning(f"Previously-submitted tx {prev_hash[:10]}... reverted")
                                        all_confirmed = False
                                except Exception:
                                    logger.warning(
                                        f"Could not get receipt for {prev_hash[:10]}..., proceeding with retry"
                                    )
                                    all_confirmed = False

                            if all_confirmed and prev_receipts:
                                logger.info(
                                    "All previously-submitted transactions confirmed -- "
                                    "skipping retry, treating as success"
                                )
                                # Update last_execution_result so downstream consumers
                                # (timeline, callbacks, IterationResult) see a successful
                                # result instead of the stale timeout failure.
                                # Preserve receipt data so ResultEnricher can extract
                                # swap amounts, position IDs, and other enriched data.
                                last_execution_result = ExecutionResult(
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
                                continue

                    # Route CLOB bundles to ClobActionHandler (off-chain orders),
                    # all other bundles to the on-chain ExecutionOrchestrator.
                    if clob_handler and clob_handler.can_handle(step_result.action_bundle):
                        clob_result = await clob_handler.execute(step_result.action_bundle)
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
                        last_execution_result = execution_result
                    else:
                        # Update native token price for USD gas guard
                        # tx_risk_config only exists on local ExecutionOrchestrator, not GatewayExecutionOrchestrator
                        tx_risk_cfg = getattr(single_chain_orch, "tx_risk_config", None)
                        if tx_risk_cfg and tx_risk_cfg.max_gas_cost_usd > 0 and price_oracle:
                            from almanak.gateway.data.balance.web3_provider import NATIVE_TOKEN_SYMBOLS

                            native_symbol = NATIVE_TOKEN_SYMBOLS.get(strategy.chain.lower(), "ETH")
                            native_price = price_oracle.get(native_symbol, 0)
                            if native_price:
                                tx_risk_cfg.native_token_price_usd = float(native_price)

                        execution_result = await single_chain_orch.execute(
                            action_bundle=step_result.action_bundle,
                            context=execution_context,
                        )
                        last_execution_result = execution_result

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

            elif step_result.error and not step_result.is_complete:
                # If execution already logged this exact error, keep this line at debug
                # to avoid duplicate warning spam in the same retry cycle.
                if last_execution_result and last_execution_result.error == step_result.error:
                    logger.debug(
                        f"Step error (already logged): {step_result.error} "
                        f"(retry {state_machine.retry_count}/{self.config.max_retries})"
                    )
                else:
                    logger.warning(
                        f"Step error: {step_result.error} (retry {state_machine.retry_count}/{self.config.max_retries})"
                    )

        # Close ClobClient to release httpx connection pool resources
        if clob_client is not None:
            try:
                clob_client.close()
            except Exception:
                logger.debug("Failed to close ClobClient", exc_info=True)

        # Always invalidate balance cache after execution (success or failure)
        # to prevent stale reads on the next decide() cycle.
        self.balance_provider.invalidate_cache()

        # State machine completed - check final result
        if state_machine.success:
            # Enrich result with intent-specific extracted data
            if last_execution_result and last_execution_context:
                try:
                    enricher = ResultEnricher()
                    last_execution_result = enricher.enrich(last_execution_result, intent, last_execution_context)
                except Exception as e:
                    logger.warning(f"Result enrichment failed: {e}")

            # Slippage circuit breaker: check actual slippage against max_slippage_bps
            # tx_risk_config only exists on local ExecutionOrchestrator, not GatewayExecutionOrchestrator
            if last_execution_result and last_execution_result.swap_amounts:
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
                if max_slippage > 0 and actual_slippage is not None and actual_slippage > max_slippage:
                    slippage_error = (
                        f"Slippage circuit breaker: actual slippage {actual_slippage} bps "
                        f"exceeds limit {max_slippage} bps "
                        f"(swap: {last_execution_result.swap_amounts.token_in} -> "
                        f"{last_execution_result.swap_amounts.token_out})"
                    )
                    logger.error(slippage_error)

                    # Emit timeline event for failed execution
                    self._emit_execution_timeline_event(strategy, intent, success=False, result=last_execution_result)

                    # Notify strategy of failure due to slippage breach
                    # Attach slippage error to result so strategy authors can access it
                    last_execution_result.error = slippage_error
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

                    return IterationResult(
                        status=IterationStatus.EXECUTION_FAILED,
                        intent=intent,
                        execution_result=last_execution_result,
                        error=slippage_error,
                        strategy_id=strategy_id,
                        duration_ms=self._calculate_duration_ms(start_time),
                    )

            # Emit timeline event for successful execution
            self._emit_execution_timeline_event(strategy, intent, success=True, result=last_execution_result)
            # Write structured trade record to transaction ledger (VIB-2402)
            await self._write_ledger_entry(strategy, intent, result=last_execution_result, success=True)
            if record_metrics:
                self._record_success(execution_proved=True)

            # Notify strategy of successful execution
            if hasattr(strategy, "on_intent_executed"):
                try:
                    strategy.on_intent_executed(intent, success=True, result=last_execution_result)
                except Exception as e:
                    logger.warning(f"Error in on_intent_executed callback: {e}")
            self._invoke_optional_hook(
                strategy,
                "on_copy_execution_result",
                intent,
                True,
                last_execution_result,
            )

            if state_machine.retry_count > 0:
                logger.info(f"Intent succeeded after {state_machine.retry_count} retries")

            # Save strategy state after successful execution
            if hasattr(strategy, "save_state"):
                try:
                    strategy.save_state()
                except Exception as e:
                    logger.warning(f"Error saving strategy state: {e}")

            # Post-execution balance reconciliation
            recon = await self._reconcile_post_execution_balances(strategy, intent, last_execution_result)

            return IterationResult(
                status=IterationStatus.SUCCESS,
                intent=intent,
                execution_result=last_execution_result,
                strategy_id=strategy_id,
                duration_ms=self._calculate_duration_ms(start_time),
                balance_reconciliation=recon,
            )
        else:
            # State machine reached FAILED state - escalate to operator
            error_msg = state_machine.error or "Unknown error after retries exhausted"
            logger.error(f"Intent failed after {state_machine.retry_count} retries: {error_msg}")

            # Emit timeline event for failed execution
            timeline_result = last_execution_result or SimpleNamespace(error=error_msg)
            self._emit_execution_timeline_event(strategy, intent, success=False, result=timeline_result)
            # Write failed trade to transaction ledger (VIB-2402)
            await self._write_ledger_entry(
                strategy, intent, result=last_execution_result, success=False, error=error_msg
            )

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

            return IterationResult(
                status=IterationStatus.EXECUTION_FAILED,
                intent=intent,
                execution_result=last_execution_result,
                error=error_msg,
                strategy_id=strategy_id,
                duration_ms=self._calculate_duration_ms(start_time),
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
            dest_chain = getattr(intent, "destination_chain", None)
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

        Args:
            strategy: The strategy being executed
            intents: List of intents to execute
            orchestrator: Multi-chain orchestrator
            start_time: When the iteration started
            resume_progress: If provided, resume from this progress (for stuck execution retry)

        Returns:
            IterationResult with execution details
        """
        import uuid

        from web3 import Web3

        strategy_id = strategy.strategy_id
        first_intent = intents[0] if intents else None

        # Get wallet address from orchestrator (works for both config and gateway modes)
        wallet_address = orchestrator.wallet_address

        # Get RPC URLs for EnsoStateProvider - gateway mode doesn't have _config
        if hasattr(orchestrator, "_config") and orchestrator._config is not None:
            rpc_urls = orchestrator._config.rpc_urls
        else:
            rpc_urls = {}

        # Create state provider for bridge tracking
        # In gateway mode, pass gateway_client so it can use gateway RPC instead of direct Web3
        gateway_client = self._get_gateway_client()
        state_provider = EnsoStateProvider(
            rpc_urls=rpc_urls,
            wallet_address=wallet_address,
            gateway_client=gateway_client,
        )

        # Determine execution progress
        start_step_index = 0
        previous_amount_received: Decimal | None = None

        if resume_progress is not None:
            # Resuming from a stuck execution (passed from _check_and_resume_stuck_execution)
            start_step_index = resume_progress.next_step_to_execute
            previous_amount_received = resume_progress.previous_amount_received
            progress = resume_progress
            logger.info(
                f"Resuming stuck execution from step {start_step_index + 1}/{len(intents)} "
                f"(execution_id={progress.execution_id})"
            )
        else:
            # Check for saved execution progress (resumption after restart)
            intents_hash = self._compute_intents_hash(intents)
            saved_progress = await self._load_execution_progress(strategy_id)

            if saved_progress and saved_progress.intents_hash == intents_hash:
                # Resume from last completed step
                start_step_index = saved_progress.next_step_to_execute
                previous_amount_received = saved_progress.previous_amount_received
                logger.info(
                    f"Resuming execution from step {start_step_index + 1}/{len(intents)} "
                    f"(execution_id={saved_progress.execution_id})"
                )
                progress = saved_progress
            else:
                # Start fresh execution
                if saved_progress:
                    logger.info("Intents changed (hash mismatch), starting fresh execution")
                    await self._clear_execution_progress(strategy_id)

                # Serialize intents for stuck execution recovery
                serialized_intents = [intent.serialize() for intent in intents]

                progress = ExecutionProgress(
                    execution_id=str(uuid.uuid4())[:8],
                    strategy_id=strategy_id,
                    intents_hash=intents_hash,
                    total_steps=len(intents),
                    serialized_intents=serialized_intents,
                )
                # Save initial progress with serialized intents
                await self._save_execution_progress(strategy_id, progress)

        logger.info(
            f"Executing {len(intents)} intents with bridge waiting for {strategy_id} "
            f"(starting from step {start_step_index + 1})"
        )

        # Track execution results
        successful_count = start_step_index  # Count already-completed steps
        failed_step: str | None = None
        error_message: str | None = None
        failed_result = None  # Explicitly track the result of the failed step
        callback_fired = False  # Track whether on_intent_executed was called for the failing step

        for i, intent in enumerate(intents):
            # Skip already-completed steps when resuming
            if i < start_step_index:
                logger.debug(f"Skipping already-completed step {i + 1}")
                continue

            step_num = i + 1
            intent_type = intent.intent_type.value
            chain = getattr(intent, "chain", None) or orchestrator.primary_chain
            is_cross_chain = is_cross_chain_intent(intent)

            logger.info(
                f"Step {step_num}/{len(intents)}: {intent_type} on {chain}"
                + (" (cross-chain)" if is_cross_chain else "")
            )

            # Resolve amount="all" if needed
            intent_to_execute = intent
            if Intent.has_chained_amount(intent) and previous_amount_received is not None:
                logger.info(f"Resolving amount='all' to {previous_amount_received}")
                intent_to_execute = Intent.set_resolved_amount(intent, previous_amount_received)

            # Get expected output for cross-chain tracking (before execution)
            expected_amount: int | None = None
            token_symbol: str | None = None
            dest_chain: str | None = None

            if is_cross_chain:
                dest_chain = getattr(intent, "destination_chain", None)
                token_symbol = getattr(intent, "to_token", None)
                # Set expected_amount=0 to accept ANY balance increase as completion
                # The actual received amount will be tracked and used for chaining
                expected_amount = 0

            # Execute the intent
            try:
                result = await orchestrator.execute(intent_to_execute, price_map=price_map, price_oracle=price_oracle)
            except Exception as e:
                logger.error(f"Step {step_num} execution failed: {e}")
                # Notify strategy of failed execution (mirrors _execute_single_chain)
                if hasattr(strategy, "on_intent_executed"):
                    try:
                        strategy.on_intent_executed(intent, success=False, result=None)
                    except Exception as cb_err:
                        logger.warning(f"Error in on_intent_executed callback: {cb_err}")
                callback_fired = True
                failed_step = f"step-{step_num}"
                error_message = str(e)
                break

            if not result.success:
                logger.error(f"Step {step_num} failed: {result.error}")
                # Notify strategy of failed execution (mirrors _execute_single_chain)
                if hasattr(strategy, "on_intent_executed"):
                    try:
                        strategy.on_intent_executed(intent, success=False, result=result)
                    except Exception as cb_err:
                        logger.warning(f"Error in on_intent_executed callback: {cb_err}")
                callback_fired = True
                failed_result = result
                failed_step = f"step-{step_num}"
                error_message = result.error
                break

            successful_count += 1

            # Track amount received for chaining
            if result.tx_result and hasattr(result.tx_result, "actual_amount_received"):
                previous_amount_received = result.tx_result.actual_amount_received
            else:
                # Fallback to intent amount
                amount_field = Intent.get_amount_field(intent_to_execute)
                if amount_field is not None and isinstance(amount_field, Decimal):
                    previous_amount_received = amount_field

            # For cross-chain swaps, verify source TX and wait for bridge completion
            if is_cross_chain and dest_chain and token_symbol:
                # Get tx hash from result
                tx_hash = None
                if result.tx_result:
                    tx_hash = getattr(result.tx_result, "tx_hash", None)

                if not tx_hash:
                    logger.error(f"Step {step_num}: No tx_hash in result, cannot track bridge")
                    failed_step = f"step-{step_num}"
                    error_message = "No transaction hash returned from execution"
                    break

                # Normalize tx_hash to include 0x prefix (some execution paths return bare hex)
                if not tx_hash.startswith("0x"):
                    tx_hash = f"0x{tx_hash}"

                # CRITICAL: Verify source TX actually succeeded on-chain before polling destination
                # This prevents polling for bridged assets when the source TX reverted
                logger.info(f"Verifying source TX confirmation on {chain}: {tx_hash}")

                try:
                    tx_verified = False

                    if gateway_client is not None:
                        # Use gateway's GetTransactionStatus RPC (no direct Web3)
                        from almanak.gateway.proto import gateway_pb2

                        for attempt in range(30):  # Max 30 attempts, ~1 minute
                            try:
                                status_response = gateway_client.execution.GetTransactionStatus(
                                    gateway_pb2.TxStatusRequest(tx_hash=tx_hash, chain=chain),
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
                                    logger.error(
                                        f"Step {step_num}: Source TX {status_response.status} on {chain}: {tx_hash}"
                                    )
                                    failed_step = f"step-{step_num}"
                                    error_message = f"Transaction {status_response.status} on {chain}: {tx_hash}"
                                    break
                            except Exception as exc:
                                logger.debug(
                                    "GetTransactionStatus attempt %s failed for %s on %s: %s",
                                    attempt + 1,
                                    tx_hash,
                                    chain,
                                    exc,
                                )
                            await asyncio.sleep(2)
                    else:
                        # Fallback to direct Web3 if no gateway client
                        source_rpc_url = rpc_urls.get(chain)
                        if not source_rpc_url:
                            logger.error(f"No RPC URL for source chain {chain}")
                            failed_step = f"step-{step_num}"
                            error_message = f"No RPC URL configured for chain {chain}"
                            break

                        source_web3 = Web3(Web3.HTTPProvider(source_rpc_url))

                        for attempt in range(30):
                            try:
                                receipt = source_web3.eth.get_transaction_receipt(
                                    tx_hash,  # type: ignore[arg-type]
                                )
                                if receipt:
                                    tx_status = receipt.get("status", 0)
                                    if tx_status == 0:
                                        logger.error(f"Step {step_num}: Source TX REVERTED on {chain}: {tx_hash}")
                                        failed_step = f"step-{step_num}"
                                        error_message = f"Transaction reverted on {chain}: {tx_hash}"
                                    else:
                                        logger.info(
                                            f"Source TX confirmed successfully on {chain}: {tx_hash}, "
                                            f"block={receipt.get('blockNumber')}"
                                        )
                                        tx_verified = True
                                    break
                            except Exception as exc:
                                logger.debug(
                                    "Receipt poll attempt %s failed for %s on %s: %s",
                                    attempt + 1,
                                    tx_hash,
                                    chain,
                                    exc,
                                )
                            await asyncio.sleep(2)

                    if failed_step:
                        break

                    if not tx_verified:
                        logger.error(f"Step {step_num}: Could not get receipt for {tx_hash}")
                        failed_step = f"step-{step_num}"
                        error_message = f"Timeout waiting for transaction receipt: {tx_hash}"
                        break

                except Exception as e:
                    logger.error(f"Step {step_num}: Error verifying source TX: {e}")
                    failed_step = f"step-{step_num}"
                    error_message = f"Failed to verify source transaction: {e}"
                    break

                # Source TX confirmed - now wait for bridge completion
                logger.info(f"Waiting for bridge completion: {chain} -> {dest_chain}, token={token_symbol}")

                # Register and wait for bridge transfer
                # expected_amount=0 means accept any positive balance increase
                deposit_id = state_provider.register_bridge_transfer(
                    source_chain=chain,
                    destination_chain=dest_chain,
                    source_tx_hash=tx_hash,
                    token_symbol=token_symbol,
                    expected_amount=expected_amount if expected_amount is not None else 0,
                )

                try:
                    bridge_status = await state_provider.wait_for_bridge_completion(
                        deposit_id=deposit_id,
                        timeout_seconds=300,  # 5 minute timeout
                        poll_interval_seconds=10,
                    )

                    if bridge_status["status"] == "completed":
                        # Update amount received with actual bridge output
                        # Balance increase is in wei - normalize using TokenResolver metadata
                        actual_received_wei = bridge_status.get("balance_increase")
                        if actual_received_wei is not None:
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
                                callback_fired = True
                                failed_step = f"step-{step_num}-bridge"
                                error_message = str(exc)
                                break

                            if normalized_amount is not None:
                                previous_amount_received = normalized_amount
                                logger.info(
                                    "Bridge completed: received %s %s on %s (%s wei, decimals=%s, token_hint=%s)",
                                    previous_amount_received,
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
                    else:
                        logger.error(f"Bridge failed: {bridge_status}")
                        # Notify strategy of bridge failure (source tx succeeded but bridge failed)
                        if hasattr(strategy, "on_intent_executed"):
                            try:
                                strategy.on_intent_executed(intent, success=False, result=result)
                            except Exception as cb_err:
                                logger.warning(f"Error in on_intent_executed callback: {cb_err}")
                        callback_fired = True
                        failed_step = f"step-{step_num}-bridge"
                        error_message = f"Bridge transfer failed: {bridge_status.get('error', 'Unknown')}"
                        break

                except TimeoutError as e:
                    logger.error(f"Bridge timeout: {e}")
                    # Notify strategy of bridge timeout (source tx succeeded but bridge timed out)
                    if hasattr(strategy, "on_intent_executed"):
                        try:
                            strategy.on_intent_executed(intent, success=False, result=result)
                        except Exception as cb_err:
                            logger.warning(f"Error in on_intent_executed callback: {cb_err}")
                    callback_fired = True
                    failed_step = f"step-{step_num}-bridge"
                    error_message = "Bridge transfer timed out after 5 minutes"
                    break

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

            # Save progress after each step completes successfully
            progress.completed_step_index = i
            progress.previous_amount_received = previous_amount_received
            await self._save_execution_progress(strategy_id, progress)
            logger.info(f"Step {step_num}/{len(intents)} completed, progress saved")

        # Ensure strategy is notified of failure even for paths that didn't fire the callback
        # inline (e.g. source TX verification failures, no-tx_hash, no-RPC-URL).
        # This single finalization block covers all break exits without per-exit patching.
        if failed_step and not callback_fired:
            if hasattr(strategy, "on_intent_executed"):
                try:
                    strategy.on_intent_executed(intent, success=False, result=failed_result)
                except Exception as cb_err:
                    logger.warning(f"Error in on_intent_executed callback: {cb_err}")

        # Build result
        if failed_step:
            logger.error(f"Multi-chain execution failed at {failed_step}: {error_message}")

            # Mark the failed step in progress so we can retry on next iteration
            # Parse failed step index from "step-N" or "step-N-bridge" format
            try:
                step_part = failed_step.split("-")[1]
                failed_intent_index = int(step_part) - 1  # Convert to 0-indexed
            except (IndexError, ValueError):
                failed_intent_index = 0

            # Save failure state for retry on next iteration
            progress.failed_at_step_index = failed_intent_index
            progress.failure_error = error_message
            progress.last_updated = datetime.now(UTC)
            await self._save_execution_progress(strategy_id, progress)
            logger.info(f"Saved failure state for retry: step {failed_intent_index + 1}, error: {error_message}")

            # Run diagnostics on the failed intent to help identify the cause
            try:
                if 0 <= failed_intent_index < len(intents):
                    failed_intent = intents[failed_intent_index]
                    failed_chain = getattr(failed_intent, "chain", strategy.chain)

                    # Create a chain-specific balance provider for diagnostics
                    from almanak.gateway.data.balance import Web3BalanceProvider

                    chain_rpc = rpc_urls.get(failed_chain)
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
                        if failed_result is None and not is_bridge_failure:
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
                            if failed_result is not None and hasattr(failed_result, "gas_warnings"):
                                cross_chain_gas_warnings = failed_result.gas_warnings or None

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

            return IterationResult(
                status=IterationStatus.EXECUTION_FAILED,
                intent=first_intent,
                error=f"{failed_step}: {error_message}",
                strategy_id=strategy_id,
                duration_ms=self._calculate_duration_ms(start_time),
            )

        # Always invalidate balance cache after execution (success or failure)
        # to prevent stale reads on the next decide() cycle.
        self.balance_provider.invalidate_cache()

        logger.info(
            f"Multi-chain execution with bridge waiting successful for {strategy_id}: "
            f"{successful_count}/{len(intents)} succeeded"
        )

        # Clear execution progress on successful completion
        await self._clear_execution_progress(strategy_id)

        self._record_success(execution_proved=True)
        return IterationResult(
            status=IterationStatus.SUCCESS,
            intent=first_intent,
            strategy_id=strategy_id,
            duration_ms=self._calculate_duration_ms(start_time),
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
        """Create an error IterationResult and update metrics."""
        self._consecutive_errors += 1
        self._total_iterations += 1

        return IterationResult(
            status=status,
            intent=intent,
            error=error,
            strategy_id=strategy_id,
            duration_ms=self._calculate_duration_ms(start_time),
        )

    async def _reconcile_post_execution_balances(self, strategy, intent, execution_result):
        from .runner_state import reconcile_post_execution_balances

        return await reconcile_post_execution_balances(self, strategy, intent, execution_result)

    @staticmethod
    def _extract_intent_tokens(intent):
        from .runner_state import extract_intent_tokens

        return extract_intent_tokens(intent)

    def _record_success(self, *, execution_proved: bool = False) -> None:
        from .runner_state import record_success

        record_success(self, execution_proved=execution_proved)

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
