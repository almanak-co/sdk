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
import hashlib
import json
import logging
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Protocol, cast

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
from ..execution.session import (
    ExecutionPhase as SessionPhase,
)
from ..execution.session import (
    ExecutionSession,
    TransactionStatus,
)
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
from ..intents.vocabulary import AnyIntent, DecideResult, HoldIntent, Intent, IntentSequence
from ..models.actions import AvailableAction, SuggestedAction
from ..models.operator_card import EventType, OperatorCard, PositionSummary, Severity
from ..models.stuck_reason import StuckReason
from ..portfolio import PortfolioMetrics, PortfolioSnapshot, ValueConfidence
from ..state.state_manager import StateData, StateManager, StateNotFoundError
from ..utils.log_formatters import (
    format_intent_type_emoji,
    format_percentage,
    format_usd,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Exceptions
# =============================================================================


class CriticalCallbackError(Exception):
    """Raised by pre/post-iteration callbacks to signal a fail-closed condition.

    When a pre_iteration_callback raises this exception, the strategy runner
    will stop the loop instead of logging and continuing. This is used by
    safety-critical callbacks like --reset-fork where continuing on failure
    would run the strategy on stale fork state.

    Regular Exception subclasses raised by callbacks are caught and logged
    without stopping the loop (backward compatible behavior).
    """


# =============================================================================
# Intent Helpers
# =============================================================================


def _extract_tokens_from_intent(intent: "AnyIntent") -> list[str]:
    """Extract token symbols from an intent for price pre-fetching.

    Returns a list of token symbols mentioned in the intent. Used to
    pre-populate the price cache when decide() doesn't call market.price().
    """
    tokens: list[str] = []
    # SwapIntent
    for attr in ("from_token", "to_token", "token_in", "token_out", "token"):
        val = getattr(intent, attr, None)
        if val and isinstance(val, str) and len(val) < 20:  # symbols only, not addresses
            tokens.append(val)
    return list(dict.fromkeys(tokens))  # dedupe preserving order


def _format_intent_for_log(intent: "AnyIntent") -> str:
    """Format an intent for user-friendly logging.

    Args:
        intent: The intent to format

    Returns:
        Human-readable string describing the intent with amounts and tokens
    """
    intent_type = intent.intent_type.value
    emoji_type = format_intent_type_emoji(intent_type)

    # SwapIntent
    if hasattr(intent, "from_token") and hasattr(intent, "to_token"):
        from_token = intent.from_token
        to_token = intent.to_token

        if hasattr(intent, "amount_usd") and intent.amount_usd:
            amount_str = format_usd(intent.amount_usd)
        elif hasattr(intent, "amount") and intent.amount:
            if intent.amount == "all":
                amount_str = "ALL"
            else:
                amount_str = f"{intent.amount}"
        else:
            amount_str = "N/A"

        slippage = getattr(intent, "max_slippage", None)
        slippage_str = f" (slippage: {format_percentage(slippage)})" if slippage else ""

        protocol = getattr(intent, "protocol", None)
        protocol_str = f" via {protocol}" if protocol else ""

        return f"{emoji_type}: {amount_str} {from_token} → {to_token}{slippage_str}{protocol_str}"

    # SupplyIntent
    if intent_type == "SUPPLY":
        token = getattr(intent, "token", "")
        amount = getattr(intent, "amount", None)
        amount_usd = getattr(intent, "amount_usd", None)
        protocol = getattr(intent, "protocol", "")

        if amount_usd:
            amount_str = format_usd(amount_usd)
        elif amount:
            amount_str = f"{amount} {token}"
        else:
            amount_str = f"N/A {token}"

        collateral = getattr(intent, "as_collateral", True)
        collateral_str = " (as collateral)" if collateral else ""

        return f"{emoji_type}: {amount_str} to {protocol}{collateral_str}"

    # BorrowIntent
    if intent_type == "BORROW":
        borrow_token = getattr(intent, "borrow_token", "")
        borrow_amount = getattr(intent, "borrow_amount", None)
        collateral_token = getattr(intent, "collateral_token", "")
        collateral_amount = getattr(intent, "collateral_amount", None)
        protocol = getattr(intent, "protocol", "")

        if borrow_amount:
            amount_str = f"{borrow_amount} {borrow_token}"
        else:
            amount_str = f"N/A {borrow_token}"

        collateral_str = ""
        if collateral_amount == "all":
            collateral_str = f" (collateral: ALL {collateral_token})"
        elif collateral_amount:
            collateral_str = f" (collateral: {collateral_amount} {collateral_token})"

        return f"{emoji_type}: {amount_str} from {protocol}{collateral_str}"

    # WithdrawIntent
    if intent_type == "WITHDRAW":
        token = getattr(intent, "token", "")
        amount = getattr(intent, "amount", None)
        protocol = getattr(intent, "protocol", "")

        if amount == "all":
            amount_str = f"ALL {token}"
        elif amount:
            amount_str = f"{amount} {token}"
        else:
            amount_str = f"N/A {token}"

        return f"{emoji_type}: {amount_str} from {protocol}"

    # RepayIntent
    if intent_type == "REPAY":
        token = getattr(intent, "token", "")
        amount = getattr(intent, "amount", None)
        protocol = getattr(intent, "protocol", "")

        if amount == "all":
            amount_str = f"ALL {token}"
        elif amount:
            amount_str = f"{amount} {token}"
        else:
            amount_str = f"N/A {token}"

        return f"{emoji_type}: {amount_str} to {protocol}"

    # LPOpenIntent
    if intent_type == "LP_OPEN":
        pool = getattr(intent, "pool", "")
        amount0 = getattr(intent, "amount0", Decimal("0"))
        amount1 = getattr(intent, "amount1", Decimal("0"))
        range_lower = getattr(intent, "range_lower", None)
        range_upper = getattr(intent, "range_upper", None)
        protocol = getattr(intent, "protocol", "")

        range_str = ""
        if range_lower and range_upper:
            range_str = f" [{range_lower:.0f} - {range_upper:.0f}]"

        return f"{emoji_type}: {pool} ({amount0}, {amount1}){range_str} via {protocol}"

    # LPCloseIntent
    if intent_type == "LP_CLOSE":
        position_id = getattr(intent, "position_id", "")
        protocol = getattr(intent, "protocol", "")
        return f"{emoji_type}: position {position_id[:8]}... via {protocol}"

    # PerpOpenIntent
    if intent_type == "PERP_OPEN":
        market = getattr(intent, "market", "")
        direction = getattr(intent, "direction", "")
        size_usd = getattr(intent, "size_usd", None)
        leverage = getattr(intent, "leverage", None)
        protocol = getattr(intent, "protocol", "")

        size_str = format_usd(size_usd) if size_usd else "N/A"
        leverage_str = f" ({leverage}x)" if leverage else ""

        return f"{emoji_type}: {direction} {market} {size_str}{leverage_str} via {protocol}"

    # PerpCloseIntent
    if intent_type == "PERP_CLOSE":
        market = getattr(intent, "market", "")
        position_id = getattr(intent, "position_id", "")
        protocol = getattr(intent, "protocol", "")
        return f"{emoji_type}: {market} position {position_id[:8] if position_id else 'N/A'}... via {protocol}"

    # BridgeIntent
    if intent_type == "BRIDGE":
        token = getattr(intent, "token", "")
        amount = getattr(intent, "amount", None)
        from_chain = getattr(intent, "from_chain", "")
        to_chain = getattr(intent, "to_chain", "")

        if amount == "all":
            amount_str = f"ALL {token}"
        elif amount:
            amount_str = f"{amount} {token}"
        else:
            amount_str = f"N/A {token}"

        return f"{emoji_type}: {amount_str} {from_chain} → {to_chain}"

    # HoldIntent
    if intent_type == "HOLD":
        reason = getattr(intent, "reason", "No action")
        return f"{emoji_type}: {reason}"

    # Default fallback
    return f"{emoji_type} (id={intent.intent_id[:8]}...)"


# =============================================================================
# Enums and Data Classes
# =============================================================================


class IterationStatus(StrEnum):
    """Status of a strategy iteration."""

    SUCCESS = "SUCCESS"
    DRY_RUN = "DRY_RUN"  # Dry run mode - no transactions submitted
    HOLD = "HOLD"  # Strategy decided to hold
    TEARDOWN = "TEARDOWN"  # Strategy is executing teardown
    COMPILATION_FAILED = "COMPILATION_FAILED"
    EXECUTION_FAILED = "EXECUTION_FAILED"
    STRATEGY_ERROR = "STRATEGY_ERROR"
    STRATEGY_TIMEOUT = "STRATEGY_TIMEOUT"  # strategy.decide() exceeded time limit
    DATA_ERROR = "DATA_ERROR"
    CIRCUIT_BREAKER_OPEN = "CIRCUIT_BREAKER_OPEN"  # Circuit breaker blocked execution


@dataclass
class IterationResult:
    """Result of a single strategy iteration.

    Attributes:
        status: Outcome status of the iteration
        intent: The intent produced by the strategy (if any)
        execution_result: Result from execution orchestrator (if executed)
        error: Error message (if failed)
        strategy_id: ID of the strategy that ran
        duration_ms: Time taken for the iteration in milliseconds
        timestamp: When the iteration completed
    """

    status: IterationStatus
    intent: AnyIntent | None = None
    execution_result: ExecutionResult | None = None
    error: str | None = None
    strategy_id: str = ""
    duration_ms: float = 0.0
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    balance_reconciliation: dict[str, Any] | None = None  # Post-execution balance check

    @property
    def success(self) -> bool:
        """Check if iteration was successful (including DRY_RUN, HOLD, and TEARDOWN)."""
        return self.status in (
            IterationStatus.SUCCESS,
            IterationStatus.DRY_RUN,
            IterationStatus.HOLD,
            IterationStatus.TEARDOWN,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "status": self.status.value,
            "intent": self.intent.serialize() if self.intent else None,
            "execution_result": self.execution_result.to_dict() if self.execution_result else None,
            "error": self.error,
            "strategy_id": self.strategy_id,
            "duration_ms": self.duration_ms,
            "timestamp": self.timestamp.isoformat(),
            "balance_reconciliation": self.balance_reconciliation,
        }


@dataclass
class ExecutionProgress:
    """Tracks execution progress for resuming after restart.

    Attributes:
        execution_id: Unique ID for this execution sequence
        strategy_id: Strategy that owns this execution
        intents_hash: Hash of serialized intents (to detect changes)
        total_steps: Total number of steps in the sequence
        completed_step_index: Index of last completed step (-1 if none)
        previous_amount_received: Amount from last step (for chaining)
        started_at: When this execution started
        last_updated: When progress was last updated
        serialized_intents: Serialized intent data for resumption
        failed_at_step_index: Index of the step that failed (None if no failure)
        failure_error: Error message from the failed step
    """

    execution_id: str
    strategy_id: str
    intents_hash: str
    total_steps: int
    completed_step_index: int = -1  # -1 means no steps completed
    previous_amount_received: Decimal | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_updated: datetime = field(default_factory=lambda: datetime.now(UTC))
    serialized_intents: list[dict[str, Any]] | None = None
    failed_at_step_index: int | None = None
    failure_error: str | None = None

    @property
    def is_stuck(self) -> bool:
        """Check if execution is stuck (has a failed step that needs retry)."""
        return self.failed_at_step_index is not None

    @property
    def next_step_to_execute(self) -> int:
        """Get the index of the next step to execute (failed step or next after completed)."""
        if self.failed_at_step_index is not None:
            return self.failed_at_step_index
        return self.completed_step_index + 1

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "execution_id": self.execution_id,
            "strategy_id": self.strategy_id,
            "intents_hash": self.intents_hash,
            "total_steps": self.total_steps,
            "completed_step_index": self.completed_step_index,
            "previous_amount_received": str(self.previous_amount_received)
            if self.previous_amount_received is not None
            else None,
            "started_at": self.started_at.isoformat(),
            "last_updated": self.last_updated.isoformat(),
            "serialized_intents": self.serialized_intents,
            "failed_at_step_index": self.failed_at_step_index,
            "failure_error": self.failure_error,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExecutionProgress":
        """Create from dictionary."""
        previous_amount = data.get("previous_amount_received")
        return cls(
            execution_id=data["execution_id"],
            strategy_id=data["strategy_id"],
            intents_hash=data["intents_hash"],
            total_steps=data["total_steps"],
            completed_step_index=data.get("completed_step_index", -1),
            previous_amount_received=Decimal(previous_amount) if previous_amount is not None else None,
            started_at=datetime.fromisoformat(data["started_at"]),
            last_updated=datetime.fromisoformat(data["last_updated"]),
            serialized_intents=data.get("serialized_intents"),
            failed_at_step_index=data.get("failed_at_step_index"),
            failure_error=data.get("failure_error"),
        )


@dataclass
class RunnerConfig:
    """Configuration for the strategy runner.

    Attributes:
        default_interval_seconds: Default interval between iterations
        max_consecutive_errors: Maximum consecutive errors before alerting
        enable_state_persistence: Whether to persist state between iterations
        enable_alerting: Whether to send alerts on errors
        dry_run: If True, compile but don't execute intents
        max_retries: Maximum number of automatic retries per intent (default 3)
        initial_retry_delay: Initial delay between retries in seconds (default 1.0)
        max_retry_delay: Maximum delay between retries in seconds (default 60.0)
        decide_timeout_seconds: Hard timeout for strategy.decide() in seconds (default 30.0)
    """

    default_interval_seconds: int = 60
    max_consecutive_errors: int = 3
    enable_state_persistence: bool = True
    enable_alerting: bool = True
    dry_run: bool = False
    max_retries: int = 3
    initial_retry_delay: float = 1.0
    max_retry_delay: float = 60.0
    lifecycle_poll_interval: float = 2.0
    decide_timeout_seconds: float = 30.0


# =============================================================================
# Strategy Protocol
# =============================================================================


class StrategyProtocol(Protocol):
    """Protocol defining the interface for strategies.

    Strategies must implement these properties and methods to be
    compatible with the StrategyRunner.
    """

    @property
    def strategy_id(self) -> str:
        """Unique identifier for the strategy."""
        ...

    @property
    def chain(self) -> str:
        """Target blockchain (e.g., 'arbitrum')."""
        ...

    @property
    def wallet_address(self) -> str:
        """Wallet address for the strategy."""
        ...

    def decide(self, market: Any) -> DecideResult:
        """Main decision method that returns an intent, sequence, list, or None."""
        ...

    def create_market_snapshot(self) -> Any:
        """Create a market snapshot for the strategy."""
        ...

    def get_portfolio_snapshot(self, market: Any = None) -> PortfolioSnapshot | None:
        """Get current portfolio value and positions (optional).

        Returns PortfolioSnapshot if implemented, None if not supported.
        """
        ...

    def generate_teardown_intents(self, mode: Any, market: Any = None) -> list:
        """Generate intents to close all positions (optional, checked via hasattr)."""
        ...

    def get_open_positions(self) -> Any:
        """Return open positions for teardown safety validation (optional)."""
        ...


class StatefulActivityProviderProtocol(Protocol):
    """Protocol for copy-trading activity providers with cursor state."""

    def get_state(self) -> dict[str, Any]: ...

    def set_state(self, state: dict[str, Any]) -> None: ...


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

    def _get_gateway_client(self) -> Any | None:
        """Get the gateway gRPC client from the execution orchestrator.

        Checks GatewayExecutionOrchestrator directly, gateway-backed
        MultiChainOrchestrator, and legacy per-chain executors.

        Returns:
            GatewayClient instance or None if not gateway-backed.
        """
        # Prefer explicitly set client
        if self._gateway_client is not None:
            return self._gateway_client

        from ..execution.gateway_orchestrator import GatewayExecutionOrchestrator

        if isinstance(self.execution_orchestrator, GatewayExecutionOrchestrator):
            return self.execution_orchestrator._client

        # Gateway-backed MultiChainOrchestrator stores gateway client directly
        if hasattr(self.execution_orchestrator, "_gateway_client"):
            client = self.execution_orchestrator._gateway_client
            if client is not None:
                return client

        # Legacy multi-chain mode: check per-chain executors for a gateway client
        if self._is_multi_chain and hasattr(self.execution_orchestrator, "_executors"):
            for executor in self.execution_orchestrator._executors.values():
                orch = getattr(executor, "orchestrator", None)
                if isinstance(orch, GatewayExecutionOrchestrator):
                    return orch._client

        return None

    def _register_with_gateway(self, strategy: StrategyProtocol) -> None:
        """Register this strategy instance with the gateway's instance registry.

        Non-fatal: catches all exceptions so the strategy continues running
        even if registration fails.
        """
        client = self._get_gateway_client()
        if client is None:
            return

        try:
            from almanak.gateway.proto import gateway_pb2

            request = gateway_pb2.RegisterInstanceRequest(
                strategy_id=strategy.strategy_id,
                strategy_name=getattr(
                    strategy,
                    "strategy_display_name",
                    getattr(getattr(strategy, "config", None), "strategy_display_name", strategy.strategy_id),
                ),
                template_name=type(strategy).__name__,
                chain=getattr(strategy, "chain", ""),
                protocol=getattr(strategy, "protocol", ""),
                wallet_address=getattr(strategy, "wallet_address", ""),
                config_json="",
                version="",
            )
            response = client.dashboard.RegisterStrategyInstance(request)
            if response.success:
                verb = "Re-registered" if response.already_existed else "Registered"
                logger.info(f"{verb} strategy instance with gateway: {strategy.strategy_id}")
            else:
                logger.warning(f"Failed to register with gateway: {response.error}")
        except Exception as e:
            logger.debug(f"Failed to register with gateway (non-fatal): {e}")

    def _deregister_from_gateway(self, strategy_id: str) -> None:
        """Mark this strategy instance as INACTIVE in the gateway registry.

        Non-fatal: catches all exceptions.
        """
        client = self._get_gateway_client()
        if client is None:
            return

        try:
            from almanak.gateway.proto import gateway_pb2

            request = gateway_pb2.UpdateInstanceStatusRequest(
                strategy_id=strategy_id,
                status="INACTIVE",
                reason="Strategy runner stopped",
            )
            client.dashboard.UpdateStrategyInstanceStatus(request)
            logger.debug(f"Deregistered strategy instance from gateway: {strategy_id}")
        except Exception as e:
            logger.debug(f"Failed to deregister from gateway (non-fatal): {e}")

    def _gateway_heartbeat(self, strategy_id: str) -> None:
        """Send a heartbeat to the gateway for this strategy instance.

        Non-fatal: catches all exceptions.
        """
        client = self._get_gateway_client()
        if client is None:
            return

        try:
            from almanak.gateway.proto import gateway_pb2

            request = gateway_pb2.UpdateInstanceStatusRequest(
                strategy_id=strategy_id,
                heartbeat_only=True,
            )
            client.dashboard.UpdateStrategyInstanceStatus(request)
        except Exception as e:
            logger.debug(f"Failed to send heartbeat to gateway (non-fatal): {e}")

    def _lifecycle_write_state(self, agent_id: str, state: str, error_message: str | None = None) -> None:
        """Write agent state to LifecycleStore via gateway.

        Non-fatal: catches all exceptions.
        """
        client = self._get_gateway_client()
        if client is None:
            return
        try:
            from almanak.gateway.proto import gateway_pb2

            request = gateway_pb2.WriteAgentStateRequest(
                agent_id=agent_id,
                state=state,
                error_message=error_message or "",
            )
            client.lifecycle.WriteState(request)
        except Exception as e:
            logger.debug(f"Failed to write lifecycle state (non-fatal): {e}")

    def _lifecycle_heartbeat(self, agent_id: str) -> None:
        """Send lifecycle heartbeat via gateway.

        Non-fatal: catches all exceptions.
        """
        client = self._get_gateway_client()
        if client is None:
            return
        try:
            from almanak.gateway.proto import gateway_pb2

            request = gateway_pb2.HeartbeatRequest(agent_id=agent_id)
            client.lifecycle.Heartbeat(request)
        except Exception as e:
            logger.debug(f"Failed to send lifecycle heartbeat (non-fatal): {e}")

    def _lifecycle_poll_command(self, agent_id: str) -> str | None:
        """Poll for pending command from LifecycleStore.

        Returns command string (PAUSE, RESUME, STOP) or None.
        The command is acknowledged only after it is returned so that callers
        can apply side-effects before the ack.  If the process crashes between
        read and ack the command will be re-delivered on the next poll.
        Non-fatal: catches all exceptions.
        """
        client = self._get_gateway_client()
        if client is None:
            return None
        try:
            from almanak.gateway.proto import gateway_pb2

            request = gateway_pb2.ReadAgentCommandRequest(agent_id=agent_id)
            response = client.lifecycle.ReadCommand(request)
            if response.found:
                command = response.command
                logger.info("Received lifecycle command: %s (from %s)", command, response.issued_by)
                # Acknowledge after reading so the command is re-delivered if we crash
                try:
                    ack_request = gateway_pb2.AckAgentCommandRequest(command_id=response.command_id)
                    client.lifecycle.AckCommand(ack_request)
                except Exception:
                    logger.warning("Failed to ack lifecycle command %s (will be re-delivered)", response.command_id)
                return command
            return None
        except Exception as e:
            logger.debug("Failed to poll lifecycle command (non-fatal): %s", e)
            return None

    def _lifecycle_handle_stop(self, strategy_id: str, strategy: Any) -> None:
        """Handle STOP command: bridge into teardown or hard-stop.

        Shared by both the normal STOP path and the STOP-while-paused path.
        """
        self._lifecycle_write_state(strategy_id, "STOPPING")
        if hasattr(strategy, "supports_teardown") and strategy.supports_teardown():
            from almanak.framework.teardown import TeardownMode, TeardownRequest, get_teardown_state_manager

            manager = get_teardown_state_manager()
            teardown_request = TeardownRequest(
                strategy_id=strategy_id,
                mode=TeardownMode.SOFT,
                reason="Lifecycle STOP command",
                requested_by="lifecycle",
            )
            manager.create_request(teardown_request)
            logger.info("Created teardown request for %s from STOP command", strategy_id)
            # Don't break -- let the next iteration pick up the teardown request
            # via _check_teardown_requested(), which will execute teardown intents
            # and then call request_shutdown()
        else:
            # Strategy doesn't support teardown -- hard stop
            logger.info("Strategy %s doesn't support teardown, stopping immediately", strategy_id)
            self._shutdown_requested = True

    def set_gateway_client(self, client: Any) -> None:
        """Explicitly set the gateway client for instance registration.

        Use this when the gateway client can't be discovered from the
        execution orchestrator (e.g. multi-chain mode).
        """
        self._gateway_client = client

    def setup_gateway_integration(self, strategy: StrategyProtocol) -> None:
        """Set up gateway dual-write and instance registration.

        Call this before run_iteration() when running outside run_loop()
        (e.g. --once mode) so that single-iteration runs also appear
        in the instance registry and emit gateway timeline events.
        """
        gateway_client = self._get_gateway_client()
        if gateway_client is not None:
            from ..api.timeline import set_event_gateway_client

            set_event_gateway_client(gateway_client)
            logger.debug("Enabled gateway dual-write for timeline events")

        self._register_with_gateway(strategy)

    def teardown_gateway_integration(self, strategy_id: str) -> None:
        """Mark instance as INACTIVE and clear gateway dual-write.

        Call this after run_iteration() when running outside run_loop().
        """
        self._deregister_from_gateway(strategy_id)

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

        logger.info(f"Starting iteration for strategy: {strategy_id}")

        try:
            # Step 0: Honor operator pause before any strategy logic/execution.
            paused, pause_reason = await self._is_strategy_paused(strategy_id)
            if paused:
                if strategy_id not in self._logged_paused_strategy_ids:
                    logger.info(
                        "⏸️ %s is paused by operator%s",
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
                if decide_timeout <= 0:
                    # Timeout disabled -- run decide() without a time limit
                    decide_result = await asyncio.to_thread(strategy.decide, market)
                else:
                    decide_result = await asyncio.wait_for(
                        asyncio.to_thread(strategy.decide, market),
                        timeout=decide_timeout,
                    )
                self._decide_in_progress = False
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
                logger.info(f"⏸️ {strategy_id} HOLD: {reason}")
                self._record_success()
                return IterationResult(
                    status=IterationStatus.HOLD,
                    intent=hold_intent,
                    strategy_id=strategy_id,
                    duration_ms=self._calculate_duration_ms(start_time),
                )

            # Step 5: Log intent(s) with detailed information
            if len(intents) == 1:
                intent_summary = _format_intent_for_log(intents[0])
                logger.info(f"📈 {strategy_id} intent: {intent_summary}")
            else:
                # Log intent sequence with details for each step
                logger.info(f"📈 {strategy_id} intent sequence ({len(intents)} steps):")
                for i, intent in enumerate(intents, 1):
                    intent_summary = _format_intent_for_log(intent)
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
                    # Resolve amount="all" from previous step's output
                    intent_to_execute = intent
                    if is_multi_intent and Intent.has_chained_amount(intent):
                        if previous_amount_received is None:
                            if self.config.dry_run:
                                logger.warning(
                                    f"  Intent {idx + 1}/{len(intents)} uses amount='all' "
                                    "but no previous step output available (dry-run mode). "
                                    "Skipping compilation of this step."
                                )
                                # In dry-run mode, the previous step didn't execute so
                                # there's no output amount to chain. Skip compilation
                                # (compiler rejects unresolved 'all') and mark as DRY_RUN.
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
                            logger.info(
                                f"  Resolving amount='all' to {previous_amount_received} "
                                f"for intent {idx + 1}/{len(intents)}"
                            )
                            intent_to_execute = Intent.set_resolved_amount(intent, previous_amount_received)

                    if is_multi_intent:
                        logger.info(
                            f"  Executing intent {idx + 1}/{len(intents)}: {_format_intent_for_log(intent_to_execute)}"
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

                return intent_result  # type: ignore[return-value]

        except Exception as e:
            logger.exception(f"Unexpected error in iteration for {strategy_id}: {e}")
            return self._create_error_result(
                strategy_id,
                IterationStatus.STRATEGY_ERROR,
                f"Unexpected error: {e}",
                start_time,
            )

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
                    await self._update_state(strategy_id, result)

                # Persist copy trading cursor state (if configured)
                if activity_provider is not None and self.config.enable_state_persistence:
                    try:
                        await self._persist_copy_trading_state(strategy_id, activity_provider)
                    except Exception as e:
                        logger.warning(f"Failed to persist copy trading state: {e}")

                # Capture portfolio snapshot for dashboard/PnL tracking
                if self.config.enable_state_persistence and result.success:
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

                # Send heartbeat to gateway after each iteration
                self._gateway_heartbeat(strategy_id)

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
                    # Wait for RESUME command (send heartbeats so operator sees liveness)
                    while not self._shutdown_requested:
                        self._lifecycle_heartbeat(strategy_id)
                        resume_cmd = self._lifecycle_poll_command(strategy_id)
                        if resume_cmd == "RESUME":
                            logger.info("Received RESUME command for %s", strategy_id)
                            self._lifecycle_write_state(strategy_id, "RUNNING")
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

        # Write TERMINATED state to LifecycleStore
        self._lifecycle_write_state(strategy_id, "TERMINATED")

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

    def request_shutdown(self) -> None:
        """Request graceful shutdown of the run loop.

        This sets a flag that causes run_loop() to exit after the
        current iteration completes.
        """
        logger.info("Shutdown requested for strategy runner")
        self._shutdown_requested = True

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
            # If empty (e.g. strategy didn't call market.price() in decide()),
            # try to pre-fetch prices for the intent's tokens so the compiler
            # gets real prices instead of placeholders.
            if not price_oracle and hasattr(market, "price"):
                intent_tokens = _extract_tokens_from_intent(intent)
                if intent_tokens:
                    for token in intent_tokens:
                        try:
                            market.price(token)
                        except Exception:
                            pass  # Token price unavailable, compiler will use placeholder
                    price_oracle = market.get_price_oracle_dict()
                    if price_oracle:
                        logger.debug(f"Pre-fetched prices for intent tokens: {list(price_oracle.keys())}")
            if not price_oracle:
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
                execution_context = ExecutionContext(
                    strategy_id=strategy_id,
                    chain=strategy.chain,
                    wallet_address=strategy.wallet_address,
                    correlation_id=intent.intent_id,
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

        # Teardown requested - check if strategy implements it
        if not hasattr(strategy, "supports_teardown"):
            logger.warning(
                f"Teardown requested for {strategy_id} but strategy doesn't have "
                f"supports_teardown() method. Continuing normal operation."
            )
            return None

        if not strategy.supports_teardown():  # type: ignore[attr-defined]
            logger.warning(
                f"Teardown requested for {strategy_id} but strategy reports "
                f"supports_teardown()=False. Continuing normal operation."
            )
            return None

        # Strategy supports teardown - acknowledge request
        if hasattr(strategy, "acknowledge_teardown_request"):
            try:
                strategy.acknowledge_teardown_request()
                logger.info(f"Acknowledged teardown request for {strategy_id}")
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Failed to acknowledge teardown request: {e}")

        # Verify generate_teardown_intents exists
        if not hasattr(strategy, "generate_teardown_intents"):
            logger.error(
                f"Strategy {strategy_id} supports_teardown()=True but doesn't "
                f"implement generate_teardown_intents(). Cannot proceed with teardown."
            )
            return None

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
            # Pre-fetch prices for intent tokens if cache is empty
            if not price_oracle and hasattr(market, "price"):
                all_tokens: set[str] = set()
                for i in intents:
                    all_tokens.update(_extract_tokens_from_intent(i))
                for token in all_tokens:
                    try:
                        market.price(token)
                    except Exception:
                        pass
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
                failed_step = f"step-{step_num}"
                error_message = str(e)
                break

            if not result.success:
                logger.error(f"Step {step_num} failed: {result.error}")
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
                        failed_step = f"step-{step_num}-bridge"
                        error_message = f"Bridge transfer failed: {bridge_status.get('error', 'Unknown')}"
                        break

                except TimeoutError as e:
                    logger.error(f"Bridge timeout: {e}")
                    failed_step = f"step-{step_num}-bridge"
                    error_message = "Bridge transfer timed out after 5 minutes"
                    break

            # Save progress after each step completes successfully
            progress.completed_step_index = i
            progress.previous_amount_received = previous_amount_received
            await self._save_execution_progress(strategy_id, progress)
            logger.info(f"Step {step_num}/{len(intents)} completed, progress saved")

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

    @staticmethod
    def _bridge_token_resolution_candidates(
        token_symbol: str | None,
        bridge_status: dict[str, Any],
    ) -> list[str]:
        """Collect token identifiers for bridge amount normalization."""
        candidates: list[str] = []
        keys = (
            "destination_token_address",
            "destinationTokenAddress",
            "token_address",
            "tokenAddress",
            "destination_token",
            "destinationToken",
            "token",
            "token_symbol",
        )

        def _append_candidate(value: Any) -> None:
            if isinstance(value, str) and value.strip():
                candidates.append(value.strip())

        for key in keys:
            _append_candidate(bridge_status.get(key))

        route_data = bridge_status.get("route_data")
        if isinstance(route_data, dict):
            for key in keys:
                _append_candidate(route_data.get(key))

        if token_symbol:
            candidates.append(token_symbol)

        # Preserve first-seen ordering while de-duplicating
        seen: set[str] = set()
        deduped: list[str] = []
        for candidate in candidates:
            candidate_key = candidate.lower()
            if candidate_key not in seen:
                seen.add(candidate_key)
                deduped.append(candidate)
        return deduped

    @staticmethod
    def _normalize_bridge_balance_increase(
        balance_increase_wei: int | str,
        destination_chain: str,
        token_symbol: str | None,
        bridge_status: dict[str, Any],
    ) -> tuple[Decimal | None, dict[str, Any]]:
        """Normalize bridge completion balance increase from wei to token units.

        Returns:
            (normalized_amount, metadata). If normalization fails, returns
            (None, metadata) with raw wei preserved for diagnostics.
        """
        try:
            raw_wei = int(balance_increase_wei)
        except (TypeError, ValueError):
            return None, {
                "raw_wei": balance_increase_wei,
                "destination_chain": destination_chain,
                "token_symbol": token_symbol,
                "error": "invalid_balance_increase_wei",
            }

        from ..data.tokens import get_token_resolver
        from ..data.tokens.exceptions import TokenNotFoundError

        resolver = get_token_resolver()
        candidates = StrategyRunner._bridge_token_resolution_candidates(token_symbol, bridge_status)
        for candidate in candidates:
            try:
                resolved = resolver.resolve(candidate, destination_chain)
                decimals = resolved.decimals
                normalized = Decimal(raw_wei) / Decimal(10**decimals)
                return normalized, {
                    "raw_wei": raw_wei,
                    "destination_chain": destination_chain,
                    "token_symbol": token_symbol,
                    "resolved_from": candidate,
                    "resolved_address": resolved.address,
                    "decimals": decimals,
                }
            except Exception:
                continue

        unresolved = token_symbol or (candidates[0] if candidates else "<unknown-token>")
        raise TokenNotFoundError(
            token=unresolved,
            chain=destination_chain,
            reason=(f"Unable to resolve token decimals for bridge balance normalization (candidates={candidates})"),
        )

    # -------------------------------------------------------------------------
    # Teardown execution
    # -------------------------------------------------------------------------

    async def _execute_teardown(
        self,
        strategy: StrategyProtocol,
        teardown_mode: "TeardownMode",
        start_time: datetime,
    ) -> IterationResult:
        """Execute teardown, routing through TeardownManager when possible.

        For single-chain strategies, delegates to TeardownManager which provides:
        - Position-aware loss caps (1-3% based on position size)
        - Escalating slippage tolerance (tight -> loose with approval gates)
        - Cancel window (configurable, default 10 seconds)
        - Post-execution verification (checks positions are actually closed)
        - State persistence for resumability

        For multi-chain strategies, uses the inline execution path (TeardownManager
        does not yet support multi-chain orchestration).

        Args:
            strategy: The strategy to teardown
            teardown_mode: SOFT (graceful) or HARD (emergency)
            start_time: When the iteration started

        Returns:
            IterationResult with teardown status
        """
        from ..teardown import get_teardown_state_manager

        strategy_id = strategy.strategy_id
        manager = get_teardown_state_manager()
        request = manager.get_active_request(strategy_id)

        # Step T1: Create market snapshot (SAME as normal decide() path)
        teardown_market = None
        try:
            teardown_market = strategy.create_market_snapshot()
            if hasattr(teardown_market, "get_price_oracle_dict"):
                logger.debug(
                    f"Created market snapshot for teardown with prices: "
                    f"{list(teardown_market.get_price_oracle_dict().keys())}"
                )
            else:
                logger.debug("Created multi-chain market snapshot for teardown")
        except Exception as e:
            logger.warning(f"Failed to create market snapshot for teardown: {e}. Continuing without market data.")

        # Step T2: Generate teardown intents WITH market (symmetric with decide(market))
        try:
            try:
                teardown_intents = strategy.generate_teardown_intents(teardown_mode, market=teardown_market)
            except TypeError as exc:
                if "unexpected keyword argument" not in str(exc):
                    raise
                # Backward compat: old-style signature def generate_teardown_intents(self, mode)
                logger.debug(f"Strategy {strategy_id} uses old teardown signature (no market param), falling back")
                teardown_intents = strategy.generate_teardown_intents(teardown_mode)
        except NotImplementedError:
            logger.error(
                f"Strategy {strategy_id} supports_teardown()=True but "
                f"generate_teardown_intents() raises NotImplementedError"
            )
            if request:
                manager.mark_failed(strategy_id, error="generate_teardown_intents not implemented")
            return self._create_error_result(
                strategy_id,
                IterationStatus.STRATEGY_ERROR,
                "generate_teardown_intents not implemented",
                start_time,
            )
        except Exception as e:
            logger.error(f"Failed to generate teardown intents for {strategy_id}: {e}")
            if request:
                manager.mark_failed(strategy_id, error=str(e))
            return self._create_error_result(strategy_id, IterationStatus.STRATEGY_ERROR, str(e), start_time)

        if not teardown_intents:
            logger.info(f"🛑 {strategy_id} teardown complete (no positions to close)")
            if request:
                manager.mark_completed(strategy_id, result={"reason": "no_positions"})
            self.request_shutdown()
            self._record_success()
            return IterationResult(
                status=IterationStatus.TEARDOWN,
                intent=None,
                strategy_id=strategy_id,
                duration_ms=self._calculate_duration_ms(start_time),
            )

        logger.info(f"🛑 {strategy_id} entering TEARDOWN mode ({len(teardown_intents)} intents to execute)")
        if request:
            manager.mark_started(strategy_id, total_positions=len(teardown_intents))

        # Step T2.5: Pre-fetch prices for tokens in teardown intents
        if teardown_market is not None and hasattr(teardown_market, "price"):
            try:
                self._prefetch_teardown_prices(teardown_market, teardown_intents)
            except Exception as e:
                logger.warning(f"Failed to pre-fetch teardown prices: {e}")

        # Note: amount="all" resolution is handled lazily inside _execute_intents
        # (per-intent, just before execution) so staged exits work correctly
        # (e.g., withdraw then swap uses tokens produced by the earlier step).

        # Step T2.7: If all intents were resolved away, teardown is complete
        if not teardown_intents:
            logger.info(f"🛑 {strategy_id} teardown complete (all positions already closed)")
            if request:
                manager.mark_completed(strategy_id, result={"reason": "all_balances_zero"})
            self.request_shutdown()
            self._lifecycle_write_state(strategy_id, "TERMINATED")
            self._record_success()
            return IterationResult(
                status=IterationStatus.TEARDOWN,
                intent=None,
                strategy_id=strategy_id,
                duration_ms=self._calculate_duration_ms(start_time),
            )

        # Step T3: Execute teardown intents
        if self._is_multi_chain:
            # Multi-chain: use inline path (TeardownManager doesn't support multi-chain yet)
            result = await self._execute_multi_chain(
                strategy=strategy,
                intents=teardown_intents,
                start_time=start_time,
                market=teardown_market,
            )
            if result.success:
                result.status = IterationStatus.TEARDOWN
                logger.info(f"🛑 {strategy_id} teardown complete - shutting down strategy runner")
                self.request_shutdown()
                if request:
                    manager.mark_completed(strategy_id, result={"intents": len(teardown_intents)})
            else:
                if request:
                    manager.mark_failed(strategy_id, error=result.error or "execution failed")
            return result
        else:
            # Single-chain: route through TeardownManager for safety guarantees
            return await self._execute_teardown_via_manager(
                strategy=strategy,
                teardown_intents=teardown_intents,
                teardown_mode=teardown_mode,
                teardown_market=teardown_market,
                start_time=start_time,
                request=request,
                state_manager=manager,
            )

    async def _execute_teardown_via_manager(
        self,
        strategy: StrategyProtocol,
        teardown_intents: list,
        teardown_mode: "TeardownMode",
        teardown_market: Any | None,
        start_time: datetime,
        request: Any | None,
        state_manager: Any,
    ) -> IterationResult:
        """Execute single-chain teardown through TeardownManager for full safety.

        TeardownManager provides safety features that the inline path lacks:
        - Position-aware loss caps (1-3% based on portfolio size)
        - Escalating slippage tolerance with operator approval gates
        - Cancel window for operator intervention
        - Post-execution verification (checks positions are closed on-chain)
        - Resumable state persistence

        Falls back to inline sequential execution if TeardownManager cannot
        be initialized (e.g., incompatible orchestrator type).

        Args:
            strategy: The strategy to teardown
            teardown_intents: Pre-resolved teardown intents
            teardown_mode: SOFT (graceful) or HARD (emergency)
            teardown_market: Market snapshot (may be None)
            start_time: When the iteration started
            request: Active teardown request from state manager
            state_manager: Teardown state manager for lifecycle tracking
        """
        import uuid

        from ..teardown import TeardownMode
        from ..teardown.teardown_manager import TeardownManager

        strategy_id = strategy.strategy_id
        mode_str = "graceful" if teardown_mode == TeardownMode.SOFT else "emergency"

        # Build compiler for TeardownManager
        compiler = self._build_teardown_compiler(strategy, teardown_market)
        if compiler is None:
            logger.warning(
                f"Cannot build compiler for TeardownManager — falling back to inline teardown for {strategy_id}"
            )
            return await self._execute_teardown_inline(
                strategy, teardown_intents, teardown_market, start_time, request, state_manager
            )

        # Create TeardownManager with safety features
        teardown_mgr = TeardownManager(
            orchestrator=self.execution_orchestrator,  # type: ignore[arg-type]
            compiler=compiler,
            alert_manager=self.alert_manager,  # type: ignore[arg-type]
        )

        # Execute with TeardownManager safety: loss caps, escalating slippage,
        # cancel window, post-execution verification
        logger.info(
            f"🛑 Routing {strategy_id} teardown through TeardownManager "
            f"(mode={mode_str}, intents={len(teardown_intents)})"
        )

        try:
            # Get positions for safety validation (loss caps).
            # If positions can't be fetched, fall back to inline execution —
            # we must NOT pass an empty portfolio through safety validation
            # as it would trivially pass loss cap checks (3% of $0 = $0).
            try:
                positions = strategy.get_open_positions()
            except Exception as pos_err:
                logger.warning(
                    f"Cannot fetch positions for safety validation — "
                    f"falling back to inline teardown for {strategy_id}: {pos_err}"
                )
                return await self._execute_teardown_inline(
                    strategy, teardown_intents, teardown_market, start_time, request, state_manager
                )

            # Safety validation: check loss caps before execution
            validation = teardown_mgr.safety_guard.validate_teardown_request(positions, teardown_mode)
            if not validation.all_passed:
                logger.error(f"🛑 Teardown safety validation failed: {validation.blocked_reason}")
                if request:
                    state_manager.mark_failed(
                        strategy_id, error=f"Safety validation failed: {validation.blocked_reason}"
                    )
                return self._create_error_result(
                    strategy_id,
                    IterationStatus.STRATEGY_ERROR,
                    f"Teardown safety validation failed: {validation.blocked_reason}",
                    start_time,
                )

            # Persist state for resumability
            teardown_id = f"td_{uuid.uuid4().hex[:12]}"
            teardown_state = await teardown_mgr._persist_state(
                teardown_id=teardown_id,
                strategy=strategy,  # type: ignore[arg-type]
                mode=teardown_mode,
                intents=teardown_intents,
            )

            # Run cancel window — gives operator time to abort
            cancel_result = await teardown_mgr.cancel_window.run_cancel_window(
                teardown_id=teardown_id,
                is_auto_mode=True,
            )
            if cancel_result.was_cancelled:
                logger.info(f"🛑 Teardown {teardown_id} cancelled during window")
                self._record_success()
                return IterationResult(
                    status=IterationStatus.TEARDOWN,
                    intent=None,
                    strategy_id=strategy_id,
                    duration_ms=self._calculate_duration_ms(start_time),
                )

            # Update state to EXECUTING after cancel window
            from ..teardown.models import TeardownStatus

            teardown_state.status = TeardownStatus.EXECUTING
            if teardown_mgr.state_manager:
                await teardown_mgr.state_manager.save_teardown_state(teardown_state)

            # Extract price oracle for accurate compilation during execution
            price_oracle = None
            if teardown_market is not None and hasattr(teardown_market, "get_price_oracle_dict"):
                price_oracle = teardown_market.get_price_oracle_dict() or None

            # Execute intents with escalating slippage
            teardown_result = await teardown_mgr._execute_intents(
                teardown_id=teardown_state.teardown_id,
                strategy=strategy,  # type: ignore[arg-type]
                intents=teardown_intents,
                positions=positions,
                mode=teardown_mode,
                teardown_state=teardown_state,
                is_auto_mode=True,
                price_oracle=price_oracle,
                market=teardown_market,
            )

            # Post-execution verification: check positions are actually closed.
            # Fail closed: if verification raises, treat teardown as failed to
            # avoid reporting success while positions may still be open.
            try:
                positions_closed = await teardown_mgr._verify_closure(strategy)  # type: ignore[arg-type]
                if not positions_closed:
                    # Log warning but don't fail — some strategies have advisory
                    # get_open_positions() that doesn't reflect on-chain state.
                    # Matches TeardownManager.execute() which also doesn't fail on this.
                    logger.warning(
                        f"Post-teardown verification: {strategy_id} still reports open positions "
                        f"(may be advisory — check strategy's get_open_positions())"
                    )
            except Exception as verify_err:
                logger.error(f"Post-teardown verification failed: {verify_err}")
                if request:
                    state_manager.mark_failed(strategy_id, error=f"Post-teardown verification failed: {verify_err}")
                return self._create_error_result(
                    strategy_id,
                    IterationStatus.STRATEGY_ERROR,
                    f"Post-teardown verification failed: {verify_err}",
                    start_time,
                )

            # Send completion alert
            if teardown_mgr.alert_manager and teardown_result.success:
                try:
                    await teardown_mgr.alert_manager.send_teardown_complete(teardown_result)
                except Exception as alert_err:
                    logger.warning(f"Failed to send teardown completion alert: {alert_err}")

            # Clean up persisted state on success
            if teardown_mgr.state_manager and teardown_result.success:
                try:
                    await teardown_mgr.state_manager.delete_teardown_state(teardown_id)
                except Exception as cleanup_err:
                    logger.warning(f"Failed to clean up teardown state: {cleanup_err}")

        except Exception as e:
            logger.error(f"🛑 TeardownManager execution failed for {strategy_id}: {e}")
            if request:
                state_manager.mark_failed(strategy_id, error=str(e))
            return self._create_error_result(strategy_id, IterationStatus.STRATEGY_ERROR, str(e), start_time)

        # Map TeardownResult -> IterationResult
        if teardown_result.success:
            logger.info(
                f"🛑 {strategy_id} teardown complete via TeardownManager "
                f"({teardown_result.intents_succeeded}/{teardown_result.intents_total} intents, "
                f"{teardown_result.duration_seconds:.1f}s)"
            )
            self.request_shutdown()
            self._lifecycle_write_state(strategy_id, "TERMINATED")
            if request:
                state_manager.mark_completed(
                    strategy_id,
                    result={
                        "intents": teardown_result.intents_succeeded,
                        "mode": mode_str,
                        "duration_s": teardown_result.duration_seconds,
                    },
                )
            self._record_success()
            return IterationResult(
                status=IterationStatus.TEARDOWN,
                intent=None,
                strategy_id=strategy_id,
                duration_ms=self._calculate_duration_ms(start_time),
            )
        else:
            logger.warning(f"🛑 {strategy_id} teardown incomplete via TeardownManager: {teardown_result.error}")
            if request:
                state_manager.mark_failed(strategy_id, error=teardown_result.error or "teardown failed")
            return IterationResult(
                status=IterationStatus.STRATEGY_ERROR,
                error=teardown_result.error,
                strategy_id=strategy_id,
                duration_ms=self._calculate_duration_ms(start_time),
            )

    async def _execute_teardown_inline(
        self,
        strategy: StrategyProtocol,
        teardown_intents: list,
        teardown_market: Any | None,
        start_time: datetime,
        request: Any | None,
        state_manager: Any,
    ) -> IterationResult:
        """Fallback inline teardown execution (no TeardownManager safety features).

        Used when TeardownManager cannot be initialized (e.g., incompatible
        orchestrator type or missing compiler dependencies).

        Executes teardown intents sequentially via _execute_single_chain.
        """
        strategy_id = strategy.strategy_id

        all_success = True
        last_result = None
        for i, intent in enumerate(teardown_intents):
            logger.info(f"🛑 Executing teardown intent {i + 1}/{len(teardown_intents)}: {intent.intent_type.value}")

            # Resolve amount="all" to actual wallet balance before execution.
            # Only resolve for intents with a token balance field (e.g., SwapIntent.from_token).
            # Intents like vault_redeem(shares="all") are handled natively by the compiler.
            intent_to_execute = intent
            if Intent.has_chained_amount(intent):
                balance_token = (
                    getattr(intent, "from_token", None)
                    or getattr(intent, "token", None)
                    or getattr(intent, "token_in", None)
                )
                if balance_token and teardown_market is not None:
                    # Resolve balance — pass chain for multi-chain market snapshots
                    intent_chain = getattr(intent, "chain", None)
                    try:
                        if intent_chain:
                            bal = teardown_market.balance(balance_token, intent_chain)
                        else:
                            bal = teardown_market.balance(balance_token)
                    except TypeError:
                        # Single-chain MarketSnapshot doesn't accept chain param
                        bal = teardown_market.balance(balance_token)
                    except Exception as e:  # noqa: BLE001
                        logger.error(
                            f"🛑 Teardown intent {i + 1}: failed to resolve balance for {balance_token}: {e}. "
                            f"Token may be missing from the registry. Position may remain open."
                        )
                        all_success = False
                        last_result = IterationResult(
                            status=IterationStatus.COMPILATION_FAILED,
                            intent=intent,
                            error=f"Cannot resolve amount='all' for {balance_token}: {e}",
                            strategy_id=strategy_id,
                            duration_ms=self._calculate_duration_ms(start_time),
                        )
                        break
                    # MarketSnapshot.balance() returns Decimal; IntentStrategy.balance() returns TokenBalance
                    balance_value = bal.balance if hasattr(bal, "balance") else bal
                    if balance_value <= 0:
                        logger.info(
                            f"🛑 Teardown intent {i + 1}: {balance_token} balance is 0, skipping (already closed)"
                        )
                        continue
                    intent_to_execute = Intent.set_resolved_amount(intent, balance_value)
                    logger.info(f"🛑 Resolved amount='all' for {balance_token}: {balance_value}")
                elif balance_token and teardown_market is None:
                    # Have a token to resolve but no market — log warning, let compiler try
                    logger.warning(
                        f"🛑 Teardown intent {i + 1}: amount='all' for {balance_token} but no market context. "
                        f"Passing to compiler as-is — compilation may fail."
                    )
                else:
                    # No token field — let compiler handle natively (e.g., shares="all")
                    logger.debug(f"🛑 Teardown intent {i + 1}: no token field, passing to compiler as-is")

            result = await self._execute_single_chain(
                strategy=strategy,
                intent=intent_to_execute,
                start_time=start_time,
                total_intents=1,
                market=teardown_market,
            )
            last_result = result
            if not result.success:
                all_success = False
                logger.error(f"🛑 Teardown intent {i + 1} failed: {result.error}")
                break  # Stop on first failure

        if last_result:
            if all_success:
                last_result.status = IterationStatus.TEARDOWN
                logger.info(f"🛑 {strategy_id} teardown complete - shutting down strategy runner")
                self.request_shutdown()
                self._lifecycle_write_state(strategy_id, "TERMINATED")
                self._record_success()
                if request:
                    state_manager.mark_completed(strategy_id, result={"intents": len(teardown_intents)})
            else:
                logger.warning(f"🛑 {strategy_id} teardown incomplete - manual intervention may be required")
                if request:
                    state_manager.mark_failed(strategy_id, error=last_result.error or "execution failed")
            return last_result

        # Edge case: no intents executed (all positions already closed)
        logger.info(f"🛑 {strategy_id} teardown: all positions already closed, shutting down")
        self.request_shutdown()
        self._lifecycle_write_state(strategy_id, "TERMINATED")
        self._record_success()
        if request:
            state_manager.mark_completed(strategy_id, result={"reason": "all_positions_already_closed"})
        return IterationResult(
            status=IterationStatus.TEARDOWN,
            intent=None,
            strategy_id=strategy_id,
            duration_ms=self._calculate_duration_ms(start_time),
        )

    def _build_teardown_compiler(
        self,
        strategy: StrategyProtocol,
        market: Any | None,
    ) -> "IntentCompiler | None":
        """Build an IntentCompiler for TeardownManager teardown execution.

        Returns None if compiler cannot be built (e.g., missing RPC access).
        """
        from ..execution.gateway_orchestrator import GatewayExecutionOrchestrator

        gateway_client = None
        rpc_url = None

        if isinstance(self.execution_orchestrator, GatewayExecutionOrchestrator):
            gateway_client = self.execution_orchestrator._client
        else:
            rpc_url = getattr(self.execution_orchestrator, "rpc_url", None)

        # Extract prices from market snapshot
        price_oracle = None
        if market is not None and hasattr(market, "get_price_oracle_dict"):
            price_oracle = market.get_price_oracle_dict() or None

        try:
            compiler_config = IntentCompilerConfig(
                allow_placeholder_prices=price_oracle is None,
            )
            return IntentCompiler(
                chain=strategy.chain,
                wallet_address=strategy.wallet_address,
                rpc_url=rpc_url,
                price_oracle=price_oracle,
                config=compiler_config,
                gateway_client=gateway_client,
            )
        except Exception as e:
            logger.warning(f"Failed to build teardown compiler: {e}")
            return None

    @staticmethod
    def _prefetch_teardown_prices(market: Any, intents: list) -> None:
        """Eagerly fetch prices for tokens referenced in teardown intents.

        MarketSnapshot uses lazy loading — prices only populate when market.price()
        is called. During teardown, generate_teardown_intents() typically doesn't call
        market.price(), so get_price_oracle_dict() returns {} and the compiler falls
        back to placeholder prices. This method pre-populates the cache.

        Teardown intents often reference tokens by address (e.g. 0xdefa1d...) rather
        than symbol. market.price() expects a symbol, so we resolve addresses to
        symbols first using the token resolver. Without this, tokens like ALMANAK
        (not in CoinGecko/Chainlink) fail price resolution during teardown.
        """
        token_attrs = ("from_token", "to_token", "token", "collateral_token", "borrow_token", "token_in")
        tokens: set[str] = set()
        for intent in intents:
            for attr in token_attrs:
                val = getattr(intent, attr, None)
                if val and isinstance(val, str):
                    tokens.add(val)

        if not tokens:
            return

        # Resolve addresses to symbols so market.price() can look them up.
        # market.price() expects symbols (e.g. "ALMANAK"), not addresses.
        chain = getattr(market, "_chain", None) or getattr(market, "chain", None)
        address_to_symbol: dict[str, str] = {}
        if chain:
            try:
                from almanak.framework.data.tokens import get_token_resolver

                resolver = get_token_resolver()
                for token in tokens:
                    if token.startswith("0x") and len(token) == 42:
                        try:
                            resolved = resolver.resolve(token, chain, log_errors=False, skip_gateway=True)
                            address_to_symbol[token] = resolved.symbol
                        except Exception as e:
                            logger.debug(f"Could not resolve teardown token address {token} to symbol: {e}")
            except Exception as e:
                logger.debug(f"Token resolver unavailable for teardown prefetch: {e}")

        fetched = []
        for token in sorted(tokens):
            # Try the symbol if we resolved the address, otherwise try the raw value
            symbol = address_to_symbol.get(token, token)
            try:
                market.price(symbol)
                fetched.append(symbol)
            except Exception:
                # If symbol lookup failed and we have the original address, try that too
                if symbol != token:
                    try:
                        market.price(token)
                        fetched.append(token)
                    except Exception:
                        logger.debug(f"Could not pre-fetch price for teardown token {token} (symbol={symbol})")
                else:
                    logger.debug(f"Could not pre-fetch price for teardown token {token}")

        if fetched:
            logger.info(f"Pre-fetched {len(fetched)} teardown prices: {fetched}")

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

    async def _reconcile_post_execution_balances(
        self,
        strategy: StrategyProtocol,
        intent: AnyIntent,
        execution_result: ExecutionResult | None,
    ) -> dict[str, Any] | None:
        """Verify post-execution token balances match intent expectations.

        After a successful on-chain execution, queries actual balances and
        compares with expected token flow direction. Returns a reconciliation
        dict with results, or None if reconciliation couldn't be performed.

        This is a safety net: even if the transaction receipt looks fine,
        the on-chain balance should reflect the expected token movements.
        """
        try:
            # Extract tokens from the intent
            tokens = self._extract_intent_tokens(intent)
            if not tokens:
                return None

            # Query current balances for involved tokens
            post_balances: dict[str, Decimal] = {}
            for token_symbol in tokens:
                try:
                    bal = await self.balance_provider.get_balance(token_symbol)
                    post_balances[token_symbol] = bal.balance
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Balance reconciliation: failed to fetch %s balance: %s", token_symbol, exc)
                    # Balance query failures are non-fatal for reconciliation
                    continue

            if not post_balances:
                return None

            # Build reconciliation report
            recon: dict[str, Any] = {
                "tokens_checked": list(post_balances.keys()),
                "post_balances": {k: str(v) for k, v in post_balances.items()},
                "warnings": [],
            }

            # For swaps, verify directional correctness using enriched swap_amounts
            if execution_result and execution_result.swap_amounts:
                sa = execution_result.swap_amounts
                if sa.amount_out_decimal is not None and sa.amount_out_decimal <= 0:
                    recon["warnings"].append(f"Swap output amount is zero or negative: {sa.amount_out_decimal}")
                if sa.amount_in_decimal is not None and sa.amount_in_decimal <= 0:
                    recon["warnings"].append(f"Swap input amount is zero or negative: {sa.amount_in_decimal}")

            if recon["warnings"]:
                logger.warning(
                    "Balance reconciliation warnings for %s: %s",
                    strategy.strategy_id,
                    recon["warnings"],
                )

            return recon

        except Exception as e:
            logger.debug(f"Balance reconciliation skipped: {e}")
            return None

    @staticmethod
    def _extract_intent_tokens(intent: AnyIntent) -> list[str]:
        """Extract token symbols involved in an intent."""
        tokens: list[str] = []
        # SwapIntent
        if hasattr(intent, "from_token") and hasattr(intent, "to_token"):
            tokens.extend([intent.from_token, intent.to_token])
        # LP intents
        elif hasattr(intent, "token0") and hasattr(intent, "token1"):
            tokens.extend([intent.token0, intent.token1])
        # Supply/Borrow intents
        elif hasattr(intent, "token"):
            tokens.append(intent.token)
        return tokens

    def _record_success(self, *, execution_proved: bool = False) -> None:
        """Record a successful iteration in metrics and circuit breaker.

        Args:
            execution_proved: True when an actual on-chain execution succeeded.
                Only execution-proved successes count toward closing a HALF_OPEN
                circuit breaker, so HOLD/DRY_RUN cannot prematurely close the
                breaker without proving the execution path works.
        """
        self._total_iterations += 1
        self._successful_iterations += 1
        self._consecutive_errors = 0
        if self._circuit_breaker is not None and execution_proved:
            self._circuit_breaker.record_success()

    def _calculate_duration_ms(self, start_time: datetime) -> float:
        """Calculate duration in milliseconds since start_time."""
        elapsed = datetime.now(UTC) - start_time
        return elapsed.total_seconds() * 1000

    async def _detect_stuck_and_alert(self, strategy: StrategyProtocol, result: IterationResult) -> None:
        """Run stuck detection on a failed iteration and generate an OperatorCard if stuck.

        Lazy-initializes StuckDetector and OperatorCardGenerator on first call to
        avoid import overhead on every iteration.

        Args:
            strategy: The strategy that failed
            result: The failed iteration result
        """
        try:
            # Lazy import and init to avoid overhead on the happy path
            if self._stuck_detector is None:
                from ..services.stuck_detector import StuckDetector

                self._stuck_detector = StuckDetector(emit_events=True)

            if self._operator_card_generator is None:
                from ..services.operator_card_generator import OperatorCardGenerator

                self._operator_card_generator = OperatorCardGenerator()

            from ..services.stuck_detector import StrategySnapshot

            # Build a lightweight snapshot from available runner state
            state_entered_at = self._first_error_at or datetime.now(UTC)
            snapshot = StrategySnapshot(
                strategy_id=strategy.strategy_id,
                chain=getattr(strategy, "chain", "unknown"),
                current_state=result.status.value,
                state_entered_at=state_entered_at,
                pending_transactions=[],
                circuit_breaker_triggered=(
                    self._circuit_breaker is not None and self._circuit_breaker.state.value != "closed"
                ),
            )

            detection = self._stuck_detector.detect_stuck(snapshot)
            if not detection.is_stuck:
                return

            logger.warning(
                "StuckDetector: %s is stuck (reason=%s, duration=%.0fs)",
                strategy.strategy_id,
                detection.reason.value if detection.reason else "unknown",
                detection.time_in_state_seconds,
            )

            # Generate OperatorCard
            from ..services.operator_card_generator import ErrorContext, StrategyState

            strategy_state = StrategyState(
                strategy_id=strategy.strategy_id,
                status="stuck",
                total_value_usd=Decimal("0"),
                available_balance_usd=Decimal("0"),
                stuck_since=state_entered_at,
            )
            error_context = ErrorContext(
                error_type=result.status.value,
                error_message=result.error or "unknown",
            )
            card = self._operator_card_generator.generate_card(
                strategy_state=strategy_state,
                error_context=error_context,
            )

            # Route card to AlertManager
            if self.alert_manager is not None:
                try:
                    await self.alert_manager.send_alert(card)
                except Exception as alert_err:
                    logger.debug("Failed to send stuck alert (non-fatal): %s", alert_err)

        except Exception as e:
            # Stuck detection is non-fatal — never block the runner
            logger.debug("Stuck detection failed (non-fatal): %s", e)

    def _emit_iteration_summary(self, result: IterationResult, chain: str | None = None) -> None:
        """Emit a structured iteration_summary log record for JSONL analysis.

        This provides a single, machine-readable record per iteration containing
        all key fields needed for post-hoc analysis by AI agents or dashboards.
        """
        # Extract intent info
        intent_type = None
        intents_serialized: list[dict[str, Any]] = []
        if result.intent:
            intent_type = result.intent.intent_type.value if hasattr(result.intent, "intent_type") else None
            try:
                intents_serialized = [result.intent.serialize()] if hasattr(result.intent, "serialize") else []
            except Exception:  # noqa: BLE001
                logger.debug("Failed to serialize intent for iteration_summary", exc_info=True)

        # Extract execution info
        tx_hashes: list[str] = []
        txs_planned = 0
        txs_sent = 0
        if result.execution_result:
            er = result.execution_result
            if hasattr(er, "transaction_results") and er.transaction_results:
                tx_hashes = [tr.tx_hash for tr in er.transaction_results if hasattr(tr, "tx_hash") and tr.tx_hash]
                txs_sent = len(tx_hashes)
            if hasattr(er, "tx_hashes") and er.tx_hashes:
                tx_hashes = tx_hashes or er.tx_hashes
                txs_sent = txs_sent or len(er.tx_hashes)
            # txs_planned: count from action bundle if available
            if hasattr(er, "receipts"):
                txs_planned = max(txs_planned, len(er.receipts))
            txs_planned = max(txs_planned, txs_sent)

        logger.info(
            "iteration_summary",
            extra={
                "event_type": "iteration_summary",
                "strategy_id": result.strategy_id,
                "chain": chain,
                "iteration": self._total_iterations,
                "decision": intent_type,
                "intents": intents_serialized,
                "dry_run": self.config.dry_run,
                "txs_planned": txs_planned,
                "txs_sent": txs_sent,
                "tx_hashes": tx_hashes,
                "status": result.status.value,
                "duration_ms": round(result.duration_ms, 1),
                "error": result.error,
            },
        )

    async def _is_strategy_paused(self, strategy_id: str) -> tuple[bool, str | None]:
        """Check persisted control state to determine if strategy is paused."""
        try:
            state_obj = await self.state_manager.load_state(strategy_id)
        except Exception as e:  # noqa: BLE001
            # Fail-open by design: if state is temporarily unavailable, continue strategy execution.
            logger.warning("Unable to load pause state for %s; continuing as unpaused: %s", strategy_id, e)
            return False, None

        if state_obj is None or not isinstance(state_obj.state, dict):
            return False, None

        state = state_obj.state
        if not bool(state.get("is_paused", False)):
            return False, None

        reason = state.get("pause_reason")
        return True, str(reason) if isinstance(reason, str) and reason else None

    async def _update_state(
        self,
        strategy_id: str,
        result: IterationResult,
    ) -> None:
        """Update persisted state after an iteration."""
        try:
            # Try to load current state, create new if not found
            try:
                state = await self.state_manager.load_state(strategy_id)
                # GatewayStateManager returns None instead of raising StateNotFoundError
                if state is None:
                    raise StateNotFoundError(strategy_id)
                expected_version = state.version
            except StateNotFoundError:
                # First run - create new state
                state = StateData(
                    strategy_id=strategy_id,
                    version=1,
                    state={},
                )
                expected_version = None  # No version check for new state
                logger.debug(f"Creating initial state for {strategy_id}")

            # Update state with iteration info
            state.state["last_iteration"] = {
                "timestamp": result.timestamp.isoformat(),
                "status": result.status.value,
                "intent_type": result.intent.intent_type.value if result.intent else None,
                "duration_ms": result.duration_ms,
            }
            state.state["total_iterations"] = self._total_iterations
            state.state["successful_iterations"] = self._successful_iterations
            state.state["consecutive_errors"] = self._consecutive_errors

            # Save with CAS (or create if new)
            await self.state_manager.save_state(state, expected_version=expected_version)

            logger.debug(f"State updated for {strategy_id}")

        except Exception as e:
            logger.error(f"Failed to update state for {strategy_id}: {e}")

    async def _persist_copy_trading_state(
        self,
        strategy_id: str,
        activity_provider: StatefulActivityProviderProtocol,
    ) -> None:
        """Persist copy trading cursor state into the strategy state dict."""
        try:
            state = await self.state_manager.load_state(strategy_id)
            if state is None:
                return
            expected_version = state.version
            state.state["copy_trading_state"] = activity_provider.get_state()
            await self.state_manager.save_state(state, expected_version=expected_version)
            logger.debug("Copy trading state persisted")
        except Exception as e:
            logger.warning(f"Failed to persist copy trading state: {e}")

    async def _persist_vault_state(
        self,
        strategy_id: str,
        vault_state_dict: dict,
        vault_state_key: str,
    ) -> None:
        """Persist vault lifecycle state into the strategy state dict."""
        try:
            state = await self.state_manager.load_state(strategy_id)
            if state is None:
                # First run -- create state so vault lifecycle is not lost
                state = StateData(
                    strategy_id=strategy_id,
                    version=1,
                    state={},
                )
                expected_version = None
            else:
                expected_version = state.version
            state.state[vault_state_key] = vault_state_dict
            await self.state_manager.save_state(state, expected_version=expected_version)
            logger.debug("Vault state persisted (phase=%s)", vault_state_dict.get("settlement_phase", "?"))
        except Exception as e:
            logger.warning(f"Failed to persist vault state: {e}")

    async def _capture_portfolio_snapshot(
        self,
        strategy: StrategyProtocol,
        iteration_number: int,
    ) -> PortfolioSnapshot | None:
        """Capture and persist portfolio snapshot after iteration.

        This method:
        1. Calls strategy.get_portfolio_snapshot() if available
        2. Stores in portfolio_snapshots table for dashboard/PnL charts
        3. Initializes portfolio_metrics on first run for baseline tracking

        Portfolio snapshots are captured at a configurable interval (default 5 min)
        to avoid storing excessive data while providing good chart resolution.

        Args:
            strategy: The strategy to capture snapshot from
            iteration_number: Current iteration count

        Returns:
            PortfolioSnapshot if captured, None if skipped or not supported
        """
        # Check if strategy supports get_portfolio_snapshot
        if not hasattr(strategy, "get_portfolio_snapshot"):
            return None

        now = datetime.now(UTC)

        # Rate-limit snapshot persistence (store every 5 min for time-series)
        if self._last_snapshot_time is not None:
            elapsed = (now - self._last_snapshot_time).total_seconds()
            if elapsed < self._snapshot_interval_seconds:
                return None

        try:
            # Get snapshot from strategy
            snapshot = strategy.get_portfolio_snapshot()
            if snapshot is None:
                return None

            # Set iteration number
            snapshot.iteration_number = iteration_number

            # Persist snapshot
            snapshot_id = await self.state_manager.save_portfolio_snapshot(snapshot)
            if snapshot_id > 0:
                self._last_snapshot_time = now
                logger.debug(
                    f"Portfolio snapshot captured for {strategy.strategy_id}: "
                    f"${snapshot.total_value_usd:.2f} (id={snapshot_id})"
                )

            # Initialize or update portfolio metrics for PnL tracking
            await self._update_portfolio_metrics(strategy.strategy_id, snapshot)

            return snapshot

        except Exception as e:
            logger.warning(f"Failed to capture portfolio snapshot: {e}")
            return None

    async def _update_portfolio_metrics(
        self,
        strategy_id: str,
        snapshot: PortfolioSnapshot,
    ) -> None:
        """Update portfolio metrics for PnL tracking.

        On first run, stores initial_value_usd as baseline for PnL calculation.
        This baseline survives restarts for accurate cumulative PnL.

        Args:
            strategy_id: Strategy identifier
            snapshot: Current portfolio snapshot
        """
        try:
            # Skip if state manager doesn't support portfolio metrics (e.g., GatewayStateManager)
            if not hasattr(self.state_manager, "get_portfolio_metrics"):
                return

            # Skip if snapshot value is unavailable (would seed bad baseline)
            if snapshot.error or snapshot.value_confidence == ValueConfidence.UNAVAILABLE:
                logger.info(f"Skipping portfolio metrics update for {strategy_id}: snapshot unavailable")
                return

            # Get existing metrics (may be None on first run)
            existing = await self.state_manager.get_portfolio_metrics(strategy_id)

            if existing is None:
                # First run - establish baseline
                metrics = PortfolioMetrics(
                    strategy_id=strategy_id,
                    timestamp=snapshot.timestamp,
                    total_value_usd=snapshot.total_value_usd,
                    initial_value_usd=snapshot.total_value_usd,
                )
                await self.state_manager.save_portfolio_metrics(metrics)
                logger.info(f"Portfolio baseline established for {strategy_id}: ${snapshot.total_value_usd:.2f}")
            else:
                # Update current value (preserve initial_value)
                existing.timestamp = snapshot.timestamp
                existing.total_value_usd = snapshot.total_value_usd
                await self.state_manager.save_portfolio_metrics(existing)

        except Exception as e:
            logger.warning(f"Failed to update portfolio metrics: {e}")

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
                    total_value_usd=Decimal("0"),
                    available_balance_usd=Decimal("0"),
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
                        total_value_usd=Decimal("0"),
                        available_balance_usd=Decimal("0"),
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
                    total_value_usd=Decimal("0"),
                    available_balance_usd=Decimal("0"),
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
                        total_value_usd=Decimal("0"),
                        available_balance_usd=Decimal("0"),
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
        except Exception as e:
            logger.error(f"Failed to trigger emergency stop for {strategy.strategy_id}: {e}")

    def get_metrics(self) -> dict[str, Any]:
        """Get current runner metrics.

        Returns:
            Dictionary with iteration counts, error counts, and success rate
        """
        success_rate = self._successful_iterations / self._total_iterations if self._total_iterations > 0 else 0.0

        return {
            "total_iterations": self._total_iterations,
            "successful_iterations": self._successful_iterations,
            "consecutive_errors": self._consecutive_errors,
            "success_rate": success_rate,
            "shutdown_requested": self._shutdown_requested,
        }

    # =========================================================================
    # Startup Recovery
    # =========================================================================

    async def _recover_incomplete_sessions(self) -> int:
        """Recover incomplete execution sessions on startup.

        Scans for sessions that were interrupted (e.g., due to crash) and
        attempts to recover them based on their phase:

        - SUBMITTED phase: Poll for receipt - the transaction may have been
          mined. If confirmed, update state; if failed/not found, mark complete.
        - SIGNING/PREPARING phase: Safe to abandon - no on-chain state change
          occurred. Mark as failed so they can be retried from scratch.
        - CONFIRMING phase: Poll for receipt like SUBMITTED.

        Duplicate transaction prevention:
        - Track recovered tx_hashes and nonces to prevent re-execution
        - If a transaction was already submitted, we skip re-submission

        Returns:
            Number of sessions recovered
        """
        if self._session_store is None:
            logger.debug("Session store not configured, skipping recovery")
            return 0

        incomplete_sessions = self._session_store.get_incomplete_sessions()

        if not incomplete_sessions:
            logger.info("No incomplete sessions found for recovery")
            return 0

        logger.info(f"Found {len(incomplete_sessions)} incomplete sessions for recovery")

        recovered_count = 0

        for session in incomplete_sessions:
            try:
                recovered = await self._recover_session(session)
                if recovered:
                    recovered_count += 1
            except Exception as e:
                logger.error(
                    f"Recovery failed for session {session.session_id}: {e}",
                    extra={"session_id": session.session_id},
                )
                # Mark session as failed to prevent infinite recovery attempts
                session.set_error(f"Recovery failed: {e}")
                session.mark_complete(success=False)
                self._session_store.save(session)

        logger.info(f"Recovered {recovered_count}/{len(incomplete_sessions)} sessions")
        return recovered_count

    async def _recover_session(self, session: ExecutionSession) -> bool:
        """Recover a single incomplete execution session.

        Args:
            session: The session to recover

        Returns:
            True if session was successfully recovered
        """
        logger.info(
            f"Recovering session {session.session_id} "
            f"(strategy={session.strategy_id}, phase={session.phase.value}, "
            f"attempt={session.attempt_number})"
        )

        # Track nonces from this session for duplicate prevention
        strategy_id = session.strategy_id
        if strategy_id not in self._recovered_nonces:
            self._recovered_nonces[strategy_id] = set()

        for tx_state in session.transactions:
            if tx_state.tx_hash:
                self._recovered_tx_hashes.add(tx_state.tx_hash)
            if tx_state.nonce > 0:
                self._recovered_nonces[strategy_id].add(tx_state.nonce)

        # Handle based on session phase
        if session.phase in (SessionPhase.SUBMITTED, SessionPhase.CONFIRMING):
            # Transaction was submitted - poll for receipt
            return await self._recover_submitted_session(session)
        elif session.phase in (SessionPhase.PREPARING, SessionPhase.SIGNING):
            # No on-chain activity yet - safe to abandon
            return await self._recover_early_phase_session(session)
        else:
            logger.warning(f"Unknown phase {session.phase.value} for session {session.session_id}")
            return False

    async def _recover_submitted_session(self, session: ExecutionSession) -> bool:
        """Recover a session that was in SUBMITTED or CONFIRMING phase.

        For submitted transactions, we poll for receipts to determine
        the final outcome. The transaction may have:
        - Succeeded (CONFIRMED)
        - Failed/reverted (FAILED)
        - Been dropped from mempool (not found)

        Args:
            session: Session with submitted transactions

        Returns:
            True if recovery completed successfully
        """
        if self._session_store is None:
            return False

        # Get tx_hashes to poll
        tx_hashes = [tx.tx_hash for tx in session.transactions if tx.tx_hash]

        if not tx_hashes:
            logger.warning(
                f"Session {session.session_id} in {session.phase.value} but no tx_hashes found - marking as failed"
            )
            session.set_error("No transaction hashes found for submitted session")
            session.mark_complete(success=False)
            self._session_store.save(session)
            return True

        logger.info(f"Polling {len(tx_hashes)} transactions for session {session.session_id}")

        # Poll for receipts via the submitter
        # Note: Session recovery currently only supports single-chain mode
        # Multi-chain recovery would require additional chain tracking in sessions
        if self._is_multi_chain:
            logger.warning(
                f"Session recovery not yet supported in multi-chain mode. "
                f"Marking session {session.session_id} as failed."
            )
            session.set_error("Session recovery not supported in multi-chain mode")
            session.mark_complete(success=False)
            self._session_store.save(session)
            return True

        # Single-chain mode - get submitter from orchestrator
        single_chain_orch = cast(ExecutionOrchestrator, self.execution_orchestrator)
        submitter = single_chain_orch.submitter

        try:
            # Poll with a shorter timeout for recovery (30s instead of 120s)
            receipts = await submitter.get_receipts(tx_hashes, timeout=30.0)

            # Update session with receipt results
            all_confirmed = True
            any_failed = False

            for receipt in receipts:
                tx_status = TransactionStatus.CONFIRMED if receipt.success else TransactionStatus.FAILED

                session.update_transaction(
                    tx_hash=receipt.tx_hash,
                    status=tx_status,
                    gas_used=receipt.gas_used,
                    block_number=receipt.block_number,
                )

                if receipt.success:
                    logger.info(
                        f"Recovered tx {receipt.tx_hash}: CONFIRMED in block {receipt.block_number}",
                        extra={"session_id": session.session_id, "tx_hash": receipt.tx_hash},
                    )
                else:
                    logger.warning(
                        f"Recovered tx {receipt.tx_hash}: FAILED/REVERTED",
                        extra={"session_id": session.session_id, "tx_hash": receipt.tx_hash},
                    )
                    all_confirmed = False
                    any_failed = True

            # Mark session complete based on results
            success = all_confirmed and not any_failed
            session.mark_complete(success=success)
            self._session_store.save(session)

            logger.info(
                f"Session {session.session_id} recovery complete: success={success}",
                extra={"session_id": session.session_id},
            )

            # Update strategy state if recovery was successful
            if success:
                await self._update_recovered_state(session)

            return True

        except TimeoutError:
            # Transaction not found in time - may have been dropped
            logger.warning(
                f"Timeout polling receipts for session {session.session_id} - transactions may have been dropped",
                extra={"session_id": session.session_id},
            )
            session.set_error("Timeout waiting for transaction receipts during recovery")
            session.mark_complete(success=False)
            self._session_store.save(session)
            return True

        except Exception as e:
            logger.error(
                f"Error polling receipts for session {session.session_id}: {e}",
                extra={"session_id": session.session_id},
            )
            raise

    async def _recover_early_phase_session(self, session: ExecutionSession) -> bool:
        """Recover a session that was in PREPARING or SIGNING phase.

        These sessions haven't submitted any transactions on-chain,
        so it's safe to simply mark them as failed and let the
        strategy retry from scratch on the next iteration.

        Args:
            session: Session in early phase

        Returns:
            True if recovery completed
        """
        if self._session_store is None:
            return False

        logger.info(
            f"Session {session.session_id} was in {session.phase.value} phase - "
            f"no on-chain activity, marking as failed for retry",
            extra={"session_id": session.session_id},
        )

        session.set_error(f"Session interrupted in {session.phase.value} phase - no on-chain activity, safe to retry")
        session.mark_complete(success=False)
        self._session_store.save(session)

        return True

    async def _update_recovered_state(self, session: ExecutionSession) -> None:
        """Update strategy state after successful session recovery.

        This ensures the strategy's state reflects the recovered execution,
        preventing the strategy from retrying already-completed actions.

        Args:
            session: Successfully recovered session
        """
        try:
            state = await self.state_manager.load_state(session.strategy_id)
            # GatewayStateManager returns None instead of raising StateNotFoundError
            if state is None:
                logger.debug(f"No state found for {session.strategy_id} during recovery marking")
                return

            # Record the recovered session in state
            recovered_sessions = state.state.get("recovered_sessions", [])
            recovered_sessions.append(
                {
                    "session_id": session.session_id,
                    "intent_id": session.intent_id,
                    "recovered_at": datetime.now(UTC).isoformat(),
                    "transactions": [{"tx_hash": tx.tx_hash, "status": tx.status.value} for tx in session.transactions],
                }
            )
            state.state["recovered_sessions"] = recovered_sessions

            await self.state_manager.save_state(state, expected_version=state.version)

            logger.debug(
                f"Updated state for strategy {session.strategy_id} with recovered session {session.session_id}"
            )

        except Exception as e:
            logger.error(
                f"Failed to update state after session recovery: {e}",
                extra={"session_id": session.session_id},
            )

    def is_duplicate_transaction(
        self,
        tx_hash: str | None = None,
        nonce: int | None = None,
        strategy_id: str | None = None,
    ) -> bool:
        """Check if a transaction would be a duplicate of a recovered session.

        This is used to prevent re-submitting transactions that were
        already submitted before a crash.

        Args:
            tx_hash: Transaction hash to check
            nonce: Transaction nonce to check
            strategy_id: Strategy ID for nonce check

        Returns:
            True if transaction would be a duplicate
        """
        if tx_hash and tx_hash in self._recovered_tx_hashes:
            logger.warning(f"Transaction {tx_hash} was already recovered - skipping to prevent duplicate")
            return True

        if nonce is not None and strategy_id:
            recovered_nonces = self._recovered_nonces.get(strategy_id, set())
            if nonce in recovered_nonces:
                logger.warning(
                    f"Nonce {nonce} for strategy {strategy_id} was already used "
                    f"in a recovered session - skipping to prevent duplicate"
                )
                return True

        return False

    # =========================================================================
    # Execution Progress Management (for resuming after restart)
    # =========================================================================

    def _compute_intents_hash(self, intents: list[AnyIntent]) -> str:
        """Compute a hash of intents to detect if they changed.

        Args:
            intents: List of intents to hash

        Returns:
            SHA256 hash of serialized intents
        """
        # Serialize intents to JSON-like string
        serialized = []
        for intent in intents:
            serialized.append(intent.serialize() if hasattr(intent, "serialize") else str(intent))
        intent_str = json.dumps(serialized, sort_keys=True, default=str)
        return hashlib.sha256(intent_str.encode()).hexdigest()[:16]

    async def _load_execution_progress(self, strategy_id: str) -> ExecutionProgress | None:
        """Load execution progress from persisted state.

        Args:
            strategy_id: Strategy identifier

        Returns:
            ExecutionProgress if found, None otherwise
        """
        try:
            state = await self.state_manager.load_state(strategy_id)
            # GatewayStateManager returns None instead of raising StateNotFoundError
            if state is None:
                return None
            progress_data = state.state.get("execution_progress")
            if progress_data:
                return ExecutionProgress.from_dict(progress_data)
        except Exception as e:
            logger.debug(f"No execution progress found for {strategy_id}: {e}")
        return None

    async def _save_execution_progress(self, strategy_id: str, progress: ExecutionProgress) -> None:
        """Save execution progress to persisted state.

        Args:
            strategy_id: Strategy identifier
            progress: Execution progress to save
        """
        try:
            # Try to load existing state, create if it doesn't exist
            try:
                state = await self.state_manager.load_state(strategy_id)
                # GatewayStateManager returns None instead of raising StateNotFoundError
                if state is None:
                    raise StateNotFoundError(strategy_id)
                expected_version = state.version
            except StateNotFoundError:
                # Create initial state for this strategy
                state = StateData(
                    strategy_id=strategy_id,
                    version=1,
                    state={},
                )
                expected_version = None  # No version check for new state
                logger.debug(f"Creating initial state for {strategy_id}")

            progress.last_updated = datetime.now(UTC)
            state.state["execution_progress"] = progress.to_dict()
            await self.state_manager.save_state(state, expected_version=expected_version)
            logger.debug(
                f"Saved execution progress for {strategy_id}: "
                f"step {progress.completed_step_index + 1}/{progress.total_steps}"
            )
        except Exception as e:
            logger.error(f"Failed to save execution progress: {e}")

    async def _clear_execution_progress(self, strategy_id: str) -> None:
        """Clear execution progress from state (after completion or abort).

        Args:
            strategy_id: Strategy identifier
        """
        try:
            state = await self.state_manager.load_state(strategy_id)
            # GatewayStateManager returns None instead of raising StateNotFoundError
            if state is None:
                return
            if "execution_progress" in state.state:
                del state.state["execution_progress"]
                await self.state_manager.save_state(state, expected_version=state.version)
                logger.debug(f"Cleared execution progress for {strategy_id}")
        except Exception as e:
            logger.debug(f"Could not clear execution progress: {e}")


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "StrategyRunner",
    "RunnerConfig",
    "IterationResult",
    "IterationStatus",
    "StrategyProtocol",
    "ExecutionProgress",
]
