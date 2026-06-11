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
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Literal, cast

import grpc

if TYPE_CHECKING:
    from ..services.emergency_manager import EmergencyManager
    from ..services.operator_card_generator import OperatorCardGenerator
    from ..services.stuck_detector import StuckDetector
    from ..teardown import TeardownMode
    from ..vault.lifecycle import VaultLifecycleManager
    from .teardown_commit import TeardownCommitOutcome

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
from ..utils.grpc_utils import TRANSIENT_GRPC_CODES, get_grpc_status_code
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


def _last_receipt_block(execution_result: Any | None) -> int | None:
    """Return the block number of the last successful receipt in ``execution_result``.

    VIB-4589 / F7 — used to pin post-execution state reads (Aave V3
    ``getUserAccountData``, Morpho Blue ``position``/``market``, Compound V3
    ``balanceOf``/``userCollateral``) to the exact block of the confirmed
    receipt. Reading at ``"latest"`` from the gateway races the upstream
    RPC's receipt indexer; the stale-collateral bug surfaced when a
    confirmed WITHDRAW receipt was not yet visible to the next ``"latest"``
    view, so the read returned a near-full collateral balance.

    For multi-tx bundles the LAST successful receipt's block is the
    correct anchor (state after the whole bundle landed). Returns ``None``
    when no receipt is available — callers fall back to ``"latest"`` which
    preserves the legacy behaviour.

    Robust to the shape variability ``_collect_candidate_receipts`` already
    handles: ``execution_result`` and each ``transaction_results`` entry may
    be either an object (``ExecutionResult`` / ``GatewayExecutionResult``)
    or a dict; the receipt may use ``block_number`` (snake) or
    ``blockNumber`` (JSON-RPC camel); a numeric block may arrive as an int,
    decimal string, or 0x-prefixed hex string.
    """
    if execution_result is None:
        return None
    tx_results = getattr(execution_result, "transaction_results", None)
    if tx_results is None and isinstance(execution_result, dict):
        tx_results = execution_result.get("transaction_results")
    if not isinstance(tx_results, list):
        return None
    for tx in reversed(tx_results):
        if isinstance(tx, dict):
            if not tx.get("success", False):
                continue
            receipt = tx.get("receipt")
        else:
            if not getattr(tx, "success", False):
                continue
            receipt = getattr(tx, "receipt", None)
        if receipt is None:
            continue
        if isinstance(receipt, dict):
            raw = receipt.get("block_number")
            if raw is None:
                raw = receipt.get("blockNumber")
        else:
            raw = getattr(receipt, "block_number", None)
            if raw is None:
                raw = getattr(receipt, "blockNumber", None)
        block_number = _coerce_block_number(raw)
        if block_number is not None and block_number > 0:
            return block_number
    return None


def _coerce_block_number(raw: Any) -> int | None:
    """Best-effort coercion of a raw block reference to ``int``.

    Accepts ``int`` (rejects ``bool`` since ``bool`` is an ``int`` subclass),
    decimal strings (``"19876543"``), and 0x-prefixed hex strings
    (``"0x12d4abc"``). Returns ``None`` for anything else — the caller
    treats ``None`` as "no anchoring available, fall back to 'latest'".
    """
    if isinstance(raw, bool) or raw is None:
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        try:
            return int(s, 16) if s.lower().startswith("0x") else int(s)
        except ValueError:
            return None
    return None


def _merge_lending_state(target: dict[str, Any] | None, lending_dict: dict[str, Any] | None) -> dict[str, Any] | None:
    """Overlay lending protocol state fields onto an existing pre/post-state dict.

    Returns ``target`` (mutated) when ``lending_dict`` is non-empty, otherwise
    the original ``target`` untouched. When ``target`` is ``None`` and
    ``lending_dict`` is non-empty, the lending dict alone is enough to
    populate the ledger column (VIB-3474: the lending handler reads
    ``collateral_usd`` / ``debt_usd`` / ``health_factor`` /
    ``liquidation_threshold_bps`` directly — wallet balances aren't required).

    Layered overlay so the ledger writer ends up with ONE merged dict per
    column, never two parallel JSON blobs the handler has to choose between.
    """
    if not lending_dict:
        return target
    if target is None:
        target = {"source": "lending_capture"}
    else:
        target = {**target, "source": (target.get("source") or "") + "+lending_capture"}
    target.update(lending_dict)
    return target


def _build_pre_state_for_ledger(
    pre_snapshot: Any,
    lending_pre_state: Any | None = None,
    *,
    protocol: str = "",
) -> dict[str, Any] | None:
    """Build a ``pre_state`` dict for the ledger writer from a balance snapshot.

    Accounting-AttemptNo17 §A4 (VIB-3480 columns finally populated): the
    runner is the single capture point. Without this, ``pre_state_json`` was
    NULL on every ledger row.

    VIB-3474: when ``lending_pre_state`` is supplied (typed AaveAccountState /
    MorphoBlueAccountState / CompoundV3AccountState), its serialized fields
    are merged into the dict so the lending handler's pre-state read returns
    real collateral_usd / debt_usd / health_factor / liquidation_threshold
    instead of ``None`` with ``unavailable_reason``.

    Returns ``None`` only when both wallet balances AND lending state are
    missing — honest absence over fabricated data.
    """
    base: dict[str, Any] | None = None
    if pre_snapshot is not None:
        balances = getattr(pre_snapshot, "balances", None) or {}
        if balances:
            timestamp = getattr(pre_snapshot, "timestamp", None)
            captured_at = timestamp.isoformat() if timestamp is not None and hasattr(timestamp, "isoformat") else ""
            base = {
                "wallet_balances": {k: str(v) for k, v in balances.items()},
                "captured_at": captured_at,
                "source": "balance_provider",
            }
    if lending_pre_state is not None:
        from almanak.framework.accounting.lending_accounting import lending_state_to_dict

        lending_dict = lending_state_to_dict(lending_pre_state, protocol=protocol)
        base = _merge_lending_state(base, lending_dict)
    return base


def _build_post_state_for_ledger(
    recon: dict[str, Any] | None,
    lending_post_state: Any | None = None,
    *,
    protocol: str = "",
) -> dict[str, Any] | None:
    """Build a ``post_state`` dict for the ledger writer from a reconciliation report.

    The reconciliation step in ``_single_chain_handle_success`` already
    queried post-execution balances and put them on ``recon["post_balances"]``.
    Reusing those avoids a redundant RPC round-trip and guarantees the
    ledger row's ``post_state_json`` matches the reconciliation balances
    byte-for-byte (zero drift between the recon report and the ledger).

    VIB-3474: when ``lending_post_state`` is supplied, its serialized fields
    are merged in so ``transaction_ledger.post_state_json`` carries the
    collateral / debt / HF / liquidation_threshold / lltv that the lending
    category handler reads to populate ``collateral_value_after_usd``,
    ``debt_value_after_usd``, ``health_factor_after``, and
    ``liquidation_threshold``.

    VIB-3888: ``post_timestamp`` is now propagated from the
    reconciliation step (both structured-report and legacy-fallback
    paths). When recon predates VIB-3888 and lacks the field, we stamp
    ``datetime.now(UTC)`` rather than emit an empty string — the
    reconciliation's existence implies the post-balance read just
    happened, so an immediate-now timestamp is a closer approximation
    than NULL.

    Returns ``None`` only when both reconciliation and lending post-state are
    missing.
    """
    base: dict[str, Any] | None = None
    if recon:
        post_balances = recon.get("post_balances")
        if post_balances:
            captured_at = recon.get("post_timestamp", "") or ""
            if not captured_at:
                # VIB-3888 — defensive fallback for legacy callers that
                # don't propagate ``post_timestamp``. ``datetime.now(UTC)``
                # is bounded above by the actual capture time (recon
                # balance reads happened a few ms before this builder runs).
                from datetime import UTC
                from datetime import datetime as _dt

                captured_at = _dt.now(UTC).isoformat()
            base = {
                "wallet_balances": dict(post_balances),
                "captured_at": captured_at,
                "source": "balance_provider",
                "incident": bool(recon.get("incident", False)),
            }
    if lending_post_state is not None:
        from almanak.framework.accounting.lending_accounting import lending_state_to_dict

        lending_dict = lending_state_to_dict(lending_post_state, protocol=protocol)
        base = _merge_lending_state(base, lending_dict)
    return base


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
    deployment_id: str
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
    deployment_id: str = ""
    gateway_client: Any = None
    rpc_url: str | None = None
    price_oracle: dict | None = None
    polymarket_config: Any = None
    clob_handler: Any = None
    clob_client: Any = None
    compiler: Any = None
    state_machine: Any = None
    pre_snapshot: Any | None = None
    # VIB-3474: lending-protocol state captured BEFORE submission so
    # transaction_ledger.pre_state_json carries collateral_usd / debt_usd /
    # health_factor / liquidation_threshold_bps for every SUPPLY/BORROW/
    # REPAY/WITHDRAW. Typed protocol state object (AaveAccountState |
    # MorphoBlueAccountState | CompoundV3AccountState) or None. The runner
    # is the single capture point — see the `_init_single_chain_state`
    # caller and `_capture_lending_pre_state_safe`.
    lending_pre_state: Any | None = None
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
    deployment_id: str = ""
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
# Position-key helpers (module-level so the parent function stays under CRAP)
# =============================================================================


def _bridge_outbox_position_key(intent: Any, chain: str, wallet_address: str) -> str:
    """Derive the BRIDGE outbox position_key (VIB-4164, T4).

    Returns the canonical ``bridge:{from_chain}:{to_chain}:{token}:{wallet}``
    string, or ``""`` when any required field is missing. Extracted from
    :meth:`StrategyRunner._compute_outbox_position_key` to keep the parent
    method under the CRAP threshold (cc-allowance budget already consumed
    by the pre-existing primitive branches).

    Auditors join a source-leg PENDING row to its eventual destination-leg
    SETTLED row by ``position_key`` alone; an empty key here would lose the
    join across cross-chain settlement gaps.
    """
    from_chain = str(getattr(intent, "from_chain", "") or chain).lower().strip()
    to_chain = str(getattr(intent, "to_chain", "") or "").lower().strip()
    token = str(getattr(intent, "token", "") or "").upper().strip()
    if from_chain and to_chain and token and wallet_address:
        return f"bridge:{from_chain}:{to_chain}:{token}:{wallet_address.lower()}"
    return ""


def _prediction_outbox_position_key(intent: Any, protocol: str, chain: str, wallet_address: str) -> tuple[str, str]:
    """Derive (position_key, market_id) for prediction-market intents (VIB-3707).

    Per-(market_id, outcome) aggregate position. ``PREDICTION_REDEEM``
    intents may carry ``outcome=None`` when redeeming all winning positions;
    in that case we cannot key the position_key here — the handler falls
    back to extracted_data/position_key reconstruction or surfaces an
    unavailable event. Extracted as a sibling to
    :func:`_bridge_outbox_position_key` to keep the parent method under
    the CRAP threshold; semantics are unchanged from the inline form.
    """
    from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry

    market_id = str(getattr(intent, "market_id", "") or "")
    outcome_raw = getattr(intent, "outcome", None)
    outcome = str(outcome_raw) if outcome_raw is not None else ""
    proto_norm = protocol or CompilerRegistry.default_protocol("PREDICTION") or ""
    if proto_norm and market_id and outcome and chain and wallet_address:
        position_key = f"prediction:{proto_norm}:{chain.lower()}:{wallet_address.lower()}:{market_id}:{outcome}"
    else:
        position_key = ""
    return position_key, market_id


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
        self._recovered_nonces: dict[str, set[int]] = {}  # deployment_id -> set of nonces

        # Portfolio snapshot tracking
        self._last_snapshot_time: datetime | None = None
        self._snapshot_interval_seconds = 300  # Capture time-series snapshot every 5 min
        self._portfolio_valuer = PortfolioValuer()
        self._iteration_had_trade = False  # Set by _write_ledger_entry on success

        # G12 teardown-lane price oracle stash (Accounting-AttemptNo17 §A4).
        # Set by ``capture_teardown_snapshot_with_accounting`` (pre-bracket)
        # and read by ``commit_teardown_intent`` so every teardown ledger row
        # carries ``price_inputs_json`` and ``gas_usd``. Cleared in the
        # post-bracket finally so a subsequent iteration after teardown never
        # sees stale teardown prices. ``None`` = no stash; the writer falls
        # back to the unpriced path (price_inputs_json="").
        self._teardown_price_oracle: dict | None = None

        # VIB-3894 — recent OPEN events cache for same-iteration snapshot
        # cost-basis enrichment. Populated when ``save_position_event``
        # succeeds for an OPEN event; consumed by
        # ``_enrich_from_open_event`` in PortfolioValuer. Necessary because
        # ``GatewayStateManager`` does not expose ``get_position_events_sync``,
        # which would otherwise back-fill ``cost_basis_usd`` from disk.
        # Keyed by ``(str(position_id), position_type)``. CLOSE events
        # delete the matching entry so subsequent snapshots correctly
        # report zero deployed capital after a teardown.
        self._recent_open_events: dict[tuple[str, str], dict] = {}

        # Optional explicit gateway client (set via set_gateway_client for multi-chain)
        self._gateway_client: Any | None = None
        # Track pause log state to avoid repetitive per-iteration info spam.
        self._logged_paused_deployment_ids: set[str] = set()

        # VIB-3418: FIFO basis store for lending interest attribution.
        # Lives for the runner's lifetime so BORROW lots are available when REPAY arrives.
        # Reconstructable from accounting_events if the runner restarts.
        from ..accounting.basis import FIFOBasisStore

        self._lending_basis_store = FIFOBasisStore()

        # VIB-3467: AccountingProcessor — drains accounting_outbox after each execution.
        # Initialised with an empty deployment_id; updated in run_loop once deployment_id is known.
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

    def _build_pool_key_lookup(self) -> Any | None:
        """Build a sync ``(pool_id_hex, chain) -> PoolKey | None`` callable.

        Wraps connector-owned gateway lookup bridges so the sync
        ``ResultEnricher`` pipeline can inject them into receipt parsers.

        Returns ``None`` when no gateway client is configured (paper / dry-run
        / unit-test modes). Parsers that require a lookup callback then emit
        their own structured missing-lookup diagnostics.
        """
        client = self._get_gateway_client()
        if client is None:
            return None
        try:
            from almanak.connectors._strategy_runner_hook_registry import (
                STRATEGY_RUNNER_HOOK_REGISTRY,
            )
        except Exception as exc:
            logger.error(
                "pool_key_lookup registry unavailable: %s: %s",
                type(exc).__name__,
                exc,
            )
            return None

        try:
            return STRATEGY_RUNNER_HOOK_REGISTRY.build_pool_key_lookup(client)
        except Exception as exc:
            # A configured gateway client exists but a connector-owned lookup
            # bridge could not be constructed. Surface loudly so operators can
            # distinguish "no gateway configured" from "gateway misconfigured".
            logger.error(
                "pool_key_lookup bridge unavailable: %s: %s",
                type(exc).__name__,
                exc,
            )
            return None

    def _register_with_gateway(self, strategy: StrategyProtocol) -> None:
        from .runner_gateway import register_with_gateway

        register_with_gateway(self, strategy)

    def _deregister_from_gateway(self, deployment_id: str) -> None:
        from .runner_gateway import deregister_from_gateway

        deregister_from_gateway(self, deployment_id)

    def _gateway_update_status(self, deployment_id: str, status: str) -> None:
        from .runner_gateway import gateway_update_status

        gateway_update_status(self, deployment_id, status)

    def _gateway_heartbeat(self, deployment_id: str, positions: list | None = None) -> None:
        from .runner_gateway import gateway_heartbeat

        gateway_heartbeat(self, deployment_id, positions)

    def _collect_position_snapshot(self, strategy: "StrategyProtocol") -> list | None:
        from .runner_gateway import collect_position_snapshot

        return collect_position_snapshot(self, strategy)

    def _lifecycle_write_state(self, deployment_id: str, state: str, error_message: str | None = None) -> None:
        from .runner_gateway import lifecycle_write_state

        lifecycle_write_state(self, deployment_id, state, error_message)

    def _lifecycle_heartbeat(self, deployment_id: str) -> None:
        from .runner_gateway import lifecycle_heartbeat

        lifecycle_heartbeat(self, deployment_id)

    def _lifecycle_poll_command(self, deployment_id: str) -> str | None:
        from .runner_gateway import lifecycle_poll_command

        return lifecycle_poll_command(self, deployment_id)

    def _lifecycle_handle_stop(self, deployment_id: str, strategy: Any) -> None:
        from .runner_gateway import lifecycle_handle_stop

        lifecycle_handle_stop(self, deployment_id, strategy)

    def set_gateway_client(self, client: Any) -> None:
        from .runner_gateway import set_gateway_client

        set_gateway_client(self, client)

    def setup_gateway_integration(self, strategy: StrategyProtocol) -> None:
        from .runner_gateway import setup_gateway_integration

        setup_gateway_integration(self, strategy)

    def teardown_gateway_integration(self, deployment_id: str) -> None:
        from .runner_gateway import teardown_gateway_integration

        teardown_gateway_integration(self, deployment_id)

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
        deployment_id = strategy.deployment_id

        # Bind correlation ID for all log messages during this iteration
        iteration_id = f"{deployment_id}_{self._total_iterations + 1}_{int(start_time.timestamp())}"
        add_context(correlation_id=iteration_id, deployment_id=deployment_id)

        # Generate cycle_id for forensic event correlation across phases
        from almanak.framework.observability.context import clear_cycle_id, new_cycle_id

        cycle_id = new_cycle_id()
        self._last_cycle_id = cycle_id  # Phase 4: preserve for snapshot capture after iteration
        add_context(cycle_id=cycle_id)

        # VIB-4843 FR-5001: open a per-iteration MarketSnapshot scope keyed by
        # the cycle_id so pre-warm → decide() → post-decide portfolio valuation
        # all reuse ONE snapshot instance (and its pre-warmed _price_cache)
        # instead of re-minting cold snapshots and re-fetching every price.
        self._begin_market_snapshot_iteration(strategy, cycle_id)

        logger.info(f"Starting iteration for strategy: {deployment_id}")

        state = RunIterationState(
            strategy=strategy,
            deployment_id=deployment_id,
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
                    deployment_id,
                    e.write_kind,
                )
                await self._alert_accounting_failure(strategy, e)
                return self._create_error_result(
                    deployment_id,
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
                    deployment_id,
                    e.field_name,
                    e.intent_type,
                    e.protocol,
                )
                await self._alert_enrichment_failure(strategy, e)
                return self._create_error_result(
                    deployment_id,
                    IterationStatus.ACCOUNTING_FAILED,
                    f"Receipt enrichment failed (field={e.field_name}, intent={e.intent_type}): {e}",
                    start_time,
                )
            logger.exception(f"Unexpected error in iteration for {deployment_id}: {e}")
            return self._create_error_result(
                deployment_id,
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
        deployment_id = state.deployment_id
        paused, pause_reason = await self._is_strategy_paused(deployment_id)
        if paused:
            if deployment_id not in self._logged_paused_deployment_ids:
                logger.info(
                    "%s %s is paused by operator%s",
                    "[PAUSED]" if not _emojis_enabled() else "⏸️",
                    deployment_id,
                    f" ({pause_reason})" if pause_reason else "",
                )
                self._logged_paused_deployment_ids.add(deployment_id)
            self._record_success()
            return IterationResult(
                status=IterationStatus.HOLD,
                intent=HoldIntent(reason=pause_reason or "Paused by operator"),
                deployment_id=deployment_id,
                duration_ms=self._calculate_duration_ms(state.start_time),
            )

        # Strategy resumed: clear pause log marker.
        self._logged_paused_deployment_ids.discard(deployment_id)
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
        deployment_id = state.deployment_id
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
                    deployment_id,
                    cb_result.reason,
                    cb_result.state.value,
                    cb_result.consecutive_failures,
                )
                cb_state_label = cb_result.state.value  # "open" or "paused"
                # VIB-4043 / PR4: strip cb_result.to_dict() — it carries
                # cumulative_loss_usd which is money-shaped. The reason and
                # state already convey the lifecycle event; loss totals live
                # in portfolio_metrics.
                # CodeRabbit on PR #2117 round 5: ``cb_result.reason`` is a
                # free-form string (e.g. ``"Circuit breaker open. Cooldown:
                # {remaining}s remaining"``) and could in the future grow to
                # include money-shaped numbers (loss thresholds, P&L), which
                # would re-introduce the drift the producer-side guardrail is
                # designed to block. The TripReason enum is bucketed by
                # construction (CONSECUTIVE_FAILURES, CUMULATIVE_LOSS,
                # MANUAL_PAUSE, …) so the description stays UX-safe.
                _trip_label = cb_result.trip_reason.value if cb_result.trip_reason else "blocked"
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.STRATEGY_STUCK,
                        description=f"Circuit breaker {cb_state_label}: {_trip_label}",
                        deployment_id=deployment_id,
                        details={
                            "circuit_breaker_state": cb_state_label,
                            "trip_reason": cb_result.trip_reason.value if cb_result.trip_reason else None,
                            "consecutive_failures": cb_result.consecutive_failures,
                        },
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
                    deployment_id=deployment_id,
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
        deployment_id = state.deployment_id

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
                            await self._persist_vault_state(deployment_id, vault_state_dict, VAULT_STATE_KEY)
                    except Exception as persist_err:
                        logger.warning("Failed to persist vault state: %s", persist_err)

    async def _step_build_snapshot(self, state: RunIterationState) -> IterationResult | None:
        """Create market snapshot, inject dry-run balances, pre-warm prices."""
        strategy = state.strategy
        deployment_id = state.deployment_id

        # Step 1: Create market snapshot
        try:
            market = strategy.create_market_snapshot()
            logger.debug(f"Created market snapshot for {deployment_id}")
        except Exception as e:
            logger.error(f"Failed to create market snapshot: {e}")
            return self._create_error_result(
                deployment_id,
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
        deployment_id = state.deployment_id
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
                        f"Resetting decide guard after {elapsed:.1f}s (timeout was {decide_timeout}s) for {deployment_id}"
                    )
                    self._decide_in_progress = False
                    self._decide_timed_out_at = None
            if self._decide_in_progress:
                msg = "strategy.decide() still running from previous timed-out call"
                logger.error(f"OVERLAP: {msg} for {deployment_id}")
                if self._circuit_breaker is not None:
                    self._circuit_breaker.record_failure(error_message=msg)
                return self._create_error_result(
                    deployment_id,
                    IterationStatus.STRATEGY_TIMEOUT,
                    msg,
                    start_time,
                )
        try:
            self._decide_in_progress = True
            from almanak.framework.observability.emitter import emit_phase_event
            from almanak.framework.observability.events import StrategyPhase

            emit_phase_event(
                deployment_id=deployment_id,
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
                deployment_id=deployment_id,
                phase=StrategyPhase.DECIDE,
                event_type="STATE_CHANGE",
                description=f"decide() returned {type(decide_result).__name__}",
            )
        except TimeoutError:
            # Worker thread may still be running; _decide_in_progress stays True
            # to block overlapping calls. Recovery allowed after 2x timeout elapsed.
            self._decide_timed_out_at = time.monotonic()
            msg = f"strategy.decide() timed out after {decide_timeout}s"
            logger.error(f"TIMEOUT: {msg} for {deployment_id}")
            if self._circuit_breaker is not None:
                self._circuit_breaker.record_failure(error_message=msg)
            return self._create_error_result(
                deployment_id,
                IterationStatus.STRATEGY_TIMEOUT,
                msg,
                start_time,
            )
        except Exception as e:
            self._decide_in_progress = False  # Normal exceptions complete; reset guard
            logger.error(f"Strategy decision failed: {e}")
            if self._circuit_breaker is not None:
                # VIB-3803: classify the failure so a transient data outage gets
                # the elevated data-class threshold (when exposure is open) and
                # cannot crash-loop a strategy holding correct positions.
                from .failure_kind import classify_failure

                self._circuit_breaker.record_failure(
                    f"decide() error: {e}",
                    kind=classify_failure(e),
                )
            return self._create_error_result(
                deployment_id,
                IterationStatus.STRATEGY_ERROR,
                f"Strategy decision failed: {e}",
                start_time,
            )

        state.decide_result = decide_result
        return None

    # crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
    def _step_extract_intents(self, state: RunIterationState) -> IterationResult | None:
        """Normalise ``decide_result`` into ``state.intents`` and handle HOLD."""
        strategy = state.strategy
        deployment_id = state.deployment_id
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

        # VIB-3742: framework auto-injects tracked LP position metadata
        # (e.g. TraderJoe V2 bin_ids captured at LP_OPEN) into LP_CLOSE /
        # LP_COLLECT_FEES intents that would otherwise lack ``protocol_params``.
        # Strategies that already supply protocol_params manually are
        # unaffected — the tracker never overwrites caller-supplied data.
        # See almanak/framework/strategies/lp_position_tracker.py.
        #
        # Gate strictly on a real LPPositionTracker — MagicMock fake
        # strategies in unit tests synthesize attributes on demand, so an
        # ``isinstance`` check is the only reliable filter.
        from ..strategies.lp_position_tracker import LPPositionTracker

        tracker = getattr(strategy, "_lp_position_tracker", None)
        inject_fn = getattr(strategy, "_framework_inject_intent_params", None)
        if isinstance(tracker, LPPositionTracker) and callable(inject_fn):
            framework_inject = cast(Callable[[AnyIntent], AnyIntent], inject_fn)
            injected_intents: list[AnyIntent] = []
            for raw_intent in intents:
                try:
                    injected_intents.append(framework_inject(raw_intent))
                except Exception as exc:  # noqa: BLE001 — defensive
                    logger.warning(
                        "Framework intent-injection hook raised (non-fatal, passing original intent): %s",
                        exc,
                        exc_info=True,
                    )
                    injected_intents.append(raw_intent)
            intents = injected_intents

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
                # Quiet-pool liveness backstop: a DEX pool with no recent swaps
                # returns *stale* (not absent) OHLCV, so trade-derived indicators
                # can't be computed — but the asset is still continuously
                # priceable from the 24/7 oracle. Holding through that is benign,
                # not a data failure; escalating would trip the breaker on a live
                # pool. Only escalate when the pool is NOT priceable (genuinely
                # dead/unreachable).
                # Fail safe: suppress the DATA_ERROR escalation only on an
                # explicit ``True``. ``is_quiet_pool_hold`` is an optional snapshot
                # method; anything other than a definite True (missing method,
                # non-bool) means "not confirmed quiet+live" → escalate as before.
                if (
                    hasattr(market, "is_quiet_pool_hold")
                    and callable(market.is_quiet_pool_hold)
                    and market.is_quiet_pool_hold() is True
                ):
                    logger.info(
                        "%s HOLD on quiet but live pool (no recent trades; price still available) "
                        "— not escalating to DATA_ERROR",
                        deployment_id,
                    )
                else:
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
                    error = (
                        f"Critical market-data failures while strategy returned HOLD (classification={classification})"
                    )
                    if details:
                        error = f"{error}: {details}"
                    logger.error("%s", error)
                    return self._create_error_result(
                        deployment_id,
                        IterationStatus.DATA_ERROR,
                        error,
                        state.start_time,
                        intent=hold_intent,
                    )

            hold_prefix = "⏸️" if _emojis_enabled() else "[HOLD]"
            logger.info(f"{hold_prefix} {deployment_id} HOLD: {reason}")
            self._record_success()
            return IterationResult(
                status=IterationStatus.HOLD,
                intent=hold_intent,
                deployment_id=deployment_id,
                duration_ms=self._calculate_duration_ms(state.start_time),
            )
        return None

    def _step_log_intents(self, state: RunIterationState) -> None:
        """Log the intent or intent sequence with human-readable formatting."""
        strategy = state.strategy
        deployment_id = state.deployment_id
        intents = state.intents

        _chain = getattr(strategy, "chain", "")
        if len(intents) == 1:
            intent_summary = _format_intent_for_log(intents[0], chain=_chain)
            intent_prefix = "📈" if _emojis_enabled() else "[INTENT]"
            logger.info(f"{intent_prefix} {deployment_id} intent: {intent_summary}")
        else:
            # Log intent sequence with details for each step
            intent_prefix = "📈" if _emojis_enabled() else "[INTENT]"
            logger.info(f"{intent_prefix} {deployment_id} intent sequence ({len(intents)} steps):")
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
        deployment_id = state.deployment_id
        intents = state.intents

        cb_check = self._circuit_breaker.check()
        if not cb_check.can_execute:
            logger.warning(
                f"Circuit breaker BLOCKED execution for {deployment_id}: "
                f"state={cb_check.state.value}, reason={cb_check.reason}"
            )
            # VIB-4043 / PR4: cumulative_loss_usd is money-shaped; loss totals
            # live in portfolio_metrics, not in timeline UX cards.
            # CodeRabbit on PR #2117 round 5 (sibling of the STRATEGY_STUCK
            # path above): use the bucketed TripReason enum value instead of
            # the free-form ``cb_check.reason`` string for the description.
            _trip_label = cb_check.trip_reason.value if cb_check.trip_reason else "blocked"
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.ERROR,
                    description=f"Circuit breaker blocked execution: {_trip_label}",
                    deployment_id=deployment_id,
                    chain=getattr(strategy, "chain", ""),
                    details={
                        "circuit_breaker_state": cb_check.state.value,
                        "trip_reason": cb_check.trip_reason.value if cb_check.trip_reason else None,
                        "consecutive_failures": cb_check.consecutive_failures,
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
                deployment_id=deployment_id,
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

    # crap-allowlist: VIB-4640 — pre-existing intent-dispatch loop; PR #2370 reduced cc 31→30 via _has_downstream_chained_amount extraction
    async def _run_single_chain_intents(self, state: RunIterationState) -> IterationResult:  # noqa: C901
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
            logger.info(f"Executing {len(intents)} intents sequentially for {strategy.deployment_id}")

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
                    if is_multi_intent and self._has_downstream_chained_amount(intents, idx):
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

    @staticmethod
    def _has_downstream_chained_amount(intents: list["AnyIntent"], idx: int) -> bool:
        """VIB-2036: True if the immediate next intent consumes a chained amount.

        The amount-chaining warning is only informative when the immediate
        next step would fail without ``previous_amount_received`` — i.e. when
        ``intents[idx + 1]`` declares ``amount='all'`` (or another chained
        reference). The pre-VIB-2036 predicate fired on every non-last step,
        producing false-positives for sequences like ``[LP_OPEN, LP_OPEN]``
        whose legs carry explicit amounts. Further-downstream chained
        consumers (``intents[idx + 2:]``) are intentionally not anticipated
        here: if their own predecessor also fails to produce output, the
        warning fires at that step instead — avoiding the false-positive
        flagged by CodeRabbit where an intermediate step with explicit amount
        would refresh ``previous_amount_received``.
        """
        next_idx = idx + 1
        return next_idx < len(intents) and Intent.has_chained_amount(intents[next_idx])

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
                    deployment_id=strategy.deployment_id,
                    duration_ms=self._calculate_duration_ms(start_time),
                )
                return intent, result, True  # continue

            logger.error(f"  Intent {idx + 1}/{len(intents)} uses amount='all' but no previous step amount available")
            result = IterationResult(
                status=IterationStatus.COMPILATION_FAILED,
                intent=intent,
                error="amount='all' used but no previous step amount available",
                deployment_id=strategy.deployment_id,
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
                        deployment_id=strategy.deployment_id,
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
                    deployment_id=strategy.deployment_id,
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
            deployment_id=strategy.deployment_id,
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
        # Explicit None check, not `or`: a caller passing 0 means "no inter-iteration
        # delay", and `0 or default` would silently fall back to default_interval_seconds.
        interval = self.config.default_interval_seconds if interval_seconds is None else interval_seconds
        deployment_id = strategy.deployment_id

        max_iter_msg = f", max_iterations={max_iterations}" if max_iterations else ""
        logger.info(f"Starting run loop for strategy {deployment_id} with interval={interval}s{max_iter_msg}")

        # Phase 1: setup (state manager init, session recovery, copy-trading
        # restore, shutdown flag reset, gateway wiring, RUNNING write,
        # STRATEGY_STARTED event).
        activity_provider = await _run_loop_helpers.initialize_run_loop(self, strategy, deployment_id, interval)

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
                    deployment_id,
                    result,
                    iteration_start_monotonic=iteration_start_monotonic,
                )

                # Update state. update_state raises AccountingPersistenceError
                # only in live mode (blueprint 27 failure-mode table); rebuild
                # the result into ACCOUNTING_FAILED so the failure branch
                # (circuit breaker, consecutive-errors alert, lifecycle ERROR
                # write) fires — mirroring capture_snapshot_with_accounting.
                if self.config.enable_state_persistence:
                    try:
                        await self._update_state(deployment_id, result, strategy=strategy)
                    except AccountingPersistenceError as acc_err:
                        # Snapshot duration BEFORE alert I/O so Slack/PagerDuty
                        # latency does not skew duration_ms (issue #1782).
                        duration_ms = (time.monotonic() - iteration_start_monotonic) * 1000.0
                        logger.exception(
                            f"Iteration-state persistence failed in live mode for "
                            f"{deployment_id} (write_kind={acc_err.write_kind})"
                        )
                        await self._alert_accounting_failure(strategy, acc_err)
                        result = IterationResult(
                            status=IterationStatus.ACCOUNTING_FAILED,
                            error=f"State persistence failed ({acc_err.write_kind}): {acc_err}",
                            deployment_id=deployment_id,
                            duration_ms=duration_ms,
                            intent=result.intent,
                            execution_result=result.execution_result,
                            balance_reconciliation=result.balance_reconciliation,
                            timestamp=result.timestamp,
                        )

                # Emit structured iteration summary for JSONL log analysis
                # (sequenced AFTER state persistence so the JSONL row reflects
                # the FINAL status, including a state-lane ACCOUNTING_FAILED
                # rebuild — same invariant as issue #1782 for the snapshot lane).
                self._emit_iteration_summary(result, chain=getattr(strategy, "chain", None))

                # Persist copy trading cursor state (if configured)
                if activity_provider is not None and self.config.enable_state_persistence:
                    try:
                        await self._persist_copy_trading_state(deployment_id, activity_provider)
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
                    await _run_loop_helpers.handle_iteration_failure(self, strategy, deployment_id, result)
                else:
                    _run_loop_helpers.handle_iteration_success(self, deployment_id, was_in_error_streak)

                # Report positions and send heartbeat to gateway after each iteration
                position_protos = self._collect_position_snapshot(strategy)
                self._gateway_heartbeat(deployment_id, positions=position_protos)

                # Send lifecycle heartbeat
                self._lifecycle_heartbeat(deployment_id)

                # Poll for + route lifecycle commands (PAUSE, RESUME, STOP).
                command = self._lifecycle_poll_command(deployment_id)
                await _run_loop_helpers.handle_lifecycle_command(self, strategy, deployment_id, command)

                # Check max iterations limit
                loop_iteration_count += 1
                if max_iterations is not None and loop_iteration_count >= max_iterations:
                    logger.info(f"Reached max iterations ({max_iterations}) for {deployment_id}. Stopping.")
                    break

                # Sleep until next iteration (unless shutdown requested)
                if not self._shutdown_requested:
                    logger.debug(f"Sleeping for {interval}s before next iteration")
                    await asyncio.sleep(interval)

            except asyncio.CancelledError:
                logger.info(f"Run loop cancelled for {deployment_id}")
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
        await _run_loop_helpers.finalize_run_loop(self, strategy, deployment_id)

    def _notify_intent_executed(
        self,
        strategy: Any,
        intent: Any,
        success: bool,
        result: Any | None,
        *,
        framework_success: bool | None = None,
    ) -> None:
        """Fire ``strategy.on_intent_executed`` with framework hooks attached.

        Calls the framework's LP position tracker (VIB-3742) BEFORE the user's
        ``on_intent_executed`` callback so the user override sees the captured
        bin_ids / position_ids on ``self.lp_position_tracker``. Both calls are
        guarded — exceptions in either are logged at WARNING and never
        propagated, mirroring the prior inline behaviour.

        ``success`` is the *user-facing verdict* — reflects slippage breach,
        reconciliation failure, etc. ``framework_success`` is the *on-chain
        truth* used for tracker bookkeeping. They diverge when the on-chain
        TX succeeded but a post-execution check downgraded the iteration
        (e.g. slippage breach, recon incident): the user heard ``False`` but
        the position state still moved on-chain. The tracker MUST track
        chain reality so a future LP_CLOSE doesn't silently leak the
        position the chain still holds. When ``framework_success`` is
        ``None`` (default), it falls back to ``success`` — preserving prior
        behaviour for callers that don't need to distinguish.
        """
        tracker_success = success if framework_success is None else framework_success

        # Step 1: framework-level hook (LP position tracker, etc.)
        # Gate strictly on a real LPPositionTracker instance — MagicMock
        # strategies in unit tests synthesize any attribute on demand, so
        # ``getattr(strategy, "_lp_position_tracker", None)`` would
        # return a MagicMock rather than None and erroneously activate
        # the framework path against a fake strategy.
        from ..strategies.lp_position_tracker import LPPositionTracker

        tracker = getattr(strategy, "_lp_position_tracker", None)
        if isinstance(tracker, LPPositionTracker) and callable(
            getattr(strategy, "_framework_record_intent_execution", None)
        ):
            try:
                strategy._framework_record_intent_execution(intent, tracker_success, result)
            except Exception as exc:  # noqa: BLE001 — hook must not poison runner
                logger.warning(
                    "Framework intent-execution hook raised (non-fatal): %s",
                    exc,
                    exc_info=True,
                )

        # Step 2: user callback
        if hasattr(strategy, "on_intent_executed"):
            try:
                strategy.on_intent_executed(intent, success=success, result=result)
            except Exception as e:
                logger.warning(f"Error in on_intent_executed callback: {e}")

    def _emit_execution_timeline_event(
        self,
        strategy: StrategyProtocol,
        intent: AnyIntent,
        success: bool,
        result: Any | None,
        related_ledger_entry_id: str = "",
    ) -> None:
        """Emit a timeline event for an intent execution (success or failure).

        VIB-4043 / PR4 — UX-only event. The financial truth (gas_used, amounts,
        prices, slippage, position attribution) lives in `transaction_ledger`
        and is referenced via `related_ledger_entry_id`. Do NOT add money-shaped
        keys to `details` here; the static guardrail in
        `tests/static/test_timeline_payload_keys.py` will fail the build.
        """
        try:
            deployment_id = strategy.deployment_id
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

            # CodeRabbit on PR #2117 round 5: in multi-tx bundles
            # (approve → swap, approve → lp_open, …) ``transaction_results[0]``
            # is typically the approval, not the value-action. The activity
            # feed deep-link should land on the terminal action so a user
            # tapping the breadcrumb sees the actual swap / LP / lend tx in
            # the explorer, not its preceding approval. Pick the last
            # non-empty hash.
            tx_hash = ""
            if result and hasattr(result, "transaction_results") and result.transaction_results:
                tx_hash = next(
                    (tr.tx_hash for tr in reversed(result.transaction_results) if getattr(tr, "tx_hash", "")),
                    "",
                )

            if success:
                description = f"{intent_type_str} executed"
            else:
                # PR4 / PRD-TimelineEvents §6.1: do NOT embed the raw error
                # string. Slippage breach + reconciliation messages carry
                # money-shaped data (bps, token deltas) that the activity feed
                # is forbidden to surface. Bucket into a small set of generic
                # reasons; the full error stays in `transaction_ledger.error`
                # for renderers to drill into via `related_ledger_entry_id`.
                error_str = getattr(result, "error", "") or "" if result else ""
                description = f"{intent_type_str} failed: {self._classify_failure_reason(error_str)}"

            # Lifecycle markers only — no token amounts, gas, prices, slippage,
            # liquidity, ticks, or receipt-parser payloads. Renderers should
            # follow `related_ledger_entry_id` to the ledger row for the money
            # trail and `cycle_id` to position_events for attribution.
            details: dict[str, Any] = {
                "intent_type": intent_type_str,
                "success": success,
            }

            event = TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=event_type,
                description=description,
                deployment_id=deployment_id,
                chain=getattr(strategy, "chain", "") or getattr(self.config, "chain", ""),
                tx_hash=tx_hash,
                details=details,
                related_ledger_entry_id=related_ledger_entry_id or "",
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

    def _update_recent_open_events_cache(self, pos_event: Any) -> None:
        """VIB-3894 — keep ``_recent_open_events`` in sync with disk writes.

        Populated when ``save_position_event`` returns truthy for an OPEN
        event so the same-iteration ``portfolio_snapshots`` row can read
        ``cost_basis_usd`` and surface ``deployed_capital_usd`` correctly.
        Removed on CLOSE so a post-teardown snapshot correctly reports
        zero deployed capital.
        """
        try:
            position_id = str(getattr(pos_event, "position_id", "") or "")
            position_type = str(getattr(pos_event, "position_type", "") or "")
            event_type = str(getattr(pos_event, "event_type", "") or "")
            if not (position_id and position_type and event_type):
                return
            key = (position_id, position_type)
            if event_type == "OPEN":
                # VIB-3919 — also stamp the immutable LP bracket so the
                # CLOSE-event writer can backfill ``tick_lower /
                # tick_upper / liquidity`` from this cache. The bracket
                # never changes over a position's lifetime; carrying it
                # here saves a state-manager round-trip at CLOSE time
                # and keeps the fields populated even when the close
                # receipt parser doesn't re-emit them.
                # VIB-4086 — also stamp ``token0`` / ``token1`` for the
                # same reason: the LP_CLOSE receipt parser doesn't
                # re-emit the pair, leaving the CLOSE row with empty
                # token columns and breaking ``_apply_lp_close_value_usd``
                # (which reads ``event.token0`` / ``event.token1`` to
                # resolve decimals + prices).
                # VIB-5018 — also stamp ``amount0`` / ``amount1`` (wei) so the
                # same-iteration V4 LP snapshot valuation
                # (``PortfolioValuer._v4_open_amounts``) can re-mark the opened
                # amounts straight from this cache without a store round-trip.
                # Empty ≠ Zero: only stamp when the OPEN event actually surfaces
                # the amount; an absent / empty value stays "" so the valuer reads
                # it as a miss (and falls through to the authoritative store
                # query) rather than a measured zero.
                # Empty ≠ Zero: ``str(x or "")`` would collapse a measured ``0``
                # amount (a legitimate single-sided open) into ``""``, which the
                # valuer reads as a MISS. Preserve "0" as measured; only None/""
                # stay "" (unmeasured → store fall-through).
                raw_amount0 = getattr(pos_event, "amount0", "")
                raw_amount1 = getattr(pos_event, "amount1", "")
                self._recent_open_events[key] = {
                    "value_usd": str(getattr(pos_event, "value_usd", "") or ""),
                    "ledger_entry_id": str(getattr(pos_event, "ledger_entry_id", "") or ""),
                    "timestamp": str(getattr(pos_event, "timestamp", "") or ""),
                    "tick_lower": getattr(pos_event, "tick_lower", None),
                    "tick_upper": getattr(pos_event, "tick_upper", None),
                    "liquidity": str(getattr(pos_event, "liquidity", "") or ""),
                    "token0": str(getattr(pos_event, "token0", "") or ""),
                    "token1": str(getattr(pos_event, "token1", "") or ""),
                    "amount0": "" if raw_amount0 in (None, "") else str(raw_amount0),
                    "amount1": "" if raw_amount1 in (None, "") else str(raw_amount1),
                }
            elif event_type == "CLOSE":
                self._recent_open_events.pop(key, None)
        except Exception:  # noqa: BLE001 — never raise from a cache update
            logger.debug("recent-open cache update failed", exc_info=True)

    async def _hydrate_lp_close_from_durable_store(
        self,
        *,
        deployment_id: str,
        position_id: str,
    ) -> dict | None:
        """VIB-4839 — durable-storage fallback for the LP_CLOSE carry-forward.

        ``_recent_open_events`` is a process-local in-memory cache.
        ``hydrate_recent_open_events_cache`` warms it from disk at boot, but:

        * On hosted (GatewayStateManager) the bulk hydration silently
          no-ops because the sync getter isn't exposed.
        * Cross-process restarts that miss the boot path (signal-driven
          teardown on a fresh process, certain harness orderings) leave
          the cache cold for positions opened in an earlier process.

        Without a fallback, ``_apply_lp_close_columns`` carries no token /
        tick / liquidity from the matching OPEN, and ``_apply_lp_close_value_usd``
        silently early-returns on blank token0/token1 — the May-22 / May-26
        ``lp_triple`` rerun bug where teardown CLOSE rows landed with
        ``value_usd=''`` and ``principal_recovered_usd=0``.

        This helper reads the most-recent OPEN for ``position_id`` via the
        async ``get_position_history`` surface (present on both
        ``StateManager`` and ``GatewayStateManager``), shaped into the same
        payload as the in-memory cache entries. Returns ``None`` on empty /
        missing API / any error — never raises. The caller writes the
        result into ``self._recent_open_events`` so the existing
        carry-forward path in ``build_position_event_from_intent`` picks
        it up transparently.
        """
        sm = getattr(self, "state_manager", None)
        if sm is None or not hasattr(sm, "get_position_history"):
            # VIB-4839 — UAT-card criterion 5 (loud + durable per blueprint
            # 27 §14.1).  A missing durable surface is the silent-failure
            # shape that hid the hosted GSM hydration gap; surface it so any
            # future backend regression that drops ``get_position_history``
            # is uniformly observable.  We still fall through to the
            # "no durable OPEN" path (Empty != Zero); never raise here.
            logger.warning(
                "lp_close_durable_hydration.unavailable deployment_id=%s position_id=%s "
                "reason=state_manager_or_method_absent has_state_manager=%s has_method=%s",
                deployment_id,
                position_id,
                sm is not None,
                hasattr(sm, "get_position_history") if sm is not None else False,
            )
            return None
        try:
            history = await sm.get_position_history(deployment_id, position_id)
        except Exception as exc:  # noqa: BLE001 — fail-closed: cache stays empty
            logger.warning(
                "lp_close_durable_hydration.failed deployment_id=%s position_id=%s err=%s",
                deployment_id,
                position_id,
                exc,
            )
            return None
        if not history:
            return None
        # get_position_history returns rows ASC by timestamp; the most-recent
        # OPEN is the one we want to mirror (re-opens of the same id are
        # rare on LP NFTs, but if they happen the later OPEN wins — same
        # ordering rule as _collect_open_positions in _run_loop_helpers.py).
        # VIB-4896 — selection rule extracted to ``select_open_for_lp_close``
        # (pnl_attributor) so the offline repair CLI shares it. ``close_
        # timestamp=None`` preserves the runner's no-upper-bound behaviour.
        from almanak.framework.observability.pnl_attributor import (
            select_open_for_lp_close,
        )

        latest_open = select_open_for_lp_close(history, close_timestamp=None)
        if latest_open is None:
            return None
        return {
            "value_usd": str(latest_open.get("value_usd") or ""),
            "ledger_entry_id": str(latest_open.get("ledger_entry_id") or ""),
            "timestamp": str(latest_open.get("timestamp") or ""),
            "tick_lower": latest_open.get("tick_lower"),
            "tick_upper": latest_open.get("tick_upper"),
            "liquidity": str(latest_open.get("liquidity") or ""),
            "token0": str(latest_open.get("token0") or ""),
            "token1": str(latest_open.get("token1") or ""),
        }

    def _maybe_enrich_result_with_runner_hooks(self, result: Any, chain: str) -> None:
        """Run connector-owned best-effort result enrichment before ledger writes."""
        try:
            gateway = self._get_gateway_client()
            if gateway is None:
                return

            from almanak.connectors._strategy_runner_hook_registry import (
                STRATEGY_RUNNER_HOOK_REGISTRY,
            )

            STRATEGY_RUNNER_HOOK_REGISTRY.enrich_result(result, gateway_client=gateway, chain=chain)
        except Exception:  # noqa: BLE001 — fail-open
            logger.debug("runner hook enrichment failed", exc_info=True)

    async def _write_ledger_entry(  # noqa: C901
        self,
        strategy: StrategyProtocol,
        intent: AnyIntent,
        result: Any | None,
        success: bool,
        error: str = "",
        price_oracle: dict | None = None,
        pre_state: dict | None = None,
        post_state: dict | None = None,
        emit_position_event: bool = True,
    ) -> str | None:
        """Returns the persisted LedgerEntry.id on success, None on non-live failure."""
        """Write a structured trade record to the transaction ledger.

        VIB-3157: in live mode a persistence failure raises
        ``AccountingPersistenceError`` so the caller (run_iteration) can halt
        the cycle and alert the operator. In paper/dry-run mode we log ERROR
        and continue -- the drift is visible but does not block the loop.

        ``price_oracle`` (VIB-3658 sequel — April 30 audit #3): when the
        caller has the per-cycle price oracle in scope (e.g.
        ``state.price_oracle``) it should pass it so ``transaction_ledger.gas_usd``
        gets populated.  Callers that don't have it (slippage circuit-breaker
        path, recon-failure path) skip the conversion — gas_usd stays empty
        and the operator sees the same diagnostic that lent before.

        ``emit_position_event`` (VIB-4895): the iteration lane emits the matching
        ``position_events`` row transitively from here on a successful chain TX
        (default ``True``). The **teardown lane** (``commit_teardown_intent``)
        passes ``False`` because it owns the emit explicitly as its own Step 2b
        under the loud-but-never-block contract (blueprint 27 §14.1) — letting
        this method also emit would write a *duplicate* CLOSE row (``id`` is a
        random UUID, so ``save_position_event``'s ``INSERT OR IGNORE`` keyed on
        ``id`` does not dedupe two independent builds). Suppressing the transitive
        emit here also keeps the teardown ``ledger_entry_id`` intact when only the
        position-event write fails, so outbox still fires and the degraded reason
        is labelled ``position_event`` rather than mislabelled ``ledger``.
        """

        try:
            from ..observability.context import get_cycle_id
            from ..observability.ledger import build_ledger_entry

            cycle_id = get_cycle_id() or ""
            chain = getattr(strategy, "chain", "") or getattr(self.config, "chain", "")

            # VIB-3893 / VIB-3940: connector-owned enrichment must run BEFORE
            # ``build_ledger_entry`` serializes ``result.extracted_data``
            # into ``transaction_ledger.extracted_data_json``. Pre-fix the
            # ledger row carried ``current_tick=None`` even though the
            # post-save position_event captured the enriched value.
            # Net effect: the LP accounting payload (built later from the
            # ledger row) showed ``in_range=None`` on every production
            # swap-then-mint-across-cycles run.
            self._maybe_enrich_result_with_runner_hooks(result, chain)

            entry = build_ledger_entry(
                deployment_id=strategy.deployment_id,
                cycle_id=cycle_id,
                intent=intent,
                result=result,
                chain=chain,
                success=success,
                error=error,
                price_oracle=price_oracle,
                pre_state=pre_state,
                post_state=post_state,
            )

            # Phase 4: stamp deployment_id and execution_mode onto the entry (VIB-2835/2837).
            # VIB-3157: tri-state (dry_run / live / paper) via the shared
            # ``derive_execution_mode_from_config`` helper so ledger,
            # snapshot, and metrics stamping stay in lockstep.
            deployment_id = strategy.deployment_id
            execution_mode = self._derive_execution_mode()
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
                        deployment_id=strategy.deployment_id,
                        message="State manager does not provide save_ledger_entry",
                    )
                logger.error(
                    "Ledger write unavailable in non-live mode for %s "
                    "(continuing, pre-prod drift; fix before promoting to live)",
                    strategy.deployment_id,
                )
            else:
                # VIB-3201 closed the gateway ledger gap (SaveLedgerEntry RPC).
                # The fail-closed contract now applies uniformly: any exception
                # propagates to the AccountingPersistenceError path below. No
                # backend-specific NotImplementedError escape hatch remains.
                #
                # VIB-4198 / T12 — registry-mode dispatch. When the boot
                # guard has cleared the (Primitive.LP, 'lp') cutover for
                # this deployment AND the intent is a UniV3 LP_OPEN /
                # LP_CLOSE that landed on-chain successfully AND the
                # parser produced a valid registry payload, route through
                # the atomic ledger+registry primitive instead of the
                # bare save_ledger_entry path. Falls back to plain
                # save_ledger_entry on any miss (cutover not active, not
                # a UniV3 LP intent, parser couldn't build payload).
                used_atomic = await self._maybe_save_ledger_with_registry(
                    strategy=strategy,
                    intent=intent,
                    result=result,
                    success=success,
                    entry=entry,
                )
                if not used_atomic:
                    await self.state_manager.save_ledger_entry(entry)

            # Emit position event whenever the chain TX succeeded — the framework
            # ``success`` verdict can be False on slippage-breach / reconciliation-
            # failure paths even though the on-chain state already changed. Without
            # this, ledger.success=False rows whose underlying TX landed leave
            # ``position_events`` and ``_recent_open_events`` desynced from chain
            # reality, and close-time IL attribution loses its OPEN bracket.
            #
            # VIB-4895: the teardown lane passes ``emit_position_event=False`` and
            # owns the emit explicitly (``commit_teardown_intent`` Step 2b). Skipping
            # it here avoids a duplicate CLOSE row on the teardown path.
            chain_success = bool(getattr(result, "success", False))
            if (
                emit_position_event
                and chain_success
                and self.state_manager
                and hasattr(self.state_manager, "save_position_event")
            ):
                await self._emit_position_event_for_intent(
                    strategy=strategy,
                    intent=intent,
                    result=result,
                    entry=entry,
                    chain=chain,
                    deployment_id=deployment_id,
                    execution_mode=execution_mode,
                    cycle_id=cycle_id,
                    price_oracle=price_oracle,
                    post_state=post_state,
                    pre_state=pre_state,
                )

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
                strategy.deployment_id,
            )
        except Exception as e:  # noqa: BLE001
            # Unexpected failure outside the persistence path (build_ledger_entry
            # raised, position_event emission re-raised, etc.). Live mode still
            # escalates -- a trade happened with no durable record.
            if self._is_live_mode():
                raise AccountingPersistenceError(
                    write_kind="ledger",
                    deployment_id=strategy.deployment_id,
                    cause=e,
                ) from e
            logger.error(f"Failed to write ledger entry (non-live): {e}")
        return None

    @staticmethod
    def _registry_intent_type_str(intent: AnyIntent) -> str:
        """Canonical intent-type string for the registry dispatch gate.

        Returns ``"LP_OPEN"`` / ``"LP_CLOSE"`` / ``""`` (empty for any
        non-LP type — the caller treats that as a path-applicability
        miss).
        """
        intent_type_val = getattr(getattr(intent, "intent_type", None), "value", None) or getattr(
            intent, "intent_type", None
        )
        return str(intent_type_val).upper() if intent_type_val is not None else ""

    def _registry_resolve_chain_and_nft_manager(
        self,
        strategy: StrategyProtocol,
        intent_type_str: str,
        protocol: str = "",
    ) -> tuple[str, str] | None:
        """Resolve ``(chain, nft_manager_addr)`` from the strategy.

        Returns ``None`` and INFO-logs when no canonical NPM address is
        registered for the strategy's (chain, protocol) pair (the caller
        falls back to ``save_ledger_entry``).

        ``protocol`` is consulted because Slipstream forks (Aerodrome on
        Base, Velodrome on Optimism) ship their OWN NonfungiblePositionManager
        contract at a different address than the canonical Uniswap V3 NPM
        on the same chain. Using the wrong NPM here would silently corrupt
        the ``physical_identity_hash`` tuple (T08 invariant #1) — the hash
        would not match the on-chain emitter address, and lookups against
        ``position_registry`` would consistently miss.
        """
        from almanak.framework.migration.backfill import _nft_manager_for_protocol_chain

        chain = getattr(strategy, "chain", "") or getattr(self.config, "chain", "")
        chain = (chain or "").lower()
        protocol_norm = (protocol or "").lower()
        nft_manager = _nft_manager_for_protocol_chain(protocol_norm, chain)
        if not nft_manager:
            logger.info(
                "Registry-mode skip: no NPM known for (protocol=%r, chain=%r); "
                "falling back to save_ledger_entry for %s",
                protocol_norm,
                chain,
                intent_type_str,
            )
            return None
        return chain, nft_manager

    def _registry_resolve_receipt_and_parser(
        self,
        *,
        result: Any,
        chain: str,
        intent_type_str: str,
        protocol: str = "",
    ) -> tuple[dict, Any] | None:
        """Resolve ``(receipt, parser)`` from the execution result.

        Returns ``None`` and INFO-logs when (a) the receipt isn't
        recoverable from the result shape or (b) the parser import fails
        (defensive — module load shouldn't fail in production).

        ``protocol`` selects the protocol-specific parser class because
        Slipstream forks emit ``IncreaseLiquidity`` / ``DecreaseLiquidity``
        events from a different NPM address than canonical Uniswap V3.
        The Uniswap V3 parser filters those events by its own NPM address
        and would silently return ``None`` from
        ``extract_lp_open_data`` / ``extract_lp_close_data`` on a Slipstream
        receipt (this exact bug, VIB-4305, was caught in production on
        lp_aerodrome).
        """
        receipt = self._extract_receipt_from_result(result)
        if receipt is None:
            logger.info(
                "Registry-mode skip: no receipt on result for %s; falling back to save_ledger_entry",
                intent_type_str,
            )
            return None
        protocol_norm = (protocol or "").lower()
        # Resolve to a receipt-parser registry key rather than importing each
        # connector's parser by name (VIB-4932). The per-protocol routing
        # rationale is unchanged — each fork emits IncreaseLiquidity /
        # DecreaseLiquidity from its own NPM address, so the canonical Uniswap
        # V3 parser would filter every event out by NPM-address and silently
        # return None (the ghost-position class VIB-4305 caught for Slipstream;
        # the same failure mode applies to Sushi and PancakeSwap). The registry
        # routes each key to the fork-specific parser class:
        #   * ``sushiswap_v3``   -> SushiSwapV3ReceiptParser
        #   * ``pancakeswap_v3`` -> PancakeSwapV3ReceiptParser
        #   * ``aerodrome_slipstream`` -> AerodromeSlipstreamReceiptParser
        # ``velodrome_slipstream`` is not a registered key (and is not a
        # protocol-alias rename), so it is mapped to ``aerodrome_slipstream``
        # here exactly as the pre-VIB-4932 branch did. Every other protocol —
        # including ``uniswap_v3`` itself, the empty string, and anything
        # unknown — falls back to the canonical Uniswap V3 parser, preserving
        # the original default-to-UV3 behaviour (the registry would otherwise
        # raise ``ValueError`` on an unknown key).
        from almanak.framework.execution.receipt_registry import get_parser

        if protocol_norm == "velodrome_slipstream":
            parser_key = "aerodrome_slipstream"
        elif protocol_norm in ("aerodrome_slipstream", "sushiswap_v3", "pancakeswap_v3"):
            parser_key = protocol_norm
        else:
            parser_key = "uniswap_v3"
        try:
            parser = get_parser(parser_key, chain=chain)
        except Exception:  # noqa: BLE001 — defensive: parser import/construction failure
            return None
        if parser is None:
            # Defensive: a registry that resolves to None without raising
            # would otherwise return ``(receipt, None)`` and trip an
            # AttributeError downstream. Honour the "return None on failure"
            # contract instead.
            return None
        return receipt, parser

    def _build_lp_open_registry_row(
        self,
        *,
        strategy: StrategyProtocol,
        intent: AnyIntent,
        result: Any,
        entry: Any,
        chain: str,
        nft_manager: str,
        receipt: dict,
        parser: Any,
        fee_tier: int | None,
    ) -> tuple[Any, dict, int] | None:
        """Build the LP_OPEN ``RegistryRow`` from the receipt.

        Returns ``(registry_row, payload, token_id)`` on success, or
        ``None`` when the parser couldn't produce a valid payload (the
        caller falls back to ``save_ledger_entry`` with an INFO log).
        """
        from almanak.framework.migration.backfill import (
            physical_identity_hash_univ3,
            semantic_grouping_key_univ3,
        )
        from almanak.framework.primitives.types import Primitive

        payload = parser.extract_registry_payload_open(receipt, fee_tier=fee_tier)
        if payload is None:
            logger.info(
                "Registry-mode skip: parser returned no LP_OPEN registry payload "
                "(token_id / pool / ticks missing); falling back to "
                "save_ledger_entry",
            )
            return None
        try:
            token_id = int(payload["token_id"])
        except (KeyError, TypeError, ValueError):
            logger.info(
                "Registry-mode skip: parser payload missing valid token_id; falling back",
            )
            return None
        pih = physical_identity_hash_univ3(
            chain=chain,
            nft_manager_addr=nft_manager,
            token_id=token_id,
        )
        sgk = semantic_grouping_key_univ3(
            chain=chain,
            pool_address=str(payload["pool_address"]),
        )
        opened_at_block = self._extract_block_number_from_result(result)
        registry_row = self._build_registry_row(
            strategy=strategy,
            primitive=Primitive.LP,
            physical_identity_hash=pih,
            semantic_grouping_key=sgk,
            payload=payload,
            status="open",
            opened_at_block=opened_at_block,
            opened_tx=getattr(entry, "tx_hash", "") or None,
            closed_at_block=None,
            closed_tx=None,
            handle=getattr(intent, "registry_handle", None),
        )
        return registry_row, payload, token_id

    async def _build_lp_close_registry_row(
        self,
        *,
        strategy: StrategyProtocol,
        intent: AnyIntent,
        result: Any,
        entry: Any,
        chain: str,
        nft_manager: str,
        receipt: dict,
        parser: Any,
        fee_tier: int | None,
    ) -> tuple[Any, dict, int] | None:
        """Build the LP_CLOSE ``RegistryRow`` from the receipt.

        Looks up the matching OPEN-side row first (so OPEN-time fields
        merge into the close payload via ``extract_registry_payload_close``).
        Returns ``(registry_row, payload, token_id)`` on success, or
        ``None`` on any path-miss (parser refuse, missing token_id, etc.).
        """
        from almanak.framework.migration.backfill import (
            physical_identity_hash_univ3,
            semantic_grouping_key_univ3,
        )
        from almanak.framework.primitives.types import Primitive

        open_payload = await self._lookup_open_registry_payload(
            deployment_id=strategy.deployment_id,
            chain=chain,
            token_id=None,
            receipt=receipt,
            parser=parser,
        )
        payload = parser.extract_registry_payload_close(
            receipt,
            open_payload=open_payload,
            fee_tier=fee_tier,
        )
        if payload is None:
            logger.info(
                "Registry-mode skip: parser returned no LP_CLOSE registry payload "
                "(token_id / pool missing); falling back to save_ledger_entry",
            )
            return None
        try:
            token_id = int(payload["token_id"])
        except (KeyError, TypeError, ValueError):
            return None
        pih = physical_identity_hash_univ3(
            chain=chain,
            nft_manager_addr=nft_manager,
            token_id=token_id,
        )
        sgk = semantic_grouping_key_univ3(
            chain=chain,
            pool_address=str(payload["pool_address"]),
        )
        closed_at_block = self._extract_block_number_from_result(result)
        opened_at_block = (open_payload or {}).get("opened_at_block") if isinstance(open_payload, dict) else None
        opened_tx = (open_payload or {}).get("opened_tx") if isinstance(open_payload, dict) else None
        registry_row = self._build_registry_row(
            strategy=strategy,
            primitive=Primitive.LP,
            physical_identity_hash=pih,
            semantic_grouping_key=sgk,
            payload=payload,
            status="closed",
            opened_at_block=opened_at_block,
            opened_tx=opened_tx,
            closed_at_block=closed_at_block,
            closed_tx=getattr(entry, "tx_hash", "") or None,
            handle=getattr(intent, "registry_handle", None),
        )
        return registry_row, payload, token_id

    def _update_lp_registry_id_cache(
        self,
        *,
        chain: str,
        pool_addr: str,
        token_id: int,
        is_open: bool,
    ) -> None:
        """Sync the ``_lp_registry_id_cache`` after a registry-mode write.

        VIB-4301: the cache value is the SET of open token_ids for the
        ``(protocol, chain, pool_address)`` key. An OPEN adds its token_id to
        the set; a CLOSE removes it. Co-pool NFTs (a delta-neutral hedge or any
        multi-NFT-per-pool strategy) therefore coexist in the set rather than
        colliding — there is no cache thrash and no spurious "multi-NFT"
        warning on a legitimate second open. The reader (``_sync_lookup``) only
        auto-injects when the set has exactly one element; with N>1 the strategy
        supplies ``position_id`` on the close intent and the tracker injects
        nothing. The registry's authoritative read remains
        ``get_position_registry_open_rows`` keyed on ``physical_identity_hash``.
        """
        from almanak.framework.migration.backfill import _UNIV3_LP_PROTOCOLS

        cache: dict[tuple[str, str, str], set[str]] = getattr(self, "_lp_registry_id_cache", {})
        if not pool_addr:
            self._lp_registry_id_cache = cache
            return
        token_str = str(token_id)
        for protocol_slug in _UNIV3_LP_PROTOCOLS:
            key = (protocol_slug, chain, pool_addr)
            if is_open:
                cache.setdefault(key, set()).add(token_str)
            else:  # closed — drop just this leg; co-pool siblings stay.
                siblings = cache.get(key)
                if siblings is not None:
                    siblings.discard(token_str)
                    if not siblings:
                        cache.pop(key, None)
        self._lp_registry_id_cache = cache

    async def _maybe_save_ledger_with_registry(
        self,
        *,
        strategy: StrategyProtocol,
        intent: AnyIntent,
        result: Any | None,
        success: bool,
        entry: Any,
    ) -> bool:
        """Route UniV3 LP_OPEN / LP_CLOSE through ``save_ledger_and_registry``.

        VIB-4198 / T12 — registry-mode atomic write hook. Returns ``True``
        if the call routed through the atomic primitive (ledger + registry
        + handle in one transaction); ``False`` if the path didn't apply
        and the caller should fall back to plain ``save_ledger_entry``.

        Path-applicability gate (ALL must hold):

        - UniV3 LP cutover boot-guard has cleared ``(Primitive.LP, 'lp')``.
        - ``intent.intent_type`` is ``IntentType.LP_OPEN`` or ``IntentType.LP_CLOSE``.
        - ``intent.protocol`` is one of the UniV3 LP family
          (uniswap_v3 / sushiswap_v3 / pancakeswap_v3 / aerodrome_slipstream / velodrome_slipstream).
        - The on-chain TX landed (``result.success``) — chain truth, NOT
          the framework verdict (audit P1: slippage / reconciliation can
          flip ``success`` to False post-confirmation, but registry must
          still record the landed state).
        - The strategy's chain matches a known NPM address.
        - The parser produced a valid registry payload from the receipt
          (``extract_registry_payload_open`` / ``_close`` returned
          non-None).

        Any miss falls back to ``save_ledger_entry`` with an INFO log so
        the runner's behavior is identical to the pre-cutover path. Per
        CLAUDE.md "Empty ≠ zero": we NEVER substitute a fabricated value
        to make the registry write succeed.

        Failures inside the atomic primitive propagate as
        ``AccountingPersistenceError`` per T11's contract; the caller's
        fail-closed pipeline (VIB-3157 / VIB-3762) handles it.

        CRAP refactor (VIB-4198 round 8): the path-applicability gate,
        OPEN-side row build, CLOSE-side row build, atomic write, and
        cache update each live in dedicated helpers
        (``_registry_intent_type_str``,
        ``_registry_resolve_chain_and_nft_manager``,
        ``_registry_resolve_receipt_and_parser``,
        ``_build_lp_open_registry_row``, ``_build_lp_close_registry_row``,
        ``_update_lp_registry_id_cache``). This orchestrator sequences
        them. Each helper has narrow responsibility, narrow cc, and is
        unit-tested in isolation.
        """
        try:
            from almanak.framework.migration.backfill import _UNIV3_LP_PROTOCOLS
            from almanak.framework.primitives.types import Primitive
            from almanak.framework.runner.cutover import is_cutover_active
        except Exception:  # noqa: BLE001 — guard against optional import miss
            return False

        # Path-applicability gate — boot guard, intent type, protocol family,
        # chain truth (audit P1).
        if not is_cutover_active(self, Primitive.LP, "lp"):
            return False
        intent_type_str = self._registry_intent_type_str(intent)
        if intent_type_str not in ("LP_OPEN", "LP_CLOSE"):
            return False
        protocol = (getattr(intent, "protocol", "") or "").lower()
        if protocol not in _UNIV3_LP_PROTOCOLS:
            return False
        if result is None or not bool(getattr(result, "success", False)):
            return False
        # ``success`` is forwarded only for telemetry; do NOT gate on it.
        _ = success

        # Resolve chain + NPM + receipt + parser. Each step short-circuits
        # to ``False`` on miss with an INFO log inside the helper. Protocol
        # is threaded through so Slipstream forks select the correct NPM
        # address AND the correct receipt parser class (VIB-4305).
        chain_resolved = self._registry_resolve_chain_and_nft_manager(strategy, intent_type_str, protocol)
        if chain_resolved is None:
            return False
        chain, nft_manager = chain_resolved
        receipt_resolved = self._registry_resolve_receipt_and_parser(
            result=result, chain=chain, intent_type_str=intent_type_str, protocol=protocol
        )
        if receipt_resolved is None:
            return False
        receipt, parser = receipt_resolved
        fee_tier = self._intent_fee_tier(intent)

        # Build the registry row by intent type.
        if intent_type_str == "LP_OPEN":
            built = self._build_lp_open_registry_row(
                strategy=strategy,
                intent=intent,
                result=result,
                entry=entry,
                chain=chain,
                nft_manager=nft_manager,
                receipt=receipt,
                parser=parser,
                fee_tier=fee_tier,
            )
        else:  # LP_CLOSE
            built = await self._build_lp_close_registry_row(
                strategy=strategy,
                intent=intent,
                result=result,
                entry=entry,
                chain=chain,
                nft_manager=nft_manager,
                receipt=receipt,
                parser=parser,
                fee_tier=fee_tier,
            )
        if built is None:
            return False
        registry_row, payload, token_id = built

        from almanak.framework.accounting.commit import save_ledger_and_registry

        await save_ledger_and_registry(
            self.state_manager,
            ledger=entry,
            registry=registry_row,
            mode="registry",
        )
        # Keep the LP-tracker registry-id cache in sync so subsequent
        # LP_CLOSE intents see the OPEN row's token_id without an extra
        # state-manager read. The collision-aware semantics live in
        # ``_update_lp_registry_id_cache``.
        self._update_lp_registry_id_cache(
            chain=chain,
            pool_addr=str(payload.get("pool_address") or "").lower(),
            token_id=token_id,
            is_open=registry_row.status == "open",
        )
        logger.info(
            "Registry-mode write OK for %s on %s NFT=%s pih=%s",
            intent_type_str,
            chain,
            token_id,
            registry_row.physical_identity_hash,
        )
        return True

    def _build_registry_row(
        self,
        *,
        strategy: StrategyProtocol,
        primitive: Any,
        physical_identity_hash: str,
        semantic_grouping_key: str,
        payload: dict,
        status: Literal["open", "closed", "reorg_invalidated"],
        opened_at_block: int | None,
        opened_tx: str | None,
        closed_at_block: int | None,
        closed_tx: str | None,
        handle: str | None,
    ) -> Any:
        """Construct a ``RegistryRow`` for the runtime atomic-primitive call.

        Pulled out as a helper so the LP_OPEN and LP_CLOSE call sites in
        :meth:`_maybe_save_ledger_with_registry` share one row-construction
        implementation. Stamps ``matching_policy_version`` from
        ``MatchingPolicy.for_primitive(primitive)`` (T09 contract — never
        hardcoded). Stamps ``grouping_policy_version`` from the per-primitive
        constant.
        """
        from almanak.framework.accounting.commit import RegistryRow
        from almanak.framework.accounting.policy import MatchingPolicy
        from almanak.framework.migration.backfill import _UNIV3_GROUPING_POLICY_VERSION
        from almanak.framework.primitives.types import AccountingCategory

        deployment_id = strategy.deployment_id
        chain = (getattr(strategy, "chain", "") or getattr(self.config, "chain", "") or "").lower()
        return RegistryRow(
            deployment_id=deployment_id,
            chain=chain,
            primitive=primitive,
            accounting_category=AccountingCategory.LP,
            physical_identity_hash=physical_identity_hash,
            semantic_grouping_key=semantic_grouping_key,
            grouping_policy_version=_UNIV3_GROUPING_POLICY_VERSION,
            handle=handle,
            status=status,
            payload=dict(payload),
            opened_at_block=opened_at_block,
            opened_tx=opened_tx,
            closed_at_block=closed_at_block,
            closed_tx=closed_tx,
            last_reconciled_at_block=None,
            matching_policy_version=MatchingPolicy.for_primitive(primitive),
        )

    @staticmethod
    def _coerce_receipt_to_dict(r: Any) -> dict | None:
        """Coerce a Receipt-shaped object (dict / object with ``to_dict`` /
        object with ``logs``) into a dict carrying ``logs``, or ``None``.

        Mirrors the canonical coercion used in
        :mod:`almanak.framework.execution.receipt_registry`. Pulled out as
        a helper so :meth:`_extract_receipt_from_result` stays under the
        CRAP threshold while still walking every candidate-source.
        """
        if r is None:
            return None
        if isinstance(r, dict):
            return r if r.get("logs") is not None else None
        if hasattr(r, "to_dict"):
            d = r.to_dict()
            if isinstance(d, dict) and d.get("logs") is not None:
                return d
        if hasattr(r, "logs"):
            logs = r.logs
            if logs is not None:
                return {"logs": logs}
        return None

    @staticmethod
    def _collect_candidate_receipts(result: Any) -> list[dict]:
        """Collect every dict-shaped receipt candidate from a result.

        Walks the four shapes the framework actually produces:

        - Singular ``transaction_receipt`` / ``receipt`` / ``tx_receipt`` /
          ``raw_receipt`` attrs (legacy single-tx).
        - ``transaction_results[*].receipt`` (local ``ExecutionResult`` —
          dominant shape on Anvil-managed gateway runs).
        - ``receipts`` / ``transaction_receipts`` lists
          (``GatewayExecutionResult``).
        """
        candidates: list[dict] = []

        for attr in ("transaction_receipt", "receipt", "tx_receipt", "raw_receipt"):
            d = StrategyRunner._coerce_receipt_to_dict(getattr(result, attr, None))
            if d is not None:
                candidates.append(d)

        tx_results = getattr(result, "transaction_results", None)
        if tx_results is None and isinstance(result, dict):
            tx_results = result.get("transaction_results")
        if isinstance(tx_results, list):
            for tx in tx_results:
                if isinstance(tx, dict):
                    if not tx.get("success", True):
                        continue
                    rec = tx.get("receipt")
                else:
                    if not getattr(tx, "success", True):
                        continue
                    rec = getattr(tx, "receipt", None)
                d = StrategyRunner._coerce_receipt_to_dict(rec)
                if d is not None:
                    candidates.append(d)

        receipts = getattr(result, "receipts", None) or getattr(result, "transaction_receipts", None)
        if isinstance(receipts, list):
            for r in receipts:
                d = StrategyRunner._coerce_receipt_to_dict(r)
                if d is not None:
                    candidates.append(d)

        return candidates

    @staticmethod
    def _receipt_has_lp_topic(rec: dict) -> bool:
        """True iff the receipt carries an NPM IncreaseLiquidity /
        DecreaseLiquidity topic.

        Pulled out so :meth:`_extract_receipt_from_result` can ask the
        question without inlining the topic-extraction loop.
        """
        from almanak.connectors._strategy_base.runner_hook_registry import RunnerHookRegistryError
        from almanak.connectors._strategy_runner_hook_registry import STRATEGY_RUNNER_HOOK_REGISTRY

        lp_topics_lower = STRATEGY_RUNNER_HOOK_REGISTRY.lp_receipt_topics()
        if not lp_topics_lower:
            raise RunnerHookRegistryError("No LP receipt topics are registered; refusing to guess the LP receipt")
        for log in rec.get("logs") or []:
            topics = log.get("topics") if isinstance(log, dict) else getattr(log, "topics", None)
            if not topics:
                continue
            first = topics[0]
            if isinstance(first, bytes):
                first = "0x" + first.hex()
            first = str(first).lower()
            if not first.startswith("0x"):
                first = "0x" + first
            if first in lp_topics_lower:
                return True
        return False

    @staticmethod
    def _extract_receipt_from_result(result: Any) -> dict | None:
        """Pull the raw EVM receipt dict out of an execution result.

        Audit P1 (CodeRabbit + Codex): the previous shape (a) missed the
        local ``ExecutionResult.transaction_results[*].receipt`` shape
        entirely (fall-back to ``save_ledger_entry`` for every gateway-
        backed run), and (b) for ``GatewayExecutionResult.receipts`` /
        ``transaction_receipts`` lists, picked the first receipt with
        any logs — typically the approval, not the NPM mint/burn the
        registry parser needs.

        The fix:

        1. Walk every candidate-source the framework uses (see
           :meth:`_collect_candidate_receipts`).
        2. Among the collected receipts, prefer one that carries the NPM
           ``IncreaseLiquidity`` (LP_OPEN) or ``DecreaseLiquidity``
           (LP_CLOSE) topic. Fall back to the LAST receipt with logs
           when no LP-event-bearing receipt is found — in multi-tx
           bundles the terminal TX is the one that mutated the position
           (the prefix is approves / swaps).

        Returns ``None`` when no candidate yields a receipt with logs.
        """
        if result is None:
            return None
        candidates = StrategyRunner._collect_candidate_receipts(result)
        if not candidates:
            return None
        for cand in candidates:
            if StrategyRunner._receipt_has_lp_topic(cand):
                return cand
        return candidates[-1]

    @staticmethod
    def _extract_block_number_from_result(result: Any) -> int | None:
        """Best-effort block number extraction from the result's receipt."""
        if result is None:
            return None
        rec = StrategyRunner._extract_receipt_from_result(result)
        if rec is None:
            return None
        bn = rec.get("blockNumber")
        if isinstance(bn, int):
            return bn
        try:
            return int(bn) if bn is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _intent_fee_tier(intent: Any) -> int | None:
        """Recover ``fee_tier`` from an LP intent's protocol_params if set."""
        if intent is None:
            return None
        params = getattr(intent, "protocol_params", None) or {}
        if isinstance(params, dict):
            ft = params.get("fee_tier") or params.get("feeTier")
            try:
                return int(ft) if ft is not None else None
            except (TypeError, ValueError):
                return None
        return None

    async def _lookup_open_registry_payload(
        self,
        *,
        deployment_id: str,
        chain: str,
        token_id: int | None,
        receipt: dict,
        parser: Any,
    ) -> dict | None:
        """Find the OPEN-side registry payload for a close-side write.

        The close needs OPEN-time fields (ticks, liquidity, fee_tier,
        token labels) the close receipt does not carry. We look those up
        from ``position_registry`` keyed on the close receipt's token_id
        — the OPEN row is the first authoritative carrier of those
        fields.

        Returns the payload dict (parsed JSON) or ``None`` when no OPEN
        row exists yet — in that case the close path uses
        ``open_payload=None`` and the registry row has a minimal payload
        (still correct under blueprint 28 §4.3 ON CONFLICT semantics).
        """
        if token_id is None:
            token_id = parser._decreaseliquidity_token_id(receipt)
        if token_id is None:
            return None
        try:
            rows = await self.state_manager.get_position_registry_open_rows(
                deployment_id,
                chain=chain,
                primitive="lp",
                accounting_category="lp",
            )
        except Exception:  # noqa: BLE001 — best effort
            return None
        for row in rows:
            payload = row.get("payload") or {}
            if not isinstance(payload, dict):
                continue
            try:
                if int(payload.get("token_id", 0)) == int(token_id):
                    enriched = dict(payload)
                    if row.get("opened_at_block") is not None:
                        enriched["opened_at_block"] = row["opened_at_block"]
                    if row.get("opened_tx"):
                        enriched["opened_tx"] = row["opened_tx"]
                    return enriched
            except (TypeError, ValueError):
                continue
        return None

    async def get_open_lp_positions_from_registry(
        self,
        *,
        deployment_id: str | None = None,
        chain: str | None = None,
    ) -> list[dict]:
        """Return open UniV3 LP rows from ``position_registry``.

        VIB-4198 / T12 acceptance criterion #5 — runner-side helper that
        the teardown / dashboard / strategy-author surfaces consult after
        cutover. Filters to ``primitive='lp' AND accounting_category='lp'
        AND status='open'`` so other LP-family primitives (Pendle LP,
        future TraderJoe LB) do not bleed into the UniV3 result. Returns
        an empty list when:

        - The state manager backend doesn't support registry reads
          (hosted gateway today; T19 lands the equivalent).
        - No rows match.
        - The boot guard hasn't cleared the cutover for this deployment
          (defense-in-depth — stale callers can't accidentally read a
          half-populated registry).
        """
        from almanak.framework.primitives.types import Primitive
        from almanak.framework.runner.cutover import is_cutover_active

        if not is_cutover_active(self, Primitive.LP, "lp"):
            return []
        # Audit M3: defense-in-depth. ``is_cutover_active=True`` means the
        # boot guard already cleared the storage check; the
        # ``CutoverStorageNotSupported`` should not fire here. Catching
        # it anyway leaves the helper safe under a hypothetical future
        # state-manager swap mid-run.
        from almanak.framework.migration import CutoverStorageNotSupported

        if self.state_manager is None:
            return []
        dep = deployment_id or ""
        try:
            return await self.state_manager.get_position_registry_open_rows(
                dep,
                chain=chain,
                primitive="lp",
                accounting_category="lp",
            )
        except (CutoverStorageNotSupported, NotImplementedError):
            return []

    async def _emit_position_event_for_intent(
        self,
        *,
        strategy: StrategyProtocol,
        intent: AnyIntent,
        result: Any | None,
        entry: Any,
        chain: str,
        deployment_id: str,
        execution_mode: str,
        cycle_id: str,
        price_oracle: dict | None,
        post_state: dict | None,
        pre_state: dict | None = None,
    ) -> None:
        """VIB-2775 / VIB-3919 / VIB-4085 — build a position_event from a
        successful intent result and persist it, then run the OPEN/CLOSE
        side-effects (entry_state stamp, IL attribution, runner cache).

        Extracted out of ``_write_ledger_entry`` to keep that method's
        cyclomatic complexity down. Live-mode save failures raise
        ``AccountingPersistenceError`` so the cycle halts and operators
        are alerted; paper/dry-run modes log ERROR and continue.
        """
        try:
            from ..intents.vocabulary import IntentType
            from ..observability.position_events import build_position_event_from_intent

            # VIB-4085 — wallet address scopes lending position_id. Try the
            # runtime config first (the runner-side source of truth) and
            # fall back to the strategy's declared wallet so dry-run /
            # paper paths still produce a deterministic position_id.
            wallet_address = (
                getattr(getattr(self, "_runtime_config", None), "wallet_address", "")
                or getattr(strategy, "wallet_address", "")
                or ""
            )

            # VIB-4839 — durable-storage fallback for LP_CLOSE column
            # carry-forward.  When the in-memory ``_recent_open_events`` cache
            # lacks the matching OPEN (cross-process restart, hosted-mode
            # hydration gap, signal-driven teardown on a fresh process), pull
            # the OPEN bracket from durable storage and seed the cache so
            # ``_apply_lp_close_columns`` carries token0/token1/ticks/liquidity
            # through and ``_apply_lp_close_value_usd`` can compute value_usd.
            # Pre-fix: lp_triple May-22 → May-26 teardown CLOSE rows landed
            # with ``value_usd=''`` and ``principal_recovered_usd=0``, making
            # per-leg PnL look like a full-principal loss.
            #
            # Position-id precedence MUST match ``_seed_event`` (result first,
            # then intent — see ``position_events.py:_seed_event``).  A
            # mismatch would seed the cache under one key while
            # ``_apply_lp_close_columns`` looks up another, defeating the
            # carry-forward.  (Codex P2 review on this PR.)
            if getattr(intent, "intent_type", None) == IntentType.LP_CLOSE:
                # Use getattr for both surfaces — ``intent`` is the AnyIntent
                # union which does not expose ``position_id`` uniformly, and
                # ``result`` may be ``None``.  Direct attribute access trips
                # the typechecker's union-narrowing.
                result_pid = str(getattr(result, "position_id", "") or "")
                intent_pid = str(getattr(intent, "position_id", "") or "")
                lp_position_id = result_pid or intent_pid
                if lp_position_id:
                    key = (lp_position_id, "LP")
                    if key not in self._recent_open_events:
                        durable_payload = await self._hydrate_lp_close_from_durable_store(
                            deployment_id=deployment_id,
                            position_id=lp_position_id,
                        )
                        if durable_payload is not None:
                            self._recent_open_events[key] = durable_payload

            pos_event = build_position_event_from_intent(
                deployment_id=deployment_id,
                intent=intent,
                result=result,
                ledger_entry_id=entry.id,
                chain=chain,
                price_oracle=price_oracle,
                # VIB-3919: LP_CLOSE bracket carry-forward.
                # VIB-4085: lending OPEN-vs-INCREASE refinement.
                recent_open_events=self._recent_open_events,
                # VIB-4085: lending CLOSE-vs-DECREASE refinement.
                post_state=post_state,
                # VIB-4493: lending CLOSE value_usd derives pre-close
                # balance from pre_state (post-state is 0 at CLOSE).
                pre_state=pre_state,
                wallet_address=wallet_address,
            )
            if pos_event is None:
                return

            pos_event.cycle_id = cycle_id
            pos_event.execution_mode = execution_mode
            saved = await self.state_manager.save_position_event(pos_event)

            if not saved:
                # Live mode raises inside _handle_position_event_save_failure;
                # paper/dry-run logs ERROR and continues. In the latter case
                # we must NOT run attribution side-effects: stamping
                # entry-state for a position event that isn't on disk leaves
                # ``position_state_snapshots`` referencing a non-existent
                # event row and makes the degraded path driftier than it
                # needs to be.
                self._handle_position_event_save_failure(strategy, pos_event)
                return

            logger.debug(
                "Position event %s emitted for %s (position=%s)",
                pos_event.event_type,
                pos_event.position_type,
                pos_event.position_id,
            )
            # VIB-3894 — only update cache on save success so we don't
            # surface cost_basis_usd for a position the books don't know about.
            if pos_event.position_id:
                self._update_recent_open_events_cache(pos_event)

            await self._run_position_event_attribution(pos_event)
        except AccountingPersistenceError:
            # Fail-closed: re-raise so run_iteration routes to ACCOUNTING_FAILED.
            raise
        except Exception as pe:  # noqa: BLE001
            logger.error(
                "Failed to emit position event for %s (position PnL enrichment lost): %s",
                getattr(intent, "intent_type", "unknown"),
                pe,
            )

    def _handle_position_event_save_failure(
        self,
        strategy: StrategyProtocol,
        pos_event: Any,
    ) -> None:
        """Save returned False — fail closed in live mode, log ERROR otherwise.

        Position events are the cost-basis and IL attribution source for
        LP/perp; a silent failure produces null PnL at close-time.
        """
        if self._is_live_mode():
            from ..state.exceptions import AccountingWriteKind

            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.ACCOUNTING,
                deployment_id=strategy.deployment_id,
                message=(
                    f"Position event save failed for {pos_event.event_type} "
                    f"{pos_event.position_type} position={pos_event.position_id} "
                    "— IL/PnL enrichment lost"
                ),
            )
        logger.error(
            "Position event save returned False for %s %s (position=%s) "
            "— IL/PnL enrichment for this position will be incomplete",
            pos_event.event_type,
            pos_event.position_type,
            pos_event.position_id,
        )

    async def _run_position_event_attribution(self, pos_event: Any) -> None:
        """VIB-2776 / VIB-3205 — run OPEN/CLOSE attribution side-effects.

        Both calls are best-effort: failures degrade attribution accuracy
        but do not block the iteration.
        """
        if not pos_event.position_id:
            return
        if pos_event.event_type == "OPEN":
            try:
                from ..observability.pnl_attributor import stamp_entry_state_on_open

                await stamp_entry_state_on_open(
                    self.state_manager,
                    pos_event,
                    price_oracle=self.price_oracle,
                )
            except Exception:  # noqa: BLE001
                logger.warning(
                    "Entry-state stamp failed (non-blocking) for position=%s",
                    pos_event.position_id,
                    exc_info=True,
                )
        elif pos_event.event_type == "CLOSE":
            try:
                from ..observability.pnl_attributor import run_attribution_on_close

                await run_attribution_on_close(self.state_manager, pos_event)
            except Exception as attr_err:  # noqa: BLE001
                logger.debug("Attribution failed (non-blocking): %s", attr_err)

    async def _write_outbox_and_fire_processor(
        self,
        strategy: "StrategyProtocol",
        intent: "AnyIntent",
        ledger_entry_id: str,
        resolved_pool: str | None = None,
    ) -> None:
        """Write accounting_outbox row and fire asyncio task to drain it (VIB-3467).

        In live mode: raises AccountingPersistenceError if outbox_id is None (write
        failed) or on unexpected exceptions, so run_iteration routes to
        ACCOUNTING_FAILED and alerts operators.
        In non-live modes (paper / dry-run): logs an ERROR and continues
        (VIB-3762 §C2 — accounting drift in any mode is operator-visible
        ERROR, not a soft warning, because the silent-failure shape is
        what we are removing).
        NotImplementedError from write_outbox_entry is always non-fatal (VIB-3482:
        gateway backend not yet deployed); handled inline, strategy continues.
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
            deployment_id = strategy.deployment_id
            cycle_id = get_cycle_id() or ""

            # Compute position_key and market_id for each supported category
            position_key, market_id = self._compute_outbox_position_key(
                intent, intent_type_str, chain, wallet_address, resolved_pool=resolved_pool
            )

            # Update processor deployment_id (set once per strategy run)
            if self._accounting_processor._deployment_id != deployment_id:
                self._accounting_processor._deployment_id = deployment_id

            outbox_id = await write_outbox_entry(
                self.state_manager,
                deployment_id=strategy.deployment_id,
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
                # outbox_id is None: write failed. In live mode this is a
                # data-loss risk; raise so run_iteration routes to
                # ACCOUNTING_FAILED and alerts operators.
                if self._is_live_mode():
                    from ..state.exceptions import AccountingWriteKind

                    raise AccountingPersistenceError(
                        write_kind=AccountingWriteKind.ACCOUNTING,
                        deployment_id=strategy.deployment_id,
                        message=f"Outbox write failed for {ledger_entry_id!r} — accounting event will be lost",
                    )
                # VIB-3762 §C2: outbox drift is operator-visible ERROR in
                # paper/dry-run, not WARNING — accounting drift in any mode
                # is the silent-failure class we are removing.
                logger.error(
                    "_write_outbox_and_fire_processor: outbox write returned None for %s — drain skipped (non-live)",
                    ledger_entry_id,
                )
        except AccountingPersistenceError:
            raise
        except Exception as e:
            if self._is_live_mode():
                from ..state.exceptions import AccountingWriteKind

                raise AccountingPersistenceError(
                    write_kind=AccountingWriteKind.ACCOUNTING,
                    deployment_id=strategy.deployment_id,
                    message=f"_write_outbox_and_fire_processor failed for {ledger_entry_id!r}",
                    cause=e,
                ) from e
            # VIB-3762 §C2: lift non-live outbox failures to ERROR.
            logger.error("_write_outbox_and_fire_processor failed (non-live)", exc_info=True)

    async def commit_teardown_intent(
        self,
        strategy: "StrategyProtocol",
        intent: "AnyIntent",
        *,
        execution_result: Any,
        execution_context: Any,
        bundle_metadata: dict[str, Any] | None = None,
        teardown_cycle_id: str,
        pre_snapshot: Any | None = None,
        recon: dict[str, Any] | None = None,
        lending_pre_state: Any | None = None,
    ) -> "TeardownCommitOutcome":
        """Run the per-intent teardown commit pipeline (VIB-3773 Phase 0).

        Thin shim over :func:`runner.teardown_commit.commit_teardown_intent`.
        Mirrors :py:meth:`_single_chain_handle_success`'s post-execution body
        (enrich → ledger → outbox+fire → sidecar) but with degraded-but-
        continue semantics — never raises. See the helper module's docstring
        for the rationale (P0-2 from Codex review).
        """
        from .teardown_commit import (
            commit_teardown_intent as _commit_teardown_intent_impl,
        )

        return await _commit_teardown_intent_impl(
            self,
            strategy,
            intent,
            execution_result=execution_result,
            execution_context=execution_context,
            bundle_metadata=bundle_metadata,
            teardown_cycle_id=teardown_cycle_id,
            pre_snapshot=pre_snapshot,
            recon=recon,
            lending_pre_state=lending_pre_state,
        )

    # Pre-T4 this function was cc=37; VIB-4164 (T4) extracted the BRIDGE and
    # Prediction branches into module-level helpers (cc dropped to 27). The
    # architectural shape — a registry-pattern dispatch parallel to T3's
    # category_handlers.HANDLERS — is tracked in VIB-4222; bundling that
    # multi-branch refactor into the BRIDGE→TRANSFER PR violates
    # .claude/rules/crap-refactor.md (registry shape needs a plan-agent
    # design pass + truth-table parity precursor).
    # crap-allowlist: VIB-4222 — pre-existing primitive-dispatch ladder; T4 reduced cc 37→27
    def _compute_outbox_position_key(
        self,
        intent: "AnyIntent",
        intent_type_str: str,
        chain: str,
        wallet_address: str,
        resolved_pool: str | None = None,
    ) -> tuple[str, str]:
        """Return (position_key, market_id) for the given intent.

        Mirrors the position_key derivation logic in the inline accounting builders
        so the outbox row and accounting_events row use identical keys.

        ``resolved_pool`` (VIB-3946): the compiler-resolved canonical pool label
        (``metadata["pool_name"]``). For the generic LP branch it is threaded
        into ``_get_pool_address`` so the position_key uses the resolved label
        instead of re-parsing raw ``intent.pool``. ``None`` for every connector
        except Curve, so all other position keys are byte-identical.
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

            # Connectors with custom accounting publish their outbox position-key
            # derivation via the strategy-side registry (VIB-4931), so the runner
            # routes them without naming the protocol. Returns (position_key,
            # market_id) when a connector contributes a custom key, else None.
            from almanak.connectors._strategy_accounting_treatment_registry import (
                AccountingTreatmentRegistry,
            )

            registry_key = AccountingTreatmentRegistry.position_key_for(
                protocol, intent_type=t, chain=chain, wallet=wallet_address, intent=intent
            )
            if registry_key is not None:
                return registry_key

            # Generic SWAP — position key groups by chain+wallet for FIFO lot tracking.
            if t == "SWAP":
                position_key = (
                    f"swap:{chain.lower().strip()}:{wallet_address.lower().strip()}"
                    if (chain and wallet_address)
                    else ""
                )
                return position_key, ""

            # Generic LP (LP_OPEN / LP_CLOSE / LP_COLLECT_FEES). Connector-owned
            # accounting treatments opt out of this fallback even when they do not
            # publish a custom position key for a particular LP event.
            if (
                t in {"LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"}
                and AccountingTreatmentRegistry.categorize(t, protocol, "") is None
            ):
                from ..accounting.lp_accounting import _get_pool_address as _lp_pool_addr

                pool_address = _lp_pool_addr(intent, resolved_pool)
                if pool_address:
                    position_key = f"lp:{protocol}:{chain.lower()}:{wallet_address.lower()}:{pool_address}"
                return position_key, pool_address

            # Perp (PERP_OPEN / PERP_CLOSE / PERP_INCREASE / PERP_DECREASE / PERP_LIQUIDATE)
            if t in {"PERP_OPEN", "PERP_CLOSE", "PERP_INCREASE", "PERP_DECREASE", "PERP_LIQUIDATE"}:
                market = str(getattr(intent, "market", "") or "").lower().replace(" ", "_")
                position_key = f"perp:{protocol}:{chain.lower()}:{wallet_address.lower()}:{market}" if market else ""
                return position_key, market

            # Vault (VAULT_DEPOSIT / VAULT_WITHDRAW / VAULT_REDEEM / VAULT_HARVEST / VAULT_REALLOCATE)
            if t in {"VAULT_DEPOSIT", "VAULT_WITHDRAW", "VAULT_REDEEM", "VAULT_HARVEST", "VAULT_REALLOCATE"}:
                vault_address = (getattr(intent, "vault_address", "") or "").lower()
                position_key = (
                    f"vault:{protocol}:{chain.lower()}:{wallet_address.lower()}:{vault_address}"
                    if vault_address
                    else ""
                )
                return position_key, vault_address

            # Prediction (PREDICTION_BUY / PREDICTION_SELL / PREDICTION_REDEEM) — VIB-3707.
            # Delegated to module-level helper to keep this function under
            # the CRAP threshold.
            if t in {"PREDICTION_BUY", "PREDICTION_SELL", "PREDICTION_REDEEM"}:
                return _prediction_outbox_position_key(intent, protocol, chain, wallet_address)

            # BRIDGE — VIB-4164 (T4). Delegated to module-level helper to
            # keep `_compute_outbox_position_key`'s cyclomatic complexity
            # under the CRAP threshold.
            if t == "BRIDGE":
                return _bridge_outbox_position_key(intent, chain, wallet_address), ""

        except Exception:
            logger.debug("_compute_outbox_position_key failed", exc_info=True)

        return "", ""

    def _accounting_context(self, strategy: "StrategyProtocol") -> tuple[str, str, str, str, str]:
        """Return (deployment_id, cycle_id, execution_mode, chain, wallet_address) for accounting builders."""
        from ..observability.context import get_cycle_id

        deployment_id = strategy.deployment_id
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
            getattr(strategy, "deployment_id", ""),
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

        Delegates to `framework.deployment.is_hosted()` — `ALMANAK_IS_HOSTED` is the
        single deployment-mode signal across the SDK.
        """
        from almanak.framework.deployment import is_hosted

        return is_hosted()

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
            deployment_id=strategy.deployment_id,
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
        deployment_id = state.deployment_id

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

        # Build the gateway-backed prediction-market (CLOB) execution handler for
        # whatever connector claims this chain (VIB-4989: registry-driven; replaces
        # the hardcoded chain=="polygon" gate + the direct connector imports). The
        # handler owns its gateway-routed client; state.clob_client stays unset (its
        # only use is a no-op close() on teardown).
        if state.gateway_client is not None:
            from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry
            from almanak.connectors._strategy_base.prediction_execute_registry import (
                PredictionExecuteRegistry,
            )

            # Resolve the CLOB handler by the intent's protocol (falling back to the
            # compiler's default prediction protocol), not the first buildable handler
            # for the chain: on a chain with >1 prediction connector, compile/outbox
            # could target one protocol while execution binds another.
            resolved_protocol = (
                (getattr(intent, "protocol", "") or "") or CompilerRegistry.default_protocol("PREDICTION") or ""
            )
            if resolved_protocol:
                handler = PredictionExecuteRegistry.build_handler(
                    resolved_protocol, gateway_client=state.gateway_client
                )
                if handler is not None:
                    state.clob_handler = handler

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
            f"Created IntentStateMachine for {deployment_id} "
            f"(intent={intent.intent_id}, max_retries={self.config.max_retries})"
        )

        from almanak.framework.observability.emitter import emit_phase_event
        from almanak.framework.observability.events import StrategyPhase

        emit_phase_event(
            deployment_id=deployment_id,
            phase=StrategyPhase.COMPILE,
            event_type="STATE_CHANGE",
            description=f"Compiling intent {intent.intent_id} ({getattr(intent, 'intent_type', 'unknown')})",
            chain=strategy.chain,
        )

        # Capture pre-execution balance snapshot for real reconciliation (VIB-3158).
        # Non-fatal: on failure we fall back to the legacy post-only mode.
        state.pre_snapshot = await self._snapshot_balances_for_intent(intent)

        # VIB-3474: capture lending protocol state (collateral / debt / HF /
        # liquidation_threshold) BEFORE submission for SUPPLY/BORROW/REPAY/
        # WITHDRAW intents.  Without this, transaction_ledger.pre_state_json
        # is empty for every lending row and the AccountingProcessor's lending
        # handler emits ESTIMATED confidence with unavailable_reason —
        # blocking G6 (looping reconciliation) and L4 (principal vs interest)
        # in the Accountant Test.
        state.lending_pre_state = self._capture_lending_state_safe(
            intent=intent,
            chain=strategy.chain,
            wallet_address=strategy.wallet_address,
            gateway_client=state.gateway_client,
            price_oracle=state.price_oracle,
            phase="pre",
        )

    @staticmethod
    def _capture_lending_state_safe(
        *,
        intent: Any,
        chain: str,
        wallet_address: str,
        gateway_client: Any | None,
        price_oracle: dict | None,
        phase: str,
        block: int | str | None = None,
    ) -> Any | None:
        """Best-effort lending pre/post state capture (VIB-3474).

        Wraps ``capture_lending_pre_state`` / ``capture_lending_post_state``
        so a gateway hiccup or unsupported protocol never raises into the
        runner.  Returns the typed protocol state object on success (Aave
        / Morpho Blue / Compound V3) or ``None`` when:
          - ``gateway_client`` is missing (local-without-gateway, paper)
          - intent isn't a lending intent (SWAP, LP_*, PERP_*, …)
          - protocol isn't yet supported (JoeLend, …)
          - any underlying gateway eth_call fails

        Returning ``None`` is correct: the ledger writer treats it as
        "no lending state captured" and the column stays empty — the
        legacy unavailable_reason path. Never fabricates state.

        CodeRabbit + Claude pr-auditor (2026-05-02): the except clause is
        narrowed to network-class errors (``ConnectionError`` /
        ``TimeoutError`` / ``OSError``) so refactor regressions
        (``ImportError`` / ``AttributeError`` / ``TypeError``) propagate
        loudly. The underlying ``capture_lending_*`` helpers ALREADY
        swallow ``Exception`` internally and return ``None`` on
        gateway-side failures — so this outer except only fires on a
        programming error in the import / dispatch path, which is
        exactly the case we want to surface.
        """
        if gateway_client is None:
            return None
        intent_type = getattr(intent, "intent_type", None)
        intent_type_value = getattr(intent_type, "value", None)
        if intent_type_value is not None:
            intent_type_str = str(intent_type_value).upper()
        else:
            intent_type_str = str(intent_type or "").upper()
        if intent_type_str not in {"SUPPLY", "BORROW", "REPAY", "WITHDRAW", "DELEVERAGE"}:
            return None
        try:
            from almanak.framework.accounting.lending_accounting import (
                capture_lending_post_state,
                capture_lending_pre_state,
            )

            capture = capture_lending_pre_state if phase == "pre" else capture_lending_post_state
            return capture(
                intent=intent,
                chain=chain,
                wallet_address=wallet_address,
                gateway_client=gateway_client,
                price_oracle=price_oracle,
                block=block,
            )
        except (ConnectionError, TimeoutError, OSError):
            logger.debug("lending %s-state capture failed (transient/non-fatal)", phase, exc_info=True)
            return None

    @staticmethod
    def _refresh_price_oracle_for_ledger(market: Any | None, intent: AnyIntent) -> dict | None:
        """Best-effort price-oracle refresh at ledger-write time.

        ``state.price_oracle`` is captured at intent-init time. For an
        indicator strategy whose ``decide()`` only calls ``market.price()``
        for a subset of tokens, the runner pre-fetch may run BEFORE the
        gateway has finished warming the rest. The ledger write that
        comes after execution still sees the empty pre-execution oracle.

        This helper re-queries ``market.get_price_oracle_dict()`` at the
        write site — by then the gateway has answered every leg and any
        post-warming has landed in the local cache. It mirrors
        :meth:`_build_single_chain_price_oracle` so the post-execution
        ledger path tops off the same set of tokens as the pre-execution
        compile path: intent legs PLUS the chain's native gas token. Without
        the native top-off, gas_usd stays empty for any intent that doesn't
        already reference the gas token (e.g. a Polygon USDC→WETH swap that
        never names MATIC). Failure is silent (returns None); the writer
        falls back to the unpriced path rather than raising.
        """
        if market is None or not hasattr(market, "get_price_oracle_dict"):
            return None
        try:
            tokens = _extract_tokens_from_intent(intent)
            if hasattr(market, "price"):
                for token in tokens:
                    try:
                        market.price(token)
                    except Exception:
                        continue
            oracle = market.get_price_oracle_dict() or {}

            # Mirror the native-gas pre-fetch in ``_build_single_chain_price_oracle``
            # so the refresh helper covers the same case the build path
            # added in VIB-3804. Same guard: only attempt this when the
            # oracle already carries at least one priced token, so we don't
            # convert an empty oracle into a "native-only" oracle which
            # would flip the placeholder-price signal downstream.
            if oracle and hasattr(market, "price"):
                chain = getattr(market, "chain", None) or getattr(intent, "chain", None)
                if chain:
                    from almanak.framework.accounting.gas_pricing import native_token_for_chain

                    native_symbol = native_token_for_chain(chain)
                    if native_symbol and native_symbol not in oracle:
                        try:
                            market.price(native_symbol)
                        except Exception:
                            logger.debug(
                                "gas_pricing: native pre-fetch failed (refresh path) for chain=%s symbol=%s",
                                chain,
                                native_symbol,
                            )
                        oracle = market.get_price_oracle_dict() or oracle

            return oracle or None
        except Exception:  # noqa: BLE001 — never raise on a best-effort refresh
            return None

    def _merge_oracle_for_ledger(self, state: Any, intent: AnyIntent) -> dict | None:
        """Refresh the market oracle and merge with the cached one.

        Threading consistency: every ledger-write call site (success,
        slippage, reconciliation-failure, generic-failure) must pass an
        oracle that is at least as complete as the cached
        ``state.price_oracle``. The previous "use cache OR refresh" pattern
        left a reachable hole — init captured intent legs but not the
        native gas token, so a SWAP whose cache was non-empty but didn't
        carry the gas-token price wrote ``gas_usd=""`` on the ledger row
        even though the market cache was warm by the time of the write.

        The merge is additive: cached values win on key collision so we
        don't trample a HIGH-confidence price with a STALE refresh, but
        any key the cache lacked gets filled from the refresh.

        VIB-3889: when the market exposes ``get_price_oracle_dict(with_sources=True)``
        (the canonical AttemptNo17 §1.2 G12 nested shape), the merge pulls
        the nested dict so ``transaction_ledger.price_inputs_json`` carries
        the actual provider name (coingecko / chainlink / binance / thegraph).
        Pre-VIB-3889 the dashboard's "Oracle quotes used" expander rendered
        every source as "unknown" because the cached flat dict had no
        provenance, and the ledger writer's normaliser defaulted to "unknown".
        Markets without ``with_sources`` support fall back to the legacy
        flat path (cleanly).
        """
        cached = getattr(state, "price_oracle", None) or {}
        refreshed = self._refresh_price_oracle_for_ledger(getattr(state, "market", None), intent) or {}

        # VIB-3889: prefer the nested shape with sources when the market
        # supports it. The ledger writer at observability/ledger.py:529-545
        # propagates the nested shape verbatim; readers (handlers via
        # parse_price_inputs, dashboard) tolerate either shape.
        market = getattr(state, "market", None)
        nested_with_sources: dict | None = None
        if market is not None and hasattr(market, "get_price_oracle_dict"):
            try:
                nested_with_sources = market.get_price_oracle_dict(with_sources=True)
            except TypeError:
                # Older market snapshots without the with_sources kwarg.
                nested_with_sources = None
            except Exception:
                logger.debug("get_price_oracle_dict(with_sources=True) raised", exc_info=True)
                nested_with_sources = None

        if nested_with_sources:
            # Overlay the source-aware nested entries on top of the flat
            # cached/refreshed dict so the ledger writer's normaliser
            # passes the provenance through. Cached/refreshed entries
            # without a nested counterpart stay as Decimals (writer wraps
            # them with oracle_source="unknown").
            merged: dict = {**refreshed, **cached}
            for sym, payload in nested_with_sources.items():
                merged[sym] = payload
            return merged or None

        if not cached and not refreshed:
            return None
        # Legacy path: ``refreshed`` first, ``cached`` overrides — preserves
        # cached provenance / confidence on overlap, fills gaps from refresh.
        merged_flat: dict = {**refreshed, **cached}
        return merged_flat or None

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
        # This covers three cases:
        # 1. Oracle is empty (strategy didn't call market.price() in decide())
        # 2. Oracle has some tokens but FlashLoanIntent callbacks reference
        #    additional tokens (e.g., WETH) not fetched by decide().
        # 3. The chain's native gas token isn't a leg of any intent (e.g. polygon
        #    USDC->WETH never references MATIC). Without pre-fetching it,
        #    accounting.gas_pricing.compute_gas_usd returns None and the ledger
        #    writes gas_usd="" on every non-native-leg swap (VIB-3804).
        if hasattr(market, "price"):
            intent_tokens: set[str] = set(_extract_tokens_from_intent(intent))
            missing_tokens = [t for t in intent_tokens if not price_oracle or t not in price_oracle]
            if missing_tokens:
                for token in missing_tokens:
                    try:
                        market.price(token)
                    except Exception:
                        pass  # Token price unavailable, compiler will use placeholder
                price_oracle = market.get_price_oracle_dict()

            # Native gas-token pre-fetch (case 3 above): ONLY runs when the
            # oracle already carries at least one real intent-token price.
            # Skipping it on an empty oracle preserves the indicator-strategy
            # placeholder path — adding only MATIC there would flip the
            # ``allow_placeholder_prices`` signal in ``_init_single_chain_state``
            # and the compiler would raise on the unresolved swap leg
            # (Codex audit P2 on the original VIB-3804 patch).
            if price_oracle:
                chain = getattr(market, "chain", None) or getattr(intent, "chain", None)
                if chain:
                    # Local import — strategy_runner uses function-scoped
                    # imports for accounting modules to avoid early-binding
                    # cycles (see ``from ..accounting...`` throughout this
                    # file).
                    from almanak.framework.accounting.gas_pricing import native_token_for_chain

                    native_symbol = native_token_for_chain(chain)
                    if native_symbol not in price_oracle:
                        try:
                            market.price(native_symbol)
                        except Exception:
                            # Cross-references the WARN at observability/ledger.py
                            # gas_usd writer when the chain's native price is
                            # missing (post-fix VIB-3804). DEBUG, not WARN —
                            # one log per ledger row is enough.
                            logger.debug(
                                "gas_pricing: native pre-fetch failed for chain=%s symbol=%s; gas_usd may stay empty",
                                chain,
                                native_symbol,
                            )
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
        deployment_id = state.deployment_id
        state_machine = state.state_machine
        compiler = state.compiler

        # VIB-3203: Persist this step's metadata at the moment of
        # execution so enrichment below can access ``expected_output_human``
        # even if a later no-op step is terminal.
        state.last_bundle_metadata = getattr(step_result.action_bundle, "metadata", None)

        # Dry run mode - skip actual execution
        if self.config.dry_run:
            logger.info(
                f"Dry run mode - skipping execution for {deployment_id}. "
                f"Would execute {len(step_result.action_bundle.transactions)} transactions."
            )
            if state.clob_client is not None:
                state.clob_client.close()
            if state.record_metrics:
                self._record_success()
            return IterationResult(
                status=IterationStatus.DRY_RUN,
                intent=intent,
                deployment_id=deployment_id,
                duration_ms=self._calculate_duration_ms(state.start_time),
            )

        # Execute the action bundle through orchestrator
        # Resolve protocol for result enrichment (intent is frozen, so we pass via context)
        resolved_protocol = getattr(intent, "protocol", None) or compiler.default_protocol
        from almanak.framework.observability.context import get_cycle_id

        execution_context = ExecutionContext(
            deployment_id=deployment_id,
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

            # Route CLOB bundles to the connector-built CLOB handler (off-chain orders),
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

            # VIB-4043 / PR4: gas_used is money-shaped — moved to
            # transaction_ledger.gas_used / gas_usd. The phase breadcrumb
            # carries lifecycle markers only.
            # PR4 / PRD-TimelineEvents §6.1 (CodeRabbit review): the raw
            # `execution_result.error` carries money-shaped data on slippage
            # / reconciliation paths (bps, token deltas). Bucket it through
            # `_classify_failure_reason` so the EXECUTE breadcrumb stays a
            # lifecycle marker — full text lives in `transaction_ledger.error`.
            details: dict[str, Any] = {
                "success": execution_result.success,
                "tx_count": len(execution_result.transaction_results),
            }
            if not execution_result.success:
                details["failure_reason"] = self._classify_failure_reason(execution_result.error or "")
            emit_phase_event(
                deployment_id=deployment_id,
                phase=StrategyPhase.EXECUTE,
                event_type="TRANSACTION_CONFIRMED" if execution_result.success else "TRANSACTION_FAILED",
                description=f"Execution {'succeeded' if execution_result.success else 'failed'}",
                chain=strategy.chain,
                tx_hash=tx_hash,
                details=details,
            )

            if execution_result.success:
                logger.info(
                    f"Execution successful for {deployment_id}: "
                    f"gas_used={execution_result.total_gas_used}, "
                    f"tx_count={len(execution_result.transaction_results)}"
                )
            else:
                logger.warning(
                    f"Execution failed for {deployment_id}: {execution_result.error} "
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
        """Execute a Polymarket CLOB bundle via the connector-built CLOB handler."""
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
                from almanak.core.chains import ChainRegistry

                descriptor = ChainRegistry.try_resolve(strategy.chain)
                native_symbol = descriptor.native.symbol if descriptor is not None else "ETH"
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
        deployment_id = state.deployment_id
        state_machine = state.state_machine

        # Enrich result with intent-specific extracted data
        if state.last_execution_result and state.last_execution_context:
            try:
                # VIB-4477 (T08): thread connector-owned pool-key lookup
                # bridges into receipt parsing. The callback is bound to
                # this runner's ``GatewayClient``; ``None`` when no gateway
                # client is configured (paper / dry-run modes), in which case
                # parsers that need it emit structured warnings and the rest
                # of the pipeline degrades cleanly.
                pool_key_lookup = self._build_pool_key_lookup()
                enricher = ResultEnricher(
                    live_mode=self._is_live_mode(),
                    pool_key_lookup=pool_key_lookup,
                )
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
        # NOTE (VIB-4043 / PR4): the timeline event is now emitted AFTER the
        # ledger write so it can carry `related_ledger_entry_id`. The ledger
        # row is the financial truth; the timeline event is a UX breadcrumb
        # that points at it.
        self._maybe_warn_deleverage(intent, strategy)
        # Write structured trade record to transaction ledger (VIB-2402).
        # VIB-3658 sequel (April 30 audit #3): pass state.price_oracle so the
        # ledger writer can convert wei-gas-cost to USD via the chain's
        # native-token price.  Without this, transaction_ledger.gas_usd is
        # always empty for swap and LP intents.
        # Accounting-AttemptNo17 §A4 (VIB-3480): pass pre/post wallet
        # balance observations so transaction_ledger.pre_state_json /
        # post_state_json land populated. The reconciliation step above
        # already computed both — we just thread them through.
        # VIB-3474: capture lending protocol state (collateral / debt / HF)
        # AFTER the TX confirms, then merge into both pre_state and post_state
        # before they are serialized to the ledger row.  ``state.lending_pre_state``
        # was captured by ``_init_single_chain_state`` *before* submission.
        intent_protocol = (getattr(intent, "protocol", "") or "").lower()
        # VIB-4589 / F7 — pin the post-state read to the confirmed receipt's
        # block. Reading at ``"latest"`` (the pre-fix default) raced the
        # upstream RPC's receipt indexer on mainnet and produced stale
        # collateral balances. Use the LAST successful receipt's
        # block_number — for multi-tx bundles that is the state after the
        # whole bundle landed.
        post_block = _last_receipt_block(state.last_execution_result)
        lending_post_state = self._capture_lending_state_safe(
            intent=intent,
            chain=strategy.chain,
            wallet_address=strategy.wallet_address,
            gateway_client=state.gateway_client,
            price_oracle=state.price_oracle,
            phase="post",
            block=post_block,
        )
        pre_state = _build_pre_state_for_ledger(
            state.pre_snapshot,
            state.lending_pre_state,
            protocol=intent_protocol,
        )
        post_state = _build_post_state_for_ledger(
            recon,
            lending_post_state,
            protocol=intent_protocol,
        )
        # Mainnet 2026-05-01 finding: state.price_oracle was empty for the
        # very first SWAP iteration of an indicator strategy because market
        # warming hadn't completed by execution time. Refresh-and-merge the
        # market's current oracle dict at write time on EVERY ledger write
        # — the partial-oracle case (init captured some legs but missed the
        # native gas token) is reachable even when ``state.price_oracle`` is
        # truthy, and a non-empty cached dict is not evidence that the
        # ledger row will pay for gas. Cached values win on key collision so
        # this is purely additive (refreshed values fill gaps).
        ledger_price_oracle = self._merge_oracle_for_ledger(state, intent)
        ledger_entry_id = await self._write_ledger_entry(
            strategy,
            intent,
            result=state.last_execution_result,
            success=True,
            price_oracle=ledger_price_oracle,
            pre_state=pre_state,
            post_state=post_state,
        )
        # VIB-4043 / PR4: emit the UX timeline breadcrumb now, threading the
        # ledger_entry_id so the renderer can navigate from the card back to
        # the financial-truth row.
        self._emit_execution_timeline_event(
            strategy,
            intent,
            success=True,
            result=state.last_execution_result,
            related_ledger_entry_id=ledger_entry_id or "",
        )
        # VIB-3467/3478: AccountingProcessor is the sole accounting write path (dual-write
        # period ended with removal of _try_write_* methods in VIB-3478).
        if ledger_entry_id:
            # VIB-3946: thread the compiler-resolved canonical pool label
            # (metadata["pool_name"], populated by Curve) so the LP outbox
            # position_key keys off "3pool" rather than a raw asset-set string.
            resolved_pool = (state.last_bundle_metadata or {}).get("pool_name")
            await self._write_outbox_and_fire_processor(strategy, intent, ledger_entry_id, resolved_pool=resolved_pool)
        # VIB-3454: append one JSON line to the per-strategy sidecar file so the
        # portfolio dashboard can consume execution data without touching gateway.db.
        # Best-effort: the writer swallows all exceptions internally.
        try:
            from ..accounting.sidecar import AccountingSidecarWriter

            AccountingSidecarWriter().append(
                deployment_id=strategy.deployment_id,
                intent=intent,
                result=state.last_execution_result,
                chain=getattr(strategy, "chain", "") or getattr(self.config, "chain", ""),
                # Use the SAME refreshed oracle as the ledger row above —
                # otherwise the sidecar (consumed by the local dashboard)
                # falls back to the empty-on-first-iteration price oracle
                # while the ledger row itself is correct, leading to
                # inconsistent dashboards/CSVs vs. the canonical SQLite row.
                price_oracle=ledger_price_oracle,
            )
        except Exception:  # noqa: BLE001
            logger.warning("Sidecar import/call failed (non-blocking)", exc_info=True)
        if state.record_metrics:
            self._record_success(execution_proved=True)

        # Notify strategy of successful execution (framework hooks first,
        # then user callback — see _notify_intent_executed for VIB-3742
        # LP position tracker integration).
        self._notify_intent_executed(strategy, intent, True, state.last_execution_result)
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
            deployment_id=deployment_id,
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

        # Notify strategy of failure due to slippage breach so strategy
        # authors can access the error on the result. Pass
        # ``framework_success=True`` because the on-chain TX itself
        # succeeded — the framework tracker (VIB-3742 LP position tracker)
        # must reflect chain reality, not the user-facing verdict, so a
        # future LP_CLOSE can find the bin_ids / position_id that the
        # opening TX actually committed on-chain.
        self._notify_intent_executed(strategy, intent, False, last_execution_result, framework_success=True)
        self._invoke_optional_hook(
            strategy,
            "on_copy_execution_result",
            intent,
            False,
            last_execution_result,
        )

        # Record slippage-breach trade in ledger (VIB-2402).
        # VIB-3658 sequel (April 30 audit #3): pass state.price_oracle so the
        # circuit-breaker row also gets gas_usd populated.  A breach is still
        # an executed transaction on-chain — the gas drag is real and must
        # show up in PnL totals.
        # Accounting-AttemptNo17 §A4: pass pre_state too. Post-state is
        # NOT captured on this path (we entered slippage-breach before the
        # reconciliation step), so post_state_json stays empty.
        slippage_ledger_id = await self._write_ledger_entry(
            strategy,
            intent,
            result=last_execution_result,
            success=False,
            error=slippage_error,
            # VIB-3804 hardening: refresh+merge so the slippage-breach row
            # gets the full oracle (including the chain's native gas token)
            # even when ``state.price_oracle`` was captured before market
            # warming. Without this, a slippage breach still landed gas on-
            # chain but the ledger row wrote ``gas_usd=""``.
            price_oracle=self._merge_oracle_for_ledger(state, intent),
            pre_state=_build_pre_state_for_ledger(
                state.pre_snapshot,
                state.lending_pre_state,
                protocol=(getattr(intent, "protocol", "") or "").lower(),
            ),
        )
        # VIB-4043 / PR4: emit the timeline failure breadcrumb pointing at
        # the just-written ledger row.
        self._emit_execution_timeline_event(
            strategy,
            intent,
            success=False,
            result=last_execution_result,
            related_ledger_entry_id=slippage_ledger_id or "",
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
            deployment_id=state.deployment_id,
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
            state.deployment_id,
            recon_error,
        )

        # Attach error to the execution result FIRST so the timeline
        # event and downstream consumers (alerts, operator cards,
        # ledger) see the reconciliation error rather than the stale
        # execution-level error.
        if last_execution_result is not None:
            last_execution_result.error = recon_error

        # Notify strategy of the failed outcome so it does not treat
        # the execution as clean. Pass ``framework_success=True`` because
        # the on-chain TX succeeded — the recon breach is an accounting
        # outcome layered on top, and the framework tracker must reflect
        # chain reality (the on-chain position state DID move) so a future
        # LP_CLOSE can find the bin_ids / position_id that the TX
        # committed.
        self._notify_intent_executed(strategy, intent, False, last_execution_result, framework_success=True)
        self._invoke_optional_hook(
            strategy,
            "on_copy_execution_result",
            intent,
            False,
            last_execution_result,
        )

        # Record failed trade in ledger (VIB-2402) -- on-chain state
        # changed, but the accounting outcome is a failure.
        # VIB-3658 sequel (April 30 audit #3): same as the success path —
        # an enforcement breach is still a real on-chain TX, so its gas
        # drag must surface in transaction_ledger.gas_usd.
        # Accounting-AttemptNo17 §A4: pass pre/post state too. The
        # reconciliation report already has post_balances by definition
        # (we got here BECAUSE recon produced an incident).
        recon_ledger_id = await self._write_ledger_entry(
            strategy,
            intent,
            result=last_execution_result,
            success=False,
            error=recon_error,
            # VIB-3804 hardening (mirrors the slippage / success branches):
            # refresh+merge so the reconciliation-failure row also carries
            # the full oracle and a populated ``gas_usd``.
            price_oracle=self._merge_oracle_for_ledger(state, intent),
            pre_state=_build_pre_state_for_ledger(
                state.pre_snapshot,
                state.lending_pre_state,
                protocol=(getattr(intent, "protocol", "") or "").lower(),
            ),
            post_state=_build_post_state_for_ledger(
                recon,
                # Reconciliation-incident path: the on-chain TX still landed
                # so post-state is meaningful. Re-read here to keep the
                # ledger row's lending fields aligned with reality.
                # VIB-4589 / F7 — pin the read to the receipt's block to
                # avoid racing the upstream RPC's receipt indexer; same
                # rationale as the clean-success path above.
                self._capture_lending_state_safe(
                    intent=intent,
                    chain=strategy.chain,
                    wallet_address=strategy.wallet_address,
                    gateway_client=state.gateway_client,
                    price_oracle=state.price_oracle,
                    phase="post",
                    block=_last_receipt_block(last_execution_result),
                ),
                protocol=(getattr(intent, "protocol", "") or "").lower(),
            ),
        )
        # VIB-4043 / PR4: emit timeline failure breadcrumb pointing at the
        # reconciliation-failure ledger row.
        self._emit_execution_timeline_event(
            strategy,
            intent,
            success=False,
            result=last_execution_result,
            related_ledger_entry_id=recon_ledger_id or "",
        )

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
            deployment_id=state.deployment_id,
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
        deployment_id = state.deployment_id
        state_machine = state.state_machine
        last_execution_result = state.last_execution_result

        # State machine reached FAILED state - escalate to operator
        error_msg = state_machine.error or "Unknown error after retries exhausted"
        logger.error(f"Intent failed after {state_machine.retry_count} retries: {error_msg}")

        # Write failed trade to transaction ledger (VIB-2402).
        # VIB-3658 sequel (April 30 audit #3): pre-execution failures have no
        # gas to convert (last_execution_result is None or carries no
        # total_gas_cost_wei).  Where a retry exhausted gas was burned, the
        # state.price_oracle is the right source so gas_usd lands populated.
        # Accounting-AttemptNo17 §A4: pass pre_state. No post-state since
        # this path means execution itself failed (or was never attempted).
        # CodeRabbit review: backfill ``last_execution_result.error`` BEFORE
        # building ``timeline_result`` and writing the ledger so both surfaces
        # see the terminal state-machine reason (was previously backfilled
        # only after the timeline emit, leaving the activity feed bucketed as
        # "unknown error" while the ledger had the correct text on the same
        # iteration).
        if last_execution_result is not None and not getattr(last_execution_result, "error", ""):
            last_execution_result.error = error_msg
        timeline_result = last_execution_result or SimpleNamespace(error=error_msg)
        failed_ledger_id = await self._write_ledger_entry(
            strategy,
            intent,
            result=last_execution_result,
            success=False,
            error=error_msg,
            # VIB-3804 hardening: even on the post-retry FAILED path, gas
            # may have been burned by the attempt(s). Refresh+merge so the
            # ledger row carries the full oracle and ``gas_usd`` is non-
            # empty when there's gas to convert.
            price_oracle=self._merge_oracle_for_ledger(state, intent),
            pre_state=_build_pre_state_for_ledger(
                state.pre_snapshot,
                state.lending_pre_state,
                protocol=(getattr(intent, "protocol", "") or "").lower(),
            ),
        )
        # VIB-4043 / PR4: emit timeline failure breadcrumb pointing at
        # the just-written ledger row (or empty when no row was written).
        self._emit_execution_timeline_event(
            strategy,
            intent,
            success=False,
            result=timeline_result,
            related_ledger_entry_id=failed_ledger_id or "",
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

        # Notify strategy of failed execution.
        # ``last_execution_result.error`` is already backfilled above (before
        # the ledger/timeline writes), so this `or SimpleNamespace(...)` only
        # catches the pre-execution path where last_execution_result is None.
        callback_result = last_execution_result or SimpleNamespace(error=error_msg)
        self._notify_intent_executed(strategy, intent, False, callback_result)
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
            deployment_id=deployment_id,
            duration_ms=self._calculate_duration_ms(state.start_time),
        )

    # crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
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

        deployment_id = strategy.deployment_id

        # Load any saved execution progress
        saved_progress = await self._load_execution_progress(deployment_id)

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
                f"Stuck execution found for {deployment_id} but no serialized intents. "
                f"Clearing progress and starting fresh."
            )
            await self._clear_execution_progress(deployment_id)
            return None

        # Deserialize the saved intents
        try:
            intents: list[AnyIntent] = [
                Intent.deserialize(intent_data) for intent_data in saved_progress.serialized_intents
            ]
        except Exception as e:
            logger.error(
                f"Failed to deserialize saved intents for {deployment_id}: {e}. Clearing progress and starting fresh."
            )
            await self._clear_execution_progress(deployment_id)
            return None

        failed_step = saved_progress.failed_at_step_index or 0
        total_steps = saved_progress.total_steps

        logger.info(
            f"Resuming stuck execution for {deployment_id}: "
            f"retrying step {failed_step + 1}/{total_steps} "
            f"(execution_id={saved_progress.execution_id}, "
            f"error was: {saved_progress.failure_error})"
        )

        # Clear the failure state so we can retry
        saved_progress.failed_at_step_index = None
        saved_progress.failure_error = None
        saved_progress.last_updated = datetime.now(UTC)
        await self._save_execution_progress(deployment_id, saved_progress)

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
        deployment_id = strategy.deployment_id

        # Check if strategy has teardown support (graceful degradation)
        if not hasattr(strategy, "should_teardown"):
            return None

        # Check if teardown is requested
        try:
            should_teardown = strategy.should_teardown()
        except Exception as e:
            from ..deployment import is_hosted

            if is_hosted():
                logger.error(f"Error checking hosted teardown status for {deployment_id}: {e}")
                raise
            logger.warning(f"Error checking teardown status for {deployment_id}: {e}")
            return None

        if not should_teardown:
            return None

        # Acknowledge teardown request
        if hasattr(strategy, "acknowledge_teardown_request"):
            try:
                strategy.acknowledge_teardown_request()
                logger.info(f"Acknowledged teardown request for {deployment_id}")
            except Exception as e:  # noqa: BLE001
                from ..deployment import is_hosted

                if is_hosted():
                    logger.error(f"Failed to acknowledge hosted teardown request: {e}")
                    raise
                logger.warning(f"Failed to acknowledge teardown request: {e}")

        # Import TeardownMode here to avoid circular imports.
        from ..local_paths import LocalPathError
        from ..teardown import TeardownMode, get_teardown_state_manager_for_runtime

        # Read the operator-selected mode from the same runtime channel used
        # by ``should_teardown``: SQLite in local mode, gateway in hosted mode.
        # In local mode, a ``LocalPathError`` is a path-helper misconfiguration;
        # re-raise so the operator sees it rather than silently downgrading.
        try:
            manager = get_teardown_state_manager_for_runtime(gateway_client=self._get_gateway_client())
            request = manager.get_active_request(deployment_id)
        except LocalPathError:
            raise
        mode = request.mode if request else TeardownMode.SOFT

        logger.info(f"Teardown requested for {deployment_id} (mode={mode.value})")
        return mode

    # crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
    async def _execute_multi_chain(  # noqa: C901
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
        deployment_id = strategy.deployment_id

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
            f"Multi-chain execution for {deployment_id}: "
            f"{len(intents)} intents across {chains_involved}, "
            f"has_cross_chain={has_cross_chain}"
        )

        # Dry run mode
        if self.config.dry_run:
            logger.info(f"Dry run mode - skipping execution for {deployment_id}. Would execute {len(intents)} intents.")
            self._record_success()
            return IterationResult(
                status=IterationStatus.DRY_RUN,
                intent=intents[0] if intents else None,
                deployment_id=deployment_id,
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
                f"Multi-chain execution successful for {deployment_id}: "
                f"{multi_result.successful_count}/{len(intents)} succeeded, "
                f"chains={list(multi_result.chains_used)}, "
                f"time={multi_result.total_execution_time_ms:.0f}ms"
            )

            self._record_success(execution_proved=True)
            return IterationResult(
                status=IterationStatus.SUCCESS,
                intent=first_intent,
                deployment_id=deployment_id,
                duration_ms=self._calculate_duration_ms(start_time),
            )
        else:
            # Aggregate errors from all chains
            error_msgs = []
            for chain, errors in multi_result.errors_by_chain.items():
                error_msgs.extend([f"[{chain}] {e}" for e in errors])
            error_summary = "; ".join(error_msgs) if error_msgs else "Unknown error"

            logger.error(
                f"Multi-chain execution failed for {deployment_id}: "
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
                deployment_id=deployment_id,
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
            deployment_id=strategy.deployment_id,
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
        deployment_id = state.deployment_id

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
            saved_progress = await self._load_execution_progress(deployment_id)

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
                    await self._clear_execution_progress(deployment_id)

                # Serialize intents for stuck execution recovery
                serialized_intents = [intent.serialize() for intent in intents]

                state.progress = ExecutionProgress(
                    execution_id=str(uuid.uuid4())[:8],
                    deployment_id=deployment_id,
                    intents_hash=intents_hash,
                    total_steps=len(intents),
                    serialized_intents=serialized_intents,
                )
                # Save initial progress with serialized intents
                await self._save_execution_progress(deployment_id, state.progress)

        logger.info(
            f"Executing {len(intents)} intents with bridge waiting for {deployment_id} "
            f"(starting from step {state.start_step_index + 1})"
        )

        # Start the successful-count at whatever was already completed so the
        # final summary line reports the full count, not just newly-executed
        # steps.
        state.successful_count = state.start_step_index

    async def _bridge_wait_process_intent(self, state: BridgeWaitState, i: int) -> bool:  # noqa: C901
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
        deployment_id = state.deployment_id

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
            # ``docs/internal/blueprints/20-gateway-security-architecture.md``.
            if state.gateway_client is None:
                raise RuntimeError(
                    "Gateway client required for cross-chain bridge source-TX verification; "
                    "direct Web3 fallback is forbidden by gateway-only architecture. "
                    "See docs/internal/blueprints/20-gateway-security-architecture.md"
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
            self._notify_intent_executed(strategy, intent, False, None)
            state.callback_fired = True
            state.failed_step = f"step-{step_num}"
            state.error_message = str(e)
            return True

        if not result.success:
            logger.error(f"Step {step_num} failed: {result.error}")
            # Notify strategy of failed execution (mirrors _execute_single_chain)
            self._notify_intent_executed(strategy, intent, False, result)
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
            # Fallback to intent amount.
            #
            # Audit F3 (VIB-4062 caller-bifurcation): ``get_amount_field``
            # returns ``ChainedAmount | None`` where
            # ``ChainedAmount = Decimal | Literal["all"]``. The sentinel
            # ``"all"`` means "chain the previous step's output amount" and
            # is resolved by ``set_resolved_amount`` BEFORE this code runs;
            # by the time the orchestrator hits this fallback, an
            # un-resolved ``"all"`` indicates the chaining contract is
            # broken upstream. We discriminate against the sentinel by
            # value (``!= "all"``) rather than by class — the consumer
            # treats the producer's union type by value, not by class —
            # to keep the no-bifurcation invariant intact. The ``cast``
            # narrows the typed remainder to ``Decimal`` once the
            # literal "all" branch is excluded.
            amount_field = Intent.get_amount_field(intent_to_execute)
            if amount_field is not None and amount_field != "all":
                state.previous_amount_received = cast(Decimal, amount_field)

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
        self._notify_intent_executed(strategy, intent, True, result)

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
        await self._save_execution_progress(deployment_id, state.progress)
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
                ``docs/internal/blueprints/20-gateway-security-architecture.md``). This
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
                "See docs/internal/blueprints/20-gateway-security-architecture.md"
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
                    exc_code = get_grpc_status_code(exc)
                    if exc_code is not None and exc_code not in TRANSIENT_GRPC_CODES:
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
            self._notify_intent_executed(strategy, intent, False, result)
            state.callback_fired = True
            state.failed_step = f"step-{step_num}-bridge"
            state.error_message = f"Bridge transfer failed: {bridge_status.get('error', 'Unknown')}"
            return True

        except TimeoutError as e:
            logger.error(f"Bridge timeout: {e}")
            # Notify strategy of bridge timeout (source tx succeeded but bridge timed out)
            self._notify_intent_executed(strategy, intent, False, result)
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
            self._notify_intent_executed(strategy, intent, False, result)
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
            self._notify_intent_executed(strategy, intent, False, result)
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
        deployment_id = state.deployment_id
        intents = state.intents

        # Ensure strategy is notified of failure even for paths that didn't fire the callback
        # inline (e.g. source TX verification failures, no-tx_hash, no-RPC-URL).
        # This single finalization block covers all break exits without per-exit patching.
        if state.failed_step and not state.callback_fired:
            self._notify_intent_executed(strategy, state.current_intent, False, state.failed_result)

        # Build result
        if state.failed_step:
            return await self._bridge_wait_build_failed_result(state)

        # Always invalidate balance cache after execution (success or failure)
        # to prevent stale reads on the next decide() cycle.
        self.balance_provider.invalidate_cache()

        logger.info(
            f"Multi-chain execution with bridge waiting successful for {deployment_id}: "
            f"{state.successful_count}/{len(intents)} succeeded"
        )

        # Clear execution progress on successful completion
        await self._clear_execution_progress(deployment_id)

        self._record_success(execution_proved=True)
        return IterationResult(
            status=IterationStatus.SUCCESS,
            intent=state.first_intent,
            deployment_id=deployment_id,
            duration_ms=self._calculate_duration_ms(state.start_time),
        )

    async def _bridge_wait_build_failed_result(self, state: BridgeWaitState) -> IterationResult:
        """Persist failure progress, run diagnostics, return failed result."""
        strategy = state.strategy
        deployment_id = state.deployment_id
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
        await self._save_execution_progress(deployment_id, state.progress)
        logger.info(f"Saved failure state for retry: step {failed_intent_index + 1}, error: {error_message}")

        # Run diagnostics on the failed intent to help identify the cause
        try:
            if 0 <= failed_intent_index < len(intents):
                failed_intent = intents[failed_intent_index]
                failed_chain = getattr(failed_intent, "chain", strategy.chain)

                # Create a chain-specific balance provider for diagnostics.
                # VIB-3896: skip the EVM revert-diagnostic path for non-EVM
                # chains — Web3BalanceProvider only speaks EVM JSON-RPC, and
                # constructing it for chain='solana' would raise NonEvmChainError
                # before we even reach diagnose_revert. Solana strategies surface
                # their failure modes through their own connector adapters.
                from almanak.core.enums import CHAIN_FAMILY_MAP, Chain, ChainFamily
                from almanak.gateway.data.balance import Web3BalanceProvider

                try:
                    failed_chain_family = (
                        CHAIN_FAMILY_MAP.get(Chain(str(failed_chain).strip().upper())) if failed_chain else None
                    )
                except (ValueError, AttributeError):
                    failed_chain_family = None
                is_evm_chain = failed_chain_family is None or failed_chain_family is ChainFamily.EVM

                chain_rpc = state.rpc_urls.get(failed_chain)
                if chain_rpc and is_evm_chain:
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
                elif chain_rpc and not is_evm_chain:
                    logger.error(
                        f"EXECUTION FAILURE on non-EVM chain: {error_message}\n"
                        f"  Intent: {failed_intent.intent_type.value} | Chain: {failed_chain}\n"
                        f"  EVM revert-diagnostic skipped (Web3BalanceProvider is EVM-only). "
                        f"See connector adapter logs for chain-family-specific diagnostics."
                    )
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
            deployment_id=deployment_id,
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

    @staticmethod
    def _begin_market_snapshot_iteration(strategy, cycle_id) -> None:
        """Open the strategy's per-iteration MarketSnapshot scope (VIB-4843 FR-5001).

        Stamps the iteration token so pre-warm, ``decide()``, and the
        post-decide portfolio valuation reuse ONE snapshot instance. Never
        raises — snapshot memo bookkeeping must not break an iteration; a
        failure just degrades to the legacy per-call snapshot behaviour.
        """
        begin = getattr(strategy, "begin_market_snapshot_iteration", None)
        if begin is None:
            return
        try:
            begin(cycle_id)
        except Exception:  # noqa: BLE001 — never let memo bookkeeping break an iteration
            logger.debug("begin_market_snapshot_iteration failed; falling back to per-call snapshots")

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

        # VIB-4843 FR-5004: the native gas token is priced every iteration by
        # the portfolio valuer (VIB-4225 G6 reconciliation requires it, and
        # live mode HALTS if it's missing — so we cannot defer it on HOLD).
        # It is rarely in _get_tracked_tokens, so without this it is the one
        # price fetched INSIDE the decide()/valuation path. Pre-warm it here
        # so the single required fetch lands OUTSIDE the decide timeout and the
        # valuation lane hits the shared snapshot's warm _price_cache instead.
        native = self._native_gas_token_for_prewarm(strategy)
        if native and native.upper() not in {t.upper() for t in tokens}:
            tokens = [*tokens, native]

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
    def _native_gas_token_for_prewarm(strategy) -> str | None:
        """Resolve the chain's native gas-token symbol for pre-warming (FR-5004).

        Returns ``None`` for multi-chain strategies (the per-chain native is
        ambiguous at the single pre-warm seam) or when the chain cannot be
        resolved — pre-warm stays best-effort and never raises.
        """
        if getattr(strategy, "is_multi_chain", None) is not None:
            try:
                if strategy.is_multi_chain():
                    return None
            except Exception:  # noqa: BLE001
                return None
        chain = getattr(strategy, "chain", None) or getattr(strategy, "_chain", None)
        if not chain:
            return None
        try:
            from almanak.framework.accounting.gas_pricing import native_token_for_chain

            return native_token_for_chain(chain)
        except Exception:  # noqa: BLE001
            return None

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
        deployment_id: str,
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
            deployment_id=deployment_id,
            duration_ms=self._calculate_duration_ms(start_time),
        )

    async def _reconcile_post_execution_balances(self, strategy, intent, execution_result, pre_snapshot=None):
        from .runner_state import reconcile_post_execution_balances

        return await reconcile_post_execution_balances(
            self, strategy, intent, execution_result, pre_snapshot=pre_snapshot
        )

    @staticmethod
    def _classify_failure_reason(error_str: Any) -> str:
        """Return a money-safe bucket label for a failure error string.

        PRD-TimelineEvents §6.1: timeline event payloads (and descriptions)
        cannot carry token amounts, bps, deltas, or any money-shaped data.
        Slippage and reconciliation error strings DO contain that data, so
        the timeline must surface the *category* of failure only and let
        renderers drill into ``transaction_ledger.error`` via
        ``related_ledger_entry_id`` for the full message.

        The buckets are stable, lower-case, ungrammatical-on-purpose so
        renderers can append them after the intent_type token without
        worrying about article agreement.

        CodeRabbit: ``error_str`` arrives via ``result.error`` from many call
        sites (orchestrator results, state-machine reasons, raw exceptions
        bubbled up). A future refactor could leak a non-string (Exception
        instance, ``SimpleNamespace``, bytes) — a raised ``AttributeError`` here
        would skip the timeline emission entirely. Coerce defensively so this
        classifier never raises; the worst case is a generic bucket.
        """
        if error_str is None:
            return "unknown error"
        if isinstance(error_str, bytes):
            error_text = error_str.decode("utf-8", errors="replace")
        elif isinstance(error_str, str):
            error_text = error_str
        else:
            error_text = str(error_str)
        if not error_text:
            return "unknown error"
        lowered = error_text.lower()
        if "slippage" in lowered:
            return "slippage breach"
        if "reconciliation" in lowered or "recon " in lowered or lowered.startswith("recon"):
            return "reconciliation incident"
        if "circuit breaker" in lowered:
            return "circuit breaker open"
        if "revert" in lowered or "reverted" in lowered:
            return "execution reverted"
        if "timeout" in lowered:
            return "execution timed out"
        if "nonce" in lowered:
            return "nonce error"
        return "execution failed"

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

    async def _is_strategy_paused(self, deployment_id):
        from .runner_state import is_strategy_paused

        return await is_strategy_paused(self, deployment_id)

    async def _update_state(self, deployment_id, result, strategy=None):
        from .runner_state import update_state

        await update_state(self, deployment_id, result, strategy)

    async def _persist_copy_trading_state(self, deployment_id, activity_provider):
        from .runner_state import persist_copy_trading_state

        await persist_copy_trading_state(self, deployment_id, activity_provider)

    async def _persist_vault_state(self, deployment_id, vault_state_dict, vault_state_key):
        from .runner_state import persist_vault_state

        await persist_vault_state(self, deployment_id, vault_state_dict, vault_state_key)

    async def _capture_portfolio_snapshot(self, strategy, iteration_number):
        from .runner_state import capture_portfolio_snapshot

        # Pass trade flag to force snapshot on trade iterations (bypass throttle).
        # Only clear the flag after successful persistence so a transient
        # snapshot failure doesn't lose the forced-snapshot opportunity.
        force = self._iteration_had_trade
        result = await capture_portfolio_snapshot(self, strategy, iteration_number, force_snapshot=force)
        if result is not None:
            self._iteration_had_trade = False
            # VIB-3803: cache last-known exposure for the breaker's data-class
            # threshold logic. ``total_value_usd`` is strategy-scoped to
            # positive positions only (VIB-3614), so > 0 == "has open exposure".
            # Wrapped in try/except because this path must never break a
            # successful snapshot — the breaker has a safe default for
            # missing exposure data.
            if self._circuit_breaker is not None:
                try:
                    has_exposure = (result.total_value_usd or Decimal("0")) > Decimal("0")
                    self._circuit_breaker.record_exposure(has_exposure)
                except Exception:  # noqa: BLE001
                    logger.debug("Failed to record exposure on circuit breaker", exc_info=True)
        return result

    async def _update_portfolio_metrics(self, deployment_id, snapshot):
        from .runner_state import update_portfolio_metrics

        await update_portfolio_metrics(self, deployment_id, snapshot)

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
                    deployment_id=strategy.deployment_id,
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
                    deployment_id=strategy.deployment_id,
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
                deployment_id=strategy.deployment_id,
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
                deployment_id=strategy.deployment_id,
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
                    deployment_id=strategy.deployment_id,
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
                        deployment_id=strategy.deployment_id,
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
                            strategy.deployment_id,
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
                    deployment_id=strategy.deployment_id,
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
                strategy.deployment_id,
                reason,
            )
            await self._emergency_manager.emergency_stop_async(
                deployment_id=strategy.deployment_id,
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
                # A trip driven purely by market-data failures (no execution
                # faults) must NOT kill the process. The correct response to a
                # data outage / quiet-pool staleness is to idle-HOLD and let the
                # breaker cool down -> HALF_OPEN -> recover when data returns.
                # Exiting would turn a transient or scheduled data gap into a
                # permanently dead agent (and abandon any position the breaker is
                # meant to protect). Exit only when action-class failures
                # contributed to the trip.
                if self._circuit_breaker.tripped_on_data_class_only:
                    logger.warning(
                        "Circuit breaker tripped on market-data failures only for %s — "
                        "keeping process alive to auto-recover after cooldown (no process exit)",
                        strategy.deployment_id,
                    )
                else:
                    self._terminal_lifecycle_state = "ERROR"
                    self._terminal_lifecycle_error_message = (
                        f"Circuit breaker tripped: {last_result.error or 'unknown'}"
                    )
                    self._lifecycle_write_state(
                        strategy.deployment_id,
                        "ERROR",
                        error_message=self._terminal_lifecycle_error_message,
                    )
                    logger.critical("Circuit breaker tripped in managed deployment — exiting process")
                    self.request_shutdown()
        except Exception as e:
            logger.error(f"Failed to trigger emergency stop for {strategy.deployment_id}: {e}")

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

    def is_duplicate_transaction(self, tx_hash=None, nonce=None, deployment_id=None):
        from .runner_recovery import is_duplicate_transaction

        return is_duplicate_transaction(self, tx_hash, nonce, deployment_id)

    # =========================================================================
    # Execution Progress Management (for resuming after restart)
    # =========================================================================

    def _compute_intents_hash(self, intents):
        from .runner_recovery import compute_intents_hash

        return compute_intents_hash(self, intents)

    async def _load_execution_progress(self, deployment_id):
        from .runner_recovery import load_execution_progress

        return await load_execution_progress(self, deployment_id)

    async def _save_execution_progress(self, deployment_id, progress):
        from .runner_recovery import save_execution_progress

        await save_execution_progress(self, deployment_id, progress)

    async def _clear_execution_progress(self, deployment_id):
        from .runner_recovery import clear_execution_progress

        await clear_execution_progress(self, deployment_id)


__all__ = [
    "StrategyRunner",
    "RunnerConfig",
    "IterationResult",
    "IterationStatus",
    "StrategyProtocol",
    "ExecutionProgress",
]
