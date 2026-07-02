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
from collections import deque
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
from almanak.framework.teardown.completeness import check_intent_coverage
from almanak.framework.teardown.config import TeardownConfig
from almanak.framework.teardown.decision_log import TeardownDecisionPhase, log_teardown_decision
from almanak.framework.teardown.error_taxonomy import Disposition, classify_teardown_failure
from almanak.framework.teardown.models import (
    ApprovalRequest,
    ApprovalResponse,
    ClosureVerification,
    PositionInfo,
    TeardownMode,
    TeardownPositionSummary,
    TeardownPreview,
    TeardownResult,
    TeardownState,
    TeardownStatus,
    VerificationStatus,
    calculate_max_acceptable_loss,
    encode_consolidation_consent,
)
from almanak.framework.teardown.oracle_warmup import warm_and_validate_oracle
from almanak.framework.teardown.plan_a_reconciliation import reconcile_known_positions_against_chain
from almanak.framework.teardown.revert_hints import annotate_teardown_error
from almanak.framework.teardown.revert_transience import Transience, classify_revert_transience
from almanak.framework.teardown.safety_guard import SafetyGuard
from almanak.framework.teardown.slippage_manager import (
    EscalatingSlippageManager,
    ExecutionAttempt,
)
from almanak.framework.teardown.swap_clamp import SwapClampDecision, decide_swap_clamp

logger = logging.getLogger(__name__)

# VIB-5573 (WI-2): bounded time-axis retry for a vetted TRANSIENT revert (e.g.
# MetaMorpho withdraw-queue Panic 0x11 that clears within blocks). The retry is
# DEFERRED — the intent is re-queued to the END of the work queue and re-fired
# after a backoff, so it never delays a not-yet-tried risk-reducing close (the
# execution lane is sequential; blueprint 14a §Stage 6 "never block a
# risk-reducing intent"). Bounded so a revert that never clears still ends the
# teardown LOUD (FAILED) rather than looping forever. Backoff is per re-attempt
# (``_TRANSIENT_BACKOFF_S * attempts``) → ~4s, 8s, 12s ⇒ ≤~24s worst case for a
# single stuck vault, comfortably inside typical teardown SLAs (Q3: confirm
# against the hosted teardown deadline if it tightens).
_TRANSIENT_MAX_ATTEMPTS = 3
_TRANSIENT_BACKOFF_S = 4.0


def _intent_field(intent: Any, name: str) -> str | None:
    """Read a string field (``intent_type`` / ``protocol``) from an intent that
    may be a dict or an object, as the bare value.

    Returns ``None`` when absent. An enum is unwrapped to its ``.value`` (so an
    ``IntentType.VAULT_REDEEM`` reads as ``"VAULT_REDEEM"``, not
    ``"IntentType.VAULT_REDEEM"``) — the transience classifier matches the bare
    verb / protocol slug.
    """
    value = intent.get(name) if isinstance(intent, dict) else getattr(intent, name, None)
    if value is None:
        return None
    return str(getattr(value, "value", value))


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
    def deployment_id(self) -> str:
        """Get deployment ID."""
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

    async def get_teardown_state(self, deployment_id: str) -> TeardownState | None:
        """Get teardown state."""
        ...

    async def delete_teardown_state(self, teardown_id: str) -> None:
        """Delete teardown state."""
        ...


class AlertManager(Protocol):
    """Protocol for alert management."""

    async def send_teardown_started(self, deployment_id: str, mode: str) -> None:
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
    # Evict the memoized balance first: earlier intents in the same teardown
    # sequence (a staircase REPAY consuming the wallet's debt token, or a
    # prior sweep draining the residual) change the wallet AFTER this
    # snapshot was built. A stale positive memo here means the no-op skip
    # never fires and the zero-balance sweep falls through to the
    # slippage-escalation loop, failing a teardown whose risk is already
    # removed (Codex review of PR #2726; same mechanism as VIB-5049).
    invalidate = getattr(market, "invalidate_balance", None)
    if callable(invalidate):
        try:
            invalidate(from_token)
        except Exception:  # noqa: BLE001 — fall back to the cached value
            logger.debug("invalidate_balance(%s) failed in skip-check; using cached balance", from_token, exc_info=True)
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


def _clampable_swap_from_token(intent: Any, market: Any) -> str | None:
    """Return the ``from_token`` if ``intent`` is an ``amount='all'`` wallet SWAP
    eligible for the ALM-2766 tracked-quantity clamp, else ``None``.

    Mirrors the gating of :func:`_zero_balance_swap_skip_reason` exactly — SWAP
    only (WITHDRAW / REPAY / LP_CLOSE / ... resolve ``all`` against protocol or
    cross-chain balances, NOT the wallet), not ``withdraw_all``, and a token to
    resolve. ``market is None`` disqualifies (the clamp needs a live read).
    """
    if market is None:
        return None
    is_dict = isinstance(intent, dict)
    amount = intent.get("amount") if is_dict else getattr(intent, "amount", None)
    if amount != "all":
        return None
    intent_type_val = intent.get("intent_type") if is_dict else getattr(intent, "intent_type", None)
    intent_type_str = str(intent_type_val).upper() if intent_type_val is not None else ""
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
    return from_token or None


def _read_live_wallet_balance(market: Any, token: str) -> Decimal | None:
    """Fresh live wallet balance for ``token`` as a ``Decimal``, or ``None``.

    Evicts the memoized balance first (VIB-5074): an earlier teardown intent
    (a REPAY consuming the debt token, a prior sweep) changed the wallet after
    the snapshot was built, so the clamp must resolve against the live
    post-intent value. Returns ``None`` (unmeasured) on any read failure — the
    ALM-2766 clamp then fails closed rather than sweeping.
    """
    invalidate = getattr(market, "invalidate_balance", None)
    if callable(invalidate):
        try:
            invalidate(token)
        except Exception:  # noqa: BLE001 — fall back to the cached value.
            logger.debug("invalidate_balance(%s) failed in clamp read; using cached balance", token, exc_info=True)
    try:
        bal = market.balance(token)
    except Exception:  # noqa: BLE001 — token may not be registered yet.
        return None
    balance_value = bal.balance if hasattr(bal, "balance") else bal
    try:
        return Decimal(str(balance_value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _set_intent_resolved_amount(intent: Any, amount: Decimal) -> Any:
    """Resolve an intent's ``amount`` to a concrete value (ALM-2766 clamp).

    Mirrors the in-closure resolution: dict intents (resume path) take a string
    amount; object intents go through :meth:`Intent.set_resolved_amount`.
    """
    if isinstance(intent, dict):
        return {**intent, "amount": str(amount)}
    from almanak.framework.intents import Intent as _Intent

    return _Intent.set_resolved_amount(intent, amount)


def _serialize_intent_for_state(intent: Any) -> Any:
    """JSON-safe serialization of an intent for ``pending_intents_json``.

    Pydantic intents (``SwapIntent`` etc.) have no ``to_dict`` — use
    ``model_dump(mode="json")`` so Decimals/enums serialize. Dicts pass
    through; anything else falls back to ``str`` (mirrors ``_persist_state``).
    """
    if hasattr(intent, "to_dict"):
        return intent.to_dict()
    if hasattr(intent, "model_dump"):
        return intent.model_dump(mode="json")
    if isinstance(intent, dict):
        return intent
    return str(intent)


def _teardown_chain(intents: list[Any]) -> str | None:
    """Best-effort chain for the teardown plan, for native-gas-token warming.

    Reads the ``chain`` field from the first intent that carries one. Handles
    both decompiled ``Intent`` objects (``execute`` path) and serialized intent
    dicts (``resume`` path). Returns ``None`` when no intent declares a chain;
    the warm step then skips native-gas warming.
    """
    for intent in intents:
        chain = intent.get("chain") if isinstance(intent, dict) else getattr(intent, "chain", None)
        if isinstance(chain, str) and chain.strip():
            return chain.strip()
    return None


def _intents_requiring_pricing(intents: list[Any], market: Any) -> list[Any]:
    """Drop intents that ``_execute_intents`` will skip as a no-op.

    The oracle warm + validate (VIB-4842) pre-flight should only require prices
    for intents that will actually compile. A zero-balance ``amount='all'`` swap
    is short-circuited downstream (``_zero_balance_swap_skip_reason``) and never
    reaches the compiler, so demanding a price for its tokens would fail the
    pre-flight for an operation that does nothing. Mirroring the executor's skip
    logic here keeps the two lanes consistent.
    """
    return [intent for intent in intents if _zero_balance_swap_skip_reason(intent, market) is None]


def _warm_oracle_best_effort(market: Any, executable: list[Any], chain: str | None) -> dict[str, Any] | None:
    """Warm the oracle without failing loud (resume-past-progress path).

    VIB-4842 Codex review P1: on a resume where some closing intents have
    already landed on-chain, the fail-loud pre-flight gate would block the next
    risk-reducing intent — a violation of teardown's inverted-failure semantics
    (AGENTS.md §Teardown). We still warm the cache for the remaining intents,
    but a still-incomplete oracle only logs and the warmed dict is returned.
    """
    return warm_and_validate_oracle(market, executable, chain, raise_on_missing=False)


def _warm_oracle_risk_first(market: Any, intents: list[Any], *, fail_loud: bool) -> dict[str, Any] | None:
    """Warm the price oracle, failing loud ONLY for risk-reducing intents.

    ALM-2766 (CodeRabbit CR#3): the VIB-4842 fail-loud pre-flight warm runs on
    the FULL closing-intent list before ``_execute_intents``. A clampable
    swap-back (``amount='all'`` wallet SWAP) is NON-risk-reducing and may be
    clamp-SKIPPED downstream, so requiring its price would let an unpriceable
    commingled swap-back raise and block the EARLIER risk-reducing intents
    (REPAY / WITHDRAW / LP_CLOSE) — violating "teardown's first job is to remove
    on-chain risk". So the fail-closed clamp is authoritative over pricing:
    swap-backs are warmed BEST-EFFORT (a proceeding tracked swap-back still gets
    a price when one is available; a missing price degrades only that swap, never
    the closing intents), and only the risk-reducing remainder is warmed
    fail-loud (when ``fail_loud``; the resume-past-progress lane passes False).

    Builds on ``_intents_requiring_pricing`` (zero-balance no-op swaps already
    excluded) and ``_clampable_swap_from_token``.
    """
    executable = _intents_requiring_pricing(intents, market)
    swap_backs = [i for i in executable if _clampable_swap_from_token(i, market)]
    risk_intents = [i for i in executable if _clampable_swap_from_token(i, market) is None]

    if fail_loud:
        oracle = warm_and_validate_oracle(market, risk_intents, _teardown_chain(risk_intents))
    else:
        oracle = _warm_oracle_best_effort(market, risk_intents, _teardown_chain(risk_intents))

    if swap_backs:
        warmed = _warm_oracle_best_effort(market, swap_backs, _teardown_chain(swap_backs))
        if warmed:
            oracle = {**(oracle or {}), **warmed}
    return oracle


def _fold_max_receipt_block(current: int | None, execution_result: Any) -> int | None:
    """Fold ``execution_result``'s receipt block into the running MAX (VIB-5140).

    A multi-intent teardown closes several positions whose txs can land in
    DIFFERENT blocks, and intents may complete non-monotonically (slippage
    retries / reordering). The post-teardown closure verifier pins its on-chain
    reads to this block; pinning to the LAST-PROCESSED intent's block would
    under-pin when that block is EARLIER than another close's, making a position
    closed in a LATER block falsely read as still-open. Reading at the HIGHEST
    close block makes every close visible (close state only moves forward), so
    MAX is the correct anchor for verifying all positions.

    Uses the same single-source extractor the iteration/lending lane uses
    (``strategy_runner._last_receipt_block``), which returns a positive block or
    ``None``. A receipt that lacks a block contributes ``0`` and so never lowers
    or erases a prior anchor; the final ``or None`` restores ``None`` when no
    block has ever been seen (caller then falls back to ``"latest"``).
    """
    from almanak.framework.runner.strategy_runner import _last_receipt_block

    return max(current or 0, _last_receipt_block(execution_result) or 0) or None


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

    # crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
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
            deployment_id=strategy.deployment_id,
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

    # crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
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
            logger.info(f"Starting teardown {teardown_id} for {strategy.deployment_id}")
            await strategy.pause()

            # Send started alert
            if self.alert_manager:
                await self.alert_manager.send_teardown_started(strategy.deployment_id, mode)

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

            # TD-11 (VIB-5469): completeness enforcement. Every KNOWN tracked-open
            # position must have a closing intent targeting it; a position with
            # none must FAIL the teardown LOUD, never be reported as a clean
            # success (VIB-5417: spark teardown returned []; ALM-2900: repaid but
            # never withdrew). This is a pre-execution INTENT-COVERAGE check,
            # distinct from on-chain verification (TD-15). Computed here; for the
            # intents-present path it is folded into the result AFTER execution so
            # the risk-reducing intents still run first (inverted semantics).
            completeness = check_intent_coverage(positions, intents)

            if not intents:
                if completeness.complete:
                    logger.info(f"No intents to execute for {strategy.deployment_id}")
                    return self._empty_result(strategy.deployment_id, mode, started_at)
                # Tracked-open positions but no intents at all → nothing to
                # execute and a known position would be stranded. Fail loud.
                logger.error("🛑 %s", completeness.error_message())
                return self._failed_result(
                    strategy.deployment_id,
                    mode,
                    started_at,
                    error=completeness.error_message(),
                    verification_status=VerificationStatus.FAILED,
                    # Stamp the breakdown so the lifecycle surface reports 0/N
                    # closed (matching the error) rather than a misleading 0/0.
                    positions_total=completeness.total_enforceable,
                    positions_closed=0,
                    has_position_breakdown=True,
                )

            # Step 3: Validate safety
            validation = self.safety_guard.validate_teardown_request(positions, internal_mode)
            if not validation.all_passed:
                logger.error(f"Safety validation failed: {validation.blocked_reason}")
                return self._failed_result(
                    strategy.deployment_id,
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
                return self._cancelled_result(strategy.deployment_id, mode, started_at)

            # Update state to executing
            teardown_state.status = TeardownStatus.EXECUTING
            if self.state_manager:
                await self.state_manager.save_teardown_state(teardown_state)

            # Step 5.5 (VIB-4842): Warm + validate the price oracle BEFORE
            # compile. A fresh MarketSnapshot has an empty price cache, so
            # ``get_price_oracle_dict()`` would return {} and the compiler would
            # fail three layers down with a generic ValueError. This is a
            # pre-flight check — no closing intent has executed yet, so failing
            # loud here cannot strand a partially-unwound position. Only warm
            # tokens for intents that will actually execute (zero-balance no-op
            # swaps are skipped downstream, so their tokens are not required).
            # ALM-2766 (CR#3): clampable swap-backs are warmed best-effort, NOT
            # fail-loud — a swap-back we may clamp-skip must never block the
            # risk-reducing closing intents on an unpriceable commingled token.
            price_oracle = _warm_oracle_risk_first(market, intents, fail_loud=True)

            # TD-15 (VIB-5473) AC-(b): capture a PRE-teardown Plan-A reconciliation
            # over the KNOWN positions BEFORE any closing intent fires. The runner
            # lane gets this from ``runner._teardown_reconciliation`` (TD-08); the
            # CLI ``execute`` lane has no runner, so compute it inline here so
            # ``verify_closure_against_chain`` can downgrade a stale / never-existed
            # position (one the chain already reports closed *now*, pre-teardown)
            # from CHAIN_VERIFIED. CHECK-only, never raises (a fault leaves it None
            # → no AC-(b) downgrade, same as before this wiring existed).
            pre_teardown_reconciliation = await self._pre_teardown_reconciliation(strategy, positions, market)

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
                    # VIB-5085: use the detailed verifier so we record how many
                    # *positions* (not intents) closed.
                    verification = await self._verify_closure_detailed(
                        strategy,
                        expected_positions=precomputed_positions,
                        pre_execution_positions=positions,
                        close_receipt_block=result.last_receipt_block,
                    )
                except Exception as verify_err:
                    logger.exception(
                        "Post-teardown verification raised for %s — treating as verify-fail",
                        strategy.deployment_id,
                    )
                    verification = ClosureVerification(
                        all_closed=False,
                        positions_total=len(getattr(positions, "positions", []) or []),
                        positions_closed=0,
                        has_position_breakdown=True,
                        verification_status=VerificationStatus.FAILED,
                    )
                    verify_error_msg = f"Post-teardown verification error: {verify_err}. Manual check required."
                else:
                    verify_error_msg = "Post-teardown verification failed: positions still open. Manual check required."

                # TD-15 (VIB-5473): fail-closed on-chain POST-teardown
                # verification on the CLI ``teardown execute`` lane too. Runs
                # AFTER closure (risk reduction first). Folds in BOTH a FRESH
                # POST-teardown reconciliation and the PRE-teardown report computed
                # above: a position the chain STILL reports OPEN flips the result to
                # FAILED (AC-(a)); a POST-teardown unconfirmable closure is a no-op
                # (a burned LP NFT reading "not found" is the SUCCESS signal, not a
                # doubt); a position that read closed / unconfirmable PRE-teardown
                # lowers CHAIN_VERIFIED to UNVERIFIED (stale / never-existed, AC-(b)).
                # Never raises.
                verification = await self.verify_closure_against_chain(
                    strategy,
                    verification=verification,
                    pre_execution_positions=positions,
                    market=market,
                    pre_teardown_reconciliation=pre_teardown_reconciliation,
                )

                # TD-11 (VIB-5469): fold the pre-execution completeness check into
                # the verification result. A tracked-open position with no closing
                # intent must FAIL the teardown even when every emitted intent
                # executed and on-chain verification of the COVERED positions
                # passed — the gap is in coverage, not execution. Force the
                # FAILED status so the existing fail-closed machinery (persist
                # FAILED + success=False + recovery options) handles it. Applied
                # AFTER the TD-15 chain re-read so the coverage gap is the final,
                # loudest word.
                if not completeness.complete:
                    # The uncovered positions are definitively NOT closed (no
                    # intent even targeted them), so cap positions_closed so the
                    # persisted positions_failed = total - closed reflects them
                    # rather than reading 0 failed on a FAILED teardown (VIB-5469).
                    uncovered_count = len(completeness.uncovered)
                    # Carry the uncovered positions into the denominator: if the
                    # verifier had no position breakdown (positions_total=0),
                    # mark_failed (positions_failed = total - closed) would
                    # otherwise record 0 failed on a teardown that FAILED
                    # specifically because known-open positions had no closing
                    # intent — a self-contradicting failure record (VIB-5469).
                    positions_total = max(verification.positions_total, completeness.total_enforceable)
                    adjusted_closed = max(
                        min(verification.positions_closed, positions_total - uncovered_count),
                        0,
                    )
                    verification = replace(
                        verification,
                        all_closed=False,
                        positions_total=positions_total,
                        positions_closed=adjusted_closed,
                        has_position_breakdown=True,
                        verification_status=VerificationStatus.FAILED,
                    )
                    verify_error_msg = completeness.error_message()

                # VIB-5085: stamp the position-level counts onto the result so
                # the CLI lifecycle writer reports positions, not intents.
                # ``has_position_breakdown`` is only True when the verifier had a real
                # pre-execution snapshot — on the in-memory fallback (empty
                # snapshot) it stays False so the writer falls back to the intent
                # count instead of persisting a misleading ``positions_closed=0``.
                # VIB-2932 / VIB-5472: also stamp the verification confidence so
                # the lifecycle surface can flag an unverifiable closure.
                result = replace(
                    result,
                    positions_total=verification.positions_total,
                    positions_closed=verification.positions_closed,
                    has_position_breakdown=verification.has_position_breakdown,
                    verification_status=verification.verification_status,
                )

                # VIB-5478: structured VERIFY decision entry (CLI execute lane).
                # Records the closure confidence (TD-14 count + TD-15 status) so
                # an unverifiable closure is auditable, never silently optimistic.
                log_teardown_decision(
                    deployment_id=strategy.deployment_id,
                    teardown_id=teardown_id,
                    phase=TeardownDecisionPhase.VERIFY,
                    outcome="verified" if verification.all_closed else "verify_failed",
                    description=(
                        f"closure verification: {verification.positions_closed}/"
                        f"{verification.positions_total} closed "
                        f"({verification.verification_status.value})"
                    ),
                    position_count=verification.positions_total,
                    positions_closed=verification.positions_closed,
                    verification_status=verification.verification_status.value,
                )

                if not verification.all_closed:
                    logger.warning(
                        f"Post-teardown verification: {strategy.deployment_id} still reports "
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

            # Step 7.5 (VIB-5011): token consolidation (Phase 2). Runs ONLY
            # after a successful closure + verification — never before, and
            # never when the unwind is incomplete (the residual-token swap
            # must not race a partially-unwound position). Covers the CLI
            # execute lane (run_teardown_with_brackets → execute()); the
            # runner lane calls _execute_intents directly and hooks the
            # phase in _teardown_helpers.execute_and_verify instead, so the
            # two hooks never both fire for one teardown. Failure here is
            # non-fatal by contract: success stays True, partial state lands
            # on the consolidation_* result fields.
            if result.success:
                from almanak.framework.teardown.consolidation import fold_consolidation_outcome

                consolidation_outcome = await self.run_token_consolidation(
                    strategy,
                    teardown_id=teardown_id,
                    teardown_state=teardown_state,
                    mode=internal_mode,
                    market=market,
                    price_oracle=price_oracle,
                    positions=positions,
                    closing_intents=intents,
                    is_auto_mode=is_auto_mode,
                    on_approval_needed=on_approval_needed,
                )
                result = fold_consolidation_outcome(result, consolidation_outcome)

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
                strategy.deployment_id,
                mode,
                started_at,
                error=str(e),
            )

    # crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
    async def cancel(self, deployment_id: str) -> bool:
        """Cancel an in-progress teardown.

        Graceful mode: Cancellable anytime before completion.
        Emergency mode: Only during 10-second window.

        Args:
            deployment_id: ID of the strategy being torn down

        Returns:
            True if cancellation succeeded
        """
        if self.state_manager is None:
            logger.warning("No state manager - cannot cancel")
            return False

        state = await self.state_manager.get_teardown_state(deployment_id)

        if not state:
            logger.warning(f"No active teardown for {deployment_id}")
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
        deployment_id: str,
        strategy: IntentStrategy,
        on_approval_needed: ApprovalCallback | None = None,
        on_progress: Callable[[int, str], Awaitable[None]] | None = None,
        market: Any = None,
    ) -> TeardownResult | None:
        """Resume an interrupted teardown.

        Called on system startup to detect and resume in-progress teardowns.
        Includes staleness check - re-generates intents if too old.

        Args:
            deployment_id: ID of the strategy
            strategy: The strategy instance
            on_approval_needed: Callback for approval requests
            on_progress: Callback for progress updates

        Returns:
            TeardownResult if resumed and completed, None if nothing to resume
        """
        if self.state_manager is None:
            return None

        state = await self.state_manager.get_teardown_state(deployment_id)

        if not state or not state.is_resumable:
            return None

        logger.info(f"Resuming teardown {state.teardown_id}")

        # Codex re-audit P1 (VIB-4842): the oracle GATE and the resume INDEX read
        # the SAME progress counters, but the staleness/regeneration branch below
        # resets ``completed_intents``/``current_intent_index`` to 0 so the resume
        # INDEX re-runs the full regenerated plan. That reset also zeroes the
        # signal the GATE needs. A partially-unwound *regenerated* teardown (old-
        # plan closes already on-chain) would then read zeroed counters, wrongly
        # take the fail-loud pre-flight path, and RAISE on an unpriceable
        # regenerated close — blocking the remaining risk-reducing intents
        # (violates teardown's inverted-failure semantics; AGENTS.md §Teardown).
        #
        # Decouple the two signals: capture whether ANY on-chain progress existed
        # BEFORE any reset, and let the GATE consume this captured flag. The
        # resume INDEX keeps consuming the (possibly-reset) live counters.
        had_prior_progress = state.completed_intents > 0 or state.current_intent_index > 0

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
            # VIB-5139: the regenerated plan is freshly hand-rolled lending intents
            # that have NOT passed the runner/CLI fresh-state guard, so sanitise it
            # here too (drop stale REPAY 0 / withdraw_all-when-flat, preserve any
            # interleaved staircase order). Pure list transform — execution still
            # routes through _execute_intents with the commit pairing intact.
            from .lending_unwind_guard import sanitize_lending_teardown_intents

            guarded = sanitize_lending_teardown_intents(intents, market, mode=state.mode)
            for reason in guarded.dropped:
                logger.info("Teardown resume lending guard dropped intent — %s", reason)
            for synth in guarded.synthesized_positions:
                logger.info(
                    "Teardown resume lending guard synthesised HF-safe unwind staircase for %s (VIB-4466)", synth
                )
            intents = guarded.intents
            state.pending_intents_json = json.dumps([_serialize_intent_for_state(i) for i in intents])
            # The guard can EXPAND (synthesis) or SHRINK (drops) the plan, so keep the
            # progress denominator in sync — otherwise a resumed teardown reports
            # progress against the stale pre-guard intent count (VIB-4466 / CodeRabbit).
            state.total_intents = len(intents)
            state.current_intent_index = 0
            # Codex re-audit P1: the freshly generated plan is a brand-new
            # intent list with zero completed work, so reset the progress
            # counter too. ``resume_from_index`` below floors at
            # ``max(current_intent_index, completed_intents)``; without this
            # reset the stale ``completed_intents = N`` from the OLD plan would
            # make the resumed run start at index N of the NEW plan, skipping
            # the first N regenerated closes (or marking teardown COMPLETE when
            # the new plan is shorter than N) and stranding on-chain risk. Only
            # reset here on the regeneration path — the non-regeneration resume
            # (same plan, e.g. completed_intents=1/current_intent_index=0) must
            # keep ``completed_intents`` so it still skips finished intents.
            state.completed_intents = 0
            # VIB-5174: the regenerated plan is a brand-new CLOSING plan, NOT the
            # consolidation tail the operator consent was stamped for. Operator
            # consent (set True only by ``run_token_consolidation`` for a MANUAL
            # consolidation) must NOT carry over to this fresh closing plan — its
            # ``amount='all'`` swap-backs would otherwise run UNCLAMPED (the
            # ALM-2766 clamp is gated loop-wide on ``consolidation_consent``),
            # sweeping commingled wallet funds. Reset to the fail-safe default so
            # the regenerated swap-backs re-clamp (safe under-sweep direction).
            # Reset BOTH the field AND the ``config_json`` reserved key, because
            # the save-site OR-merge (``consolidation_consent or decode(config_json)``)
            # would otherwise resurrect True from the snapshot on the next save.
            state.consolidation_consent = False
            state.config_json = encode_consolidation_consent(state.config_json, False)

        # Parse intents from state
        intents_data = json.loads(state.pending_intents_json) if state.pending_intents_json else []

        if not intents_data:
            logger.info(f"No pending intents for {state.teardown_id}")
            return None

        # VIB-4842: Warm + validate the oracle on resume too — a restarted
        # runner has a fresh MarketSnapshot, so the cache is empty.
        #
        # Codex review P1: the *loud* validate-and-fail gate is a PRE-FLIGHT
        # check — it may only fail loud when NO closing intent has executed yet.
        # On a resume that has already made progress (some intents landed
        # on-chain), failing loud here would block the next risk-reducing intent
        # and strand a partially-unwound position — exactly the inverted-failure
        # semantics teardown forbids (AGENTS.md §Teardown; blueprint 14
        # §loud-but-non-blocking).
        #
        # Codex re-audit P1: ``current_intent_index`` is persisted BEFORE
        # executing intent ``i`` (it marks "about to run i", not "finished i")
        # and is never advanced after a success — only ``completed_intents`` is
        # bumped post-execution. So after a restart where the first intent
        # (``i == 0``) already landed on-chain, the persisted state can have
        # ``completed_intents > 0`` while ``current_intent_index`` is still 0.
        # Gating the loud branch on the index alone would misread that very
        # common "made progress on intent 0" state as a fresh start and could
        # block the next risk-reducing intent. The authoritative progress signal
        # is ``completed_intents`` (incremented on both the success and the
        # zero-balance-skip paths). Treat the run as a genuine fresh start —
        # safe to fail loud — ONLY when there is no on-chain progress at all:
        # ``completed_intents == 0`` AND ``current_intent_index == 0``.
        #
        # Codex re-audit P1 (VIB-4842): use ``had_prior_progress`` captured at the
        # TOP of resume() — BEFORE the staleness/regeneration branch may have
        # reset the live counters to 0. Re-reading ``state.completed_intents`` /
        # ``state.current_intent_index`` here would, on a regenerated stale
        # teardown that DID make prior on-chain progress, see the just-zeroed
        # counters and wrongly take the fail-loud path. The captured flag keeps
        # the GATE decision tied to genuine prior progress, fully decoupled from
        # the resume INDEX (which legitimately reads the reset counters so the
        # regenerated plan runs from index 0).
        #
        # Any progress → warm best-effort (populate the cache for the remaining
        # intents) but DO NOT raise: fall back to the legacy lenient fetch so a
        # still-incomplete oracle cannot halt the unwind.
        if had_prior_progress:
            logger.info(
                "Resuming teardown %s past prior progress (current completed=%d, intent index=%d) — "
                "warming oracle best-effort; the fail-loud pre-flight gate is skipped "
                "to preserve teardown's inverted-failure semantics.",
                state.teardown_id,
                state.completed_intents,
                state.current_intent_index,
            )
            # ALM-2766 (CR#3): clampable swap-backs are warmed best-effort here
            # too (already non-fail-loud on this resume path).
            price_oracle = _warm_oracle_risk_first(market, intents_data, fail_loud=False)
        else:
            # ALM-2766 (CR#3): only the risk-reducing intents are warmed
            # fail-loud; clampable swap-backs are best-effort so an unpriceable
            # commingled swap-back cannot block the risk-reducing closers.
            price_oracle = _warm_oracle_risk_first(market, intents_data, fail_loud=True)

        # Continue execution from where we left off
        positions = strategy.get_open_positions()

        # Codex re-audit P1: resume must start at the next UNFINISHED intent,
        # never re-run a completed one. ``current_intent_index`` is persisted
        # BEFORE executing intent ``i`` (it marks "about to run i") and is never
        # advanced after a success — only ``completed_intents`` is bumped
        # post-execution (success path: ``completed_intents = succeeded`` after
        # the per-intent commit; zero-balance-skip path: same assignment after
        # ``succeeded += 1``). So ``completed_intents`` equals the count of fully
        # handled intents = the index of the next unfinished intent, while
        # ``current_intent_index`` lags it by one whenever a restart happens
        # mid-loop after an intent already landed on-chain (e.g. intent 0
        # committed → completed_intents=1 but current_intent_index=0). Starting
        # from the raw index alone would re-execute that completed closing
        # action (a second LP_CLOSE / withdraw), which the per-intent zero-
        # balance skip does NOT cover for non-``amount='all'`` closers.
        # Take the floor at ``completed_intents`` to skip finished work, but
        # keep ``current_intent_index`` in the max() so a legitimately larger
        # persisted index is still honored.
        resume_from_index = max(state.current_intent_index, state.completed_intents)

        return await self._execute_intents(
            teardown_id=state.teardown_id,
            strategy=strategy,
            intents=intents_data,  # Already serialized
            positions=positions,
            mode=state.mode,
            teardown_state=state,
            on_approval_needed=on_approval_needed,
            on_progress=on_progress,
            start_from_index=resume_from_index,
            price_oracle=price_oracle,
            market=market,
            # VIB-5174: thread the persisted consent so a resumed MANUAL
            # consolidation tail does NOT re-clamp the operator-consented
            # full-wallet sweep. Default False (closing-phase resume, or an
            # AUTOMATIC teardown, or a pre-feature row) keeps the clamp ON.
            # Safe either way: the resume index is past every closing intent,
            # so consent only ever reaches the consolidation swaps it was
            # stamped for.
            consolidation_consent=state.consolidation_consent,
        )

    async def run_token_consolidation(
        self,
        strategy: IntentStrategy,
        *,
        teardown_id: str,
        teardown_state: TeardownState,
        mode: TeardownMode,
        market: Any = None,
        price_oracle: dict | None = None,
        positions: TeardownPositionSummary | None = None,
        closing_intents: list | None = None,
        is_auto_mode: bool = False,
        on_approval_needed: ApprovalCallback | None = None,
    ) -> Any:
        """Run the token-consolidation phase (Phase 2, VIB-5011).

        Plans residual-token swaps via the pure planner
        (:mod:`almanak.framework.teardown.consolidation`) from live
        post-closure wallet balances, then — when the plan is non-empty —
        extends the persisted ``teardown_state`` plan and REUSES
        :meth:`_execute_intents` with a ``start_from_index`` offset. That
        reuse is the load-bearing part: consolidation swaps run the same
        slippage-escalation ladder, the same per-intent commit pairing via
        the runner helpers (the anti-bypass guard sees no new orchestrator
        execute site), the same zero-balance skips, and the same resume-safe
        progress persistence as closing intents. There is deliberately
        **no second cancel window**.

        Never raises: every exception is folded into the returned
        :class:`~almanak.framework.teardown.consolidation.ConsolidationOutcome`
        — a consolidation failure after a successful closure must never
        un-succeed the teardown (the on-chain risk is already removed).
        """
        from almanak.framework.teardown.consolidation import (
            ConsolidationOutcome,
            derive_strategy_token_universe,
            plan_consolidation,
            resolve_consolidation_targets,
        )

        try:
            cfg = self.config.token_consolidation
            closing = list(closing_intents or [])

            # Strategy-scoped token universe — via the runner-bound helper
            # when wired (includes the accounting-event footprint), else the
            # planner's intents/positions/profile-only derivation.
            if self.runner_helpers.has_token_universe:
                token_universe = self.runner_helpers.get_token_universe(  # type: ignore[misc]
                    strategy, closing, positions
                )
            else:
                token_universe = derive_strategy_token_universe(
                    None, strategy.deployment_id, strategy, closing, positions
                )

            accounting_events = None
            if self.runner_helpers.has_accounting_events:
                accounting_events = self.runner_helpers.get_accounting_events(strategy)  # type: ignore[misc]

            target_token = cfg.target_token or self.config.target_token
            targets, target_warnings = resolve_consolidation_targets(
                self.config.asset_policy,
                target_token,
                strategy,
                accounting_events=accounting_events,
            )

            chain = _teardown_chain(closing) or (getattr(strategy, "chain", None) or None)
            plan = plan_consolidation(
                market=market,
                chain=chain,
                asset_policy=self.config.asset_policy,
                target_token=target_token,
                token_consolidation_cfg=cfg,
                token_universe=token_universe,
                mode=mode,
                targets=targets,
            )
            warnings = [*target_warnings, *plan.warnings]
            if plan.intents:
                # Wallet-scope disclosure (pr-auditor): token SELECTION is
                # strategy-scoped, but each swap's AMOUNT is the full wallet
                # balance of that token. On a wallet shared across
                # deployments this includes sibling strategies' balances of
                # the same token. Surfaced as a warning on the result (not
                # just a log line) so the operator sees it in `teardown
                # status` / `--wait`.
                swept = ", ".join(getattr(i, "from_token", "?") for i in plan.intents)
                warnings.append(
                    f"consolidation amounts are wallet-scoped (amount=all) for: {swept} — "
                    "on a shared wallet this includes balances owned by other "
                    "deployments holding the same token(s)"
                )
            for decision in plan.decisions:
                logger.info(
                    "Token consolidation decision for %s: %s %s (reason=%s, value_usd=%s)",
                    strategy.deployment_id,
                    decision.action,
                    decision.token,
                    decision.reason,
                    decision.value_usd,
                )
            for warning in warnings:
                logger.warning("Token consolidation: %s", warning)

            if not plan.intents:
                return ConsolidationOutcome(
                    planned=0, succeeded=0, failed=0, warnings=warnings, decisions=plan.decisions
                )

            logger.info(
                "Token consolidation for %s: %d swap(s) planned → %s",
                strategy.deployment_id,
                len(plan.intents),
                target_token,
            )

            # Extend the persisted plan so a crash mid-consolidation resumes
            # at the right index (the closing intents at [0, offset) are
            # already complete — _execute_intents starts past them).
            try:
                existing = (
                    json.loads(teardown_state.pending_intents_json) if teardown_state.pending_intents_json else []
                )
                # A corrupted non-list value (JSON object/string) would make
                # len() and the [*existing, ...] splat below misbehave —
                # treat it as an empty plan (Gemini review).
                if not isinstance(existing, list):
                    existing = []
            except (TypeError, ValueError):
                existing = []
            start_from_index = len(existing)
            serialized = [_serialize_intent_for_state(i) for i in plan.intents]
            teardown_state.pending_intents_json = json.dumps([*existing, *serialized])
            teardown_state.total_intents = start_from_index + len(plan.intents)
            teardown_state.status = TeardownStatus.EXECUTING
            # VIB-5174: persist the consent decision ALONGSIDE the extended plan so
            # a crash mid-consolidation resumes with the same consent. The closing
            # intents at [0, start_from_index) are already complete, so on resume
            # ``_execute_intents`` only re-touches the consolidation tail — stamping
            # the teardown-level flag never disables the clamp for a closing-intent
            # swap-back. Mirrors the in-process ``consolidation_consent=`` arg below.
            teardown_state.consolidation_consent = not is_auto_mode
            # Keep the in-memory ``config_json`` snapshot in sync with the field so
            # the two never diverge in-process (the save layer OR-merges anyway, but
            # an in-process reader of ``config_json`` must see the same grant). This
            # is the in-memory mirror of the regeneration reset in ``resume()``.
            teardown_state.config_json = encode_consolidation_consent(
                teardown_state.config_json, teardown_state.consolidation_consent
            )
            teardown_state.updated_at = datetime.now(UTC)
            if self.state_manager:
                await self.state_manager.save_teardown_state(teardown_state)

            # Warm the oracle best-effort for the consolidation tokens — the
            # closing-phase warm only covered closing-intent tokens. The
            # closure already executed, so a still-incomplete oracle must
            # never raise here (inverted-failure semantics).
            oracle_for_swaps = price_oracle
            warmed = _warm_oracle_best_effort(market, plan.intents, chain)
            if warmed:
                oracle_for_swaps = {**(price_oracle or {}), **warmed}

            combined = [*existing, *plan.intents]
            consolidation_positions = positions or TeardownPositionSummary.empty(strategy.deployment_id)
            result = await self._execute_intents(
                teardown_id=teardown_id,
                strategy=strategy,
                intents=combined,
                positions=consolidation_positions,
                mode=mode,
                teardown_state=teardown_state,
                on_approval_needed=on_approval_needed,
                start_from_index=start_from_index,
                is_auto_mode=is_auto_mode,
                price_oracle=oracle_for_swaps,
                market=market,
                # ALM-2766: consent to a full-wallet consolidation sweep ONLY on
                # an operator-initiated (manual) teardown. Consolidation also
                # runs on AUTOMATIC teardowns (risk-guard / auto-protect /
                # config-reload carry a non-None request, so
                # ``_teardown_config_from_request`` leaves consolidation enabled
                # and they run SOFT mode) — and an automatic teardown has NO
                # operator present to consent to sweeping commingled / sibling-
                # deployment balances. So the clamp must STAY ON for auto-mode
                # consolidation; only a manual teardown (``is_auto_mode`` False
                # → ``requested_by`` in {cli, dashboard, dashboard_api}) is the
                # consented full-wallet sweep. ``TeardownRequest.asset_policy``
                # defaults to TARGET_TOKEN, so it carries NO distinguishable
                # explicit-consent signal — manual-vs-auto is the only reliable
                # operator-initiated signal (derive_teardown_auto_mode).
                consolidation_consent=not is_auto_mode,
            )

            # _execute_intents counts only the loop it ran (offset onward),
            # so its succeeded/failed totals ARE the consolidation counts.
            succeeded = min(result.intents_succeeded, len(plan.intents))
            failed = max(len(plan.intents) - succeeded, 0)
            if failed:
                warnings.append(
                    f"{failed} consolidation swap(s) failed ({result.error or 'see logs'}) — "
                    "wallet holds residual non-target tokens; teardown closure itself succeeded"
                )
            return ConsolidationOutcome(
                planned=len(plan.intents),
                succeeded=succeeded,
                failed=failed,
                warnings=warnings,
                decisions=plan.decisions,
                accounting_degraded_count=result.accounting_degraded_count,
            )
        except Exception as exc:  # noqa: BLE001 — consolidation must never un-succeed the teardown
            logger.exception(
                "Token consolidation phase raised for %s — closure already complete; continuing without consolidation",
                strategy.deployment_id,
            )
            return ConsolidationOutcome(
                planned=0,
                succeeded=0,
                failed=1,
                warnings=[f"token consolidation raised: {exc}"],
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
        consolidation_consent: bool = False,
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
            consolidation_consent: ALM-2766 — when True, the ``amount='all'``
                swap-back clamp is SKIPPED (full-wallet sweep preserved). Set
                by the VIB-5011 token-consolidation lane ONLY for
                operator-initiated (MANUAL) teardowns (``not is_auto_mode``).
                Consolidation also runs on AUTOMATIC teardowns (risk-guard /
                auto-protect / config-reload), but those have no operator
                present to consent to a wallet-scoped sweep, so the clamp STAYS
                ON for them (consent False). All non-consolidation closing-
                intent calls leave it False (clamp on). See blueprint 14 §4.5.

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

        # VIB-5140: track the block of the last successful close-tx receipt so
        # the post-teardown on-chain closure verifier can pin its state reads
        # to it (instead of an unpinned "latest" that a trailing read replica
        # can answer with PRE-close state → false-negative → STRATEGY_ERROR).
        last_receipt_block: int | None = None

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

        # VIB-5573 (WI-2): iterate a mutable work QUEUE, not a fixed range, so a
        # TRANSIENT-reverting close can be re-queued to the TAIL and retried on
        # the time axis AFTER every first-attempt intent. ``attempts`` counts a
        # given intent's deferrals. The first pass preserves the planner's
        # risk-priority order (enumerate from start_from_index); deferred items
        # always trail, so a transient retry never delays a not-yet-tried
        # risk-reducing close (blueprint 14a §Stage 6).
        work: deque[tuple[int, Any, int]] = deque(
            (idx, it, 0) for idx, it in enumerate(intents[start_from_index:], start=start_from_index)
        )

        # VIB-5573 (WI-2): resume-safe progress cutoff. Deferred transient retries
        # make completion NON-CONTIGUOUS (intent 0 can finish AFTER intent 1), so
        # the running ``succeeded`` count is NOT a safe resume cutoff — persisting
        # ``start_from_index + succeeded`` could let ``resume()``
        # (``max(current_intent_index, completed_intents)``) skip a deferred but
        # unfinished intent and strand it (CodeRabbit). Track the still-PENDING
        # original indices and persist the resume FLOOR = the lowest not-yet-done
        # index, so resume always re-runs a pending deferred intent. Re-running a
        # later-already-completed close on resume is a safe no-op (live resolution
        # → zero-balance skip). For a purely sequential teardown the floor equals
        # ``start_from_index + succeeded`` exactly, so behaviour is unchanged.
        _pending_indices: set[int] = {idx for idx, _, _ in work}

        def _resume_floor() -> int:
            return min(_pending_indices) if _pending_indices else len(intents)

        while work:
            i, intent, attempts = work.popleft()

            # VIB-5573 (WI-2): a re-queued transient waits a bounded backoff
            # BEFORE re-firing so the underflow/queue-inconsistency has time to
            # clear. Placed at the queue level (not inside the per-intent await)
            # so the wait never delays a first-attempt risk-reducing close —
            # deferred items sit behind every attempts==0 intent in the queue.
            if attempts > 0:
                await asyncio.sleep(_TRANSIENT_BACKOFF_S * attempts)

            # Update progress
            progress_pct = int((i / len(intents)) * 100)
            if on_progress:
                await on_progress(progress_pct, f"Executing step {i + 1}/{len(intents)}")

            # Update state — persist the resume FLOOR (lowest pending index), NOT
            # the raw ``i``: a deferred retry pops out of original order, so ``i``
            # can be ahead of an earlier still-pending intent (VIB-5573).
            _floor = _resume_floor()
            teardown_state.current_intent_index = _floor
            teardown_state.completed_intents = _floor
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
                # Mirror the success-path persist so a crash mid-teardown records
                # the skip as completed and resume picks up past it. Persist the
                # resume FLOOR (lowest still-pending index) — a no-op skip is done,
                # so drop it from the pending set first (VIB-5573).
                _pending_indices.discard(i)
                _floor = _resume_floor()
                teardown_state.completed_intents = _floor
                teardown_state.current_intent_index = _floor
                teardown_state.updated_at = datetime.now(UTC)
                if self.state_manager:
                    await self.state_manager.save_teardown_state(teardown_state)
                continue

            # ALM-2766: clamp an ``amount='all'`` swap-back to the strategy's
            # TRACKED quantity so a DEFAULT teardown never sweeps commingled
            # wallet funds (a shared wallet's sibling balances, or pre-existing
            # holdings the strategy never owned). ``tracked_qty = Σ open
            # wallet-basis lot remaining`` (SWAP / BORROW / WITHDRAW sources —
            # so a looping teardown's withdrawn collateral counts), and we swap
            # ``min(tracked_qty, live)``. Fail-closed on an unprovable quantity
            # (skip the swap, flag degraded) — a swap-back is never the
            # risk-reducing intent, so skipping it strands no on-chain risk.
            #
            # SKIPPED entirely under ``consolidation_consent`` — set ONLY by the
            # VIB-5011 consolidation lane for an operator-initiated (MANUAL)
            # teardown (§4.5). Automatic teardowns (risk-guard / auto-protect /
            # config-reload) DO run consolidation, but with consent False so the
            # clamp stays on (no operator present to consent to a wallet sweep).
            #
            # Placed at loop scope (not inside ``execute_at_slippage``) so a
            # skip can ``continue`` the loop — the same pattern as the
            # zero-balance skip above; a skip from inside the per-slippage
            # closure would be miscounted as a failure and pointlessly escalate
            # slippage. Once a clampable swap is identified here, this branch
            # OWNS the outcome (proceed-clamped or skip): the swap never falls
            # through to the closure's full-``all`` resolution, fully closing
            # the sweep bypass.
            if not consolidation_consent:
                clamp_token = _clampable_swap_from_token(intent, market)
                if clamp_token:
                    live_balance = _read_live_wallet_balance(market, clamp_token)
                    if live_balance is None:
                        # Unmeasured live balance → cannot prove the clamp.
                        # Fail closed: skip rather than risk a full sweep, and
                        # flag degraded for reconciliation.
                        decision = SwapClampDecision(None, True, True, "live_balance_unmeasured")
                    else:
                        tracked_map = (
                            self.runner_helpers.get_tracked_swap_inventory(strategy)  # type: ignore[misc]
                            if self.runner_helpers.has_tracked_inventory
                            else None
                        )
                        decision = decide_swap_clamp(
                            live_balance=live_balance,
                            tracked_map=tracked_map,
                            from_token=clamp_token,
                        )
                    if decision.degraded:
                        accounting_degraded_records.append(
                            {
                                "kind": "swap_clamp_degraded",
                                "intent_index": i,
                                "token": clamp_token,
                                "reason": decision.reason,
                            }
                        )
                    if decision.skip:
                        # Preserve the VIB-4587 sweep WARNING as the operator
                        # signal — especially for the untracked-token skip
                        # (this is exactly the commingled-funds case it warns
                        # about). Best-effort; never blocks the unwind.
                        if self.runner_helpers.has_sweep_warning:
                            try:
                                self.runner_helpers.warn_sweep_non_strategy_balance(  # type: ignore[misc]
                                    strategy,
                                    intent,
                                    clamp_token,
                                    live_balance if live_balance is not None else Decimal("0"),
                                )
                            except Exception:  # noqa: BLE001
                                logger.debug("sweep-warning helper raised in clamp skip; ignored", exc_info=True)
                        logger.warning(
                            "🛑 ALM-2766 teardown swap-back clamp: SKIPPING %s swap "
                            "(reason=%s, degraded=%s) — not sweeping commingled wallet funds.",
                            clamp_token,
                            decision.reason,
                            decision.degraded,
                        )
                        # VIB-5478: structured BLOCK decision entry — a swap-back
                        # the clamp REFUSED to sweep (untracked / unmeasured /
                        # commingled). Audit trail only; never blocks the unwind.
                        log_teardown_decision(
                            deployment_id=strategy.deployment_id,
                            teardown_id=teardown_id,
                            phase=TeardownDecisionPhase.BLOCK,
                            outcome="swap_clamp_skipped",
                            description=f"swap-back clamp skipped {clamp_token} ({decision.reason})",
                            token=clamp_token,
                            reason=decision.reason,
                            degraded=decision.degraded,
                            intent_count=1,
                        )
                        # No-op success (nothing of ours to swap) — preserves
                        # the ``intents_total = succeeded + failed`` invariant
                        # and does not mark the teardown failed. Persist the
                        # ABSOLUTE completed count (see the zero-balance skip).
                        succeeded += 1
                        skipped += 1
                        if on_progress:
                            await on_progress(
                                progress_pct, f"Skipped step {i + 1}/{len(intents)}: clamp {decision.reason}"
                            )
                        # Done → drop from pending; persist the resume floor (VIB-5573).
                        _pending_indices.discard(i)
                        _floor = _resume_floor()
                        teardown_state.completed_intents = _floor
                        teardown_state.current_intent_index = _floor
                        teardown_state.updated_at = datetime.now(UTC)
                        if self.state_manager:
                            await self.state_manager.save_teardown_state(teardown_state)
                        continue
                    # Proceed with the clamped amount: resolve the intent here so
                    # the per-slippage closure's ``amount='all'`` branch is
                    # bypassed and every retry uses the clamped quantity.
                    intent = _set_intent_resolved_amount(intent, decision.amount)  # type: ignore[arg-type]
                    logger.info(
                        "🛑 ALM-2766 clamped %s swap-back to tracked qty %s (live wallet %s).",
                        clamp_token,
                        decision.amount,
                        live_balance,
                    )
                    # VIB-5478: structured SIZE decision entry — the swap-back was
                    # sized to the strategy's tracked inventory (TD-07). The
                    # resolved amount itself is money-shaped, so it lands in the
                    # ledger, not here; the decision records only token + reason.
                    log_teardown_decision(
                        deployment_id=strategy.deployment_id,
                        teardown_id=teardown_id,
                        phase=TeardownDecisionPhase.SIZE,
                        outcome="swap_clamp_applied",
                        description=f"swap-back {clamp_token} sized to tracked inventory",
                        token=clamp_token,
                        reason="clamped_to_tracked_quantity",
                        intent_count=1,
                    )

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
                    # VIB-2928: a SWAP leg needs a real USD price for both
                    # tokens to size expected-out / slippage; the gate below
                    # hard-stops it on a missing/placeholder/zero price.
                    _is_swap = (
                        str(_intent_type_val).upper() in ("SWAP", "INTENTTYPE.SWAP") if _intent_type_val else False
                    )
                    _to_token = (
                        intent_with_slippage.get("to_token")
                        if _is_dict
                        else getattr(intent_with_slippage, "to_token", None)
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
                        # Earlier intents in this teardown sequence (e.g. the
                        # leverage staircase's REPAY) may have changed the
                        # wallet since this snapshot was built; evict the
                        # memoized balance so "all" resolves against the live
                        # post-intent value instead of over-resolving by
                        # exactly the amount the earlier intent consumed.
                        _invalidate = getattr(market, "invalidate_balance", None)
                        if callable(_invalidate):
                            try:
                                _invalidate(from_token)
                            except Exception:  # noqa: BLE001
                                logger.debug(
                                    "invalidate_balance(%s) failed; falling back to cached balance",
                                    from_token,
                                    exc_info=True,
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
                        # VIB-4587 / F5 — DX warning when sweeping a from-token
                        # the strategy never emitted accounting events for.
                        # Best-effort: never blocks the unwind.
                        if self.runner_helpers.has_sweep_warning:
                            try:
                                self.runner_helpers.warn_sweep_non_strategy_balance(  # type: ignore[misc]
                                    strategy,
                                    intent_with_slippage,
                                    from_token,
                                    bal.balance,
                                )
                            except Exception:  # noqa: BLE001
                                logger.debug("sweep-warning helper raised; ignored", exc_info=True)

                    # Apply real prices to compiler if available
                    original_oracle = getattr(self.compiler, "price_oracle", None)
                    original_placeholders = getattr(self.compiler, "_using_placeholders", True)
                    if price_oracle and hasattr(self.compiler, "update_prices"):
                        self.compiler.update_prices(price_oracle)

                    # Compile intent to ActionBundle
                    try:
                        # VIB-2928 HARD STOP: refuse to compile a teardown SWAP
                        # on a missing/placeholder/zero price. Swap adapters
                        # silently fall back to $1 for expected-out / slippage
                        # math, so a price gap would size the trade off a fake
                        # number. This is PER-LEG (it fails only this swap) so a
                        # different, priceable leg still reduces on-chain risk —
                        # blueprint 14's inverted failure semantics: never block
                        # the next risk-reducing intent.
                        if _is_swap:
                            _assert_prices = getattr(self.compiler, "assert_prices_available", None)
                            if not callable(_assert_prices):
                                # Fail closed: a SWAP that cannot be price-gated
                                # must not compile on a possibly-fake price.
                                raise ValueError(
                                    "compiler does not support the teardown SWAP price hard-stop "
                                    "(assert_prices_available) — refusing to compile a swap unguarded"
                                )
                            _assert_prices([from_token, _to_token])
                        compilation_result = self.compiler.compile(intent_with_slippage)
                    except ValueError as price_err:
                        logger.error(
                            "🛑 Teardown SWAP price HARD STOP (VIB-2928) for %s: %s",
                            getattr(strategy, "deployment_id", "?"),
                            price_err,
                        )
                        return ExecutionAttempt(
                            success=False,
                            slippage_used=slippage,
                            actual_slippage=Decimal("0"),
                            error=f"Price HARD STOP (VIB-2928): {price_err}",
                            retryable=False,
                        )
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
                        deployment_id=strategy.deployment_id,
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
                                strategy.deployment_id,
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
                                strategy.deployment_id,
                                exc,
                            )

                    # VIB-4482 — capture Uniswap V4 uncollected fees on the same
                    # pre-execute boundary so LP_CLOSE / LP_COLLECT_FEES teardown
                    # rows carry measured ``fees0/1`` lane-symmetric with the
                    # iteration lane's ``state.v4_lp_close_fees``. The read MUST
                    # happen before ``orchestrator.execute`` — a post-burn read
                    # returns zero liquidity. Returns ``None`` for non-V4-LP-close
                    # intents and never raises.
                    v4_lp_close_fees_for_intent: tuple[int, int] | None = None
                    if self.runner_helpers.has_v4_lp_close_fees:
                        try:
                            v4_lp_close_fees_for_intent = await self.runner_helpers.snapshot_intent_v4_lp_close_fees(  # type: ignore[misc]
                                strategy, intent_to_exec
                            )
                        except Exception as exc:  # noqa: BLE001 — best-effort
                            logger.debug(
                                "teardown V4 LP-close pre-fee snapshot failed for %s: %s",
                                strategy.deployment_id,
                                exc,
                            )

                    # VIB-5117 — on the SAME pre-execute boundary, capture the
                    # closing V4 position's native-leg PRINCIPAL. A native-ETH leg
                    # is withdrawn as raw ETH (no Transfer) so the burn receipt
                    # leaves ``amount{0,1}_collected = None``; the principal is
                    # derived from the pre-burn position state (post-burn read =
                    # zero liquidity). Threaded into the commit so the LP handler
                    # records the real native proceeds. Returns ``None`` for
                    # non-native-leg closes and never raises.
                    v4_lp_close_native_principal_for_intent: tuple[int | None, int | None] | None = None
                    if self.runner_helpers.has_v4_lp_close_native_principal:
                        try:
                            v4_lp_close_native_principal_for_intent = (
                                await self.runner_helpers.snapshot_intent_v4_lp_close_native_principal(  # type: ignore[misc]
                                    strategy, intent_to_exec
                                )
                            )
                        except Exception as exc:  # noqa: BLE001 — best-effort
                            logger.debug(
                                "teardown V4 LP-close native-principal snapshot failed for %s: %s",
                                strategy.deployment_id,
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
                                    strategy.deployment_id,
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
                                v4_lp_close_fees=v4_lp_close_fees_for_intent,
                                v4_lp_close_native_principal=v4_lp_close_native_principal_for_intent,
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
                        # VIB-4532 / VIB-4664 / VIB-4258: classify the revert so a
                        # deterministic failure (insufficient balance, contract-arg,
                        # ERC-721 not-approved) short-circuits and a transport/RPC
                        # blip retries at the same level — instead of every failure
                        # escalating slippage to the operator-approval gate.
                        revert_class, disposition = classify_teardown_failure(exec_result.error)
                        # VIB-5470 (subsumes VIB-5152): decode raw lending /
                        # Safe-Roles revert selectors (e.g. 0x6679996d dust-debt
                        # withdraw-all, 0xd27b44a9 ModuleTransactionFailed,
                        # 0xd0a9bf58 ConditionViolation) into an operator-clear
                        # explanation so the failure is self-diagnosing. No-op
                        # when no known selector is embedded.
                        annotated_error = annotate_teardown_error(exec_result.error)
                        logger.error(
                            "Intent %d/%d execution failed [%s -> %s]: %s",
                            intent_index + 1,
                            len(intents),
                            revert_class.value,
                            disposition.value,
                            annotated_error,
                        )
                        return ExecutionAttempt(
                            success=False,
                            slippage_used=slippage,
                            actual_slippage=Decimal("0"),
                            error=annotated_error,
                            retryable=disposition != Disposition.NON_RETRYABLE,
                            disposition=disposition.value,
                        )

                except Exception as e:
                    revert_class, disposition = classify_teardown_failure(str(e))
                    logger.exception(
                        "Exception during intent execution [%s -> %s]: %s",
                        revert_class.value,
                        disposition.value,
                        e,
                    )
                    return ExecutionAttempt(
                        success=False,
                        slippage_used=slippage,
                        actual_slippage=Decimal("0"),
                        error=str(e),
                        retryable=disposition != Disposition.NON_RETRYABLE,
                        disposition=disposition.value,
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
                deployment_id=strategy.deployment_id,
                is_auto_mode=is_auto_mode,
                intent_slippage=intent_slippage,
            )

            if exec_result.success:
                succeeded += 1
                _pending_indices.discard(i)  # done → advances the resume floor (VIB-5573)
                # VIB-5140: fold this intent's receipt block into the running
                # MAX across all successful closes (see _fold_max_receipt_block
                # for why MAX, not last-processed, is correct under
                # non-monotonic completion).
                last_receipt_block = _fold_max_receipt_block(last_receipt_block, exec_result)
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
                # VIB-5573 (WI-2): a vetted TRANSIENT revert (context-scoped by
                # intent_type + protocol + signature — e.g. MetaMorpho
                # withdraw-queue Panic 0x11) is retried on the time axis rather
                # than counted failed now: re-queue to the TAIL (retried after
                # every other close) up to a bounded number of attempts. A
                # paused_awaiting_approval is an approval pause, not a revert, so
                # it is never deferred here. On attempt exhaustion the intent
                # falls through to the normal failed path (loud).
                if exec_result.status != "paused_awaiting_approval" and attempts < _TRANSIENT_MAX_ATTEMPTS:
                    # The aggregate ExecutionResult has no ``error`` attribute —
                    # the raw revert text is on the last ExecutionAttempt (or the
                    # summarized ``message``). Prefer the attempt's raw error so
                    # the classifier's selector/panic regex sees the exact string.
                    _revert_text = None
                    if exec_result.attempts and exec_result.attempts[-1].error:
                        _revert_text = exec_result.attempts[-1].error
                    if not _revert_text:
                        _revert_text = exec_result.message
                    _it = _intent_field(intent, "intent_type")
                    _proto = _intent_field(intent, "protocol")
                    if (
                        classify_revert_transience(_revert_text, intent_type=_it, protocol=_proto)
                        is Transience.TRANSIENT
                    ):
                        work.append((i, intent, attempts + 1))
                        logger.warning(
                            "Teardown intent %d/%d reverted TRANSIENT (%s/%s): %s — deferring "
                            "retry %d/%d to end of queue (time-axis backoff).",
                            i + 1,
                            len(intents),
                            _proto,
                            _it,
                            _revert_text,
                            attempts + 1,
                            _TRANSIENT_MAX_ATTEMPTS,
                        )
                        continue

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
                        deployment_id=strategy.deployment_id,
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

            # Persist the resume FLOOR (lowest still-pending original index) so a
            # crash/restart resumes at the first unfinished intent — including a
            # deferred transient retry that has not completed yet. For a purely
            # sequential teardown this equals ``start_from_index + succeeded``;
            # with deferrals it does NOT (completion is non-contiguous), which is
            # exactly the resume-skip strand this fixes (VIB-5573).
            _floor = _resume_floor()
            teardown_state.completed_intents = _floor
            teardown_state.current_intent_index = _floor
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
                strategy.deployment_id,
                succeeded - skipped,
                skipped,
                failed,
            )

        return TeardownResult(
            success=failed == 0,
            deployment_id=strategy.deployment_id,
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
            last_receipt_block=last_receipt_block,
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
            deployment_id=strategy.deployment_id,
            mode=mode,
            status=TeardownStatus.CANCEL_WINDOW,
            total_intents=len(intents),
            completed_intents=0,
            current_intent_index=0,
            started_at=now,
            updated_at=now,
            pending_intents_json=json.dumps([_serialize_intent_for_state(i) for i in intents]),
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
        close_receipt_block: int | None = None,
    ) -> bool:
        """Verify positions are closed on-chain — returns the all-closed bool.

        Thin back-compat wrapper over :meth:`_verify_closure_detailed`
        (VIB-5085). Existing callers and the post-condition test suite depend
        on the bare ``bool`` contract; the position-level breakdown lives on
        the detailed variant. ``close_receipt_block`` (VIB-5140) is forwarded
        so the on-chain reads pin to the close-tx block.
        """
        verification = await self._verify_closure_detailed(
            strategy,
            expected_positions=expected_positions,
            pre_execution_positions=pre_execution_positions,
            close_receipt_block=close_receipt_block,
        )
        return verification.all_closed

    async def _verify_closure_detailed(
        self,
        strategy: IntentStrategy,
        expected_positions: Any = None,
        pre_execution_positions: Any = None,
        close_receipt_block: int | None = None,
    ) -> ClosureVerification:
        """Verify that positions are actually closed on-chain (VIB-5085).

        Returns a :class:`ClosureVerification` carrying ``all_closed`` plus the
        position-level counts (``positions_total`` / ``positions_closed``) so
        lifecycle counters report *positions* closed, not *intents* landed.
        ``_verify_closure`` wraps this and returns only ``all_closed``.

        VIB-5140: ``close_receipt_block`` is the block of the last successful
        close-tx receipt (from ``TeardownResult.last_receipt_block``). It is
        forwarded to each post-condition hook so on-chain state reads pin to
        the exact block the close landed at — a read replica that trails the
        writer by a block then cannot return PRE-close state and falsely
        report the position still open (the false-negative teardown verify
        that drove STRATEGY_ERROR + double-close). ``None`` falls back to the
        legacy ``"latest"`` read.

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
            # VIB-2932 / VIB-5472: track how many positions actually had an
            # on-chain post-condition prove closure. When this equals
            # ``positions_total`` the closure is fully chain-confirmed
            # (CHAIN_VERIFIED); when some positions had no registered hook the
            # closure is reported but only counted closed-by-execution
            # (UNVERIFIED) — the count must not masquerade as chain-confirmed.
            positions_with_hook = 0
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
                        block=close_receipt_block,
                    )
                except Exception as exc:  # noqa: BLE001 — fail-safe
                    # VIB-5573 (Empty ≠ Zero): a hook that RAISES could not MEASURE
                    # on-chain state — it is a read fault, not a measured residual.
                    # Treating it as a residual (pre-VIB-5573 behaviour) fabricates
                    # a FAILED → hosted shutdown + entry latch on a transient
                    # gateway/RPC blip. Treat as UNMEASURED → lowers to UNVERIFIED,
                    # exactly like a no-hook position (skip: not counted, not failed).
                    # exc_info=True preserves the traceback — a hook that raises is
                    # usually a programming error (NameError/AttributeError) we must
                    # be able to debug, not just a one-line message (Gemini).
                    logger.warning(
                        "Teardown post-condition for %s raised — treating as UNMEASURED (UNVERIFIED, not FAILED): %s",
                        protocol,
                        exc,
                        exc_info=True,
                    )
                    continue

                # VIB-5573: an explicit UNMEASURED result (gateway/RPC fault after
                # the hook's own bounded read-retry, missing client, unsupported
                # vault interface) is honest "don't-know". It must NOT be counted as
                # chain-verified NOR as a residual — lower to UNVERIFIED by skipping,
                # same as a no-hook position. Only a *measured* residual is FAILED.
                if getattr(check, "unmeasured", False):
                    logger.warning(
                        "Teardown verification UNMEASURED for %s position %s: %s — counting UNVERIFIED (not FAILED).",
                        protocol,
                        getattr(position, "position_id", ""),
                        check.error,
                    )
                    continue

                positions_with_hook += 1
                if not check.closed:
                    failed_results.append(check)

            positions_total = len(snapshot_positions)
            if failed_results:
                for check in failed_results:
                    logger.error(
                        "Post-teardown on-chain verification FAILED for %s position %s: residual=%s error=%s",
                        check.protocol,
                        check.position_id,
                        check.residual,
                        check.error,
                    )
                # VIB-5085: positions_closed = total − positions whose on-chain
                # post-condition reported residual ("known not closed"). A
                # position with NO registered post-condition (only uniswap_v3 /
                # traderjoe_v2 register one today; lending — Aave / Morpho /
                # Compound — has none) is counted closed-by-execution: its
                # closing intents ran and nothing contradicts closure. This is
                # required, not incidental — the field-report case (Aave
                # looping) has no hooks, so excluding no-hook positions would
                # collapse positions_closed back to the intent count and
                # re-introduce the very bug this fixes.
                #
                # Caveat (Codex Finding 2): on a PARTIAL failure a no-hook
                # position with undetected residual is optimistically counted
                # closed. That is acceptable because the per-position count is
                # NOT the risk authority here — the teardown is flagged FAILED
                # with "manual check required" + recovery options, which is the
                # loud signal operators act on. ``has_position_breakdown`` means
                # "a position-level count is available", NOT "every position was
                # on-chain verified".
                return ClosureVerification(
                    all_closed=False,
                    positions_total=positions_total,
                    positions_closed=positions_total - len(failed_results),
                    has_position_breakdown=True,
                    verification_status=VerificationStatus.FAILED,
                )

            # All registered post-conditions passed. We still log the
            # discover-path summary so the existing audit trail is intact.
            # VIB-2932 / VIB-5472: a closure is CHAIN_VERIFIED only when EVERY
            # pre-execution position had a post-condition that proved it closed.
            # If any position lacked a registered hook it was counted
            # closed-by-execution — report UNVERIFIED so the optimistic count is
            # visible, never presented as chain-confirmed.
            fully_chain_verified = positions_with_hook == positions_total
            status = VerificationStatus.CHAIN_VERIFIED if fully_chain_verified else VerificationStatus.UNVERIFIED
            ids = [getattr(p, "position_id", "") for p in snapshot_positions]
            if fully_chain_verified:
                logger.info(
                    "Teardown verification: %d position(s) passed on-chain post-condition checks: %s",
                    positions_total,
                    ids,
                )
            else:
                logger.warning(
                    "Teardown verification UNVERIFIED: %d of %d position(s) had an on-chain "
                    "post-condition; the remainder are counted closed-by-execution (no chain "
                    "proof). positions=%s",
                    positions_with_hook,
                    positions_total,
                    ids,
                )
            return ClosureVerification(
                all_closed=True,
                positions_total=positions_total,
                positions_closed=positions_total,
                has_position_breakdown=True,
                verification_status=status,
            )

        # Last-resort: legacy in-memory state read. Used when neither a
        # pre-execution snapshot nor an expected-positions list reaches us
        # (paper / unit-test paths). This is the path the original
        # implementation used end-to-end; it still works as a "did the
        # strategy at least clear its own state?" smoke test.
        #
        # No pre-execution snapshot reached us, so there is no trustworthy
        # ``positions_total`` to report — leave the counts at 0 and surface
        # only the all-closed signal (VIB-5085). Callers fall back to intent
        # counts when ``has_position_breakdown`` is not asserted.
        #
        # VIB-2932 / VIB-5472: this path never reads the chain, so a clear
        # in-memory state is UNVERIFIED (closed-by-execution, not proven), and a
        # residual is FAILED. Either way the closure was not chain-confirmed.
        positions = strategy.get_open_positions()
        all_closed = len(positions.positions) == 0
        return ClosureVerification(
            all_closed=all_closed,
            verification_status=(VerificationStatus.UNVERIFIED if all_closed else VerificationStatus.FAILED),
        )

    async def _pre_teardown_reconciliation(self, strategy: Any, positions: Any, market: Any) -> Any | None:
        """PRE-teardown Plan-A reconciliation for the CLI ``execute`` lane (TD-15 AC-(b)).

        Reads each KNOWN position's live chain state BEFORE any closing intent
        fires, so :meth:`verify_closure_against_chain` can lower CHAIN_VERIFIED for
        a stale / never-existed enumeration (a position the chain already reports
        closed / unconfirmable pre-teardown). The runner lane stashes this on
        ``runner._teardown_reconciliation`` (TD-08); the CLI lane has no runner, so
        it computes the same CHECK inline. CHECK-only — closes nothing, emits no
        intent, and NEVER faults the teardown lane (a fault returns ``None``, which
        simply skips the AC-(b) downgrade).
        """
        try:
            return await reconcile_known_positions_against_chain(
                summary=positions,
                gateway_client=self._teardown_gateway_client(),
                market=market,
                network=str(getattr(strategy, "_gateway_network", "") or ""),
            )
        except Exception:  # noqa: BLE001 — the CHECK must never fault the teardown lane
            logger.exception(
                "TD-15 PRE-teardown reconciliation raised for %s — proceeding without "
                "the PRE report (no AC-(b) downgrade)",
                getattr(strategy, "deployment_id", "") or "",
            )
            return None

    async def verify_closure_against_chain(
        self,
        strategy: IntentStrategy,
        *,
        verification: ClosureVerification,
        pre_execution_positions: Any,
        market: Any | None,
        pre_teardown_reconciliation: Any | None = None,
    ) -> ClosureVerification:
        """Fail-closed on-chain POST-teardown verification (TD-15 / VIB-5473).

        Runs AFTER every closing intent has fired — teardown's inverted failure
        semantics (blueprint 14 §Teardown) mean risk reduction happens FIRST and
        this check only then fails loudly; it never blocks a risk-reducing intent.
        It composes three independent signals into the final
        :class:`ClosureVerification` the lanes act on, and **only ever lowers
        confidence or fails** — it never upgrades a status:

        1. ``verification`` — the per-protocol post-condition result from
           :meth:`_verify_closure_detailed` (TD-14; covers the primitives with a
           registered ``TeardownPostCondition`` — uniswap_v3 / traderjoe_v2). When
           it already reports ``all_closed=False`` the teardown has failed and its
           residual error is the actionable one, so this method returns it
           unchanged (no redundant chain read).
        2. A FRESH POST-teardown Plan-A reconciliation
           (:func:`reconcile_known_positions_against_chain`) over the SAME
           pre-execution KNOWN set. This adds the lending chain read the
           post-condition hooks lack (Aave / Morpho / Compound), so a stranded
           collateral or debt leg the hook-less UNVERIFIED path would have waved
           through as success is caught. A position the chain STILL reports OPEN
           flips the result to FAILED (AC-(a)); a POST-teardown position the chain
           cannot re-read is a deliberate **no-op** (a burned LP NFT reads back
           "not found" — the SUCCESS signal), so it never downgrades CHAIN_VERIFIED.
           The never-existed / stale-enumeration downgrade is owned by the
           PRE-teardown report below (AC-(b)), a different signal.
        3. ``pre_teardown_reconciliation`` — the report stashed/computed BEFORE the
           closing intents fired (runner lane: ``runner._teardown_reconciliation``
           via TD-08; CLI ``execute`` lane: computed inline by
           :meth:`_pre_teardown_reconciliation`). A position
           the WARM ledger believed open but the chain reported closed /
           unconfirmable pre-teardown means the enumeration was stale or the
           position never existed; certifying CHAIN_VERIFIED off it would be a
           false success on a never-existed position (AC-(b)), so it lowers
           CHAIN_VERIFIED → UNVERIFIED.

        Never raises — a reconciliation fault degrades to the incoming
        ``verification`` (the CHECK must never fault the teardown lane).
        """
        # Already failing: the post-condition residual error is the actionable
        # one; do not spend a second chain read or risk masking it.
        if not verification.all_closed:
            return verification

        deployment_id = getattr(strategy, "deployment_id", "") or ""
        try:
            gateway_client = self._teardown_gateway_client()
            network = str(getattr(strategy, "_gateway_network", "") or "")
            # VIB-5523 (Bug B): the POST-teardown reconciliation MUST read LIVE
            # on-chain state. ``market`` was built at teardown START and memoizes
            # ``position_health`` per (protocol, market_id, …); reusing it here
            # serves the PRE-WITHDRAW health and falsely reports a zeroed lending
            # position CONFIRMED_OPEN (every stranded leg shows the identical
            # pre-teardown value). Read fresh — one extra gateway round-trip at
            # teardown end is acceptable for correctness.
            post_market = self._fresh_post_execution_market(strategy, market)
            post_report = await reconcile_known_positions_against_chain(
                summary=pre_execution_positions,
                gateway_client=gateway_client,
                market=post_market,
                network=network,
            )
        except Exception:  # noqa: BLE001 — the CHECK must never fault the teardown lane
            logger.exception(
                "TD-15 post-teardown reconciliation raised for %s — keeping the TD-14 "
                "post-condition verdict unchanged (fail-safe)",
                deployment_id,
            )
            return verification

        # Fold the POST-teardown reconciliation into the status, then the
        # PRE-teardown report. Neither can raise confidence; the POST report only
        # ever FAILS (on residual open), the PRE report only ever lowers
        # CHAIN_VERIFIED (never-existed / stale enumeration — AC-(b)).
        status = post_report.apply_post_teardown_to_verification_status(verification.verification_status)
        if pre_teardown_reconciliation is not None:
            status = pre_teardown_reconciliation.apply_to_verification_status(status)

        # AC-(a): a KNOWN position the chain STILL reports OPEN after every closing
        # intent fired is residual on-chain risk → fail closed. This is the lane
        # that catches the hook-less lending strand the post-condition path counts
        # closed-by-execution. An UNVERIFIABLE re-read (e.g. a burned LP NFT) is
        # NOT residual — only CONFIRMED_OPEN is.
        if post_report.has_confirmed_open:
            residual = post_report.confirmed
            for entry in residual:
                logger.error(
                    "🛑 TD-15 fail-closed: %s %s (%s) on %s is STILL OPEN on-chain after teardown — %s. "
                    "Flipping teardown result to FAILED (residual on-chain risk).",
                    entry.protocol,
                    entry.position_type,
                    entry.position_id,
                    entry.chain,
                    entry.detail,
                )
            positions_total = max(verification.positions_total, post_report.checked_count, len(residual))
            return replace(
                verification,
                all_closed=False,
                positions_total=positions_total,
                positions_closed=max(positions_total - len(residual), 0),
                has_position_breakdown=True,
                verification_status=VerificationStatus.FAILED,  # == status (post report failed)
            )

        if status is not verification.verification_status:
            logger.warning(
                "TD-15 post-teardown verification: lowering %s closure confidence %s → %s (pre-reconcile not-clean=%s)",
                deployment_id,
                verification.verification_status,
                status,
                None
                if pre_teardown_reconciliation is None
                else (pre_teardown_reconciliation.has_divergence or pre_teardown_reconciliation.has_unverifiable),
            )
            return replace(verification, verification_status=status)
        return verification

    @staticmethod
    def _fresh_post_execution_market(strategy: Any, fallback: Any | None) -> Any | None:
        """Return a FRESH market snapshot for the POST-teardown chain re-read (VIB-5523).

        The pre-execution snapshot memoizes ``position_health`` AND wallet
        ``balance``, so reusing it to verify post-closure state returns stale
        (pre-unwind) values and falsely reports a zeroed position still open.
        Build a fresh snapshot from the strategy so the read reflects live
        on-chain state. When a fresh snapshot cannot be built, fall back to
        EVICTING the stale memos on the reused snapshot — BOTH the health memo
        and the wallet-balance memos — so the post read still re-queries the
        chain rather than serving any pre-execution value. Never raises —
        verification must never fault the teardown lane.
        """
        creator = getattr(strategy, "create_market_snapshot", None)
        if callable(creator):
            try:
                fresh = creator()
                if fresh is not None:
                    return fresh
            except Exception:  # noqa: BLE001 — fall back to cache eviction below
                logger.warning(
                    "TD-15: could not build a fresh post-execution market snapshot for %s — "
                    "evicting the stale health cache on the reused snapshot instead",
                    getattr(strategy, "deployment_id", ""),
                    exc_info=True,
                )
        if fallback is not None:
            invalidate = getattr(fallback, "invalidate_position_health", None)
            if callable(invalidate):
                try:
                    invalidate()
                except Exception:  # noqa: BLE001 — best-effort; degrade to cached read
                    logger.debug("TD-15: invalidate_position_health failed; using cached health", exc_info=True)
            # The reused snapshot also memoizes wallet balances; evict them too so
            # a post-execution balance read reflects live (post-unwind) state
            # rather than the pre-execution memo (VIB-5523).
            invalidate_balances = getattr(fallback, "invalidate_balances", None)
            if callable(invalidate_balances):
                try:
                    invalidate_balances()
                except Exception:  # noqa: BLE001 — best-effort; degrade to cached read
                    logger.debug("TD-15: invalidate_balances failed; using cached balances", exc_info=True)
        return fallback

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
        deployment_id: str,
        mode: str,
        started_at: datetime,
    ) -> TeardownResult:
        """Create a result for empty teardown (no positions)."""
        return TeardownResult(
            success=True,
            deployment_id=deployment_id,
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
        deployment_id: str,
        mode: str,
        started_at: datetime,
    ) -> TeardownResult:
        """Create a result for cancelled teardown."""
        return TeardownResult(
            success=False,
            deployment_id=deployment_id,
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
        deployment_id: str,
        mode: str,
        started_at: datetime,
        error: str,
        verification_status: VerificationStatus = VerificationStatus.NOT_RUN,
        positions_total: int = 0,
        positions_closed: int = 0,
        has_position_breakdown: bool = False,
    ) -> TeardownResult:
        """Create a result for failed teardown.

        ``verification_status`` defaults to ``NOT_RUN`` (no closure verification
        ran for an early failure); the completeness gate (TD-11) passes
        ``FAILED`` so a coverage failure is recorded as a confidence-FAILED
        closure, not merely "not run".

        ``positions_total`` / ``positions_closed`` / ``has_position_breakdown``
        let the no-intents coverage-failure path stamp an accurate position
        breakdown (e.g. ``0/N`` closed) so the lifecycle surface does not read a
        misleading ``0/0`` while the error names N stranded positions (VIB-5469).
        """
        return TeardownResult(
            success=False,
            deployment_id=deployment_id,
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
            verification_status=verification_status,
            positions_total=positions_total,
            positions_closed=positions_closed,
            has_position_breakdown=has_position_breakdown,
        )
