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
    calculate_max_acceptable_loss,
)
from almanak.framework.teardown.oracle_warmup import warm_and_validate_oracle
from almanak.framework.teardown.safety_guard import SafetyGuard
from almanak.framework.teardown.slippage_manager import (
    EscalatingSlippageManager,
    ExecutionAttempt,
)
from almanak.framework.teardown.swap_clamp import SwapClampDecision, decide_swap_clamp

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

            if not intents:
                logger.info(f"No intents to execute for {strategy.deployment_id}")
                return self._empty_result(strategy.deployment_id, mode, started_at)

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
                    )
                    verify_error_msg = f"Post-teardown verification error: {verify_err}. Manual check required."
                else:
                    verify_error_msg = "Post-teardown verification failed: positions still open. Manual check required."

                # VIB-5085: stamp the position-level counts onto the result so
                # the CLI lifecycle writer reports positions, not intents.
                # ``has_position_breakdown`` is only True when the verifier had a real
                # pre-execution snapshot — on the in-memory fallback (empty
                # snapshot) it stays False so the writer falls back to the intent
                # count instead of persisting a misleading ``positions_closed=0``.
                result = replace(
                    result,
                    positions_total=verification.positions_total,
                    positions_closed=verification.positions_closed,
                    has_position_breakdown=verification.has_position_breakdown,
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

            guarded = sanitize_lending_teardown_intents(intents, market)
            for reason in guarded.dropped:
                logger.info("Teardown resume lending guard dropped intent — %s", reason)
            intents = guarded.intents
            state.pending_intents_json = json.dumps([_serialize_intent_for_state(i) for i in intents])
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
                # ABSOLUTE count (offset + this call's successes): ``succeeded``
                # is call-relative, and this method runs with a non-zero
                # ``start_from_index`` both on resume and for the VIB-5011
                # consolidation extension — persisting the relative count would
                # rewind ``completed_intents`` below already-finished work
                # (pr-auditor finding).
                teardown_state.completed_intents = start_from_index + succeeded
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
                        teardown_state.completed_intents = start_from_index + succeeded
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
                        logger.error(
                            "Intent %d/%d execution failed [%s -> %s]: %s",
                            intent_index + 1,
                            len(intents),
                            revert_class.value,
                            disposition.value,
                            exec_result.error,
                        )
                        return ExecutionAttempt(
                            success=False,
                            slippage_used=slippage,
                            actual_slippage=Decimal("0"),
                            error=exec_result.error,
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

            # Update completed count and persist teardown progress so that
            # a crash/restart resumes from the correct index. ABSOLUTE count
            # (offset + this call's successes) — see the skip-path comment
            # above: a relative count would rewind already-finished work on
            # resume and during the VIB-5011 consolidation extension.
            teardown_state.completed_intents = start_from_index + succeeded
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
                )

            # All registered post-conditions passed. We still log the
            # discover-path summary so the existing audit trail is intact.
            ids = [getattr(p, "position_id", "") for p in snapshot_positions]
            logger.info(
                "Teardown verification: %d position(s) passed on-chain post-condition checks: %s",
                positions_total,
                ids,
            )
            return ClosureVerification(
                all_closed=True,
                positions_total=positions_total,
                positions_closed=positions_total,
                has_position_breakdown=True,
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
        positions = strategy.get_open_positions()
        return ClosureVerification(all_closed=len(positions.positions) == 0)

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
    ) -> TeardownResult:
        """Create a result for failed teardown."""
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
        )
