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
from almanak.framework.teardown.completeness import (
    check_intent_coverage,
    resolve_consolidation_noop_target,
)
from almanak.framework.teardown.config import TeardownConfig
from almanak.framework.teardown.decision_log import TeardownDecisionPhase, log_teardown_decision
from almanak.framework.teardown.error_taxonomy import Disposition, classify_teardown_failure
from almanak.framework.teardown.models import (
    CLOSURE_UNKNOWN_ERROR,
    LARGE_POSITION_WARNING_THRESHOLD_USD,
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
from almanak.framework.teardown.single_close_guard import collapse_duplicate_perp_closes
from almanak.framework.teardown.slippage_manager import (
    EscalatingSlippageManager,
    ExecutionAttempt,
)
from almanak.framework.teardown.swap_clamp import SwapClampDecision, decide_swap_clamp

from .lp_clamp import LpClampUnresolved

logger = logging.getLogger(__name__)

_ACCEPTED_ASYNC_SUBMISSION_KEY = "_teardown_async_submission_accepted"
_ACCEPTED_ASYNC_ORDER_KEYS_KEY = "_teardown_async_submission_order_keys"
_ACCEPTED_ASYNC_LEDGER_ID_KEY = "_teardown_async_submission_ledger_id"
ASYNC_SETTLEMENT_PENDING_ERROR = "Accepted async submission remains unsettled; teardown is resumable"

# Deferred retries never delay untried risk-reducing closes and remain bounded.
# Per-attempt backoff is 4s, 8s, then 12s: at most about 24s per intent.
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

    def generate_teardown_intents(self, mode: TeardownMode, market: Any = None) -> list[Any]:
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
    # Earlier intents may have changed the wallet balance; evict the stale snapshot value.
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
    if hasattr(intent, "serialize"):
        # serialize() includes the dispatch discriminator that model_dump() omits.
        return intent.serialize()
    if hasattr(intent, "model_dump"):
        return intent.model_dump(mode="json")
    if isinstance(intent, dict):
        return intent
    return str(intent)


def _mark_persisted_async_submission_accepted(
    state: TeardownState,
    intent_index: int,
    *,
    order_keys: tuple[str, ...],
    ledger_entry_id: str | None,
) -> bool:
    """Durably mark one accepted async intent so resume can never submit it again."""
    try:
        plan = json.loads(state.pending_intents_json) if state.pending_intents_json else []
        if not isinstance(plan, list) or not 0 <= intent_index < len(plan):
            return False
        serialized = plan[intent_index]
        if not isinstance(serialized, dict):
            return False
        plan[intent_index] = {
            **serialized,
            _ACCEPTED_ASYNC_SUBMISSION_KEY: True,
            _ACCEPTED_ASYNC_ORDER_KEYS_KEY: list(order_keys),
            _ACCEPTED_ASYNC_LEDGER_ID_KEY: ledger_entry_id,
        }
        state.pending_intents_json = json.dumps(plan)
        return True
    except (TypeError, ValueError):
        return False


def _is_persisted_async_submission_accepted(intent: Any) -> bool:
    return isinstance(intent, dict) and intent.get(_ACCEPTED_ASYNC_SUBMISSION_KEY) is True


def has_accepted_async_submission(state: TeardownState | None) -> bool:
    """Return whether a persisted plan owns a durable accepted async order."""
    if state is None or not state.pending_intents_json:
        return False
    try:
        plan = json.loads(state.pending_intents_json)
    except (TypeError, ValueError):
        return False
    return isinstance(plan, list) and any(_is_persisted_async_submission_accepted(item) for item in plan)


def accepted_async_order_keys(state: TeardownState | None) -> frozenset[str]:
    """Return exact durable order keys owned by accepted plan markers."""
    if state is None or not state.pending_intents_json:
        return frozenset()
    try:
        plan = json.loads(state.pending_intents_json)
    except (TypeError, ValueError):
        return frozenset()
    if not isinstance(plan, list):
        return frozenset()
    return frozenset(
        key
        for item in plan
        if _is_persisted_async_submission_accepted(item)
        for key in _accepted_async_submission_metadata(item)[1]
    )


def _accepted_async_submission_metadata(intent: Any) -> tuple[str | None, tuple[str, ...]]:
    if not _is_persisted_async_submission_accepted(intent):
        return None, ()
    ledger_entry_id = str(intent.get(_ACCEPTED_ASYNC_LEDGER_ID_KEY) or "") or None
    raw_keys = intent.get(_ACCEPTED_ASYNC_ORDER_KEYS_KEY)
    order_keys = tuple(str(key).lower() for key in raw_keys or () if str(key)) if isinstance(raw_keys, list) else ()
    return ledger_entry_id, order_keys


def _clear_persisted_async_submission(state: TeardownState, intent_index: int) -> dict[str, Any] | None:
    """Clear a terminal old submission before dispatching its replacement close."""
    try:
        plan = json.loads(state.pending_intents_json) if state.pending_intents_json else []
        if not isinstance(plan, list) or not 0 <= intent_index < len(plan):
            return None
        serialized = plan[intent_index]
        if not isinstance(serialized, dict):
            return None
        clean = {
            key: value
            for key, value in serialized.items()
            if key
            not in {
                _ACCEPTED_ASYNC_SUBMISSION_KEY,
                _ACCEPTED_ASYNC_ORDER_KEYS_KEY,
                _ACCEPTED_ASYNC_LEDGER_ID_KEY,
            }
        }
        plan[intent_index] = clean
        state.pending_intents_json = json.dumps(plan)
        return clean
    except (TypeError, ValueError):
        return None


def _deserialize_persisted_intent(payload: dict[str, Any]) -> Any:
    """Restore a JSON plan item while tolerating the legacy ``intent_type`` key."""
    from almanak.framework.intents import Intent

    clean = dict(payload)
    serialized_kind = clean.pop("intent_type", None)
    if "type" not in clean and serialized_kind is not None:
        clean["type"] = serialized_kind
    return Intent.deserialize(clean)


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


def _teardown_wallet_for_chain(strategy: Any, chain: str) -> str:
    """Effective execution address for ``chain`` (VIB-6043).

    Prefers the strategy's per-chain wallet map (``get_wallet_for_chain``) so a
    multi-chain teardown leg stamps the wallet that actually holds the funds on
    that chain; falls back to ``wallet_address`` for single-chain strategies and
    for any strategy object that does not expose the accessor.

    Only a NON-EMPTY STRING is accepted as a resolved wallet (PR #3531). The
    accessor's contract is ``str | None``, but the previous ``if resolved:
    return str(resolved)`` coerced *any* truthy object into a wallet — so a
    registry (or a test double) handing back a non-string produced a plausible
    but meaningless address like ``"<MagicMock id=…>"``, which then reached
    calldata and made every read on that leg fail as "unmeasured". Falling back
    to the primary wallet is the honest answer: a value we cannot interpret is
    not a per-chain wallet.

    Deliberately a type check and not an address-shape check — this helper is
    chain-agnostic and serves non-EVM legs (Solana) whose addresses are not
    EVM-shaped. Address-shape validation belongs in the EVM-only reader that
    encodes the calldata, e.g. the TOKEN post-condition's own guard.
    """
    getter = getattr(strategy, "get_wallet_for_chain", None)
    if callable(getter) and chain:
        try:
            resolved = getter(chain)
        except Exception:  # noqa: BLE001 — never let wallet resolution break teardown
            resolved = None
        if isinstance(resolved, str) and resolved.strip():
            return resolved.strip()
    return str(getattr(strategy, "wallet_address", "") or "")


def _resolve_and_run_post_condition(
    position: Any,
    *,
    wallet_address: str,
    gateway_client: Any | None,
    rpc_url: str | None,
    block: int | str | None,
) -> Any | None:
    """Run the closure authority that applies to ``position`` (VIB-6285).

    Resolution order, and the whole reason this is not a one-line registry
    lookup: the TD-14 registry is keyed by PROTOCOL slug alone, which cannot
    express "this primitive has a closure authority regardless of venue".

    1. The protocol-registered hook, when one exists.
    2. Otherwise the position-TYPE default (``position_type_post_condition``).
    3. If the protocol hook answers ``not_applicable`` — structurally out of
       scope for this position, e.g. an NFT-shaped LP hook handed a
       ``PositionType.TOKEN`` row because the slug is shared — hand off to the
       type default rather than leaving the position unmeasured.

    Returns the ``ClosureCheckResult``, or ``None`` when no authority applies
    (the caller's "no hook" path). Deliberately does NOT catch: a raising hook
    is a read fault the caller converts to UNMEASURED, and swallowing it here
    would hide the traceback VIB-5573 kept on purpose.
    """
    from almanak.framework.teardown.post_conditions import (
        get_teardown_post_condition,
        position_type_post_condition,
    )

    protocol = (getattr(position, "protocol", "") or "").lower()
    hook = get_teardown_post_condition(protocol)
    type_default = position_type_post_condition(position)
    if hook is None:
        hook = type_default
    if hook is None:
        return None

    def _run(chosen: Any) -> Any:
        return chosen(
            position=position,
            wallet_address=wallet_address,
            gateway_client=gateway_client,
            rpc_url=rpc_url,
            block=block,
        )

    check = _run(hook)
    if getattr(check, "not_applicable", False) and type_default is not None and type_default is not hook:
        check = _run(type_default)
    return check


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
        internal_mode = TeardownMode.from_cli_string(mode)

        positions = strategy.get_open_positions()

        try:
            intents = strategy.generate_teardown_intents(internal_mode, market=market)
        except TypeError as exc:
            if "market" in str(exc):
                intents = strategy.generate_teardown_intents(internal_mode)
            else:
                raise

        max_loss_pct = calculate_max_acceptable_loss(positions.total_value_usd)
        max_loss_usd = positions.total_value_usd * max_loss_pct
        protected_min = positions.total_value_usd - max_loss_usd

        min_return, max_return = self.safety_guard.calculate_estimated_return_range(
            positions.total_value_usd, internal_mode
        )

        duration = self._estimate_duration(internal_mode, intents)

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
        internal_mode = TeardownMode.from_cli_string(mode)
        started_at = datetime.now(UTC)
        if teardown_id is None:
            teardown_id = f"td_{uuid.uuid4().hex[:12]}"

        try:
            logger.info(f"Starting teardown {teardown_id} for {strategy.deployment_id}")
            await strategy.pause()

            if self.alert_manager:
                await self.alert_manager.send_teardown_started(strategy.deployment_id, mode)

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
                        intents = strategy.generate_teardown_intents(internal_mode)
                    else:
                        raise

            # Collapse duplicate perp dispatches, but check coverage against the original plan;
            # intent-to-intent and intent-to-position identity are not equivalent.
            single_close = collapse_duplicate_perp_closes(intents)
            intents = single_close.dispatch

            # Evaluate coverage now, but fold failure after execution so risk-reducing intents still run.
            completeness = check_intent_coverage(
                positions,
                # Coverage requires the pre-collapse plan.
                single_close.for_coverage,
                consolidation_target_token=self._consolidation_noop_target(strategy, intents),
                wallet_for_chain=lambda c: _teardown_wallet_for_chain(strategy, c) or None,
            )

            if not intents:
                if completeness.complete:
                    logger.info(f"No intents to execute for {strategy.deployment_id}")
                    return self._empty_result(strategy.deployment_id, mode, started_at)
                logger.error("🛑 %s", completeness.error_message())
                return self._failed_result(
                    strategy.deployment_id,
                    mode,
                    started_at,
                    error=completeness.error_message(),
                    verification_status=VerificationStatus.FAILED,
                    # Preserve the known position denominator when no intent can run.
                    positions_total=completeness.total_enforceable,
                    positions_closed=0,
                    has_position_breakdown=True,
                )

            validation = self.safety_guard.validate_teardown_request(positions, internal_mode)
            if not validation.all_passed:
                logger.error(f"Safety validation failed: {validation.blocked_reason}")
                return self._failed_result(
                    strategy.deployment_id,
                    mode,
                    started_at,
                    error=validation.blocked_reason or "Safety validation failed",
                )

            teardown_state = await self._persist_state(teardown_id, strategy, internal_mode, intents)

            cancel_result = await self.cancel_window.run_cancel_window(
                teardown_id=teardown_id,
                on_check_cancelled=on_cancel_check,
                is_auto_mode=is_auto_mode,
            )

            if cancel_result.was_cancelled:
                logger.info(f"Teardown {teardown_id} cancelled during window")
                return self._cancelled_result(strategy.deployment_id, mode, started_at)

            teardown_state.status = TeardownStatus.EXECUTING
            if self.state_manager:
                await self.state_manager.save_teardown_state(teardown_state)

            price_oracle = _warm_oracle_risk_first(market, intents, fail_loud=True)

            pre_teardown_reconciliation = await self._pre_teardown_reconciliation(strategy, positions, market)

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

            # Verify only successful execution so an earlier actionable failure is not masked.
            if result.success:
                try:
                    # Verify the pre-execution set because callbacks may already have cleared local state.
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

                verification = await self.verify_closure_against_chain(
                    strategy,
                    verification=verification,
                    pre_execution_positions=positions,
                    market=market,
                    pre_teardown_reconciliation=pre_teardown_reconciliation,
                )

                # Coverage failure is folded after chain verification and remains the final verdict.
                if not completeness.complete:
                    uncovered_count = len(completeness.uncovered)
                    # Include uncovered positions in the denominator and never count them closed.
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

                result = replace(
                    result,
                    positions_total=verification.positions_total,
                    positions_closed=verification.positions_closed,
                    has_position_breakdown=verification.has_position_breakdown,
                    verification_status=verification.verification_status,
                )

                log_teardown_decision(
                    deployment_id=strategy.deployment_id,
                    teardown_id=teardown_id,
                    phase=TeardownDecisionPhase.VERIFY,
                    # Unknown closure is distinct from both verified and measured-open failure.
                    outcome=(
                        "verify_failed"
                        if not verification.all_closed
                        else ("verify_unmeasured" if verification.closure_unknown else "verified")
                    ),
                    description=(
                        f"closure verification: {verification.positions_closed}/"
                        f"{verification.positions_total} closed "
                        f"({verification.verification_status.value}"
                        f"{', closure_unknown' if verification.closure_unknown else ''})"
                    ),
                    position_count=verification.positions_total,
                    positions_closed=verification.positions_closed,
                    verification_status=verification.verification_status.value,
                )

                # Check measured residual first; unknown closure must use a distinct message.
                if not verification.all_closed:
                    logger.warning(
                        f"Post-teardown verification: {strategy.deployment_id} still reports "
                        f"open positions (or verification errored). Marking teardown as incomplete."
                    )
                elif verification.closure_unknown:
                    logger.warning(
                        "Post-teardown verification: %s closure is UNPROVEN (no measured "
                        "on-chain evidence either way). Refusing to certify success — this is "
                        "NOT a claim that positions are open.",
                        strategy.deployment_id,
                    )
                    verify_error_msg = CLOSURE_UNKNOWN_ERROR

                if not verification.all_closed or verification.closure_unknown:
                    result = replace(
                        result,
                        success=False,
                        error=verify_error_msg,
                        recovery_options=["Verify positions on-chain", "Re-run teardown"],
                    )
                    # Failed or unproven verification must not leave persisted state COMPLETED.
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

            # Consolidation runs only after verified closure, never races a partial unwind,
            # and reports failure without undoing successful risk reduction.
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

            if self.alert_manager:
                await self.alert_manager.send_teardown_complete(result)

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

        if state.mode == TeardownMode.HARD:
            if not state.is_in_cancel_window:
                raise ValueError("Cancel window has expired for emergency teardown")

        if state.status == TeardownStatus.EXECUTING and state.completed_intents > 0:
            logger.info(f"Pausing teardown {state.teardown_id} (intents in progress)")
            state.status = TeardownStatus.PAUSED
            await self.state_manager.save_teardown_state(state)
            return True

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
        is_auto_mode: bool = False,
        market: Any = None,
        accepted_async_recovery_intents: list[Any] | None = None,
    ) -> TeardownResult | None:
        """Resume an interrupted teardown.

        Called on system startup to detect and resume in-progress teardowns.
        Includes staleness check - re-generates intents if too old.

        Args:
            deployment_id: ID of the strategy
            strategy: The strategy instance
            on_approval_needed: Callback for approval requests
            on_progress: Callback for progress updates
            is_auto_mode: Whether this is auto-protect mode

        Returns:
            TeardownResult if resumed and completed, None if nothing to resume
        """
        if self.state_manager is None:
            return None

        state = await self.state_manager.get_teardown_state(deployment_id)

        if not state or not state.is_resumable:
            return None

        logger.info(f"Resuming teardown {state.teardown_id}")

        # Capture progress before stale-plan regeneration resets counters; an oracle fault
        # after any prior close must not block the remaining risk-reducing intents.
        had_prior_progress = state.completed_intents > 0 or state.current_intent_index > 0

        age_seconds = (datetime.now(UTC) - state.updated_at).total_seconds()
        previous_plan = json.loads(state.pending_intents_json) if state.pending_intents_json else []
        if age_seconds > self.config.staleness_threshold_seconds and any(
            _is_persisted_async_submission_accepted(item) for item in previous_plan
        ):
            # Keep exact order-key correlation until settlement is measured terminal.
            logger.warning(
                "State is stale (%.1fs old) but contains an accepted async submission; "
                "retaining the correlated plan until terminal settlement",
                age_seconds,
            )
            age_seconds = 0
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
            # Fresh plans still require measured-zero filtering and HF-safe lending ordering.
            from .lending_unwind_guard import sanitize_lending_teardown_intents

            guarded = sanitize_lending_teardown_intents(intents, market, mode=state.mode)
            for reason in guarded.dropped:
                logger.info("Teardown resume lending guard dropped intent — %s", reason)
            for synth in guarded.synthesized_positions:
                logger.info(
                    "Teardown resume lending guard synthesised HF-safe unwind staircase for %s (VIB-4466)", synth
                )
            intents = guarded.intents
            # Collapse duplicate full perp closes before persisting the resumable plan.
            intents = collapse_duplicate_perp_closes(intents).dispatch
            state.pending_intents_json = json.dumps([_serialize_intent_for_state(intent) for intent in intents])
            # Sanitization may expand or shrink the plan, so replace the stale denominator.
            state.total_intents = len(intents)
            state.current_intent_index = 0
            # A regenerated plan has no completed prefix; old progress must not skip new closes.
            state.completed_intents = 0
            # Regenerated plans remain clamp-scoped.
            state.consolidation_consent = False
            state.config_json = encode_consolidation_consent(state.config_json, False)

        intents_data = json.loads(state.pending_intents_json) if state.pending_intents_json else []

        if not intents_data:
            logger.info(f"No pending intents for {state.teardown_id}")
            return None

        # After any prior on-chain progress, oracle warming is best-effort so a
        # partially unwound plan can continue reducing risk.
        if had_prior_progress:
            logger.info(
                "Resuming teardown %s past prior progress (current completed=%d, intent index=%d) — "
                "warming oracle best-effort; the fail-loud pre-flight gate is skipped "
                "to preserve teardown's inverted-failure semantics.",
                state.teardown_id,
                state.completed_intents,
                state.current_intent_index,
            )
            price_oracle = _warm_oracle_risk_first(market, intents_data, fail_loud=False)
        else:
            price_oracle = _warm_oracle_risk_first(market, intents_data, fail_loud=True)

        positions = strategy.get_open_positions()

        # current_intent_index marks "about to run"; completed_intents marks the
        # next unfinished intent. Taking both prevents replay of landed closes.
        resume_from_index = max(state.current_intent_index, state.completed_intents)
        accepted_indices = [
            index for index, item in enumerate(intents_data) if _is_persisted_async_submission_accepted(item)
        ]
        if accepted_indices:
            # Exact settlement markers outrank counters; terminal proof decides completion.
            resume_from_index = min(resume_from_index, min(accepted_indices))

        return await self._execute_intents(
            teardown_id=state.teardown_id,
            strategy=strategy,
            intents=intents_data,
            positions=positions,
            mode=state.mode,
            teardown_state=state,
            on_approval_needed=on_approval_needed,
            on_progress=on_progress,
            start_from_index=resume_from_index,
            is_auto_mode=is_auto_mode,
            price_oracle=price_oracle,
            market=market,
            accepted_async_recovery_intents=accepted_async_recovery_intents,
        )

    def _consolidation_noop_target(
        self,
        strategy: IntentStrategy | None,
        intents: list[Any] | None,
    ) -> str | None:
        """The token Phase-2 consolidation swaps residual holdings INTO, threaded
        into the TD-11 completeness gate (VIB-5494 Item 1).

        Uses the SAME target expression AND the same chain derivation
        ``run_token_consolidation`` resolves with, so the gate credits exactly
        the token consolidation will actually target. Gated to the
        ``TARGET_TOKEN`` policy by :func:`resolve_consolidation_noop_target` so a
        held STAKE/TOKEN position already denominated in that target is credited a
        no-op close instead of false-failing the teardown. Returns ``None`` for
        entry-token / keep-outputs policies (no single "already done" token).

        Passing the chain is load-bearing (VIB-5727): the configured target is
        normally the "no preference" sentinel, and resolving it without a chain
        yields the legacy ``USDC`` — which on a USDC-less chain credits a token
        the wallet cannot hold while denying credit to the one it does (robinhood
        holds USDG), re-arming the recurring failed-teardown loop this credit
        exists to prevent.

        Both parameters are REQUIRED — deliberately, even though ``None`` is an
        accepted *value* for each. They were briefly optional, and the runner
        lane (``_teardown_helpers.execute_and_verify``) called this with no
        arguments: the chain silently resolved to ``None`` → legacy ``USDC``,
        reintroducing the exact bug above on the one chain this ticket is about.
        A defaulted parameter turns "the caller forgot" into a wrong answer;
        a required one turns it into a TypeError at the call site. Callers with
        genuinely no strategy/intents must pass ``None`` explicitly and thereby
        state that they know the chain is unknown.
        """
        cfg = self.config.token_consolidation
        chain = _teardown_chain(list(intents or [])) or (getattr(strategy, "chain", None) or None)
        return resolve_consolidation_noop_target(
            self.config.asset_policy,
            (getattr(cfg, "target_token", None) or self.config.target_token),
            chain=chain,
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
            resolve_chain_swap_protocol,
            resolve_chain_target_token,
            resolve_consolidation_targets,
        )

        # Preserve an already-resolved target when a later consolidation step fails.
        target_token: str | None = None

        try:
            cfg = self.config.token_consolidation
            closing = list(closing_intents or [])

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

            # Target resolution is chain-dependent.
            chain = _teardown_chain(closing) or (getattr(strategy, "chain", None) or None)

            requested_token = cfg.target_token or self.config.target_token
            target_token, chain_target_warnings = resolve_chain_target_token(requested_token, chain)

            targets, target_warnings = resolve_consolidation_targets(
                self.config.asset_policy,
                target_token,
                strategy,
                accounting_events=accounting_events,
            )

            # Reuse a same-chain strategy DEX; never route through another chain's intent.
            same_chain_protocols = [
                _intent_field(i, "protocol")
                for i in closing
                if (ic := _intent_field(i, "chain")) is None or (chain is not None and ic.lower() == chain.lower())
            ]
            swap_protocol = resolve_chain_swap_protocol(same_chain_protocols)
            if swap_protocol is not None:
                logger.info(
                    "Token consolidation routing swaps through strategy DEX %s for %s",
                    swap_protocol,
                    strategy.deployment_id,
                )

            plan = plan_consolidation(
                market=market,
                chain=chain,
                asset_policy=self.config.asset_policy,
                target_token=target_token,
                token_consolidation_cfg=cfg,
                token_universe=token_universe,
                mode=mode,
                targets=targets,
                swap_protocol=swap_protocol,
            )
            warnings = [*chain_target_warnings, *target_warnings, *plan.warnings]
            if plan.intents:
                # Token selection is strategy-scoped, but amount="all" sweeps the wallet.
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
                    planned=0, succeeded=0, failed=0, warnings=warnings, decisions=plan.decisions, target=target_token
                )

            logger.info(
                "Token consolidation for %s: %d swap(s) planned → %s",
                strategy.deployment_id,
                len(plan.intents),
                target_token,
            )

            # Persist consolidation after completed closes so crash recovery resumes at its offset.
            try:
                existing = (
                    json.loads(teardown_state.pending_intents_json) if teardown_state.pending_intents_json else []
                )
                if not isinstance(existing, list):
                    existing = []
            except (TypeError, ValueError):
                existing = []
            start_from_index = len(existing)
            serialized = [_serialize_intent_for_state(i) for i in plan.intents]
            teardown_state.pending_intents_json = json.dumps([*existing, *serialized])
            teardown_state.total_intents = start_from_index + len(plan.intents)
            teardown_state.status = TeardownStatus.EXECUTING
            # Request provenance never authorizes consuming untracked wallet funds.
            teardown_state.consolidation_consent = False
            teardown_state.config_json = encode_consolidation_consent(teardown_state.config_json, False)
            teardown_state.updated_at = datetime.now(UTC)
            if self.state_manager:
                await self.state_manager.save_teardown_state(teardown_state)

            # Consolidation follows closure, so incomplete pricing must not undo risk reduction.
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
            )

            # Resume treats safe skips as complete, but reporting must not count them as transactions.
            skipped = min(result.intents_skipped, len(plan.intents))
            succeeded = min(max(result.intents_succeeded - skipped, 0), len(plan.intents))
            failed = max(len(plan.intents) - succeeded - skipped, 0)
            if skipped:
                warnings.append(
                    f"{skipped} planned consolidation swap(s) skipped by execution safety guards; "
                    "no transaction was executed for those intents"
                )
            if failed:
                warnings.append(
                    f"{failed} consolidation swap(s) failed ({result.error or 'see logs'}) — "
                    "wallet holds residual non-target tokens; teardown closure itself succeeded"
                )
            return ConsolidationOutcome(
                planned=len(plan.intents),
                succeeded=succeeded,
                skipped=skipped,
                failed=failed,
                warnings=warnings,
                decisions=plan.decisions,
                accounting_degraded_count=result.accounting_degraded_count,
                target=target_token,
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
                target=target_token,
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
        accepted_async_recovery_intents: list[Any] | None = None,
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
            consolidation_consent: Retired compatibility argument. Ignored;
                teardown swaps are always clamp-scoped (VIB-5938).

        Returns:
            TeardownResult with execution outcome
        """
        del consolidation_consent
        started_at = teardown_state.started_at
        mode_str = "graceful" if mode == TeardownMode.SOFT else "emergency"

        succeeded = 0
        failed = 0
        skipped = 0
        total_costs = Decimal("0")
        final_balances: dict[str, Decimal] = {}

        # Pin verification to the latest successful receipt block, not a lagging "latest" read.
        last_receipt_block: int | None = None

        # All per-intent and bracket accounting writes share one teardown cycle identity.
        teardown_cycle_id = f"teardown-{teardown_id}"

        accounting_degraded_records: list[Any] = []
        # An accepted but non-terminal async order must remain resumable to prevent a duplicate close.
        unsettled_async_submission = False

        # Deferred retries trail every first attempt, preserving risk-priority order.
        work: deque[tuple[int, Any, int]] = deque(
            (idx, it, 0) for idx, it in enumerate(intents[start_from_index:], start=start_from_index)
        )

        # Deferrals make completion non-contiguous; persist the lowest pending index as the resume floor.
        _pending_indices: set[int] = {idx for idx, _, _ in work}

        def _resume_floor() -> int:
            return min(_pending_indices) if _pending_indices else len(intents)

        while work:
            i, intent, attempts = work.popleft()

            if _is_persisted_async_submission_accepted(intent):
                # A durable accepted order is dispatch-complete but remains pending until terminal proof.
                ledger_entry_id, order_keys = _accepted_async_submission_metadata(intent)
                settlement_status: str | None = None
                if self.runner_helpers.check_intent_settlement is not None:
                    settlement_status = await self.runner_helpers.check_intent_settlement(
                        strategy,
                        ledger_entry_id=ledger_entry_id,
                        order_keys=order_keys,
                        cycle_id=teardown_cycle_id,
                        chain=str(intent.get("chain") or getattr(strategy, "chain", "") or ""),
                        wallet_address=_teardown_wallet_for_chain(
                            strategy,
                            str(intent.get("chain") or getattr(strategy, "chain", "") or ""),
                        ),
                    )
                if settlement_status == "executed":
                    succeeded += 1
                    _pending_indices.discard(i)
                    logger.info(
                        "Accepted async teardown intent %d/%d is terminally executed and booked; completing resume",
                        i + 1,
                        len(intents),
                    )
                    try:
                        callback_payload = {
                            key: value
                            for key, value in intent.items()
                            if key
                            not in {
                                _ACCEPTED_ASYNC_SUBMISSION_KEY,
                                _ACCEPTED_ASYNC_ORDER_KEYS_KEY,
                                _ACCEPTED_ASYNC_LEDGER_ID_KEY,
                            }
                        }
                        callback_intent = _deserialize_persisted_intent(callback_payload)
                        if hasattr(strategy, "on_intent_executed"):
                            callback_result = strategy.on_intent_executed(callback_intent, True, None)
                            if asyncio.iscoroutine(callback_result):
                                await callback_result
                        if hasattr(strategy, "save_state"):
                            strategy.save_state()
                        if hasattr(strategy, "flush_pending_saves"):
                            await strategy.flush_pending_saves()
                    except Exception:  # noqa: BLE001 — settlement proof remains authoritative
                        logger.exception("Failed to persist strategy state after recovered async settlement")
                    continue

                if settlement_status == "terminal_failed":
                    # Persist marker removal before dispatching a replacement.
                    prior_pending_intents_json = teardown_state.pending_intents_json
                    replacement_intent = _clear_persisted_async_submission(teardown_state, i)
                    if replacement_intent is None:
                        unsettled_async_submission = True
                        failed += 1
                        logger.error(
                            "Terminally failed async teardown intent %d/%d could not clear its persisted marker; "
                            "refusing replacement submission",
                            i + 1,
                            len(intents),
                        )
                        continue
                    teardown_state.updated_at = datetime.now(UTC)
                    if self.state_manager:
                        try:
                            await self.state_manager.save_teardown_state(teardown_state)
                        except Exception:
                            # Keep memory aligned with the backend's conservative no-resubmit marker.
                            teardown_state.pending_intents_json = prior_pending_intents_json
                            unsettled_async_submission = True
                            failed += 1
                            logger.exception(
                                "Terminally failed async teardown intent %d/%d could not persist marker removal; "
                                "refusing replacement submission",
                                i + 1,
                                len(intents),
                            )
                            continue
                    try:
                        intent = _deserialize_persisted_intent(replacement_intent)
                    except (TypeError, ValueError):
                        failed += 1
                        logger.exception(
                            "Terminally failed async teardown intent %d/%d could not deserialize its "
                            "persisted replacement; recording intent failure",
                            i + 1,
                            len(intents),
                        )
                        continue
                    logger.warning(
                        "Accepted async teardown intent %d/%d is terminally failed; dispatching one replacement close",
                        i + 1,
                        len(intents),
                    )
                else:
                    unsettled_async_submission = True
                    exact_cancel = next(
                        (
                            candidate
                            for candidate in accepted_async_recovery_intents or []
                            if str(_intent_field(candidate, "intent_type") or "").upper() == "PERP_CANCEL_ORDER"
                            and str(_intent_field(candidate, "order_key") or "").lower() in set(order_keys)
                        ),
                        None,
                    )
                    if exact_cancel is not None:
                        # Cancel only the exact durable key; keep its marker until terminal failure is measured.
                        intent = exact_cancel
                        logger.warning(
                            "Accepted async teardown intent %d/%d remains pending and is now cancellable; "
                            "dispatching exact-order recovery",
                            i + 1,
                            len(intents),
                        )
                    else:
                        failed += 1
                        logger.error(
                            "Refusing to resubmit accepted async teardown intent %d/%d; "
                            "terminal settlement is unproven",
                            i + 1,
                            len(intents),
                        )
                        floor = _resume_floor()
                        teardown_state.completed_intents = floor
                        teardown_state.current_intent_index = floor
                        teardown_state.updated_at = datetime.now(UTC)
                        if self.state_manager:
                            await self.state_manager.save_teardown_state(teardown_state)
                        continue

            # Queue-level backoff cannot delay first-attempt risk-reducing closes.
            if attempts > 0:
                await asyncio.sleep(_TRANSIENT_BACKOFF_S * attempts)

            progress_pct = int((i / len(intents)) * 100)
            if on_progress:
                await on_progress(progress_pct, f"Executing step {i + 1}/{len(intents)}")

            # Persist the floor, not the out-of-order queue index.
            _floor = _resume_floor()
            teardown_state.current_intent_index = _floor
            teardown_state.completed_intents = _floor
            teardown_state.updated_at = datetime.now(UTC)
            if self.state_manager:
                await self.state_manager.save_teardown_state(teardown_state)

            # A zero-balance sweep is complete without execution; slippage cannot change that.
            skip_reason = _zero_balance_swap_skip_reason(intent, market)
            if skip_reason:
                logger.info(f"Teardown intent {i + 1}/{len(intents)}: skipping — {skip_reason}")
                succeeded += 1
                skipped += 1
                if on_progress:
                    await on_progress(progress_pct, f"Skipped step {i + 1}/{len(intents)}: {skip_reason}")
                # Persist the skip as completed before advancing the resume floor.
                _pending_indices.discard(i)
                _floor = _resume_floor()
                teardown_state.completed_intents = _floor
                teardown_state.current_intent_index = _floor
                teardown_state.updated_at = datetime.now(UTC)
                if self.state_manager:
                    await self.state_manager.save_teardown_state(teardown_state)
                continue

            # Clamp swap-backs to tracked inventory so shared-wallet funds are never swept.
            # Resolve or skip outside the slippage closure to prevent an unclamped retry.
            clamp_token = _clampable_swap_from_token(intent, market)
            if clamp_token:
                live_balance = _read_live_wallet_balance(market, clamp_token)
                if live_balance is None:
                    # An unmeasured clamp fails closed because consolidation is not risk-reducing.
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
                        chain=(intent.get("chain") if isinstance(intent, dict) else getattr(intent, "chain", None)),
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
                    # A refused consolidation sweep is a completed no-op, not a teardown failure.
                    succeeded += 1
                    skipped += 1
                    if on_progress:
                        await on_progress(progress_pct, f"Skipped step {i + 1}/{len(intents)}: clamp {decision.reason}")
                    _pending_indices.discard(i)
                    _floor = _resume_floor()
                    teardown_state.completed_intents = _floor
                    teardown_state.current_intent_index = _floor
                    teardown_state.updated_at = datetime.now(UTC)
                    if self.state_manager:
                        await self.state_manager.save_teardown_state(teardown_state)
                    continue
                # Resolve before escalation so every retry uses the same clamped quantity.
                intent = _set_intent_resolved_amount(intent, decision.amount)  # type: ignore[arg-type]
                logger.info(
                    "🛑 ALM-2766 clamped %s swap-back to tracked qty %s (live wallet %s).",
                    clamp_token,
                    decision.amount,
                    live_balance,
                )
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

            # Post-submit failures must never re-enter the slippage ladder.
            submission_landed = False

            async def execute_at_slippage(  # noqa: C901
                intent_to_exec: Any, slippage: Decimal, *, intent_index: int = i
            ) -> ExecutionAttempt:
                """Execute a single intent at given slippage.

                Compiles the intent to an ActionBundle and executes it via the
                orchestrator. Returns the execution result.
                """
                nonlocal submission_landed, unsettled_async_submission
                if submission_landed:
                    return ExecutionAttempt(
                        success=False,
                        slippage_used=slippage,
                        actual_slippage=Decimal("0"),
                        error="On-chain submission already landed; refusing duplicate teardown execution",
                        retryable=False,
                        disposition=Disposition.NON_RETRYABLE.value,
                    )

                logger.info(f"Executing intent {intent_index + 1}/{len(intents)} at {slippage:.1%} slippage")

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
                    # Intents are frozen, so escalation must clone rather than mutate them.
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

                    # Bound fungible LP closes to this deployment; refuse an unbounded close
                    # without delaying other risk-reducing intents.
                    intent_with_slippage, clamp_refusal = await self._attach_lp_outstanding(
                        strategy, intent_with_slippage
                    )
                    if clamp_refusal is not None:
                        return ExecutionAttempt(
                            success=False,
                            slippage_used=slippage,
                            actual_slippage=Decimal("0"),
                            error=clamp_refusal,
                            retryable=False,
                            disposition=Disposition.NON_RETRYABLE.value,
                        )

                    _is_dict = isinstance(intent_with_slippage, dict)
                    amount_value = (
                        intent_with_slippage.get("amount")
                        if _is_dict
                        else getattr(intent_with_slippage, "amount", None)
                    )
                    from_token = (
                        intent_with_slippage.get("from_token") or intent_with_slippage.get("token")
                        if _is_dict
                        else getattr(intent_with_slippage, "from_token", None)
                        or getattr(intent_with_slippage, "token", None)
                    )
                    # Protocol-held withdraw and repay amounts use compiler-side resolution.
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
                    _is_swap = (
                        str(_intent_type_val).upper() in ("SWAP", "INTENTTYPE.SWAP") if _intent_type_val else False
                    )
                    _to_token = (
                        intent_with_slippage.get("to_token")
                        if _is_dict
                        else getattr(intent_with_slippage, "to_token", None)
                    )
                    if amount_value == "all" and not _withdraw_all and not _is_withdraw and not _is_repay:
                        if not from_token or market is None:
                            return ExecutionAttempt(
                                success=False,
                                slippage_used=slippage,
                                actual_slippage=Decimal("0"),
                                error="Cannot resolve amount='all': missing from_token or market context",
                            )
                        # Earlier intents may have changed the wallet; resolve "all" from live state.
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

                    original_oracle = getattr(self.compiler, "price_oracle", None)
                    original_placeholders = getattr(self.compiler, "_using_placeholders", True)
                    if price_oracle and hasattr(self.compiler, "update_prices"):
                        self.compiler.update_prices(price_oracle)

                    try:
                        # Refuse fake-price swap sizing per leg without blocking later closes.
                        if _is_swap:
                            _assert_prices = getattr(self.compiler, "assert_prices_available", None)
                            if not callable(_assert_prices):
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

                    from almanak.framework.execution.orchestrator import ExecutionContext

                    intent_chain = getattr(intent_to_exec, "chain", None) or strategy.chain
                    context = ExecutionContext(
                        deployment_id=strategy.deployment_id,
                        intent_id=f"teardown_{teardown_id}_{intent_index}",
                        chain=intent_chain,
                        intent_description=self._describe_intent(intent_to_exec),
                        # Receipt ownership must use the intent chain's wallet, not the signer EOA.
                        wallet_address=_teardown_wallet_for_chain(strategy, intent_chain),
                    )

                    # Snapshot each accounting boundary immediately before its transaction.
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

                    # V4 fees must be measured before a burn zeroes the position liquidity.
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

                    # Native V4 principal has no Transfer event and must also be captured pre-burn.
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

                    exec_result = await self.orchestrator.execute(
                        compilation_result.action_bundle,
                        context,
                    )

                    if exec_result.success:
                        submission_landed = True
                        # Async create-order submissions need durable identity before settlement waits.
                        settlement_error: str | None = None
                        async_submission_accepted = False
                        accepted_order_keys: tuple[str, ...] = ()
                        accepted_marker_persisted = False
                        accepted_marker_save_error: Exception | None = None
                        durable_async_correlation = False
                        if self.runner_helpers.has_async_settlement:
                            preparation = self.runner_helpers.prepare_intent_settlement(  # type: ignore[misc]
                                strategy,
                                intent_to_exec,
                                exec_result,
                                context,
                                bundle_metadata=getattr(compilation_result.action_bundle, "metadata", None) or None,
                            )
                            async_submission_accepted = preparation.applicable
                            settlement_error = preparation.error
                            if async_submission_accepted:
                                accepted_order_keys = tuple(
                                    str(getattr(order, "order_id", "") or getattr(order, "order_key", "") or "").lower()
                                    for order in preparation.orders
                                    if str(getattr(order, "order_id", "") or getattr(order, "order_key", "") or "")
                                )

                        actual_slippage = slippage * Decimal("0.5")  # Protocol-independent estimate
                        tx_hash = (
                            exec_result.transaction_results[0].tx_hash if exec_result.transaction_results else "unknown"
                        )
                        if settlement_error is None:
                            logger.info(
                                f"Intent {intent_index + 1}/{len(intents)} executed successfully. "
                                f"TX: {tx_hash}, Gas used: {exec_result.total_gas_used}"
                            )

                        # Pair the pre-intent snapshot with confirmed post-execution balances.
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

                        # Accounting degradation is recorded but cannot block the next risk-reducing intent.
                        commit_outcome = None
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

                        if async_submission_accepted:
                            # Never persist an async marker without its reconstructible ledger identity.
                            ledger_entry_id = (
                                str(commit_outcome.ledger_entry_id)
                                if commit_outcome and commit_outcome.ledger_entry_id
                                else None
                            )
                            if not accepted_order_keys and ledger_entry_id:
                                recover_keys = self.runner_helpers.recover_accepted_order_keys
                                if recover_keys is not None:
                                    accepted_order_keys = await recover_keys(ledger_entry_id)
                            durable_async_correlation = bool(ledger_entry_id and accepted_order_keys)
                            marked = (
                                _mark_persisted_async_submission_accepted(
                                    teardown_state,
                                    intent_index,
                                    order_keys=accepted_order_keys,
                                    ledger_entry_id=ledger_entry_id,
                                )
                                if durable_async_correlation
                                else False
                            )
                            if not marked:
                                logger.error(
                                    "Accepted async teardown intent %d/%d could not be marked in the persisted plan; "
                                    "the contiguous resume floor remains the no-resubmit backstop",
                                    intent_index + 1,
                                    len(intents),
                                )
                            accepted_floor = _resume_floor()
                            teardown_state.completed_intents = accepted_floor
                            teardown_state.current_intent_index = accepted_floor
                            teardown_state.updated_at = datetime.now(UTC)
                            if marked and self.state_manager:
                                try:
                                    await self.state_manager.save_teardown_state(teardown_state)
                                    accepted_marker_persisted = True
                                    accepted_marker_save_error = None
                                except Exception as exc:  # noqa: BLE001 — submission already landed; never retry it
                                    accepted_marker_save_error = exc
                                    logger.exception(
                                        "Accepted async teardown marker persistence failed after Phase-1 commit"
                                    )
                            elif marked:
                                accepted_marker_persisted = True
                                accepted_marker_save_error = None
                            if not accepted_marker_persisted:
                                settlement_error = (
                                    "Accepted async submission could not be durably correlated for crash-safe recovery; "
                                    f"refusing resubmission: {accepted_marker_save_error or 'missing Phase-1 ledger/order key'}"
                                )

                        if async_submission_accepted and settlement_error is None:
                            settlement_error = await self.runner_helpers.await_intent_settlement(  # type: ignore[misc]
                                strategy,
                                intent_to_exec,
                                exec_result,
                                context,
                                bundle_metadata=getattr(compilation_result.action_bundle, "metadata", None) or None,
                                preparation=preparation,
                            )
                            if settlement_error is None:
                                phase2_degraded = await self.runner_helpers.reconcile_intent_settlement(  # type: ignore[misc]
                                    strategy,
                                    exec_result,
                                    context,
                                    teardown_cycle_id,
                                )
                                if phase2_degraded:
                                    accounting_degraded_records.extend(phase2_degraded)
                                    logger.error(
                                        "Teardown intent %d/%d terminal settlement accounting degraded — %s",
                                        intent_index + 1,
                                        len(intents),
                                        "; ".join(
                                            str(getattr(record, "error", "") or "unknown") for record in phase2_degraded
                                        ),
                                    )
                                    settlement_error = (
                                        "Terminal async settlement was observed but Phase-2 booking remains degraded; "
                                        "keeping the correlated teardown resumable"
                                    )

                        if settlement_error is not None:
                            # Accepted orders fail without retry; the exact order remains reconcilable.
                            logger.error(
                                "Intent %d/%d was accepted but did not settle terminally; refusing resubmission: %s",
                                intent_index + 1,
                                len(intents),
                                settlement_error,
                            )
                            if async_submission_accepted and (accepted_marker_persisted or durable_async_correlation):
                                unsettled_async_submission = True
                            return ExecutionAttempt(
                                success=False,
                                slippage_used=slippage,
                                actual_slippage=Decimal("0"),
                                error=settlement_error,
                                retryable=False,
                                disposition=Disposition.NON_RETRYABLE.value,
                            )

                        return ExecutionAttempt(
                            success=True,
                            slippage_used=slippage,
                            actual_slippage=actual_slippage,
                        )
                    else:
                        # Known hashes with incomplete receipts require reconciliation, never replay.
                        from almanak.framework.execution.reconciliation import (
                            failed_submission_requires_reconciliation,
                            reconciliation_required_error,
                        )

                        if failed_submission_requires_reconciliation(exec_result):
                            terminal_error = reconciliation_required_error(exec_result)
                            logger.error(
                                "Intent %d/%d requires receipt reconciliation; refusing teardown replay: %s",
                                intent_index + 1,
                                len(intents),
                                terminal_error,
                            )
                            return ExecutionAttempt(
                                success=False,
                                slippage_used=slippage,
                                actual_slippage=Decimal("0"),
                                error=terminal_error,
                                retryable=False,
                                disposition=Disposition.NON_RETRYABLE.value,
                            )
                        revert_class, disposition = classify_teardown_failure(exec_result.error)
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
                    if submission_landed:
                        logger.exception(
                            "Post-submit teardown processing failed; refusing duplicate execution: %s",
                            e,
                        )
                        return ExecutionAttempt(
                            success=False,
                            slippage_used=slippage,
                            actual_slippage=Decimal("0"),
                            error=f"On-chain submission landed but post-submit processing failed: {e}",
                            retryable=False,
                            disposition=Disposition.NON_RETRYABLE.value,
                        )
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

            # Strategy slippage is a floor for thin-liquidity exits.
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
                _pending_indices.discard(i)
                # Completion may be non-monotonic, so retain the maximum receipt block.
                last_receipt_block = _fold_max_receipt_block(last_receipt_block, exec_result)
                actual_slippage = exec_result.final_slippage
                intent_value = positions.total_value_usd / len(intents)  # Approximation
                total_costs += intent_value * actual_slippage

                # Persist strategy-side effects so a redeploy cannot retry completed work.
                try:
                    # Clear framework trackers before invoking the strategy callback.
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
                # Only vetted transient reverts are deferred; approvals and exhausted retries fail normally.
                if exec_result.status != "paused_awaiting_approval" and attempts < _TRANSIENT_MAX_ATTEMPTS:
                    # Classification needs the raw attempt error when available.
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
                    teardown_state.status = TeardownStatus.PAUSED
                    if self.state_manager:
                        await self.state_manager.save_teardown_state(teardown_state)

                    if self.alert_manager and exec_result.approval_request:
                        await self.alert_manager.send_approval_needed(exec_result.approval_request)

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
                        intents_skipped=skipped,
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

            # Persist the first unfinished index, including deferred retries.
            _floor = _resume_floor()
            teardown_state.completed_intents = _floor
            teardown_state.current_intent_index = _floor
            teardown_state.updated_at = datetime.now(UTC)
            if self.state_manager:
                await self.state_manager.save_teardown_state(teardown_state)

        finished_at = datetime.now(UTC)
        completed_at = None if unsettled_async_submission else finished_at
        teardown_state.status = TeardownStatus.EXECUTING if unsettled_async_submission else TeardownStatus.COMPLETED
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
            success=failed == 0 and not unsettled_async_submission,
            deployment_id=strategy.deployment_id,
            mode=mode_str,
            started_at=started_at,
            completed_at=completed_at,
            duration_seconds=(finished_at - started_at).total_seconds(),
            intents_total=len(intents),
            intents_succeeded=succeeded,
            intents_failed=failed,
            intents_skipped=skipped,
            starting_value_usd=positions.total_value_usd,
            final_value_usd=final_value,
            total_costs_usd=total_costs,
            final_balances=final_balances,
            error=(
                ASYNC_SETTLEMENT_PENDING_ERROR
                if unsettled_async_submission
                else None
                if failed == 0
                else f"{failed} intents failed"
            ),
            async_settlement_pending=unsettled_async_submission,
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
            cancel_window_until=now,
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
        snapshot = pre_execution_positions
        if snapshot is None or not getattr(snapshot, "positions", None):
            snapshot = expected_positions

        snapshot_positions = list(getattr(snapshot, "positions", []) or [])

        if snapshot_positions:
            from almanak.framework.teardown.post_conditions import ClosureCheckResult

            gateway_client = self._teardown_gateway_client()
            rpc_url = self._teardown_rpc_url()
            wallet_address = self._teardown_wallet_address(strategy)

            failed_results: list[ClosureCheckResult] = []
            # Only positions measured by an applicable hook contribute chain proof.
            positions_with_hook = 0
            # Full identities prevent closure proof leaking across protocols or chains.
            hook_proven_keys: list[tuple[str, str, str]] = []
            for position in snapshot_positions:
                protocol = (getattr(position, "protocol", "") or "").lower()
                # A wrong-chain wallet can fabricate zero-balance closure evidence.
                position_wallet = (
                    _teardown_wallet_for_chain(strategy, str(getattr(position, "chain", "") or "")) or wallet_address
                )
                try:
                    check = _resolve_and_run_post_condition(
                        position,
                        wallet_address=position_wallet,
                        gateway_client=gateway_client,
                        rpc_url=rpc_url,
                        block=close_receipt_block,
                    )
                except Exception as exc:  # noqa: BLE001 — fail-safe
                    # Read faults are unmeasured, not evidence of residual exposure.
                    logger.warning(
                        "Teardown post-condition for %s raised — treating as UNMEASURED (UNVERIFIED, not FAILED): %s",
                        protocol,
                        exc,
                        exc_info=True,
                    )
                    continue

                if check is None:
                    logger.debug(
                        "Teardown verification: no on-chain post-condition "
                        "registered for protocol %r (position_id=%s); "
                        "falling back to in-memory state.",
                        protocol,
                        getattr(position, "position_id", ""),
                    )
                    continue

                # Unmeasured means neither verified closure nor measured residual.
                if getattr(check, "unmeasured", False):
                    logger.warning(
                        "Teardown verification UNMEASURED for %s position %s: %s — counting UNVERIFIED (not FAILED).",
                        protocol,
                        getattr(position, "position_id", ""),
                        check.error,
                    )
                    continue

                # Out-of-scope hooks contribute neither proof nor doubt.
                if getattr(check, "not_applicable", False):
                    logger.debug(
                        "Teardown verification NOT_APPLICABLE for %s position %s: %s — "
                        "contributes no closure evidence and no doubt.",
                        protocol,
                        getattr(position, "position_id", ""),
                        check.residual,
                    )
                    continue

                positions_with_hook += 1
                if not check.closed:
                    failed_results.append(check)
                else:
                    proven_id = str(getattr(position, "position_id", "") or "").strip()
                    # Empty identities cannot safely carry closure proof.
                    if proven_id:
                        hook_proven_keys.append(
                            (
                                protocol.strip(),
                                str(getattr(position, "chain", "") or "").strip().lower(),
                                proven_id.lower(),
                            )
                        )

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
                # No-hook positions remain closed-by-execution; the FAILED verdict carries risk authority.
                return ClosureVerification(
                    all_closed=False,
                    positions_total=positions_total,
                    positions_closed=positions_total - len(failed_results),
                    has_position_breakdown=True,
                    verification_status=VerificationStatus.FAILED,
                )

            # Every position needs applicable hook proof for CHAIN_VERIFIED.
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
                hook_proven_position_keys=tuple(hook_proven_keys),
            )

        # In-memory fallback cannot provide position counts or chain proof.
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
                wallet_address=self._teardown_wallet_address(strategy),
                wallet_for_chain=getattr(strategy, "get_wallet_for_chain", None),
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

        On top of those three it sets a fourth, orthogonal signal (VIB-6285 / W0.1):
        ``closure_unknown`` — True when NEITHER authority measured a single position
        closed while at least one position existed to prove. It never touches
        ``all_closed`` (it makes no claim that anything is open); it only refuses to
        certify success off zero evidence. See the block comment at the bottom of
        this method for the exact rule, why it is a deliberately weak ratchet stage,
        and the hole it knowingly leaves open.

        Never raises — a reconciliation fault degrades to the incoming
        ``verification`` (the CHECK must never fault the teardown lane).
        """
        # Preserve the first actionable residual verdict.
        if not verification.all_closed:
            return verification

        deployment_id = getattr(strategy, "deployment_id", "") or ""
        try:
            gateway_client = self._teardown_gateway_client()
            network = str(getattr(strategy, "_gateway_network", "") or "")
            # Post-teardown reconciliation must not reuse pre-withdraw cached state.
            post_market = self._fresh_post_execution_market(strategy, market)
            post_report = await reconcile_known_positions_against_chain(
                summary=pre_execution_positions,
                gateway_client=gateway_client,
                market=post_market,
                network=network,
                wallet_address=self._teardown_wallet_address(strategy),
                wallet_for_chain=getattr(strategy, "get_wallet_for_chain", None),
                phase="post",
            )
        except Exception:  # noqa: BLE001 — the CHECK must never fault the teardown lane
            logger.exception(
                "TD-15 post-teardown reconciliation raised for %s — keeping the TD-14 "
                "post-condition verdict unchanged (fail-safe)",
                deployment_id,
            )
            return verification

        # Position-scoped zero proof outranks an unattributable whole-account residual.
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        def _entry_key(entry: Any) -> tuple[str, str, str]:
            return (
                str(entry.protocol or "").strip().lower(),
                str(entry.chain or "").strip().lower(),
                str(entry.position_id or "").strip().lower(),
            )

        proven_keys = set(verification.hook_proven_position_keys)
        unattributable = tuple(
            entry
            for entry in post_report.confirmed
            if str(entry.position_id or "").strip()
            and _entry_key(entry) in proven_keys
            and LendingReadRegistry.whole_account_read(entry.protocol)
        )
        if unattributable:
            for entry in unattributable:
                logger.warning(
                    "TD-15 (VIB-5936): %s %s (%s) on %s — whole-account read reports residual "
                    "exposure (%s), but this position was hook-proven CLOSED on-chain by its "
                    "TD-14 post-condition. The residual cannot be attributed to this position "
                    "and likely belongs to OTHER %s markets this wallet holds; NOT failing the "
                    "teardown on it. Inspect the wallet's remaining %s exposure separately.",
                    entry.protocol,
                    entry.position_type,
                    entry.position_id,
                    entry.chain,
                    entry.detail,
                    entry.protocol,
                    entry.protocol,
                )
            post_report = post_report.downgrade_unattributable_confirmed_open(
                frozenset(_entry_key(entry) for entry in unattributable),
                "whole-account aggregate unattributable to this hook-proven-closed position (VIB-5936)",
            )

        # Reconciliation can only lower confidence; it never upgrades the hook verdict.
        status = post_report.apply_post_teardown_to_verification_status(verification.verification_status)
        if pre_teardown_reconciliation is not None:
            status = pre_teardown_reconciliation.apply_to_verification_status(status)

        # Only measured open state is residual risk; an unverifiable read is not.
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

        # Certification requires measured closure from every represented protocol and
        # no measured-open position. This is weaker than per-position proof because
        # duplicate physical positions can have different identifiers; within one
        # protocol, measured closure can therefore still cover unmeasured siblings.
        def _protocol_of(obj: Any) -> str:
            return str(getattr(obj, "protocol", "") or "").strip().lower()

        # Union both position views so every observed protocol needs evidence.
        # Blank protocols remain unprovable and cannot become catch-all proof groups.
        pre_positions = getattr(pre_execution_positions, "positions", None) or []
        protocols_to_prove = {_protocol_of(p) for p in pre_positions} | {_protocol_of(e) for e in post_report.entries}
        measured_closed_protocols = {proto for e in post_report.diverged if (proto := _protocol_of(e))} | {
            proto for proto, _chain, _pid in verification.hook_proven_position_keys if proto
        }

        verification = replace(
            verification,
            protocols_to_prove=tuple(sorted(protocols_to_prove)),
            measured_closed_protocols=tuple(sorted(measured_closed_protocols)),
        )
        if verification.closure_unknown:
            logger.error(
                "🛑 TD-15 (VIB-6285): %s teardown closure is UNMEASURED for protocol(s) %s — "
                "no Plan-A DIVERGED_CLOSED read and no TD-14 hook proof for any of their "
                "positions, and nothing measured OPEN. This does NOT mean positions are open; "
                "it means closure was not proven. Refusing to certify success — manual on-chain "
                "verification required. (proved: %s; positions_total=%d)",
                deployment_id,
                ", ".join(verification.unproven_protocols) or "<unnamed>",
                ", ".join(verification.measured_closed_protocols) or "none",
                verification.positions_total,
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
        """Return a FRESH market snapshot for the POST-teardown chain re-read.

        The pre-execution snapshot memoizes ``position_health`` AND wallet
        ``balance``, so reusing it to verify post-closure state returns stale
        (pre-unwind) values and falsely reports a zeroed position still open.
        ``strategy.create_market_snapshot()`` is itself memoized per iteration
        token, and only a live ``StrategyRunner`` rotates that token between
        teardown phases — the no-runner CLI ``teardown execute`` lane never
        does, so stamp a dedicated, always-unique token first to force a cold
        rebuild regardless of which lane is calling. When a fresh snapshot
        cannot be built, fall back to EVICTING the stale memos (both health
        and wallet-balance) on the reused snapshot instead. Never raises —
        verification must never fault the teardown lane.
        """
        # Trust a snapshot only after cache rotation or when it is a distinct instance.
        begin_iteration = getattr(strategy, "begin_market_snapshot_iteration", None)
        token_stamped = False
        if callable(begin_iteration):
            try:
                begin_iteration(f"td15-post-execution:{uuid.uuid4()}")
                token_stamped = True
            except Exception:  # noqa: BLE001 — not stamped; creator() below is untrusted on faith
                logger.warning(
                    "TD-15: begin_market_snapshot_iteration failed for %s — "
                    "create_market_snapshot() cannot be trusted as fresh purely on "
                    "faith; will still accept it if it returns a genuinely different "
                    "instance, otherwise evicting the stale health cache instead",
                    getattr(strategy, "deployment_id", ""),
                    exc_info=True,
                )
        creator = getattr(strategy, "create_market_snapshot", None)
        if callable(creator):
            try:
                fresh = creator()
                if fresh is not None and (token_stamped or fresh is not fallback):
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
            # Reused snapshots must evict both health and wallet-balance caches.
            invalidate_balances = getattr(fallback, "invalidate_balances", None)
            if callable(invalidate_balances):
                try:
                    invalidate_balances()
                except Exception:  # noqa: BLE001 — best-effort; degrade to cached read
                    logger.debug("TD-15: invalidate_balances failed; using cached balances", exc_info=True)
        return fallback

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
            return max(15, len(intents) * 3)
        else:
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

        if positions.total_value_usd > LARGE_POSITION_WARNING_THRESHOLD_USD:
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

    async def _attach_lp_outstanding(self, strategy: Any, intent: Any) -> tuple[Any, str | None]:
        """Attach this deployment's outstanding fungible-LP liquidity to an LP close.

        Returns ``(intent, None)`` when the close may proceed — either bounded, or
        untouched because the venue declares no clamp on its connector manifest — and
        ``(intent, reason)`` when it must be refused.

        The framework supplies only the LEDGER figure. Enforcing it against the live
        wallet balance is the connector's job, because only the connector knows what
        this venue's identifiers denote and it already reads ``balanceOf`` because it
        must. Resolving that here would mean a framework-side chain read, which is what
        broke the earlier attempt at this ticket: the read went through
        ``market.balance()``, a Solidly pool is not a registry-resolvable token, and the
        clamp refused 100% of closes and stranded every position.

        A refusal is deliberately NON_RETRYABLE: retrying cannot make an unresolvable
        identifier resolvable, and a retry loop here would delay the remaining
        risk-reducing intents for no possible gain.
        """
        # Enum intent types must be unwrapped before string comparison.
        raw_type = intent.get("intent_type") if isinstance(intent, dict) else getattr(intent, "intent_type", "")
        intent_type = str(getattr(raw_type, "value", raw_type) or "").upper()
        if intent_type != "LP_CLOSE":
            return intent, None
        if not self.runner_helpers.has_lp_clamp:
            return intent, None

        get_outstanding = self.runner_helpers.get_lp_outstanding
        assert get_outstanding is not None  # noqa: S101 — narrowed by has_lp_clamp

        def _field(name: str) -> Any:
            return intent.get(name) if isinstance(intent, dict) else getattr(intent, name, None)

        protocol = str(_field("protocol") or "")
        position_id = _field("position_id")
        pool = _field("pool")

        try:
            outstanding = await get_outstanding(strategy, protocol, position_id, pool)
        except LpClampUnresolved as exc:
            logger.error(
                "VIB-6162: refusing LP_CLOSE on %s (position_id=%r) — %s. The close is "
                "SKIPPED rather than executed unbounded; remaining teardown intents "
                "continue.",
                protocol,
                position_id,
                exc,
            )
            return intent, f"LP close refused: cannot bound to this deployment's own liquidity ({exc})"

        # None is the connector manifest's explicit no-clamp result.
        if outstanding is None:
            return intent, None

        params = dict(_field("protocol_params") or {})
        params["deployment_outstanding_lp"] = str(outstanding)
        if isinstance(intent, dict):
            updated = dict(intent)
            updated["protocol_params"] = params
            return updated, None
        try:
            return intent.model_copy(update={"protocol_params": params}), None
        except (AttributeError, TypeError, ValueError):
            logger.error(
                "VIB-6162: could not attach the outstanding bound to %s — refusing the "
                "close rather than letting it compile unbounded",
                type(intent).__name__,
            )
            return intent, "LP close refused: could not attach the deployment's outstanding-liquidity bound"

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
            elif intent_type == "VAULT_REDEEM":
                return "Redeem vault shares"
            elif intent_type == "UNSTAKE":
                return "Unstake staked tokens"
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
