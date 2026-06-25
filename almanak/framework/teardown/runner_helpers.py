"""Callable bag plumbed from StrategyRunner into TeardownManager — VIB-3773.

The teardown lane needs to call back into the runner for two purposes:

1. Per-intent **commit pipeline** (enrich → ledger → outbox+fire → sidecar)
   after a successful ``orchestrator.execute_bundle`` call.
2. Pre- and post-teardown **snapshot bracket** (snapshot + metrics writes
   stamped with the teardown's cycle id).

Rather than widening :class:`TeardownManager`'s protocol surface to a full
``StrategyRunner`` instance — which would couple a deliberately narrow
component to the runner's whole API — we pass two pre-bound async callables.

* :attr:`commit` is :func:`runner.teardown_commit.commit_teardown_intent`
  with the runner already bound, exposing the keyword-only contract:
  ``commit(strategy, intent, *, execution_result, execution_context,
  bundle_metadata=None, teardown_cycle_id) -> TeardownCommitOutcome``.
* :attr:`capture_snapshot` is
  :func:`_run_loop_helpers.capture_teardown_snapshot_with_accounting`
  bound similarly: ``capture_snapshot(strategy, *, teardown_cycle_id,
  pre_teardown) -> TeardownSnapshotOutcome``.

Either may be ``None`` for backward compatibility — :class:`TeardownManager`
falls back to the legacy bypass behaviour (no accounting writes) so existing
unit tests that construct the manager without a runner keep working. Phase
3 wiring at ``_teardown_helpers.build_teardown_manager`` always populates
both in production.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from ..runner._run_loop_helpers import TeardownSnapshotOutcome
    from ..runner.teardown_commit import TeardownCommitOutcome

logger = logging.getLogger(__name__)


CommitTeardownIntent = Callable[..., Awaitable["TeardownCommitOutcome"]]
"""Type alias for the runner-bound commit callable."""

CaptureTeardownSnapshot = Callable[..., Awaitable["TeardownSnapshotOutcome"]]
"""Type alias for the runner-bound snapshot-bracket callable."""

SnapshotIntentBalances = Callable[..., Awaitable[Any | None]]
"""Async ``(strategy, intent) -> BalanceSnapshot | None``. Captures wallet
balances for the tokens this intent will move, BEFORE it executes — the
teardown counterpart of the iteration lane's
``_snapshot_balances_for_intent``. Used to seed
``transaction_ledger.pre_state_json`` with per-intent (not pre-bracket-only)
wallet snapshots so the second teardown intent's pre-state correctly
follows the first's post-state."""

ReconcilePostBalances = Callable[..., Awaitable[dict[str, Any] | None]]
"""Async ``(strategy, intent, execution_result, pre_snapshot) -> recon dict``.
Mirrors the iteration lane's ``_reconcile_post_execution_balances`` so
``transaction_ledger.post_state_json`` lands populated on every teardown
row."""

SnapshotIntentLendingState = Callable[..., Awaitable[Any | None]]
"""Async ``(strategy, intent) -> lending state object | None``. Captures the
on-chain lending position state (collateral / debt / HF) BEFORE the intent
executes — the teardown counterpart of the iteration lane's pre-state
capture at ``_init_single_chain_state``. Threaded into the commit pipeline
so ``transaction_ledger.pre_state_json`` carries lending fields lane-
symmetric with iteration (VIB-3934)."""

SnapshotIntentV4LpCloseFees = Callable[..., Awaitable[tuple[int, int] | None]]
"""Async ``(strategy, intent) -> (tokens_owed0, tokens_owed1) | None``. Reads
Uniswap V4 uncollected fees ON-CHAIN BEFORE the LP_CLOSE / LP_COLLECT_FEES burn
executes — the teardown counterpart of the iteration lane's
``state.v4_lp_close_fees`` capture at ``_init_single_chain_state`` (VIB-4482).
A post-burn read returns zero liquidity, so the read MUST happen pre-execute.
Threaded into the commit pipeline so the LP accounting handler emits measured
fees (``fees0/1``) lane-symmetric with iteration. Returns ``None`` for
non-V4-LP-close intents, missing gateway, undeployed chains, or read failures —
never raises, never fabricates a zero (Empty ≠ Zero)."""

SnapshotIntentV4LpCloseNativePrincipal = Callable[..., Awaitable[tuple[int | None, int | None] | None]]
"""Async ``(strategy, intent) -> (amount0, amount1) | None``. Reads the closing
V4 position's native-leg PRINCIPAL ON-CHAIN BEFORE the LP_CLOSE burn executes —
the teardown counterpart of the iteration lane's
``state.v4_lp_close_native_principal`` capture at ``_init_single_chain_state``
(VIB-5117). A native-ETH leg is withdrawn as raw ETH (no Transfer), so the burn
receipt cannot measure it; the principal is derived from the pre-burn position
state (post-burn read = zero liquidity). Threaded into the commit pipeline so the
LP accounting handler records the real native proceeds (instead of a measured-
zero lie) lane-symmetric with iteration. Returns ``None`` for non-native-leg
closes, missing gateway, undeployed chains, or read failures — never raises,
never fabricates a zero (Empty ≠ Zero)."""

WarnSweepNonStrategyBalance = Callable[..., None]
"""Sync ``(strategy, intent, balance_token, balance_value) -> None``. Logs a
WARNING when teardown's ``amount='all'`` SWAP would sweep a wallet balance
the strategy never emitted any accounting events for. Bound via
:func:`build_runner_helpers` against the runner's **accounting** StateManager
(``runner.state_manager``) — the teardown lifecycle state manager does not
expose ``get_accounting_events_sync`` (VIB-4587 / F5)."""

GetTokenUniverse = Callable[..., set[str]]
"""Sync ``(strategy, closing_intents, positions) -> set[str]``. Derives the
strategy-scoped token universe for the token-consolidation phase (VIB-5011).
Bound via :func:`build_runner_helpers` to
:func:`almanak.framework.teardown.consolidation.derive_strategy_token_universe`
with the runner's **accounting** StateManager — so the universe includes the
deployment's accounting-event token footprint, never the full shared wallet."""

GetAccountingEvents = Callable[..., list]
"""Sync ``(strategy) -> list[dict]``. Returns the deployment's accounting
events (timestamp ASC) via the runner's accounting StateManager. Used by the
token-consolidation phase to resolve the ``entry_token`` policy's
earliest-SWAP fallback (VIB-5011). Best-effort: returns ``[]`` on any
failure."""

GetTrackedSwapInventory = Callable[..., dict[str, Any] | None]
"""Sync ``(strategy) -> {canonical_symbol: Decimal} | None``. Deployment-scoped
tracked wallet inventory (Σ open wallet-basis lot ``remaining`` per token, all
sources) used by the ALM-2766 teardown swap-back clamp: a default teardown may
swap back only ``min(tracked, live_balance)``, never the full commingled wallet.
``None`` is the UNMEASURED sentinel (empty deployment id / unreadable events /
FIFO replay failure) and the clamp then fails closed. Bound via
:func:`build_runner_helpers` against the runner's **accounting** StateManager
(``runner.state_manager``) — the teardown lifecycle SM does not expose
``get_accounting_events_sync``. Read-only; never raises."""

DiscoverLpPositions = Callable[..., Awaitable[Any]]
"""Async ``(strategy) -> LpDiscoveryResult``. Runs BOUNDED on-chain LP
discovery (VIB-5138) for the strategy's wallet/chain via the gateway
RpcService — the same NPM scan the ``--discover`` CLI flag uses
(``teardown.discovery``). The teardown manager's auto-fallback path calls
this when the strategy reports no LP (state desync — NFT live on-chain but
``_position_id`` lost, often after an ``AccountingPersistenceError`` on LP
open) so the orphaned NFT is still closed instead of being silently
stranded. Returns an ``LpDiscoveryResult`` carrying the discovered
``TeardownPositionSummary`` and an ``incomplete`` flag (True when discovery
could not enumerate every NPM-reported position — strict mode raised
``DiscoveryIncomplete``). Never raises: discovery failure degrades the
teardown loudly but must never block the next risk-reducing intent."""

GetDeploymentLpOwnership = Callable[..., Awaitable[Any]]
"""Async ``(strategy, chain) -> DeploymentLpOwnership``. Returns the LP NFT
token ids attributable to THIS deployment on ``chain`` (VIB-5138 / VIB-4976
fund-safety scoping). Built from the deployment's own durable accounting state
— ``position_registry`` OPEN rows (``payload.token_id``, the robust
post-cutover signal that survives the LP-open ``AccountingPersistenceError``
because it is committed atomically with the ledger BEFORE the typed accounting
event) unioned with ``position_events`` LP OPEN rows (``position_id`` = token
id, the pre-cutover fallback). NEVER enumerates the shared wallet. Used to
scope on-chain LP discovery so teardown can only ever close positions this
deployment opened — a sibling strategy's live LP on the same wallet is not in
the set. Never raises; on total read failure returns ``available=False`` so
recovery refuses to close anything (ownership unprovable)."""


@dataclass(frozen=True)
class TeardownRunnerHelpers:
    """Callable bag supplied to :class:`TeardownManager` by Phase 3 wiring.

    All callables are async and pre-bound to a :class:`StrategyRunner`
    instance via :func:`functools.partial`; the teardown manager does not
    need to know about the runner directly.

    Set fields to ``None`` (the dataclass default) to retain pre-VIB-3773
    / pre-VIB-3918 behaviour (no accounting writes from the teardown lane,
    or no per-intent pre/post state). Tests that don't care about the
    accounting lane construct ``TeardownRunnerHelpers()`` and pass it
    straight through.
    """

    commit: CommitTeardownIntent | None = None
    capture_snapshot: CaptureTeardownSnapshot | None = None
    snapshot_intent_balances: SnapshotIntentBalances | None = None
    reconcile_post_balances: ReconcilePostBalances | None = None
    snapshot_intent_lending_state: SnapshotIntentLendingState | None = None
    snapshot_intent_v4_lp_close_fees: SnapshotIntentV4LpCloseFees | None = None
    snapshot_intent_v4_lp_close_native_principal: SnapshotIntentV4LpCloseNativePrincipal | None = None
    warn_sweep_non_strategy_balance: WarnSweepNonStrategyBalance | None = None
    get_token_universe: GetTokenUniverse | None = None
    get_accounting_events: GetAccountingEvents | None = None
    get_tracked_swap_inventory: GetTrackedSwapInventory | None = None
    discover_lp_positions: DiscoverLpPositions | None = None
    get_deployment_lp_ownership: GetDeploymentLpOwnership | None = None

    @property
    def has_commit(self) -> bool:
        return self.commit is not None

    @property
    def has_snapshot(self) -> bool:
        return self.capture_snapshot is not None

    @property
    def has_per_intent_balances(self) -> bool:
        """True iff both pre- and post-execution balance helpers are wired.
        Either-only is useless: pre without post can't produce post_state,
        post without pre can't produce pre_state. Treat as all-or-nothing.
        """
        return self.snapshot_intent_balances is not None and self.reconcile_post_balances is not None

    @property
    def has_lending_pre_state(self) -> bool:
        """True iff the lending pre-state capture helper is wired (VIB-3934)."""
        return self.snapshot_intent_lending_state is not None

    @property
    def has_v4_lp_close_fees(self) -> bool:
        """True iff the V4 LP-close pre-fee capture helper is wired (VIB-4482)."""
        return self.snapshot_intent_v4_lp_close_fees is not None

    @property
    def has_v4_lp_close_native_principal(self) -> bool:
        """True iff the V4 LP-close native-principal capture helper is wired (VIB-5117)."""
        return self.snapshot_intent_v4_lp_close_native_principal is not None

    @property
    def has_sweep_warning(self) -> bool:
        """True iff the teardown-sweep DX warning helper is wired (VIB-4587 / F5)."""
        return self.warn_sweep_non_strategy_balance is not None

    @property
    def has_token_universe(self) -> bool:
        """True iff the consolidation token-universe helper is wired (VIB-5011)."""
        return self.get_token_universe is not None

    @property
    def has_accounting_events(self) -> bool:
        """True iff the accounting-events accessor is wired (VIB-5011)."""
        return self.get_accounting_events is not None

    @property
    def has_tracked_inventory(self) -> bool:
        """True iff the ALM-2766 tracked-inventory accessor is wired."""
        return self.get_tracked_swap_inventory is not None

    @property
    def has_lp_discovery(self) -> bool:
        """True iff the on-chain LP discovery fallback is wired (VIB-5138)."""
        return self.discover_lp_positions is not None and self.get_deployment_lp_ownership is not None


def build_runner_helpers(runner: Any) -> TeardownRunnerHelpers:
    """Bind the runner instance into a :class:`TeardownRunnerHelpers` bag.

    The runner is bound via :func:`functools.partial` so the consumer
    (``TeardownManager``) calls a plain function with the strategy/intent
    arguments, never the runner.
    """
    from functools import partial

    from ..runner._run_loop_helpers import capture_teardown_snapshot_with_accounting
    from ..runner.runner_state import (
        reconcile_post_execution_balances,
        snapshot_balances_for_intent,
    )
    from ..runner.teardown_commit import commit_teardown_intent
    from .sweep_warning import warn_if_sweep_non_strategy_balance

    async def _snapshot_intent_balances(strategy: Any, intent: Any) -> Any | None:
        # ``snapshot_balances_for_intent`` only needs the runner + intent
        # — strategy is unused but kept on the helper signature for
        # symmetry with reconcile and future protocol-aware variants.
        del strategy
        return await snapshot_balances_for_intent(runner, intent)

    def _warn_sweep_non_strategy_balance(strategy: Any, intent: Any, balance_token: str, balance_value: Any) -> None:
        # VIB-4587 / F5 — wallet-scope teardown sweep DX warning. We compute
        # ``deployment_id`` here (using the same fallback the runner uses
        # for accounting writes) so the call site doesn't have to recompute
        # it, and pass the runner's **accounting** StateManager — the
        # teardown lifecycle SM does not expose ``get_accounting_events_sync``.
        deployment_id = strategy.deployment_id
        warn_if_sweep_non_strategy_balance(
            state_manager=getattr(runner, "state_manager", None),
            deployment_id=deployment_id,
            intent=intent,
            balance_token=balance_token,
            balance_value=balance_value,
        )

    async def _commit_with_heartbeat(strategy: Any, intent: Any, **kwargs: Any) -> Any:
        # VIB-3951 — refresh the teardown crash-watchdog heartbeat once per
        # committed teardown intent so the staleness window reflects REAL
        # liveness (not just time-since-mark_started). A long multi-intent
        # unwind (REPAY → WITHDRAW → SWAP, each ~100s on a slow fork) keeps the
        # owning runner out of the watchdog's stale-by-time bucket. Local-only:
        # the hosted gateway teardown manager has no ``heartbeat`` method (the
        # platform owns hosted liveness), so this is guarded and a no-op there.
        # Best-effort — a heartbeat failure must NEVER interrupt the
        # risk-reducing commit (teardown loud-but-non-blocking contract).
        outcome = await commit_teardown_intent(runner, strategy, intent, **kwargs)
        try:
            from . import get_teardown_state_manager_for_runtime

            manager = get_teardown_state_manager_for_runtime(gateway_client=runner._get_gateway_client())
            beat = getattr(manager, "heartbeat", None)
            if beat is not None:
                beat(strategy.deployment_id)
        except Exception as exc:  # noqa: BLE001 — heartbeat is best-effort
            logger.debug("Teardown heartbeat refresh failed (non-fatal): %s", exc)
        return outcome

    def _get_token_universe(strategy: Any, closing_intents: Any, positions: Any) -> set[str]:
        # VIB-5011 — strategy-scoped token universe for the consolidation
        # planner. The accounting StateManager (runner.state_manager) supplies
        # the deployment's event footprint; the wallet is never enumerated
        # (shared across deployments — a wallet-wide sweep would steal
        # sibling-strategy inventory).
        from .consolidation import derive_strategy_token_universe

        return derive_strategy_token_universe(
            getattr(runner, "state_manager", None),
            strategy.deployment_id,
            strategy,
            closing_intents,
            positions,
        )

    def _get_accounting_events(strategy: Any) -> list:
        # VIB-5011 — best-effort accounting-event read for the entry_token
        # policy's earliest-SWAP fallback. Never raises.
        sm = getattr(runner, "state_manager", None)
        if sm is None or not hasattr(sm, "get_accounting_events_sync"):
            return []
        try:
            return sm.get_accounting_events_sync(strategy.deployment_id)
        except Exception:  # noqa: BLE001 — consolidation is best-effort
            logger.debug("accounting-event read for consolidation failed (non-fatal)", exc_info=True)
            return []

    def _get_tracked_swap_inventory(strategy: Any) -> dict[str, Any] | None:
        # ALM-2766 — deployment-scoped tracked wallet inventory for the
        # teardown swap-back clamp. Reads the runner's accounting StateManager
        # and replays FIFO lots; returns the UNMEASURED sentinel (None) on any
        # failure (the clamp then fails closed). Never raises.
        #
        # VIB-5416 — pass the deployment's chain + wallet so the clamp can key
        # NO_ACCOUNTING ledger acquisitions (STAKE/WRAP/MINT) into the SAME
        # ``swap:{chain}:{wallet}`` pool as real swap lots (1 gateway : 1
        # strategy guarantees these equal every real swap lot's key).
        from .swap_clamp import read_tracked_swap_inventory

        return read_tracked_swap_inventory(
            state_manager=getattr(runner, "state_manager", None),
            deployment_id=(getattr(strategy, "deployment_id", "") or ""),
            chain=(getattr(strategy, "chain", "") or ""),
            wallet_address=(getattr(strategy, "wallet_address", "") or ""),
        )

    async def _snapshot_intent_lending_state(strategy: Any, intent: Any) -> Any | None:
        # VIB-3934 — capture lending pre-state via the runner's safe wrapper
        # so REPAY/WITHDRAW/DELEVERAGE teardown rows carry collateral/debt/HF
        # in ``pre_state_json``, lane-symmetric with the iteration lane's
        # ``state.lending_pre_state``. Returns ``None`` for non-lending
        # intents, missing gateway, unsupported protocols, or transient
        # gateway failures — never raises.
        return runner._capture_lending_state_safe(
            intent=intent,
            chain=getattr(strategy, "chain", "") or "",
            wallet_address=getattr(strategy, "wallet_address", "") or "",
            gateway_client=runner._get_gateway_client(),
            price_oracle=getattr(runner, "_teardown_price_oracle", None),
            phase="pre",
        )

    async def _snapshot_intent_v4_lp_close_fees(strategy: Any, intent: Any) -> tuple[int, int] | None:
        # VIB-4482 — capture Uniswap V4 uncollected fees on-chain BEFORE the
        # LP_CLOSE / LP_COLLECT_FEES burn executes, via the runner's safe
        # wrapper. A post-burn read returns zero liquidity, so this MUST run
        # pre-execute. Returns ``None`` for non-V4-LP-close intents, missing
        # gateway, undeployed chains, or transient gateway failures — never
        # raises, never fabricates a zero (Empty ≠ Zero). Lane-symmetric with
        # the iteration lane's ``state.v4_lp_close_fees``.
        return runner._capture_v4_lp_close_fees_safe(
            intent=intent,
            chain=getattr(strategy, "chain", "") or "",
            gateway_client=runner._get_gateway_client(),
        )

    async def _snapshot_intent_v4_lp_close_native_principal(
        strategy: Any, intent: Any
    ) -> tuple[int | None, int | None] | None:
        # VIB-5117 — capture the closing V4 position's native-leg PRINCIPAL
        # on-chain BEFORE the LP_CLOSE burn executes, via the runner's safe
        # wrapper. A native-ETH leg is withdrawn as raw ETH (no Transfer), so a
        # post-burn read returns zero liquidity — this MUST run pre-execute.
        # Returns ``None`` for non-native-leg closes, missing gateway, undeployed
        # chains, or read failures — never raises, never fabricates a zero
        # (Empty ≠ Zero). Lane-symmetric with the iteration lane's
        # ``state.v4_lp_close_native_principal``.
        return runner._capture_v4_lp_close_native_principal_safe(
            intent=intent,
            chain=getattr(strategy, "chain", "") or "",
            gateway_client=runner._get_gateway_client(),
        )

    return TeardownRunnerHelpers(
        commit=_commit_with_heartbeat,
        capture_snapshot=partial(capture_teardown_snapshot_with_accounting, runner),
        snapshot_intent_balances=_snapshot_intent_balances,
        reconcile_post_balances=partial(reconcile_post_execution_balances, runner),
        snapshot_intent_lending_state=_snapshot_intent_lending_state,
        snapshot_intent_v4_lp_close_fees=_snapshot_intent_v4_lp_close_fees,
        snapshot_intent_v4_lp_close_native_principal=_snapshot_intent_v4_lp_close_native_principal,
        warn_sweep_non_strategy_balance=_warn_sweep_non_strategy_balance,
        get_token_universe=_get_token_universe,
        discover_lp_positions=partial(_discover_lp_for_teardown, runner),
        get_deployment_lp_ownership=partial(_deployment_lp_ownership, runner),
        get_accounting_events=_get_accounting_events,
        get_tracked_swap_inventory=_get_tracked_swap_inventory,
    )


async def _read_registry_lp_token_ids(state_manager: Any, deployment_id: str, chain: str) -> tuple[set[str], bool]:
    """LP NFT token ids from this deployment's ``position_registry`` OPEN rows.

    Source 1 of :func:`_deployment_lp_ownership` (robust, survives the LP-open
    ``AccountingPersistenceError`` desync — the registry row is committed
    atomically with the ledger BEFORE the typed accounting event). Owns its own
    try/except: a backend without registry storage raises
    ``CutoverStorageNotSupported`` (caught → treated as "no registry signal").

    Returns ``(token_ids, ok)``. ``ok`` is True only when the read completed
    (so the coordinator can compute ``available = registry_ok or events_ok``).
    Never raises.
    """
    if state_manager is None or not hasattr(state_manager, "get_position_registry_open_rows"):
        return set(), False
    token_ids: set[str] = set()
    try:
        rows = await state_manager.get_position_registry_open_rows(deployment_id, chain=chain, primitive="lp")
    except Exception as exc:  # noqa: BLE001 — CutoverStorageNotSupported et al.
        logger.debug(
            "Teardown LP ownership: position_registry read unavailable for %s (%s); falling back to position_events",
            deployment_id,
            exc,
        )
        return set(), False
    for row in rows or []:
        payload = row.get("payload") if isinstance(row, dict) else None
        tid = payload.get("token_id") if isinstance(payload, dict) else None
        if tid is not None:
            token_ids.add(str(tid))
    return token_ids, True


def _read_position_event_lp_token_ids(state_manager: Any, deployment_id: str, chain: str) -> tuple[set[str], bool]:
    """LP NFT token ids from this deployment's ``position_events`` LP OPEN rows.

    Source 2 of :func:`_deployment_lp_ownership` (pre- and post-cutover
    fallback — ``position_id`` IS the NFT token id). Sync read. Only this
    chain's OPENs are counted. Owns its own try/except.

    Returns ``(token_ids, ok)`` like :func:`_read_registry_lp_token_ids`.
    Never raises.
    """
    if state_manager is None or not hasattr(state_manager, "get_position_events_sync"):
        return set(), False
    token_ids: set[str] = set()
    try:
        events = state_manager.get_position_events_sync(deployment_id, position_type="LP", event_type="OPEN")
    except Exception as exc:  # noqa: BLE001 — best-effort attribution read
        logger.debug("Teardown LP ownership: position_events read failed for %s (%s)", deployment_id, exc)
        return set(), False
    for ev in events or []:
        ev_chain = (ev.get("chain") or "") if isinstance(ev, dict) else ""
        pid = ev.get("position_id") if isinstance(ev, dict) else None
        if pid is None:
            continue
        # Only count this chain's OPENs toward token_ids.
        if ev_chain and ev_chain != chain:
            continue
        token_ids.add(str(pid))
    return token_ids, True


async def _deployment_lp_ownership(runner: Any, strategy: Any, chain: str) -> Any:
    """LP token ids attributable to THIS deployment on ``chain`` (VIB-5138).

    Fund-safety scoping (VIB-4976): the on-chain discovery scan is wallet-scoped
    and a wallet may be shared across deployments. This reads the deployment's
    OWN durable accounting state to learn which NFT token ids it opened, so
    teardown recovery can never close a sibling strategy's live LP on the same
    wallet.

    Thin coordinator over two complementary read sources (both have independent
    survival in the desync the ticket targets) — see
    :func:`_read_registry_lp_token_ids` (robust, post-cutover) and
    :func:`_read_position_event_lp_token_ids` (pre-cutover fallback). The ids are
    unioned; ``had_lp_open`` is True iff any source contributed an id;
    ``available`` is True iff at least one read completed. When BOTH reads fail
    ``available=False`` so the caller refuses to close anything (ownership
    unprovable). Never raises.

    Bound to the runner via :func:`functools.partial` so the consumer calls
    ``(strategy, chain) -> DeploymentLpOwnership``.
    """
    from .lp_recovery import DeploymentLpOwnership

    deployment_id = (getattr(strategy, "deployment_id", "") or "").strip()
    sm = getattr(runner, "state_manager", None)

    registry_ids, registry_ok = await _read_registry_lp_token_ids(sm, deployment_id, chain)
    event_ids, events_ok = _read_position_event_lp_token_ids(sm, deployment_id, chain)

    token_ids = registry_ids | event_ids
    available = registry_ok or events_ok
    if not available:
        logger.warning(
            "Teardown LP ownership: NO attribution source readable for %s on %s — "
            "recovery will refuse to close discovered NFTs (ownership unprovable).",
            deployment_id,
            chain,
        )
    return DeploymentLpOwnership(
        token_ids=frozenset(token_ids),
        had_lp_open=bool(token_ids),
        available=available,
    )


async def _discover_lp_for_teardown(runner: Any, strategy: Any) -> Any:
    """Bounded on-chain LP discovery fallback for teardown recovery (VIB-5138).

    Reuses the SAME gateway-backed NPM scan the ``--discover`` CLI flag uses
    (``teardown.discovery``), so the recovery path stays on the gateway
    boundary (no direct network). Strict mode is REQUIRED: a partial scan that
    silently drops a position would re-create the very orphan this fix closes,
    so ``DiscoveryIncomplete`` is caught and surfaced as ``incomplete=True``
    rather than swallowed. Never raises — discovery failure degrades the
    teardown loudly but must never block the next risk-reducing intent.

    Bound to the runner via :func:`functools.partial` in
    :func:`build_runner_helpers` so the consumer calls ``(strategy) ->
    LpDiscoveryResult``.
    """
    from .discovery import DiscoveryIncomplete, discover_lp_positions, to_teardown_summary
    from .lp_recovery import LpDiscoveryResult
    from .models import TeardownPositionSummary

    deployment_id = (getattr(strategy, "deployment_id", "") or "").strip()
    chain = (getattr(strategy, "chain", "") or "").strip()
    wallet = (getattr(strategy, "wallet_address", "") or "").strip()
    empty = TeardownPositionSummary.empty(deployment_id or "unknown")

    if not (deployment_id and chain and wallet):
        logger.warning(
            "Teardown LP discovery skipped: missing deployment_id/chain/wallet (deployment_id=%r chain=%r wallet=%r)",
            deployment_id,
            chain,
            wallet,
        )
        return LpDiscoveryResult(summary=empty, incomplete=False)

    try:
        discovered = await discover_lp_positions(
            client=runner._get_gateway_client(),
            chain=chain,
            wallet=wallet,
            strict=True,
        )
    except DiscoveryIncomplete as exc:
        return LpDiscoveryResult(summary=empty, incomplete=True, error=str(exc))
    except Exception as exc:  # noqa: BLE001 — never let discovery block risk reduction
        logger.error("Teardown LP discovery failed (non-fatal): %s", exc, exc_info=True)
        return LpDiscoveryResult(summary=empty, incomplete=True, error=str(exc))

    summary = to_teardown_summary(deployment_id=deployment_id, chain=chain, positions=discovered)
    return LpDiscoveryResult(summary=summary, incomplete=False)


__all__ = [
    "CaptureTeardownSnapshot",
    "CommitTeardownIntent",
    "DiscoverLpPositions",
    "GetAccountingEvents",
    "GetDeploymentLpOwnership",
    "GetTokenUniverse",
    "GetTrackedSwapInventory",
    "ReconcilePostBalances",
    "SnapshotIntentBalances",
    "SnapshotIntentLendingState",
    "SnapshotIntentV4LpCloseFees",
    "SnapshotIntentV4LpCloseNativePrincipal",
    "TeardownRunnerHelpers",
    "WarnSweepNonStrategyBalance",
    "build_runner_helpers",
]
