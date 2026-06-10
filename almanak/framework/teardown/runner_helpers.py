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
    warn_sweep_non_strategy_balance: WarnSweepNonStrategyBalance | None = None
    get_token_universe: GetTokenUniverse | None = None
    get_accounting_events: GetAccountingEvents | None = None

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

    return TeardownRunnerHelpers(
        commit=_commit_with_heartbeat,
        capture_snapshot=partial(capture_teardown_snapshot_with_accounting, runner),
        snapshot_intent_balances=_snapshot_intent_balances,
        reconcile_post_balances=partial(reconcile_post_execution_balances, runner),
        snapshot_intent_lending_state=_snapshot_intent_lending_state,
        warn_sweep_non_strategy_balance=_warn_sweep_non_strategy_balance,
        get_token_universe=_get_token_universe,
        get_accounting_events=_get_accounting_events,
    )


__all__ = [
    "CaptureTeardownSnapshot",
    "CommitTeardownIntent",
    "GetAccountingEvents",
    "GetTokenUniverse",
    "ReconcilePostBalances",
    "SnapshotIntentBalances",
    "SnapshotIntentLendingState",
    "TeardownRunnerHelpers",
    "WarnSweepNonStrategyBalance",
    "build_runner_helpers",
]
