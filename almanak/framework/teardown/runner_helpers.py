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

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from ..runner._run_loop_helpers import TeardownSnapshotOutcome
    from ..runner.teardown_commit import TeardownCommitOutcome


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

    async def _snapshot_intent_balances(strategy: Any, intent: Any) -> Any | None:
        # ``snapshot_balances_for_intent`` only needs the runner + intent
        # — strategy is unused but kept on the helper signature for
        # symmetry with reconcile and future protocol-aware variants.
        del strategy
        return await snapshot_balances_for_intent(runner, intent)

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
        commit=partial(commit_teardown_intent, runner),
        capture_snapshot=partial(capture_teardown_snapshot_with_accounting, runner),
        snapshot_intent_balances=_snapshot_intent_balances,
        reconcile_post_balances=partial(reconcile_post_execution_balances, runner),
        snapshot_intent_lending_state=_snapshot_intent_lending_state,
    )


__all__ = [
    "CaptureTeardownSnapshot",
    "CommitTeardownIntent",
    "ReconcilePostBalances",
    "SnapshotIntentBalances",
    "SnapshotIntentLendingState",
    "TeardownRunnerHelpers",
    "build_runner_helpers",
]
