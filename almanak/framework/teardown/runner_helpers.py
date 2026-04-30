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


@dataclass(frozen=True)
class TeardownRunnerHelpers:
    """Callable bag supplied to :class:`TeardownManager` by Phase 3 wiring.

    Both callables are async. They are pre-bound to a specific
    :class:`StrategyRunner` instance via :func:`functools.partial`; the
    teardown manager does not need to know about the runner directly.

    Set both fields to ``None`` (the dataclass default) to retain
    pre-VIB-3773 behaviour (no accounting writes from the teardown lane).
    Tests that don't care about the accounting lane construct
    ``TeardownRunnerHelpers()`` and pass it straight through.
    """

    commit: CommitTeardownIntent | None = None
    capture_snapshot: CaptureTeardownSnapshot | None = None

    @property
    def has_commit(self) -> bool:
        return self.commit is not None

    @property
    def has_snapshot(self) -> bool:
        return self.capture_snapshot is not None


def build_runner_helpers(runner: Any) -> TeardownRunnerHelpers:
    """Bind the runner instance into a :class:`TeardownRunnerHelpers` bag.

    The runner is bound via :func:`functools.partial` so the consumer
    (``TeardownManager``) calls a plain function with the strategy/intent
    arguments, never the runner.
    """
    from functools import partial

    from ..runner._run_loop_helpers import capture_teardown_snapshot_with_accounting
    from ..runner.teardown_commit import commit_teardown_intent

    return TeardownRunnerHelpers(
        commit=partial(commit_teardown_intent, runner),
        capture_snapshot=partial(capture_teardown_snapshot_with_accounting, runner),
    )


__all__ = [
    "CaptureTeardownSnapshot",
    "CommitTeardownIntent",
    "TeardownRunnerHelpers",
    "build_runner_helpers",
]
