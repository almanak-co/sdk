"""Tests for the async-settlement fill-reconciliation pump (VIB-5614).

VIB-5597 added the fill-reconciliation SEAM (a perp strategy submits PERP_OPEN →
PENDING → HOLDs until ``reconcile_fill`` observes the fill) but nothing drove it,
so a continuous perp strategy stayed PENDING forever. This runner-level pump reads
the fill verdict for a cached pending handle each tick (before decide) and calls
``strategy.reconcile_fill(intent_type, status)``.

These pin the framework contract:
- A PENDING position PROMOTES after an observed FILL (terminal verdict clears the
  cached handle).
- A PENDING position STAYS PENDING on UNMEASURED (non-terminal → handle retained,
  re-pumped next tick).
- The pump is inert for strategies without ``reconcile_fill`` and when no handle
  is cached (non-perp strategies untouched).
- Fail-closed: no gateway → stays PENDING (handle retained).
- Warn-only: a raising capability/read never breaks the loop.
- Capture: a successful open caches the connector-produced handle; a failed one
  does not.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.runner_hook_registry import (
    STRATEGY_RUNNER_HOOK_REGISTRY,
    FillReconciliationVerdict,
    RunnerFillReconciliationCapability,
    RunnerHookConnector,
)
from almanak.framework.runner.strategy_runner import (
    RunIterationState,
    RunnerConfig,
    StrategyRunner,
)

_TEST_PROTOCOL = "fillrecon_test"


class _Handle:
    """Minimal protocol-tagged pending-fill handle."""

    def __init__(self, intent_type: str = "PERP_OPEN") -> None:
        self.protocol = _TEST_PROTOCOL
        self.intent_type = intent_type


class _FakeCapability(RunnerHookConnector, RunnerFillReconciliationCapability):
    """A registrable fill-reconciliation connector whose verdicts are scripted."""

    protocol: ClassVar[ProtocolName] = ProtocolName(_TEST_PROTOCOL)
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def __init__(self) -> None:
        self.next_handle: Any | None = None
        self.next_verdict: FillReconciliationVerdict | None = None
        self.raise_on_resolve = False
        self.resolve_calls = 0

    def extract_pending_fill_handle(self, result: Any) -> Any | None:  # noqa: ARG002
        return self.next_handle

    def resolve_fill_status(
        self, *, gateway_client: Any, wallet_address: str, handle: Any  # noqa: ARG002
    ) -> FillReconciliationVerdict | None:
        self.resolve_calls += 1
        if self.raise_on_resolve:
            raise RuntimeError("boom")
        return self.next_verdict


class _FakeStrategy:
    """A real (non-Mock) strategy tracking PENDING/live like the reference perp."""

    def __init__(self) -> None:
        self.deployment_id = "deployment:fillrecon"
        self.wallet_address = "0x" + "11" * 20
        self._position_side: str | None = None
        self._fill_confirmed = False
        self.calls: list[tuple[str, Any]] = []

    def reconcile_fill(self, intent_type: str, status: Any) -> None:
        self.calls.append((intent_type, status))
        if intent_type != "PERP_OPEN" or self._position_side is None or self._fill_confirmed:
            return
        # Mirror the reference strategy's transitions off the opaque status value.
        if str(status) in ("filled", "partially_filled"):
            self._fill_confirmed = True
        elif str(status) == "rejected":
            self._position_side = None
            self._fill_confirmed = False


@pytest.fixture
def registered_capability():
    """Register a fake fill-reconciliation connector; restore the registry after.

    Teardown restores the registry to its CANONICAL populated state via
    ``_register_all()`` rather than replaying a snapshot. The global registry is
    populated by import-time discovery (``_register_discovered_runner_hooks``),
    and whether that has run by the time this fixture sets up depends on which
    modules a given pytest-split shard imported first — so a naive save/restore
    can capture (and replay) an EMPTY registry, leaving the real connectors'
    LP-receipt topics unregistered for every later test in the same worker
    (surfaced as ``RunnerHookRegistryError: No LP receipt topics are registered``
    in an unrelated registry-dispatch test that happens to be co-scheduled).
    Re-running canonical registration is idempotent-after-clear and shard-order
    independent.
    """
    from almanak.connectors._strategy_runner_hook_registry import _register_all

    STRATEGY_RUNNER_HOOK_REGISTRY.clear()
    cap = _FakeCapability()
    STRATEGY_RUNNER_HOOK_REGISTRY.register(cap)
    try:
        yield cap
    finally:
        STRATEGY_RUNNER_HOOK_REGISTRY.clear()
        _register_all()


def _make_runner() -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=False,
        decide_timeout_seconds=30.0,
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
    )


def _make_state(strategy: Any) -> RunIterationState:
    state = RunIterationState(
        strategy=strategy,
        deployment_id=strategy.deployment_id,
        start_time=datetime.now(UTC),
    )
    state.market = MagicMock()
    return state


def _pending_strategy() -> _FakeStrategy:
    s = _FakeStrategy()
    s._position_side = "long"  # submitted, PENDING (fill unconfirmed)
    s._fill_confirmed = False
    return s


@pytest.mark.asyncio
async def test_pending_promotes_on_observed_fill(registered_capability) -> None:
    runner = _make_runner()
    runner._get_gateway_client = MagicMock(return_value=object())  # type: ignore[method-assign]
    strategy = _pending_strategy()
    state = _make_state(strategy)

    runner._pending_fill_handles[strategy.deployment_id] = _Handle()
    registered_capability.next_verdict = FillReconciliationVerdict(status="filled", terminal=True)

    await runner._step_pump_fill_reconciliation(state)

    assert strategy.calls == [("PERP_OPEN", "filled")]
    assert strategy._fill_confirmed is True  # PENDING → live
    # Terminal verdict clears the cached handle so it stops pumping.
    assert strategy.deployment_id not in runner._pending_fill_handles


@pytest.mark.asyncio
async def test_stays_pending_on_unmeasured(registered_capability) -> None:
    runner = _make_runner()
    runner._get_gateway_client = MagicMock(return_value=object())  # type: ignore[method-assign]
    strategy = _pending_strategy()
    state = _make_state(strategy)

    runner._pending_fill_handles[strategy.deployment_id] = _Handle()
    registered_capability.next_verdict = FillReconciliationVerdict(status="unmeasured", terminal=False)

    await runner._step_pump_fill_reconciliation(state)

    assert strategy.calls == [("PERP_OPEN", "unmeasured")]
    assert strategy._fill_confirmed is False  # still PENDING
    # Non-terminal → handle RETAINED so the next tick re-pumps.
    assert strategy.deployment_id in runner._pending_fill_handles


@pytest.mark.asyncio
async def test_rejected_clears_pending(registered_capability) -> None:
    runner = _make_runner()
    runner._get_gateway_client = MagicMock(return_value=object())  # type: ignore[method-assign]
    strategy = _pending_strategy()
    state = _make_state(strategy)

    runner._pending_fill_handles[strategy.deployment_id] = _Handle()
    registered_capability.next_verdict = FillReconciliationVerdict(status="rejected", terminal=True)

    await runner._step_pump_fill_reconciliation(state)

    assert strategy._position_side is None  # phantom cleared → FLAT
    assert strategy.deployment_id not in runner._pending_fill_handles


@pytest.mark.asyncio
async def test_no_handle_is_noop(registered_capability) -> None:
    runner = _make_runner()
    runner._get_gateway_client = MagicMock(return_value=object())  # type: ignore[method-assign]
    strategy = _pending_strategy()
    state = _make_state(strategy)

    # No cached handle → the pump never touches the strategy or the capability.
    await runner._step_pump_fill_reconciliation(state)

    assert strategy.calls == []
    assert registered_capability.resolve_calls == 0


@pytest.mark.asyncio
async def test_non_reconciling_strategy_is_inert(registered_capability) -> None:
    runner = _make_runner()
    runner._get_gateway_client = MagicMock(return_value=object())  # type: ignore[method-assign]
    strategy = MagicMock(spec=["deployment_id", "wallet_address"])
    strategy.deployment_id = "deployment:noreconcile"
    strategy.wallet_address = "0x" + "22" * 20
    state = _make_state(strategy)

    runner._pending_fill_handles[strategy.deployment_id] = _Handle()
    registered_capability.next_verdict = FillReconciliationVerdict(status="filled", terminal=True)

    await runner._step_pump_fill_reconciliation(state)

    # A strategy without reconcile_fill: handle dropped, capability never called.
    assert registered_capability.resolve_calls == 0
    assert strategy.deployment_id not in runner._pending_fill_handles


@pytest.mark.asyncio
async def test_no_gateway_stays_pending(registered_capability) -> None:
    runner = _make_runner()
    runner._get_gateway_client = MagicMock(return_value=None)  # type: ignore[method-assign]
    strategy = _pending_strategy()
    state = _make_state(strategy)

    runner._pending_fill_handles[strategy.deployment_id] = _Handle()

    await runner._step_pump_fill_reconciliation(state)

    # Fail-closed: cannot measure → no reconcile_fill call, handle retained.
    assert strategy.calls == []
    assert strategy.deployment_id in runner._pending_fill_handles


@pytest.mark.asyncio
async def test_raising_capability_never_breaks_loop(registered_capability) -> None:
    runner = _make_runner()
    runner._get_gateway_client = MagicMock(return_value=object())  # type: ignore[method-assign]
    strategy = _pending_strategy()
    state = _make_state(strategy)

    runner._pending_fill_handles[strategy.deployment_id] = _Handle()
    registered_capability.raise_on_resolve = True

    # Must not raise; the registry swallows and returns None → stays PENDING.
    await runner._step_pump_fill_reconciliation(state)
    assert strategy.calls == []
    assert strategy.deployment_id in runner._pending_fill_handles


@pytest.mark.asyncio
async def test_resolve_read_raising_never_breaks_loop(registered_capability, monkeypatch) -> None:
    """A raise from the top-level registry resolve call is warn-only (stays PENDING).

    Distinct from ``raise_on_resolve`` (which the registry swallows internally):
    here the registry dispatch itself raises, exercising the pump's own
    ``except`` guard so a read fault can never break the iteration loop.
    """
    runner = _make_runner()
    runner._get_gateway_client = MagicMock(return_value=object())  # type: ignore[method-assign]
    strategy = _pending_strategy()
    state = _make_state(strategy)
    runner._pending_fill_handles[strategy.deployment_id] = _Handle()

    def _boom(**_kw: Any) -> Any:
        raise RuntimeError("registry dispatch failed")

    monkeypatch.setattr(STRATEGY_RUNNER_HOOK_REGISTRY, "resolve_fill_status", _boom)

    # Must not raise; handle retained (fail-closed → stays PENDING).
    await runner._step_pump_fill_reconciliation(state)
    assert strategy.calls == []
    assert strategy.deployment_id in runner._pending_fill_handles


@pytest.mark.asyncio
async def test_reconcile_fill_raising_never_breaks_loop(registered_capability) -> None:
    """A raise from ``strategy.reconcile_fill`` is warn-only and does not clear the handle."""
    runner = _make_runner()
    runner._get_gateway_client = MagicMock(return_value=object())  # type: ignore[method-assign]

    class _RaisingStrategy(_FakeStrategy):
        def reconcile_fill(self, intent_type: str, status: Any) -> None:
            raise RuntimeError("strategy reconcile blew up")

    strategy = _RaisingStrategy()
    strategy._position_side = "long"
    state = _make_state(strategy)
    runner._pending_fill_handles[strategy.deployment_id] = _Handle()
    # A terminal verdict WOULD normally clear the handle — but reconcile raises
    # first, so the pump returns early (handle retained) without crashing.
    registered_capability.next_verdict = FillReconciliationVerdict(status="filled", terminal=True)

    await runner._step_pump_fill_reconciliation(state)
    assert strategy.deployment_id in runner._pending_fill_handles


def test_capture_on_successful_open_caches_handle(registered_capability) -> None:
    runner = _make_runner()
    strategy = _pending_strategy()
    registered_capability.next_handle = _Handle()

    intent = MagicMock()
    intent.intent_type = MagicMock(value="PERP_OPEN")
    runner._maybe_capture_pending_fill_handle(strategy, intent, success=True, result=object())

    assert runner._pending_fill_handles.get(strategy.deployment_id) is registered_capability.next_handle


def test_capture_skips_failed_intent(registered_capability) -> None:
    runner = _make_runner()
    strategy = _pending_strategy()
    registered_capability.next_handle = _Handle()

    intent = MagicMock()
    intent.intent_type = MagicMock(value="PERP_OPEN")
    runner._maybe_capture_pending_fill_handle(strategy, intent, success=False, result=object())

    assert strategy.deployment_id not in runner._pending_fill_handles


def test_capture_close_clears_cached_handle(registered_capability) -> None:
    runner = _make_runner()
    strategy = _pending_strategy()
    runner._pending_fill_handles[strategy.deployment_id] = _Handle()
    registered_capability.next_handle = None  # a close returns no handle

    intent = MagicMock()
    intent.intent_type = MagicMock(value="PERP_CLOSE")
    runner._maybe_capture_pending_fill_handle(strategy, intent, success=True, result=object())

    assert strategy.deployment_id not in runner._pending_fill_handles
