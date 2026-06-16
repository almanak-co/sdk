"""Tests for the post-resume side-state reconciliation guardrail (VIB-5155 / ALM-2719).

The runner calls a strategy's optional ``reconcile_resumed_state(market)`` hook
exactly once, before the first ``decide()``, so a strategy that caches a
position-side flag can re-derive it from live balance. A stale/false flag must
never HOLD-lock a valid risk-off exit.

These tests pin the framework contract:
- The hook runs exactly once per process.
- It is warn-only: it never early-exits the iteration and never raises out.
- A corrected desync (hook returns True) logs a WARNING + emits a forensic event.
- A None return (base-class default) is a silent no-op.
- A raising hook is swallowed and does not break the loop.
"""

from unittest.mock import MagicMock

import pytest

from almanak.framework.runner.strategy_runner import (
    RunIterationState,
    RunnerConfig,
    StrategyRunner,
)


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


def _make_state(strategy: MagicMock) -> RunIterationState:
    from datetime import UTC, datetime

    state = RunIterationState(
        strategy=strategy,
        deployment_id=strategy.deployment_id,
        start_time=datetime.now(UTC),
    )
    state.market = MagicMock()
    return state


@pytest.mark.asyncio
async def test_hook_called_once_with_market() -> None:
    runner = _make_runner()
    strategy = MagicMock()
    strategy.deployment_id = "deployment:abc"
    strategy.reconcile_resumed_state.return_value = False
    state = _make_state(strategy)

    await runner._step_reconcile_resumed_state(state)
    # Second iteration must NOT call the hook again.
    await runner._step_reconcile_resumed_state(state)

    strategy.reconcile_resumed_state.assert_called_once_with(state.market)
    assert runner._resume_state_reconciled is True


@pytest.mark.asyncio
async def test_corrected_desync_warns_and_emits_event(caplog: pytest.LogCaptureFixture) -> None:
    runner = _make_runner()
    strategy = MagicMock()
    strategy.deployment_id = "deployment:abc"
    strategy.reconcile_resumed_state.return_value = True  # desync corrected
    state = _make_state(strategy)

    emitted: list[dict] = []

    import almanak.framework.observability.emitter as emitter_mod

    original = emitter_mod.emit_phase_event

    def _capture(**kwargs):
        emitted.append(kwargs)

    emitter_mod.emit_phase_event = _capture
    try:
        with caplog.at_level("WARNING"):
            await runner._step_reconcile_resumed_state(state)
    finally:
        emitter_mod.emit_phase_event = original

    assert any("disagreed with live on-chain balance" in r.message for r in caplog.records)
    assert len(emitted) == 1
    assert emitted[0]["event_type"] == "STATE_CHANGE"
    assert emitted[0]["details"]["reconciled"] is True


@pytest.mark.asyncio
async def test_none_return_is_silent_noop(caplog: pytest.LogCaptureFixture) -> None:
    runner = _make_runner()
    strategy = MagicMock()
    strategy.deployment_id = "deployment:abc"
    strategy.reconcile_resumed_state.return_value = None  # base-class default
    state = _make_state(strategy)

    with caplog.at_level("WARNING"):
        await runner._step_reconcile_resumed_state(state)

    assert not any("reconcil" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_raising_hook_is_swallowed(caplog: pytest.LogCaptureFixture) -> None:
    runner = _make_runner()
    strategy = MagicMock()
    strategy.deployment_id = "deployment:abc"
    strategy.reconcile_resumed_state.side_effect = RuntimeError("boom")
    state = _make_state(strategy)

    # Must not raise; guardrail must never break the loop.
    with caplog.at_level("WARNING"):
        await runner._step_reconcile_resumed_state(state)
    assert runner._resume_state_reconciled is True
    # Warn-only contract: a raising hook is logged at WARNING, not silently dropped.
    assert any("reconciliation hook raised" in r.message and r.levelname == "WARNING" for r in caplog.records)


@pytest.mark.asyncio
async def test_missing_hook_is_noop() -> None:
    runner = _make_runner()
    # A strategy that never defined the hook (older user strategy).
    strategy = MagicMock(spec=["deployment_id"])
    strategy.deployment_id = "deployment:abc"
    state = _make_state(strategy)

    await runner._step_reconcile_resumed_state(state)
    assert runner._resume_state_reconciled is True
