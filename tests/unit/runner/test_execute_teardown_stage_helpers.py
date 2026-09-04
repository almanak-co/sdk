from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, sentinel

import pytest

from almanak.core.lifecycle import LifecycleState
from almanak.framework.runner import runner_teardown as rt
from almanak.framework.runner.runner_models import IterationStatus
from almanak.framework.teardown.models import TeardownMode


def _runner(*, multi_chain: bool = False) -> MagicMock:
    runner = MagicMock()
    runner._is_multi_chain = multi_chain
    runner._last_cycle_id = "cycle-1"
    runner._get_gateway_client.return_value = sentinel.gateway
    runner._calculate_duration_ms.return_value = 17
    runner._execute_teardown_via_manager = AsyncMock(return_value=sentinel.manager_result)
    runner._execute_multi_chain = AsyncMock()
    runner._teardown_entry_blocked = False
    runner._teardown_entry_blocked_reason = None
    return runner


def _strategy() -> SimpleNamespace:
    return SimpleNamespace(deployment_id="deployment:test")


def _patch_entry_stages(
    monkeypatch: pytest.MonkeyPatch,
    runner: MagicMock,
    *,
    generated_intents: list[Any] | None = None,
) -> dict[str, Any]:
    from almanak.framework import teardown as teardown_package

    events: list[str] = []
    generated = generated_intents if generated_intents is not None else [sentinel.generated_intent]
    recovered = [sentinel.recovered_intent]
    guarded = [sentinel.guarded_intent]
    request = SimpleNamespace(teardown_id="td-1")
    manager = MagicMock()
    manager.get_active_request.side_effect = lambda _deployment_id: events.append("request") or request

    monkeypatch.setattr(
        teardown_package,
        "get_teardown_state_manager_for_runtime",
        lambda **_kwargs: events.append("manager") or manager,
    )

    reset = MagicMock(side_effect=lambda _runner: events.append("reset"))

    async def pre_gate(*_args: Any) -> str | None:
        events.append("pre_gate")
        return "pre-gate-degraded"

    market = MagicMock(name="market")
    create_market = MagicMock(side_effect=lambda _strategy: events.append("market") or market)

    async def generate(*_args: Any) -> rt._TeardownIntentGenerationOutcome:
        events.append("generate")
        return rt._TeardownIntentGenerationOutcome(intents=generated)

    async def recover(*_args: Any) -> rt._TeardownRecoveryOutcome:
        events.append("recover")
        assert _args[2] is generated
        return rt._TeardownRecoveryOutcome(recovered, True, "recovery warning")

    def guard(intents: Any, *_args: Any, **_kwargs: Any) -> list[Any]:
        events.append("guard")
        assert intents is recovered
        return guarded

    async def restore(*_args: Any) -> tuple[list[Any], None]:
        events.append("restore")
        assert _args[3] is guarded
        return guarded, None

    async def prepare(*_args: Any) -> int:
        events.append("prepare")
        assert _args[5] is guarded
        return 2

    async def manager_dispatch(**kwargs: Any) -> Any:
        events.append("manager_dispatch")
        assert kwargs["teardown_intents"] is guarded
        assert kwargs["teardown_market"] is market
        assert kwargs["request"] is request
        assert kwargs["state_manager"] is manager
        return sentinel.manager_result

    runner._execute_teardown_via_manager.side_effect = manager_dispatch
    mocks = {
        "reset": reset,
        "pre_gate": AsyncMock(side_effect=pre_gate),
        "create_market": create_market,
        "generate": AsyncMock(side_effect=generate),
        "recover": AsyncMock(side_effect=recover),
        "guard": MagicMock(side_effect=guard),
        "restore": AsyncMock(side_effect=restore),
        "complete_empty": AsyncMock(return_value=sentinel.no_intents_result),
        "prepare": AsyncMock(side_effect=prepare),
        "multichain": AsyncMock(return_value=sentinel.multichain_result),
    }
    monkeypatch.setattr(rt, "_reset_teardown_verification_signals", mocks["reset"])
    monkeypatch.setattr(rt, "_run_pre_teardown_settlement_accounting", mocks["pre_gate"])
    monkeypatch.setattr(rt, "_create_teardown_market", mocks["create_market"])
    monkeypatch.setattr(rt, "_generate_teardown_plan", mocks["generate"])
    monkeypatch.setattr(rt, "_recover_teardown_positions", mocks["recover"])
    monkeypatch.setattr(rt, "_apply_lending_unwind_guard", mocks["guard"])
    monkeypatch.setattr(rt, "_restore_resumable_teardown_plan", mocks["restore"])
    monkeypatch.setattr(rt, "_complete_teardown_without_intents", mocks["complete_empty"])
    monkeypatch.setattr(rt, "_prepare_teardown_dispatch", mocks["prepare"])
    monkeypatch.setattr(rt, "_execute_multichain_teardown", mocks["multichain"])
    return {
        **mocks,
        "events": events,
        "generated": generated,
        "guarded": guarded,
        "market": market,
        "request": request,
        "manager": manager,
    }


@pytest.mark.asyncio
async def test_execute_teardown_threads_stage_order_and_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _runner()
    stages = _patch_entry_stages(monkeypatch, runner)
    strategy = _strategy()
    started = datetime.now(UTC)

    result = await rt.execute_teardown(runner, strategy, TeardownMode.SOFT, started)

    assert result is sentinel.manager_result
    assert stages["events"] == [
        "reset",
        "manager",
        "request",
        "pre_gate",
        "market",
        "generate",
        "recover",
        "guard",
        "restore",
        "prepare",
        "manager_dispatch",
    ]


@pytest.mark.asyncio
async def test_execute_teardown_stops_on_plan_generation_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _runner()
    stages = _patch_entry_stages(monkeypatch, runner)
    stages["generate"].return_value = rt._TeardownIntentGenerationOutcome(
        failure_result=sentinel.generation_failure,
        failed=True,
    )
    stages["generate"].side_effect = None

    result = await rt.execute_teardown(runner, _strategy(), TeardownMode.HARD, datetime.now(UTC))

    assert result is sentinel.generation_failure
    stages["recover"].assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_teardown_defers_unproven_resume(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _runner()
    stages = _patch_entry_stages(monkeypatch, runner)
    stages["restore"].side_effect = None
    stages["restore"].return_value = (stages["guarded"], sentinel.resume_pending)

    result = await rt.execute_teardown(runner, _strategy(), TeardownMode.SOFT, datetime.now(UTC))

    assert result is sentinel.resume_pending
    stages["complete_empty"].assert_not_awaited()
    stages["prepare"].assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_teardown_routes_empty_plan_through_completeness(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _runner()
    stages = _patch_entry_stages(monkeypatch, runner, generated_intents=[])
    stages["recover"].side_effect = None
    stages["recover"].return_value = rt._TeardownRecoveryOutcome([], True, "incomplete")
    stages["guard"].side_effect = None
    stages["guard"].return_value = []
    stages["restore"].side_effect = None
    stages["restore"].return_value = ([], None)

    result = await rt.execute_teardown(runner, _strategy(), TeardownMode.SOFT, datetime.now(UTC))

    assert result is sentinel.no_intents_result
    assert stages["complete_empty"].await_args.args[6:] == (True, "incomplete", "pre-gate-degraded")
    stages["prepare"].assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_teardown_routes_multichain_after_shared_stages(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _runner(multi_chain=True)
    stages = _patch_entry_stages(monkeypatch, runner)

    result = await rt.execute_teardown(runner, _strategy(), TeardownMode.HARD, datetime.now(UTC))

    assert result is sentinel.multichain_result
    args = stages["multichain"].await_args.args
    assert args[2] is stages["guarded"]
    assert args[4] is stages["market"]
    assert args[5] is stages["manager"]
    assert args[6] is stages["request"]
    assert args[8:] == (True, "recovery warning", 2)
    runner._execute_teardown_via_manager.assert_not_awaited()


@pytest.mark.asyncio
async def test_deferred_record_failure_does_not_block_teardown(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    from almanak.framework.accounting import deferred_log
    from almanak.framework.runner import perp_settlement_reconciler

    outcome = SimpleNamespace(accounting_degraded=True, degraded_reasons=("settlement write failed",))
    monkeypatch.setattr(perp_settlement_reconciler, "reconcile_perp_settlements", AsyncMock(return_value=outcome))
    monkeypatch.setattr(deferred_log, "append", MagicMock(side_effect=OSError("deferred log unavailable")))
    runner = _runner()

    with caplog.at_level("ERROR"):
        degraded = await rt._run_pre_teardown_settlement_accounting(runner, _strategy(), "deployment:test")

    assert degraded == "settlement write failed"
    assert any("Could not persist teardown pre-gate settlement degradation" in message for message in caplog.messages)


def test_create_teardown_market_failure_is_nonblocking() -> None:
    strategy = SimpleNamespace(create_market_snapshot=MagicMock(side_effect=RuntimeError("snapshot unavailable")))

    assert rt._create_teardown_market(strategy) is None


@pytest.mark.asyncio
async def test_generate_teardown_plan_preserves_old_signature_fallback() -> None:
    intents = [sentinel.intent]
    calls: list[TeardownMode] = []

    def generate(mode: TeardownMode) -> list[Any]:
        calls.append(mode)
        return intents

    strategy = SimpleNamespace(generate_teardown_intents=generate)

    outcome = await rt._generate_teardown_plan(
        _runner(),
        strategy,
        TeardownMode.SOFT,
        sentinel.market,
        None,
        MagicMock(),
        "deployment:test",
        datetime.now(UTC),
    )

    assert outcome.failed is False
    assert outcome.intents is intents
    assert calls == [TeardownMode.SOFT]


@pytest.mark.asyncio
async def test_generate_teardown_plan_does_not_misclassify_internal_type_error() -> None:
    def generate(_mode: TeardownMode, market: Any = None) -> list[Any]:
        raise TypeError("strategy implementation failed")

    runner = _runner()
    runner._create_error_result.return_value = sentinel.failure
    outcome = await rt._generate_teardown_plan(
        runner,
        SimpleNamespace(generate_teardown_intents=generate),
        TeardownMode.SOFT,
        sentinel.market,
        None,
        MagicMock(),
        "deployment:test",
        datetime.now(UTC),
    )

    assert outcome.failed is True
    assert outcome.failure_result is sentinel.failure
    runner._request_teardown_failure_shutdown.assert_called_once_with("strategy implementation failed")


@pytest.mark.asyncio
async def test_recovery_combines_warnings_and_preserves_final_plan_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    lp_intents = [sentinel.lp]
    final_intents = [sentinel.pending]
    monkeypatch.setattr(
        rt,
        "_recover_orphaned_lp_intents",
        AsyncMock(return_value=(lp_intents, True, "LP recovery incomplete")),
    )
    monkeypatch.setattr(
        rt,
        "_recover_pending_order_intents",
        AsyncMock(return_value=(final_intents, True, "pending-order recovery incomplete")),
    )
    runner = _runner()

    outcome = await rt._recover_teardown_positions(runner, _strategy(), [], TeardownMode.HARD)

    assert outcome.intents is final_intents
    assert outcome.incomplete is True
    assert outcome.warning == "LP recovery incomplete; pending-order recovery incomplete"
    assert runner._teardown_recovery_warning == outcome.warning
    assert runner._teardown_lp_recovery_warning == "LP recovery incomplete"


@pytest.mark.asyncio
async def test_restore_resumable_plan_returns_pending_result_when_lookup_is_unproven(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lookup = rt._AcceptedAsyncResumeLookup(blocked_reason="ledger read unmeasured")
    monkeypatch.setattr(rt, "_load_runtime_resumable_accepted_async_state", AsyncMock(return_value=lookup))
    pending_result = MagicMock(return_value=sentinel.pending)
    monkeypatch.setattr(rt, "_accepted_async_recovery_pending_result", pending_result)
    runner = _runner()
    started = datetime.now(UTC)

    intents, result = await rt._restore_resumable_teardown_plan(runner, sentinel.manager, "dep", [], started)

    assert intents == []
    assert result is sentinel.pending
    pending_result.assert_called_once_with(runner, "dep", started, "ledger read unmeasured")


@pytest.mark.parametrize("persisted_plan", ["{invalid", "{}", "null"])
@pytest.mark.asyncio
async def test_restore_resumable_plan_defers_malformed_persisted_plan(
    monkeypatch: pytest.MonkeyPatch,
    persisted_plan: str,
) -> None:
    lookup = rt._AcceptedAsyncResumeLookup(state=SimpleNamespace(pending_intents_json=persisted_plan))
    monkeypatch.setattr(rt, "_load_runtime_resumable_accepted_async_state", AsyncMock(return_value=lookup))
    pending_result = MagicMock(return_value=sentinel.pending)
    monkeypatch.setattr(rt, "_accepted_async_recovery_pending_result", pending_result)
    runner = _runner()
    started = datetime.now(UTC)

    intents, result = await rt._restore_resumable_teardown_plan(runner, sentinel.manager, "dep", [], started)

    assert intents == []
    assert result is sentinel.pending
    pending_result.assert_called_once_with(
        runner,
        "dep",
        started,
        "the persisted teardown plan could not be parsed",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("completeness", "recovery_incomplete", "warning", "active_request", "error_fragment"),
    [
        (None, False, None, sentinel.request, "could not read the known open-position set"),
        (None, False, None, None, "could not read the known open-position set"),
        (SimpleNamespace(complete=True), True, "recovery unmeasured", sentinel.request, "recovery unmeasured"),
    ],
)
async def test_no_intent_completion_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    completeness: Any,
    recovery_incomplete: bool,
    warning: str | None,
    active_request: Any,
    error_fragment: str,
) -> None:
    monkeypatch.setattr(rt, "_check_no_intent_completeness", AsyncMock(return_value=completeness))
    runner = _runner()
    runner._create_error_result.return_value = sentinel.failure
    manager = MagicMock()

    result = await rt._complete_teardown_without_intents(
        runner,
        _strategy(),
        manager,
        active_request,
        "deployment:test",
        datetime.now(UTC),
        recovery_incomplete,
        warning,
        None,
    )

    assert result is sentinel.failure
    if active_request is None:
        manager.mark_failed.assert_not_called()
        error = runner._request_teardown_failure_shutdown.call_args.args[0]
    else:
        error = manager.mark_failed.call_args.kwargs["error"]
    assert error_fragment in error
    runner._request_teardown_failure_shutdown.assert_called_once_with(error)
    runner._record_success.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("position_count", "active_request", "market"),
    [(2, sentinel.request, sentinel.market), (0, sentinel.request, None), (None, None, None)],
)
async def test_prepare_dispatch_preserves_position_count_fallback_and_nonblocking_prefetch(
    monkeypatch: pytest.MonkeyPatch,
    position_count: int | None,
    active_request: Any,
    market: Any,
) -> None:
    intents = [sentinel.intent_a, sentinel.intent_b, sentinel.intent_c]
    monkeypatch.setattr(rt, "_count_open_positions", AsyncMock(return_value=position_count))
    monkeypatch.setattr(rt, "reconcile_known_positions", AsyncMock(return_value=sentinel.reconciliation))
    decision_log = MagicMock()
    monkeypatch.setattr(rt, "log_teardown_decision", decision_log)
    prefetch = MagicMock(side_effect=RuntimeError("price unavailable"))
    monkeypatch.setattr(rt, "prefetch_teardown_prices", prefetch)
    runner = _runner()
    manager = MagicMock()
    if active_request is sentinel.request:
        active_request = SimpleNamespace(teardown_id="td-1")
    if market is sentinel.market:
        market = SimpleNamespace(price=MagicMock())

    count = await rt._prepare_teardown_dispatch(
        runner,
        _strategy(),
        manager,
        active_request,
        "deployment:test",
        intents,
        market,
    )

    assert count == position_count
    runner._lifecycle_write_state.assert_called_once_with("deployment:test", LifecycleState.TEARING_DOWN)
    if active_request is None:
        manager.mark_started.assert_not_called()
        prefetch.assert_not_called()
    else:
        expected_total = position_count if position_count is not None else len(intents)
        manager.mark_started.assert_called_once_with("deployment:test", total_positions=expected_total)
        if market is None:
            prefetch.assert_not_called()
        else:
            prefetch.assert_called_once_with(market, intents)
    assert decision_log.call_args.kwargs["position_count"] == position_count
    assert decision_log.call_args.kwargs["intent_count"] == len(intents)
    assert runner._teardown_reconciliation is sentinel.reconciliation


def _multichain_result(success: bool, error: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(success=success, status=IterationStatus.SUCCESS, error=error)


@pytest.mark.asyncio
async def test_multichain_success_persists_position_counts() -> None:
    intents = [sentinel.first, sentinel.second]
    result = _multichain_result(True)
    runner = _runner(multi_chain=True)
    runner._execute_multi_chain.return_value = result
    manager = MagicMock()

    returned = await rt._execute_multichain_teardown(
        runner,
        _strategy(),
        intents,
        datetime.now(UTC),
        sentinel.market,
        manager,
        sentinel.request,
        "deployment:test",
        False,
        None,
        1,
    )

    assert returned is result
    assert result.status == IterationStatus.TEARDOWN
    assert runner._execute_multi_chain.await_args.kwargs["intents"] is intents
    assert runner._execute_multi_chain.await_args.kwargs["market"] is sentinel.market
    assert manager.mark_completed.call_args.kwargs["result"] == rt._positions_completion_result(1, 2)


@pytest.mark.asyncio
async def test_multichain_request_backed_incomplete_recovery_latches_failure() -> None:
    runner = _runner(multi_chain=True)
    runner._execute_multi_chain.return_value = _multichain_result(True)
    manager = MagicMock()

    await rt._execute_multichain_teardown(
        runner,
        _strategy(),
        [sentinel.intent],
        datetime.now(UTC),
        None,
        manager,
        sentinel.request,
        "deployment:test",
        True,
        "recovery incomplete",
        1,
    )

    manager.mark_failed.assert_called_once_with("deployment:test", error="recovery incomplete")
    assert runner._teardown_entry_blocked is True
    assert runner._teardown_entry_blocked_reason == "teardown failed — recovery incomplete"


@pytest.mark.asyncio
async def test_multichain_self_signalled_incomplete_recovery_remains_success_pending_vib6832() -> None:
    runner = _runner(multi_chain=True)
    result = _multichain_result(True)
    runner._execute_multi_chain.return_value = result
    manager = MagicMock()

    returned = await rt._execute_multichain_teardown(
        runner,
        _strategy(),
        [sentinel.intent],
        datetime.now(UTC),
        None,
        manager,
        None,
        "deployment:test",
        True,
        "recovery incomplete",
        1,
    )

    assert returned is result
    assert result.status == IterationStatus.TEARDOWN
    runner.request_shutdown.assert_called_once()
    manager.mark_failed.assert_not_called()
    assert runner._teardown_entry_blocked is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("active_request", "annotated", "expected"),
    [(sentinel.request, "decoded", "decoded"), (None, None, "multi-chain teardown execution failed")],
)
async def test_multichain_failure_preserves_annotation_and_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    active_request: Any,
    annotated: str | None,
    expected: str,
) -> None:
    from almanak.framework.teardown import revert_hints

    monkeypatch.setattr(revert_hints, "annotate_teardown_error", MagicMock(return_value=annotated))
    runner = _runner(multi_chain=True)
    result = _multichain_result(False, "raw revert")
    runner._execute_multi_chain.return_value = result
    manager = MagicMock()

    returned = await rt._execute_multichain_teardown(
        runner,
        _strategy(),
        [sentinel.intent],
        datetime.now(UTC),
        None,
        manager,
        active_request,
        "deployment:test",
        False,
        None,
        None,
    )

    assert returned is result
    runner._request_teardown_failure_shutdown.assert_called_once_with(expected)
    if active_request is None:
        manager.mark_failed.assert_not_called()
    else:
        manager.mark_failed.assert_called_once_with("deployment:test", error=expected)
