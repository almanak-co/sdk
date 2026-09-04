"""Direct branch and public-contract tests for ``TeardownManager.execute`` stages."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.teardown.completeness import CompletenessReport
from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownPositionSummary,
    TeardownResult,
    TeardownState,
    TeardownStatus,
    VerificationStatus,
)
from almanak.framework.teardown.teardown_manager import (
    TeardownManager,
    _ExecuteDispatch,
    _ExecutePlan,
)


def _strategy(intents: list[object] | None = None) -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "stage-test"
    strategy.name = "Stage Test"
    strategy.chain = "arbitrum"
    strategy.uses_safe_wallet = False
    strategy.pause = AsyncMock()
    strategy.get_open_positions.return_value = _positions()
    strategy.generate_teardown_intents.return_value = intents or []
    return strategy


def _positions() -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id="stage-test",
        timestamp=datetime.now(UTC),
    )


def _intent() -> MagicMock:
    intent = MagicMock()
    intent.intent_type = "SWAP"
    intent.chain = "arbitrum"
    intent.to_dict.return_value = {"type": "swap", "chain": "arbitrum"}
    return intent


def _state() -> TeardownState:
    now = datetime.now(UTC)
    return TeardownState(
        teardown_id="td_contract",
        deployment_id="stage-test",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=1,
        completed_intents=0,
        current_intent_index=0,
        started_at=now,
        updated_at=now,
    )


def _result(*, success: bool = True) -> TeardownResult:
    now = datetime.now(UTC)
    return TeardownResult(
        success=success,
        deployment_id="stage-test",
        mode="graceful",
        started_at=now,
        completed_at=now,
        duration_seconds=0,
        intents_total=1,
        intents_succeeded=int(success),
        intents_failed=int(not success),
        starting_value_usd=Decimal("1"),
        final_value_usd=Decimal("1"),
        total_costs_usd=Decimal("0"),
        final_balances={},
    )


@pytest.mark.asyncio
async def test_execute_routes_the_public_happy_path_through_stages_in_order():
    manager = TeardownManager()
    strategy = _strategy()
    positions = _positions()
    plan = _ExecutePlan(positions=positions, intents=[object()], completeness=CompletenessReport())
    state = _state()
    result = _result()
    dispatch = _ExecuteDispatch(result=result, price_oracle={}, pre_teardown_reconciliation=None)
    stages: list[str] = []

    manager._start_execute = AsyncMock(side_effect=lambda *_args: stages.append("start"))
    manager._discover_execute_positions = MagicMock(
        side_effect=lambda *_args: (stages.append("discover"), positions)[1]
    )
    manager._plan_execute_intents = MagicMock(side_effect=lambda *_args: (stages.append("plan"), plan)[1])
    manager._resolve_empty_execute_plan = MagicMock(side_effect=lambda *_args: (stages.append("empty_gate"), None)[1])
    manager._approve_execute_plan = AsyncMock(side_effect=lambda *_args: (stages.append("approve"), state)[1])
    manager._dispatch_execute_plan = AsyncMock(side_effect=lambda *_args: (stages.append("dispatch"), dispatch)[1])
    manager._verify_execute_result = AsyncMock(side_effect=lambda *_args: (stages.append("verify"), result)[1])
    manager._consolidate_execute_result = AsyncMock(
        side_effect=lambda *_args: (stages.append("consolidate"), result)[1]
    )
    manager._finalize_execute_result = AsyncMock(side_effect=lambda *_args: stages.append("finalize"))

    actual = await manager.execute(strategy, "graceful", teardown_id="td_contract")

    assert actual is result
    assert stages == [
        "start",
        "discover",
        "plan",
        "empty_gate",
        "approve",
        "dispatch",
        "verify",
        "consolidate",
        "finalize",
    ]


@pytest.mark.asyncio
async def test_execute_does_not_retry_an_internal_typeerror_that_mentions_market():
    strategy = _strategy()
    calls = 0

    def fail_generation(_mode, **_kwargs):
        nonlocal calls
        calls += 1
        raise TypeError("market snapshot is malformed")

    strategy.generate_teardown_intents = fail_generation

    result = await TeardownManager().execute(strategy, "graceful", market=object())

    assert result.success is False
    assert result.error == "market snapshot is malformed"
    assert calls == 1


@pytest.mark.asyncio
async def test_execute_safety_rejection_precedes_state_persistence_and_cancel_window():
    manager = TeardownManager()
    strategy = _strategy([_intent()])
    manager.safety_guard.validate_teardown_request = MagicMock(
        return_value=MagicMock(all_passed=False, blocked_reason="unsafe request")
    )
    manager._persist_state = AsyncMock()
    manager.cancel_window.run_cancel_window = AsyncMock()
    manager._dispatch_execute_plan = AsyncMock()

    result = await manager.execute(strategy, "graceful")

    assert result.success is False
    assert result.error == "unsafe request"
    manager._persist_state.assert_not_awaited()
    manager.cancel_window.run_cancel_window.assert_not_awaited()
    manager._dispatch_execute_plan.assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_cancel_preserves_current_cancel_window_state_contract():
    state_manager = MagicMock()
    state_manager.save_teardown_state = AsyncMock()
    state_manager.delete_teardown_state = AsyncMock()
    manager = TeardownManager(state_manager=state_manager)
    strategy = _strategy([_intent()])
    manager.safety_guard.validate_teardown_request = MagicMock(return_value=MagicMock(all_passed=True))
    manager.cancel_window.run_cancel_window = AsyncMock(return_value=MagicMock(was_cancelled=True))
    manager._dispatch_execute_plan = AsyncMock()

    result = await manager.execute(strategy, "graceful", teardown_id="td_cancel")

    assert result.success is False
    assert result.error == "Cancelled by user"
    assert state_manager.save_teardown_state.await_count == 1
    persisted = state_manager.save_teardown_state.await_args.args[0]
    assert persisted.status == TeardownStatus.CANCEL_WINDOW
    state_manager.delete_teardown_state.assert_not_awaited()
    manager._dispatch_execute_plan.assert_not_awaited()


@pytest.mark.asyncio
async def test_verification_and_consolidation_skip_nested_work_after_execution_failure():
    manager = TeardownManager()
    strategy = _strategy()
    plan = _ExecutePlan(positions=_positions(), intents=[object()], completeness=CompletenessReport())
    failed = _result(success=False)
    dispatch = _ExecuteDispatch(result=failed, price_oracle={}, pre_teardown_reconciliation=None)
    manager._collect_execute_verification = AsyncMock()
    manager.run_token_consolidation = AsyncMock()

    verified = await manager._verify_execute_result(
        strategy,
        "td_contract",
        _state(),
        plan,
        dispatch,
        market=None,
        precomputed_positions=None,
    )
    consolidated = await manager._consolidate_execute_result(
        strategy,
        "td_contract",
        _state(),
        TeardownMode.SOFT,
        plan,
        dispatch,
        verified,
        market=None,
        is_auto_mode=False,
        on_approval_needed=None,
    )

    assert verified is failed
    assert consolidated is failed
    manager._collect_execute_verification.assert_not_awaited()
    manager.run_token_consolidation.assert_not_awaited()


@pytest.mark.asyncio
async def test_verification_exception_is_folded_before_chain_verification():
    manager = TeardownManager()
    strategy = _strategy()
    plan = _ExecutePlan(positions=_positions(), intents=[object()], completeness=CompletenessReport())
    dispatch = _ExecuteDispatch(result=_result(), price_oracle={}, pre_teardown_reconciliation=None)
    manager._verify_closure_detailed = AsyncMock(side_effect=RuntimeError("reader unavailable"))

    async def preserve_verification(_strategy, *, verification, **_kwargs):
        return verification

    manager.verify_closure_against_chain = AsyncMock(side_effect=preserve_verification)

    verification, error = await manager._collect_execute_verification(
        strategy,
        plan,
        dispatch,
        market=None,
        precomputed_positions=None,
    )

    assert verification.all_closed is False
    assert verification.verification_status == VerificationStatus.FAILED
    assert error == "Post-teardown verification error: reader unavailable. Manual check required."
    manager.verify_closure_against_chain.assert_awaited_once()


@pytest.mark.asyncio
async def test_lifecycle_callbacks_preserve_pause_alert_complete_cleanup_order():
    events: list[str] = []
    strategy = _strategy()
    strategy.pause = AsyncMock(side_effect=lambda: events.append("pause"))
    alert_manager = MagicMock()
    alert_manager.send_teardown_started = AsyncMock(side_effect=lambda *_args: events.append("started"))
    alert_manager.send_teardown_complete = AsyncMock(side_effect=lambda *_args: events.append("complete"))
    state_manager = MagicMock()
    state_manager.delete_teardown_state = AsyncMock(side_effect=lambda *_args: events.append("delete"))
    manager = TeardownManager(state_manager=state_manager, alert_manager=alert_manager)

    await manager._start_execute(strategy, "graceful", "td_contract")
    await manager._finalize_execute_result("td_contract", _result())

    assert events == ["pause", "started", "complete", "delete"]


@pytest.mark.asyncio
async def test_failed_final_result_is_alerted_but_not_deleted():
    alert_manager = MagicMock()
    alert_manager.send_teardown_complete = AsyncMock()
    state_manager = MagicMock()
    state_manager.delete_teardown_state = AsyncMock()
    manager = TeardownManager(state_manager=state_manager, alert_manager=alert_manager)
    result = _result(success=False)

    await manager._finalize_execute_result("td_contract", result)

    alert_manager.send_teardown_complete.assert_awaited_once_with(result)
    state_manager.delete_teardown_state.assert_not_awaited()
