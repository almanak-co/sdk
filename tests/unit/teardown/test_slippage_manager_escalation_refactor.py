"""Behavior matrix for the escalating-slippage execution stages."""

from decimal import Decimal
from unittest.mock import AsyncMock, call, patch

import pytest

from almanak.framework.teardown.config import TeardownConfig
from almanak.framework.teardown.models import ApprovalResponse, EscalationConfig, EscalationLevel
from almanak.framework.teardown.slippage_manager import EscalatingSlippageManager, ExecutionAttempt


def _level(
    level: EscalationLevel,
    slippage: str,
    *,
    auto_approve: bool = True,
    retries: int = 1,
) -> dict:
    return {
        "level": level,
        "slippage": Decimal(slippage),
        "auto_approve": auto_approve,
        "retries": retries,
    }


def _manager(*levels: dict, retry_delay_seconds: float = 0) -> EscalatingSlippageManager:
    return EscalatingSlippageManager(
        config=TeardownConfig(retry_delay_seconds=retry_delay_seconds),
        levels=list(levels) or None,
    )


def _failure(
    slippage: Decimal,
    *,
    disposition: str = "escalate",
    retryable: bool = True,
    retry_after_seconds: float | None = None,
) -> ExecutionAttempt:
    return ExecutionAttempt(
        success=False,
        slippage_used=slippage,
        error="boom",
        retryable=retryable,
        retry_after_seconds=retry_after_seconds,
        disposition=disposition,
    )


@pytest.mark.parametrize(
    ("intent_slippage", "slippages", "auto_approvals", "auto_max"),
    [
        (None, ["0.02", "0.03", "0.05", "0.08"], [True, True, False, False], "0.05"),
        (Decimal("0"), ["0.02", "0.03", "0.05", "0.08"], [True, True, False, False], "0.05"),
        (Decimal("0.02"), ["0.02", "0.03", "0.05", "0.08"], [True, True, False, False], "0.05"),
        (
            Decimal("0.06"),
            ["0.02", "0.03", "0.05", "0.06", "0.08"],
            [True, True, True, True, False],
            "0.06",
        ),
        (
            Decimal("0.50"),
            ["0.02", "0.03", "0.05", "0.08", "0.10"],
            [True, True, True, True, True],
            "0.10",
        ),
    ],
)
def test_prepare_escalation_matrix(
    intent_slippage: Decimal | None,
    slippages: list[str],
    auto_approvals: list[bool],
    auto_max: str,
) -> None:
    manager = _manager()

    plan = manager._prepare_escalation(intent_slippage)

    assert [level.slippage for level in plan.levels] == [Decimal(value) for value in slippages]
    assert [level.auto_approve for level in plan.levels] == auto_approvals
    assert plan.auto_max_slippage == Decimal(auto_max)
    assert all(prepared is not configured for prepared, configured in zip(plan.levels, manager.levels, strict=False))


def test_prepare_escalation_preserves_custom_order_without_a_positive_intent_ceiling() -> None:
    manager = _manager(
        _level(EscalationLevel.LEVEL_2, "0.03"),
        _level(EscalationLevel.LEVEL_1, "0.02"),
    )

    unmodified = manager._prepare_escalation(None)
    with_ceiling = manager._prepare_escalation(Decimal("0.01"))

    assert [level.slippage for level in unmodified.levels] == [Decimal("0.03"), Decimal("0.02")]
    assert [level.slippage for level in with_ceiling.levels] == [
        Decimal("0.01"),
        Decimal("0.02"),
        Decimal("0.03"),
    ]


@pytest.mark.parametrize(
    ("actual_slippage", "expected_final_slippage"),
    [(None, Decimal("0.02")), (Decimal("0"), Decimal("0")), (Decimal("0.007"), Decimal("0.007"))],
)
@pytest.mark.asyncio
async def test_public_success_result_preserves_measured_actual_slippage(
    actual_slippage: Decimal | None,
    expected_final_slippage: Decimal,
) -> None:
    manager = _manager(_level(EscalationLevel.LEVEL_1, "0.02"))
    attempt = ExecutionAttempt(
        success=True,
        slippage_used=Decimal("0.02"),
        actual_slippage=actual_slippage,
    )

    result = await manager.execute_with_escalation(object(), Decimal("100"), AsyncMock(return_value=attempt))

    assert result.status == "completed"
    assert result.final_slippage == expected_final_slippage
    assert result.attempts == [attempt]
    assert result.current_level is EscalationLevel.LEVEL_1


@pytest.mark.parametrize(
    ("disposition", "retryable", "expected_status", "expected_attempts", "expected_sleeps"),
    [
        ("escalate", False, "failed_non_retryable", 1, []),
        ("non_retryable", True, "failed_non_retryable", 1, []),
        ("retry_same_level", True, "failed_rpc_unreachable", 2, [call(1.25)]),
        ("escalate", True, "failed_manual_intervention_required", 2, [call(1.25), call(1.25)]),
    ],
)
@pytest.mark.asyncio
async def test_public_failure_disposition_matrix(
    disposition: str,
    retryable: bool,
    expected_status: str,
    expected_attempts: int,
    expected_sleeps: list,
) -> None:
    manager = _manager(
        _level(EscalationLevel.LEVEL_1, "0.02", retries=2),
        retry_delay_seconds=0.5,
    )

    async def execute(_intent: object, slippage: Decimal) -> ExecutionAttempt:
        return _failure(
            slippage,
            disposition=disposition,
            retryable=retryable,
            retry_after_seconds=1.25,
        )

    with patch("almanak.framework.teardown.slippage_manager.asyncio.sleep", new_callable=AsyncMock) as sleep:
        result = await manager.execute_with_escalation(object(), Decimal("100"), execute)

    assert result.status == expected_status
    assert len(result.attempts) == expected_attempts
    assert [attempt.retry_count for attempt in result.attempts] == list(range(expected_attempts))
    assert sleep.await_args_list == expected_sleeps


@pytest.mark.asyncio
async def test_public_auto_mode_stops_before_dispatching_a_rung_above_its_cap() -> None:
    manager = _manager(
        _level(EscalationLevel.LEVEL_1, "0.02"),
        _level(EscalationLevel.LEVEL_4, "0.08"),
    )
    execute = AsyncMock(side_effect=lambda _intent, slippage: _failure(slippage))

    result = await manager.execute_with_escalation(
        object(),
        Decimal("100"),
        execute,
        is_auto_mode=True,
    )

    assert result.status == "paused_auto_limit_reached"
    assert result.final_slippage == Decimal("0.05")
    assert result.current_level is EscalationLevel.LEVEL_4
    assert [await_call.args[1] for await_call in execute.await_args_list] == [Decimal("0.02")]


@pytest.mark.asyncio
async def test_public_missing_approval_callback_returns_the_complete_request() -> None:
    manager = _manager(_level(EscalationLevel.LEVEL_3, "0.05", auto_approve=False))
    execute = AsyncMock()

    result = await manager.execute_with_escalation(
        object(),
        Decimal("100"),
        execute,
        teardown_id="td-1",
        deployment_id="deployment:1",
    )

    assert result.status == "paused_awaiting_approval"
    assert result.approval_request is not None
    assert result.approval_request.teardown_id == "td-1"
    assert result.approval_request.deployment_id == "deployment:1"
    assert result.approval_request.current_slippage == Decimal("0.05")
    assert result.approval_request.estimated_loss_usd == Decimal("5.00")
    execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_public_wait_and_escalate_skips_the_declined_rung_then_awaits_the_next() -> None:
    manager = _manager(
        _level(EscalationLevel.LEVEL_3, "0.05", auto_approve=False),
        _level(EscalationLevel.LEVEL_4, "0.08", auto_approve=False),
        retry_delay_seconds=0.75,
    )
    approval = AsyncMock(
        side_effect=[
            ApprovalResponse(approved=False, teardown_id="td-1", action="wait_and_escalate"),
            ApprovalResponse(approved=True, teardown_id="td-1", action="approve"),
        ]
    )
    execute = AsyncMock(return_value=ExecutionAttempt(success=True, slippage_used=Decimal("0.08")))
    intent = object()

    with patch("almanak.framework.teardown.slippage_manager.asyncio.sleep", new_callable=AsyncMock) as sleep:
        result = await manager.execute_with_escalation(
            intent,
            Decimal("100"),
            execute,
            approval,
            teardown_id="td-1",
        )

    assert result.status == "completed"
    assert [item.args[0].current_level for item in approval.await_args_list] == [
        EscalationLevel.LEVEL_3,
        EscalationLevel.LEVEL_4,
    ]
    assert execute.await_args_list == [call(intent, Decimal("0.08"))]
    sleep.assert_awaited_once_with(1.5)


@pytest.mark.asyncio
async def test_public_declined_approval_cancels_without_dispatch() -> None:
    manager = _manager(_level(EscalationLevel.LEVEL_3, "0.05", auto_approve=False))
    approval = AsyncMock(return_value=ApprovalResponse(approved=False, teardown_id="td-1", action="cancel"))
    execute = AsyncMock()

    result = await manager.execute_with_escalation(
        object(),
        Decimal("100"),
        execute,
        approval,
        teardown_id="td-1",
    )

    assert result.status == "cancelled_by_user"
    assert result.final_slippage == Decimal("0.05")
    execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_approved_slippage_does_not_change_the_current_rung_pending_vib_6837() -> None:
    manager = _manager(_level(EscalationLevel.LEVEL_3, "0.05", auto_approve=False))
    approval = AsyncMock(
        return_value=ApprovalResponse(
            approved=True,
            teardown_id="td-1",
            approved_slippage=Decimal("0.06"),
            action="approve",
        )
    )
    execute = AsyncMock(return_value=ExecutionAttempt(success=True, slippage_used=Decimal("0.05")))

    await manager.execute_with_escalation(
        object(),
        Decimal("100"),
        execute,
        approval,
        teardown_id="td-1",
    )

    assert execute.await_args.args[1] == Decimal("0.05")


@pytest.mark.asyncio
async def test_public_callback_and_dispatch_errors_still_propagate() -> None:
    manager = _manager(_level(EscalationLevel.LEVEL_3, "0.05", auto_approve=False))

    with pytest.raises(RuntimeError, match="approval unavailable"):
        await manager.execute_with_escalation(
            object(),
            Decimal("100"),
            AsyncMock(),
            AsyncMock(side_effect=RuntimeError("approval unavailable")),
        )

    manager = _manager(_level(EscalationLevel.LEVEL_1, "0.02"))
    with pytest.raises(RuntimeError, match="dispatch unavailable"):
        await manager.execute_with_escalation(
            object(),
            Decimal("100"),
            AsyncMock(side_effect=RuntimeError("dispatch unavailable")),
        )


@pytest.mark.parametrize(
    ("success", "retryable", "disposition", "retry", "expected_status"),
    [
        (True, True, "escalate", 0, "completed"),
        (False, False, "escalate", 0, "failed_non_retryable"),
        (False, True, "non_retryable", 0, "failed_non_retryable"),
        (False, True, "retry_same_level", 0, None),
        (False, True, "retry_same_level", 1, "failed_rpc_unreachable"),
        (False, True, "escalate", 1, None),
    ],
)
def test_fold_attempt_result_helper_matrix(
    success: bool,
    retryable: bool,
    disposition: str,
    retry: int,
    expected_status: str | None,
) -> None:
    manager = _manager()
    level = EscalationConfig(
        level=EscalationLevel.LEVEL_1,
        slippage=Decimal("0.02"),
        auto_approve=True,
        retries=2,
    )
    attempt = ExecutionAttempt(
        success=success,
        slippage_used=level.slippage,
        error=None if success else "boom",
        retryable=retryable,
        disposition=disposition,
    )

    decision = manager._fold_attempt_result(
        attempt=attempt,
        level_config=level,
        retry=retry,
        attempts=[attempt],
    )

    assert decision.disposition == ("escalate" if success else disposition)
    assert (decision.result.status if decision.result is not None else None) == expected_status
