from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from almanak.core.lifecycle import LifecycleState
from almanak.framework.runner import runner_teardown as rt
from almanak.framework.runner.runner_models import IterationResult, IterationStatus
from almanak.framework.state.exceptions import AccountingPersistenceError


def _intent(name: str, *, token: str | None = None) -> SimpleNamespace:
    values: dict[str, Any] = {
        "intent_type": SimpleNamespace(value=name),
        "chain": "arbitrum",
    }
    if token is not None:
        values["from_token"] = token
    return SimpleNamespace(**values)


def _runner() -> SimpleNamespace:
    return SimpleNamespace(
        _execute_single_chain=AsyncMock(),
        _calculate_duration_ms=MagicMock(return_value=11),
        _request_teardown_failure_shutdown=MagicMock(),
        _lifecycle_write_state=MagicMock(),
        _record_success=MagicMock(),
        request_shutdown=MagicMock(),
        _vault_lifecycle=None,
        _last_cycle_id="iteration-1",
        state_manager=None,
    )


def _strategy() -> SimpleNamespace:
    return SimpleNamespace(
        deployment_id="deployment:test",
        chain="arbitrum",
        wallet_address="0x1234",
    )


async def _run_body(
    runner: Any,
    intents: list[Any],
    *,
    market: Any = None,
    request: Any = None,
    state_manager: Any = None,
    deferred_append: Any = None,
) -> tuple[IterationResult, int]:
    return await rt._execute_teardown_inline_body(
        runner,
        _strategy(),
        intents,
        market,
        datetime.now(UTC),
        request,
        state_manager,
        teardown_cycle_id="teardown-test",
        deferred_append=deferred_append or MagicMock(),
    )


@pytest.mark.asyncio
async def test_accounting_failure_is_deferred_then_chain_failure_stops_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    intents = [_intent("LP_CLOSE"), _intent("SWAP"), _intent("WITHDRAW")]
    failed = IterationResult(
        status=IterationStatus.EXECUTION_FAILED,
        intent=intents[1],
        error="swap reverted",
        deployment_id="deployment:test",
    )
    runner = _runner()
    runner._execute_single_chain.side_effect = [
        AccountingPersistenceError("ledger", deployment_id="deployment:test", message="ledger unavailable"),
        failed,
    ]
    monkeypatch.setattr(rt.Intent, "has_chained_amount", MagicMock(return_value=False))
    monkeypatch.setattr(rt, "_count_open_positions", AsyncMock(return_value=2))
    deferred_calls: list[dict[str, Any]] = []

    def unavailable_deferred_log(**kwargs: Any) -> None:
        deferred_calls.append(kwargs)
        raise OSError("deferred log unavailable")

    state_manager = MagicMock()
    result, degraded = await _run_body(
        runner,
        intents,
        request=SimpleNamespace(teardown_id="test"),
        state_manager=state_manager,
        deferred_append=unavailable_deferred_log,
    )

    assert result is failed
    assert degraded == 1
    assert [entry.kwargs["intent"] for entry in runner._execute_single_chain.await_args_list] == intents[:2]
    assert deferred_calls == [
        {
            "kind": "ledger",
            "deployment_id": "deployment:test",
            "cycle_id": "teardown-test",
            "intent_type": "LP_CLOSE",
            "error": "ledger unavailable",
            "extra": {"phase": "inline-per-intent"},
        }
    ]
    state_manager.mark_completed.assert_not_called()
    state_manager.mark_failed.assert_called_once_with("deployment:test", error="swap reverted")
    runner._request_teardown_failure_shutdown.assert_called_once_with("swap reverted")


@pytest.mark.asyncio
async def test_unmeasured_accounting_clamp_skips_swap_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    swap = _intent("SWAP", token="WETH")
    close = _intent("LP_CLOSE")
    runner = _runner()
    success = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=close,
        deployment_id="deployment:test",
    )
    runner._execute_single_chain.return_value = success
    market = MagicMock()
    market.balance.return_value = Decimal("1.5")
    monkeypatch.setattr(rt.Intent, "has_chained_amount", MagicMock(side_effect=lambda intent: intent is swap))
    monkeypatch.setattr(rt.Intent, "set_resolved_amount", MagicMock())
    monkeypatch.setattr(rt, "_count_open_positions", AsyncMock(return_value=None))
    monkeypatch.setattr(rt, "warn_if_sweep_non_strategy_balance", MagicMock())
    from almanak.framework.teardown import swap_clamp

    monkeypatch.setattr(swap_clamp, "read_tracked_swap_inventory", MagicMock(return_value=None))

    result, degraded = await _run_body(runner, [swap, close], market=market)

    assert result.status is IterationStatus.TEARDOWN
    assert degraded == 1
    runner._execute_single_chain.assert_awaited_once()
    assert runner._execute_single_chain.await_args.kwargs["intent"] is close
    rt.Intent.set_resolved_amount.assert_not_called()
    market.invalidate_balance.assert_called_once_with("WETH")


@pytest.mark.asyncio
async def test_single_chain_balance_retry_failure_is_a_loud_compilation_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    swap = _intent("SWAP", token="WETH")
    later = _intent("WITHDRAW")
    runner = _runner()
    market = MagicMock()
    market.balance.side_effect = [TypeError("single-chain signature"), RuntimeError("RPC unavailable")]
    monkeypatch.setattr(rt.Intent, "has_chained_amount", MagicMock(side_effect=lambda intent: intent is swap))
    monkeypatch.setattr(rt, "_count_open_positions", AsyncMock(return_value=1))
    state_manager = MagicMock()

    result, degraded = await _run_body(
        runner,
        [swap, later],
        market=market,
        request=SimpleNamespace(teardown_id="test"),
        state_manager=state_manager,
    )

    assert result.status is IterationStatus.COMPILATION_FAILED
    assert result.error == "Cannot resolve amount='all' for WETH: RPC unavailable"
    assert degraded == 0
    assert market.balance.call_args_list == [call("WETH", "arbitrum"), call("WETH")]
    runner._execute_single_chain.assert_not_awaited()
    state_manager.mark_failed.assert_called_once_with("deployment:test", error=result.error)
    runner._request_teardown_failure_shutdown.assert_called_once_with(result.error)


@pytest.mark.parametrize("invalidation", ["unavailable", "raises"])
def test_prepare_uses_single_chain_balance_when_invalidation_cannot_run(
    monkeypatch: pytest.MonkeyPatch,
    invalidation: str,
) -> None:
    intent = _intent("SWAP", token="WETH")
    intent.chain = None
    resolved_intent = _intent("SWAP", token="WETH")
    market = SimpleNamespace(balance=MagicMock(return_value=Decimal("2")))
    if invalidation == "raises":
        market.invalidate_balance = MagicMock(side_effect=RuntimeError("cache unavailable"))
    monkeypatch.setattr(rt.Intent, "has_chained_amount", MagicMock(return_value=True))
    monkeypatch.setattr(rt.Intent, "set_resolved_amount", MagicMock(return_value=resolved_intent))
    monkeypatch.setattr(rt, "_apply_inline_swap_clamp", MagicMock(return_value=(False, False, Decimal("2"))))
    monkeypatch.setattr(rt, "warn_if_sweep_non_strategy_balance", MagicMock())

    prepared = rt._prepare_inline_teardown_intent(
        _runner(),
        _strategy(),
        intent,
        market,
        datetime.now(UTC),
        0,
    )

    assert prepared == rt._InlinePreparedIntent(resolved_intent)
    market.balance.assert_called_once_with("WETH")
    rt.Intent.set_resolved_amount.assert_called_once_with(intent, Decimal("2"))


def test_finalize_without_execution_marks_request_completed() -> None:
    runner = _runner()
    state_manager = MagicMock()

    result = rt._finalize_inline_teardown(
        runner,
        _strategy(),
        [],
        datetime.now(UTC),
        SimpleNamespace(teardown_id="test"),
        state_manager,
        0,
        rt._InlineDispatchOutcome(),
    )

    assert result.status is IterationStatus.TEARDOWN
    state_manager.mark_completed.assert_called_once_with(
        "deployment:test",
        result={"reason": "all_positions_already_closed"},
    )
    runner._lifecycle_write_state.assert_called_once_with("deployment:test", LifecycleState.TERMINATED)


def test_finalize_vault_without_execution_fails_without_request_state() -> None:
    runner = _runner()
    runner._vault_lifecycle = object()
    state_manager = MagicMock()

    result = rt._finalize_inline_teardown(
        runner,
        _strategy(),
        [],
        datetime.now(UTC),
        None,
        state_manager,
        None,
        rt._InlineDispatchOutcome(),
    )

    assert result.status is IterationStatus.EXECUTION_FAILED
    assert "vault is still OPEN" in (result.error or "")
    state_manager.mark_failed.assert_not_called()
    runner._request_teardown_failure_shutdown.assert_called_once_with(result.error)


@pytest.mark.asyncio
async def test_public_inline_wrapper_matches_body_and_keeps_snapshot_brackets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from almanak.framework.teardown import runner_helpers, single_close_guard

    intent = _intent("LP_CLOSE")
    started = datetime.now(UTC)
    request = SimpleNamespace(teardown_id="public")
    monkeypatch.setattr(rt.Intent, "has_chained_amount", MagicMock(return_value=False))
    monkeypatch.setattr(rt, "_count_open_positions", AsyncMock(return_value=1))
    monkeypatch.setattr(
        single_close_guard,
        "collapse_duplicate_perp_closes",
        MagicMock(return_value=SimpleNamespace(dispatch=[intent])),
    )
    capture_snapshot = AsyncMock(
        side_effect=[
            SimpleNamespace(accounting_degraded=False, degraded_reason=None),
            SimpleNamespace(accounting_degraded=False, degraded_reason=None),
        ]
    )
    monkeypatch.setattr(
        runner_helpers,
        "build_runner_helpers",
        MagicMock(return_value=SimpleNamespace(has_snapshot=True, capture_snapshot=capture_snapshot)),
    )

    body_runner = _runner()
    body_runner._execute_single_chain.return_value = IterationResult(
        status=IterationStatus.SUCCESS,
        intent=intent,
        deployment_id="deployment:test",
    )
    body_state = MagicMock()
    body_result, body_degraded = await rt._execute_teardown_inline_body(
        body_runner,
        _strategy(),
        [intent],
        None,
        started,
        request,
        body_state,
        teardown_cycle_id="teardown-public",
        deferred_append=MagicMock(),
    )

    public_runner = _runner()
    executed_cycle_ids: list[str] = []

    async def execute_public(**kwargs: Any) -> IterationResult:
        executed_cycle_ids.append(public_runner._last_cycle_id)
        return IterationResult(
            status=IterationStatus.SUCCESS,
            intent=kwargs["intent"],
            deployment_id="deployment:test",
        )

    public_runner._execute_single_chain.side_effect = execute_public
    public_state = MagicMock()
    public_result = await rt.execute_teardown_inline(
        public_runner,
        _strategy(),
        [intent],
        None,
        started,
        request,
        public_state,
    )

    assert public_result.status is body_result.status is IterationStatus.TEARDOWN
    assert body_degraded == 0
    assert public_result.intent is body_result.intent is intent
    assert public_state.mark_completed.call_args.kwargs == body_state.mark_completed.call_args.kwargs
    assert capture_snapshot.await_args_list == [
        call(_strategy(), teardown_cycle_id="teardown-public", pre_teardown=True),
        call(_strategy(), teardown_cycle_id="teardown-public", pre_teardown=False),
    ]
    assert executed_cycle_ids == ["teardown-public"]
    assert public_runner._last_cycle_id == "iteration-1"
    public_runner._lifecycle_write_state.assert_called_once_with("deployment:test", LifecycleState.TERMINATED)
