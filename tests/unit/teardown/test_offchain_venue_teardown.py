"""Teardown dispatches off-chain venue bundles to the connector's handler, still paired with commit."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from eth_account import Account

from almanak.connectors.aster_perps.compiler import AsterPerpsCompiler
from almanak.connectors.aster_perps.execution import ASTER_WITHDRAW_KEY, AsterOrderHandler
from almanak.connectors.aster_perps.proto import aster_perps_pb2
from almanak.framework.intents.vocabulary import Intent
from almanak.framework.teardown.teardown_manager import TeardownManager, _IntentAttemptState
from tests.unit.connectors.aster_perps.test_aster_pro_funding import _ctx

MAIN = Account.create()


class _Gateway:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def withdraw(
        self, *, asset: str, amount: str, wallet_address: str, client_request_id: str = ""
    ) -> aster_perps_pb2.AsterWithdrawResponse:
        self.calls.append({"asset": asset, "amount": amount})
        return aster_perps_pb2.AsterWithdrawResponse(
            success=True, withdraw_id="77", amount="1.99", fee="0.11", receiver=MAIN.address
        )


def _manager(handler_for) -> TeardownManager:
    manager = object.__new__(TeardownManager)
    manager.orchestrator = SimpleNamespace(execute=AsyncMock())
    manager.offchain_handler_for = handler_for
    manager.runner_helpers = SimpleNamespace(
        has_commit=True,
        has_per_intent_balances=False,
        commit=AsyncMock(return_value=SimpleNamespace(accounting_degraded=False)),
    )
    manager._capture_pre_attempt_snapshots = AsyncMock(return_value=(None, None, None, None))
    manager._capture_inventory_exit = MagicMock(return_value=None)
    manager._complete_inventory_exit = MagicMock()
    manager._prepare_async_submission = MagicMock(return_value=(None, None))
    return manager


def _withdraw_all_bundle():
    intent = Intent.perp_withdraw(amount="all", asset="USDT", protocol="aster_perps", chain="bsc")
    return intent, AsterPerpsCompiler().compile(_ctx(), intent)


async def _attempt(manager: TeardownManager, intent, compilation):
    return await manager._execute_and_commit_attempt(
        SimpleNamespace(deployment_id="dep-1"),
        intent,
        compilation,
        SimpleNamespace(chain="bsc"),
        "teardown-1",
        [],
        _IntentAttemptState(),
        0,
        1,
    )


@pytest.mark.asyncio
async def test_venue_withdraw_runs_through_the_venue_handler_and_is_committed() -> None:
    gateway = _Gateway()
    manager = _manager(lambda protocol, chain: AsterOrderHandler(gateway, wallet_address=MAIN.address))
    intent, compilation = _withdraw_all_bundle()
    exec_result, _, _ = await _attempt(manager, intent, compilation)
    manager.orchestrator.execute.assert_not_awaited()
    assert gateway.calls == [{"asset": "USDT", "amount": "all"}]
    assert exec_result.success and exec_result.extracted_data[ASTER_WITHDRAW_KEY]["fee"] == "0.11"
    assert manager.runner_helpers.commit.await_args.kwargs["execution_result"] is exec_result


@pytest.mark.asyncio
async def test_bundles_no_handler_claims_still_go_to_the_orchestrator() -> None:
    manager = _manager(lambda protocol, chain: None)
    manager.orchestrator.execute.return_value = SimpleNamespace(success=False, transaction_results=[])
    intent, compilation = _withdraw_all_bundle()
    await _attempt(manager, intent, compilation)
    manager.orchestrator.execute.assert_awaited_once()


def _plan_state(intent_id: str = "old-id") -> Any:
    import json

    return SimpleNamespace(pending_intents_json=json.dumps([{"type": "PERP_CLOSE", "intent_id": intent_id}]))


def _plan_intent_id(state: Any) -> str:
    import json

    return json.loads(state.pending_intents_json)[0]["intent_id"]


def _ladder_manager(book: AsyncMock, calls: list[str] | None = None) -> TeardownManager:
    calls = calls if calls is not None else []
    manager = object.__new__(TeardownManager)
    book.side_effect = book.side_effect or (lambda *a: calls.append("book"))
    manager.runner_helpers = SimpleNamespace(book_failed_fill=book)
    manager._transient_retry_due = MagicMock(return_value=None)
    manager._save_execute_floor = AsyncMock()
    manager.state_manager = SimpleNamespace(save_teardown_state=AsyncMock(side_effect=lambda *a: calls.append("save")))
    manager.alert_manager = None
    manager._paused_result = MagicMock(return_value="paused")
    return manager


async def _apply(manager: TeardownManager, status: str, state: _IntentAttemptState, plan: Any = None) -> Any:
    from almanak.framework.teardown.teardown_manager import _IntentExecutionTotals

    return await manager._apply_ladder_result(
        SimpleNamespace(success=False, status=status, approval_request=None),
        state,
        _IntentExecutionTotals(),
        SimpleNamespace(deployment_id="dep-1"),
        "intent",
        0,
        0,
        ["intent"],
        SimpleNamespace(total_value_usd=0),
        plan if plan is not None else _plan_state(),
        lambda: 0,
        {0},
        "graceful",
        None,
        [],
    )


@pytest.mark.asyncio
async def test_a_failed_rung_that_filled_records_its_ledger_row() -> None:
    from almanak.framework.execution.clob_handler import ClobExecutionResult, ClobOrderStatus
    from almanak.framework.execution.offchain_venue import offchain_execution_result

    state = _IntentAttemptState()
    filled = offchain_execution_result(
        ClobExecutionResult(success=False, status=ClobOrderStatus.FAILED, filled_size=Decimal("0.003"), error="partly")
    )
    TeardownManager._remember_failed_fill(filled, SimpleNamespace(ledger_entry_id="L1"), state)
    unfilled = offchain_execution_result(ClobExecutionResult(success=False, status=ClobOrderStatus.FAILED, error="x"))
    TeardownManager._remember_failed_fill(unfilled, SimpleNamespace(ledger_entry_id="L2"), state)
    assert state.failed_fill_ledger_id == "L1"


@pytest.mark.asyncio
async def test_a_failed_ladder_attempt_remembers_its_partial_fill() -> None:
    from almanak.framework.execution.clob_handler import ClobExecutionResult, ClobOrderStatus
    from almanak.framework.execution.offchain_venue import offchain_execution_result

    manager = object.__new__(TeardownManager)
    manager.orchestrator, manager.compiler = object(), object()
    manager._prepare_execution_attempt = AsyncMock(return_value=SimpleNamespace())
    manager._build_execution_context = MagicMock(return_value=SimpleNamespace())
    filled = offchain_execution_result(
        ClobExecutionResult(success=False, status=ClobOrderStatus.FAILED, filled_size=Decimal("0.003"), error="partly")
    )
    manager._execute_and_commit_attempt = AsyncMock(return_value=(filled, SimpleNamespace(ledger_entry_id="L1"), None))
    state = _IntentAttemptState()
    await manager._execute_intent_at_slippage(
        "intent",
        Decimal("0.01"),
        strategy=SimpleNamespace(),
        teardown_id="t",
        intent_index=0,
        intent_count=1,
        teardown_state=SimpleNamespace(),
        teardown_cycle_id="c",
        price_oracle=None,
        market=None,
        resume_floor=lambda: 0,
        accounting_degraded_records=[],
        attempt_state=state,
    )
    assert state.failed_fill_ledger_id == "L1"


@pytest.mark.parametrize("status", ["failed_manual_intervention_required", "paused_awaiting_approval"])
@pytest.mark.asyncio
async def test_an_ended_ladder_books_its_partial_fill_after_persisting_a_fresh_intent_id(status: str) -> None:
    """The venue aggregates legs per intent id: a resumed re-send must not report the booked legs again."""
    calls: list[str] = []
    book = AsyncMock()
    manager = _ladder_manager(book, calls)
    plan = _plan_state()
    await _apply(manager, status, _IntentAttemptState(failed_fill_ledger_id="L1"), plan)
    book.assert_awaited_once_with(SimpleNamespace(deployment_id="dep-1"), "intent", "L1")
    assert _plan_intent_id(plan) != "old-id"
    assert calls[:2] == ["save", "book"]


@pytest.mark.asyncio
async def test_a_requeued_intent_books_nothing_and_keeps_its_id() -> None:
    book = AsyncMock()
    manager = _ladder_manager(book)
    manager._transient_retry_due = MagicMock(return_value="transient revert")
    plan = _plan_state()
    retry, _ = await _apply(
        manager, "failed_manual_intervention_required", _IntentAttemptState(failed_fill_ledger_id="L1"), plan
    )
    assert retry is not None
    book.assert_not_awaited()
    assert _plan_intent_id(plan) == "old-id"


@pytest.mark.asyncio
async def test_a_fill_whose_fresh_id_cannot_be_saved_stays_unbooked_never_double_booked() -> None:
    book = AsyncMock()
    manager = _ladder_manager(book)
    manager.state_manager = SimpleNamespace(save_teardown_state=AsyncMock(side_effect=RuntimeError("db down")))
    plan = _plan_state()
    await _apply(manager, "failed_manual_intervention_required", _IntentAttemptState(failed_fill_ledger_id="L1"), plan)
    book.assert_not_awaited()
    assert _plan_intent_id(plan) == "old-id"


@pytest.mark.asyncio
async def test_a_booking_fault_never_blocks_the_teardown() -> None:
    manager = _ladder_manager(AsyncMock(side_effect=RuntimeError("outbox down")))
    await _apply(manager, "failed_manual_intervention_required", _IntentAttemptState(failed_fill_ledger_id="L1"))


def test_the_runner_binds_failed_fill_booking_to_its_accounting_processor() -> None:
    import asyncio

    from almanak.framework.teardown.runner_helpers import build_runner_helpers

    runner = MagicMock()
    runner._write_outbox_and_fire_processor = AsyncMock()
    helpers = build_runner_helpers(runner)
    asyncio.run(helpers.book_failed_fill("strategy", "intent", "L1"))
    runner._write_outbox_and_fire_processor.assert_awaited_once_with("strategy", "intent", "L1")


def test_a_definitive_venue_refusal_stays_retryable_in_the_teardown_ladder() -> None:
    """A venue that answered (e.g. nothing withdrawable yet) did not execute: the ladder may retry."""
    from almanak.framework.execution.clob_handler import ClobExecutionResult, ClobOrderStatus
    from almanak.framework.execution.offchain_venue import offchain_execution_result

    refused = offchain_execution_result(
        ClobExecutionResult(
            success=False, status=ClobOrderStatus.FAILED, error="no withdrawable USDT on BSC", venue_answered=True
        )
    )
    attempt = object.__new__(TeardownManager)._failed_execution_attempt(refused, Decimal("0.01"), 0, 1)
    assert "BROADCAST_RECONCILIATION_REQUIRED" not in (attempt.error or "")


def test_a_failure_the_handler_does_not_vouch_for_is_never_replayed() -> None:
    """A transport error (e.g. a Polymarket submit that may have reached the book) is not proof of non-execution."""
    from almanak.framework.execution.clob_handler import ClobExecutionResult, ClobOrderStatus
    from almanak.framework.execution.offchain_venue import offchain_execution_result

    failed = offchain_execution_result(
        ClobExecutionResult(success=False, status=ClobOrderStatus.FAILED, error="connection reset")
    )
    attempt = object.__new__(TeardownManager)._failed_execution_attempt(failed, Decimal("0.01"), 0, 1)
    assert "BROADCAST_RECONCILIATION_REQUIRED" in (attempt.error or "") and not attempt.retryable


def test_an_unknown_venue_outcome_is_never_replayed_by_the_teardown_ladder() -> None:
    from almanak.framework.execution.clob_handler import ClobExecutionResult, ClobOrderStatus
    from almanak.framework.execution.offchain_venue import offchain_execution_result

    unknown = offchain_execution_result(
        ClobExecutionResult(success=False, status=ClobOrderStatus.SUBMITTED, error="timeout", outcome_unknown=True)
    )
    attempt = object.__new__(TeardownManager)._failed_execution_attempt(unknown, Decimal("0.01"), 0, 1)
    assert "BROADCAST_RECONCILIATION_REQUIRED" in (attempt.error or "") and not attempt.retryable


@pytest.mark.asyncio
async def test_a_resumed_teardown_resends_a_booked_intent_under_a_new_venue_client_id() -> None:
    import json
    from datetime import UTC, datetime

    from almanak.connectors.aster_perps.markets import client_order_id
    from almanak.framework.teardown.models import TeardownMode, TeardownPositionSummary, TeardownState, TeardownStatus
    from almanak.framework.teardown.slippage_manager import ExecutionResult

    async def escalate(**kwargs: Any) -> ExecutionResult:
        kwargs["execute_func"].__kwdefaults__["attempt_state_for_intent"].failed_fill_ledger_id = "L1"
        return ExecutionResult(
            success=False, final_slippage=Decimal("0.01"), status="failed_manual_intervention_required", attempts=[]
        )

    manager = TeardownManager()
    manager.state_manager = AsyncMock()
    manager.slippage_manager.execute_with_escalation = escalate
    manager._transient_retry_due = MagicMock(return_value=None)
    book = AsyncMock()
    manager.runner_helpers = SimpleNamespace(book_failed_fill=book)
    now = datetime.now(UTC)
    intent = SimpleNamespace(max_slippage=None, intent_type="PERP_CLOSE", chain="bsc", intent_id="first-attempt")
    state = TeardownState(
        teardown_id="t",
        deployment_id="dep-1",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=1,
        completed_intents=0,
        current_intent_index=0,
        started_at=now,
        updated_at=now,
        pending_intents_json=json.dumps([{"type": "PERP_CLOSE", "intent_id": "first-attempt"}]),
    )
    await manager._execute_intents(
        teardown_id="t",
        strategy=MagicMock(deployment_id="dep-1", chain="bsc"),
        intents=[intent],
        positions=TeardownPositionSummary(deployment_id="dep-1", timestamp=now, positions=[]),
        mode=TeardownMode.SOFT,
        teardown_state=state,
    )
    book.assert_awaited_once()
    resumed_id = json.loads(state.pending_intents_json)[0]["intent_id"]
    assert client_order_id(resumed_id, leg="close") != client_order_id("first-attempt", leg="close")


@pytest.mark.asyncio
async def test_a_withdrawal_the_venue_refused_is_retryable_in_the_ladder() -> None:
    from almanak.framework.execution.offchain_venue import offchain_execution_result

    class _Refusing(_Gateway):
        def withdraw(self, **kwargs: Any) -> aster_perps_pb2.AsterWithdrawResponse:
            return aster_perps_pb2.AsterWithdrawResponse(success=False, error="no withdrawable USDT")

    _, compilation = _withdraw_all_bundle()
    handler = AsterOrderHandler(_Refusing(), wallet_address=MAIN.address)  # type: ignore[arg-type]
    refused = offchain_execution_result(await handler.execute(compilation.action_bundle))
    attempt = object.__new__(TeardownManager)._failed_execution_attempt(refused, Decimal("0.01"), 0, 1)
    assert "BROADCAST_RECONCILIATION_REQUIRED" not in (attempt.error or "")
