"""The off-chain venue lane holds the same no-replay barrier as the on-chain lane.

An off-chain order whose outcome the venue did not confirm may have filled. It
must never be re-sent, and the strategy must not decide again, until the venue
answers: a confirmed fill is delivered through the normal success path, a
proven non-execution releases the barrier.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.connectors.aster_perps.compiler import AsterPerpsCompiler
from almanak.connectors.aster_perps.execution import AsterOrderHandler
from almanak.connectors.aster_perps.proto import aster_perps_pb2
from almanak.framework.intents.compiler import CompilationResult, CompilationStatus
from almanak.framework.intents.state_machine import IntentStateMachine, RetryConfig, StateMachineConfig
from almanak.framework.intents.vocabulary import PerpOpenIntent
from almanak.framework.runner.runner_models import ExecutionBarrierPhase, ExecutionProgress
from almanak.framework.runner.strategy_runner import IterationStatus
from almanak.framework.state.state_manager import StateConflictError
from almanak.framework.state.strategy_state import StateValuePreconditionError
from almanak.framework.strategies.intent_strategy import IntentStrategy
from tests.unit.connectors.aster_perps.test_aster_pro_funding import _ctx
from tests.unit.runner.test_execute_single_chain_steps import _make_runner, _make_state, _make_strategy

WALLET = "0x" + "ab" * 20


class _Timeout(grpc.RpcError):
    def details(self) -> str:
        return "Deadline Exceeded"


class _Gateway:
    """The Aster gateway RPCs the handler calls; records every order placement."""

    def __init__(self, place: Any = None, order: Any = None) -> None:
        self.place, self.order = place, order
        self.placed: list[dict] = []
        self.lookups: list[dict] = []

    def place_market_order(self, order_request: dict, *, wallet_address: str) -> Any:
        self.placed.append(order_request)
        if isinstance(self.place, Exception):
            raise self.place
        return self.place

    def get_order(self, **kwargs: Any) -> Any:
        self.lookups.append(kwargs)
        if isinstance(self.order, Exception):
            raise self.order
        return self.order


def _fill() -> aster_perps_pb2.AsterOrderResponse:
    return aster_perps_pb2.AsterOrderResponse(
        success=True,
        order_id=42,
        client_order_id="c",
        status="FILLED",
        side="BUY",
        executed_qty="0.002",
        requested_qty="0.002",
        avg_price="2727.5",
        cum_quote="5.455",
        fee="0.0022",
        fee_asset="USDT",
    )


def _open_state(gateway: _Gateway) -> tuple[Any, Any, Any]:
    intent = PerpOpenIntent(
        market="ETH/USD",
        collateral_token="USDT",
        collateral_amount=Decimal("1.2"),
        size_usd=Decimal("6"),
        is_long=True,
        leverage=Decimal("5"),
        protocol="aster_perps",
    )
    bundle = AsterPerpsCompiler().compile(_ctx(), intent).action_bundle
    runner = _make_runner()
    runner._save_execution_progress = AsyncMock()  # type: ignore[method-assign]
    runner._clear_execution_progress = AsyncMock()  # type: ignore[method-assign]
    strategy = _make_strategy()
    strategy.chain = "bsc"
    state = _make_state(strategy, intent=intent)
    state.clob_handler = AsterOrderHandler(gateway, wallet_address=WALLET)  # type: ignore[arg-type]
    compiler = MagicMock(default_protocol=None)
    compiler.compile.return_value = CompilationResult(
        status=CompilationStatus.SUCCESS, intent_id=intent.intent_id, action_bundle=bundle
    )
    state.compiler = compiler
    state.state_machine = IntentStateMachine(
        intent=intent,
        compiler=compiler,
        config=StateMachineConfig(
            retry_config=RetryConfig(max_retries=2, initial_delay_seconds=0.0, jitter_factor=0.0)
        ),
        on_sadflow_enter=runner._on_sadflow_enter,
    )
    return runner, state, bundle


@pytest.mark.asyncio
async def test_an_unknown_venue_outcome_seals_the_barrier_and_is_never_resent() -> None:
    gateway = _Gateway(place=_Timeout(), order=_Timeout())
    runner, state, _ = _open_state(gateway)
    with patch("almanak.framework.observability.emitter.emit_phase_event"):
        assert await runner._single_chain_state_machine_loop(state) is None

    assert state.state_machine.is_complete and not state.state_machine.success
    assert state.state_machine.retry_count == 0
    assert "BROADCAST_RECONCILIATION_REQUIRED" in (state.state_machine.error or "")
    assert len(gateway.placed) == 1
    marker = runner._save_execution_progress.await_args.args[1]
    assert marker.effective_barrier_phase is ExecutionBarrierPhase.RECONCILIATION_REQUIRED
    assert marker.recovery_context.bundle_metadata["order_request"]["client_order_id"]
    runner._clear_execution_progress.assert_not_awaited()

    pending = await runner._single_chain_handle_failure(state)
    assert pending.status is IterationStatus.EXECUTION_PENDING
    state.strategy.on_intent_executed.assert_not_called()

    # Next cycle: the venue still cannot say, so the strategy stays held and nothing is re-sent.
    runner._load_execution_progress = AsyncMock(return_value=marker)  # type: ignore[method-assign]
    resumed = await runner._check_and_resume_stuck_execution(state.strategy, datetime.now(UTC))
    assert resumed is not None and resumed.status is IterationStatus.EXECUTION_PENDING
    assert len(gateway.placed) == 1


@pytest.mark.asyncio
async def test_a_definitive_venue_rejection_releases_the_barrier() -> None:
    gateway = _Gateway(place=aster_perps_pb2.AsterOrderResponse(success=False, error="Margin is insufficient."))
    runner, state, bundle = _open_state(gateway)
    state.state_machine = MagicMock(retry_count=0)
    assert await runner._single_chain_execute_step(state, SimpleNamespace(action_bundle=bundle)) is None
    runner._clear_execution_progress.assert_awaited_once_with(state.deployment_id)
    receipt = state.state_machine.set_receipt.call_args.args[0]
    assert not receipt.success and "BROADCAST_RECONCILIATION_REQUIRED" not in receipt.error


@pytest.mark.asyncio
async def test_a_venue_fill_keeps_the_marker_until_accounting_completes() -> None:
    runner, state, bundle = _open_state(_Gateway(place=_fill()))
    state.state_machine = MagicMock(retry_count=0)
    assert await runner._single_chain_execute_step(state, SimpleNamespace(action_bundle=bundle)) is None
    runner._clear_execution_progress.assert_not_awaited()
    retained = runner._save_execution_progress.await_args.args[1]
    assert retained.is_accounting_pending
    assert state.replay_barrier is retained
    assert state.state_machine.set_receipt.call_args.args[0].success


def _held_marker(bundle: Any, *, sealed: bool = True) -> ExecutionProgress:
    from almanak.framework.execution.orchestrator import ExecutionContext
    from almanak.framework.execution.submission import execution_plan_hash
    from almanak.framework.runner.recovery_context import ExecutionRecoveryContext

    intent = PerpOpenIntent(
        market="ETH/USD",
        collateral_token="USDT",
        collateral_amount=Decimal("1.2"),
        size_usd=Decimal("6"),
        is_long=True,
        leverage=Decimal("5"),
        protocol="aster_perps",
    )
    marker = ExecutionProgress(
        execution_id=intent.intent_id,
        deployment_id="test-strategy",
        intents_hash="broadcast-pending",
        total_steps=1,
        failure_error="BROADCAST_RECONCILIATION_REQUIRED: pending",
        reconciliation_required_step_index=0,
        serialized_intents=[intent.serialize()],
        barrier_phase=ExecutionBarrierPhase.PRE_BROADCAST,
        started_at=datetime.now(UTC) - timedelta(minutes=5),
    )
    from almanak.framework.runner.runner_models import ExecutionLane

    marker.execution_lane = ExecutionLane.SINGLE_CHAIN
    context = ExecutionRecoveryContext.capture(
        plan_hash=execution_plan_hash(bundle),
        execution=ExecutionContext(
            deployment_id="test-strategy",
            chain="bsc",
            wallet_address=WALLET,
            intent_id=intent.intent_id,
            correlation_id=intent.intent_id,
            cycle_id="cycle-1",
        ),
        pre_snapshot=None,
        prices=None,
        bundle_metadata=dict(bundle.metadata),
        failed_attempt_receipts={},
    )
    marker.recovery_context = context.with_strategy_checkpoint({"phase": "open"}, {})
    if sealed:
        marker.mark_reconciliation_required(0, "BROADCAST_RECONCILIATION_REQUIRED: unknown")
    return marker


def _recovering_strategy(runner: Any) -> Any:
    strategy = MagicMock(spec=IntentStrategy)
    strategy.deployment_id = "test-strategy"
    strategy.chain = "bsc"
    strategy.wallet_address = WALLET
    strategy._state_manager = runner.state_manager
    return strategy


@pytest.mark.parametrize("sealed", [True, False])
@pytest.mark.asyncio
async def test_a_venue_confirmed_fill_is_delivered_through_the_success_path(sealed: bool) -> None:
    gateway = _Gateway(order=_fill())
    runner, _, bundle = _open_state(gateway)
    marker = _held_marker(bundle, sealed=sealed)
    runner._load_execution_progress = AsyncMock(return_value=marker)  # type: ignore[method-assign]
    claimed = ExecutionProgress.from_dict(marker.to_dict())
    claimed.mark_landed_repair_pending(0, "observed")
    runner._single_chain_handle_success = AsyncMock(return_value="delivered")  # type: ignore[method-assign]
    strategy = _recovering_strategy(runner)
    with (
        patch(
            "almanak.framework.runner.offchain_recovery.build_offchain_handler",
            return_value=AsterOrderHandler(gateway, wallet_address=WALLET),  # type: ignore[arg-type]
        ),
        patch(
            "almanak.framework.runner.offchain_recovery.claim_observed_offchain_recovery",
            AsyncMock(return_value=(claimed, 7)),
        ),
        patch.object(IntentStrategy, "_restore_framework_state"),
    ):
        assert await runner._check_and_resume_stuck_execution(strategy, datetime.now(UTC)) == "delivered"

    state = runner._single_chain_handle_success.await_args.args[0]
    assert state.replay_barrier is claimed
    assert state.last_execution_result.success
    assert state.last_execution_result.extracted_data["aster_order"]["cum_quote"] == "5.455"
    strategy.load_persistent_state.assert_called_once_with({"phase": "open"})
    assert gateway.placed == []
    assert gateway.lookups[0]["client_order_id"] == bundle.metadata["order_request"]["client_order_id"]


@pytest.mark.asyncio
async def test_a_proven_non_execution_releases_the_barrier_for_a_fresh_decision() -> None:
    gateway = _Gateway(order=aster_perps_pb2.AsterOrderResponse(success=False, order_not_found=True))
    runner, _, bundle = _open_state(gateway)
    runner._load_execution_progress = AsyncMock(return_value=_held_marker(bundle))  # type: ignore[method-assign]
    with patch(
        "almanak.framework.runner.offchain_recovery.build_offchain_handler",
        return_value=AsterOrderHandler(gateway, wallet_address=WALLET),  # type: ignore[arg-type]
    ):
        assert await runner._check_and_resume_stuck_execution(_recovering_strategy(runner), datetime.now(UTC)) is None
    runner._clear_execution_progress.assert_awaited_once_with("test-strategy")
    assert gateway.placed == []


@pytest.mark.asyncio
async def test_a_still_unknown_outcome_keeps_the_strategy_held() -> None:
    gateway = _Gateway(order=_Timeout())
    runner, _, bundle = _open_state(gateway)
    runner._load_execution_progress = AsyncMock(return_value=_held_marker(bundle))  # type: ignore[method-assign]
    with patch(
        "almanak.framework.runner.offchain_recovery.build_offchain_handler",
        return_value=AsterOrderHandler(gateway, wallet_address=WALLET),  # type: ignore[arg-type]
    ):
        held = await runner._check_and_resume_stuck_execution(_recovering_strategy(runner), datetime.now(UTC))
    assert held is not None and held.status is IterationStatus.EXECUTION_PENDING
    runner._clear_execution_progress.assert_not_awaited()
    assert gateway.placed == []


@pytest.mark.parametrize("refusal", [StateValuePreconditionError("state changed"), StateConflictError("t", 1, 2)])
@pytest.mark.asyncio
async def test_a_confirmed_fill_whose_claim_is_refused_keeps_the_strategy_held(refusal: Exception) -> None:
    """Returning "nothing pending" here would let decide() mint a new order over a confirmed fill."""
    gateway = _Gateway(order=_fill())
    runner, _, bundle = _open_state(gateway)
    runner._load_execution_progress = AsyncMock(return_value=_held_marker(bundle))  # type: ignore[method-assign]
    runner._single_chain_handle_success = AsyncMock()  # type: ignore[method-assign]
    with (
        patch(
            "almanak.framework.runner.offchain_recovery.build_offchain_handler",
            return_value=AsterOrderHandler(gateway, wallet_address=WALLET),  # type: ignore[arg-type]
        ),
        patch(
            "almanak.framework.runner.offchain_recovery.claim_observed_offchain_recovery",
            AsyncMock(side_effect=refusal),
        ),
    ):
        held = await runner._check_and_resume_stuck_execution(_recovering_strategy(runner), datetime.now(UTC))
    assert held is not None and held.status is IterationStatus.EXECUTION_PENDING
    runner._single_chain_handle_success.assert_not_awaited()
    runner._clear_execution_progress.assert_not_awaited()
    assert gateway.placed == []


@pytest.mark.asyncio
async def test_a_venue_without_a_buildable_handler_keeps_the_strategy_held() -> None:
    runner, _, bundle = _open_state(_Gateway())
    runner._load_execution_progress = AsyncMock(return_value=_held_marker(bundle))  # type: ignore[method-assign]
    with patch(
        "almanak.framework.runner.offchain_recovery.build_offchain_handler", side_effect=KeyError("unknown venue")
    ):
        held = await runner._check_and_resume_stuck_execution(_recovering_strategy(runner), datetime.now(UTC))
    assert held is not None and held.status is IterationStatus.EXECUTION_PENDING
    runner._clear_execution_progress.assert_not_awaited()


def _partial_close() -> aster_perps_pb2.AsterOrderResponse:
    return aster_perps_pb2.AsterOrderResponse(
        success=False,
        error="position only partly closed: 0.003 closed, 0.001 still open",
        order_id=42,
        client_order_id="c",
        status="EXPIRED",
        side="SELL",
        executed_qty="0.003",
        requested_qty="0.004",
        avg_price="2727.5",
        cum_quote="8.1825",
        fee="0.0066",
        fee_asset="USDT",
        realized_pnl="0.03",
    )


async def _final_failure(gateway: _Gateway) -> tuple[Any, Any]:
    runner, state, _ = _open_state(gateway)
    runner._write_ledger_entry = AsyncMock(return_value="ledger-1")  # type: ignore[method-assign]
    runner._write_outbox_and_fire_processor = AsyncMock()  # type: ignore[method-assign]
    runner._emit_execution_timeline_event = MagicMock()  # type: ignore[method-assign]
    runner._handle_execution_error = AsyncMock()  # type: ignore[method-assign]
    with patch("almanak.framework.observability.emitter.emit_phase_event"):
        assert await runner._single_chain_state_machine_loop(state) is None
    with patch("almanak.framework.runner.strategy_runner.diagnose_revert", new_callable=AsyncMock):
        await runner._single_chain_handle_failure(state)
    return runner, state


@pytest.mark.asyncio
async def test_an_intent_that_finally_fails_with_a_fill_books_that_fill_once() -> None:
    gateway = _Gateway(place=_partial_close())
    runner, state = await _final_failure(gateway)
    assert not state.state_machine.success
    runner._write_ledger_entry.assert_awaited_once()
    assert runner._write_ledger_entry.await_args.kwargs["success"] is False
    booked = runner._write_ledger_entry.await_args.kwargs["result"]
    assert booked.extracted_data["offchain_filled_size"] == "0.003"
    assert booked.extracted_data["aster_order"]["realized_pnl"] == "0.03"
    runner._write_outbox_and_fire_processor.assert_awaited_once_with(state.strategy, state.intent, "ledger-1")


@pytest.mark.asyncio
async def test_a_rejection_with_no_fill_books_nothing() -> None:
    gateway = _Gateway(place=aster_perps_pb2.AsterOrderResponse(success=False, error="Margin is insufficient."))
    runner, _ = await _final_failure(gateway)
    runner._write_outbox_and_fire_processor.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_reconciled_partial_execution_is_booked_before_the_barrier_is_released() -> None:
    gateway = _Gateway(order=_partial_close())
    runner, _, bundle = _open_state(gateway)
    runner._load_execution_progress = AsyncMock(return_value=_held_marker(bundle))  # type: ignore[method-assign]
    runner._write_ledger_entry = AsyncMock(return_value="ledger-2")  # type: ignore[method-assign]
    runner._write_outbox_and_fire_processor = AsyncMock()  # type: ignore[method-assign]
    strategy = _recovering_strategy(runner)
    order: list[str] = []
    claim = AsyncMock(side_effect=lambda *a: order.append("claim") or (None, 1))
    runner._write_ledger_entry.side_effect = lambda *a, **k: order.append("book") or "ledger-2"
    with (
        patch(
            "almanak.framework.runner.offchain_recovery.build_offchain_handler",
            return_value=AsterOrderHandler(gateway, wallet_address=WALLET),  # type: ignore[arg-type]
        ),
        patch("almanak.framework.runner.offchain_recovery.claim_observed_offchain_recovery", claim),
    ):
        assert await runner._check_and_resume_stuck_execution(strategy, datetime.now(UTC)) is None
    # Claimed before booking: a crash or a failed release then leaves an
    # accounting-pending marker, never one that reconciles and books again.
    assert order == ["claim", "book"]
    booked = runner._write_ledger_entry.await_args.kwargs["result"]
    assert booked.extracted_data["offchain_filled_size"] == "0.003"
    runner._write_outbox_and_fire_processor.assert_awaited_once()
    runner._clear_execution_progress.assert_awaited_once_with("test-strategy")
    assert gateway.placed == []


@pytest.mark.asyncio
async def test_a_partial_execution_whose_claim_is_refused_is_held_not_booked() -> None:
    gateway = _Gateway(order=_partial_close())
    runner, _, bundle = _open_state(gateway)
    runner._load_execution_progress = AsyncMock(return_value=_held_marker(bundle))  # type: ignore[method-assign]
    runner._write_ledger_entry = AsyncMock()  # type: ignore[method-assign]
    with (
        patch(
            "almanak.framework.runner.offchain_recovery.build_offchain_handler",
            return_value=AsterOrderHandler(gateway, wallet_address=WALLET),  # type: ignore[arg-type]
        ),
        patch(
            "almanak.framework.runner.offchain_recovery.claim_observed_offchain_recovery",
            AsyncMock(side_effect=StateConflictError("t", 1, 2)),
        ),
    ):
        held = await runner._check_and_resume_stuck_execution(_recovering_strategy(runner), datetime.now(UTC))
    assert held is not None and held.status is IterationStatus.EXECUTION_PENDING
    runner._write_ledger_entry.assert_not_awaited()
    runner._clear_execution_progress.assert_not_awaited()


def _claimable_marker() -> ExecutionProgress:
    _, _, bundle = _open_state(_Gateway(order=_fill()))
    return _held_marker(bundle)


def _without_checkpoint(marker: ExecutionProgress) -> None:
    from dataclasses import replace

    marker.recovery_context = replace(marker.recovery_context, strategy_checkpoint=None)  # type: ignore[type-var]


def _on_another_lane(marker: ExecutionProgress) -> None:
    from almanak.framework.runner.runner_models import ExecutionLane

    marker.execution_lane = ExecutionLane.BRIDGE


@pytest.mark.parametrize(
    "spoil",
    [
        _on_another_lane,
        lambda m: m.mark_landed_repair_pending(0, "already claimed"),
        lambda m: setattr(m, "total_steps", 2),
        lambda m: setattr(m, "recovery_context", None),
        _without_checkpoint,
        lambda m: setattr(m, "deployment_id", "another-deployment"),
    ],
    ids=["other_lane", "already_claimed", "multi_step", "no_context", "no_checkpoint", "other_deployment"],
)
@pytest.mark.asyncio
async def test_an_incomplete_off_chain_checkpoint_is_never_claimed(spoil: Any) -> None:
    from almanak.framework.runner.runner_recovery import claim_observed_offchain_recovery

    marker = _claimable_marker()
    spoil(marker)
    claim = AsyncMock()
    with (
        patch("almanak.framework.runner.runner_recovery._claim_recovery_state", claim),
        pytest.raises(StateValuePreconditionError),
    ):
        await claim_observed_offchain_recovery(MagicMock(), marker)
    claim.assert_not_awaited()


@pytest.mark.parametrize("sealed", [True, False])
@pytest.mark.asyncio
async def test_a_complete_off_chain_checkpoint_is_claimed_with_no_receipts(sealed: bool) -> None:
    """A runner that died during the venue call never sealed the marker; the venue's answer is the evidence."""
    from almanak.framework.runner.runner_recovery import claim_observed_offchain_recovery

    _, _, bundle = _open_state(_Gateway(order=_fill()))
    marker = _held_marker(bundle, sealed=sealed)
    runner = MagicMock()
    claim = AsyncMock(return_value=("claimed", 3))
    with patch("almanak.framework.runner.runner_recovery._claim_recovery_state", claim):
        assert await claim_observed_offchain_recovery(runner, marker) == ("claimed", 3)
    claim.assert_awaited_once_with(runner, marker, recovery_receipts=None)
