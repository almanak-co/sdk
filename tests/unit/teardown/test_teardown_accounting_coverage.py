"""End-to-end coverage tests for the VIB-3773 teardown accounting wiring
(Phase 3, tests T9–T13).

These tests exercise ``TeardownManager._execute_intents`` *with* a populated
``TeardownRunnerHelpers`` bag — i.e. the production wiring path. They use a
fake runner that records every call, and verify:

* T9 — after a successful ``orchestrator.execute`` the manager calls
  ``runner.commit_teardown_intent`` with the right cycle id, intent,
  result, context and bundle metadata.
* T10 — happy-path 2-intent teardown produces 2 commit invocations + 2
  snapshot invocations (pre + post) with cycle ids prefixed ``teardown-``.
* T11 — Lane-C parity covered separately in the runner_teardown tests.
* T12 — when the commit helper reports ``accounting_degraded=True`` for
  intent #1, intent #2 still runs and the final ``TeardownResult`` shows
  ``accounting_degraded=True`` + ``accounting_degraded_count >= 1``.
* T13 — ``execute_teardown_via_manager``'s outer cycle-id swap is verified
  in ``runner_teardown`` integration; the commit helper's local swap is
  covered in ``test_teardown_commit``. Here we just assert the manager
  passes ``teardown-{teardown_id}`` through to ``commit``.

The TeardownManager is exercised directly (not via
``execute_teardown_via_manager``) so we don't need a full runner
test-stand. Lane-B integration (snapshot bracket + cycle-id outer swap) is
exercised by the next file (``test_runner_teardown_brackets.py``) — the two
together cover the wiring end-to-end.
"""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.intents.perp_intents import PerpCloseIntent
from almanak.framework.runner.teardown_commit import TeardownCommitOutcome
from almanak.framework.teardown.config import TeardownConfig
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownMode,
    TeardownPositionSummary,
    TeardownState,
    TeardownStatus,
)
from almanak.framework.teardown.runner_helpers import SettlementPreparation, TeardownRunnerHelpers
from almanak.framework.teardown.teardown_manager import TeardownManager, _serialize_intent_for_state

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


def _make_intent(intent_type_value: str = "SWAP") -> SimpleNamespace:
    """Minimal intent with attributes the teardown loop actually reads."""
    return SimpleNamespace(
        intent_type=SimpleNamespace(value=intent_type_value),
        protocol="uniswap_v3",
        chain="arbitrum",
        max_slippage=Decimal("0.005"),
        # to_dict for state persistence in skip-path
        to_dict=lambda: {"intent_type": intent_type_value},
    )


def _make_strategy() -> SimpleNamespace:
    return SimpleNamespace(
        deployment_id="dep-1",
        chain="arbitrum",
        wallet_address="0xWALLET",
    )


def test_persisted_perp_close_keeps_dispatch_discriminator() -> None:
    persisted = _serialize_intent_for_state(
        PerpCloseIntent(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
            chain="arbitrum",
        )
    )

    assert persisted["type"] == "PERP_CLOSE"


def _successful_exec_result() -> SimpleNamespace:
    return SimpleNamespace(
        success=True,
        transaction_results=[SimpleNamespace(tx_hash="0xabc")],
        total_gas_used=120_000,
        gas_cost_usd="0.50",
        extracted_data={},
        error="",
    )


def _make_state(*, total_intents: int) -> TeardownState:
    now = datetime.now(UTC)
    return TeardownState(
        teardown_id="td-uuid-7",
        deployment_id="strat-1",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=total_intents,
        completed_intents=0,
        current_intent_index=0,
        started_at=now,
        updated_at=now,
    )


def _make_position_summary() -> TeardownPositionSummary:
    pos = PositionInfo(
        protocol="uniswap_v3",
        position_id="5459812",
        chain="arbitrum",
        position_type=PositionType.LP,
        value_usd=Decimal("4.0"),
    )
    return TeardownPositionSummary(
        deployment_id="strat-1",
        timestamp=datetime.now(UTC),
        positions=[pos],
        total_value_usd=Decimal("4.0"),
    )


@pytest.fixture
def fake_orchestrator() -> MagicMock:
    orch = MagicMock(name="ExecutionOrchestrator")
    orch.execute = AsyncMock(return_value=_successful_exec_result())
    return orch


@pytest.fixture
def fake_compiler() -> MagicMock:
    comp = MagicMock(name="IntentCompiler")
    bundle = SimpleNamespace(
        metadata={"expected_output_human": "1.5"},
    )
    comp.compile.return_value = SimpleNamespace(
        status=SimpleNamespace(value="SUCCESS"),
        action_bundle=bundle,
        error=None,
        is_transient=False,
        retry_after_seconds=0,
    )
    return comp


@pytest.fixture
def state_manager_mock() -> MagicMock:
    sm = MagicMock(name="state_manager")
    sm.save_teardown_state = AsyncMock()
    return sm


def _make_helpers(
    *,
    commit_outcomes: list[TeardownCommitOutcome] | None = None,
    native_principal_snapshot=None,
    await_intent_settlement=None,
    reconcile_intent_settlement=None,
):
    """Build a TeardownRunnerHelpers whose ``commit`` callable returns the
    given outcomes in order. ``capture_snapshot`` is a no-op (tested
    separately in the runner_teardown integration tests).

    ``native_principal_snapshot`` (VIB-5117): when provided, wired as the
    ``snapshot_intent_v4_lp_close_native_principal`` hook so the manager's
    ``has_v4_lp_close_native_principal`` gate fires and the captured pair is
    forwarded into ``commit(...)``.
    """
    commit_calls: list[dict] = []
    outcomes_iter = iter(
        commit_outcomes
        or [TeardownCommitOutcome(ledger_entry_id="ledger-1", accounting_degraded=False, degraded_reason=None)]
    )

    async def _commit(
        strategy,
        intent,
        *,
        execution_result,
        execution_context,
        bundle_metadata,
        teardown_cycle_id,
        **_kwargs,
    ):
        # Absorb pre_snapshot / recon kwargs added by VIB-3918 per-intent
        # balance capture so this thin test stub stays signature-compatible
        # with the real ``commit_teardown_intent``. Capture the VIB-5117 native
        # principal so threading tests can assert it was forwarded.
        commit_calls.append(
            {
                "deployment_id": strategy.deployment_id,
                "intent_type": intent.intent_type.value,
                "tx_hash": execution_result.transaction_results[0].tx_hash,
                "bundle_metadata": bundle_metadata,
                "teardown_cycle_id": teardown_cycle_id,
                "v4_lp_close_native_principal": _kwargs.get("v4_lp_close_native_principal"),
                "v4_lp_close_fees": _kwargs.get("v4_lp_close_fees"),
            }
        )
        try:
            return next(outcomes_iter)
        except StopIteration:
            return TeardownCommitOutcome(
                ledger_entry_id="ledger-x",
                accounting_degraded=False,
                degraded_reason=None,
            )

    def _prepare_settlement(*_args, **_kwargs):
        return SettlementPreparation(
            applicable=True,
            orders=(SimpleNamespace(order_id="0x" + "42" * 32),),
        )

    async def _reconcile_settlement(*_args, **_kwargs):
        return ()

    return (
        TeardownRunnerHelpers(
            commit=_commit,
            capture_snapshot=None,
            snapshot_intent_v4_lp_close_native_principal=native_principal_snapshot,
            prepare_intent_settlement=_prepare_settlement if await_intent_settlement is not None else None,
            await_intent_settlement=await_intent_settlement,
            reconcile_intent_settlement=(
                reconcile_intent_settlement or _reconcile_settlement if await_intent_settlement is not None else None
            ),
        ),
        commit_calls,
    )


# ---------------------------------------------------------------------------
# T9 — manager passes the right args to commit_teardown_intent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t9_manager_invokes_commit_with_correct_args(fake_orchestrator, fake_compiler, state_manager_mock):
    helpers, commit_calls = _make_helpers()
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=helpers,
        config=TeardownConfig.default(),
    )

    intent = _make_intent("SWAP")
    state = _make_state(total_intents=1)
    positions = _make_position_summary()
    strategy = _make_strategy()

    result = await mgr._execute_intents(
        teardown_id=state.teardown_id,
        strategy=strategy,
        intents=[intent],
        positions=positions,
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )

    assert result.success is True
    assert result.intents_succeeded == 1
    assert len(commit_calls) == 1
    call = commit_calls[0]
    assert call["intent_type"] == "SWAP"
    assert call["tx_hash"] == "0xabc"
    assert call["bundle_metadata"] == {"expected_output_human": "1.5"}
    assert call["teardown_cycle_id"] == "teardown-td-uuid-7"


@pytest.mark.asyncio
async def test_async_submission_commits_before_waiting_for_settlement(
    fake_orchestrator, fake_compiler, state_manager_mock
):
    """Phase 1 and its accepted marker must both be durable before the keeper wait."""
    events: list[str] = []

    async def _settle(*_args, **_kwargs):
        events.append("settled")
        return None

    async def _save_marker(state: TeardownState) -> None:
        if '"_teardown_async_submission_accepted": true' in state.pending_intents_json:
            marker = json.loads(state.pending_intents_json)[0]
            events.append("marker+ledger" if marker.get("_teardown_async_submission_ledger_id") else "marker")

    state_manager_mock.save_teardown_state.side_effect = _save_marker

    helpers, commit_calls = _make_helpers(await_intent_settlement=_settle)
    original_commit = helpers.commit

    async def _ordered_commit(*args, **kwargs):
        events.append("commit")
        assert original_commit is not None
        return await original_commit(*args, **kwargs)

    helpers = TeardownRunnerHelpers(
        commit=_ordered_commit,
        prepare_intent_settlement=helpers.prepare_intent_settlement,
        await_intent_settlement=helpers.await_intent_settlement,
        reconcile_intent_settlement=helpers.reconcile_intent_settlement,
    )
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=helpers,
        config=TeardownConfig.default(),
    )

    state = _make_state(total_intents=1)
    state.pending_intents_json = json.dumps([_make_intent("PERP_CLOSE").to_dict()])
    result = await mgr._execute_intents(
        teardown_id="td-settle-first",
        strategy=_make_strategy(),
        intents=[_make_intent("PERP_CLOSE")],
        positions=_make_position_summary(),
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )

    assert result.success is True
    assert events[:3] == ["commit", "marker+ledger", "settled"]
    assert "marker" not in events
    assert len(commit_calls) == 1


@pytest.mark.asyncio
async def test_pending_async_submission_fails_once_after_durable_commit_without_slippage_retry(
    fake_orchestrator, fake_compiler, state_manager_mock
):
    """A landed async order must never be duplicated by the slippage ladder."""
    state = _make_state(total_intents=1)
    state.pending_intents_json = json.dumps([_make_intent("PERP_CLOSE").to_dict()])
    saved_states: list[TeardownState] = []

    async def _capture_state(saved: TeardownState) -> None:
        saved_states.append(deepcopy(saved))

    state_manager_mock.save_teardown_state.side_effect = _capture_state

    async def _settle(*_args, **_kwargs):
        assert state.completed_intents == 0
        assert state.current_intent_index == 0
        assert state_manager_mock.save_teardown_state.await_count >= 2
        assert any(
            '"_teardown_async_submission_accepted": true' in saved.pending_intents_json for saved in saved_states
        )
        return "Async settlement PENDING_SETTLEMENT_TIMEOUT: order remains pending"

    helpers, commit_calls = _make_helpers(await_intent_settlement=_settle)
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=helpers,
        config=TeardownConfig.default(),
    )

    result = await mgr._execute_intents(
        teardown_id="td-no-resubmit",
        strategy=_make_strategy(),
        intents=[_make_intent("PERP_CLOSE")],
        positions=_make_position_summary(),
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )

    assert result.success is False
    assert result.intents_failed == 1
    assert fake_orchestrator.execute.await_count == 1
    assert len(commit_calls) == 1
    assert state.completed_intents == 0
    assert state.current_intent_index == 0
    assert state.status == TeardownStatus.EXECUTING
    assert state.is_resumable is True
    assert result.completed_at is None
    assert state.completed_at is None
    assert result.async_settlement_pending is True
    assert result.error == "Accepted async submission remains unsettled; teardown is resumable"
    assert '"_teardown_async_submission_accepted": true' in state.pending_intents_json


@pytest.mark.asyncio
async def test_post_submit_state_failure_never_reenters_slippage_ladder(
    fake_orchestrator, fake_compiler, state_manager_mock
):
    """Any exception after a landed transaction is permanently non-retryable."""
    state = _make_state(total_intents=1)
    state.pending_intents_json = json.dumps([_make_intent("PERP_CLOSE").to_dict()])
    injected_failure = False

    async def _save(saved: TeardownState) -> None:
        nonlocal injected_failure
        if not injected_failure and '"_teardown_async_submission_accepted": true' in saved.pending_intents_json:
            injected_failure = True
            raise RuntimeError("state backend unavailable after submission")

    state_manager_mock.save_teardown_state.side_effect = _save

    async def _settle(*_args, **_kwargs):
        return "Async settlement PENDING_SETTLEMENT_TIMEOUT: order remains pending"

    helpers, commit_calls = _make_helpers(await_intent_settlement=_settle)
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=helpers,
        config=TeardownConfig.default(),
    )

    state = _make_state(total_intents=1)
    state.pending_intents_json = json.dumps([_make_intent("PERP_CLOSE").to_dict()])
    result = await mgr._execute_intents(
        teardown_id="td-post-submit-failure",
        strategy=_make_strategy(),
        intents=[_make_intent("PERP_CLOSE")],
        positions=_make_position_summary(),
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )

    assert result.success is False
    assert result.intents_failed == 1
    assert fake_orchestrator.execute.await_count == 1
    assert len(commit_calls) == 1
    assert injected_failure is True
    assert state_manager_mock.save_teardown_state.await_count >= 2
    assert state.status == TeardownStatus.EXECUTING
    assert state.is_resumable is True


@pytest.mark.asyncio
async def test_uncorrelated_async_submission_fails_terminally_without_fresh_tick_retry(
    fake_orchestrator, fake_compiler, state_manager_mock
):
    """No marker is written when neither receipt nor Phase 1 yields an exact key."""

    async def _settle(*_args, **_kwargs):
        raise AssertionError("uncorrelated async submission must not enter the settlement barrier")

    helpers, commit_calls = _make_helpers(await_intent_settlement=_settle)

    def _unmeasured_preparation(*_args, **_kwargs):
        return SettlementPreparation(
            applicable=True,
            error="receipt enrichment failed",
            orders=(),
        )

    helpers = replace(
        helpers,
        prepare_intent_settlement=_unmeasured_preparation,
        recover_accepted_order_keys=AsyncMock(return_value=()),
    )
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=helpers,
        config=TeardownConfig.default(),
    )
    state = _make_state(total_intents=1)
    state.pending_intents_json = json.dumps([_make_intent("PERP_CLOSE").to_dict()])

    result = await mgr._execute_intents(
        teardown_id="td-uncorrelated",
        strategy=_make_strategy(),
        intents=[_make_intent("PERP_CLOSE")],
        positions=_make_position_summary(),
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )

    assert result.success is False
    assert result.completed_at is not None
    assert state.status == TeardownStatus.COMPLETED
    assert fake_orchestrator.execute.await_count == 1
    assert len(commit_calls) == 1
    assert '"_teardown_async_submission_accepted": true' not in state.pending_intents_json


@pytest.mark.asyncio
async def test_resume_skips_durably_accepted_async_submission(fake_orchestrator, fake_compiler, state_manager_mock):
    """A persisted accepted marker outranks a stale non-contiguous resume floor."""
    accepted = {
        "intent_type": "PERP_CLOSE",
        "protocol": "gmx_v2",
        "_teardown_async_submission_accepted": True,
    }
    state = _make_state(total_intents=1)
    state.pending_intents_json = json.dumps([accepted])
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=TeardownRunnerHelpers(),
        config=TeardownConfig.default(),
    )

    result = await mgr._execute_intents(
        teardown_id="td-resume-accepted",
        strategy=_make_strategy(),
        intents=[accepted],
        positions=_make_position_summary(),
        mode=TeardownMode.SOFT,
        teardown_state=state,
        start_from_index=0,
        is_auto_mode=True,
    )

    assert result.success is False
    assert result.intents_succeeded == 0
    assert result.intents_failed == 1
    fake_compiler.compile.assert_not_called()
    fake_orchestrator.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_resume_routes_only_exact_accepted_order_cancel_through_commit_lane(
    fake_orchestrator, fake_compiler, state_manager_mock
):
    """An age-gated exact cancel restores liveness without re-submitting close."""
    order_key = "0x" + "59" * 32
    accepted = {
        "type": "PERP_CLOSE",
        "protocol": "gmx_v2",
        "chain": "arbitrum",
        "market": "ETH/USD",
        "collateral_token": "USDC",
        "is_long": True,
        "_teardown_async_submission_accepted": True,
        "_teardown_async_submission_order_keys": [order_key],
        "_teardown_async_submission_ledger_id": "ledger-old",
    }
    state = _make_state(total_intents=1)
    state.pending_intents_json = json.dumps([accepted])

    async def _check(*_args, **_kwargs):
        return "unproven"

    cancel = SimpleNamespace(
        intent_type=SimpleNamespace(value="PERP_CANCEL_ORDER"),
        protocol="gmx_v2",
        chain="arbitrum",
        order_key=order_key,
        max_slippage=Decimal("0"),
    )
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=TeardownRunnerHelpers(check_intent_settlement=_check),
        config=TeardownConfig.default(),
    )

    result = await mgr._execute_intents(
        teardown_id="td-resume-exact-cancel",
        strategy=_make_strategy(),
        intents=[accepted],
        positions=_make_position_summary(),
        mode=TeardownMode.SOFT,
        teardown_state=state,
        start_from_index=0,
        is_auto_mode=True,
        accepted_async_recovery_intents=[cancel],
    )

    assert result.success is False
    assert result.completed_at is None
    assert state.status == TeardownStatus.EXECUTING
    assert json.loads(state.pending_intents_json) == [accepted]
    assert fake_compiler.compile.call_count == 1
    assert fake_compiler.compile.call_args.args[0] is cancel
    assert fake_orchestrator.execute.await_count == 1


@pytest.mark.asyncio
async def test_resume_completes_booked_executed_async_submission(fake_orchestrator, fake_compiler, state_manager_mock):
    """A late keeper fill must complete resume without resubmitting the close."""
    order_key = "0x" + "56" * 32
    accepted = {
        "type": "PERP_CLOSE",
        "intent_type": "PERP_CLOSE",
        "protocol": "gmx_v2",
        "chain": "arbitrum",
        "market": "ETH/USD",
        "collateral_token": "USDC",
        "is_long": True,
        "_teardown_async_submission_accepted": True,
        "_teardown_async_submission_order_keys": [order_key],
        "_teardown_async_submission_ledger_id": "ledger-1",
    }
    state = _make_state(total_intents=1)
    state.pending_intents_json = json.dumps([accepted])
    callbacks: list[tuple[object, bool]] = []

    async def _check(*_args, **_kwargs):
        return "executed"

    strategy = _make_strategy()
    strategy.on_intent_executed = lambda intent, success, _result: callbacks.append((intent, success))
    strategy.save_state = MagicMock()
    strategy.flush_pending_saves = AsyncMock()
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=TeardownRunnerHelpers(check_intent_settlement=_check),
        config=TeardownConfig.default(),
    )

    result = await mgr._execute_intents(
        teardown_id="td-resume-settled",
        strategy=strategy,
        intents=[accepted],
        positions=_make_position_summary(),
        mode=TeardownMode.SOFT,
        teardown_state=state,
        start_from_index=0,
        is_auto_mode=True,
    )

    assert result.success is True
    assert result.intents_succeeded == 1
    assert result.intents_failed == 0
    assert callbacks and callbacks[0][1] is True
    strategy.save_state.assert_called_once()
    strategy.flush_pending_saves.assert_awaited_once()
    fake_compiler.compile.assert_not_called()
    fake_orchestrator.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_resume_replaces_only_terminally_failed_async_submission(
    fake_orchestrator, fake_compiler, state_manager_mock
):
    """A cancelled/frozen old order cannot execute later, so one fresh close is safe."""
    order_key = "0x" + "57" * 32
    accepted = {
        "type": "PERP_CLOSE",
        "protocol": "gmx_v2",
        "chain": "arbitrum",
        "market": "ETH/USD",
        "collateral_token": "USDC",
        "is_long": True,
        "_teardown_async_submission_accepted": True,
        "_teardown_async_submission_order_keys": [order_key],
        "_teardown_async_submission_ledger_id": "ledger-old",
    }
    state = _make_state(total_intents=1)
    state.pending_intents_json = json.dumps([accepted])
    marker_cleared_before_execute = False

    async def _save(saved: TeardownState) -> None:
        nonlocal marker_cleared_before_execute
        payload = json.loads(saved.pending_intents_json)[0]
        if "_teardown_async_submission_accepted" not in payload:
            marker_cleared_before_execute = True

    state_manager_mock.save_teardown_state.side_effect = _save

    async def _check(*_args, **_kwargs):
        return "terminal_failed"

    async def _settle(*_args, **_kwargs):
        assert marker_cleared_before_execute is True
        return None

    helpers, commit_calls = _make_helpers(await_intent_settlement=_settle)
    helpers = replace(helpers, check_intent_settlement=_check)
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=helpers,
        config=TeardownConfig.default(),
    )

    result = await mgr._execute_intents(
        teardown_id="td-resume-terminal-failed",
        strategy=_make_strategy(),
        intents=[accepted],
        positions=_make_position_summary(),
        mode=TeardownMode.SOFT,
        teardown_state=state,
        start_from_index=0,
        is_auto_mode=True,
    )

    assert result.success is True
    assert result.intents_succeeded == 1
    assert fake_compiler.compile.call_count == 1
    assert fake_orchestrator.execute.await_count == 1
    assert len(commit_calls) == 1


@pytest.mark.asyncio
async def test_terminal_failed_replacement_requires_durable_marker_clear(
    fake_orchestrator, fake_compiler, state_manager_mock
):
    accepted = {
        "type": "PERP_CLOSE",
        "protocol": "gmx_v2",
        "chain": "arbitrum",
        "market": "ETH/USD",
        "collateral_token": "USDC",
        "is_long": True,
        "_teardown_async_submission_accepted": True,
        "_teardown_async_submission_order_keys": ["0x" + "58" * 32],
        "_teardown_async_submission_ledger_id": "ledger-old",
    }
    state = _make_state(total_intents=1)
    state.pending_intents_json = json.dumps([accepted])
    state_manager_mock.save_teardown_state.side_effect = RuntimeError("state backend unavailable")

    async def _check(*_args, **_kwargs):
        return "terminal_failed"

    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=TeardownRunnerHelpers(check_intent_settlement=_check),
        config=TeardownConfig.default(),
    )

    with pytest.raises(RuntimeError, match="state backend unavailable"):
        await mgr._execute_intents(
            teardown_id="td-terminal-failed-save-fault",
            strategy=_make_strategy(),
            intents=[accepted],
            positions=_make_position_summary(),
            mode=TeardownMode.SOFT,
            teardown_state=state,
            start_from_index=0,
            is_auto_mode=True,
        )

    assert json.loads(state.pending_intents_json) == [accepted]
    fake_compiler.compile.assert_not_called()
    fake_orchestrator.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_terminal_failed_malformed_replacement_is_counted_without_dispatch(
    fake_orchestrator, fake_compiler, state_manager_mock
):
    accepted = {
        "type": "UNKNOWN_CLOSE",
        "protocol": "gmx_v2",
        "chain": "arbitrum",
        "_teardown_async_submission_accepted": True,
        "_teardown_async_submission_order_keys": ["0x" + "59" * 32],
        "_teardown_async_submission_ledger_id": "ledger-old",
    }
    state = _make_state(total_intents=1)
    state.pending_intents_json = json.dumps([accepted])

    async def _check(*_args, **_kwargs):
        return "terminal_failed"

    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=TeardownRunnerHelpers(check_intent_settlement=_check),
        config=TeardownConfig.default(),
    )

    result = await mgr._execute_intents(
        teardown_id="td-terminal-failed-malformed",
        strategy=_make_strategy(),
        intents=[accepted],
        positions=_make_position_summary(),
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )

    assert result.success is False
    assert result.intents_failed == 1
    assert result.error == "1 intents failed"
    fake_compiler.compile.assert_not_called()
    fake_orchestrator.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_resume_retains_accepted_close_and_never_resubmits(
    fake_orchestrator, fake_compiler, state_manager_mock
):
    """Plan regeneration must preserve the durable no-resubmit identity."""
    payload = {
        "intent_type": "PERP_CLOSE",
        "protocol": "gmx_v2",
        "chain": "arbitrum",
        "market": "ETH/USD",
        "collateral_token": "USDC",
        "is_long": True,
    }
    accepted = {**payload, "_teardown_async_submission_accepted": True}
    state = _make_state(total_intents=1)
    state.completed_intents = 1
    state.pending_intents_json = json.dumps([accepted])
    state.updated_at = datetime(2020, 1, 1, tzinfo=UTC)
    state_manager_mock.get_teardown_state = AsyncMock(return_value=state)

    # A fresh position read may spell the same market/collateral as on-chain
    # addresses rather than the strategy aliases stored in the accepted plan.
    # The correlated order-key plan must win; do not regenerate and attempt a
    # brittle alias-to-address identity match.
    regenerated = SimpleNamespace(
        to_dict=lambda: {
            **payload,
            "market": "0x" + "11" * 20,
            "collateral_token": "0x" + "22" * 20,
        }
    )
    generate_teardown_intents = MagicMock(return_value=[regenerated])
    strategy = SimpleNamespace(
        deployment_id="dep-1",
        chain="arbitrum",
        wallet_address="0xWALLET",
        get_open_positions=lambda: _make_position_summary(),
        generate_teardown_intents=generate_teardown_intents,
    )
    config = TeardownConfig.default()
    config.staleness_threshold_seconds = 1
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=TeardownRunnerHelpers(),
        config=config,
    )

    result = await mgr.resume(deployment_id="dep-1", strategy=strategy)

    assert result is not None
    assert result.success is False
    assert result.intents_failed == 1
    assert json.loads(state.pending_intents_json) == [accepted]
    assert state.status == TeardownStatus.EXECUTING
    assert state.is_resumable is True
    generate_teardown_intents.assert_not_called()
    fake_compiler.compile.assert_not_called()
    fake_orchestrator.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_terminal_settlement_accounting_degradation_surfaces_on_result(
    fake_orchestrator, fake_compiler, state_manager_mock
):
    """Phase-2 degradation keeps the correlated request active without resubmission."""
    record = SimpleNamespace(error="perp settlement write failed")

    async def _settle(*_args, **_kwargs):
        return None

    async def _reconcile(*_args, **_kwargs):
        return (record,)

    helpers, _commit_calls = _make_helpers(
        await_intent_settlement=_settle,
        reconcile_intent_settlement=_reconcile,
    )
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=helpers,
        config=TeardownConfig.default(),
    )

    state = _make_state(total_intents=1)
    state.pending_intents_json = json.dumps([_make_intent("PERP_CLOSE").to_dict()])
    result = await mgr._execute_intents(
        teardown_id="td-phase2-degraded",
        strategy=_make_strategy(),
        intents=[_make_intent("PERP_CLOSE")],
        positions=_make_position_summary(),
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )

    assert result.success is False
    assert result.error == "Accepted async submission remains unsettled; teardown is resumable"
    assert state.status == TeardownStatus.EXECUTING
    assert fake_orchestrator.execute.await_count == 1
    assert result.accounting_degraded is True
    assert result.accounting_degraded_count == 1


# ---------------------------------------------------------------------------
# VIB-5117 — native-principal snapshot threads into commit on the teardown lane
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_native_principal_threaded_into_commit_for_lp_close(fake_orchestrator, fake_compiler, state_manager_mock):
    """When the runner advertises the native-principal snapshot hook, the manager
    captures it pre-execute and forwards it into ``commit(...)`` for the LP_CLOSE
    intent — lane-symmetric with the iteration lane. Guards the teardown wiring
    (CodeRabbit PR #2810): a native V4 close in teardown must book its real native
    proceeds, not 0.
    """
    captured_principal = (123_456, 789)
    snapshot_calls: list[tuple] = []

    async def _native_principal_snapshot(strategy, intent):
        snapshot_calls.append((strategy, intent))
        return captured_principal

    helpers, commit_calls = _make_helpers(native_principal_snapshot=_native_principal_snapshot)
    assert helpers.has_v4_lp_close_native_principal is True

    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=helpers,
        config=TeardownConfig.default(),
    )

    intent = _make_intent("LP_CLOSE")
    state = _make_state(total_intents=1)
    result = await mgr._execute_intents(
        teardown_id=state.teardown_id,
        strategy=_make_strategy(),
        intents=[intent],
        positions=_make_position_summary(),
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )

    assert result.success is True
    assert result.intents_succeeded == 1
    # The snapshot hook fired once (pre-execute) and its pair reached commit.
    assert len(snapshot_calls) == 1
    assert len(commit_calls) == 1
    assert commit_calls[0]["intent_type"] == "LP_CLOSE"
    assert commit_calls[0]["v4_lp_close_native_principal"] == captured_principal


@pytest.mark.asyncio
async def test_no_native_principal_snapshot_forwards_none(fake_orchestrator, fake_compiler, state_manager_mock):
    """No native-principal hook (non-V4 runner) → commit receives None for the
    principal — the gate is off, never a fabricated value (Empty ≠ Zero)."""
    helpers, commit_calls = _make_helpers()  # native_principal_snapshot=None
    assert helpers.has_v4_lp_close_native_principal is False

    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=helpers,
        config=TeardownConfig.default(),
    )

    state = _make_state(total_intents=1)
    result = await mgr._execute_intents(
        teardown_id=state.teardown_id,
        strategy=_make_strategy(),
        intents=[_make_intent("LP_CLOSE")],
        positions=_make_position_summary(),
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )

    assert result.success is True
    assert commit_calls[0]["v4_lp_close_native_principal"] is None


# ---------------------------------------------------------------------------
# T10 — happy-path 2-intent teardown: 2 commit calls, no degradation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t10_happy_path_two_intents_two_commits(fake_orchestrator, fake_compiler, state_manager_mock):
    helpers, commit_calls = _make_helpers()
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=helpers,
        config=TeardownConfig.default(),
    )

    intents = [_make_intent("LP_CLOSE"), _make_intent("SWAP")]
    state = _make_state(total_intents=2)
    positions = _make_position_summary()
    strategy = _make_strategy()

    result = await mgr._execute_intents(
        teardown_id=state.teardown_id,
        strategy=strategy,
        intents=intents,
        positions=positions,
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )

    assert result.success is True
    assert result.intents_succeeded == 2
    assert result.accounting_degraded is False
    assert result.accounting_degraded_count == 0
    assert len(commit_calls) == 2
    assert [c["intent_type"] for c in commit_calls] == ["LP_CLOSE", "SWAP"]
    # All cycle ids carry the teardown prefix.
    assert all(c["teardown_cycle_id"].startswith("teardown-") for c in commit_calls)


# ---------------------------------------------------------------------------
# T12 — degraded commit on intent #1 → intent #2 still runs, result reports it
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t12_degraded_commit_does_not_halt_unwind(fake_orchestrator, fake_compiler, state_manager_mock):
    from almanak.framework.accounting.deferred_log import DeferredWrite

    degraded_record = DeferredWrite.now(
        kind="ledger",
        deployment_id="dep-1",
        cycle_id="teardown-td-uuid-7",
        intent_type="LP_CLOSE",
        tx_hash="0xabc",
        error="forced ledger fail",
    )
    helpers, commit_calls = _make_helpers(
        commit_outcomes=[
            TeardownCommitOutcome(
                ledger_entry_id=None,
                accounting_degraded=True,
                degraded_reason="ledger: AccountingPersistenceError",
                degraded_writes=(degraded_record,),
            ),
            TeardownCommitOutcome(
                ledger_entry_id="ledger-2",
                accounting_degraded=False,
                degraded_reason=None,
            ),
        ]
    )
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=helpers,
        config=TeardownConfig.default(),
    )

    intents = [_make_intent("LP_CLOSE"), _make_intent("SWAP")]
    state = _make_state(total_intents=2)
    positions = _make_position_summary()
    strategy = _make_strategy()

    result = await mgr._execute_intents(
        teardown_id=state.teardown_id,
        strategy=strategy,
        intents=intents,
        positions=positions,
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )

    # Both intents executed on-chain — the degraded contract did NOT halt.
    assert result.intents_succeeded == 2
    assert result.success is True  # chain-side OK
    # But the result flags the degraded accounting.
    assert result.accounting_degraded is True
    assert result.accounting_degraded_count >= 1
    assert len(commit_calls) == 2


# ---------------------------------------------------------------------------
# Backward compat: when runner_helpers is None / empty, manager still works
# (legacy behaviour preserved for tests that don't construct a runner).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_runner_helpers_legacy_path_still_succeeds(fake_orchestrator, fake_compiler, state_manager_mock):
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        config=TeardownConfig.default(),
    )

    intent = _make_intent("SWAP")
    state = _make_state(total_intents=1)
    positions = _make_position_summary()
    strategy = _make_strategy()

    result = await mgr._execute_intents(
        teardown_id=state.teardown_id,
        strategy=strategy,
        intents=[intent],
        positions=positions,
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )
    assert result.success is True
    assert result.accounting_degraded is False
    assert result.accounting_degraded_count == 0
