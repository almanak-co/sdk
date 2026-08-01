"""Tests for ``execute_teardown_via_manager``'s VIB-3773 wiring (T13).

Specifically:

* T13 — the outer cycle-id swap stamps both ``runner._last_cycle_id`` and
  the contextvar to ``teardown-{teardown_id}`` for the duration of the
  teardown, and restores them on exit (success path).
* The pre/post snapshot brackets fire when the runner-helpers bag carries
  a ``capture_snapshot`` callable.
* The brackets do NOT halt the teardown when they report degraded
  accounting — the result still maps to TEARDOWN, and
  ``accounting_degraded_count`` is incremented.

These are integration-shaped: we monkeypatch the TeardownManager + adapter
just enough to drive the surrounding ``execute_teardown_via_manager`` body
end-to-end without spinning up real orchestration.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.observability.context import (
    clear_cycle_id,
    get_cycle_id,
    set_cycle_id,
)
from almanak.framework.runner._run_loop_helpers import TeardownSnapshotOutcome
from almanak.framework.runner.runner_teardown import execute_teardown, execute_teardown_via_manager
from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownPositionSummary,
    TeardownResult,
    TeardownState,
    TeardownStatus,
)
from almanak.framework.teardown.runner_helpers import TeardownRunnerHelpers


def _make_teardown_state(*, teardown_id: str = "td-uuid-13") -> TeardownState:
    now = datetime.now(UTC)
    return TeardownState(
        teardown_id=teardown_id,
        deployment_id="strat-1",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=1,
        completed_intents=0,
        current_intent_index=0,
        started_at=now,
        updated_at=now,
    )


def _make_teardown_result(*, accounting_degraded: bool = False) -> TeardownResult:
    now = datetime.now(UTC)
    return TeardownResult(
        success=True,
        deployment_id="strat-1",
        mode="graceful",
        started_at=now,
        completed_at=now,
        duration_seconds=2.5,
        intents_total=1,
        intents_succeeded=1,
        intents_failed=0,
        starting_value_usd=Decimal("4.0"),
        final_value_usd=Decimal("4.0"),
        total_costs_usd=Decimal("0.05"),
        final_balances={},
        accounting_degraded=accounting_degraded,
        accounting_degraded_count=int(accounting_degraded),
    )


def _phase1_close_row(ledger_id: str, teardown_id: str, order_key: str | None) -> dict[str, Any]:
    orders = [] if order_key is None else [{"order_id": order_key, "is_long": True}]
    return {
        "id": ledger_id,
        "cycle_id": f"teardown-{teardown_id}",
        "intent_type": "PERP_CLOSE",
        "protocol": "gmx_v2",
        "success": True,
        "extracted_data_json": json.dumps({"async_orders": orders}),
    }


def _phase1_inventory(*rows: dict[str, Any], measured: bool = True, reason: str | None = None) -> Any:
    return SimpleNamespace(rows=tuple(rows), measured=measured, degraded_reason=reason)


def _strategy() -> Any:
    return SimpleNamespace(
        deployment_id="dep-1",
        chain="arbitrum",
        wallet_address="0xWALLET",
        get_open_positions=lambda: TeardownPositionSummary(
            deployment_id="strat-1",
            timestamp=datetime.now(UTC),
            positions=[],
            total_value_usd=Decimal("4.0"),
        ),
    )


def _make_runner() -> MagicMock:
    runner = MagicMock(name="StrategyRunner")
    runner._last_cycle_id = "outer-iter-cycle"
    runner._total_iterations = 7
    runner.alert_manager = MagicMock()
    runner.alert_manager.send_teardown_complete = AsyncMock()
    runner.execution_orchestrator = MagicMock()
    runner.config = SimpleNamespace(allow_unsafe_teardown_fallback=False, chain="arbitrum")
    runner._build_teardown_compiler = MagicMock(return_value=MagicMock())
    runner._calculate_duration_ms = MagicMock(return_value=2500)
    runner._record_success = MagicMock()
    runner.request_shutdown = MagicMock()
    runner._lifecycle_write_state = MagicMock()
    runner._request_teardown_failure_shutdown = MagicMock()
    return runner


@pytest.fixture
def patched_helpers(monkeypatch: pytest.MonkeyPatch):
    """Patch out the heavy phases so we test the *brackets* in isolation.

    Yields a dict of records the test can inspect:
        commit_calls       — list of (intent_type, cycle_id) tuples
        snapshot_calls     — list of dicts {phase, cycle_id, runner_last_cycle_id, ctx_cycle_id}
        teardown_state     — the synthetic TeardownState the wiring uses
    """
    snapshot_calls: list[dict] = []
    commit_calls: list[dict] = []

    async def _fake_capture_snapshot(strategy, *, teardown_cycle_id, pre_teardown):
        snapshot_calls.append(
            {
                "phase": "pre" if pre_teardown else "post",
                "teardown_cycle_id": teardown_cycle_id,
                "runner_last_cycle_id": getattr(strategy, "_observed_last_cycle_id", None),
                "ctx_cycle_id": get_cycle_id(),
            }
        )
        return TeardownSnapshotOutcome(
            snapshot_captured=True,
            accounting_degraded=False,
            degraded_reason=None,
            phase="pre" if pre_teardown else "post",
        )

    async def _fake_commit(strategy, intent, **kwargs):
        commit_calls.append(
            {
                "intent_type": getattr(intent.intent_type, "value", str(intent.intent_type)),
                "teardown_cycle_id": kwargs.get("teardown_cycle_id"),
            }
        )
        from almanak.framework.runner.teardown_commit import TeardownCommitOutcome

        return TeardownCommitOutcome(
            ledger_entry_id="ledger-1",
            accounting_degraded=False,
            degraded_reason=None,
        )

    helpers = TeardownRunnerHelpers(commit=_fake_commit, capture_snapshot=_fake_capture_snapshot)

    state = _make_teardown_state()

    # Patch the lazily-imported phase helpers so the body is exercised but
    # heavy work is mocked.
    from almanak.framework.runner import _teardown_helpers as _h

    monkeypatch.setattr(_h, "fetch_positions_or_fallback", AsyncMock(
        return_value=(SimpleNamespace(total_value_usd=Decimal("4.0"), positions=[]), None)
    ))
    monkeypatch.setattr(_h, "validate_safety_or_error", MagicMock(return_value=None))
    monkeypatch.setattr(_h, "run_cancel_window_and_persist", AsyncMock(
        return_value=(state, None)
    ))
    monkeypatch.setattr(_h, "resolve_price_oracle", MagicMock(return_value={}))
    monkeypatch.setattr(_h, "execute_and_verify", AsyncMock(
        return_value=_make_teardown_result()
    ))
    monkeypatch.setattr(_h, "send_alert_and_cleanup", AsyncMock())

    # The map_teardown_result helper is what builds the IterationResult; we
    # just need a return value here.
    from almanak.framework.runner.runner_models import IterationResult, IterationStatus

    fake_iter_result = IterationResult(
        status=IterationStatus.TEARDOWN,
        intent=None,
        deployment_id="strat-1",
        duration_ms=2500,
    )
    monkeypatch.setattr(_h, "map_teardown_result", MagicMock(return_value=fake_iter_result))

    # Build the manager mock with the helper bag attached.
    mgr = MagicMock(name="TeardownManager")
    mgr.runner_helpers = helpers
    adapter = MagicMock()
    adapter.get_teardown_state = AsyncMock(return_value=None)
    monkeypatch.setattr(_h, "build_teardown_manager", MagicMock(return_value=(mgr, adapter)))

    return {
        "commit_calls": commit_calls,
        "snapshot_calls": snapshot_calls,
        "state": state,
        "manager": mgr,
    }


# ---------------------------------------------------------------------------
# T13 — outer cycle-id swap and restore (dual surface)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t13_outer_cycle_id_swap_dual_surface(
    patched_helpers, monkeypatch: pytest.MonkeyPatch
):
    runner = _make_runner()
    set_cycle_id("outer-ctx-cycle")

    state_mgr = MagicMock(name="state_manager")
    state_mgr.db_path = None  # forces adapter default

    try:
        result = await execute_teardown_via_manager(
            runner=runner,
            strategy=_strategy(),
            teardown_intents=[],
            teardown_mode=TeardownMode.SOFT,
            teardown_market=None,
            start_time=datetime.now(UTC),
            request=None,
            state_manager=state_mgr,
        )

        # Brackets fired with the teardown cycle id.
        snap_calls = patched_helpers["snapshot_calls"]
        assert len(snap_calls) == 2
        assert {c["phase"] for c in snap_calls} == {"pre", "post"}
        expected_cycle = f"teardown-{patched_helpers['state'].teardown_id}"
        assert all(c["teardown_cycle_id"] == expected_cycle for c in snap_calls)
        assert all(c["ctx_cycle_id"] == expected_cycle for c in snap_calls)

        # After return, the runner's _last_cycle_id has been restored to its
        # pre-call value — NOT left at the teardown cycle id. Same for the
        # contextvar. The test fixture's own ``finally`` below will clean
        # up; we MUST NOT clear before this assertion runs.
        assert runner._last_cycle_id == "outer-iter-cycle"
        assert get_cycle_id() == "outer-ctx-cycle"

        assert result is not None  # mapped IterationResult
    finally:
        clear_cycle_id()


@pytest.mark.asyncio
async def test_production_reentry_resumes_correlated_accepted_async_plan(
    patched_helpers, monkeypatch: pytest.MonkeyPatch
):
    """Runner re-entry must not persist a fresh submit-ready teardown plan."""
    from almanak.framework.runner import _teardown_helpers as _h

    state = patched_helpers["state"]
    state.pending_intents_json = json.dumps(
        [
            {
                "type": "PERP_CLOSE",
                "protocol": "gmx_v2",
                "_teardown_async_submission_accepted": True,
                "_teardown_async_submission_order_keys": ["0x" + "77" * 32],
            }
        ]
    )
    adapter = MagicMock(name="teardown_state_adapter")
    adapter.get_teardown_state = AsyncMock(return_value=state)
    monkeypatch.setattr(
        _h,
        "build_teardown_manager",
        MagicMock(return_value=(patched_helpers["manager"], adapter)),
    )

    runner = _make_runner()
    runner._teardown_recovery_incomplete = True
    runner._teardown_recovery_warning = "accepted order was not yet cancellable"
    runner._teardown_lp_recovery_incomplete = False
    runner._teardown_lp_recovery_warning = None
    runner._teardown_pending_recovery_keys = frozenset({"0x" + "77" * 32})
    state_mgr = MagicMock(name="state_manager")
    state_mgr.db_path = None

    await execute_teardown_via_manager(
        runner=runner,
        strategy=_strategy(),
        teardown_intents=[SimpleNamespace(intent_type="PERP_CLOSE")],
        teardown_mode=TeardownMode.SOFT,
        teardown_market=None,
        start_time=datetime.now(UTC),
        request=SimpleNamespace(requested_by="system"),
        state_manager=state_mgr,
    )

    _h.run_cancel_window_and_persist.assert_not_awaited()
    assert _h.execute_and_verify.await_args.kwargs["resume_accepted_async"] is True
    assert _h.execute_and_verify.await_args.args[3] is state
    assert _h.map_teardown_result.call_args.args[3].success is True


@pytest.mark.asyncio
async def test_unmeasured_accepted_recovery_defers_manager_without_fresh_dispatch(
    patched_helpers, monkeypatch: pytest.MonkeyPatch
):
    """A transient ledger read cannot fall through to plan persistence/execution."""
    from almanak.framework.runner import _teardown_helpers as _h
    from almanak.framework.runner import perp_settlement_reconciler as reconciler
    from almanak.framework.runner.runner_models import IterationStatus

    state = patched_helpers["state"]
    state.pending_intents_json = json.dumps([{"type": "PERP_CLOSE", "protocol": "gmx_v2"}])
    adapter = MagicMock()
    adapter.get_teardown_state = AsyncMock(return_value=state)
    monkeypatch.setattr(
        _h,
        "build_teardown_manager",
        MagicMock(return_value=(patched_helpers["manager"], adapter)),
    )

    monkeypatch.setattr(
        reconciler,
        "_read_phase1_close_inventory",
        AsyncMock(return_value=_phase1_inventory(measured=False, reason="Phase-1 ledger inventory was unmeasured")),
    )
    runner = _make_runner()

    result = await execute_teardown_via_manager(
        runner=runner,
        strategy=_strategy(),
        teardown_intents=[SimpleNamespace(intent_type="PERP_CLOSE")],
        teardown_mode=TeardownMode.SOFT,
        teardown_market=None,
        start_time=datetime.now(UTC),
        request=SimpleNamespace(requested_by="system"),
        state_manager=SimpleNamespace(db_path=None),
    )

    assert result.status == IterationStatus.TEARDOWN
    assert result.error == "Phase-1 ledger inventory was unmeasured"
    _h.run_cancel_window_and_persist.assert_not_awaited()
    _h.execute_and_verify.assert_not_awaited()
    runner._request_teardown_failure_shutdown.assert_not_called()


@pytest.mark.asyncio
async def test_missing_marker_is_recovered_from_exact_phase1_ledger(monkeypatch: pytest.MonkeyPatch):
    """A failed marker save must not permit a fresh close on the next tick."""
    from almanak.framework.runner import perp_settlement_reconciler as reconciler
    from almanak.framework.runner import runner_teardown as rt

    state = _make_teardown_state(teardown_id="td-ledger-recovery")
    state.pending_intents_json = json.dumps(
        [{"type": "PERP_CLOSE", "protocol": "gmx_v2", "chain": "arbitrum"}]
    )
    adapter = MagicMock()
    adapter.get_teardown_state = AsyncMock(return_value=state)
    adapter.save_teardown_state = AsyncMock()
    order_key = "0x" + "79" * 32
    ledger = _phase1_close_row("ledger-close", "td-ledger-recovery", order_key)

    monkeypatch.setattr(
        reconciler,
        "_read_phase1_close_inventory",
        AsyncMock(return_value=_phase1_inventory(ledger)),
    )

    lookup = await rt._load_resumable_accepted_async_state(
        adapter,
        "strat-1",
        runner=_make_runner(),
    )

    assert lookup.state is state
    assert lookup.blocked_reason is None
    marker = json.loads(state.pending_intents_json)[0]
    assert marker["_teardown_async_submission_accepted"] is True
    assert marker["_teardown_async_submission_ledger_id"] == "ledger-close"
    assert marker["_teardown_async_submission_order_keys"] == [order_key]
    adapter.save_teardown_state.assert_awaited_once_with(state)


@pytest.mark.asyncio
async def test_missing_marker_recovery_refuses_ambiguous_same_protocol_plan(monkeypatch: pytest.MonkeyPatch):
    """Ledger recovery must never guess which same-protocol close was accepted."""
    from almanak.framework.runner import perp_settlement_reconciler as reconciler
    from almanak.framework.runner import runner_teardown as rt

    state = _make_teardown_state(teardown_id="td-ledger-ambiguous")
    state.pending_intents_json = json.dumps(
        [
            {"type": "PERP_CLOSE", "protocol": "gmx_v2", "position_id": "one"},
            {"type": "PERP_CLOSE", "protocol": "gmx_v2", "position_id": "two"},
        ]
    )
    adapter = MagicMock()
    adapter.get_teardown_state = AsyncMock(return_value=state)
    adapter.save_teardown_state = AsyncMock()
    ledger = _phase1_close_row("ledger-close", "td-ledger-ambiguous", "0x" + "80" * 32)

    monkeypatch.setattr(
        reconciler,
        "_read_phase1_close_inventory",
        AsyncMock(return_value=_phase1_inventory(ledger)),
    )

    lookup = await rt._load_resumable_accepted_async_state(
        adapter,
        "strat-1",
        runner=_make_runner(),
    )

    assert lookup.state is None
    assert "unique pending plan match" in str(lookup.blocked_reason)
    assert '"_teardown_async_submission_accepted": true' not in state.pending_intents_json
    adapter.save_teardown_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_marker_recovery_defers_when_inventory_is_unmeasured(monkeypatch: pytest.MonkeyPatch):
    """An unreadable ledger is not authoritative evidence that no close exists."""
    from almanak.framework.runner import perp_settlement_reconciler as reconciler
    from almanak.framework.runner import runner_teardown as rt

    state = _make_teardown_state(teardown_id="td-ledger-unmeasured")
    state.pending_intents_json = json.dumps([{"type": "PERP_CLOSE", "protocol": "gmx_v2"}])
    adapter = MagicMock()
    adapter.get_teardown_state = AsyncMock(return_value=state)

    monkeypatch.setattr(
        reconciler,
        "_read_phase1_close_inventory",
        AsyncMock(return_value=_phase1_inventory(measured=False, reason="Phase-1 ledger inventory was unmeasured")),
    )

    lookup = await rt._load_resumable_accepted_async_state(adapter, "strat-1", runner=_make_runner())

    assert lookup.state is None
    assert lookup.blocked_reason == "Phase-1 ledger inventory was unmeasured"


@pytest.mark.asyncio
async def test_missing_marker_recovery_defers_unhydrated_exact_cycle_close(monkeypatch: pytest.MonkeyPatch):
    """A measured candidate with an unmeasured hydrate must block dispatch."""
    from almanak.framework.runner import perp_settlement_reconciler as reconciler
    from almanak.framework.runner import runner_teardown as rt

    state = _make_teardown_state(teardown_id="td-ledger-unhydrated")
    state.pending_intents_json = json.dumps([{"type": "PERP_CLOSE", "protocol": "gmx_v2"}])
    adapter = MagicMock()
    adapter.get_teardown_state = AsyncMock(return_value=state)

    monkeypatch.setattr(
        reconciler,
        "_read_phase1_close_inventory",
        AsyncMock(return_value=_phase1_inventory(measured=False, reason="Phase-1 close ledger hydrate failed")),
    )

    lookup = await rt._load_resumable_accepted_async_state(adapter, "strat-1", runner=_make_runner())

    assert lookup.state is None
    assert lookup.blocked_reason == "Phase-1 close ledger hydrate failed"


@pytest.mark.asyncio
async def test_missing_marker_recovery_defers_measured_ledger_without_order_keys(monkeypatch: pytest.MonkeyPatch):
    """A measured Phase-1 row is not absent merely because key parsing is empty."""
    from almanak.framework.runner import perp_settlement_reconciler as reconciler
    from almanak.framework.runner import runner_teardown as rt

    state = _make_teardown_state(teardown_id="td-ledger-unkeyed")
    state.pending_intents_json = json.dumps([{"type": "PERP_CLOSE", "protocol": "gmx_v2"}])
    adapter = MagicMock()
    adapter.get_teardown_state = AsyncMock(return_value=state)
    ledger = _phase1_close_row("ledger-unkeyed", "td-ledger-unkeyed", None)
    monkeypatch.setattr(
        reconciler,
        "_read_phase1_close_inventory",
        AsyncMock(return_value=_phase1_inventory(ledger)),
    )

    lookup = await rt._load_resumable_accepted_async_state(adapter, "strat-1", runner=_make_runner())

    assert lookup.state is None
    assert lookup.blocked_reason == "the Phase-1 close ledger has no exact order keys"


@pytest.mark.asyncio
async def test_missing_marker_recovery_refuses_multiple_phase1_ledgers(monkeypatch: pytest.MonkeyPatch):
    """Two old close submissions cannot be collapsed into one accepted marker."""
    from almanak.framework.runner import perp_settlement_reconciler as reconciler
    from almanak.framework.runner import runner_teardown as rt

    state = _make_teardown_state(teardown_id="td-ledger-duplicate")
    state.pending_intents_json = json.dumps([{"type": "PERP_CLOSE", "protocol": "gmx_v2"}])
    adapter = MagicMock()
    adapter.get_teardown_state = AsyncMock(return_value=state)
    rows = (
        _phase1_close_row("ledger-81", "td-ledger-duplicate", "0x" + "81" * 32),
        _phase1_close_row("ledger-82", "td-ledger-duplicate", None),
    )

    monkeypatch.setattr(
        reconciler,
        "_read_phase1_close_inventory",
        AsyncMock(return_value=_phase1_inventory(*rows)),
    )

    lookup = await rt._load_resumable_accepted_async_state(adapter, "strat-1", runner=_make_runner())

    assert lookup.state is None
    assert lookup.blocked_reason == "multiple Phase-1 close ledgers match this teardown cycle"
    adapter.save_teardown_state.assert_not_called()


@pytest.mark.asyncio
async def test_zero_generated_intents_still_route_persisted_accepted_plan(monkeypatch: pytest.MonkeyPatch):
    """A late keeper fill must not hit the generic no-positions completion path."""
    from almanak.framework import teardown as teardown_module
    from almanak.framework.runner import runner_teardown as rt
    from almanak.framework.runner.runner_models import IterationResult, IterationStatus

    state = _make_teardown_state(teardown_id="td-zero-intents-resume")
    accepted = {
        "type": "PERP_CLOSE",
        "protocol": "gmx_v2",
        "_teardown_async_submission_accepted": True,
        "_teardown_async_submission_order_keys": ["0x" + "78" * 32],
    }
    state.pending_intents_json = json.dumps([accepted])
    runner = _make_runner()
    runner._is_multi_chain = False
    runner._get_gateway_client = MagicMock(return_value=MagicMock())
    runner._execute_teardown_via_manager = AsyncMock(
        return_value=IterationResult(
            status=IterationStatus.TEARDOWN,
            deployment_id="dep-1",
            duration_ms=1,
        )
    )
    strategy = _strategy()
    strategy.create_market_snapshot = MagicMock(return_value=None)
    strategy.generate_teardown_intents = MagicMock(return_value=[])
    request_manager = MagicMock()
    request_manager.get_active_request.return_value = SimpleNamespace(requested_by="system")

    monkeypatch.setattr(
        teardown_module,
        "get_teardown_state_manager_for_runtime",
        lambda **_kwargs: request_manager,
    )
    monkeypatch.setattr(
        rt,
        "_recover_orphaned_lp_intents",
        AsyncMock(side_effect=lambda _r, _s, intents, _m: (intents, False, None)),
    )
    monkeypatch.setattr(
        rt,
        "_recover_pending_order_intents",
        AsyncMock(side_effect=lambda _r, _s, intents, _m: (intents, False, None)),
    )
    monkeypatch.setattr(rt, "_apply_lending_unwind_guard", lambda intents, *_args, **_kwargs: intents)
    monkeypatch.setattr(
        rt,
        "_load_runtime_resumable_accepted_async_state",
        AsyncMock(return_value=rt._AcceptedAsyncResumeLookup(state=state)),
    )

    result = await execute_teardown(
        runner,
        strategy,
        TeardownMode.SOFT,
        datetime.now(UTC),
    )

    assert result.status == IterationStatus.TEARDOWN
    runner._execute_teardown_via_manager.assert_awaited_once()
    assert runner._execute_teardown_via_manager.await_args.kwargs["teardown_intents"] == [accepted]
    request_manager.mark_completed.assert_not_called()
    runner.request_shutdown.assert_not_called()


def test_unsettled_async_result_keeps_request_active_and_runner_alive():
    from almanak.framework.runner import _teardown_helpers as _h
    from almanak.framework.runner.runner_models import IterationStatus
    from almanak.framework.teardown.teardown_manager import ASYNC_SETTLEMENT_PENDING_ERROR

    runner = _make_runner()
    request = MagicMock()
    state_manager = MagicMock()
    now = datetime.now(UTC)
    result = TeardownResult(
        success=False,
        deployment_id="dep-1",
        mode="graceful",
        started_at=now,
        completed_at=None,
        duration_seconds=1.0,
        intents_total=1,
        intents_succeeded=0,
        intents_failed=1,
        starting_value_usd=Decimal("4"),
        final_value_usd=Decimal("4"),
        total_costs_usd=Decimal("0"),
            final_balances={},
            error=ASYNC_SETTLEMENT_PENDING_ERROR,
            async_settlement_pending=True,
        )

    mapped = _h.map_teardown_result(
        runner,
        _strategy(),
        now,
        result,
        TeardownMode.SOFT,
        request,
        state_manager,
    )

    assert mapped.status == IterationStatus.TEARDOWN
    state_manager.mark_failed.assert_not_called()
    runner._request_teardown_failure_shutdown.assert_not_called()
    runner.request_shutdown.assert_not_called()


# ---------------------------------------------------------------------------
# Pre/post snapshot bracket — degraded outcome bumps the count, doesn't halt
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_degraded_snapshot_bracket_increments_result_count(
    monkeypatch: pytest.MonkeyPatch,
):
    """Both brackets report degraded → final IterationResult still TEARDOWN
    and the upstream TeardownResult is mutated to flag accounting_degraded.
    """
    runner = _make_runner()

    async def _degraded_capture(strategy, *, teardown_cycle_id, pre_teardown):
        return TeardownSnapshotOutcome(
            snapshot_captured=False,
            accounting_degraded=True,
            degraded_reason=f"snapshot/{('pre' if pre_teardown else 'post')}: forced",
            phase="pre" if pre_teardown else "post",
        )

    async def _ok_commit(*args, **kwargs):
        from almanak.framework.runner.teardown_commit import TeardownCommitOutcome

        return TeardownCommitOutcome(
            ledger_entry_id="ledger-1", accounting_degraded=False, degraded_reason=None
        )

    helpers = TeardownRunnerHelpers(commit=_ok_commit, capture_snapshot=_degraded_capture)

    state = _make_teardown_state(teardown_id="td-degrade")

    from almanak.framework.runner import _teardown_helpers as _h

    monkeypatch.setattr(
        _h,
        "fetch_positions_or_fallback",
        AsyncMock(
            return_value=(
                SimpleNamespace(total_value_usd=Decimal("4.0"), positions=[]),
                None,
            )
        ),
    )
    monkeypatch.setattr(_h, "validate_safety_or_error", MagicMock(return_value=None))
    monkeypatch.setattr(
        _h, "run_cancel_window_and_persist", AsyncMock(return_value=(state, None))
    )
    monkeypatch.setattr(_h, "resolve_price_oracle", MagicMock(return_value={}))

    teardown_result_holder: dict[str, Any] = {}

    async def _fake_execute_and_verify(*args, **kwargs):
        result = _make_teardown_result()
        teardown_result_holder["result"] = result
        return result

    monkeypatch.setattr(_h, "execute_and_verify", _fake_execute_and_verify)
    monkeypatch.setattr(_h, "send_alert_and_cleanup", AsyncMock())
    from almanak.framework.runner.runner_models import IterationResult, IterationStatus

    monkeypatch.setattr(
        _h,
        "map_teardown_result",
        MagicMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN,
                intent=None,
                deployment_id="strat-1",
                duration_ms=2500,
            )
        ),
    )

    mgr = MagicMock(name="TeardownManager")
    mgr.runner_helpers = helpers
    adapter = MagicMock()
    adapter.get_teardown_state = AsyncMock(return_value=None)
    monkeypatch.setattr(
        _h, "build_teardown_manager", MagicMock(return_value=(mgr, adapter))
    )

    state_mgr = MagicMock(name="state_manager")
    state_mgr.db_path = None

    await execute_teardown_via_manager(
        runner=runner,
        strategy=_strategy(),
        teardown_intents=[],
        teardown_mode=TeardownMode.SOFT,
        teardown_market=None,
        start_time=datetime.now(UTC),
        request=None,
        state_manager=state_mgr,
    )

    # Both brackets failed → the TeardownResult was mutated to reflect that.
    assert teardown_result_holder["result"].accounting_degraded is True
    assert teardown_result_holder["result"].accounting_degraded_count >= 2
