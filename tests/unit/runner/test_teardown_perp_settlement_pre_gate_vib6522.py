"""Teardown books keeper-settled perp orders BEFORE the completeness gate (VIB-6522).

The iteration lane's PERP_CLOSE cannot key ``position_registry`` at submission
time — the venue positionKey is measured at keeper fill (two-phase design,
VIB-3872 §3 D2) — so the registry open→closed transition is owned by the perp
settlement reconciler's registry-completion write, which normally runs as the
pre-decide Step 1.7. Teardown routing (Step 0a) early-exits BEFORE that step,
so a teardown requested inside the close-to-next-tick window used to find the
row still ``'open'``: the fail-closed completeness gate refused, the FAILED
entry latch (VIB-5572) then starved Step 1.7 forever, and the deployment could
never be torn down again (mainnet repro: run 20260804-1325-gmxpipes-dir-arb,
deployment:acdb82bc087a).

The fix (Step T0.5 in ``execute_teardown``) runs one correlated reconcile tick
before intent generation / enumeration / the gate. These tests drive the REAL
``execute_teardown`` body and the REAL ``check_intent_coverage`` gate, faking
only the process boundaries:

* the reconciler is replaced by a fake that transitions a shared registry row
  open→closed when (and only when) it is invoked — the observable contract of
  the settlement commit lane's ``_complete_registry`` on a keeper-EXECUTED
  close;
* registry-reconciled enumeration reads that same shared row, so the gate sees
  exactly what the reconciler did (or did not) write.

Negative control: on pre-fix code the reconcile step does not exist, the row
stays open, the REAL gate fails closed, and
``test_teardown_reconciles_settlement_before_completeness_gate`` FAILS.

Liveness control: ``test_reconcile_failure_leaves_gate_fail_closed`` proves the
gate still fires on a stale-open row — i.e. the passing test above passes
because the row was closed, not because the gate stopped looking.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.perp_settlement_reconciler import PerpSettlementReconcileOutcome
from almanak.framework.runner.runner_models import IterationResult, IterationStatus
from almanak.framework.runner.runner_teardown import execute_teardown
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownMode,
    TeardownPositionSummary,
)

_DEPLOYMENT_ID = "deployment:vib6522test"
_POSITION_KEY = "0x7afaf95363543e89218bf62ebd48c55fca929acf5b183345ad7a23382945bbaf"


def _summary_for(registry_row: dict[str, str]) -> TeardownPositionSummary:
    """Registry-reconciled enumeration truth: one tracked-open PERP iff the row is open."""
    positions = []
    if registry_row["status"] == "open":
        positions.append(
            PositionInfo(
                position_type=PositionType.PERP,
                position_id=_POSITION_KEY,
                chain="arbitrum",
                protocol="gmx_v2",
                value_usd=Decimal("5"),
            )
        )
    return TeardownPositionSummary(
        deployment_id=_DEPLOYMENT_ID,
        timestamp=datetime.now(UTC),
        positions=positions,
        total_value_usd=Decimal("5"),
    )


def _make_runner() -> MagicMock:
    runner = MagicMock(name="StrategyRunner")
    runner._last_cycle_id = "iter-cycle-1"
    runner._is_multi_chain = False
    runner.config = SimpleNamespace(allow_unsafe_teardown_fallback=False, chain="arbitrum")
    runner._get_gateway_client = MagicMock(return_value=MagicMock(name="gateway_client"))
    runner._calculate_duration_ms = MagicMock(return_value=1500)
    runner._record_success = MagicMock()
    runner.request_shutdown = MagicMock()
    runner._lifecycle_write_state = MagicMock()
    runner._request_teardown_failure_shutdown = MagicMock()
    runner._create_error_result = MagicMock(
        side_effect=lambda deployment_id, status, error, _start: IterationResult(
            status=status,
            deployment_id=deployment_id,
            duration_ms=1,
            error=error,
        )
    )
    return runner


def _make_strategy(events: list[str]) -> Any:
    strategy = SimpleNamespace(
        deployment_id=_DEPLOYMENT_ID,
        chain="arbitrum",
        wallet_address="0x" + "d7" * 20,
    )
    strategy.create_market_snapshot = MagicMock(return_value=None)

    def _generate(_mode: Any, market: Any = None) -> list[Any]:
        events.append("generate")
        return []

    strategy.generate_teardown_intents = _generate
    return strategy


@pytest.fixture
def teardown_env(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Wire execute_teardown's process boundaries around one shared registry row."""
    from almanak.framework import teardown as teardown_module
    from almanak.framework.accounting import deferred_log
    from almanak.framework.runner import perp_settlement_reconciler as reconciler
    from almanak.framework.runner import runner_teardown as rt
    from almanak.framework.teardown import registry_enumeration

    events: list[str] = []
    registry_row = {"status": "open"}
    reconcile_calls: list[dict[str, Any]] = []
    deferred_records: list[Any] = []
    monkeypatch.setattr(deferred_log, "append", deferred_records.append)

    async def _fake_reconcile(
        _runner: Any,
        _strategy: Any,
        *,
        deployment_id: str,
        cycle_id: str,
        gateway_client: Any,
        chain: str | None = None,
        wallet_address: str | None = None,
    ) -> PerpSettlementReconcileOutcome:
        events.append("reconcile")
        reconcile_calls.append(
            {
                "deployment_id": deployment_id,
                "cycle_id": cycle_id,
                "gateway_client": gateway_client,
            }
        )
        # Observable contract of the settlement commit lane on a keeper-EXECUTED
        # close verdict: the durable registry row transitions open -> closed.
        registry_row["status"] = "closed"
        return PerpSettlementReconcileOutcome(
            attempted=1,
            booked=1,
            attempted_order_keys=("0x" + "20" * 32,),
            booked_order_keys=("0x" + "20" * 32,),
        )

    async def _fake_enumeration(_strategy: Any, **_kwargs: Any) -> TeardownPositionSummary:
        events.append("enumerate")
        return _summary_for(registry_row)

    manager = MagicMock(name="teardown_state_manager")
    manager.get_active_request.return_value = SimpleNamespace(
        requested_by="operator",
        teardown_id="td-vib6522",
        asset_policy=None,
        target_token=None,
    )

    monkeypatch.setattr(reconciler, "reconcile_perp_settlements", _fake_reconcile)
    monkeypatch.setattr(registry_enumeration, "resolve_open_positions_with_registry", _fake_enumeration)
    monkeypatch.setattr(teardown_module, "get_teardown_state_manager_for_runtime", lambda **_kw: manager)
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
    monkeypatch.setattr(rt, "_apply_lending_unwind_guard", lambda intents, *_a, **_kw: intents)
    monkeypatch.setattr(
        rt,
        "_load_runtime_resumable_accepted_async_state",
        AsyncMock(return_value=rt._AcceptedAsyncResumeLookup(state=None)),
    )

    return {
        "events": events,
        "registry_row": registry_row,
        "reconcile_calls": reconcile_calls,
        "deferred_records": deferred_records,
        "manager": manager,
        "monkeypatch": monkeypatch,
    }


@pytest.mark.asyncio
async def test_teardown_reconciles_settlement_before_completeness_gate(teardown_env: dict[str, Any]) -> None:
    """A decide()-lane close whose settlement is bookable must not block teardown.

    Pre-fix negative control: without Step T0.5 the reconcile never runs, the
    registry row stays 'open', the REAL completeness gate fails closed, and the
    assertions below fail (mark_failed instead of mark_completed).
    """
    runner = _make_runner()
    strategy = _make_strategy(teardown_env["events"])

    result = await execute_teardown(runner, strategy, TeardownMode.SOFT, datetime.now(UTC))

    events = teardown_env["events"]
    assert "reconcile" in events, "teardown never ran the pre-enumeration settlement reconcile tick"
    assert events.index("reconcile") < events.index("enumerate"), (
        "the reconcile tick must run BEFORE registry-reconciled enumeration reads the row"
    )
    assert events.index("reconcile") < events.index("generate"), (
        "the reconcile tick must run BEFORE the strategy generates teardown intents"
    )
    assert teardown_env["registry_row"]["status"] == "closed"
    assert teardown_env["reconcile_calls"][0]["deployment_id"] == _DEPLOYMENT_ID
    assert teardown_env["reconcile_calls"][0]["cycle_id"] == "iter-cycle-1"
    assert teardown_env["reconcile_calls"][0]["gateway_client"] is not None

    manager = teardown_env["manager"]
    manager.mark_completed.assert_called_once()
    assert manager.mark_completed.call_args.kwargs["result"] == {"reason": "no_positions"}
    manager.mark_failed.assert_not_called()
    runner._request_teardown_failure_shutdown.assert_not_called()
    assert result.status == IterationStatus.TEARDOWN
    # A clean, booked reconcile leaves no degradation record behind.
    assert teardown_env["deferred_records"] == []


@pytest.mark.asyncio
async def test_reconcile_failure_leaves_gate_fail_closed(teardown_env: dict[str, Any]) -> None:
    """A reconcile fault is warn-only AND the completeness gate still refuses.

    This is the liveness control for the test above: with the row left 'open'
    the REAL gate must fire — proving the passing case passes because the row
    was closed, never because the gate stopped looking. It also proves the new
    step is warn-only: the reconciler raising must not crash the teardown lane
    or mask the gate's own error.
    """
    from almanak.framework.runner import perp_settlement_reconciler as reconciler

    events = teardown_env["events"]

    async def _raising_reconcile(*_args: Any, **_kwargs: Any) -> PerpSettlementReconcileOutcome:
        events.append("reconcile")
        raise RuntimeError("gateway read blew up")

    teardown_env["monkeypatch"].setattr(reconciler, "reconcile_perp_settlements", _raising_reconcile)

    runner = _make_runner()
    strategy = _make_strategy(events)

    result = await execute_teardown(runner, strategy, TeardownMode.SOFT, datetime.now(UTC))

    assert "reconcile" in events
    assert teardown_env["registry_row"]["status"] == "open"
    manager = teardown_env["manager"]
    manager.mark_completed.assert_not_called()
    manager.mark_failed.assert_called_once()
    error = manager.mark_failed.call_args.kwargs["error"]
    assert "Teardown completeness check FAILED" in error
    assert "PERP" in error
    runner._request_teardown_failure_shutdown.assert_called_once()
    assert result.status == IterationStatus.STRATEGY_ERROR
    assert result.error is not None and "completeness" in result.error.lower()
    # The raise is still recorded loudly and durably (inverted failure
    # semantics): one deferred-write record carrying the exception.
    records = teardown_env["deferred_records"]
    assert len(records) == 1
    assert records[0].kind == "perp_settlement"
    assert "gateway read blew up" in records[0].error


@pytest.mark.asyncio
async def test_degraded_reconcile_outcome_is_recorded_not_discarded(
    teardown_env: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    """A degraded reconcile OUTCOME (no exception) must leave a loud, durable record.

    The reconciler reports most failures by RETURNING
    ``PerpSettlementReconcileOutcome.accounting_degraded`` — its internal
    AccountingPersistenceError catch boundary usually does not raise. With no
    registered row (e.g. the settlement that would have created it was never
    booked) the completeness gate sees nothing tracked-open and teardown
    completes "no positions" — which must NOT read as a clean exit: the
    degradation must land in an ERROR log, a deferred-write record, and the
    persisted completion payload. (Codex review of PR #3608: a discarded
    outcome produced a silent clean completion over an unbooked settlement.)
    """
    from almanak.framework.runner import perp_settlement_reconciler as reconciler

    events = teardown_env["events"]
    # No tracked-open row: the enumeration fake reads this as "nothing known".
    teardown_env["registry_row"]["status"] = "closed"

    async def _degraded_reconcile(*_args: Any, **_kwargs: Any) -> PerpSettlementReconcileOutcome:
        events.append("reconcile")
        return PerpSettlementReconcileOutcome(
            attempted=1,
            booked=0,
            attempted_order_keys=("0x" + "20" * 32,),
            degraded_reasons=("order 0x2020…: settlement commit failed",),
        )

    teardown_env["monkeypatch"].setattr(reconciler, "reconcile_perp_settlements", _degraded_reconcile)

    runner = _make_runner()
    strategy = _make_strategy(events)

    with caplog.at_level("ERROR"):
        result = await execute_teardown(runner, strategy, TeardownMode.SOFT, datetime.now(UTC))

    assert result.status == IterationStatus.TEARDOWN
    manager = teardown_env["manager"]
    manager.mark_failed.assert_not_called()
    manager.mark_completed.assert_called_once()
    completion = manager.mark_completed.call_args.kwargs["result"]
    assert completion["reason"] == "no_positions"
    assert completion["accounting_degraded"] is True
    assert "settlement commit failed" in completion["accounting_degraded_reason"]

    records = teardown_env["deferred_records"]
    assert len(records) == 1
    assert records[0].kind == "perp_settlement"
    assert records[0].intent_type == "PERP_SETTLEMENT"
    assert "settlement commit failed" in records[0].error

    assert any("pre-gate perp settlement reconciliation degraded" in message for message in caplog.messages), (
        "the degradation must be logged at ERROR, not swallowed"
    )


@pytest.mark.asyncio
async def test_no_gateway_client_is_passed_through_not_fabricated(teardown_env: dict[str, Any]) -> None:
    """Paper/dry-run without a fork: the reconciler owns the None-gateway no-op.

    ``execute_teardown`` must hand the reconciler whatever gateway the runner
    has (including None) rather than gating or fabricating one — the
    reconciler's own None-gateway early-return is its tested contract.
    """
    runner = _make_runner()
    runner._get_gateway_client = MagicMock(return_value=None)
    strategy = _make_strategy(teardown_env["events"])

    await execute_teardown(runner, strategy, TeardownMode.SOFT, datetime.now(UTC))

    assert teardown_env["reconcile_calls"], "reconcile tick must still be attempted"
    assert teardown_env["reconcile_calls"][0]["gateway_client"] is None
