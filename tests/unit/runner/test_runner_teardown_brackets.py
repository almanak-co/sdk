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

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.observability.context import (
    clear_cycle_id,
    get_cycle_id,
    set_cycle_id,
)
from almanak.framework.runner._run_loop_helpers import TeardownSnapshotOutcome
from almanak.framework.runner.runner_teardown import execute_teardown_via_manager
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
    monkeypatch.setattr(_h, "build_teardown_manager", MagicMock(return_value=(mgr, MagicMock())))

    return {
        "commit_calls": commit_calls,
        "snapshot_calls": snapshot_calls,
        "state": state,
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
    monkeypatch.setattr(
        _h, "build_teardown_manager", MagicMock(return_value=(mgr, MagicMock()))
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
