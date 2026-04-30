"""Tests for ``_run_loop_helpers.capture_teardown_snapshot_with_accounting``
(VIB-3773 Phase 2).

Covers T7 + T8 from ``docs/internal/AccountingTeardown.md`` §6:

* T7 — pre + post bracket each persist a snapshot row stamped with
       ``cycle_id LIKE 'teardown-...'``.
* T8 — backend snapshot save raising in **live** mode does NOT propagate;
       outcome reports degraded; deferred-log line written.

Plus invariants:

* Cycle-id surfaces (``runner._last_cycle_id`` AND the contextvar) are
  stamped to ``teardown_cycle_id`` *during* the helper and restored on
  exit. P1-4 — ``runner_state.py:486`` reads ``runner._last_cycle_id``
  first; both must move.
* ``enable_state_persistence=False`` short-circuits cleanly.
* Generic exceptions in the snapshot path are also caught.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.accounting.deferred_log import DEFERRED_LOG_FILENAME
from almanak.framework.observability.context import (
    clear_cycle_id,
    get_cycle_id,
    set_cycle_id,
)
from almanak.framework.runner._run_loop_helpers import (
    TeardownSnapshotOutcome,
    capture_teardown_snapshot_with_accounting,
)
from almanak.framework.state.exceptions import (
    AccountingPersistenceError,
    AccountingWriteKind,
)


@pytest.fixture
def local_db_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    monkeypatch.delenv("AGENT_ID", raising=False)
    monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "state.db"))
    return tmp_path


@pytest.fixture
def fake_strategy() -> SimpleNamespace:
    return SimpleNamespace(
        strategy_id="strat-1",
        deployment_id="dep-1",
        chain="arbitrum",
        wallet_address="0xWALLET",
    )


def _make_runner(*, persistence_enabled: bool = True, live_mode: bool = True) -> MagicMock:
    runner = MagicMock(name="StrategyRunner")
    runner.config = SimpleNamespace(
        enable_state_persistence=persistence_enabled, chain="arbitrum"
    )
    runner._is_live_mode.return_value = live_mode
    runner._total_iterations = 7
    runner._last_cycle_id = ""
    return runner


# ---------------------------------------------------------------------------
# T7 — pre + post bracket persist snapshots stamped with teardown cycle_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t7_pre_and_post_bracket_persist_snapshots(
    monkeypatch: pytest.MonkeyPatch,
    fake_strategy,
    local_db_dir: Path,
):
    runner = _make_runner()

    seen_cycle_ids: list[tuple[str, str | None]] = []

    async def _fake_capture(runner_arg, strategy, *, iteration_number, force_snapshot):
        # Capture both the contextvar and the runner's _last_cycle_id at the
        # moment the writer would stamp them — this is the core invariant.
        seen_cycle_ids.append(
            (runner_arg._last_cycle_id, get_cycle_id())
        )
        # Return a truthy snapshot to signal "captured".
        return SimpleNamespace(strategy_id=strategy.strategy_id)

    monkeypatch.setattr(
        "almanak.framework.runner.runner_state.capture_portfolio_snapshot", _fake_capture
    )

    out_pre = await capture_teardown_snapshot_with_accounting(
        runner,
        fake_strategy,
        teardown_cycle_id="teardown-pre-1",
        pre_teardown=True,
    )
    out_post = await capture_teardown_snapshot_with_accounting(
        runner,
        fake_strategy,
        teardown_cycle_id="teardown-pre-1",
        pre_teardown=False,
    )

    assert isinstance(out_pre, TeardownSnapshotOutcome)
    assert out_pre.snapshot_captured is True
    assert out_pre.accounting_degraded is False
    assert out_pre.phase == "pre"
    assert out_post.snapshot_captured is True
    assert out_post.phase == "post"

    # During each call, both surfaces saw the teardown cycle id.
    assert seen_cycle_ids == [
        ("teardown-pre-1", "teardown-pre-1"),
        ("teardown-pre-1", "teardown-pre-1"),
    ]

    # No deferred-log on the happy path.
    assert not (local_db_dir / DEFERRED_LOG_FILENAME).exists()


# ---------------------------------------------------------------------------
# T8 — live AccountingPersistenceError → degraded, no raise, deferred logged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t8_live_persistence_error_degrades_does_not_raise(
    monkeypatch: pytest.MonkeyPatch,
    fake_strategy,
    local_db_dir: Path,
):
    runner = _make_runner(live_mode=True)

    async def _fail_capture(*args, **kwargs):
        raise AccountingPersistenceError(
            write_kind=AccountingWriteKind.SNAPSHOT,
            strategy_id="strat-1",
            message="snapshot save returned False",
        )

    monkeypatch.setattr(
        "almanak.framework.runner.runner_state.capture_portfolio_snapshot", _fail_capture
    )

    outcome = await capture_teardown_snapshot_with_accounting(
        runner,
        fake_strategy,
        teardown_cycle_id="teardown-fail-1",
        pre_teardown=True,
    )

    assert outcome.accounting_degraded is True
    assert outcome.snapshot_captured is False
    assert "snapshot/pre" in (outcome.degraded_reason or "")

    # Deferred-log row written.
    log = local_db_dir / DEFERRED_LOG_FILENAME
    assert log.exists()
    rows = [json.loads(line) for line in log.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["kind"] == "snapshot"
    assert rows[0]["cycle_id"] == "teardown-fail-1"
    assert rows[0]["extra"]["phase"] == "pre"


@pytest.mark.asyncio
async def test_generic_exception_in_snapshot_path_also_caught(
    monkeypatch: pytest.MonkeyPatch,
    fake_strategy,
    local_db_dir: Path,
):
    runner = _make_runner()

    async def _boom(*args, **kwargs):
        raise RuntimeError("valuer crashed")

    monkeypatch.setattr(
        "almanak.framework.runner.runner_state.capture_portfolio_snapshot", _boom
    )

    outcome = await capture_teardown_snapshot_with_accounting(
        runner,
        fake_strategy,
        teardown_cycle_id="teardown-generic-1",
        pre_teardown=False,
    )

    assert outcome.accounting_degraded is True
    assert outcome.phase == "post"
    assert "RuntimeError" in (outcome.degraded_reason or "")


# ---------------------------------------------------------------------------
# Cycle-id swap + restore (P1-4 dual surface)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cycle_id_dual_surface_swap_and_restore(
    monkeypatch: pytest.MonkeyPatch,
    fake_strategy,
    local_db_dir: Path,
):
    runner = _make_runner()
    runner._last_cycle_id = "outer-cycle"

    # Pre-set the contextvar too.
    set_cycle_id("outer-cycle")

    async def _fake_capture(runner_arg, strategy, *, iteration_number, force_snapshot):
        return SimpleNamespace(strategy_id=strategy.strategy_id)

    monkeypatch.setattr(
        "almanak.framework.runner.runner_state.capture_portfolio_snapshot", _fake_capture
    )

    try:
        await capture_teardown_snapshot_with_accounting(
            runner,
            fake_strategy,
            teardown_cycle_id="teardown-restore-1",
            pre_teardown=True,
        )
    finally:
        # Both surfaces must be restored to their pre-call values.
        assert runner._last_cycle_id == "outer-cycle"
        assert get_cycle_id() == "outer-cycle"
        clear_cycle_id()


@pytest.mark.asyncio
async def test_cycle_id_restored_to_none_when_no_outer(
    monkeypatch: pytest.MonkeyPatch,
    fake_strategy,
    local_db_dir: Path,
):
    runner = _make_runner()
    runner._last_cycle_id = ""
    clear_cycle_id()
    assert get_cycle_id() is None

    async def _fake_capture(*args, **kwargs):
        return SimpleNamespace(strategy_id="strat-1")

    monkeypatch.setattr(
        "almanak.framework.runner.runner_state.capture_portfolio_snapshot", _fake_capture
    )

    await capture_teardown_snapshot_with_accounting(
        runner,
        fake_strategy,
        teardown_cycle_id="teardown-no-outer",
        pre_teardown=True,
    )

    assert get_cycle_id() is None
    assert runner._last_cycle_id == ""


# ---------------------------------------------------------------------------
# enable_state_persistence=False short-circuits
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_persistence_disabled_short_circuits_no_writes(
    monkeypatch: pytest.MonkeyPatch,
    fake_strategy,
    local_db_dir: Path,
):
    runner = _make_runner(persistence_enabled=False)

    captured = AsyncMock()
    monkeypatch.setattr(
        "almanak.framework.runner.runner_state.capture_portfolio_snapshot", captured
    )

    outcome = await capture_teardown_snapshot_with_accounting(
        runner,
        fake_strategy,
        teardown_cycle_id="teardown-nop",
        pre_teardown=True,
    )

    assert outcome.snapshot_captured is False
    assert outcome.accounting_degraded is False
    captured.assert_not_called()
    assert not (local_db_dir / DEFERRED_LOG_FILENAME).exists()
