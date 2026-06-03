"""VIB-3951 — local-only teardown crash watchdog.

Proves a teardown_requests row left at status='executing' by a dead/stale
runner process is RE-QUEUED to status='pending' by the boot-time watchdog
sweep, instead of being stuck forever (the F-01 CRITICAL incident: runner
crashed mid-teardown after an on-chain WITHDRAW, status='executing' forever).

Why 'pending', not 'failed': a 'pending' row stays ``is_active=True``, so the
runner's ``should_teardown()`` re-enters teardown on boot and finishes the
unwind — preserving the teardown risk contract (never block the next
risk-reducing intent; a crash mid-unwind leaves residual on-chain risk).
Marking 'failed' (is_active=False) would force a manual operator re-trigger.

LOCAL-ONLY mechanism: the owner_pid / heartbeat_at columns live on the
SDK-owned local SQLite ``teardown_requests`` table. No Postgres / metrics_db
schema is touched (the sweep is skipped entirely in hosted mode).
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownRequest,
    TeardownStatus,
)
from almanak.framework.teardown.state_manager import TeardownStateManager

_DEAD_PID = 2_000_000_000  # implausibly high — guaranteed not a live process


def _mgr(tmp_path: Path) -> TeardownStateManager:
    return TeardownStateManager(db_path=tmp_path / "watchdog.db")


def _make_request(deployment_id: str = "MyStrat:1") -> TeardownRequest:
    return TeardownRequest(deployment_id=deployment_id, mode=TeardownMode.HARD, requested_by="cli")


def _row(mgr: TeardownStateManager, deployment_id: str) -> sqlite3.Row:
    with sqlite3.connect(str(mgr.db_path)) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute("SELECT * FROM teardown_requests WHERE deployment_id = ?", (deployment_id,)).fetchone()


def _force_executing(mgr: TeardownStateManager, deployment_id: str, *, owner_pid, heartbeat_at) -> None:
    """Put a row directly into the stuck-executing state a crash would leave."""
    with sqlite3.connect(str(mgr.db_path)) as conn:
        conn.execute(
            "UPDATE teardown_requests SET status = ?, owner_pid = ?, heartbeat_at = ? WHERE deployment_id = ?",
            (
                TeardownStatus.EXECUTING.value,
                owner_pid,
                heartbeat_at.isoformat() if heartbeat_at else None,
                deployment_id,
            ),
        )
        conn.commit()


# =============================================================================
# Schema migration
# =============================================================================


def test_watchdog_columns_present(tmp_path: Path):
    mgr = _mgr(tmp_path)
    mgr.create_request(_make_request())
    with sqlite3.connect(str(mgr.db_path)) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(teardown_requests)").fetchall()}
    assert "owner_pid" in cols
    assert "heartbeat_at" in cols


def test_migration_adds_columns_to_legacy_db(tmp_path: Path):
    """A pre-VIB-3951 DB (no owner_pid/heartbeat_at) is migrated on open."""
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE teardown_requests (
                deployment_id TEXT PRIMARY KEY, mode TEXT NOT NULL, asset_policy TEXT NOT NULL,
                target_token TEXT NOT NULL, reason TEXT, requested_at TEXT NOT NULL,
                requested_by TEXT NOT NULL, status TEXT NOT NULL, acknowledged_at TEXT,
                started_at TEXT, completed_at TEXT, current_phase TEXT,
                positions_total INTEGER DEFAULT 0, positions_closed INTEGER DEFAULT 0,
                positions_failed INTEGER DEFAULT 0, cancel_requested INTEGER DEFAULT 0,
                cancel_deadline TEXT, error_message TEXT, result_json TEXT, updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
    # Opening through the manager runs the idempotent migration.
    TeardownStateManager(db_path=db_path)
    with sqlite3.connect(str(db_path)) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(teardown_requests)").fetchall()}
    assert {"owner_pid", "heartbeat_at"} <= cols


# =============================================================================
# mark_started stamps owner + heartbeat
# =============================================================================


def test_mark_started_stamps_owner_pid_and_heartbeat(tmp_path: Path):
    import os

    mgr = _mgr(tmp_path)
    mgr.create_request(_make_request())
    mgr.acknowledge_request("MyStrat:1")
    mgr.mark_started("MyStrat:1", total_positions=3)

    row = _row(mgr, "MyStrat:1")
    assert row["status"] == TeardownStatus.EXECUTING.value
    assert row["owner_pid"] == os.getpid()
    assert row["heartbeat_at"] is not None


# =============================================================================
# Core acceptance: stuck 'executing' row from a dead process → re-queued 'pending'
# =============================================================================


def test_sweep_requeues_dead_process_executing_row(tmp_path: Path):
    """The F-01 scenario: runner died mid-teardown, status stuck at 'executing'.

    A fresh runner's boot sweep must RE-QUEUE it to 'pending' (not leave it
    stuck forever, and not mark it terminal 'failed') so the runner auto-
    re-enters teardown on boot and finishes unwinding residual on-chain risk.
    """
    mgr = _mgr(tmp_path)
    mgr.create_request(_make_request())
    # Dead owner pid, fresh heartbeat — the PID-liveness path alone must catch it.
    _force_executing(mgr, "MyStrat:1", owner_pid=_DEAD_PID, heartbeat_at=datetime.now(UTC))

    requeued = mgr.sweep_stale_executing()

    assert requeued == 1
    row = _row(mgr, "MyStrat:1")
    assert row["status"] == TeardownStatus.PENDING.value
    # Crash recorded loudly on the request reason.
    assert row["reason"] and "watchdog" in row["reason"]
    # Stays active → should_teardown() re-enters teardown on boot.
    request = mgr.get_request("MyStrat:1")
    assert request is not None and request.is_active
    # In-flight stamps + progress cleared so re-entry starts clean.
    assert row["owner_pid"] is None
    assert row["heartbeat_at"] is None
    assert row["started_at"] is None
    assert row["current_phase"] is None
    assert row["positions_closed"] == 0


def test_sweep_requeues_stale_heartbeat_row(tmp_path: Path):
    """Owner pid unknown (legacy row) + old heartbeat → swept by the time window."""
    mgr = _mgr(tmp_path)
    mgr.create_request(_make_request())
    _force_executing(
        mgr,
        "MyStrat:1",
        owner_pid=None,
        heartbeat_at=datetime.now(UTC) - timedelta(hours=2),
    )

    requeued = mgr.sweep_stale_executing()

    assert requeued == 1
    assert _row(mgr, "MyStrat:1")["status"] == TeardownStatus.PENDING.value


def test_sweep_does_not_touch_current_process_row_with_fresh_heartbeat(tmp_path: Path):
    """A current-pid row with a FRESH heartbeat is the genuine 'this process is
    mid-teardown' case — left alone."""
    import os

    mgr = _mgr(tmp_path)
    mgr.create_request(_make_request())
    _force_executing(mgr, "MyStrat:1", owner_pid=os.getpid(), heartbeat_at=datetime.now(UTC))

    failed = mgr.sweep_stale_executing()

    assert failed == 0
    assert _row(mgr, "MyStrat:1")["status"] == TeardownStatus.EXECUTING.value


def test_sweep_requeues_recycled_current_pid_stale_row(tmp_path: Path):
    """Major 1 — PID reuse: a row stamped with the CURRENT pid but a STALE
    heartbeat is a recycled-pid orphan (the prior runner crashed and the OS
    reassigned its pid to this process). It MUST be requeued, not exempted by a
    current-pid shortcut.
    """
    import os

    mgr = _mgr(tmp_path)
    mgr.create_request(_make_request())
    _force_executing(
        mgr,
        "MyStrat:1",
        owner_pid=os.getpid(),  # same pid as the sweeping process
        heartbeat_at=datetime.now(UTC) - timedelta(hours=2),  # but stale
    )

    requeued = mgr.sweep_stale_executing()

    assert requeued == 1
    assert _row(mgr, "MyStrat:1")["status"] == TeardownStatus.PENDING.value


def test_sweep_does_not_touch_fresh_live_owner(tmp_path: Path):
    """A live owner pid with a fresh heartbeat is left alone."""
    import os

    mgr = _mgr(tmp_path)
    mgr.create_request(_make_request("Other:1"))
    # Use the current pid as a stand-in for a *different* live process, but
    # exercise the not-current-process branch by spoofing via parent pid which
    # is also alive; simplest: a live pid (os.getppid) + fresh heartbeat.
    _force_executing(mgr, "Other:1", owner_pid=os.getppid(), heartbeat_at=datetime.now(UTC))

    failed = mgr.sweep_stale_executing()

    assert failed == 0
    assert _row(mgr, "Other:1")["status"] == TeardownStatus.EXECUTING.value


def test_sweep_ignores_terminal_rows(tmp_path: Path):
    """COMPLETED / FAILED / CANCELLED rows are never touched."""
    mgr = _mgr(tmp_path)
    for did, status in (("A:1", "completed"), ("B:1", "failed"), ("C:1", "cancelled")):
        mgr.create_request(_make_request(did))
        with sqlite3.connect(str(mgr.db_path)) as conn:
            conn.execute(
                "UPDATE teardown_requests SET status = ?, owner_pid = ? WHERE deployment_id = ?",
                (status, _DEAD_PID, did),
            )
            conn.commit()

    assert mgr.sweep_stale_executing() == 0


def test_heartbeat_refreshes_stamp(tmp_path: Path):
    """heartbeat() keeps an executing row out of the stale-by-time bucket."""
    import os

    mgr = _mgr(tmp_path)
    mgr.create_request(_make_request())
    _force_executing(mgr, "MyStrat:1", owner_pid=os.getpid(), heartbeat_at=datetime.now(UTC) - timedelta(hours=2))
    mgr.heartbeat("MyStrat:1")
    row = _row(mgr, "MyStrat:1")
    hb = datetime.fromisoformat(row["heartbeat_at"])
    if hb.tzinfo is None:
        hb = hb.replace(tzinfo=UTC)
    assert datetime.now(UTC) - hb < timedelta(seconds=30)
    assert row["owner_pid"] == os.getpid()


def test_sweep_returns_zero_when_no_executing_rows(tmp_path: Path):
    mgr = _mgr(tmp_path)
    mgr.create_request(_make_request())  # PENDING
    assert mgr.sweep_stale_executing() == 0


def test_requeue_cas_noop_when_snapshot_does_not_match(tmp_path: Path):
    """Major 2 — TOCTOU: _requeue_abandoned_executing is a compare-and-swap.

    When the observed snapshot (owner_pid, heartbeat_at) no longer matches the
    on-disk row (another process changed it since the sweep read it), the CAS
    UPDATE must no-op: return False, leave the row untouched, and the sweep must
    NOT count it. Here we pass a STALE snapshot heartbeat that differs from the
    row's actual (fresh) heartbeat.
    """
    import os

    mgr = _mgr(tmp_path)
    mgr.create_request(_make_request())
    fresh_hb = datetime.now(UTC)
    _force_executing(mgr, "MyStrat:1", owner_pid=os.getpid(), heartbeat_at=fresh_hb)

    # Caller's snapshot is stale relative to what's on disk now.
    stale_snapshot_hb = (fresh_hb - timedelta(hours=3)).isoformat()
    changed = mgr._requeue_abandoned_executing(
        "MyStrat:1",
        owner_pid=os.getpid(),
        heartbeat_at=stale_snapshot_hb,
    )

    assert changed is False  # CAS no-op
    # Row left exactly as it was — still executing, heartbeat intact.
    row = _row(mgr, "MyStrat:1")
    assert row["status"] == TeardownStatus.EXECUTING.value
    assert row["heartbeat_at"] == fresh_hb.isoformat()


def test_requeue_cas_succeeds_when_snapshot_matches(tmp_path: Path):
    """CAS happy path: a matching snapshot transitions the row and returns True."""
    mgr = _mgr(tmp_path)
    mgr.create_request(_make_request())
    _force_executing(mgr, "MyStrat:1", owner_pid=_DEAD_PID, heartbeat_at=None)

    # Observed snapshot exactly matches the on-disk row.
    changed = mgr._requeue_abandoned_executing("MyStrat:1", owner_pid=_DEAD_PID, heartbeat_at=None)

    assert changed is True
    row = _row(mgr, "MyStrat:1")
    assert row["status"] == TeardownStatus.PENDING.value
    assert row["owner_pid"] is None
    assert row["heartbeat_at"] is None


def test_heartbeat_is_failure_safe_at_its_own_level(tmp_path: Path, monkeypatch):
    """heartbeat() must NOT raise even when the underlying DB write fails.

    The teardown loud-but-non-blocking contract requires the heartbeat to be
    best-effort regardless of call site — a future caller that forgets to
    swallow must not reintroduce the hazard. heartbeat() catches the DB error
    itself and logs (defense in depth alongside the _commit_with_heartbeat
    wrapper's own swallow).
    """
    mgr = _mgr(tmp_path)
    mgr.create_request(_make_request())

    def _boom(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(mgr, "_stamp_owner", _boom)

    # Must not raise.
    mgr.heartbeat("MyStrat:1")


# =============================================================================
# Item 2 — heartbeat() is wired into the per-intent teardown commit path
# =============================================================================


@pytest.mark.asyncio
async def test_commit_helper_refreshes_heartbeat_per_intent(monkeypatch):
    """build_runner_helpers().commit fires a teardown heartbeat per intent.

    Proves the heartbeat() primitive is a LIVE liveness signal (called once per
    committed teardown intent), not a dead method the staleness math depends on.
    """
    from unittest.mock import AsyncMock, MagicMock

    import almanak.framework.runner.teardown_commit as tc
    import almanak.framework.teardown as teardown_pkg
    import almanak.framework.teardown.runner_helpers as rh

    # Stub the per-intent commit pipeline at its source (build_runner_helpers
    # imports it locally) so the test exercises only the heartbeat wrapper.
    commit_mock = AsyncMock(return_value="commit-outcome")
    monkeypatch.setattr(tc, "commit_teardown_intent", commit_mock)

    # Capture the heartbeat call via a fake runtime teardown manager.
    fake_manager = MagicMock()
    monkeypatch.setattr(
        teardown_pkg,
        "get_teardown_state_manager_for_runtime",
        lambda gateway_client=None: fake_manager,
    )

    runner = MagicMock()
    runner._get_gateway_client.return_value = None
    helpers = rh.build_runner_helpers(runner)

    strategy = MagicMock()
    strategy.deployment_id = "MyStrat:1"

    outcome = await helpers.commit(strategy, MagicMock(), execution_result=MagicMock())

    assert outcome == "commit-outcome"
    commit_mock.assert_awaited_once()
    fake_manager.heartbeat.assert_called_once_with("MyStrat:1")


@pytest.mark.asyncio
async def test_commit_helper_swallows_heartbeat_failure(monkeypatch):
    """A heartbeat failure must NEVER interrupt the risk-reducing commit."""
    from unittest.mock import AsyncMock, MagicMock

    import almanak.framework.runner.teardown_commit as tc
    import almanak.framework.teardown as teardown_pkg

    commit_mock = AsyncMock(return_value="ok")
    monkeypatch.setattr(tc, "commit_teardown_intent", commit_mock)
    monkeypatch.setattr(
        teardown_pkg,
        "get_teardown_state_manager_for_runtime",
        MagicMock(side_effect=RuntimeError("manager unavailable")),
    )
    import almanak.framework.teardown.runner_helpers as rh

    runner = MagicMock()
    runner._get_gateway_client.return_value = None
    helpers = rh.build_runner_helpers(runner)
    strategy = MagicMock()
    strategy.deployment_id = "MyStrat:1"

    # Must not raise — commit outcome is still returned.
    assert await helpers.commit(strategy, MagicMock(), execution_result=MagicMock()) == "ok"
