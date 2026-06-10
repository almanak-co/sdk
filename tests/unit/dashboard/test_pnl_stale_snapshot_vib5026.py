"""VIB-5026 regression: dashboard money tiles must read the *latest* snapshot.

The PnL/CostStack/QuantHeader loader paired
``get_snapshots_since(since=now-365d, ORDER BY timestamp ASC, LIMIT 168)``
with ``compute_pnl_summary``'s ``latest = snapshots[-1]``. Once a deployment
accrued more than 168 snapshots (~14h at the 5-min cadence), the ASC+LIMIT
window returned the OLDEST 168 rows, so ``snapshots[-1]`` was the
**168th-oldest** snapshot — the card froze ~14h after launch and grew more
stale over time.

Production proof (deployment ``d81baaea-…``, Arbitrum dual-leg LP): 187
snapshots; the 168th-oldest read total=$2.43 / cash=$2.58 (what the operator
saw) while the true latest read total=$4.93 / cash=$0.08.

The fix routes the latest-window consumers through ``get_recent_snapshots``
(newest ``limit`` rows, oldest-first) so ``[-1]`` is the true latest. These
tests reproduce the divergence on a real SQLite backend and pin the facade
delegation.
"""

from __future__ import annotations

import os
import tempfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.dashboard.quant_aggregations import compute_pnl_summary
from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager

_DEPLOYMENT_ID = "deployment:vib5026-stale-snap"
_BASE_TS = datetime(2026, 6, 10, 0, 0, 0, tzinfo=UTC)
_CAP = 168

# Era values mirror the prod incident: a "1 LP + idle wallet" era before the
# second leg opened, then a "2 LP" era after.
_ONE_LEG = (Decimal("2.43"), Decimal("2.58"))  # (open-position total, idle cash)
_TWO_LEG = (Decimal("4.93"), Decimal("0.08"))


@pytest.fixture
def temp_db_path():
    fd, path = tempfile.mkstemp(suffix=".db", prefix="vib5026_")
    os.close(fd)
    yield path
    for ext in ("", "-wal", "-shm", "-journal"):
        try:
            os.unlink(path + ext)
        except FileNotFoundError:
            pass


def _snap(i: int) -> PortfolioSnapshot:
    # First _CAP snapshots are the one-leg era; everything after is two-leg.
    total, cash = _ONE_LEG if i < _CAP else _TWO_LEG
    return PortfolioSnapshot(
        timestamp=_BASE_TS + timedelta(minutes=i),
        deployment_id=_DEPLOYMENT_ID,
        total_value_usd=total,
        available_cash_usd=cash,
        deployed_capital_usd=total,
        wallet_total_value_usd=total + cash,
        value_confidence=ValueConfidence.HIGH,
        chain="arbitrum",
        iteration_number=i,
        cycle_id=f"iter-{i}",
    )


async def _seed(store: SQLiteStore, count: int) -> None:
    for i in range(count):
        await store.save_portfolio_snapshot(_snap(i))


@pytest.mark.asyncio
async def test_recent_window_surfaces_latest_not_168th_oldest(temp_db_path):
    """Over the row cap, get_recent_snapshots[-1] is newest; the legacy
    since+ASC+LIMIT path's [-1] is the stale 168th-oldest."""
    store = SQLiteStore(SQLiteConfig(db_path=temp_db_path))
    await store.initialize()
    try:
        await _seed(store, count=_CAP + 32)  # 200 > 168 → triggers the bug

        recent = await store.get_recent_snapshots(_DEPLOYMENT_ID, limit=_CAP)
        legacy = await store.get_snapshots_since(_DEPLOYMENT_ID, since=_BASE_TS - timedelta(days=365), limit=_CAP)
    finally:
        await store.close()

    assert len(recent) == _CAP and len(legacy) == _CAP
    # Both ASC, but different windows:
    assert recent[0].timestamp < recent[-1].timestamp
    assert legacy[0].timestamp < legacy[-1].timestamp

    # FIXED path: tail is the true latest (two-leg era).
    assert recent[-1].iteration_number == _CAP + 31
    assert recent[-1].total_value_usd == _TWO_LEG[0]
    assert recent[-1].available_cash_usd == _TWO_LEG[1]

    # BUGGY path (documented): tail is the 168th-OLDEST (one-leg era).
    assert legacy[-1].iteration_number == _CAP - 1
    assert legacy[-1].total_value_usd == _ONE_LEG[0]
    assert legacy[-1].available_cash_usd == _ONE_LEG[1]


@pytest.mark.asyncio
async def test_pnl_summary_uses_recent_window_value(temp_db_path):
    """compute_pnl_summary fed the recent window reports the live position
    split; fed the legacy window it reports the frozen one (the user's bug)."""
    store = SQLiteStore(SQLiteConfig(db_path=temp_db_path))
    await store.initialize()
    try:
        await _seed(store, count=_CAP + 32)
        recent = await store.get_recent_snapshots(_DEPLOYMENT_ID, limit=_CAP)
        legacy = await store.get_snapshots_since(_DEPLOYMENT_ID, since=_BASE_TS - timedelta(days=365), limit=_CAP)
    finally:
        await store.close()

    def _summary(snaps):
        return compute_pnl_summary(
            portfolio_metrics=None,
            snapshots=snaps,
            ledger_entries=[],
            accounting_events=[],
            position_summary=None,
        )

    fixed = _summary(recent)
    buggy = _summary(legacy)

    # Dashboard "Open position NAV" == nav - available_cash (see _detail_header).
    assert fixed.available_cash_usd == _TWO_LEG[1]
    assert (fixed.nav_usd - fixed.available_cash_usd) == _TWO_LEG[0]

    # The pre-fix list reproduces exactly what the operator screenshotted.
    assert buggy.available_cash_usd == _ONE_LEG[1]
    assert (buggy.nav_usd - buggy.available_cash_usd) == _ONE_LEG[0]


@pytest.mark.asyncio
async def test_statemanager_facade_delegates_to_warm_get_recent_snapshots():
    """The StateManager facade forwards to the warm backend's
    get_recent_snapshots (the method dashboard_service now calls)."""
    calls: list[tuple[str, int]] = []
    sentinel = [object()]

    class _RecordingWarm:
        async def get_recent_snapshots(self, deployment_id: str, limit: int):
            calls.append((deployment_id, limit))
            return sentinel

    sm = StateManager.__new__(StateManager)
    sm._initialized = True
    sm._metrics = []
    sm._config = SimpleNamespace(metrics_callback=None)
    sm._warm = _RecordingWarm()  # type: ignore[assignment]

    result = await sm.get_recent_snapshots(_DEPLOYMENT_ID, limit=168)

    assert result is sentinel
    assert calls == [(_DEPLOYMENT_ID, 168)]


@pytest.mark.asyncio
async def test_statemanager_facade_returns_empty_when_warm_lacks_method():
    """A warm backend without get_recent_snapshots degrades to [] (no raise)."""
    sm = StateManager.__new__(StateManager)
    sm._initialized = True
    sm._metrics = []
    sm._config = SimpleNamespace(metrics_callback=None)
    sm._warm = SimpleNamespace()  # no get_recent_snapshots attribute

    assert await sm.get_recent_snapshots(_DEPLOYMENT_ID, limit=168) == []
